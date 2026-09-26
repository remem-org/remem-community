package discovery

import (
	"context"
	"errors"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// Scope is the tenant and namespace one run works inside.
//
// It is a parameter rather than something read from a context, for the reason
// jobs.Queue's methods take one: a handler legitimately runs for whichever
// tenant the queue handed it, and forging a context to satisfy a convention
// would make the scope less visible rather than more.
type Scope struct {
	Tenant    tenant.ID
	Namespace tenant.Namespace
}

func (s Scope) namespace() tenant.Namespace {
	if s.Namespace == "" {
		return tenant.DefaultNamespace
	}
	return s.Namespace
}

// Deps are what the discovery handler needs.
//
// Narrow interfaces rather than internal/memory, and that is the dependency
// direction rather than fastidiousness: internal/memory is a service over
// records, it holds the enqueuer this package implements, and a cycle between
// the two is the compile error internal/lifecycle predicted in writing when it
// named this phase.
type Deps struct {
	KV     storage.KV
	Repo   record.Repo
	Index  vector.Index
	Edges  graph.Service
	Clock  clock.Clock
	Metric distance.Metric

	// Strategy decides what the candidates justify. Nil takes [NewSimilarity]
	// with the bounds below, which is the only strategy that ships.
	Strategy Strategy

	// Metrics is where a run reports what it did. Nil in a build with no
	// metrics registry, which is every test that is not about metrics.
	Metrics *obs.DiscoveryMetrics

	Threshold     float32
	TopK          int
	MaxCandidates int
}

// Discoverer is the per-tenant discovery job.
type Discoverer struct {
	deps     Deps
	strategy Strategy
	edges    *graph.Store
}

// NewDiscoverer builds the handler.
func NewDiscoverer(d Deps) (*Discoverer, error) {
	const op = "discovery.NewDiscoverer"
	switch {
	case d.KV == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("discovery needs a store"))
	case d.Repo == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("discovery needs the record repository"))
	case d.Index == nil:
		return nil, errs.E(errs.Invalid, op, errors.New(
			"discovery needs a vector index: nearest-neighbour search is what proposes candidates"))
	case d.Edges == nil:
		return nil, errs.E(errs.Invalid, op, errors.New(
			"discovery needs the graph: an edge it cannot write is work it cannot do"))
	case d.Clock == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("discovery needs a clock"))
	}
	s := d.Strategy
	if s == nil {
		s = NewSimilarity(d.Threshold, d.TopK)
	}
	return &Discoverer{deps: d, strategy: s, edges: graph.NewStore(d.KV)}, nil
}

func (d *Discoverer) topK() int {
	if d.deps.TopK > 0 {
		return d.deps.TopK
	}
	return DefaultTopK
}

func (d *Discoverer) maxCandidates() int {
	if d.deps.MaxCandidates > 0 {
		return d.deps.MaxCandidates
	}
	return MaxCandidates
}

func (d *Discoverer) metric() distance.Metric {
	if d.deps.Metric == 0 {
		return distance.L2
	}
	return d.deps.Metric
}

// Outcome is what happened to one subject, and is the metric label.
type Outcome string

const (
	// Linked means the subject gained at least one relationship.
	Linked Outcome = "linked"
	// Unlinked means it was considered and nothing met the threshold. It is an
	// ordinary answer for the first memory in an empty corpus.
	Unlinked Outcome = "unlinked"
	// Skipped means it was archived, or had nothing to compare with.
	Skipped Outcome = "skipped"
	// Missing means it was hard-deleted between the write and this run.
	Missing Outcome = "missing"
)

// Run is what one job did, reported.
type Run struct {
	Subjects   int
	Edges      int
	Candidates int
	Outcomes   map[Outcome]int
}

func (r *Run) note(o Outcome) {
	if r.Outcomes == nil {
		r.Outcomes = map[Outcome]int{}
	}
	r.Outcomes[o]++
}

// Handle implements jobs.Handler.
//
// # It checkpoints per subject, not per job
//
// A backfill job carries up to [MaxSubjects] memories and each one is its own
// vector search and its own transaction. Taking the whole list as the unit of
// work would mean an interrupted run redoes everything it finished — which is
// safe, because every subject is idempotent, and wasteful in exactly the way
// jobs.Checkpointer exists to avoid. The checkpoint is the payload encoding of
// what is left, so the cursor moves strictly forward and there is one decoder.
//
// # A vanished subject is not a failure
//
// A memory hard-deleted between the write and this run is an ordinary race. The
// subject is skipped and the run continues; failing would retry six times and
// leave a Failed row an operator has to read to discover nothing was wrong.
func (d *Discoverer) Handle(ctx context.Context, j *jobs.Job, cp jobs.Checkpointer) error {
	const op = "discovery.Handle"

	if j.Tenant == "" {
		return errs.E(errs.Invalid, op, errors.New(
			"a discovery job requires a tenant: there is no unscoped read path (Invariant 1)"))
	}

	// The checkpoint wins over the payload: it is what is left of it.
	source := j.Checkpoint
	if len(source) == 0 {
		source = j.Payload
	}
	subjects, err := DecodePayload(source)
	if err != nil {
		return err
	}

	sc := Scope{Tenant: j.Tenant, Namespace: j.Namespace}
	ctx = tenant.NewContext(ctx, j.Tenant)
	started := d.deps.Clock.Now()

	var run Run
	for i, rid := range subjects {
		if err := ctx.Err(); err != nil {
			// Shutdown or cancellation. Record where we got to and let the
			// framework decide what happens to the row.
			d.checkpoint(ctx, cp, subjects[i:])
			d.report(sc.Tenant, run, d.deps.Clock.Now().Sub(started))
			reportCounts(cp, run)
			return err
		}

		edges, cands, outcome, err := d.discover(ctx, sc, rid)
		if err != nil {
			d.checkpoint(ctx, cp, subjects[i:])
			d.report(sc.Tenant, run, d.deps.Clock.Now().Sub(started))
			reportCounts(cp, run)
			return err
		}
		run.Subjects++
		run.Edges += edges
		run.Candidates += cands
		run.note(outcome)

		if i+1 < len(subjects) {
			d.checkpoint(ctx, cp, subjects[i+1:])
		}
	}

	d.report(sc.Tenant, run, d.deps.Clock.Now().Sub(started))
	reportCounts(cp, run)
	if run.Edges > 0 {
		obs.Logger(ctx).Info("relationships discovered",
			"tenant", sc.Tenant.String(), "subjects", run.Subjects, "edges", run.Edges)
	}
	return nil
}

// reportCounts tells the job framework what this run touched, for the attempt
// history an operator reads.
//
// The unit is a subject, not a candidate: a discovery run is asked to consider
// a list of memories, and the candidates it scores to do so are the cost rather
// than the work. Changed is the edges it wrote, which is what a second run over
// the same subjects produces none of.
//
// It is called on the failure paths as well, because a run that broke after
// linking three subjects linked three subjects.
func reportCounts(cp jobs.Checkpointer, run Run) {
	if cp == nil {
		return
	}
	cp.Processed(uint64(run.Subjects))
	cp.Changed(uint64(run.Edges))
}

// checkpoint records what is left, and never fails the run.
//
// A checkpoint that could not be saved costs a resumed job some repeated work,
// and every subject is idempotent, so turning that into a failure would trade a
// completed job for a retried one.
func (d *Discoverer) checkpoint(ctx context.Context, cp jobs.Checkpointer, rest []id.ID) {
	if cp == nil || len(rest) == 0 {
		return
	}
	b, err := EncodePayload(rest)
	if err != nil {
		return
	}
	// A cancelled context cannot write, so the checkpoint is saved outside it.
	// Losing the position of a job that was interrupted is the one case where
	// the checkpoint is worth most.
	if err := cp.Save(context.WithoutCancel(ctx), b); err != nil {
		obs.Logger(ctx).Warn("a discovery run could not save its position",
			"remaining", len(rest), "error", err)
	}
}

// discover is one subject's whole pass: read it, gather, evaluate, write.
func (d *Discoverer) discover(ctx context.Context, sc Scope, rid id.ID) (edges, cands int, o Outcome, err error) {
	subject, err := d.deps.Repo.Get(ctx, rid)
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			return 0, 0, Missing, nil
		}
		return 0, 0, "", err
	}
	// An archived memory is retired from retrieval, so it neither discovers nor
	// is discovered. Growing edges out of one manufactures relationships to
	// something no ordinary read reaches.
	if subject.Fields.Archived {
		return 0, 0, Skipped, nil
	}

	candidates, err := d.gather(ctx, sc, subject)
	if err != nil {
		// A subject with no comparable vector is not work that can succeed on a
		// retry. It is skipped, loudly enough to find, rather than failing a
		// job that may carry ninety-nine healthy subjects with it.
		if errs.Is(err, errs.Invalid) {
			obs.Logger(ctx).Warn("a memory could not be considered for discovery",
				"tenant", sc.Tenant.String(), "memory", rid.String(), "reason", err)
			return 0, 0, Skipped, nil
		}
		return 0, 0, "", err
	}

	proposed, err := d.strategy.Evaluate(ctx, subject, candidates)
	if err != nil {
		return 0, len(candidates), "", err
	}
	written, err := d.write(ctx, sc, subject.ID, proposed)
	if err != nil {
		return 0, len(candidates), "", err
	}
	if written == 0 {
		return 0, len(candidates), Unlinked, nil
	}
	return written, len(candidates), Linked, nil
}

// write commits the edges a strategy proposed, minus the pairs already linked.
//
// # Why a linked pair is left entirely alone
//
// graph.Service.Add replaces. A similar_to edge written over a hand-made
// `contradicts` would be a heuristic destroying an assertion, which is what plan
// §II.10 row 8 forbids in the delete direction and is no better here. Skipping
// the pair costs one already-existing relationship and loses nothing: the two
// memories are connected either way.
//
// Rewriting even a *discovered* edge would cost something too. Its created_at
// would move on every run, so the keyspace after two runs would differ from the
// keyspace after one — and TestEveryRegisteredHandlerIsIdempotent compares
// exactly that.
//
// # The check is inside the transaction, and that is not a detail
//
// The first version read a snapshot, decided which pairs were fresh, and then
// opened a transaction to write them. That is a read-modify-write, and two of
// them race: Phase 11's end-to-end run wrote two memories together, two workers
// discovered them at once, both were told the pair was unlinked, and both wrote
// — leaving A→B and B→A one millisecond apart for a relationship that is
// symmetric.
//
// graph.Service.Linked answers through the transaction and holds the answer to
// the commit, so the loser of that race conflicts instead of writing. txn.Do
// re-runs the body, which asks again and this time is told the truth. The whole
// decision therefore lives inside the retried body; nothing is carried in from
// a view taken before it.
//
// # Both directions
//
// Rust checks the source's out-neighbours only (connection_manager.rs:155-166),
// so discovering A and later discovering B produces both A→B and B→A. Checking
// both makes similar_to cost one row rather than two, and makes discovery
// idempotent *across subjects* and not merely across runs.
func (d *Discoverer) write(ctx context.Context, sc Scope, from id.ID, proposed []graph.Edge) (int, error) {
	if len(proposed) == 0 {
		return 0, nil
	}

	// One transaction per subject. A subject's edges land together or not at
	// all, and a subject that fails does not roll back the ones before it.
	//
	// written is assigned by the body rather than accumulated across attempts,
	// because the body runs again on a conflict and the second pass may find
	// fewer pairs fresh than the first.
	written := 0
	err := txn.Do(ctx, d.deps.KV, func(tx txn.Tx) error {
		written = 0
		for _, e := range proposed {
			linked, err := d.deps.Edges.Linked(ctx, tx, from, e.To)
			if err != nil {
				return err
			}
			if linked {
				continue
			}
			if err := d.deps.Edges.Add(ctx, tx, e); err != nil {
				return err
			}
			written++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return written, nil
}

func (d *Discoverer) report(t tenant.ID, run Run, took time.Duration) {
	m := d.deps.Metrics
	if m == nil {
		return
	}
	label := t.String()
	for outcome, n := range run.Outcomes {
		m.SubjectsTotal.WithLabelValues(label, string(outcome)).Add(float64(n))
	}
	if run.Edges > 0 {
		m.EdgesCreatedTotal.WithLabelValues(label).Add(float64(run.Edges))
	}
	if run.Subjects > 0 {
		m.CandidatesConsidered.WithLabelValues(label).Observe(float64(run.Candidates) / float64(run.Subjects))
	}
	m.RunDuration.WithLabelValues(label).Observe(took.Seconds())
}
