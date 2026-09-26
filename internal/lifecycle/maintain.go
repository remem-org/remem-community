package lifecycle

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

// DefaultRunBudget is how many memories one run acts on before leaving the rest
// for the next. services/lifecycle_manager.rs:38.
//
// A bound on a single run, not on the work: whatever a run does not reach stays
// due. Without it a large backlog — a long outage, or the first run after an
// upgrade — is one unbounded pass competing with live traffic.
const DefaultRunBudget = 10_000

// SweepEffortFactor multiplies the run budget to bound candidates examined.
// services/lifecycle_manager.rs:45.
//
// In Rust it does most of the work: five sweeps share one attention time, so
// each is woken for memories the other four own and discards roughly four in
// five. Here there is one pass and every due memory has something owed to it,
// so this rarely binds — it is kept as the backstop for the one case that does
// reach it, a corpus of memories due but owed nothing because they are younger
// than a day.
const SweepEffortFactor = 8

// DefaultEventRetention is how long an audit row is kept.
//
// Rust has no event stream, so this number is Go's. Ninety days is long enough
// that "what happened to this memory last quarter" is answerable and short
// enough that a memory recalled every window does not accumulate rows forever.
//
// It is enforced per subject, by the sweep, in the transaction it was already
// opening for that memory — because the key is subject-major, so "every event
// older than T in this tenant" would be a walk of the whole event space and
// this is a bounded scan of one prefix. Every live memory is due at least
// daily, so every subject is reached.
const DefaultEventRetention = 90 * 24 * time.Hour

// sweepBatch is how many due memories one index page holds. It bounds the
// iterator's working set, not the run.
const sweepBatch = 256

// Deps are what the maintenance handler needs.
//
// It is given narrow interfaces rather than internal/memory, and that is the
// dependency direction rather than fastidiousness: internal/memory is a service
// over records that will want to consult a policy when it writes one, and a
// cycle between the two is a compile error waiting for Phase 11.
type Deps struct {
	KV       storage.KV
	Repo     record.Repo
	Slots    *schema.Slots
	Events   *events.Store
	Policies Source
	Clock    clock.Clock

	// Edges removes a deleted memory's relationships. Nil in a build with no
	// graph, and cleanup then leaves none behind because there are none.
	Edges graph.Service
	// Vectors is told after a commit that a canonical vector is gone, so the
	// approximate index can tidy its own structure. Nil for an exact index,
	// which is a scan of those vectors and has nothing to tidy.
	Vectors vector.Maintainer

	// Submit enqueues the continuation. Nil disables it, and a backlog then
	// drains one run budget per scheduled sweep instead of at worker speed.
	Submit func(ctx context.Context, j *jobs.Job) error

	// Metrics is where a run reports what it did. Nil in a build with no
	// metrics registry, which is every test that is not about metrics.
	Metrics *obs.LifecycleMetrics

	// Type is the job type the continuation is enqueued as. It is passed in
	// rather than named here because the composition root owns the durable
	// type names, and internal/lifecycle registering its own would be the
	// framework-imports-its-work cycle by another route.
	Type jobs.Type

	RunBudget      int
	EventRetention time.Duration
	RecallWindow   time.Duration
}

func (d Deps) budget() int {
	if d.RunBudget > 0 {
		return d.RunBudget
	}
	return DefaultRunBudget
}

func (d Deps) retention() time.Duration {
	if d.EventRetention > 0 {
		return d.EventRetention
	}
	return DefaultEventRetention
}

// Maintainer is the per-tenant lifecycle job.
//
// One handler, one walk, one write per memory. It replaces Rust's five sweeps,
// and the instrumentation that proves it is TestOneWalkPerTenantPerRun.
type Maintainer struct{ d Deps }

// NewMaintainer builds the handler.
func NewMaintainer(d Deps) (*Maintainer, error) {
	const op = "lifecycle.NewMaintainer"
	switch {
	case d.KV == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("the maintenance job needs a store"))
	case d.Repo == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("the maintenance job needs the record repository"))
	case d.Slots == nil:
		return nil, errs.E(errs.Invalid, op, errors.New(
			"the maintenance job needs the slot table: the due-time index is what it walks"))
	case d.Events == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("the maintenance job needs the event store"))
	case d.Clock == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("the maintenance job needs a clock"))
	}
	return &Maintainer{d: d}, nil
}

// reportCounts tells the job framework what this pass touched, for the attempt
// history an operator reads.
//
// Examined is the unit the walk paid for and Handled is the subset it wrote —
// which includes the deletions, since a cleanup is a write. Skipped is neither:
// a memory that lost a race with a user's write was examined and not changed,
// and it is still due.
func reportCounts(cp jobs.Checkpointer, run Run) {
	if cp == nil {
		return
	}
	cp.Processed(uint64(run.Examined))
	cp.Changed(uint64(run.Handled))
}

// Run is one pass over a tenant's due memories, reported.
type Run struct {
	// Examined is every candidate the walk looked at, owed something or not.
	Examined int
	// Handled is the memories this run wrote, deleted or folded a recall into.
	Handled int
	// Deleted is how many were removed by cleanup.
	Deleted int
	// Skipped is how many lost a race with a concurrent write and were left
	// due. A background pass must never make a user's write fail.
	Skipped int
	// Exhausted means the walk reached the end of the due range.
	Exhausted bool
	// Next is where the walk stopped, for a continuation.
	Next *attr.Position
	// Transitions counts what happened, by event kind. It is what the metrics
	// are published from and what a test asserts against without reading the
	// event stream back.
	Transitions map[events.Kind]int
}

// Handle implements jobs.Handler.
//
// # It takes the instant once
//
// One `now` for the whole run, read here and passed down. A pass that read the
// clock per memory would make two memories in one run disagree about what day
// it is, and it is what Invariant 8 forbids.
//
// # A run that spends its budget enqueues its own continuation
//
// Every record written before Phase 10 has `next_attention_at = 0` — due now —
// so the first sweep after an upgrade meets the whole corpus. At the sweep
// interval alone a million memories would take a hundred hours to become
// scheduled, decaying nothing meanwhile. The continuation carries the position
// the run stopped at, so the backlog drains at worker speed and the cursor
// moves strictly forward: it terminates because the range is finite and each
// run starts past where the last one stopped.
func (m *Maintainer) Handle(ctx context.Context, j *jobs.Job, cp jobs.Checkpointer) error {
	const op = "lifecycle.Handle"

	if j.Tenant == "" {
		return errs.E(errs.Invalid, op, errors.New(
			"a maintenance job requires a tenant: there is no unscoped write path (Invariant 1)"))
	}

	// Resume from this job's own checkpoint if it was interrupted, and
	// otherwise from where the run that enqueued it stopped. The two are the
	// same kind of position and differ only in which failure they cover: a
	// crash mid-run, and a budget spent.
	from, err := decodePosition(j.Checkpoint)
	if err != nil {
		return err
	}
	if from == nil {
		if from, err = decodePosition(j.Payload); err != nil {
			return err
		}
	}

	started := m.d.Clock.Now()
	run, err := m.Sweep(ctx, j.Tenant, j.Namespace, from, cp)
	// Reported before the error is returned, because a pass that broke halfway
	// did the half it finished and its attempt history has to say so. Sweep
	// returns the run it had reached either way.
	reportCounts(cp, run)
	if err != nil {
		return err
	}

	log := obs.Logger(ctx).With("tenant", j.Tenant.String())
	m.report(j.Tenant, run, m.d.Clock.Now().Sub(started))
	if run.Handled > 0 || run.Deleted > 0 {
		log.Info("a lifecycle pass completed",
			"examined", run.Examined, "handled", run.Handled,
			"deleted", run.Deleted, "skipped", run.Skipped)
	}

	if run.Exhausted || run.Next == nil || m.d.Submit == nil {
		return nil
	}
	// Work is still due. Enqueue one successor from where this stopped.
	payload, err := encodePosition(run.Next)
	if err != nil {
		return err
	}
	next := &jobs.Job{
		Tenant:      j.Tenant,
		Namespace:   j.Namespace,
		Type:        m.d.Type,
		Payload:     payload,
		MaxAttempts: j.MaxAttempts,
		Priority:    j.Priority,
		RunAt:       m.d.Clock.Now(),
	}
	if err := m.d.Submit(ctx, next); err != nil {
		// A backlog that cannot enqueue its continuation still drains, one run
		// budget per scheduled sweep. Failing the job that just did ten
		// thousand memories' work would undo none of it and retry all of it.
		log.Warn("a lifecycle pass could not enqueue its continuation; "+
			"the backlog will drain at the sweep interval instead", "error", err)
		return nil
	}
	log.Info("a lifecycle pass spent its budget and enqueued a continuation",
		"job_id", next.ID.String(), "handled", run.Handled)
	return nil
}

// report publishes what a run did.
//
// The backlog is what an operator alerts on: it is what this run left behind,
// and zero is the healthy steady state. A run that is exhausted left nothing,
// so it reports zero rather than nothing at all — a gauge that stops being
// written looks the same as a gauge nobody is scraping.
func (m *Maintainer) report(t tenant.ID, run Run, took time.Duration) {
	if m.d.Metrics == nil {
		return
	}
	label := t.String()
	m.d.Metrics.SweepDuration.WithLabelValues(label).Observe(took.Seconds())

	backlog := 0.0
	if !run.Exhausted {
		// What is known is that at least this many were due and not reached:
		// the walk stopped with the range non-empty. Counting the rest would
		// mean a second walk of exactly the range this run declined to do.
		backlog = float64(run.Examined - run.Handled)
		if backlog < 1 {
			backlog = 1
		}
	}
	m.d.Metrics.DueBacklog.WithLabelValues(label).Set(backlog)

	for kind, n := range run.Transitions {
		m.d.Metrics.TransitionsTotal.WithLabelValues(label, string(kind)).Add(float64(n))
	}
}

// Sweep walks one tenant's due memories once.
//
// It is exported for the reason jobs.Pool.Poll is: it is the unit Handle
// performs, and a test that drives one sweep is far more legible than one that
// advances a clock and hopes.
func (m *Maintainer) Sweep(ctx context.Context, t tenant.ID, ns tenant.Namespace,
	from *attr.Position, cp jobs.Checkpointer) (Run, error) {
	const op = "lifecycle.Sweep"

	if ns == "" {
		ns = tenant.DefaultNamespace
	}
	// The tenant travels in the context because every read below is a scoped
	// read; the job carried it as a parameter, and this is where it turns back
	// into a scope.
	ctx = tenant.NewContext(ctx, t)

	now := m.d.Clock.Now().UTC()
	policies := m.d.Policies
	table, _ := NewPolicies(nil)
	if policies != nil {
		table = policies.Policies(ctx, t)
	}

	budget := m.d.budget()
	effort := budget * SweepEffortFactor
	run := Run{Next: from, Transitions: map[events.Kind]int{}}

	for run.Handled < budget {
		page, err := attr.Run(ctx, m.d.KV, attr.Select{
			Tenant:    t,
			Namespace: ns,
			Slots:     m.d.Slots,
			Slot:      attr.SlotNextAttentionAt,
			// Due at or before now. It is a predicate on the ordering slot, so
			// it narrows the walk itself rather than being settled per row —
			// which is what makes this proportional to what is due rather than
			// to what exists.
			Preds:  attr.Preds{attr.Range(attr.SlotNextAttentionAt, nil, ptr(attr.U64(uint64(now.UnixMilli()))), false, true)},
			After:  run.Next,
			Limit:  min(sweepBatch, budget-run.Handled),
			Effort: max(effort-run.Examined, 1),
		})
		if err != nil {
			return run, errs.E(errs.KindOf(err), op, err)
		}
		run.Examined += page.Examined
		if page.Next != nil {
			run.Next = page.Next
		}

		for _, rid := range page.IDs {
			acted, deleted, notes, err := m.visit(ctx, table, t, ns, rid, now)
			switch {
			case errs.Is(err, errs.Conflict):
				// A user wrote this memory while the pass was deciding about
				// it. The conditional commit refused, nothing landed, and
				// next_attention_at is unmoved — so it is still due and the
				// next run takes it. A background pass must never make a
				// user's write fail.
				run.Skipped++
			case errs.Is(err, errs.NotFound):
				// Deleted underneath us. The index entry catches up.
			case err != nil:
				return run, err
			case acted:
				run.Handled++
				if deleted {
					run.Deleted++
				}
				for _, n := range notes {
					run.Transitions[n.Kind]++
				}
			}
			if run.Handled >= budget {
				break
			}
		}

		if cp != nil && run.Next != nil {
			cursor, err := encodePosition(run.Next)
			if err != nil {
				return run, err
			}
			if err := cp.Save(ctx, cursor); err != nil {
				return run, err
			}
		}

		if page.Exhausted {
			run.Exhausted = true
			return run, nil
		}
		if run.Examined >= effort {
			return run, nil
		}
		if err := ctx.Err(); err != nil {
			return run, errs.E(errs.Unavailable, op, err)
		}
	}
	return run, nil
}

// visit folds, decides and writes one memory, in one transaction.
//
// One transaction per memory rather than one per batch. A batch would be fewer
// commits and would make a single conflicting memory discard the work done for
// the other two hundred and fifty-five — and the conflict is the ordinary case,
// because the memories a sweep touches are the memories users touch.
func (m *Maintainer) visit(ctx context.Context, table *Policies, t tenant.ID, ns tenant.Namespace,
	rid id.ID, now time.Time) (acted, deleted bool, notes []Note, err error) {
	const op = "lifecycle.visit"

	rec, err := m.d.Repo.Get(ctx, rid)
	if err != nil {
		return false, false, nil, err
	}
	scope := events.Scope{Tenant: t, Namespace: ns, Subject: rid}

	folded, err := Fold(ctx, m.d.Events, m.d.KV, scope, rec)
	if err != nil {
		return false, false, nil, err
	}
	res := Apply(table, rec, now)
	trimBefore := now.Add(-m.d.retention())

	if !res.Changed && !res.Delete && folded == 0 {
		// Owed nothing. Nothing is written — not even a corrected schedule,
		// which would cost a record rewrite to save a row read. The memory
		// stays due and the next run reads its row again, which is the
		// cheapest read in the system.
		return false, false, nil, nil
	}

	tx := txn.New(m.d.KV)
	defer tx.Close()

	if res.Delete {
		if err := m.d.Repo.Delete(ctx, tx, rid); err != nil {
			return false, false, nil, err
		}
		if m.d.Edges != nil {
			if _, err := m.d.Edges.RemoveEvery(ctx, tx, rid); err != nil {
				return false, false, nil, errs.E(errs.KindOf(err), op, err)
			}
		}
		// The whole stream goes, and the row saying why is written after it —
		// so the one event that survives a hard delete is the one that
		// explains it.
		if _, err := m.d.Events.DeleteSubject(ctx, tx, scope); err != nil {
			return false, false, nil, err
		}
	} else {
		if err := m.d.Repo.Put(ctx, tx, rec); err != nil {
			return false, false, nil, err
		}
		if _, err := m.d.Events.Trim(ctx, tx, scope, trimBefore, sweepBatch); err != nil {
			return false, false, nil, err
		}
	}

	for _, n := range res.Notes {
		if _, err := m.d.Events.Append(ctx, tx, events.Event{
			Tenant: t, Namespace: ns, Subject: rid, At: now,
			Kind: n.Kind, Actor: n.Actor, Reason: n.Reason,
			Before: n.Before, After: n.After,
		}); err != nil {
			return false, false, nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, false, nil, err
	}

	if res.Delete && m.d.Vectors != nil {
		// After the commit, never inside it. The canonical vector went with
		// the record, so the memory is already unfindable; this tidies the
		// approximate index's own structure and nothing depends on it landing.
		_ = m.d.Vectors.Removed(ctx, t, rid)
	}
	return true, res.Delete, res.Notes, nil
}

// encodePosition is <value length><value><record id>.
//
// It is not a storage key, for the reason the attribute backfill's cursor is
// not: a key would tie the resume point to the key encoding's version, so a
// key-encoding migration running beside a sweep would leave a cursor neither
// could read.
func encodePosition(p *attr.Position) ([]byte, error) {
	if p == nil {
		return nil, nil
	}
	out := make([]byte, 0, 2+len(p.Value)+16)
	out = binary.AppendUvarint(out, uint64(len(p.Value)))
	out = append(out, p.Value...)
	return append(out, p.ID[:]...), nil
}

func decodePosition(b []byte) (*attr.Position, error) {
	const op = "lifecycle.decodePosition"
	if len(b) == 0 {
		return nil, nil
	}
	n, w := binary.Uvarint(b)
	if w <= 0 || uint64(len(b[w:])) < n+16 {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"a lifecycle sweep's saved position will not parse; it cannot be resumed from"))
	}
	rid, err := id.FromBytes(b[w+int(n):])
	if err != nil {
		return nil, errs.E(errs.Corruption, op, fmt.Errorf(
			"a lifecycle sweep's saved position holds a malformed record id"))
	}
	return &attr.Position{Value: append([]byte(nil), b[w:w+int(n)]...), ID: rid}, nil
}

func ptr[T any](v T) *T { return &v }
