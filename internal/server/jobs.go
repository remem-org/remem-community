package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

// The built-in job types.
//
// The names are durable: each one is written into every row of that type, and a
// retired name stays retired.
const (
	// TypeVectorRebuild rebuilds the approximate vector index for a tenant.
	TypeVectorRebuild jobs.Type = "vector.rebuild"
	// TypeTextRebuild rebuilds the keyword index for a tenant.
	TypeTextRebuild jobs.Type = "text.rebuild"
	// TypeGraphRebuild rebuilds the derived in-edge index for a tenant.
	TypeGraphRebuild jobs.Type = "graph.rebuild_in"
	// TypeAttrRebuild reprojects a tenant's attribute rows and slot entries
	// from its record bodies. Before Phase 13 only `remem-admin rebuild --index
	// attr` could, which needs a stopped server.
	TypeAttrRebuild jobs.Type = "attr.rebuild"
	// TypeJobsReap removes finished job rows past the retention.
	TypeJobsReap jobs.Type = "jobs.reap"
	// TypeLifecycleMaintain is the per-tenant lifecycle pass: expiry,
	// promotion, decay, archival and cleanup, in one walk of the due-time
	// index. It replaces the five sweeps Rust runs.
	TypeLifecycleMaintain jobs.Type = "lifecycle.maintain"
	// TypeDiscoverySimilar finds the relationships a memory justifies. It is
	// enqueued in the same transaction as the write that created the memory,
	// which is what makes it the thing Rust's bounded channel dropped.
	TypeDiscoverySimilar jobs.Type = "discovery.similar"
)

// rebuildJobFor names the job that rebuilds a derived key space.
//
// TestEveryDerivedIndexHasARebuildPath walks keys.AllSpaces against this and
// against the registry, so a derived space with no job — or a job named here and
// never registered — fails by name rather than waiting for the first operator
// with a damaged index and no way to repair it on a running server.
func rebuildJobFor(s keys.Space) (jobs.Type, bool) {
	switch s {
	case keys.SpaceVectorIndex:
		return TypeVectorRebuild, true
	case keys.SpaceText:
		return TypeTextRebuild, true
	case keys.SpaceEdgeIn:
		return TypeGraphRebuild, true
	case keys.SpaceAttrRow, keys.SpaceAttrIndex:
		// One job for both: the slot entries are written by the same Stage
		// call that writes the row they index.
		return TypeAttrRebuild, true
	}
	return "", false
}

// registerBuiltinJobs registers the handlers this server runs.
//
// # Why they live in the composition root
//
// The three rebuilds and the reaper are each a closure two lines long over a
// method that already exists. Putting them in internal/jobs would make the job
// framework import every index in the system, and internal/discovery — which
// imports jobs to enqueue — would close the cycle. Putting them in
// internal/vector, internal/text and internal/graph would make three index
// packages depend on the job framework to gain nothing. They are wiring, so
// they live where the wiring is.
//
// # All six are idempotent, and all six are so by construction
//
// The three rebuilds read canonical rows and write only derived ones, so
// running one twice produces the same index as running it once — that is
// Invariant 3 in operational form. The reaper deletes rows that are already
// past their retention, and deleting an absent row is not an error. The
// lifecycle pass decides from timestamps it advances itself: a second run at
// the same instant finds nothing owed, because whole-day arithmetic makes a
// pass under a day a no-op. Discovery leaves a pair that is already linked in
// either direction entirely alone, so a second run writes nothing — not even a
// refreshed created_at. TestEveryRegisteredHandlerIsIdempotent holds all six to
// it, by comparing the whole keyspace.
//
// The lifecycle and discovery handlers are the exceptions to "each is a closure
// two lines long". Both are real domain logic, both live in their own packages,
// and both import internal/jobs to be a handler and to enqueue — the direction
// the boundary guard permits. The reverse would make the framework depend on
// retention policies and on the graph, and would close a cycle.
func (d *deps) registerBuiltinJobs() error {
	entries := []jobs.Entry{{
		Type:        TypeVectorRebuild,
		Description: "rebuild the approximate vector index for this tenant from its canonical vectors",
		Handler:     jobs.HandlerFunc(d.rebuildVectors),
		// Two attempts, not six. A rebuild that failed twice has met something
		// a third pass will meet as well, and it is expensive enough that
		// retrying it four more times is worse than leaving it Failed where an
		// operator can see it — which is exactly what the state is for.
		MaxAttempts: 2,
	}, {
		Type:        TypeTextRebuild,
		Description: "rebuild the keyword index for this tenant from its record bodies",
		Handler:     jobs.HandlerFunc(d.rebuildText),
		MaxAttempts: 2,
	}, {
		Type:        TypeGraphRebuild,
		Description: "rebuild the derived in-edge index for this tenant from its canonical out-edges",
		Handler:     jobs.HandlerFunc(d.rebuildGraph),
		MaxAttempts: 2,
	}, {
		Type:        TypeAttrRebuild,
		Description: "reproject the attribute rows and slot index for this tenant from its record bodies",
		Handler:     jobs.HandlerFunc(d.rebuildAttrs),
		MaxAttempts: 2,
	}, {
		Type: TypeLifecycleMaintain,
		Description: "expire, promote, decay, archive and clean up this tenant's memories, " +
			"and fold in the recalls recorded since the last pass",
		Handler: d.maintainer,
		Every:   d.cfg.Lifecycle.SweepInterval,
		// Six attempts, unlike the rebuilds. A lifecycle pass that failed is a
		// pass that did part of its work and stopped; retrying is how the rest
		// of it happens, and every operation in it is idempotent — a memory
		// already archived is not archived twice, and decay is a function of
		// timestamps the pass itself advances.
	}, {
		Type: TypeDiscoverySimilar,
		Description: "find and record the relationships the memories this job names justify, " +
			"by similarity to the rest of the tenant's corpus",
		Handler: d.discoverer,
		// Registered whether or not discovery.enabled is set. Turning discovery
		// off stops new work being enqueued; it must not strand the durable
		// rows already queued, because a worker meeting an unregistered type
		// has nowhere to put the job.
		//
		// Six attempts, like the lifecycle pass and unlike the rebuilds. A
		// discovery run that failed halfway did part of its work and
		// checkpointed; retrying is how the rest of it happens, and every
		// subject is idempotent — a pair already linked is left alone.
		//
		// Not recurring. Discovery is enqueued by the write that created the
		// work and by `remem-admin discovery backfill`; a periodic sweep over
		// every memory would redo a corpus's worth of vector searches to write
		// nothing, since the second pass over an unchanged memory finds the
		// same neighbours already linked.
	}, {
		Type:        TypeJobsReap,
		Description: "remove finished job rows older than the retention",
		Handler:     jobs.HandlerFunc(d.reapJobs),
		Every:       d.cfg.Jobs.Retention / 4,
		// Housekeeping yields to work a user is waiting for.
		Priority: -100,
	}}

	for _, e := range entries {
		if err := d.registry.Register(e); err != nil {
			return err
		}
	}
	return nil
}

// rebuildVectors repairs the approximate index from the canonical vectors.
//
// It is what the "the vector index needs rebuilding" warning used to only log.
// The rebuild reads canonical vectors and writes only node records, so it
// cannot lose a memory: its input is not among its outputs.
//
// The job's checkpoint is the rebuild's cursor, so a rebuild interrupted by a
// crash, a lapsed lease or a shutdown continues where it stopped on the next
// attempt rather than starting the tenant again.
func (d *deps) rebuildVectors(ctx context.Context, j *jobs.Job, cp jobs.Checkpointer) error {
	if d.vectors == nil {
		return fmt.Errorf("this build has no vector index to rebuild")
	}
	return d.vectors.Rebuild(ctx, j.Tenant, vector.NewStore(d.kv),
		vector.WithProgress(j.Checkpoint, cp.Save))
}

// rebuildText repairs the keyword index from the canonical record bodies.
//
// An interrupted run leaves the rebuild marker set, so every keyword search for
// that tenant keeps reporting itself incomplete until a later run finishes —
// which is the same disposition `remem-admin text rebuild` has, and the reason
// this is a job rather than something a search does for itself.
func (d *deps) rebuildText(ctx context.Context, j *jobs.Job, _ jobs.Checkpointer) error {
	if d.texts == nil {
		return fmt.Errorf("this build has keyword search turned off, so there is no index to rebuild")
	}
	return d.texts.Rebuild(ctx, d.kv, j.Tenant, j.Namespace, record.NewContentSource(d.kv))
}

// rebuildGraph repairs the derived in-edge index from the canonical out-edges.
func (d *deps) rebuildGraph(ctx context.Context, j *jobs.Job, cp jobs.Checkpointer) error {
	written, err := graph.RebuildIn(ctx, d.kv, graph.Scope{Tenant: j.Tenant, Namespace: j.Namespace})
	// The rebuild's unit is an in-edge, and every one it writes is one it
	// changed: the whole index was cleared first. Nothing reports a processed
	// count, because the out-edges it read are not returned and a number
	// invented here would look exact and not be.
	cp.Changed(uint64(written))
	return err
}

// rebuildAttrs reprojects the attribute rows and their slot entries.
//
// It restages without clearing, so a listing running beside it never sees a
// tenant with no rows: an entry is replaced in the transaction that writes its
// successor.
func (d *deps) rebuildAttrs(ctx context.Context, j *jobs.Job, cp jobs.Checkpointer) error {
	written, err := attr.RebuildRows(ctx, d.kv, j.Tenant, j.Namespace)
	cp.Changed(uint64(written))
	return err
}

// reapJobs removes finished job rows past the retention.
//
// It bounds its own work rather than deleting everything due at once: a tenant
// with a month of unreaped history should not spend one worker for minutes to
// catch up, and the next run continues from where this one stopped, because
// "the oldest finished rows" is what the scan returns either way.
func (d *deps) reapJobs(ctx context.Context, j *jobs.Job, cp jobs.Checkpointer) error {
	const perRun = 5000
	cutoff := d.clk.Now().Add(-d.cfg.Jobs.Retention)

	removed, err := d.queue.Reap(ctx, j.Tenant, cutoff, perRun)
	if err != nil {
		return err
	}
	// The attempt history is bounded by the same retention and by the same
	// reaper. It is the one job space whose row count grows with time rather
	// than with work outstanding, so a second thing that removed it on a
	// second schedule would be a second number an operator has to know.
	runs, err := d.queue.ReapRuns(ctx, j.Tenant, cutoff, perRun)
	if err != nil {
		return err
	}
	// Expired pauses are honoured on every read, so removing them is
	// housekeeping rather than correctness — without it a tenant keeps one
	// dead row per pause anybody ever set.
	pauses, err := d.queue.ReapPauses(ctx, j.Tenant, cutoff, perRun)
	if err != nil {
		return err
	}

	// Everything this handler touches it deletes, so processed and changed are
	// the same number and both are reported rather than one being left to be
	// inferred from the other.
	total := uint64(removed + runs + pauses)
	cp.Processed(total)
	cp.Changed(total)
	if total > 0 {
		obs.Logger(ctx).Info("finished job rows removed",
			"tenant", j.Tenant.String(), "jobs", removed, "attempts", runs, "pauses", pauses)
	}
	return nil
}

// repairs turns "this tenant's index is damaged" into a durable job.
//
// # Why a read path may ask for this at all
//
// Neither trigger fires unless the tenant is already degraded: the vector index
// reports damage it has already worked around, and the text marker means an
// earlier rebuild was interrupted. The rebuild does not cause the degradation,
// it ends it. Declining to schedule it would leave a tenant answering
// incompletely until somebody read a log line.
//
// # Why it does not do the work here
//
// A rebuild started inside the search that discovered the damage charges one
// user for everyone's repair. Enqueuing is cheap, but it is still a scan and a
// write, and this is called from a search — so the read path pays one map
// lookup and a goroutine does the rest.
//
// Dropping a duplicate request is safe and is not the bounded-channel defect
// this framework replaces: the request is "repair this tenant", it is
// idempotent, and the next search raises it again. What is never dropped is
// work somebody asked for.
type repairs struct {
	queue *jobs.Queue
	clk   clock.Clock

	// asked remembers when each tenant and type last produced an enqueue, so a
	// tenant searching in a loop does not start a goroutine per search.
	mu    sync.Mutex
	asked map[repairKey]time.Time

	wg   sync.WaitGroup
	stop chan struct{}
	once sync.Once
}

type repairKey struct {
	tenant tenant.ID
	typ    jobs.Type
}

// repairEvery is how often one tenant and type may produce an enqueue.
//
// It is a bound on goroutines, not on repairs: the durable check below is what
// stops a second job, and this only stops a search loop from starting a
// goroutine per request while one is already in flight.
const repairEvery = time.Minute

func newRepairs(q *jobs.Queue, clk clock.Clock) *repairs {
	return &repairs{queue: q, clk: clk, asked: map[repairKey]time.Time{}, stop: make(chan struct{})}
}

// schedule asks for a rebuild, without blocking the caller.
func (r *repairs) schedule(typ jobs.Type, t tenant.ID, reason string) {
	if r == nil || r.queue == nil {
		return
	}
	now := r.clk.Now()
	key := repairKey{tenant: t, typ: typ}

	r.mu.Lock()
	last, seen := r.asked[key]
	if seen && now.Sub(last) < repairEvery {
		r.mu.Unlock()
		return
	}
	r.asked[key] = now
	select {
	case <-r.stop:
		r.mu.Unlock()
		return
	default:
	}
	r.wg.Add(1)
	r.mu.Unlock()

	go func() {
		defer r.wg.Done()
		ctx := context.Background()
		log := obs.Logger(ctx).With("tenant", t.String(), "job_type", typ.String())

		// The durable check: one outstanding repair per tenant and type. Two
		// concurrent searches that both got past the in-memory gate must not
		// produce two rebuilds of the same index.
		outstanding, err := r.queue.HasOutstanding(ctx, t, typ)
		if err != nil {
			log.Warn("could not check for an outstanding index repair", "error", err)
			return
		}
		if outstanding {
			return
		}
		job := &jobs.Job{Tenant: t, Namespace: tenant.DefaultNamespace, Type: typ, MaxAttempts: 2}
		if err := r.queue.Submit(ctx, job); err != nil {
			log.Warn("could not schedule an index repair", "error", err, "reason", reason)
			return
		}
		log.Info("an index repair has been scheduled", "job_id", job.ID.String(), "reason", reason)
	}()
}

// close stops accepting requests and waits for the ones in flight, so that
// nothing this owns outlives the server (spec §57).
func (r *repairs) close() {
	if r == nil {
		return
	}
	r.once.Do(func() { close(r.stop) })
	r.wg.Wait()
}

// vectorRebuilder is the hook the approximate index reports damage through.
func (r *repairs) vectorRebuilder() vector.Rebuilder {
	return vector.RebuilderFunc(func(t tenant.ID, reason string) {
		obs.Logger(context.Background()).Warn("the vector index needs rebuilding",
			"tenant", t.String(), "reason", reason)
		r.schedule(TypeVectorRebuild, t, reason)
	})
}

// textRebuilder is the hook the keyword index reports incompleteness through.
func (r *repairs) textRebuilder() text.Rebuilder {
	return text.RebuilderFunc(func(t tenant.ID, reason string) {
		r.schedule(TypeTextRebuild, t, reason)
	})
}

// textReindexer queues a keyword-index rebuild for a migration, as a
// text.rebuild job staged in the migration's own transaction.
//
// One outstanding rebuild per tenant is enough, so a tenant that already has
// one is left alone: a directory being repaired when it was upgraded does not
// rebuild twice.
type textReindexer struct{ queue *jobs.Queue }

var _ schema.TextReindexer = textReindexer{}

func (r textReindexer) StageTextRebuild(ctx context.Context, tx txn.Tx, t tenant.ID) error {
	outstanding, err := r.queue.HasOutstanding(ctx, t, TypeTextRebuild)
	if err != nil || outstanding {
		return err
	}
	return r.queue.Enqueue(ctx, tx, &jobs.Job{
		Tenant: t, Namespace: tenant.DefaultNamespace, Type: TypeTextRebuild, MaxAttempts: 2,
	})
}
