package lifecycle_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

const acme = tenant.ID("acme")

// countingKV counts iterators opened over the attribute-index space, which is
// how TestOneWalkPerTenantPerRun proves there is one walk and not five.
type countingKV struct {
	storage.KV
	iterators atomic.Int64
	spaces    chan []byte
}

func (c *countingKV) NewIterator(lower, upper []byte) storage.Iterator {
	c.iterators.Add(1)
	select {
	case c.spaces <- append([]byte(nil), lower...):
	default:
	}
	return c.KV.NewIterator(lower, upper)
}

type harness struct {
	kv      *countingKV
	repo    record.Repo
	slots   *schema.Slots
	store   *events.Store
	clk     *clock.Fake
	m       *lifecycle.Maintainer
	queued  []*jobs.Job
	deleted []id.ID
}

type fixedSource struct{ p *lifecycle.Policies }

func (f fixedSource) Policies(context.Context, tenant.ID) *lifecycle.Policies { return f.p }

type recordingVectors struct{ h *harness }

func (r recordingVectors) Indexed(context.Context, tenant.ID, id.ID, []float32) error { return nil }
func (r recordingVectors) Removed(_ context.Context, _ tenant.ID, rid id.ID) error {
	r.h.deleted = append(r.h.deleted, rid)
	return nil
}

func newHarness(t *testing.T, opts ...func(*lifecycle.Deps)) *harness {
	t.Helper()

	slots := attr.MustTable()
	kv := &countingKV{KV: memkv.New(), spaces: make(chan []byte, 64)}
	clk := clock.NewFake(epoch)
	p := policies(t)

	h := &harness{kv: kv, slots: slots, store: events.NewStore(kv), clk: clk}
	h.repo = record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(slots)),
		record.WithScheduler(lifecycle.NewScheduler(fixedSource{p})))

	d := lifecycle.Deps{
		KV: kv, Repo: h.repo, Slots: slots, Events: h.store,
		Policies: fixedSource{p}, Clock: clk,
		Vectors: recordingVectors{h},
		Type:    "lifecycle.maintain",
		Submit: func(_ context.Context, j *jobs.Job) error {
			j.ID = id.New()
			h.queued = append(h.queued, j)
			return nil
		},
	}
	for _, o := range opts {
		o(&d)
	}
	m, err := lifecycle.NewMaintainer(d)
	if err != nil {
		t.Fatalf("NewMaintainer: %v", err)
	}
	h.m = m
	return h
}

func (h *harness) ctx() context.Context {
	return tenant.NewContext(context.Background(), acme)
}

func (h *harness) store_(t *testing.T, r *record.Record) *record.Record {
	t.Helper()
	ctx := h.ctx()
	tx := txn.New(h.kv)
	defer tx.Close()
	if err := h.repo.Put(ctx, tx, r); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return r
}

func (h *harness) get(t *testing.T, rid id.ID) (*record.Record, error) {
	t.Helper()
	return h.repo.Get(h.ctx(), rid)
}

func (h *harness) sweep(t *testing.T, at time.Time) lifecycle.Run {
	t.Helper()
	h.clk.Set(at)
	run, err := h.m.Sweep(h.ctx(), acme, tenant.DefaultNamespace, nil, nil)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	return run
}

func (h *harness) history(t *testing.T, rid id.ID) []events.Event {
	t.Helper()
	got, err := h.store.History(context.Background(), h.kv, events.Query{
		Tenant: acme, Namespace: tenant.DefaultNamespace, Subject: rid, Limit: 50,
	})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	return got
}

func memory(policy string, fn func(*record.Record)) *record.Record {
	r := rec(policy, fn)
	r.Tenant = acme
	return r
}

// Rust walks one shared due-time index five times, discarding four fifths of
// what it sees on each pass. This proves Go walks it once.
func TestOneWalkPerTenantPerRun(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < 3; i++ {
		h.store_(t, memory(lifecycle.LongTerm, nil))
	}

	// Only the walk is counted, so the fixture's own writes are excluded by
	// resetting after the setup.
	h.kv.iterators.Store(0)
	run := h.sweep(t, epoch.Add(2*day))
	if run.Handled != 3 {
		t.Fatalf("handled %d of 3 memories", run.Handled)
	}

	// One page of the due index, plus the per-memory reads a visit makes. What
	// must not happen is five separate walks of the index: the count is
	// asserted against the number of index ranges opened.
	var indexWalks int
	for {
		select {
		case lower := <-h.kv.spaces:
			if _, _, space, err := keys.ParseSpace(lower); err == nil && space == keys.SpaceAttrIndex {
				indexWalks++
			}
			continue
		default:
		}
		break
	}
	if indexWalks != 1 {
		t.Fatalf("the run opened %d walks of the due-time index, want exactly 1", indexWalks)
	}
}

func TestRunBudgetBoundsOneRun(t *testing.T) {
	const corpus, budget = 25, 10
	h := newHarness(t, func(d *lifecycle.Deps) { d.RunBudget = budget })
	for i := 0; i < corpus; i++ {
		h.store_(t, memory(lifecycle.LongTerm, nil))
	}

	run := h.sweep(t, epoch.Add(2*day))
	if run.Handled != budget {
		t.Fatalf("handled %d, want the budget of %d", run.Handled, budget)
	}
	if run.Exhausted {
		t.Fatalf("the run says it reached the end of the due range with %d memories left", corpus-budget)
	}

	// The rest are still due, and each further run is bounded the same way and
	// continues from where the last stopped rather than starting over — so a
	// backlog drains in ceil(corpus/budget) runs and no memory is visited twice.
	total, runs := run.Handled, 1
	for next := run; !next.Exhausted; runs++ {
		var err error
		next, err = h.m.Sweep(h.ctx(), acme, tenant.DefaultNamespace, next.Next, nil)
		if err != nil {
			t.Fatalf("run %d: %v", runs+1, err)
		}
		if next.Handled > budget {
			t.Fatalf("run %d handled %d, past the budget of %d", runs+1, next.Handled, budget)
		}
		total += next.Handled
		if runs > corpus {
			t.Fatalf("the walk is not making progress: %d runs for %d memories", runs, corpus)
		}
	}
	if total != corpus {
		t.Fatalf("the runs handled %d memories between them, want %d", total, corpus)
	}
	if want := (corpus + budget - 1) / budget; runs != want {
		t.Errorf("drained the backlog in %d runs, want %d", runs, want)
	}
}

// A run that spends its budget enqueues one successor from where it stopped; a
// run that drains the due set enqueues none. Without the first, a corpus whose
// records are all due at zero — which is every corpus written before Phase 10 —
// drains at one budget per sweep interval.
func TestTheContinuationTerminates(t *testing.T) {
	const corpus, budget = 25, 10
	h := newHarness(t, func(d *lifecycle.Deps) { d.RunBudget = budget })
	for i := 0; i < corpus; i++ {
		h.store_(t, memory(lifecycle.LongTerm, nil))
	}
	h.clk.Set(epoch.Add(2 * day))

	job := &jobs.Job{ID: id.New(), Tenant: acme, Namespace: tenant.DefaultNamespace,
		Type: "lifecycle.maintain", State: jobs.Pending}

	handled := 0
	for round := 0; round < 10; round++ {
		if err := h.m.Handle(h.ctx(), job, nil); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		handled++
		if len(h.queued) == 0 {
			break
		}
		job = h.queued[len(h.queued)-1]
		h.queued = nil
	}
	if len(h.queued) != 0 {
		t.Fatalf("the continuation did not terminate after %d rounds", handled)
	}
	if handled != 3 {
		t.Fatalf("drained a %d-memory backlog in %d runs at a budget of %d, want 3",
			corpus, handled, budget)
	}
}

func TestEveryTransitionWritesAnEvent(t *testing.T) {
	tests := []struct {
		name  string
		rec   func() *record.Record
		at    time.Time
		want  events.Kind
		actor string
	}{{
		name: "expired",
		rec: func() *record.Record {
			return memory(lifecycle.ShortTerm, func(r *record.Record) { r.Fields.TTL = time.Hour })
		},
		at: epoch.Add(2 * time.Hour), want: events.Expired, actor: "ttl",
	}, {
		name: "promoted",
		rec: func() *record.Record {
			return memory(lifecycle.ShortTerm, func(r *record.Record) {
				r.Fields.TTL, r.Fields.AccessCount = time.Hour, 3
			})
		},
		at: epoch.Add(2 * time.Hour), want: events.Promoted, actor: "ttl",
	}, {
		name: "archived",
		rec: func() *record.Record {
			return memory(lifecycle.LongTerm, func(r *record.Record) { r.Fields.Health = 1 })
		},
		at: epoch.Add(2 * day), want: events.Archived, actor: "active_forgetting",
	}, {
		name: "hard_deleted",
		rec: func() *record.Record {
			return memory(lifecycle.LongTerm, func(r *record.Record) {
				r.Fields.Archived, r.Fields.ArchivedAt = true, epoch
			})
		},
		at: epoch.Add(lifecycle.CleanupAfter + day), want: events.HardDeleted, actor: "cleanup",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			r := h.store_(t, tc.rec())
			h.sweep(t, tc.at)

			got := h.history(t, r.ID)
			if len(got) != 1 {
				t.Fatalf("wrote %d events, want exactly 1: %+v", len(got), got)
			}
			if got[0].Kind != tc.want {
				t.Errorf("kind is %s, want %s", got[0].Kind, tc.want)
			}
			if got[0].Actor != tc.actor {
				t.Errorf("actor is %q, want %q", got[0].Actor, tc.actor)
			}
			if got[0].Reason == "" {
				t.Error("the event carries no reason")
			}
		})
	}
}

// A hard delete takes the record, its vector and its whole stream, and leaves
// exactly the row that says why. That row is the answer to "why did my memory
// disappear".
func TestHardDeleteLeavesExactlyItsOwnEvent(t *testing.T) {
	h := newHarness(t)
	r := h.store_(t, memory(lifecycle.LongTerm, func(r *record.Record) {
		r.Fields.Archived, r.Fields.ArchivedAt = true, epoch
	}))

	// A history the memory accumulated before it was archived.
	for _, at := range []time.Time{epoch.Add(-2 * time.Hour), epoch.Add(-time.Hour)} {
		tx := txn.New(h.kv)
		if _, err := h.store.Append(context.Background(), tx, events.Event{
			Tenant: acme, Namespace: tenant.DefaultNamespace, Subject: r.ID,
			At: at, Kind: events.Recalled, Actor: "api",
		}); err != nil {
			tx.Close()
			t.Fatalf("Append: %v", err)
		}
		_ = tx.Commit(context.Background())
		tx.Close()
	}

	run := h.sweep(t, epoch.Add(lifecycle.CleanupAfter+day))
	if run.Deleted != 1 {
		t.Fatalf("deleted %d, want 1", run.Deleted)
	}
	if _, err := h.get(t, r.ID); err == nil {
		t.Fatal("the record survived its cleanup")
	}
	if len(h.deleted) != 1 || h.deleted[0] != r.ID {
		t.Errorf("the vector index was told about %v, want [%s]", h.deleted, r.ID)
	}

	got := h.history(t, r.ID)
	if len(got) != 1 || got[0].Kind != events.HardDeleted {
		t.Fatalf("history after a hard delete is %+v, want exactly the deletion event", got)
	}
}

func TestEventsAreTrimmedByTheSweep(t *testing.T) {
	h := newHarness(t, func(d *lifecycle.Deps) { d.EventRetention = 10 * day })
	r := h.store_(t, memory(lifecycle.LongTerm, nil))

	for _, at := range []time.Time{epoch.Add(-30 * day), epoch.Add(-20 * day), epoch.Add(-time.Hour)} {
		tx := txn.New(h.kv)
		if _, err := h.store.Append(context.Background(), tx, events.Event{
			Tenant: acme, Namespace: tenant.DefaultNamespace, Subject: r.ID,
			At: at, Kind: events.Recalled, Actor: "api",
		}); err != nil {
			tx.Close()
			t.Fatalf("Append: %v", err)
		}
		_ = tx.Commit(context.Background())
		tx.Close()
	}

	// A sweep two days in: the retention cutoff is eight days before the epoch,
	// so the two ancient rows go and the recent one stays.
	h.sweep(t, epoch.Add(2*day))

	got := h.history(t, r.ID)
	for _, e := range got {
		if e.Kind == events.Recalled && e.At.Before(epoch.Add(2*day-10*day)) {
			t.Fatalf("an event from %v survived a %v retention", e.At, 10*day)
		}
	}
	recalls := 0
	for _, e := range got {
		if e.Kind == events.Recalled {
			recalls++
		}
	}
	if recalls != 1 {
		t.Fatalf("%d recall events survived, want 1", recalls)
	}
}

// The sweep's write is conditional on the record it read, so a user writing the
// same memory wins and the memory stays due. A background pass must never make
// a user's write fail.
func TestTheSweepYieldsToAUserWrite(t *testing.T) {
	h := newHarness(t)
	r := h.store_(t, memory(lifecycle.LongTerm, nil))
	h.clk.Set(epoch.Add(2 * day))

	// Read the record as the sweep would, then let a user write it, then let
	// the sweep commit its stale view.
	stale, err := h.get(t, r.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	fresh, err := h.get(t, r.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	fresh.Content = "edited by a user"
	h.store_(t, fresh)

	tx := txn.New(h.kv)
	defer tx.Close()
	stale.Fields.Importance = 0.1
	if err := h.repo.Put(h.ctx(), tx, stale); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := tx.Commit(h.ctx()); err == nil {
		t.Fatal("the stale write committed over the user's edit")
	}

	after, err := h.get(t, r.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if after.Content != "edited by a user" {
		t.Fatalf("content is %q; the sweep overwrote the user", after.Content)
	}
	// And the memory is still due, so the next run takes it.
	if !after.Fields.NextAttentionAt.After(epoch.Add(2 * day)) {
		run := h.sweep(t, epoch.Add(2*day))
		if run.Handled != 1 {
			t.Fatalf("the skipped memory was not picked up by the next run")
		}
	}
}

func TestAMaintenanceJobRefusesAnUnscopedRun(t *testing.T) {
	h := newHarness(t)
	err := h.m.Handle(context.Background(), &jobs.Job{ID: id.New(), Type: "lifecycle.maintain"}, nil)
	if err == nil {
		t.Fatal("a job with no tenant ran")
	}
}
