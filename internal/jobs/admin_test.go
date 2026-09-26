package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
)

func TestReapRemovesOnlyOldFinishedJobs(t *testing.T) {
	// The audit trail is what keeps "what happened to my rebuild" answerable,
	// and it is also what makes Get a scan. Retention is the setting that keeps
	// both true at once.
	ctx := context.Background()
	q, clk, kv := newQueue(t)

	old := finish(t, q, "vector.rebuild")
	clk.Advance(48 * time.Hour)
	recent := finish(t, q, "text.rebuild")
	waiting := &jobs.Job{Tenant: acme, Type: "jobs.reap"}
	if err := q.Submit(ctx, waiting); err != nil {
		t.Fatal(err)
	}
	running := &jobs.Job{Tenant: acme, Type: "jobs.reap"}
	if err := q.Submit(ctx, running); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Claim(ctx, acme, owner, []jobs.Type{"jobs.reap"}, 1); err != nil {
		t.Fatal(err)
	}

	n, err := q.Reap(ctx, acme, clk.Now().Add(-24*time.Hour), 100)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if n != 1 {
		t.Fatalf("the reaper removed %d rows, want the one older than the retention", n)
	}
	if _, err := q.Get(ctx, acme, old.ID); !errs.Is(err, errs.NotFound) {
		t.Errorf("the old finished job is still there: %v", err)
	}
	if _, err := q.Get(ctx, acme, recent.ID); err != nil {
		t.Errorf("a recently finished job was reaped: %v", err)
	}
	assertPartitionCounts(t, kv, acme, map[keys.JobState]int{
		keys.JobPending: 1, keys.JobRunning: 1, keys.JobDone: 1,
	})
}

func TestReapIsBoundedAndScoped(t *testing.T) {
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	for range 5 {
		finish(t, q, "vector.rebuild")
	}
	other := tenant.ID("globex")
	theirs := &jobs.Job{Tenant: other, Type: "vector.rebuild"}
	if err := q.Submit(ctx, theirs); err != nil {
		t.Fatal(err)
	}
	claimed, err := q.Claim(ctx, other, owner, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(ctx, claimed[0]); err != nil {
		t.Fatal(err)
	}

	clk.Advance(48 * time.Hour)
	cutoff := clk.Now().Add(-24 * time.Hour)

	if n, err := q.Reap(ctx, acme, cutoff, 2); err != nil || n != 2 {
		t.Fatalf("a bounded reap removed %d rows, want 2 (%v)", n, err)
	}
	if n, err := q.Reap(ctx, acme, cutoff, 100); err != nil || n != 3 {
		t.Fatalf("the second pass removed %d rows, want the remaining 3 (%v)", n, err)
	}
	if _, err := q.Get(ctx, other, theirs.ID); err != nil {
		t.Errorf("another tenant's audit trail was reaped: %v", err)
	}
	if _, err := q.Reap(ctx, "", cutoff, 10); !errs.Is(err, errs.Invalid) {
		t.Error("an unscoped reap must be refused (Invariant 1)")
	}
}

func TestListingNarrowsByStateAndType(t *testing.T) {
	ctx := context.Background()
	q, _, _ := newQueue(t)

	pending := &jobs.Job{Tenant: acme, Type: "vector.rebuild"}
	if err := q.Submit(ctx, pending); err != nil {
		t.Fatal(err)
	}
	done := finish(t, q, "text.rebuild")

	all, err := q.List(ctx, acme, jobs.Filter{})
	if err != nil || len(all) != 2 {
		t.Fatalf("an unfiltered listing returned %d jobs (%v)", len(all), err)
	}
	if all[0].ID != pending.ID {
		t.Fatal("a listing must show what is waiting before what has happened")
	}

	byState, err := q.List(ctx, acme, jobs.Filter{States: []jobs.State{jobs.Completed}})
	if err != nil || len(byState) != 1 || byState[0].ID != done.ID {
		t.Fatalf("a state filter returned %d jobs (%v)", len(byState), err)
	}
	byType, err := q.List(ctx, acme, jobs.Filter{Types: []jobs.Type{"vector.rebuild"}})
	if err != nil || len(byType) != 1 || byType[0].ID != pending.ID {
		t.Fatalf("a type filter returned %d jobs (%v)", len(byType), err)
	}
	if got, _ := q.List(ctx, acme, jobs.Filter{Limit: 1}); len(got) != 1 {
		t.Fatalf("a limit of one returned %d jobs", len(got))
	}
	if _, err := q.List(ctx, "", jobs.Filter{}); !errs.Is(err, errs.Invalid) {
		t.Error("an unscoped listing must be refused (Invariant 1)")
	}
}

func TestAListingRestrictedToOneStateReadsOnlyItsPartition(t *testing.T) {
	// Not a performance nicety: the done partition is the audit trail, and a
	// listing of what is waiting should not walk a week of history to find it.
	ctx := context.Background()
	q, _, kv := newQueue(t)

	for range 20 {
		finish(t, q, "vector.rebuild")
	}
	pending := &jobs.Job{Tenant: acme, Type: "vector.rebuild"}
	if err := q.Submit(ctx, pending); err != nil {
		t.Fatal(err)
	}

	counting := &countingStore{KV: kv}
	counted := jobs.NewQueue(counting, clockOf(t))
	got, err := counted.List(ctx, acme, jobs.Filter{States: []jobs.State{jobs.Pending}})
	if err != nil || len(got) != 1 {
		t.Fatalf("the listing returned %d jobs (%v)", len(got), err)
	}
	if counting.iterators != 1 {
		t.Fatalf("a listing of one state opened %d iterators, want 1", counting.iterators)
	}
}

func TestGetFindsAJobWhereverItIs(t *testing.T) {
	ctx := context.Background()
	q, _, _ := newQueue(t)

	waiting := &jobs.Job{Tenant: acme, Type: "vector.rebuild"}
	if err := q.Submit(ctx, waiting); err != nil {
		t.Fatal(err)
	}
	running := &jobs.Job{Tenant: acme, Type: "text.rebuild"}
	if err := q.Submit(ctx, running); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Claim(ctx, acme, owner, []jobs.Type{"text.rebuild"}, 1); err != nil {
		t.Fatal(err)
	}
	finished := finish(t, q, "jobs.reap")

	for name, want := range map[string]*jobs.Job{
		"pending": waiting, "running": running, "finished": finished,
	} {
		got, err := q.Get(ctx, acme, want.ID)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if got.ID != want.ID {
			t.Errorf("%s: found %s", name, got.ID)
		}
	}
	if _, err := q.Get(ctx, acme, id.New()); !errs.Is(err, errs.NotFound) {
		t.Errorf("an unknown id was found: %v", err)
	}
	// Another tenant's job is not visible, and reads as absent rather than
	// forbidden: its existence is not this tenant's business.
	other := &jobs.Job{Tenant: "globex", Type: "vector.rebuild"}
	if err := q.Submit(ctx, other); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Get(ctx, acme, other.ID); !errs.Is(err, errs.NotFound) {
		t.Errorf("another tenant's job was visible: %v", err)
	}
}

func TestCancellingAPendingJobStopsItRunning(t *testing.T) {
	ctx := context.Background()
	q, _, kv := newQueue(t)

	j := &jobs.Job{Tenant: acme, Type: "vector.rebuild"}
	if err := q.Submit(ctx, j); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(ctx, acme, j.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if claimed, err := q.Claim(ctx, acme, owner, nil, 10); err != nil || len(claimed) != 0 {
		t.Fatalf("a cancelled job was claimed: %d, %v", len(claimed), err)
	}
	assertPartitionCounts(t, kv, acme, map[keys.JobState]int{keys.JobDone: 1})

	got, err := q.Get(ctx, acme, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != jobs.Cancelled || got.LastError == "" {
		t.Fatalf("the cancelled job is %s / %q", got.State, got.LastError)
	}
	// A second cancellation is refused by name: "cancelled" and "finished
	// twenty minutes ago" are different answers.
	if err := q.Cancel(ctx, acme, j.ID); !errs.Is(err, errs.Conflict) {
		t.Errorf("a finished job was cancelled again: %v", err)
	}
	if err := q.Cancel(ctx, acme, id.New()); !errs.Is(err, errs.NotFound) {
		t.Errorf("cancelling an unknown job did not say so: %v", err)
	}
}

func TestCountsReadOnlyTheLivePartitions(t *testing.T) {
	// The gauges answer "is the queue keeping up", which is a question about
	// work outstanding. Counting the audit trail as well would make a metric
	// refresh proportional to everything that has ever run.
	ctx := context.Background()
	q, _, _ := newQueue(t)

	for range 3 {
		if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "vector.rebuild"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "text.rebuild"}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Claim(ctx, acme, owner, []jobs.Type{"text.rebuild"}, 1); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		finish(t, q, "jobs.reap")
	}

	counts, err := q.Counts(ctx, acme)
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	if counts.Pending["vector.rebuild"] != 3 {
		t.Fatalf("pending vector.rebuild is %d, want 3", counts.Pending["vector.rebuild"])
	}
	if counts.Running["text.rebuild"] != 1 {
		t.Fatalf("running text.rebuild is %d, want 1", counts.Running["text.rebuild"])
	}
	if counts.Capped {
		t.Fatal("a nine-row queue reported itself capped")
	}
}

// --- helpers ----------------------------------------------------------------

// finish submits, claims and completes one job, leaving a row in the audit
// trail.
func finish(t *testing.T, q *jobs.Queue, typ jobs.Type) *jobs.Job {
	t.Helper()
	ctx := context.Background()
	j := &jobs.Job{Tenant: acme, Type: typ}
	if err := q.Submit(ctx, j); err != nil {
		t.Fatal(err)
	}
	claimed, err := q.Claim(ctx, acme, owner, []jobs.Type{typ}, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim: %d, %v", len(claimed), err)
	}
	if err := q.Complete(ctx, claimed[0]); err != nil {
		t.Fatal(err)
	}
	return claimed[0]
}

// countingStore reports how many scans a read actually performed. It is the
// only way to assert "this listing did not walk the audit trail" — the answer
// is identical either way, and the cost is the whole point.
type countingStore struct {
	storage.KV
	iterators int
}

func (c *countingStore) NewIterator(lower, upper []byte) storage.Iterator {
	c.iterators++
	return c.KV.NewIterator(lower, upper)
}

func clockOf(t *testing.T) clock.Clock {
	t.Helper()
	return clock.NewFake(clock.FakeStart)
}
