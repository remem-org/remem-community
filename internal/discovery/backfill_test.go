package discovery_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/tenant"
)

const backfillType jobs.Type = "discovery.similar"

// Every live memory reaches a job exactly once, in batches, and an archived one
// reaches none: queuing it would be a job that reads a record to decide to do
// nothing.
func TestBackfillEnqueuesEveryMemory(t *testing.T) {
	h := newHarness(t)
	queue := jobs.NewQueue(h.kv, clock.NewFake(clock.FakeStart))
	enq := discovery.NewEnqueuer(queue, backfillType)

	const live = 250
	want := map[id.ID]bool{}
	for i := range live {
		m := h.store(t, "acme", fmt.Sprintf("memory %d", i), unit(0))
		want[m.rid] = true
	}
	gone := h.store(t, "acme", "retired", unit(0), func(r *record.Record) {
		r.Fields.Archived = true
		r.Fields.ArchivedAt = r.CreatedAt
	})
	// A second tenant, to prove the walk is scoped.
	stranger := h.store(t, "globex", "somebody else's memory", unit(0))

	stats, err := discovery.Backfill(context.Background(), h.kv, h.repo, enq,
		discovery.Scope{Tenant: "acme", Namespace: tenant.DefaultNamespace}, 100)
	must(t, err)

	if stats.Memories != live {
		t.Fatalf("the backfill queued %d memories, want %d", stats.Memories, live)
	}
	if stats.Archived != 1 {
		t.Fatalf("the backfill counted %d archived memories, want 1", stats.Archived)
	}
	if stats.Jobs != 3 {
		t.Fatalf("250 memories at 100 per job produced %d jobs, want 3", stats.Jobs)
	}

	got := map[id.ID]int{}
	queued, err := queue.List(context.Background(), "acme", jobs.Filter{Limit: jobs.MaxListLimit})
	must(t, err)
	for _, j := range queued {
		subjects, err := discovery.DecodePayload(j.Payload)
		must(t, err)
		for _, s := range subjects {
			got[s]++
		}
	}
	for rid := range want {
		switch got[rid] {
		case 1:
		case 0:
			t.Fatalf("memory %s was never queued", rid)
		default:
			t.Fatalf("memory %s was queued %d times", rid, got[rid])
		}
	}
	if got[gone.rid] != 0 {
		t.Fatalf("the archived memory was queued %d times", got[gone.rid])
	}
	if got[stranger.rid] != 0 {
		t.Fatal("a backfill of acme queued globex's memory")
	}
	if n := len(got); n != live {
		t.Fatalf("the jobs name %d distinct memories, want %d", n, live)
	}
}

// Invariant 1 at the signature, not by convention inside the walk.
func TestAnUnscopedBackfillIsRefused(t *testing.T) {
	h := newHarness(t)
	queue := jobs.NewQueue(h.kv, clock.NewFake(clock.FakeStart))
	enq := discovery.NewEnqueuer(queue, backfillType)

	_, err := discovery.Backfill(context.Background(), h.kv, h.repo, enq, discovery.Scope{}, 0)
	if errs.KindOf(err) != errs.Invalid {
		t.Fatalf("a backfill with no tenant returned %v", err)
	}
}

// A backfill with nowhere to enqueue would read the whole corpus and do
// nothing, which is the shape of a command that appears to work.
func TestABackfillWithNoQueueIsRefused(t *testing.T) {
	h := newHarness(t)
	_, err := discovery.Backfill(context.Background(), h.kv, h.repo, nil,
		discovery.Scope{Tenant: "acme"}, 0)
	if errs.KindOf(err) != errs.Invalid {
		t.Fatalf("a backfill with no queue returned %v", err)
	}
}

// An empty tenant is an ordinary answer, not an error: an operator running this
// on a corpus that has nothing to backfill should be told so.
func TestABackfillOfAnEmptyTenantEnqueuesNothing(t *testing.T) {
	h := newHarness(t)
	queue := jobs.NewQueue(h.kv, clock.NewFake(clock.FakeStart))
	enq := discovery.NewEnqueuer(queue, backfillType)

	stats, err := discovery.Backfill(context.Background(), h.kv, h.repo, enq,
		discovery.Scope{Tenant: "acme"}, 0)
	must(t, err)
	if stats.Memories != 0 || stats.Jobs != 0 {
		t.Fatalf("an empty tenant produced %+v", stats)
	}
}
