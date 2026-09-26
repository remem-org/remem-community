package jobs_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

const owner = "node-1"

func TestAnEnqueuedJobComesBackFromAClaim(t *testing.T) {
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	j := &jobs.Job{Tenant: acme, Type: "vector.rebuild", Payload: []byte("p")}
	if err := q.Submit(ctx, j); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if j.ID == id.Zero || j.State != jobs.Pending || j.CreatedAt.IsZero() {
		t.Fatalf("Submit did not fill in the job: %+v", j)
	}

	claimed, err := q.Claim(ctx, acme, owner, nil, 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(claimed))
	}
	got := claimed[0]
	if got.ID != j.ID || got.Type != j.Type || string(got.Payload) != "p" {
		t.Fatalf("claimed the wrong job: %+v", got)
	}
	if got.State != jobs.Running || got.Attempts != 1 {
		t.Fatalf("a claimed job is %s after %d attempts", got.State, got.Attempts)
	}
	if got.Lease == nil || got.Lease.Owner != owner || got.Lease.Token == id.Zero {
		t.Fatalf("a claimed job has no usable lease: %+v", got.Lease)
	}
	if want := clk.Now().Add(time.Minute); !got.Lease.ExpiresAt.Equal(want) {
		t.Fatalf("the lease expires at %v, want %v", got.Lease.ExpiresAt, want)
	}

	// And it is gone from the pending partition, so a second poll finds nothing.
	again, err := q.Claim(ctx, acme, owner, nil, 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("a claimed job was claimed again by the same poll: %d", len(again))
	}
}

func TestAJobIsNotClaimableBeforeItIsDue(t *testing.T) {
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	j := &jobs.Job{Tenant: acme, Type: "vector.rebuild", RunAt: clk.Now().Add(time.Hour)}
	if err := q.Submit(ctx, j); err != nil {
		t.Fatal(err)
	}
	if claimed, err := q.Claim(ctx, acme, owner, nil, 10); err != nil || len(claimed) != 0 {
		t.Fatalf("a job due in an hour was claimed now: %d, %v", len(claimed), err)
	}

	clk.Advance(time.Hour)
	if claimed, err := q.Claim(ctx, acme, owner, nil, 10); err != nil || len(claimed) != 1 {
		t.Fatalf("a job due now was not claimed: %d, %v", len(claimed), err)
	}
}

func TestClaimIsExclusive(t *testing.T) {
	// The completion criterion: 100 jobs, 16 concurrent claimers, every job
	// claimed exactly once, under -race.
	//
	// It runs twice. Exclusivity comes from the conditional commit — the loser
	// of a race is refused and neither of its writes lands — and the tenant
	// gate exists only to stop sixteen workers converging on it slowly. A
	// guard that is never exercised is a guard nobody has verified, so the
	// second run disables the gate and requires the same result from the
	// conditional commit alone.
	for _, tc := range []struct {
		name string
		opts []jobs.QueueOption
	}{
		{"through the tenant gate", nil},
		{"on the conditional commit alone", []jobs.QueueOption{jobs.WithoutGate()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			q, _, _ := newQueue(t, tc.opts...)

			const total = 100
			for range total {
				if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "vector.rebuild"}); err != nil {
					t.Fatal(err)
				}
			}

			var (
				mu    sync.Mutex
				seen  = map[id.ID]int{}
				wg    sync.WaitGroup
				fails []error
			)
			for w := range 16 {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for {
						claimed, err := q.Claim(ctx, acme, owner, nil, 3)
						if err != nil {
							mu.Lock()
							fails = append(fails, err)
							mu.Unlock()
							return
						}
						if len(claimed) == 0 {
							return
						}
						mu.Lock()
						for _, j := range claimed {
							seen[j.ID]++
						}
						mu.Unlock()
						for _, j := range claimed {
							if err := q.Complete(ctx, j); err != nil {
								mu.Lock()
								fails = append(fails, err)
								mu.Unlock()
								return
							}
						}
					}
				}(w)
			}
			wg.Wait()

			if len(fails) > 0 {
				t.Fatalf("%d claimers failed, first: %v", len(fails), fails[0])
			}
			if len(seen) != total {
				t.Fatalf("%d distinct jobs were claimed, want %d", len(seen), total)
			}
			for jid, n := range seen {
				if n != 1 {
					t.Fatalf("job %s was claimed %d times", jid, n)
				}
			}
		})
	}
}

func TestJobsAreTenantScoped(t *testing.T) {
	ctx := context.Background()
	q, _, _ := newQueue(t)

	other := tenant.ID("globex")
	mine := &jobs.Job{Tenant: acme, Type: "vector.rebuild"}
	theirs := &jobs.Job{Tenant: other, Type: "vector.rebuild"}
	for _, j := range []*jobs.Job{mine, theirs} {
		if err := q.Submit(ctx, j); err != nil {
			t.Fatal(err)
		}
	}

	claimed, err := q.Claim(ctx, acme, owner, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != mine.ID {
		t.Fatalf("a claim for %q returned %d jobs, and they were not only its own", acme, len(claimed))
	}

	// And an unscoped claim is not spellable: there is no signature for it.
	if _, err := q.Claim(ctx, "", owner, nil, 10); !errs.Is(err, errs.Invalid) {
		t.Fatal("a claim with no tenant must be refused (Invariant 1)")
	}
}

func TestClaimTakesOnlyTheTypesAskedFor(t *testing.T) {
	ctx := context.Background()
	q, _, _ := newQueue(t)

	wanted := &jobs.Job{Tenant: acme, Type: "text.rebuild"}
	for _, j := range []*jobs.Job{
		{Tenant: acme, Type: "vector.rebuild"},
		wanted,
		{Tenant: acme, Type: "jobs.reap"},
	} {
		if err := q.Submit(ctx, j); err != nil {
			t.Fatal(err)
		}
	}

	claimed, err := q.Claim(ctx, acme, owner, []jobs.Type{"text.rebuild"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != wanted.ID {
		t.Fatalf("a filtered claim returned %d jobs", len(claimed))
	}
}

func TestPriorityOrdersOneClaimBatch(t *testing.T) {
	// Priority cannot order the queue — the key is ordered by due time and
	// priority is not in it — but it orders what one claim hands to the pool,
	// which is all it claims to do.
	ctx := context.Background()
	q, _, _ := newQueue(t)

	low := &jobs.Job{Tenant: acme, Type: "vector.rebuild", Priority: -5}
	high := &jobs.Job{Tenant: acme, Type: "vector.rebuild", Priority: 5}
	for _, j := range []*jobs.Job{low, high} {
		if err := q.Submit(ctx, j); err != nil {
			t.Fatal(err)
		}
	}

	claimed, err := q.Claim(ctx, acme, owner, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 2 || claimed[0].ID != high.ID {
		t.Fatalf("the higher priority job was not first in the batch")
	}
}

func TestCompletingAJobLeavesOneRowInTheDonePartition(t *testing.T) {
	ctx := context.Background()
	q, clk, kv := newQueue(t)

	j := &jobs.Job{Tenant: acme, Type: "vector.rebuild"}
	if err := q.Submit(ctx, j); err != nil {
		t.Fatal(err)
	}
	claimed, err := q.Claim(ctx, acme, owner, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)
	if err := q.Complete(ctx, claimed[0]); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if got := claimed[0].State; got != jobs.Completed {
		t.Fatalf("a completed job is %s", got)
	}
	if claimed[0].Lease != nil {
		t.Fatal("a completed job still holds a lease")
	}
	// Exactly one row, in the done partition: a transition that forgot to
	// delete the old key would leave a job that is claimable and finished.
	assertPartitionCounts(t, kv, acme, map[keys.JobState]int{keys.JobDone: 1})
}

func TestEnqueueStagesIntoTheCallersTransaction(t *testing.T) {
	// This is what Phase 11 needs: a discovery job enqueued in the same
	// transaction as the memory write, so a failed write leaves no orphan job
	// and a successful one cannot lose its follow-up work.
	ctx := context.Background()
	q, _, kv := newQueue(t)

	tx := txn.New(kv)
	j := &jobs.Job{Tenant: acme, Type: "vector.rebuild"}
	if err := q.Enqueue(ctx, tx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	tx.Close() // abandoned, as a failed write would abandon it

	if claimed, err := q.Claim(ctx, acme, owner, nil, 10); err != nil || len(claimed) != 0 {
		t.Fatalf("an abandoned transaction left %d jobs behind (%v)", len(claimed), err)
	}
}

func TestSubmitRefusesAJobThatCannotBeStored(t *testing.T) {
	ctx := context.Background()
	q, _, _ := newQueue(t)
	for name, j := range map[string]*jobs.Job{
		"no tenant": {Type: "vector.rebuild"},
		"no type":   {Tenant: acme},
		"bad type":  {Tenant: acme, Type: "Vector Rebuild"},
	} {
		if err := q.Submit(ctx, j); !errs.Is(err, errs.Invalid) {
			t.Errorf("%s: want Invalid, got %v", name, err)
		}
	}
}

func TestCompletingAJobTwiceIsRefused(t *testing.T) {
	// At-least-once delivery means a worker may believe it still holds a job
	// it does not. Silently accepting the second completion would write a
	// second done row for one job.
	ctx := context.Background()
	q, _, _ := newQueue(t)

	j := &jobs.Job{Tenant: acme, Type: "vector.rebuild"}
	if err := q.Submit(ctx, j); err != nil {
		t.Fatal(err)
	}
	claimed, err := q.Claim(ctx, acme, owner, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	stale := claimed[0].Clone()
	if err := q.Complete(ctx, claimed[0]); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(ctx, stale); !errs.Is(err, errs.Conflict) {
		t.Fatalf("a second completion was accepted: %v", err)
	}
}

// --- helpers ----------------------------------------------------------------

func newQueue(t *testing.T, opts ...jobs.QueueOption) (*jobs.Queue, *clock.Fake, storage.KV) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(clock.FakeStart)
	return jobs.NewQueue(kv, clk, opts...), clk, kv
}

func assertPartitionCounts(t *testing.T, kv storage.KV, tid tenant.ID, want map[keys.JobState]int) {
	t.Helper()
	for _, part := range keys.AllJobStates() {
		lo, hi := keys.JobStateRange(tid, tenant.DefaultNamespace, part)
		it := kv.NewIterator(lo, hi)
		n := 0
		for ok := it.First(); ok; ok = it.Next() {
			n++
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
		if n != want[part] {
			t.Errorf("the %s partition holds %d rows, want %d", part, n, want[part])
		}
	}
}
