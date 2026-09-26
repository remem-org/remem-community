package jobs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
)

func TestRenewingALeaseMovesTheRowForward(t *testing.T) {
	// The running partition is ordered by lease expiry, so renewing is not a
	// field update: it moves the row. A renewal that left the key alone would
	// leave the job at the head of the reclamation scan forever.
	ctx := context.Background()
	q, clk, kv := newQueue(t)

	if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "vector.rebuild"}); err != nil {
		t.Fatal(err)
	}
	j := claimOne(t, q)
	first := j.Lease.ExpiresAt
	firstToken := j.Lease.Token

	clk.Advance(30 * time.Second)
	if err := q.Renew(ctx, j); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if !j.Lease.ExpiresAt.After(first) {
		t.Fatalf("the renewed lease expires at %v, not after %v", j.Lease.ExpiresAt, first)
	}
	if j.Lease.Token != firstToken {
		t.Fatal("a renewal changed the fencing token; only a reclamation may do that")
	}
	assertPartitionCounts(t, kv, acme, map[keys.JobState]int{keys.JobRunning: 1})

	// The old expiry no longer selects it, which is the point of moving it.
	n, err := q.Reclaim(ctx, acme, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("a renewed job was reclaimed %d times", n)
	}
}

func TestExpiredLeaseReturnsTheJob(t *testing.T) {
	ctx := context.Background()
	q, clk, kv := newQueue(t)

	if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "vector.rebuild"}); err != nil {
		t.Fatal(err)
	}
	j := claimOne(t, q)

	if n, err := q.Reclaim(ctx, acme, 10); err != nil || n != 0 {
		t.Fatalf("a live lease was reclaimed: %d, %v", n, err)
	}

	clk.Advance(q.Lease() + time.Millisecond)
	n, err := q.Reclaim(ctx, acme, 10)
	if err != nil {
		t.Fatalf("Reclaim: %v", err)
	}
	if n != 1 {
		t.Fatalf("reclaimed %d jobs, want 1", n)
	}
	assertPartitionCounts(t, kv, acme, map[keys.JobState]int{keys.JobPending: 1})

	again := claimOne(t, q)
	if again.ID != j.ID {
		t.Fatal("the reclaimed job is not the one that was lost")
	}
	if again.Attempts != 2 {
		t.Fatalf("the reclaimed job is on attempt %d, want 2", again.Attempts)
	}
	if again.Lease.Token == j.Lease.Token {
		t.Fatal("the reclaimed job kept its old fencing token, so the old worker could still finish it")
	}
}

func TestFencedWorkerCannotComplete(t *testing.T) {
	// The scenario: a worker's process paused long enough for its lease to
	// lapse, the job was reclaimed and re-run by somebody else, and now the
	// original worker wakes up and reports success. Accepting that would
	// discard the second run's bookkeeping and, worse, tell the queue a job is
	// finished on the strength of a run whose output has since been redone.
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "vector.rebuild"}); err != nil {
		t.Fatal(err)
	}
	stale := claimOne(t, q)

	clk.Advance(q.Lease() + time.Millisecond)
	if _, err := q.Reclaim(ctx, acme, 10); err != nil {
		t.Fatal(err)
	}
	fresh := claimOne(t, q)

	// Every way the stale worker could touch the job is refused, and each of
	// them is a way a real worker actually returns.
	for name, act := range map[string]func() error{
		"complete": func() error { return q.Complete(ctx, stale.Clone()) },
		"fail":     func() error { return q.Fail(ctx, stale.Clone(), errors.New("too late")) },
		"renew":    func() error { return q.Renew(ctx, stale.Clone()) },
		"save":     func() error { return q.Save(ctx, stale.Clone(), []byte("progress")) },
		"release":  func() error { return q.Release(ctx, stale.Clone()) },
	} {
		if err := act(); !errs.Is(err, errs.Conflict) {
			t.Errorf("a fenced worker's %s was accepted: %v", name, err)
		}
	}

	// And the worker that actually holds the job is unaffected.
	if err := q.Complete(ctx, fresh); err != nil {
		t.Fatalf("the current owner could not complete its own job: %v", err)
	}
}

func TestTheFenceHoldsWhenTheNewOwnerLandsOnTheSameKey(t *testing.T) {
	// The key alone is nearly enough to fence: a reclaimed job is almost always
	// at a different key, so a stale worker's read fails before the token is
	// ever consulted. Nearly.
	//
	// A running key encodes the lease expiry, so two successive owners collide
	// on one key whenever the second lease expires at the same instant as the
	// first — which a clock stepped backwards by NTP, or a lease duration
	// changed across a restart, will both produce. The case is constructed here
	// by writing the row a second owner would leave, because a forward-only
	// fake clock cannot produce it and a defect nobody can reach in a test is a
	// defect that ships.
	ctx := context.Background()
	q, _, kv := newQueue(t)

	if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "vector.rebuild"}); err != nil {
		t.Fatal(err)
	}
	stale := claimOne(t, q)

	key := stale.StoredKey()
	value, err := kv.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	current, err := jobs.Unmarshal(key, value)
	if err != nil {
		t.Fatal(err)
	}
	current.Lease.Owner = "node-2"
	current.Lease.Token = id.New() // a reclamation always draws a new one
	rewritten, err := jobs.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(ctx, key, rewritten); err != nil {
		t.Fatal(err)
	}

	if err := q.Complete(ctx, stale); !errs.Is(err, errs.Conflict) {
		t.Fatalf("a stale worker completed a job at the key it still held: %v", err)
	}
	if err := q.Renew(ctx, stale); !errs.Is(err, errs.Conflict) {
		t.Fatalf("a stale worker renewed a job at the key it still held: %v", err)
	}
}

func TestReclamationIsBoundedAndScoped(t *testing.T) {
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	other := tenant.ID("globex")
	for _, tid := range []tenant.ID{acme, acme, acme, other} {
		if err := q.Submit(ctx, &jobs.Job{Tenant: tid, Type: "vector.rebuild"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := q.Claim(ctx, acme, owner, nil, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Claim(ctx, other, owner, nil, 1); err != nil {
		t.Fatal(err)
	}
	clk.Advance(q.Lease() + time.Millisecond)

	if n, err := q.Reclaim(ctx, acme, 2); err != nil || n != 2 {
		t.Fatalf("a bounded reclamation took %d, want 2 (%v)", n, err)
	}
	if n, err := q.Reclaim(ctx, acme, 10); err != nil || n != 1 {
		t.Fatalf("the second pass took %d, want the remaining 1 (%v)", n, err)
	}
	// The other tenant's lapsed lease was never this tenant's business.
	if n, err := q.Reclaim(ctx, other, 10); err != nil || n != 1 {
		t.Fatalf("another tenant's job was reclaimed by this tenant's pass: %d, %v", n, err)
	}
	if _, err := q.Reclaim(ctx, "", 10); !errs.Is(err, errs.Invalid) {
		t.Fatal("an unscoped reclamation must be refused (Invariant 1)")
	}
}

func TestReleasingAJobDoesNotChargeTheAttempt(t *testing.T) {
	// Shutdown is not a failure. A job released because the process is stopping
	// must come back with the attempt it never got to finish still available,
	// or a few rolling restarts would exhaust a job that never once broke.
	ctx := context.Background()
	q, _, kv := newQueue(t)

	if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "vector.rebuild", MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	j := claimOne(t, q)
	if err := q.Save(ctx, j, []byte("halfway")); err != nil {
		t.Fatal(err)
	}
	if err := q.Release(ctx, j); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if j.State != jobs.Pending || j.Lease != nil {
		t.Fatalf("a released job is %s with lease %+v", j.State, j.Lease)
	}
	assertPartitionCounts(t, kv, acme, map[keys.JobState]int{keys.JobPending: 1})

	again := claimOne(t, q)
	if again.Attempts != 1 {
		t.Fatalf("a released and re-claimed job is on attempt %d, want 1", again.Attempts)
	}
	if string(again.Checkpoint) != "halfway" {
		t.Fatalf("a released job lost its checkpoint: %q", again.Checkpoint)
	}
}

func TestARepeatedlyAbandonedJobEventuallyFails(t *testing.T) {
	// A handler that hangs every time is reclaimed every time, and each
	// reclamation is followed by a claim that charges an attempt. It must
	// therefore reach Failed rather than looping for ever — a job nothing can
	// finish and nothing gives up on is a worker slot leaking at every poll.
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "vector.rebuild", MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if len(mustClaim(t, q, 1)) != 1 {
			t.Fatal("the job stopped being claimable before its attempts ran out")
		}
		clk.Advance(q.Lease() + time.Millisecond)
		if _, err := q.Reclaim(ctx, acme, 10); err != nil {
			t.Fatal(err)
		}
	}
	if got := mustClaim(t, q, 1); len(got) != 0 {
		t.Fatalf("a job that used all three attempts is still claimable: %+v", got[0])
	}
	// And it says so, rather than vanishing: the reclamation that found it out
	// of attempts is the one that has to record why.
	found, err := q.Get(ctx, acme, jobID(t, q))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found.State != jobs.Failed || found.LastError == "" {
		t.Fatalf("the abandoned job ended as %s / %q", found.State, found.LastError)
	}
}

// jobID returns the id of the one job in the tenant, for a test that never held
// a reference to it.
func jobID(t *testing.T, q *jobs.Queue) id.ID {
	t.Helper()
	list, err := q.List(context.Background(), acme, jobs.Filter{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("the tenant holds %d jobs, want 1", len(list))
	}
	return list[0].ID
}

func mustClaim(t *testing.T, q *jobs.Queue, n int) []*jobs.Job {
	t.Helper()
	claimed, err := q.Claim(context.Background(), acme, owner, nil, n)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return claimed
}
