package jobs

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
)

// A renewal pass that meets a job which has just completed must say nothing.
//
// The pass takes a snapshot of what is running and then renews each one; a
// handler that finishes in between leaves a job whose lease the completion has
// already dropped. Renewing it fails with "holds no lease to renew" and logs a
// warning about a job that did exactly what it was supposed to — one per job
// that finishes near a renewal tick.
//
// The Phase 9 verification run found it in the shutdown log of a reaper that
// had just succeeded. It is noise rather than damage, and noise at warning
// level is how an operator learns that a server's warnings do not mean
// anything, which is the failure that matters.
func TestARenewalPassIsQuietAboutAJobThatJustFinished(t *testing.T) {
	ctx := context.Background()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(clock.FakeStart)

	queue := NewQueue(kv, clk)
	pool, err := NewPool(PoolConfig{
		Queue: queue, Registry: NewRegistry(), Tenants: tenantkv.New(kv, clk), Clock: clk,
		Owner: "node-test", Workers: 1, PollInterval: DefaultLease,
	})
	if err != nil {
		t.Fatal(err)
	}

	const tid = tenant.ID("acme")
	j := &Job{Tenant: tid, Type: "vector.rebuild"}
	if err := queue.Submit(ctx, j); err != nil {
		t.Fatal(err)
	}
	claimed, err := queue.Claim(ctx, tid, "node-test", nil, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim: %d, %v", len(claimed), err)
	}

	// A job the pool believes is running, which has in fact just completed —
	// the exact interleaving the renewal pass meets in production.
	rj := &runningJob{job: claimed[0], cancel: func() {}}
	pool.running[claimed[0].ID] = rj
	if err := queue.Complete(ctx, claimed[0]); err != nil {
		t.Fatal(err)
	}

	renewed, finished, lost := pool.renewAll(ctx)
	if finished != 1 || renewed != 0 || lost != 0 {
		t.Fatalf("renewAll over a finished job reports renewed=%d finished=%d lost=%d; "+
			"a completion is not a lost lease and it is not a failure to renew",
			renewed, finished, lost)
	}
	if rj.lost.Load() {
		t.Fatal("a job that completed normally was marked as taken away from this worker")
	}
}

// And a live job is still renewed, so the skip above did not turn renewal off.
func TestARenewalPassStillRenewsALiveJob(t *testing.T) {
	ctx := context.Background()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(clock.FakeStart)

	queue := NewQueue(kv, clk)
	pool, err := NewPool(PoolConfig{
		Queue: queue, Registry: NewRegistry(), Tenants: tenantkv.New(kv, clk), Clock: clk,
		Owner: "node-test", Workers: 1, PollInterval: DefaultLease,
	})
	if err != nil {
		t.Fatal(err)
	}

	const tid = tenant.ID("acme")
	if err := queue.Submit(ctx, &Job{Tenant: tid, Type: "vector.rebuild"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := queue.Claim(ctx, tid, "node-test", nil, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim: %d, %v", len(claimed), err)
	}
	before := claimed[0].Lease.ExpiresAt

	pool.running[claimed[0].ID] = &runningJob{job: claimed[0], cancel: func() {}}
	clk.Advance(DefaultLease / 3)

	renewed, finished, lost := pool.renewAll(ctx)
	if renewed != 1 || finished != 0 || lost != 0 {
		t.Fatalf("renewAll over a live job reports renewed=%d finished=%d lost=%d", renewed, finished, lost)
	}
	if !claimed[0].Lease.ExpiresAt.After(before) {
		t.Fatal("the live job's lease was not extended")
	}
}
