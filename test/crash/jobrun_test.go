//go:build crash

package crash

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// submitJobOnce submits a job of typ for tid on a directory's first life and
// finds the same job on every later one, by an id recorded under path.
//
// The id is recorded through a synced transaction, because the Pebble adapter's
// plain Set commits unsynced: a first life killed before that write reached the
// log would make the second life submit a second job, and every "resumed"
// assertion after it would be about the wrong job.
func submitJobOnce(t *testing.T, ctx context.Context, kv storage.KV, queue *jobs.Queue,
	tid tenant.ID, typ jobs.Type, path string,
) id.ID {
	t.Helper()
	key := keys.System(path)
	raw, err := kv.Get(ctx, key)
	if err == nil {
		jid, err := id.FromBytes(raw)
		if err != nil {
			t.Fatal(err)
		}
		return jid
	}
	if !errs.Is(err, errs.NotFound) {
		t.Fatal(err)
	}
	j := &jobs.Job{Tenant: tid, Namespace: tenant.DefaultNamespace, Type: typ, MaxAttempts: 10}
	if err := queue.Submit(ctx, j); err != nil {
		t.Fatalf("submitting: %v", err)
	}
	tx := txn.New(kv)
	defer tx.Close()
	tx.Set(key, j.ID.Bytes())
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return j.ID
}

// runJobToCompletion runs a single-worker pool until the job completes, then
// stops the pool and prints "job completed". A job that ends failed or
// cancelled fails the child, with the job's own last error.
func runJobToCompletion(t *testing.T, kv storage.KV, tenants tenant.Directory,
	queue *jobs.Queue, registry *jobs.Registry, tid tenant.ID, jid id.ID,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := jobs.NewPool(jobs.PoolConfig{
		Queue: queue, Registry: registry, Tenants: tenants, Clock: clock.System(),
		Owner: fmt.Sprintf("crash-%d", time.Now().UnixNano()), Workers: 1,
		PollInterval: 50 * time.Millisecond, DrainTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = pool.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		j, err := queue.Get(ctx, tid, jid)
		if err != nil {
			t.Fatalf("reading the job: %v", err)
		}
		switch j.State {
		case jobs.Completed:
			cancel()
			pool.Wait()
			fmt.Println("job completed")
			return
		case jobs.Failed, jobs.Cancelled:
			t.Fatalf("the job ended %s: %s", j.State, j.LastError)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the job did not complete within two minutes")
}
