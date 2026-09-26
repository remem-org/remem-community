package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// soloDeps is a single-tenant server's dependencies, with a second tenant
// smuggled into the unconfined directory *after* the start-up inventory has
// passed.
//
// That is not a state an operator can reach — the inventory refuses such a
// directory at start-up — and it is exactly the state worth testing. The
// inventory is one check at one moment; the confinement is what holds if a
// tenant row appears afterwards, whether from a migration, a restore, or a
// future code path nobody has written yet. A test that only ever had one tenant
// in the store would be a test of nothing.
func soloDeps(t *testing.T, mutate func(*config.Config)) (*deps, tenant.ID) {
	t.Helper()
	cfg := config.Default()
	cfg.Storage.Engine = "memory"
	cfg.Storage.SyncWrites = false
	cfg.Server.Env = "development"
	cfg.Tenant.Default = "solo"
	cfg.Jobs.Retention = time.Hour
	if mutate != nil {
		mutate(&cfg)
	}

	d, err := build(cfg, options{
		embedder:   embeddingtest.New(),
		clk:        clock.System(),
		capability: tenant.SingleTenant,
	})
	if err != nil {
		t.Fatalf("building a single-tenant server's dependencies: %v", err)
	}
	t.Cleanup(func() { _ = d.close() })

	implicit := tenant.ID(cfg.Tenant.Default)
	for _, id := range []tenant.ID{implicit, "elsewhere"} {
		if _, err := tenant.Ensure(tenant.NewContext(context.Background(), id), d.allTenants, id); err != nil {
			t.Fatal(err)
		}
	}
	return d, implicit
}

// Background work is scheduled by walking the directory, so a single-tenant
// build's directory is what keeps it single-tenant. Nothing in the job
// framework asks a second question, which is the point: an edition check inside
// the dispatcher would be one more place to forget it.
func TestBackgroundWorkRunsOnlyForTheImplicitTenant(t *testing.T) {
	d, implicit := soloDeps(t, func(c *config.Config) {
		c.Jobs.PollInterval = 20 * time.Millisecond
	})

	ctx := context.Background()
	for _, tid := range []tenant.ID{implicit, "elsewhere"} {
		scoped := tenant.NewContext(ctx, tid)
		if err := txn.Do(scoped, d.kv, func(tx txn.Tx) error {
			return d.queue.Enqueue(scoped, tx, &jobs.Job{
				Tenant: tid, Namespace: tenant.DefaultNamespace, Type: TypeJobsReap,
			})
		}); err != nil {
			t.Fatalf("enqueueing for %s: %v", tid, err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.pool.Run(runCtx) }()

	// Wait for the implicit tenant's job to finish, which is also how we know
	// the dispatcher has polled at all.
	deadline := time.Now().Add(10 * time.Second)
	var mine []*jobs.Job
	for time.Now().Before(deadline) {
		var err error
		mine, err = d.queue.List(ctx, implicit, jobs.Filter{Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(mine) == 1 && mine[0].State == jobs.Completed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("the pool stopped with %v", err)
	}
	if len(mine) != 1 || mine[0].State != jobs.Completed {
		t.Fatalf("the implicit tenant's job is %+v; the pool never ran it", mine)
	}

	theirs, err := d.queue.List(ctx, "elsewhere", jobs.Filter{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(theirs) != 1 {
		t.Fatalf("the other tenant has %d jobs, want the one that was enqueued", len(theirs))
	}
	if theirs[0].State != jobs.Pending {
		t.Fatalf("a single-tenant build ran another tenant's job: it is %s", theirs[0].State)
	}
}

// The scheduler fans out one recurring job per tenant, and under one tenant
// that is one job. Its fan-out is the same directory walk the dispatcher makes,
// so the two are confined by the same wrapper — and this asserts the second
// half rather than assuming it from the first.
func TestTheSchedulerFansOutToTheImplicitTenantOnly(t *testing.T) {
	d, implicit := soloDeps(t, nil)

	var visited []tenant.ID
	if err := d.tenants.ForEach(context.Background(), func(id tenant.ID) error {
		visited = append(visited, id)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(visited) != 1 || visited[0] != implicit {
		t.Fatalf("the fan-out visits %v, want only %s", visited, implicit)
	}
}

// The unconfined directory stays reachable for exactly two things — the
// inventory and the migration runner — and nothing else in deps holds it. A
// migration that skipped a tenant's rows would leave them at an old format with
// nothing reporting it, which is why the raw directory exists at all.
func TestTheUnconfinedDirectoryIsStillThereForMigrations(t *testing.T) {
	d, implicit := soloDeps(t, nil)
	metas, err := d.allTenants.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 2 {
		t.Fatalf("the unconfined directory lists %d tenants, want both", len(metas))
	}

	inv, err := tenant.TakeInventory(context.Background(), d.allTenants, implicit)
	if err != nil {
		t.Fatal(err)
	}
	if inv.SingleTenant() {
		t.Fatal("the inventory cannot see the tenant it would have to refuse")
	}
}

// Provisioning through the directory everything uses is refused for anything
// but the implicit tenant, so no request, job or restore can widen the set
// behind the inventory's back.
func TestNothingCanProvisionASecondTenant(t *testing.T) {
	d, implicit := soloDeps(t, nil)
	err := d.tenants.Create(context.Background(), "another", tenant.Meta{})
	if !errs.Is(err, errs.Forbidden) {
		t.Fatalf("got %s, want Forbidden", errs.KindOf(err))
	}
	if !strings.Contains(err.Error(), string(implicit)) {
		t.Fatalf("the refusal does not name the tenant this build serves: %v", err)
	}
}
