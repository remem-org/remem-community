package server

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/version"
)

func tenancyConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Storage.Engine = "memory"
	cfg.Storage.SyncWrites = false
	cfg.Server.HTTPAddr = "127.0.0.1:0"
	cfg.Log.Level = "error"
	return cfg
}

// The build's own declaration is the default policy, and today every officially
// supported build declares one tenant. A server composed without an explicit
// capability is that build's server.
func TestTheBuildsDeclaredCapabilityIsTheDefault(t *testing.T) {
	want, err := tenant.ParseCapability(version.TenantCapability)
	if err != nil {
		t.Fatalf("this build's declared capability does not parse: %v", err)
	}
	srv, err := New(tenancyConfig(t), WithEmbedder(embeddingtest.New()), WithClock(clock.System()))
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	t.Cleanup(func() { _ = srv.deps.close() })
	if srv.deps.capability != want {
		t.Fatalf("the server composed itself as %s, and this build declares %s",
			srv.deps.capability, want)
	}
}

// Dependencies built without a policy hand out no directory at all. The
// directory is the one thing that must not be unconfined by default: the job
// scheduler, the metrics labels and the snapshot all read it, and none of them
// asks a second question.
func TestDependenciesWithoutACapabilityAreRefused(t *testing.T) {
	_, err := build(tenancyConfig(t), options{embedder: embeddingtest.New(), clk: clock.System()})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
	if !strings.Contains(err.Error(), "tenant capability") {
		t.Fatalf("the refusal does not name what is missing: %v", err)
	}
}

// Under one tenant the directory everything downstream reads is confined, and
// under identity tenancy it is not. This is the whole of the mechanism, so it is
// asserted on the field rather than inferred from a behaviour three layers up.
func TestTheDirectoryIsConfinedOnlyUnderSingleTenancy(t *testing.T) {
	for _, tc := range []struct {
		capability tenant.Capability
		confined   bool
	}{
		{tenant.SingleTenant, true},
		{tenant.IdentityScoped, false},
	} {
		t.Run(tc.capability.String(), func(t *testing.T) {
			d, err := build(tenancyConfig(t), options{
				embedder: embeddingtest.New(), clk: clock.System(), capability: tc.capability,
			})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			t.Cleanup(func() { _ = d.close() })

			_, isRaw := d.tenants.(*tenantkv.Directory)
			if isRaw == tc.confined {
				t.Fatalf("under %s the directory is %T", tc.capability, d.tenants)
			}
			// The migration runner and the inventory keep the unconfined one:
			// a migration that skipped a tenant's rows would leave them at an
			// old format with nothing reporting it.
			if d.allTenants == nil {
				t.Fatal("the unconfined directory is missing; migrations need it")
			}
			if err := d.tenants.Create(context.Background(), "acme", tenant.Meta{}); tc.confined != errs.Is(err, errs.Forbidden) {
				t.Fatalf("creating a second tenant under %s returned %v", tc.capability, err)
			}
		})
	}
}

// A single-tenant build refuses a directory holding another tenant, and it
// refuses before the migrations, before any index is opened and before anything
// listens. Nothing has been written at that point, so the directory is exactly
// as the operator left it — which is what makes rollback the prior binary over
// it rather than a repair.
func TestASingleTenantBuildRefusesAMultiTenantDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	directory := tenantkv.New(kv, clock.System())
	for _, id := range []tenant.ID{"default", "acme"} {
		if err := directory.Create(context.Background(), id, tenant.Meta{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := tenancyConfig(t)
	cfg.Storage.Engine = "pebble"
	cfg.Storage.Path = dir

	_, err = New(cfg, WithEmbedder(embeddingtest.New()), WithClock(clock.System()),
		WithTenantCapability(tenant.SingleTenant))
	if !errs.Is(err, errs.MigrationRequired) {
		t.Fatalf("got %v, want migration_required", err)
	}
	for _, want := range []string{"acme", "remem-admin export", "docs/MIGRATION.md"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not mention %q:\n%v", want, err)
		}
	}

	// The same directory under identity tenancy opens, which is what makes the
	// refusal a policy rather than a corruption report.
	srv, err := New(cfg, WithEmbedder(embeddingtest.New()), WithClock(clock.System()),
		WithTenantCapability(tenant.IdentityScoped))
	if err != nil {
		t.Fatalf("identity tenancy refused a multi-tenant directory: %v", err)
	}
	_ = srv.deps.close()
}

// A directory holding only the implicit tenant is what a single-tenant build is
// for, and a fresh one is too: "no tenants yet" is not a migration problem.
func TestASingleTenantBuildOpensTheDirectoriesItServes(t *testing.T) {
	for _, registered := range [][]tenant.ID{nil, {"default"}} {
		dir := filepath.Join(t.TempDir(), "data")
		kv, err := pebble.Open(dir, pebble.Options{})
		if err != nil {
			t.Fatal(err)
		}
		directory := tenantkv.New(kv, clock.System())
		for _, id := range registered {
			if err := directory.Create(context.Background(), id, tenant.Meta{}); err != nil {
				t.Fatal(err)
			}
		}
		if err := kv.Close(); err != nil {
			t.Fatal(err)
		}

		cfg := tenancyConfig(t)
		cfg.Storage.Engine = "pebble"
		cfg.Storage.Path = dir
		srv, err := New(cfg, WithEmbedder(embeddingtest.New()), WithClock(clock.System()),
			WithTenantCapability(tenant.SingleTenant))
		if err != nil {
			t.Fatalf("a directory holding %v was refused: %v", registered, err)
		}
		_ = srv.deps.close()
	}
}
