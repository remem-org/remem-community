package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/txn"
)

func seedTenant(t *testing.T, kv storage.KV, dir *tenantkv.Directory, name tenant.ID, breakIt bool) {
	t.Helper()
	ctx := tenant.NewContext(context.Background(), name)
	if err := dir.Create(ctx, name, tenant.Meta{ID: name}); err != nil {
		t.Fatalf("creating tenant %s: %v", name, err)
	}

	a, b := id.New(), id.New()
	repo := record.NewRepo(kv)
	edges := graph.NewService(kv, clock.NewFake(time.UnixMilli(1_725_000_000_000).UTC()))

	tx := txn.New(kv, txn.Sync(false))
	defer tx.Close()
	for _, rid := range []id.ID{a, b} {
		rec := &record.Record{ID: rid, Tenant: name, Namespace: tenant.DefaultNamespace,
			Type: record.TypeMemory, Content: "a memory",
			CreatedAt: time.UnixMilli(1_725_000_000_000).UTC()}
		if err := repo.Put(ctx, tx, rec); err != nil {
			t.Fatalf("writing a record: %v", err)
		}
	}
	if err := edges.Add(ctx, tx, graph.Edge{From: a, To: b, Type: graph.Supports, Strength: 0.7}); err != nil {
		t.Fatalf("adding an edge: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("committing: %v", err)
	}

	if breakIt {
		sc := graph.Scope{Tenant: name, Namespace: tenant.DefaultNamespace}
		if err := kv.Delete(ctx, graph.InKey(sc, a, graph.Supports, b)); err != nil {
			t.Fatalf("damaging the reverse index: %v", err)
		}
	}
}

// The command walks every tenant through tenant.Directory.ForEach, which is the
// single audited cross-tenant path (Invariant 1). graph.Verify itself is
// tenant-scoped and has no way to reach across.
func TestVerifyWalksEveryTenant(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()
	dir := tenantkv.New(kv, clock.System())
	seedTenant(t, kv, dir, "acme", false)
	seedTenant(t, kv, dir, "globex", false)

	reports, err := verifyTenants(context.Background(), kv, dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 2 {
		t.Fatalf("got %d reports, want one per tenant: %+v", len(reports), reports)
	}
	for _, rep := range reports {
		if !rep.Clean() {
			t.Errorf("tenant %s reported problems on a clean corpus: %+v", rep.Tenant, rep.Findings)
		}
	}
}

func TestVerifyOneTenantSkipsTheOthers(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()
	dir := tenantkv.New(kv, clock.System())
	seedTenant(t, kv, dir, "acme", true)
	seedTenant(t, kv, dir, "globex", false)

	reports, err := verifyTenants(context.Background(), kv, dir, "globex")
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].Tenant != "globex" {
		t.Fatalf("got %+v, want only globex", reports)
	}
	if !reports[0].Clean() {
		t.Fatalf("globex is intact and reported problems: %+v", reports[0].Findings)
	}
}

// "The command worked and found problems" is neither a success nor a failure: a
// checker that exited zero on a broken corpus could not be used as a check.
func TestVerifyReportsDamageAcrossTenants(t *testing.T) {
	kv := memkv.New()
	defer func() { _ = kv.Close() }()
	dir := tenantkv.New(kv, clock.System())
	seedTenant(t, kv, dir, "acme", true)
	seedTenant(t, kv, dir, "globex", false)

	reports, err := verifyTenants(context.Background(), kv, dir, "")
	if err != nil {
		t.Fatal(err)
	}
	broken := 0
	for _, rep := range reports {
		broken += rep.Broken
	}
	if broken == 0 {
		t.Fatal("a damaged reverse index was not reported")
	}
}

func TestGraphVerifyNeedsADataDirectory(t *testing.T) {
	if _, err := graphVerify(nil); err == nil {
		t.Fatal("graph verify ran without --data-dir")
	}
}

func TestUnknownCommandsAreRefusedByName(t *testing.T) {
	if _, err := run([]string{"nonsense"}); err == nil {
		t.Fatal("an unknown command was accepted")
	}
	if _, err := graphCommand([]string{"repair"}); err == nil {
		t.Fatal("an unknown graph subcommand was accepted")
	}
}

// Found by the Phase 6 end-to-end verification: a mistyped --data-dir created an
// empty Pebble database and reported "no tenants found", which reads as a clean
// corpus. Read-only mode refuses a directory that holds no database, and it also
// matches what verify does — it never writes.
func TestVerifyRefusesADirectoryThatHoldsNoDatabase(t *testing.T) {
	dir := t.TempDir() + "/not-a-data-directory"

	code, err := graphVerify([]string{"--data-dir", dir})
	if err == nil {
		t.Fatalf("a path with no database was accepted, exit code %d", code)
	}
	if _, statErr := os.Stat(dir); statErr == nil {
		t.Fatal("the failed open created the directory; an inspection tool must not")
	}
}
