package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/version"
)

// singleTenantOnly skips a test on a build that resolves tenants from an
// identity, where none of these refusals applies.
func singleTenantOnly(t *testing.T) {
	t.Helper()
	if version.TenantCapability != tenant.SingleTenant.String() {
		t.Skipf("this build declares the %q capability", version.TenantCapability)
	}
}

func scopeStore(t *testing.T, ids ...tenant.ID) *memkv.Store {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	dir := tenantkv.New(kv, clock.System())
	for _, id := range ids {
		if err := dir.Create(context.Background(), id, tenant.Meta{}); err != nil {
			t.Fatal(err)
		}
	}
	return kv
}

// Naming a tenant this build does not serve is refused rather than answered
// with an empty result. A rebuild that reported success over a tenant it never
// looked at is the worst of the available behaviours.
func TestACommandRefusesATenantThisBuildDoesNotServe(t *testing.T) {
	singleTenantOnly(t)
	t.Setenv("REMEM_TENANT_DEFAULT", "solo")

	_, _, err := scope(scopeStore(t, "solo"), clock.System(), "elsewhere")
	if !errs.Is(err, errs.Forbidden) {
		t.Fatalf("got %s, want Forbidden", errs.KindOf(err))
	}
	for _, want := range []string{"solo", "elsewhere", "REMEM_TENANT_DEFAULT", "docs/MIGRATION.md"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not mention %q:\n%v", want, err)
		}
	}
}

// And a directory this build will not serve is refused too, exactly as the
// server refuses to start over it. Working quietly on a subset would hide what
// the inventory exists to surface.
func TestAWritingCommandRefusesAMultiTenantDirectory(t *testing.T) {
	singleTenantOnly(t)
	t.Setenv("REMEM_TENANT_DEFAULT", "solo")

	_, _, err := scope(scopeStore(t, "solo", "elsewhere"), clock.System(), "")
	if !errs.Is(err, errs.MigrationRequired) {
		t.Fatalf("got %s, want migration_required", errs.KindOf(err))
	}
	if !strings.Contains(err.Error(), "elsewhere") {
		t.Fatalf("the refusal does not name the tenant that has to move:\n%v", err)
	}
}

// An unqualified run is narrowed to the implicit tenant, so an ordinary
// rebuild, check or backup of a single-tenant deployment touches one tenant.
func TestAnUnqualifiedCommandIsNarrowedToTheImplicitTenant(t *testing.T) {
	singleTenantOnly(t)
	t.Setenv("REMEM_TENANT_DEFAULT", "solo")

	dir, only, err := scope(scopeStore(t, "solo"), clock.System(), "")
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	if only != "solo" {
		t.Fatalf("the command was scoped to %q, want the implicit tenant", only)
	}
	if _, err := dir.Get(context.Background(), "elsewhere"); !errs.Is(err, errs.NotFound) {
		t.Fatalf("the directory is not confined: %v", err)
	}
}

// Export is the boundary's escape route, so it reads a directory the writing
// commands refuse. scope.go says why at length; this is the assertion.
func TestExportReadsADirectoryTheWritingCommandsRefuse(t *testing.T) {
	singleTenantOnly(t)
	t.Setenv("REMEM_TENANT_DEFAULT", "solo")
	kv := scopeStore(t, "solo", "elsewhere")

	if _, _, err := scope(kv, clock.System(), ""); err == nil {
		t.Fatal("a writing command accepted a directory holding another tenant")
	}

	var notes bytes.Buffer
	dir, only, err := migrationScope(kv, clock.System(), "elsewhere", &notes)
	if err != nil {
		t.Fatalf("export was refused the tenant it exists to move: %v", err)
	}
	if only != "elsewhere" {
		t.Fatalf("the export was scoped to %q", only)
	}
	if _, err := dir.Get(context.Background(), "elsewhere"); err != nil {
		t.Fatalf("the export cannot see the tenant it is moving: %v", err)
	}
	// It says out loud what it is doing, for whoever finds the file later.
	for _, want := range []string{"serves one tenant", "elsewhere", "docs/MIGRATION.md"} {
		if !strings.Contains(notes.String(), want) {
			t.Fatalf("the export note does not mention %q:\n%s", want, notes.String())
		}
	}
}

// An export that names no tenant is an ordinary backup, and an ordinary backup
// of a single-tenant deployment holds one tenant — even from a directory that
// still has others in it.
func TestAnUnqualifiedExportWritesOnlyTheImplicitTenant(t *testing.T) {
	singleTenantOnly(t)
	t.Setenv("REMEM_TENANT_DEFAULT", "solo")

	var notes bytes.Buffer
	dir, only, err := migrationScope(scopeStore(t, "solo", "elsewhere"), clock.System(), "", &notes)
	if err != nil {
		t.Fatalf("migrationScope: %v", err)
	}
	if only != "solo" {
		t.Fatalf("an unqualified export was scoped to %q", only)
	}
	metas, err := dir.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 1 || metas[0].ID != "solo" {
		t.Fatalf("an unqualified export would have written %v", metas)
	}
	if notes.Len() != 0 {
		t.Fatalf("an ordinary backup printed a migration note:\n%s", notes.String())
	}
}

// A restore is the one way a tenant can arrive on disk without anything asking
// the resolver, so it is the one place the boundary is checked against a file.
// The whole file is refused rather than a subset imported.
func TestAnImportRefusesASnapshotCarryingAnotherTenant(t *testing.T) {
	singleTenantOnly(t)
	t.Setenv("REMEM_TENANT_DEFAULT", "solo")

	if err := refuseForeignTenants([]string{"solo"}); err != nil {
		t.Fatalf("a snapshot of this build's own tenant was refused: %v", err)
	}
	err := refuseForeignTenants([]string{"solo", "elsewhere", "third"})
	if !errs.Is(err, errs.Forbidden) {
		t.Fatalf("got %s, want Forbidden", errs.KindOf(err))
	}
	for _, want := range []string{"elsewhere", "third", "no database was created", "docs/MIGRATION.md"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not mention %q:\n%v", want, err)
		}
	}
}

// The refusal fires before the destination is opened, so a refused import
// leaves no database behind — the defect Phase 12 found in the vector-policy
// refusal, which fired from inside the library after the store was open.
func TestARefusedImportOfAnotherTenantLeavesNoDatabase(t *testing.T) {
	singleTenantOnly(t)

	// A snapshot of "acme", imported by a build serving "solo".
	t.Setenv("REMEM_TENANT_DEFAULT", "acme")
	src, _ := seedCorpus(t)
	file := filepath.Join(t.TempDir(), "acme.rsnap")
	if _, err := run([]string{"export", "--data-dir", src, "--out", file}); err != nil {
		t.Fatalf("export: %v", err)
	}

	t.Setenv("REMEM_TENANT_DEFAULT", "solo")
	dst := filepath.Join(t.TempDir(), "destination")
	if _, err := run([]string{"import", "--data-dir", dst, "--in", file,
		"--vectors", "verbatim"}); err == nil {
		t.Fatal("the import accepted a snapshot of a tenant this build does not serve")
	}
	if entries, err := filepath.Glob(dst + "/*"); err == nil && len(entries) > 0 {
		t.Fatalf("the refused import left %d file(s) behind in %s", len(entries), dst)
	}
}
