package tenant_test

import (
	"context"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// The confined directory is what makes a single-tenant build single-tenant
// everywhere the resolver does not reach: the job scheduler's fan-out, the
// metrics labels, the snapshot's tenant list, the session sweep, and the
// retention-policy route's existence check all ask the directory.
func TestAConfinedDirectoryShowsOnlyItsTenant(t *testing.T) {
	inner := directoryOf("acme", "default", "globex")
	d := tenant.Confine(inner, "default")

	metas, err := d.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(metas) != 1 || metas[0].ID != "default" {
		t.Fatalf("List returned %v, want only default", metas)
	}

	var visited []tenant.ID
	if err := d.ForEach(context.Background(), func(id tenant.ID) error {
		visited = append(visited, id)
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if len(visited) != 1 || visited[0] != "default" {
		t.Fatalf("ForEach visited %v, want only default", visited)
	}
}

// Another tenant reads as absent, which is the disposition a memory in another
// tenant already has: whether some other tenant exists is not information this
// deployment has any business confirming.
func TestAConfinedDirectoryReportsAnotherTenantAsAbsent(t *testing.T) {
	d := tenant.Confine(directoryOf("acme", "default"), "default")
	if _, err := d.Get(context.Background(), "acme"); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
	if _, err := d.Get(context.Background(), "default"); err != nil {
		t.Fatalf("the implicit tenant is not readable: %v", err)
	}
}

// Creating one is refused rather than reported absent: a caller asking for it
// has asked for something this build will not do, and "not found" would read as
// a bug in the request.
func TestAConfinedDirectoryRefusesToCreateAnotherTenant(t *testing.T) {
	inner := directoryOf("default")
	d := tenant.Confine(inner, "default")
	err := d.Create(context.Background(), "acme", tenant.Meta{})
	if !errs.Is(err, errs.Forbidden) {
		t.Fatalf("got %s, want Forbidden", errs.KindOf(err))
	}
	if !strings.Contains(err.Error(), "acme") || !strings.Contains(err.Error(), "default") {
		t.Fatalf("the refusal names neither tenant: %v", err)
	}
	if inner.created != 0 {
		t.Fatal("the refusal still reached the real directory")
	}
	if err := d.Create(context.Background(), "default", tenant.Meta{}); err != nil {
		t.Fatalf("the implicit tenant could not be created: %v", err)
	}
}

// "One tenant" and "one tenant that exists" are different answers. A directory
// that does not hold the implicit tenant yet lists nothing, because a caller
// reading a synthesised row would believe the second — and the start-up
// provisioning step that makes it true has not run.
func TestAConfinedDirectoryDoesNotInventItsTenant(t *testing.T) {
	d := tenant.Confine(directoryOf("acme"), "default")
	metas, err := d.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(metas) != 0 {
		t.Fatalf("List returned %v over a directory that does not hold default", metas)
	}
	visited := 0
	if err := d.ForEach(context.Background(), func(tenant.ID) error { visited++; return nil }); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visited != 0 {
		t.Fatalf("ForEach visited %d tenants over a directory that holds none of them", visited)
	}
}

// Ensure is the auto-provisioning path, and it must stay usable for the
// implicit tenant and refused for anything else — otherwise a request naming an
// unknown tenant would create it.
func TestEnsureThroughAConfinedDirectory(t *testing.T) {
	d := tenant.Confine(directoryOf(), "default")
	if _, err := tenant.Ensure(context.Background(), d, "default"); err != nil {
		t.Fatalf("Ensure on the implicit tenant: %v", err)
	}
	if _, err := tenant.Ensure(context.Background(), d, "acme"); err == nil {
		t.Fatal("Ensure provisioned a second tenant through a confined directory")
	}
}
