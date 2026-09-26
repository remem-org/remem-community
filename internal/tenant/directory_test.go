package tenant_test

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
)

func newDirectory(t *testing.T) tenant.Directory {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	return tenantkv.New(kv, clock.NewFake(clock.FakeStart))
}

func TestCreateThenGet(t *testing.T) {
	d := newDirectory(t)
	ctx := context.Background()

	if err := d.Create(ctx, "acme", tenant.Meta{DisplayName: "Acme Corp"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := d.Get(ctx, "acme")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != "acme" || got.DisplayName != "Acme Corp" {
		t.Fatalf("Get = %+v", got)
	}
	// Part II.5b: the seam for per-tenant user schema versions exists from the
	// first version, defaulting to 1, so adding it later is additive.
	if got.SchemaVersion != 1 {
		t.Fatalf("SchemaVersion = %d, want 1", got.SchemaVersion)
	}
	if got.CreatedAt.IsZero() {
		t.Fatal("CreatedAt was not stamped")
	}
}

func TestGetOfAnAbsentTenantIsNotFound(t *testing.T) {
	if _, err := newDirectory(t).Get(context.Background(), "nobody"); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
}

func TestCreateIsIdempotentlyRefused(t *testing.T) {
	d := newDirectory(t)
	ctx := context.Background()
	if err := d.Create(ctx, "acme", tenant.Meta{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Silently overwriting would discard the existing tenant's metadata —
	// including, later, its schema version.
	if err := d.Create(ctx, "acme", tenant.Meta{DisplayName: "impostor"}); !errs.Is(err, errs.Conflict) {
		t.Fatalf("second Create = %v, want Conflict", err)
	}
}

func TestCreateRefusesAMalformedID(t *testing.T) {
	if err := newDirectory(t).Create(context.Background(), "../etc", tenant.Meta{}); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

func TestListAndForEachSeeEveryTenantInOrder(t *testing.T) {
	d := newDirectory(t)
	ctx := context.Background()
	for _, id := range []tenant.ID{"zulu", "acme", "middle"} {
		if err := d.Create(ctx, id, tenant.Meta{}); err != nil {
			t.Fatalf("Create(%s): %v", id, err)
		}
	}

	metas, err := d.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var listed []string
	for _, m := range metas {
		listed = append(listed, string(m.ID))
	}
	want := []string{"acme", "middle", "zulu"}
	if !sort.StringsAreSorted(listed) {
		t.Fatalf("List returned %v; a directory scan is key-ordered", listed)
	}
	if len(listed) != 3 || listed[0] != want[0] || listed[2] != want[2] {
		t.Fatalf("List = %v, want %v", listed, want)
	}

	var seen []string
	if err := d.ForEach(ctx, func(id tenant.ID) error {
		seen = append(seen, string(id))
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if len(seen) != 3 {
		t.Fatalf("ForEach visited %v, want all three", seen)
	}
}

// ForEach is the one cross-tenant path. A maintenance job that fails partway
// must report that, not silently finish: a rebuild that skipped a tenant and
// said nothing is worse than one that stopped.
func TestForEachStopsAndReportsTheCallbackError(t *testing.T) {
	d := newDirectory(t)
	ctx := context.Background()
	for _, id := range []tenant.ID{"a", "b", "c"} {
		if err := d.Create(ctx, id, tenant.Meta{}); err != nil {
			t.Fatal(err)
		}
	}
	boom := errors.New("handler failed")
	var visited int
	err := d.ForEach(ctx, func(tenant.ID) error {
		visited++
		if visited == 2 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("ForEach = %v, want the callback's error", err)
	}
	if visited != 2 {
		t.Fatalf("ForEach visited %d tenants after a failure, want 2", visited)
	}
}

func TestEnsureProvisionsOnceAndThenReturnsTheExistingTenant(t *testing.T) {
	d := newDirectory(t)
	ctx := context.Background()

	first, err := tenant.Ensure(ctx, d, "acme")
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	second, err := tenant.Ensure(ctx, d, "acme")
	if err != nil {
		t.Fatalf("Ensure again: %v", err)
	}
	if !first.CreatedAt.Equal(second.CreatedAt) {
		t.Fatal("Ensure re-created an existing tenant: its creation time moved")
	}
	metas, _ := d.List(ctx)
	if len(metas) != 1 {
		t.Fatalf("Ensure produced %d tenants, want 1", len(metas))
	}
}

// A tenant row that will not decode is corruption, not an absent tenant.
// Reporting NotFound would let a request proceed against a tenant whose
// metadata this binary cannot read.
func TestACorruptTenantRowIsCorruptionNotAMiss(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	d := tenantkv.New(kv, clock.NewFake(clock.FakeStart))
	ctx := context.Background()
	if err := d.Create(ctx, "acme", tenant.Meta{}); err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(ctx, tenantkv.Key("acme"), []byte("not a tenant row")); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Get(ctx, "acme"); !errs.Is(err, errs.Corruption) {
		t.Fatalf("Get = %v, want Corruption", err)
	}
}
