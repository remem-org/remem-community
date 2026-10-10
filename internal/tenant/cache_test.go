package tenant_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// countingDirectory records what reached the inner directory, which is the
// whole subject of these tests: the value of the cache is the read that does
// not happen.
type countingDirectory struct {
	*fakeDirectory
	gets int
}

func (d *countingDirectory) Get(ctx context.Context, id tenant.ID) (tenant.Meta, error) {
	d.gets++
	return d.fakeDirectory.Get(ctx, id)
}

func cachedOf(ttl time.Duration, ids ...tenant.ID) (tenant.Directory, *countingDirectory, *clock.Fake) {
	inner := &countingDirectory{fakeDirectory: directoryOf(ids...)}
	clk := clock.NewFake(clock.FakeStart)
	return tenant.Cached(inner, clk, ttl), inner, clk
}

// Every authenticated request checks that its tenant exists, so that check is
// one KV read on the hot path of every call Remem serves.
func TestACachedDirectoryServesALaterLookupWithoutReadingTheDirectory(t *testing.T) {
	d, inner, _ := cachedOf(30*time.Second, "acme")

	first, err := d.Get(context.Background(), "acme")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	second, err := d.Get(context.Background(), "acme")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if inner.gets != 1 {
		t.Fatalf("the inner directory saw %d reads, want 1", inner.gets)
	}
	if second != first {
		t.Fatalf("the cached meta is %+v, want %+v", second, first)
	}
}

// The one thing this cache must never do. A tenant that does not exist has to
// stay absent, and a tenant that has just been created has to be visible at
// once — which is the same requirement seen from either side, because
// auto-provisioning reads, creates and reads again.
func TestACachedDirectoryNeverCachesAnAbsentTenant(t *testing.T) {
	d, inner, _ := cachedOf(30 * time.Second)

	if _, err := d.Get(context.Background(), "acme"); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
	// Created behind the cache, as another process would.
	if err := inner.fakeDirectory.Create(context.Background(), "acme", tenant.Meta{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := d.Get(context.Background(), "acme"); err != nil {
		t.Fatalf("a tenant created after an absent lookup is still absent: %v", err)
	}
}

// A write through the decorator drops what the cache believes about that
// tenant, so nothing the decorator itself does can be read back stale.
func TestACachedDirectoryForgetsATenantItWrites(t *testing.T) {
	d, inner, _ := cachedOf(30*time.Second, "acme")

	if _, err := d.Get(context.Background(), "acme"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := d.Create(context.Background(), "acme", tenant.Meta{DisplayName: "Acme"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := d.Get(context.Background(), "acme"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if inner.gets != 2 {
		t.Fatalf("the inner directory saw %d reads, want 2: a Create must drop the entry", inner.gets)
	}
}

// The TTL is the bound on how long a tenant deleted by another process stays
// accepted, so it has to actually expire.
func TestACachedDirectoryReadsAgainAfterTheTTL(t *testing.T) {
	d, inner, clk := cachedOf(30*time.Second, "acme")

	if _, err := d.Get(context.Background(), "acme"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	clk.Advance(29 * time.Second)
	if _, err := d.Get(context.Background(), "acme"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if inner.gets != 1 {
		t.Fatalf("the inner directory saw %d reads within the TTL, want 1", inner.gets)
	}

	clk.Advance(2 * time.Second)
	if _, err := d.Get(context.Background(), "acme"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if inner.gets != 2 {
		t.Fatalf("the inner directory saw %d reads past the TTL, want 2", inner.gets)
	}
}

// anyDirectory answers for every tenant, so a test can fill the cache without
// the inner directory's own cost dominating it.
type anyDirectory struct{ gets int }

func (d *anyDirectory) Create(context.Context, tenant.ID, tenant.Meta) error { return nil }
func (d *anyDirectory) Get(_ context.Context, id tenant.ID) (tenant.Meta, error) {
	d.gets++
	return tenant.Meta{ID: id}, nil
}
func (d *anyDirectory) List(context.Context) ([]tenant.Meta, error)          { return nil, nil }
func (d *anyDirectory) ForEach(context.Context, func(tenant.ID) error) error { return nil }

// A cache with no bound is a leak with a hit rate. The bound is generous
// because entries exist only for tenants that do exist, so the real limit is
// the deployment's tenant count; it is here so that "the real limit" is never
// the operator's discovery.
func TestACachedDirectoryIsBounded(t *testing.T) {
	inner := &anyDirectory{}
	d := tenant.Cached(inner, clock.NewFake(clock.FakeStart), 30*time.Second)

	for i := 0; i <= tenant.CacheMaxEntries; i++ {
		if _, err := d.Get(context.Background(), tenant.ID("t"+strconv.Itoa(i))); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}
	reads := inner.gets
	if _, err := d.Get(context.Background(), "t0"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if inner.gets != reads+1 {
		t.Fatal("the cache kept every entry past its bound")
	}
}
