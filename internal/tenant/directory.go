package tenant

import (
	"context"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
)

// Meta is what the directory knows about a tenant.
//
// It is canonical data (Part II.4): nothing rebuilds it, and losing it loses
// the answer to "which tenants exist", which is what every cross-tenant
// maintenance job iterates.
type Meta struct {
	ID          ID
	DisplayName string

	// SchemaVersion is the tenant's user-visible schema version (spec §19.3,
	// plan §II.5b). Nothing reads it before Phase 13; it is persisted from the
	// first version so that introducing per-tenant schemas later is additive
	// rather than a migration of every tenant row. It must never move when
	// Remem's storage format version moves, and vice versa.
	SchemaVersion uint32

	CreatedAt time.Time
	UpdatedAt time.Time
}

// DefaultSchemaVersion is the user-schema version a new tenant starts at.
const DefaultSchemaVersion uint32 = 1

// Directory is the register of tenants.
type Directory interface {
	// Create registers a tenant. An id that already exists is errs.Conflict:
	// silently overwriting would discard metadata — including, later, the
	// tenant's schema version — that nothing can reconstruct.
	Create(ctx context.Context, id ID, m Meta) error

	// Get returns a tenant's metadata, or errs.NotFound.
	Get(ctx context.Context, id ID) (Meta, error)

	// List returns every tenant, in id order.
	List(ctx context.Context) ([]Meta, error)

	// ForEach calls fn for every tenant, in id order, stopping at the first
	// error and returning it.
	//
	// This is the only cross-tenant path in Remem (Invariant 1), and it is
	// deliberately awkward to reach: a guard in internal/arch fails the build
	// if a package outside jobs, snapshot and schema calls it. The mistake it
	// exists to prevent is the one Rust Remem made by shipping both
	// vector_search and vector_search_partitioned — once an unscoped variant
	// exists, something eventually calls it.
	ForEach(ctx context.Context, fn func(ID) error) error
}

// Ensure returns the tenant's metadata, creating it if it does not exist.
//
// It is the auto-provisioning path behind config's tenant.auto_provision, kept
// as a function over the interface rather than a fifth method: it is a policy
// composed from two operations, and an implementation that had to provide it
// would be free to make it mean something subtly different.
//
// The Create race is handled by treating Conflict as success and re-reading:
// two requests provisioning the same tenant at once is the normal case for a
// fresh deployment, not an error either of them should see.
func Ensure(ctx context.Context, d Directory, id ID) (Meta, error) {
	m, err := d.Get(ctx, id)
	if err == nil {
		return m, nil
	}
	if !errs.Is(err, errs.NotFound) {
		return Meta{}, err
	}
	if err := d.Create(ctx, id, Meta{}); err != nil && !errs.Is(err, errs.Conflict) {
		return Meta{}, err
	}
	return d.Get(ctx, id)
}
