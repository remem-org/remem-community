// Package auth verifies credentials and turns them into a principal.
//
// It is deliberately small and deliberately below the API layer: it knows
// nothing about HTTP, headers or status codes, and internal/tenant knows
// nothing about it. The one place the two meet is [Principal.Claim], which
// converts a verified principal into the tenant.Claim the resolver reads.
//
// # What a principal is, and what it is not
//
// A principal is the thing holding a credential — an agent, a service, a
// person's CLI. Plan §II.5 is explicit that this is *not* a level of the
// identity hierarchy: making an agent a containment level would mean a memory
// belongs to one agent, and shared memory across an agent fleet is the normal
// case rather than the exception. Ownership, if it is ever wanted, is a field
// on the record.
package auth

import (
	"context"

	"github.com/remem-org/remem-go/internal/tenant"
)

// Principal is a verified caller.
type Principal struct {
	// ID names the credential, for logs and audit. It is never a secret.
	ID string
	// Tenant is the tenant this credential is bound to, empty if unbound.
	// A bound credential is an isolation boundary: it cannot be talked out of
	// its tenant by any header.
	Tenant tenant.ID
	// CrossTenant reports whether this credential may name a tenant per
	// request. Only unbound credentials can carry it.
	CrossTenant bool
	// Authorized is the explicit set of tenants this credential may act in,
	// empty for an authority configuration did not narrow. A cross-tenant
	// operation acts on this scope rather than on every tenant that happens to
	// exist: an unbound credential is not, on its own, authorisation for a
	// tenant created tomorrow.
	Authorized []tenant.ID
}

// Anonymous is the principal of a development server with no credentials
// configured. It is unbound and may name a tenant, which is what makes
// `remem start --data-dir ./data` work without configuration (Invariant 10).
var Anonymous = Principal{ID: "anonymous", CrossTenant: true}

// Claim converts the principal into the tenant claim the resolver reads.
// requested is the tenant the request named, unvalidated — the resolver
// validates it, because it is caller input either way.
func (p Principal) Claim(requested string) tenant.Claim {
	return tenant.Claim{
		Bound:       p.Tenant,
		CrossTenant: p.CrossTenant,
		Authorized:  p.Authorized,
		Requested:   requested,
	}
}

type principalKey struct{}

// NewContext returns ctx carrying the verified principal.
func NewContext(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// FromContext returns the principal ctx carries, if any.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
