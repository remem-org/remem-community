package tenant

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/remem-org/remem-go/internal/errs"
)

// Claim is what an authenticated caller asserts about its tenant scope.
//
// It exists so that this package never has to know credentials exist:
// internal/auth imports internal/tenant, so the dependency cannot run the other
// way, and a Resolver that took an auth.Principal would be a cycle. The auth
// middleware converts a verified principal into a Claim and puts it in the
// context; the Resolver reads only this.
type Claim struct {
	// Bound is the tenant the credential belongs to, empty for a credential
	// that is not tied to one.
	Bound ID
	// CrossTenant reports whether the credential may name a tenant other than
	// the one it is bound to.
	CrossTenant bool
	// Authorized is the explicit set of tenants a cross-tenant credential may
	// act in, in configuration order. Empty means the credential's authority
	// is not narrowed to a set — which is the single operator credential, and
	// is why configuration has to say so rather than leave it to be inferred
	// from the absence of a binding.
	//
	// A one-element set is a binding and arrives as Bound instead; anything
	// here has more than one entry.
	Authorized []ID
	// Requested is the tenant the request named — the X-Remem-Tenant header
	// over HTTP. It is caller input and is validated here, not trusted.
	Requested string
}

// authorizes reports whether the claim's explicit set admits t. A claim with no
// set is not narrowed, so it admits everything it was otherwise allowed.
func (c Claim) authorizes(t ID) bool {
	if len(c.Authorized) == 0 {
		return true
	}
	for _, a := range c.Authorized {
		if a == t {
			return true
		}
	}
	return false
}

func (c Claim) authorizedNames() string {
	names := make([]string, len(c.Authorized))
	for i, a := range c.Authorized {
		names[i] = string(a)
	}
	return strings.Join(names, ", ")
}

// Resolver decides which tenant a request belongs to, under the tenant policy
// the build was composed with.
//
// There are two policies and they differ in what they refuse, which is why
// [Capability] has no default: a resolver built without one refuses everything
// rather than silently picking the more permissive.
//
// Under [SingleTenant] there is one tenant and no way to name another. Under
// [IdentityScoped] the order is Part II.5's, and each step exists for a reason:
//
//  1. A bound credential decides, and a header cannot override it. This is
//     what makes an issued API key an isolation boundary rather than a
//     suggestion — a stolen key is confined to the tenant it was cut for.
//  2. A header is honoured only for a principal authorised across tenants, and
//     only for a tenant that principal is authorised for. Otherwise any caller
//     could read any tenant by guessing a name.
//  3. Otherwise the configured default. This is what keeps
//     `remem start --data-dir ./data` a complete experience (Invariant 10):
//     a single-node deployment that never mentions tenancy still has one.
//
// There is no fourth step. A Resolver with no default refuses rather than
// inventing a tenant, because Invariant 1 has no unscoped fallback.
type Resolver struct {
	// Capability is the edition's tenant policy. It is required.
	Capability Capability
	// Default is the tenant an unclaimed request belongs to. Under
	// [SingleTenant] it is *the* tenant — the implicit one every operation
	// resolves to — and there is nothing else to fall back from.
	Default ID
}

// FromContext resolves the tenant for ctx.
func (r Resolver) FromContext(ctx context.Context) (ID, error) {
	const op = "tenant.Resolver.FromContext"

	c, _ := ClaimFrom(ctx)

	switch r.Capability {
	case SingleTenant:
		return r.single(c, op)
	case IdentityScoped:
		return r.identity(c, op)
	default:
		// Fail closed. The two policies differ in what they refuse, so a
		// resolver with neither cannot answer, and answering with the
		// permissive one would be an isolation decision nobody made.
		return "", errs.E(errs.Invalid, op, errors.New(
			"this server was composed without a tenant capability, so no request can be scoped to a tenant"))
	}
}

// single is the Community policy: one implicit tenant, and no caller-supplied
// selector.
//
// The selector is refused rather than ignored. Ignoring it would answer a
// request for another tenant's memories with this tenant's, which is the worst
// of the three possible behaviours: the caller cannot tell, and an integration
// written against it silently reads the wrong corpus.
func (r Resolver) single(c Claim, op string) (ID, error) {
	if c.Requested != "" {
		return "", errs.E(errs.Forbidden, op, fmt.Errorf(
			"this build serves one tenant, so a request may not name %q. A deployment that selects "+
				"tenants per request resolves them from an authenticated identity; docs/EDITION_BOUNDARY.md "+
				"describes the two", c.Requested))
	}
	if r.Default == "" {
		return "", errs.E(errs.Invalid, op, errors.New(
			"this build serves one tenant and none is configured; set tenant.default"))
	}
	if !r.Default.Valid() {
		return "", errs.E(errs.Invalid, op, fmt.Errorf(
			"the implicit tenant is configured as %q, which is not a tenant id", r.Default))
	}
	// A credential cut for another tenant is refused rather than quietly
	// served the implicit one: it is evidence that this directory was serving
	// more than one tenant, which is what the start-up inventory refuses.
	if c.Bound != "" && c.Bound != r.Default {
		return "", errs.E(errs.Forbidden, op, fmt.Errorf(
			"this credential is bound to tenant %q and this build serves only %q",
			c.Bound, r.Default))
	}
	return r.Default, nil
}

// identity is the Enterprise/Cloud policy: the authenticated identity and its
// authorization decide, and a selector is input to that decision.
func (r Resolver) identity(c Claim, op string) (ID, error) {
	if c.Bound != "" {
		if !c.Bound.Valid() {
			return "", errs.E(errs.Invalid, op, fmt.Errorf("the credential is bound to %q, which is not a tenant id", c.Bound))
		}
		return c.Bound, nil
	}

	if c.Requested != "" {
		if !c.CrossTenant {
			return "", errs.E(errs.Forbidden, op, errors.New(
				"this credential may not name a tenant; the tenant a request belongs to is the one its credential is bound to"))
		}
		id, err := Parse(c.Requested)
		if err != nil {
			return "", err
		}
		if !c.authorizes(id) {
			return "", errs.E(errs.Forbidden, op, fmt.Errorf(
				"this credential is authorized for %s, and not for %q", c.authorizedNames(), id))
		}
		return id, nil
	}

	// A credential authorized for an explicit set of tenants has to be told
	// which one a request is for. There is no sensible default: falling back to
	// the deployment's default tenant would serve a tenant the credential may
	// not even be authorized for, and picking the first of the set would make
	// the answer depend on configuration order.
	if len(c.Authorized) > 0 {
		return "", errs.E(errs.Invalid, op, fmt.Errorf(
			"this credential is authorized for %s; name one of them with the tenant header",
			c.authorizedNames()))
	}

	if r.Default == "" {
		return "", errs.E(errs.Invalid, op, errors.New(
			"the request named no tenant, the credential is bound to none, and no default tenant is configured"))
	}
	return r.Default, nil
}

type claimKey struct{}

// WithClaim attaches a caller's tenant claim to ctx. Only the authentication
// middleware calls it.
func WithClaim(ctx context.Context, c Claim) context.Context {
	return context.WithValue(ctx, claimKey{}, c)
}

// ClaimFrom returns the claim ctx carries, if any.
func ClaimFrom(ctx context.Context) (Claim, bool) {
	c, ok := ctx.Value(claimKey{}).(Claim)
	return c, ok
}

type tenantKey struct{}

// NewContext returns ctx scoped to the resolved tenant t.
//
// Everything below the API layer reads the tenant from the context rather than
// from a parameter, so that a function several layers down cannot be called
// with the wrong one — and so that adding a new read path does not mean adding
// a new place to forget the scope.
func NewContext(ctx context.Context, t ID) context.Context {
	return context.WithValue(ctx, tenantKey{}, t)
}

// FromContext returns the resolved tenant, and whether ctx carries one.
func FromContext(ctx context.Context) (ID, bool) {
	t, ok := ctx.Value(tenantKey{}).(ID)
	return t, ok && t != ""
}

// Require returns the resolved tenant, or an error.
//
// It is what every read and write path below the API layer calls, and it is
// the last place Invariant 1 can be enforced in code rather than by review. It
// refuses rather than returning a zero id, because keys would build a
// perfectly valid key from a zero id and put a record where nobody owns it.
func Require(ctx context.Context) (ID, error) {
	t, ok := FromContext(ctx)
	if !ok {
		return "", errs.E(errs.Invalid, "tenant.Require", errors.New(
			"this context carries no tenant: there is no unscoped read path (Invariant 1)"))
	}
	if !t.Valid() {
		return "", errs.E(errs.Invalid, "tenant.Require", fmt.Errorf("context carries %q, which is not a tenant id", t))
	}
	return t, nil
}
