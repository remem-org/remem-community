package tenant_test

import (
	"context"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// resolve runs one claim through one capability. The whole of the edition
// tenancy contract is what this returns, so the tests below are a table over it
// rather than prose.
func resolve(c tenant.Capability, def tenant.ID, claim tenant.Claim) (tenant.ID, error) {
	r := tenant.Resolver{Capability: c, Default: def}
	return r.FromContext(tenant.WithClaim(context.Background(), claim))
}

// A resolver composed without a capability answers nothing. The two policies
// differ in what they refuse, so picking one would be an isolation decision
// nobody made — and the permissive one is the wrong guess.
func TestAResolverWithNoCapabilityRefusesEverything(t *testing.T) {
	for _, claim := range []tenant.Claim{
		{},
		{Bound: "acme"},
		{CrossTenant: true, Requested: "acme"},
	} {
		got, err := resolve(tenant.NoCapability, "default", claim)
		if err == nil {
			t.Fatalf("a resolver with no capability resolved %q for %+v", got, claim)
		}
		if !errs.Is(err, errs.Invalid) {
			t.Fatalf("got %s, want Invalid", errs.KindOf(err))
		}
	}
}

// The Community contract: every supported operation resolves to the configured
// implicit tenant, and any caller-supplied selector is refused.
func TestSingleTenantResolvesToTheImplicitTenant(t *testing.T) {
	for _, tc := range []struct {
		name  string
		claim tenant.Claim
	}{
		{"no credential at all", tenant.Claim{}},
		{"an unbound credential", tenant.Claim{CrossTenant: true}},
		{"a credential bound to the implicit tenant", tenant.Claim{Bound: "default"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolve(tenant.SingleTenant, "default", tc.claim)
			if err != nil {
				t.Fatalf("FromContext: %v", err)
			}
			if got != "default" {
				t.Fatalf("resolved %q, want the implicit tenant", got)
			}
		})
	}
}

// A selector is refused rather than ignored. Ignoring it would answer a request
// for another tenant's memories with this tenant's, which the caller cannot
// tell apart from a correct answer.
func TestSingleTenantRefusesEveryCallerSuppliedSelector(t *testing.T) {
	for _, tc := range []struct {
		name  string
		claim tenant.Claim
	}{
		{"a selector from an ordinary credential", tenant.Claim{Requested: "acme"}},
		{"a selector from a cross-tenant credential", tenant.Claim{CrossTenant: true, Requested: "acme"}},
		{"a selector naming the implicit tenant", tenant.Claim{Requested: "default"}},
		{"a selector from a bound credential", tenant.Claim{Bound: "default", Requested: "acme"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolve(tenant.SingleTenant, "default", tc.claim)
			if err == nil {
				t.Fatalf("a single-tenant build resolved %q for a request that named a tenant", got)
			}
			if !errs.Is(err, errs.Forbidden) {
				t.Fatalf("got %s, want Forbidden", errs.KindOf(err))
			}
		})
	}
}

// Even a selector naming the implicit tenant is refused, and the reason is not
// pedantry: a client that gets away with sending the header against one
// deployment sends it against the next, where it names something else.
func TestSingleTenantRefusesTheImplicitTenantByNameToo(t *testing.T) {
	_, err := resolve(tenant.SingleTenant, "default", tenant.Claim{Requested: "default"})
	if !errs.Is(err, errs.Forbidden) {
		t.Fatalf("got %v, want Forbidden", err)
	}
}

// A credential cut for another tenant is evidence that this directory served
// more than one, which is what the start-up inventory refuses. Serving it the
// implicit tenant's memories instead would be the silent wrong answer.
func TestSingleTenantRefusesACredentialBoundElsewhere(t *testing.T) {
	_, err := resolve(tenant.SingleTenant, "default", tenant.Claim{Bound: "acme"})
	if !errs.Is(err, errs.Forbidden) {
		t.Fatalf("got %v, want Forbidden", err)
	}
	if !strings.Contains(err.Error(), "acme") || !strings.Contains(err.Error(), "default") {
		t.Fatalf("the refusal names neither tenant: %v", err)
	}
}

func TestSingleTenantWithNoImplicitTenantConfiguredRefuses(t *testing.T) {
	_, err := resolve(tenant.SingleTenant, "", tenant.Claim{})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
	if !strings.Contains(err.Error(), "tenant.default") {
		t.Fatalf("the refusal does not name the setting to fix: %v", err)
	}
}

// The Enterprise/Cloud contract: a bound credential stays bound, an authorized
// selection is honoured, an unauthorized one is refused before any data is
// read.
func TestIdentityScopedHonoursAnAuthorizedSelection(t *testing.T) {
	got, err := resolve(tenant.IdentityScoped, "default", tenant.Claim{
		CrossTenant: true, Authorized: []tenant.ID{"acme", "globex"}, Requested: "globex",
	})
	if err != nil {
		t.Fatalf("FromContext: %v", err)
	}
	if got != "globex" {
		t.Fatalf("resolved %q, want globex", got)
	}
}

func TestIdentityScopedRefusesATenantOutsideTheAuthorizedSet(t *testing.T) {
	_, err := resolve(tenant.IdentityScoped, "default", tenant.Claim{
		CrossTenant: true, Authorized: []tenant.ID{"acme", "globex"}, Requested: "beta",
	})
	if !errs.Is(err, errs.Forbidden) {
		t.Fatalf("got %s, want Forbidden", errs.KindOf(err))
	}
	if !strings.Contains(err.Error(), "acme, globex") {
		t.Fatalf("the refusal does not name the authorized set: %v", err)
	}
}

// A credential authorized for several tenants has to be told which one. The two
// alternatives are both worse: the deployment default may be a tenant this
// credential is not authorized for at all, and the first of the set would make
// the answer depend on configuration order.
func TestIdentityScopedRefusesAnUnnamedTenantForAMultiTenantCredential(t *testing.T) {
	_, err := resolve(tenant.IdentityScoped, "default", tenant.Claim{
		CrossTenant: true, Authorized: []tenant.ID{"acme", "globex"},
	})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %s, want Invalid", errs.KindOf(err))
	}
	if !strings.Contains(err.Error(), "acme, globex") {
		t.Fatalf("the refusal does not name the authorized set: %v", err)
	}
}

// A fixed credential stays fixed under both policies: that is what makes an
// issued key an isolation boundary rather than a suggestion.
func TestAFixedCredentialStaysBoundUnderIdentityTenancy(t *testing.T) {
	got, err := resolve(tenant.IdentityScoped, "default", tenant.Claim{
		Bound: "acme", Requested: "globex",
	})
	if err != nil {
		t.Fatalf("FromContext: %v", err)
	}
	if got != "acme" {
		t.Fatalf("resolved %q; a header must not talk a bound credential out of its tenant", got)
	}
}

// An authorized set on a credential that may not name a tenant narrows nothing
// it could have reached anyway, and must not become a way to reach one.
func TestIdentityScopedStillRefusesASelectorFromAnOrdinaryCredential(t *testing.T) {
	_, err := resolve(tenant.IdentityScoped, "default", tenant.Claim{
		Authorized: []tenant.ID{"acme"}, Requested: "acme",
	})
	if !errs.Is(err, errs.Forbidden) {
		t.Fatalf("got %s, want Forbidden", errs.KindOf(err))
	}
}

func TestCapabilityNamesAreStable(t *testing.T) {
	for _, tc := range []struct {
		c    tenant.Capability
		want string
	}{
		{tenant.NoCapability, "none"},
		{tenant.SingleTenant, "single-tenant"},
		{tenant.IdentityScoped, "identity"},
	} {
		if got := tc.c.String(); got != tc.want {
			t.Fatalf("capability %d names itself %q, want %q", tc.c, got, tc.want)
		}
	}
	if got := tenant.Capability(99).String(); !strings.Contains(got, "99") {
		t.Fatalf("an unknown capability names itself %q", got)
	}
}

func TestParseCapabilityRoundTripsAndRefusesTheRest(t *testing.T) {
	for _, c := range []tenant.Capability{tenant.SingleTenant, tenant.IdentityScoped} {
		got, err := tenant.ParseCapability(c.String())
		if err != nil || got != c {
			t.Fatalf("ParseCapability(%q) = %v, %v", c, got, err)
		}
	}
	for _, name := range []string{"", "none", "community", "enterprise", "multi"} {
		if _, err := tenant.ParseCapability(name); !errs.Is(err, errs.Invalid) {
			t.Fatalf("ParseCapability(%q) was accepted", name)
		}
	}
}
