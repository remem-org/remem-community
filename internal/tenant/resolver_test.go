package tenant_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// TestResolutionOrder is plan Task 3.1's table, and it is the whole of the
// tenancy policy Part II.5 describes: a bound credential decides, a header is
// honoured only for a principal authorised to name another tenant, and an
// unconfigured deployment falls back to the configured default.
func TestResolutionOrder(t *testing.T) {
	for _, tc := range []struct {
		name            string
		principalTenant string
		crossTenant     bool
		header          string
		want            tenant.ID
		wantErr         bool
	}{
		{"bound principal wins", "acme", false, "evil", "acme", false},
		{"header honoured only for cross-tenant principals", "", true, "acme", "acme", false},
		{"header refused for ordinary principals", "", false, "acme", "", true},
		{"falls back to the configured default", "", false, "", "default", false},
		{"cross-tenant principal with no header still defaults", "", true, "", "default", false},
		{"bound principal with no header", "acme", false, "", "acme", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := tenant.Resolver{Capability: tenant.IdentityScoped, Default: "default"}
			ctx := tenant.WithClaim(context.Background(), tenant.Claim{
				Bound:       tenant.ID(tc.principalTenant),
				CrossTenant: tc.crossTenant,
				Requested:   tc.header,
			})

			got, err := r.FromContext(ctx)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("FromContext resolved %q; a header from a principal that may not name a tenant must be refused", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("FromContext: %v", err)
			}
			if got != tc.want {
				t.Fatalf("resolved %q, want %q", got, tc.want)
			}
		})
	}
}

// A request that carried no credential at all still resolves, to the default.
// That is what keeps `remem start --data-dir ./data` a complete experience
// (spec §52, Invariant 10).
func TestResolutionWithoutAClaimIsTheDefault(t *testing.T) {
	got, err := tenant.Resolver{Capability: tenant.IdentityScoped, Default: "default"}.FromContext(context.Background())
	if err != nil {
		t.Fatalf("FromContext: %v", err)
	}
	if got != "default" {
		t.Fatalf("resolved %q, want the configured default", got)
	}
}

func TestResolverRefusesAMalformedHeader(t *testing.T) {
	ctx := tenant.WithClaim(context.Background(), tenant.Claim{CrossTenant: true, Requested: "../etc"})
	_, err := tenant.Resolver{Capability: tenant.IdentityScoped, Default: "default"}.FromContext(ctx)
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid: a header is caller input", err)
	}
}

// A Resolver with no default cannot resolve an anonymous request, and saying so
// is better than inventing a tenant: Invariant 1 has no unscoped fallback.
func TestResolverWithNoDefaultRefusesAnonymousRequests(t *testing.T) {
	for _, c := range []tenant.Capability{tenant.SingleTenant, tenant.IdentityScoped} {
		if _, err := (tenant.Resolver{Capability: c}).FromContext(context.Background()); err == nil {
			t.Fatalf("the %s resolver with no default must refuse rather than invent a tenant", c)
		}
	}
}

func TestContextCarriesTheResolvedTenant(t *testing.T) {
	if _, ok := tenant.FromContext(context.Background()); ok {
		t.Fatal("an unscoped context must not report a tenant")
	}
	ctx := tenant.NewContext(context.Background(), "acme")
	got, ok := tenant.FromContext(ctx)
	if !ok || got != "acme" {
		t.Fatalf("FromContext = %q, %v; want acme, true", got, ok)
	}
}

// Require is what every read path below the API layer calls. It is the last
// place Invariant 1 can be enforced structurally, so it must refuse rather
// than return a zero value that keys would then happily build a key from.
func TestRequireRefusesAnUnscopedContext(t *testing.T) {
	if _, err := tenant.Require(context.Background()); err == nil {
		t.Fatal("Invariant 1: there is no unscoped read path")
	}
	got, err := tenant.Require(tenant.NewContext(context.Background(), "acme"))
	if err != nil || got != "acme" {
		t.Fatalf("Require = %q, %v", got, err)
	}
}
