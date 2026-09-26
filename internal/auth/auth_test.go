package auth_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/tenant"
)

func keyring(t *testing.T, creds ...auth.Credential) *auth.Keyring {
	t.Helper()
	k, err := auth.NewKeyring(creds)
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}
	return k
}

func TestAVerifiedCredentialCarriesItsTenant(t *testing.T) {
	k := keyring(t,
		auth.Credential{ID: "acme-agent", Secret: "key-acme", Tenant: "acme"},
		auth.Credential{ID: "other-agent", Secret: "key-other", Tenant: "other"},
	)

	p, err := k.Verify(context.Background(), "key-acme")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if p.ID != "acme-agent" || p.Tenant != "acme" {
		t.Fatalf("principal = %+v", p)
	}
	if p.CrossTenant {
		t.Fatal("a tenant-bound credential must not be authorised across tenants")
	}
}

func TestAnUnknownCredentialIsUnauthorized(t *testing.T) {
	k := keyring(t, auth.Credential{ID: "a", Secret: "key-acme", Tenant: "acme"})
	if _, err := k.Verify(context.Background(), "guess"); !errs.Is(err, errs.Unauthorized) {
		t.Fatalf("got %v, want Unauthorized", err)
	}
	if _, err := k.Verify(context.Background(), ""); !errs.Is(err, errs.Unauthorized) {
		t.Fatalf("empty credential = %v, want Unauthorized", err)
	}
}

// An empty keyring is a development server with no api_key configured. It
// admits an anonymous principal rather than refusing every request, because
// `remem start --data-dir ./data` has to be a complete experience (Invariant
// 10). Production configuration is what forbids this, not this package.
func TestAnEmptyKeyringAdmitsAnAnonymousPrincipal(t *testing.T) {
	k := keyring(t)
	if !k.Anonymous() {
		t.Fatal("an empty keyring is the unauthenticated development case")
	}
	p, err := k.Verify(context.Background(), "")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if p.Tenant != "" || !p.CrossTenant {
		t.Fatalf("anonymous principal = %+v; it is unbound and may name a tenant", p)
	}
}

// An unbound credential is the operator's key: it names its tenant per request.
func TestAnUnboundCredentialIsCrossTenant(t *testing.T) {
	k := keyring(t, auth.Credential{ID: "root", Secret: "root-key", CrossTenant: true})
	p, err := k.Verify(context.Background(), "root-key")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !p.CrossTenant || p.Tenant != "" {
		t.Fatalf("principal = %+v", p)
	}
}

func TestNewKeyringRefusesAmbiguousOrMalformedCredentials(t *testing.T) {
	for name, creds := range map[string][]auth.Credential{
		"duplicate secret": {
			{ID: "a", Secret: "same", Tenant: "acme"},
			{ID: "b", Secret: "same", Tenant: "other"},
		},
		"empty secret":     {{ID: "a", Secret: "", Tenant: "acme"}},
		"malformed tenant": {{ID: "a", Secret: "s", Tenant: "../etc"}},
		"bound and cross":  {{ID: "a", Secret: "s", Tenant: "acme", CrossTenant: true}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := auth.NewKeyring(creds); err == nil {
				t.Fatal("NewKeyring accepted a credential set it cannot serve unambiguously")
			}
		})
	}
}

// Verification must not leak which credentials exist through timing. This is
// not a proof of constant time — it asserts that a wrong secret of the right
// length is refused exactly like a wrong secret of any other length.
func TestVerifyDoesNotDistinguishWrongSecretsByShape(t *testing.T) {
	k := keyring(t, auth.Credential{ID: "a", Secret: "key-acme", Tenant: "acme"})
	for _, guess := range []string{"key-acmf", "k", "key-acme-and-more"} {
		if _, err := k.Verify(context.Background(), guess); !errs.Is(err, errs.Unauthorized) {
			t.Fatalf("Verify(%q) = %v, want Unauthorized", guess, err)
		}
	}
}

// The principal is what the tenant resolver reads. The conversion lives here
// so internal/tenant never has to know that credentials exist.
func TestPrincipalBecomesATenantClaim(t *testing.T) {
	c := auth.Principal{ID: "root", CrossTenant: true}.Claim("acme")
	if c.Bound != "" || !c.CrossTenant || c.Requested != "acme" {
		t.Fatalf("claim = %+v", c)
	}

	bound := auth.Principal{ID: "a", Tenant: "acme"}.Claim("evil")
	if bound.Bound != tenant.ID("acme") || bound.CrossTenant {
		t.Fatalf("claim = %+v", bound)
	}
}

func TestContextCarriesThePrincipal(t *testing.T) {
	if _, ok := auth.FromContext(context.Background()); ok {
		t.Fatal("an unauthenticated context must not report a principal")
	}
	want := auth.Principal{ID: "a", Tenant: "acme", Authorized: []tenant.ID{"acme"}}
	got, ok := auth.FromContext(auth.NewContext(context.Background(), want))
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("FromContext = %+v, %v", got, ok)
	}
}

// A cross-tenant operation must act on a scope configuration stated, not on
// one inferred from a credential having no binding. The authorised set is how a
// deployment states it, and the keyring is where a set that could never be
// exercised is refused.
func TestAnAuthorizedSetTravelsIntoTheClaim(t *testing.T) {
	k, err := auth.NewKeyring([]auth.Credential{{
		ID: "two-tenants", Secret: "a-secret-long-enough", CrossTenant: true,
		Authorized: []tenant.ID{"acme", "globex"},
	}})
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}
	p, err := k.Verify(context.Background(), "a-secret-long-enough")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	claim := p.Claim("globex")
	if !reflect.DeepEqual(claim.Authorized, []tenant.ID{"acme", "globex"}) {
		t.Fatalf("the claim carries %v", claim.Authorized)
	}
	if !claim.CrossTenant || claim.Bound != "" {
		t.Fatalf("claim = %+v", claim)
	}
}

func TestContradictoryCredentialsAreRefusedByName(t *testing.T) {
	for _, tc := range []struct {
		name string
		cred auth.Credential
		want string
	}{
		{
			"bound and authorised for a set",
			auth.Credential{ID: "x", Secret: "s", Tenant: "acme", Authorized: []tenant.ID{"globex"}},
			"a binding is already a set of one",
		},
		{
			"an authorised set it could never name",
			auth.Credential{ID: "x", Secret: "s", Authorized: []tenant.ID{"acme", "globex"}},
			"could never name one of them",
		},
		{
			"an authorised set holding something that is not a tenant id",
			auth.Credential{ID: "x", Secret: "s", CrossTenant: true, Authorized: []tenant.ID{"../etc"}},
			"is authorised for",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := auth.NewKeyring([]auth.Credential{tc.cred})
			if err == nil {
				t.Fatal("the credential was accepted")
			}
			if !errs.Is(err, errs.Invalid) {
				t.Fatalf("got %s, want Invalid", errs.KindOf(err))
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the refusal does not say why:\n%v", err)
			}
		})
	}
}
