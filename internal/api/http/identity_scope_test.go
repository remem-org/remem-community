package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/tenant"
)

// keyScoped is authorised for an explicit set of two tenants out of the three
// the fixture's keyring knows about.
const keyScoped = "key-scoped"

// newIdentityScopedServer is the surface a deployment that resolves tenants
// from an authenticated identity serves, with one credential narrowed to an
// explicit set. The narrowing is the thing under test: an unbound credential is
// not, on its own, authorisation for every tenant that happens to exist.
func newIdentityScopedServer(t *testing.T) http.Handler {
	t.Helper()
	return newServerWith(t, func(d *remhttp.Deps) {
		d.Capability = tenant.IdentityScoped
		keyring, err := auth.NewKeyring([]auth.Credential{
			{ID: "acme-agent", Secret: keyAcme, Tenant: "acme"},
			{ID: "other-agent", Secret: keyOther, Tenant: "other"},
			{ID: "operator", Secret: keyRoot, CrossTenant: true},
			{ID: "scoped", Secret: keyScoped, CrossTenant: true,
				Authorized: []tenant.ID{"acme", "other"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		d.Auth = keyring
	})
}

// sendWithTenant issues a request naming a tenant, which is the only way an
// identity-scoped deployment's cross-tenant credential reaches one.
func sendWithTenant(t *testing.T, srv http.Handler, key, tid, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	}
	r.Header.Set("Authorization", "Bearer "+key)
	if tid != "" {
		r.Header.Set(remhttp.TenantHeader, tid)
	}
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	return rr
}

// The resolved scope reaches every operation surface, not merely the ones that
// happen to be read paths.
func TestAnAuthorizedScopeReachesEveryOperationSurface(t *testing.T) {
	srv := newIdentityScopedServer(t)

	rr := sendWithTenant(t, srv, keyScoped, "acme", "POST", "/api/v1/memories",
		`{"content":"a memory the scoped credential wrote"}`)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create in an authorised tenant = %d: %s", rr.Code, rr.Body.String())
	}
	id, _ := jsonBody(t, rr)["id"].(string)
	if id == "" {
		t.Fatalf("the create returned no id: %s", rr.Body.String())
	}

	for _, tc := range []struct {
		name, method, path, body string
		want                     int
	}{
		{"read", "GET", "/api/v1/memories/" + id, "", http.StatusOK},
		{"list", "GET", "/api/v1/memories?limit=5", "", http.StatusOK},
		{"search", "POST", "/api/v1/memories/search", `{"query":"memory","search_type":"keyword"}`, http.StatusOK},
		{"connections", "GET", "/api/v1/memories/" + id + "/connections", "", http.StatusOK},
		{"update", "PATCH", "/api/v1/memories/" + id, `{"importance":0.5}`, http.StatusOK},
	} {
		// History is absent from this list because this fixture records none;
		// the single-tenant fixture covers that surface instead.
		t.Run(tc.name, func(t *testing.T) {
			rr := sendWithTenant(t, srv, keyScoped, "acme", tc.method, tc.path, tc.body)
			if rr.Code != tc.want {
				t.Fatalf("%s %s = %d: %s", tc.method, tc.path, rr.Code, rr.Body.String())
			}
		})
	}

	// The memory the scoped credential wrote in acme belongs to acme, which the
	// tenant-bound credential for the other tenant confirms by not finding it.
	if rr := send(t, srv, keyOther, "GET", "/api/v1/memories/"+id, nil); rr.Code != http.StatusNotFound {
		t.Fatalf("another tenant read the memory: %d %s", rr.Code, rr.Body.String())
	}
}

// A tenant outside the authorised set is refused before anything is read or
// changed, on read and write paths alike.
func TestATenantOutsideTheAuthorizedScopeIsRefused(t *testing.T) {
	srv := newIdentityScopedServer(t)
	for _, tc := range []struct{ method, path, body string }{
		{"GET", "/api/v1/memories?limit=5", ""},
		{"POST", "/api/v1/memories", `{"content":"x"}`},
		{"POST", "/api/v1/memories/search", `{"query":"x","search_type":"keyword"}`},
		{"GET", "/api/v1/admin/jobs", ""},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rr := sendWithTenant(t, srv, keyScoped, "beta", tc.method, tc.path, tc.body)
			if rr.Code != http.StatusForbidden {
				t.Fatalf("an unauthorised tenant was answered %d: %s", rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), "acme, other") {
				t.Fatalf("the refusal does not name the authorised set: %s", rr.Body.String())
			}
		})
	}
}

// With no tenant named, a credential authorised for several is told to name
// one. Falling back to the deployment default would serve a tenant it may not
// be authorised for at all.
func TestAScopedCredentialMustNameOneOfItsTenants(t *testing.T) {
	rr := sendWithTenant(t, newIdentityScopedServer(t), keyScoped, "", "GET", "/api/v1/memories?limit=5", "")
	if rr.Code != http.StatusUnprocessableEntity && rr.Code != http.StatusBadRequest {
		t.Fatalf("a scoped credential with no tenant named was answered %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "acme, other") {
		t.Fatalf("the refusal does not name the authorised set: %s", rr.Body.String())
	}
}

// A bound credential stays bound: a header cannot talk it into another tenant,
// which is the whole point of issuing one.
func TestABoundCredentialIgnoresTheHeaderUnderIdentityTenancy(t *testing.T) {
	srv := newIdentityScopedServer(t)
	id := createAs(t, srv, keyAcme, "a memory in acme")
	// The acme credential presenting another tenant's name still reads its own.
	if rr := sendWithTenant(t, srv, keyAcme, "other", "GET", "/api/v1/memories/"+id, ""); rr.Code != http.StatusOK {
		t.Fatalf("a bound credential was moved by a header: %d %s", rr.Code, rr.Body.String())
	}
}

// A cross-tenant listing shows the credential's explicit scope rather than
// every tenant that exists. An unbound credential is not, on its own,
// authorisation for a tenant created tomorrow.
func TestATenantListingShowsOnlyTheAuthorizedScope(t *testing.T) {
	srv := newIdentityScopedServer(t)
	// Three tenants exist, because auto-provisioning creates one per request.
	createAs(t, srv, keyAcme, "a memory in acme")
	createAs(t, srv, keyOther, "a memory in other")
	if rr := sendWithTenant(t, srv, keyRoot, "beta", "POST", "/api/v1/memories",
		`{"content":"a memory in beta"}`); rr.Code != http.StatusCreated {
		t.Fatalf("seeding beta = %d: %s", rr.Code, rr.Body.String())
	}

	rr := sendWithTenant(t, srv, keyScoped, "acme", "GET", "/api/v1/tenants", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("list tenants = %d: %s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, want := range []string{`"acme"`, `"other"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("the listing is missing %s: %s", want, body)
		}
	}
	if strings.Contains(body, `"beta"`) {
		t.Fatalf("the listing showed a tenant outside the credential's scope: %s", body)
	}

	// The operator credential, which configuration deliberately did not narrow,
	// still sees all three.
	rr = sendWithTenant(t, srv, keyRoot, "acme", "GET", "/api/v1/tenants", "")
	if !strings.Contains(rr.Body.String(), `"beta"`) {
		t.Fatalf("the operator credential lost a tenant: %s", rr.Body.String())
	}
}

// Provisioning is the one cross-tenant operation that widens the set, so a
// narrowed credential that could do it would have narrowed nothing.
func TestAScopedCredentialCannotProvisionOutsideItsScope(t *testing.T) {
	srv := newIdentityScopedServer(t)
	rr := sendWithTenant(t, srv, keyScoped, "acme", "POST", "/api/v1/tenants", `{"id":"gamma"}`)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("a scoped credential provisioned a tenant outside its scope: %d %s",
			rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "gamma") {
		t.Fatalf("the refusal does not name the tenant asked for: %s", rr.Body.String())
	}
}

// A scrape names no tenant, so its scope cannot come from the request. It comes
// from the credential's explicit authorisation, and a credential narrowed to a
// set sees that set's series and no others.
func TestAScrapeIsNarrowedToTheCredentialsScope(t *testing.T) {
	srv := newIdentityScopedServer(t)
	createAs(t, srv, keyAcme, "a memory in acme")
	if rr := sendWithTenant(t, srv, keyRoot, "beta", "POST", "/api/v1/memories",
		`{"content":"a memory in beta"}`); rr.Code != http.StatusCreated {
		t.Fatalf("seeding beta = %d: %s", rr.Code, rr.Body.String())
	}

	scoped := sendWithTenant(t, srv, keyScoped, "acme", "GET", "/metrics", "")
	if scoped.Code != http.StatusOK {
		t.Fatalf("a scoped scrape = %d: %s", scoped.Code, scoped.Body.String())
	}
	if !strings.Contains(scoped.Body.String(), `tenant="acme"`) {
		t.Fatalf("the scoped scrape lost its own tenant's series:\n%s", scoped.Body.String())
	}
	if strings.Contains(scoped.Body.String(), `tenant="beta"`) {
		t.Fatalf("the scoped scrape carries a tenant outside its scope:\n%s", scoped.Body.String())
	}
	// A series with no tenant label belongs to the deployment and is still
	// there: filtering by tenant must not blind an operator to process health.
	if !strings.Contains(scoped.Body.String(), "remem_") {
		t.Fatalf("the scoped scrape exported nothing at all:\n%s", scoped.Body.String())
	}

	// The unnarrowed operator credential sees both.
	all := sendWithTenant(t, srv, keyRoot, "acme", "GET", "/metrics", "")
	if !strings.Contains(all.Body.String(), `tenant="beta"`) {
		t.Fatalf("the operator scrape lost a tenant:\n%s", all.Body.String())
	}
}
