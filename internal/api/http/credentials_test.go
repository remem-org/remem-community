package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/id"
)

// Credentials, the tenant header, and tenant administration. Split from
// http_test.go in Phase 13 to keep that file under the project's size limit;
// the tests did not change.

func TestTenantIsolationAcrossCredentials(t *testing.T) {
	srv := newServer(t)
	mID := createAs(t, srv, keyAcme, "acme's note")

	rr := send(t, srv, keyOther, "GET", remhttp.APIPrefix+"/memories/"+mID, nil)
	if rr.Code != 404 {
		t.Fatalf("another tenant's memory must be 404, not %d — 403 would confirm it exists", rr.Code)
	}
	// And the owner can still read it, so the 404 is isolation and not a bug.
	if rr := send(t, srv, keyAcme, "GET", remhttp.APIPrefix+"/memories/"+mID, nil); rr.Code != 200 {
		t.Fatalf("the owning tenant got %d", rr.Code)
	}
}

func TestUnauthenticatedRequestsAreRejected(t *testing.T) {
	rr := doAs(t, "", "GET", remhttp.APIPrefix+"/memories/"+id.New().String(), nil)
	if rr.Code != 401 {
		t.Fatalf("got %d, want 401", rr.Code)
	}
	if got := rr.Header().Get("WWW-Authenticate"); got == "" {
		t.Fatal("a 401 without WWW-Authenticate leaves a client unable to tell " +
			"'you sent no credential' from 'your credential was rejected'")
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content type %q", ct)
	}
}

func TestAnUnknownCredentialIsRejected(t *testing.T) {
	rr := doAs(t, "guess", "GET", remhttp.APIPrefix+"/memories/"+id.New().String(), nil)
	if rr.Code != 401 {
		t.Fatalf("got %d, want 401", rr.Code)
	}
}

// A bound credential cannot be talked out of its tenant by a header. This is
// what makes an issued API key an isolation boundary rather than a suggestion.
func TestABoundCredentialIgnoresTheTenantHeader(t *testing.T) {
	srv := newServer(t)
	mID := createAs(t, srv, keyAcme, "acme's note")

	r := httptest.NewRequest("GET", remhttp.APIPrefix+"/memories/"+mID, nil)
	r.Header.Set("Authorization", "Bearer "+keyOther)
	r.Header.Set(remhttp.TenantHeader, "acme")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)

	if rr.Code != 404 {
		t.Fatalf("a header moved a bound credential into another tenant: %d", rr.Code)
	}
}

// An operator credential may name a tenant, which is what makes one process
// serve many.
func TestACrossTenantCredentialHonoursTheTenantHeader(t *testing.T) {
	srv := newServer(t)
	mID := createAs(t, srv, keyAcme, "acme's note")

	r := httptest.NewRequest("GET", remhttp.APIPrefix+"/memories/"+mID, nil)
	r.Header.Set("Authorization", "Bearer "+keyRoot)
	r.Header.Set(remhttp.TenantHeader, "acme")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)

	if rr.Code != 200 {
		t.Fatalf("an operator credential naming acme got %d", rr.Code)
	}
}

func TestTenantAdministrationRequiresACrossTenantCredential(t *testing.T) {
	rr := doAs(t, keyAcme, "GET", remhttp.APIPrefix+"/tenants", nil)
	if rr.Code != 403 {
		t.Fatalf("got %d, want 403", rr.Code)
	}
	rr = doAs(t, keyRoot, "GET", remhttp.APIPrefix+"/tenants", nil)
	if rr.Code != 200 {
		t.Fatalf("an operator credential got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestCreatingATenant(t *testing.T) {
	srv := newServer(t)
	rr := send(t, srv, keyRoot, "POST", remhttp.APIPrefix+"/tenants",
		strings.NewReader(`{"id":"newco","display_name":"New Co"}`))
	if rr.Code != http.StatusCreated {
		t.Fatalf("got %d: %s", rr.Code, rr.Body.String())
	}
	body := jsonBody(t, rr)
	if body["id"] != "newco" || body["schema_version"].(float64) != 1 {
		t.Fatalf("tenant = %v", body)
	}
	// Creating it twice is a conflict, not a silent overwrite.
	rr = send(t, srv, keyRoot, "POST", remhttp.APIPrefix+"/tenants",
		strings.NewReader(`{"id":"newco"}`))
	if rr.Code != http.StatusConflict {
		t.Fatalf("re-creating a tenant returned %d, want 409", rr.Code)
	}
}

func TestAMalformedTenantIDIsRefused(t *testing.T) {
	rr := doAs(t, keyRoot, "POST", remhttp.APIPrefix+"/tenants", strings.NewReader(`{"id":"../etc"}`))
	if rr.Code != 422 {
		t.Fatalf("got %d, want 422", rr.Code)
	}
}
