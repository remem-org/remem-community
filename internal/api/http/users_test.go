package http_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/users"
)

// keyUser stands for a user token. The fixture's verifier turns it into a user
// principal of tenant acme, so these tests exercise the routes' rules without
// depending on how a token is stored.
const keyUser = "user-token-for-tests"

// keyNarrow stands for a cross-tenant credential configuration narrowed to one
// tenant ("<secret>@acme,..."): not bound, and authorised for acme alone.
const keyNarrow = "narrowed-operator-key-for-tests"

type userVerifier struct {
	next auth.Verifier
	user id.ID
}

func (v userVerifier) Verify(ctx context.Context, cred string) (auth.Principal, error) {
	if cred == keyUser {
		return auth.Principal{ID: "user:test", Tenant: "acme", Kind: auth.KindUser, User: v.user}, nil
	}
	if cred == keyNarrow {
		return auth.Principal{ID: "narrow", CrossTenant: true, Authorized: []tenant.ID{"acme"}}, nil
	}
	return v.next.Verify(ctx, cred)
}

// newUsersServer is the HTTP fixture with a user store, under capability.
func newUsersServer(t *testing.T, capability tenant.Capability) (http.Handler, *users.Service) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	store := users.New(kv, clock.NewFake(clock.FakeStart))
	srv := newServerWith(t, func(d *remhttp.Deps) {
		d.Users = store
		d.Auth = userVerifier{next: d.Auth, user: id.New()}
		d.Capability = capability
		d.Resolver.Capability = capability
		if capability == tenant.SingleTenant {
			d.Resolver.Default = "acme"
			d.Tenants = tenant.Confine(d.Tenants, "acme")
		}
	})
	// The tenants the routes name must exist. A request by a tenant-bound key
	// provisions its tenant, as the fixture is configured to.
	send(t, srv, keyAcme, "GET", remhttp.APIPrefix+"/memories", nil)
	if capability == tenant.IdentityScoped {
		send(t, srv, keyOther, "GET", remhttp.APIPrefix+"/memories", nil)
	}
	return srv, store
}

func usersPath(t tenant.ID, rest string) string {
	return remhttp.APIPrefix + "/tenants/" + string(t) + "/users" + rest
}

func TestTheOperatorAdministersUsersAndTokens(t *testing.T) {
	srv, _ := newUsersServer(t, tenant.IdentityScoped)

	rr := send(t, srv, keyRoot, "POST", usersPath("acme", ""), strings.NewReader(`{"name":"Planner"}`))
	if rr.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rr.Code, rr.Body)
	}
	u := jsonBody(t, rr)
	uid := u["id"].(string)
	if u["name"] != "Planner" || u["state"] != "active" {
		t.Fatalf("created %v", u)
	}

	rr = send(t, srv, keyRoot, "POST", usersPath("acme", "/"+uid+"/tokens"), strings.NewReader(`{}`))
	if rr.Code != http.StatusCreated {
		t.Fatalf("issue: %d %s", rr.Code, rr.Body)
	}
	issued := jsonBody(t, rr)
	secret, _ := issued["secret"].(string)
	tok := issued["token"].(map[string]any)
	if !strings.HasPrefix(secret, "rmm_") || tok["state"] != "active" || tok["user_id"] != uid {
		t.Fatalf("issued %v", issued)
	}

	rr = send(t, srv, keyRoot, "GET", usersPath("acme", "/"+uid+"/tokens"), nil)
	if rr.Code != http.StatusOK || strings.Contains(rr.Body.String(), secret[4:]) ||
		strings.Contains(rr.Body.String(), "secret") || strings.Contains(rr.Body.String(), "digest") {
		t.Fatalf("listing tokens: %d %s", rr.Code, rr.Body)
	}

	rr = send(t, srv, keyRoot, "DELETE", usersPath("acme", "/"+uid+"/tokens/"+tok["id"].(string)), nil)
	if rr.Code != http.StatusOK || jsonBody(t, rr)["state"] != "revoked" {
		t.Fatalf("revoke: %d %s", rr.Code, rr.Body)
	}

	rr = send(t, srv, keyRoot, "PATCH", usersPath("acme", "/"+uid), strings.NewReader(`{"name":"planner-2","state":"disabled"}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("patch: %d %s", rr.Code, rr.Body)
	}
	if p := jsonBody(t, rr); p["name"] != "planner-2" || p["state"] != "disabled" || p["id"] != uid {
		t.Fatalf("patched %v", p)
	}

	rr = send(t, srv, keyRoot, "GET", usersPath("acme", ""), nil)
	if rr.Code != http.StatusOK || len(jsonBody(t, rr)["users"].([]any)) != 1 {
		t.Fatalf("list: %d %s", rr.Code, rr.Body)
	}
	rr = send(t, srv, keyRoot, "GET", usersPath("other", ""), nil)
	if rr.Code != http.StatusOK || len(jsonBody(t, rr)["users"].([]any)) != 0 {
		t.Fatalf("another tenant lists %d %s", rr.Code, rr.Body)
	}
	if rr := send(t, srv, keyRoot, "GET", usersPath("other", "/"+uid), nil); rr.Code != http.StatusNotFound {
		t.Fatalf("acme's user read under other: %d, want 404", rr.Code)
	}
}

// A user token administers nothing, its own user and tokens included, and is
// told which subject it was refused for. So is a tenant-bound key.
func TestOnlyTheOperatorAdministersUsers(t *testing.T) {
	srv, store := newUsersServer(t, tenant.IdentityScoped)
	u, err := store.Create(tenant.NewContext(context.Background(), "acme"), "planner")
	if err != nil {
		t.Fatal(err)
	}
	uid := u.ID.String()
	routes := []struct{ method, path, body string }{
		{"GET", usersPath("acme", ""), ""},
		{"POST", usersPath("acme", ""), `{"name":"x"}`},
		{"GET", usersPath("acme", "/"+uid), ""},
		{"PATCH", usersPath("acme", "/"+uid), `{"state":"disabled"}`},
		{"GET", usersPath("acme", "/"+uid+"/tokens"), ""},
		{"POST", usersPath("acme", "/"+uid+"/tokens"), `{}`},
		{"DELETE", usersPath("acme", "/"+uid+"/tokens/"+id.New().String()), ""},
	}
	for _, key := range []string{keyUser, keyAcme} {
		for _, rt := range routes {
			var body *strings.Reader
			if rt.body != "" {
				body = strings.NewReader(rt.body)
			}
			var rr = send(t, srv, key, rt.method, rt.path, nilIfEmpty(body))
			if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "administering users and tokens") {
				t.Errorf("%s %s with %s: %d %s, want 403 naming user administration", rt.method, rt.path, key, rr.Code, rr.Body)
			}
		}
	}
}

func TestAUserIsNeverDeletedAndTheRefusalSaysDisable(t *testing.T) {
	srv, store := newUsersServer(t, tenant.IdentityScoped)
	u, _ := store.Create(tenant.NewContext(context.Background(), "acme"), "planner")
	rr := send(t, srv, keyRoot, "DELETE", usersPath("acme", "/"+u.ID.String()), nil)
	if rr.Code == http.StatusOK || rr.Code == http.StatusNoContent || !strings.Contains(rr.Body.String(), "disable") {
		t.Fatalf("delete: %d %s, want a refusal naming disable", rr.Code, rr.Body)
	}
	if _, err := store.Get(tenant.NewContext(context.Background(), "acme"), u.ID); err != nil {
		t.Fatalf("the user is gone: %v", err)
	}
}

func TestADuplicateNameIsAConflict(t *testing.T) {
	srv, _ := newUsersServer(t, tenant.IdentityScoped)
	send(t, srv, keyRoot, "POST", usersPath("acme", ""), strings.NewReader(`{"name":"planner"}`))
	rr := send(t, srv, keyRoot, "POST", usersPath("acme", ""), strings.NewReader(`{"name":"PLANNER"}`))
	if rr.Code != http.StatusConflict {
		t.Fatalf("a duplicate name: %d %s, want 409", rr.Code, rr.Body)
	}
}

// A single-tenant build serves users in its implicit tenant only, and refuses
// another tenant in the path before reading or writing anything.
func TestASingleTenantBuildServesUsersOfItsTenantOnly(t *testing.T) {
	srv, store := newUsersServer(t, tenant.SingleTenant)
	if rr := send(t, srv, keyRoot, "POST", usersPath("acme", ""), strings.NewReader(`{"name":"planner"}`)); rr.Code != http.StatusCreated {
		t.Fatalf("the implicit tenant: %d %s", rr.Code, rr.Body)
	}
	rr := send(t, srv, keyRoot, "POST", usersPath("beta", ""), strings.NewReader(`{"name":"planner"}`))
	if rr.Code != http.StatusNotFound && rr.Code != http.StatusForbidden {
		t.Fatalf("another tenant: %d %s, want it refused", rr.Code, rr.Body)
	}
	if list, _ := store.List(tenant.NewContext(context.Background(), "beta")); len(list) != 0 {
		t.Fatalf("a user was written in beta: %v", list)
	}
}

func TestATokenExpiryIsReadAsRFC3339(t *testing.T) {
	srv, store := newUsersServer(t, tenant.IdentityScoped)
	u, _ := store.Create(tenant.NewContext(context.Background(), "acme"), "planner")
	rr := send(t, srv, keyRoot, "POST", usersPath("acme", "/"+u.ID.String()+"/tokens"),
		strings.NewReader(`{"expires_at":"2099-01-01T00:00:00Z"}`))
	if rr.Code != http.StatusCreated {
		t.Fatalf("issue with expiry: %d %s", rr.Code, rr.Body)
	}
	if got := jsonBody(t, rr)["token"].(map[string]any)["expires_at"]; got != "2099-01-01T00:00:00Z" {
		t.Fatalf("expires_at %v", got)
	}
	rr = send(t, srv, keyRoot, "POST", usersPath("acme", "/"+u.ID.String()+"/tokens"),
		strings.NewReader(`{"expires_at":"2000-01-01T00:00:00Z"}`))
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("a past expiry: %d %s, want 422", rr.Code, rr.Body)
	}
}

// sendNarrow sends as keyNarrow, which the resolver requires to name its tenant
// with the header on every request: header acme, whatever the path says.
func sendNarrow(t *testing.T, srv http.Handler, method, path string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, body)
	r.Header.Set("Authorization", "Bearer "+keyNarrow)
	r.Header.Set("X-Remem-Tenant", "acme")
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	return rr
}

func nilIfEmpty(r *strings.Reader) interface{ Read([]byte) (int, error) } {
	if r == nil {
		return nil
	}
	return r
}

// A cross-tenant credential narrowed to a set of tenants administers users in
// that set and nowhere else. Without this, a key authorised for acme could
// create a user in another tenant and mint itself a token for it: full access to
// a tenant it was never authorised for.
func TestANarrowedCredentialAdministersOnlyItsTenants(t *testing.T) {
	srv, store := newUsersServer(t, tenant.IdentityScoped)
	if rr := sendNarrow(t, srv, "POST", usersPath("acme", ""), strings.NewReader(`{"name":"planner"}`)); rr.Code != http.StatusCreated {
		t.Fatalf("its own tenant: %d %s", rr.Code, rr.Body)
	}
	for _, rt := range []struct{ method, path, body string }{
		{"GET", usersPath("other", ""), ""},
		{"POST", usersPath("other", ""), `{"name":"intruder"}`},
	} {
		var body io.Reader
		if rt.body != "" {
			body = strings.NewReader(rt.body)
		}
		rr := sendNarrow(t, srv, rt.method, rt.path, body)
		if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "acme") {
			t.Fatalf("%s %s: %d %s, want 403 naming the authorised set", rt.method, rt.path, rr.Code, rr.Body)
		}
	}
	if list, _ := store.List(tenant.NewContext(context.Background(), "other")); len(list) != 0 {
		t.Fatalf("a user was written in a tenant the credential is not authorised for: %v", list)
	}
}

// The same hole existed on the retention-policy routes this one copied: a key
// narrowed to acme could shorten another tenant's cleanup until its memories
// were deleted.
func TestANarrowedCredentialChangesOnlyItsTenantsPolicies(t *testing.T) {
	srv, _ := newUsersServer(t, tenant.IdentityScoped)
	if rr := sendNarrow(t, srv, "GET", remhttp.APIPrefix+"/tenants/acme/policies", nil); rr.Code != http.StatusOK {
		t.Fatalf("its own tenant's policies: %d %s", rr.Code, rr.Body)
	}
	rr := sendNarrow(t, srv, "PATCH", remhttp.APIPrefix+"/tenants/other/policies",
		strings.NewReader(`{"policies":{"long_term":{"cleanup_after_seconds":1}}}`))
	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "acme") {
		t.Fatalf("another tenant's policies: %d %s, want 403 naming the authorised set", rr.Code, rr.Body)
	}
}
