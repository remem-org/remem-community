package http_test

import (
	"net/http"
	"strings"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
)

func policyByName(t *testing.T, body map[string]any, name string) map[string]any {
	t.Helper()
	list, ok := body["policies"].([]any)
	if !ok {
		t.Fatalf("the response carries no policies: %v", body)
	}
	for _, raw := range list {
		p, _ := raw.(map[string]any)
		if p["name"] == name {
			return p
		}
	}
	t.Fatalf("no policy named %q in %v", name, body)
	return nil
}

func TestPoliciesListTheBuiltInsUntilOneIsOverridden(t *testing.T) {
	srv := newServer(t)
	// Provision the tenant, so the route has something to answer about.
	if rr := send(t, srv, keyRoot, "POST", remhttp.APIPrefix+("/tenants"),
		strings.NewReader(`{"id":"acme"}`)); rr.Code != http.StatusCreated {
		t.Fatalf("creating the tenant: %d %s", rr.Code, rr.Body)
	}

	rr := send(t, srv, keyRoot, "GET", remhttp.APIPrefix+("/tenants/acme/policies"), nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("listing policies: %d %s", rr.Code, rr.Body)
	}
	long := policyByName(t, jsonBody(t, rr), "long_term")
	if long["importance_decay"] != 0.995 {
		t.Errorf("long_term decays at %v, want the built-in 0.995", long["importance_decay"])
	}
	if long["overridden"] != false {
		t.Errorf("an untouched policy reports itself overridden")
	}
}

// One tenant's override does not reach another's. It is the whole point of a
// per-tenant policy and the property an isolation bug here would break
// silently: nothing would fail, memories would just decay on the wrong
// schedule.
func TestAPolicyOverrideIsScopedToItsTenant(t *testing.T) {
	srv := newServer(t)
	for _, id := range []string{"acme", "other"} {
		if rr := send(t, srv, keyRoot, "POST", remhttp.APIPrefix+("/tenants"),
			strings.NewReader(`{"id":"`+id+`"}`)); rr.Code != http.StatusCreated {
			t.Fatalf("creating %s: %d %s", id, rr.Code, rr.Body)
		}
	}

	rr := send(t, srv, keyRoot, "PATCH", remhttp.APIPrefix+("/tenants/acme/policies"),
		strings.NewReader(`{"policies":{"long_term":{"importance_decay":0.999}}}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("patching: %d %s", rr.Code, rr.Body)
	}
	got := policyByName(t, jsonBody(t, rr), "long_term")
	if got["importance_decay"] != 0.999 {
		t.Fatalf("after the patch acme's long_term decays at %v", got["importance_decay"])
	}
	if got["overridden"] != true {
		t.Errorf("the patched policy does not report itself overridden")
	}
	// The fields the patch did not name are unchanged, which is what makes a
	// patch over the resolved policy usable: an operator changing one number
	// does not restate the other six.
	if got["health_decay"] != 2.0 {
		t.Errorf("health decay moved to %v", got["health_decay"])
	}

	other := send(t, srv, keyRoot, "GET", remhttp.APIPrefix+("/tenants/other/policies"), nil)
	if p := policyByName(t, jsonBody(t, other), "long_term"); p["importance_decay"] != 0.995 {
		t.Fatalf("acme's override reached tenant other: %v", p["importance_decay"])
	}
}

// Zero means "never" for a duration, because a JSON field that cannot carry
// null needs some way to say it — and a cleanup_after of 0 would otherwise mean
// "delete the moment it is archived", which is the opposite.
func TestAZeroDurationTurnsARuleOff(t *testing.T) {
	srv := newServer(t)
	if rr := send(t, srv, keyRoot, "POST", remhttp.APIPrefix+("/tenants"),
		strings.NewReader(`{"id":"acme"}`)); rr.Code != http.StatusCreated {
		t.Fatalf("creating the tenant: %d %s", rr.Code, rr.Body)
	}

	rr := send(t, srv, keyRoot, "PATCH", remhttp.APIPrefix+("/tenants/acme/policies"),
		strings.NewReader(`{"policies":{"long_term":{"cleanup_after_seconds":0}}}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("patching: %d %s", rr.Code, rr.Body)
	}
	got := policyByName(t, jsonBody(t, rr), "long_term")
	if _, present := got["cleanup_after_seconds"]; present {
		t.Fatalf("cleanup_after is still set: %v", got["cleanup_after_seconds"])
	}
}

func TestAnInvalidPatchIsRefusedAndChangesNothing(t *testing.T) {
	srv := newServer(t)
	if rr := send(t, srv, keyRoot, "POST", remhttp.APIPrefix+("/tenants"),
		strings.NewReader(`{"id":"acme"}`)); rr.Code != http.StatusCreated {
		t.Fatalf("creating the tenant: %d %s", rr.Code, rr.Body)
	}

	for _, tc := range []struct{ name, body string }{
		{"decay above one", `{"policies":{"long_term":{"importance_decay":1.5}}}`},
		{"unknown policy", `{"policies":{"medium_term":{"importance_decay":0.9}}}`},
		{"nothing named", `{"policies":{}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := send(t, srv, keyRoot, "PATCH", remhttp.APIPrefix+("/tenants/acme/policies"), strings.NewReader(tc.body))
			if rr.Code < 400 || rr.Code >= 500 {
				t.Fatalf("got %d, want a client error: %s", rr.Code, rr.Body)
			}
		})
	}

	rr := send(t, srv, keyRoot, "GET", remhttp.APIPrefix+("/tenants/acme/policies"), nil)
	if p := policyByName(t, jsonBody(t, rr), "long_term"); p["importance_decay"] != 0.995 {
		t.Fatalf("a refused patch changed the policy to %v", p["importance_decay"])
	}
}

// A tenant-bound credential cannot read or change retention rules, and the
// refusal names what it asked for rather than a fixed subject — the defect
// Phase 9's verification run found on the job routes.
func TestPolicyRoutesNeedACrossTenantCredential(t *testing.T) {
	srv := newServer(t)
	if rr := send(t, srv, keyRoot, "POST", remhttp.APIPrefix+("/tenants"),
		strings.NewReader(`{"id":"acme"}`)); rr.Code != http.StatusCreated {
		t.Fatalf("creating the tenant: %d %s", rr.Code, rr.Body)
	}

	for _, method := range []string{"GET", "PATCH"} {
		rr := send(t, srv, keyAcme, method, remhttp.APIPrefix+("/tenants/acme/policies"),
			strings.NewReader(`{"policies":{"long_term":{"importance_decay":0.9}}}`))
		if rr.Code != http.StatusForbidden {
			t.Fatalf("%s got %d, want 403: %s", method, rr.Code, rr.Body)
		}
		if !strings.Contains(rr.Body.String(), "retention") {
			t.Errorf("%s: the refusal does not say what was refused: %s", method, rr.Body)
		}
	}
}
