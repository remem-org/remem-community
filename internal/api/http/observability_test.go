package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/id"
)

// Metrics and request ids through the router. Split from http_test.go in Phase
// 13 to keep that file under the project's size limit; the tests did not change.

func TestMetricsAreServed(t *testing.T) {
	srv := newServer(t)
	// A counter with no observations has no series, so a scrape of a server
	// that has served nothing else would legitimately not mention it.
	createAs(t, srv, keyAcme, "observed")

	rr := send(t, srv, keyRoot, "GET", "/metrics", nil)
	if rr.Code != 200 {
		t.Fatalf("got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "remem_api_requests_total") {
		t.Error("the API metrics are not exported")
	}
	// The route label must be the registered pattern, never the request path:
	// a path holds record ids, and one time series per memory would take a
	// metrics store down inside a day.
	if strings.Contains(body, "/api/v1/memories/") {
		t.Error("a metric label carries a request path, which means one series per memory")
	}
}

// Every response carries a request id, and a client-supplied one is echoed so
// a trace that began upstream stays one trace.
func TestRequestIDsAreAssignedAndEchoed(t *testing.T) {
	srv := newServer(t)

	rr := send(t, srv, keyAcme, "GET", remhttp.APIPrefix+"/health", nil)
	if rr.Header().Get("X-Request-Id") == "" {
		t.Fatal("no request id was assigned")
	}

	r := httptest.NewRequest("GET", remhttp.APIPrefix+"/health", nil)
	r.Header.Set("X-Request-Id", "upstream-trace-1")
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	if got := rr.Header().Get("X-Request-Id"); got != "upstream-trace-1" {
		t.Fatalf("request id = %q; a client-supplied one must be kept", got)
	}
}

// A problem document carries the request id, which is what makes a user's
// report actionable: it is the one string that finds the log line.
func TestProblemDocumentsCarryTheRequestID(t *testing.T) {
	rr := do(t, "GET", remhttp.APIPrefix+"/memories/"+id.New().String(), nil)
	p := jsonBody(t, rr)
	if p["request_id"] == "" || p["request_id"] == nil {
		t.Fatal("the problem document carries no request id")
	}
	if p["request_id"] != rr.Header().Get("X-Request-Id") {
		t.Fatal("the document's request id differs from the header's")
	}
}

// Invariant 1 reaches the metrics, not only the keyspace: a metric counting
// work done on a tenant's behalf carries that tenant.
//
// This is easy to break and was: the authentication middleware resolves the
// tenant into a derived context, so an observe middleware wrapped around it
// sees the original and labels everything with an empty tenant.
func TestMetricsCarryTheTenant(t *testing.T) {
	srv := newServer(t)
	createAs(t, srv, keyAcme, "acme's note")

	rr := send(t, srv, keyRoot, "GET", "/metrics", nil)
	if !strings.Contains(rr.Body.String(), `tenant="acme"`) {
		t.Fatalf("no metric carries the tenant label:\n%s",
			firstLines(rr.Body.String(), "remem_api_requests_total"))
	}
}

// TestMetricsRequireACrossTenantCredential: a scrape lists every tenant id the
// server has served in its labels, which is cross-tenant information. So it
// needs a credential, and one that is not bound to a tenant — the rule every
// administration route already follows. Appendix B said "authenticated"; the
// route was not.
func TestMetricsRequireACrossTenantCredential(t *testing.T) {
	srv := newServer(t)
	createAs(t, srv, keyAcme, "acme's note")
	createAs(t, srv, keyOther, "other's note")

	if rr := send(t, srv, "", "GET", "/metrics", nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("an anonymous scrape: %d, want 401", rr.Code)
	}

	rr := send(t, srv, keyAcme, "GET", "/metrics", nil)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("a scrape with acme's bound key: %d, want 403", rr.Code)
	}
	if strings.Contains(rr.Body.String(), `tenant="other"`) {
		t.Fatal("the refusal leaked another tenant's label")
	}
	if !strings.Contains(rr.Body.String(), "metrics") {
		t.Fatalf("the refusal does not say it was about the metrics: %s", rr.Body.String())
	}

	rr = send(t, srv, keyRoot, "GET", "/metrics", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "remem_api_requests_total") {
		t.Fatalf("a scrape with the operator key: %d", rr.Code)
	}
}

func firstLines(body, prefix string) string {
	var out []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, prefix) {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}
