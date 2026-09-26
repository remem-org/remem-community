package http_test

import (
	"net/http"
	"strings"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
)

// The metrics endpoint is served at the path an operator configured, and only
// there, and keeps its access rule at that path: a credential bound to one
// tenant is refused, in words that say it asked for the metrics.
//
// Found in Phase 13's security review. metrics.path was validated and never
// read, so the endpoint stayed at /metrics whatever the configuration said.
func TestMetricsAreServedAtTheConfiguredPath(t *testing.T) {
	srv := newServerWith(t, func(d *remhttp.Deps) { d.MetricsPath = "/ops/scrape" })

	if rr := send(t, srv, keyRoot, "GET", "/ops/scrape", nil); rr.Code != http.StatusOK {
		t.Fatalf("GET /ops/scrape returned %d: %s", rr.Code, rr.Body.String())
	}
	if rr := send(t, srv, keyRoot, "GET", "/metrics", nil); rr.Code != http.StatusNotFound {
		t.Fatalf("GET /metrics returned %d with the endpoint moved, want 404", rr.Code)
	}
	rr := send(t, srv, keyAcme, "GET", "/ops/scrape", nil)
	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "metrics") {
		t.Fatalf("a tenant-bound key at the moved endpoint got %d: %s", rr.Code, rr.Body.String())
	}
}
