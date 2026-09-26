package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// metrics.enabled and metrics.path reach the running server. Turning the
// endpoint off closes it: an operator who switched it off so that the tenant
// names in its labels are not served at all gets exactly that.
//
// Found in Phase 13's security review: both settings were validated and never
// read, so /metrics was served whatever the configuration said.
func TestTheMetricsSettingsReachTheServer(t *testing.T) {
	const key = "an-operator-key-long-enough-for-this-test"
	get := func(h http.Handler, path string) int {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+key)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, r)
		return rr.Code
	}
	for _, c := range []struct {
		name      string
		enabled   bool
		path      string
		atDefault int
		atPath    int
	}{
		{"on, at the default path", true, "/metrics", http.StatusOK, http.StatusOK},
		{"on, moved", true, "/ops/scrape", http.StatusNotFound, http.StatusOK},
		{"off", false, "/metrics", http.StatusNotFound, http.StatusNotFound},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Server.APIKey = key
			cfg.Metrics.Enabled = c.enabled
			cfg.Metrics.Path = c.path
			srv := mustNew(t, cfg)
			defer stopImmediately(t, srv)
			if got := get(srv.Handler(), "/metrics"); got != c.atDefault {
				t.Errorf("GET /metrics returned %d, want %d", got, c.atDefault)
			}
			if got := get(srv.Handler(), c.path); got != c.atPath {
				t.Errorf("GET %s returned %d, want %d", c.path, got, c.atPath)
			}
		})
	}
}
