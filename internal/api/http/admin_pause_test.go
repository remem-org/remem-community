package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/jobs"
)

func TestPausingARecurringTypeIsVisibleInTheTypeListing(t *testing.T) {
	srv, q, clk := newJobsServer(t)
	until := clk.Now().Add(time.Hour).UTC()

	rr := send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/jobs.reap/pause",
		jsonRequest(t, map[string]any{"expires_at": until.Format(time.RFC3339)}))
	if rr.Code != http.StatusOK {
		t.Fatalf("pausing = %d: %s", rr.Code, rr.Body)
	}
	body := jsonBody(t, rr)
	if body["type"] != "jobs.reap" || body["paused"] != true {
		t.Fatalf("the pause response reads as %v", body)
	}
	if body["pause_expires_at"] == nil || body["paused_by"] != "operator" {
		t.Fatalf("the pause does not say when it lifts or who set it: %v", body)
	}

	// The status surface an operator reads is the type listing.
	types := typeByName(t, srv, "jobs.reap")
	if types["paused"] != true || types["pause_expires_at"] == nil {
		t.Fatalf("the type listing does not report the pause: %v", types)
	}
	// And the other types are untouched.
	other := typeByName(t, srv, "vector.rebuild")
	if other["paused"] == true {
		t.Fatalf("pausing one type paused another: %v", other)
	}

	// The durable state agrees with what the surface said.
	if _, err := q.PauseOf(context.Background(), "default", "jobs.reap"); err != nil {
		t.Fatalf("the route reported a pause the queue does not hold: %v", err)
	}
}

func TestAPauseIsAcceptedAsADurationToo(t *testing.T) {
	srv, _, _ := newJobsServer(t)
	rr := send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/jobs.reap/pause",
		jsonRequest(t, map[string]any{"duration_seconds": 1800}))
	if rr.Code != http.StatusOK {
		t.Fatalf("pausing for a duration = %d: %s", rr.Code, rr.Body)
	}
	if jsonBody(t, rr)["paused"] != true {
		t.Fatalf("the response reads as %s", rr.Body)
	}
}

func TestAPauseWithNoUsableExpiryIsRefusedByName(t *testing.T) {
	srv, q, clk := newJobsServer(t)
	ctx := context.Background()

	cases := []struct {
		name string
		body map[string]any
		says string
	}{
		{"neither", map[string]any{}, "expires_at"},
		{"both", map[string]any{
			"expires_at":       clk.Now().Add(time.Hour).UTC().Format(time.RFC3339),
			"duration_seconds": 3600,
		}, "not both"},
		{"in the past", map[string]any{
			"expires_at": clk.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
		}, "future"},
		{"beyond the maximum", map[string]any{
			"duration_seconds": int((jobs.MaxPauseDuration + time.Hour).Seconds()),
		}, "longest"},
		{"unparseable", map[string]any{"expires_at": "next tuesday"}, "expires_at"},
		{"negative", map[string]any{"duration_seconds": -60}, "future"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rr := send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/jobs.reap/pause", jsonRequest(t, c.body))
			if rr.Code != http.StatusUnprocessableEntity {
				t.Fatalf("pausing with %s = %d, want 422: %s", c.name, rr.Code, rr.Body)
			}
			if !strings.Contains(rr.Body.String(), c.says) {
				t.Errorf("the refusal does not say what was wrong (%q): %s", c.says, rr.Body)
			}
			if _, err := q.PauseOf(ctx, "default", "jobs.reap"); err == nil {
				t.Fatal("a refused pause was written anyway")
			}
		})
	}
}

// TestPausingATypeNothingSchedulesIsRefused: a pause suppresses a schedule. On
// a type nothing schedules it would report success and do nothing, which is the
// one answer an operator cannot act on.
func TestPausingATypeNothingSchedulesIsRefused(t *testing.T) {
	srv, _, _ := newJobsServer(t)
	rr := send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/vector.rebuild/pause",
		jsonRequest(t, map[string]any{"duration_seconds": 60}))
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("pausing a non-recurring type = %d, want 422: %s", rr.Code, rr.Body)
	}
	if !strings.Contains(rr.Body.String(), "recurring") {
		t.Errorf("the refusal does not say why: %s", rr.Body)
	}
}

func TestPausingAnUnknownTypeIs404(t *testing.T) {
	srv, _, _ := newJobsServer(t)
	rr := send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/nothing.here/pause",
		jsonRequest(t, map[string]any{"duration_seconds": 60}))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("pausing an unregistered type = %d, want 404: %s", rr.Code, rr.Body)
	}
}

func TestResumeLiftsAPauseAndIsIdempotent(t *testing.T) {
	srv, q, _ := newJobsServer(t)
	ctx := context.Background()

	if rr := send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/jobs.reap/pause",
		jsonRequest(t, map[string]any{"duration_seconds": 600})); rr.Code != http.StatusOK {
		t.Fatalf("pausing = %d: %s", rr.Code, rr.Body)
	}
	rr := send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/jobs.reap/resume", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("resuming = %d: %s", rr.Code, rr.Body)
	}
	if body := jsonBody(t, rr); body["paused"] != false {
		t.Fatalf("the resume response reads as %v", body)
	}
	if _, err := q.PauseOf(ctx, "default", "jobs.reap"); err == nil {
		t.Fatal("the pause survived a resume")
	}
	// Resuming again is a success: the post-condition already holds, and an
	// operator clicking resume on a pause that expired while they were looking
	// at it should not be told the server is broken.
	if again := send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/jobs.reap/resume", nil); again.Code != http.StatusOK {
		t.Fatalf("a second resume = %d: %s", again.Code, again.Body)
	}
}

func TestAnExpiredPauseReportsItselfUnpaused(t *testing.T) {
	srv, _, clk := newJobsServer(t)
	if rr := send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/jobs.reap/pause",
		jsonRequest(t, map[string]any{"duration_seconds": 60})); rr.Code != http.StatusOK {
		t.Fatalf("pausing = %d: %s", rr.Code, rr.Body)
	}
	clk.Advance(2 * time.Minute)

	got := typeByName(t, srv, "jobs.reap")
	if got["paused"] == true {
		t.Fatalf("an expired pause still reports itself in force: %v", got)
	}
	if _, says := got["pause_expires_at"]; says {
		t.Fatalf("an expired pause still reports an expiry: %v", got)
	}
}

// TestAManualRunWorksWhilePaused: the pause is the schedule's, not the
// operator's, and the API has to make that true rather than only say it.
func TestAManualRunWorksWhilePaused(t *testing.T) {
	srv, _, _ := newJobsServer(t)
	if rr := send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/jobs.reap/pause",
		jsonRequest(t, map[string]any{"duration_seconds": 600})); rr.Code != http.StatusOK {
		t.Fatalf("pausing = %d: %s", rr.Code, rr.Body)
	}
	rr := send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/jobs.reap/run", nil)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("a manual run while paused = %d, want 202: %s", rr.Code, rr.Body)
	}
	body := jsonBody(t, rr)
	if body["state"] != "pending" || body["id"] == "" {
		t.Fatalf("the manual run did not report a queued job: %v", body)
	}
	// And the pause is still in force.
	if got := typeByName(t, srv, "jobs.reap"); got["paused"] != true {
		t.Fatalf("a manual run cleared the pause: %v", got)
	}
}

func TestPausingAndResumingNeedAnOperatorCredential(t *testing.T) {
	srv, _, _ := newJobsServer(t)
	for _, path := range []string{
		"/api/v1/admin/jobs/jobs.reap/pause",
		"/api/v1/admin/jobs/jobs.reap/resume",
	} {
		rr := send(t, srv, keyAcme, http.MethodPost, path, jsonRequest(t, map[string]any{"duration_seconds": 60}))
		if rr.Code != http.StatusForbidden {
			t.Errorf("POST %s with a tenant-bound key = %d, want 403", path, rr.Code)
		}
		if !strings.Contains(rr.Body.String(), "background jobs") {
			t.Errorf("the refusal does not name what was refused: %s", rr.Body)
		}
	}
}

// TestAPauseIsScopedToTheRequestTenant holds Invariant 1 on the new surface:
// the tenant a pause lands in is the one the request resolved to, and a second
// tenant's listing does not see it.
func TestAPauseIsScopedToTheRequestTenant(t *testing.T) {
	srv, _, _ := newJobsServer(t)
	rr := sendTenant(t, srv, keyRoot, "acme", http.MethodPost, "/api/v1/admin/jobs/jobs.reap/pause",
		jsonRequest(t, map[string]any{"duration_seconds": 600}))
	if rr.Code != http.StatusOK {
		t.Fatalf("pausing in acme = %d: %s", rr.Code, rr.Body)
	}
	if got := typeByName(t, srv, "jobs.reap"); got["paused"] == true {
		t.Fatalf("a pause set in acme reached the default tenant: %v", got)
	}
}

// typeByName reads one entry out of the job-type listing: the status surface.
func typeByName(t *testing.T, srv http.Handler, name string) map[string]any {
	t.Helper()
	rr := send(t, srv, keyRoot, http.MethodGet, "/api/v1/admin/jobs/types", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /admin/jobs/types = %d: %s", rr.Code, rr.Body)
	}
	types, _ := jsonBody(t, rr)["types"].([]any)
	for _, raw := range types {
		entry, _ := raw.(map[string]any)
		if entry["type"] == name {
			return entry
		}
	}
	t.Fatalf("the listing has no type %q: %s", name, rr.Body)
	return nil
}

// jsonRequest renders a request body. The pause routes take one and the resume
// routes take none, so the two forms sit side by side in these tests.
func jsonRequest(t *testing.T, v map[string]any) io.Reader {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(raw)
}

// sendTenant is send with an X-Remem-Tenant header, which is how every
// administrative route names a tenant other than the one the request would
// resolve to by default.
func sendTenant(t *testing.T, srv http.Handler, key, tid, method, path string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, body)
	r.Header.Set("Authorization", "Bearer "+key)
	r.Header.Set("X-Remem-Tenant", tid)
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	return rr
}
