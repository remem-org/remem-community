package http_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/tenant"
)

func TestJobHistoryReportsEachAttemptAndItsCounts(t *testing.T) {
	srv, q, clk := newJobsServer(t)
	ctx := context.Background()

	jid := id.New()
	processed, changed := uint64(120), uint64(7)
	first := &jobs.Run{
		JobID: jid, Tenant: "default", Namespace: tenant.DefaultNamespace,
		Type: "vector.rebuild", Attempt: 1,
		StartedAt: clk.Now().UTC(), FinishedAt: clk.Now().Add(2 * time.Second).UTC(),
		Outcome: jobs.Retry, Error: "the index was busy", Owner: "node-1",
		RecordsProcessed: &processed, RecordsChanged: &changed,
	}
	if err := q.RecordRun(ctx, first); err != nil {
		t.Fatalf("RecordRun: %v", err)
	}
	second := &jobs.Run{
		JobID: jid, Tenant: "default", Namespace: tenant.DefaultNamespace,
		Type: "vector.rebuild", Attempt: 2,
		StartedAt: clk.Now().Add(time.Minute).UTC(), FinishedAt: clk.Now().Add(time.Minute + time.Second).UTC(),
		Outcome: jobs.Completed, Owner: "node-1",
	}
	if err := q.RecordRun(ctx, second); err != nil {
		t.Fatalf("RecordRun: %v", err)
	}

	rr := send(t, srv, keyRoot, http.MethodGet, "/api/v1/admin/jobs/history", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /admin/jobs/history = %d: %s", rr.Code, rr.Body)
	}
	body := jsonBody(t, rr)
	runs, _ := body["runs"].([]any)
	if len(runs) != 2 {
		t.Fatalf("two attempts returned %d rows: %s", len(runs), rr.Body)
	}

	// Newest first.
	newest, _ := runs[0].(map[string]any)
	if newest["attempt"] != float64(2) || newest["outcome"] != "completed" {
		t.Fatalf("the newest attempt reads as %v", newest)
	}
	if newest["job_id"] != jid.String() || newest["id"] == newest["job_id"] {
		t.Fatalf("an attempt must have an identity of its own beside its job's: %v", newest)
	}
	if newest["duration_ms"] != float64(1000) {
		t.Fatalf("the attempt reports duration_ms %v, want 1000", newest["duration_ms"])
	}
	// A handler that reported nothing leaves both counts null, and null is in
	// the document: a field that disappears is one a client reads as zero.
	if newest["records_processed"] != nil || newest["records_changed"] != nil {
		t.Fatalf("an attempt nobody counted reports counts: %v", newest)
	}

	oldest, _ := runs[1].(map[string]any)
	if oldest["attempt"] != float64(1) || oldest["outcome"] != "retry" {
		t.Fatalf("the first attempt reads as %v", oldest)
	}
	if oldest["error"] != "the index was busy" {
		t.Fatalf("the failed attempt does not say why: %v", oldest)
	}
	if oldest["records_processed"] != float64(120) || oldest["records_changed"] != float64(7) {
		t.Fatalf("the counts read as %v / %v", oldest["records_processed"], oldest["records_changed"])
	}

	// The window that bounds the history is reported, so an empty page and a
	// short retention stay distinguishable.
	if body["retention_seconds"] == nil || body["retention_seconds"] == float64(0) {
		t.Fatalf("the history does not disclose its retention: %s", rr.Body)
	}
}

func TestJobHistoryFiltersByTypeAndBounds(t *testing.T) {
	srv, q, clk := newJobsServer(t)
	ctx := context.Background()

	for i, typ := range []jobs.Type{"vector.rebuild", "text.rebuild", "vector.rebuild"} {
		if err := q.RecordRun(ctx, &jobs.Run{
			JobID: id.New(), Tenant: "default", Namespace: tenant.DefaultNamespace,
			Type: typ, Attempt: 1,
			StartedAt:  clk.Now().Add(time.Duration(i) * time.Second).UTC(),
			FinishedAt: clk.Now().Add(time.Duration(i) * time.Second).UTC(),
			Outcome:    jobs.Completed,
		}); err != nil {
			t.Fatal(err)
		}
	}

	filtered := send(t, srv, keyRoot, http.MethodGet,
		"/api/v1/admin/jobs/history?type=vector.rebuild", nil)
	runs, _ := jsonBody(t, filtered)["runs"].([]any)
	if len(runs) != 2 {
		t.Fatalf("the filter returned %d rows, want 2: %s", len(runs), filtered.Body)
	}
	for _, raw := range runs {
		row, _ := raw.(map[string]any)
		if row["type"] != "vector.rebuild" {
			t.Fatalf("the filter returned a %v row", row["type"])
		}
	}

	bounded := send(t, srv, keyRoot, http.MethodGet, "/api/v1/admin/jobs/history?limit=1", nil)
	boundedBody := jsonBody(t, bounded)
	if runs, _ := boundedBody["runs"].([]any); len(runs) != 1 {
		t.Fatalf("a limit of 1 returned %d rows", len(runs))
	}
	if boundedBody["has_more"] != true || boundedBody["next_cursor"] == "" {
		t.Fatalf("a bounded page did not disclose continuation: %s", bounded.Body)
	}
	cursor := boundedBody["next_cursor"].(string)
	continued := send(t, srv, keyRoot, http.MethodGet,
		"/api/v1/admin/jobs/history?type=vector.rebuild&limit=1&cursor="+cursor, nil)
	continuedRuns, _ := jsonBody(t, continued)["runs"].([]any)
	if len(continuedRuns) != 1 {
		t.Fatalf("the filtered continuation returned %d rows: %s", len(continuedRuns), continued.Body)
	}
	if got := continuedRuns[0].(map[string]any)["type"]; got != "vector.rebuild" {
		t.Fatalf("the cursor continuation returned type %v", got)
	}
	if bad := send(t, srv, keyRoot, http.MethodGet,
		"/api/v1/admin/jobs/history?type=Vector%20Rebuild", nil); bad.Code != http.StatusUnprocessableEntity {
		t.Fatalf("a malformed type filter = %d, want 422: %s", bad.Code, bad.Body)
	}
}

// TestJobHistoryIsNotConfusedWithAJobLookup: /history is a literal and takes
// precedence over /{id}, the way /types already does. Without it "history"
// would be parsed as a job id and answered with a message about a malformed
// identifier.
func TestJobHistoryIsNotConfusedWithAJobLookup(t *testing.T) {
	srv, _, _ := newJobsServer(t)
	rr := send(t, srv, keyRoot, http.MethodGet, "/api/v1/admin/jobs/history", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /admin/jobs/history = %d: %s", rr.Code, rr.Body)
	}
	if _, present := jsonBody(t, rr)["runs"]; !present {
		t.Fatalf("the history route answered with something else: %s", rr.Body)
	}
}

func TestJobHistoryIsTenantScopedAndNeedsAnOperatorCredential(t *testing.T) {
	srv, q, clk := newJobsServer(t)
	ctx := context.Background()

	if err := q.RecordRun(ctx, &jobs.Run{
		JobID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
		Type: "vector.rebuild", Attempt: 1,
		StartedAt: clk.Now().UTC(), FinishedAt: clk.Now().UTC(), Outcome: jobs.Completed,
	}); err != nil {
		t.Fatal(err)
	}

	// The default tenant sees nothing of acme's.
	rr := send(t, srv, keyRoot, http.MethodGet, "/api/v1/admin/jobs/history", nil)
	if runs, _ := jsonBody(t, rr)["runs"].([]any); len(runs) != 0 {
		t.Fatalf("one tenant's history reached another: %s", rr.Body)
	}
	// And acme's own, named by header, sees exactly its own.
	mine := sendTenant(t, srv, keyRoot, "acme", http.MethodGet, "/api/v1/admin/jobs/history", nil)
	if runs, _ := jsonBody(t, mine)["runs"].([]any); len(runs) != 1 {
		t.Fatalf("the tenant that ran the job sees %s", mine.Body)
	}

	// Read-only or not, it is administration: it needs a credential that is
	// not bound to one tenant, like every other route under /admin.
	refused := send(t, srv, keyAcme, http.MethodGet, "/api/v1/admin/jobs/history", nil)
	if refused.Code != http.StatusForbidden {
		t.Fatalf("history with a tenant-bound key = %d, want 403: %s", refused.Code, refused.Body)
	}
}
