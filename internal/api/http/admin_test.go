package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
)

func TestAdministeringJobsNeedsAnOperatorCredential(t *testing.T) {
	// 403 rather than 404, for the reason the tenant routes give: the existence
	// of the administration surface is not a secret, and hiding it would make a
	// misconfigured operator key look like a routing bug.
	srv, _, _ := newJobsServer(t)
	for _, path := range []string{
		"/api/v1/admin/jobs",
		"/api/v1/admin/jobs/types",
	} {
		rr := send(t, srv, keyAcme, http.MethodGet, path, nil)
		if rr.Code != http.StatusForbidden {
			t.Errorf("GET %s with a tenant-bound key = %d, want 403", path, rr.Code)
		}
	}
	rr := send(t, srv, keyAcme, http.MethodPost, "/api/v1/admin/jobs/vector.rebuild/run", nil)
	if rr.Code != http.StatusForbidden {
		t.Errorf("running a job with a tenant-bound key = %d, want 403", rr.Code)
	}

	// The refusal must be about what was asked for. The Phase 9 verification
	// run hit the earlier version, which answered a question about background
	// jobs with "administering tenants requires…" — a sentence that sends an
	// operator to look at the wrong thing.
	if body := rr.Body.String(); !strings.Contains(body, "background jobs") {
		t.Errorf("the refusal does not name what was refused: %s", body)
	}
	if body := rr.Body.String(); strings.Contains(body, "administering tenants") {
		t.Errorf("a jobs endpoint refused with a message about tenants: %s", body)
	}

	// And the tenant routes keep saying what they are about.
	tenants := send(t, srv, keyAcme, http.MethodGet, "/api/v1/tenants", nil)
	if !strings.Contains(tenants.Body.String(), "administering tenants") {
		t.Errorf("the tenants refusal lost its own subject: %s", tenants.Body)
	}
}

func TestListingJobsShowsWhatIsQueued(t *testing.T) {
	srv, q, _ := newJobsServer(t)
	ctx := context.Background()
	j := &jobs.Job{Tenant: "default", Type: "vector.rebuild", Priority: 3}
	if err := q.Submit(ctx, j); err != nil {
		t.Fatal(err)
	}

	rr := send(t, srv, keyRoot, http.MethodGet, "/api/v1/admin/jobs", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /admin/jobs = %d: %s", rr.Code, rr.Body)
	}
	body := jsonBody(t, rr)
	list, _ := body["jobs"].([]any)
	if len(list) != 1 {
		t.Fatalf("the listing holds %d jobs: %s", len(list), rr.Body)
	}
	got, _ := list[0].(map[string]any)
	if got["id"] != j.ID.String() || got["state"] != "pending" || got["type"] != "vector.rebuild" {
		t.Fatalf("the job reads as %v", got)
	}
	// The payload is reported as a size and never as content.
	if _, leaked := got["payload"]; leaked {
		t.Fatal("the listing returned a job payload")
	}
}

func TestAJobListingIsScopedToItsTenant(t *testing.T) {
	srv, q, _ := newJobsServer(t)
	ctx := context.Background()
	mine := &jobs.Job{Tenant: "default", Type: "vector.rebuild"}
	theirs := &jobs.Job{Tenant: "other", Type: "vector.rebuild"}
	for _, j := range []*jobs.Job{mine, theirs} {
		if err := q.Submit(ctx, j); err != nil {
			t.Fatal(err)
		}
	}

	rr := send(t, srv, keyRoot, http.MethodGet, "/api/v1/admin/jobs", nil)
	list, _ := jsonBody(t, rr)["jobs"].([]any)
	if len(list) != 1 {
		t.Fatalf("an unheadered operator listing showed %d jobs, want only the default tenant's", len(list))
	}

	// The other tenant is reachable, but only by naming it.
	r := httptest.NewRequest(http.MethodGet, "/api/v1/admin/jobs", nil)
	r.Header.Set("Authorization", "Bearer "+keyRoot)
	r.Header.Set(remhttp.TenantHeader, "other")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, r)
	list, _ = jsonBody(t, rec)["jobs"].([]any)
	if len(list) != 1 {
		t.Fatalf("naming the other tenant showed %d jobs", len(list))
	}
	if got, _ := list[0].(map[string]any); got["id"] != theirs.ID.String() {
		t.Fatalf("the header selected the wrong tenant's jobs: %v", got)
	}
}

func TestFilteringAListingByStateAndType(t *testing.T) {
	srv, q, _ := newJobsServer(t)
	ctx := context.Background()
	pending := &jobs.Job{Tenant: "default", Type: "vector.rebuild"}
	if err := q.Submit(ctx, pending); err != nil {
		t.Fatal(err)
	}
	other := &jobs.Job{Tenant: "default", Type: "text.rebuild"}
	if err := q.Submit(ctx, other); err != nil {
		t.Fatal(err)
	}

	rr := send(t, srv, keyRoot, http.MethodGet,
		"/api/v1/admin/jobs?state=pending&type=text.rebuild", nil)
	list, _ := jsonBody(t, rr)["jobs"].([]any)
	if len(list) != 1 {
		t.Fatalf("the filtered listing holds %d jobs: %s", len(list), rr.Body)
	}

	// A state or type that is not one is refused by name rather than matching
	// nothing: an empty page is what a working filter over an empty queue looks
	// like, and the two must not be confusable.
	for _, path := range []string{
		"/api/v1/admin/jobs?state=asleep",
		"/api/v1/admin/jobs?type=Vector%20Rebuild",
	} {
		if rr := send(t, srv, keyRoot, http.MethodGet, path, nil); rr.Code != http.StatusUnprocessableEntity {
			t.Errorf("GET %s = %d, want 422: %s", path, rr.Code, rr.Body)
		}
	}
}

func TestRunningAJobQueuesItOnce(t *testing.T) {
	srv, _, _ := newJobsServer(t)

	rr := send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/vector.rebuild/run", nil)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("running a job = %d, want 202: %s", rr.Code, rr.Body)
	}
	// 202 and not 201: the job is queued, not done. For a rebuild the work is
	// minutes away.
	body := jsonBody(t, rr)
	if body["state"] != "pending" || body["type"] != "vector.rebuild" {
		t.Fatalf("the queued job reads as %v", body)
	}

	// A second run of the same type is a conflict, not a second job.
	if rr := send(t, srv, keyRoot, http.MethodPost,
		"/api/v1/admin/jobs/vector.rebuild/run", nil); rr.Code != http.StatusConflict {
		t.Fatalf("a second run = %d, want 409: %s", rr.Code, rr.Body)
	}

	// A type this binary has no handler for is a 404 that says where to look.
	rr = send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/nonsense.type/run", nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("an unknown type = %d, want 404: %s", rr.Code, rr.Body)
	}
	if !strings.Contains(rr.Body.String(), "types") {
		t.Fatalf("the refusal does not name the endpoint that lists them: %s", rr.Body)
	}
}

func TestCancellingAJobThroughTheAPI(t *testing.T) {
	srv, q, _ := newJobsServer(t)
	j := &jobs.Job{Tenant: "default", Type: "vector.rebuild"}
	if err := q.Submit(context.Background(), j); err != nil {
		t.Fatal(err)
	}

	rr := send(t, srv, keyRoot, http.MethodPost,
		"/api/v1/admin/jobs/"+j.ID.String()+"/cancel", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("cancelling = %d: %s", rr.Code, rr.Body)
	}
	if body := jsonBody(t, rr); body["state"] != "cancelled" {
		t.Fatalf("the cancelled job reads as %v", body)
	}
	// A second cancellation is a 409, because "cancelled" and "finished twenty
	// minutes ago" are different answers.
	if rr := send(t, srv, keyRoot, http.MethodPost,
		"/api/v1/admin/jobs/"+j.ID.String()+"/cancel", nil); rr.Code != http.StatusConflict {
		t.Fatalf("a second cancellation = %d, want 409", rr.Code)
	}
	if rr := send(t, srv, keyRoot, http.MethodPost,
		"/api/v1/admin/jobs/not-a-uuid/cancel", nil); rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("a malformed id = %d, want 422", rr.Code)
	}
}

func TestTheJobTypeListingSaysWhatThisBinaryCanRun(t *testing.T) {
	// Without it, "which types may I run" is answered by reading the source,
	// and a deployment on an older binary has no way to say what it is missing.
	srv, _, _ := newJobsServer(t)
	rr := send(t, srv, keyRoot, http.MethodGet, "/api/v1/admin/jobs/types", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /admin/jobs/types = %d: %s", rr.Code, rr.Body)
	}
	types, _ := jsonBody(t, rr)["types"].([]any)
	if len(types) != 3 {
		t.Fatalf("the registry reports %d types: %s", len(types), rr.Body)
	}
	first, _ := types[0].(map[string]any)
	if first["type"] != "jobs.reap" || first["description"] == "" {
		t.Fatalf("the first type reads as %v; the listing must be ordered and described", first)
	}
	// A recurring type says how often it recurs, and a type nothing schedules
	// says nothing rather than zero.
	if first["every_seconds"] != float64(3600) {
		t.Fatalf("the recurring type does not report its interval: %v", first)
	}
	last, _ := types[2].(map[string]any)
	if _, recurring := last["every_seconds"]; recurring {
		t.Fatalf("a type nothing schedules reported an interval: %v", last)
	}
}

func TestAProcessWithoutTheJobFrameworkSaysSo(t *testing.T) {
	// Not a 404. The difference between "this build does not run jobs" and "you
	// typed the URL wrong" is one an operator needs.
	srv := newServer(t)
	rr := send(t, srv, keyRoot, http.MethodGet, "/api/v1/admin/jobs", nil)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /admin/jobs without a queue = %d, want 503: %s", rr.Code, rr.Body)
	}
}

// --- harness ----------------------------------------------------------------

func newJobsServer(t *testing.T) (http.Handler, *jobs.Queue, *clock.Fake) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	keyring, err := auth.NewKeyring([]auth.Credential{
		{ID: "acme-agent", Secret: keyAcme, Tenant: "acme"},
		{ID: "operator", Secret: keyRoot, CrossTenant: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	clk := clock.NewFake(clock.FakeStart)
	queue := jobs.NewQueue(kv, clk)
	reg := jobs.NewRegistry()
	for _, e := range []jobs.Entry{
		{Type: "vector.rebuild", Description: "rebuild the vector index", Handler: noopHandler{}},
		{Type: "text.rebuild", Description: "rebuild the keyword index", Handler: noopHandler{}},
		// One recurring type, because a pause suppresses a schedule and
		// there is nothing to suppress on a type nothing schedules.
		{Type: "jobs.reap", Description: "remove finished job rows", Handler: noopHandler{}, Every: time.Hour},
	} {
		if err := reg.Register(e); err != nil {
			t.Fatal(err)
		}
	}

	srv := remhttp.NewRouter(remhttp.Deps{
		Tenants:       tenantkv.New(kv, clk),
		Resolver:      tenant.Resolver{Capability: tenant.IdentityScoped, Default: "default"},
		Auth:          keyring,
		Metrics:       obs.NewMetrics(),
		Clock:         clk,
		Jobs:          remhttp.JobsDeps{Queue: queue, Registry: reg, Retention: 24 * time.Hour},
		AutoProvision: true,
		Ready:         func() error { return nil },
	})
	return srv, queue, clk
}

type noopHandler struct{}

func (noopHandler) Handle(context.Context, *jobs.Job, jobs.Checkpointer) error { return nil }

// TestAManualRunConflictNamesTheOutstandingJob: "something of this type is
// already outstanding" sends an operator to a listing to find out what. The
// refusal names the job and its state, so the next thing they do is look at
// that job rather than search for it.
func TestAManualRunConflictNamesTheOutstandingJob(t *testing.T) {
	srv, q, _ := newJobsServer(t)
	existing := &jobs.Job{Tenant: "default", Type: "vector.rebuild"}
	if err := q.Submit(context.Background(), existing); err != nil {
		t.Fatal(err)
	}

	rr := send(t, srv, keyRoot, http.MethodPost, "/api/v1/admin/jobs/vector.rebuild/run", nil)
	if rr.Code != http.StatusConflict {
		t.Fatalf("running a type with one outstanding = %d, want 409: %s", rr.Code, rr.Body)
	}
	body := rr.Body.String()
	if !strings.Contains(body, existing.ID.String()) {
		t.Errorf("the conflict does not name the outstanding job: %s", body)
	}
	if !strings.Contains(body, "pending") {
		t.Errorf("the conflict does not say what that job is doing: %s", body)
	}
}
