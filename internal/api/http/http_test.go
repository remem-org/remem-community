package http_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/api/mcp"
	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/lifecycle/policykv"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

// The two credentials every isolation test uses. They are bound, so no header
// can talk either out of its tenant — which is the property under test.
const (
	keyAcme  = "key-acme"
	keyOther = "key-other"
	keyRoot  = "key-root"
)

func newServer(t *testing.T) http.Handler {
	t.Helper()
	return newServerWith(t, func(*remhttp.Deps) {})
}

// newServerWith is newServer with its dependencies adjusted before the router
// is built.
func newServerWith(t *testing.T, adjust func(*remhttp.Deps)) http.Handler {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	keyring, err := auth.NewKeyring([]auth.Credential{
		{ID: "acme-agent", Secret: keyAcme, Tenant: "acme"},
		{ID: "other-agent", Secret: keyOther, Tenant: "other"},
		{ID: "operator", Secret: keyRoot, CrossTenant: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	clk := clock.NewFake(clock.FakeStart)
	svc := memory.New(kv, record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(text.New())),
		flat.New(vector.NewStore(kv), distance.L2),
		embeddingtest.New(), clk, attr.MustTable(), graph.NewService(kv, clk), memory.Config{},
		memory.WithTextIndex(text.New()))

	d := remhttp.Deps{
		Memories:      svc,
		Tenants:       tenantkv.New(kv, clk),
		Resolver:      tenant.Resolver{Capability: tenant.IdentityScoped, Default: "default"},
		Auth:          keyring,
		Metrics:       obs.NewMetrics(),
		Clock:         clk,
		Policies:      policykv.New(kv, clk),
		AutoProvision: true,
		Ready:         func() error { return nil },
	}
	adjust(&d)
	return remhttp.NewRouter(d)
}

// do issues a request with the acme credential.
func do(t *testing.T, method, path string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	return doAs(t, keyAcme, method, path, body)
}

func doAs(t *testing.T, key, method, path string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	return send(t, newServer(t), key, method, path, body)
}

func send(t *testing.T, srv http.Handler, key, method, path string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, body)
	if key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	return rr
}

func jsonBody(t *testing.T, rr *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil {
		t.Fatalf("response is not JSON (%d): %s", rr.Code, rr.Body.String())
	}
	return m
}

// createAs stores a memory and returns its id.
func createAs(t *testing.T, srv http.Handler, key, content string) string {
	t.Helper()
	rr := send(t, srv, key, "POST", remhttp.APIPrefix+"/memories",
		strings.NewReader(fmt.Sprintf(`{"content":%q}`, content)))
	if rr.Code != http.StatusCreated {
		t.Fatalf("create returned %d: %s", rr.Code, rr.Body.String())
	}
	return jsonBody(t, rr)["id"].(string)
}

// --- the plan's four tests -------------------------------------------------

func TestErrorsAreProblemDocuments(t *testing.T) {
	rr := do(t, "GET", remhttp.APIPrefix+"/memories/"+id.New().String(), nil)
	if rr.Code != 404 {
		t.Fatalf("got %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content type %q", ct)
	}
	p := jsonBody(t, rr)
	for _, f := range []string{"type", "title", "status", "detail", "instance"} {
		if _, ok := p[f]; !ok {
			t.Errorf("problem document missing %q", f)
		}
	}
	if p["status"].(float64) != 404 {
		t.Errorf("the document's status field disagrees with the response: %v", p["status"])
	}
	if p["instance"] != remhttp.APIPrefix+"/memories/"+strings.TrimPrefix(p["instance"].(string), remhttp.APIPrefix+"/memories/") {
		t.Errorf("instance does not identify the occurrence: %v", p["instance"])
	}
}

func TestValidationIsFourHundred(t *testing.T) {
	// Rust returned 422 for validation; 400 is correct for a malformed request
	// body and 422 for a well-formed one that fails semantic validation.
	rr := do(t, "POST", remhttp.APIPrefix+"/memories", strings.NewReader(`{"content":`))
	if rr.Code != 400 {
		t.Fatalf("malformed JSON should be 400, got %d: %s", rr.Code, rr.Body.String())
	}

	rr = do(t, "POST", remhttp.APIPrefix+"/memories", strings.NewReader(`{"content":""}`))
	if rr.Code != 422 {
		t.Fatalf("semantic failure should be 422, got %d: %s", rr.Code, rr.Body.String())
	}
}

// --- the rest ---------------------------------------------------------------

func TestTheHappyPath(t *testing.T) {
	srv := newServer(t)

	rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories",
		strings.NewReader(`{"content":"the sky is blue","tags":["Weather"],"source":"test"}`))
	if rr.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
	}
	created := jsonBody(t, rr)
	mID := created["id"].(string)
	if created["content"] != "the sky is blue" {
		t.Fatalf("content = %v", created["content"])
	}
	if tags := created["tags"].([]any); len(tags) != 1 || tags[0] != "weather" {
		t.Fatalf("tags = %v; they are normalised on write", created["tags"])
	}

	rr = send(t, srv, keyAcme, "GET", remhttp.APIPrefix+"/memories/"+mID, nil)
	if rr.Code != 200 {
		t.Fatalf("get: %d", rr.Code)
	}

	rr = send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories/search",
		strings.NewReader(`{"query":"the sky is blue","search_type":"hybrid","limit":5}`))
	if rr.Code != 200 {
		t.Fatalf("search: %d %s", rr.Code, rr.Body.String())
	}
	res := jsonBody(t, rr)
	results := res["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("search returned %d results", len(results))
	}
	first := results[0].(map[string]any)
	if first["memory"].(map[string]any)["id"] != mID {
		t.Fatal("search returned a different memory")
	}
	// Both numbers are present, and they are different numbers.
	if _, ok := first["score"]; !ok {
		t.Error("no score")
	}
	if _, ok := first["fused_score"]; !ok {
		t.Error("no fused_score")
	}
	// truncated is always present, even when false: a flag that appears only
	// when set is a flag clients forget to check.
	if _, ok := res["truncated"]; !ok {
		t.Error("truncated is absent from the response")
	}

	rr = send(t, srv, keyAcme, "DELETE", remhttp.APIPrefix+"/memories/"+mID, nil)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", rr.Code)
	}
	if rr := send(t, srv, keyAcme, "GET", remhttp.APIPrefix+"/memories/"+mID, nil); rr.Code != 404 {
		t.Fatalf("an archived memory reads %d, want 404", rr.Code)
	}
	if rr := send(t, srv, keyAcme, "GET",
		remhttp.APIPrefix+"/memories/"+mID+"?include_archived=true", nil); rr.Code != 200 {
		t.Fatalf("archiving is not deletion, got %d", rr.Code)
	}
}

func TestBatchCreate(t *testing.T) {
	rr := do(t, "POST", remhttp.APIPrefix+"/memories:batch",
		strings.NewReader(`{"memories":[{"content":"one"},{"content":"two"}]}`))
	if rr.Code != http.StatusCreated {
		t.Fatalf("got %d: %s", rr.Code, rr.Body.String())
	}
	if n := len(jsonBody(t, rr)["memories"].([]any)); n != 2 {
		t.Fatalf("got %d memories", n)
	}
}

// A search query is user content. It travels in a body rather than a URL so it
// does not land in access logs, proxy logs and browser history.
func TestSearchIsNotAGet(t *testing.T) {
	rr := do(t, "GET", remhttp.APIPrefix+"/memories/search?query=secret", nil)
	if rr.Code == 200 {
		t.Fatal("search is served over GET; a query would then appear in every access log")
	}
}

// The two surfaces must not disagree about what a search can be asked. A
// capability gap between them is the same bug as an auth gap with a slower
// fuse.
func TestBothSurfacesAcceptTheSameSearchFilters(t *testing.T) {
	for _, field := range []string{
		"policy", "importance_min", "importance_max",
		"created_after", "created_before", "related_to", "cursor",
	} {
		t.Run(field, func(t *testing.T) {
			if !restSearchAccepts(field) {
				t.Errorf("the REST search body has no %s", field)
			}
			if !mcpSearchSchemaHas(field) {
				t.Errorf("the search_memories schema has no %s", field)
			}
		})
	}
}

func restSearchAccepts(field string) bool {
	typ := reflect.TypeOf(remhttp.SearchRequest{})
	for i := range typ.NumField() {
		jsonName := strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]
		if jsonName == field {
			return true
		}
	}
	return false
}

func mcpSearchSchemaHas(field string) bool {
	for _, tool := range mcp.Tools() {
		if tool.Name == mcp.ToolSearchMemories {
			_, ok := tool.InputSchema.Properties[field]
			return ok
		}
	}
	return false
}

// A short page used to mean "that is everything". After paging it does not,
// which is why has_more is always present rather than omitted when false.
func TestSearchResponseAlwaysCarriesHasMore(t *testing.T) {
	srv := newServer(t)
	rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories/search",
		strings.NewReader(`{"query":"nothing","search_type":"keyword","limit":5}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("search returned %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"has_more"`) {
		t.Fatalf("has_more is absent from a response with no results: %s", rr.Body.String())
	}
}

// A field name a client got wrong must not be silently ignored. A client that
// sends {"contnet": "..."} and receives 201 has stored an empty memory and
// will find out much later.
func TestUnknownFieldsAreRefused(t *testing.T) {
	rr := do(t, "POST", remhttp.APIPrefix+"/memories",
		strings.NewReader(`{"content":"fine","contnet":"typo"}`))
	if rr.Code != 400 {
		t.Fatalf("got %d, want 400", rr.Code)
	}
}

func TestAMalformedIDInThePathIsFourHundred(t *testing.T) {
	rr := do(t, "GET", remhttp.APIPrefix+"/memories/not-a-uuid", nil)
	if rr.Code != 400 {
		t.Fatalf("got %d, want 400: the request line itself is wrong", rr.Code)
	}
}

func TestAnUnroutedPathIsAProblemDocument(t *testing.T) {
	rr := do(t, "GET", "/api/v1/nonsense", nil)
	if rr.Code != 404 {
		t.Fatalf("got %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("a mistyped URL returns %q rather than a problem document", ct)
	}
}

func TestHealthAndReadinessAreUnauthenticated(t *testing.T) {
	for _, path := range []string{remhttp.APIPrefix + "/health", remhttp.APIPrefix + "/ready"} {
		rr := doAs(t, "", "GET", path, nil)
		if rr.Code != 200 {
			t.Errorf("%s returned %d without a credential", path, rr.Code)
		}
	}
}

// Liveness and readiness answer different questions. A process that is up but
// not yet able to serve must fail readiness and pass liveness, or an
// orchestrator restarts it in a loop while it is trying to start.
func TestReadinessFailsWhileTheServerIsNotReady(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(clock.FakeStart)
	srv := remhttp.NewRouter(remhttp.Deps{
		Memories: memory.New(kv, record.NewRepo(kv,
			record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(text.New())),
			flat.New(vector.NewStore(kv), distance.L2),
			embeddingtest.New(), clk, attr.MustTable(), graph.NewService(kv, clk), memory.Config{},
			memory.WithTextIndex(text.New())),
		Tenants:  tenantkv.New(kv, clk),
		Resolver: tenant.Resolver{Capability: tenant.IdentityScoped, Default: "default"},
		Auth:     mustKeyring(t),
		Metrics:  obs.NewMetrics(),
		Clock:    clk,
		Ready:    func() error { return errStillOpening },
	})

	rr := send(t, srv, "", "GET", remhttp.APIPrefix+"/ready", nil)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness returned %d while not ready", rr.Code)
	}
	if rr := send(t, srv, "", "GET", remhttp.APIPrefix+"/health", nil); rr.Code != 200 {
		t.Fatalf("liveness returned %d while the process is running", rr.Code)
	}
}

var errStillOpening = fmt.Errorf("the store is still opening")

func mustKeyring(t *testing.T) *auth.Keyring {
	t.Helper()
	k, err := auth.NewKeyring([]auth.Credential{{ID: "a", Secret: keyAcme, Tenant: "acme"}})
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// Spec §58: no engine error reaches a client. An internal failure returns a
// generic detail, and the real one goes to the log.
func TestInternalFailuresDoNotLeakTheirCause(t *testing.T) {
	kv := memkv.New()
	clk := clock.NewFake(clock.FakeStart)
	srv := remhttp.NewRouter(remhttp.Deps{
		Memories: memory.New(kv, record.NewRepo(kv,
			record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(text.New())),
			flat.New(vector.NewStore(kv), distance.L2),
			embeddingtest.New(), clk, attr.MustTable(), graph.NewService(kv, clk), memory.Config{},
			memory.WithTextIndex(text.New())),
		Tenants:       tenantkv.New(kv, clk),
		Resolver:      tenant.Resolver{Capability: tenant.IdentityScoped, Default: "default"},
		Auth:          mustKeyring(t),
		Metrics:       obs.NewMetrics(),
		Clock:         clk,
		AutoProvision: true,
	})
	// Closing the store makes every operation fail as Unavailable, which is the
	// closest a unit test can get to an engine failure.
	_ = kv.Close()

	rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories",
		strings.NewReader(`{"content":"anything"}`))
	if rr.Code < 500 {
		t.Fatalf("got %d, want a server-side status", rr.Code)
	}
	detail := jsonBody(t, rr)["detail"].(string)
	for _, leak := range []string{"memkv", "pebble", "closed", "store"} {
		if strings.Contains(strings.ToLower(detail), leak) {
			t.Fatalf("the detail names an internal cause (%q): %s", leak, detail)
		}
	}
}

// A panicking handler must not drop the connection: a client reports that as a
// network error, sending an operator to the load balancer for a bug that is in
// this process.
func TestAPanicBecomesAProblemDocument(t *testing.T) {
	srv := remhttp.NewRouter(remhttp.Deps{
		Metrics: obs.NewMetrics(),
		Clock:   clock.NewFake(clock.FakeStart),
		Auth:    mustKeyring(t),
		// No Memories: the handler dereferences a nil service and panics.
		Tenants:       tenantkv.New(memkv.New(), clock.NewFake(clock.FakeStart)),
		Resolver:      tenant.Resolver{Capability: tenant.IdentityScoped, Default: "default"},
		AutoProvision: true,
	})
	rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories",
		strings.NewReader(`{"content":"boom"}`))
	if rr.Code != 500 {
		t.Fatalf("got %d, want 500", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content type %q", ct)
	}
}

func TestAnOversizedBodyIsRefused(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(clock.FakeStart)
	srv := remhttp.NewRouter(remhttp.Deps{
		Memories: memory.New(kv, record.NewRepo(kv,
			record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(text.New())),
			flat.New(vector.NewStore(kv), distance.L2),
			embeddingtest.New(), clk, attr.MustTable(), graph.NewService(kv, clk), memory.Config{},
			memory.WithTextIndex(text.New())),
		Tenants:         tenantkv.New(kv, clk),
		Resolver:        tenant.Resolver{Capability: tenant.IdentityScoped, Default: "default"},
		Auth:            mustKeyring(t),
		Metrics:         obs.NewMetrics(),
		Clock:           clk,
		AutoProvision:   true,
		MaxRequestBytes: 64,
	})
	body := fmt.Sprintf(`{"content":%q}`, strings.Repeat("x", 500))
	rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories", strings.NewReader(body))
	if rr.Code != 400 && rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("an oversized body returned %d", rr.Code)
	}
}

// --- PATCH /memories/{id} --------------------------------------------------

func TestPatchUpdatesOnlyTheFieldsSent(t *testing.T) {
	srv := newServer(t)

	rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories",
		strings.NewReader(`{"content":"the first version","tags":["alpha"],"importance":0.9}`))
	if rr.Code != http.StatusCreated {
		t.Fatalf("create returned %d: %s", rr.Code, rr.Body.String())
	}
	rid := jsonBody(t, rr)["id"].(string)

	rr = send(t, srv, keyAcme, "PATCH", remhttp.APIPrefix+"/memories/"+rid,
		strings.NewReader(`{"content":"the second version"}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("patch returned %d: %s", rr.Code, rr.Body.String())
	}
	body := jsonBody(t, rr)

	if got := body["content"]; got != "the second version" {
		t.Fatalf("content is %v", got)
	}
	// Absent means "leave it alone", and the response must show that rather
	// than a zeroed field.
	if got := body["importance"].(float64); got < 0.89 || got > 0.91 {
		t.Fatalf("importance is %v, want 0.9 — an absent field must not be zeroed", got)
	}
	tags, _ := body["tags"].([]any)
	if len(tags) != 1 || tags[0] != "alpha" {
		t.Fatalf("tags are %v, want [alpha] — an absent field must not be cleared", body["tags"])
	}
}

// A field the server does not know is refused by name. A client that guessed a
// field and got a 200 has stored something other than what it meant to, and
// will not find out.
func TestPatchRefusesAnUnknownFieldByName(t *testing.T) {
	srv := newServer(t)
	rid := createAs(t, srv, keyAcme, "anything")

	rr := send(t, srv, keyAcme, "PATCH", remhttp.APIPrefix+"/memories/"+rid,
		strings.NewReader(`{"health":100}`))
	if rr.Code != http.StatusBadRequest && rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("setting health returned %d, want a refusal", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "health") {
		t.Fatalf("the refusal does not name the field: %s", rr.Body.String())
	}
}

func TestPatchOnAnotherTenantsMemoryIs404(t *testing.T) {
	srv := newServer(t)
	rid := createAs(t, srv, keyAcme, "acme's memory")

	rr := send(t, srv, keyOther, "PATCH", remhttp.APIPrefix+"/memories/"+rid,
		strings.NewReader(`{"content":"changed"}`))
	// 404, never 403. A 403 confirms the memory exists, which is a
	// cross-tenant disclosure of the shape of the data.
	if rr.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant PATCH returned %d, want 404: %s", rr.Code, rr.Body.String())
	}
}

func TestPatchWithNoFieldsIsRefused(t *testing.T) {
	srv := newServer(t)
	rid := createAs(t, srv, keyAcme, "anything")

	rr := send(t, srv, keyAcme, "PATCH", remhttp.APIPrefix+"/memories/"+rid,
		strings.NewReader(`{}`))
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("an empty patch returned %d, want 422: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "change something") {
		t.Fatalf("the message does not say what is wrong: %s", rr.Body.String())
	}
}

// An archived memory is a retirement, and the refusal is a 409 rather than a
// 404: the memory is there, and saying so is right for a caller who can still
// fetch it with include_archived.
func TestPatchOnAnArchivedMemoryIsAConflict(t *testing.T) {
	srv := newServer(t)
	rid := createAs(t, srv, keyAcme, "retire me")

	if rr := send(t, srv, keyAcme, "DELETE", remhttp.APIPrefix+"/memories/"+rid, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("archiving returned %d", rr.Code)
	}
	rr := send(t, srv, keyAcme, "PATCH", remhttp.APIPrefix+"/memories/"+rid,
		strings.NewReader(`{"content":"changed"}`))
	if rr.Code != http.StatusConflict {
		t.Fatalf("patching an archived memory returned %d, want 409: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "archived") {
		t.Fatalf("the refusal does not name the cause: %s", rr.Body.String())
	}
}
