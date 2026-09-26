package mcp_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/api/mcp"
	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/session"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

const (
	keyAcme  = "key-acme"
	keyOther = "key-other"
)

// noSession and withSession spell out what a request carries, so the two
// failure modes below read as the different things they are.
const noSession = ""

func withSession(id string) string { return id }

func TestMain(m *testing.M) {
	obs.SetDefault(obs.LoggerTo(io.Discard))
	os.Exit(m.Run())
}

func newServer(t *testing.T) http.Handler {
	t.Helper()
	return newServerWithSetup(t, nil)
}

func newServerWithSetup(t *testing.T, setup func(storage.KV, record.Repo, *clock.Fake)) http.Handler {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	keyring, err := auth.NewKeyring([]auth.Credential{
		{ID: "acme-agent", Secret: keyAcme, Tenant: "acme"},
		{ID: "other-agent", Secret: keyOther, Tenant: "other"},
	})
	if err != nil {
		t.Fatal(err)
	}

	clk := clock.NewFake(clock.FakeStart)
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(text.New()))
	if setup != nil {
		setup(kv, repo, clk)
	}
	svc := memory.New(kv, repo,
		flat.New(vector.NewStore(kv), distance.L2),
		embeddingtest.New(), clk, attr.MustTable(), graph.NewService(kv, clk), memory.Config{},
		memory.WithTextIndex(text.New()), memory.WithRecall(events.NewStore(kv), time.Minute))
	t.Cleanup(func() { _ = svc.Close() })

	return remhttp.NewRouter(remhttp.Deps{
		Memories:      svc,
		Tenants:       tenantkv.New(kv, clk),
		Resolver:      tenant.Resolver{Capability: tenant.IdentityScoped, Default: "default"},
		Auth:          keyring,
		Metrics:       obs.NewMetrics(),
		Clock:         clk,
		AutoProvision: true,
		MCP: mcp.NewHandler(mcp.Deps{
			Memories: svc,
			Sessions: session.New(kv, clk, 0),
		}),
	})
}

// post sends one JSON-RPC message.
func post(t *testing.T, srv http.Handler, body string, sessionID string) *httptest.ResponseRecorder {
	t.Helper()
	return postAs(t, srv, keyAcme, body, sessionID)
}

func postAs(t *testing.T, srv http.Handler, key, body, sessionID string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("POST", "/mcp", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	if sessionID != "" {
		r.Header.Set(mcp.SessionHeader, sessionID)
	}
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	return rr
}

// initialise performs the handshake and returns the session id.
func initialise(t *testing.T, srv http.Handler, key string) string {
	t.Helper()
	rr := postAs(t, srv, key, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{
		"protocolVersion":"2025-06-18","clientInfo":{"name":"test","version":"1"}}}`, noSession)
	if rr.Code != 200 {
		t.Fatalf("initialize returned %d: %s", rr.Code, rr.Body.String())
	}
	sid := rr.Header().Get(mcp.SessionHeader)
	if sid == "" {
		t.Fatal("initialize returned no session id")
	}
	return sid
}

func rpc(t *testing.T, rr *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil {
		t.Fatalf("response is not JSON (%d): %s", rr.Code, rr.Body.String())
	}
	return m
}

// toolsCall builds a tools/call message.
func toolsCall(name string, args map[string]any) string {
	body := map[string]any{
		"jsonrpc": "2.0", "id": 2, "method": "tools/call",
		"params": map[string]any{"name": name, "arguments": args},
	}
	b, _ := json.Marshal(body)
	return string(b)
}

// callTool runs a tool and returns its decoded payload.
func callTool(t *testing.T, srv http.Handler, sid, name string, args map[string]any) map[string]any {
	t.Helper()
	rr := post(t, srv, toolsCall(name, args), sid)
	if rr.Code != 200 {
		t.Fatalf("%s returned %d: %s", name, rr.Code, rr.Body.String())
	}
	res := rpc(t, rr)
	if e, ok := res["error"]; ok {
		t.Fatalf("%s failed: %v", name, e)
	}
	result := res["result"].(map[string]any)
	if isErr, _ := result["isError"].(bool); isErr {
		t.Fatalf("%s reported a tool error: %v", name, result["content"])
	}
	content := result["content"].([]any)[0].(map[string]any)
	var payload map[string]any
	if err := json.Unmarshal([]byte(content["text"].(string)), &payload); err != nil {
		t.Fatalf("the tool's answer is not JSON: %v", content["text"])
	}
	return payload
}

// --- the plan's three tests -------------------------------------------------

func TestMissingSessionIsFourHundredAndUnknownSessionIsFourOhFour(t *testing.T) {
	srv := newServer(t)

	// The two mean different things: 400 is "you never initialised",
	// 404 is "re-initialise". Serving 400 for an expired session strands
	// every client that idled past the sweep.
	rr := post(t, srv, toolsCall("get_memory", nil), noSession)
	if rr.Code != 400 {
		t.Fatalf("no session id should be 400, got %d: %s", rr.Code, rr.Body.String())
	}

	rr = post(t, srv, toolsCall("get_memory", nil), withSession("00000000-0000-0000-0000-000000000000"))
	if rr.Code != 404 {
		t.Fatalf("unknown session should be 404, got %d: %s", rr.Code, rr.Body.String())
	}
}

// TestToolSchemasAreValidJSONSchema checks that every inputSchema is one a
// client can compile: it describes an object, every required name is defined,
// and every type is one JSON Schema recognises.
//
// It is a structural check rather than a validator dependency, and it catches
// what actually goes wrong here — a required field renamed in the struct but
// not in the Required list, which turns a mandatory argument into an optional
// one without failing anything.
func TestToolSchemasAreValidJSONSchema(t *testing.T) {
	valid := map[string]bool{
		"string": true, "number": true, "integer": true,
		"boolean": true, "object": true, "array": true, "null": true,
	}

	// The count is asserted rather than merely iterated, so that a tool added
	// without a schema review fails here. Phase 3 shipped five; Phase 10 adds
	// recall and memory_history; Phase 13 adds update_memory, find_related and
	// list_memories.
	tools := mcp.Tools()
	if len(tools) != 10 {
		t.Fatalf("this build ships ten tools, found %d", len(tools))
	}

	seen := map[string]bool{}
	for _, tool := range tools {
		t.Run(tool.Name, func(t *testing.T) {
			if seen[tool.Name] {
				t.Fatal("two tools share a name")
			}
			seen[tool.Name] = true
			if tool.Description == "" {
				t.Error("a tool with no description is one a model will not use correctly")
			}
			if tool.InputSchema.Type != "object" {
				t.Fatalf("inputSchema type is %q, want object", tool.InputSchema.Type)
			}
			if len(tool.InputSchema.Properties) == 0 {
				t.Fatal("inputSchema defines no properties")
			}
			for name, p := range tool.InputSchema.Properties {
				if !valid[p.Type] {
					t.Errorf("property %q has type %q", name, p.Type)
				}
				if p.Description == "" {
					t.Errorf("property %q has no description", name)
				}
				if p.Type == "array" && p.Items == nil {
					t.Errorf("array property %q does not say what it holds", name)
				}
			}
			for _, req := range tool.InputSchema.Required {
				if _, ok := tool.InputSchema.Properties[req]; !ok {
					t.Errorf("required property %q is not defined", req)
				}
			}
			// And the whole thing must survive a round trip, since that is how
			// a client receives it.
			b, err := json.Marshal(tool.InputSchema)
			if err != nil {
				t.Fatalf("the schema does not encode: %v", err)
			}
			var back map[string]any
			if err := json.Unmarshal(b, &back); err != nil {
				t.Fatalf("the encoded schema does not parse: %v", err)
			}
		})
	}
}

// --- the rest ---------------------------------------------------------------

func TestTheHandshake(t *testing.T) {
	srv := newServer(t)
	rr := postAs(t, srv, keyAcme, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{
		"protocolVersion":"2025-06-18","clientInfo":{"name":"claude","version":"1.0"}}}`, noSession)
	if rr.Code != 200 {
		t.Fatalf("got %d: %s", rr.Code, rr.Body.String())
	}
	res := rpc(t, rr)["result"].(map[string]any)
	if res["protocolVersion"] != mcp.ProtocolVersion {
		t.Fatalf("protocolVersion = %v", res["protocolVersion"])
	}
	caps := res["capabilities"].(map[string]any)
	if _, ok := caps["tools"]; !ok {
		t.Error("the server does not declare tool support")
	}
	if rr.Header().Get(mcp.SessionHeader) == "" {
		t.Error("no session id was issued")
	}
}

// A tool's own failure is a successful JSON-RPC response with isError set. A
// memory that does not exist is not a protocol fault, and reporting it as one
// makes a client treat it as "Remem is broken" rather than showing the model
// something it can act on.
func TestAToolFailureIsNotAProtocolError(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)

	rr := post(t, srv, toolsCall("get_memory",
		map[string]any{"id": "11111111-1111-4111-8111-111111111111"}), sid)
	if rr.Code != 200 {
		t.Fatalf("got %d", rr.Code)
	}
	res := rpc(t, rr)
	if _, isRPCError := res["error"]; isRPCError {
		t.Fatal("a missing memory was reported as a protocol error")
	}
	result := res["result"].(map[string]any)
	if isErr, _ := result["isError"].(bool); !isErr {
		t.Fatal("a missing memory was reported as a success")
	}
}

// An unknown tool *is* a protocol error: the client asked for something that
// does not exist, which is a different class from a call that ran and failed.
func TestAnUnknownToolIsAProtocolError(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	rr := post(t, srv, toolsCall("delete_everything", nil), sid)
	res := rpc(t, rr)
	e, ok := res["error"].(map[string]any)
	if !ok {
		t.Fatalf("no error object: %v", res)
	}
	if e["code"].(float64) != mcp.CodeMethodNotFound {
		t.Fatalf("code = %v", e["code"])
	}
}

func TestToolsList(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	rr := post(t, srv, `{"jsonrpc":"2.0","id":3,"method":"tools/list"}`, sid)
	res := rpc(t, rr)["result"].(map[string]any)
	tools := res["tools"].([]any)
	names := map[string]bool{}
	for _, tl := range tools {
		names[tl.(map[string]any)["name"].(string)] = true
	}
	for _, want := range []string{"store_memory", "store_memories", "get_memory", "delete_memory", "search_memories"} {
		if !names[want] {
			t.Errorf("tools/list omits %s", want)
		}
	}
}

func TestTheToolsRoundTrip(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)

	stored := callTool(t, srv, sid, "store_memory",
		map[string]any{"content": "the sky is blue", "tags": []string{"Weather"}})
	mID := stored["id"].(string)
	if tags := stored["tags"].([]any); len(tags) != 1 || tags[0] != "weather" {
		t.Fatalf("tags = %v", stored["tags"])
	}

	batch := callTool(t, srv, sid, "store_memories", map[string]any{
		"memories": []map[string]any{{"content": "one"}, {"content": "two"}},
	})
	if n := len(batch["memories"].([]any)); n != 2 {
		t.Fatalf("stored %d memories", n)
	}

	got := callTool(t, srv, sid, "get_memory", map[string]any{"id": mID})
	if got["content"] != "the sky is blue" {
		t.Fatalf("content = %v", got["content"])
	}

	found := callTool(t, srv, sid, "search_memories", map[string]any{"query": "the sky is blue", "search_type": "hybrid", "limit": 5})
	if len(found["results"].([]any)) == 0 {
		t.Fatal("search found nothing")
	}

	deleted := callTool(t, srv, sid, "delete_memory", map[string]any{"id": mID})
	if deleted["status"] != "archived" {
		t.Fatalf("status = %v; the default delete archives", deleted["status"])
	}
}

// The MCP surface goes through the same authentication and tenant resolution as
// REST. A client that could reach a tenant the REST API refuses would be an
// isolation hole with two doors.
func TestMCPIsTenantIsolated(t *testing.T) {
	srv := newServer(t)

	acmeSession := initialise(t, srv, keyAcme)
	stored := callTool(t, srv, acmeSession, "store_memory", map[string]any{"content": "acme's note"})
	mID := stored["id"].(string)

	otherSession := initialise(t, srv, keyOther)
	rr := postAs(t, srv, keyOther, toolsCall("get_memory", map[string]any{"id": mID}), otherSession)
	res := rpc(t, rr)["result"].(map[string]any)
	if isErr, _ := res["isError"].(bool); !isErr {
		t.Fatal("another tenant read the memory over MCP")
	}
}

// A session belongs to a tenant. One tenant's session id must not work as
// another's, even with that tenant's credential.
func TestASessionDoesNotCrossTenants(t *testing.T) {
	srv := newServer(t)
	acmeSession := initialise(t, srv, keyAcme)

	rr := postAs(t, srv, keyOther, `{"jsonrpc":"2.0","id":9,"method":"ping"}`, acmeSession)
	if rr.Code != 404 {
		t.Fatalf("acme's session resolved under another tenant's credential: %d", rr.Code)
	}
}

func TestMCPRequiresACredential(t *testing.T) {
	srv := newServer(t)
	rr := postAs(t, srv, "", `{"jsonrpc":"2.0","id":1,"method":"initialize"}`, noSession)
	if rr.Code != 401 {
		t.Fatalf("got %d, want 401", rr.Code)
	}
}

// A notification is answered with 202 and no body. It is handled before the
// session check so that notifications/initialized — sent immediately after the
// handshake — is not refused for a session the client is still establishing.
func TestANotificationIsAcceptedWithoutABody(t *testing.T) {
	srv := newServer(t)
	rr := post(t, srv, `{"jsonrpc":"2.0","method":"notifications/initialized"}`, noSession)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("got %d, want 202", rr.Code)
	}
	if rr.Body.Len() != 0 {
		t.Fatalf("a notification was answered with a body: %s", rr.Body.String())
	}
}

func TestPing(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	rr := post(t, srv, `{"jsonrpc":"2.0","id":7,"method":"ping"}`, sid)
	if _, ok := rpc(t, rr)["result"]; !ok {
		t.Fatalf("ping = %s", rr.Body.String())
	}
}

func TestAnUnknownMethodIsMethodNotFound(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	rr := post(t, srv, `{"jsonrpc":"2.0","id":8,"method":"prompts/list"}`, sid)
	e := rpc(t, rr)["error"].(map[string]any)
	if e["code"].(float64) != mcp.CodeMethodNotFound {
		t.Fatalf("code = %v", e["code"])
	}
}

func TestMalformedJSONIsAParseError(t *testing.T) {
	srv := newServer(t)
	rr := post(t, srv, `{"jsonrpc":`, noSession)
	e := rpc(t, rr)["error"].(map[string]any)
	if e["code"].(float64) != mcp.CodeParseError {
		t.Fatalf("code = %v", e["code"])
	}
}

func TestAMessageWithoutJSONRPCTwoIsRejected(t *testing.T) {
	srv := newServer(t)
	rr := post(t, srv, `{"id":1,"method":"ping"}`, noSession)
	e := rpc(t, rr)["error"].(map[string]any)
	if e["code"].(float64) != mcp.CodeInvalidRequest {
		t.Fatalf("code = %v", e["code"])
	}
}

// A model that guessed a field name and got a success has stored something
// other than what it meant to, and will not find out.
func TestUnknownToolArgumentsAreRefused(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	rr := post(t, srv, toolsCall("store_memory",
		map[string]any{"content": "fine", "contnet": "typo"}), sid)
	res := rpc(t, rr)["result"].(map[string]any)
	if isErr, _ := res["isError"].(bool); !isErr {
		t.Fatal("a misspelled argument was accepted")
	}
}

func TestResourcesListAndRead(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)

	rr := post(t, srv, `{"jsonrpc":"2.0","id":4,"method":"resources/list"}`, sid)
	res := rpc(t, rr)["result"].(map[string]any)
	list := res["resources"].([]any)
	if len(list) == 0 {
		t.Fatal("no resources")
	}
	uri := list[0].(map[string]any)["uri"].(string)

	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 5, "method": "resources/read",
		"params": map[string]any{"uri": uri},
	})
	rr = post(t, srv, string(body), sid)
	read := rpc(t, rr)["result"].(map[string]any)
	contents := read["contents"].([]any)
	if len(contents) == 0 || contents[0].(map[string]any)["text"] == "" {
		t.Fatalf("resource %s read empty", uri)
	}
}

func TestReadingAnUnknownResourceIsInvalidParams(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 6, "method": "resources/read",
		"params": map[string]any{"uri": "remem://nowhere"},
	})
	rr := post(t, srv, string(body), sid)
	e := rpc(t, rr)["error"].(map[string]any)
	if e["code"].(float64) != mcp.CodeInvalidParams {
		t.Fatalf("code = %v", e["code"])
	}
}

// Ending a session is idempotent: a client tidying up on shutdown should not
// have to handle a 404 for a session that has already expired.
func TestDeleteEndsTheSession(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)

	r := httptest.NewRequest("DELETE", "/mcp", nil)
	r.Header.Set("Authorization", "Bearer "+keyAcme)
	r.Header.Set(mcp.SessionHeader, sid)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("got %d", rr.Code)
	}

	if rr := post(t, srv, `{"jsonrpc":"2.0","id":1,"method":"ping"}`, sid); rr.Code != 404 {
		t.Fatalf("the ended session still works: %d", rr.Code)
	}

	// And ending it again is still a success.
	r = httptest.NewRequest("DELETE", "/mcp", nil)
	r.Header.Set("Authorization", "Bearer "+keyAcme)
	r.Header.Set(mcp.SessionHeader, sid)
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("ending an ended session returned %d", rr.Code)
	}
}

func TestAnUnsupportedMethodIsRejectedWithAnAllowHeader(t *testing.T) {
	srv := newServer(t)
	r := httptest.NewRequest("PUT", "/mcp", strings.NewReader("{}"))
	r.Header.Set("Authorization", "Bearer "+keyAcme)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("got %d", rr.Code)
	}
	if rr.Header().Get("Allow") == "" {
		t.Error("a 405 without Allow leaves a client guessing")
	}
}

// --- the stdio bridge -------------------------------------------------------

// --- update_memory ----------------------------------------------------------

// callToolExpectingError runs a tool that is meant to fail and returns the text
// the model is shown. A tool's own failure rides inside a successful JSON-RPC
// response with isError set, so this is not the same as a protocol error.
func callToolExpectingError(t *testing.T, srv http.Handler, sid, name string, args map[string]any) string {
	t.Helper()
	rr := post(t, srv, toolsCall(name, args), sid)
	if rr.Code != 200 {
		t.Fatalf("%s returned %d: %s", name, rr.Code, rr.Body.String())
	}
	res := rpc(t, rr)
	if e, ok := res["error"]; ok {
		t.Fatalf("%s failed as a protocol error rather than a tool error: %v", name, e)
	}
	result := res["result"].(map[string]any)
	if isErr, _ := result["isError"].(bool); !isErr {
		t.Fatalf("%s succeeded; it was expected to fail: %v", name, result["content"])
	}
	return result["content"].([]any)[0].(map[string]any)["text"].(string)
}
