package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/api/mcp"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
)

const usersOperatorKey = "an-operator-key-long-enough-for-this-test"

// runUsersServer starts a server under capability with the given tenants
// provisioned, issues a token to one user of home, and returns the server's
// base URL and the token.
func runUsersServer(t *testing.T, capability tenant.Capability, home tenant.ID, others ...tenant.ID) (string, string) {
	base, tok, _ := runUsersServerFor(t, capability, home, others...)
	return base, tok
}

// runUsersServerFor is runUsersServer that also returns the user's id.
func runUsersServerFor(t *testing.T, capability tenant.Capability, home tenant.ID, others ...tenant.ID) (string, string, id.ID) {
	t.Helper()
	cfg := tenancyConfig(t)
	cfg.Server.APIKey = usersOperatorKey
	srv, err := New(cfg, WithEmbedder(embeddingtest.New()), WithClock(clock.System()),
		WithTenantCapability(capability))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, id := range append([]tenant.ID{home}, others...) {
		if _, err := tenant.Ensure(tenant.NewContext(ctx, id), srv.deps.allTenants, id); err != nil {
			t.Fatal(err)
		}
	}
	u, err := srv.deps.users.Create(tenant.NewContext(ctx, home), "planner")
	if err != nil {
		t.Fatal(err)
	}
	issued, err := srv.deps.users.IssueToken(tenant.NewContext(ctx, home), u.ID, nil)
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- srv.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for !srv.Ready() || srv.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("the server never became ready")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return "http://" + srv.Addr(), issued.Secret, u.ID
}

func call(t *testing.T, method, url, bearer, tenantHeader, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	if tenantHeader != "" {
		req.Header.Set("X-Remem-Tenant", tenantHeader)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// A user token is served in its user's tenant and no header moves it. Under
// identity tenancy the bound tenant wins over a header; a single-tenant build
// refuses any header outright. Either way nothing lands in the tenant named.
func TestAUserTokenIsServedInItsUsersTenant(t *testing.T) {
	t.Run("identity", func(t *testing.T) {
		base, tok := runUsersServer(t, tenant.IdentityScoped, "acme", "beta")
		code, body := call(t, "POST", base+"/api/v1/memories", tok, "beta", `{"content":"written by planner"}`)
		if code != http.StatusCreated {
			t.Fatalf("store with a user token: %d %s", code, body)
		}
		_, inAcme := call(t, "GET", base+"/api/v1/memories", usersOperatorKey, "acme", "")
		_, inBeta := call(t, "GET", base+"/api/v1/memories", usersOperatorKey, "beta", "")
		if !strings.Contains(inAcme, "written by planner") || strings.Contains(inBeta, "written by planner") {
			t.Fatalf("the memory is in the wrong tenant:\nacme %s\nbeta %s", inAcme, inBeta)
		}
	})
	t.Run("single-tenant", func(t *testing.T) {
		base, tok := runUsersServer(t, tenant.SingleTenant, "default")
		if code, body := call(t, "POST", base+"/api/v1/memories", tok, "", `{"content":"written by planner"}`); code != http.StatusCreated {
			t.Fatalf("store with a user token: %d %s", code, body)
		}
		if code, _ := call(t, "POST", base+"/api/v1/memories", tok, "default", `{"content":"x"}`); code != http.StatusForbidden {
			t.Fatalf("a header on a single-tenant build: %d, want 403", code)
		}
	})
}

// A token that does not verify is refused with the server's ordinary challenge,
// and a malformed one is not sent to the configured keys instead.
func TestAnUnknownUserTokenIsUnauthorized(t *testing.T) {
	base, _ := runUsersServer(t, tenant.SingleTenant, "default")
	for _, tok := range []string{"rmm_aaaaaaaaaaaaaaaaaaaaaaaaaa_" + strings.Repeat("a", 52), "rmm_" + usersOperatorKey} {
		if code, _ := call(t, "GET", base+"/api/v1/memories", tok, "", ""); code != http.StatusUnauthorized {
			t.Fatalf("%q: %d, want 401", tok, code)
		}
	}
}

// What a user does through its token is attributed to it in the memory's
// history; what the operator credential does stays "api". A recall is a fetch
// by id, and an update is a PATCH.
func TestHistoryNamesTheUserWhoActed(t *testing.T) {
	base, tok, uid := runUsersServerFor(t, tenant.SingleTenant, "default")
	code, body := call(t, "POST", base+"/api/v1/memories", usersOperatorKey, "", `{"content":"the build runs on ci"}`)
	if code != http.StatusCreated {
		t.Fatalf("store: %d %s", code, body)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil || created.ID == "" {
		t.Fatalf("store returned %s", body)
	}
	mem := base + "/api/v1/memories/" + created.ID

	if code, body := call(t, "GET", mem, tok, "", ""); code != http.StatusOK {
		t.Fatalf("recall: %d %s", code, body)
	}
	if code, body := call(t, "PATCH", mem, tok, "", `{"content":"the build runs on ci and nightly"}`); code != http.StatusOK {
		t.Fatalf("update: %d %s", code, body)
	}
	_, hist := call(t, "GET", mem+"/history", usersOperatorKey, "", "")
	var h struct {
		Events []struct{ Kind, Actor string } `json:"entries"`
	}
	if err := json.Unmarshal([]byte(hist), &h); err != nil {
		t.Fatalf("history: %v %s", err, hist)
	}
	want := map[string]string{"recalled": "user:" + uid.String(), "updated": "user:" + uid.String()}
	for _, e := range h.Events {
		if w, ok := want[e.Kind]; ok {
			if e.Actor != w {
				t.Fatalf("the %s event's actor is %q, want %q", e.Kind, e.Actor, w)
			}
			delete(want, e.Kind)
		}
	}
	if len(want) != 0 {
		t.Fatalf("history is missing %v: %s", want, hist)
	}

	// The operator's own recall stays unattributed. A second fetch by the
	// operator falls inside the recall window, so it is checked on a new memory.
	_, body = call(t, "POST", base+"/api/v1/memories", usersOperatorKey, "", `{"content":"a second memory"}`)
	_ = json.Unmarshal([]byte(body), &created)
	call(t, "GET", base+"/api/v1/memories/"+created.ID, usersOperatorKey, "", "")
	_, hist = call(t, "GET", base+"/api/v1/memories/"+created.ID+"/history", usersOperatorKey, "", "")
	if !strings.Contains(hist, `"actor":"api"`) || strings.Contains(hist, "user:") {
		t.Fatalf("the operator's recall is not attributed to api: %s", hist)
	}
}

// mcpCall posts one JSON-RPC message to /mcp and returns the session id the
// response carries and the body.
func mcpCall(t *testing.T, base, bearer, sid, body string) (string, string) {
	t.Helper()
	req, err := http.NewRequest("POST", base+"/mcp", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	if sid != "" {
		req.Header.Set(mcp.SessionHeader, sid)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/mcp answered %d: %s", resp.StatusCode, b)
	}
	return resp.Header.Get(mcp.SessionHeader), string(b)
}

func mcpTool(t *testing.T, base, bearer, sid, name string, args map[string]any) string {
	t.Helper()
	msg, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 2, "method": "tools/call",
		"params": map[string]any{"name": name, "arguments": args},
	})
	_, body := mcpCall(t, base, bearer, sid, string(msg))
	if strings.Contains(body, `"isError":true`) || strings.Contains(body, `"error"`) {
		t.Fatalf("%s failed: %s", name, body)
	}
	return body
}

// An agent on MCP is attributed exactly as one on HTTP: the same verifier
// authenticates every /mcp request, so update_memory and get_memory — which
// registers a recall — name the user in the memory's history.
func TestMCPToolCallsAreAttributedToTheUser(t *testing.T) {
	base, tok, uid := runUsersServerFor(t, tenant.SingleTenant, "default")
	code, body := call(t, "POST", base+"/api/v1/memories", usersOperatorKey, "", `{"content":"deploys go out on thursday"}`)
	if code != http.StatusCreated {
		t.Fatalf("store: %d %s", code, body)
	}
	var created struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal([]byte(body), &created)

	sid, _ := mcpCall(t, base, tok, "", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{
		"protocolVersion":"2025-06-18","clientInfo":{"name":"test","version":"1"}}}`)
	if sid == "" {
		t.Fatal("initialize returned no session")
	}
	mcpTool(t, base, tok, sid, "get_memory", map[string]any{"id": created.ID})
	mcpTool(t, base, tok, sid, "update_memory", map[string]any{"id": created.ID, "content": "deploys go out on thursday and friday"})

	_, hist := call(t, "GET", base+"/api/v1/memories/"+created.ID+"/history", usersOperatorKey, "", "")
	want := "user:" + uid.String()
	for _, kind := range []string{"recalled", "updated"} {
		if !strings.Contains(hist, `"kind":"`+kind+`","actor":"`+want+`"`) {
			t.Fatalf("the %s event is not attributed to %s: %s", kind, want, hist)
		}
	}
}

// The routes are wired to the store the verifier reads: a token the operator
// issues over HTTP authenticates on the next request, and stops on the request
// after its revocation.
func TestATokenIssuedOverHTTPAuthenticates(t *testing.T) {
	base, _ := runUsersServer(t, tenant.SingleTenant, "default")
	code, body := call(t, "POST", base+"/api/v1/tenants/default/users", usersOperatorKey, "", `{"name":"researcher"}`)
	if code != http.StatusCreated {
		t.Fatalf("create: %d %s", code, body)
	}
	var u struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal([]byte(body), &u)
	code, body = call(t, "POST", base+"/api/v1/tenants/default/users/"+u.ID+"/tokens", usersOperatorKey, "", `{}`)
	if code != http.StatusCreated {
		t.Fatalf("issue: %d %s", code, body)
	}
	var issued struct {
		Token struct {
			ID string `json:"id"`
		} `json:"token"`
		Secret string `json:"secret"`
	}
	_ = json.Unmarshal([]byte(body), &issued)

	if code, body := call(t, "GET", base+"/api/v1/memories", issued.Secret, "", ""); code != http.StatusOK {
		t.Fatalf("the issued token: %d %s", code, body)
	}
	if code, body := call(t, "DELETE", base+"/api/v1/tenants/default/users/"+u.ID+"/tokens/"+issued.Token.ID, usersOperatorKey, "", ""); code != http.StatusOK {
		t.Fatalf("revoke: %d %s", code, body)
	}
	if code, _ := call(t, "GET", base+"/api/v1/memories", issued.Secret, "", ""); code != http.StatusUnauthorized {
		t.Fatalf("the revoked token: %d, want 401", code)
	}
}
