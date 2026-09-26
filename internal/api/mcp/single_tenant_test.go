package mcp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

const (
	implicitTenant = tenant.ID("solo")
	keySolo        = "key-solo"
)

// An MCP surface served by a single-tenant build, over a store that also holds
// another tenant's memory. A tool has no tenant argument — the only selector is
// the header the shared middleware reads — so what this fixture proves is that
// there is no second door: the tools answer from the implicit tenant and the
// other tenant's memory is not reachable through any of them.
func newSingleTenantServer(t *testing.T) http.Handler {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	keyring, err := auth.NewKeyring([]auth.Credential{
		{ID: "solo-agent", Secret: keySolo, Tenant: implicitTenant},
	})
	if err != nil {
		t.Fatal(err)
	}

	clk := clock.NewFake(clock.FakeStart)
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(text.New()))
	svc := memory.New(kv, repo,
		flat.New(vector.NewStore(kv), distance.L2),
		embeddingtest.New(), clk, attr.MustTable(), graph.NewService(kv, clk), memory.Config{},
		memory.WithTextIndex(text.New()), memory.WithRecall(events.NewStore(kv), 0))
	t.Cleanup(func() { _ = svc.Close() })

	raw := tenantkv.New(kv, clk)
	for _, id := range []tenant.ID{implicitTenant, "elsewhere"} {
		if _, err := tenant.Ensure(tenant.NewContext(context.Background(), id), raw, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.Create(tenant.NewContext(context.Background(), "elsewhere"),
		memory.CreateReq{Content: "the marzipan recipe belongs to the other tenant"}); err != nil {
		t.Fatal(err)
	}

	return remhttp.NewRouter(remhttp.Deps{
		Memories:   svc,
		Tenants:    tenant.Confine(raw, implicitTenant),
		Resolver:   tenant.Resolver{Capability: tenant.SingleTenant, Default: implicitTenant},
		Capability: tenant.SingleTenant,
		Auth:       keyring,
		Metrics:    obs.NewMetrics(),
		Clock:      clk,
		MCP: mcp.NewHandler(mcp.Deps{
			Memories: svc,
			Sessions: session.New(kv, clk, 0),
		}),
	})
}

// callToolAs is callTool for a named credential. The shared helper sends the
// fixture's acme key, and this fixture's credential is bound to the implicit
// tenant instead.
func callToolAs(t *testing.T, srv http.Handler, key, sid, name string, args map[string]any) map[string]any {
	t.Helper()
	rr := postAs(t, srv, key, toolsCall(name, args), sid)
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

// Every tool answers from the implicit tenant, and none of them reaches the
// other tenant's memory. The list is every tool that reads the corpus, because
// "no second door" is a claim about all of them rather than about search.
func TestEveryToolAnswersFromTheImplicitTenant(t *testing.T) {
	srv := newSingleTenantServer(t)
	sid := initialise(t, srv, keySolo)

	stored := callToolAs(t, srv, keySolo, sid, "store_memory",
		map[string]any{"content": "the invoice was paid on Tuesday"})
	id, _ := stored["id"].(string)
	if id == "" {
		t.Fatalf("store_memory returned no id: %v", stored)
	}

	for _, tc := range []struct {
		name string
		tool string
		args map[string]any
	}{
		{"keyword search", "search_memories",
			map[string]any{"query": "marzipan", "search_type": "keyword", "limit": 10}},
		{"semantic search", "search_memories",
			map[string]any{"query": "marzipan recipe", "search_type": "semantic", "limit": 10}},
		{"hybrid search", "search_memories",
			map[string]any{"query": "marzipan recipe", "search_type": "hybrid", "limit": 10}},
		{"listing", "list_memories", map[string]any{"limit": 10}},
		{"fetch", "get_memory", map[string]any{"id": id, "include_connections": true}},
		{"related", "find_related", map[string]any{"id": id, "limit": 10}},
		{"recall", "recall",
			map[string]any{"context": "marzipan recipe", "search_type": "hybrid", "token_budget": 400}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := callToolAs(t, srv, keySolo, sid, tc.tool, tc.args)
			raw, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "marzipan") {
				t.Fatalf("%s reached the other tenant: %s", tc.tool, raw)
			}
		})
	}
}

// The header is the only tenant selector an MCP client has, and it is refused
// before a session even exists — which is the point of mounting /mcp behind the
// same middleware as the REST surface rather than authenticating it separately.
func TestTheTenantHeaderIsRefusedBeforeTheHandshake(t *testing.T) {
	srv := newSingleTenantServer(t)
	r := httptest.NewRequest("POST", "/mcp", strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{
			"protocolVersion":"2025-06-18","clientInfo":{"name":"test","version":"1"}}}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+keySolo)
	r.Header.Set(remhttp.TenantHeader, "elsewhere")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("initialize with a tenant header = %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "serves one tenant") {
		t.Fatalf("the refusal does not say why: %s", rr.Body.String())
	}
}

// The server-info resource reports the tenant a session is serving, so it must
// report the implicit one rather than anything a client asked for.
func TestServerInfoReportsTheImplicitTenant(t *testing.T) {
	srv := newSingleTenantServer(t)
	sid := initialise(t, srv, keySolo)

	rr := postAs(t, srv, keySolo, `{"jsonrpc":"2.0","id":3,"method":"resources/read",
		"params":{"uri":"remem://server/info"}}`, withSession(sid))
	if rr.Code != 200 {
		t.Fatalf("resources/read = %d: %s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, string(implicitTenant)) {
		t.Fatalf("server info does not name the implicit tenant: %s", body)
	}
	if strings.Contains(body, "elsewhere") {
		t.Fatalf("server info names another tenant: %s", body)
	}
}

// No tool takes a tenant argument, on any edition.
//
// This is the assertion behind the test above, and the one that can actually
// fail. A tool's arguments are the only place an MCP client could name a tenant
// other than through the header, and the header is refused by the shared
// middleware — so the tools are safe precisely as long as none of them grows an
// argument that selects one. A property called "tenant", "namespace" or
// "tenant_id" would move tenant selection into a schema nobody reviewed for it.
//
// It is not conditional on the capability. Under identity tenancy the tenant
// comes from the authenticated principal and is no more a tool argument than it
// is here: a selector in the arguments would bypass the authorization layer
// rather than feed it.
func TestNoToolTakesATenantArgument(t *testing.T) {
	forbidden := map[string]bool{
		"tenant": true, "tenant_id": true, "tenantId": true,
		"namespace": true, "ns": true, "org": true, "organisation": true, "organization": true,
	}
	for _, tool := range mcp.Tools() {
		for name := range tool.InputSchema.Properties {
			if forbidden[name] {
				t.Errorf("tool %s takes an argument %q.\n"+
					"A tenant is resolved from the credential and the edition's policy, never from a "+
					"tool argument: an argument that selected one would bypass the authorization layer "+
					"rather than feed it, and would reach a tenant the header is refused for.",
					tool.Name, name)
			}
		}
	}
}
