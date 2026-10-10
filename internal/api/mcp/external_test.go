package mcp_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/api/mcp"
	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
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

// newExternalServer is newServer for a server whose vectors come from the
// caller, holding one memory put there through the service, since no MCP tool
// can write one.
func newExternalServer(t *testing.T) (http.Handler, string) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	keyring, err := auth.NewKeyring([]auth.Credential{{ID: "acme-agent", Secret: keyAcme, Tenant: "acme"}})
	if err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFake(clock.FakeStart)
	texts := text.New()
	svc := memory.New(kv, record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(texts)),
		flat.New(vector.NewStore(kv), distance.Cosine),
		embedding.NewExternal("cohere-embed-english-v3.0", 4), clk, attr.MustTable(), graph.NewService(kv, clk),
		memory.Config{Metric: distance.Cosine},
		memory.WithTextIndex(texts), memory.WithRecall(events.NewStore(kv), time.Minute))
	t.Cleanup(func() { _ = svc.Close() })

	m, err := svc.Create(tenant.NewContext(t.Context(), "acme"),
		memory.CreateReq{Content: "the invoice INV-2024-8871 was paid", Embedding: []float32{1, 0, 0, 0}})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	return remhttp.NewRouter(remhttp.Deps{
		Memories:      svc,
		Tenants:       tenantkv.New(kv, clk),
		Resolver:      tenant.Resolver{Capability: tenant.IdentityScoped, Default: "default"},
		Auth:          keyring,
		Metrics:       obs.NewMetrics(),
		Clock:         clk,
		AutoProvision: true,
		MCP:           mcp.NewHandler(mcp.Deps{Memories: svc, Sessions: session.New(kv, clk, 0)}),
	}), m.ID.String()
}

func TestMCPSaysWhyItCannotEmbedOnAnExternalServer(t *testing.T) {
	srv, memID := newExternalServer(t)
	sid := initialise(t, srv, keyAcme)

	for _, tc := range []struct {
		tool string
		args map[string]any
	}{
		{mcp.ToolStoreMemory, map[string]any{"content": "a new fact"}},
		{mcp.ToolStoreMemories, map[string]any{"memories": []map[string]any{{"content": "a new fact"}}}},
		{mcp.ToolUpdateMemory, map[string]any{"id": memID, "content": "a corrected fact"}},
		{mcp.ToolSearchMemories, map[string]any{"query": "invoice", "search_type": "semantic"}},
		{mcp.ToolSearchMemories, map[string]any{"query": "invoice", "search_type": "hybrid"}},
		{mcp.ToolRecall, map[string]any{"context": "invoice", "search_type": "hybrid", "token_budget": 500}},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			msg := callToolExpectingError(t, srv, sid, tc.tool, tc.args)
			if strings.Contains(msg, "could not complete") {
				t.Fatalf("the refusal reads as a server failure: %q", msg)
			}
			for _, w := range []string{"external", "vector", "no vector argument"} {
				if !strings.Contains(msg, w) {
					t.Errorf("refusal %q does not say %q", msg, w)
				}
			}
		})
	}
}

func TestMCPStillServesWhatNeedsNoVectorOnAnExternalServer(t *testing.T) {
	srv, memID := newExternalServer(t)
	sid := initialise(t, srv, keyAcme)

	callTool(t, srv, sid, mcp.ToolSearchMemories, map[string]any{"query": "INV-2024-8871", "search_type": "keyword"})
	callTool(t, srv, sid, mcp.ToolListMemories, map[string]any{})
	callTool(t, srv, sid, mcp.ToolGetMemory, map[string]any{"id": memID})
	callTool(t, srv, sid, mcp.ToolUpdateMemory, map[string]any{"id": memID, "tags": []string{"paid"}})
}
