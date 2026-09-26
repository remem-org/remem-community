package mcp_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/api/mcp"
)

// Removing schema inputs prevents agents from expressing the corresponding
// traversal; the integration tests below exercise their delivery to the service.
func TestFindRelatedSchemaExposesTraversalAndPagination(t *testing.T) {
	for _, tool := range mcp.Tools() {
		if tool.Name != "find_related" {
			continue
		}
		for name, typ := range map[string]string{
			"id": "string", "depth": "integer", "types": "array", "direction": "string",
			"min_strength": "number", "limit": "integer", "cursor": "string", "include_archived": "boolean",
		} {
			if p, ok := tool.InputSchema.Properties[name]; !ok || p.Type != typ {
				t.Errorf("%s missing or wrong schema type: %+v", name, p)
			}
		}
		if !reflect.DeepEqual(tool.InputSchema.Required, []string{"id"}) {
			t.Fatalf("required = %v", tool.InputSchema.Required)
		}
		return
	}
	t.Fatal("find_related is not advertised")
}

// Dropping depth, types, direction, strength or archive filtering changes the
// concrete reached set. Dropping cursor repeats the first page.
func TestFindRelatedForwardsTraversalAndPaginates(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	store := func(content string) string {
		return callTool(t, srv, sid, "store_memory", map[string]any{"content": content})["id"].(string)
	}
	anchor, hub, far := store("anchor"), store("hub"), store("far")
	weak, wrong := store("weak"), store("wrong type")
	connectForRelated(t, srv, hub, anchor, "supports", 0.9)
	connectForRelated(t, srv, far, hub, "supports", 0.9)
	connectForRelated(t, srv, weak, anchor, "supports", 0.1)
	connectForRelated(t, srv, wrong, anchor, "contradicts", 1)
	callTool(t, srv, sid, "delete_memory", map[string]any{"id": hub})
	args := map[string]any{
		"id": anchor, "depth": 2, "types": []string{"supports"}, "direction": "in",
		"min_strength": 0.5, "include_archived": true, "limit": 1,
	}
	first := callTool(t, srv, sid, "find_related", args)
	hits := first["memories"].([]any)
	if len(hits) != 1 || hits[0].(map[string]any)["memory"].(map[string]any)["id"] != hub || first["has_more"] != true || first["truncated"] != false {
		t.Fatalf("first related page = %v", first)
	}
	cursor, ok := first["next_cursor"].(string)
	if !ok || cursor == "" {
		t.Fatal("first page omitted next_cursor")
	}
	args["cursor"] = cursor
	last := callTool(t, srv, sid, "find_related", args)
	hits = last["memories"].([]any)
	if len(hits) != 1 || hits[0].(map[string]any)["memory"].(map[string]any)["id"] != far || last["has_more"] != false || last["truncated"] != false {
		t.Fatalf("last related page = %v", last)
	}
	if _, ok := last["next_cursor"]; ok {
		t.Fatal("exhausted page included next_cursor")
	}
	if score := hits[0].(map[string]any)["score"].(float64); score < 0.809 || score > 0.811 {
		t.Fatalf("two-hop score = %v, want 0.81", score)
	}
	delete(args, "cursor")
	delete(args, "include_archived")
	onlyLive := callTool(t, srv, sid, "find_related", args)
	if hits := onlyLive["memories"].([]any); len(hits) != 1 || hits[0].(map[string]any)["memory"].(map[string]any)["id"] != far || onlyLive["has_more"] != false {
		t.Fatalf("default archive filter did not cross the archived hub: %v", onlyLive)
	}
	args["depth"] = 1
	empty := callTool(t, srv, sid, "find_related", args)
	if hits := empty["memories"].([]any); len(hits) != 0 || empty["has_more"] != false {
		t.Fatalf("one-hop filtered result = %v", empty)
	}
}

func TestFindRelatedRejectsMalformedIDAndCursor(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	callToolExpectingError(t, srv, sid, "find_related", map[string]any{"id": "not-a-uuid"})
	anchor := callTool(t, srv, sid, "store_memory", map[string]any{"content": "anchor"})["id"]
	callToolExpectingError(t, srv, sid, "find_related", map[string]any{"id": anchor, "cursor": "malformed"})
}

func connectForRelated(t *testing.T, srv http.Handler, from, to, typ string, strength float32) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"target_id": to, "relationship_type": typ, "strength": strength})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/api/v1/memories/"+from+"/connections", strings.NewReader(string(body)))
	r.Header.Set("Authorization", "Bearer "+keyAcme)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	if rr.Code != http.StatusCreated {
		t.Fatalf("connect returned %d: %s", rr.Code, rr.Body.String())
	}
}

// include_connections returned the first page of ten and said nothing about
// the rest: over the real binary a memory with thirty-one relationships
// reported ten, which a model reads as "this memory has ten relationships".
// Found by the Task 4.2 end-to-end run. Rust returns every outgoing
// connection; this returns them up to a stated bound and says when it stopped.
func TestGetMemoryReturnsEveryConnectionOrSaysSo(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	anchor := callTool(t, srv, sid, "store_memory", map[string]any{"content": "anchor"})["id"].(string)
	connect := func(n int) {
		for i := 0; i < n; i++ {
			target := callTool(t, srv, sid, "store_memory", map[string]any{"content": "target"})["id"].(string)
			connectForRelated(t, srv, anchor, target, "supports", 0.5)
		}
	}

	connect(12)
	got := callTool(t, srv, sid, "get_memory", map[string]any{"id": anchor, "include_connections": true})
	if edges := got["connections"].([]any); len(edges) != 12 || got["connections_has_more"] != false {
		t.Fatalf("twelve connections: got %d, connections_has_more %v", len(edges), got["connections_has_more"])
	}

	connect(189)
	got = callTool(t, srv, sid, "get_memory", map[string]any{"id": anchor, "include_connections": true})
	if edges := got["connections"].([]any); len(edges) != 200 || got["connections_has_more"] != true {
		t.Fatalf("201 connections: got %d, connections_has_more %v", len(edges), got["connections_has_more"])
	}
}
