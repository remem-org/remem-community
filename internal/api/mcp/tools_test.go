package mcp_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// Each tool's behaviour through the protocol. Split from mcp_test.go in Phase
// 13 to keep that file under the project's size limit; the tests did not change.

func TestSearchResultsOmitSourcesUnlessExplainIsSet(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	callTool(t, srv, sid, "store_memory", map[string]any{"content": "the sky is blue"})

	plain := callTool(t, srv, sid, "search_memories", map[string]any{"query": "the sky is blue", "search_type": "semantic"})
	first := plain["results"].([]any)[0].(map[string]any)
	if _, ok := first["sources"]; ok {
		t.Error("the default search carries per-source evidence, which the model pays for on every call")
	}
	if _, ok := first["matched"]; !ok {
		t.Error("the default search drops the compact matched list too")
	}
	if _, ok := first["score"]; !ok {
		t.Error("the default search drops the score, which is the one number the model can act on")
	}

	explained := callTool(t, srv, sid, "search_memories",
		map[string]any{"query": "the sky is blue", "search_type": "semantic", "explain": true})
	first = explained["results"].([]any)[0].(map[string]any)
	sources, ok := first["sources"].([]any)
	if !ok || len(sources) == 0 {
		t.Fatal("explain:true did not produce the per-source breakdown")
	}
	src := sources[0].(map[string]any)
	for _, f := range []string{"source", "score", "fused_score", "distance"} {
		if _, ok := src[f]; !ok {
			t.Errorf("the breakdown is missing %q", f)
		}
	}
}

// Search is one capability through two doors. This reaches the MCP door all
// the way to memory.Service, so a schema property that is dropped while
// constructing SearchReq cannot claim it works.
func TestSearchToolForwardsFiltersAndPaginates(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)

	callTool(t, srv, sid, "store_memory", map[string]any{
		"content": "alpha wanted", "policy": "long_term", "importance": 0.9,
	})
	callTool(t, srv, sid, "store_memory", map[string]any{
		"content": "alpha low", "policy": "long_term", "importance": 0.1,
	})
	callTool(t, srv, sid, "store_memory", map[string]any{
		"content": "alpha short", "policy": "short_term", "importance": 0.9,
	})

	filtered := callTool(t, srv, sid, "search_memories", map[string]any{
		"query": "alpha", "search_type": "keyword", "policy": "long_term",
		"importance_min": 0.8, "importance_max": 1.0,
		"created_after": "2020-01-01T00:00:00Z", "created_before": "2030-01-01T00:00:00Z",
	})
	hits := filtered["results"].([]any)
	if len(hits) != 1 || hits[0].(map[string]any)["memory"].(map[string]any)["content"] != "alpha wanted" {
		t.Fatalf("conjoined filters returned %v, want only alpha wanted", hits)
	}
	upper := callTool(t, srv, sid, "search_memories", map[string]any{
		"query": "alpha", "search_type": "keyword", "policy": "long_term", "importance_max": 0.2,
	})
	upperHits := upper["results"].([]any)
	if len(upperHits) != 1 || upperHits[0].(map[string]any)["memory"].(map[string]any)["content"] != "alpha low" {
		t.Fatalf("importance_max returned %v, want only alpha low", upperHits)
	}
	for name, args := range map[string]map[string]any{
		"created_after":  {"query": "alpha", "search_type": "keyword", "created_after": "2030-01-01T00:00:00Z"},
		"created_before": {"query": "alpha", "search_type": "keyword", "created_before": "2020-01-01T00:00:00Z"},
	} {
		t.Run(name, func(t *testing.T) {
			if hits := callTool(t, srv, sid, "search_memories", args)["results"].([]any); len(hits) != 0 {
				t.Fatalf("%s returned %v, want no out-of-window memories", name, hits)
			}
		})
	}
	callToolExpectingError(t, srv, sid, "search_memories", map[string]any{
		"query": "alpha", "search_type": "keyword", "related_to": "11111111-1111-4111-8111-111111111111",
	})

	page := callTool(t, srv, sid, "search_memories", map[string]any{
		"query": "alpha", "search_type": "keyword", "limit": 1,
	})
	if more, ok := page["has_more"].(bool); !ok || !more {
		t.Fatalf("first page has_more = %v, want true", page["has_more"])
	}
	cursor, ok := page["next_cursor"].(string)
	if !ok || cursor == "" {
		t.Fatalf("first page next_cursor = %v, want a continuation token", page["next_cursor"])
	}
	firstID := page["results"].([]any)[0].(map[string]any)["memory"].(map[string]any)["id"]
	next := callTool(t, srv, sid, "search_memories", map[string]any{
		"query": "alpha", "search_type": "keyword", "limit": 1, "cursor": cursor,
	})
	nextID := next["results"].([]any)[0].(map[string]any)["memory"].(map[string]any)["id"]
	if nextID == firstID {
		t.Fatalf("cursor repeated the first hit %v", firstID)
	}
}

func TestSearchToolRejectsMalformedRelatedTo(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	callToolExpectingError(t, srv, sid, "search_memories", map[string]any{
		"query": "alpha", "search_type": "keyword", "related_to": "not-a-uuid",
	})
}

func TestUpdateMemoryToolChangesOnlyWhatItWasGiven(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)

	stored := callTool(t, srv, sid, "store_memory",
		map[string]any{"content": "the first version", "tags": []string{"alpha"}})
	rid := stored["id"].(string)

	got := callTool(t, srv, sid, "update_memory",
		map[string]any{"id": rid, "content": "the second version"})

	if c := got["content"]; c != "the second version" {
		t.Fatalf("content is %v", c)
	}
	tags, _ := got["tags"].([]any)
	if len(tags) != 1 || tags[0] != "alpha" {
		t.Fatalf("tags were dropped by an update that did not mention them: %v", got["tags"])
	}
}

// decodeArgs disallows unknown fields, so a model that guessed at `health`
// learns it guessed rather than getting a success for something it did not do.
func TestUpdateMemoryToolRefusesHealth(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	rid := callTool(t, srv, sid, "store_memory", map[string]any{"content": "anything"})["id"].(string)

	res := callToolExpectingError(t, srv, sid, "update_memory",
		map[string]any{"id": rid, "health": 100})
	if !strings.Contains(res, "health") {
		t.Fatalf("the refusal does not name the field: %s", res)
	}
}

func TestUpdateMemoryToolReportsAnArchivedMemory(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	rid := callTool(t, srv, sid, "store_memory", map[string]any{"content": "retire me"})["id"].(string)
	callTool(t, srv, sid, "delete_memory", map[string]any{"id": rid})

	res := callToolExpectingError(t, srv, sid, "update_memory",
		map[string]any{"id": rid, "content": "changed"})
	// A tool's own failure rides inside a successful JSON-RPC response and
	// must say something the model can act on — not "the server could not
	// complete this request", which is what a Storage-kind error becomes.
	if !strings.Contains(res, "archived") {
		t.Fatalf("the model is not told why: %s", res)
	}
}

// The same refusal on a tool that has shipped since Phase 3. A bad argument is
// the caller's mistake and has to read like one: "the server could not complete
// this request" tells a model that Remem is broken, which is the one conclusion
// that makes it stop trying rather than fix its call.
func TestABadArgumentNamesItselfOnEveryTool(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)

	res := callToolExpectingError(t, srv, sid, "store_memory",
		map[string]any{"content": "anything", "contnet": "a typo"})
	if !strings.Contains(res, "contnet") {
		t.Fatalf("the refusal does not name the field: %s", res)
	}
}

// Ignoring order_by, desc, cursor or the archive filter changes these pages.
// Each indexed slot has its own permutation, so a hardcoded ordering cannot pass.
func TestListMemoriesOrdersAndPaginates(t *testing.T) {
	srv := newServerWithSetup(t, func(kv storage.KV, repo record.Repo, clk *clock.Fake) {
		ctx := tenant.NewContext(context.Background(), "acme")
		for i, content := range []string{"a", "b", "c", "archived"} {
			r := &record.Record{
				ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
				Type: record.TypeMemory, Content: content,
				CreatedAt: clk.Now().Add(time.Duration(i) * time.Second),
				UpdatedAt: clk.Now().Add(time.Duration([]int{1, 2, 0, 3}[i]) * time.Second),
				Fields: record.Fields{
					Policy: record.DefaultPolicy, Importance: []float32{0.3, 0.1, 0.2, 0.4}[i],
					Health: []float32{20, 10, 30, 40}[i], Archived: i == 3,
					LastRecalledAt: clk.Now().Add(time.Duration([]int{2, 1, 0, 3}[i]) * time.Second),
				},
			}
			tx := txn.New(kv)
			if err := repo.Put(ctx, tx, r); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			tx.Close()
		}
	})
	sid := initialise(t, srv, keyAcme)
	for _, tt := range []struct {
		order string
		want  []string
	}{
		{"created_at", []string{"a", "b", "c"}},
		{"updated_at", []string{"c", "a", "b"}},
		{"importance", []string{"b", "c", "a"}},
		{"health", []string{"b", "a", "c"}},
		{"last_recalled_at", []string{"c", "b", "a"}},
	} {
		for _, desc := range []bool{false, true} {
			t.Run(tt.order+map[bool]string{false: "/asc", true: "/desc"}[desc], func(t *testing.T) {
				args := map[string]any{"order_by": tt.order, "desc": desc, "limit": 2}
				var got []string
				for i := 0; i < 2; i++ {
					page := callTool(t, srv, sid, "list_memories", args)
					rows := page["memories"].([]any)
					if len(rows) != 2-i || page["truncated"] != false || page["has_more"] != (i == 0) {
						t.Fatalf("page %d: %v", i, page)
					}
					for _, row := range rows {
						got = append(got, row.(map[string]any)["content"].(string))
					}
					if i == 0 {
						cursor, ok := page["next_cursor"].(string)
						if !ok || cursor == "" {
							t.Fatal("missing cursor")
						}
						args["cursor"] = cursor
					} else if _, ok := page["next_cursor"]; ok {
						t.Fatal("exhausted page has a cursor")
					}
				}
				want := append([]string(nil), tt.want...)
				if desc {
					want[0], want[2] = want[2], want[0]
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %v, want %v", got, want)
				}
			})
		}
	}
	page := callTool(t, srv, sid, "list_memories", nil)
	if rows := page["memories"].([]any); len(rows) != 3 || rows[0].(map[string]any)["content"] != "c" {
		t.Fatalf("default listing: %v", page)
	}
	page = callTool(t, srv, sid, "list_memories", map[string]any{"include_archived": true})
	if rows := page["memories"].([]any); len(rows) != 4 || rows[0].(map[string]any)["archived"] != true {
		t.Fatalf("archive inclusion: %v", page)
	}
	for _, args := range []map[string]any{{"order_by": "content"}, {"limit": 201}, {"cursor": "bad"}} {
		callToolExpectingError(t, srv, sid, "list_memories", args)
	}
	other := initialise(t, srv, keyOther)
	rr := postAs(t, srv, keyOther, toolsCall("list_memories", nil), other)
	result := rpc(t, rr)["result"].(map[string]any)
	var empty map[string]any
	if err := json.Unmarshal([]byte(result["content"].([]any)[0].(map[string]any)["text"].(string)), &empty); err != nil {
		t.Fatal(err)
	}
	if rows, ok := empty["memories"].([]any); !ok || len(rows) != 0 {
		t.Fatalf("other tenant: %v", empty)
	}
	first := callTool(t, srv, sid, "list_memories", map[string]any{"limit": 1})
	rr = postAs(t, srv, keyOther, toolsCall("list_memories", map[string]any{"limit": 1, "cursor": first["next_cursor"]}), other)
	if rpc(t, rr)["result"].(map[string]any)["isError"] != true {
		t.Fatalf("foreign cursor accepted: %s", rr.Body.String())
	}
}

// Ignoring include_connections must fail on an actual edge, and false must not
// inflate the ordinary memory payload. Outgoing only, as Rust's
// fetch_connections is, and in a wire shape of its own: memory.Connection is a
// domain type, and a field added to it must not silently join the payload.
func TestGetMemoryIncludesConnectionsOnlyWhenAsked(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	anchor := callTool(t, srv, sid, "store_memory", map[string]any{"content": "anchor"})["id"].(string)
	target := callTool(t, srv, sid, "store_memory", map[string]any{"content": "target"})["id"].(string)
	connectForRelated(t, srv, anchor, target, "supports", 0.75)
	plain := callTool(t, srv, sid, "get_memory", map[string]any{"id": anchor})
	if _, ok := plain["connections"]; ok {
		t.Fatal("ordinary get includes connections")
	}
	got := callTool(t, srv, sid, "get_memory", map[string]any{"id": anchor, "include_connections": true})
	edges, ok := got["connections"].([]any)
	if !ok || len(edges) != 1 {
		t.Fatalf("connections: %v", got)
	}
	edge := edges[0].(map[string]any)
	if edge["from"] != anchor || edge["to"] != target || edge["relationship_type"] != "supports" || edge["strength"] != 0.75 {
		t.Fatalf("edge: %v", edge)
	}
	if _, leaked := edge["From"]; leaked || got["connections_has_more"] != false {
		t.Fatalf("wire shape: %v", got)
	}
	empty := callTool(t, srv, sid, "get_memory", map[string]any{"id": target, "include_connections": true})
	if edges, ok := empty["connections"].([]any); !ok || len(edges) != 0 {
		t.Fatalf("empty out-edges: %v", empty)
	}
	callTool(t, srv, sid, "delete_memory", map[string]any{"id": anchor})
	callToolExpectingError(t, srv, sid, "get_memory", map[string]any{"id": anchor, "include_connections": true})
	got = callTool(t, srv, sid, "get_memory", map[string]any{"id": anchor, "include_connections": true, "include_archived": true})
	if len(got["connections"].([]any)) != 1 || got["archived"] != true {
		t.Fatalf("archived get: %v", got)
	}
}

// Dropping already_have resends the held memory; dropping the budget includes
// a memory the caller has no room for.
func TestRecallToolHonorsBudgetAndAlreadyHave(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	rid := callTool(t, srv, sid, "store_memory", map[string]any{"content": "quartz"})["id"]
	args := map[string]any{"context": "quartz", "search_type": "keyword", "token_budget": 100}
	got := callTool(t, srv, sid, "recall", args)
	if rows := got["memories"].([]any); len(rows) != 1 || rows[0].(map[string]any)["id"] != rid || got["used_tokens"] != float64(18) || got["omitted_count"] != float64(0) || got["truncated"] != false {
		t.Fatalf("recall: %v", got)
	}
	args["already_have"] = []any{rid}
	got = callTool(t, srv, sid, "recall", args)
	if len(got["memories"].([]any)) != 0 || got["used_tokens"] != float64(0) {
		t.Fatalf("already held: %v", got)
	}
	delete(args, "already_have")
	args["token_budget"] = 1
	got = callTool(t, srv, sid, "recall", args)
	if len(got["memories"].([]any)) != 0 || got["omitted_count"] != float64(1) {
		t.Fatalf("over budget: %v", got)
	}
}

// History must deliver durable events and forward its cursor, rather than
// answering from the current record or repeating the newest event.
func TestMemoryHistoryToolPaginatesEvents(t *testing.T) {
	var clk *clock.Fake
	srv := newServerWithSetup(t, func(_ storage.KV, _ record.Repo, c *clock.Fake) { clk = c })
	sid := initialise(t, srv, keyAcme)
	rid := callTool(t, srv, sid, "store_memory", map[string]any{"content": "quartz"})["id"]
	callTool(t, srv, sid, "get_memory", map[string]any{"id": rid})
	clk.Advance(2 * time.Minute)
	callTool(t, srv, sid, "get_memory", map[string]any{"id": rid})
	args := map[string]any{"id": rid, "limit": 1}
	first := callTool(t, srv, sid, "memory_history", args)
	entries := first["entries"].([]any)
	if len(entries) != 1 || entries[0].(map[string]any)["kind"] != "recalled" || entries[0].(map[string]any)["at"] != "2026-01-01T00:02:00Z" || first["has_more"] != true {
		t.Fatalf("newest history: %v", first)
	}
	args["cursor"] = first["next_cursor"]
	last := callTool(t, srv, sid, "memory_history", args)
	entries = last["entries"].([]any)
	if len(entries) != 1 || entries[0].(map[string]any)["kind"] != "recalled" || entries[0].(map[string]any)["at"] != "2026-01-01T00:00:00Z" || last["has_more"] != false {
		t.Fatalf("older history: %v", last)
	}
	if _, ok := last["next_cursor"]; ok {
		t.Fatal("exhausted history has cursor")
	}
}
