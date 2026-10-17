package mcp_test

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
)

// The same contract as HTTP, on the surface a model reads: a semantic hit names
// its passage as {index, start, end}, a keyword hit has no passage key, and
// under explain only the vector source carries a passage index.
func TestASearchHitCarriesItsPassage(t *testing.T) {
	srv := newServer(t)
	sid := initialise(t, srv, keyAcme)
	words := make([]string, 8*embeddingtest.PassageWords)
	for i := range words {
		words[i] = "harbour"
		if i >= 4*embeddingtest.PassageWords && i < 5*embeddingtest.PassageWords {
			words[i] = "zanzibar"
		}
	}
	content := strings.Join(words, " ")
	callTool(t, srv, sid, "store_memory", map[string]any{"content": content})

	found := callTool(t, srv, sid, "search_memories", map[string]any{
		"query": "zanzibar", "search_type": "hybrid", "limit": 1, "explain": true})
	hit := found["results"].([]any)[0].(map[string]any)
	p, ok := hit["passage"].(map[string]any)
	if !ok {
		t.Fatalf("a hybrid hit carries no passage: %v", hit)
	}
	start, end := int(p["start"].(float64)), int(p["end"].(float64))
	if p["index"].(float64) != 4 || content[start:end] != strings.TrimSpace(strings.Repeat("zanzibar ", embeddingtest.PassageWords)) {
		t.Fatalf("passage = %v, want index 4 bounding the fifth passage", p)
	}
	for _, s := range hit["sources"].([]any) {
		src := s.(map[string]any)
		_, has := src["passage"]
		if (src["source"] == "vector") != has {
			t.Fatalf("source %v carries a passage: %v; only the vector source names one", src["source"], has)
		}
	}

	kw := callTool(t, srv, sid, "search_memories", map[string]any{
		"query": "zanzibar", "search_type": "keyword", "limit": 1})
	if _, has := kw["results"].([]any)[0].(map[string]any)["passage"]; has {
		t.Fatal("a keyword hit carries a passage key")
	}
}
