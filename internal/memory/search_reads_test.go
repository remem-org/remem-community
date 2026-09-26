package memory_test

import (
	"testing"

	"github.com/remem-org/remem-go/internal/memory"
)

// A search reads the body of each memory it returns, and never its vector.
//
// A result carries no embedding, so reading every returned memory's canonical
// vector was a point read per result for nothing. Profiled in Phase 13, a page
// of ten was thirty point reads through the search's snapshot — a body, a
// vector and an attribute row each — where twenty answer it.
func TestASearchPageReadsNoVectors(t *testing.T) {
	h := newListHarness(t)
	h.seedGradient(t, 50)

	h.counter.reset()
	res, err := h.svc.Search(acmeCtx(), memory.SearchReq{
		Type: memory.SearchSemantic, Query: "memory 07", Limit: 10,
	})
	must(t, err)
	if len(res.Results) != 10 {
		t.Fatalf("the search returned %d memories, want a full page of 10", len(res.Results))
	}
	if got := h.counter.vectors; got != 0 {
		t.Fatalf("a search page of %d read %d canonical vectors; a result carries no embedding",
			len(res.Results), got)
	}
	if got := h.counter.bodies; got != len(res.Results) {
		t.Fatalf("a search page of %d read %d record bodies, want one each", len(res.Results), got)
	}
}
