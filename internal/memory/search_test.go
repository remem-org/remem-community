package memory_test

import (
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/memory"
)

// A search must say how to look. There is no default, because the three modes
// answer materially different questions and a default would make one of them
// the silent answer to "I did not think about it" — see memory.SearchType.
func TestASearchWithoutAModeIsRefusedByName(t *testing.T) {
	svc := newService(t)
	seed(t, svc, "the sky is blue")

	_, err := svc.Search(acmeCtx(), memory.SearchReq{Query: "sky", Limit: 5})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("a search with no mode returned %v, want errs.Invalid", err)
	}
	for _, mode := range memory.SearchTypes() {
		if !strings.Contains(err.Error(), mode) {
			t.Errorf("the refusal does not name %q; a caller cannot fix what they are not told: %v", mode, err)
		}
	}

	_, err = svc.Search(acmeCtx(), memory.SearchReq{Query: "sky", Type: "fuzzy", Limit: 5})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("a search naming a mode this build does not have returned %v", err)
	}
}

// A keyword search finds a memory by a term the embedder has no idea about,
// and it does so without running the model at all. Both halves matter: the
// first is why the mode exists and the second is what it costs.
func TestKeywordSearchFindsAnExactTermAndRunsNoModel(t *testing.T) {
	e := embeddingtest.New()
	svc := newService(t, withEmbedder(e))
	ids := seed(t, svc,
		"paid the invoice INV-2024-8871 on Tuesday",
		"discussed the budget at length with everyone involved",
	)

	before := e.Calls()
	res, err := svc.Search(acmeCtx(), memory.SearchReq{
		Type: memory.SearchKeyword, Query: "INV-2024-8871", Limit: 5,
	})
	must(t, err)

	if len(res.Results) != 1 || res.Results[0].Memory.ID != ids[0] {
		t.Fatalf("a keyword search for an identifier returned %d results: %+v", len(res.Results), res.Results)
	}
	if got := e.Calls(); got != before {
		t.Fatalf("a keyword search ran the embedding model %d times; the mode exists partly "+
			"because it does not have to", got-before)
	}
}

// The three modes are distinct answers to one query, and a caller has to be
// able to tell which one they asked for. `keyword` cannot reach a memory that
// shares no term; `semantic` can.
func TestTheThreeModesAnswerDifferently(t *testing.T) {
	svc := newService(t)
	seed(t, svc, "cats are mammals and cats purr", "quantum chromodynamics")

	keyword, err := svc.Search(acmeCtx(), memory.SearchReq{
		Type: memory.SearchKeyword, Query: "chromodynamics", Limit: 5,
	})
	must(t, err)
	if len(keyword.Results) != 1 {
		t.Fatalf("a keyword search matched %d memories, want the one carrying the word", len(keyword.Results))
	}

	semantic, err := svc.Search(acmeCtx(), memory.SearchReq{
		Type: memory.SearchSemantic, Query: "chromodynamics", Limit: 5,
	})
	must(t, err)
	if len(semantic.Results) != 2 {
		t.Fatalf("a semantic search returned %d memories, want every one in the corpus ranked",
			len(semantic.Results))
	}

	hybrid, err := svc.Search(acmeCtx(), memory.SearchReq{
		Type: memory.SearchHybrid, Query: "chromodynamics", Limit: 5,
	})
	must(t, err)
	if hybrid.Results[0].Memory.Content != "quantum chromodynamics" {
		t.Fatalf("hybrid did not rank the memory both indexes found first: %+v", hybrid.Results)
	}
}

// A tag filter narrows every mode, because a filter removes candidates. It also
// works with no query at all: "everything tagged X" is a real request.
func TestATagFilterNarrowsAndCanStandAlone(t *testing.T) {
	svc := newService(t)
	tagged, err := svc.Create(acmeCtx(), memory.CreateReq{
		Content: "the quarterly numbers", Tags: []string{"finance"},
	})
	must(t, err)
	_, err = svc.Create(acmeCtx(), memory.CreateReq{Content: "the quarterly numbers"})
	must(t, err)

	for _, mode := range []memory.SearchType{memory.SearchSemantic, memory.SearchKeyword, memory.SearchHybrid} {
		res, err := svc.Search(acmeCtx(), memory.SearchReq{
			Type: mode, Query: "quarterly numbers", Tags: []string{"finance"}, Limit: 10,
		})
		must(t, err)
		if len(res.Results) != 1 || res.Results[0].Memory.ID != tagged.ID {
			t.Errorf("%s with a tag filter returned %d memories, want only the tagged one",
				mode, len(res.Results))
		}
	}

	only, err := svc.Search(acmeCtx(), memory.SearchReq{
		Type: memory.SearchKeyword, Tags: []string{"finance"}, Limit: 10,
	})
	must(t, err)
	if len(only.Results) != 1 || only.Results[0].Memory.ID != tagged.ID {
		t.Fatalf("a tag-only search returned %d memories: %+v", len(only.Results), only.Results)
	}
}

// The per-source evidence is present only when asked for, and names the indexes
// that actually found each memory.
func TestExplainReportsWhichIndexesFoundEachMemory(t *testing.T) {
	svc := newService(t)
	seed(t, svc, "kubernetes cluster autoscaling")

	quiet, err := svc.Search(acmeCtx(), memory.SearchReq{
		Type: memory.SearchHybrid, Query: "kubernetes cluster", Limit: 5,
	})
	must(t, err)
	for _, r := range quiet.Results {
		if len(r.Sources) != 0 {
			t.Fatalf("evidence was returned without being asked for: %+v", r.Sources)
		}
	}

	loud, err := svc.Search(acmeCtx(), memory.SearchReq{
		Type: memory.SearchHybrid, Query: "kubernetes cluster", Limit: 5, Explain: true,
	})
	must(t, err)
	if len(loud.Results) == 0 {
		t.Fatal("the explained search returned nothing")
	}
	seen := map[string]bool{}
	for _, s := range loud.Results[0].Sources {
		seen[s.Source] = true
	}
	if !seen["vector"] || !seen["text"] {
		t.Fatalf("a hybrid hit reports evidence from %v; both indexes found it", seen)
	}
}
