package query_test

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/query"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

// hybridFixture is a corpus a hybrid query can tell three things apart in: a
// memory both indexes find, one only the vector index finds, and one only the
// text index finds.
type hybridFixture struct {
	kv    storage.KV
	texts *text.Index
	vecs  vector.Index
	ctx   context.Context

	both  id.ID
	near  id.ID
	words id.ID
}

func newHybridFixture(t *testing.T) hybridFixture {
	t.Helper()

	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	f := hybridFixture{
		kv:    kv,
		texts: text.New(),
		vecs:  flat.New(vector.NewStore(kv), distance.Cosine),
		ctx:   tenant.NewContext(context.Background(), graphTenant),
		both:  id.New(),
		near:  id.New(),
		words: id.New(),
	}

	// `both` is a near neighbour of the query vector and carries its words.
	f.store(t, f.both, "kubernetes cluster autoscaling", []float32{0.9, 0.436, 0, 0})
	// `near` is the *nearest* neighbour and shares no words, so it leads the
	// vector list and appears in no other.
	f.store(t, f.near, "unrelated musings entirely", []float32{1, 0, 0, 0})
	// `words` carries the query's words and has no embedding at all, so it is
	// reachable only through the text index.
	f.store(t, f.words, "kubernetes cluster autoscaling runbook", nil)

	return f
}

func (f hybridFixture) store(t *testing.T, rid id.ID, content string, v []float32) {
	t.Helper()
	err := txn.Do(f.ctx, f.kv, func(tx txn.Tx) error {
		return f.texts.Add(f.ctx, tx, graphTenant, tenant.DefaultNamespace, rid, content, nil)
	})
	if err != nil {
		t.Fatalf("indexing text: %v", err)
	}
	if v == nil {
		return
	}
	err = vector.NewStore(f.kv).Put(f.ctx, graphTenant, tenant.DefaultNamespace, rid,
		&vector.Vector{ModelID: "test", Dim: len(v), Values: v})
	if err != nil {
		t.Fatalf("storing a vector: %v", err)
	}
}

func (f hybridFixture) run(t *testing.T, q *query.Query) query.Result {
	t.Helper()
	plan, err := query.NewPlanner(slotTable(t), 0, 0).Plan(q)
	if err != nil {
		t.Fatalf("planning: %v", err)
	}
	executor := query.NewExecutor(slotTable(t), f.vecs, distance.Cosine, query.WithTextIndex(f.texts))
	res, err := executor.Run(f.ctx, f.kv, q, plan)
	if err != nil {
		t.Fatalf("running: %v", err)
	}
	return res
}

func rankOf(res query.Result, rid id.ID) int {
	for i, h := range res.Hits {
		if h.ID == rid {
			return i
		}
	}
	return -1
}

func sourcesOf(res query.Result, rid id.ID) []query.Source {
	for _, h := range res.Hits {
		if h.ID != rid {
			continue
		}
		out := make([]query.Source, 0, len(h.Sources))
		for _, s := range h.Sources {
			out = append(out, s.Source)
		}
		return out
	}
	return nil
}

// TestHybridFusesAllThreeSources is the plan's test: a result matching by
// vector and by text carries both sources and outranks single-source matches.
// Agreement between indexes is the whole reason rank fusion exists.
func TestHybridFusesAllThreeSources(t *testing.T) {
	f := newHybridFixture(t)

	res := f.run(t, &query.Query{
		Tenant:  graphTenant,
		Text:    "kubernetes cluster autoscaling",
		Keyword: true,
		Vector:  []float32{1, 0, 0, 0},
		Limit:   10,
		Explain: true,
	})

	if len(res.Hits) != 3 {
		t.Fatalf("got %d hits, want 3: %+v", len(res.Hits), res.Hits)
	}
	if got := rankOf(res, f.both); got != 0 {
		t.Fatalf("the memory both indexes found ranked %d, behind a single-source match: %+v", got, res.Hits)
	}

	got := sourcesOf(res, f.both)
	sawVector, sawText := false, false
	for _, s := range got {
		switch s {
		case query.SourceVector:
			sawVector = true
		case query.SourceText:
			sawText = true
		}
	}
	if !sawVector || !sawText {
		t.Fatalf("the fused hit reports sources %v; both indexes found it", got)
	}
	if s := sourcesOf(res, f.words); len(s) != 1 || s[0] != query.SourceText {
		t.Fatalf("the text-only hit reports sources %v", s)
	}
	if s := sourcesOf(res, f.near); len(s) != 1 || s[0] != query.SourceVector {
		t.Fatalf("the vector-only hit reports sources %v", s)
	}
}

// TestHybridScoreIsComparableAcrossSearchTypes is the plan's test and the
// REM-74 defect pinned: the same memory scores within 0.05 under `semantic` and
// `hybrid`.
//
// It holds by construction rather than by luck. Relevance is the best
// *calibrated* source's score and a BM25 sum is not one — it is unbounded and
// corpus-dependent — so a memory found by both indexes takes its cosine under
// either search type, and the two numbers are equal rather than merely close.
// See query.deriveScore.
func TestHybridScoreIsComparableAcrossSearchTypes(t *testing.T) {
	f := newHybridFixture(t)
	q := []float32{1, 0, 0, 0}

	semantic := f.run(t, &query.Query{Tenant: graphTenant, Vector: q, Limit: 10})
	hybrid := f.run(t, &query.Query{
		Tenant: graphTenant, Vector: q,
		Text: "kubernetes cluster autoscaling", Keyword: true, Limit: 10,
	})

	compared := 0
	for _, a := range semantic.Hits {
		for _, b := range hybrid.Hits {
			if a.ID != b.ID {
				continue
			}
			compared++
			if diff := math.Abs(float64(a.Score - b.Score)); diff > 0.05 {
				t.Errorf("memory %s scores %v under semantic and %v under hybrid, a difference of %v",
					a.ID, a.Score, b.Score, diff)
			}
		}
	}
	if compared == 0 {
		t.Fatal("no memory appeared under both search types, so nothing was compared")
	}
}

// TestExplainReturnsPerSourceEvidence is the plan's test: rank, native score
// and evidence per source. Without `explain` the breakdown is absent, because
// it is context a caller pays for on every search.
func TestExplainReturnsPerSourceEvidence(t *testing.T) {
	f := newHybridFixture(t)
	ask := func(explain bool) query.Result {
		return f.run(t, &query.Query{
			Tenant: graphTenant, Text: "kubernetes cluster autoscaling", Keyword: true,
			Vector: []float32{1, 0, 0, 0}, Limit: 10, Explain: explain,
		})
	}

	quiet := ask(false)
	for _, h := range quiet.Hits {
		if len(h.Sources) != 0 {
			t.Fatalf("a hit carried %d sources without explain", len(h.Sources))
		}
	}

	loud := ask(true)
	for _, h := range loud.Hits {
		if len(h.Sources) == 0 {
			t.Fatalf("hit %s carried no evidence under explain", h.ID)
		}
		for _, s := range h.Sources {
			if s.Source == query.SourceUnspecified {
				t.Errorf("hit %s has a contribution from no named source", h.ID)
			}
			if s.Rank < 0 {
				t.Errorf("hit %s has a contribution at rank %d", h.ID, s.Rank)
			}
			if s.Score < 0 || s.Score > 1 {
				t.Errorf("hit %s reports a %s score of %v, outside [0,1]", h.ID, s.Source, s.Score)
			}
		}
	}
}

// The lesson Phase 7 paid for, applied to this index: an incomplete index must
// say so, and the caller that asks has to be pinned by a test. Without this the
// search returns a short page that reads exactly like a small corpus.
func TestKeywordSearchOverARebuildingIndexSaysSo(t *testing.T) {
	f := newHybridFixture(t)

	intact := f.run(t, &query.Query{
		Tenant: graphTenant, Text: "kubernetes", Keyword: true, Limit: 10,
	})
	if intact.Truncated {
		t.Fatal("a search over an intact index reported itself truncated")
	}

	// A rebuild interrupted after it marked itself and before it finished.
	if err := f.texts.Rebuild(f.ctx, f.kv, graphTenant, tenant.DefaultNamespace, failingSource{}); err == nil {
		t.Fatal("the failing rebuild reported success")
	}

	during := f.run(t, &query.Query{
		Tenant: graphTenant, Text: "kubernetes", Keyword: true, Limit: 10,
	})
	if !during.Truncated {
		t.Fatal("a search over a rebuilding index did not report itself truncated")
	}
}

type failingSource struct{}

func (failingSource) Scan(context.Context, tenant.ID, tenant.Namespace,
	func(id.ID, string, []string) error) error {
	return context.Canceled
}

// Tags narrow the result set rather than merely scoring it, and they are
// ordinary terms in the same index (plan §II.10 row 2). Until this phase the
// planner refused a tag filter by name; refusing it is now wrong.
func TestTagsNarrowAKeywordQuery(t *testing.T) {
	f := newHybridFixture(t)
	tagged := id.New()
	err := txn.Do(f.ctx, f.kv, func(tx txn.Tx) error {
		return f.texts.Add(f.ctx, tx, graphTenant, tenant.DefaultNamespace, tagged,
			"kubernetes cluster autoscaling", []string{"infra"})
	})
	if err != nil {
		t.Fatalf("indexing: %v", err)
	}

	res := f.run(t, &query.Query{
		Tenant: graphTenant, Text: "kubernetes cluster autoscaling",
		Tags: []string{"infra"}, Keyword: true, Limit: 10,
	})
	if len(res.Hits) != 1 || res.Hits[0].ID != tagged {
		t.Fatalf("a tag filter returned %d hits, want only the tagged memory: %+v", len(res.Hits), res.Hits)
	}
}

// A keyword query needs no vector, which is the point of having the mode: it
// answers without running the model at all.
func TestAKeywordQueryNeedsNoVectorIndex(t *testing.T) {
	f := newHybridFixture(t)

	q := &query.Query{Tenant: graphTenant, Text: "kubernetes", Keyword: true, Limit: 10}
	plan, err := query.NewPlanner(slotTable(t), 0, 0).Plan(q)
	if err != nil {
		t.Fatalf("planning: %v", err)
	}
	// No vector index at all, as a deployment with the model unavailable has.
	executor := query.NewExecutor(slotTable(t), nil, distance.Cosine, query.WithTextIndex(f.texts))
	res, err := executor.Run(f.ctx, f.kv, q, plan)
	if err != nil {
		t.Fatalf("running: %v", err)
	}
	if len(res.Hits) == 0 {
		t.Fatal("a keyword search over a build with no vector index returned nothing")
	}
}

// A build with no text index refuses a keyword query by name rather than
// answering it emptily. An empty page from a corpus that is not empty is the
// failure this whole layer exists to avoid.
func TestAKeywordQueryWithoutATextIndexIsRefused(t *testing.T) {
	f := newHybridFixture(t)

	q := &query.Query{Tenant: graphTenant, Text: "kubernetes", Keyword: true, Limit: 10}
	plan, err := query.NewPlanner(slotTable(t), 0, 0).Plan(q)
	if err != nil {
		t.Fatalf("planning: %v", err)
	}
	if _, err := query.NewExecutor(slotTable(t), f.vecs, distance.Cosine).Run(f.ctx, f.kv, q, plan); err == nil {
		t.Fatal("a keyword query was answered by a build with no text index")
	}
}

// A tag is a filter, so it narrows every step rather than only the one that
// can answer it from an index. A semantic search with a tag filter must not
// return a memory that lacks the tag, however close its embedding — and it must
// not *add* one that carries the tag but is nowhere near the query.
func TestATagFilterNarrowsTheSemanticStepToo(t *testing.T) {
	f := newHybridFixture(t)

	// The nearest neighbour of the query vector carries no tag at all.
	res := f.run(t, &query.Query{
		Tenant: graphTenant,
		Vector: []float32{1, 0, 0, 0},
		Tags:   []string{"infra"},
		Limit:  10,
	})
	if len(res.Hits) != 0 {
		t.Fatalf("a tag filter returned %d memories, none of which carry the tag: %+v",
			len(res.Hits), res.Hits)
	}

	// Give the near neighbour the tag, and it comes back.
	err := txn.Do(f.ctx, f.kv, func(tx txn.Tx) error {
		return f.texts.Add(f.ctx, tx, graphTenant, tenant.DefaultNamespace, f.near,
			"unrelated musings entirely", []string{"infra"})
	})
	if err != nil {
		t.Fatalf("tagging: %v", err)
	}

	res = f.run(t, &query.Query{
		Tenant: graphTenant,
		Vector: []float32{1, 0, 0, 0},
		Tags:   []string{"infra"},
		Limit:  10,
	})
	if len(res.Hits) != 1 || res.Hits[0].ID != f.near {
		t.Fatalf("got %+v, want only the tagged memory", res.Hits)
	}
}

// A tag filter must add no access path.
//
// This is the defect the end-to-end run of Phase 8 found, asserted at its cause.
// A semantic search for "distributed consensus and raft leader election" with a
// tag filter returned "marzipan recipes from a Bavarian bakery" first, at score
// 0.077, above a memory scored 0.114 — because the tag had been given a step of
// its own, and a step is a *source*. Every tagged memory became a candidate
// whether or not anything in the query reached it, and rank fusion then weighed
// the tag's opinion equally with the vector's.
//
// The symptom was an ordering inversion, and an ordering assertion is the wrong
// place to catch it: the two memories tie under RRF, so which one surfaces
// depends on a tiebreak. The cause does not tie. A query that is not a keyword
// query has one access path per index that can genuinely rank it, and a filter
// is not one of them.
func TestATagFilterAddsNoAccessPath(t *testing.T) {
	planner := query.NewPlanner(slotTable(t), 0, 0)

	kinds := func(q *query.Query) []query.StepKind {
		t.Helper()
		plan, err := planner.Plan(q)
		if err != nil {
			t.Fatalf("planning: %v", err)
		}
		out := make([]query.StepKind, 0, len(plan.Steps))
		for _, s := range plan.Steps {
			out = append(out, s.Kind)
		}
		return out
	}

	base := &query.Query{Tenant: graphTenant, Vector: []float32{1, 0, 0, 0},
		Text: "kubernetes", Limit: 10}
	tagged := *base
	tagged.Tags = []string{"infra"}

	if got, want := kinds(base), []query.StepKind{query.StepVector}; !slices.Equal(got, want) {
		t.Fatalf("a semantic query planned %v, want %v", got, want)
	}
	if got, want := kinds(&tagged), []query.StepKind{query.StepVector}; !slices.Equal(got, want) {
		t.Fatalf("adding a tag filter planned %v, want %v: a filter removes candidates, "+
			"and a step produces them", got, want)
	}

	keyword := tagged
	keyword.Keyword = true
	if got := kinds(&keyword); !slices.Contains(got, query.StepText) {
		t.Fatalf("a keyword query planned %v, which has no text step", got)
	}
}
