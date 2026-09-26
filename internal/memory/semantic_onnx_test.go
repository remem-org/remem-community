//go:build cgo && onnx

package memory_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/onnx"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

// realEmbedder opens the actual model, skipping when it has not been fetched.
func realEmbedder(t *testing.T) embedding.Embedder {
	t.Helper()
	path := os.Getenv("REMEM_EMBEDDING_MODEL_PATH")
	if path == "" {
		path = filepath.Join("..", "..", ".models", "all-MiniLM-L6-v2")
	}
	if _, err := os.Stat(filepath.Join(path, "model.onnx")); err != nil {
		t.Skipf("no model at %s; run scripts/fetch-model.sh", path)
	}
	e, err := onnx.Open(onnx.Config{
		ModelPath:         path,
		SharedLibraryPath: os.Getenv("REMEM_ONNX_LIBRARY_PATH"),
		IntraOpThreads:    2,
	})
	if err != nil {
		t.Fatalf("opening the model: %v", err)
	}
	t.Cleanup(func() {
		if c, ok := e.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	})
	return e
}

// TestSemanticSearchRanksByCosine is plan Task 3.5's test, run against the real
// model rather than the deterministic fake.
//
// The fake in service_test.go is a bag of words: it cannot rank "feline
// biology" above "dogs are mammals", because the query shares no word with
// either. That is not a defect in the fake — it is why the fake is used for
// plumbing and this test exists for meaning. Splitting them keeps the ordinary
// suite fast and machine-independent while still asserting the thing a user
// actually cares about, which is that a search understands a synonym.
func TestSemanticSearchRanksByCosine(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	svc := memory.New(kv, record.NewRepo(kv), flat.New(vector.NewStore(kv), distance.L2),
		realEmbedder(t), clock.NewFake(clock.FakeStart), attr.MustTable(),
		graph.NewService(kv, clock.NewFake(clock.FakeStart)), memory.Config{})

	ids := seed(t, svc, "cats are mammals", "dogs are mammals", "quantum chromodynamics")

	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "feline biology", Limit: 3})
	must(t, err)
	if len(res.Results) != 3 {
		t.Fatalf("got %d results", len(res.Results))
	}
	if res.Results[0].Memory.ID != ids[0] {
		t.Fatalf("closest match should rank first, got %q", res.Results[0].Memory.Content)
	}
	if res.Results[0].Score <= res.Results[2].Score {
		t.Fatal("scores must decrease with relevance")
	}
	for _, r := range res.Results {
		if r.Score < 0 || r.Score > 1 {
			t.Fatalf("score %f outside [0,1]", r.Score)
		}
	}
	t.Logf("feline biology: %q %.3f | %q %.3f | %q %.3f",
		res.Results[0].Memory.Content, res.Results[0].Score,
		res.Results[1].Memory.Content, res.Results[1].Score,
		res.Results[2].Memory.Content, res.Results[2].Score)
}

// The whole argument for mean pooling was that a relevance threshold becomes
// usable. Assert it end to end: an unrelated memory must score low enough that
// a caller can reject it by number.
func TestAnUnrelatedMemoryScoresLowEnoughToThreshold(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	svc := memory.New(kv, record.NewRepo(kv), flat.New(vector.NewStore(kv), distance.L2),
		realEmbedder(t), clock.NewFake(clock.FakeStart), attr.MustTable(),
		graph.NewService(kv, clock.NewFake(clock.FakeStart)), memory.Config{})

	seed(t, svc, "the deployment pipeline runs on Tuesdays")

	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "recipes for sourdough bread", Limit: 1})
	must(t, err)
	if len(res.Results) != 1 {
		t.Fatalf("got %d results", len(res.Results))
	}
	if got := res.Results[0].Score; got > 0.35 {
		t.Fatalf("an unrelated memory scores %.3f; under CLS pooling this sat near 0.6, "+
			"which is what made a relevance threshold useless (plan §II.10 row 16)", got)
	}
}
