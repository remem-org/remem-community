package memory_test

import (
	"context"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

const externalModel = "cohere-embed-english-v3.0"

// externalService is a service whose vectors come from the caller, wired the
// way the server wires one: no model, a declared model id and width, and a
// cosine index. It returns the vector store so a test can read what was
// stamped on the canonical vector.
func externalService(t *testing.T, dim int) (*memory.Service, *vector.Store) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	texts := text.New()
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(texts))
	store := vector.NewStore(kv)
	index := flat.New(store, distance.Cosine)
	svc := memory.New(kv, repo, index, embedding.NewExternal(externalModel, dim),
		clock.NewFake(clock.FakeStart), attr.MustTable(), graph.NewService(kv, clock.NewFake(clock.FakeStart)),
		memory.Config{Metric: distance.Cosine}, memory.WithTextIndex(texts))
	t.Cleanup(func() { _ = svc.Close() })
	return svc, store
}

// unit returns a dim-wide vector pointing along axis i, scaled by k. Scaling
// is deliberate: a supplied vector is stored as given, not normalised.
func unit(dim, i int, k float32) []float32 {
	v := make([]float32, dim)
	v[i] = k
	return v
}

func wantInvalidNaming(t *testing.T, err error, words ...string) {
	t.Helper()
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("want an Invalid refusal, got %v", err)
	}
	for _, w := range words {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("refusal %q does not name %q", err, w)
		}
	}
}

func TestAnExternalCreateStoresTheSuppliedVectorUnderTheDeclaredModel(t *testing.T) {
	svc, store := externalService(t, 4)
	m, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "vdb 17", Embedding: []float32{3, 0, 4, 0}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	v, err := store.Get(context.Background(), "acme", tenant.DefaultNamespace, m.ID)
	if err != nil {
		t.Fatalf("read the canonical vector: %v", err)
	}
	if v.ModelID != externalModel || v.Dim != 4 {
		t.Fatalf("stamped %q/%d, want %q/4", v.ModelID, v.Dim, externalModel)
	}
	if v.Values[0] != 3 || v.Values[2] != 4 {
		t.Fatalf("a supplied vector is stored as given, got %v", v.Values)
	}
}

func TestAnExternalCreateWithoutAVectorIsRefused(t *testing.T) {
	svc, _ := externalService(t, 4)
	_, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "no vector"})
	wantInvalidNaming(t, err, "embedding", "4")
}

func TestAVectorOfTheWrongWidthIsRefusedAndNothingIsWritten(t *testing.T) {
	svc, _ := externalService(t, 4)
	_, err := svc.CreateBatch(acmeCtx(), []memory.CreateReq{
		{Content: "fine", Embedding: unit(4, 0, 1)},
		{Content: "too narrow", Embedding: []float32{1, 2, 3}},
	})
	wantInvalidNaming(t, err, "embedding", "3", "4", "memory 1 of the batch")

	page, err := svc.List(acmeCtx(), memory.ListReq{Limit: 10})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Memories) != 0 {
		t.Fatalf("a refused batch wrote %d memories; it must write none", len(page.Memories))
	}
}

func TestAZeroVectorIsRefusedUnderCosine(t *testing.T) {
	svc, _ := externalService(t, 4)
	_, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "zero", Embedding: make([]float32, 4)})
	wantInvalidNaming(t, err, "embedding", "zero")
}

func TestAVectorSentToAComputingServerIsRefused(t *testing.T) {
	svc := newService(t)
	_, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "hello", Embedding: unit(384, 0, 1)})
	wantInvalidNaming(t, err, "embedding", "computes its own")
}

func TestAnExternalContentUpdateRequiresAndStagesItsVector(t *testing.T) {
	svc, store := externalService(t, 4)
	m, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "before", Embedding: unit(4, 0, 1)})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	after := "after"
	_, err = svc.Update(acmeCtx(), memory.UpdateReq{ID: m.ID, Content: &after})
	wantInvalidNaming(t, err, "embedding")

	if _, err := svc.Update(acmeCtx(), memory.UpdateReq{ID: m.ID, Content: &after, Embedding: unit(4, 2, 1)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	v, err := store.Get(context.Background(), "acme", tenant.DefaultNamespace, m.ID)
	if err != nil {
		t.Fatalf("read the canonical vector: %v", err)
	}
	if v.Values[2] != 1 || v.ModelID != externalModel {
		t.Fatalf("the update did not stage its vector: %+v", v)
	}
}

func TestAnExternalVectorWithoutContentIsRefusedOnUpdate(t *testing.T) {
	svc, _ := externalService(t, 4)
	m, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "text", Embedding: unit(4, 0, 1)})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = svc.Update(acmeCtx(), memory.UpdateReq{ID: m.ID, Embedding: unit(4, 1, 1)})
	wantInvalidNaming(t, err, "embedding", "content")
}

func TestAnUpdateThatLeavesContentAloneNeedsNoVector(t *testing.T) {
	svc, _ := externalService(t, 4)
	m, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "text", Embedding: unit(4, 0, 1)})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	tags := []string{"reviewed"}
	if _, err := svc.Update(acmeCtx(), memory.UpdateReq{ID: m.ID, Tags: &tags}); err != nil {
		t.Fatalf("a tag-only update moves no vector and needs none: %v", err)
	}
}

// countingExternal is an external declaration that counts embedding calls of
// either kind, so a test can prove a request ran no model rather than infer it.
type countingExternal struct {
	*embedding.External
	calls int
}

func (c *countingExternal) EmbedQueries(ctx context.Context, texts []string) ([][]float32, error) {
	c.calls++
	return c.External.EmbedQueries(ctx, texts)
}

func (c *countingExternal) EmbedDocuments(ctx context.Context, texts []string) ([]embedding.Document, error) {
	c.calls++
	return c.External.EmbedDocuments(ctx, texts)
}

// seedAxes stores one memory per axis, each pointing along its own axis, and
// returns their ids in axis order. With a decoy on every other axis, a search
// by one memory's vector can only rank that memory first if the vector is what
// decided the ranking.
func seedAxes(t *testing.T, svc *memory.Service, dim int) []id.ID {
	t.Helper()
	reqs := make([]memory.CreateReq, dim)
	for i := range reqs {
		reqs[i] = memory.CreateReq{Content: "axis memory", Embedding: unit(dim, i, float32(i+1))}
	}
	out, err := svc.CreateBatch(acmeCtx(), reqs)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	ids := make([]id.ID, len(out))
	for i, m := range out {
		ids[i] = m.ID
	}
	return ids
}

func TestAnExternalSemanticSearchRanksByTheSuppliedVector(t *testing.T) {
	svc, _ := externalService(t, 6)
	ids := seedAxes(t, svc, 6)
	for axis := range ids {
		res, err := svc.Search(acmeCtx(), memory.SearchReq{
			Type: memory.SearchSemantic, QueryVector: unit(6, axis, 0.5), Limit: 3,
		})
		if err != nil {
			t.Fatalf("search along axis %d: %v", axis, err)
		}
		if len(res.Results) == 0 || res.Results[0].Memory.ID != ids[axis] {
			t.Fatalf("axis %d: the memory stored along that axis must rank first among %d decoys", axis, len(ids)-1)
		}
	}
}

func TestAnExternalSearchInvokesNoModel(t *testing.T) {
	counter := &countingExternal{External: embedding.NewExternal(externalModel, 4)}
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	texts := text.New()
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(texts))
	svc := memory.New(kv, repo, flat.New(vector.NewStore(kv), distance.Cosine), counter,
		clock.NewFake(clock.FakeStart), attr.MustTable(), graph.NewService(kv, clock.NewFake(clock.FakeStart)),
		memory.Config{Metric: distance.Cosine}, memory.WithTextIndex(texts))
	t.Cleanup(func() { _ = svc.Close() })

	seedAxes(t, svc, 4)
	for _, typ := range []memory.SearchType{memory.SearchSemantic, memory.SearchHybrid} {
		if _, err := svc.Search(acmeCtx(), memory.SearchReq{
			Type: typ, Query: "axis", QueryVector: unit(4, 1, 1), Limit: 2,
		}); err != nil {
			t.Fatalf("%s search: %v", typ, err)
		}
	}
	if counter.calls != 0 {
		t.Fatalf("an external server embedded %d times; it must never embed", counter.calls)
	}
}

func TestAnExternalSearchRefusesWhatItCannotAnswer(t *testing.T) {
	svc, _ := externalService(t, 4)
	seedAxes(t, svc, 4)
	for _, tc := range []struct {
		name string
		req  memory.SearchReq
		want []string
	}{
		{"semantic without a vector", memory.SearchReq{Type: memory.SearchSemantic, Query: "axis"}, []string{"query_vector", "4"}},
		{"hybrid without a vector", memory.SearchReq{Type: memory.SearchHybrid, Query: "axis"}, []string{"query_vector"}},
		{"hybrid without text", memory.SearchReq{Type: memory.SearchHybrid, QueryVector: unit(4, 0, 1)}, []string{"hybrid", "query"}},
		{"keyword with a vector", memory.SearchReq{Type: memory.SearchKeyword, Query: "axis", QueryVector: unit(4, 0, 1)}, []string{"query_vector", "keyword"}},
		{"a vector of the wrong width", memory.SearchReq{Type: memory.SearchSemantic, QueryVector: []float32{1, 2}}, []string{"query_vector", "2", "4"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Search(acmeCtx(), tc.req)
			wantInvalidNaming(t, err, tc.want...)
		})
	}
}

func TestAnExternalKeywordSearchNeedsNoVector(t *testing.T) {
	svc, _ := externalService(t, 4)
	seedAxes(t, svc, 4)
	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchKeyword, Query: "axis", Limit: 10})
	if err != nil {
		t.Fatalf("keyword search: %v", err)
	}
	if len(res.Results) != 4 {
		t.Fatalf("keyword search found %d of 4", len(res.Results))
	}
}

func TestAQueryVectorSentToAComputingServerIsRefused(t *testing.T) {
	svc := newService(t)
	seed(t, svc, "hello world")
	_, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "hello", QueryVector: unit(384, 0, 1)})
	wantInvalidNaming(t, err, "query_vector", "computes its own")
}

func TestAnExternalRecallUsesTheSuppliedVector(t *testing.T) {
	svc, _ := externalService(t, 4)
	ids := seedAxes(t, svc, 4)
	got, err := svc.Recall(acmeCtx(), memory.RecallReq{
		Type: memory.SearchSemantic, QueryVector: unit(4, 2, 1), TokenBudget: 1000,
	})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(got.Memories) == 0 || got.Memories[0].Memory.ID != ids[2] {
		t.Fatal("recall must rank by the supplied vector")
	}
	_, err = svc.Recall(acmeCtx(), memory.RecallReq{Type: memory.SearchHybrid, Context: "axis", TokenBudget: 1000})
	wantInvalidNaming(t, err, "query_vector")
}
