package vector_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
	"github.com/remem-org/remem-go/internal/vector/hnsw"
)

// A rebuild must never erase the identity of the model that produced a vector.
//
// This is a regression, and it reached a shipped command before it was found.
// `remem-admin vector rebuild` — the repair an operator is told to run when the
// index is damaged — wrote "unknown" over the model id of every vector in the
// corpus it was repairing, because a rebuild reads coordinates through
// vector.Source and has no way to know what produced them. The next search then
// refused the tenant outright: the server runs all-MiniLM-L6-v2 and the corpus
// no longer claimed any model. A repair that makes a corpus unsearchable is
// worse than no repair.
//
// Nothing in the unit tests could see it. vector.Index exposes no model id, so
// the contract suite cannot assert on one, and every test built its corpus and
// its index in the same breath with the same (empty) model. It took driving the
// real binary against the real model.
func TestARebuildKeepsTheModelThatProducedTheVectors(t *testing.T) {
	ctx := context.Background()
	const model = "all-MiniLM-L6-v2"

	for _, kind := range []string{"flat", "hnsw"} {
		t.Run(kind, func(t *testing.T) {
			kv := memkv.New()
			t.Cleanup(func() { _ = kv.Close() })
			store := vector.NewStore(kv)

			ids := make([]id.ID, 8)
			for i := range ids {
				ids[i] = id.New()
				if err := store.Put(ctx, "acme", tenant.DefaultNamespace, ids[i], &vector.Vector{
					ModelID: model, Dim: 4, Values: []float32{float32(i), 1, 0, 0},
				}); err != nil {
					t.Fatal(err)
				}
			}

			// A rebuild by something that was never told which model is running
			// — which is exactly the admin command's position.
			var idx vector.Index
			if kind == "flat" {
				idx = flat.New(store, distance.L2)
			} else {
				h, err := hnsw.New(kv, hnsw.Options{Metric: distance.L2})
				if err != nil {
					t.Fatal(err)
				}
				idx = h
			}
			if err := idx.Rebuild(ctx, "acme", store); err != nil {
				t.Fatalf("rebuild: %v", err)
			}

			for _, rid := range ids {
				v, err := store.Get(ctx, "acme", tenant.DefaultNamespace, rid)
				if err != nil {
					t.Fatal(err)
				}
				if v.ModelID != model {
					t.Fatalf("%s rebuild left %s claiming model %q, want %q: the corpus can no longer be told "+
						"from one embedded by something else", kind, rid, v.ModelID, model)
				}
			}
		})
	}
}

// A vector with nothing to inherit still gets a value, because the encoding
// refuses an empty one and because "unknown" is a thing an audit can find.
func TestAVectorWithNoModelAnywhereBecomesUnknown(t *testing.T) {
	ctx := context.Background()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	store := vector.NewStore(kv)

	rid := id.New()
	if err := store.Replace(ctx, "acme", tenant.DefaultNamespace, map[id.ID]*vector.Vector{
		rid: {Dim: 4, Values: []float32{1, 0, 0, 0}},
	}); err != nil {
		t.Fatal(err)
	}
	v, err := store.Get(ctx, "acme", tenant.DefaultNamespace, rid)
	if err != nil {
		t.Fatal(err)
	}
	if v.ModelID != vector.UnknownModel {
		t.Fatalf("model id is %q, want %q", v.ModelID, vector.UnknownModel)
	}
}

// A caller that does know overwrites. An import carrying vectors from a named
// model is not corrected by whatever happened to be stored before.
func TestAStatedModelOverwrites(t *testing.T) {
	ctx := context.Background()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	store := vector.NewStore(kv)

	rid := id.New()
	if err := store.Put(ctx, "acme", tenant.DefaultNamespace, rid, &vector.Vector{
		ModelID: "old-model", Dim: 4, Values: []float32{1, 0, 0, 0},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Replace(ctx, "acme", tenant.DefaultNamespace, map[id.ID]*vector.Vector{
		rid: {ModelID: "new-model", Dim: 4, Values: []float32{0, 1, 0, 0}},
	}); err != nil {
		t.Fatal(err)
	}
	v, err := store.Get(ctx, "acme", tenant.DefaultNamespace, rid)
	if err != nil {
		t.Fatal(err)
	}
	if v.ModelID != "new-model" {
		t.Fatalf("model id is %q, want new-model", v.ModelID)
	}
}
