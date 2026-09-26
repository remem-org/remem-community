package flat_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
	"github.com/remem-org/remem-go/internal/vector/vectortest"
)

// The shared contract. flat is the definition of correct, so it runs the same
// suite Phase 7's HNSW will be held to.
func TestFlatSatisfiesTheIndexContract(t *testing.T) {
	vectortest.Run(t, func(t *testing.T) vector.Index {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		return flat.New(vector.NewStore(kv), distance.L2)
	})
}

func TestFlatSatisfiesTheContractUnderEveryMetric(t *testing.T) {
	for _, m := range []distance.Metric{distance.Cosine, distance.DotProduct} {
		t.Run(m.String(), func(t *testing.T) {
			vectortest.Run(t, func(t *testing.T) vector.Index {
				kv := memkv.New()
				t.Cleanup(func() { _ = kv.Close() })
				return flat.New(vector.NewStore(kv), m)
			})
		})
	}
}

func newIndex(t *testing.T) (*flat.Index, *vector.Store) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	store := vector.NewStore(kv)
	return flat.New(store, distance.L2), store
}

// Model identity is stamped by the index, because a vector without one cannot
// later be told apart from one produced by a model that has since changed.
func TestInsertStampsTheRunningModel(t *testing.T) {
	idx, store := newIndex(t)
	ctx := flat.WithModel(context.Background(), "all-MiniLM-L6-v2")
	rid := id.New()
	if err := idx.Insert(ctx, "acme", rid, []float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, "acme", tenant.DefaultNamespace, rid)
	if err != nil {
		t.Fatal(err)
	}
	if got.ModelID != "all-MiniLM-L6-v2" {
		t.Fatalf("stored model id is %q", got.ModelID)
	}
}

// A context that names no model still produces a storable vector, because the
// codec refuses one with an empty model id — and "unknown" is a value an audit
// can find, where an empty string is a write that failed for a reason nobody
// will connect to the model.
func TestInsertWithoutAModelStampsUnknownRatherThanNothing(t *testing.T) {
	idx, store := newIndex(t)
	rid := id.New()
	if err := idx.Insert(context.Background(), "acme", rid, []float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(context.Background(), "acme", tenant.DefaultNamespace, rid)
	if err != nil {
		t.Fatal(err)
	}
	if got.ModelID != "unknown" {
		t.Fatalf("stored model id is %q, want %q", got.ModelID, "unknown")
	}
}

// A flat index is an exact scan, so it must return the true nearest neighbours
// — not merely plausible ones. This is what Phase 7 is measured against.
func TestSearchIsExact(t *testing.T) {
	idx, _ := newIndex(t)
	ctx := context.Background()

	type entry struct {
		rid id.ID
		v   []float32
	}
	var all []entry
	for i := range 200 {
		v := []float32{float32(i%13) / 13, float32(i%7) / 7, float32(i%5) / 5, float32(i%3) / 3}
		normalise(v)
		rid := id.New()
		if err := idx.Insert(ctx, "acme", rid, v); err != nil {
			t.Fatal(err)
		}
		all = append(all, entry{rid, v})
	}

	q := []float32{0.5, 0.5, 0.5, 0.5}
	normalise(q)

	hits, err := idx.Search(ctx, "acme", q, 5, nil)
	if err != nil {
		t.Fatal(err)
	}

	// The true answer, computed the slow and obvious way.
	type scored struct {
		rid id.ID
		d   float32
	}
	var truth []scored
	for _, e := range all {
		d, err := distance.Compute(distance.L2, q, e.v)
		if err != nil {
			t.Fatal(err)
		}
		truth = append(truth, scored{e.rid, d})
	}
	for i := 1; i < len(truth); i++ {
		for j := i; j > 0 && truth[j].d < truth[j-1].d; j-- {
			truth[j], truth[j-1] = truth[j-1], truth[j]
		}
	}
	for i, h := range hits {
		if h.Distance != truth[i].d {
			t.Fatalf("hit %d is at distance %v, the true %dth nearest is at %v", i, h.Distance, i, truth[i].d)
		}
	}
}

// The heap holds the k best, and the eviction rule is the easiest thing here to
// get wrong: a comparison the wrong way round evicts the best rather than the
// worst, and the result still looks like a ranked list.
func TestTheNearestIsKeptWhenTheHeapIsFull(t *testing.T) {
	idx, _ := newIndex(t)
	ctx := context.Background()

	q := []float32{1, 0, 0, 0}
	for range 20 {
		if err := idx.Insert(ctx, "acme", id.New(), []float32{0, 1, 0, 0}); err != nil {
			t.Fatal(err)
		}
	}
	// Inserted last, so it is scanned last and must displace something.
	near := id.New()
	if err := idx.Insert(ctx, "acme", near, q); err != nil {
		t.Fatal(err)
	}

	hits, err := idx.Search(ctx, "acme", q, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if hits[0].ID != near {
		t.Fatal("the nearest vector was evicted from a full result heap")
	}
}

// A vector of another width means a vector from another model reached the
// index. Comparing the overlapping components would produce a ranking nobody
// could explain, so the scan fails instead.
func TestAStoredVectorOfTheWrongWidthFailsTheSearch(t *testing.T) {
	idx, store := newIndex(t)
	ctx := context.Background()
	if err := idx.Insert(ctx, "acme", id.New(), []float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "acme", tenant.DefaultNamespace, id.New(), &vector.Vector{
		ModelID: "some-other-model", Dim: 2, Values: []float32{1, 0},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.Search(ctx, "acme", []float32{1, 0, 0, 0}, 5, nil); !errs.Is(err, errs.Invalid) {
		t.Fatalf("Search = %v, want Invalid", err)
	}
}

func TestRebuildNeedsASource(t *testing.T) {
	idx, _ := newIndex(t)
	if err := idx.Rebuild(context.Background(), "acme", nil); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

func TestUnscopedOperationsAreRefused(t *testing.T) {
	idx, _ := newIndex(t)
	ctx := context.Background()
	if err := idx.Insert(ctx, "", id.New(), []float32{1}); !errs.Is(err, errs.Invalid) {
		t.Fatalf("Insert = %v", err)
	}
	if err := idx.Delete(ctx, "", id.New()); !errs.Is(err, errs.Invalid) {
		t.Fatalf("Delete = %v", err)
	}
	if _, err := idx.Stats(ctx, ""); !errs.Is(err, errs.Invalid) {
		t.Fatalf("Stats = %v", err)
	}
	if err := idx.Rebuild(ctx, "", &emptySource{}); !errs.Is(err, errs.Invalid) {
		t.Fatalf("Rebuild = %v", err)
	}
}

type emptySource struct{}

func (emptySource) Scan(context.Context, tenant.ID, func(id.ID, []float32) error) error {
	return nil
}

func normalise(v []float32) {
	var sum float32
	for _, x := range v {
		sum += x * x
	}
	if sum == 0 {
		v[0] = 1
		return
	}
	inv := 1 / sqrt32(sum)
	for i := range v {
		v[i] *= inv
	}
}

func sqrt32(x float32) float32 {
	// A local square root keeps the test's arithmetic identical to the
	// package's own, which matters when the assertion is exact equality.
	f := float64(x)
	g := f
	for range 40 {
		g = 0.5 * (g + f/g)
	}
	return float32(g)
}
