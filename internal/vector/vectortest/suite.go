// Package vectortest is the contract every vector.Index implementation must
// satisfy.
//
// It exists because `flat` is the *definition* of correct: it scans every
// vector a tenant owns and returns the true nearest neighbours. Phase 7's HNSW
// is approximate, and the whole risk of an approximate index is that its
// mistakes look like results. On the small, unambiguous cases below there is no
// approximation to hide behind — an implementation that disagrees here has a
// bug, not a recall trade-off.
//
// A shared suite rather than duplicated tests, for the same reason
// internal/storage/storagetest is shared: two implementations tested separately
// drift, and the drift is discovered by a user.
package vectortest

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
)

// Dim is the width every vector in this suite uses. It is small so a failure
// message is readable, and the vectors are unit-norm because that is what the
// embedder produces and what cosine recovery assumes.
const Dim = 4

// Run executes the suite against an index built by open.
//
// open must return a fresh, empty index each time it is called, backed by
// storage the test owns.
func Run(t *testing.T, open func(*testing.T) vector.Index) {
	t.Helper()

	t.Run("FindsTheExactMatchFirst", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()

		exact := unit([]float32{1, 0, 0, 0})
		near := unit([]float32{0.9, 0.4, 0, 0})
		far := unit([]float32{0, 0, 0, 1})

		want := insert(t, idx, "acme", exact)
		insert(t, idx, "acme", near)
		insert(t, idx, "acme", far)

		hits, err := idx.Search(ctx, "acme", exact, 3, nil)
		must(t, err)
		if len(hits) != 3 {
			t.Fatalf("got %d hits, want 3", len(hits))
		}
		if hits[0].ID != want {
			t.Fatalf("the exact match did not rank first")
		}
		for i := 1; i < len(hits); i++ {
			if hits[i-1].Distance > hits[i].Distance {
				t.Fatalf("hits are not ordered by distance: %v then %v", hits[i-1].Distance, hits[i].Distance)
			}
		}
	})

	t.Run("RespectsK", func(t *testing.T) {
		idx := open(t)
		for range 10 {
			insert(t, idx, "acme", randomUnit())
		}
		hits, err := idx.Search(context.Background(), "acme", randomUnit(), 3, nil)
		must(t, err)
		if len(hits) != 3 {
			t.Fatalf("k=3 returned %d hits", len(hits))
		}
	})

	t.Run("KLargerThanTheIndexReturnsEverything", func(t *testing.T) {
		idx := open(t)
		for range 3 {
			insert(t, idx, "acme", randomUnit())
		}
		hits, err := idx.Search(context.Background(), "acme", randomUnit(), 100, nil)
		must(t, err)
		if len(hits) != 3 {
			t.Fatalf("got %d hits over a 3-vector index", len(hits))
		}
	})

	t.Run("DeletedVectorsNeverSurface", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()
		q := unit([]float32{1, 0, 0, 0})
		gone := insert(t, idx, "acme", q)
		insert(t, idx, "acme", unit([]float32{0, 1, 0, 0}))

		must(t, idx.Delete(ctx, "acme", gone))
		hits, err := idx.Search(ctx, "acme", q, 10, nil)
		must(t, err)
		for _, h := range hits {
			if h.ID == gone {
				t.Fatal("a deleted vector was returned")
			}
		}
		// Deleting again is not an error: the post-condition already holds.
		must(t, idx.Delete(ctx, "acme", gone))
	})

	t.Run("SearchIsTenantScoped", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()
		q := unit([]float32{1, 0, 0, 0})
		hidden := insert(t, idx, "acme", q)

		hits, err := idx.Search(ctx, "other", q, 10, nil)
		must(t, err)
		if len(hits) != 0 {
			t.Fatalf("tenant other saw %d of acme's vectors, starting with %v", len(hits), hits[0].ID)
		}
		// And acme still sees its own.
		hits, err = idx.Search(ctx, "acme", q, 10, nil)
		must(t, err)
		if len(hits) != 1 || hits[0].ID != hidden {
			t.Fatalf("acme lost its own vector: %v", hits)
		}
	})

	t.Run("EmptyIndexReturnsNoHits", func(t *testing.T) {
		idx := open(t)
		hits, err := idx.Search(context.Background(), "acme", randomUnit(), 10, nil)
		must(t, err)
		if len(hits) != 0 {
			t.Fatalf("an empty index returned %d hits", len(hits))
		}
	})

	t.Run("ReinsertingReplacesRatherThanDuplicates", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()
		rid := id.New()
		must(t, idx.Insert(ctx, "acme", rid, unit([]float32{1, 0, 0, 0})))
		must(t, idx.Insert(ctx, "acme", rid, unit([]float32{0, 1, 0, 0})))

		hits, err := idx.Search(ctx, "acme", unit([]float32{0, 1, 0, 0}), 10, nil)
		must(t, err)
		if len(hits) != 1 {
			t.Fatalf("re-inserting the same id produced %d entries", len(hits))
		}
		if hits[0].Distance > 1e-5 {
			t.Fatalf("the id kept its old vector: distance %v to the new one", hits[0].Distance)
		}
	})

	t.Run("RebuildReproducesSearchResults", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()

		src := &fixedSource{}
		q := unit([]float32{1, 0, 0, 0})
		for range 20 {
			src.add(id.New(), randomUnit())
		}
		want := id.New()
		src.add(want, q)

		must(t, idx.Rebuild(ctx, "acme", src))
		hits, err := idx.Search(ctx, "acme", q, 5, nil)
		must(t, err)
		if len(hits) != 5 {
			t.Fatalf("got %d hits after a rebuild", len(hits))
		}
		if hits[0].ID != want {
			t.Fatal("the rebuilt index does not find the exact match")
		}

		// A second rebuild from the same source is idempotent.
		must(t, idx.Rebuild(ctx, "acme", src))
		again, err := idx.Search(ctx, "acme", q, 5, nil)
		must(t, err)
		if len(again) != len(hits) || again[0].ID != hits[0].ID {
			t.Fatal("rebuilding twice changed the results")
		}
	})

	// Progress reporting is an implementation's business: an index that
	// resumes in batches and one that replaces atomically must answer the same.
	t.Run("RebuildWithProgressAnswersAsWithout", func(t *testing.T) {
		ctx := context.Background()
		src := &fixedSource{}
		q := unit([]float32{1, 0, 0, 0})
		for range 20 {
			src.add(id.New(), randomUnit())
		}
		src.add(id.New(), q)

		plain := open(t)
		must(t, plain.Rebuild(ctx, "acme", src))
		want, err := plain.Search(ctx, "acme", q, 21, nil)
		must(t, err)

		paced := open(t)
		var saves int
		must(t, paced.Rebuild(ctx, "acme", src,
			vector.WithBatchSize(3),
			vector.WithProgress(nil, func(context.Context, []byte) error { saves++; return nil })))
		got, err := paced.Search(ctx, "acme", q, 21, nil)
		must(t, err)

		if len(got) != len(want) {
			t.Fatalf("with progress: %d hits, without: %d", len(got), len(want))
		}
		for i := range want {
			if got[i].ID != want[i].ID {
				t.Fatalf("hit %d differs with progress reporting (%d saves)", i, saves)
			}
		}
	})

	// Invariant 3: a derived index is rebuildable. Rebuilding must therefore
	// discard what was there, not merge with it — an index that kept stale
	// entries would "recover" into a state the canonical data never had.
	t.Run("RebuildReplacesRatherThanMerges", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()
		stale := insert(t, idx, "acme", unit([]float32{1, 0, 0, 0}))

		src := &fixedSource{}
		src.add(id.New(), unit([]float32{0, 1, 0, 0}))
		must(t, idx.Rebuild(ctx, "acme", src))

		hits, err := idx.Search(ctx, "acme", unit([]float32{1, 0, 0, 0}), 10, nil)
		must(t, err)
		for _, h := range hits {
			if h.ID == stale {
				t.Fatal("a rebuild kept an entry the source did not contain")
			}
		}
	})

	t.Run("RebuildPublishesReplacementAndRemovalTogether", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()
		replaced := id.New()
		removed := id.New()
		must(t, idx.Insert(ctx, "acme", replaced, unit([]float32{1, 0, 0, 0})))
		must(t, idx.Insert(ctx, "acme", removed, unit([]float32{0, 0, 1, 0})))

		src := &fixedSource{}
		src.add(replaced, unit([]float32{0, 1, 0, 0}))
		added := id.New()
		src.add(added, unit([]float32{0, 0, 0, 1}))
		must(t, idx.Rebuild(ctx, "acme", src))

		hits, err := idx.Search(ctx, "acme", unit([]float32{0, 1, 0, 0}), 10, nil)
		must(t, err)
		if len(hits) != 2 || hits[0].ID != replaced || hits[0].Distance > 1e-5 {
			t.Fatalf("rebuilt vectors = %v, want replaced vector first and one added vector", hits)
		}
		for _, h := range hits {
			if h.ID == removed {
				t.Fatal("successful rebuild kept an entry absent from the source")
			}
		}
	})

	t.Run("SourceFailureLeavesCanonicalVectorsUnchanged", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()
		replaced := id.New()
		kept := id.New()
		old := unit([]float32{1, 0, 0, 0})
		must(t, idx.Insert(ctx, "acme", replaced, old))
		must(t, idx.Insert(ctx, "acme", kept, unit([]float32{0, 0, 1, 0})))

		src := &failingSource{fixedSource: fixedSource{}}
		src.add(replaced, unit([]float32{0, 1, 0, 0}))
		if err := idx.Rebuild(ctx, "acme", src); !errors.Is(err, ErrSourceFailed) {
			t.Fatalf("Rebuild = %v, want source failure", err)
		}

		assertOriginalVectors(t, idx, replaced, kept, old)
	})

	t.Run("InvalidVectorMidwayLeavesCanonicalVectorsUnchanged", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()
		replaced := id.New()
		kept := id.New()
		old := unit([]float32{1, 0, 0, 0})
		must(t, idx.Insert(ctx, "acme", replaced, old))
		must(t, idx.Insert(ctx, "acme", kept, unit([]float32{0, 0, 1, 0})))

		src := &fixedSource{}
		src.add(replaced, unit([]float32{0, 1, 0, 0}))
		src.add(id.New(), nil)
		if err := idx.Rebuild(ctx, "acme", src); err == nil {
			t.Fatal("Rebuild accepted an empty vector")
		}

		assertOriginalVectors(t, idx, replaced, kept, old)
	})

	t.Run("RebuildIsTenantScoped", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()
		keep := insert(t, idx, "other", unit([]float32{1, 0, 0, 0}))

		src := &fixedSource{}
		src.add(id.New(), unit([]float32{0, 1, 0, 0}))
		must(t, idx.Rebuild(ctx, "acme", src))

		hits, err := idx.Search(ctx, "other", unit([]float32{1, 0, 0, 0}), 10, nil)
		must(t, err)
		if len(hits) != 1 || hits[0].ID != keep {
			t.Fatal("rebuilding one tenant disturbed another")
		}
	})

	t.Run("FilterExcludesRejectedIDs", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()
		q := unit([]float32{1, 0, 0, 0})
		rejected := insert(t, idx, "acme", q)
		kept := insert(t, idx, "acme", unit([]float32{0.9, 0.4, 0, 0}))

		hits, err := idx.Search(ctx, "acme", q, 10, func(rid id.ID) bool { return rid != rejected })
		must(t, err)
		if len(hits) != 1 || hits[0].ID != kept {
			t.Fatalf("the filter did not exclude the rejected id: %v", hits)
		}

		// A filter that rejects everything yields nothing, rather than falling
		// back to unfiltered results.
		hits, err = idx.Search(ctx, "acme", q, 10, func(id.ID) bool { return false })
		must(t, err)
		if len(hits) != 0 {
			t.Fatalf("a filter rejecting everything returned %d hits", len(hits))
		}
	})

	// k counts results, not candidates. An index that applied the filter after
	// truncating to k would return fewer results than it has, silently.
	t.Run("KCountsResultsAfterFiltering", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()
		q := unit([]float32{1, 0, 0, 0})

		var rejected []id.ID
		for range 5 {
			rejected = append(rejected, insert(t, idx, "acme", q))
		}
		for range 3 {
			insert(t, idx, "acme", unit([]float32{0, 1, 0, 0}))
		}

		reject := make(map[id.ID]bool, len(rejected))
		for _, r := range rejected {
			reject[r] = true
		}
		hits, err := idx.Search(ctx, "acme", q, 3, func(rid id.ID) bool { return !reject[rid] })
		must(t, err)
		if len(hits) != 3 {
			t.Fatalf("k=3 with five nearer candidates filtered out returned %d hits", len(hits))
		}
	})

	t.Run("StatsCountTheTenantsVectors", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()
		for range 4 {
			insert(t, idx, "acme", randomUnit())
		}
		insert(t, idx, "other", randomUnit())

		s, err := idx.Stats(ctx, "acme")
		must(t, err)
		if s.Vectors != 4 {
			t.Fatalf("Stats reports %d vectors for acme, want 4", s.Vectors)
		}
		if s.Dim != Dim {
			t.Fatalf("Stats reports dimension %d, want %d", s.Dim, Dim)
		}
	})

	// An index with nothing wrong with it must say so, and must say so for a
	// tenant it has never heard of. The interesting half of Health belongs to
	// the implementation that can be damaged; what belongs here is that the
	// question is answerable at all, and cheaply, since the query layer asks it
	// on every search.
	t.Run("AnUndamagedIndexReportsHealthy", func(t *testing.T) {
		idx := open(t)
		ctx := context.Background()

		h, err := idx.Health(ctx, "acme")
		must(t, err)
		if h.Degraded {
			t.Fatalf("a fresh index reports degraded: %s", h.Reason)
		}

		insert(t, idx, "acme", randomUnit())
		h, err = idx.Health(ctx, "acme")
		must(t, err)
		if h.Degraded {
			t.Fatalf("an index with one vector reports degraded: %s", h.Reason)
		}
		if _, err := idx.Health(ctx, ""); err == nil {
			t.Fatal("Invariant 1: health is a per-tenant question and has no unscoped form")
		}
	})

	t.Run("AnUnscopedSearchIsRefused", func(t *testing.T) {
		idx := open(t)
		if _, err := idx.Search(context.Background(), "", randomUnit(), 10, nil); err == nil {
			t.Fatal("Invariant 1: an index has no unscoped read path")
		}
	})

	t.Run("AQueryOfTheWrongWidthIsRefused", func(t *testing.T) {
		idx := open(t)
		insert(t, idx, "acme", randomUnit())
		if _, err := idx.Search(context.Background(), "acme", []float32{1, 0}, 10, nil); err == nil {
			t.Fatal("a query of the wrong width was accepted; it means a vector from another model reached the index")
		}
	})

	t.Run("KMustBePositive", func(t *testing.T) {
		idx := open(t)
		if _, err := idx.Search(context.Background(), "acme", randomUnit(), 0, nil); err == nil {
			t.Fatal("k=0 was accepted")
		}
	})
}

// --- helpers ---------------------------------------------------------------

func insert(t *testing.T, idx vector.Index, tn tenant.ID, v []float32) id.ID {
	t.Helper()
	rid := id.New()
	if err := idx.Insert(context.Background(), tn, rid, v); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	return rid
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertOriginalVectors(t *testing.T, idx vector.Index, replaced, kept id.ID, old []float32) {
	t.Helper()
	hits, err := idx.Search(context.Background(), "acme", old, 10, nil)
	must(t, err)
	if len(hits) != 2 || hits[0].ID != replaced || hits[0].Distance > 1e-5 {
		t.Fatalf("vectors changed after failed rebuild: %v", hits)
	}
	foundKept := false
	for _, h := range hits {
		foundKept = foundKept || h.ID == kept
	}
	if !foundKept {
		t.Fatal("failed rebuild removed an existing vector")
	}
}

// fixedSource is a deterministic vector.Source. It is deliberately not random:
// a rebuild test that produced different vectors on each call could not assert
// that rebuilding twice changes nothing.
type fixedSource struct {
	ids  []id.ID
	vecs [][]float32
}

type failingSource struct{ fixedSource }

func (s *failingSource) Scan(ctx context.Context, t tenant.ID, fn func(id.ID, []float32) error) error {
	if err := s.fixedSource.Scan(ctx, t, fn); err != nil {
		return err
	}
	return ErrSourceFailed
}

func (s *fixedSource) add(rid id.ID, v []float32) {
	s.ids = append(s.ids, rid)
	s.vecs = append(s.vecs, v)
}

func (s *fixedSource) Scan(_ context.Context, _ tenant.ID, fn func(id.ID, []float32) error) error {
	for i, rid := range s.ids {
		if err := fn(rid, s.vecs[i]); err != nil {
			return err
		}
	}
	return nil
}

// ErrSourceFailed is what a failing source returns, for implementations that
// want to assert a rebuild leaves the index untouched.
var ErrSourceFailed = errors.New("the rebuild source failed")

// seed is a plain linear congruential generator rather than math/rand, so that
// running the suite twice compares like with like and a failure is
// reproducible from the test output alone.
var seed uint64 = 0x9E3779B97F4A7C15

func randomUnit() []float32 {
	v := make([]float32, Dim)
	for i := range v {
		seed = seed*6364136223846793005 + 1442695040888963407
		v[i] = float32(int64(seed>>33))/float32(1<<30) - 1
	}
	return unit(v)
}

func unit(v []float32) []float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		v[0] = 1
		return v
	}
	inv := float32(1 / math.Sqrt(sum))
	out := make([]float32, len(v))
	for i := range v {
		out[i] = v[i] * inv
	}
	return out
}
