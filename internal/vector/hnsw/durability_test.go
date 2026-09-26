package hnsw_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/hnsw"
)

// The Rust phantom-resurrection defect, pinned (PROJECT_REVIEW §2.1 #4).
//
// Rust kept its deleted set in a memory-mapped entry that every checkpoint
// erased, so after a restart each hard-deleted vector came back, consumed index
// memory for ever, and let get_vector serve a stale embedding. This index has no
// deleted set to lose: a deleted memory has no canonical vector, and a node
// record naming a record with no canonical vector is dropped when the tenant is
// materialised.
func TestDeletedVectorsStayDeletedAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")

	kv := openPebble(t, path)
	idx := newIndex(t, kv, hnsw.Options{})
	ctx := context.Background()

	rng := newTestRNG(11)
	ids := make([]id.ID, 200)
	for i := range ids {
		ids[i] = id.New()
		if err := idx.Insert(ctx, "acme", ids[i], rng.unit(8)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	// Half are deleted the tidy way, through the index, and half by removing
	// only the canonical vector — which is what a committed transaction
	// followed by a crash leaves behind, and the half that actually reproduces
	// Rust's phantom. Deleting only the tidy way passes even against an index
	// that never drops an orphaned node, which is the whole defect.
	gone := map[id.ID]bool{}
	store := vector.NewStore(kv)
	for i, rid := range ids[:50] {
		if i%2 == 0 {
			if err := idx.Delete(ctx, "acme", rid); err != nil {
				t.Fatalf("delete: %v", err)
			}
		} else if err := store.Delete(ctx, "acme", tenant.DefaultNamespace, rid); err != nil {
			t.Fatalf("deleting the canonical vector: %v", err)
		}
		gone[rid] = true
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Restart.
	kv = openPebble(t, path)
	idx = newIndex(t, kv, hnsw.Options{})

	s, err := idx.Stats(ctx, "acme")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if s.Vectors != 150 {
		t.Fatalf("the reopened index holds %d vectors, want 150", s.Vectors)
	}
	for range 50 {
		hits, err := idx.Search(ctx, "acme", rng.unit(8), 25, nil)
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		for _, h := range hits {
			if gone[h.ID] {
				t.Fatalf("deleted vector %s came back after a restart", h.ID)
			}
		}
	}
}

// The crash window the design exists to close: the record's transaction commits
// — taking the canonical vector with it — and the process dies before the
// asynchronous index write. Nothing tidied the graph, and the node must still
// not resurrect.
func TestACrashBetweenCommitAndIndexWriteStillDeletes(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	idx := newIndex(t, kv, hnsw.Options{})
	ctx := context.Background()

	rng := newTestRNG(3)
	ids := make([]id.ID, 60)
	for i := range ids {
		ids[i] = id.New()
		if err := idx.Insert(ctx, "acme", ids[i], rng.unit(8)); err != nil {
			t.Fatal(err)
		}
	}

	// Delete the canonical vector behind the index's back, exactly as a
	// committed transaction followed by a crash would leave things: the node
	// record for it is still on disk, complete and readable.
	victim := ids[7]
	store := vector.NewStore(kv)
	if err := store.Delete(ctx, "acme", tenant.DefaultNamespace, victim); err != nil {
		t.Fatal(err)
	}

	// A fresh index materialises from what is on disk.
	restarted := newIndex(t, kv, hnsw.Options{})
	s, err := restarted.Stats(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if s.Vectors != 59 {
		t.Fatalf("the materialised index holds %d vectors, want 59", s.Vectors)
	}
	for range 20 {
		hits, err := restarted.Search(ctx, "acme", rng.unit(8), 20, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range hits {
			if h.ID == victim {
				t.Fatal("a record whose canonical vector was deleted resurrected as a phantom")
			}
		}
	}
}

// The other direction of the same reconciliation: the record committed and the
// asynchronous index write was lost. The memory must not be permanently
// unfindable.
func TestACanonicalVectorWithNoNodeIsIndexedOnLoad(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ctx := context.Background()

	store := vector.NewStore(kv)
	rng := newTestRNG(5)
	want := id.New()
	q := rng.unit(8)
	for range 30 {
		if err := store.Put(ctx, "acme", tenant.DefaultNamespace, id.New(), &vector.Vector{
			ModelID: "test", Dim: 8, Values: rng.unit(8),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Put(ctx, "acme", tenant.DefaultNamespace, want, &vector.Vector{
		ModelID: "test", Dim: 8, Values: q,
	}); err != nil {
		t.Fatal(err)
	}

	idx := newIndex(t, kv, hnsw.Options{})
	hits, err := idx.Search(ctx, "acme", q, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].ID != want {
		t.Fatalf("a canonical vector with no node record was not indexed on load: %v", hits)
	}

	// And the repair was persisted, not merely done in memory: a second index
	// over the same store finds it without redoing the work.
	again := newIndex(t, kv, hnsw.Options{})
	s, err := again.Stats(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if s.Vectors != 31 {
		t.Fatalf("the repaired index holds %d vectors, want 31", s.Vectors)
	}
}

// A corpus embedded by one model and queried by another returns confident
// nonsense, and nothing about the vectors' width or norm gives it away. It is
// refused rather than degraded, because no rebuild can fix it: the canonical
// data is what is wrong.
func TestModelMismatchIsRefused(t *testing.T) {
	ctx := context.Background()

	t.Run("TheServerRunsADifferentModel", func(t *testing.T) {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		store := vector.NewStore(kv)
		rng := newTestRNG(9)
		for range 5 {
			if err := store.Put(ctx, "acme", tenant.DefaultNamespace, id.New(), &vector.Vector{
				ModelID: "all-MiniLM-L6-v2", Dim: 8, Values: rng.unit(8),
			}); err != nil {
				t.Fatal(err)
			}
		}

		idx := newIndex(t, kv, hnsw.Options{ModelID: "bge-small-en"})
		_, err := idx.Search(ctx, "acme", rng.unit(8), 3, nil)
		if err == nil {
			t.Fatal("a corpus embedded with another model was searched without complaint")
		}
		for _, want := range []string{"all-MiniLM-L6-v2", "bge-small-en"} {
			if !contains(err.Error(), want) {
				t.Fatalf("the refusal does not name %q: %v", want, err)
			}
		}
	})

	t.Run("TheCorpusHoldsTwoModels", func(t *testing.T) {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		store := vector.NewStore(kv)
		rng := newTestRNG(13)
		for i := range 6 {
			model := "model-a"
			if i == 4 {
				model = "model-b"
			}
			if err := store.Put(ctx, "acme", tenant.DefaultNamespace, id.New(), &vector.Vector{
				ModelID: model, Dim: 8, Values: rng.unit(8),
			}); err != nil {
				t.Fatal(err)
			}
		}
		idx := newIndex(t, kv, hnsw.Options{})
		if _, err := idx.Search(ctx, "acme", rng.unit(8), 3, nil); err == nil {
			t.Fatal("a tenant holding vectors from two models was searched as though it were one space")
		}
	})
}

// A damaged index must answer, must say the answer may be incomplete, and must
// get a rebuild scheduled. What it must never do is return an empty result set
// that reads exactly like a tenant with no memories.
func TestUnhealthyIndexReportsTruncated(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ctx := context.Background()

	idx := newIndex(t, kv, hnsw.Options{})
	rng := newTestRNG(17)
	for range 100 {
		if err := idx.Insert(ctx, "acme", id.New(), rng.unit(8)); err != nil {
			t.Fatal(err)
		}
	}

	// Damage a third of the node records in place.
	lower, upper := keys.SpaceRange("acme", tenant.DefaultNamespace, keys.SpaceVectorIndex)
	var doomed [][]byte
	it := kv.NewIterator(lower, upper)
	i := 0
	for ok := it.First(); ok; ok = it.Next() {
		if i%3 == 0 {
			doomed = append(doomed, append([]byte(nil), it.Key()...))
		}
		i++
	}
	_ = it.Close()
	if len(doomed) < 10 {
		t.Fatalf("only %d node records to damage; the test is not testing anything", len(doomed))
	}
	for _, k := range doomed {
		if err := kv.Set(ctx, k, []byte("not a node record")); err != nil {
			t.Fatal(err)
		}
	}

	var scheduled []tenant.ID
	damaged := newIndex(t, kv, hnsw.Options{
		Rebuilds: vector.RebuilderFunc(func(tn tenant.ID, reason string) {
			scheduled = append(scheduled, tn)
			if reason == "" {
				t.Error("a rebuild was scheduled with no reason an operator could act on")
			}
		}),
	})

	hits, err := damaged.Search(ctx, "acme", rng.unit(8), 10, nil)
	if err != nil {
		t.Fatalf("a damaged index refused to answer: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("a damaged index returned nothing, which reads exactly like an empty corpus")
	}

	h, err := damaged.Health(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if !h.Degraded {
		t.Fatal("a damaged index reported itself healthy")
	}
	if h.Reason == "" {
		t.Fatal("Health says degraded and does not say why")
	}
	if len(scheduled) != 1 || scheduled[0] != "acme" {
		t.Fatalf("rebuilds scheduled = %v, want exactly one for acme", scheduled)
	}

	// Every memory is still findable: the damaged records were dropped and
	// their vectors re-indexed from the canonical rows.
	s, err := damaged.Stats(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if s.Vectors != 100 {
		t.Fatalf("the repaired index holds %d vectors, want 100", s.Vectors)
	}
}

// --- helpers ---------------------------------------------------------------

func openPebble(t *testing.T, path string) storage.KV {
	t.Helper()
	kv, err := pebble.Open(path, pebble.Options{})
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	return kv
}

func newIndex(t *testing.T, kv storage.KV, opts hnsw.Options) *hnsw.Index {
	t.Helper()
	if opts.Metric == 0 {
		opts.Metric = distance.L2
	}
	idx, err := hnsw.New(kv, opts)
	if err != nil {
		t.Fatalf("hnsw.New: %v", err)
	}
	return idx
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
