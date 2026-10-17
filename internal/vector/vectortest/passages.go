package vectortest

import (
	"bytes"
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

// The passage half of the contract (openspec/changes/passage-vectors, design
// decisions 4 and 6). A memory's entries are its passages, or its canonical
// vector when it has none; a search returns memories, each ranked by its
// nearest entry, and k counts memories.
//
// These run against rows written two ways: through Insert, and directly into
// the store the way the record repository writes them, behind the index's
// back. The second is what a fresh index meets after a restart, and the only
// way to hold an index to the entry rule rather than to whatever it was told.

// passageMemory writes a memory as the production write path does: its
// canonical vector (the document vector, which is no entry) and its passage
// rows, in one transaction, without telling any index.
func passageMemory(t *testing.T, kv storage.KV, tn tenant.ID, canonical []float32, passages ...[]float32) id.ID {
	t.Helper()
	rid := id.New()
	store := vector.NewStore(kv)
	rows := make([]vector.PassageRow, len(passages))
	for i, p := range passages {
		rows[i] = vector.PassageRow{Start: uint32(10 * i), End: uint32(10*i + 10),
			Vector: &vector.Vector{ModelID: vector.UnknownModel, Dim: len(p), Values: p}}
	}
	ctx := context.Background()
	must(t, txn.Do(ctx, kv, func(tx txn.Tx) error {
		if err := store.Stage(tx, tn, tenant.DefaultNamespace, rid,
			&vector.Vector{ModelID: vector.UnknownModel, Dim: len(canonical), Values: canonical}); err != nil {
			return err
		}
		return vector.StagePassages(tx, tn, tenant.DefaultNamespace, rid, rows)
	}))
	return rid
}

func entriesOf(vs ...[]float32) []vector.Entry {
	out := make([]vector.Entry, len(vs))
	for i, v := range vs {
		out[i] = vector.Entry{Passage: uint32(i), Vector: v}
	}
	return out
}

func distinct(t *testing.T, hits []vector.Hit) {
	t.Helper()
	seen := map[id.ID]bool{}
	for _, h := range hits {
		if seen[h.ID] {
			t.Fatalf("memory %s was returned twice: a search returns memories, not passages", h.ID)
		}
		seen[h.ID] = true
	}
}

func runPassages(t *testing.T, opener Opener) {
	fresh := func(t *testing.T) (storage.KV, vector.Index) {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		return kv, opener(t, kv)
	}

	t.Run("AMemoryIsRankedByItsNearestPassage", func(t *testing.T) {
		_, idx := fresh(t)
		ctx := context.Background()
		q := unit([]float32{1, 0, 0, 0})
		near := unit([]float32{0.96, 0.28, 0, 0})
		a := id.New()
		must(t, idx.Insert(ctx, "acme", a, entriesOf(
			unit([]float32{0, 1, 0, 0}), near, unit([]float32{0, 0, 1, 0}))))
		b := insert(t, idx, "acme", unit([]float32{0.8, 0.6, 0, 0}))
		insert(t, idx, "acme", unit([]float32{0, 0, 0, 1}))

		hits, err := idx.Search(ctx, "acme", q, 2, nil)
		must(t, err)
		if len(hits) != 2 || hits[0].ID != a || hits[1].ID != b {
			t.Fatalf("hits = %v, want the three-passage memory and then the single one", hits)
		}
		if hits[0].Passage != 1 {
			t.Fatalf("the memory was ranked by passage %d, want 1, its nearest", hits[0].Passage)
		}
		// Its distance is its nearest passage's: the passage that ranked it is
		// nearer the query than the single memory, as near was built to be.
		if hits[0].Distance >= hits[1].Distance {
			t.Fatalf("the memory's distance %v is not below the next memory's %v", hits[0].Distance, hits[1].Distance)
		}

		all, err := idx.Search(ctx, "acme", q, 10, nil)
		must(t, err)
		if len(all) != 3 {
			t.Fatalf("k=10 over three memories returned %d hits", len(all))
		}
		distinct(t, all)
	})

	// The case grouping in the executor would fail: one memory with a hundred
	// passages nearest the query, more than any search effort the index would
	// spend by default, must still leave room for the other memories.
	t.Run("KCountsMemoriesNotPassages", func(t *testing.T) {
		_, idx := fresh(t)
		ctx := context.Background()
		q := unit([]float32{1, 0, 0, 0})
		var many [][]float32
		for i := range 100 {
			many = append(many, unit([]float32{1, 0.001 * float32(i+1), 0, 0}))
		}
		crowd := id.New()
		must(t, idx.Insert(ctx, "acme", crowd, entriesOf(many...)))
		others := map[id.ID]bool{}
		for _, v := range [][]float32{{0.9, 0.43, 0, 0}, {0.8, 0.6, 0, 0}, {0.7, 0.71, 0, 0}} {
			others[insert(t, idx, "acme", unit(v))] = true
		}

		hits, err := idx.Search(ctx, "acme", q, 3, nil)
		must(t, err)
		if len(hits) != 3 {
			t.Fatalf("k=3 returned %d hits", len(hits))
		}
		distinct(t, hits)
		if hits[0].ID != crowd || hits[0].Passage != 0 {
			t.Fatalf("hit 0 = %v, want the crowded memory at its nearest passage, 0", hits[0])
		}
		for _, h := range hits[1:] {
			if !others[h.ID] {
				t.Fatalf("hit %v is not one of the other memories", h)
			}
		}
	})

	// A multi-passage memory's canonical vector is its document vector, the
	// mean its passages cover, and it is no entry: a whole-memory match must not
	// outrank the passage that holds the answer, or a memory beat by it.
	t.Run("AMultiPassageMemorysCanonicalVectorIsNoEntry", func(t *testing.T) {
		kv, _ := fresh(t)
		q := unit([]float32{1, 0, 0, 0})
		a := passageMemory(t, kv, "acme", q, unit([]float32{0, 1, 0, 0}), unit([]float32{0, 0, 1, 0}))
		b := passageMemory(t, kv, "acme", unit([]float32{0.9, 0.43, 0, 0}))

		idx := opener(t, kv)
		hits, err := idx.Search(context.Background(), "acme", q, 2, nil)
		must(t, err)
		if len(hits) != 2 || hits[0].ID != b || hits[1].ID != a {
			t.Fatalf("hits = %v: the memory whose canonical vector is the query ranked by it", hits)
		}
		if hits[1].Distance <= hits[0].Distance {
			t.Fatal("the multi-passage memory scored by its canonical vector")
		}
	})

	t.Run("EntriesWrittenBehindTheIndexAreFoundWhenItOpens", func(t *testing.T) {
		kv, idx := fresh(t)
		ctx := context.Background()
		q := unit([]float32{0, 0, 1, 0})
		insert(t, idx, "acme", unit([]float32{1, 0, 0, 0}))
		// Materialise the tenant, so the rows below are news to it.
		_, err := idx.Search(ctx, "acme", q, 1, nil)
		must(t, err)
		behind := passageMemory(t, kv, "acme", unit([]float32{0, 1, 0, 0}),
			unit([]float32{0, 1, 0, 0}), unit([]float32{0.1, 0, 0.99, 0}))

		again := opener(t, kv)
		hits, err := again.Search(ctx, "acme", q, 1, nil)
		must(t, err)
		if len(hits) != 1 || hits[0].ID != behind || hits[0].Passage != 1 {
			t.Fatalf("hits = %v, want passage 1 of the memory written behind the index", hits)
		}
	})

	// Whatever structure an index keeps of its own is derived. Losing all of it
	// is a rebuild at the next open, never a memory that exists and cannot be
	// found, and that holds for every passage of every memory.
	t.Run("LostIndexStructureIsRestoredFromTheEntries", func(t *testing.T) {
		kv, idx := fresh(t)
		ctx := context.Background()
		a := id.New()
		must(t, idx.Insert(ctx, "acme", a, entriesOf(
			unit([]float32{1, 0, 0, 0}), unit([]float32{0, 1, 0, 0}), unit([]float32{0, 0, 1, 0}))))
		insert(t, idx, "acme", unit([]float32{0, 0, 0, 1}))
		wipe(t, kv, "acme", keys.SpaceVectorIndex)

		again := opener(t, kv)
		for p, q := range [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}, {0, 0, 1, 0}} {
			hits, err := again.Search(ctx, "acme", unit(q), 1, nil)
			must(t, err)
			if len(hits) != 1 || hits[0].ID != a || hits[0].Passage != uint32(p) {
				t.Fatalf("query %d: hits = %v, want passage %d of the memory", p, hits, p)
			}
		}
	})

	t.Run("DeletingAMultiPassageMemoryRemovesEveryPassage", func(t *testing.T) {
		kv, idx := fresh(t)
		ctx := context.Background()
		q := unit([]float32{1, 0, 0, 0})
		gone := id.New()
		must(t, idx.Insert(ctx, "acme", gone, entriesOf(q, unit([]float32{0.9, 0.43, 0, 0}), q)))
		kept := insert(t, idx, "acme", unit([]float32{0, 1, 0, 0}))

		must(t, idx.Delete(ctx, "acme", gone))
		for _, ix := range []vector.Index{idx, opener(t, kv)} {
			hits, err := ix.Search(ctx, "acme", q, 10, nil)
			must(t, err)
			if len(hits) != 1 || hits[0].ID != kept {
				t.Fatalf("hits = %v after deleting a multi-passage memory, want only the other one", hits)
			}
		}
		lo, hi := keys.PassagesOfRange("acme", tenant.DefaultNamespace, gone)
		it := kv.NewIterator(lo, hi)
		defer func() { _ = it.Close() }()
		if it.First() {
			t.Fatal("a deleted memory's passage rows are still stored")
		}
	})

	t.Run("ReindexingWithFewerPassagesDropsTheRest", func(t *testing.T) {
		kv, idx := fresh(t)
		ctx := context.Background()
		q := unit([]float32{0, 0, 0, 1})
		a := id.New()
		must(t, idx.Insert(ctx, "acme", a, entriesOf(
			unit([]float32{1, 0, 0, 0}), unit([]float32{0, 1, 0, 0}), q)))
		b := insert(t, idx, "acme", unit([]float32{0, 0, 0.6, 0.8}))

		must(t, idx.Insert(ctx, "acme", a, entriesOf(unit([]float32{1, 0, 0, 0}), unit([]float32{0, 1, 0, 0}))))
		for _, ix := range []vector.Index{idx, opener(t, kv)} {
			hits, err := ix.Search(ctx, "acme", q, 1, nil)
			must(t, err)
			if len(hits) != 1 || hits[0].ID != b {
				t.Fatalf("hits = %v: a passage the memory no longer has still matched", hits)
			}
		}
		s, err := idx.Stats(ctx, "acme")
		must(t, err)
		if s.Vectors != 3 {
			t.Fatalf("Stats reports %d entries, want 3: two passages and one single memory", s.Vectors)
		}
	})

	t.Run("AFilterRejectsAMemoryWithAllItsPassages", func(t *testing.T) {
		_, idx := fresh(t)
		ctx := context.Background()
		q := unit([]float32{1, 0, 0, 0})
		rejected := id.New()
		must(t, idx.Insert(ctx, "acme", rejected, entriesOf(q, q, q)))
		kept := insert(t, idx, "acme", unit([]float32{0.5, 0.87, 0, 0}))
		hits, err := idx.Search(ctx, "acme", q, 5, func(rid id.ID) bool { return rid != rejected })
		must(t, err)
		if len(hits) != 1 || hits[0].ID != kept {
			t.Fatalf("hits = %v, want only the memory the filter admits", hits)
		}
	})
}

// wipe deletes every row one tenant holds in one space.
func wipe(t *testing.T, kv storage.KV, tn tenant.ID, s keys.Space) {
	t.Helper()
	lo, hi := keys.SpaceRange(tn, tenant.DefaultNamespace, s)
	it := kv.NewIterator(lo, hi)
	var doomed [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		doomed = append(doomed, bytes.Clone(it.Key()))
	}
	_ = it.Close()
	for _, k := range doomed {
		must(t, kv.Delete(context.Background(), k))
	}
}
