package hnsw_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/hnsw"
)

// Vectors of another width and what degradation means for them: the majority
// width decides, an unplaceable vector keeps the tenant degraded without asking
// for a rebuild that cannot help, and only repairable damage schedules one.

// A canonical vector of another width than the rest of the tenant's is a
// corrupt or hand-edited row, and it must cost that one record rather than the
// tenant: the vector arena holds a single width, so the row cannot be placed,
// and every path that materialises a graph has to leave it out instead of
// carrying it forward as work it will later fail or panic on.
func TestReloadSkipsAVectorOfAnotherWidthInsteadOfPanicking(t *testing.T) {
	ctx := context.Background()

	// The row still has its node record, so materialisation meets it while
	// placing nodes. Before this was fixed the id stayed in the pending list
	// its vector had just been removed from, and the first search after a
	// restart dereferenced the missing map entry.
	t.Run("ANodeRecordsVectorChangedWidth", func(t *testing.T) {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		idx := newIndex(t, kv, hnsw.Options{})
		keep, odd := seqID(1), seqID(2)
		if err := idx.Insert(ctx, "acme", keep, []float32{1, 0}); err != nil {
			t.Fatal(err)
		}
		if err := idx.Insert(ctx, "acme", odd, []float32{0, 1}); err != nil {
			t.Fatal(err)
		}
		putWidened(t, kv, odd)

		reopened := newIndex(t, kv, hnsw.Options{})
		hits, err := reopened.Search(ctx, "acme", []float32{1, 0}, 10, nil)
		if err != nil {
			t.Fatalf("a store holding one vector of another width refused to answer: %v", err)
		}
		if len(hits) != 1 || hits[0].ID != keep {
			t.Fatalf("search returned %v, want only the record whose width the arena holds", hits)
		}
		assertDegraded(ctx, t, reopened, "acme")
	})

	// The same row with no node record — the ordinary state after a crash
	// between a commit and its asynchronous index write. Here materialisation
	// meets it in the pending list, and g.insert would refuse it with an error
	// that failed the whole tenant's load.
	t.Run("APendingVectorHasAnotherWidth", func(t *testing.T) {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		idx := newIndex(t, kv, hnsw.Options{})
		keep := seqID(1)
		if err := idx.Insert(ctx, "acme", keep, []float32{1, 0}); err != nil {
			t.Fatal(err)
		}
		putWidened(t, kv, seqID(2))

		reopened := newIndex(t, kv, hnsw.Options{})
		hits, err := reopened.Search(ctx, "acme", []float32{1, 0}, 10, nil)
		if err != nil {
			t.Fatalf("a pending vector of another width refused the tenant: %v", err)
		}
		if len(hits) != 1 || hits[0].ID != keep {
			t.Fatalf("search returned %v, want only the record whose width the arena holds", hits)
		}
		assertDegraded(ctx, t, reopened, "acme")
	})

	// A rebuild consumes the same pending work, so one bad row must not fail
	// the rebuild that exists to repair the tenant. The width is the first
	// pending vector's, because no node record placed one.
	t.Run("ARebuildDropsAPendingVectorOfAnotherWidth", func(t *testing.T) {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		keep := putNarrow(t, kv, 1, 2, 3)
		putWidened(t, kv, seqID(9))

		idx := newIndex(t, kv, hnsw.Options{})
		if err := idx.Rebuild(ctx, "acme", vector.NewStore(kv)); err != nil {
			t.Fatalf("one vector of another width failed the whole rebuild: %v", err)
		}
		hits, err := idx.Search(ctx, "acme", []float32{0, 1}, 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) != len(keep) {
			t.Fatalf("the rebuilt index returned %d hits, want the %d records whose width it holds",
				len(hits), len(keep))
		}
		// The bad row is still canonical, so a tenant materialised afresh from
		// the rebuilt store still says it wants attention.
		assertDegraded(ctx, t, newIndex(t, kv, hnsw.Options{}), "acme")
	})

	// The odd row sorts *first* and no node record places anything, which is
	// the state a rebuild materialises once replace has cleared them. A
	// tenant's width has to be the width most of its vectors have: while the
	// first row in key order settled it, this one corrupt record made every
	// good row the mismatch, so the tenant kept a single vector and answered
	// every ordinary query — asked at the width its records actually have —
	// with an error.
	t.Run("AnOddWidthRowSortingFirstDoesNotDecideTheWidth", func(t *testing.T) {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		putWidened(t, kv, seqID(0))
		keep := putNarrow(t, kv, 1, 2, 3)

		reopened := newIndex(t, kv, hnsw.Options{})
		hits, err := reopened.Search(ctx, "acme", []float32{0, 1}, 10, nil)
		if err != nil {
			t.Fatalf("an odd-width row sorting first refused the whole tenant: %v", err)
		}
		if len(hits) != len(keep) {
			t.Fatalf("search returned %d hits, want the %d records that share the tenant's width",
				len(hits), len(keep))
		}
		// The odd row is counted, and counted as the kind of damage it is: a
		// tenant that merely said "degraded" would also pass if the reason
		// were the repairable one, which would then schedule rebuilds for ever.
		h := assertDegraded(ctx, t, reopened, "acme")
		if !contains(h.Reason, "1 of this tenant's canonical vectors") || !contains(h.Reason, "another width") {
			t.Fatalf("the reason does not say that one row is of another width: %q", h.Reason)
		}
	})

	// The same store through a rebuild. replace drains and rewrites every
	// canonical vector including the odd one, so the rebuild's own
	// materialisation meets it first in key order with no node records at all.
	t.Run("ARebuildIsNotDecidedByAnOddWidthRowSortingFirst", func(t *testing.T) {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		putWidened(t, kv, seqID(0))
		keep := putNarrow(t, kv, 1, 2, 3)

		idx := newIndex(t, kv, hnsw.Options{})
		if err := idx.Rebuild(ctx, "acme", vector.NewStore(kv)); err != nil {
			t.Fatalf("an odd-width row sorting first failed the whole rebuild: %v", err)
		}
		hits, err := idx.Search(ctx, "acme", []float32{0, 1}, 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) != len(keep) {
			t.Fatalf("the rebuilt index returned %d hits, want the %d records that share the tenant's width",
				len(hits), len(keep))
		}
		h := assertDegraded(ctx, t, idx, "acme")
		if !contains(h.Reason, "1 of this tenant's canonical vectors") || !contains(h.Reason, "another width") {
			t.Fatalf("the reason does not say that one row is of another width: %q", h.Reason)
		}
	})
}

// A rebuild is the repair, so what it leaves behind is what the tenant is. A
// vector of another width survives one — replace rewrites the row and the arena
// still cannot hold it — so the rebuilt tenant must go on saying so, and must
// not ask for the rebuild that has just failed to fix it. Both halves were
// wrong: residentForRebuild discarded the count and markRebuilding cleared the
// flag, so a repair that had silently dropped a record reported a healthy
// tenant.
func TestARebuildThatCouldNotPlaceAVectorStaysDegradedAndAsksForNoRepair(t *testing.T) {
	ctx := context.Background()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	putNarrow(t, kv, 1, 2, 3)
	putWidened(t, kv, seqID(9))

	var scheduled []string
	idx := newIndex(t, kv, hnsw.Options{Rebuilds: vector.RebuilderFunc(func(_ tenant.ID, reason string) {
		scheduled = append(scheduled, reason)
	})})
	if err := idx.Rebuild(ctx, "acme", vector.NewStore(kv)); err != nil {
		t.Fatal(err)
	}

	// The same index, not a fresh one: a reopened index materialises the
	// tenant again and would report the damage whatever the rebuild recorded,
	// which is exactly the lie this test exists to catch.
	h, err := idx.Health(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if !h.Degraded {
		t.Fatal("a rebuild that could not place one of the tenant's vectors reported a healthy tenant")
	}
	if !contains(h.Reason, "another width") {
		t.Fatalf("the reason does not name the width problem an operator has to act on: %q", h.Reason)
	}
	if len(scheduled) != 0 {
		t.Fatalf("a width mismatch no rebuild repairs scheduled %d of them: %v", len(scheduled), scheduled)
	}
}

// Only corruption a rebuild repairs may ask for one. An unreadable node record
// is repairable: the canonical vector behind it is intact, the load re-inserts
// it, and a rebuild restores the links that pointed into it. A vector the arena
// cannot hold is not: re-reading the same row produces the same mismatch, so
// scheduling a rebuild for it is a repair that never resolves and that then
// runs for ever, at whatever rate the server throttles it to.
func TestOnlyRepairableCorruptionSchedulesARebuild(t *testing.T) {
	ctx := context.Background()

	t.Run("AnUnreadableNodeRecordAsksForARebuild", func(t *testing.T) {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		idx := newIndex(t, kv, hnsw.Options{})
		for i, rid := range []id.ID{seqID(1), seqID(2), seqID(3)} {
			if err := idx.Insert(ctx, "acme", rid, []float32{float32(i), 1}); err != nil {
				t.Fatal(err)
			}
		}
		damageFirstNodeRecord(ctx, t, kv)

		var scheduled []string
		reopened := newIndex(t, kv, hnsw.Options{Rebuilds: vector.RebuilderFunc(func(_ tenant.ID, reason string) {
			scheduled = append(scheduled, reason)
		})})
		assertDegraded(ctx, t, reopened, "acme")
		if len(scheduled) != 1 {
			t.Fatalf("an unreadable node record scheduled %d rebuilds, want exactly one: %v",
				len(scheduled), scheduled)
		}
	})

	t.Run("AVectorOfAnotherWidthDoesNot", func(t *testing.T) {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		putNarrow(t, kv, 1, 2, 3)
		putWidened(t, kv, seqID(9))

		var scheduled []string
		reopened := newIndex(t, kv, hnsw.Options{Rebuilds: vector.RebuilderFunc(func(_ tenant.ID, reason string) {
			scheduled = append(scheduled, reason)
		})})
		assertDegraded(ctx, t, reopened, "acme")
		if len(scheduled) != 0 {
			t.Fatalf("a width mismatch no rebuild repairs scheduled %d of them: %v",
				len(scheduled), scheduled)
		}
	})
}

// damageFirstNodeRecord overwrites one of the tenant's node records with bytes
// the decoder refuses. That is the repairable kind of damage: the canonical
// vector the record named is untouched.
func damageFirstNodeRecord(ctx context.Context, t *testing.T, kv storage.KV) {
	t.Helper()
	lower, upper := keys.SpaceRange("acme", tenant.DefaultNamespace, keys.SpaceVectorIndex)
	it := kv.NewIterator(lower, upper)
	if !it.First() {
		_ = it.Close()
		t.Fatal("the tenant has no node records to damage; the test is not testing anything")
	}
	k := append([]byte(nil), it.Key()...)
	_ = it.Close()
	if err := kv.Set(ctx, k, []byte("not a node record")); err != nil {
		t.Fatal(err)
	}
}

// putNarrow writes one two-dimensional canonical vector per n, at the id seqID
// gives that n, and returns those ids in key order. The coordinates differ by
// row so that a search can tell the records apart.
func putNarrow(t *testing.T, kv storage.KV, ns ...int) []id.ID {
	t.Helper()
	store := vector.NewStore(kv)
	ids := make([]id.ID, 0, len(ns))
	for i, n := range ns {
		rid := seqID(n)
		if err := store.Put(context.Background(), "acme", tenant.DefaultNamespace, rid,
			&vector.Vector{ModelID: vector.UnknownModel, Dim: 2, Values: []float32{float32(i), 1}}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, rid)
	}
	return ids
}

// putWidened writes a canonical vector one wider than the two-dimensional ones
// these tests index, over whatever rid already held. The model id is the one
// Insert stamps when no model is configured, so that the model guard — which
// refuses a tenant outright — is not what the test ends up measuring.
func putWidened(t *testing.T, kv storage.KV, rid id.ID) {
	t.Helper()
	if err := vector.NewStore(kv).Put(context.Background(), "acme", tenant.DefaultNamespace, rid,
		&vector.Vector{ModelID: vector.UnknownModel, Dim: 3, Values: []float32{0, 1, 0}}); err != nil {
		t.Fatal(err)
	}
}

// seqID returns a well-formed v7 id whose bytes sort by n, so that a test can
// decide the key order a materialisation walks. Two ids from id.New in the same
// millisecond differ only in their random bits and would not settle it.
func seqID(n int) id.ID {
	var rid id.ID
	rid[6] = 0x70 // the version nibble
	rid[8] = 0x80 // the variant bits
	rid[15] = byte(n)
	return rid
}

// assertDegraded checks that the tenant says it wants attention and says why,
// and returns the health so a caller can go on to check *what* it says: the two
// kinds of damage have different remedies and must not be reported alike.
func assertDegraded(ctx context.Context, t *testing.T, idx *hnsw.Index, tid tenant.ID) vector.Health {
	t.Helper()
	h, err := idx.Health(ctx, tid)
	if err != nil {
		t.Fatal(err)
	}
	if !h.Degraded {
		t.Fatal("a tenant holding a vector the index could not place reported itself healthy")
	}
	if h.Reason == "" {
		t.Fatal("Health says degraded and does not say why")
	}
	return h
}
