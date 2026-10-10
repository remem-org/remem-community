package attr_test

import (
	"context"
	"errors"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/txn"
)

// A commit that failed may still have become visible, so closing it moves the
// generation exactly as acknowledging it would.
//
// Pebble applies a batch unsynced inside the store's write lock and then syncs it
// separately (`internal/storage/pebble.batch.Commit` calls apply and then
// `LogData(nil, Sync)`), so a sync failure returns an error from a commit whose
// writes are already visible to every snapshot taken afterwards. The row cache
// cannot tell that apart from a commit that failed its conditions and wrote
// nothing — both reach it as a Close and nothing else — so it must treat the
// visibility of a closed-without-committing write as unknown and move the
// generation on either end.
//
// Treating the close as "nothing became visible" is the bug: the generation stays
// where it was, so a reader opened after the failed commit shares it with a reader
// opened before, their snapshots straddle the write, and whichever of them reads
// first puts its own row where the other is served it.
func TestAFailedCommitThatMayBeVisibleStillMovesTheGeneration(t *testing.T) {
	ctx, kv, ix, cache := newCachingIndexer(t)
	rid := id.New()
	rec := sampleRecord(rid)
	rec.Fields.Importance = 0.25
	stage(t, ctx, kv, ix, rid, rec)

	// A reader whose snapshot predates the failed commit, so it holds 0.25.
	old := openCachedReader(t, kv, cache)
	defer old.Report()

	before := cache.Generation(testTenant)
	rec.Fields.Importance = 0.75
	func() {
		tx := txn.New(visiblyFailingKV{kv})
		defer tx.Close()
		if err := ix.Stage(ctx, tx, testTenant, testNS, rid, rec); err != nil {
			t.Fatalf("staging: %v", err)
		}
		if err := tx.Commit(ctx); err == nil {
			t.Fatal("the injected sync failure was not reported")
		}
	}()
	if g := cache.Generation(testTenant); g == before {
		t.Errorf("a commit that failed after its writes became visible left the "+
			"generation at %d; a reader opened before it and one opened after it now "+
			"share a generation while their snapshots straddle the write", g)
	}

	// A reader opened after the failed commit, whose snapshot therefore holds the
	// 0.75 the failed commit made visible. Two lookups each, interleaved, because
	// neither reader may populate an entry the other is then served: the first
	// lookup can only miss, so it is the second that reads what the cache retained.
	fresh := openCachedReader(t, kv, cache)
	defer fresh.Report()
	for pass := range 2 {
		got, err := fresh.Row(ctx, testTenant, testNS, rid)
		if err != nil {
			t.Fatalf("pass %d, the fresh reader: %v", pass, err)
		}
		if v, _ := got.Get(attr.SlotImportance); v != attr.F32(0.75) {
			t.Errorf("pass %d: the fresh reader read importance %s; its own snapshot "+
				"holds the 0.75 the failed commit made visible", pass, v)
		}
		got, err = old.Row(ctx, testTenant, testNS, rid)
		if err != nil {
			t.Fatalf("pass %d, the older reader: %v", pass, err)
		}
		if v, _ := got.Get(attr.SlotImportance); v != attr.F32(0.25) {
			t.Errorf("pass %d: a reader on the older snapshot read importance %s, want "+
				"its own 0.25", pass, v)
		}
	}
}

// visiblyFailingKV hands out batches that apply their writes and then report a
// failure, which is what a Pebble commit whose sync fails does: the apply is
// already visible to new snapshots and the error says nothing about that.
type visiblyFailingKV struct {
	storage.KV
}

func (k visiblyFailingKV) NewBatch() storage.Batch {
	return visiblyFailingBatch{k.KV.NewBatch()}
}

type visiblyFailingBatch struct {
	storage.Batch
}

// Commit applies the batch unsynced — so it is visible — and then fails, as a
// batch whose separate sync returns an error does.
func (b visiblyFailingBatch) Commit(ctx context.Context, _ bool) error {
	if err := b.Batch.Commit(ctx, false); err != nil {
		return err
	}
	return errors.New("injected sync failure after the batch was applied")
}
