package attr_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// A committed row write moves the tenant's generation, which is what retires
// every entry a reader of the older state had cached.
func TestACommittedRowWriteMovesTheTenantsGeneration(t *testing.T) {
	ctx, kv, ix, cache := newCachingIndexer(t)
	rid := id.New()

	before := cache.Generation(testTenant)
	stage(t, ctx, kv, ix, rid, sampleRecord(rid))
	after := cache.Generation(testTenant)
	if after == before {
		t.Fatalf("a committed Stage left the generation at %d", after)
	}

	// And a drop does too: it is a row write like any other.
	tx := txn.New(kv)
	defer tx.Close()
	if err := ix.StageDrop(ctx, tx, testTenant, testNS, rid); err != nil {
		t.Fatalf("dropping: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if g := cache.Generation(testTenant); g == after {
		t.Errorf("a committed StageDrop left the generation at %d", g)
	}
}

// A transaction that never commits invalidates all the same, because the cache
// cannot know that nothing of it became visible: a close is all it is told, and a
// close follows a sync failure over an applied batch as readily as a rollback that
// wrote nothing. See [attr.RowCache] and
// TestAFailedCommitThatMayBeVisibleStillMovesTheGeneration.
func TestARolledBackRowWriteMovesTheGenerationToo(t *testing.T) {
	ctx, kv, ix, cache := newCachingIndexer(t)
	rid := id.New()

	before := cache.Generation(testTenant)
	func() {
		tx := txn.New(kv)
		defer tx.Close()
		if err := ix.Stage(ctx, tx, testTenant, testNS, rid, sampleRecord(rid)); err != nil {
			t.Fatalf("staging: %v", err)
		}
		// No commit: the transaction is abandoned.
	}()
	if g := cache.Generation(testTenant); g == before {
		t.Errorf("an abandoned transaction left the generation at %d", g)
	}

	// What the rollback costs is one generation, not the cache: a reader opened
	// after it caches rows of this tenant exactly as before.
	other := id.New()
	stage(t, ctx, kv, ix, other, sampleRecord(other))
	r := openCachedReader(t, kv, cache)
	if _, err := r.Row(ctx, testTenant, testNS, other); err != nil {
		t.Fatalf("reading the other row: %v", err)
	}
	if h, _, _ := cache.Stats(); h != 0 {
		t.Fatalf("the first read of a row was a hit")
	}
	if _, err := r.Row(ctx, testTenant, testNS, other); err != nil {
		t.Fatalf("re-reading the other row: %v", err)
	}
	r.Report()
	if h, _, _ := cache.Stats(); h != 1 {
		t.Errorf("%d hits after re-reading one row through one reader, want 1", h)
	}
}

// A reader pinned to a snapshot taken before a write never sees the newer row,
// not even after a reader on the newer state has put it in the cache.
func TestAnOlderSnapshotNeverSeesARowANewerReaderCached(t *testing.T) {
	ctx, kv, ix, cache := newCachingIndexer(t)
	rid := id.New()
	rec := sampleRecord(rid)
	rec.Fields.Importance = 0.25
	stage(t, ctx, kv, ix, rid, rec)

	old := openCachedReader(t, kv, cache)
	defer old.Report()

	rec.Fields.Importance = 0.75
	stage(t, ctx, kv, ix, rid, rec)

	// A reader on the new state populates the cache with the new row.
	fresh := openCachedReader(t, kv, cache)
	defer fresh.Report()
	got, err := fresh.Row(ctx, testTenant, testNS, rid)
	if err != nil {
		t.Fatalf("the fresh reader: %v", err)
	}
	if v, _ := got.Get(attr.SlotImportance); v != attr.F32(0.75) {
		t.Fatalf("the fresh reader read importance %s, want 0.75", v)
	}

	// The older reader must still see its own state.
	got, err = old.Row(ctx, testTenant, testNS, rid)
	if err != nil {
		t.Fatalf("the older reader: %v", err)
	}
	if v, _ := got.Get(attr.SlotImportance); v != attr.F32(0.25) {
		t.Errorf("a reader on the older snapshot read importance %s, want its own 0.25", v)
	}
}

// A write that lands between a reader taking its generation and taking its
// snapshot leaves that reader with a generation that has moved on, so it reads
// everything from its own snapshot and caches nothing.
func TestAWriteBetweenTheGenerationAndTheSnapshotBypassesTheCache(t *testing.T) {
	ctx, kv, ix, cache := newCachingIndexer(t)
	rid := id.New()
	rec := sampleRecord(rid)
	rec.Fields.Importance = 0.25
	stage(t, ctx, kv, ix, rid, rec)

	// The reader's generation, taken first, as a paging session takes it.
	g0 := cache.Generation(testTenant)

	rec.Fields.Importance = 0.75
	stage(t, ctx, kv, ix, rid, rec)

	// Only now the snapshot, which therefore shows the newer row.
	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()
	lower, upper := keys.TenantRange(testTenant)
	r := attr.NewCachedReader(storage.NewSeekReader(snap, lower, upper),
		cache, attr.MustTable(), testTenant, g0)

	got, err := r.Row(ctx, testTenant, testNS, rid)
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	if v, _ := got.Get(attr.SlotImportance); v != attr.F32(0.75) {
		t.Errorf("read importance %s, want the 0.75 this reader's snapshot holds", v)
	}
	r.Report()
	if n := cache.Len(); n != 0 {
		t.Errorf("a reader holding a stale generation left %d entries in the cache", n)
	}
	if h, m, _ := cache.Stats(); h != 0 || m != 1 {
		t.Errorf("%d hits and %d misses, want 0 and 1", h, m)
	}
}

// The commit window, deterministically.
//
// A batch becomes visible to a new snapshot a moment before the transaction's
// commit callbacks run, so there is an interval in which the generation still
// reads as it did before the write and a snapshot taken inside it already shows
// the write. Two readers holding that one generation then disagree about the row:
// one cached the old value, the other must see the new one.
//
// The stress test cannot be relied on to land inside an interval that is a few
// instructions wide, so this one *is* the interval: [windowTx] makes its writes
// visible at once and holds its callbacks until the test releases them.
func TestAReaderInsideACommitWindowReadsItsOwnSnapshot(t *testing.T) {
	ctx, kv, ix, cache := newCachingIndexer(t)
	rid := id.New()
	rec := sampleRecord(rid)
	rec.Fields.Importance = 0.25
	stage(t, ctx, kv, ix, rid, rec)

	// A reader whose snapshot predates the write, opened before the window.
	older := openCachedReader(t, kv, cache)

	// The window opens: staged, visible to a new snapshot, not yet acknowledged.
	tx := &windowTx{ctx: ctx, kv: kv}
	rec.Fields.Importance = 0.75
	if err := ix.Stage(ctx, tx, testTenant, testNS, rid, rec); err != nil {
		t.Fatalf("staging: %v", err)
	}

	// The older reader reads inside the window. It must see its own 0.25 — and it
	// must not leave that 0.25 where a reader on the newer state can find it, which
	// is the half of this the generation cannot do: the generation has not moved,
	// so the two readers' tags are identical.
	got, err := older.Row(ctx, testTenant, testNS, rid)
	if err != nil {
		t.Fatalf("the older reader inside the window: %v", err)
	}
	if v, _ := got.Get(attr.SlotImportance); v != attr.F32(0.25) {
		t.Errorf("a reader on the older snapshot read importance %s, want its own 0.25", v)
	}
	older.Report()

	// A reader whose generation was taken before the write and whose snapshot was
	// taken after it. Both readers hold the same generation, so only the staging
	// mark can keep them apart.
	inside := openCachedReader(t, kv, cache)
	got, err = inside.Row(ctx, testTenant, testNS, rid)
	if err != nil {
		t.Fatalf("reading inside the commit window: %v", err)
	}
	if v, _ := got.Get(attr.SlotImportance); v != attr.F32(0.75) {
		t.Errorf("a reader inside the commit window read importance %s; its own snapshot "+
			"holds 0.75, and the older reader's 0.25 was served to it instead", v)
	}
	inside.Report()

	// The window closes, and the row is cacheable again.
	tx.finish()
	after := openCachedReader(t, kv, cache)
	if _, err := after.Row(ctx, testTenant, testNS, rid); err != nil {
		t.Fatalf("reading after the window closed: %v", err)
	}
	after.Report()
	if cache.Len() != 1 {
		t.Errorf("the row was still quiesced after the transaction acknowledged its commit")
	}
}

// windowTx is a transaction whose writes are visible the instant they are staged
// and whose commit callbacks run only when the test says so. It is the interval
// between a batch landing in the store and the writer being told it landed.
type windowTx struct {
	ctx context.Context
	kv  storage.KV
	// beforeFirstGet runs once, immediately before the first read this
	// transaction makes. [attr.Indexer.Stage] marks the row in the cache and
	// then reads the row it is replacing, so this hook is exactly the instant
	// between those two — the one another writer can commit inside.
	beforeFirstGet func()
	read           bool
	callbacks      []func()
	onClose        []func()
}

func (w *windowTx) Get(key []byte) ([]byte, error) {
	if !w.read {
		w.read = true
		if w.beforeFirstGet != nil {
			w.beforeFirstGet()
		}
	}
	return w.kv.Get(w.ctx, key)
}
func (w *windowTx) Set(key, value []byte)          { _ = w.kv.Set(w.ctx, key, value) }
func (w *windowTx) Delete(key []byte)              { _ = w.kv.Delete(w.ctx, key) }
func (w *windowTx) Expect(_, _ []byte, _ bool)     {}
func (w *windowTx) Len() int                       { return len(w.callbacks) }
func (w *windowTx) Commit(_ context.Context) error { return nil }
func (w *windowTx) OnCommit(f func()) bool         { w.callbacks = append(w.callbacks, f); return true }
func (w *windowTx) OnClose(f func()) bool          { w.onClose = append(w.onClose, f); return true }

// Close abandons the window: the close callbacks run and the commit ones never
// will, which is what a transaction dropped mid-flight does.
func (w *windowTx) Close() {
	for _, f := range w.onClose {
		f()
	}
	w.onClose, w.callbacks = nil, nil
}

// finish acknowledges the commit, which retires the close callbacks first for
// the same reason txn.Commit does.
func (w *windowTx) finish() {
	w.onClose = nil
	for _, f := range w.callbacks {
		f()
	}
	w.callbacks = nil
}

var _ txn.Tx = (*windowTx)(nil)

// A committed write must not take another writer's staging mark with it.
//
// This is the interleaving the mark exists to cover, and the one a mark meaning
// "the latest stager for this row" rather than "this transaction" got wrong. W1
// marks the row and is suspended before it reads it; W2 marks the same row and
// commits, which moves the generation; W1 then reads the row W2 left — so its own
// expectation still holds — and makes its write visible. If W2's commit took W1's
// mark away, W1's row is visible with no mark and a generation that will not move
// until W1 acknowledges its own commit, which for a synced write is a whole fsync
// later. Two readers on that generation then straddle W1's write, and the cache
// serves one of them the other's row.
//
// It is deterministic: [windowTx] makes W1's writes visible the instant they are
// staged and holds its callbacks, and its beforeFirstGet hook is where W2 runs.
func TestACommittedWriteDoesNotTakeAnotherWritersStagingMark(t *testing.T) {
	ctx, kv, ix, cache := newCachingIndexer(t)
	rid := id.New()
	rec := sampleRecord(rid)
	rec.Fields.Importance = 0.25
	stage(t, ctx, kv, ix, rid, rec)

	w1 := &windowTx{ctx: ctx, kv: kv}
	w1.beforeFirstGet = func() {
		// W2 writes the same row and commits. Its commit callback moves the
		// generation, and it must clear only the mark it put down itself.
		second := sampleRecord(rid)
		second.Fields.Importance = 0.5
		stage(t, ctx, kv, ix, rid, second)

		// A reader on the generation W2 left behind, whose snapshot holds W2's
		// row. Its own answer is right either way; what matters is that W1's open
		// window stops it leaving that answer where W1's readers find it.
		r := openCachedReader(t, kv, cache)
		got, err := r.Row(ctx, testTenant, testNS, rid)
		if err != nil {
			t.Errorf("the reader inside W1's window: %v", err)
			return
		}
		if v, _ := got.Get(attr.SlotImportance); v != attr.F32(0.5) {
			t.Errorf("the reader inside W1's window read importance %s, want W2's 0.5", v)
		}
		r.Report()
	}

	rec.Fields.Importance = 0.75
	if err := ix.Stage(ctx, w1, testTenant, testNS, rid, rec); err != nil {
		t.Fatalf("W1 staging: %v", err)
	}

	// W1's row is visible and its commit is not yet acknowledged, so the
	// generation is still the one the reader above held. That reader must not have
	// been allowed to cache the row it read.
	after := openCachedReader(t, kv, cache)
	got, err := after.Row(ctx, testTenant, testNS, rid)
	if err != nil {
		t.Fatalf("reading after W1 staged: %v", err)
	}
	if v, _ := got.Get(attr.SlotImportance); v != attr.F32(0.75) {
		t.Errorf("a reader whose snapshot holds W1's row read importance %s; W1's staging "+
			"mark was taken away by W2's commit, so the 0.5 a reader sharing its "+
			"generation had cached was served instead", v)
	}
	after.Report()
	w1.finish()
}

// A Stage on a transaction that is already spent must leave no staging mark.
//
// Such a transaction accepts no callbacks, so there is nothing that would ever
// give the mark back, and the row would be uncacheable for the life of the
// process. One of these per failed write is a cache that quietly empties itself.
func TestStagingOnASpentTransactionLeavesNoStagingMark(t *testing.T) {
	ctx, kv, ix, cache := newCachingIndexer(t)

	for name, spend := range map[string]func(tx txn.Tx){
		"committed": func(tx txn.Tx) {
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("committing: %v", err)
			}
		},
		"closed": func(tx txn.Tx) { tx.Close() },
	} {
		t.Run(name, func(t *testing.T) {
			rid := id.New()
			stage(t, ctx, kv, ix, rid, sampleRecord(rid))
			before := cache.Generation(testTenant)

			tx := txn.New(kv)
			spend(tx)
			if err := ix.Stage(ctx, tx, testTenant, testNS, rid, sampleRecord(rid)); err == nil {
				t.Fatal("staging on a spent transaction reported success")
			}
			tx.Close()

			// The mark goes straight back through the same release a close uses, so it
			// moves the generation once, like any other unfinished write. That one bump
			// is more than a Stage which staged nothing needs — it costs a tenant's
			// entries and no correctness — and it is the price of the two ends of a mark
			// being one piece of code.
			if g := cache.Generation(testTenant); g != before+1 {
				t.Errorf("a write that never staged left the generation at %d, want %d: its "+
					"mark came back exactly once", g, before+1)
			}

			// The row is not quiesced, so a reader may still cache it.
			r := openCachedReader(t, kv, cache)
			if _, err := r.Row(ctx, testTenant, testNS, rid); err != nil {
				t.Fatalf("reading the row: %v", err)
			}
			if _, err := r.Row(ctx, testTenant, testNS, rid); err != nil {
				t.Fatalf("re-reading the row: %v", err)
			}
			r.Report()
			if hits, _, _ := cache.Stats(); hits == 0 {
				t.Error("the row was left quiesced by a Stage that could register no callback")
			}
		})
	}
}

// A write whose commit conflicts gives its staging mark back, like any other write
// that did not land.
//
// This is the common failure, not an exotic one: two writers that read the same row
// cannot both commit, and `txn.Do` answers the loser's conflict with a retry. A
// conflict that kept its mark would quiesce that row for the life of the process,
// one row per conflict, so a busy tenant's cache would quietly empty itself. Nothing
// of the losing write became visible, so there is nothing for the mark to guard.
//
// The txn half of the claim — that a failed commit runs the close callbacks and not
// the commit ones — is TestAFailedCommitRunsTheCloseCallbacksAndNotTheCommitOnes.
func TestAConflictingStageGivesItsStagingMarkBack(t *testing.T) {
	ctx, kv, ix, cache := newCachingIndexer(t)
	rid := id.New()
	stage(t, ctx, kv, ix, rid, sampleRecord(rid))

	// Two writers stage a write to the same row, so each has recorded the row it read
	// as the condition of its own commit and only one of them can win.
	first, second := txn.New(kv), txn.New(kv)
	defer first.Close()
	defer second.Close()
	for _, w := range []struct {
		tx         txn.Tx
		importance float32
	}{{first, 0.25}, {second, 0.75}} {
		rec := sampleRecord(rid)
		rec.Fields.Importance = w.importance
		if err := ix.Stage(ctx, w.tx, testTenant, testNS, rid, rec); err != nil {
			t.Fatalf("staging: %v", err)
		}
	}

	if err := first.Commit(ctx); err != nil {
		t.Fatalf("the first commit: %v", err)
	}
	first.Close()
	if err := second.Commit(ctx); !errs.Is(err, errs.Conflict) {
		t.Fatalf("the second commit returned %v, want a conflict", err)
	}
	// Closing is what gives the loser's mark back, and twice is allowed.
	second.Close()
	second.Close()

	// Nothing quiesces the row now, so a reader may cache it and the next read of it
	// is a hit.
	r := openCachedReader(t, kv, cache)
	for range 2 {
		if _, err := r.Row(ctx, testTenant, testNS, rid); err != nil {
			t.Fatalf("reading the row: %v", err)
		}
	}
	r.Report()
	if hits, _, _ := cache.Stats(); hits == 0 {
		t.Error("the row was left quiesced by a write whose commit conflicted: its mark " +
			"outlived the transaction that put it there")
	}
}

// A reader given no cache reads exactly as it did before, through the reader
// beneath it. This is the `row_cache_entries = 0` deployment.
func TestAReaderWithNoCacheStillReadsRows(t *testing.T) {
	ctx, kv, ix, _ := newCachingIndexer(t)
	rid := id.New()
	stage(t, ctx, kv, ix, rid, sampleRecord(rid))

	snap := kv.NewSnapshot()
	defer func() { _ = snap.Close() }()
	lower, upper := keys.TenantRange(testTenant)
	r := attr.NewCachedReader(storage.NewSeekReader(snap, lower, upper),
		attr.NewRowCache(0), attr.MustTable(), testTenant, 0)
	defer r.Report()

	got, err := r.Row(ctx, testTenant, testNS, rid)
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	if got == nil {
		t.Fatal("a reader with no cache returned no row")
	}
	if _, err := r.Row(ctx, testTenant, testNS, id.New()); err != nil {
		t.Errorf("a record with no row must read as absent, not as a failure: %v", err)
	}
}

// A rebuild writes attribute rows like any other writer, so a running server has
// to hand it the cache — or every reader keeps exactly the rows the rebuild was
// run to replace. `remem-admin` runs against a stopped server and passes none,
// which is the other half of this test.
func TestARebuildInvalidatesThroughTheCacheItIsGiven(t *testing.T) {
	ctx, kv, ix, cache := newCachingIndexer(t)
	repo := record.NewRepo(kv, record.WithIndexer(ix))
	rid := id.New()
	func() {
		tx := txn.New(kv)
		defer tx.Close()
		if err := repo.Put(ctx, tx, sampleRecord(rid)); err != nil {
			t.Fatalf("storing the record: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}()

	// Without the cache: the rows are rewritten and nothing is told.
	before := cache.Generation(testTenant)
	if _, err := attr.RebuildRows(ctx, kv, testTenant, testNS); err != nil {
		t.Fatalf("rebuilding without the cache: %v", err)
	}
	if g := cache.Generation(testTenant); g != before {
		t.Errorf("a rebuild given no cache moved the generation from %d to %d", before, g)
	}

	// With it: the generation moves, so every entry this tenant holds is retired.
	n, err := attr.RebuildRows(ctx, kv, testTenant, testNS, attr.WithRowCache(cache))
	if err != nil {
		t.Fatalf("rebuilding with the cache: %v", err)
	}
	if n != 1 {
		t.Fatalf("the rebuild restaged %d records, want the one that exists", n)
	}
	if g := cache.Generation(testTenant); g == before {
		t.Error("a rebuild given the cache left the generation alone; every reader would " +
			"keep the rows the rebuild was run to replace")
	}
}

// --- helpers ----------------------------------------------------------------

func newCachingIndexer(t *testing.T) (context.Context, storage.KV, *attr.Indexer, *attr.RowCache) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ctx := tenant.NewContext(context.Background(), testTenant)
	cache := attr.NewRowCache(64)
	return ctx, kv, attr.NewIndexer(attr.MustTable(), attr.WithRowCache(cache)), cache
}

// openCachedReader takes the generation and then the snapshot, in that order,
// which is what makes the cache safe. See [attr.RowCache].
func openCachedReader(t *testing.T, kv storage.KV, cache *attr.RowCache) *attr.CachedReader {
	t.Helper()
	g0 := cache.Generation(testTenant)
	snap := kv.NewSnapshot()
	t.Cleanup(func() { _ = snap.Close() })
	lower, upper := keys.TenantRange(testTenant)
	return attr.NewCachedReader(storage.NewSeekReader(snap, lower, upper),
		cache, attr.MustTable(), testTenant, g0)
}
