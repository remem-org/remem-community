package attr

import (
	"context"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// Indexer maintains attribute rows and their slot indexes inside a write
// transaction. It satisfies [record.Indexer], so the write path cannot forget
// it: a record body and its derived rows are staged by the same call.
//
// It is safe for concurrent use — it holds a slot table and a row cache, and
// both are registered once at start-up and safe for concurrent use themselves.
type Indexer struct {
	slots *schema.Slots
	// rows is the read path's cache of decoded rows, or nil. The indexer is what
	// tells it a row has changed, which is why the cache is wired here rather
	// than only at the reader: a write path that could forget to invalidate is a
	// write path that serves stale rows.
	rows *RowCache
}

// IndexerOption supplies something the indexer can run without.
type IndexerOption func(*Indexer)

// WithRowCache makes every row this indexer writes invalidate its cached copy:
// the row is quiesced for the span of the transaction and the tenant's
// generation moves when the transaction commits. See [RowCache].
//
// Without it the indexer maintains rows and entries exactly as before, which is
// what a test that is not about the cache wants.
func WithRowCache(c *RowCache) IndexerOption {
	return func(ix *Indexer) { ix.rows = c }
}

// NewIndexer returns the maintainer for a registered slot table.
func NewIndexer(s *schema.Slots, opts ...IndexerOption) *Indexer {
	ix := &Indexer{slots: s}
	for _, o := range opts {
		o(ix)
	}
	return ix
}

// Slots is the table this indexer maintains, for a reader that needs to
// resolve a slot name or list the access paths.
func (ix *Indexer) Slots() *schema.Slots { return ix.slots }

// Stage brings the derived rows for rid into line with rec, inside tx. A nil
// rec removes them.
//
// # Why the old row is read first
//
// This is the whole of Task 5.2 and the failure it exists to prevent is the
// expensive one. An update writes the new index entry; if it does not *delete
// the old one*, a record whose importance moved from 0.9 to 0.1 keeps an entry
// at 0.9 forever. The stale entry is not detectably wrong — it points at a
// record that exists — so a listing of the most important memories returns it,
// at a rank the record has not deserved since the update, and nothing in the
// system ever notices.
//
// Verification against the row catches it on the way out: `select` re-reads
// every candidate's row and drops one whose value no longer matches the entry
// that produced it. That makes a stale entry harmless rather than absent, which
// is not the same thing — it is still read, still costs a row read, and still
// counts against the walk's effort budget. Removing it is what keeps the index
// the size of the corpus instead of the size of its history.
func (ix *Indexer) Stage(ctx context.Context, tx txn.Tx, t tenant.ID, ns tenant.Namespace,
	rid id.ID, rec *record.Record) error {
	const op = "attr.Indexer.Stage"

	rowKey := RowKey(t, ns, rid)
	ix.invalidate(tx, t, ns, rid)

	old, err := ix.currentRow(tx, rowKey)
	if err != nil {
		return errs.E(errs.KindOf(err), op, err)
	}

	if rec == nil {
		tx.Delete(rowKey)
		ix.stageEntries(tx, t, ns, rid, old, nil)
		return nil
	}

	row := Project(rec)
	encoded, err := row.Encode(ix.slots)
	if err != nil {
		return errs.E(errs.KindOf(err), op, err)
	}
	tx.Set(rowKey, encoded)
	ix.stageEntries(tx, t, ns, rid, old, row)
	return nil
}

// invalidate takes the row out of the read path's cache for as long as this
// transaction might change it, and registers both of the ends that give it back.
//
// The mark goes down *now*, before the commit, because a batch becomes visible
// to a new snapshot a moment before its commit callback runs, and inside that
// moment two readers holding one generation would disagree about this row. The
// generation moves when the transaction ends, because a reader on an older
// snapshot must not be able to repopulate the entry after the write has landed.
//
// The mark is this transaction's, so this transaction gives it back, and whichever
// end it reaches moves the generation: its commit, and its close too. The close
// end is not belt and braces. A batch that failed its conditions is not visible, a
// batch whose separate sync failed *is* — the store applies inside its write lock
// and syncs afterwards, see internal/storage/pebble.batch.Commit — and from here
// the two are one event, a Close that says nothing about what landed. So both ends
// move it, which costs an abandoned write one atomic increment and keeps a reader
// from being served a row from the wrong side of a write whose fate the cache
// cannot know. So both callbacks register the same [RowCache.finishWrite]: there
// are two of them because a transaction reaches one end or the other, not because
// the ends do different things.
//
// A transaction accepts neither callback once it is spent, and a mark nobody will
// give back would quiesce that row for the life of the process — so both
// registrations are checked and a refusal is undone here and now. The undo goes
// through the same finish, so it moves the generation too — one bump a write that
// staged nothing did not need, which costs a tenant's cached entries and no
// correctness. See [RowCache].
//
// The close callback is registered *first*, and that order is what makes the two
// refusals unambiguous. Refused here, nothing is registered yet, so releasing the
// mark on the spot is the whole of the undo. Refused on the commit callback instead —
// which cannot happen, the two guard the same word and only this goroutine, the
// transaction's owner, can move it — the close callback is already registered and
// still gives the mark back, because a transaction that cannot commit is one that
// will be closed. Either way the mark comes back exactly once, which is what the
// gate's count of the marks outstanding requires.
func (ix *Indexer) invalidate(tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID) {
	seq, ok := ix.rows.beginWrite(t, ns, rid)
	if !ok {
		return
	}
	if !tx.OnClose(func() { ix.rows.finishWrite(t, ns, rid, seq) }) {
		ix.rows.finishWrite(t, ns, rid, seq)
		return
	}
	if !tx.OnCommit(func() { ix.rows.finishWrite(t, ns, rid, seq) }) {
		// Nothing to undo, and nothing to release: see above. Releasing the mark here
		// as well would hand back a mark the close callback hands back too.
		return
	}
}

// currentRow reads the row as the transaction sees it — its own staged writes
// first, then the store.
//
// A row that will not decode is *not* an error here. It is derived data, and
// the only thing this function needs it for is knowing which index entries to
// withdraw; refusing the write would mean one damaged sidecar row makes the
// record it describes permanently unwritable. The stale entries survive
// instead, and `select` verifies them away on read until a rebuild clears them.
func (ix *Indexer) currentRow(tx txn.Tx, rowKey []byte) (*Row, error) {
	value, err := tx.Get(rowKey)
	if errs.Is(err, errs.NotFound) {
		tx.Expect(rowKey, nil, false)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	tx.Expect(rowKey, value, true)
	row, err := DecodeRow(value, ix.slots)
	if err != nil {
		return nil, nil
	}
	return row, nil
}

// stageEntries reconciles the index entries between two states of a row.
// Either may be nil: no old row is a create, no new row is a delete.
func (ix *Indexer) stageEntries(tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID, old, row *Row) {
	for _, def := range ix.slots.IndexedSlots() {
		var oldV, newV Value
		var hadOld, hasNew bool
		if old != nil {
			oldV, hadOld = old.Get(def.Slot)
		}
		if row != nil {
			newV, hasNew = row.Get(def.Slot)
		}

		if hadOld && (!hasNew || oldV != newV) {
			tx.Delete(IndexKey(t, ns, def.Slot, oldV, rid))
		}
		if hasNew {
			// The value is entirely in the key, so the entry's payload is
			// empty. Putting the value in both places would be two copies of
			// one fact, and the key is the copy the store sorts on.
			tx.Set(IndexKey(t, ns, def.Slot, newV, rid), nil)
		}
	}
}

// StageDrop removes a record's row and every index entry it produced, without
// needing the record.
//
// It is what a rebuild uses to clear a tenant before repopulating it
// (Invariant 3: a derived index is rebuilt, never repaired), and what a delete
// path that has already lost the record body falls back to.
func (ix *Indexer) StageDrop(ctx context.Context, tx txn.Tx, t tenant.ID, ns tenant.Namespace, rid id.ID) error {
	return ix.Stage(ctx, tx, t, ns, rid, nil)
}

// Ensure that the write path's hook and this implementation cannot drift: if
// the interface gains a parameter, this fails to compile here rather than
// silently leaving the repository with no indexer.
var _ record.Indexer = (*Indexer)(nil)
