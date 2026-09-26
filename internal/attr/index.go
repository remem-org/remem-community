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
// It is safe for concurrent use — it holds a slot table and nothing else, and
// the table is registered once at start-up and read thereafter.
type Indexer struct {
	slots *schema.Slots
}

// NewIndexer returns the maintainer for a registered slot table.
func NewIndexer(s *schema.Slots) *Indexer { return &Indexer{slots: s} }

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
