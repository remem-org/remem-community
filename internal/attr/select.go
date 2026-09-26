package attr

import (
	"bytes"
	"context"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
)

// idLen is the width of a record id inside an index key.
const idLen = 16

// Reader is the read surface a selection needs. Both [storage.KV] and
// [storage.Snapshot] satisfy it, which is the point: a listing reads through a
// snapshot so its pages see one state, and a maintenance walk reads through the
// store.
type Reader interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	NewIterator(lower, upper []byte) storage.Iterator
}

// Position is where a walk stopped: the ordering value it had reached, in its
// encoded form, and the record that carried it.
//
// Both halves are needed. The value alone is ambiguous — many records share an
// importance of 0.5 — and the id alone says nothing about where in value order
// to resume. Together they are a total order over the index, which is what
// makes "strictly after this" mean exactly one place.
//
// It carries the *encoded* value rather than a Value because that is what the
// index key holds, what a cursor travels as, and what a resume compares; a
// decode in the middle would be a second encoding of the same fact.
type Position struct {
	Value []byte
	ID    id.ID
}

// Clone returns a copy that does not alias the caller's bytes.
func (p *Position) Clone() *Position {
	if p == nil {
		return nil
	}
	return &Position{Value: append([]byte(nil), p.Value...), ID: p.ID}
}

// Select is a request for one bounded page of an ordered selection.
type Select struct {
	Tenant    tenant.ID
	Namespace tenant.Namespace

	// Slots is the registered table, needed to decode a candidate's row.
	Slots *schema.Slots

	// Slot supplies the order. It is named by the caller, never chosen by a
	// cost model: the order a listing comes back in is part of its contract,
	// not something to optimise away.
	Slot uint16
	// Desc walks from the high end of the range towards the low one.
	Desc bool

	// Preds are the conditions every returned record satisfies. One naming
	// Slot also narrows the walk itself; the rest are settled from each
	// candidate's row.
	Preds []Pred

	// After resumes strictly after a position. Nil starts at the range edge.
	After *Position

	// Limit is how many matching records to return.
	Limit int

	// Effort caps how many candidates may be examined before the walk gives up
	// and reports itself truncated. Without it, a predicate matching one row in
	// ten thousand makes a "return me ten" request an unbounded scan by the
	// back door.
	Effort int
}

// Page is one page of an ordered selection, and what the walk learned about the
// rest of it.
//
// Exhausted and Truncated are deliberately separate. A short page means "that
// was everything" in the first case and "I stopped looking" in the second, and
// a caller that cannot tell them apart eventually reports one as the other —
// which is how "no more results" comes to mean "I gave up".
type Page struct {
	// IDs are the matching records, in the requested order.
	IDs []id.ID
	// Positions are those records' positions, so a caller can mint a cursor
	// from the page it is returning rather than re-deriving one.
	Positions []Position

	// Examined is how many candidates the walk looked at, matching or not. It
	// is what the effort budget is denominated in, so a caller running several
	// walks under one budget can subtract honestly — the number of results says
	// nothing about the work done to find them.
	Examined int

	// Next is the position of the last candidate examined. Resuming from it
	// visits what follows without revisiting what this page covered.
	Next *Position

	// Exhausted means the walk reached the end of its range.
	Exhausted bool
	// Truncated means the effort budget ran out first.
	Truncated bool
}

// Run walks one slot's index and returns the records that satisfy every
// predicate.
//
// The shape is: an access path narrows, the row decides. The index supplies
// candidates in value order and nothing else is trusted about them — every
// candidate's row is read and re-checked, including against the very predicate
// that produced the bounds. That is what makes a stale index entry harmless.
//
// Record bodies are never read here. The caller reads exactly the page it
// returns, which is the whole economy of the sidecar row: ten results out of a
// hundred thousand records costs ten payload reads, not a hundred thousand.
func Run(ctx context.Context, r Reader, sel Select) (Page, error) {
	const op = "attr.Run"

	switch {
	case sel.Tenant == "":
		return Page{}, errs.E(errs.Invalid, op,
			fmt.Errorf("a selection requires a tenant: there is no unscoped read path (Invariant 1)"))
	case sel.Limit <= 0:
		return Page{}, errs.E(errs.Invalid, op, fmt.Errorf("a selection limit must be greater than zero"))
	case sel.Slots == nil:
		return Page{}, errs.E(errs.Invalid, op, fmt.Errorf("a selection needs the registered slot table to read rows with"))
	}
	ns := sel.Namespace
	if ns == "" {
		ns = tenant.DefaultNamespace
	}
	effort := sel.Effort
	if effort <= 0 {
		effort = sel.Limit
	}

	prefix, rangeEnd := keys.AttrSlotRange(sel.Tenant, ns, sel.Slot)
	lower, upper := narrow(prefix, rangeEnd, sel.Preds, sel.Slot)
	if lower == nil {
		// The predicates on the ordering slot contradict each other, so the
		// walk has no range at all. That is an empty and *complete* answer.
		return Page{Exhausted: true}, nil
	}

	it := r.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	page := Page{IDs: make([]id.ID, 0, sel.Limit), Positions: make([]Position, 0, sel.Limit)}
	for ok := seek(it, prefix, sel); ok; ok = advance(it, sel.Desc) {
		if page.Examined >= effort {
			page.Truncated = true
			break
		}
		value, rid, err := parseEntry(it.Key(), prefix, op)
		if err != nil {
			return Page{}, err
		}
		page.Examined++
		page.Next = &Position{Value: value, ID: rid}

		match, err := settle(ctx, r, sel, rid, value)
		if err != nil {
			return Page{}, err
		}
		if match {
			page.IDs = append(page.IDs, rid)
			page.Positions = append(page.Positions, Position{Value: value, ID: rid})
			if len(page.IDs) == sel.Limit {
				break
			}
		}
	}
	if err := it.Error(); err != nil {
		return Page{}, err
	}
	// Exhausted only when the iterator itself ran out: neither the limit nor
	// the effort budget stopped it.
	page.Exhausted = !page.Truncated && len(page.IDs) < sel.Limit
	return page, nil
}

// settle checks one candidate: read its row, confirm the index entry still
// reflects it, and evaluate every predicate.
//
// A candidate whose row is missing is dropped, not failed. The row is derived
// data and the record is the authority on what exists; a listing that returned
// an error because one sidecar row was lost would be a listing that a single
// damaged derived byte takes offline.
func settle(ctx context.Context, r Reader, sel Select, rid id.ID, entryValue []byte) (bool, error) {
	row, err := readRow(ctx, r, sel, rid)
	if err != nil || row == nil {
		return false, err
	}
	// The entry claims this record holds this value. If the row disagrees the
	// entry is stale — an update whose old entry outlived it — and the record
	// does not belong at this position in the order.
	v, ok := row.Get(sel.Slot)
	if !ok || !bytes.Equal(v.OrderBytes(), entryValue) {
		return false, nil
	}
	return Match(row, sel.Preds), nil
}

// readRow reads and decodes one record's row, reporting a missing or
// undecodable row as absent rather than as a failure. See [settle].
func readRow(ctx context.Context, r Reader, sel Select, rid id.ID) (*Row, error) {
	ns := sel.Namespace
	if ns == "" {
		ns = tenant.DefaultNamespace
	}
	value, err := r.Get(ctx, RowKey(sel.Tenant, ns, rid))
	if errs.Is(err, errs.NotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	row, err := DecodeRow(value, sel.Slots)
	if err != nil {
		return nil, nil
	}
	return row, nil
}

// seek positions the iterator at the first candidate, honouring the direction
// and any resume point.
func seek(it storage.Iterator, prefix []byte, sel Select) bool {
	if sel.After == nil {
		if sel.Desc {
			return it.Last()
		}
		return it.First()
	}
	at := entryKey(prefix, sel.After.Value, sel.After.ID)
	if sel.Desc {
		// Strictly before: land on the first key at or after the resume point,
		// then step back one. A resume point that has since been deleted lands
		// on its successor, and stepping back is still the right place.
		if it.SeekGE(at) {
			return it.Prev()
		}
		return it.Last()
	}
	return it.SeekGE(successor(at))
}

func advance(it storage.Iterator, desc bool) bool {
	if desc {
		return it.Prev()
	}
	return it.Next()
}

// parseEntry splits an index key into its ordering value and record id.
//
// The key is prefix | value | id, and the id is fixed-width at the end, so the
// value is whatever lies between — which is how a variable-width string slot
// works without the key having to say how long it is.
func parseEntry(key, prefix []byte, op string) ([]byte, id.ID, error) {
	if len(key) < len(prefix)+idLen {
		return nil, id.Zero, errs.E(errs.Corruption, op, fmt.Errorf(
			"an attribute index key is %d bytes, too short to hold a record id", len(key)))
	}
	value := append([]byte(nil), key[len(prefix):len(key)-idLen]...)
	rid, err := id.FromBytes(key[len(key)-idLen:])
	if err != nil {
		return nil, id.Zero, errs.E(errs.Corruption, op, fmt.Errorf(
			"an attribute index key holds a malformed record id"))
	}
	return value, rid, nil
}

// narrow turns the predicates on the ordering slot into iterator bounds.
//
// A nil lower means the bounds are empty — a contradiction such as
// "importance > 0.9 and importance < 0.1" — which is an answer, not an error.
func narrow(prefix, rangeEnd []byte, preds []Pred, slot uint16) (lower, upper []byte) {
	var lo, hi []byte
	loIncl, hiIncl := true, true
	for _, p := range preds {
		pLo, pHi, pLoIncl, pHiIncl := bounds(p, slot)
		lo, loIncl = tightenLow(lo, loIncl, pLo, pLoIncl)
		hi, hiIncl = tightenHigh(hi, hiIncl, pHi, pHiIncl)
	}

	lower, upper = prefix, rangeEnd
	if lo != nil {
		at := concat(prefix, lo)
		if loIncl {
			lower = at
		} else {
			// Every key for this value starts with prefix|value, so the first
			// key of the next value is the successor of that whole span.
			lower = spanEnd(at)
		}
	}
	if hi != nil {
		at := concat(prefix, hi)
		if hiIncl {
			upper = spanEnd(at)
		} else {
			// prefix|value sorts below prefix|value|id for every id, so it is
			// an exclusive upper bound on the whole value.
			upper = at
		}
	}
	if upper != nil && bytes.Compare(lower, upper) >= 0 {
		return nil, nil
	}
	return lower, upper
}

func tightenLow(cur []byte, curIncl bool, next []byte, nextIncl bool) ([]byte, bool) {
	if next == nil {
		return cur, curIncl
	}
	if cur == nil {
		return next, nextIncl
	}
	switch bytes.Compare(next, cur) {
	case 1:
		return next, nextIncl
	case 0:
		return cur, curIncl && nextIncl
	default:
		return cur, curIncl
	}
}

func tightenHigh(cur []byte, curIncl bool, next []byte, nextIncl bool) ([]byte, bool) {
	if next == nil {
		return cur, curIncl
	}
	if cur == nil {
		return next, nextIncl
	}
	switch bytes.Compare(next, cur) {
	case -1:
		return next, nextIncl
	case 0:
		return cur, curIncl && nextIncl
	default:
		return cur, curIncl
	}
}

// entryKey rebuilds an index key from its parts: prefix | value | id.
func entryKey(prefix, value []byte, rid id.ID) []byte {
	k := make([]byte, 0, len(prefix)+len(value)+idLen)
	k = append(k, prefix...)
	k = append(k, value...)
	return append(k, rid[:]...)
}

// successor is the smallest key greater than k: k with a zero byte appended.
// Nothing sorts between the two, so it is exactly "the row after this one".
func successor(k []byte) []byte {
	return append(append([]byte(nil), k...), 0x00)
}

// concat joins two slices into a fresh one, so neither input is aliased into a
// bound that outlives it.
func concat(a, b []byte) []byte {
	out := make([]byte, 0, len(a)+len(b))
	out = append(out, a...)
	return append(out, b...)
}

// spanEnd is the first key above everything sharing the prefix k — the
// exclusive end of "every entry holding this value".
func spanEnd(k []byte) []byte {
	_, upper := keys.PrefixRange(k)
	return upper
}
