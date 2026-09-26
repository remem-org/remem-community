// Package events is Remem's lifecycle audit stream: what happened to a memory,
// when, because of what, and at whose hand.
//
// # It is canonical
//
// Nothing rebuilds it (plan §II.4). An event is the record of something that
// happened; a lost one is a lost fact about the past, and there is no index to
// recompute it from. It is exported with the corpus and never reconstructed.
//
// # It reverses one Rust decision on purpose
//
// Rust holds recall telemetry in process memory and folds it into the record on
// the next write, so a process that dies loses the window (behaviour baseline
// §4). The reasoning was measured and is worth keeping intact: writing it
// durably made a read append to the WAL and fsync inside the engine's global
// write lock, at full record-rewrite cost.
//
// One half of that cost is removed here and the other is paid. An event is **one
// key**, appended at a position nothing else can be writing, so it never reads
// first, never conflicts, and takes no global write lock — where a record write
// is a body, an attribute row and two index entries per indexed slot, under a
// conditional commit a concurrent writer can lose. The fsync stays, because an
// earlier version of this skipped it on the reasoning that an unsynced commit
// still reaches the write-ahead log, and Phase 10's end-to-end run disproved
// that with a `kill -9`: the recall was gone. See lifecycle.Record.
//
// # Content never enters this stream
//
// The rule that keeps memory content out of logs holds here too, and the
// encoder enforces it. An audit row is read by operators, shipped to log
// aggregators, and kept long after the memory it describes was deleted —
// content inside it would outlive every deletion the product offers.
//
// # Every read takes a tenant
//
// There is no unscoped path (Invariant 1), and the key is subject-major, so
// every operation this package offers is a bounded scan of one subject's
// prefix. Nothing here walks the event space as a whole, which is why the
// retention in internal/lifecycle is enforced per subject by a sweep that was
// visiting that memory anyway.
package events

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// MaxValueBytes bounds one before/after value.
//
// It is what keeps content out of the stream, and it is blunt on purpose: a
// rule that tried to recognise prose would need to know what prose looks like,
// and the point is that content never comes close. A field delta is a number,
// a timestamp, a policy name or a boolean — none of them is 256 bytes.
const MaxValueBytes = 256

// MaxDeltaFields bounds how many fields one event may describe, so that a row
// stays the size of a transition rather than the size of a record.
const MaxDeltaFields = 16

// MaxReasonBytes bounds the human sentence.
const MaxReasonBytes = 512

// Event is one thing that happened to one record.
type Event struct {
	Tenant    tenant.ID
	Namespace tenant.Namespace
	Subject   id.ID

	// At is the instant, supplied by the caller. Nothing here reads a clock:
	// a sweep stamps one instant and every row of that pass carries it, which
	// is what makes a pass reproducible (Invariant 8).
	At time.Time
	// Seq breaks ties within a millisecond and is assigned by [Store.Append].
	Seq uint32

	Kind   Kind
	Actor  string
	Reason string

	// Before and After are the fields that moved. Never content.
	Before, After map[string]string
}

// Scope names one subject's stream. It is a struct rather than three
// parameters because every method here takes all three and a positional
// tenant/namespace pair is the kind of thing that gets swapped once.
type Scope struct {
	Tenant    tenant.ID
	Namespace tenant.Namespace
	Subject   id.ID
}

func (s Scope) namespace() tenant.Namespace {
	if s.Namespace == "" {
		return tenant.DefaultNamespace
	}
	return s.Namespace
}

func (s Scope) validate(op string) error {
	switch {
	case s.Tenant == "":
		return errs.E(errs.Invalid, op, errors.New(
			"an event stream requires a tenant: there is no unscoped read path (Invariant 1)"))
	case !s.Tenant.Valid():
		return errs.E(errs.Invalid, op, fmt.Errorf("tenant %q is not a valid tenant id", s.Tenant))
	case s.Subject.IsZero():
		return errs.E(errs.Invalid, op, errors.New("an event stream is about a subject; none was named"))
	}
	return nil
}

// Reader is the read surface this package needs. Both [storage.KV] and
// [storage.Snapshot] satisfy it, which is the point: a history endpoint reads
// through the store, and a sweep folding recalls reads through the same pinned
// snapshot it walked the due index with.
type Reader interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	NewIterator(lower, upper []byte) storage.Iterator
}

// Store reads and writes the audit stream.
type Store struct{ kv storage.KV }

// NewStore returns the store over kv.
func NewStore(kv storage.KV) *Store { return &Store{kv: kv} }

// Append stages one event and returns it with its sequence number assigned.
//
// It stages rather than commits, so a transition and the record of it land
// together or not at all (spec §12). An event that committed separately would
// be an audit trail that can claim something the store never did.
func (s *Store) Append(ctx context.Context, tx txn.Tx, e Event) (Event, error) {
	const op = "events.Append"

	scope := Scope{Tenant: e.Tenant, Namespace: e.Namespace, Subject: e.Subject}
	if err := scope.validate(op); err != nil {
		return Event{}, err
	}
	if !e.Kind.Valid() {
		return Event{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"%q is not an event kind this binary writes; the kinds are %v", e.Kind, AllKinds()))
	}
	if e.At.IsZero() {
		return Event{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"the %s event about %s carries no time; an audit row with no instant cannot be ordered",
			e.Kind, e.Subject))
	}
	if err := checkDelta(e, op); err != nil {
		return Event{}, err
	}

	e.Namespace = scope.namespace()
	ms := unixMilli(e.At)
	seq, err := s.nextSeq(ctx, tx, scope, ms)
	if err != nil {
		return Event{}, err
	}
	e.Seq = seq

	value, err := marshal(e, op)
	if err != nil {
		return Event{}, err
	}
	tx.Set(keys.Event(scope.Tenant, e.Namespace, scope.Subject, ms, seq), value)
	return e, nil
}

// Restore stages one event exactly as it was, sequence number and all.
//
// It is the import path, and it is separate from [Append] for one reason:
// Append assigns the sequence number, because appending is asking the stream
// where the next row goes. Restoring is not appending — a stream read back from
// a snapshot already knows its own order, and renumbering it would silently
// reorder two events that happened in the same millisecond.
//
// The kind is *not* validated here, where Append refuses one it does not know.
// A snapshot written by a newer binary carries kinds this one has never heard
// of, and [Store.unmarshal] already takes the position that returning such a
// row verbatim beats hiding it: an import that dropped them would answer "why
// did my memory disappear" with silence for exactly the transitions it did not
// recognise.
func (s *Store) Restore(ctx context.Context, tx txn.Tx, e Event) error {
	const op = "events.Restore"

	scope := Scope{Tenant: e.Tenant, Namespace: e.Namespace, Subject: e.Subject}
	if err := scope.validate(op); err != nil {
		return err
	}
	if e.At.IsZero() {
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"the %s event about %s carries no time; an audit row with no instant cannot be ordered",
			e.Kind, e.Subject))
	}
	if err := checkDelta(e, op); err != nil {
		return err
	}
	e.Namespace = scope.namespace()
	value, err := marshal(e, op)
	if err != nil {
		return err
	}
	tx.Set(keys.Event(scope.Tenant, e.Namespace, scope.Subject, unixMilli(e.At), e.Seq), value)
	return nil
}

// nextSeq finds a free sequence number for this subject at this millisecond.
//
// Two lookups rather than one, because neither alone is sufficient. The
// iterator sees what the store holds and not what this transaction has staged;
// [txn.Tx.Get] sees the staged writes and cannot scan. A sweep that writes an
// expiry and an archive for one memory in one transaction — the ordinary case —
// needs the second, and a second recall a millisecond after the first needs the
// first.
func (s *Store) nextSeq(ctx context.Context, tx txn.Tx, scope Scope, ms uint64) (uint32, error) {
	const op = "events.Append"

	ns := scope.namespace()
	lower := keys.Event(scope.Tenant, ns, scope.Subject, ms, 0)
	upper := keys.Event(scope.Tenant, ns, scope.Subject, ms+1, 0)

	next := uint32(0)
	it := s.kv.NewIterator(lower, upper)
	if it.Last() {
		_, _, _, _, seq, err := keys.ParseEvent(it.Key())
		if err != nil {
			_ = it.Close()
			return 0, err
		}
		next = seq + 1
	}
	err := it.Error()
	if closeErr := it.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return 0, errs.E(errs.KindOf(err), op, err)
	}

	// Then walk past anything this transaction has already staged at the same
	// instant. The bound is the delta cap rather than open-ended: more than a
	// handful of events about one memory in one millisecond is a caller in a
	// loop, and failing is better than spinning.
	for probe := 0; probe < MaxDeltaFields; probe++ {
		k := keys.Event(scope.Tenant, ns, scope.Subject, ms, next)
		_, err := tx.Get(k)
		if errs.Is(err, errs.NotFound) {
			return next, nil
		}
		if err != nil {
			return 0, errs.E(errs.KindOf(err), op, err)
		}
		next++
	}
	return 0, errs.E(errs.Invalid, op, fmt.Errorf(
		"more than %d events about memory %s in one millisecond; this is a caller in a loop",
		MaxDeltaFields, scope.Subject))
}

// Query asks for a subject's history.
type Query struct {
	Tenant    tenant.ID
	Namespace tenant.Namespace
	Subject   id.ID

	// Limit is how many events to return. It is required: an unbounded
	// history over a memory recalled for a year is a response nobody wants.
	Limit int

	// Before resumes a page. Only events strictly older than it are returned,
	// which pages newest-first without an offset that shifts as rows are
	// appended.
	Before *Position
}

// Position is where a page of history stopped: the instant and the sequence
// number, which together are a total order over one subject's stream.
type Position struct {
	At  time.Time
	Seq uint32
}

// History returns a subject's events, newest first.
//
// Newest first because the question is nearly always "what just happened to
// this", and because the stream has no end — a memory that keeps being recalled
// keeps growing, so oldest-first would page from a fixed point away from what
// the caller wants.
func (s *Store) History(ctx context.Context, r Reader, q Query) ([]Event, error) {
	const op = "events.History"

	scope := Scope{Tenant: q.Tenant, Namespace: q.Namespace, Subject: q.Subject}
	if err := scope.validate(op); err != nil {
		return nil, err
	}
	if q.Limit <= 0 {
		return nil, errs.E(errs.Invalid, op, errors.New("a history request needs a positive limit"))
	}

	lower, upper := keys.EventsForRange(scope.Tenant, scope.namespace(), scope.Subject)
	if q.Before != nil {
		// Strictly older: the resume point itself was returned by the previous
		// page, and the range end is exclusive, so naming it is enough.
		upper = keys.Event(scope.Tenant, scope.namespace(), scope.Subject, unixMilli(q.Before.At), q.Before.Seq)
	}

	out := make([]Event, 0, q.Limit)
	err := s.each(ctx, r, lower, upper, true, func(e Event) (bool, error) {
		out = append(out, e)
		return len(out) < q.Limit, nil
	})
	if err != nil {
		return nil, errs.E(errs.KindOf(err), op, err)
	}
	return out, nil
}

// NewestRecall returns the subject's most recent recall, if it has one.
//
// It is what decides whether a recall falls inside the coalescing window, so it
// is on a read path and walks backwards from the newest row. The scan is
// bounded by how many non-recall events sit on top of the newest recall, which
// is a handful: the lifecycle writes at most a few transitions per memory per
// day and a recall is by far the commonest kind.
func (s *Store) NewestRecall(ctx context.Context, r Reader, scope Scope) (Event, bool, error) {
	const op = "events.NewestRecall"
	if err := scope.validate(op); err != nil {
		return Event{}, false, err
	}

	lower, upper := keys.EventsForRange(scope.Tenant, scope.namespace(), scope.Subject)
	var found Event
	ok := false
	err := s.each(ctx, r, lower, upper, true, func(e Event) (bool, error) {
		if e.Kind != Recalled {
			return true, nil
		}
		found, ok = e, true
		return false, nil
	})
	if err != nil {
		return Event{}, false, errs.E(errs.KindOf(err), op, err)
	}
	return found, ok, nil
}

// RecallsSince counts the recalls strictly newer than a watermark, and reports
// the newest one's instant.
//
// This is the fold. The watermark is the record's own last_recalled_at, which
// works precisely because two recalls about one subject are at least one
// coalescing window apart — so no two can share a millisecond and be split by a
// strict comparison. A second durable cursor would be a second thing that can
// disagree with the first.
func (s *Store) RecallsSince(ctx context.Context, r Reader, scope Scope, after time.Time) (int, time.Time, error) {
	const op = "events.RecallsSince"
	if err := scope.validate(op); err != nil {
		return 0, time.Time{}, err
	}

	ns := scope.namespace()
	lower, upper := keys.EventsForRange(scope.Tenant, ns, scope.Subject)
	if !after.IsZero() {
		// Strictly after: start at the first key of the next millisecond.
		lower = keys.Event(scope.Tenant, ns, scope.Subject, unixMilli(after)+1, 0)
	}

	n := 0
	var newest time.Time
	err := s.each(ctx, r, lower, upper, false, func(e Event) (bool, error) {
		if e.Kind != Recalled {
			return true, nil
		}
		n++
		newest = e.At
		return true, nil
	})
	if err != nil {
		return 0, time.Time{}, errs.E(errs.KindOf(err), op, err)
	}
	return n, newest, nil
}

// Trim stages the removal of a subject's events older than cutoff, up to max
// rows, and reports how many it staged.
//
// It is bounded because it runs inside a sweep's per-memory transaction: a
// memory with a decade of history must not turn one visit into one enormous
// batch, and what this run leaves behind the next visit removes.
func (s *Store) Trim(ctx context.Context, tx txn.Tx, scope Scope, cutoff time.Time, max int) (int, error) {
	const op = "events.Trim"
	if err := scope.validate(op); err != nil {
		return 0, err
	}
	if max <= 0 {
		return 0, errs.E(errs.Invalid, op, errors.New("a trim needs a positive bound"))
	}

	ns := scope.namespace()
	lower, _ := keys.EventsForRange(scope.Tenant, ns, scope.Subject)
	upper := keys.Event(scope.Tenant, ns, scope.Subject, unixMilli(cutoff), 0)

	removed := 0
	err := s.each(ctx, s.kv, lower, upper, false, func(e Event) (bool, error) {
		tx.Delete(keys.Event(scope.Tenant, ns, scope.Subject, unixMilli(e.At), e.Seq))
		removed++
		return removed < max, nil
	})
	if err != nil {
		return 0, errs.E(errs.KindOf(err), op, err)
	}
	return removed, nil
}

// DeleteSubject stages the removal of a subject's whole stream.
//
// A hard delete calls this and then appends the event that says why, in that
// order, in one transaction: the deletion is staged first so the row written
// after it survives. See internal/lifecycle/archive.go.
func (s *Store) DeleteSubject(ctx context.Context, tx txn.Tx, scope Scope) (int, error) {
	const op = "events.DeleteSubject"
	if err := scope.validate(op); err != nil {
		return 0, err
	}

	ns := scope.namespace()
	lower, upper := keys.EventsForRange(scope.Tenant, ns, scope.Subject)

	removed := 0
	err := s.each(ctx, s.kv, lower, upper, false, func(e Event) (bool, error) {
		tx.Delete(keys.Event(scope.Tenant, ns, scope.Subject, unixMilli(e.At), e.Seq))
		removed++
		return true, nil
	})
	if err != nil {
		return 0, errs.E(errs.KindOf(err), op, err)
	}
	return removed, nil
}

// each walks a range and decodes each row, stopping when fn says to.
//
// A row that will not decode stops the walk with [errs.Corruption] rather than
// being skipped. The stream is canonical: an audit that silently omitted the
// entry it could not read would be an audit that reports a memory was never
// touched because the row saying otherwise was unreadable.
func (s *Store) each(ctx context.Context, r Reader, lower, upper []byte, reverse bool,
	fn func(Event) (bool, error)) error {
	const op = "events.each"

	it := r.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	next := it.Next
	ok := it.First()
	if reverse {
		next = it.Prev
		ok = it.Last()
	}
	for ; ok; ok = next() {
		e, err := unmarshal(it.Key(), it.Value())
		if err != nil {
			return err
		}
		more, err := fn(e)
		if err != nil {
			return err
		}
		if !more {
			break
		}
		if err := ctx.Err(); err != nil {
			return errs.E(errs.Unavailable, op, err)
		}
	}
	return it.Error()
}

// ScanTenant walks every event of one tenant, in key order, and calls fn for
// each.
//
// It is the whole-tenant read [Store.History] deliberately does not offer:
// History answers "what happened to this memory" and is bounded by a page,
// because that is a user's question. This is for the two callers that want the
// stream itself — an export, and the verification that checks one against a
// live store — and it hands over one event at a time so the bound belongs to
// the caller.
//
// The order is the key order: subject, then time, then sequence. It is not
// chronological across subjects, and nothing here needs it to be — a restore
// writes each row back at the timestamp it carries.
func (s *Store) ScanTenant(ctx context.Context, t tenant.ID, ns tenant.Namespace,
	fn func(Event) error,
) error {
	const op = "events.ScanTenant"

	if t == "" {
		return errs.E(errs.Invalid, op, errors.New(
			"an event scan requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	if ns == "" {
		ns = tenant.DefaultNamespace
	}
	lower, upper := keys.SpaceRange(t, ns, keys.SpaceEvent)
	return s.each(ctx, s.kv, lower, upper, false, func(e Event) (bool, error) {
		if err := fn(e); err != nil {
			return false, err
		}
		return true, nil
	})
}

func checkDelta(e Event, op string) error {
	if len(e.Reason) > MaxReasonBytes {
		return errs.E(errs.Invalid, op, fmt.Errorf(
			"the reason on this %s event is %d bytes, the limit is %d", e.Kind, len(e.Reason), MaxReasonBytes))
	}
	for _, m := range []map[string]string{e.Before, e.After} {
		if len(m) > MaxDeltaFields {
			return errs.E(errs.Invalid, op, fmt.Errorf(
				"this %s event describes %d fields, the limit is %d; an event is a transition, not a record",
				e.Kind, len(m), MaxDeltaFields))
		}
		for name, v := range m {
			if len(v) > MaxValueBytes {
				return errs.E(errs.Invalid, op, fmt.Errorf(
					"the %q field on this %s event is %d bytes, the limit is %d; "+
						"an audit row carries what moved, never memory content",
					name, e.Kind, len(v), MaxValueBytes))
			}
		}
	}
	return nil
}

func unixMilli(t time.Time) uint64 {
	ms := t.UnixMilli()
	if ms < 0 {
		return 0
	}
	return uint64(ms)
}
