package graph

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/txn"
)

// idLen is the width of a record id inside an edge key.
const idLen = 16

// relLen is the width of the relationship type inside an edge key.
const relLen = 2

// Neighbour bounds. There is no unbounded neighbour read: a hub memory in a
// discovered corpus has as many neighbours as discovery found for it, and a
// caller that asked for "the neighbours" would get a response sized by
// somebody else's background job.
const (
	// DefaultNeighbourLimit is how many neighbours a caller who named no limit
	// gets.
	DefaultNeighbourLimit = 100
	// MaxNeighbourLimit is the most a caller may ask for in one read.
	MaxNeighbourLimit = 1000
)

// Reader is the read surface every edge read needs.
//
// Both [storage.KV] and [storage.Snapshot] satisfy it, and so does
// attr.Reader — which is the point. A query that traverses the graph and walks
// the attribute index reads both through the same pinned snapshot, so the
// neighbours it found and the rows it filtered them with describe one state.
type Reader interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	NewIterator(lower, upper []byte) storage.Iterator
}

// Store owns the two edge key spaces: their keys, their framing, and every read
// and write of them.
//
// One owner, for the reason [vector.Store] has one. There are two writers of
// these rows — a service staging an edge into a caller's transaction, and a
// rebuild writing outside one — and two copies of a durable key layout are two
// things that can disagree.
type Store struct{ kv storage.KV }

// NewStore returns the edge store over kv.
func NewStore(kv storage.KV) *Store { return &Store{kv: kv} }

// OutKey is the storage key of a canonical out-edge.
//
// It is exported so that remem-admin can name a row it is inspecting and a test
// can damage exactly one. Nothing in the read path calls it from outside this
// package.
func OutKey(sc Scope, from id.ID, typ RelationshipType, to id.ID) []byte {
	return keys.EdgeOut(sc.Tenant, sc.namespace(), from, keys.RelType(typ), to)
}

// InKey is the storage key of the derived in-edge.
func InKey(sc Scope, from id.ID, typ RelationshipType, to id.ID) []byte {
	return keys.EdgeIn(sc.Tenant, sc.namespace(), to, keys.RelType(typ), from)
}

// Stage writes an edge into tx, in both directions.
//
// Both rows are staged into the caller's transaction rather than written here,
// which is what makes the derived index safe: a reader never observes an
// out-edge without its reverse entry, or the reverse without its edge (spec
// §12). It is also why an edge created alongside a memory commits with that
// memory or not at all.
//
// Re-staging an edge that already exists is an update, not a duplicate: the
// identity of an edge is (from, type, to), so the same triple addresses the same
// two rows and the new value replaces the old one in both.
func (s *Store) Stage(ctx context.Context, tx txn.Tx, sc Scope, e Edge) error {
	const op = "graph.Store.Stage"

	if err := sc.validate(op); err != nil {
		return err
	}
	if err := e.Validate(); err != nil {
		return err
	}

	value, err := EncodeBody(e)
	if err != nil {
		return errs.E(errs.KindOf(err), op, err)
	}

	// The two rows hold the same bytes. That copy is what makes "what points at
	// this memory, and how strongly" one iterator rather than a read per
	// neighbour — see the package comment and codec.MarshalEdge for the price.
	tx.Set(OutKey(sc, e.From, e.Type, e.To), value)
	tx.Set(InKey(sc, e.From, e.Type, e.To), value)
	return nil
}

// StageDelete removes an edge from both key spaces as part of tx.
//
// Removing an edge that is not there is not an error: the post-condition — that
// no such relationship exists — already holds.
func (s *Store) StageDelete(ctx context.Context, tx txn.Tx, sc Scope,
	from, to id.ID, typ RelationshipType) error {
	const op = "graph.Store.StageDelete"

	if err := sc.validate(op); err != nil {
		return err
	}
	if from.IsZero() || to.IsZero() {
		return errs.E(errs.Invalid, op, errors.New("removing an edge needs both endpoints"))
	}
	if !typ.Valid() {
		return errs.E(errs.Invalid, op, fmt.Errorf("%q is not a relationship type", typ))
	}
	tx.Delete(OutKey(sc, from, typ, to))
	tx.Delete(InKey(sc, from, typ, to))
	return nil
}

// Get returns one edge, or errs.NotFound.
//
// It reads the canonical out-edge. The derived copy is not consulted, because a
// read that could be answered by either would be a read that hides a
// disagreement between them — which is what `remem-admin graph verify` exists
// to surface rather than paper over.
func (s *Store) Get(ctx context.Context, r Reader, sc Scope,
	from, to id.ID, typ RelationshipType) (Edge, error) {
	const op = "graph.Store.Get"

	if err := sc.validate(op); err != nil {
		return Edge{}, err
	}
	value, err := r.Get(ctx, OutKey(sc, from, typ, to))
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			return Edge{}, errs.E(errs.NotFound, op, fmt.Errorf(
				"no %s relationship from %s to %s", typ, from, to))
		}
		return Edge{}, err
	}
	e, err := DecodeBody(value, from, to, typ)
	if err != nil {
		return Edge{}, errs.E(errs.KindOf(err), op, fmt.Errorf(
			"the %s edge from %s to %s: %w", typ, from, to, err))
	}
	return e, nil
}

// NeighbourOpts narrows a neighbour read.
type NeighbourOpts struct {
	// Types restricts the read to these relationship types. Empty means every
	// type.
	Types []RelationshipType
	// MinStrength drops edges weaker than this. It is settled from the row the
	// iterator is already positioned on, in both directions, which is what the
	// derived index's copy of the value buys.
	MinStrength float32
	// Limit caps how many edges are returned. Zero means
	// [DefaultNeighbourLimit]; anything above [MaxNeighbourLimit] is refused.
	Limit int
	// After resumes strictly after one edge in this record's key range.
	// Edge keys order the relationship type before the far endpoint.
	After *Position
}

// Position is where a neighbour walk resumes.
//
// It is (relationship type, other endpoint) because that is the key order:
// keys.EdgeOut is (tenant, namespace, space, from, rel, to), so a walk over
// one record's edges is ordered by rel then to.
type Position struct {
	Rel RelationshipType
	ID  id.ID
}

// Neighbours returns the edges attached to one record, in the given direction.
//
// The direction decides which key space is walked, and both are prefix scans:
// out-edges are keyed from the source, in-edges from the target, so "what does
// this point at" and "what points at this" cost the same. [Both] walks each in
// turn and returns the union, out-edges first.
//
// Edges come back in key order — relationship type, then the far endpoint —
// which is a stable order and deliberately not a ranked one. Ranking by strength
// is traversal's job, and a neighbour read that sorted would have to read the
// whole neighbourhood before it could return the first row.
func Neighbours(ctx context.Context, r Reader, sc Scope, anchor id.ID,
	dir Direction, opts NeighbourOpts) ([]Edge, error) {
	const op = "graph.Neighbours"

	if err := sc.validate(op); err != nil {
		return nil, err
	}
	if anchor.IsZero() {
		return nil, errs.E(errs.Invalid, op, errors.New("a neighbour read needs a record"))
	}
	if !dir.Valid() {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("%q is not a direction", dir))
	}
	limit, err := opts.limit(op)
	if err != nil {
		return nil, err
	}

	out := make([]Edge, 0, min(limit, 16))
	collect := func(d Direction) error {
		return scanNeighbours(ctx, r, sc, anchor, d, opts.After, func(e Edge) (bool, error) {
			if !opts.wants(e) {
				return true, nil
			}
			out = append(out, e)
			return len(out) < limit, nil
		})
	}

	for _, d := range dir.each() {
		if len(out) >= limit {
			break
		}
		if err := collect(d); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// each is the key spaces a direction walks, in the order it walks them.
func (d Direction) each() []Direction {
	if d == Both {
		return []Direction{Out, In}
	}
	return []Direction{d}
}

// wants reports whether an edge survives the options' filters.
func (o NeighbourOpts) wants(e Edge) bool {
	if e.Strength < o.MinStrength {
		return false
	}
	if len(o.Types) == 0 {
		return true
	}
	for _, t := range o.Types {
		if t == e.Type {
			return true
		}
	}
	return false
}

func (o NeighbourOpts) limit(op string) (int, error) {
	switch {
	case o.Limit < 0:
		return 0, errs.E(errs.Invalid, op, errors.New("a neighbour limit may not be negative"))
	case o.Limit == 0:
		return DefaultNeighbourLimit, nil
	case o.Limit > MaxNeighbourLimit:
		return 0, errs.E(errs.Invalid, op, fmt.Errorf(
			"a neighbour read returns at most %d edges, not %d", MaxNeighbourLimit, o.Limit))
	default:
		return o.Limit, nil
	}
}

// scanNeighbours walks one record's edges in one direction, calling fn for each.
// fn returns false to stop the walk.
//
// The edge handed to fn is decoded from the iterator's borrowed value, so it is
// safe to keep: DecodeBody copies everything it returns.
func scanNeighbours(ctx context.Context, r Reader, sc Scope, anchor id.ID,
	dir Direction, after *Position, fn func(Edge) (bool, error)) error {
	const op = "graph.scanNeighbours"

	var lower, upper []byte
	if dir == In {
		lower, upper = keys.EdgesToRange(sc.Tenant, sc.namespace(), anchor)
	} else {
		lower, upper = keys.EdgesFromRange(sc.Tenant, sc.namespace(), anchor)
	}

	it := r.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	ok := it.First()
	if after != nil {
		key := OutKey(sc, anchor, after.Rel, after.ID)
		if dir == In {
			key = InKey(sc, after.ID, after.Rel, anchor)
		}
		ok = it.SeekGE(key)
		if ok && bytes.Equal(it.Key(), key) {
			ok = it.Next()
		}
	}
	for ; ok; ok = it.Next() {
		typ, far, err := parseEdgeKey(it.Key(), op)
		if err != nil {
			return err
		}
		from, to := anchor, far
		if dir == In {
			from, to = far, anchor
		}
		e, err := DecodeBody(it.Value(), from, to, typ)
		if err != nil {
			return errs.E(errs.KindOf(err), op, fmt.Errorf(
				"the %s edge between %s and %s: %w", typ, from, to, err))
		}
		more, err := fn(e)
		if err != nil {
			return err
		}
		if !more {
			return it.Error()
		}
		if err := ctx.Err(); err != nil {
			return errs.E(errs.Unavailable, op, err)
		}
	}
	return it.Error()
}

// ScanOut walks every canonical out-edge of one tenant and namespace, in key
// order, decoding each into an [Edge].
//
// It is the whole-tenant read the neighbour API deliberately does not offer.
// [Neighbours] is bounded because a caller asking for "the neighbours" of a hub
// memory would otherwise get a response sized by somebody else's background
// job; this is for the three callers that legitimately want all of them —
// export, verification and rebuild — and it hands them one edge at a time
// rather than a slice, so the bound is the caller's own and not this package's
// guess at one.
//
// It takes a [Reader], so an export walks the graph through the same pinned
// snapshot it walked the records with. Reading them from two states would let a
// snapshot contain an edge whose record it does not contain.
func ScanOut(ctx context.Context, r Reader, sc Scope, fn func(Edge) error) error {
	const op = "graph.ScanOut"

	if err := sc.validate(op); err != nil {
		return err
	}
	lower, upper := keys.SpaceRange(sc.Tenant, sc.namespace(), keys.SpaceEdgeOut)
	it := r.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	for ok := it.First(); ok; ok = it.Next() {
		from, typ, to, err := parseOutKey(it.Key(), op)
		if err != nil {
			return err
		}
		e, err := DecodeBody(it.Value(), from, to, typ)
		if err != nil {
			return err
		}
		if err := fn(e); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return errs.E(errs.Unavailable, op, err)
		}
	}
	return it.Error()
}

// parseEdgeKey recovers the relationship type and the far endpoint from the
// tail of an edge key. Both key spaces share the layout — anchor, type, far
// endpoint — which is why one parser serves both.
//
// A key that does not decode is [errs.Corruption]. These bytes came from the
// store; a key it holds that this binary cannot read means something wrote it
// that should not have, and guessing would attach a relationship to the wrong
// memory.
func parseEdgeKey(k []byte, op string) (RelationshipType, id.ID, error) {
	const tail = idLen + relLen + idLen
	if len(k) < tail {
		return 0, id.Zero, errs.E(errs.Corruption, op, fmt.Errorf(
			"a key in the edge space is %d bytes, too short to hold an edge", len(k)))
	}
	body := k[len(k)-tail:]
	typ := RelationshipType(uint16(body[idLen])<<8 | uint16(body[idLen+1]))
	far, err := id.FromBytes(body[idLen+relLen:])
	if err != nil {
		return 0, id.Zero, errs.E(errs.Corruption, op, errors.New("an edge key holds a malformed record id"))
	}
	return typ, far, nil
}
