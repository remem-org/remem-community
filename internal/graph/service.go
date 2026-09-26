package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// Service is the tenant-scoped surface over the edge store.
//
// Every method takes a context from which the tenant is already resolved. There
// is no method that takes a tenant as a parameter, for the reason record.Repo
// has none: a parameter is something a caller can pass wrongly, where a context
// is something the request boundary sets once (Invariant 1).
//
// The writes take the caller's transaction, so an edge created alongside a
// memory commits with that memory or not at all. The reads do not, because the
// caller that needs a pinned view — the query executor — uses the package
// functions with its own snapshot instead.
type Service interface {
	// Add creates an edge, or replaces one that already exists.
	Add(ctx context.Context, tx txn.Tx, e Edge) error
	// Update changes an existing edge's strength. An edge that is not there is
	// errs.NotFound: Add is the call that creates one.
	Update(ctx context.Context, tx txn.Tx, from, to id.ID, typ RelationshipType, strength float32) error
	// Remove deletes one relationship. Removing an absent one is not an error.
	Remove(ctx context.Context, tx txn.Tx, from, to id.ID, typ RelationshipType) error
	// RemoveAll deletes every relationship from one record to another.
	RemoveAll(ctx context.Context, tx txn.Tx, from, to id.ID) (int, error)
	// RemoveEvery deletes every edge touching a record, in both directions. It
	// is what a hard delete calls.
	RemoveEvery(ctx context.Context, tx txn.Tx, rid id.ID) (int, error)

	// Out is what this record points at.
	Out(ctx context.Context, from id.ID, opts NeighbourOpts) ([]Edge, error)
	// In is what points at this record.
	In(ctx context.Context, to id.ID, opts NeighbourOpts) ([]Edge, error)
	// Get returns one edge, or errs.NotFound.
	Get(ctx context.Context, from, to id.ID, typ RelationshipType) (Edge, error)
	// Linked reports whether any relationship connects two records, in either
	// direction, and makes tx conditional on that answer still holding when it
	// commits.
	Linked(ctx context.Context, tx txn.Tx, a, b id.ID) (bool, error)
	// Traverse walks outward from a record, bounded in depth and node count.
	Traverse(ctx context.Context, start id.ID, opts TraverseOpts) (Traversal, error)
}

// NewService returns the service over kv.
//
// The clock is injected because an edge's timestamps are durable business logic
// and Invariant 8 forbids reading the wall clock inside one.
func NewService(kv storage.KV, clk clock.Clock) Service {
	if clk == nil {
		clk = clock.System()
	}
	return &service{kv: kv, store: NewStore(kv), clk: clk}
}

type service struct {
	kv    storage.KV
	store *Store
	clk   clock.Clock
}

// scope resolves the tenant the request is running as.
func (s *service) scope(ctx context.Context) (Scope, error) {
	t, err := tenant.Require(ctx)
	if err != nil {
		return Scope{}, err
	}
	return Scope{Tenant: t, Namespace: tenant.DefaultNamespace}, nil
}

// Add creates an edge, or replaces one that already exists.
//
// Re-adding an existing relationship keeps its original creation time and moves
// only its update time. That matters more than it looks: Phase 11's discovery
// re-runs over the same corpus, and an edge whose created_at moved every time a
// background job re-confirmed it would make "when did these two memories become
// related" unanswerable.
func (s *service) Add(ctx context.Context, tx txn.Tx, e Edge) error {
	const op = "graph.Add"

	sc, err := s.scope(ctx)
	if err != nil {
		return err
	}
	if err := e.Validate(); err != nil {
		return err
	}

	// Read through the transaction — its own staged writes first, then the
	// store — and make the write conditional on what was read. That is the
	// posture the completed-phase review settled for every read-modify-write in
	// the system: without it, a concurrent writer's metadata is silently
	// replaced by whatever this call happened to read a moment earlier.
	key := OutKey(sc, e.From, e.Type, e.To)
	old, err := tx.Get(key)
	switch {
	case err == nil:
		if prev, derr := DecodeBody(old, e.From, e.To, e.Type); derr == nil {
			e.CreatedAt = prev.CreatedAt
		}
		tx.Expect(key, old, true)
	case errs.Is(err, errs.NotFound):
		tx.Expect(key, nil, false)
		if e.CreatedAt.IsZero() {
			e.CreatedAt = s.clk.Now().UTC()
		}
	default:
		return errs.E(errs.KindOf(err), op, err)
	}
	e.UpdatedAt = s.clk.Now().UTC()

	return s.store.Stage(ctx, tx, sc, e)
}

// Update changes an existing edge's strength.
//
// It is new: Rust supports create and delete only, while spec §14 lists
// relationship updates as required. An edge that is not there is errs.NotFound
// rather than an implicit create, because a caller who meant to strengthen an
// existing relationship and instead invented one has stated something about
// their corpus they did not mean.
func (s *service) Update(ctx context.Context, tx txn.Tx, from, to id.ID,
	typ RelationshipType, strength float32) error {
	const op = "graph.Update"

	sc, err := s.scope(ctx)
	if err != nil {
		return err
	}
	key := OutKey(sc, from, typ, to)
	old, err := tx.Get(key)
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			return errs.E(errs.NotFound, op, fmt.Errorf(
				"there is no %s relationship from %s to %s to update", typ, from, to))
		}
		return errs.E(errs.KindOf(err), op, err)
	}
	e, err := DecodeBody(old, from, to, typ)
	if err != nil {
		return errs.E(errs.KindOf(err), op, err)
	}
	tx.Expect(key, old, true)
	e.Strength = strength
	e.UpdatedAt = s.clk.Now().UTC()
	return s.store.Stage(ctx, tx, sc, e)
}

func (s *service) Remove(ctx context.Context, tx txn.Tx, from, to id.ID, typ RelationshipType) error {
	sc, err := s.scope(ctx)
	if err != nil {
		return err
	}
	return s.store.StageDelete(ctx, tx, sc, from, to, typ)
}

// RemoveAll deletes every relationship from one record to another and reports
// how many there were.
//
// Two records may be connected several ways, and a caller who says "these two
// are not related" usually means all of them rather than one named kind.
func (s *service) RemoveAll(ctx context.Context, tx txn.Tx, from, to id.ID) (int, error) {
	const op = "graph.RemoveAll"

	sc, err := s.scope(ctx)
	if err != nil {
		return 0, err
	}
	// The scan reads the store rather than the transaction, because a batch has
	// no iterator: staged writes are invisible to it. Both callers stage their
	// deletions after this walk rather than during a sequence of edits, so the
	// state it reads is the state the deletions apply to.
	var removed int
	err = scanNeighbours(ctx, s.kv, sc, from, Out, nil, func(e Edge) (bool, error) {
		if e.To != to {
			return true, nil
		}
		if err := s.store.StageDelete(ctx, tx, sc, from, to, e.Type); err != nil {
			return false, err
		}
		removed++
		return true, nil
	})
	if err != nil {
		return 0, errs.E(errs.KindOf(err), op, err)
	}
	return removed, nil
}

// RemoveEvery deletes every edge touching a record, in both directions.
//
// It is what a hard delete calls, and it is why hard-deleting a memory does not
// leave the graph pointing at it. The alternative — leaving the edges — is not
// merely untidy: traversal spends its node budget reaching records that no
// longer exist, so "memories related to this one" silently returns fewer than
// it was asked for, and nothing says why.
//
// The cost is one staged deletion per edge the record has, inside the caller's
// transaction. That is bounded by the record's degree rather than by the corpus,
// and a hard delete is a rare, explicit act. Orphans can still arise from an
// import or a crash between the two key spaces, which is what
// `remem-admin graph verify` reports.
func (s *service) RemoveEvery(ctx context.Context, tx txn.Tx, rid id.ID) (int, error) {
	const op = "graph.RemoveEvery"

	sc, err := s.scope(ctx)
	if err != nil {
		return 0, err
	}

	var removed int
	for _, dir := range []Direction{Out, In} {
		err := scanNeighbours(ctx, s.kv, sc, rid, dir, nil, func(e Edge) (bool, error) {
			if err := s.store.StageDelete(ctx, tx, sc, e.From, e.To, e.Type); err != nil {
				return false, err
			}
			removed++
			return true, nil
		})
		if err != nil {
			return 0, errs.E(errs.KindOf(err), op, err)
		}
	}
	return removed, nil
}

func (s *service) Out(ctx context.Context, from id.ID, opts NeighbourOpts) ([]Edge, error) {
	sc, err := s.scope(ctx)
	if err != nil {
		return nil, err
	}
	return Neighbours(ctx, s.kv, sc, from, Out, opts)
}

func (s *service) In(ctx context.Context, to id.ID, opts NeighbourOpts) ([]Edge, error) {
	sc, err := s.scope(ctx)
	if err != nil {
		return nil, err
	}
	return Neighbours(ctx, s.kv, sc, to, In, opts)
}

func (s *service) Get(ctx context.Context, from, to id.ID, typ RelationshipType) (Edge, error) {
	sc, err := s.scope(ctx)
	if err != nil {
		return Edge{}, err
	}
	return s.store.Get(ctx, s.kv, sc, from, to, typ)
}

// Traverse walks outward from a record.
//
// It reads through the store rather than a snapshot. A caller that needs the
// traversal and its filtering to describe one state — the query executor — calls
// the package-level [Traverse] with its own pinned snapshot instead, which is
// why that function takes a [Reader].
// Linked reports whether any relationship connects a and b, in either
// direction, and holds the answer to the end of the transaction.
//
// # Why it is not just a read
//
// A caller that reads "these two are not linked" and then writes an edge has a
// read-modify-write, and two of them racing both read "no" and both write. That
// is not hypothetical: it is what Phase 11's end-to-end run found, two memories
// written together and discovered by two workers at once, ending up connected in
// both directions one millisecond apart. A relationship is a fact about a pair,
// so two rows saying it is a duplicate — and neither run of either subject would
// ever remove the second.
//
// So the reads go through tx, and when the answer is "not linked" every key that
// would have changed it is expected absent at commit. A concurrent writer that
// creates one of them makes this transaction conflict rather than proceed, and
// txn.Do re-runs the body, which asks again and gets the true answer.
//
// # Why sixteen keys
//
// Eight relationship types, two directions. It is deliberately not a neighbour
// scan: a neighbour read is capped, so a hub record with more edges than the cap
// would report "not linked" for a pair that is. Sixteen keyed reads are bounded
// by the type table rather than by the corpus, and the in-edge rows need no
// expectation of their own because each is written by the same Stage call as its
// out-edge.
//
// # When they are already linked, nothing is expected
//
// The caller writes nothing for this pair, so holding the existing edge would
// only make an unrelated concurrent update of it fail somebody else's
// transaction.
func (s *service) Linked(ctx context.Context, tx txn.Tx, a, b id.ID) (bool, error) {
	const op = "graph.Linked"

	sc, err := s.scope(ctx)
	if err != nil {
		return false, err
	}
	switch {
	case a.IsZero() || b.IsZero():
		return false, errs.E(errs.Invalid, op, errors.New("a link check needs two records"))
	case a == b:
		return false, errs.E(errs.Invalid, op, errors.New(
			"a memory cannot be connected to itself, so it is never linked to itself"))
	}

	absent := make([][]byte, 0, 2*len(RelationshipTypes()))
	for _, typ := range RelationshipTypes() {
		for _, pair := range [2][2]id.ID{{a, b}, {b, a}} {
			key := OutKey(sc, pair[0], typ, pair[1])
			switch _, err := tx.Get(key); {
			case err == nil:
				return true, nil
			case errs.Is(err, errs.NotFound):
				absent = append(absent, key)
			default:
				return false, errs.E(errs.KindOf(err), op, err)
			}
		}
	}
	for _, key := range absent {
		tx.Expect(key, nil, false)
	}
	return false, nil
}

func (s *service) Traverse(ctx context.Context, start id.ID, opts TraverseOpts) (Traversal, error) {
	sc, err := s.scope(ctx)
	if err != nil {
		return Traversal{}, err
	}
	return Traverse(ctx, s.kv, sc, start, opts)
}
