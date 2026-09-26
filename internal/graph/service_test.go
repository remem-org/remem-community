package graph_test

import (
	"context"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

func serviceUnderTest(t *testing.T) (graph.Service, storage.KV, *clock.Fake, context.Context) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(time.UnixMilli(1_725_000_000_000).UTC())
	ctx := tenant.NewContext(context.Background(), testTenant)
	return graph.NewService(kv, clk), kv, clk, ctx
}

// write runs one staged operation and commits it.
func write(t *testing.T, kv storage.KV, fn func(tx txn.Tx) error) error {
	t.Helper()
	tx := txn.New(kv, txn.Sync(false))
	defer tx.Close()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(context.Background())
}

func TestServiceAddStampsTimestampsFromTheInjectedClock(t *testing.T) {
	s, kv, clk, ctx := serviceUnderTest(t)
	a, b := id.New(), id.New()

	if err := write(t, kv, func(tx txn.Tx) error {
		return s.Add(ctx, tx, graph.Edge{From: a, To: b, Type: graph.Supports, Strength: 0.5})
	}); err != nil {
		t.Fatal(err)
	}
	e, err := s.Get(ctx, a, b, graph.Supports)
	if err != nil {
		t.Fatal(err)
	}
	if !e.CreatedAt.Equal(clk.Now().UTC()) || !e.UpdatedAt.Equal(clk.Now().UTC()) {
		t.Fatalf("timestamps are %v/%v, want the fake clock's %v", e.CreatedAt, e.UpdatedAt, clk.Now())
	}
}

// Phase 11's discovery re-runs over the same corpus. An edge whose created_at
// moved every time a background job re-confirmed it would make "when did these
// two become related" unanswerable.
func TestReAddingAnEdgeKeepsItsCreationTime(t *testing.T) {
	s, kv, clk, ctx := serviceUnderTest(t)
	a, b := id.New(), id.New()

	if err := write(t, kv, func(tx txn.Tx) error {
		return s.Add(ctx, tx, graph.Edge{From: a, To: b, Type: graph.SimilarTo, Strength: 0.4})
	}); err != nil {
		t.Fatal(err)
	}
	created := clk.Now().UTC()
	clk.Advance(time.Hour)

	if err := write(t, kv, func(tx txn.Tx) error {
		return s.Add(ctx, tx, graph.Edge{From: a, To: b, Type: graph.SimilarTo, Strength: 0.9})
	}); err != nil {
		t.Fatal(err)
	}
	e, err := s.Get(ctx, a, b, graph.SimilarTo)
	if err != nil {
		t.Fatal(err)
	}
	if !e.CreatedAt.Equal(created) {
		t.Errorf("creation time moved to %v, want %v", e.CreatedAt, created)
	}
	if !e.UpdatedAt.Equal(clk.Now().UTC()) {
		t.Errorf("update time is %v, want %v", e.UpdatedAt, clk.Now())
	}
	if e.Strength != 0.9 {
		t.Errorf("strength is %v, want 0.9", e.Strength)
	}
}

func TestUpdateOfAMissingEdgeIsNotFound(t *testing.T) {
	s, kv, _, ctx := serviceUnderTest(t)
	err := write(t, kv, func(tx txn.Tx) error {
		return s.Update(ctx, tx, id.New(), id.New(), graph.Supports, 0.5)
	})
	if !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound: Add is the call that creates an edge", err)
	}
}

// Update rewrites the whole row to change one field, which is the shape the
// completed-phase review made conditional everywhere else in the system.
func TestUpdateKeepsTheMetadataItDidNotTouch(t *testing.T) {
	s, kv, _, ctx := serviceUnderTest(t)
	a, b := id.New(), id.New()

	if err := write(t, kv, func(tx txn.Tx) error {
		return s.Add(ctx, tx, graph.Edge{From: a, To: b, Type: graph.Supports, Strength: 0.2,
			Meta: map[string]string{"by": "discovery"}})
	}); err != nil {
		t.Fatal(err)
	}
	if err := write(t, kv, func(tx txn.Tx) error {
		return s.Update(ctx, tx, a, b, graph.Supports, 0.8)
	}); err != nil {
		t.Fatal(err)
	}
	e, err := s.Get(ctx, a, b, graph.Supports)
	if err != nil {
		t.Fatal(err)
	}
	if e.Strength != 0.8 {
		t.Errorf("strength is %v, want 0.8", e.Strength)
	}
	if e.Meta["by"] != "discovery" {
		t.Errorf("the metadata was lost by an update that only meant to change the strength: %v", e.Meta)
	}
}

// A write conditional on what it read: a second writer that changed the row
// between the read and the commit must not have its change silently replaced.
func TestUpdateFailsWhenTheEdgeChangedUnderneathIt(t *testing.T) {
	s, kv, _, ctx := serviceUnderTest(t)
	a, b := id.New(), id.New()
	if err := write(t, kv, func(tx txn.Tx) error {
		return s.Add(ctx, tx, graph.Edge{From: a, To: b, Type: graph.Supports, Strength: 0.2})
	}); err != nil {
		t.Fatal(err)
	}

	tx := txn.New(kv, txn.Sync(false))
	defer tx.Close()
	if err := s.Update(ctx, tx, a, b, graph.Supports, 0.5); err != nil {
		t.Fatal(err)
	}
	// Another writer lands first.
	if err := write(t, kv, func(other txn.Tx) error {
		return s.Update(ctx, other, a, b, graph.Supports, 0.9)
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); !errs.Is(err, errs.Conflict) {
		t.Fatalf("the stale update committed with %v, want Conflict", err)
	}
	e, err := s.Get(ctx, a, b, graph.Supports)
	if err != nil {
		t.Fatal(err)
	}
	if e.Strength != 0.9 {
		t.Fatalf("strength is %v; the losing writer overwrote the winner", e.Strength)
	}
}

func TestRemoveAllClearsEveryRelationshipBetweenAPair(t *testing.T) {
	s, kv, _, ctx := serviceUnderTest(t)
	a, b, other := id.New(), id.New(), id.New()
	for _, ty := range []graph.RelationshipType{graph.Supports, graph.References, graph.SimilarTo} {
		if err := write(t, kv, func(tx txn.Tx) error {
			return s.Add(ctx, tx, graph.Edge{From: a, To: b, Type: ty, Strength: 0.5})
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := write(t, kv, func(tx txn.Tx) error {
		return s.Add(ctx, tx, graph.Edge{From: a, To: other, Type: graph.Supports, Strength: 0.5})
	}); err != nil {
		t.Fatal(err)
	}

	var removed int
	if err := write(t, kv, func(tx txn.Tx) error {
		n, err := s.RemoveAll(ctx, tx, a, b)
		removed = n
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if removed != 3 {
		t.Fatalf("RemoveAll reported %d removals, want 3", removed)
	}
	out, err := s.Out(ctx, a, graph.NeighbourOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].To != other {
		t.Fatalf("RemoveAll took the wrong edges: %+v", out)
	}
	in, err := s.In(ctx, b, graph.NeighbourOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(in) != 0 {
		t.Fatalf("reverse entries outlived RemoveAll: %+v", in)
	}
}

// A hard delete that left the graph pointing at the deleted memory would spend
// traversal's node budget reaching records that no longer exist, so "memories
// related to this one" silently returns fewer than it was asked for.
func TestRemoveEveryClearsBothDirections(t *testing.T) {
	s, kv, _, ctx := serviceUnderTest(t)
	doomed, downstream, upstream := id.New(), id.New(), id.New()
	edges := []graph.Edge{
		{From: doomed, To: downstream, Type: graph.Supports, Strength: 0.5},
		{From: upstream, To: doomed, Type: graph.References, Strength: 0.5},
		{From: upstream, To: downstream, Type: graph.SimilarTo, Strength: 0.5},
	}
	for _, e := range edges {
		e := e
		if err := write(t, kv, func(tx txn.Tx) error { return s.Add(ctx, tx, e) }); err != nil {
			t.Fatal(err)
		}
	}

	var removed int
	if err := write(t, kv, func(tx txn.Tx) error {
		n, err := s.RemoveEvery(ctx, tx, doomed)
		removed = n
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Fatalf("RemoveEvery reported %d removals, want the two edges touching the record", removed)
	}

	for _, rid := range []id.ID{downstream, upstream} {
		both, err := s.Out(ctx, rid, graph.NeighbourOpts{})
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range both {
			if e.To == doomed {
				t.Errorf("%s still points at the removed record", rid)
			}
		}
		in, err := s.In(ctx, rid, graph.NeighbourOpts{})
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range in {
			if e.From == doomed {
				t.Errorf("%s is still pointed at by the removed record", rid)
			}
		}
	}
	// The edge that had nothing to do with the deleted record survives.
	out, err := s.Out(ctx, upstream, graph.NeighbourOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].To != downstream {
		t.Fatalf("RemoveEvery took an unrelated edge: %+v", out)
	}
}

func TestServiceRefusesAnUnscopedContext(t *testing.T) {
	s, kv, _, _ := serviceUnderTest(t)
	ctx := context.Background()
	err := write(t, kv, func(tx txn.Tx) error {
		return s.Add(ctx, tx, graph.Edge{From: id.New(), To: id.New(), Type: graph.RelatedTo, Strength: 1})
	})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid: there is no unscoped write path (Invariant 1)", err)
	}
}
