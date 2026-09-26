package graph_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

const testTenant = tenant.ID("acme")

func scope() graph.Scope {
	return graph.Scope{Tenant: testTenant, Namespace: tenant.DefaultNamespace}
}

func newStore(t *testing.T) (*graph.Store, storage.KV) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	return graph.NewStore(kv), kv
}

// add stages one edge and commits it, which is how every caller outside a
// larger write does it.
func add(t *testing.T, s *graph.Store, kv storage.KV, e graph.Edge) {
	t.Helper()
	tx := txn.New(kv, txn.Sync(false))
	defer tx.Close()
	if err := s.Stage(context.Background(), tx, scope(), e); err != nil {
		t.Fatalf("staging %v: %v", e.Type, err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("committing: %v", err)
	}
}

func remove(t *testing.T, s *graph.Store, kv storage.KV, from, to id.ID, typ graph.RelationshipType) {
	t.Helper()
	tx := txn.New(kv, txn.Sync(false))
	defer tx.Close()
	if err := s.StageDelete(context.Background(), tx, scope(), from, to, typ); err != nil {
		t.Fatalf("staging the removal: %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("committing: %v", err)
	}
}

// fixedTime is the timestamp every fixture edge and record carries, so a byte
// comparison between two runs is meaningful.
func fixedTime() time.Time { return time.UnixMilli(1_725_000_000_000).UTC() }

func edge(from, to id.ID, typ graph.RelationshipType, strength float32) graph.Edge {
	now := fixedTime()
	return graph.Edge{From: from, To: to, Type: typ, Strength: strength, CreatedAt: now, UpdatedAt: now}
}

func TestAddWritesBothDirections(t *testing.T) {
	s, kv := newStore(t)
	a, b := id.New(), id.New()
	add(t, s, kv, edge(a, b, graph.Supports, 0.8))

	ctx := context.Background()
	out, err := graph.Neighbours(ctx, kv, scope(), a, graph.Out, graph.NeighbourOpts{})
	if err != nil {
		t.Fatalf("Out: %v", err)
	}
	if len(out) != 1 || out[0].To != b || out[0].Type != graph.Supports || out[0].Strength != 0.8 {
		t.Fatalf("Out(a) = %+v, want one Supports edge to b at 0.8", out)
	}

	in, err := graph.Neighbours(ctx, kv, scope(), b, graph.In, graph.NeighbourOpts{})
	if err != nil {
		t.Fatalf("In: %v", err)
	}
	if len(in) != 1 || in[0].From != a || in[0].Type != graph.Supports || in[0].Strength != 0.8 {
		t.Fatalf("In(b) = %+v, want one Supports edge from a at 0.8", in)
	}
}

func TestRemoveClearsBothDirections(t *testing.T) {
	s, kv := newStore(t)
	a, b := id.New(), id.New()
	add(t, s, kv, edge(a, b, graph.Supports, 0.8))
	remove(t, s, kv, a, b, graph.Supports)

	ctx := context.Background()
	out, err := graph.Neighbours(ctx, kv, scope(), a, graph.Out, graph.NeighbourOpts{})
	if err != nil {
		t.Fatal(err)
	}
	in, err := graph.Neighbours(ctx, kv, scope(), b, graph.In, graph.NeighbourOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 || len(in) != 0 {
		t.Fatalf("after removal Out(a) = %v and In(b) = %v; a reverse entry outlived its edge", out, in)
	}
}

func TestUpdateChangesStrengthInBothDirections(t *testing.T) {
	s, kv := newStore(t)
	a, b := id.New(), id.New()
	add(t, s, kv, edge(a, b, graph.SimilarTo, 0.2))
	add(t, s, kv, edge(a, b, graph.SimilarTo, 0.9))

	ctx := context.Background()
	out, _ := graph.Neighbours(ctx, kv, scope(), a, graph.Out, graph.NeighbourOpts{})
	in, _ := graph.Neighbours(ctx, kv, scope(), b, graph.In, graph.NeighbourOpts{})
	if len(out) != 1 || len(in) != 1 {
		t.Fatalf("re-adding the same edge duplicated it: out=%v in=%v", out, in)
	}
	if out[0].Strength != 0.9 || in[0].Strength != 0.9 {
		t.Fatalf("out strength %v, in strength %v; both copies must agree at 0.9",
			out[0].Strength, in[0].Strength)
	}
}

func TestGetReturnsOneEdgeOrNotFound(t *testing.T) {
	s, kv := newStore(t)
	a, b := id.New(), id.New()
	add(t, s, kv, edge(a, b, graph.PartOf, 0.5))

	ctx := context.Background()
	got, err := s.Get(ctx, kv, scope(), a, b, graph.PartOf)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Strength != 0.5 || got.From != a || got.To != b {
		t.Fatalf("Get returned %+v", got)
	}
	if _, err := s.Get(ctx, kv, scope(), a, b, graph.Supports); !errs.Is(err, errs.NotFound) {
		t.Fatalf("Get of an absent relationship type returned %v, want NotFound", err)
	}
}

// Two records may be connected several ways, and each way is its own edge with
// its own strength.
func TestSeveralRelationshipTypesBetweenOnePairCoexist(t *testing.T) {
	s, kv := newStore(t)
	a, b := id.New(), id.New()
	add(t, s, kv, edge(a, b, graph.Supports, 0.9))
	add(t, s, kv, edge(a, b, graph.References, 0.3))

	out, _ := graph.Neighbours(context.Background(), kv, scope(), a, graph.Out, graph.NeighbourOpts{})
	if len(out) != 2 {
		t.Fatalf("Out(a) = %v, want both the Supports and the References edge", out)
	}
	remove(t, s, kv, a, b, graph.Supports)
	out, _ = graph.Neighbours(context.Background(), kv, scope(), a, graph.Out, graph.NeighbourOpts{})
	if len(out) != 1 || out[0].Type != graph.References {
		t.Fatalf("after removing Supports, Out(a) = %v, want only the References edge", out)
	}
}

func TestStagingASelfEdgeIsRefused(t *testing.T) {
	s, kv := newStore(t)
	a := id.New()
	tx := txn.New(kv, txn.Sync(false))
	defer tx.Close()
	err := s.Stage(context.Background(), tx, scope(), edge(a, a, graph.RelatedTo, 1))
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("staging a self-edge returned %v, want Invalid", err)
	}
}

func TestEdgeWritesAreTenantScoped(t *testing.T) {
	s, kv := newStore(t)
	a, b := id.New(), id.New()
	add(t, s, kv, edge(a, b, graph.Supports, 0.8))

	other := graph.Scope{Tenant: "globex", Namespace: tenant.DefaultNamespace}
	ctx := context.Background()
	out, err := graph.Neighbours(ctx, kv, other, a, graph.Out, graph.NeighbourOpts{})
	if err != nil {
		t.Fatal(err)
	}
	in, err := graph.Neighbours(ctx, kv, other, b, graph.In, graph.NeighbourOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 || len(in) != 0 {
		t.Fatalf("tenant globex sees acme's edges: out=%v in=%v", out, in)
	}
}

func TestAnUnscopedEdgeReadIsRefused(t *testing.T) {
	_, kv := newStore(t)
	_, err := graph.Neighbours(context.Background(), kv, graph.Scope{}, id.New(), graph.Out, graph.NeighbourOpts{})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("an edge read with no tenant returned %v, want Invalid (Invariant 1)", err)
	}
}

// A hundred concurrent writers under -race must leave the two key spaces
// agreeing: every out-edge has its reverse entry and nothing else does.
func TestConcurrentEdgeWritesAreSerialised(t *testing.T) {
	s, kv := newStore(t)
	anchor := id.New()
	targets := make([]id.ID, 100)
	for i := range targets {
		targets[i] = id.New()
	}

	var wg sync.WaitGroup
	for i, to := range targets {
		wg.Add(1)
		go func(i int, to id.ID) {
			defer wg.Done()
			tx := txn.New(kv, txn.Sync(false))
			defer tx.Close()
			e := edge(anchor, to, graph.SimilarTo, float32(i)/100)
			if err := s.Stage(context.Background(), tx, scope(), e); err != nil {
				t.Errorf("staging: %v", err)
				return
			}
			if err := tx.Commit(context.Background()); err != nil {
				t.Errorf("committing: %v", err)
			}
		}(i, to)
	}
	wg.Wait()

	ctx := context.Background()
	out, err := graph.Neighbours(ctx, kv, scope(), anchor, graph.Out, graph.NeighbourOpts{Limit: len(targets)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(targets) {
		t.Fatalf("Out(anchor) returned %d edges, want %d", len(out), len(targets))
	}
	for _, e := range out {
		in, err := graph.Neighbours(ctx, kv, scope(), e.To, graph.In, graph.NeighbourOpts{})
		if err != nil {
			t.Fatal(err)
		}
		if len(in) != 1 || in[0].From != anchor || in[0].Strength != e.Strength {
			t.Fatalf("the reverse of %v is %v", e, in)
		}
	}
}
