package graph_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
)

func TestNeighbourTypeFilter(t *testing.T) {
	s, kv := newStore(t)
	a := id.New()
	supports, references, similar := id.New(), id.New(), id.New()
	add(t, s, kv, edge(a, supports, graph.Supports, 0.9))
	add(t, s, kv, edge(a, references, graph.References, 0.9))
	add(t, s, kv, edge(a, similar, graph.SimilarTo, 0.9))

	got, err := graph.Neighbours(context.Background(), kv, scope(), a, graph.Out,
		graph.NeighbourOpts{Types: []graph.RelationshipType{graph.Supports, graph.SimilarTo}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d edges, want the two requested types: %+v", len(got), got)
	}
	for _, e := range got {
		if e.Type != graph.Supports && e.Type != graph.SimilarTo {
			t.Errorf("a %s edge was returned though it was not asked for", e.Type)
		}
	}
}

// The strength is settled from the row the iterator is already positioned on,
// in both directions. That is what the derived index's copy of the value buys:
// filtering the in-direction would otherwise cost a read per neighbour.
func TestNeighbourMinimumStrengthFilters(t *testing.T) {
	s, kv := newStore(t)
	a := id.New()
	strong, weak := id.New(), id.New()
	add(t, s, kv, edge(a, strong, graph.SimilarTo, 0.9))
	add(t, s, kv, edge(a, weak, graph.SimilarTo, 0.1))

	ctx := context.Background()
	for _, dir := range []graph.Direction{graph.Out, graph.In} {
		anchor := a
		if dir == graph.In {
			anchor = strong
		}
		got, err := graph.Neighbours(ctx, kv, scope(), anchor, dir,
			graph.NeighbourOpts{MinStrength: 0.5})
		if err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
		for _, e := range got {
			if e.Strength < 0.5 {
				t.Errorf("%s: an edge at %v survived a 0.5 minimum", dir, e.Strength)
			}
		}
		if dir == graph.Out && len(got) != 1 {
			t.Errorf("out: got %d edges, want only the 0.9 one", len(got))
		}
	}
}

func TestNeighbourLimitIsHonouredAndDefaulted(t *testing.T) {
	s, kv := newStore(t)
	a := id.New()
	for i := 0; i < graph.DefaultNeighbourLimit+10; i++ {
		add(t, s, kv, edge(a, id.New(), graph.SimilarTo, 0.5))
	}

	ctx := context.Background()
	got, err := graph.Neighbours(ctx, kv, scope(), a, graph.Out, graph.NeighbourOpts{Limit: 7})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 7 {
		t.Fatalf("a limit of 7 returned %d edges", len(got))
	}

	// No limit means a bounded default, never the whole neighbourhood.
	got, err = graph.Neighbours(ctx, kv, scope(), a, graph.Out, graph.NeighbourOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != graph.DefaultNeighbourLimit {
		t.Fatalf("an unlimited read returned %d edges, want the %d default",
			len(got), graph.DefaultNeighbourLimit)
	}
}

func TestNeighbourLimitAboveTheCapIsRefused(t *testing.T) {
	_, kv := newStore(t)
	_, err := graph.Neighbours(context.Background(), kv, scope(), id.New(), graph.Out,
		graph.NeighbourOpts{Limit: graph.MaxNeighbourLimit + 1})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

func TestNeighboursBothReturnsTheUnionWithOrientationKept(t *testing.T) {
	s, kv := newStore(t)
	a, points, pointed := id.New(), id.New(), id.New()
	add(t, s, kv, edge(a, points, graph.Supports, 0.7))
	add(t, s, kv, edge(pointed, a, graph.References, 0.6))

	got, err := graph.Neighbours(context.Background(), kv, scope(), a, graph.Both, graph.NeighbourOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d edges, want both directions: %+v", len(got), got)
	}
	for _, e := range got {
		switch e.Type {
		case graph.Supports:
			if e.From != a || e.To != points {
				t.Errorf("the out-edge came back as %s -> %s", e.From, e.To)
			}
		case graph.References:
			if e.From != pointed || e.To != a {
				t.Errorf("the in-edge came back as %s -> %s, losing its orientation", e.From, e.To)
			}
		}
	}
}

func TestNeighboursAreTenantScoped(t *testing.T) {
	s, kv := newStore(t)
	a, b := id.New(), id.New()
	add(t, s, kv, edge(a, b, graph.Supports, 0.8))

	other := graph.Scope{Tenant: "globex", Namespace: "default"}
	got, err := graph.Neighbours(context.Background(), kv, other, a, graph.Both, graph.NeighbourOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("tenant globex read acme's neighbours: %+v", got)
	}
}
