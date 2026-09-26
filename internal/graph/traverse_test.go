package graph_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage"
)

// chain builds a -> b -> c -> d, every edge at the given strength.
func chain(t *testing.T, s *graph.Store, kv storage.KV, strength float32, n int) []id.ID {
	t.Helper()
	nodes := make([]id.ID, n)
	for i := range nodes {
		nodes[i] = id.New()
	}
	for i := 0; i+1 < n; i++ {
		add(t, s, kv, edge(nodes[i], nodes[i+1], graph.RelatedTo, strength))
	}
	return nodes
}

func reachedBy(tr graph.Traversal, rid id.ID) (graph.Reached, bool) {
	for _, r := range tr.Reached {
		if r.ID == rid {
			return r, true
		}
	}
	return graph.Reached{}, false
}

func TestTraversalRespectsMaxDepth(t *testing.T) {
	s, kv := newStore(t)
	nodes := chain(t, s, kv, 1, 4)

	tr, err := graph.Traverse(context.Background(), kv, scope(), nodes[0],
		graph.TraverseOpts{MaxDepth: 2, MaxNodes: 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reachedBy(tr, nodes[3]); ok {
		t.Fatalf("a depth-3 node was returned from a depth-2 traversal: %+v", tr.Reached)
	}
	if _, ok := reachedBy(tr, nodes[2]); !ok {
		t.Fatalf("the depth-2 node was not returned: %+v", tr.Reached)
	}
	for _, r := range tr.Reached {
		if r.Depth > 2 {
			t.Errorf("node %s came back at depth %d", r.ID, r.Depth)
		}
	}
}

// The hard stop, and the honest report that it bit. A traversal that returned a
// short list with no flag would be indistinguishable from one that had found
// everything there was.
func TestTraversalRespectsMaxNodes(t *testing.T) {
	s, kv := newStore(t)
	anchor := id.New()
	for i := 0; i < 50; i++ {
		add(t, s, kv, edge(anchor, id.New(), graph.SimilarTo, 0.5))
	}

	tr, err := graph.Traverse(context.Background(), kv, scope(), anchor,
		graph.TraverseOpts{MaxDepth: 1, MaxNodes: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(tr.Reached) != 10 {
		t.Fatalf("got %d nodes, want exactly the 10 asked for", len(tr.Reached))
	}
	if !tr.Truncated {
		t.Fatal("the node budget was spent and the traversal did not say so")
	}
}

func TestTraversalIsNotTruncatedWhenItFinishes(t *testing.T) {
	s, kv := newStore(t)
	nodes := chain(t, s, kv, 1, 3)

	tr, err := graph.Traverse(context.Background(), kv, scope(), nodes[0],
		graph.TraverseOpts{MaxDepth: 5, MaxNodes: 100})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Truncated {
		t.Fatalf("a traversal that exhausted the graph reported truncation: %+v", tr)
	}
	if len(tr.Reached) != 2 {
		t.Fatalf("got %d nodes, want the two reachable ones", len(tr.Reached))
	}
}

func TestTraversalHandlesCycles(t *testing.T) {
	s, kv := newStore(t)
	a, b, c := id.New(), id.New(), id.New()
	add(t, s, kv, edge(a, b, graph.RelatedTo, 1))
	add(t, s, kv, edge(b, c, graph.RelatedTo, 1))
	add(t, s, kv, edge(c, a, graph.RelatedTo, 1))

	tr, err := graph.Traverse(context.Background(), kv, scope(), a,
		graph.TraverseOpts{MaxDepth: graph.MaxTraversalDepth, MaxNodes: 100})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[id.ID]int{}
	for _, r := range tr.Reached {
		seen[r.ID]++
	}
	for rid, n := range seen {
		if n != 1 {
			t.Errorf("node %s was returned %d times", rid, n)
		}
	}
	if _, ok := seen[a]; ok {
		t.Error("the anchor came back among its own neighbours")
	}
	if len(seen) != 2 {
		t.Fatalf("a three-node cycle returned %d nodes, want b and c", len(seen))
	}
}

func TestTraversalIsTenantScoped(t *testing.T) {
	s, kv := newStore(t)
	a, b := id.New(), id.New()
	add(t, s, kv, edge(a, b, graph.RelatedTo, 1))

	other := graph.Scope{Tenant: "globex", Namespace: "default"}
	tr, err := graph.Traverse(context.Background(), kv, other, a,
		graph.TraverseOpts{MaxDepth: 3, MaxNodes: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(tr.Reached) != 0 {
		t.Fatalf("tenant globex reached acme's neighbours: %+v", tr.Reached)
	}
}

// The heart of the REM-83 fix. A node reachable two ways must report the
// stronger chain, and which one that is must not depend on the order the store
// happened to yield the edges in.
func TestTraversalReportsTheStrongestPath(t *testing.T) {
	s, kv := newStore(t)
	a, hub, target := id.New(), id.New(), id.New()
	// Direct but weak: 0.1. Two hops but strong: 0.9 * 0.9 = 0.81.
	add(t, s, kv, edge(a, target, graph.RelatedTo, 0.1))
	add(t, s, kv, edge(a, hub, graph.RelatedTo, 0.9))
	add(t, s, kv, edge(hub, target, graph.RelatedTo, 0.9))

	tr, err := graph.Traverse(context.Background(), kv, scope(), a,
		graph.TraverseOpts{MaxDepth: 3, MaxNodes: 100})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := reachedBy(tr, target)
	if !ok {
		t.Fatalf("the target was not reached: %+v", tr.Reached)
	}
	if got.PathStrength < 0.80 || got.PathStrength > 0.82 {
		t.Fatalf("the target reports path strength %v, want the 0.81 two-hop chain "+
			"rather than the 0.1 direct edge", got.PathStrength)
	}
	if got.Depth != 2 {
		t.Fatalf("the target reports depth %d, want the depth of the strongest path", got.Depth)
	}
}

// Reporting the strongest path must not shrink what the depth bound reaches.
//
// The target is one hop away on a weak edge and two hops away on a strong
// chain. The walk reports the strong chain, correctly, and until Phase 13 it
// also *expanded* the target only at that path's depth. With MaxDepth 2 the
// target sat at the bound, and its neighbour, two hops from the anchor through
// the weak edge, was never returned. MaxDepth promises hops from the anchor,
// and Rust's breadth-first walk returns that neighbour. §II.10 row 7 changed
// how Go ranks what it reaches, never what it reaches.
//
// Found reading the walk against Rust's for the Phase 13 reachability surface.
func TestTraversalReachesEveryNodeWithinMaxDepth(t *testing.T) {
	s, kv := newStore(t)
	a, hub, target, beyond := id.New(), id.New(), id.New(), id.New()
	add(t, s, kv, edge(a, target, graph.RelatedTo, 0.1))
	add(t, s, kv, edge(a, hub, graph.RelatedTo, 0.9))
	add(t, s, kv, edge(hub, target, graph.RelatedTo, 0.9))
	add(t, s, kv, edge(target, beyond, graph.RelatedTo, 1))

	tr, err := graph.Traverse(context.Background(), kv, scope(), a,
		graph.TraverseOpts{MaxDepth: 2, MaxNodes: 100})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := reachedBy(tr, beyond)
	if !ok {
		t.Fatalf("a node two hops from the anchor was not reached at MaxDepth 2: %+v", tr.Reached)
	}
	if got.Depth != 2 || got.PathStrength < 0.099 || got.PathStrength > 0.101 {
		t.Fatalf("the node reports depth %d at strength %v, want the only path within the "+
			"bound: two hops at 0.1", got.Depth, got.PathStrength)
	}
	if tgt, _ := reachedBy(tr, target); tgt.Depth != 2 || tgt.PathStrength < 0.80 {
		t.Fatalf("the intermediate node reports %+v, want its strongest path, 0.81 at depth 2", tgt)
	}
	if len(tr.Reached) != 3 {
		t.Fatalf("reached %d nodes, want each of hub, target and beyond exactly once: %+v",
			len(tr.Reached), tr.Reached)
	}
}

// The ordering the plan's completion criterion names: results come back
// strongest first, not shallowest first.
func TestTraversalOrdersByPathStrength(t *testing.T) {
	s, kv := newStore(t)
	a, hub, strong, weak := id.New(), id.New(), id.New(), id.New()
	add(t, s, kv, edge(a, weak, graph.RelatedTo, 0.1))
	add(t, s, kv, edge(a, hub, graph.RelatedTo, 0.9))
	add(t, s, kv, edge(hub, strong, graph.RelatedTo, 0.9))

	tr, err := graph.Traverse(context.Background(), kv, scope(), a,
		graph.TraverseOpts{MaxDepth: 3, MaxNodes: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(tr.Reached) != 3 {
		t.Fatalf("got %d nodes, want three: %+v", len(tr.Reached), tr.Reached)
	}
	for i := 1; i < len(tr.Reached); i++ {
		if tr.Reached[i-1].PathStrength < tr.Reached[i].PathStrength {
			t.Fatalf("results are not strongest-first: %+v", tr.Reached)
		}
	}
	if tr.Reached[len(tr.Reached)-1].ID != weak {
		t.Fatalf("the 0.1 direct edge did not sort last: %+v", tr.Reached)
	}
	if got, _ := reachedBy(tr, strong); got.Depth != 2 {
		t.Fatalf("the strong two-hop node reports depth %d", got.Depth)
	}
}

// The node budget goes to the strongest connections, not the shallowest. This
// is why best-first rather than breadth-first: a hub with many weak direct
// neighbours must not consume the budget before the strong two-hop node is
// reached.
func TestTheNodeBudgetIsSpentOnTheStrongestConnections(t *testing.T) {
	s, kv := newStore(t)
	a, hub, prize := id.New(), id.New(), id.New()
	for i := 0; i < 40; i++ {
		add(t, s, kv, edge(a, id.New(), graph.SimilarTo, 0.1))
	}
	add(t, s, kv, edge(a, hub, graph.RelatedTo, 0.95))
	add(t, s, kv, edge(hub, prize, graph.RelatedTo, 0.95))

	tr, err := graph.Traverse(context.Background(), kv, scope(), a,
		graph.TraverseOpts{MaxDepth: 2, MaxNodes: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reachedBy(tr, hub); !ok {
		t.Fatalf("the 0.95 direct neighbour was crowded out: %+v", tr.Reached)
	}
	if _, ok := reachedBy(tr, prize); !ok {
		t.Fatalf("the strongest two-hop node was crowded out by forty weak direct edges; "+
			"the budget was spent breadth-first: %+v", tr.Reached)
	}
}

func TestTraversalDirection(t *testing.T) {
	s, kv := newStore(t)
	a, downstream, upstream := id.New(), id.New(), id.New()
	add(t, s, kv, edge(a, downstream, graph.RelatedTo, 0.8))
	add(t, s, kv, edge(upstream, a, graph.RelatedTo, 0.8))

	ctx := context.Background()
	cases := []struct {
		dir  graph.Direction
		want []id.ID
	}{
		{graph.Out, []id.ID{downstream}},
		{graph.In, []id.ID{upstream}},
		{graph.Both, []id.ID{downstream, upstream}},
	}
	for _, tc := range cases {
		tr, err := graph.Traverse(ctx, kv, scope(), a,
			graph.TraverseOpts{MaxDepth: 1, MaxNodes: 100, Direction: tc.dir})
		if err != nil {
			t.Fatalf("%s: %v", tc.dir, err)
		}
		if len(tr.Reached) != len(tc.want) {
			t.Errorf("%s: got %d nodes, want %d", tc.dir, len(tr.Reached), len(tc.want))
		}
		for _, w := range tc.want {
			if _, ok := reachedBy(tr, w); !ok {
				t.Errorf("%s: %s was not reached", tc.dir, w)
			}
		}
	}
}

func TestTraversalTypeFilter(t *testing.T) {
	s, kv := newStore(t)
	a, supported, referenced := id.New(), id.New(), id.New()
	add(t, s, kv, edge(a, supported, graph.Supports, 0.8))
	add(t, s, kv, edge(a, referenced, graph.References, 0.8))

	tr, err := graph.Traverse(context.Background(), kv, scope(), a, graph.TraverseOpts{
		MaxDepth: 1, MaxNodes: 100, Types: []graph.RelationshipType{graph.Supports}})
	if err != nil {
		t.Fatal(err)
	}
	if len(tr.Reached) != 1 || tr.Reached[0].ID != supported {
		t.Fatalf("the type filter did not hold: %+v", tr.Reached)
	}
}

func TestTraversalBoundsAboveTheCeilingAreRefused(t *testing.T) {
	_, kv := newStore(t)
	ctx := context.Background()
	cases := map[string]graph.TraverseOpts{
		"depth":          {MaxDepth: graph.MaxTraversalDepth + 1, MaxNodes: 10},
		"nodes":          {MaxDepth: 1, MaxNodes: graph.MaxTraversalNodes + 1},
		"negative depth": {MaxDepth: -1, MaxNodes: 10},
	}
	for name, opts := range cases {
		if _, err := graph.Traverse(ctx, kv, scope(), id.New(), opts); !errs.Is(err, errs.Invalid) {
			t.Errorf("%s: got %v, want Invalid", name, err)
		}
	}
}

func TestTraversalBoundsDefault(t *testing.T) {
	s, kv := newStore(t)
	nodes := chain(t, s, kv, 1, 4)

	// Zero means the defaults, and the default depth is one hop.
	tr, err := graph.Traverse(context.Background(), kv, scope(), nodes[0], graph.TraverseOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(tr.Reached) != 1 || tr.Reached[0].ID != nodes[1] {
		t.Fatalf("the default traversal returned %+v, want the one direct neighbour", tr.Reached)
	}
}

func TestAnUnscopedTraversalIsRefused(t *testing.T) {
	_, kv := newStore(t)
	_, err := graph.Traverse(context.Background(), kv, graph.Scope{}, id.New(),
		graph.TraverseOpts{MaxDepth: 1, MaxNodes: 10})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid (Invariant 1)", err)
	}
}
