package graph_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/txn"
	"pgregory.net/rapid"
)

// The traversal contract, as a property: for any graph and any depth bound, the
// walk returns exactly the nodes within MaxDepth hops of the anchor, each once,
// each reported with the strongest path to it that fits inside the bound.
//
// The reference is written the plainest way there is: strongest product over
// paths of exactly k hops, for k from 1 to the bound. It needs no heap and no
// pruning, so it cannot share a bug with the walk.
//
// Written in Phase 13 after TestTraversalReachesEveryNodeWithinMaxDepth. The
// defect it found — a node expanded only at the depth of its strongest path —
// belongs to a family, not a single case, and one concrete test would let a
// partial fix through: remembering each node's strongest path and its shallowest
// path, and no more, still misses a middling path that is the only one reaching
// a further node strongly within the bound.
func TestTraversalMatchesAHopBoundedReference(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		kv := memkv.New()
		defer func() { _ = kv.Close() }()
		s := graph.NewStore(kv)
		ctx := context.Background()

		n := rapid.IntRange(2, 7).Draw(rt, "nodes")
		nodes := make([]id.ID, n)
		for i := range nodes {
			// From the index, never id.New(): rapid replays draws and nothing
			// else, so a subject built from a random id cannot be shrunk.
			nid, err := id.Parse(fmt.Sprintf("00000000-0000-7000-8000-%012x", i+1))
			if err != nil {
				rt.Fatalf("building node %d: %v", i, err)
			}
			nodes[i] = nid
		}

		type arc struct {
			from, to int
			strength float32
		}
		// A handful of strengths rather than a continuous range, so ties and
		// zero-strength edges both turn up rather than almost never.
		strengths := []float32{0, 0.1, 0.25, 0.5, 0.9, 1}
		var arcs []arc
		linked := map[[2]int]bool{}
		for range rapid.IntRange(0, 16).Draw(rt, "edges") {
			from := rapid.IntRange(0, n-1).Draw(rt, "from")
			to := rapid.IntRange(0, n-1).Draw(rt, "to")
			strength := strengths[rapid.IntRange(0, len(strengths)-1).Draw(rt, "strength")]
			if from == to || linked[[2]int{from, to}] {
				continue
			}
			linked[[2]int{from, to}] = true
			arcs = append(arcs, arc{from, to, strength})
			stageEdge(ctx, rt, s, kv, edge(nodes[from], nodes[to], graph.RelatedTo, strength))
		}
		maxDepth := rapid.IntRange(1, 4).Draw(rt, "max depth")

		// want[v] is the strongest product over paths from node 0 to v of at
		// most maxDepth hops, or -1 when v is not reachable within the bound.
		const none = float32(-1)
		want := make([]float32, n)
		exact := make([]float32, n) // strongest over paths of exactly this many hops
		for v := range want {
			want[v], exact[v] = none, none
		}
		exact[0] = 1
		for hop := 1; hop <= maxDepth; hop++ {
			next := make([]float32, n)
			for v := range next {
				next[v] = none
			}
			for _, a := range arcs {
				if exact[a.from] == none {
					continue
				}
				if p := exact[a.from] * a.strength; p > next[a.to] {
					next[a.to] = p
				}
			}
			for v := range next {
				if next[v] > want[v] {
					want[v] = next[v]
				}
			}
			exact = next
		}

		tr, err := graph.Traverse(ctx, kv, scope(), nodes[0],
			graph.TraverseOpts{MaxDepth: maxDepth, MaxNodes: 100})
		if err != nil {
			rt.Fatalf("Traverse: %v", err)
		}
		if tr.Truncated {
			rt.Fatalf("a walk over %d nodes with a budget of 100 reports truncated", n)
		}

		got := map[id.ID]graph.Reached{}
		for _, r := range tr.Reached {
			if _, dup := got[r.ID]; dup {
				rt.Fatalf("node %s was reported twice: %+v", r.ID, tr.Reached)
			}
			got[r.ID] = r
			if r.Depth < 1 || r.Depth > maxDepth {
				rt.Fatalf("node %s reported at depth %d with MaxDepth %d", r.ID, r.Depth, maxDepth)
			}
		}
		if _, ok := got[nodes[0]]; ok {
			rt.Fatalf("the anchor was returned among its own neighbours")
		}
		for v := 1; v < n; v++ {
			r, ok := got[nodes[v]]
			switch {
			case want[v] == none && ok:
				rt.Fatalf("node %d is not within %d hops, and was reached: %+v", v, maxDepth, r)
			case want[v] != none && !ok:
				rt.Fatalf("node %d is within %d hops at strength %v, and was not reached: %+v",
					v, maxDepth, want[v], tr.Reached)
			case ok && math.Abs(float64(r.PathStrength-want[v])) > 1e-6:
				rt.Fatalf("node %d reports strength %v, want the strongest path within %d hops, %v",
					v, r.PathStrength, maxDepth, want[v])
			}
		}
	})
}

// stageEdge is add for a property test: rapid's T is not a testing.T.
func stageEdge(ctx context.Context, rt *rapid.T, s *graph.Store, kv *memkv.Store, e graph.Edge) {
	tx := txn.New(kv, txn.Sync(false))
	defer tx.Close()
	if err := s.Stage(ctx, tx, scope(), e); err != nil {
		rt.Fatalf("staging an edge: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		rt.Fatalf("committing an edge: %v", err)
	}
}
