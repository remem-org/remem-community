package hnsw

import (
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// A neighbour list is allowed to grow half as far again past its bound before
// the diversity heuristic prunes it back, rather than being pruned on every
// link past the bound.
//
// Profiled in Phase 13, 81% of an insert was that heuristic re-run on every
// neighbour whose list the new node pushed one over. Letting a list overflow
// runs it once per bound/2 links instead. Measured over 10,000 384-dimensional
// vectors, an insert went from 3.76ms to 1.08ms, and recall@10 at ef=64 rose
// from 0.997 to 0.999 on the clustered corpus and from 0.459 to 0.554 on the
// uniform one. Rust's alternative, keeping the closest bound, was measured in
// the same run at 1.14ms and failed the recall gate at 0.8965: the heuristic is
// what the recall is made of, and how often it runs is what it costs.
func TestANeighbourListOverflowsHalfItsBoundBeforeItIsPruned(t *testing.T) {
	g := newGraph(Params{M: 4, M0: 8, EfConstruction: 16, EfSearch: 16}, distance.L2)
	const bound, limit = 8, 12

	hub := g.alloc(id.New(), []float32{0, 0})
	// A second node linking to every neighbour, so none of them is the hub's
	// last in-edge and the reachability rescue keeps nothing back.
	other := g.alloc(id.New(), []float32{100, 100})

	for i := range limit + 1 {
		angle := 2 * math.Pi * float64(i) / float64(limit+1)
		r := 1 + float64(i)/10
		e := g.alloc(id.New(), []float32{float32(r * math.Cos(angle)), float32(r * math.Sin(angle))})
		g.nodes[other].links[0] = append(g.nodes[other].links[0], e)
		g.addBack(e, other, 0)

		if _, err := g.link(hub, e, 0); err != nil {
			t.Fatal(err)
		}
		got := len(g.nodes[hub].links[0])
		switch linked := i + 1; {
		case linked <= limit && got != linked:
			t.Fatalf("after %d links the hub holds %d neighbours; a list is not pruned "+
				"until it passes %d (its bound %d and half again)", linked, got, limit, bound)
		case linked > limit && got != bound:
			t.Fatalf("after %d links the hub holds %d neighbours; past %d it is pruned back to %d",
				linked, got, limit, bound)
		}
	}
}
