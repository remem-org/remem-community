package hnsw

import (
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// The vector arena's two invariants, which no higher-level test can see.
//
// Vectors live in the graph rather than one slice per node (plan item 15), so a
// distance call reads a sub-slice of a chunk instead of chasing a pointer per
// node. That only holds together if a slot keeps its own vector across the
// growth that crossing a chunk boundary causes, and if a removal's slot is
// handed out again rather than leaked — a graph that churned a million records
// would otherwise hold a million vectors for the thousand it still has.

// TestEverySlotKeepsItsOwnVectorAcrossGrowth inserts well past a chunk boundary
// and then reads every node's vector back through the arena.
func TestEverySlotKeepsItsOwnVectorAcrossGrowth(t *testing.T) {
	g := newGraph(Defaults(), distance.Cosine)

	const dim = 3
	const count = chunkSlots + 50 // two chunks, and the first one grown to full
	want := make(map[uint32][]float32, count)
	for i := range count {
		rid := digestID(uint64(i) + 1)
		v := []float32{float32(i), float32(i) * 2, float32(i) * 3}
		if _, err := g.insert(vector.Key{ID: rid}, v); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
		want[g.byID[vector.Key{ID: rid}]] = v
	}
	if len(g.chunks) < 2 {
		t.Fatalf("%d vectors fit in %d chunk(s): the arena never chunked", count, len(g.chunks))
	}
	// The growth policy, which is a memory claim and not an implementation
	// detail: the first chunk doubles up to full, and every chunk after it is
	// allocated full and copies nothing.
	for c, want := range map[int]int{0: chunkSlots * dim, 1: chunkSlots * dim} {
		if got := len(g.chunks[c]); got != want {
			t.Fatalf("chunk %d holds %d floats, want %d", c, got, want)
		}
	}
	for num, v := range want {
		got := g.vecAt(g.nodes[num].slot)
		if len(got) != dim {
			t.Fatalf("node %d reads back %d dimensions, want %d", num, len(got), dim)
		}
		for i := range v {
			if got[i] != v[i] {
				t.Fatalf("node %d component %d is %v, want %v: the arena moved a vector", num, i, got[i], v[i])
			}
		}
	}
}

// TestARemovedSlotIsHandedOutAgain churns the graph and checks the arena does
// not grow for records that are no longer there.
func TestARemovedSlotIsHandedOutAgain(t *testing.T) {
	g := newGraph(Defaults(), distance.Cosine)
	rng := newRNG(5)

	const corpus = 200
	ids := make([]id.ID, corpus)
	for i := range ids {
		ids[i] = digestID(uint64(i) + 1)
		if _, err := g.insert(vector.Key{ID: ids[i]}, rng.unit(8)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	grown := g.nextSlot
	if int(grown) != corpus {
		t.Fatalf("%d vectors took %d slots, want one each", corpus, grown)
	}
	for _, rid := range ids {
		g.remove(g.byID[vector.Key{ID: rid}])
	}
	for i := range corpus {
		if _, err := g.insert(vector.Key{ID: digestID(uint64(1<<20 + i))}, rng.unit(8)); err != nil {
			t.Fatalf("re-insert %d: %v", i, err)
		}
	}
	if g.nextSlot != grown {
		t.Fatalf("the arena grew to %d slots over a churn of %d records that left it at %d: "+
			"a removed slot was leaked", g.nextSlot, corpus, grown)
	}
}
