package hnsw

import ()

// The vector arena: every node's vector in fixed-size chunks, one slot per
// node, so a walk reads vectors from contiguous memory rather than chasing a
// pointer per node. Memory layout only; node records on disk are unchanged.

// The arena's geometry: chunkSlots vectors to a chunk.
//
// A power of two, so that a slot's chunk and its offset within it are a shift
// and a mask. That arithmetic runs once per distance computation, in the
// innermost loop of every walk, and an integer division there would cost more
// than the pointer chase the arena exists to remove.
const (
	chunkShift = 12
	chunkSlots = 1 << chunkShift
	chunkMask  = chunkSlots - 1
)

// vecAt is the vector held in slot s.
//
// The slice is the arena's own and is only valid until the arena next grows,
// which is why every caller is inside one distance computation. See [graph.grow].
func (g *graph) vecAt(s uint32) []float32 {
	off := int(s&chunkMask) * g.dim
	return g.chunks[s>>chunkShift][off : off+g.dim : off+g.dim]
}

// allocSlot copies v into the arena and returns the slot holding it.
//
// It copies rather than retaining v because the arena is what the graph owns:
// a caller's slice may be a view into a decoded row that is about to be reused,
// and a vector that changed under the index would silently invalidate every
// neighbour decision ever made about it.
func (g *graph) allocSlot(v []float32) uint32 {
	if k := len(g.freeSlots); k > 0 {
		s := g.freeSlots[k-1]
		g.freeSlots = g.freeSlots[:k-1]
		copy(g.vecAt(s), v)
		return s
	}
	s := g.nextSlot
	g.nextSlot++
	g.grow(s)
	copy(g.vecAt(s), v)
	return s
}

// grow makes room for slot s.
//
// The first chunk starts at one vector and doubles, rather than being allocated
// in full: most tenants hold a handful of records, the residency budget is
// enforced against what the arena actually holds, and a graph with three
// vectors must cost three vectors and not six thousand. Doubling costs a copy,
// of one chunk and never of the whole arena — which is the point of chunking it
// — and over the first chunk's life that copying allocates about one extra
// vector per vector inserted.
//
// Every later chunk is allocated in full and copies nothing. A graph that
// filled the first chunk holds four thousand records, so a chunk is
// proportionate to it by then, and from there the arena allocates once per
// chunkSlots inserts: less than one allocation per record held, which is the
// whole point.
//
// A chunk moving is why no arena slice may be held across an insert. Every
// caller of [graph.vecAt] is inside a single distance computation, and the only
// slot a walk allocates is the one taken before the walk starts.
func (g *graph) grow(s uint32) {
	c := int(s >> chunkShift)
	for len(g.chunks) <= c {
		g.chunks = append(g.chunks, nil)
	}
	need := (int(s&chunkMask) + 1) * g.dim
	if len(g.chunks[c]) >= need {
		return
	}
	full := chunkSlots * g.dim
	size := full
	if c == 0 {
		for size = g.dim; size < need; size *= 2 {
		}
		if size > full {
			size = full
		}
	}
	grown := make([]float32, size)
	copy(grown, g.chunks[c])
	g.arenaFloats += int64(cap(grown) - cap(g.chunks[c]))
	g.chunks[c] = grown
}

// freeSlot returns a removed node's slot to the arena.
func (g *graph) freeSlot(s uint32) { g.freeSlots = append(g.freeSlots, s) }

// arenaBytes is what the arena holds, including the slots churn left free: the
// memory is resident whether or not a node is using it.
func (g *graph) arenaBytes() int64 {
	var n int64
	for _, c := range g.chunks {
		n += int64(cap(c)) * 4
	}
	return n
}
