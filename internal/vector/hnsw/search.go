package hnsw

import (
	"container/heap"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// greedy walks one layer towards the query until no neighbour is closer.
func (g *graph) greedy(q []float32, qn float64, ep uint32, l int) (uint32, error) {
	best := ep
	bestDist, err := g.dist(q, qn, best)
	if err != nil {
		return 0, err
	}
	for moved := true; moved; {
		moved = false
		n := g.nodes[best]
		if n == nil || l >= len(n.links) {
			break
		}
		for _, m := range n.links[l] {
			if g.nodes[m] == nil {
				continue
			}
			d, err := g.dist(q, qn, m)
			if err != nil {
				return 0, err
			}
			if d < bestDist {
				best, bestDist, moved = m, d, true
			}
		}
	}
	return best, nil
}

// searchLayer is the ef-bounded best-first walk of one layer, returning the
// candidates nearest first.
//
// skip is a node number to ignore — the node currently being inserted, which is
// already in g.nodes and must not become its own neighbour. Zero skips nothing.
func (g *graph) searchLayer(q []float32, qn float64, entries []uint32, ef, l int, skip uint32) ([]candidate, error) {
	if ef < 1 {
		ef = 1
	}
	visited := newVisited(len(g.nodes))
	frontier := &nearestFirst{}
	results := &furthestFirst{}

	for _, ep := range entries {
		if ep == 0 || ep == skip || g.nodes[ep] == nil || visited.seen(ep) {
			continue
		}
		d, err := g.dist(q, qn, ep)
		if err != nil {
			return nil, err
		}
		frontier.push(candidate{node: ep, dist: d})
		results.push(candidate{node: ep, dist: d})
	}

	for frontier.Len() > 0 {
		c := frontier.pop()
		if results.Len() >= ef && c.dist > results.peek().dist {
			break
		}
		n := g.nodes[c.node]
		if n == nil || l >= len(n.links) {
			continue
		}
		for _, m := range n.links[l] {
			if m == skip || g.nodes[m] == nil || visited.seen(m) {
				continue
			}
			d, err := g.dist(q, qn, m)
			if err != nil {
				return nil, err
			}
			if results.Len() < ef {
				frontier.push(candidate{node: m, dist: d})
				results.push(candidate{node: m, dist: d})
				continue
			}
			if d < results.peek().dist {
				frontier.push(candidate{node: m, dist: d})
				results.replaceWorst(candidate{node: m, dist: d})
			}
		}
	}
	return results.sortedNearestFirst(), nil
}

// nearest walks the graph for the k nearest nodes to q, with ef controlling the
// effort. It is the read half of the index and takes no locks of its own.
func (g *graph) nearest(q []float32, ef int) ([]candidate, error) {
	const op = "hnsw.nearest"
	if g.entry == 0 {
		return nil, nil
	}
	if g.dim != 0 && len(q) != g.dim {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"this index holds %d-dimensional vectors and was queried with a %d-dimensional one", g.dim, len(q)))
	}

	// The query's length, once per search rather than once per distance.
	qn := distance.Norm(q)
	ep := g.entry
	for l := g.entryLevel; l > 0; l-- {
		next, err := g.greedy(q, qn, ep, l)
		if err != nil {
			return nil, err
		}
		ep = next
	}
	return g.searchLayer(q, qn, []uint32{ep}, ef, 0, 0)
}

// candidate is a node and its distance from whatever is being searched for.
type candidate struct {
	node uint32
	dist float32
}

// visited is a bitset over node numbers.
//
// A map would be simpler and is what the first version used; at a quarter of a
// million nodes the hashing dominated the distance computations, which is the
// wrong shape entirely for a structure whose whole purpose is to compute fewer
// distances.
type visited []uint64

func newVisited(n int) visited { return make(visited, (n+63)/64) }

func (v visited) seen(n uint32) bool {
	w, b := n/64, uint64(1)<<(n%64)
	if int(w) >= len(v) {
		return true // out of range cannot be a live node; treat it as done
	}
	if v[w]&b != 0 {
		return true
	}
	v[w] |= b
	return false
}

func keys32(m map[uint32]bool) []uint32 {
	out := make([]uint32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// sortCandidates orders nearest first. It is insertion sort because the slices
// are neighbour lists — tens of elements, never thousands.
func sortCandidates(c []candidate) {
	for i := 1; i < len(c); i++ {
		for j := i; j > 0 && c[j].dist < c[j-1].dist; j-- {
			c[j], c[j-1] = c[j-1], c[j]
		}
	}
}

// The two heaps below are separate types rather than one with a comparison
// function, because a search runs both at once and an indirect call per
// comparison inside the innermost loop of the innermost loop is the one place
// in this package where that cost is visible.

// nearestFirst is the exploration frontier: a min-heap, so the walk always
// expands the most promising candidate it has seen.
type nearestFirst []candidate

func (h nearestFirst) Len() int           { return len(h) }
func (h nearestFirst) Less(i, j int) bool { return h[i].dist < h[j].dist }
func (h nearestFirst) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *nearestFirst) Push(x any)        { *h = append(*h, x.(candidate)) }
func (h *nearestFirst) Pop() any          { old := *h; n := len(old); *h = old[:n-1]; return old[n-1] }
func (h *nearestFirst) push(c candidate)  { heap.Push(h, c) }
func (h *nearestFirst) pop() candidate    { return heap.Pop(h).(candidate) }

// furthestFirst is the result set: a max-heap bounded at ef, so deciding
// whether a candidate belongs is one comparison against the root.
type furthestFirst []candidate

func (h furthestFirst) Len() int           { return len(h) }
func (h furthestFirst) Less(i, j int) bool { return h[i].dist > h[j].dist }
func (h furthestFirst) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *furthestFirst) Push(x any)        { *h = append(*h, x.(candidate)) }
func (h *furthestFirst) Pop() any          { old := *h; n := len(old); *h = old[:n-1]; return old[n-1] }
func (h *furthestFirst) push(c candidate)  { heap.Push(h, c) }
func (h furthestFirst) peek() candidate    { return h[0] }

func (h *furthestFirst) replaceWorst(c candidate) {
	(*h)[0] = c
	heap.Fix(h, 0)
}

// sortedNearestFirst drains the heap into a slice ordered nearest first.
func (h *furthestFirst) sortedNearestFirst() []candidate {
	out := make([]candidate, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(candidate)
	}
	return out
}
