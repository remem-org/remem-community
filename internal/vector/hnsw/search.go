package hnsw

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
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
//
// s is the caller's working set, held exclusively for the whole walk; see
// [scratch].
func (g *graph) searchLayer(q []float32, qn float64, entries []uint32, ef, l int, skip uint32, s *scratch) ([]candidate, error) {
	if ef < 1 {
		ef = 1
	}
	s.prepare(len(g.nodes), ef)
	visited := s.visited
	frontier := &s.frontier
	results := &s.results

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
	s := g.takeScratch()
	defer g.putScratch(s)
	return g.searchLayer(q, qn, []uint32{ep}, ef, 0, 0, s)
}

// nearestMemories walks the graph for the ef nearest *memories* to q, each at its
// nearest entry, nearest first.
//
// It is [graph.nearest] with the bottom layer grouped by memory, which is what
// lets ef bound distinct memories rather than passages. A memory with a
// thousand passages nearest the query would otherwise fill an ef-64 result set
// with its own passages, and a search for ten memories would find one and
// double its effort until the graph was exhausted.
func (g *graph) nearestMemories(q []float32, ef int) ([]candidate, error) {
	const op = "hnsw.nearestMemories"
	if g.entry == 0 {
		return nil, nil
	}
	if g.dim != 0 && len(q) != g.dim {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"this index holds %d-dimensional vectors and was queried with a %d-dimensional one", g.dim, len(q)))
	}
	qn := distance.Norm(q)
	ep := g.entry
	for l := g.entryLevel; l > 0; l-- {
		next, err := g.greedy(q, qn, ep, l)
		if err != nil {
			return nil, err
		}
		ep = next
	}
	s := g.takeScratch()
	defer g.putScratch(s)
	return g.searchGrouped(q, qn, ep, ef, s)
}

// searchGrouped is the bottom layer's best-first walk with a result set keyed
// by memory.
//
// Exploration is the ordinary walk: every neighbour that could improve the
// result set is expanded, whichever memory it belongs to, because a memory's
// passages are how the walk reaches the memories beyond it. What changes is the
// result set. It holds at most one candidate per memory, the nearest of its
// passages seen so far; a nearer passage of a memory already present replaces
// it rather than taking a second slot, and the stopping rule compares against
// the worst of ef distinct memories.
//
// The replacement is lazy: the heap keeps the superseded candidate, and best,
// which maps each memory to its current one, decides which heap entries are
// live. A stale entry is discarded when it reaches the top.
func (g *graph) searchGrouped(q []float32, qn float64, ep uint32, ef int, s *scratch) ([]candidate, error) {
	if ef < 1 {
		ef = 1
	}
	s.prepare(len(g.nodes), ef)
	visited := s.visited
	frontier := &s.frontier
	results := &s.results
	if s.best == nil {
		s.best = make(map[id.ID]candidate, ef)
	} else {
		clear(s.best)
	}
	best := s.best

	memoryOf := func(n uint32) id.ID { return g.nodes[n].key.ID }
	// worst is the furthest live candidate, discarding stale ones on the way.
	worst := func() candidate {
		for results.Len() > 0 {
			top := results.peek()
			if cur, ok := best[memoryOf(top.node)]; ok && cur.node == top.node {
				return top
			}
			results.pop()
		}
		return candidate{}
	}
	offer := func(c candidate) {
		mem := memoryOf(c.node)
		if cur, ok := best[mem]; ok {
			if c.dist < cur.dist {
				best[mem] = c
				results.push(c)
			}
			return
		}
		if len(best) < ef {
			best[mem] = c
			results.push(c)
			return
		}
		if w := worst(); c.dist < w.dist {
			results.pop()
			delete(best, memoryOf(w.node))
			best[mem] = c
			results.push(c)
		}
	}

	if ep == 0 || g.nodes[ep] == nil || visited.seen(ep) {
		return nil, nil
	}
	d, err := g.dist(q, qn, ep)
	if err != nil {
		return nil, err
	}
	frontier.push(candidate{node: ep, dist: d})
	offer(candidate{node: ep, dist: d})

	for frontier.Len() > 0 {
		c := frontier.pop()
		if len(best) >= ef && c.dist > worst().dist {
			break
		}
		n := g.nodes[c.node]
		if n == nil || len(n.links) == 0 {
			continue
		}
		for _, m := range n.links[0] {
			if g.nodes[m] == nil || visited.seen(m) {
				continue
			}
			d, err := g.dist(q, qn, m)
			if err != nil {
				return nil, err
			}
			if len(best) < ef || d < worst().dist {
				frontier.push(candidate{node: m, dist: d})
				offer(candidate{node: m, dist: d})
			}
		}
	}

	out := make([]candidate, 0, len(best))
	for _, c := range best {
		out = append(out, c)
	}
	// Nearest first, and a tie by node number, so the order never depends on
	// map iteration.
	slices.SortFunc(out, func(a, b candidate) int {
		if a.dist != b.dist {
			return cmp.Compare(a.dist, b.dist)
		}
		return cmp.Compare(a.node, b.node)
	})
	return out, nil
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

// scratch is one graph walk's working set: the visited bitset and the two heaps.
//
// A walk used to make all three per layer and let the heaps grow by append,
// which on a ten-thousand-node graph was most of what a search allocated and
// most of what an insert allocated too, since an insert walks every layer. They
// are pooled per graph instead: one scratch serves all the layers of one walk,
// and the walk holds it exclusively between takeScratch and putScratch, so the
// concurrent searches running under a resident graph's read lock need no
// coordination beyond the pool itself.
//
// What a walk hands back to its caller is always a copy. Pooled memory never
// leaves the walk.
type scratch struct {
	visited  visited
	frontier nearestFirst
	results  furthestFirst
	// best is a grouped walk's current candidate per memory. Only
	// [graph.searchGrouped] uses it.
	best map[id.ID]candidate
}

// prepare readies the scratch for a walk over nodes node numbers bounded at ef,
// keeping whatever capacity the previous walk left behind.
func (s *scratch) prepare(nodes, ef int) {
	words := (nodes + 63) / 64
	if cap(s.visited) < words {
		// The graph grew past the bitset. A fresh one is already zeroed.
		s.visited = make(visited, words)
	} else {
		// A shorter previous walk leaves set bits past its own length, so the
		// clear has to cover this walk's length and not the last one's.
		s.visited = s.visited[:words]
		clear(s.visited)
	}
	if cap(s.frontier) < ef {
		s.frontier = make(nearestFirst, 0, ef)
	} else {
		s.frontier = s.frontier[:0]
	}
	if cap(s.results) < ef {
		s.results = make(furthestFirst, 0, ef)
	} else {
		s.results = s.results[:0]
	}
}

// takeScratch borrows this graph's working set for one walk. The caller returns
// it with putScratch, and holds it exclusively until it does.
func (g *graph) takeScratch() *scratch {
	if s, ok := g.walks.Get().(*scratch); ok {
		return s
	}
	return &scratch{}
}

func (g *graph) putScratch(s *scratch) { g.walks.Put(s) }

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
//
// They also sift by hand rather than through container/heap, which takes its
// elements as `any`: boxing a candidate into an interface allocated once per
// push and once per pop, and one ef-64 search over ten thousand nodes does about
// five hundred of each — 96% of everything the search allocated. The up and down
// loops below are container/heap's own, transcribed with the comparison written
// out, so the heap order and its tie-breaking are unchanged.

// nearestFirst is the exploration frontier: a min-heap, so the walk always
// expands the most promising candidate it has seen.
type nearestFirst []candidate

func (h nearestFirst) Len() int { return len(h) }

func (h *nearestFirst) push(c candidate) {
	*h = append(*h, c)
	h.up(len(*h) - 1)
}

func (h *nearestFirst) pop() candidate {
	s := *h
	n := len(s) - 1
	s[0], s[n] = s[n], s[0]
	s.down(0, n)
	*h = s[:n]
	return s[n]
}

func (h nearestFirst) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !(h[j].dist < h[i].dist) {
			break
		}
		h[i], h[j] = h[j], h[i]
		j = i
	}
}

func (h nearestFirst) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h[j2].dist < h[j1].dist {
			j = j2
		}
		if !(h[j].dist < h[i].dist) {
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
	return i > i0
}

// furthestFirst is the result set: a max-heap bounded at ef, so deciding
// whether a candidate belongs is one comparison against the root.
type furthestFirst []candidate

func (h furthestFirst) Len() int        { return len(h) }
func (h furthestFirst) peek() candidate { return h[0] }

func (h *furthestFirst) push(c candidate) {
	*h = append(*h, c)
	h.up(len(*h) - 1)
}

func (h *furthestFirst) pop() candidate {
	s := *h
	n := len(s) - 1
	s[0], s[n] = s[n], s[0]
	s.down(0, n)
	*h = s[:n]
	return s[n]
}

// replaceWorst overwrites the furthest candidate and sifts the new one into
// place: container/heap's Fix, which tries down first and only then up.
func (h *furthestFirst) replaceWorst(c candidate) {
	(*h)[0] = c
	if !(*h).down(0, len(*h)) {
		(*h).up(0)
	}
}

func (h furthestFirst) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !(h[j].dist > h[i].dist) {
			break
		}
		h[i], h[j] = h[j], h[i]
		j = i
	}
}

func (h furthestFirst) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h[j2].dist > h[j1].dist {
			j = j2
		}
		if !(h[j].dist > h[i].dist) {
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
	return i > i0
}

// sortedNearestFirst drains the heap into a slice ordered nearest first. The
// slice is the walk's own: the heap it came from is pooled and the caller's copy
// must outlive it.
func (h *furthestFirst) sortedNearestFirst() []candidate {
	out := make([]candidate, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = h.pop()
	}
	return out
}
