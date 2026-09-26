package hnsw

import (
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// node is one record's place in the graph.
//
// The vector is held in memory and never written to a node record. It came
// from the canonical row and it goes back there — see the package comment.
type node struct {
	id  id.ID
	vec []float32
	// norm is vec's length, computed once when the node is made. A node's
	// vector never changes in place — a replacement is a removal and an insert
	// — so it cannot go stale.
	norm  float64
	level int
	// links[l] are the neighbours at layer l. No order is maintained: the
	// bound is a set bound, and search re-orders anyway.
	links [][]uint32
	// back[l] are the nodes that link *to* this one at layer l.
	//
	// It is derived from links and is not persisted — the loader rebuilds it
	// from the node records it reads. It exists because HNSW edges are not
	// symmetric: an insert adds a back-link, and a later prune may drop it
	// again, so a removed node cannot find everyone still pointing at it by
	// walking its own neighbours. Without this, removals leave references to
	// nodes that no longer exist, and after enough churn a node whose whole
	// neighbour list has rotted away is unreachable while still holding a
	// vector nobody can find.
	back [][]uint32
}

// graph is one tenant's resident index.
//
// Node numbers are dense and start at one. Zero is reserved for the meta
// record in the same key space, so a node number is never confused with "no
// node" — which is the value entry takes on an empty graph.
type graph struct {
	params Params
	metric distance.Metric
	dim    int

	nodes []*node // indexed by node number; nodes[0] is always nil
	byID  map[id.ID]uint32

	entry      uint32 // 0 when the graph is empty
	entryLevel int

	// free holds node numbers left by removals, so allocation is O(1).
	//
	// The first version scanned for a hole instead, which is correct and is
	// quadratic: a rebuild inserting a quarter of a million records walked the
	// node slice once per insert. Reuse itself is worth having — a graph that
	// only counted upwards would leave the key space sparse after a churn of
	// deletes, and only a rebuild would compact it.
	free []uint32

	live int
}

func newGraph(p Params, m distance.Metric) *graph {
	return &graph{
		params: p.withDefaults(),
		metric: m,
		nodes:  []*node{nil}, // node number zero is the meta slot
		byID:   map[id.ID]uint32{},
	}
}

// bound is the neighbour limit at layer l.
func (g *graph) bound(l int) int {
	if l == 0 {
		return g.params.M0
	}
	return g.params.M
}

// dist compares a query against a node, and treats a width mismatch as what it
// is: a vector from another model reached the graph.
func (g *graph) dist(q []float32, qn float64, n uint32) (float32, error) {
	return g.between(q, qn, g.nodes[n].vec, g.nodes[n].norm)
}

// between is the distance from a to b under the index's metric, given both
// vectors' lengths.
//
// Under cosine it uses the lengths instead of recomputing them, which is the
// whole reason nodes carry one: an insert computes thousands of distances, and
// in Phase 13 recomputing two norms for each was a third of the write path's
// CPU. The result is bit-identical to distance.Compute's. The other metrics
// ignore the lengths.
func (g *graph) between(a []float32, an float64, b []float32, bn float64) (float32, error) {
	if g.metric == distance.Cosine && len(a) == len(b) {
		return distance.CosineWithNorms(a, b, an, bn), nil
	}
	return distance.Compute(g.metric, a, b)
}

// insert adds or replaces a record's vector, returning the node numbers whose
// links changed so the caller can persist exactly those.
//
// A replacement is a removal followed by an insert rather than an in-place
// edit: the vector moved, so every neighbour decision made about the old
// position is now about the wrong point in the space.
func (g *graph) insert(rid id.ID, v []float32) ([]uint32, error) {
	const op = "hnsw.insert"

	if len(v) == 0 {
		return nil, errs.E(errs.Invalid, op, errors.New("an empty vector cannot be indexed"))
	}
	if g.dim == 0 {
		g.dim = len(v)
	} else if len(v) != g.dim {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"this index holds %d-dimensional vectors and was given a %d-dimensional one", g.dim, len(v)))
	}

	dirty := map[uint32]bool{}
	if old, ok := g.byID[rid]; ok {
		for _, n := range g.remove(old) {
			dirty[n] = true
		}
	}

	n := g.alloc(rid, v)
	dirty[n] = true

	if g.entry == 0 {
		g.entry = n
		g.entryLevel = g.nodes[n].level
		return keys32(dirty), nil
	}

	level := g.nodes[n].level
	vn := g.nodes[n].norm
	ep := g.entry

	// Above the new node's own top layer the walk is a plain greedy descent:
	// there is nothing to link, only a better entry point to find.
	for l := g.entryLevel; l > level; l-- {
		next, err := g.greedy(v, vn, ep, l)
		if err != nil {
			return nil, err
		}
		ep = next
	}

	top := level
	if g.entryLevel < top {
		top = g.entryLevel
	}
	for l := top; l >= 0; l-- {
		found, err := g.searchLayer(v, vn, []uint32{ep}, g.params.EfConstruction, l, n)
		if err != nil {
			return nil, err
		}
		chosen, err := g.selectNeighbours(v, found, g.bound(l))
		if err != nil {
			return nil, err
		}
		g.nodes[n].links[l] = chosen
		for _, m := range chosen {
			g.addBack(m, n, l)
		}

		for _, m := range chosen {
			changed, err := g.link(m, n, l)
			if err != nil {
				return nil, err
			}
			if changed {
				dirty[m] = true
			}
		}
		// The nearest candidate is the next layer's entry point. Carrying the
		// whole set down instead is the paper's formulation and was measured
		// here at 0.908 against 0.906 on the same corpus — indistinguishable,
		// for a walk that starts from ef points instead of one.
		if len(found) > 0 {
			ep = found[0].node
		}
	}

	if level > g.entryLevel {
		g.entry = n
		g.entryLevel = level
	}
	return keys32(dirty), nil
}

// alloc gives a record a node number, reusing a hole left by a removal.
//
// Reuse matters because node numbers address rows: a graph that only ever
// counted upwards would leave the key space sparse after a churn of deletes,
// and a rebuild would be the only way to get it back.
func (g *graph) alloc(rid id.ID, v []float32) uint32 {
	n := &node{id: rid, vec: v, norm: distance.Norm(v), level: levelOf(rid, g.params.M)}
	n.links = make([][]uint32, n.level+1)
	n.back = make([][]uint32, n.level+1)

	if k := len(g.free); k > 0 {
		num := g.free[k-1]
		g.free = g.free[:k-1]
		g.nodes[num] = n
		g.byID[rid] = num
		g.live++
		return num
	}
	g.nodes = append(g.nodes, n)
	num := uint32(len(g.nodes) - 1)
	g.byID[rid] = num
	g.live++
	return num
}

// link adds an edge from m to n at layer l, pruning m's neighbours back to the
// layer bound when it overflows. It reports whether m's links changed.
func (g *graph) link(m, n uint32, l int) (bool, error) {
	mn := g.nodes[m]
	if l >= len(mn.links) {
		return false, nil
	}
	for _, e := range mn.links[l] {
		if e == n {
			return false, nil
		}
	}
	mn.links[l] = append(mn.links[l], n)
	g.addBack(n, m, l)

	// A list may overflow its bound by half before it is pruned back to the
	// bound. Profiled in Phase 13, 81% of an insert was the diversity heuristic
	// re-run on every neighbour the new node pushed one past its bound; letting
	// the list overflow runs it once per bound/2 links instead. Measured over
	// 10,000 384-dimensional vectors, an insert fell from 3.76ms to 1.08ms and
	// recall@10 at ef=64 *rose*, 0.997 to 0.999 clustered and 0.459 to 0.554
	// uniform, since a search meets the extra edges before they are pruned.
	// Rust's cheaper rule, keeping the closest bound, measured 1.14ms and 0.8965
	// in the same run, below the recall gate: the heuristic is what the recall is
	// made of, and how often it runs is what it costs.
	bound := g.bound(l)
	if len(mn.links[l]) <= bound+bound/2 {
		return true, nil
	}

	cands := make([]candidate, 0, len(mn.links[l]))
	for _, e := range mn.links[l] {
		d, err := g.dist(mn.vec, mn.norm, e)
		if err != nil {
			return false, err
		}
		cands = append(cands, candidate{node: e, dist: d})
	}
	sortCandidates(cands)
	pruned, err := g.selectNeighbours(mn.vec, cands, bound)
	if err != nil {
		return false, err
	}
	// A neighbour about to be dropped is kept anyway when m is the last node
	// linking to it. Search follows out-edges, so a node nothing points at
	// cannot be reached at all: it keeps its own edges, keeps its vector, and
	// simply stops being findable.
	//
	// This is not a corner case invented for a test. The diversity heuristic
	// above never prunes a near-duplicate in favour of a distant node, so one
	// memory unlike a cluster of similar ones is dropped by every member of
	// that cluster in turn — and a search for the outlier's own vector then
	// returns an unrelated memory at maximum distance, confidently. Rescuing
	// the last in-edge costs a handful of slots above the bound and is the
	// difference between an approximate answer and a wrong one.
	for _, e := range mn.links[l] {
		if contains(pruned, e) {
			continue
		}
		if n := g.nodes[e]; n != nil && l < len(n.back) && len(n.back[l]) <= 1 {
			pruned = append(pruned, e)
			continue
		}
		g.dropBack(e, m, l)
	}
	mn.links[l] = pruned
	return true, nil
}

// addBack records that from links to to at layer l.
func (g *graph) addBack(to, from uint32, l int) {
	n := g.nodes[to]
	if n == nil || l >= len(n.back) {
		return
	}
	if !contains(n.back[l], from) {
		n.back[l] = append(n.back[l], from)
	}
}

// dropBack forgets that from linked to to at layer l.
func (g *graph) dropBack(to, from uint32, l int) {
	n := g.nodes[to]
	if n == nil || l >= len(n.back) {
		return
	}
	n.back[l] = without(n.back[l], from)
}

func contains(s []uint32, v uint32) bool {
	for _, e := range s {
		if e == v {
			return true
		}
	}
	return false
}

func without(s []uint32, v uint32) []uint32 {
	out := s[:0]
	for _, e := range s {
		if e != v {
			out = append(out, e)
		}
	}
	return out
}

// remove takes a record out of the graph and returns the node numbers whose
// links changed.
//
// The node is unlinked rather than tombstoned. There is no durable deleted set
// in this package: the canonical vector's absence is the deletion, and a node
// record naming a record with no canonical vector is dropped when the tenant is
// materialised. So this loop exists for the quality of the resident graph and
// the rows it writes back, and correctness never depends on it landing.
func (g *graph) remove(n uint32) []uint32 {
	victim := g.nodes[n]
	if victim == nil {
		return nil
	}
	dirty := map[uint32]bool{}

	// Everyone pointing at the victim stops pointing at it, and is offered the
	// victim's own neighbours in its place. The repair matters: a node whose
	// only route into a region was through the victim would otherwise be cut
	// off from it, and the search would return the wrong answer confidently
	// rather than slowly.
	for l := range victim.back {
		for _, m := range victim.back[l] {
			mn := g.nodes[m]
			if mn == nil || l >= len(mn.links) {
				continue
			}
			mn.links[l] = without(mn.links[l], n)
			dirty[m] = true
			for _, repl := range victim.links[l] {
				if repl == m || g.nodes[repl] == nil {
					continue
				}
				if len(mn.links[l]) >= g.bound(l) {
					break
				}
				if _, err := g.link(m, repl, l); err != nil {
					// A distance failure here means a vector of the wrong
					// width is resident, which insert already refuses. The
					// removal itself must still complete.
					break
				}
			}
		}
	}
	for l := range victim.links {
		for _, m := range victim.links[l] {
			g.dropBack(m, n, l)
		}
	}

	delete(g.byID, victim.id)
	g.nodes[n] = nil
	g.free = append(g.free, n)
	g.live--

	if g.entry == n {
		g.entry, g.entryLevel = g.findEntry()
	}
	return keys32(dirty)
}

// findEntry picks a new entry point after the old one was removed: the highest
// node still present, and any of them will do at that height.
func (g *graph) findEntry() (uint32, int) {
	best, bestLevel := uint32(0), -1
	for i := uint32(1); i < uint32(len(g.nodes)); i++ {
		if n := g.nodes[i]; n != nil && n.level > bestLevel {
			best, bestLevel = i, n.level
		}
	}
	if bestLevel < 0 {
		return 0, 0
	}
	return best, bestLevel
}

// selectNeighbours is the paper's heuristic (Algorithm 4), not a plain nearest-M.
//
// A candidate is kept only when it is closer to the base point than to
// everything already kept. That is what stops a node's entire neighbour list
// collapsing onto one dense cluster and leaving the graph unable to route out
// of it — the failure that shows up as recall falling off a cliff on clustered
// corpora while it looks fine on uniform random ones.
//
// candidates must be ordered nearest-first.
func (g *graph) selectNeighbours(base []float32, candidates []candidate, bound int) ([]uint32, error) {
	if len(candidates) <= bound {
		out := make([]uint32, len(candidates))
		for i, c := range candidates {
			out[i] = c.node
		}
		return out, nil
	}

	chosen := make([]uint32, 0, bound)
	// Rejected candidates are kept, in order, and used to top the list back up
	// to the bound. Dropping them instead leaves sparse nodes with fewer
	// neighbours than they are allowed, which costs recall for nothing.
	deferred := make([]uint32, 0, len(candidates))

	for _, c := range candidates {
		if len(chosen) >= bound {
			break
		}
		keep := true
		for _, s := range chosen {
			cn, sn := g.nodes[c.node], g.nodes[s]
			d, err := g.between(cn.vec, cn.norm, sn.vec, sn.norm)
			if err != nil {
				return nil, err
			}
			if d < c.dist {
				keep = false
				break
			}
		}
		if keep {
			chosen = append(chosen, c.node)
		} else {
			deferred = append(deferred, c.node)
		}
	}
	for i := 0; len(chosen) < bound && i < len(deferred); i++ {
		chosen = append(chosen, deferred[i])
	}
	return chosen, nil
}

// residentBytes estimates what this graph costs in memory, for the budget the
// residency cache enforces. It is an estimate and says so: Go gives no exact
// answer, and a budget enforced against a number nobody can compute would be a
// setting that does nothing.
func (g *graph) residentBytes() int64 {
	const perNodeOverhead = 96 // struct, slice headers, map entry
	var links int64
	for _, n := range g.nodes {
		if n == nil {
			continue
		}
		for _, l := range n.links {
			links += int64(len(l)) * 4
		}
		for _, l := range n.back {
			links += int64(len(l)) * 4
		}
	}
	return int64(g.live)*(perNodeOverhead+int64(g.dim)*4) + links
}
