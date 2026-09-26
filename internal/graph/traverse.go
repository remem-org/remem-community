package graph

import (
	"container/heap"
	"context"
	"errors"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
)

// Traversal bounds. A traversal is never unbounded in either dimension: the
// corpus these run over is one a background job populates, so "the
// neighbourhood" is a size nobody chose.
const (
	// DefaultMaxDepth is how far a caller who named no depth goes. One hop,
	// which is Rust's `find_related` default.
	DefaultMaxDepth = 1
	// MaxTraversalDepth is the deepest a caller may ask for. Rust's cap, kept.
	MaxTraversalDepth = 5
	// DefaultMaxNodes is how many nodes a caller who named no bound gets.
	DefaultMaxNodes = 200
	// MaxTraversalNodes is the hard ceiling on one traversal.
	MaxTraversalNodes = 10000
)

// TraverseOpts bounds and narrows a traversal.
type TraverseOpts struct {
	// MaxDepth is how many hops from the anchor the walk goes. Zero means
	// [DefaultMaxDepth]; above [MaxTraversalDepth] is refused.
	MaxDepth int
	// MaxNodes is the hard stop on how many nodes are returned. Zero means
	// [DefaultMaxNodes]; above [MaxTraversalNodes] is refused. When it bites,
	// [Traversal.Truncated] says so.
	MaxNodes int
	// Types restricts the walk to these relationship types. Empty means every
	// type.
	Types []RelationshipType
	// Direction is which way edges are followed. The zero value is [Out].
	Direction Direction
	// MinStrength drops edges weaker than this before they are followed. It
	// prunes the walk rather than filtering its output, which is the point:
	// a weak edge is not worth a node out of the budget.
	MinStrength float32
}

// Reached is one node a traversal arrived at.
type Reached struct {
	ID id.ID `json:"id"`
	// Depth is how many hops away the node is along the path that produced
	// PathStrength — not necessarily the fewest hops there are.
	Depth int `json:"depth"`
	// PathStrength is the product of the edge strengths along the strongest
	// path reaching this node within MaxDepth.
	PathStrength float32 `json:"path_strength"`
	// Via is the last edge on that path: what connected this node to the rest
	// of the walk, and the relationship a caller wants to show.
	Via Edge `json:"via"`
}

// Traversal is what one walk found.
//
// It is a struct rather than the plan's bare `[]Reached` because the plan's own
// completion criterion requires a traversal that hit its node bound to report
// that it did, and a slice has nowhere to put it. A short list that cannot be
// told apart from a complete one is the failure this whole phase's honesty
// contract exists to prevent — the same distinction query.Result draws between
// "there are none" and "I stopped looking".
type Traversal struct {
	// Reached is every node found, strongest path first, then shallowest.
	Reached []Reached `json:"reached"`
	// Truncated reports that the node budget was spent before the reachable
	// neighbourhood was exhausted, so more weakly connected memories exist that
	// were never reached.
	Truncated bool `json:"truncated"`
}

// Traverse walks outward from a record and returns the nodes it reached.
//
// # Why best-first and not breadth-first
//
// The walk is bounded by a node budget, and how that budget is spent is the
// whole of the ranking. Breadth-first spends it on the nearest hops: a memory
// with five hundred weak direct neighbours consumes the entire budget on those,
// and the strongly connected memory two hops out is never looked at. Sorting the
// results afterwards sorts a sample that was already chosen on the wrong
// criterion.
//
// Worse, a breadth-first walk's path strength is whichever path it happened to
// find first, which depends on the order the store yields edges in. That number
// would change after a compaction, could not have a threshold put against it,
// and could not be compared against the Rust reference in Phase 13.
//
// So the frontier is a max-heap ordered by descending path strength. Each node
// reports the strongest chain that reaches it, `MaxNodes` truncates to the
// strongest N rather than the shallowest N, and Truncated means "weaker
// connections exist that were not reached".
//
// This is a deliberate correction to plan §Phase 6's "bounded BFS" wording, and
// it is what fixes REM-83 (plan §II.10 row 7): Rust ranks by hop count and
// discards the weight its own edges carry.
//
// # Why a node is remembered per depth, not once
//
// The bound is hops from the anchor, so a path is worth following while no
// other path reaching the same node is both at least as strong and no deeper.
// Remembering only each node's strongest path is not enough, and until Phase 13
// it was what the walk did. A node reached strongly at depth 2 and weakly at
// depth 1 was expanded only at depth 2, so with MaxDepth 2 its neighbours —
// two hops from the anchor through the weak edge — were never returned, where
// Rust's breadth-first walk returns them. Row 7 changed how the reached set is
// ranked, never what it contains. Keeping one strongest path per node *and*
// one shallowest is not enough either: a middling path can be beaten on
// strength by the first and on depth by the second, and still be the only one
// that reaches a further node strongly within the bound.
// TestTraversalReachesEveryNodeWithinMaxDepth holds the case, and
// TestTraversalMatchesAHopBoundedReference holds the whole family.
//
// # Why it terminates, and why a node is reported by its first pop
//
// Strengths lie in [0, 1], so a path's product is non-increasing as it grows.
// The heap pops strongest first, so the first time a node is popped it holds
// the strongest path within the bound, and that is the path it is reported by.
// A later, weaker pop at a shallower depth is expanded but not reported again.
// A path is pushed only when it strictly beats every path to that node at the
// same or a shallower depth. Going round a cycle multiplies by at most 1 and
// adds a hop, so it never qualifies, and a cycle terminates without a
// visited-set special case.
//
// # What it does not do
//
// It reads no record bodies. A traversal that checked whether each node still
// exists would pay a record read per node inside the bound that is supposed to
// make traversal cheap. An edge pointing at a deleted record therefore survives
// here and is dropped by the caller that materialises records — and is reported
// by `remem-admin graph verify`, which is where an orphan is meant to surface.
func Traverse(ctx context.Context, r Reader, sc Scope, start id.ID, opts TraverseOpts) (Traversal, error) {
	const op = "graph.Traverse"

	if err := sc.validate(op); err != nil {
		return Traversal{}, err
	}
	if start.IsZero() {
		return Traversal{}, errs.E(errs.Invalid, op, errors.New("a traversal needs a record to start from"))
	}
	if !opts.Direction.Valid() {
		return Traversal{}, errs.E(errs.Invalid, op, fmt.Errorf("%q is not a direction", opts.Direction))
	}
	maxDepth, maxNodes, err := opts.bounds(op)
	if err != nil {
		return Traversal{}, err
	}

	// best[node][d] is the strongest path found reaching node in exactly d
	// hops, or -1 for none. The anchor sits at 1 at depth 0 and is never
	// emitted: it is where the caller already is, and returning it among its
	// own neighbours is the self-edge mistake by another route.
	best := map[id.ID][]float32{}
	improves := func(node id.ID, depth int, strength float32) bool {
		at, ok := best[node]
		if !ok {
			at = make([]float32, maxDepth+1)
			for d := range at {
				at[d] = -1
			}
			best[node] = at
		}
		for d := 0; d <= depth; d++ {
			if at[d] >= strength {
				return false
			}
		}
		at[depth] = strength
		return true
	}
	improves(start, 0, 1)
	frontier := &frontier{{node: start, depth: 0, strength: 1}}
	heap.Init(frontier)

	reached := make([]Reached, 0, min(maxNodes, 32))
	emitted := map[id.ID]bool{}
	var truncated bool

	for frontier.Len() > 0 {
		cur := heap.Pop(frontier).(step)

		// A stale heap entry: a stronger path at the same depth or shallower
		// was pushed after this one, so this one has nothing to add.
		if stale(best[cur.node], cur) {
			continue
		}
		if cur.node != start && !emitted[cur.node] {
			if len(reached) >= maxNodes {
				// The budget is spent. Everything still on the frontier is
				// weaker than what has been returned, which is exactly what
				// Truncated is telling the caller.
				truncated = true
				break
			}
			emitted[cur.node] = true
			reached = append(reached, Reached{
				ID: cur.node, Depth: cur.depth, PathStrength: cur.strength, Via: cur.via,
			})
		}
		if cur.depth >= maxDepth {
			continue
		}

		for _, dir := range opts.Direction.each() {
			err := scanNeighbours(ctx, r, sc, cur.node, dir, nil, func(e Edge) (bool, error) {
				if !opts.follows(e) {
					return true, nil
				}
				next := e.To
				if dir == In {
					next = e.From
				}
				strength := cur.strength * e.Strength
				if !improves(next, cur.depth+1, strength) {
					// A path at least as strong and no deeper already reaches
					// it. This is also what stops a cycle: going round one
					// multiplies by at most 1 and adds a hop.
					return true, nil
				}
				heap.Push(frontier, step{node: next, depth: cur.depth + 1, strength: strength, via: e})
				return true, nil
			})
			if err != nil {
				return Traversal{}, err
			}
		}
		if err := ctx.Err(); err != nil {
			return Traversal{}, errs.E(errs.Unavailable, op, err)
		}
	}

	return Traversal{Reached: reached, Truncated: truncated}, nil
}

// stale reports whether a popped entry has been superseded: some path at its
// depth or shallower is strictly stronger than it.
func stale(at []float32, cur step) bool {
	for d := 0; d <= cur.depth; d++ {
		if at[d] > cur.strength {
			return true
		}
	}
	return false
}

// follows reports whether the walk crosses an edge.
func (o TraverseOpts) follows(e Edge) bool {
	if e.Strength < o.MinStrength {
		return false
	}
	if len(o.Types) == 0 {
		return true
	}
	for _, t := range o.Types {
		if t == e.Type {
			return true
		}
	}
	return false
}

// bounds resolves the defaults and refuses anything above the ceilings.
//
// The ceilings are refused by name rather than silently clamped. A caller who
// asked for depth 20 and got depth 5 without being told has a different picture
// of their graph than the one they were given.
func (o TraverseOpts) bounds(op string) (maxDepth, maxNodes int, err error) {
	bad := func(format string, args ...any) error {
		return errs.E(errs.Invalid, op, fmt.Errorf(format, args...))
	}
	switch {
	case o.MaxDepth < 0:
		return 0, 0, bad("a traversal depth may not be negative")
	case o.MaxDepth > MaxTraversalDepth:
		return 0, 0, bad("a traversal goes at most %d hops, not %d", MaxTraversalDepth, o.MaxDepth)
	case o.MaxNodes < 0:
		return 0, 0, bad("a traversal node bound may not be negative")
	case o.MaxNodes > MaxTraversalNodes:
		return 0, 0, bad("a traversal returns at most %d nodes, not %d", MaxTraversalNodes, o.MaxNodes)
	}
	maxDepth, maxNodes = o.MaxDepth, o.MaxNodes
	if maxDepth == 0 {
		maxDepth = DefaultMaxDepth
	}
	if maxNodes == 0 {
		maxNodes = DefaultMaxNodes
	}
	return maxDepth, maxNodes, nil
}

// step is one entry on the traversal frontier.
type step struct {
	node     id.ID
	depth    int
	strength float32
	via      Edge
}

// frontier is a max-heap over path strength, breaking ties towards the shallower
// path so that two equally strong routes to the same node report the shorter
// one — the depth a caller is more likely to have meant.
type frontier []step

func (f frontier) Len() int { return len(f) }
func (f frontier) Less(i, j int) bool {
	if f[i].strength != f[j].strength {
		return f[i].strength > f[j].strength
	}
	return f[i].depth < f[j].depth
}
func (f frontier) Swap(i, j int) { f[i], f[j] = f[j], f[i] }
func (f *frontier) Push(x any)   { *f = append(*f, x.(step)) }
func (f *frontier) Pop() any     { old := *f; n := len(old); s := old[n-1]; *f = old[:n-1]; return s }
