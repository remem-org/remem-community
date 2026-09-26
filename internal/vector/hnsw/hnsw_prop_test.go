package hnsw

import (
	"encoding/binary"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"pgregory.net/rapid"
)

// Property tests on the two things this package owns that a unit test reaches
// only by example: the durable node record, and the graph's behaviour under
// arbitrary sequences of insertion and removal.
//
// The second one earns its keep. An approximate index is allowed to return a
// worse neighbour than the true one — that is what approximate means — but it is
// never allowed to lose a vector it holds. Searching for a stored vector with
// that exact vector must return it: the answer is at distance zero and nothing
// can beat it. A node that has become unreachable fails this and passes every
// recall measurement, because recall averages over queries and this does not.
//
// That is not hypothetical. It is how the outlier-among-duplicates defect in
// TestAnOutlierAmongDuplicatesStaysReachable would have been found.

func TestANodeRecordRoundTripsForAnyLinks(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		level := rapid.IntRange(0, maxLevel).Draw(rt, "level")
		want := &node{id: id.New(), level: level, links: make([][]uint32, level+1)}
		for l := range want.links {
			want.links[l] = rapid.SliceOfN(
				rapid.Uint32Range(1, 1<<20), 0, 64).Draw(rt, "links")
		}

		got, err := decodeNode(7, encodeNode(want))
		if err != nil {
			rt.Fatalf("decode: %v", err)
		}
		if got.id != want.id {
			rt.Fatalf("record id %s, want %s", got.id, want.id)
		}
		if got.level != want.level {
			rt.Fatalf("level %d, want %d", got.level, want.level)
		}
		if len(got.links) != len(want.links) {
			rt.Fatalf("%d layers, want %d", len(got.links), len(want.links))
		}
		for l := range want.links {
			if len(got.links[l]) != len(want.links[l]) {
				rt.Fatalf("layer %d has %d links, want %d", l, len(got.links[l]), len(want.links[l]))
			}
			for i := range want.links[l] {
				if got.links[l][i] != want.links[l][i] {
					rt.Fatalf("layer %d link %d is %d, want %d", l, i, got.links[l][i], want.links[l][i])
				}
			}
		}
	})
}

// A truncated node record is corruption, never a partial decode into something
// plausible. A record that lost its tail would otherwise become a node with
// fewer neighbours than it has, which no reader could detect.
func TestATruncatedNodeRecordIsRefused(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		level := rapid.IntRange(0, 6).Draw(rt, "level")
		n := &node{id: id.New(), level: level, links: make([][]uint32, level+1)}
		for l := range n.links {
			n.links[l] = rapid.SliceOfN(rapid.Uint32Range(1, 1<<16), 1, 16).Draw(rt, "links")
		}
		full := encodeNode(n)
		cut := rapid.IntRange(0, len(full)-1).Draw(rt, "cut")
		if _, err := decodeNode(3, full[:cut]); err == nil {
			rt.Fatalf("a node record truncated to %d of %d bytes decoded", cut, len(full))
		}
	})
}

// Every vector the graph holds must be reachable, across the shape that breaks
// reachability and every parameter combination of it.
//
// The shape is deliberate rather than drawn from a uniform distribution, and
// that is the point. Vectors drawn at random are all distinct, and distinct
// vectors keep each other reachable: a neighbour list with room to spare prunes
// nothing. What breaks the graph is a pile of identical memories with one
// unlike them — every duplicate drops the odd one out in favour of another
// duplicate, until nothing points at it at all. A generator that never produces
// that shape tests the easy case a thousand times.
//
// What rapid varies is everything about the shape: how many dimensions, how
// many duplicates, where the outlier lands, and the neighbour bounds the
// pruning runs under. The assertion is reachability at exhaustive effort, not
// recall: an approximate index may legitimately miss a vector at a low effort
// setting, and may never hold one that no effort can reach.
func TestEveryStoredVectorStaysReachable(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		dim := rapid.IntRange(2, 8).Draw(rt, "dim")
		m := rapid.IntRange(2, 16).Draw(rt, "m")
		duplicates := rapid.IntRange(2*m, 8*m).Draw(rt, "duplicates")
		at := rapid.IntRange(0, duplicates).Draw(rt, "outlier position")
		hot := rapid.IntRange(0, dim-1).Draw(rt, "hot axis")
		away := (hot + 1 + rapid.IntRange(0, dim-2).Draw(rt, "outlier axis")) % dim

		// Ids are drawn, not generated. A record's layer is a function of its
		// id, so the whole graph is a function of the ids — and with id.New()
		// the shape would change on every run, which makes a failure something
		// rapid reports as flaky and nobody can reproduce.
		seed := rapid.Uint64().Draw(rt, "id seed")
		next := 0
		newID := func() id.ID {
			var rid id.ID
			binary.BigEndian.PutUint64(rid[0:8], seed)
			binary.BigEndian.PutUint64(rid[8:16], uint64(next))
			next++
			return rid
		}

		g := newGraph(Params{M: m, EfConstruction: 4 * m}, distance.L2)

		outlier := newID()
		live := map[id.ID][]float32{}
		insert := func(rid id.ID, axis int) {
			v := make([]float32, dim)
			v[axis] = 1
			if _, err := g.insert(rid, v); err != nil {
				rt.Fatalf("insert: %v", err)
			}
			live[rid] = v
		}
		for i := range duplicates {
			if i == at {
				insert(outlier, away)
			}
			insert(newID(), hot)
		}
		if at == duplicates {
			insert(outlier, away)
		}

		if g.live != len(live) {
			rt.Fatalf("the graph counts %d live nodes and the model has %d", g.live, len(live))
		}
		for rid, v := range live {
			found, err := g.nearest(v, len(live)+1)
			if err != nil {
				rt.Fatalf("search: %v", err)
			}
			reached := false
			for _, c := range found {
				if g.nodes[c.node] != nil && g.nodes[c.node].id == rid {
					reached = true
					break
				}
			}
			if !reached {
				which := "a duplicate"
				if rid == outlier {
					which = "the outlier"
				}
				rt.Fatalf("%s (%s) is in the graph and cannot be reached from the entry point "+
					"even at exhaustive effort: nothing links to it", which, rid)
			}
		}
	})
}
