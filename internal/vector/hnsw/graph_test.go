package hnsw

import (
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// The graph on its own, before any storage: if the walk is wrong, every test
// above it fails for a reason that has nothing to do with the reason.
func TestGraphFindsNearestNeighbours(t *testing.T) {
	g := newGraph(Defaults(), distance.L2)

	const n = 2000
	rng := newRNG(1)
	ids := make([]id.ID, n)
	vecs := make([][]float32, n)
	for i := range n {
		ids[i] = id.New()
		vecs[i] = rng.unit(16)
		if _, err := g.insert(ids[i], vecs[i]); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if g.live != n {
		t.Fatalf("graph holds %d live nodes, want %d", g.live, n)
	}

	const k, queries = 10, 100
	hits := 0
	for q := range queries {
		query := rng.unit(16)
		want := bruteForce(vecs, query, k)
		got, err := g.nearest(query, 64)
		if err != nil {
			t.Fatalf("query %d: %v", q, err)
		}
		if len(got) < k {
			t.Fatalf("query %d returned %d candidates, want at least %d", q, len(got), k)
		}
		in := map[int]bool{}
		for _, w := range want {
			in[w] = true
		}
		for _, c := range got[:k] {
			// node numbers are 1-based and allocated in insertion order here
			if in[int(c.node)-1] {
				hits++
			}
		}
	}
	recall := float64(hits) / float64(k*queries)
	if recall < 0.95 {
		t.Fatalf("recall@%d = %.3f over %d vectors, want >= 0.95", k, recall, n)
	}
	t.Logf("recall@%d = %.3f", k, recall)
}

func TestGraphRemoveLeavesNoDanglingLinks(t *testing.T) {
	g := newGraph(Params{M: 4, M0: 8, EfConstruction: 32, EfSearch: 16}, distance.L2)
	rng := newRNG(7)

	ids := make([]id.ID, 200)
	for i := range ids {
		ids[i] = id.New()
		if _, err := g.insert(ids[i], rng.unit(8)); err != nil {
			t.Fatal(err)
		}
	}
	for _, rid := range ids[:100] {
		g.remove(g.byID[rid])
	}
	if g.live != 100 {
		t.Fatalf("graph holds %d live nodes after 100 removals, want 100", g.live)
	}
	for num, n := range g.nodes {
		if n == nil {
			continue
		}
		for l, links := range n.links {
			for _, m := range links {
				if g.nodes[m] == nil {
					t.Fatalf("node %d layer %d still links to removed node %d", num, l, m)
				}
			}
		}
	}
	// The graph must still be searchable, and the entry point must have
	// survived being removed.
	got, err := g.nearest(rng.unit(8), 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("a graph with 100 live nodes returned nothing")
	}
}

// Levels come from the id, so the same corpus inserted in any order produces
// the same heights. That is what makes a rebuilt graph comparable with an
// incrementally built one.
func TestLevelsAreDerivedFromTheRecordID(t *testing.T) {
	rid := id.New()
	first := levelOf(rid, 16)
	for range 100 {
		if got := levelOf(rid, 16); got != first {
			t.Fatalf("levelOf is not a function of the id: %d then %d", first, got)
		}
	}

	// And the distribution is the exponential one the algorithm needs: layer
	// zero should hold the overwhelming majority.
	counts := map[int]int{}
	for range 100000 {
		counts[levelOf(id.New(), 16)]++
	}
	if got := float64(counts[0]) / 100000; got < 0.92 || got > 0.96 {
		t.Fatalf("%.3f of nodes landed on layer zero, want about 1-1/16", got)
	}
	if counts[1] == 0 || counts[2] == 0 {
		t.Fatalf("no nodes above layer zero: %v", counts)
	}
}

// --- helpers ---------------------------------------------------------------

func bruteForce(vecs [][]float32, q []float32, k int) []int {
	type sc struct {
		i int
		d float32
	}
	all := make([]sc, len(vecs))
	for i, v := range vecs {
		d, _ := distance.Compute(distance.L2, q, v)
		all[i] = sc{i, d}
	}
	for i := 1; i < len(all); i++ {
		for j := i; j > 0 && all[j].d < all[j-1].d; j-- {
			all[j], all[j-1] = all[j-1], all[j]
		}
	}
	out := make([]int, k)
	for i := range out {
		out[i] = all[i].i
	}
	return out
}

// rng is a linear congruential generator, so a failure reproduces from the seed
// in the test rather than from whatever math/rand happened to be doing.
type rng struct{ s uint64 }

func newRNG(seed uint64) *rng { return &rng{s: seed*6364136223846793005 + 1442695040888963407} }

func (r *rng) next() float32 {
	r.s = r.s*6364136223846793005 + 1442695040888963407
	return float32(int64(r.s>>33))/float32(1<<30) - 1
}

func (r *rng) unit(dim int) []float32 {
	v := make([]float32, dim)
	var sum float64
	for i := range v {
		v[i] = r.next()
		sum += float64(v[i]) * float64(v[i])
	}
	if sum == 0 {
		v[0] = 1
		return v
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
	return v
}
