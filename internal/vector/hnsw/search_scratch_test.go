package hnsw

import (
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// A pooled bitset outlives the graph size it was made for. Reusing one at a
// larger size must start from zero, or every node the previous walk touched —
// and every word beyond the previous length — reads back as already visited and
// the walk returns nothing at all.
func TestScratchClearsStaleBitsWhenReusedAtALargerSize(t *testing.T) {
	var s scratch

	s.prepare(4096, 64)
	for i := range s.visited {
		s.visited[i] = ^uint64(0)
	}
	s.prepare(64, 64) // a smaller graph: the words past the new length keep ones
	s.prepare(4096, 64)

	for i, w := range s.visited {
		if w != 0 {
			t.Fatalf("word %d of the reused bitset is %#016x, want 0", i, w)
		}
	}
	if got, want := len(s.visited), (4096+63)/64; got != want {
		t.Fatalf("the bitset is %d words long, want %d", got, want)
	}
}

// The point of the pool is that preparing a scratch allocates nothing once it
// has been used at a given size.
func TestScratchReusesHeapCapacity(t *testing.T) {
	var s scratch
	s.prepare(1024, 64)
	s.frontier = append(s.frontier, candidate{node: 1, dist: 1})
	s.results = append(s.results, candidate{node: 2, dist: 2})
	fc, rc := cap(s.frontier), cap(s.results)

	s.prepare(1024, 64)
	if len(s.frontier) != 0 || len(s.results) != 0 {
		t.Fatalf("a prepared scratch holds %d frontier and %d result entries, want none",
			len(s.frontier), len(s.results))
	}
	if cap(s.frontier) < fc || cap(s.results) < rc {
		t.Fatalf("preparing the scratch dropped capacity: frontier %d->%d, results %d->%d",
			fc, cap(s.frontier), rc, cap(s.results))
	}
	if cap(s.frontier) < 64 || cap(s.results) < 64 {
		t.Fatalf("the heaps were not pre-sized to ef: frontier %d, results %d",
			cap(s.frontier), cap(s.results))
	}
}

// A graph grows under the pool's feet: a scratch returned when the graph held
// two hundred nodes is handed back to a walk over two thousand. The walk must
// size up and see none of the old walk's bits.
func TestGraphSearchCorrectAfterGrowingPastAPooledScratch(t *testing.T) {
	g := newGraph(Defaults(), distance.L2)
	rng := newRNG(11)

	const small, grown = 200, 2000
	vecs := make([][]float32, grown)
	for i := range small {
		vecs[i] = rng.unit(16)
		if _, err := g.insert(id.New(), vecs[i]); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Hand the pool the worst scratch a small graph could leave behind: sized
	// for two hundred nodes and with every bit set.
	s := g.takeScratch()
	s.prepare(len(g.nodes), g.params.EfSearch)
	for i := range s.visited {
		s.visited[i] = ^uint64(0)
	}
	g.putScratch(s)

	for i := small; i < grown; i++ {
		vecs[i] = rng.unit(16)
		if _, err := g.insert(id.New(), vecs[i]); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	const k, queries = 10, 100
	hits := 0
	for q := range queries {
		query := rng.unit(16)
		want := map[int]bool{}
		for _, w := range bruteForce(vecs, query, k) {
			want[w] = true
		}
		got, err := g.nearest(query, 64)
		if err != nil {
			t.Fatalf("query %d: %v", q, err)
		}
		if len(got) < k {
			t.Fatalf("query %d returned %d candidates over %d nodes, want at least %d",
				q, len(got), grown, k)
		}
		for _, c := range got[:k] {
			if want[int(c.node)-1] { // node numbers are 1-based, allocated in order
				hits++
			}
		}
	}
	if recall := float64(hits) / float64(k*queries); recall < 0.95 {
		t.Fatalf("recall@%d = %.3f after the graph grew past the pooled scratch, want >= 0.95",
			k, recall)
	}
}
