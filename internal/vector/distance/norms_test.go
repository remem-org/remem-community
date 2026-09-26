package distance_test

import (
	"math/rand"
	"testing"

	"github.com/remem-org/remem-go/internal/vector/distance"
)

// A cosine distance computed from cached norms is the same number, bit for bit,
// as one that recomputes them.
//
// The HNSW graph caches each node's norm so an insert's thousands of distance
// calls pay one dot product each rather than a dot product and two norms: in
// Phase 13 that recomputation was a third of the write path's CPU. Exact rather
// than approximate equality is the point. The flat index is the oracle HNSW is
// checked against, and a distance that moved in its last bit could reorder a
// tie between them.
func TestCosineWithNormsMatchesCosineDistance(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for trial := range 2000 {
		dim := 1 + rng.Intn(400)
		a, b := make([]float32, dim), make([]float32, dim)
		for i := range a {
			a[i] = float32(rng.NormFloat64())
			b[i] = float32(rng.NormFloat64())
		}
		if trial%97 == 0 {
			for i := range b {
				b[i] = 0 // a zero vector, which has no direction
			}
		}
		want := distance.CosineDistance(a, b)
		got := distance.CosineWithNorms(a, b, distance.Norm(a), distance.Norm(b))
		if got != want {
			t.Fatalf("trial %d (dim %d): CosineWithNorms = %v, CosineDistance = %v", trial, dim, got, want)
		}
	}
}
