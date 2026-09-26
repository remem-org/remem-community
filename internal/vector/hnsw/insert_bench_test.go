package hnsw_test

import (
	"context"
	"math"
	"math/rand"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/hnsw"
)

// BenchmarkInsert384Cosine measures inserting unit-length 384-dimensional
// vectors under the cosine metric at the default parameters — the shape of
// every production write, and 39% of the write-path benchmark's CPU when it was
// first profiled in Phase 13, almost all of it distance.CosineDistance
// recomputing both norms on every call. It is the before-and-after evidence for
// caching a node's norm.
func BenchmarkInsert384Cosine(b *testing.B) {
	kv := memkv.New()
	b.Cleanup(func() { _ = kv.Close() })
	ix, err := hnsw.New(kv, hnsw.Options{Metric: distance.Cosine})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	rng := rand.New(rand.NewSource(1))
	vecs := make([][]float32, 2000)
	for i := range vecs {
		vecs[i] = unitVector(rng, 384)
	}
	// A populated graph, so the timed inserts pay a real construction search.
	for _, v := range vecs[:1000] {
		if err := ix.Indexed(ctx, benchTenant, id.New(), v); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := range b.N {
		if err := ix.Indexed(ctx, benchTenant, id.New(), vecs[1000+i%1000]); err != nil {
			b.Fatal(err)
		}
	}
}

const benchTenant = "bench"

func unitVector(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	var n float64
	for i := range v {
		v[i] = float32(rng.NormFloat64())
		n += float64(v[i]) * float64(v[i])
	}
	s := float32(1 / math.Sqrt(n))
	for i := range v {
		v[i] *= s
	}
	return v
}
