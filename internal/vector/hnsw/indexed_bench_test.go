package hnsw_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/hnsw"
)

// BenchmarkIndexedPebble measures [hnsw.Index.Indexed] against a real Pebble
// store rather than memkv, because the cost this benchmark exists to show is
// the node-record commit itself, which memkv does not pay: on disk a synced
// commit waits for an fsync, and the write path pays one per created memory
// strictly after the record batch has already paid its own.
//
// The graph grows as the benchmark runs, so a faster variant inserts into a
// larger graph than a slower one over the same -benchtime. That makes a
// measured reduction here a conservative one.
func BenchmarkIndexedPebble(b *testing.B) {
	kv, err := pebble.Open(b.TempDir(), pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = kv.Close() })
	ix, err := hnsw.New(kv, hnsw.Options{Metric: distance.Cosine})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	rng := rand.New(rand.NewSource(1))
	vecs := make([][]float32, 1000)
	for i := range vecs {
		vecs[i] = unitVector(rng, 384)
	}

	b.ResetTimer()
	for i := range b.N {
		if err := ix.Indexed(ctx, benchTenant, id.New(), vector.Single(vecs[i%len(vecs)])); err != nil {
			b.Fatal(err)
		}
	}
}
