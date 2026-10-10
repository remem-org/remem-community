package hnsw_test

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/hnsw"
)

// BenchmarkSearch384Cosine measures the read half of the index at production
// shape: ten thousand unit-length 384-dimensional vectors under cosine, ef 64
// (the default) and k 10. It is the counterpart to BenchmarkInsert384Cosine and
// the evidence for anything that changes the search walk's working set.
func BenchmarkSearch384Cosine(b *testing.B) {
	ix, queries := searchCorpus(b)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		hits, err := ix.Search(ctx, benchTenant, queries[i%len(queries)], 10, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(hits) != 10 {
			b.Fatalf("search returned %d hits, want 10", len(hits))
		}
	}
}

// searchCorpus builds the ten-thousand-node graph once per process. Building it
// costs seconds, and -count=6 would otherwise pay that per repetition for a
// graph no search mutates.
func searchCorpus(b *testing.B) (*hnsw.Index, [][]float32) {
	searchCorpusOnce.Do(func() {
		kv := memkv.New()
		ix, err := hnsw.New(kv, hnsw.Options{Metric: distance.Cosine})
		if err != nil {
			panic(err)
		}
		ctx := context.Background()
		rng := rand.New(rand.NewSource(7))
		for range 10000 {
			if err := ix.Indexed(ctx, benchTenant, id.New(), unitVector(rng, 384)); err != nil {
				panic(err)
			}
		}
		queries := make([][]float32, 256)
		for i := range queries {
			queries[i] = unitVector(rng, 384)
		}
		searchCorpusIndex, searchCorpusQueries = ix, queries
	})
	if searchCorpusIndex == nil {
		b.Fatal("the search corpus was not built")
	}
	return searchCorpusIndex, searchCorpusQueries
}

var (
	searchCorpusOnce    sync.Once
	searchCorpusIndex   *hnsw.Index
	searchCorpusQueries [][]float32
)
