package hnsw_test

import (
	"context"
	"sync"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/hnsw"
)

// Searches on one resident graph run concurrently under its read lock, so
// whatever working set a walk uses must be exclusively its own. Sixteen
// goroutines asking the same questions must get the same answers the sequential
// run got — not merely plausible ones, the same ones, distances included.
func TestConcurrentSearchesMatchSequentialResults(t *testing.T) {
	const (
		corpus     = 1500
		dim        = 32
		queries    = 200
		k          = 10
		goroutines = 16
	)

	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix, err := hnsw.New(kv, hnsw.Options{Metric: distance.Cosine})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	rng := newTestRNG(23)
	for range corpus {
		if err := ix.Indexed(ctx, "concurrent", id.New(), rng.unit(dim)); err != nil {
			t.Fatal(err)
		}
	}

	qs := make([][]float32, queries)
	for i := range qs {
		qs[i] = rng.unit(dim)
	}

	want := make([][]vector.Hit, queries)
	for i, q := range qs {
		hits, err := ix.Search(ctx, "concurrent", q, k, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) != k {
			t.Fatalf("query %d returned %d hits over %d vectors, want %d", i, len(hits), corpus, k)
		}
		want[i] = hits
	}

	got := make([][][]vector.Hit, goroutines)
	failures := make([]error, goroutines)
	var wg sync.WaitGroup
	for w := range goroutines {
		got[w] = make([][]vector.Hit, queries)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i, q := range qs {
				hits, err := ix.Search(ctx, "concurrent", q, k, nil)
				if err != nil {
					failures[w] = err
					return
				}
				got[w][i] = hits
			}
		}(w)
	}
	wg.Wait()

	for w := range goroutines {
		if failures[w] != nil {
			t.Fatalf("goroutine %d: %v", w, failures[w])
		}
		for i := range qs {
			if len(got[w][i]) != len(want[i]) {
				t.Fatalf("goroutine %d query %d returned %d hits, want %d",
					w, i, len(got[w][i]), len(want[i]))
			}
			for j := range want[i] {
				if got[w][i][j] != want[i][j] {
					t.Fatalf("goroutine %d query %d hit %d is %v, want %v",
						w, i, j, got[w][i][j], want[i][j])
				}
			}
		}
	}
}
