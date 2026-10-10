package memory_test

import (
	"fmt"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

// BenchmarkListingRowCache is plan item 18's own measurement, and it exists
// because `bench/` cannot make it.
//
// `bench/`'s fixture now wires the cache as internal/server does, so its figures
// are the warm steady state and nothing else: it has no arm without a cache and no
// arm with a cache too small to hold the working set. Those two are what this
// measures. The shape is the same — one cache behind both the indexer and the
// service, over a real Pebble directory, so the row read the cache replaces costs
// what it costs in a deployment rather than what a map lookup costs — with the
// entry bound as the variable.
//
// Read it with:
//
//	go test -run '^$' -bench BenchmarkListingRowCache -benchtime=2s -count=6 ./internal/memory/ > out.txt
//	benchstat -col /cache out.txt
//
// The corpus is bench/'s two hundred memories and the page is ten, ordered by
// importance, with the default `archived == false` predicate — so every candidate
// the walk examines is one attribute-row read and one decode, which is the cost
// under test. The cache is warm after the first iteration and stays warm, because
// a listing writes nothing; that is the steady state a read-mostly deployment is
// in, and it is the honest arm to quote. A cold figure is one iteration of the
// `off` arm, since the first pass of the `on` arm pays exactly the same reads.
func BenchmarkListingRowCache(b *testing.B) {
	for _, tc := range []struct {
		name    string
		entries int
	}{
		{"off", 0},
		// One entry: every row the page examines misses and is stored, and
		// evicts the row before it. It is the cold query's cost and the worst
		// case of a corpus far larger than the bound, in one arm — the figure
		// that says what a cache too small for the working set costs.
		{"thrashing", 1},
		{"on", 4096},
	} {
		b.Run("cache="+tc.name, func(b *testing.B) {
			svc := newBenchListingService(b, tc.entries)
			ctx := acmeCtx()
			req := memory.ListReq{OrderBy: "importance", Limit: 10}
			if res, err := svc.List(ctx, req); err != nil || len(res.Memories) != 10 {
				b.Fatalf("warm-up listing: %d memories, %v", len(res.Memories), err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := svc.List(ctx, req); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// newBenchListingService is the composition root's wiring over a Pebble
// directory, seeded with bench/'s corpus size. entries of zero is the cache off.
func newBenchListingService(b *testing.B, entries int) *memory.Service {
	b.Helper()
	kv, err := pebble.Open(b.TempDir(), pebble.Options{})
	if err != nil {
		b.Fatalf("opening pebble: %v", err)
	}
	b.Cleanup(func() { _ = kv.Close() })

	cache := attr.NewRowCache(entries)
	clk := clock.NewFake(clock.FakeStart)
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable(), attr.WithRowCache(cache))))
	svc := memory.New(kv, repo, flat.New(vector.NewStore(kv), distance.L2),
		embeddingtest.New(), clk, attr.MustTable(), graph.NewService(kv, clk),
		memory.Config{Metric: distance.L2}, memory.WithRowCache(cache))
	b.Cleanup(func() { _ = svc.Close() })

	ctx := acmeCtx()
	for i := range 200 {
		importance := float32(i+1) / 201
		if _, err := svc.Create(ctx, memory.CreateReq{
			Content:    fmt.Sprintf("row cache benchmark record %d", i),
			Importance: &importance,
		}); err != nil {
			b.Fatalf("seeding record %d: %v", i, err)
		}
	}
	return svc
}
