package query

import (
	"math/rand/v2"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// BenchmarkFilteredVectorStepRowCache is what finding 2's last paragraph costs,
// measured with and without plan item 18's cache.
//
// The step is a page of ten under a filter keeping one record in five, over three
// hundred records in an HNSW index — bench/'s `selective_filter` shape, one layer
// down, so that what is timed is the walk and the settles rather than a whole
// request. Item 16 already stopped a widening round settling a candidate twice;
// what is left is the first settle of each, and that is the read this measures.
//
// Read it with:
//
//	go test -run '^$' -bench BenchmarkFilteredVectorStepRowCache -benchtime=2s -count=6 ./internal/query/ > out.txt
//	benchstat -col /cache out.txt
//
// The store is in-memory, so a row read here is a map lookup rather than a Pebble
// seek: this is the conservative end of the saving, and BenchmarkListingRowCache
// in internal/memory is the same question over a real directory.
func BenchmarkFilteredVectorStepRowCache(b *testing.B) {
	c := newWidenCorpus(b, 5, 300, 8, 64, 5)
	q := c.query(rand.New(rand.NewPCG(23, 23)), 10, true)
	plan := c.plan(b, q)
	e := NewExecutor(c.slots, c.index, distance.L2)
	step := Step{Kind: StepVector, Source: SourceVector}

	for _, tc := range []struct {
		name    string
		entries int
	}{
		{"off", 0},
		{"on", 4096},
	} {
		b.Run("cache="+tc.name, func(b *testing.B) {
			cache := attr.NewRowCache(tc.entries)
			// One round outside the timer, so the `on` arm measures the steady
			// state a read-mostly deployment is in rather than its first query.
			warm := attr.NewCachedReader(c.kv, cache, c.slots, widenTenant, cache.Generation(widenTenant))
			if _, err := e.runVector(c.ctx, warm, q, plan, step); err != nil {
				b.Fatal(err)
			}
			warm.Report()

			b.ReportAllocs()
			for b.Loop() {
				// A fresh reader each iteration, as each request builds its own,
				// taking the generation before it reads anything.
				r := attr.NewCachedReader(c.kv, cache, c.slots, widenTenant, cache.Generation(widenTenant))
				if _, err := e.runVector(c.ctx, r, q, plan, step); err != nil {
					b.Fatal(err)
				}
				r.Report()
			}
		})
	}
}
