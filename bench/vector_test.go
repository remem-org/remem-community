package bench

import (
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/memory"
)

// BenchmarkVectorRetrieval mirrors Rust's vector_retrieval Criterion benchmark
// (crates/remem-server/benches/vector_retrieval.rs at pre-go-freeze): three
// 200-record fixtures, 4-dimensional vectors, a page of 10, ef_search 64.
//
//   - normal: ordinary vector retrieval.
//   - phantom_heavy: 80% of the records hard-deleted. In Rust that leaves HNSW
//     phantoms the search must widen past. Go has none by construction — the
//     canonical vector row is what says a memory is in the index (plan Phase 7)
//     — so this fixture measures that claim rather than a repair.
//   - selective_filter: one record in five long-term, searched with
//     policy long_term, which exercises widening under a filter.
//
// The boundary is the memory service's semantic search, the nearest Go has to
// Rust's QueryEngine. It includes the (trivial) embedder call and materialising
// the page's records, which Rust's query engine also returns.
func BenchmarkVectorRetrieval(b *testing.B) {
	for _, w := range []struct {
		name          string
		longTermEvery int
		deleteOthers  bool
		policy        string
	}{
		{name: "normal"},
		{name: "phantom_heavy", deleteOthers: true},
		{name: "selective_filter", longTermEvery: 5, policy: "long_term"},
	} {
		b.Run(w.name, func(b *testing.B) {
			svc := service(b, "vector-"+w.name, serviceOpts{}, seedCorpus(w.longTermEvery, w.deleteOthers))
			ctx := benchCtx()
			req := memory.SearchReq{
				Type: memory.SearchSemantic, Query: "Remem vector retrieval benchmark query",
				Limit: 10, Policy: w.policy,
			}

			// The first search materialises the index; it is not the thing
			// measured. It also proves the fixture has the shape claimed.
			res, err := svc.Search(ctx, req)
			if err != nil {
				b.Fatalf("warm-up search: %v", err)
			}
			if len(res.Results) != 10 {
				b.Fatalf("the %s fixture answered a page of %d, want 10", w.name, len(res.Results))
			}

			var lat latencies
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				start := time.Now()
				if _, err := svc.Search(ctx, req); err != nil {
					b.Fatal(err)
				}
				lat.add(time.Since(start))
			}
			b.StopTimer()
			lat.report(b, "ms")
		})
	}
}
