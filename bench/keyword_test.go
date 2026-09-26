package bench

import (
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/memory"
)

// BenchmarkKeywordSearch is a Go-only baseline: Rust scans the corpus for a
// keyword query, where Go reads an inverted index (plan §II.10 row 1), so there
// is nothing like-for-like to compare. A page of ten over the 200-memory corpus,
// every memory matching.
func BenchmarkKeywordSearch(b *testing.B) {
	svc := service(b, "keyword", serviceOpts{text: true}, seedCorpus(0, false))
	ctx := benchCtx()
	req := memory.SearchReq{Type: memory.SearchKeyword, Query: "benchmark record", Limit: 10}
	if res, err := svc.Search(ctx, req); err != nil || len(res.Results) != 10 {
		b.Fatalf("warm-up keyword search: %d results, %v", len(res.Results), err)
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
}
