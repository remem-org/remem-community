package bench

import (
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/memory"
)

// BenchmarkListing is a Go-only baseline: Rust lists by created_at alone, and
// Go by five orderings (plan §II.10 row 3). A page of ten over the 200-memory
// corpus, by creation time and by importance.
func BenchmarkListing(b *testing.B) {
	svc := service(b, "listing", serviceOpts{}, seedCorpus(0, false))
	for _, order := range []string{"created_at", "importance"} {
		b.Run(order, func(b *testing.B) {
			ctx := benchCtx()
			req := memory.ListReq{OrderBy: order, Limit: 10}
			if res, err := svc.List(ctx, req); err != nil || len(res.Memories) != 10 {
				b.Fatalf("warm-up listing by %s: %d memories, %v", order, len(res.Memories), err)
			}

			var lat latencies
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				start := time.Now()
				if _, err := svc.List(ctx, req); err != nil {
					b.Fatal(err)
				}
				lat.add(time.Since(start))
			}
			b.StopTimer()
			lat.report(b, "ms")
		})
	}
}
