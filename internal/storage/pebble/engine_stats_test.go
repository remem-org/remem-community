package pebble_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/remem-org/remem-go/internal/storage"
)

// TestEngineStatsReportTheDirectory: after real writes reach disk, the engine
// reports a size. It is the one fact of the three that must be non-zero on any
// directory holding data; compactions and cache hits legitimately start at zero.
func TestEngineStatsReportTheDirectory(t *testing.T) {
	kv := open(t)
	r, ok := kv.(storage.StatsReporter)
	if !ok {
		t.Fatal("the Pebble store does not report engine statistics")
	}

	ctx := context.Background()
	for i := range 1000 {
		if err := kv.Set(ctx, []byte(fmt.Sprintf("key-%05d", i)), make([]byte, 256)); err != nil {
			t.Fatal(err)
		}
	}
	if err := kv.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	s := r.EngineStats()
	if s.DiskBytes == 0 {
		t.Fatalf("after 1,000 writes and a flush the engine reports %d bytes on disk", s.DiskBytes)
	}
	if s.Compactions < 0 || s.CacheHits < 0 || s.CacheMisses < 0 {
		t.Fatalf("a cumulative engine count is negative: %+v", s)
	}
}
