package pebble_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/remem-org/remem-go/internal/storage/pebble"
)

// BenchmarkConcurrentSyncedCommits measures fifty goroutines each committing a
// synced batch with a precondition, which is the shape of fifty concurrent
// creates: every memory write is a conditional commit with fsync.
//
// It is the before-and-after evidence for taking the WAL sync outside the
// store's write mutex. Held inside it, every synced commit in the process
// fsyncs one at a time, and throughput is bounded by how many fsyncs the disk
// completes a second, whatever the concurrency. Found profiling the Phase 13
// write-path benchmark.
//
// It writes to a directory under the test's temporary directory. On a host
// whose /tmp is a tmpfs that measures memory rather than a disk, so its
// absolute numbers mean little; compare before and after on the same host.
func BenchmarkConcurrentSyncedCommits(b *testing.B) {
	kv, err := pebble.Open(b.TempDir(), pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = kv.Close() })

	const writers = 50
	var seq atomic.Uint64
	ctx := context.Background()

	b.ResetTimer()
	var wg sync.WaitGroup
	per := (b.N + writers - 1) / writers
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range per {
				n := seq.Add(1)
				batch := kv.NewBatch()
				key := []byte(fmt.Sprintf("bench/%020d", n))
				batch.Expect(key, nil, false)
				batch.Set(key, []byte("value"))
				if err := batch.Commit(ctx, true); err != nil {
					b.Error(err)
				}
				batch.Close()
			}
		}()
	}
	wg.Wait()
}
