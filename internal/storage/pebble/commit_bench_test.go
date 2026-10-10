package pebble_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
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

// BenchmarkConcurrentSyncedCommitsMultiTenant is
// [BenchmarkConcurrentSyncedCommits] spread over eight tenants: fifty writers,
// each committing a synced, conditional batch into the tenant it serves.
//
// It is the before-and-after evidence for striping the store's write mutex by
// tenant. One mutex for the whole store serialised these commits' condition
// checks and applies whatever the tenancy, even though a condition only ever
// guards a key inside one tenant; striped, writers in different tenants no
// longer wait on each other.
//
// Compare it only against itself on the same host, for the reason
// [BenchmarkConcurrentSyncedCommits] gives.
func BenchmarkConcurrentSyncedCommitsMultiTenant(b *testing.B) {
	multiTenantConditionalCommits(b, true)
}

// BenchmarkConcurrentUnsyncedCommitsMultiTenant is the same work without the
// fsync, which is the only shape in which the store's write lock is the
// bottleneck at all.
//
// [BenchmarkConcurrentSyncedCommitsMultiTenant] spends its whole budget waiting
// for the disk: at fifty writers it completes a few thousand commits a second,
// and the condition check and apply the lock covers take microseconds, so one
// lock for the whole store is idle almost all of the time and striping it can
// win nothing. Unsynced, the commits run two orders of magnitude faster and the
// lock is what they queue behind — which is where the job queue, the session
// registry, the lifecycle sweep and every derived-index write live, because
// none of those fsync.
func BenchmarkConcurrentUnsyncedCommitsMultiTenant(b *testing.B) {
	multiTenantConditionalCommits(b, false)
}

// multiTenantConditionalCommits is fifty writers spread over eight tenants, each
// committing a conditional batch into the tenant it serves.
func multiTenantConditionalCommits(b *testing.B, durable bool) {
	kv, err := pebble.Open(b.TempDir(), pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = kv.Close() })

	const (
		writers = 50
		tenants = 8
	)
	var seq atomic.Uint64
	ctx := context.Background()

	b.ResetTimer()
	var wg sync.WaitGroup
	per := (b.N + writers - 1) / writers
	for w := range writers {
		tid := tenant.ID(fmt.Sprintf("bench-%d", w%tenants))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range per {
				n := seq.Add(1)
				batch := kv.NewBatch()
				key := keys.Record(tid, tenant.DefaultNamespace, keys.RecordMemory, benchID(n))
				batch.Expect(key, nil, false)
				batch.Set(key, []byte("value"))
				if err := batch.Commit(ctx, durable); err != nil {
					b.Error(err)
				}
				batch.Close()
			}
		}()
	}
	wg.Wait()
}

// benchID turns a counter into a distinct record id.
func benchID(n uint64) id.ID {
	var out id.ID
	binary.BigEndian.PutUint64(out[8:], n)
	return out
}
