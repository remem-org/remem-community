package attr

import (
	"sync/atomic"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
)

// BenchmarkRowCacheConcurrentLookup is the contention the shards exist to remove.
//
// A filtered page settles about fifty candidates, and every one of them is a
// lookup. The cache is process-wide, so a server answering eight concurrent
// queries is eight goroutines taking the same structure fifty times each — and a
// lookup is not read-only, because an LRU moves the entry it served to the front.
// One mutex makes that a queue whose length is the request concurrency, which is
// the thing a cache was added to avoid.
//
// Read it with the parallelism pinned, so the figure is of a fixed number of
// goroutines rather than of the machine:
//
//	go test -run '^$' -bench RowCacheConcurrent -benchmem -benchtime=2s -count=6 \
//	  -cpu=8 ./internal/attr/ > out.txt
//	benchstat out.txt
//
// The working set is four thousand rows against a cache given twice that bound —
// twice, because the bound is enforced per shard and an even hash still leaves one
// shard above its share — so every lookup hits and nothing is evicted: the
// measurement is the structure's cost and nothing else. The `spread` arm walks the
// keys, which is what a query settling a page of distinct candidates does; the
// `hot` arm hammers sixteen of them, which is the worst case for a sharded lock
// because the shards cannot separate what the keys do not.
func BenchmarkRowCacheConcurrentLookup(b *testing.B) {
	const rows = 4096
	for _, arm := range []struct {
		name   string
		window int
	}{
		{"spread", rows},
		{"hot", 16},
	} {
		b.Run(arm.name, func(b *testing.B) {
			c, ids, gen := warmRowCache(b, rows)
			var missed atomic.Int64

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if _, ok := c.lookup(cacheTenant, cacheNS, ids[i%arm.window], gen); !ok {
						missed.Add(1)
					}
					i++
				}
			})
			b.StopTimer()
			if n := missed.Load(); n != 0 {
				b.Fatalf("%d of the lookups missed; the benchmark is measuring the read "+
					"behind the cache rather than the cache", n)
			}
		})
	}
}

// BenchmarkRowCacheConcurrentSettle is the same contention with one writer in it,
// which is the shape of a server that is serving queries and ingesting at once.
//
// One goroutine in the pool stages and finishes a row write on every iteration
// instead of reading. That moves the tenant's generation, so the readers mostly
// miss and store rather than hit — the write-heavy end of the trade the generation
// makes, and the arm in which the staging marks and the generation are touched as
// often as the entries are.
func BenchmarkRowCacheConcurrentSettle(b *testing.B) {
	const rows = 4096
	c, ids, _ := warmRowCache(b, rows)
	var writer atomic.Bool

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		writes := writer.CompareAndSwap(false, true)
		i := 0
		for pb.Next() {
			rid := ids[i%rows]
			i++
			if writes {
				seq, ok := c.beginWrite(cacheTenant, cacheNS, rid)
				if ok {
					c.finishWrite(cacheTenant, cacheNS, rid, seq)
				}
				continue
			}
			gen := c.Generation(cacheTenant)
			if _, ok := c.lookup(cacheTenant, cacheNS, rid, gen); !ok {
				c.store(cacheTenant, cacheNS, rid, gen, NewRow(SchemaVersion))
			}
		}
	})
}

// warmRowCache returns a cache holding one row per id, at the generation they
// were stored under.
func warmRowCache(b *testing.B, rows int) (*RowCache, []id.ID, uint64) {
	b.Helper()
	c := NewRowCache(rows * 2)
	gen := c.Generation(cacheTenant)
	ids := make([]id.ID, rows)
	for i := range ids {
		ids[i] = idOf(i)
		row := NewRow(SchemaVersion)
		row.Set(SlotImportance, F32(float32(i)/float32(rows)))
		row.Set(SlotArchived, Bool(false))
		c.store(cacheTenant, cacheNS, ids[i], gen, row)
	}
	if n := c.Len(); n != rows {
		b.Fatalf("the warmed cache holds %d of %d rows", n, rows)
	}
	return c, ids, gen
}
