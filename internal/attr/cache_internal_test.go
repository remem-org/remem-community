package attr

import (
	"fmt"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
)

const (
	cacheTenant = tenant.ID("acme")
	otherTenant = tenant.ID("globex")
	cacheNS     = tenant.DefaultNamespace
)

// A row the cache was given at a generation comes back at that generation.
func TestARowStoredAtAGenerationIsServedAtThatGeneration(t *testing.T) {
	c := NewRowCache(8)
	rid := id.New()
	g := c.Generation(cacheTenant)
	row := NewRow(SchemaVersion)
	row.Set(SlotImportance, F32(0.5))

	c.store(cacheTenant, cacheNS, rid, g, row)
	got, ok := c.lookup(cacheTenant, cacheNS, rid, g)
	if !ok {
		t.Fatal("a row stored at the current generation did not come back")
	}
	if got != row {
		t.Errorf("the cache returned a different row; entries are shared read-only")
	}
}

// The generation is what makes a pinned snapshot safe: a reader holding an older
// generation is refused, and so is an entry tagged with one.
func TestAGenerationThatHasMovedRefusesBothSides(t *testing.T) {
	c := NewRowCache(8)
	rid := id.New()
	g0 := c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g0, NewRow(SchemaVersion))

	// A committed write to some row of this tenant.
	seq, ok := c.beginWrite(cacheTenant, cacheNS, id.New())
	if !ok {
		t.Fatal("beginWrite refused an enabled cache")
	}
	c.finishWrite(cacheTenant, cacheNS, id.New(), seq)

	if g := c.Generation(cacheTenant); g == g0 {
		t.Fatalf("a committed write left the generation at %d", g)
	}
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, g0); ok {
		t.Error("a reader pinned to the old generation was served an entry")
	}
	g1 := c.Generation(cacheTenant)
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, g1); ok {
		t.Error("an entry tagged with the old generation was served to a new reader")
	}
	// And a store at a generation that has already moved is dropped, which is
	// what stops a reader on an older snapshot repopulating the cache.
	c.store(cacheTenant, cacheNS, rid, g0, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, g1); ok {
		t.Error("a store at a stale generation was accepted")
	}
}

// One tenant's write does not move another tenant's generation.
func TestAGenerationIsPerTenant(t *testing.T) {
	c := NewRowCache(8)
	other := c.Generation(otherTenant)
	seq, _ := c.beginWrite(cacheTenant, cacheNS, id.New())
	c.finishWrite(cacheTenant, cacheNS, id.New(), seq)
	if g := c.Generation(otherTenant); g != other {
		t.Errorf("another tenant's generation moved from %d to %d", other, g)
	}
}

// A staged write quiesces its own row until the write has committed and bumped
// the generation. This is the window between a batch becoming visible to new
// snapshots and its commit callback running: inside it, no reader may cache that
// row, because two readers holding one generation would disagree about it.
func TestAStagedRowIsNotCachedUntilItsWriteFinishes(t *testing.T) {
	c := NewRowCache(8)
	rid := id.New()
	g := c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g, NewRow(SchemaVersion))

	seq, _ := c.beginWrite(cacheTenant, cacheNS, rid)
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, g); ok {
		t.Error("a staged row was served from the cache")
	}
	c.store(cacheTenant, cacheNS, rid, g, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, g); ok {
		t.Error("a staged row was accepted into the cache")
	}

	c.finishWrite(cacheTenant, cacheNS, rid, seq)
	g2 := c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g2, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, g2); !ok {
		t.Error("the row was still quiesced after its write committed")
	}
}

// A write that is abandoned gives its own mark back *and* moves the generation,
// because whether anything of it became visible is not knowable from here: a
// condition check that failed wrote nothing, a sync that failed left an applied
// batch behind, and the cache sees the same close either way.
func TestAnAbandonedWriteGivesBackItsOwnStagingMarkAndMovesTheGeneration(t *testing.T) {
	c := NewRowCache(8)
	rid := id.New()

	seq, ok := c.beginWrite(cacheTenant, cacheNS, rid)
	if !ok {
		t.Fatal("beginWrite refused an enabled cache")
	}
	g := c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, g); ok {
		t.Fatal("a staged row was cached before its write finished")
	}

	c.finishWrite(cacheTenant, cacheNS, rid, seq)
	after := c.Generation(cacheTenant)
	if after == g {
		t.Errorf("an abandoned write left the generation at %d, so a reader from before "+
			"it and one from after it still share it across a write that may have become "+
			"visible", after)
	}
	// And the row is quiesced no longer: a reader at the generation the close left
	// behind may cache it again.
	c.store(cacheTenant, cacheNS, rid, after, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, after); !ok {
		t.Error("the row stayed quiesced after the only writer of it was abandoned")
	}
}

// A commit by the *earlier* of two writers leaves the row quiesced: the later
// writer's window is still open.
func TestACommitDoesNotClearALaterWritersStagingMark(t *testing.T) {
	c := NewRowCache(8)
	rid := id.New()
	first, _ := c.beginWrite(cacheTenant, cacheNS, rid)
	second, _ := c.beginWrite(cacheTenant, cacheNS, rid)

	c.finishWrite(cacheTenant, cacheNS, rid, first)
	g := c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, g); ok {
		t.Error("the row was cached while a second writer's window was open")
	}

	c.finishWrite(cacheTenant, cacheNS, rid, second)
	g = c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, g); !ok {
		t.Error("the row stayed quiesced after every writer had finished")
	}
}

// And the mirror, which is the order a mark meaning "the latest stager" got
// wrong: the *later* writer commits first and must not take the earlier one's
// mark with it. The earlier writer's window is the one still open, and the row
// it will make visible is not the row this commit did.
//
// The whole-path version is TestACommittedWriteDoesNotTakeAnotherWritersStagingMark.
func TestACommitDoesNotClearAnEarlierWritersStagingMark(t *testing.T) {
	c := NewRowCache(8)
	rid := id.New()
	first, _ := c.beginWrite(cacheTenant, cacheNS, rid)
	second, _ := c.beginWrite(cacheTenant, cacheNS, rid)

	c.finishWrite(cacheTenant, cacheNS, rid, second)
	g := c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, g); ok {
		t.Error("the row was cached while the first writer's window was open")
	}

	c.finishWrite(cacheTenant, cacheNS, rid, first)
	g = c.Generation(cacheTenant)
	c.store(cacheTenant, cacheNS, rid, g, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, cacheNS, rid, g); !ok {
		t.Error("the row stayed quiesced after every writer had finished")
	}
}

// An empty namespace is the default one on both sides of the cache. [ReadRow]
// normalises it, so a side of the cache that did not would key a row no other side
// looks at: on the write side a staging mark that guards nothing, on the read side
// two entries for one row, each invisible to the other's spelling.
func TestAnEmptyNamespaceIsTheDefaultNamespaceOnBothSides(t *testing.T) {
	c := NewRowCache(8)
	rid := id.New()
	g := c.Generation(cacheTenant)
	c.store(cacheTenant, tenant.DefaultNamespace, rid, g, NewRow(SchemaVersion))

	seq, _ := c.beginWrite(cacheTenant, "", rid)
	if _, ok := c.lookup(cacheTenant, tenant.DefaultNamespace, rid, g); ok {
		t.Error("a write staged with an empty namespace left the default namespace's row cached")
	}
	c.store(cacheTenant, tenant.DefaultNamespace, rid, g, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, tenant.DefaultNamespace, rid, g); ok {
		t.Error("a write staged with an empty namespace let the default namespace's row be cached")
	}

	c.finishWrite(cacheTenant, "", rid, seq)
	g = c.Generation(cacheTenant)
	c.store(cacheTenant, tenant.DefaultNamespace, rid, g, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, tenant.DefaultNamespace, rid, g); !ok {
		t.Error("the row stayed quiesced after the empty-namespace write committed")
	}

	// And the read side: a store and a lookup that spell the default namespace
	// differently are one row, in either order.
	stored, asked := idOf(1), idOf(2)
	c.store(cacheTenant, "", stored, g, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, tenant.DefaultNamespace, stored, g); !ok {
		t.Error("a row stored with an empty namespace was not served to the default namespace")
	}
	c.store(cacheTenant, tenant.DefaultNamespace, asked, g, NewRow(SchemaVersion))
	if _, ok := c.lookup(cacheTenant, "", asked, g); !ok {
		t.Error("a row stored in the default namespace was not served to a lookup that " +
			"left the namespace empty")
	}
}

// The bound is on entries, and it is respected.
//
// It is enforced per shard, each holding its share, so the total a cache can hold
// is the entry count rounded up to a multiple of the shard count — the same number
// for a count that is a multiple of it, as the 16384 default is. Eviction is
// therefore LRU within a shard rather than across the whole cache, which is the
// price of not having one lock.
func TestTheCacheHoldsNoMoreThanItsBound(t *testing.T) {
	const bound = rowCacheShards * 8
	c := NewRowCache(bound)
	g := c.Generation(cacheTenant)
	const stores = bound * 16
	for i := range stores {
		c.store(cacheTenant, cacheNS, idOf(i), g, NewRow(SchemaVersion))
		if n := c.Len(); n > bound {
			t.Fatalf("after %d stores the cache holds %d entries, bound is %d", i+1, n, bound)
		}
	}
	if n := c.Len(); n != bound {
		t.Errorf("the cache holds %d entries, want its full bound of %d", n, bound)
	}
	// The oldest are the ones gone.
	if _, ok := c.lookup(cacheTenant, cacheNS, idOf(0), g); ok {
		t.Error("the least recently used entry of its shard survived eviction")
	}
	if _, ok := c.lookup(cacheTenant, cacheNS, idOf(stores-1), g); !ok {
		t.Error("the most recently stored entry was evicted")
	}
}

// A bound that is not a multiple of the shard count is rounded up rather than
// down: a shard holding nothing would be a cache that is smaller than it was asked
// to be, and the rounding is at most one entry per shard.
func TestABoundIsRoundedUpToTheShards(t *testing.T) {
	for _, asked := range []int{1, 7, rowCacheShards + 1, 1000} {
		want := rowCacheShards * ((asked + rowCacheShards - 1) / rowCacheShards)
		c := NewRowCache(asked)
		g := c.Generation(cacheTenant)
		for i := range want * 8 {
			c.store(cacheTenant, cacheNS, idOf(i), g, NewRow(SchemaVersion))
		}
		if n := c.Len(); n != want {
			t.Errorf("a cache asked for %d entries holds %d, want %d", asked, n, want)
		}
	}
}

// The shards spread the keys. A hash that put everything in one of them would
// leave the lock exactly as contended as the one it replaced, and the only way
// that shows up is by looking.
func TestTheShardsSpreadTheKeys(t *testing.T) {
	const keys = rowCacheShards * 64
	c := NewRowCache(keys)
	counts := make([]int, rowCacheShards)
	for i := range keys {
		sh := c.shard(c.key(cacheTenant, cacheNS, idOf(i)))
		for j := range c.shards {
			if sh == &c.shards[j] {
				counts[j]++
			}
		}
	}
	// A quarter of the even share is a generous floor; the point is that no shard
	// is starved and none holds most of the keys.
	low, high := keys/rowCacheShards/4, keys/rowCacheShards*4
	for i, n := range counts {
		if n < low || n > high {
			t.Errorf("shard %d holds %d of %d keys, want between %d and %d: %v",
				i, n, keys, low, high, counts)
			return
		}
	}
}

// Zero entries disables the cache entirely: nothing is held, nothing is served,
// and the generation machinery costs nothing.
func TestZeroEntriesDisablesTheCache(t *testing.T) {
	for name, c := range map[string]*RowCache{"zero": NewRowCache(0), "nil": nil} {
		t.Run(name, func(t *testing.T) {
			rid := id.New()
			if g := c.Generation(cacheTenant); g != 0 {
				t.Errorf("a disabled cache reported generation %d", g)
			}
			c.store(cacheTenant, cacheNS, rid, 0, NewRow(SchemaVersion))
			if _, ok := c.lookup(cacheTenant, cacheNS, rid, 0); ok {
				t.Error("a disabled cache served a row")
			}
			if n := c.Len(); n != 0 {
				t.Errorf("a disabled cache holds %d entries", n)
			}
			if _, ok := c.beginWrite(cacheTenant, cacheNS, rid); ok {
				t.Error("a disabled cache asked for a commit callback")
			}
		})
	}
}

func idOf(i int) id.ID {
	var b [16]byte
	copy(b[:], fmt.Sprintf("row-%011d", i))
	rid, err := id.FromBytes(b[:])
	if err != nil {
		panic(err)
	}
	return rid
}
