package attr

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// The write side of the row cache: the per-tenant generation, the staging
// gate, and the marks a write holds across its own commit window. RowCache's
// doc comment in cache.go is the argument for why these make a cached row safe
// under snapshot isolation; this file is the bookkeeping that argument relies on.

// generations is the per-tenant finished-row-write counter.
//
// Read on every lookup and written only when a row write finishes, so it is a
// read-mostly map of atomic counters rather than a map under a mutex: a read is a
// sync.Map load and an atomic load, neither of which writes a shared word.
//
// It is *not* under the shard locks, and the order the two are touched in is
// what keeps a reader from seeing an inconsistent pair. The rule, in both
// directions:
//
//   - [RowCache.finishWrite] bumps the generation *before* it removes its mark,
//     on either of a transaction's ends.
//   - [RowCache.lookup] and [RowCache.store] read the marks, under the shard
//     lock, *before* they read the generation.
//
// So a mark found absent proves the bump that removed it is already visible —
// the removal and the mark read are ordered by the shard mutex, and the bump
// precedes the removal — and a reader can never pass the generation check on the
// strength of a write whose mark it has just seen cleared.
type generations struct {
	m sync.Map // tenant.ID -> *atomic.Uint64
}

// counter is the tenant's own counter, created if this is the first time anyone
// has asked. A reader resolves it once for its whole request: the map lookup is
// cheap but a page settles fifty candidates, and a map lookup per candidate is a
// measurable share of what the cache saves.
func (g *generations) counter(t tenant.ID) *atomic.Uint64 {
	if v, ok := g.m.Load(t); ok {
		return v.(*atomic.Uint64)
	}
	v, _ := g.m.LoadOrStore(t, new(atomic.Uint64))
	return v.(*atomic.Uint64)
}

func (g *generations) get(t tenant.ID) uint64 { return g.counter(t).Load() }

func (g *generations) bump(t tenant.ID) { g.counter(t).Add(1) }

// stagingGate counts the staging marks handed out and not given back, holds the
// switch the bound throws, and owns the clear that throwing it performs. See
// [maxStagedRows].
//
// # Why the clear is the gate's and not its caller's
//
// Turning the cache off and dropping what it holds is one transition, and it has to
// be atomic with respect to turning it back on. It was not: the switch was thrown
// under mu, mu was released, and the caller cleared afterwards. In between, the
// marks could drain to zero, [stagingGate.leave] could turn the cache back on, and
// a new write could stage a row in it — and the clear, arriving late, wiped that
// live writer's mark and left its commit window unguarded. So the clear runs inside
// the hold that decided on it, and a leave that would re-enable waits for it.
//
// The shard locks therefore nest inside mu, and nothing anywhere takes mu under a
// shard lock: [RowCache.finishWrite] has released its shard before it calls leave.
//
// # Why this is a mutex and not an atomic counter
//
// A counter with a compare-and-swap for the trip would be cheaper per write, and it
// would not be enough. The trip has to publish the switch, run the clear and keep
// the re-enable out until that is done — three steps over two words — and the
// underflow below has to latch the cache off against every concurrent leave. Both
// are mutual exclusion written out by hand, and a gate that is nearly right about
// when the cache is off is a gate that serves a row from the wrong side of a write.
// The cost is one uncontended mutex on the *write* path, beside a shard lock and a
// batch commit; the read path reads disabled and takes nothing.
type stagingGate struct {
	mu          sync.Mutex
	outstanding int
	// latched records that outstanding went negative — a mark given back more times
	// than it was handed out, which is a bug in the pairing rather than a workload.
	// The cache then stays off for good: see [stagingGate.leave].
	latched bool
	// clear drops every entry and every mark the cache holds. It is
	// [RowCache.releaseEverything], bound once by [NewRowCache] rather than passed
	// in per call, so that the write path hands nothing over to say so.
	clear func()
	// disabled mirrors, for lock-free reading, the decision taken under mu. Every
	// lookup and store reads it and none of them may take a lock to do so.
	disabled atomic.Bool
}

// enter counts one more staged write, and reports whether a mark may be recorded
// for it together with the count, if this is the call that tripped the bound.
//
// A write staged while the cache is off is still counted and still has to be
// given back: the generations must keep moving, and a write the gate has not
// counted is one that could be inside its commit window at the instant the cache
// comes back on.
func (g *stagingGate) enter() (record bool, tripped int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.outstanding++
	if g.disabled.Load() {
		return false, 0
	}
	if g.outstanding > maxStagedRows {
		g.disabled.Store(true)
		g.clearHeld()
		return false, g.outstanding
	}
	return true, 0
}

// clearHeld drops what the cache holds, under mu, as part of the transition that
// decided to. Running it after the lock was released is the bug the type's comment
// describes; a leave that would re-enable the cache waits here instead.
//
// A gate with no clear is a zero value no constructor makes; the check is so that a
// trip in a hand-built one reports the bound rather than panicking inside it.
func (g *stagingGate) clearHeld() {
	if g.clear != nil {
		g.clear()
	}
}

// gateEvent is what a [stagingGate.leave] did beyond giving one mark back.
type gateEvent int

const (
	// gateUnchanged: marks are still outstanding, or the cache was already serving.
	gateUnchanged gateEvent = iota
	// gateRecovered: that was the last mark a disabled cache was waiting for, so it
	// is serving again.
	gateRecovered
	// gateUnderflowed: a mark came back that was never handed out, so the cache has
	// stopped for good. See [stagingGate.leave].
	gateUnderflowed
)

// leave gives one mark back and reports what the gate did about it.
//
// A count that goes below zero is a mark given back more times than it was handed
// out, which is a bug in the pairing rather than a workload — and a dangerous one to
// carry on from. The gate re-enables on the strength of "nothing is in flight", and
// an undercount reaches zero while writes are still inside their commit windows, so
// the cache would serve exactly the straddle the marks exist to prevent. It
// therefore clamps the count, drops what it holds and stays off for the life of the
// process: that costs the latency the cache was there to save and nothing else.
func (g *stagingGate) leave() gateEvent {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.outstanding--
	if g.outstanding < 0 {
		g.outstanding = 0
		g.latched = true
		g.disabled.Store(true)
		g.clearHeld()
		return gateUnderflowed
	}
	if g.latched || g.outstanding > 0 || !g.disabled.Load() {
		return gateUnchanged
	}
	// Nothing is in flight, so no write is between its stage and its end: every
	// entry still held was either stored outside every write's window or has had
	// its generation moved on by the write whose window it was in. Turning back on
	// here cannot serve a straddle.
	g.disabled.Store(false)
	return gateRecovered
}

func (g *stagingGate) off() bool { return g.disabled.Load() }

// maxStagedRows bounds the staging marks a cache has handed out and not had
// back.
//
// A mark is given back when its transaction commits or is closed, and every
// transaction is closed — `defer tx.Close()` is the shape of every write in the
// repository. So the number outstanding is the number of row writes in flight,
// and reaching sixty-five thousand of those at once is a transaction that was
// dropped without being closed: a broken deployment rather than a workload. The
// cache turns itself off rather than grow without a bound or drop a mark it
// still needs, and comes back if they drain.
const maxStagedRows = 1 << 16

// beginWrite quiesces a row for the span of one transaction's write to it, and
// returns the sequence number identifying *this* transaction's mark together
// with whether the caller must give it back.
//
// It runs inside the writing transaction, before the commit, which is the whole
// point: the mark has to be in place before the write can become visible to a
// new snapshot. The mark is this transaction's, not the row's — a row carries
// one per in-flight writer — so a second writer committing cannot take the
// first's away, and only [RowCache.finishWrite] with this same sequence number
// removes it.
//
// The false it returns means "there is nothing to give back", which only a
// disabled cache answers. A cache that has turned itself off still hands out a
// sequence number and still asks for it back, because its generations must keep
// moving and a write it does not count is a write that could straddle the
// instant it comes back on; it simply records no mark for it.
func (c *RowCache) beginWrite(t tenant.ID, ns tenant.Namespace, rid id.ID) (uint64, bool) {
	if c == nil {
		return 0, false
	}
	k := c.key(t, ns, rid)
	seq := c.stageSeq.Add(1)

	record, tripped := c.staging.enter()
	if record {
		sh := c.shard(k)
		sh.mu.Lock()
		sh.staged[k] = append(sh.staged[k], seq)
		sh.removeLocked(k)
		sh.mu.Unlock()
	}

	if tripped > 0 {
		obs.Logger(context.Background()).Warn("the attribute row cache has turned itself off: "+
			"more row writes are staged and unfinished than it can mark, and a write it "+
			"cannot mark is a commit window it cannot guard; it will serve nothing until "+
			"they drain",
			"outstanding", tripped, "limit", maxStagedRows)
	}
	return seq, true
}

// finishWrite records that a staged row write is over: the tenant's generation
// moves, the entry is dropped, and this transaction's mark on the row is given
// back.
//
// It runs on both of a transaction's ends — in its commit callback and in its
// close callback — and it does the same thing on each, because the two differ in
// nothing this cache can act on. A commit that reported success made its write
// visible. A commit that reported failure *may* have: the store applies a batch
// unsynced inside its write lock and syncs it separately
// (internal/storage/pebble.batch.Commit: apply, then LogData(nil, Sync)), so a
// sync failure returns an error from a batch every snapshot taken afterwards
// already holds. That close arrives here indistinguishably from the close of a
// transaction that lost its condition check and wrote nothing, so the visibility
// of a write that did not commit is treated as unknown and the generation moves
// either way.
//
// Treating the close end as "nothing became visible" was a real straddle: the
// generation stayed where it was, a reader opened before such a write and one
// opened after it shared it while their snapshots straddled the write, and
// whichever read first put its own row where the other was served it.
// TestAFailedCommitThatMayBeVisibleStillMovesTheGeneration is that straddle, and
// TestAnAbandonedWriteGivesBackItsOwnStagingMarkAndMovesTheGeneration the unit of
// it. The cost is one atomic increment per abandoned transaction — `txn.Do`
// retrying a conflict is the common case — which is in the noise beside the
// increment every committed write already pays.
//
// Dropping the entry as well as bumping the generation is belt and braces on both
// ends: the generation alone already retires it, and no reader at the new one
// would use an entry stored before the mark went down, so this only keeps it from
// occupying the bound until it is evicted.
//
// The row stays quiesced if another transaction's mark is still on it. That is
// the whole of why a mark is per transaction: the other writer's commit window
// is still open, and its row is not the one this transaction made visible.
func (c *RowCache) finishWrite(t tenant.ID, ns tenant.Namespace, rid id.ID, seq uint64) {
	if c == nil {
		return
	}
	k := c.key(t, ns, rid)

	// The generation moves before the mark comes off, which is what makes the two
	// readable without one lock over both. See [generations].
	c.gens.bump(t)
	sh := c.shard(k)
	sh.mu.Lock()
	sh.removeLocked(k)
	sh.finishLocked(k, seq)
	sh.mu.Unlock()
	c.reportGate(c.staging.leave())
}

// finishLocked gives back one transaction's mark on a row.
func (sh *rowCacheShard) finishLocked(k rowCacheKey, seq uint64) {
	marks := sh.staged[k]
	for i, s := range marks {
		if s != seq {
			continue
		}
		// The marks on a row are a set, so the hole is filled from the end.
		last := len(marks) - 1
		marks[i] = marks[last]
		if marks = marks[:last]; len(marks) == 0 {
			delete(sh.staged, k)
		} else {
			sh.staged[k] = marks
		}
		return
	}
	// Not found is normal: the cache was off when this mark was handed out, or
	// was turned off and dropped what it held while this write was in flight.
}

// reportGate says out loud what the gate just did, if it did anything. A cache
// that has silently stopped — or started — caching is a latency change an operator
// can otherwise only find by subtracting two counters.
func (c *RowCache) reportGate(ev gateEvent) {
	switch ev {
	case gateRecovered:
		obs.Logger(context.Background()).Info("the attribute row cache is on again: " +
			"every staged row write it was waiting for has finished")
	case gateUnderflowed:
		obs.Logger(context.Background()).Error("the attribute row cache has stopped for " +
			"good: a staging mark was given back more times than it was handed out, so the " +
			"count that tells it when no write is in flight is not a count of anything and " +
			"coming back on would mean serving a row from inside a commit window; this is a " +
			"bug in the pairing of beginWrite with finishWrite, not a workload")
	case gateUnchanged:
	}
}

// releaseEverything drops every entry and every mark the cache holds, which is
// what a cache that has just turned itself off does. It stays off until every
// mark it had handed out has come back. See [maxStagedRows].
//
// The marks go with the entries. Nothing is served while the cache is off, so a
// mark has nothing to protect in the meantime, and dropping them is what keeps
// the memory bounded — which is the thing that went wrong to get here. A mark
// recorded by a write that had already been told to record one is removed by that
// write's own end: [rowCacheShard.finishLocked] does not mind a mark that is no
// longer there, and the write is counted, so the cache cannot come back on while it
// is still in flight.
//
// No *entry* survives this. The switch is thrown before the first shard is taken and
// every store reads it under the shard lock, so a store still on its way to a shard
// this has emptied finds the cache off and stores nothing. What the clear leaves
// empty stays empty until the marks drain.
//
// It runs under the gate's lock, as part of the transition that decided on it. See
// [stagingGate].
func (c *RowCache) releaseEverything() {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		sh.entries = make(map[rowCacheKey]*list.Element)
		sh.order.Init()
		sh.staged = make(map[rowCacheKey][]uint64)
		sh.mu.Unlock()
	}
}
