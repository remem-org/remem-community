package attr

import (
	"container/list"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// RowCache is a bounded, process-wide cache of decoded attribute rows, shared by
// every read a tenant's queries make and correct under snapshot isolation.
//
// # Why it exists
//
// Every candidate a filtered query examines costs one point read plus one
// [DecodeRow], and a page of ten under a one-in-five filter settles about fifty
// candidates. Rows change rarely relative to how often they are read, so the
// same few bytes are fetched and unpacked again on every query. Phase 13's
// profile put that at a tenth of a filtered search's CPU, against a thirtieth
// for the similarity walk it exists to serve.
//
// # The hard part, and the two things that answer it
//
// A read goes through a snapshot pinned for the life of a paging session, and
// the cache is shared by sessions holding different snapshots. Serving a row one
// session's snapshot does not show would be worse than any saving: a filtered
// listing would include a memory that was archived before its snapshot, or drop
// one that was not. So the cache must never hand a reader a row that differs
// from what that reader's own snapshot holds.
//
// Two mechanisms together make that true, and neither is sufficient alone.
//
// A per-tenant **generation** counts row writes that have finished — committed,
// or closed without committing, which from here is the same event. A session reads
// `g0 := Generation(t)` *before* it takes its snapshot, and the cache serves it
// only while `entry.gen == g0 == Generation(t)`. Because a generation only ever
// increases, that equality proves no row write of this tenant finished between
// the session reading g0 and this lookup — and the session's
// snapshot was taken inside that interval. It catches every write whose
// commit *completes* during a reader's life, in both directions: a reader on an
// older snapshot never sees a newer row, because the newer row's entry carries a
// generation it does not hold; and a reader on a newer snapshot is never served
// an older row, because a stale entry's generation has been left behind. The
// cost is that one write invalidates the whole of that tenant's cache.
//
// A **staging mark** covers what the generation cannot. A write becomes visible
// to new snapshots when its batch is applied, and bumps the generation a moment
// later, when the transaction ends. Inside that window two readers can hold one
// generation while their snapshots straddle the write, and they would disagree
// about the row. [Indexer.Stage] therefore marks the row *before* the commit,
// through [RowCache.beginWrite], and gives the mark back in the same callback
// that bumps the generation. A marked row is neither served nor stored, so no
// entry for it can exist across its own commit window.
//
// # Both ends of a transaction move the generation
//
// A transaction that ends without having committed moves the generation exactly
// as one that committed does, because from here the two are not distinguishable
// in the way that would matter. The store applies a batch unsynced inside its
// write lock and syncs it separately, so a sync failure returns an error from a
// commit that every later snapshot already sees; a condition check that failed
// wrote nothing at all. Both arrive as a Close and nothing else. Treating a close
// as "nothing became visible" therefore left a real straddle open, and the two
// ends are now one function, [RowCache.finishWrite].
//
// # The mark belongs to its transaction
//
// This is the invariant, and it is the whole of what makes the mark sound:
//
//	A row carries one mark per transaction that has staged a write to it and
//	not yet finished, and a mark is removed only by the transaction that put it
//	there — on its commit or on its close, through [RowCache.finishWrite]
//	either way. So the row is quiesced for the *union* of every in-flight
//	writer's window, and no writer can shorten another's.
//
// A mark that was instead the row's "latest stager" got this wrong, in an
// interleaving that is a few instructions wide and a whole fsync long under
// `storage.sync_writes`. W1 marks the row and is preempted before it reads it;
// W2 marks the same row and commits, and its callback both bumps the generation
// and — being the latest stager — takes W1's mark away; W1 then reads the row W2
// left, so its own condition still holds, and commits. For the length of W1's
// commit its row is visible with no mark and a generation that has not moved for
// it, and two readers on that generation are served each other's rows.
// TestACommittedWriteDoesNotTakeAnotherWritersStagingMark is that interleaving,
// made deterministic, and TestACommitDoesNotClearAnEarlierWritersStagingMark is
// the unit of it.
//
// Because a close releases the mark, nothing has to heal a mark left behind: an
// abandoned attempt — the common case, `txn.Do` retrying a conflict — gives its
// own back, and only a transaction dropped without being closed leaks one.
//
// Together: for any row write W with stage s, the earliest instant it could be
// visible c, and the end that bumps the generation and releases its mark b —
// its commit callback or its close, whichever it reaches (s < c < b) — a reader
// that passes both checks at time x has either x < s — so its snapshot, taken
// before x, is before c and does not see W — or no bump of W's in its interval,
// which with W's own mark absent at x forces x < s
// again. Two readers sharing a generation therefore share a view of every row
// either of them caches. The unit test for the straddle is
// TestAStagedRowIsNotCachedUntilItsWriteFinishes, and the property test over
// concurrent writers and readers is TestACachedReaderAgreesWithItsOwnSnapshot.
//
// # What the ordinary two-writer case rests on, and what it does not
//
// Two writers interleaving their stages on one row is rarer than it sounds:
// `memory.Service.write` takes a per-tenant txn.Gate around the whole
// transaction, so a tenant's memory writes do not overlap at all. What is
// ungated is the rest — `lifecycle`'s maintenance pass writes through
// `record.Repo.Put` on a bare transaction, and [RebuildRows] commits a batch of
// five hundred rows at a time — so the overlap is reachable, and the invariant
// above rather than the gate is what the cache may rely on.
//
// What the *data* rests on in that case is Indexer.currentRow: it reads the row
// inside the transaction and records `tx.Expect(rowKey, value, exists)`, and the
// store checks that condition while it applies the batch, under the write stripe
// of the row's key — so the check and every competing apply for that stripe are
// serialised (see internal/storage/pebble.batch.apply). Two writers that read the
// same row therefore cannot both commit: the loser conflicts, and `txn.Do`'s
// retry re-reads and re-stages, taking a fresh mark. That is what keeps the
// visible values of a row a chain in which every writer read its predecessor. It
// is *not* what makes the cache safe — a commit that wins its condition check
// still has a window, which is the mark's job.
//
// # What it gives up
//
// A write-heavy tenant gets few hits: one finished row write retires every
// entry that tenant holds. That is the deliberate trade — a per-row version
// would need a sequence number the storage interface does not expose — and it
// costs nothing but the saving, because a miss is the read the cache replaced.
//
// # Shared rows
//
// An entry holds the decoded *[Row] itself, not a copy, and every reader on a
// hit gets that same pointer. Rows are read-only to every consumer: [Match] and
// every [Pred] only call [Row.Get], and [Row.Set] is reached solely from
// [DecodeRow] and [Project], both of which build a fresh row. A [Value] is a
// comparable struct holding a string, so there is nothing inside one to mutate
// either. A consumer that needs to change a row must copy it first.
//
// A nil *RowCache is a disabled cache, so a caller given none needs no branch.
type RowCache struct {
	// capacity is the entry bound asked for. The one enforced is per shard, so
	// the total is this rounded up to a multiple of [rowCacheShards]; the default
	// is a multiple of it, and the two are then the same number.
	capacity int
	shards   [rowCacheShards]rowCacheShard
	// gens is the finished-row-write counter per tenant. It is outside the
	// shards because every lookup of every shard reads it, and a mutex on the
	// path of every row read is the thing the shards exist to remove.
	gens generations
	// staging is the one piece of bookkeeping that is not per row: how many marks
	// are outstanding, and whether the bound has turned the cache off.
	staging  stagingGate
	stageSeq atomic.Uint64

	hits, misses atomic.Uint64
	// metrics is where a reader's tally is published, or nil. It is folded once
	// per request rather than once per row: a labelled counter increment per
	// candidate would be a measurable share of what the cache saves.
	metrics *obs.RowCacheMetrics
}

// rowCacheShards is how many independently locked pieces the entries are spread
// over, by a hash of the row's id.
//
// The entries are not the contended part on their own — a lookup is a map read —
// but an LRU makes every lookup a *write*, because the entry it served moves to
// the front of the order. One mutex therefore turns the concurrency of a server
// into a queue on the path of every candidate a query settles, which is the
// opposite of what the cache was added for. Sixteen is enough that eight
// concurrent queries rarely meet and few enough that the bound divides cleanly;
// `BenchmarkRowCacheConcurrentLookup` is the measurement.
//
// It is a power of two so a shard is the top bits of a multiplicative hash.
const rowCacheShards = 16

// rowCacheShard is one lock's worth of the cache: its own entries, its own LRU
// order with its own share of the bound, and the staging marks for its own keys.
//
// The marks live with the entries rather than beside the generations, and that is
// load-bearing: a store checks the marks and inserts under one hold of this
// mutex, so no write can place a mark in between. See [RowCache.store].
type rowCacheShard struct {
	mu       sync.Mutex
	capacity int
	entries  map[rowCacheKey]*list.Element
	order    *list.List // front is most recently used
	// staged holds, for every row some transaction has written and not yet
	// finished, the sequence number of each of those transactions. A mark
	// belongs to its transaction and nothing else: the writer that commits
	// removes its own and leaves every other, so the row stays quiesced for as
	// long as any writer's window is still open.
	staged map[rowCacheKey][]uint64
}

// RowCacheOption supplies something the cache can run without.
type RowCacheOption func(*RowCache)

// WithRowCacheMetrics publishes hits, misses and the entries held. Without it
// nothing is published, which is what a test that is not about metrics wants.
func WithRowCacheMetrics(m *obs.RowCacheMetrics) RowCacheOption {
	return func(c *RowCache) { c.metrics = m }
}

type rowCacheKey struct {
	tenant    tenant.ID
	namespace tenant.Namespace
	id        id.ID
}

type rowCacheEntry struct {
	key rowCacheKey
	// gen is the tenant generation the row was read at. An entry is served only
	// to a reader holding exactly this generation.
	gen uint64
	row *Row
}

// NewRowCache returns a cache holding at most entries decoded rows. Zero — or no
// cache at all — disables it: every lookup misses and nothing is retained.
//
// The bound is divided evenly between the shards and rounded up, so the total
// held is `rowCacheShards * ceil(entries/rowCacheShards)`. A count that is a
// multiple of the shard count — as the 16384 default is — gets exactly itself.
func NewRowCache(entries int, opts ...RowCacheOption) *RowCache {
	if entries <= 0 {
		return nil
	}
	perShard := (entries + rowCacheShards - 1) / rowCacheShards
	c := &RowCache{capacity: perShard * rowCacheShards}
	for i := range c.shards {
		c.shards[i] = rowCacheShard{
			capacity: perShard,
			entries:  make(map[rowCacheKey]*list.Element, perShard),
			order:    list.New(),
			staged:   make(map[rowCacheKey][]uint64),
		}
	}
	c.staging.clear = c.releaseEverything
	for _, o := range opts {
		o(c)
	}
	return c
}

// Generation is the tenant's finished-row-write counter: every row write that has
// committed, and every one that was closed without committing. See [RowCache].
//
// A reader must take it *before* the snapshot it will read through, so that a
// write landing in between leaves the reader holding a generation that has moved
// on — which turns the cache off for that reader rather than letting it serve
// the wrong side of the write.
func (c *RowCache) Generation(t tenant.ID) uint64 {
	if c == nil {
		return 0
	}
	return c.gens.get(t)
}

// Len is how many rows the cache holds.
func (c *RowCache) Len() int {
	if c == nil {
		return 0
	}
	n := 0
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		n += sh.order.Len()
		sh.mu.Unlock()
	}
	return n
}

// Stats is the hits and misses readers have reported, and the entries held.
func (c *RowCache) Stats() (hits, misses uint64, entries int) {
	if c == nil {
		return 0, 0, 0
	}
	return c.hits.Load(), c.misses.Load(), c.Len()
}

// shard is the piece of the cache k belongs to.
//
// The hash is the two halves of the record id folded together and multiplied by
// the golden ratio, which spreads both a UUIDv7 — whose entropy is all in its
// tail — and the structured ids a test builds, whose entropy is in the middle.
// The tenant and the namespace are left out on purpose: they are part of the map
// key, so they need not be part of the hash, and a tenant that landed in one
// shard would have the contention this is here to remove.
func (c *RowCache) shard(k rowCacheKey) *rowCacheShard {
	h := binary.LittleEndian.Uint64(k.id[0:8]) ^ binary.LittleEndian.Uint64(k.id[8:16])
	return &c.shards[(h*0x9e3779b97f4a7c15)>>(64-rowCacheShardBits)]
}

// rowCacheShardBits is log2(rowCacheShards), so a shard is that many top bits of
// the hash.
const rowCacheShardBits = 4

// lookup returns the row cached for a record, if the reader's generation is
// still the tenant's and the entry was read at it.
//
// It resolves the tenant's counter itself, which is the spelling for a caller
// holding no reader. A [CachedReader] resolved it once when it was opened and
// calls [RowCache.lookupAt] instead.
func (c *RowCache) lookup(t tenant.ID, ns tenant.Namespace, rid id.ID, gen uint64) (*Row, bool) {
	if c == nil {
		return nil, false
	}
	return c.lookupAt(c.gens.counter(t), t, ns, rid, gen)
}

func (c *RowCache) lookupAt(current *atomic.Uint64, t tenant.ID, ns tenant.Namespace,
	rid id.ID, gen uint64) (*Row, bool) {
	if c == nil {
		return nil, false
	}
	k := c.key(t, ns, rid)

	sh := c.shard(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	// All three conditions under this one hold. The switch is in here rather than
	// ahead of the lock so that "off" means it at the instant the row is served
	// rather than at the instant the call started — see [stagingGate] — and the marks
	// are read before the generation, which is what lets those two live under
	// different locks; see [generations].
	if c.staging.off() || len(sh.staged[k]) > 0 || current.Load() != gen {
		return nil, false
	}
	el, ok := sh.entries[k]
	if !ok {
		return nil, false
	}
	e := el.Value.(*rowCacheEntry)
	if e.gen != gen {
		return nil, false
	}
	sh.order.MoveToFront(el)
	return e.row, true
}

// store records a row a reader decoded from its own snapshot, tagged with the
// generation that reader holds.
//
// The conditions are checked here rather than by the caller because this is the
// moment that matters: the reader read the row somewhere inside the interval
// that ends now, and it is this instant's view of the generation and the staging
// marks that proves the interval contained no write it could have straddled. The
// check and the insert are one hold of the shard's mutex, so no write can place a
// mark between them — and the cache's own switch is read inside that same hold, so
// a store cannot land in a shard a clear has already emptied.
func (c *RowCache) store(t tenant.ID, ns tenant.Namespace, rid id.ID, gen uint64, row *Row) {
	if c == nil {
		return
	}
	c.storeAt(c.gens.counter(t), t, ns, rid, gen, row)
}

func (c *RowCache) storeAt(current *atomic.Uint64, t tenant.ID, ns tenant.Namespace,
	rid id.ID, gen uint64, row *Row) {
	if c == nil || row == nil {
		return
	}
	k := c.key(t, ns, rid)

	sh := c.shard(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if c.staging.off() || len(sh.staged[k]) > 0 || current.Load() != gen {
		return
	}
	if el, ok := sh.entries[k]; ok {
		e := el.Value.(*rowCacheEntry)
		e.gen, e.row = gen, row
		sh.order.MoveToFront(el)
		return
	}
	sh.entries[k] = sh.order.PushFront(&rowCacheEntry{key: k, gen: gen, row: row})
	for sh.order.Len() > sh.capacity {
		oldest := sh.order.Back()
		sh.order.Remove(oldest)
		delete(sh.entries, oldest.Value.(*rowCacheEntry).key)
	}
}

// report folds one reader's tally into the cache's counters and publishes it.
func (c *RowCache) report(t tenant.ID, hits, misses int) {
	if c == nil || (hits == 0 && misses == 0) {
		return
	}
	c.hits.Add(uint64(hits))
	c.misses.Add(uint64(misses))
	if c.metrics == nil {
		return
	}
	metrics := c.metrics
	label := t.String()
	if hits > 0 {
		metrics.HitsTotal.WithLabelValues(label).Add(float64(hits))
	}
	if misses > 0 {
		metrics.MissesTotal.WithLabelValues(label).Add(float64(misses))
	}
	metrics.Entries.WithLabelValues("attr_row").Set(float64(c.Len()))
}

// key is the cache's key for a row, with an empty namespace meaning the default one
// exactly as [ReadRow] reads it.
//
// Every side of the cache keys a row through here, which is what makes the two
// spellings one row: a write that normalised and a read that did not would mark a
// row no reader looks at, and a read side that normalised only in [CachedReader]
// would key two entries for one row the moment anything else asked.
func (c *RowCache) key(t tenant.ID, ns tenant.Namespace, rid id.ID) rowCacheKey {
	if ns == "" {
		ns = tenant.DefaultNamespace
	}
	return rowCacheKey{tenant: t, namespace: ns, id: rid}
}

func (sh *rowCacheShard) removeLocked(k rowCacheKey) {
	if el, ok := sh.entries[k]; ok {
		sh.order.Remove(el)
		delete(sh.entries, k)
	}
}
