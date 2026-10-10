// Package pebble is the only package in Remem that may import Pebble
// (Invariant 6, spec §40.1), and the import-graph guard in internal/arch
// fails the build if that stops being true.
//
// Containment is not only about imports. Two things leak through an interface
// without any import at the other end, and both are handled here deliberately:
//
//   - Error values. Every Pebble error is translated in errors.go and its text
//     discarded; TestPebbleErrorsAreNeverExposed is what holds that.
//   - Borrowed memory. Pebble's Get returns a slice plus a closer, and the
//     slice is invalid once the closer runs. Get therefore copies before
//     closing — returning the borrowed slice would hand the caller memory that
//     is correct in a unit test and garbage under load.
package pebble

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/bits"
	"sync"
	"sync/atomic"

	pebbledb "github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/storage"
)

// Options configures a store. It is deliberately small: every knob added here
// is a Pebble concept that the rest of Remem then learns to reason about.
type Options struct {
	// CacheSizeBytes is the block cache size. Zero takes Pebble's default.
	CacheSizeBytes int64

	// MemTableSizeBytes is how much a write buffer holds before it is flushed
	// to a table. Zero takes Pebble's default, 4 MiB.
	MemTableSizeBytes int64

	// BloomBitsPerKey sizes the bloom filter written into every table. Zero
	// writes no filter, which is Pebble's default.
	BloomBitsPerKey int

	// ReadOnly opens the directory without taking a write lock, for
	// `remem-admin inspect` and for export from a running node's data
	// directory.
	ReadOnly bool

	// Logger receives the engine's own messages. Nil discards them, which is
	// what tests want; the composition root passes the server's logger.
	Logger *slog.Logger
}

// Store is a storage.KV backed by Pebble.
type Store struct {
	db *pebbledb.DB

	// writeMu serialises writers, striped by tenant. Pebble is safe for
	// concurrent writers on its own; these locks exist for one reason, which is
	// that a conditional commit checks its conditions with point reads and must
	// then apply before anything can change what it just read (see
	// batch.apply).
	//
	// A key's stripe is a hash of its tenant, so the invariant is: a write holds
	// the stripe of every key it touches, and a conditional commit therefore
	// runs atomically against every other write that touches the same tenant,
	// or the system space. That is all a condition ever needs — it names a key,
	// and only a writer of that key can falsify it — and it leaves two tenants'
	// writers out of each other's way.
	writeMu [stripeCount]sync.Mutex

	closed atomic.Bool
}

// stripeCount is how many write stripes the store has.
//
// It is 64 so that the set of stripes one batch touches is a uint64 bitmask
// (batch.stripes): the batch collects its stripes a key at a time with an OR,
// and apply walks the set bits in order with no allocation and no sorting.
//
// Collisions between tenants cost throughput, never correctness: two tenants
// sharing a stripe wait for each other exactly as every tenant did before.
const stripeCount = 64

// allStripes is every stripe: what a store-wide exclusion takes.
const allStripes = ^uint64(0)

// stripeOf is the write stripe a key belongs to.
//
// Every key one tenant owns hashes to one stripe, across every namespace and
// every space, because that is the unit a condition is scoped to: Remem's
// conditions guard a key, and a key's writers are the tenant's own writers.
//
// Stripe 0 holds the system space, which has no tenant, and also any key whose
// scope prefix does not decode — a key no builder in internal/keys produced.
// For such a key any stripe is correct so long as it is always the same one:
// what the lock needs is that two writers of the same bytes agree, and stripeOf
// is a function of the bytes alone.
func stripeOf(key []byte) uint {
	t, ok := keys.TenantOf(key)
	if !ok || len(t) == 0 {
		return 0
	}
	// FNV-1a over the tenant id. It needs to spread the tenants one node serves
	// across the stripes and to be the same function in every process that
	// opens the directory; it is not used for anything an adversary chooses, so
	// it does not need to be a cryptographic hash. Writing it out rather than
	// calling hash/fnv avoids allocating a hash.Hash per key.
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := range len(t) {
		h ^= uint64(t[i])
		h *= prime64
	}
	return uint(h % stripeCount)
}

// stripeBit is the one-element stripe set holding key's stripe.
func stripeBit(key []byte) uint64 { return 1 << stripeOf(key) }

// lockStripes takes every stripe in set, lowest first.
//
// The order is the whole reason this is a function: two batches whose stripe
// sets overlap would deadlock if either took them in any other order.
func (s *Store) lockStripes(set uint64) {
	for rest := set; rest != 0; rest &= rest - 1 {
		s.writeMu[bits.TrailingZeros64(rest)].Lock()
	}
}

// unlockStripes releases every stripe in set, highest first: the reverse of
// lockStripes, which releases the stripe most recently taken first.
func (s *Store) unlockStripes(set uint64) {
	for rest := set; rest != 0; {
		i := bits.Len64(rest) - 1
		s.writeMu[i].Unlock()
		rest &^= 1 << i
	}
}

var (
	_ storage.KV            = (*Store)(nil)
	_ storage.StatsReporter = (*Store)(nil)
)

// EngineStats samples Pebble's own counters. It is cheap enough to call on every
// scrape: Metrics copies counters Pebble already keeps rather than walking
// anything.
func (s *Store) EngineStats() storage.EngineStats {
	m := s.db.Metrics()
	return storage.EngineStats{
		DiskBytes:   m.DiskSpaceUsage(),
		Compactions: m.Compact.Count,
		CacheHits:   m.BlockCache.Hits,
		CacheMisses: m.BlockCache.Misses,
	}
}

// Open opens or creates the store in dir.
func Open(dir string, opts Options) (storage.KV, error) {
	po := &pebbledb.Options{ReadOnly: opts.ReadOnly}
	po.Logger = engineLogger{log: opts.Logger}
	if opts.CacheSizeBytes > 0 {
		cache := pebbledb.NewCache(opts.CacheSizeBytes)
		defer cache.Unref()
		po.Cache = cache
	}
	applyTuning(po, opts)

	db, err := pebbledb.Open(dir, po)
	if err != nil {
		// The returned error carries no engine text, per this package's rule.
		// The cause is logged instead, which is what the rule promises and
		// what Open in particular owes an operator: a directory that will not
		// open is almost always a permission, a lock or a path problem, and
		// "the storage engine failed" is not something anybody can act on.
		log := opts.Logger
		if log == nil {
			// obs.Logger, not slog.Default: internal/obs is the only package
			// that constructs a logger, and its own test enforces that.
			log = obs.Logger(context.Background())
		}
		log.Error("the data directory could not be opened", "path", dir, "error", err)
		return nil, translate("pebble.Open", err)
	}
	return &Store{db: db}, nil
}

// applyTuning puts the two size options into force.
//
// It is a function of its own so that what it sets can be asserted without
// opening a directory: Pebble fills in its defaults during Open, after which
// "the configured value" and "Pebble's default" are the same field.
func applyTuning(po *pebbledb.Options, opts Options) {
	if opts.MemTableSizeBytes > 0 {
		po.MemTableSize = uint64(opts.MemTableSizeBytes)
	}
	if opts.BloomBitsPerKey > 0 {
		// Levels is a fixed-size array of per-level options, and Pebble copies
		// a level's filter policy down from the level above only where the
		// level's own is nil. Setting every one says it outright rather than
		// relying on that inheritance, which is a detail of EnsureDefaults.
		policy := bloom.FilterPolicy(opts.BloomBitsPerKey)
		for level := range po.Levels {
			po.Levels[level].FilterPolicy = policy
		}
	}
}

func (s *Store) Get(_ context.Context, key []byte) ([]byte, error) {
	if s.closed.Load() {
		return nil, errs.E(errs.Unavailable, "pebble.Get", errClosed)
	}
	value, closer, err := s.db.Get(key)
	if err != nil {
		return nil, translate("pebble.Get", err)
	}
	return copyAndClose(value, closer, "pebble.Get")
}

func (s *Store) Set(_ context.Context, key, value []byte) error {
	mu := &s.writeMu[stripeOf(key)]
	mu.Lock()
	defer mu.Unlock()
	if s.closed.Load() {
		return errs.E(errs.Unavailable, "pebble.Set", errClosed)
	}
	if err := s.db.Set(key, value, pebbledb.NoSync); err != nil {
		return translate("pebble.Set", err)
	}
	return nil
}

func (s *Store) Delete(_ context.Context, key []byte) error {
	mu := &s.writeMu[stripeOf(key)]
	mu.Lock()
	defer mu.Unlock()
	if s.closed.Load() {
		return errs.E(errs.Unavailable, "pebble.Delete", errClosed)
	}
	if err := s.db.Delete(key, pebbledb.NoSync); err != nil {
		return translate("pebble.Delete", err)
	}
	return nil
}

// NewBatch returns an *indexed* batch: the plain one cannot be read from, and
// storage.Batch requires read-your-writes so that a transaction can consult
// what it has already staged before deciding what to write next.
func (s *Store) NewBatch() storage.Batch {
	return &batch{store: s, b: s.db.NewIndexedBatch()}
}

func (s *Store) NewSnapshot() storage.Snapshot {
	return &snapshot{snap: s.db.NewSnapshot()}
}

func (s *Store) NewIterator(lower, upper []byte) storage.Iterator {
	it, err := s.db.NewIter(iterOptions(lower, upper))
	if err != nil {
		return failedIter(translate("pebble.NewIterator", err))
	}
	return &iterator{it: it}
}

func (s *Store) Flush(_ context.Context) error {
	if s.closed.Load() {
		return errs.E(errs.Unavailable, "pebble.Flush", errClosed)
	}
	// LogData with Sync forces the WAL to disk, which is what durability means
	// here. Pebble's Flush() rotates the memtable to an sstable — a compaction
	// concern, and far more expensive than the caller is asking for.
	if err := s.db.LogData(nil, pebbledb.Sync); err != nil {
		return translate("pebble.Flush", err)
	}
	return nil
}

// Close takes every stripe, not one: it is the single operation that excludes
// all writers rather than the writers of one tenant.
func (s *Store) Close() error {
	s.lockStripes(allStripes)
	defer s.unlockStripes(allStripes)
	if s.closed.Swap(true) {
		return nil // idempotent
	}
	return translate("pebble.Close", s.db.Close())
}

func iterOptions(lower, upper []byte) *pebbledb.IterOptions {
	return &pebbledb.IterOptions{LowerBound: lower, UpperBound: upper}
}

// copyAndClose copies a borrowed value and releases the borrow.
//
// The copy is the whole point: value aliases a Pebble block that the closer
// releases. The returned slice is non-nil even when empty, so that an empty
// value stays distinguishable from an absent key.
func copyAndClose(value []byte, closer io.Closer, op string) ([]byte, error) {
	out := make([]byte, len(value))
	copy(out, value)
	if err := closer.Close(); err != nil {
		return nil, translate(op, err)
	}
	return out, nil
}

// engineLogger routes Pebble's own chatter into Remem's structured logger.
//
// Pebble's default logger writes unstructured lines to standard error with the
// standard library's log package. Adapting is cheap and keeps the compaction
// and WAL messages — genuinely useful when a directory misbehaves — instead of
// discarding them.
//
// A nil logger drops every message rather than constructing a discarding one:
// only internal/obs may construct a logger, and TestOnlyObsConstructsLoggers
// enforces that by scanning for the constructor calls. Passing nil is what
// tests want and what an embedder who has not wired observability yet gets.
//
// Fatalf is Pebble's "this process cannot continue" path. It is logged and then
// re-raised as a panic, because returning from it would let Pebble carry on
// past a condition it has itself declared unrecoverable.
type engineLogger struct{ log *slog.Logger }

func (l engineLogger) Infof(format string, args ...any) {
	if l.log == nil {
		return
	}
	l.log.Info(fmt.Sprintf(format, args...))
}

func (l engineLogger) Errorf(format string, args ...any) {
	if l.log == nil {
		return
	}
	l.log.Error(fmt.Sprintf(format, args...))
}

func (l engineLogger) Fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if l.log != nil {
		l.log.Error(msg, slog.Bool("fatal", true))
	}
	panic("pebble: " + msg)
}
