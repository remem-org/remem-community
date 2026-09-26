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
	"sync"
	"sync/atomic"

	pebbledb "github.com/cockroachdb/pebble/v2"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/storage"
)

// Options configures a store. It is deliberately small: every knob added here
// is a Pebble concept that the rest of Remem then learns to reason about.
type Options struct {
	// CacheSizeBytes is the block cache size. Zero takes Pebble's default.
	CacheSizeBytes int64

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
	db      *pebbledb.DB
	writeMu sync.Mutex

	closed atomic.Bool
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
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return errs.E(errs.Unavailable, "pebble.Set", errClosed)
	}
	if err := s.db.Set(key, value, pebbledb.NoSync); err != nil {
		return translate("pebble.Set", err)
	}
	return nil
}

func (s *Store) Delete(_ context.Context, key []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
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

func (s *Store) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
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
