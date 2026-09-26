// Package metered counts what Remem asks of its store: reads and writes by
// tenant and key space, and how long each range scan was held open.
//
// It is a decorator over storage.KV rather than a counter at each call site,
// for the reason internal/record indexes itself through Indexer: a count that
// every caller must remember to make is a count that is missing from the one
// caller that forgot. Wrapped once at the composition root, every read and
// write in the process is counted, including the ones written after this
// package was.
//
// # Labels
//
// The tenant and the space come from the key, through keys.ParseSpace. The
// system space has no tenant and is labelled with an empty one. A key that does
// not parse is labelled op="unparsed" rather than dropped — it is exactly the
// key an operator needs to see. A scan whose bound names a tenant but no space,
// or no bound at all, is labelled as the range it is.
//
// # What it deliberately does not do
//
// It does not change an answer. It runs the storage contract suite, and it
// forwards exactly the optional capabilities the wrapped store has — a store
// that can back itself up still can, and a store that cannot does not start
// claiming to, because schema.BackUp finds that capability by type assertion
// and refuses a data-rewriting migration without it.
package metered

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/storage"
)

// New wraps kv so that its reads, writes and scans are recorded on m.
func New(kv storage.KV, m *obs.StorageMetrics) storage.KV {
	w := &store{inner: kv, m: m, reads: newCounters(m.ReadsTotal), writes: newCounters(m.WritesTotal)}
	if b, ok := kv.(storage.Backupper); ok {
		return &backupStore{store: w, backup: b}
	}
	return w
}

type store struct {
	inner storage.KV
	m     *obs.StorageMetrics

	reads, writes *counters
}

var _ storage.KV = (*store)(nil)

// backupStore is store for an engine that can take a consistent copy of itself.
type backupStore struct {
	*store
	backup storage.Backupper
}

var _ storage.Backupper = (*backupStore)(nil)

func (b *backupStore) Backup(ctx context.Context, dest string) error {
	return b.backup.Backup(ctx, dest)
}

// labels are the tenant and space a key belongs to.
func labels(k []byte) (tenant, op string) {
	t, _, s, err := keys.ParseSpace(k)
	if err != nil {
		return "", "unparsed"
	}
	return string(t), s.String()
}

// scanLabels are the tenant and space a range scan covers, from its lower bound.
func scanLabels(lower []byte) (tenant, op string) {
	if lower == nil {
		return "", "all"
	}
	t, _, s, err := keys.ParseSpace(lower)
	if err != nil {
		// A tenant or namespace range ends before its space byte, which is
		// not damage: it is a scan across every space the tenant owns.
		return "", "range"
	}
	return string(t), s.String()
}

func (s *store) read(k []byte) { s.reads.counter(k).Inc() }

func (s *store) write(k []byte) { s.writes.counter(k).Inc() }

func (s *store) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.read(key)
	return s.inner.Get(ctx, key)
}

func (s *store) Set(ctx context.Context, key, value []byte) error {
	if err := s.inner.Set(ctx, key, value); err != nil {
		return err
	}
	s.write(key)
	return nil
}

func (s *store) Delete(ctx context.Context, key []byte) error {
	if err := s.inner.Delete(ctx, key); err != nil {
		return err
	}
	s.write(key)
	return nil
}

func (s *store) NewBatch() storage.Batch {
	return &batch{inner: s.inner.NewBatch(), s: s}
}

func (s *store) NewSnapshot() storage.Snapshot {
	return &snapshot{inner: s.inner.NewSnapshot(), s: s}
}

func (s *store) NewIterator(lower, upper []byte) storage.Iterator {
	return s.iterator(s.inner.NewIterator(lower, upper), lower)
}

func (s *store) iterator(it storage.Iterator, lower []byte) storage.Iterator {
	t, op := scanLabels(lower)
	return &iterator{Iterator: it, s: s, tenant: t, op: op, opened: time.Now()}
}

func (s *store) Flush(ctx context.Context) error { return s.inner.Flush(ctx) }

func (s *store) Close() error { return s.inner.Close() }

// batch counts its writes when, and only if, its commit succeeds. A write that
// never happened is not a write, and counting at staging time would report
// every conflicted retry of a transaction as work done.
type batch struct {
	inner storage.Batch
	s     *store
	// staged holds the counter each staged write is recorded on, not its key:
	// the counter is all a successful commit needs, and a key would be a second
	// copy of bytes the inner batch already holds.
	staged []prometheus.Counter
}

func (b *batch) Expect(key, value []byte, exists bool) { b.inner.Expect(key, value, exists) }

func (b *batch) Set(key, value []byte) {
	b.inner.Set(key, value)
	b.staged = append(b.staged, b.s.writes.counter(key))
}

func (b *batch) Delete(key []byte) {
	b.inner.Delete(key)
	b.staged = append(b.staged, b.s.writes.counter(key))
}

func (b *batch) Get(key []byte) ([]byte, error) {
	b.s.read(key)
	return b.inner.Get(key)
}

func (b *batch) Len() int { return b.inner.Len() }

func (b *batch) Commit(ctx context.Context, sync bool) error {
	if err := b.inner.Commit(ctx, sync); err != nil {
		return err
	}
	for _, c := range b.staged {
		c.Inc()
	}
	b.staged = nil
	return nil
}

func (b *batch) Close() error { return b.inner.Close() }

type snapshot struct {
	inner storage.Snapshot
	s     *store
}

func (sn *snapshot) Get(ctx context.Context, key []byte) ([]byte, error) {
	sn.s.read(key)
	return sn.inner.Get(ctx, key)
}

func (sn *snapshot) NewIterator(lower, upper []byte) storage.Iterator {
	return sn.s.iterator(sn.inner.NewIterator(lower, upper), lower)
}

func (sn *snapshot) Close() error { return sn.inner.Close() }

// iterator observes how long a scan was held open, once, at its first Close.
//
// The wall clock rather than an injected one: this is a latency measurement,
// not durable business logic, and a fake clock would report every scan in every
// test as taking no time at all.
type iterator struct {
	storage.Iterator
	s          *store
	tenant, op string
	opened     time.Time
	closed     bool
}

func (it *iterator) Close() error {
	if !it.closed {
		it.closed = true
		it.s.m.ScanDuration.WithLabelValues(it.tenant, it.op).Observe(time.Since(it.opened).Seconds())
	}
	return it.Iterator.Close()
}
