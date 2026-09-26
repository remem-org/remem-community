package pebble

import (
	"bytes"
	"context"
	"errors"

	pebbledb "github.com/cockroachdb/pebble/v2"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
)

type batch struct {
	store *Store
	b     *pebbledb.Batch
	// err holds the first staging failure. Set and Delete cannot report one —
	// the interface is deliberately fire-and-forget so that building a
	// transaction is not forty error checks — so the failure is carried here
	// and returned by Commit, which is the point at which it matters.
	err        error
	closed     bool
	conditions []condition
	written    map[string]bool
}

var _ storage.Batch = (*batch)(nil)

func (b *batch) Set(key, value []byte) {
	if b.written == nil {
		b.written = map[string]bool{}
	}
	b.written[string(key)] = true
	if b.err != nil {
		return
	}
	// Pebble copies key and value into the batch's own buffer, so the caller's
	// slices are not retained.
	if err := b.b.Set(key, value, nil); err != nil {
		b.err = translate("pebble.Batch.Set", err)
	}
}

func (b *batch) Delete(key []byte) {
	if b.written == nil {
		b.written = map[string]bool{}
	}
	b.written[string(key)] = true
	if b.err != nil {
		return
	}
	if err := b.b.Delete(key, nil); err != nil {
		b.err = translate("pebble.Batch.Delete", err)
	}
}

func (b *batch) Get(key []byte) ([]byte, error) {
	value, closer, err := b.b.Get(key)
	if err != nil {
		return nil, translate("pebble.Batch.Get", err)
	}
	return copyAndClose(value, closer, "pebble.Batch.Get")
}

// Len reports staged operations, not staged bytes. Pebble's Batch.Len is the
// encoded size; Count is the record count, which is what the contract means.
func (b *batch) Len() int { return int(b.b.Count()) }

func (b *batch) Commit(_ context.Context, sync bool) error {
	if b.err != nil {
		return b.err
	}
	if b.closed {
		return errs.E(errs.Unavailable, "pebble.Batch.Commit", errClosed)
	}
	if err := b.apply(); err != nil {
		return err
	}
	if !sync {
		return nil
	}
	// The sync happens after the store's write mutex is released, and the commit
	// still does not return until it has.
	//
	// Held inside the mutex, every synced commit in the process fsynced one at a
	// time: throughput was bounded by the disk's fsyncs a second whatever the
	// concurrency — about 405 commits a second for fifty writers on the Phase 13
	// benchmark host. Outside it, concurrent callers' syncs travel Pebble's commit
	// pipeline together and share one fsync.
	//
	// Durability is unchanged. The batch is already in the write-ahead log, which
	// is sequential, so a sync issued after it covers it; and nothing is
	// acknowledged until that sync returns. What changes is only visibility: a
	// concurrent reader may see a batch a moment before it is durable. A crash in
	// that moment loses it, as it always could before its commit returned — and
	// nobody was told it had landed. test/crash's
	// TestCrashDuringWriteLeavesNoPartialRecord holds the promise that matters:
	// every acknowledged write survives a kill.
	if err := b.store.db.LogData(nil, pebbledb.Sync); err != nil {
		return translate("pebble.Batch.Commit", err)
	}
	return nil
}

// apply checks the batch's conditions and applies it, unsynced, under the
// store's write mutex, which is what makes a condition and the write it guards
// atomic against every other write in the process.
func (b *batch) apply() error {
	b.store.writeMu.Lock()
	defer b.store.writeMu.Unlock()
	for _, c := range b.conditions {
		v, err := b.store.Get(context.Background(), c.key)
		if err != nil && !errs.Is(err, errs.NotFound) {
			return err
		}
		exists := err == nil
		if exists != c.exists || (exists && !bytes.Equal(v, c.value)) {
			return errs.E(errs.Conflict, "pebble.Batch.Commit", errors.New("conditional write conflict"))
		}
	}
	if err := b.b.Commit(pebbledb.NoSync); err != nil {
		return translate("pebble.Batch.Commit", err)
	}
	return nil
}

func (b *batch) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	return translate("pebble.Batch.Close", b.b.Close())
}

type condition struct {
	key, value []byte
	exists     bool
}

func (b *batch) Expect(key, value []byte, exists bool) {
	if b.written[string(key)] {
		return
	}
	for _, c := range b.conditions {
		if bytes.Equal(c.key, key) {
			return
		}
	}
	b.conditions = append(b.conditions, condition{bytes.Clone(key), bytes.Clone(value), exists})
}
