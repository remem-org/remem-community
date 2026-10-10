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
	// stripes is the set of the store's write stripes this batch's keys fall
	// in, one bit per stripe. Every key handed to Set, Delete or Expect
	// contributes its stripe, and apply locks exactly this set — the written
	// keys because another batch may have a condition on one of them, and the
	// condition keys because that is what a condition asks to be protected
	// from.
	stripes uint64
}

var _ storage.Batch = (*batch)(nil)

func (b *batch) Set(key, value []byte) {
	b.stripes |= stripeBit(key)
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
	b.stripes |= stripeBit(key)
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
	// The store, not just this batch: a commit into a closed Pebble store
	// panics rather than erroring, and a writer that outlived the shutdown it
	// was not told about would take the process down. The answer a closed
	// store gives is Store.Set's and Store.Delete's — Unavailable — and apply
	// repeats it under the stripes, which is where it cannot be raced.
	if b.store.closed.Load() {
		return errs.E(errs.Unavailable, "pebble.Batch.Commit", errClosed)
	}
	if err := b.apply(); err != nil {
		return err
	}
	if !sync {
		return nil
	}
	// The sync happens after the store's write stripes are released, and the
	// commit still does not return until it has.
	//
	// Held inside the lock, every synced commit in the process fsynced one at a
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

// apply checks the batch's conditions and applies it, unsynced, holding the
// store's write stripe for every key the batch touches.
//
// That is what makes a condition and the write it guards atomic, and it is the
// whole of what a condition needs: a condition names one key, only a writer of
// that key can falsify it, and every writer in the process — Store.Set,
// Store.Delete and another batch's apply — holds the stripe of each key it
// writes. A conditional commit is therefore atomic against every other write
// touching the same tenant, or the system space, and against nothing else,
// because there is nothing else it has to be atomic against.
//
// The set is empty only for a batch with no keys at all, which guards nothing
// and locks nothing.
func (b *batch) apply() error {
	b.store.lockStripes(b.stripes)
	defer b.store.unlockStripes(b.stripes)
	// Checked while the stripes are held, exactly as Store.Set and
	// Store.Delete check it under theirs. Store.Close takes every stripe
	// before it sets the flag, so a batch that got its stripes first commits
	// into a store that is still open, and one that did not sees the flag
	// here. Without this, a commit that lost that race reached Pebble's
	// panic(ErrClosed) — a background worker still applying when the store
	// closed killed the process instead of reporting a failure nobody even
	// needed to act on, since what it was writing is derived.
	if b.store.closed.Load() {
		return errs.E(errs.Unavailable, "pebble.Batch.Commit", errClosed)
	}
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
	// Before the early returns: a key already written, or already carrying a
	// condition, has contributed its stripe, so this is idempotent — and
	// stating it unconditionally is what makes "every key this batch is told
	// about is in the stripe set" true by inspection.
	b.stripes |= stripeBit(key)
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
