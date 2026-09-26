// Package storage is Remem's ordered key-value abstraction and the contract
// every implementation of it must satisfy.
//
// It exists for four reasons, and deliberately for no more than four (spec
// §10): to isolate Remem from Pebble so that Invariant 6 is enforceable by the
// import graph, to provide an in-memory implementation fast enough to use in
// every unit test, to keep a future engine migration possible, and to give
// transactions a logical boundary.
//
// It is therefore not a general storage abstraction layer. There is no column
// family, no merge operator and no compaction control, because Remem does not
// need them and every one of them would be a Pebble concept escaping through
// the interface it was meant to contain.
//
// # Byte slices are borrowed, not owned
//
// Keys and values handed to an implementation are read during the call and not
// retained. Keys and values returned by an [Iterator] are valid only until the
// next positioning call on that iterator — copy anything that must outlive it.
// [KV.Get] and [Snapshot.Get] are the exception: they return a fresh copy the
// caller owns.
//
// # Errors
//
// A missing key is [errs.NotFound], never a nil value with a nil error, so that
// an empty value and an absent key stay distinguishable. No implementation ever
// returns an error value from its underlying engine (spec §58).
package storage

import "context"

// KV is an ordered key-value store with atomic batches and consistent
// snapshots. Implementations are safe for concurrent use.
type KV interface {
	// Get returns a copy of the value stored at key, or errs.NotFound.
	Get(ctx context.Context, key []byte) ([]byte, error)

	// Set stores value at key, overwriting any previous value. Neither slice
	// is retained after the call returns.
	Set(ctx context.Context, key, value []byte) error

	// Delete removes key. Deleting an absent key is not an error: the
	// post-condition — key is absent — already holds.
	Delete(ctx context.Context, key []byte) error

	// NewBatch returns an atomic write batch. Its writes are invisible to
	// every reader until Commit succeeds.
	NewBatch() Batch

	// NewSnapshot pins the current state. Writes made after the call are
	// invisible through the returned Snapshot, however long it is held open.
	NewSnapshot() Snapshot

	// NewIterator returns an iterator over the half-open range
	// [lower, upper). A nil lower starts at the first key; a nil upper runs to
	// the last. The iterator observes the state at the moment it was created.
	NewIterator(lower, upper []byte) Iterator

	// Flush makes every preceding write durable.
	Flush(ctx context.Context) error

	// Close releases the store. Operations after Close return errs.Unavailable.
	Close() error
}
