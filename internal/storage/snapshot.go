package storage

import "context"

// Snapshot is a consistent read-only view of the store, pinned at the moment
// it was created.
//
// This is what makes a paged listing safe to order by a mutable field such as
// importance or health (Part II.9): every page of one listing reads the same
// state, so a record whose importance changes mid-listing can neither be
// skipped nor returned twice. The paging service retains the Snapshot object;
// a numeric sequence cannot reopen this state.
//
// An open Snapshot holds back reclamation of the versions it pins. Close it.
type Snapshot interface {
	// Get returns a copy of the value at key as of the pinned state.
	Get(ctx context.Context, key []byte) ([]byte, error)

	// NewIterator returns an iterator over [lower, upper) as of the pinned
	// state. It must be closed before the Snapshot is.
	NewIterator(lower, upper []byte) Iterator

	// Close releases the snapshot.
	Close() error
}
