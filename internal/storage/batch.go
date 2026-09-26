package storage

import "context"

// Batch accumulates writes that become visible together or not at all
// (spec §12: a record and its indexes are written atomically).
//
// A Batch is not safe for concurrent use; it is owned by the goroutine that
// created it. Two batches committing concurrently do not interleave — a reader
// never observes half of either.
//
// Close is idempotent and must be called; Commit does not release the batch.
type Batch interface {
	// Expect requires the pre-write value/existence at commit. Slices are copied.
	// Expectations after staging that key are ignored (read-your-writes).
	// A mismatch returns errs.Conflict without applying any writes.
	Expect(key, value []byte, exists bool)

	// Set stages a write. The slices are copied, because a batch outlives the
	// statement that built the key.
	Set(key, value []byte)

	// Delete stages a deletion.
	Delete(key []byte)

	// Get reads through the batch: staged writes first, then the underlying
	// store. A key staged for deletion reads as errs.NotFound even when it is
	// still present in the store.
	Get(key []byte) ([]byte, error)

	// Len reports how many operations are staged. Two writes to the same key
	// count twice: this is the size of the batch, not of its effect.
	Len() int

	// Commit applies every staged operation atomically. When sync is true the
	// batch is durable before Commit returns.
	Commit(ctx context.Context, sync bool) error

	// Close discards the batch. Closing a committed batch is a no-op.
	Close() error
}
