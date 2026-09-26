package storage

// Iterator walks a key range in order. It is not safe for concurrent use.
//
// Every positioning method reports whether the iterator now sits on a valid
// entry. When one returns false the iterator is exhausted or has failed, and
// [Iterator.Error] distinguishes the two:
//
//	it := kv.NewIterator(lo, hi)
//	defer it.Close()
//	for ok := it.First(); ok; ok = it.Next() {
//	    k := append([]byte(nil), it.Key()...)   // copy: see below
//	    ...
//	}
//	if err := it.Error(); err != nil { ... }
//
// # Key and Value are borrowed
//
// Both return slices that the implementation may overwrite on the next
// positioning call — Pebble hands back a window into a block buffer it reuses.
// A caller that retains one without copying reads a plausible wrong value
// later, which is the worst failure mode available: it does not crash and it
// does not look wrong. The in-memory implementation therefore overwrites its
// buffers deliberately, so that a missing copy fails in tests rather than in
// production.
type Iterator interface {
	// First positions on the smallest key in range.
	First() bool
	// Last positions on the largest key in range.
	Last() bool
	// SeekGE positions on the smallest key >= key that is in range.
	SeekGE(key []byte) bool
	// Next advances to the following key.
	Next() bool
	// Prev retreats to the preceding key.
	Prev() bool

	// Key is the current key. Valid only until the next positioning call.
	Key() []byte
	// Value is the current value. Valid only until the next positioning call.
	Value() []byte

	// Error reports the failure that stopped the iteration, or nil if it
	// simply reached the end of the range.
	Error() error

	// Close releases the iterator. It is idempotent.
	Close() error
}
