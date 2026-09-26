package storage

import (
	"bytes"
	"context"
	"errors"

	"github.com/remem-org/remem-go/internal/errs"
)

// SeekReader answers point reads within a range of a snapshot through one
// iterator, rather than one point lookup each.
//
// A search reads the attribute row and the body of every memory it returns,
// twenty point reads for a page of ten. Measured in Phase 13 on Pebble, a point
// Get cost about 2µs and a seek on a reused iterator about 0.9µs, because every
// Get builds and tears down an iterator of its own.
//
// It is not safe for concurrent use: an iterator has one position. A caller
// owns one per request and closes it before the snapshot beneath it. A key
// outside the range is read from the snapshot directly, so the range decides
// what is fast and never what is answered.
type SeekReader struct {
	snap         Snapshot
	lower, upper []byte
	it           Iterator
}

// NewSeekReader reads snap through one iterator over [lower, upper). A nil
// upper bound is unbounded. The iterator is opened by the first read inside the
// range.
func NewSeekReader(snap Snapshot, lower, upper []byte) *SeekReader {
	return &SeekReader{snap: snap, lower: lower, upper: upper}
}

// Get returns a copy of the value at key as of the snapshot, or errs.NotFound.
func (r *SeekReader) Get(ctx context.Context, key []byte) ([]byte, error) {
	if bytes.Compare(key, r.lower) < 0 || (r.upper != nil && bytes.Compare(key, r.upper) >= 0) {
		return r.snap.Get(ctx, key)
	}
	if r.it == nil {
		r.it = r.snap.NewIterator(r.lower, r.upper)
	}
	// SeekGE lands on the smallest key at or after this one, which for an
	// absent key is its neighbour. Only an exact match is an answer.
	if !r.it.SeekGE(key) || !bytes.Equal(r.it.Key(), key) {
		if err := r.it.Error(); err != nil {
			return nil, err
		}
		return nil, errs.E(errs.NotFound, "storage.SeekReader.Get", errors.New("key not found"))
	}
	v := r.it.Value()
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

// NewIterator returns an iterator of the snapshot's own over [lower, upper).
func (r *SeekReader) NewIterator(lower, upper []byte) Iterator {
	return r.snap.NewIterator(lower, upper)
}

// Close releases the reader's iterator. It does not close the snapshot, which
// the reader does not own. It is idempotent.
func (r *SeekReader) Close() error {
	if r.it == nil {
		return nil
	}
	err := r.it.Close()
	r.it = nil
	return err
}
