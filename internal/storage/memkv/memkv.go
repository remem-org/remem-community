// Package memkv is the in-memory storage.KV used by tests.
//
// It is not a cache and not a production store: it holds everything in a map,
// copies the whole map to take a snapshot, and is correct rather than fast.
// What it must be is *indistinguishable* from the Pebble adapter in behaviour,
// which is why both run the identical storagetest suite and why this
// implementation goes out of its way to be hostile in one specific place —
// see [iter].
package memkv

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
)

// Store is an in-memory ordered key-value store. The zero value is not usable;
// call [New].
type Store struct {
	mu     sync.RWMutex
	data   map[string][]byte
	keys   []string // sorted; the ordered index over data
	closed bool
}

// New returns an empty store.
func New() *Store {
	return &Store{data: make(map[string][]byte)}
}

var _ storage.KV = (*Store)(nil)

// errClosed is the cause behind every post-Close failure. Operating on a
// closed store is a lifecycle bug in the caller, but reporting it as
// Unavailable rather than Invalid matches what a real engine does during
// shutdown, and shutdown is when this actually happens.
var errClosed = errors.New("store is closed")

func (s *Store) Get(_ context.Context, key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errs.E(errs.Unavailable, "memkv.Get", errClosed)
	}
	return lookup(s.data, key, "memkv.Get")
}

func (s *Store) Set(_ context.Context, key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errs.E(errs.Unavailable, "memkv.Set", errClosed)
	}
	s.set(key, value)
	return nil
}

func (s *Store) Delete(_ context.Context, key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errs.E(errs.Unavailable, "memkv.Delete", errClosed)
	}
	s.del(key)
	return nil
}

// set stores a copy of value; the caller's slices are never retained.
func (s *Store) set(key, value []byte) {
	k := string(key)
	if _, exists := s.data[k]; !exists {
		i := sort.SearchStrings(s.keys, k)
		s.keys = append(s.keys, "")
		copy(s.keys[i+1:], s.keys[i:])
		s.keys[i] = k
	}
	s.data[k] = append([]byte(nil), value...)
}

func (s *Store) del(key []byte) {
	k := string(key)
	if _, exists := s.data[k]; !exists {
		return
	}
	delete(s.data, k)
	if i := sort.SearchStrings(s.keys, k); i < len(s.keys) && s.keys[i] == k {
		s.keys = append(s.keys[:i], s.keys[i+1:]...)
	}
}

func (s *Store) NewBatch() storage.Batch { return &batch{store: s} }

// NewSnapshot copies the entire store. That is O(n) and unashamedly so: memkv
// exists to make tests deterministic, and a copy is the shortest path to a
// view that later writes provably cannot reach.
func (s *Store) NewSnapshot() storage.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &snapshot{view: s.view()}
}

func (s *Store) NewIterator(lower, upper []byte) storage.Iterator {
	// An iterator observes the state at the moment it was created, exactly as
	// Pebble's does, so it takes the same copy a snapshot does.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return failedIter(errs.E(errs.Unavailable, "memkv.NewIterator", errClosed))
	}
	return newIter(s.view(), lower, upper)
}

func (s *Store) Flush(_ context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errs.E(errs.Unavailable, "memkv.Flush", errClosed)
	}
	return nil // memory is as durable as memory gets
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.data = nil
	s.keys = nil
	return nil
}

// view is an immutable copy of the store's contents. Callers hold s.mu.
func (s *Store) view() *view {
	v := &view{
		data: make(map[string][]byte, len(s.data)),
		keys: append([]string(nil), s.keys...),
	}
	for k, val := range s.data {
		v.data[k] = val
	}
	return v
}

// view is a point-in-time copy shared by snapshots and iterators. Its byte
// slices are shared with the store, which is safe because [Store.set] replaces
// values rather than mutating them in place.
type view struct {
	data map[string][]byte
	keys []string
}

func lookup(data map[string][]byte, key []byte, op string) ([]byte, error) {
	v, ok := data[string(key)]
	if !ok {
		return nil, errs.E(errs.NotFound, op, errors.New("key not found"))
	}
	// Non-nil even when empty: an empty value is present, and a caller that
	// checks for nil must not conclude otherwise.
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

// --- snapshot ---------------------------------------------------------------

type snapshot struct {
	view *view
}

var _ storage.Snapshot = (*snapshot)(nil)

func (s *snapshot) Get(_ context.Context, key []byte) ([]byte, error) {
	if s.view == nil {
		return nil, errs.E(errs.Unavailable, "memkv.Snapshot.Get", errClosed)
	}
	return lookup(s.view.data, key, "memkv.Snapshot.Get")
}

func (s *snapshot) NewIterator(lower, upper []byte) storage.Iterator {
	if s.view == nil {
		return failedIter(errs.E(errs.Unavailable, "memkv.Snapshot.NewIterator", errClosed))
	}
	return newIter(s.view, lower, upper)
}

func (s *snapshot) Close() error {
	s.view = nil
	return nil
}

// --- batch ------------------------------------------------------------------

type op struct {
	key    string
	value  []byte
	delete bool
}

type batch struct {
	store      *Store
	ops        []op
	conditions []condition
}

var _ storage.Batch = (*batch)(nil)

func (b *batch) Set(key, value []byte) {
	b.ops = append(b.ops, op{key: string(key), value: append([]byte(nil), value...)})
}

func (b *batch) Delete(key []byte) {
	b.ops = append(b.ops, op{key: string(key), delete: true})
}

// Get reads through the batch. Staged operations win, latest first, so a
// staged delete shadows a value that is still in the store.
func (b *batch) Get(key []byte) ([]byte, error) {
	k := string(key)
	for i := len(b.ops) - 1; i >= 0; i-- {
		if b.ops[i].key != k {
			continue
		}
		if b.ops[i].delete {
			return nil, errs.E(errs.NotFound, "memkv.Batch.Get", errors.New("key deleted in this batch"))
		}
		out := make([]byte, len(b.ops[i].value))
		copy(out, b.ops[i].value)
		return out, nil
	}
	return b.store.Get(context.Background(), key)
}

func (b *batch) Len() int { return len(b.ops) }

// Commit applies every staged operation under one exclusive lock, which is
// what makes the batch atomic: no reader can be scheduled between two of them.
func (b *batch) Commit(_ context.Context, _ bool) error {
	b.store.mu.Lock()
	defer b.store.mu.Unlock()
	if b.store.closed {
		return errs.E(errs.Unavailable, "memkv.Batch.Commit", errClosed)
	}
	for _, c := range b.conditions {
		v, exists := b.store.data[string(c.key)]
		if exists != c.exists || (exists && !bytes.Equal(v, c.value)) {
			return errs.E(errs.Conflict, "memkv.Batch.Commit", errors.New("conditional write conflict"))
		}
	}
	for _, o := range b.ops {
		if o.delete {
			b.store.del([]byte(o.key))
		} else {
			b.store.set([]byte(o.key), o.value)
		}
	}
	b.ops = nil
	return nil
}

func (b *batch) Close() error {
	b.ops = nil
	return nil
}

// --- iterator ---------------------------------------------------------------

// iter walks a half-open range over a point-in-time view.
//
// It reuses — and actively poisons — the buffers behind Key and Value. Nothing
// about a map and a sorted slice requires that; it is done because Pebble's
// iterator hands back a window into a block buffer it will overwrite, and a
// caller who retains one without copying must fail here, in a test, rather
// than read a plausible wrong value in production.
type iter struct {
	view   *view
	lower  []byte
	upper  []byte
	lo, hi int // the view.keys slice [lo, hi) that falls inside the range
	pos    int
	valid  bool
	keyBuf []byte
	valBuf []byte
	closed bool
}

var _ storage.Iterator = (*iter)(nil)

func newIter(v *view, lower, upper []byte) *iter {
	it := &iter{view: v, lower: lower, upper: upper}
	it.lo = 0
	if lower != nil {
		it.lo = sort.SearchStrings(v.keys, string(lower))
	}
	it.hi = len(v.keys)
	if upper != nil {
		it.hi = sort.SearchStrings(v.keys, string(upper))
	}
	if it.hi < it.lo {
		it.hi = it.lo
	}
	it.pos = it.lo - 1
	return it
}

func (it *iter) First() bool { return it.settle(it.lo) }

func (it *iter) Last() bool { return it.settle(it.hi - 1) }

func (it *iter) SeekGE(key []byte) bool {
	i := sort.SearchStrings(it.view.keys, string(key))
	if i < it.lo {
		i = it.lo
	}
	return it.settle(i)
}

func (it *iter) Next() bool {
	if !it.valid {
		return false
	}
	return it.settle(it.pos + 1)
}

func (it *iter) Prev() bool {
	if !it.valid {
		return false
	}
	return it.settle(it.pos - 1)
}

// settle positions at index i, invalidating the previously exposed buffers
// whether or not the new position is valid.
func (it *iter) settle(i int) bool {
	if it.closed || i < it.lo || i >= it.hi {
		it.stage(nil, nil)
		it.valid = false
		if i >= it.hi {
			it.pos = it.hi
		} else {
			it.pos = it.lo - 1
		}
		return false
	}
	it.pos = i
	k := it.view.keys[i]
	it.stage([]byte(k), it.view.data[k])
	it.valid = true
	return true
}

func (it *iter) stage(key, value []byte) {
	it.keyBuf = refill(it.keyBuf, key)
	it.valBuf = refill(it.valBuf, value)
}

// refill overwrites everything the caller may still be holding, then copies
// src in. Growing allocates a new array; the old one is poisoned first, so a
// retained slice is wrong either way.
func refill(buf, src []byte) []byte {
	full := buf[:cap(buf)]
	for i := range full {
		full[i] = 0xFF
	}
	if cap(full) < len(src) {
		full = make([]byte, len(src))
	}
	copy(full[:len(src)], src)
	return full[:len(src)]
}

func (it *iter) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.keyBuf
}

func (it *iter) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.valBuf
}

// Error is always nil: an in-memory walk over a copied slice has nothing that
// can fail. The method exists because the contract has it, and because an
// implementation that can fail must not be the only one callers test against.
func (it *iter) Error() error { return nil }

func (it *iter) Close() error {
	it.stage(nil, nil)
	it.closed = true
	it.valid = false
	it.view = nil
	return nil
}

// failed is the iterator returned when one could not be created.
//
// NewIterator has no error return — an iterator reports failure through
// Error() — so a construction failure has to be carried by something that
// satisfies the interface and yields nothing. Returning a working iterator
// over an empty view instead would make "this snapshot is closed"
// indistinguishable from "this range is empty".
type failed struct{ err error }

var _ storage.Iterator = (*failed)(nil)

func failedIter(err error) storage.Iterator { return &failed{err: err} }

func (f *failed) First() bool        { return false }
func (f *failed) Last() bool         { return false }
func (f *failed) SeekGE([]byte) bool { return false }
func (f *failed) Next() bool         { return false }
func (f *failed) Prev() bool         { return false }
func (f *failed) Key() []byte        { return nil }
func (f *failed) Value() []byte      { return nil }
func (f *failed) Error() error       { return f.err }
func (f *failed) Close() error       { return nil }

type condition struct {
	key, value []byte
	exists     bool
}

func (b *batch) Expect(key, value []byte, exists bool) {
	for _, o := range b.ops {
		if o.key == string(key) {
			return
		}
	}
	for _, c := range b.conditions {
		if bytes.Equal(c.key, key) {
			return
		}
	}
	b.conditions = append(b.conditions, condition{bytes.Clone(key), bytes.Clone(value), exists})
}
