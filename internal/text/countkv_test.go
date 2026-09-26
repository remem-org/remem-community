package text_test

import (
	"context"
	"sync/atomic"

	"github.com/remem-org/remem-go/internal/storage"
)

// countingKV records how many stored entries a read touched.
//
// It is the instrument behind TestKeywordSearchCostIsProportionalToMatches, and
// the reason that test needs an instrument at all: "keyword search is fast" is
// unfalsifiable, where "answering a five-match query over a hundred thousand
// records touched fewer than a hundred entries" is a number that fails when
// somebody reintroduces a scan.
type countingKV struct {
	storage.KV
	entries atomic.Int64 // iterator positions that landed on a row
	gets    atomic.Int64
}

func counting(kv storage.KV) *countingKV { return &countingKV{KV: kv} }

func (c *countingKV) reset() {
	c.entries.Store(0)
	c.gets.Store(0)
}

// touched is every stored entry the read looked at, however it reached them.
func (c *countingKV) touched() int64 { return c.entries.Load() + c.gets.Load() }

func (c *countingKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	c.gets.Add(1)
	return c.KV.Get(ctx, key)
}

func (c *countingKV) NewIterator(lower, upper []byte) storage.Iterator {
	return &countingIterator{Iterator: c.KV.NewIterator(lower, upper), on: &c.entries}
}

type countingIterator struct {
	storage.Iterator
	on *atomic.Int64
}

func (i *countingIterator) count(ok bool) bool {
	if ok {
		i.on.Add(1)
	}
	return ok
}

func (i *countingIterator) First() bool          { return i.count(i.Iterator.First()) }
func (i *countingIterator) Last() bool           { return i.count(i.Iterator.Last()) }
func (i *countingIterator) Next() bool           { return i.count(i.Iterator.Next()) }
func (i *countingIterator) Prev() bool           { return i.count(i.Iterator.Prev()) }
func (i *countingIterator) SeekGE(k []byte) bool { return i.count(i.Iterator.SeekGE(k)) }
