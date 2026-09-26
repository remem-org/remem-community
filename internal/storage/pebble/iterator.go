package pebble

import (
	pebbledb "github.com/cockroachdb/pebble/v2"
	"github.com/remem-org/remem-go/internal/storage"
)

// iterator adapts Pebble's iterator. Key and Value are passed through without
// copying, which is the contract: both are valid only until the next
// positioning call, and copying here would impose an allocation per entry on
// every scan in Remem to protect callers the contract already warns.
type iterator struct {
	it     *pebbledb.Iterator
	closed bool
}

var _ storage.Iterator = (*iterator)(nil)

func (i *iterator) First() bool            { return i.it.First() }
func (i *iterator) Last() bool             { return i.it.Last() }
func (i *iterator) SeekGE(key []byte) bool { return i.it.SeekGE(key) }
func (i *iterator) Next() bool             { return i.it.Next() }
func (i *iterator) Prev() bool             { return i.it.Prev() }
func (i *iterator) Key() []byte            { return i.it.Key() }
func (i *iterator) Value() []byte          { return i.it.Value() }
func (i *iterator) Error() error           { return translate("pebble.Iterator", i.it.Error()) }
func (i *iterator) Close() error {
	if i.closed {
		return nil
	}
	i.closed = true
	return translate("pebble.Iterator.Close", i.it.Close())
}

// failed is the iterator returned when one could not be created.
//
// NewIterator has no error return — an iterator reports failure through
// Error() — so a construction failure has to be carried by something that
// satisfies the interface and yields nothing.
type failed struct{ err error }

var _ storage.Iterator = (*failed)(nil)

func failedIter(err error) storage.Iterator { return &failed{err: err} }

func (f *failed) First() bool            { return false }
func (f *failed) Last() bool             { return false }
func (f *failed) SeekGE(key []byte) bool { return false }
func (f *failed) Next() bool             { return false }
func (f *failed) Prev() bool             { return false }
func (f *failed) Key() []byte            { return nil }
func (f *failed) Value() []byte          { return nil }
func (f *failed) Error() error           { return f.err }
func (f *failed) Close() error           { return nil }
