package storagetest

import (
	"github.com/remem-org/remem-go/internal/storage"
	"testing"
)

// iteratorClose checks that repeated release of one handle cannot release a
// different live iterator, even when the engine pools underlying allocations.
func iteratorClose(t *testing.T, open func(*testing.T) storage.KV) {
	kv := open(t)
	must(t, kv.Set(ctx(), []byte("a"), []byte("one")))
	must(t, kv.Set(ctx(), []byte("b"), []byte("two")))
	old := kv.NewIterator(nil, nil)
	if !old.First() {
		t.Fatal("old iterator did not position")
	}
	must(t, old.Close())
	live := kv.NewIterator(nil, nil)
	defer func() { _ = live.Close() }()
	if !live.First() {
		t.Fatal("live iterator did not position")
	}
	must(t, old.Close())
	if string(live.Key()) != "a" || string(live.Value()) != "one" {
		t.Fatalf("closing old iterator changed live position: %q=%q", live.Key(), live.Value())
	}
	if !live.Next() || string(live.Key()) != "b" {
		t.Fatal("closing old iterator stopped live iteration")
	}
	must(t, live.Error())
}
