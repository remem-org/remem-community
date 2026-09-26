package memkv_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/storage/storagetest"
)

func TestContract(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.KV {
		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		return kv
	})
}

// TestIteratorBuffersArePoisoned holds memkv to a promise stronger than the
// storage contract makes.
//
// The contract permits an implementation to overwrite Key and Value on the
// next positioning call; it does not require it, and the Pebble adapter does
// not fully do so. memkv does, on purpose. Every layer above storage is unit
// tested against memkv, so this is where a caller that retained a key without
// copying has to fail — the alternative is that it passes here and reads a
// plausible wrong value in production, which is the worst failure mode
// available because it neither crashes nor looks wrong.
func TestIteratorBuffersArePoisoned(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	ctx := context.Background()
	for _, k := range []string{"aaaa", "bbbb"} {
		if err := kv.Set(ctx, []byte(k), []byte("value-of-"+k)); err != nil {
			t.Fatal(err)
		}
	}

	it := kv.NewIterator(nil, nil)
	defer it.Close()
	if !it.First() {
		t.Fatal("First found nothing")
	}
	key, val := it.Key(), it.Value() // deliberately not copied
	if string(key) != "aaaa" || string(val) != "value-of-aaaa" {
		t.Fatalf("First landed on %q=%q", key, val)
	}

	if !it.Next() {
		t.Fatal("Next found nothing")
	}
	if string(key) == "aaaa" {
		t.Error("the key survived Next: a caller that forgot to copy would never find out")
	}
	if string(val) == "value-of-aaaa" {
		t.Error("the value survived Next: a caller that forgot to copy would never find out")
	}

	// Close must poison too: an iterator held past its Close is the same bug.
	if !it.First() {
		t.Fatal("First found nothing on rewind")
	}
	key = it.Key()
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if string(key) == "aaaa" {
		t.Error("the key survived Close")
	}
}
