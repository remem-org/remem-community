package pebble_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/storage/storagetest"
)

func TestContract(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.KV {
		return open(t)
	})
}

// Spec §58 forbids exposing a Pebble error through a public API. The boundary
// guard in internal/arch cannot catch this: it sees imports, and a leaked
// error is a *value*, not an import. So it is checked here.
func TestPebbleErrorsAreNeverExposed(t *testing.T) {
	kv := open(t)
	_, err := kv.Get(context.Background(), []byte("absent"))
	if err == nil {
		t.Fatal("want an error for an absent key")
	}
	if strings.Contains(fmt.Sprintf("%T", err), "pebble") {
		t.Fatalf("a Pebble error reached the caller: %T", err)
	}
	if !errs.Is(err, errs.NotFound) {
		t.Fatalf("want NotFound, got %v (%T)", err, err)
	}
	// The op label may legitimately say "pebble.Get" — that names the Remem
	// operation that failed, and errs documents exactly that form. What must
	// not appear is Pebble's own message, which is prefixed "pebble:" and goes
	// on to name sstables, manifests and sequence numbers the caller cannot
	// act on.
	if strings.Contains(err.Error(), "pebble:") {
		t.Fatalf("a Pebble error message reached the caller: %v", err)
	}
}

// The two tuning options are a Pebble concept each, so what has to be shown is
// that setting them still produces a store that reads back what it wrote: a
// bloom filter that answered wrongly would turn a present key into a miss, and
// a memtable size Pebble refused would turn the open into an error.
func TestABloomFilterAndALargerMemtableStillRoundTrip(t *testing.T) {
	ctx := context.Background()
	kv, err := pebble.Open(t.TempDir(), pebble.Options{
		MemTableSizeBytes: 64 << 20,
		BloomBitsPerKey:   10,
	})
	if err != nil {
		t.Fatalf("open with a filter and a larger memtable: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })

	if err := kv.Set(ctx, []byte("present"), []byte("value")); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := kv.Get(ctx, []byte("present"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("get = %q, want %q", got, "value")
	}
	// A filter may say "maybe"; it may never say "no" about a key that is
	// there, and it must not turn an absent key into anything but NotFound.
	if _, err := kv.Get(ctx, []byte("absent")); !errs.Is(err, errs.NotFound) {
		t.Fatalf("absent key: want NotFound, got %v", err)
	}
}

func open(t *testing.T) storage.KV {
	t.Helper()
	kv, err := pebble.Open(t.TempDir(), pebble.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	return kv
}
