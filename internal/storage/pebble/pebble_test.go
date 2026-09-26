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

func open(t *testing.T) storage.KV {
	t.Helper()
	kv, err := pebble.Open(t.TempDir(), pebble.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	return kv
}
