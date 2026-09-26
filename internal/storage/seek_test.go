package storage_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
)

// A SeekReader answers every point read exactly as the snapshot beneath it
// would, in any order: present keys with their values, absent keys as
// NotFound, and keys outside its range too.
//
// The case that matters is an absent key lying between two present ones. An
// iterator seeks to the smallest key at or after the one asked for, so a
// reader that forgot to check the key it landed on would return its
// neighbour's value — a plausible record, silently the wrong one.
func TestASeekReaderAnswersExactlyAsItsSnapshot(t *testing.T) {
	ctx := context.Background()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	key := func(prefix string, i int) []byte { return []byte(fmt.Sprintf("%s/%04d", prefix, i)) }
	// Even keys inside the range exist; odd ones do not. Keys under "b/" and
	// "d/" lie either side of the range and exist too.
	for i := 0; i < 200; i += 2 {
		if err := kv.Set(ctx, key("c", i), []byte(fmt.Sprintf("value %d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []string{"b", "d"} {
		if err := kv.Set(ctx, key(p, 1), []byte("outside "+p)); err != nil {
			t.Fatal(err)
		}
	}

	snap := kv.NewSnapshot()
	t.Cleanup(func() { _ = snap.Close() })
	// Written after the snapshot, so neither reader may see it.
	if err := kv.Set(ctx, key("c", 1), []byte("too late")); err != nil {
		t.Fatal(err)
	}

	r := storage.NewSeekReader(snap, []byte("c/"), []byte("c0"))
	t.Cleanup(func() { _ = r.Close() })

	probes := [][]byte{key("b", 1), key("d", 1), key("c", 1), key("c", 199), key("c", 200), []byte("c/")}
	for i := 0; i < 200; i++ {
		probes = append(probes, key("c", i))
	}
	rng := rand.New(rand.NewSource(1))
	rng.Shuffle(len(probes), func(i, j int) { probes[i], probes[j] = probes[j], probes[i] })

	for _, k := range probes {
		want, wantErr := snap.Get(ctx, k)
		got, gotErr := r.Get(ctx, k)
		if errs.Is(wantErr, errs.NotFound) != errs.Is(gotErr, errs.NotFound) || (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("Get(%q): snapshot said %v, seek reader said %v", k, wantErr, gotErr)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q) = %q through the seek reader, %q through the snapshot", k, got, want)
		}
	}
}

// A value a SeekReader returns is the caller's: the next read must not change
// it, though an iterator's value is only valid until it moves.
func TestASeekReaderValueSurvivesTheNextRead(t *testing.T) {
	ctx := context.Background()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	for _, k := range []string{"k/a", "k/b"} {
		if err := kv.Set(ctx, []byte(k), []byte("value of "+k)); err != nil {
			t.Fatal(err)
		}
	}
	snap := kv.NewSnapshot()
	t.Cleanup(func() { _ = snap.Close() })
	r := storage.NewSeekReader(snap, []byte("k/"), []byte("k0"))
	t.Cleanup(func() { _ = r.Close() })

	first, err := r.Get(ctx, []byte("k/a"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get(ctx, []byte("k/b")); err != nil {
		t.Fatal(err)
	}
	if string(first) != "value of k/a" {
		t.Fatalf("the first value read %q after a second read", first)
	}
}
