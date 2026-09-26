package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/indexes"
)

// A mistyped --data-dir must be refused, not turned into a new empty database
// that reports success. This is the Phase 6 finding, and `vector rebuild` is
// the more dangerous case: unlike `graph verify` it genuinely writes, so it
// cannot simply open read-only and has to check first.
func TestVectorRebuildRefusesADirectoryWithNoDatabase(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "typo")
	if _, err := run([]string{"vector", "rebuild", "--data-dir", missing}); err == nil {
		t.Fatal("a path holding no database was accepted")
	}
	if _, err := pebble.Open(missing, pebble.Options{ReadOnly: true}); err == nil {
		t.Fatal("the refused path now holds a database: the command created one on its way out")
	}
}

func TestVectorRebuildNeedsADataDir(t *testing.T) {
	if _, err := run([]string{"vector", "rebuild"}); err == nil {
		t.Fatal("vector rebuild ran without --data-dir")
	}
}

// The repair an operator is told to run, end to end: destroy every node record
// and rebuild them from the canonical vectors.
func TestVectorRebuildRestoresTheIndex(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	ctx := context.Background()

	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	idx, err := indexes.Open(kv, indexes.Config{Kind: "hnsw", Metric: "cosine"})
	if err != nil {
		t.Fatal(err)
	}
	want := id.New()
	q := []float32{1, 0, 0, 0}
	if err := idx.Insert(ctx, "acme", want, q); err != nil {
		t.Fatal(err)
	}
	for range 50 {
		if err := idx.Insert(ctx, "acme", id.New(), []float32{0, 1, 0, 0}); err != nil {
			t.Fatal(err)
		}
	}

	// Destroy the derived index entirely, leaving the canonical vectors.
	lower, upper := keys.SpaceRange("acme", tenant.DefaultNamespace, keys.SpaceVectorIndex)
	var doomed [][]byte
	it := kv.NewIterator(lower, upper)
	for ok := it.First(); ok; ok = it.Next() {
		doomed = append(doomed, append([]byte(nil), it.Key()...))
	}
	_ = it.Close()
	if len(doomed) == 0 {
		t.Fatal("no node records were written, so destroying them proves nothing")
	}
	for _, k := range doomed {
		if err := kv.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	if code, err := run([]string{"vector", "rebuild", "--data-dir", dir, "--tenant", "acme"}); err != nil || code != 0 {
		t.Fatalf("vector rebuild: code %d, %v", code, err)
	}

	kv, err = pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	idx, err = indexes.Open(kv, indexes.Config{Kind: "hnsw", Metric: "cosine"})
	if err != nil {
		t.Fatal(err)
	}
	s, err := idx.Stats(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if s.Vectors != 51 {
		t.Fatalf("the rebuilt index holds %d vectors, want 51", s.Vectors)
	}
	hits, err := idx.Search(ctx, "acme", q, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].ID != want {
		t.Fatalf("the rebuilt index does not find the exact match: %v", hits)
	}

	// And the canonical vectors are untouched: a rebuild reads them, it does
	// not rewrite them into something else.
	count := 0
	if err := vector.NewStore(kv).Scan(ctx, "acme", func(id.ID, []float32) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 51 {
		t.Fatalf("the store holds %d canonical vectors after a rebuild, want 51", count)
	}
}
