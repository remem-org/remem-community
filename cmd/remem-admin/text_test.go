package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
)

// A mistyped --data-dir must be refused, not turned into a new empty database
// that reports a clean corpus. This is the Phase 6 finding, and it applies here
// for the same reason it applies to `vector rebuild`: the command writes, so it
// cannot simply open read-only and has to check first.
func TestTextRebuildRefusesADirectoryWithNoDatabase(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "typo")
	if _, err := run([]string{"text", "rebuild", "--data-dir", missing}); err == nil {
		t.Fatal("a path holding no database was accepted")
	}
	if _, err := pebble.Open(missing, pebble.Options{ReadOnly: true}); err == nil {
		t.Fatal("the refused path now holds a database: the command created one on its way out")
	}
}

func TestTextRebuildNeedsADataDir(t *testing.T) {
	if _, err := run([]string{"text", "rebuild"}); err == nil {
		t.Fatal("text rebuild ran without --data-dir")
	}
}

// The repair an operator is told to run, end to end: destroy the postings,
// leave the memories, and rebuild the index from the record bodies.
func TestTextRebuildRestoresTheIndexFromTheRecords(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	ctx := tenant.NewContext(context.Background(), "acme")
	ns := tenant.DefaultNamespace

	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	index := text.New()
	repo := record.NewRepo(kv, record.WithIndexer(index))

	want := writeMemory(t, ctx, kv, repo, "the quarterly revenue report for Berlin", "finance")
	for range 20 {
		writeMemory(t, ctx, kv, repo, "an ordinary note about ordinary matters", "batch")
	}

	found := searchOne(t, ctx, index, kv, "quarterly revenue")
	if found != want {
		t.Fatalf("the corpus does not answer before the damage: got %s, want %s", found, want)
	}

	// Wipe every posting, leaving the memories untouched — which is what a
	// half-finished rebuild, or an upgrade that changed the tokeniser, leaves
	// behind.
	wipeTextSpace(t, kv, "acme", ns)
	if got := searchOne(t, ctx, index, kv, "quarterly revenue"); !got.IsZero() {
		t.Fatal("the damaged index still answered")
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	if code, err := run([]string{"text", "rebuild", "--data-dir", dir, "--tenant", "acme"}); err != nil || code != 0 {
		t.Fatalf("rebuild: code %d, %v", code, err)
	}

	kv, err = pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()

	if got := searchOne(t, ctx, index, kv, "quarterly revenue"); got != want {
		t.Fatalf("the rebuilt index returned %s, want %s", got, want)
	}
	// And the tag, which lives in the same index and is what Rust needed a
	// second structure for.
	hits, err := index.Search(ctx, kv, "acme", ns, text.SearchReq{
		Required: []string{text.TagTerm("finance")}, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].ID != want {
		t.Fatalf("the rebuilt tag filter returned %d memories", len(hits))
	}

	health, err := index.Health(ctx, kv, "acme", ns)
	if err != nil {
		t.Fatal(err)
	}
	if health.Degraded {
		t.Fatalf("a completed rebuild left the index reporting itself incomplete: %q", health.Reason)
	}
	stats, err := text.ReadStats(ctx, kv, "acme", ns)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Documents != 21 {
		t.Fatalf("the rebuilt corpus reports %d memories, want 21", stats.Documents)
	}
}

func writeMemory(t *testing.T, ctx context.Context, kv storage.KV, repo record.Repo,
	content, tag string) id.ID {
	t.Helper()
	rec := &record.Record{
		ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
		Type: record.TypeMemory, Content: content,
		Fields:    record.Fields{Tags: []string{tag}}.WithDefaults(),
		CreatedAt: time.Unix(1, 0).UTC(),
	}
	err := txn.Do(ctx, kv, func(tx txn.Tx) error { return repo.Put(ctx, tx, rec) })
	if err != nil {
		t.Fatalf("writing a memory: %v", err)
	}
	return rec.ID
}

func searchOne(t *testing.T, ctx context.Context, index *text.Index, r text.Reader, q string) id.ID {
	t.Helper()
	hits, err := index.Search(ctx, r, "acme", tenant.DefaultNamespace,
		text.SearchReq{Optional: text.QueryTerms(q), Limit: 1})
	if err != nil {
		t.Fatalf("searching: %v", err)
	}
	if len(hits) == 0 {
		return id.Zero
	}
	return hits[0].ID
}

// wipeTextSpace destroys every derived row of a tenant's index, leaving its
// memories untouched. It is what a half-finished rebuild leaves behind, and it
// is the only kind of damage this index can take — postings are written in the
// record's own transaction, so they cannot drift on their own.
func wipeTextSpace(t *testing.T, kv storage.KV, tn tenant.ID, ns tenant.Namespace) {
	t.Helper()
	lower, upper := keys.SpaceRange(tn, ns, keys.SpaceText)
	it := kv.NewIterator(lower, upper)
	var doomed [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		doomed = append(doomed, bytes.Clone(it.Key()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterating: %v", err)
	}
	_ = it.Close()

	for _, key := range doomed {
		if err := kv.Delete(context.Background(), key); err != nil {
			t.Fatalf("deleting: %v", err)
		}
	}
	if len(doomed) == 0 {
		t.Fatal("there was nothing to destroy; the test damaged nothing")
	}
}
