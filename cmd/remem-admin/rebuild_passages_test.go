package main

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/embedding/onnx"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// longMemoryWithPassages adds a multi-passage memory to a seeded directory,
// staged the way the server stages one, and returns its id.
func longMemoryWithPassages(t *testing.T, dir string) id.ID {
	t.Helper()
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	ctx := tenant.NewContext(context.Background(), "acme")
	content := strings.Repeat("the harbour authority recorded the tide tables ", 14)
	docs, err := embeddingtest.New().EmbedDocuments(ctx, []string{content})
	if err != nil {
		t.Fatal(err)
	}
	rec := &record.Record{
		ID: id.New(), Tenant: "acme", Type: record.TypeMemory, Content: content,
		Fields:    record.Fields{Policy: "short_term", Health: 100, Importance: 0.5},
		Vectors:   map[string]*record.Vector{record.VectorContent: {ModelID: embedding.Model, Dim: embedding.Dim, Values: docs[0].Vector}},
		Passages:  record.PassageSetOf(embedding.Model, docs[0]),
		CreatedAt: time.Unix(1, 0), UpdatedAt: time.Unix(1, 0),
	}
	repo := record.NewRepo(kv, record.WithIndexer(record.NewPassageIndexer()))
	if err := txn.Do(ctx, kv, func(tx txn.Tx) error { return repo.Put(ctx, tx, rec) }); err != nil {
		t.Fatal(err)
	}
	return rec.ID
}

// The passage rebuild restores what was there, through the same function the
// passages.rebuild job runs.
func TestRebuildRestoresThePassageIndex(t *testing.T) {
	dir, _ := seedCorpus(t)
	longMemoryWithPassages(t, dir)
	before := rowsIn(t, dir, keys.SpacePassage)
	if before < 2 {
		t.Fatalf("%d passage rows seeded; the fixture needs a multi-passage memory", before)
	}
	wipeSpace(t, dir, keys.SpacePassage)

	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	n, err := rebuildOne(context.Background(), kv, "passages", "acme", "cosine", embeddingtest.New())
	_ = kv.Close()
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("restaged %d memories, want all 5", n)
	}
	if got := rowsIn(t, dir, keys.SpacePassage); got != before {
		t.Fatalf("%d passage rows after the rebuild, want %d", got, before)
	}
}

// A build without the model cannot recompute passages, and says so by name
// before it opens the directory — for --index passages and for --index all,
// which includes it.
func TestAPassageRebuildWithoutTheModelIsRefusedByName(t *testing.T) {
	if onnx.Available {
		t.Skip("this build has the model")
	}
	dir, _ := seedCorpus(t)
	for _, which := range []string{"passages", "all"} {
		_, err := run([]string{"rebuild", "--data-dir", dir, "--index", which})
		if err == nil || !strings.Contains(err.Error(), "built without the model") ||
			!strings.Contains(err.Error(), "passage") {
			t.Fatalf("--index %s on a build without the model: %v", which, err)
		}
	}
}

// An external-mode directory has no passages and needs no model: the rebuild
// clears any stray rows rather than refusing.
func TestAnExternalDirectorysPassageRebuildNeedsNoModel(t *testing.T) {
	dir, _ := seedCorpus(t)
	longMemoryWithPassages(t, dir)
	if _, err := run([]string{"rebuild", "--data-dir", dir, "--index", "passages",
		"--embedding-provider", "external"}); err != nil {
		t.Fatalf("an external-mode passage rebuild: %v", err)
	}
	if got := rowsIn(t, dir, keys.SpacePassage); got != 0 {
		t.Fatalf("%d passage rows remain after an external-mode rebuild", got)
	}
}

// A plain build restoring a Go snapshot of this model has no model to compute
// passages with: the memories arrive with their canonical vectors, and the
// tenant is marked for the server to finish before it serves.
func TestAnImportWithoutTheModelLeavesThePassagesOwed(t *testing.T) {
	if onnx.Available {
		t.Skip("this build has the model and computes the passages during the import")
	}
	src, _ := seedCorpus(t)
	longMemoryWithPassages(t, src)
	out := t.TempDir() + "/corpus.rsnap"
	dst := t.TempDir() + "/restored"
	if _, err := run([]string{"export", "--data-dir", src, "--out", out}); err != nil {
		t.Fatalf("export: %v", err)
	}
	if _, err := run([]string{"import", "--data-dir", dst, "--in", out}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if n := rowsIn(t, dst, keys.SpacePassage); n != 0 {
		t.Fatalf("%d passage rows written by a build without the model", n)
	}
	kv, err := pebble.Open(dst, pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	pending, err := record.PassagesPending(context.Background(), kv)
	if err != nil || len(pending) != 1 || pending[0] != "acme" {
		t.Fatalf("pending tenants %v (%v), want [acme]", pending, err)
	}
}

// The offline rebuild is how a stopped server pays what an import owed: it
// computes the passages and clears the mark. And --index all includes it.
func TestThePassageRebuildClearsThePendingMark(t *testing.T) {
	dir, _ := seedCorpus(t)
	longMemoryWithPassages(t, dir)
	wipeSpace(t, dir, keys.SpacePassage)
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	if err := txn.Do(context.Background(), kv, func(tx txn.Tx) error {
		record.MarkPassagesPending(tx, "acme")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := rebuildOne(context.Background(), kv, "passages", "acme", "cosine", embeddingtest.New()); err != nil {
		t.Fatal(err)
	}
	if pending, err := record.PassagesPending(context.Background(), kv); err != nil || len(pending) != 0 {
		t.Fatalf("still pending after the rebuild: %v %v", pending, err)
	}
	if !slices.Contains(rebuildIndexes, "passages") {
		t.Fatal("--index all does not include passages")
	}
}
