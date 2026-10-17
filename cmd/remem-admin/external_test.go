package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

// seedExternalCorpus writes memories whose vectors a caller supplied: another
// model's id, four wide, and not unit length.
func seedExternalCorpus(t *testing.T, model string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	ctx := tenant.NewContext(context.Background(), "acme")
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	repo := record.NewRepo(kv, record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(text.New()))
	if _, err := tenant.Ensure(ctx, tenantkv.New(kv, clock.System()), "acme"); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		rec := &record.Record{
			ID: id.New(), Tenant: "acme", Namespace: tenant.DefaultNamespace,
			Type: record.TypeMemory, Content: "vdbb " + string(rune('a'+i)),
			Fields: record.Fields{}.WithDefaults(),
			Vectors: map[string]*vector.Vector{record.VectorContent: {
				ModelID: model, Dim: 4, Values: []float32{float32(i + 1), 2, 0, 1},
			}},
			CreatedAt: time.Unix(1700000000, 0).UTC(), UpdatedAt: time.Unix(1700000000, 0).UTC(),
		}
		if err := txn.Do(ctx, kv, func(tx txn.Tx) error { return repo.Put(ctx, tx, rec) }); err != nil {
			t.Fatal(err)
		}
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func externalImport(dst, in, model, dim string) []string {
	return []string{"import", "--data-dir", dst, "--in", in,
		"--embedding-provider", "external", "--external-model-id", model, "--dimension", dim}
}

func exportTo(t *testing.T, src string) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "corpus.rsnap")
	if _, err := run([]string{"export", "--data-dir", src, "--out", out}); err != nil {
		t.Fatalf("export: %v", err)
	}
	return out
}

func wantNoDatabase(t *testing.T, dst string) {
	t.Helper()
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("the refused import created %s", dst)
	}
}

func TestAnExternalImportOfTheSameModelRoundTrips(t *testing.T) {
	out := exportTo(t, seedExternalCorpus(t, "cohere-embed-english-v3.0"))
	dst := filepath.Join(t.TempDir(), "restored")
	if _, err := run(externalImport(dst, out, "cohere-embed-english-v3.0", "4")); err != nil {
		t.Fatalf("import: %v", err)
	}
	if code, err := run([]string{"verify", "--data-dir", dst, "--in", out}); err != nil || code != 0 {
		t.Fatalf("verify = %d, %v", code, err)
	}
}

func TestAnExternalImportOfAnotherModelLeavesNoDatabase(t *testing.T) {
	src, _ := seedCorpus(t) // embedding.Model, embedding.Dim wide
	out := exportTo(t, src)
	dst := filepath.Join(t.TempDir(), "restored")
	_, err := run(externalImport(dst, out, "cohere-embed-english-v3.0", "384"))
	if err == nil || !strings.Contains(err.Error(), embedding.Model) || !strings.Contains(err.Error(), "cohere-embed-english-v3.0") {
		t.Fatalf("want a refusal naming both models, got %v", err)
	}
	wantNoDatabase(t, dst)
}

func TestAnExternalImportOfAForeignSnapshotLeavesNoDatabase(t *testing.T) {
	src, _ := seedCorpus(t)
	rustLike := rewriteSourceImpl(t, exportTo(t, src), "rust")
	dst := filepath.Join(t.TempDir(), "restored")
	_, err := run(externalImport(dst, rustLike, "cohere-embed-english-v3.0", "384"))
	if err == nil || !strings.Contains(err.Error(), "external") {
		t.Fatalf("want a refusal naming external mode, got %v", err)
	}
	wantNoDatabase(t, dst)
}

func TestAnExternalImportNeedsItsModelNamed(t *testing.T) {
	out := exportTo(t, seedExternalCorpus(t, "cohere-embed-english-v3.0"))
	dst := filepath.Join(t.TempDir(), "restored")
	_, err := run([]string{"import", "--data-dir", dst, "--in", out, "--embedding-provider", "external", "--dimension", "4"})
	if err == nil || !strings.Contains(err.Error(), "external-model-id") {
		t.Fatalf("want a refusal naming --external-model-id, got %v", err)
	}
	wantNoDatabase(t, dst)
}
