package main

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/version"
)

// Forgetting to run the migration before rebuilding passages leaves these
// canonical vectors in the old space and never advances passage_index.
func offlineLegacy(t *testing.T) (string, storage.KV, tenant.Directory) {
	t.Helper()
	dir, ids := seedCorpus(t)
	longMemoryWithPassages(t, dir)
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	f := schema.CurrentFormat(version.Current())
	delete(f.Subsystems, "passage_index")
	if err := schema.WriteFormat(context.Background(), kv, f); err != nil {
		t.Fatal(err)
	}
	old := make([]float32, 384)
	old[0] = 1
	for _, rid := range ids {
		if err := txn.Do(context.Background(), kv, func(tx txn.Tx) error {
			return vector.NewStore(kv).Stage(tx, "acme", tenant.DefaultNamespace, rid, &vector.Vector{ModelID: "all-MiniLM-L6-v2", Dim: 384, Values: old})
		}); err != nil {
			t.Fatal(err)
		}
	}
	return dir, kv, tenantkv.New(kv, clock.System())
}

func TestOfflinePassageUpgradeRewritesCanonicalVectorsAndBacksUp(t *testing.T) {
	dir, kv, tenants := offlineLegacy(t)
	if err := upgradePassageFormat(context.Background(), kv, tenants, dir, "", embeddingtest.New()); err != nil {
		t.Fatal(err)
	}
	f, err := schema.ReadFormat(context.Background(), kv)
	if err != nil || f.Subsystems["passage_index"].Current != version.PassageIndex {
		t.Fatalf("format after offline upgrade: %v, %v", f, err)
	}
	lo, hi := keys.SpaceRange("acme", tenant.DefaultNamespace, keys.SpaceVector)
	it := kv.NewIterator(lo, hi)
	defer func() { _ = it.Close() }()
	for ok := it.First(); ok; ok = it.Next() {
		v, err := vector.Decode(it.Value())
		if err != nil || v.ModelID != embedding.Model || v.Dim != embedding.Dim {
			t.Fatalf("canonical vector: %+v, %v", v, err)
		}
	}
	backups, err := filepath.Glob(filepath.Join(dir, schema.BackupsDir, "pre-*"))
	if err != nil || len(backups) != 1 {
		t.Fatalf("backups: %v, %v", backups, err)
	}
	backup, err := pebble.Open(backups[0], pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = backup.Close() }()
	bf, err := schema.ReadFormat(context.Background(), backup)
	if err != nil || bf.Subsystems["passage_index"].Current != 0 {
		t.Fatalf("backup format: %v, %v", bf, err)
	}
}

type interruptedOfflineEmbedder struct {
	*embeddingtest.Fake
	calls int
	fail  bool
}

func (e *interruptedOfflineEmbedder) EmbedDocuments(ctx context.Context, texts []string) ([]embedding.Document, error) {
	e.calls++
	if e.fail && e.calls == 2 {
		return nil, errors.New("interrupted offline upgrade")
	}
	return e.Fake.EmbedDocuments(ctx, texts)
}
func TestOfflinePassageUpgradeResumesWithoutReembeddingCommittedPages(t *testing.T) {
	dir, kv, _ := offlineLegacy(t)
	// Add enough records to commit one page before the second call fails.
	_ = kv.Close()
	for range 17 {
		longMemoryWithPassages(t, dir)
	}
	reopened, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	tenants := tenantkv.New(reopened, clock.System())
	e := &interruptedOfflineEmbedder{Fake: embeddingtest.New(), fail: true}
	err = upgradePassageFormat(context.Background(), reopened, tenants, dir, "", e)
	if err == nil || !strings.Contains(err.Error(), "interrupted offline upgrade") {
		t.Fatalf("interruption: %v", err)
	}
	state, found, err := schema.ReadState(context.Background(), reopened, schema.PassageReembedID)
	if err != nil || !found || state.Processed != 16 {
		t.Fatalf("checkpoint: %+v, %v", state, err)
	}
	e.calls = 0
	e.fail = false
	if err := upgradePassageFormat(context.Background(), reopened, tenants, dir, "", e); err != nil {
		t.Fatal(err)
	}
	if e.calls != 1 {
		t.Fatalf("resume embedded %d pages, want only remaining page", e.calls)
	}
}
func TestOfflinePassageUpgradeRefusesPartialTenantMigration(t *testing.T) {
	dir, kv, tenants := offlineLegacy(t)
	err := upgradePassageFormat(context.Background(), kv, tenants, dir, "acme", embeddingtest.New())
	if err == nil || !strings.Contains(err.Error(), "--tenant") {
		t.Fatalf("partial upgrade accepted: %v", err)
	}
	f, _ := schema.ReadFormat(context.Background(), kv)
	if f.Subsystems["passage_index"].Current != 0 {
		t.Fatal("refusal advanced the manifest")
	}
}
