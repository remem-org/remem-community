package snapshot_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
)

// Passage rows are derived — from the content, the model and the passage
// format — so a snapshot never carries them, and an import computes them or
// says it could not (openspec/changes/passage-vectors, design decision 9).

// longContent is several passages under the fake embedder.
var longContent = strings.Repeat("the harbour authority recorded the tide tables ", 12)

// withPassages gives a stored memory the passage rows the write path would
// have staged for its content under the fake embedder.
func withPassages() memoOpt {
	return func(r *record.Record) {
		docs, err := embeddingtest.New().EmbedDocuments(context.Background(), []string{r.Content})
		if err != nil {
			panic(err)
		}
		r.Passages = record.PassageSetOf(embedding.Model, docs[0])
	}
}

// fromMiniLM stamps a memory's vector as the previous model's.
func fromMiniLM() memoOpt {
	return func(r *record.Record) {
		v := make([]float32, 384)
		v[int(r.ID[0])%384] = 1
		r.Vectors[record.VectorContent] = &vector.Vector{ModelID: "all-MiniLM-L6-v2", Dim: 384, Values: v}
	}
}

func passageRows(t *testing.T, kv storage.KV, tn tenant.ID, rid id.ID) int {
	t.Helper()
	lo, hi := keys.PassagesOfRange(tn, tenant.DefaultNamespace, rid)
	it := kv.NewIterator(lo, hi)
	defer func() { _ = it.Close() }()
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	return n
}

func TestAnExportCarriesNoPassageRows(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	rid := w.store(t, "default", longContent, embeddedByThisModel(), withPassages())
	if passageRows(t, w.kv, "default", rid) < 2 {
		t.Fatal("the store holds no passage rows, so this test proves nothing about them")
	}
	raw, stats := exportWorld(t, w, snapshot.ExportOpts{})
	got := decode(t, raw)
	if stats.Vectors != 1 || len(got.vectors) != 1 {
		t.Fatalf("%d vectors exported for one memory: a passage row was exported", len(got.vectors))
	}
	if len(got.vectors[0].GetValues()) != embedding.Dim {
		t.Fatalf("the exported vector is %d wide", len(got.vectors[0].GetValues()))
	}
}

// A Rust snapshot's vectors are another model's: every one is recomputed under
// auto, and a long memory gets its passages from the same embedding.
func TestARustSnapshotIsReEmbeddedWithPassages(t *testing.T) {
	raw := rustLikeSnapshot(t, withLongRecord())
	d := newDestination(t, fakeEmbedder())
	d.importAll(t, raw, snapshot.ImportOpts{})
	if n := passageRows(t, d.kv, "default", rustRecordID(9)); n < 2 {
		t.Fatalf("the long Rust memory has %d passage rows after a re-embedding import", n)
	}
}

// A Go snapshot written under the previous model is re-embedded under auto,
// like a Rust one: the model id says it is another space.
func TestAGoSnapshotOfThePreviousModelIsReEmbeddedWithPassages(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	rid := w.store(t, "default", longContent, fromMiniLM())
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})

	d := newDestination(t, fakeEmbedder())
	rep := d.importAll(t, raw, snapshot.ImportOpts{})
	if rep.ReEmbedded != 1 || rep.VectorsVerbatim != 0 {
		t.Fatalf("re-embedded %d, kept %d: want the MiniLM vector replaced", rep.ReEmbedded, rep.VectorsVerbatim)
	}
	rec, err := d.repo.Get(tenant.NewContext(context.Background(), "default"), rid)
	must(t, err)
	if v := rec.Vectors[record.VectorContent]; v.ModelID != embedding.Model || v.Dim != embedding.Dim {
		t.Fatalf("the imported vector is %q/%d", v.ModelID, v.Dim)
	}
	if n := passageRows(t, d.kv, "default", rid); n < 2 {
		t.Fatalf("%d passage rows after re-embedding a long memory", n)
	}
}

// Verbatim keeps a snapshot's vectors as they are, which is only honest for
// vectors of the model this directory serves. Another model's are refused by
// name, whichever implementation wrote the file.
func TestVerbatimOfAnotherModelsGoSnapshotIsRefusedByName(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	w.store(t, "default", "a memory", fromMiniLM())
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	d := newDestination(t, fakeEmbedder())
	_, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(raw),
		snapshot.ImportOpts{Vectors: snapshot.VectorsVerbatim})
	wantRefused(t, err, "all-MiniLM-L6-v2", embedding.Model, "verbatim")
}

// Without a model, auto cannot recompute anything, so a vector of another
// model is refused rather than kept: keeping it would serve two spaces in one
// ranking.
func TestAnotherModelsGoSnapshotWithoutAModelIsRefusedByName(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	w.store(t, "default", "a memory", fromMiniLM())
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	d := newDestination(t, nil)
	_, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(raw), snapshot.ImportOpts{})
	wantRefused(t, err, "all-MiniLM-L6-v2", embedding.Model)
}

// A Go snapshot of this model is taken verbatim. With the model present the
// passages are computed inline; without it, the import writes a per-tenant
// passages-pending marker, which the server finishes before it binds.
func TestAGoSnapshotOfThisModelGetsItsPassagesNowOrLater(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	rid := w.store(t, "default", longContent, embeddedByThisModel(), withPassages())
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})

	t.Run("with the model", func(t *testing.T) {
		d := newDestination(t, fakeEmbedder())
		rep := d.importAll(t, raw, snapshot.ImportOpts{})
		if rep.VectorsVerbatim != 1 {
			t.Fatalf("kept %d vectors verbatim, want 1", rep.VectorsVerbatim)
		}
		if n := passageRows(t, d.kv, "default", rid); n < 2 {
			t.Fatalf("%d passage rows; with the model they are computed during the import", n)
		}
		if pending, err := record.PassagesPending(context.Background(), d.kv); err != nil || len(pending) != 0 {
			t.Fatalf("pending %v (%v), want none", pending, err)
		}
	})
	t.Run("without the model", func(t *testing.T) {
		d := newDestination(t, nil)
		rep := d.importAll(t, raw, snapshot.ImportOpts{})
		if rep.VectorsVerbatim != 1 {
			t.Fatalf("kept %d vectors verbatim, want 1", rep.VectorsVerbatim)
		}
		if n := passageRows(t, d.kv, "default", rid); n != 0 {
			t.Fatalf("%d passage rows written without a model", n)
		}
		pending, err := record.PassagesPending(context.Background(), d.kv)
		must(t, err)
		if len(pending) != 1 || pending[0] != "default" {
			t.Fatalf("pending tenants %v, want [default]", pending)
		}
	})
}
