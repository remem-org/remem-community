package snapshot_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/tenant"
)

// The test world stamps every vector worldModel, worldDim wide. A destination
// whose vectors come from the caller and which declares that model is the one
// place those vectors are usable as they stand.
func externalDestination(t *testing.T, model string, dim int) *destination {
	t.Helper()
	return newDestination(t, embedding.NewExternal(model, dim))
}

func TestAGoSnapshotOfTheSameExternalModelRoundTrips(t *testing.T) {
	w := populated(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})

	d := externalDestination(t, worldModel, worldDim)
	rep := d.importAll(t, raw, snapshot.ImportOpts{})
	if rep.ReEmbedded != 0 {
		t.Fatalf("re-embedded %d vectors; an external destination can compute none", rep.ReEmbedded)
	}
	if v := verifyAgainst(t, d, raw); !v.Clean() {
		t.Fatalf("verify is not clean after a verbatim round trip: %+v", v)
	}
}

func TestAForeignSnapshotIntoAnExternalDestinationIsRefusedByName(t *testing.T) {
	d := externalDestination(t, worldModel, worldDim)
	_, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(rustLikeSnapshot(t)), snapshot.ImportOpts{})
	wantRefused(t, err, "external", "re-embed")
}

func TestReEmbeddingIntoAnExternalDestinationIsRefusedByName(t *testing.T) {
	w := populated(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	d := externalDestination(t, worldModel, worldDim)
	_, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(raw),
		snapshot.ImportOpts{Vectors: snapshot.VectorsReEmbed})
	wantRefused(t, err, "external", "re-embed")
}

func TestAVectorFromAnotherModelIsRefusedNamingBoth(t *testing.T) {
	w := populated(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	d := externalDestination(t, "cohere-embed-english-v3.0", 4)
	_, err := snapshot.Import(context.Background(), d.dst, bytes.NewReader(raw), snapshot.ImportOpts{})
	wantRefused(t, err, worldModel, "cohere-embed-english-v3.0")
}

func TestVectorModelsListsEveryModelInTheFile(t *testing.T) {
	w := populated(t)
	raw, stats := exportWorld(t, w, snapshot.ExportOpts{})
	models, err := snapshot.VectorModels(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(models) != 1 || models[snapshot.VectorModel{ModelID: worldModel, Dim: worldDim}] != stats.Vectors {
		t.Fatalf("models = %v, want test-model/4 × %d", models, stats.Vectors)
	}
}

func wantRefused(t *testing.T, err error, words ...string) {
	t.Helper()
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("want an Invalid refusal, got %v", err)
	}
	for _, w := range words {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("refusal %q does not name %q", err, w)
		}
	}
}

// scaleStoredVector multiplies one stored memory's vector by k, damaging it in
// the one way the norm check exists to catch.
func scaleStoredVector(t *testing.T, d *destination, rid id.ID, k float32) {
	t.Helper()
	ctx := tenant.NewContext(context.Background(), "default")
	rec, err := d.repo.Get(ctx, rid)
	must(t, err)
	v := rec.Vectors[record.VectorContent]
	for i := range v.Values {
		v.Values[i] *= k
	}
	must(t, txnPut(ctx, d, rec))
}

func TestADriftedBuiltInVectorIsStillFlagged(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	a := w.store(t, "default", "a memory", embeddedByThisModel())
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	d := newDestination(t, fakeEmbedder())
	d.importAll(t, raw, snapshot.ImportOpts{})

	scaleStoredVector(t, d, a, 2)
	if rep := verifyAgainst(t, d, raw); rep.Clean() {
		t.Fatal("a built-in vector whose norm has drifted from one must be reported")
	}
}

func TestAnExternalVectorIsComparedWithItsOwnNormNotWithOne(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	a := w.store(t, "default", "a memory") // test-model, norm about 1.47
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})
	d := externalDestination(t, worldModel, worldDim)
	d.importAll(t, raw, snapshot.ImportOpts{})
	if rep := verifyAgainst(t, d, raw); !rep.Clean() {
		t.Fatalf("a caller's vector stored as given is faithful: %v", rep.Findings)
	}

	scaleStoredVector(t, d, a, 2)
	if rep := verifyAgainst(t, d, raw); rep.Clean() {
		t.Fatal("a caller's vector whose norm moved from the snapshot's must be reported")
	}
}
