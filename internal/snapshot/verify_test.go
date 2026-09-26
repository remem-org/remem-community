package snapshot_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

func verifyAgainst(t *testing.T, d *destination, raw []byte) snapshot.VerifyReport {
	t.Helper()
	snap := d.kv.NewSnapshot()
	defer func() { _ = snap.Close() }()
	rep, err := snapshot.Verify(context.Background(), snapshot.Sources{
		Snap: snap, Tenants: d.dst.Tenants, Records: d.repo, Events: d.dst.Events,
	}, bytes.NewReader(raw))
	must(t, err)
	return rep
}

// The step between an import and decommissioning what the snapshot came from:
// an import that reported success proved the file was readable, not that what
// is in the store is what was in the file.
func TestVerifyIsCleanAfterAFaithfulImport(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	a := w.store(t, "default", "the first memory", embeddedByThisModel(), tagged("x"))
	b := w.store(t, "default", "the second memory", embeddedByThisModel())
	w.connect(t, "default", a, b, graph.RelatedTo, 0.9, map[string]string{"why": "by hand"})
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})

	d := newDestination(t, fakeEmbedder())
	d.importAll(t, raw, snapshot.ImportOpts{})

	rep := verifyAgainst(t, d, raw)
	if !rep.Clean() {
		t.Fatalf("a faithful import does not verify: %v", rep.Findings)
	}
	if rep.Snapshot.Records != 2 || rep.Store.Records != 2 {
		t.Fatalf("counts are %+v and %+v", rep.Snapshot, rep.Store)
	}
}

func TestVerifyNamesEveryKindOfDisagreement(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	a := w.store(t, "default", "kept", embeddedByThisModel())
	b := w.store(t, "default", "removed", embeddedByThisModel())
	c := w.store(t, "default", "changed", embeddedByThisModel())
	w.connect(t, "default", a, c, graph.RelatedTo, 0.9, nil)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})

	d := newDestination(t, fakeEmbedder())
	d.importAll(t, raw, snapshot.ImportOpts{})

	ctx := tenant.NewContext(context.Background(), "default")

	// Remove one memory, change another, and invent an extra one and an extra
	// relationship.
	must(t, txn.Do(ctx, d.kv, func(tx txn.Tx) error { return d.repo.Delete(ctx, tx, b) }))
	changed, err := d.repo.Get(ctx, c)
	must(t, err)
	changed.Content = "no longer what the snapshot says"
	must(t, txnPut(ctx, d, changed))
	must(t, txn.Do(ctx, d.kv, func(tx txn.Tx) error {
		return graph.NewService(d.kv, w.clk).Add(ctx, tx,
			graph.Edge{From: c, To: a, Type: graph.Supports, Strength: 0.3})
	}))

	rep := verifyAgainst(t, d, raw)
	if rep.Clean() {
		t.Fatal("a damaged store verified clean")
	}
	kinds := map[string]int{}
	for _, f := range rep.Findings {
		kinds[f.Kind.String()+" "+f.What]++
	}
	for _, want := range []string{"missing record", "differs record", "extra edge"} {
		if kinds[want] == 0 {
			t.Fatalf("no %q finding in %v", want, rep.Findings)
		}
	}
}

// The header's own claims are checked against the file's own blocks, because a
// header nobody checks is a header nobody can trust.
func TestVerifyChecksTheHeaderAgainstTheBlocks(t *testing.T) {
	w := populated(t)
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{Compression: snapshot.CompressionNone})
	lied := lieAboutRecordCount(t, raw, 99)

	d := newDestination(t, fakeEmbedder())
	d.importAll(t, raw, snapshot.ImportOpts{})

	rep := verifyAgainst(t, d, lied)
	found := false
	for _, f := range rep.Findings {
		if f.Kind == snapshot.FindingCountMismatch {
			found = true
			if !strings.Contains(f.Detail, "claims 99 records") {
				t.Fatalf("the finding does not name the claim: %v", f)
			}
		}
	}
	if !found {
		t.Fatalf("no count mismatch reported: %v", rep.Findings)
	}
}

// next_attention_at is derived on the way in from the retention policy in force
// here, so a correctly imported record is *expected* to differ from the file in
// that one respect. Comparing it would report every good import as broken.
func TestVerifyIgnoresTheDerivedSchedule(t *testing.T) {
	w := newWorld(t)
	w.tenant(t, "default")
	w.store(t, "default", "a memory", embeddedByThisModel())
	raw, _ := exportWorld(t, w, snapshot.ExportOpts{})

	d := newDestination(t, fakeEmbedder())
	d.importAll(t, raw, snapshot.ImportOpts{})

	ctx := tenant.NewContext(context.Background(), "default")
	rec, err := d.repo.Scan(ctx, d.kv.NewSnapshot(), nil, 10)
	must(t, err)
	if len(rec) != 1 {
		t.Fatalf("expected one record, got %d", len(rec))
	}
	rec[0].Fields.NextAttentionAt = w.clk.Now().Add(1000)
	must(t, txnPut(ctx, d, rec[0]))

	if rep := verifyAgainst(t, d, raw); !rep.Clean() {
		t.Fatalf("the schedule was compared: %v", rep.Findings)
	}
}
