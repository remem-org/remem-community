package graph_test

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// existing puts a record body in the store, so an edge endpoint is not an
// orphan.
func existing(t *testing.T, kv storage.KV, rid id.ID) {
	t.Helper()
	ctx := tenant.NewContext(context.Background(), testTenant)
	repo := record.NewRepo(kv)
	tx := txn.New(kv, txn.Sync(false))
	defer tx.Close()
	rec := &record.Record{
		ID: rid, Tenant: testTenant, Namespace: tenant.DefaultNamespace,
		Type: record.TypeMemory, Content: "a memory", CreatedAt: fixedTime(),
	}
	if err := repo.Put(ctx, tx, rec); err != nil {
		t.Fatalf("writing a record: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("committing a record: %v", err)
	}
}

func verify(t *testing.T, kv storage.KV, sc graph.Scope) graph.Report {
	t.Helper()
	rep, err := graph.Verify(context.Background(), kv, sc)
	if err != nil {
		t.Fatalf("verifying: %v", err)
	}
	return rep
}

func TestVerifyReportsNothingOnACleanCorpus(t *testing.T) {
	s, kv := newStore(t)
	a, b := id.New(), id.New()
	existing(t, kv, a)
	existing(t, kv, b)
	add(t, s, kv, edge(a, b, graph.Supports, 0.7))

	rep := verify(t, kv, scope())
	if !rep.Clean() {
		t.Fatalf("a clean corpus reported %d findings: %+v", len(rep.Findings), rep.Findings)
	}
	if rep.Edges != 1 {
		t.Fatalf("the report counted %d edges, want 1", rep.Edges)
	}
}

// The plan's criterion: an edge to a deleted record is reported, not silently
// traversed. Traversal reads no record bodies by design, so this is the only
// place an orphan surfaces.
func TestOrphanEdgeSurfacesInVerify(t *testing.T) {
	s, kv := newStore(t)
	present, gone := id.New(), id.New()
	existing(t, kv, present)
	add(t, s, kv, edge(present, gone, graph.References, 0.5))

	rep := verify(t, kv, scope())
	if rep.Clean() {
		t.Fatal("an edge pointing at a record that does not exist was not reported")
	}
	found := false
	for _, f := range rep.Findings {
		if f.Kind == graph.FindingMissingRecord && f.To == gone {
			found = true
		}
	}
	if !found {
		t.Fatalf("the missing record was not named: %+v", rep.Findings)
	}
}

func TestVerifyReportsAnOutEdgeWithNoReverseEntry(t *testing.T) {
	s, kv := newStore(t)
	a, b := id.New(), id.New()
	existing(t, kv, a)
	existing(t, kv, b)
	add(t, s, kv, edge(a, b, graph.Supports, 0.5))

	if err := kv.Delete(context.Background(), graph.InKey(scope(), a, graph.Supports, b)); err != nil {
		t.Fatal(err)
	}
	rep := verify(t, kv, scope())
	if !hasFinding(rep, graph.FindingMissingReverse) {
		t.Fatalf("a missing reverse entry was not reported: %+v", rep.Findings)
	}
}

func TestVerifyReportsAReverseEntryWithNoEdge(t *testing.T) {
	_, kv := newStore(t)
	a, b := id.New(), id.New()
	existing(t, kv, a)
	existing(t, kv, b)

	value, err := graph.EncodeBody(edge(a, b, graph.Supports, 0.5))
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(context.Background(), graph.InKey(scope(), a, graph.Supports, b), value); err != nil {
		t.Fatal(err)
	}
	rep := verify(t, kv, scope())
	if !hasFinding(rep, graph.FindingOrphanReverse) {
		t.Fatalf("a reverse entry with no edge behind it was not reported: %+v", rep.Findings)
	}
}

// The two copies holding different bytes is the failure the derived index's
// verbatim copy exists to make impossible, so verify has to be able to see it.
func TestVerifyReportsADisagreeingReverseEntry(t *testing.T) {
	s, kv := newStore(t)
	a, b := id.New(), id.New()
	existing(t, kv, a)
	existing(t, kv, b)
	add(t, s, kv, edge(a, b, graph.Supports, 0.5))

	wrong, err := graph.EncodeBody(edge(a, b, graph.Supports, 0.9))
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(context.Background(), graph.InKey(scope(), a, graph.Supports, b), wrong); err != nil {
		t.Fatal(err)
	}
	rep := verify(t, kv, scope())
	if !hasFinding(rep, graph.FindingReverseDisagrees) {
		t.Fatalf("a reverse entry holding different bytes was not reported: %+v", rep.Findings)
	}
}

func TestVerifyReportsASelfEdge(t *testing.T) {
	_, kv := newStore(t)
	a := id.New()
	existing(t, kv, a)

	value, err := graph.EncodeBody(graph.Edge{From: a, To: a, Type: graph.RelatedTo, Strength: 1})
	if err != nil {
		t.Fatal(err)
	}
	// Written past the store, because Stage refuses one. This is what an import
	// of the `pathological` fixture would have produced before Phase 6.
	if err := kv.Set(context.Background(), graph.OutKey(scope(), a, graph.RelatedTo, a), value); err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(context.Background(), graph.InKey(scope(), a, graph.RelatedTo, a), value); err != nil {
		t.Fatal(err)
	}
	rep := verify(t, kv, scope())
	if !hasFinding(rep, graph.FindingSelfEdge) {
		t.Fatalf("a self-edge was not reported: %+v", rep.Findings)
	}
}

func TestVerifyIsTenantScoped(t *testing.T) {
	s, kv := newStore(t)
	present, gone := id.New(), id.New()
	existing(t, kv, present)
	add(t, s, kv, edge(present, gone, graph.References, 0.5))

	other := graph.Scope{Tenant: "globex", Namespace: tenant.DefaultNamespace}
	rep := verify(t, kv, other)
	if !rep.Clean() || rep.Edges != 0 {
		t.Fatalf("verifying globex saw acme's edges: %+v", rep)
	}
}

func TestVerifyBoundsWhatItReports(t *testing.T) {
	s, kv := newStore(t)
	a := id.New()
	existing(t, kv, a)
	for i := 0; i < graph.MaxFindings+10; i++ {
		add(t, s, kv, edge(a, id.New(), graph.SimilarTo, 0.5))
	}
	rep := verify(t, kv, scope())
	if len(rep.Findings) != graph.MaxFindings {
		t.Fatalf("the report holds %d findings, want the %d cap", len(rep.Findings), graph.MaxFindings)
	}
	if !rep.Truncated {
		t.Fatal("the report was capped and did not say so")
	}
	// The counts are still complete: a bounded report must not become a
	// misleading one.
	if rep.Broken < graph.MaxFindings+10 {
		t.Fatalf("the report counted %d broken edges, want at least %d",
			rep.Broken, graph.MaxFindings+10)
	}
}

func hasFinding(rep graph.Report, kind graph.FindingKind) bool {
	for _, f := range rep.Findings {
		if f.Kind == kind {
			return true
		}
	}
	return false
}
