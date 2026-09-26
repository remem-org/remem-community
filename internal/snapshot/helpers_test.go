package snapshot_test

import (
	"context"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/snapshot"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// world is a store written through the ordinary write path, so an export reads
// what a running server would actually have produced — attribute rows, text
// postings, in-edges and all. Building the rows by hand would test the exporter
// against a corpus no server writes.
type world struct {
	kv     *memkv.Store
	repo   record.Repo
	edges  graph.Service
	events *events.Store
	dir    *tenantkv.Directory
	clk    *clock.Fake
}

func newWorld(t *testing.T) *world {
	t.Helper()
	clk := clock.NewFake(clock.FakeStart)
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())),
		record.WithIndexer(text.New()))
	return &world{
		kv:     kv,
		repo:   repo,
		edges:  graph.NewService(kv, clk),
		events: events.NewStore(kv),
		dir:    tenantkv.New(kv, clk),
		clk:    clk,
	}
}

func (w *world) tenant(t *testing.T, tid tenant.ID) {
	t.Helper()
	_, err := tenant.Ensure(context.Background(), w.dir, tid)
	must(t, err)
}

type memoOpt func(*record.Record)

func archived(at time.Time) memoOpt {
	return func(r *record.Record) { r.Fields.Archived = true; r.Fields.ArchivedAt = at }
}

func noVector() memoOpt {
	return func(r *record.Record) { r.Vectors = nil }
}

// embeddedByThisModel stamps a record's vector with the running model's
// identity and width, which is what makes an import take it verbatim.
func embeddedByThisModel() memoOpt {
	return func(r *record.Record) {
		v := make([]float32, embedding.Dim)
		for i := range v {
			v[i] = float32(r.ID[i%len(r.ID)]) / 255
		}
		r.Vectors[record.VectorContent] = &vector.Vector{
			// Unit norm, because every embedder in this system promises it and
			// everything downstream — cosine recovered from L2 above all —
			// assumes it.
			ModelID: embedding.Model, Dim: embedding.Dim, Values: embedding.Normalise(v),
		}
	}
}

func tagged(tags ...string) memoOpt {
	return func(r *record.Record) { r.Fields.Tags = tags }
}

// store writes one memory. The vector is deterministic in the id so that a
// round trip can be compared byte for byte without an embedder.
func (w *world) store(t *testing.T, tid tenant.ID, content string, opts ...memoOpt) id.ID {
	t.Helper()
	rid := id.New()
	rec := &record.Record{
		ID:        rid,
		Tenant:    tid,
		Namespace: tenant.DefaultNamespace,
		Type:      record.TypeMemory,
		Content:   content,
		Fields: record.Fields{
			Policy:            record.DefaultPolicy,
			Importance:        record.DefaultImportance,
			Health:            record.DefaultHealth,
			Valence:           0.25,
			Arousal:           0.5,
			Source:            "test",
			AccessCount:       3,
			AccessedAt:        w.clk.Now().UTC(),
			LastRecalledAt:    w.clk.Now().UTC().Add(-time.Hour),
			TTL:               2 * time.Hour,
			ProtectedUntil:    w.clk.Now().UTC().Add(24 * time.Hour),
			LastDecayAt:       w.clk.Now().UTC().Add(-48 * time.Hour),
			LastHealthCheckAt: w.clk.Now().UTC().Add(-12 * time.Hour),
		},
		SchemaVersion: 1,
		Vectors: map[string]*vector.Vector{
			record.VectorContent: {ModelID: "test-model", Dim: 4, Values: vectorFor(rid)},
		},
		CreatedAt: w.clk.Now().UTC(),
		UpdatedAt: w.clk.Now().UTC(),
	}
	for _, o := range opts {
		o(rec)
	}
	ctx := tenant.NewContext(context.Background(), tid)
	must(t, txn.Do(ctx, w.kv, func(tx txn.Tx) error { return w.repo.Put(ctx, tx, rec) }))
	return rid
}

func vectorFor(rid id.ID) []float32 {
	return []float32{float32(rid[0]) / 255, float32(rid[1]) / 255, float32(rid[2]) / 255, 1}
}

func (w *world) connect(t *testing.T, tid tenant.ID, from, to id.ID,
	typ graph.RelationshipType, strength float32, meta map[string]string,
) {
	t.Helper()
	ctx := tenant.NewContext(context.Background(), tid)
	must(t, txn.Do(ctx, w.kv, func(tx txn.Tx) error {
		return w.edges.Add(ctx, tx, graph.Edge{
			From: from, To: to, Type: typ, Strength: strength, Meta: meta,
		})
	}))
}

func (w *world) event(t *testing.T, tid tenant.ID, subject id.ID, kind events.Kind, reason string) {
	t.Helper()
	ctx := tenant.NewContext(context.Background(), tid)
	must(t, txn.Do(ctx, w.kv, func(tx txn.Tx) error {
		_, err := w.events.Append(ctx, tx, events.Event{
			Tenant: tid, Namespace: tenant.DefaultNamespace, Subject: subject,
			At: w.clk.Now().UTC(), Kind: kind, Actor: "test", Reason: reason,
			Before: map[string]string{"health": "100"},
			After:  map[string]string{"health": "92"},
		})
		return err
	}))
}

// sources builds the export inputs over a fresh pinned snapshot. The caller
// closes it.
func (w *world) sources() (snapshot.Sources, func()) {
	snap := w.kv.NewSnapshot()
	return snapshot.Sources{
		Snap:    snap,
		Tenants: w.dir,
		Records: w.repo,
		Events:  w.events,
	}, func() {
		_ = snap.Close()
	}
}

// tenantNamed registers a tenant with a display name, which tenant.Ensure
// cannot: it creates with an empty Meta. A migration has to carry the metadata
// an operator set, so a test about migration needs some to carry.
func (w *world) tenantNamed(t *testing.T, tid tenant.ID, name string) {
	t.Helper()
	must(t, w.dir.Create(context.Background(), tid, tenant.Meta{DisplayName: name}))
}
