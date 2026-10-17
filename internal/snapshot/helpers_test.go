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
	"github.com/remem-org/remem-go/internal/users"
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
	users  *users.Service
}

func newWorld(t *testing.T) *world {
	t.Helper()
	clk := clock.NewFake(clock.FakeStart)
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())),
		record.WithIndexer(text.New()),
		record.WithIndexer(record.NewPassageIndexer()))
	return &world{
		kv:     kv,
		repo:   repo,
		edges:  graph.NewService(kv, clk),
		events: events.NewStore(kv),
		dir:    tenantkv.New(kv, clk),
		clk:    clk,
		users:  users.New(kv, clk),
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
			record.VectorContent: {ModelID: worldModel, Dim: worldDim, Values: vectorFor(rid)},
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

// worldModel and worldDim are what the test world stamps on every vector: the
// model this build serves, so a Go snapshot of the world is one this build may
// take verbatim. Before passages the world used a four-wide "test-model" and
// relied on an import with no model keeping any Go vector; that rule is gone,
// because a vector of another model kept verbatim is two spaces in one ranking.
const (
	worldModel = embedding.Model
	worldDim   = embedding.Dim
)

// vectorFor is a deterministic unit vector of the world's width.
func vectorFor(rid id.ID) []float32 {
	v := make([]float32, worldDim)
	for i := range v {
		v[i] = float32(rid[i%len(rid)]) / 255
	}
	v[0] += 1
	return embedding.Normalise(v)
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
		Users:   w.users,
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
