package discovery_test

import (
	"context"
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// harness is a whole discovery world: a store, the canonical records, the exact
// vector index and the graph.
//
// The index is `flat` rather than `hnsw` because flat is the definition of
// correct (internal/vector/vectortest) and discovery's assertions are about
// thresholds and edges, not about recall. A test that could not tell an
// approximation's miss from a threshold bug would be testing neither.
type harness struct {
	kv    *memkv.Store
	repo  record.Repo
	index vector.Index
	edges graph.Service
	clk   *clock.Fake
	disc  *discovery.Discoverer
}

func newHarness(t *testing.T, opts ...func(*discovery.Deps)) *harness {
	t.Helper()
	clk := clock.NewFake(clock.FakeStart)
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	repo := record.NewRepo(kv, record.WithIndexer(attr.NewIndexer(attr.MustTable())))
	index := flat.New(vector.NewStore(kv), distance.L2)
	edges := graph.NewService(kv, clk)

	deps := discovery.Deps{
		KV: kv, Repo: repo, Index: index, Edges: edges,
		Clock: clk, Metric: distance.L2,
	}
	for _, o := range opts {
		o(&deps)
	}
	d, err := discovery.NewDiscoverer(deps)
	must(t, err)
	return &harness{kv: kv, repo: repo, index: index, edges: edges, clk: clk, disc: d}
}

// unit is a unit vector at the given angle from (1, 0), padded to four
// dimensions. Two of them at angles a and b have cosine cos(a-b) exactly, which
// is what lets a threshold test name the number it is asserting.
func unit(theta float64) []float32 {
	return []float32{float32(math.Cos(theta)), float32(math.Sin(theta)), 0, 0}
}

// angleFor is the angle whose cosine against unit(0) is the given similarity.
func angleFor(cos float64) float64 { return math.Acos(cos) }

// memo is what a stored fixture memory is addressed by.
type memo struct{ rid id.ID }

// store writes one memory directly through the repository, so its vector is
// exactly the one the test chose rather than whatever an embedder produced.
func (h *harness) store(t *testing.T, tid tenant.ID, content string, v []float32, mutate ...func(*record.Record)) memo {
	t.Helper()
	rec := &record.Record{
		ID:        id.New(),
		Tenant:    tid,
		Namespace: tenant.DefaultNamespace,
		Type:      record.TypeMemory,
		Content:   content,
		Fields: record.Fields{
			Policy:     record.DefaultPolicy,
			Importance: record.DefaultImportance,
			Health:     record.DefaultHealth,
		},
		Vectors: map[string]*vector.Vector{
			record.VectorContent: {ModelID: "test-model", Dim: len(v), Values: v},
		},
		CreatedAt: h.clk.Now().UTC(),
		UpdatedAt: h.clk.Now().UTC(),
	}
	for _, m := range mutate {
		m(rec)
	}
	ctx := tenant.NewContext(context.Background(), tid)
	must(t, txn.Do(ctx, h.kv, func(tx txn.Tx) error { return h.repo.Put(ctx, tx, rec) }))
	return memo{rid: rec.ID}
}

// run drives the handler over one subject, the way a worker would.
func (h *harness) run(t *testing.T, tid tenant.ID, subjects ...id.ID) error {
	t.Helper()
	payload, err := discovery.EncodePayload(subjects)
	must(t, err)
	j := &jobs.Job{
		ID: id.New(), Tenant: tid, Namespace: tenant.DefaultNamespace,
		Type: "discovery.similar", Payload: payload,
	}
	return h.disc.Handle(context.Background(), j, nil)
}

// outEdges is what a memory points at, for asserting on what discovery wrote.
func (h *harness) outEdges(t *testing.T, tid tenant.ID, rid id.ID) []graph.Edge {
	t.Helper()
	ctx := tenant.NewContext(context.Background(), tid)
	out, err := h.edges.Out(ctx, rid, graph.NeighbourOpts{})
	must(t, err)
	return out
}

// link writes an edge the way a user would, so a test can assert discovery
// leaves it alone.
func (h *harness) link(t *testing.T, tid tenant.ID, from, to id.ID, typ graph.RelationshipType, strength float32) {
	t.Helper()
	ctx := tenant.NewContext(context.Background(), tid)
	must(t, txn.Do(ctx, h.kv, func(tx txn.Tx) error {
		return h.edges.Add(ctx, tx, graph.Edge{From: from, To: to, Type: typ, Strength: strength})
	}))
}

// keyspace is every key and value in the store, for the comparison that says a
// second run changed nothing at all.
func (h *harness) keyspace(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	it := h.kv.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for ok := it.First(); ok; ok = it.Next() {
		out[string(it.Key())] = string(it.Value())
	}
	must(t, it.Error())
	return out
}

// recordingCheckpointer keeps every cursor a handler saved, so a test can
// assert the run resumes from where it stopped rather than from the start.
type recordingCheckpointer struct {
	saved [][]byte
	// processed and changed are what the handler last reported, as pointers so
	// that "nothing was reported" stays distinguishable from "zero".
	processed *uint64
	changed   *uint64
}

func (c *recordingCheckpointer) Save(_ context.Context, cursor []byte) error {
	c.saved = append(c.saved, append([]byte(nil), cursor...))
	return nil
}

func (c *recordingCheckpointer) Processed(n uint64) { c.processed = &n }
func (c *recordingCheckpointer) Changed(n uint64)   { c.changed = &n }
