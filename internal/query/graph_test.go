package query_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/query"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

const graphTenant = tenant.ID("acme")

func graphFixture(t *testing.T) (storage.KV, graph.Service, context.Context) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(time.UnixMilli(1_725_000_000_000).UTC())
	ctx := tenant.NewContext(context.Background(), graphTenant)
	return kv, graph.NewService(kv, clk), ctx
}

func connect(t *testing.T, kv storage.KV, s graph.Service, ctx context.Context,
	from, to id.ID, strength float32) {
	t.Helper()
	tx := txn.New(kv, txn.Sync(false))
	defer tx.Close()
	if err := s.Add(ctx, tx, graph.Edge{From: from, To: to, Type: graph.RelatedTo, Strength: strength}); err != nil {
		t.Fatalf("connecting: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("committing: %v", err)
	}
}

// The plan's REM-83 regression, at the query layer: a 0.9 edge at depth 2
// outranks a 0.1 edge at depth 1.
func TestRelatedRanksByPathStrength(t *testing.T) {
	kv, edges, ctx := graphFixture(t)
	anchor, hub, strong, weak := id.New(), id.New(), id.New(), id.New()
	connect(t, kv, edges, ctx, anchor, weak, 0.1)
	connect(t, kv, edges, ctx, anchor, hub, 0.9)
	connect(t, kv, edges, ctx, hub, strong, 0.9)

	planner := query.NewPlanner(slotTable(t), 0, 0)
	executor := query.NewExecutor(slotTable(t), nil, 0)
	q := &query.Query{Tenant: graphTenant, RelatedTo: &anchor, RelatedDepth: 2, Limit: 10}

	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("planning: %v", err)
	}
	res, err := executor.Run(ctx, kv, q, plan)
	if err != nil {
		t.Fatalf("running: %v", err)
	}
	if len(res.Hits) != 3 {
		t.Fatalf("got %d hits, want the three reachable memories: %+v", len(res.Hits), res.Hits)
	}
	if res.Hits[len(res.Hits)-1].ID != weak {
		t.Fatalf("the 0.1 edge at depth 1 did not sort last: %+v", res.Hits)
	}
	var strongRank, weakRank int
	for i, h := range res.Hits {
		switch h.ID {
		case strong:
			strongRank = i
		case weak:
			weakRank = i
		}
	}
	if strongRank >= weakRank {
		t.Fatalf("the 0.9 chain at depth 2 ranked below the 0.1 edge at depth 1: %+v", res.Hits)
	}
}

// A graph-only hit takes its graph score, because there is nothing else to take.
// The rule that graph proximity does not set relevance only applies when
// something else matched.
func TestAGraphOnlyHitTakesItsPathStrengthAsScore(t *testing.T) {
	kv, edges, ctx := graphFixture(t)
	anchor, near := id.New(), id.New()
	connect(t, kv, edges, ctx, anchor, near, 0.6)

	planner := query.NewPlanner(slotTable(t), 0, 0)
	executor := query.NewExecutor(slotTable(t), nil, 0)
	q := &query.Query{Tenant: graphTenant, RelatedTo: &anchor, Limit: 10, Explain: true}
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatal(err)
	}
	res, err := executor.Run(ctx, kv, q, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hits) != 1 {
		t.Fatalf("got %d hits, want one: %+v", len(res.Hits), res.Hits)
	}
	if got := res.Hits[0].Score; got < 0.59 || got > 0.61 {
		t.Fatalf("score is %v, want the 0.6 path strength", got)
	}
	if len(res.Hits[0].Sources) != 1 || res.Hits[0].Sources[0].Source != query.SourceGraph {
		t.Fatalf("the hit does not name the graph as its evidence: %+v", res.Hits[0].Sources)
	}
}

func TestGraphTraversalIsTenantScopedInTheExecutor(t *testing.T) {
	kv, edges, ctx := graphFixture(t)
	anchor, near := id.New(), id.New()
	connect(t, kv, edges, ctx, anchor, near, 0.9)

	planner := query.NewPlanner(slotTable(t), 0, 0)
	executor := query.NewExecutor(slotTable(t), nil, 0)
	q := &query.Query{Tenant: "globex", RelatedTo: &anchor, Limit: 10}
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatal(err)
	}
	res, err := executor.Run(tenant.NewContext(context.Background(), "globex"), kv, q, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hits) != 0 {
		t.Fatalf("tenant globex traversed acme's graph: %+v", res.Hits)
	}
}

func TestARelatedQueryNoLongerRefusedByName(t *testing.T) {
	anchor := id.New()
	q := &query.Query{Tenant: graphTenant, RelatedTo: &anchor, Limit: 10}
	if err := q.Validate(); err != nil {
		t.Fatalf("a related query is still refused: %v", err)
	}
}

func TestRelatedDepthAboveTheCapIsRefused(t *testing.T) {
	anchor := id.New()
	q := &query.Query{Tenant: graphTenant, RelatedTo: &anchor,
		RelatedDepth: graph.MaxTraversalDepth + 1, Limit: 10}
	if err := q.Validate(); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

func TestAQueryCarryingGraphFieldsRoundTripsThroughJSON(t *testing.T) {
	anchor := id.New()
	want := query.Query{
		Tenant:           graphTenant,
		RelatedTo:        &anchor,
		RelatedDepth:     3,
		RelatedTypes:     []graph.RelationshipType{graph.Supports, graph.SimilarTo},
		RelatedDirection: graph.Both,
		Limit:            10,
	}
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	var got query.Query
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshalling %s: %v", b, err)
	}
	if got.RelatedTo == nil || *got.RelatedTo != anchor {
		t.Fatalf("the anchor did not survive: %v", got.RelatedTo)
	}
	if got.RelatedDepth != 3 || got.RelatedDirection != graph.Both || len(got.RelatedTypes) != 2 {
		t.Fatalf("round-tripped to %+v, want %+v", got, want)
	}
	// The wire form names the types rather than numbering them: a durable
	// number in a wire format is one nobody reading a request body can read.
	if !bytesContain(b, `"supports"`) || !bytesContain(b, `"both"`) {
		t.Fatalf("the wire form does not use names: %s", b)
	}
}

func bytesContain(haystack []byte, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if string(haystack[i:i+len(needle)]) == needle {
			return true
		}
	}
	return false
}

func slotTable(t *testing.T) *schema.Slots {
	t.Helper()
	table, err := attr.Table()
	if err != nil {
		t.Fatalf("building the slot table: %v", err)
	}
	return table
}

// fixedIndex is a vector index that returns a stated ranking, so a fused plan
// can be exercised without a model.
type fixedIndex struct {
	hits   []vector.Hit
	health vector.Health
}

func (f fixedIndex) Search(_ context.Context, _ tenant.ID, _ []float32, k int, filter vector.Filter) ([]vector.Hit, error) {
	out := make([]vector.Hit, 0, k)
	for _, h := range f.hits {
		if !filter.Allows(h.ID) {
			continue
		}
		out = append(out, h)
		if len(out) == k {
			break
		}
	}
	return out, nil
}
func (f fixedIndex) Insert(context.Context, tenant.ID, id.ID, []float32) error { return nil }
func (f fixedIndex) Delete(context.Context, tenant.ID, id.ID) error            { return nil }
func (f fixedIndex) Rebuild(context.Context, tenant.ID, vector.Source, ...vector.RebuildOption) error {
	return nil
}
func (f fixedIndex) Health(context.Context, tenant.ID) (vector.Health, error) { return f.health, nil }
func (f fixedIndex) Stats(context.Context, tenant.ID) (vector.Stats, error) {
	return vector.Stats{Vectors: len(f.hits)}, nil
}

// The rule result.go states, over a real graph step: a hit that both the vector
// index and the traversal found reports the vector's relevance, not the path
// strength. Being one hop from an anchor is context, not evidence that the
// content matches the query.
func TestScoreIgnoresTheGraphWhenSomethingElseMatched(t *testing.T) {
	kv, edges, ctx := graphFixture(t)
	anchor, near := id.New(), id.New()
	connect(t, kv, edges, ctx, anchor, near, 0.2)

	index := fixedIndex{hits: []vector.Hit{{ID: near, Distance: 0}}}
	planner := query.NewPlanner(slotTable(t), 0, 0)
	executor := query.NewExecutor(slotTable(t), index, distance.Cosine)
	q := &query.Query{
		Tenant: graphTenant, Vector: []float32{1, 0}, RelatedTo: &anchor, Limit: 10, Explain: true,
	}
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 2 || !plan.Fused {
		t.Fatalf("a vector-plus-graph query planned as %+v, want two fused steps", plan)
	}
	res, err := executor.Run(ctx, kv, q, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hits) != 1 {
		t.Fatalf("got %d hits, want one: %+v", len(res.Hits), res.Hits)
	}
	hit := res.Hits[0]
	if len(hit.Sources) != 2 {
		t.Fatalf("the hit reports %d sources, want both the vector and the graph", len(hit.Sources))
	}
	if hit.Score < 0.99 {
		t.Fatalf("score is %v; a hit at distance 0 must take the vector's relevance, "+
			"not the 0.2 path strength", hit.Score)
	}
}

// The one caller of vector.Health, pinned. An index that cannot answer
// completely for a tenant must make the page say so — the alternative is a
// short page that reads exactly like a small corpus, which is the failure the
// truncated flag exists for.
func TestSearchOverADegradedIndexSaysSo(t *testing.T) {
	slots := slotTable(t)
	rid := id.New()
	index := fixedIndex{
		hits:   []vector.Hit{{ID: rid, Distance: 0}},
		health: vector.Health{Degraded: true, Reason: "being rebuilt"},
	}
	e := query.NewExecutor(slots, index, distance.Cosine)

	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	q := &query.Query{Tenant: graphTenant, Vector: []float32{1, 0}, Limit: 5}
	plan, err := query.NewPlanner(slots, 32, 0).Plan(q)
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Run(context.Background(), kv, q, plan)
	if err != nil {
		t.Fatalf("a degraded index made the search fail rather than degrade: %v", err)
	}
	if !res.Truncated {
		t.Fatal("a search over a degraded index reported a complete answer")
	}

	// And an undamaged index does not set the flag for no reason.
	healthy := query.NewExecutor(slots, fixedIndex{hits: index.hits}, distance.Cosine)
	res, err = healthy.Run(context.Background(), kv, q, plan)
	if err != nil {
		t.Fatal(err)
	}
	if res.Truncated {
		t.Fatal("a healthy index reported its answer as incomplete")
	}
}
