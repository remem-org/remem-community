package query

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/indexes"
)

// This file is plan item 16's guard: a filtered vector search must stop paying
// twice without answering anything differently.
//
// It holds three things. runVectorReference is the loop the change replaced —
// blind doubling of the budget, every candidate read and decoded again on every
// round — kept as the oracle, because "the new widening returns the same page"
// is the only claim that matters about the change and a hand-written expectation
// table cannot make it. countingReader and countingIndex are what make the
// repeated work visible: one counts attribute-row reads per record, the other
// counts walks. And the randomised comparison runs the two loops over hundreds
// of corpora, filters and limits.
//
// The reference must not be "tidied" into sharing code with the implementation.
// An oracle that shares the code under test agrees with it by construction and
// proves nothing.

const widenTenant = tenant.ID("widen")

// widenCorpus is a tenant with vectors in an HNSW index and one attribute row
// per record, which is everything runVector touches.
//
// It is built through indexes.Open rather than by naming the implementation,
// which is what internal/arch's §40.3 rule requires of every caller and is how
// the composition root and bench/ build theirs.
type widenCorpus struct {
	kv    storage.KV
	slots *schema.Slots
	index vector.Index
	ctx   context.Context
	dim   int
}

// newWidenCorpus builds size records whose policy is long_term for one in
// passEvery of them and short_term for the rest, so a query filtering on
// long_term has a known pass rate. ef is the index's ef_search.
//
// Record ids are derived from the corpus parameters rather than random, because
// a node's level in HNSW is derived from its id: random ids would build a
// different graph on every run, and a divergence between the two loops would
// then not reproduce.
func newWidenCorpus(t testing.TB, seed uint64, size, dim, ef, passEvery int) widenCorpus {
	t.Helper()

	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	slots, err := attr.Table()
	if err != nil {
		t.Fatalf("building the slot table: %v", err)
	}
	ix, err := indexes.Open(kv, indexes.Config{
		Kind: "hnsw", Metric: "l2",
		Neighbours: 8, BuildEffort: 64, SearchEffort: ef,
		ModelID: "widen-test",
	})
	if err != nil {
		t.Fatalf("opening the index: %v", err)
	}
	ctx := tenant.NewContext(context.Background(), widenTenant)

	rng := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	tx := txn.New(kv, txn.Sync(false))
	defer tx.Close()
	for i := range size {
		rid := widenID(seed, uint64(i))
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()
		}
		if err := ix.Insert(ctx, widenTenant, rid, vector.Single(v)); err != nil {
			t.Fatalf("inserting record %d: %v", i, err)
		}
		policy := "short_term"
		if passEvery > 0 && i%passEvery == 0 {
			policy = "long_term"
		}
		row := attr.NewRow(attr.SchemaVersion)
		row.Set(attr.SlotPolicy, attr.Str(policy))
		enc, err := row.Encode(slots)
		if err != nil {
			t.Fatalf("encoding row %d: %v", i, err)
		}
		tx.Set(attr.RowKey(widenTenant, tenant.DefaultNamespace, rid), enc)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("committing the rows: %v", err)
	}
	return widenCorpus{kv: kv, slots: slots, index: ix, ctx: ctx, dim: dim}
}

// query is a long_term-only semantic query over this corpus.
func (c widenCorpus) query(rng *rand.Rand, limit int, filtered bool) *Query {
	v := make([]float32, c.dim)
	for j := range v {
		v[j] = rng.Float32()
	}
	q := &Query{Tenant: widenTenant, Vector: v, Limit: limit}
	if filtered {
		q.Preds = attr.Preds{attr.Eq(attr.SlotPolicy, attr.Str("long_term"))}
	}
	return q
}

// plan is what the planner would produce for q.
func (c widenCorpus) plan(t testing.TB, q *Query) Plan {
	t.Helper()
	plan, err := NewPlanner(c.slots, 0, 0).Plan(q)
	if err != nil {
		t.Fatalf("planning: %v", err)
	}
	return plan
}

// widenID is a record id derived from a counter: unique, non-zero and the same
// on every run, so levelOf gives the same heights and the corpus reproduces.
func widenID(seed, n uint64) id.ID {
	var out id.ID
	binary.BigEndian.PutUint64(out[0:8], (seed+1)*0xbf58476d1ce4e5b9+n*0x9e3779b97f4a7c15+1)
	binary.BigEndian.PutUint64(out[8:16], n*0x94d049bb133111eb+0xda942042e4dd58b5)
	return out
}

// countingReader counts how often each key was read.
type countingReader struct {
	inner attr.Reader
	reads map[string]int
}

func newCountingReader(r attr.Reader) *countingReader {
	return &countingReader{inner: r, reads: map[string]int{}}
}

func (c *countingReader) Get(ctx context.Context, key []byte) ([]byte, error) {
	c.reads[string(key)]++
	return c.inner.Get(ctx, key)
}

func (c *countingReader) NewIterator(lower, upper []byte) storage.Iterator {
	return c.inner.NewIterator(lower, upper)
}

// total is how many reads were made, and repeated how many of them were a key
// already read once.
func (c *countingReader) total() (total, repeated int) {
	for _, n := range c.reads {
		total += n
		repeated += n - 1
	}
	return total, repeated
}

// countingIndex counts the walks a query cost. It forwards SearchGranularity
// so that wrapping the index does not change what the executor asks for.
type countingIndex struct {
	vector.Index
	searches int
}

func (c *countingIndex) Search(ctx context.Context, t tenant.ID, q []float32, k int,
	f vector.Filter) ([]vector.Hit, error) {
	c.searches++
	return c.Index.Search(ctx, t, q, k, f)
}

func (c *countingIndex) SearchGranularity() int {
	if g, ok := c.Index.(vector.Granular); ok {
		return g.SearchGranularity()
	}
	return 1
}

// A candidate the previous round already read and decoded must not be read and
// decoded again. This is finding 2's "pays twice", stated as a test.
func TestAWidenedFilteredSearchReadsEachCandidateOnce(t *testing.T) {
	c := newWidenCorpus(t, 1, 300, 8, 64, 5)
	q := c.query(rand.New(rand.NewPCG(7, 7)), 10, true)
	plan := c.plan(t, q)
	r := newCountingReader(c.kv)

	e := NewExecutor(c.slots, c.index, distance.L2)
	page, err := e.runVector(c.ctx, r, q, plan, Step{Kind: StepVector, Source: SourceVector})
	if err != nil {
		t.Fatalf("running the vector step: %v", err)
	}
	if len(page.items) != plan.Target {
		t.Fatalf("the page holds %d of %d results; the fixture is meant to fill one "+
			"only after widening", len(page.items), plan.Target)
	}
	total, repeated := r.total()
	if repeated != 0 {
		t.Fatalf("%d of %d attribute-row reads were a row this request had already read "+
			"and decoded; a widening round must not settle a candidate twice", repeated, total)
	}
}

// And the widening must reach the target without restarting the walk when the
// candidates it needs were already computed: ef_search is 64, so asking for 30
// and then 60 pays for two traversals that found the same 64 nodes.
func TestASelectiveFilterFillsThePageInOneRound(t *testing.T) {
	c := newWidenCorpus(t, 2, 300, 8, 64, 5)
	q := c.query(rand.New(rand.NewPCG(11, 11)), 10, true)
	plan := c.plan(t, q)
	ix := &countingIndex{Index: c.index}

	e := NewExecutor(c.slots, ix, distance.L2)
	page, err := e.runVector(c.ctx, c.kv, q, plan, Step{Kind: StepVector, Source: SourceVector})
	if err != nil {
		t.Fatalf("running the vector step: %v", err)
	}
	if len(page.items) != plan.Target {
		t.Fatalf("the page holds %d of %d results", len(page.items), plan.Target)
	}
	if ix.searches != 1 {
		t.Fatalf("the search walked the graph %d times for a page the first walk's own "+
			"candidates could fill; want 1", ix.searches)
	}
}

// The claim the change rests on: whatever the widening schedule, the page is the
// one the restart loop produced.
//
// The corpora vary in size, dimension, ef_search, filter selectivity and
// requested limit, which between them cover the cases the schedules disagree
// about: a page filled inside the first budget, a page filled only after
// widening, a page that cannot be filled at all and comes back truncated, and a
// corpus smaller than the budget.
//
// `examined` is deliberately not compared. It counts the candidates the final
// round was offered, and a round that asked for the whole granularity was
// offered more of them; it is an `explain` diagnostic rather than part of the
// page, and runVector's own documentation says so.
//
// The counters at the end are the guard on the guard. A comparison between two
// loops that happened to take the same path every time would pass while proving
// nothing, so the test insists it saw the cases it is about: pages that could
// only be filled by widening, pages that could not be filled at all, and rounds
// where the new schedule reached the answer in strictly fewer walks.
func TestWidenedVectorSearchAnswersWhatTheRestartLoopDid(t *testing.T) {
	cases := []struct {
		size, dim, ef, passEvery int
	}{
		{300, 8, 64, 5},    // the bench's shape: one in five passes
		{300, 8, 64, 1},    // everything passes
		{300, 8, 64, 25},   // one in twenty-five: truncation territory
		{300, 8, 64, 64},   // barely anything passes
		{300, 8, 16, 5},    // a granularity below the first budget
		{300, 8, 200, 5},   // a granularity above every budget
		{40, 4, 64, 5},     // a corpus smaller than ef_search
		{1000, 16, 64, 11}, // more nodes than any budget reaches
		{500, 8, 32, 13},   // the estimate overshoots a doubling by a lot
		{2000, 16, 64, 37}, // budgets far above the granularity, where the two
		//                     schedules land on different efs and therefore on
		//                     different walks
	}
	var widened, truncated, fewerWalks int
	for _, tc := range cases {
		name := fmt.Sprintf("size%d_dim%d_ef%d_every%d", tc.size, tc.dim, tc.ef, tc.passEvery)
		t.Run(name, func(t *testing.T) {
			c := newWidenCorpus(t, uint64(tc.size*31+tc.passEvery), tc.size, tc.dim, tc.ef, tc.passEvery)
			oldIx, newIx := &countingIndex{Index: c.index}, &countingIndex{Index: c.index}
			oldE := NewExecutor(c.slots, oldIx, distance.L2)
			newE := NewExecutor(c.slots, newIx, distance.L2)
			rng := rand.New(rand.NewPCG(uint64(tc.ef), uint64(tc.passEvery)))

			for round := range 100 {
				limit := []int{1, 3, 5, 10, 25}[round%5]
				filtered := round%7 != 0
				q := c.query(rng, limit, filtered)
				plan := c.plan(t, q)
				step := Step{Kind: StepVector, Source: SourceVector}

				oldIx.searches, newIx.searches = 0, 0
				want, err := oldE.runVectorReference(c.ctx, c.kv, q, plan, step)
				if err != nil {
					t.Fatalf("the reference loop failed: %v", err)
				}
				got, err := newE.runVector(c.ctx, c.kv, q, plan, step)
				if err != nil {
					t.Fatalf("the widening loop failed: %v", err)
				}
				if oldIx.searches > 1 {
					widened++
				}
				if newIx.searches < oldIx.searches {
					fewerWalks++
				}
				if want.truncated {
					truncated++
				}
				if got.truncated != want.truncated || got.hasMore != want.hasMore {
					t.Fatalf("round %d (limit %d, filtered %v): flags are "+
						"{truncated %v hasMore %v}, want {%v %v}",
						round, limit, filtered, got.truncated, got.hasMore,
						want.truncated, want.hasMore)
				}
				if len(got.items) != len(want.items) {
					t.Fatalf("round %d (limit %d, filtered %v): the page holds %d results, want %d",
						round, limit, filtered, len(got.items), len(want.items))
				}
				for i := range got.items {
					if got.items[i] != want.items[i] {
						t.Fatalf("round %d (limit %d, filtered %v): result %d is %+v, want %+v",
							round, limit, filtered, i, got.items[i], want.items[i])
					}
				}
			}
		})
	}

	switch {
	case widened == 0:
		t.Error("no query in the whole table needed a second round, so the comparison " +
			"never reached the widening it is about")
	case truncated == 0:
		t.Error("no query in the whole table exhausted its budget, so the truncated " +
			"semantics were never compared")
	case fewerWalks == 0:
		t.Error("the new schedule never reached an answer in fewer walks than the " +
			"restart loop, so the two took the same path and this proves nothing")
	}
	t.Logf("compared %d queries: %d widened, %d truncated, %d answered in fewer walks",
		len(cases)*100, widened, truncated, fewerWalks)
}

// runVectorReference is runVector as it stood before plan item 16: the budget
// doubles blindly, and every round reads and decodes every candidate again. See
// this file's opening comment for why it is kept.
func (e *Executor) runVectorReference(ctx context.Context, r attr.Reader, q *Query,
	plan Plan, step Step) (stepPage, error) {
	const op = "query.Executor.runVector"

	if e.index == nil {
		return stepPage{}, errs.E(errs.Invalid, op,
			fmt.Errorf("this build has no vector index, so a semantic search cannot be answered"))
	}
	health, err := e.index.Health(ctx, q.Tenant)
	if err != nil {
		return stepPage{}, err
	}

	var out stepPage
	out.truncated = health.Degraded
	budget := plan.StartBudget
	for {
		hits, err := e.index.Search(ctx, q.Tenant, q.Vector, budget, nil)
		if err != nil {
			return stepPage{}, err
		}
		out.examined = len(hits)
		out.items = out.items[:0]

		for _, h := range hits {
			ok, err := e.settle(ctx, r, q, h.ID)
			if err != nil {
				return stepPage{}, err
			}
			if !ok {
				continue
			}
			out.items = append(out.items, RankedItem{
				ID:       h.ID,
				Score:    distance.Relevance(e.metric, h.Distance),
				Ordering: distance.Score(h.Distance),
				Distance: h.Distance,
			})
			if len(out.items) == plan.Target {
				break
			}
		}

		switch {
		case len(out.items) >= plan.Target:
			return out, nil
		case len(hits) < budget:
			return out, nil
		case budget >= plan.MaxBudget:
			out.truncated = true
			return out, nil
		}
		budget *= 2
		if budget > plan.MaxBudget {
			budget = plan.MaxBudget
		}
	}
}
