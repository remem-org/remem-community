package query

import (
	"math/rand/v2"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/vector/distance"
)

// Plan item 18: a filtered search settles its candidates from the row cache when
// one is in front of it, so a second query over an unchanged corpus pays no
// attribute-row reads at all.
//
// It is stated against the executor rather than against the cache because the
// seam is the thing that can be lost: [attr.ReadRow] takes the cache when the
// reader has one, and a settle that went back to r.Get plus attr.DecodeRow would
// still return the right page while paying for every row again.
func TestTheExecutorSettlesAFilteredSearchFromTheRowCache(t *testing.T) {
	c := newWidenCorpus(t, 3, 300, 8, 64, 5)
	cache := attr.NewRowCache(4096)
	q := c.query(rand.New(rand.NewPCG(13, 13)), 10, true)
	plan := c.plan(t, q)
	e := NewExecutor(c.slots, c.index, distance.L2)

	// Each round is a fresh request over the same corpus: its own reader, its own
	// generation taken before it starts, and the shared cache behind both.
	round := func() (reads int, hits, misses uint64) {
		counting := newCountingReader(c.kv)
		r := attr.NewCachedReader(counting, cache, c.slots, widenTenant, cache.Generation(widenTenant))
		if _, err := e.runVector(c.ctx, r, q, plan, Step{Kind: StepVector, Source: SourceVector}); err != nil {
			t.Fatalf("running the vector step: %v", err)
		}
		r.Report()
		total, _ := counting.total()
		h, m, _ := cache.Stats()
		return total, h, m
	}

	first, _, misses := round()
	if first == 0 {
		t.Fatal("the first query read no attribute rows at all; the fixture is meant to filter")
	}
	if misses == 0 {
		t.Fatal("the first query reported no cache misses; the executor did not use the cache")
	}

	second, hits, _ := round()
	if second != 0 {
		t.Errorf("a second query over an unchanged corpus read %d attribute rows from the "+
			"store; the cache holds every one of them", second)
	}
	if hits == 0 {
		t.Error("the second query reported no cache hits")
	}
}

// And the same query must answer identically with and without the cache. A cache
// that changes an answer is worse than no cache.
func TestACachedSettleReturnsTheSamePage(t *testing.T) {
	c := newWidenCorpus(t, 4, 300, 8, 64, 5)
	q := c.query(rand.New(rand.NewPCG(17, 17)), 10, true)
	plan := c.plan(t, q)
	e := NewExecutor(c.slots, c.index, distance.L2)

	bare, err := e.runVector(c.ctx, c.kv, q, plan, Step{Kind: StepVector, Source: SourceVector})
	if err != nil {
		t.Fatalf("running without a cache: %v", err)
	}

	cache := attr.NewRowCache(4096)
	for round := range 2 {
		r := attr.NewCachedReader(c.kv, cache, c.slots, widenTenant, cache.Generation(widenTenant))
		cached, err := e.runVector(c.ctx, r, q, plan, Step{Kind: StepVector, Source: SourceVector})
		if err != nil {
			t.Fatalf("round %d with a cache: %v", round, err)
		}
		r.Report()
		if len(cached.items) != len(bare.items) {
			t.Fatalf("round %d returned %d results, want the uncached %d",
				round, len(cached.items), len(bare.items))
		}
		for i := range bare.items {
			if cached.items[i].ID != bare.items[i].ID {
				t.Fatalf("round %d position %d is %s, want the uncached %s",
					round, i, cached.items[i].ID, bare.items[i].ID)
			}
		}
		if cached.examined != bare.examined {
			t.Errorf("round %d examined %d candidates, want the uncached %d",
				round, cached.examined, bare.examined)
		}
	}
}
