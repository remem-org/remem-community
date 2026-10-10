package memory_test

import (
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

// A listing reads one attribute row per candidate it examines. A second listing
// over an unchanged corpus reads none: the rows the first one decoded are still
// the rows this one's snapshot holds, and the shared cache says so.
func TestASecondListingSettlesItsCandidatesFromTheRowCache(t *testing.T) {
	cache := attr.NewRowCache(4096)
	h := newCachedListHarness(t, cache)
	h.seedGradient(t, 40)

	req := memory.ListReq{OrderBy: "importance", Desc: true, Limit: 10}

	h.counter.reset()
	first, err := h.svc.List(acmeCtx(), req)
	must(t, err)
	firstRows := h.counter.rows
	if firstRows == 0 {
		t.Fatal("the first listing read no attribute rows; it settles a predicate per candidate")
	}

	h.counter.reset()
	second, err := h.svc.List(acmeCtx(), req)
	must(t, err)
	if h.counter.rows != 0 {
		t.Errorf("the second listing read %d attribute rows where the first read %d; "+
			"the corpus did not change, so the cache holds every one of them",
			h.counter.rows, firstRows)
	}
	if len(second.Memories) != len(first.Memories) {
		t.Fatalf("the second listing returned %d memories, want the first's %d",
			len(second.Memories), len(first.Memories))
	}
	for i := range first.Memories {
		if second.Memories[i].ID != first.Memories[i].ID {
			t.Fatalf("position %d is %s, want the first listing's %s",
				i, second.Memories[i].ID, first.Memories[i].ID)
		}
	}
	if hits, _, _ := cache.Stats(); hits == 0 {
		t.Error("the cache reported no hits")
	}
}

// And one write puts the tenant back to reading rows: the generation moved, so
// every entry it holds is retired. This is the trade the cache makes, stated as a
// test so that a later change cannot quietly weaken it into "mostly fresh".
func TestAWriteRetiresTheTenantsCachedRows(t *testing.T) {
	cache := attr.NewRowCache(4096)
	h := newCachedListHarness(t, cache)
	h.seedGradient(t, 40)

	req := memory.ListReq{OrderBy: "importance", Desc: true, Limit: 10}
	if _, err := h.svc.List(acmeCtx(), req); err != nil {
		t.Fatal(err)
	}

	h.advance()
	h.write(t, "one more memory", 0.99, 99, h.now())

	h.counter.reset()
	res, err := h.svc.List(acmeCtx(), req)
	must(t, err)
	if h.counter.rows == 0 {
		t.Error("a listing after a write read no attribute rows; it was served from a cache " +
			"the write should have retired")
	}
	if len(res.Memories) == 0 {
		t.Fatal("the listing after a write returned nothing")
	}
	if got := res.Memories[0].Content; got != "one more memory" {
		t.Errorf("the most important memory is %q; the write is not visible", got)
	}
}

// Nothing is cached when the knob is zero, and the listing is unchanged by it.
func TestARowCacheOfZeroChangesNothing(t *testing.T) {
	cache := attr.NewRowCache(0)
	h := newCachedListHarness(t, cache)
	h.seedGradient(t, 20)

	req := memory.ListReq{OrderBy: "importance", Desc: true, Limit: 5}
	if _, err := h.svc.List(acmeCtx(), req); err != nil {
		t.Fatal(err)
	}
	h.counter.reset()
	res, err := h.svc.List(acmeCtx(), req)
	must(t, err)
	if h.counter.rows == 0 {
		t.Error("a listing with the cache off read no attribute rows")
	}
	if len(res.Memories) != 5 {
		t.Errorf("the listing returned %d memories, want 5", len(res.Memories))
	}
	if n := cache.Len(); n != 0 {
		t.Errorf("a disabled cache holds %d entries", n)
	}
}

// newCachedListHarness is newListHarness with one row cache behind both the write
// path's indexer and the read path's service, which is how internal/server wires
// it. Two caches, or one on only one side, would be a cache nothing invalidates.
func newCachedListHarness(t *testing.T, cache *attr.RowCache) *listHarness {
	t.Helper()
	base := memkv.New()
	t.Cleanup(func() { _ = base.Close() })

	counter := &countingKV{KV: base}
	clk := clock.NewFake(clock.FakeStart)
	embedder := embeddingtest.New()
	repo := record.NewRepo(counter,
		record.WithIndexer(attr.NewIndexer(attr.MustTable(), attr.WithRowCache(cache))))
	svc := memory.New(counter, repo, flat.New(vector.NewStore(counter), distance.L2),
		embedder, clk, attr.MustTable(), graph.NewService(counter, clk), memory.Config{},
		memory.WithRowCache(cache))

	t.Cleanup(func() { _ = svc.Close() })
	return &listHarness{
		svc: svc, repo: repo, kv: counter, counter: counter,
		clk: clk, embedder: embedder, tick: time.Second,
	}
}
