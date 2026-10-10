package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/indexqueue"
)

// Which index gets a queue in front of it is decided by the interface, not by
// the configured name — the same rule the write path follows. An exact index
// *is* a scan of the canonical rows, so there is nothing to defer and nothing
// to queue, and wrapping it would make the memory service believe it needed
// telling about writes.
func TestTheIndexQueueIsWiredOnlyWhereThereIsMaintenanceToDefer(t *testing.T) {
	for _, tc := range []struct {
		name  string
		index string
		async bool
		want  bool
	}{
		{"an approximate index with async indexing on", "hnsw", true, true},
		{"an approximate index with async indexing off", "hnsw", false, false},
		{"an exact index has nothing to defer", "flat", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := mustDepsWith(t, func(c *config.Config) {
				c.Vector.Index = tc.index
				c.Vector.AsyncIndexing = tc.async
			})
			if (d.indexQueue != nil) != tc.want {
				t.Fatalf("index queue present = %v, want %v", d.indexQueue != nil, tc.want)
			}
			if tc.want && d.vectors != vector.Index(d.indexQueue) {
				t.Error("the queue was built and not handed to anything; the write path still reaches the index directly")
			}
			// Whatever is wired, the write path's question must have the same
			// answer: the approximate index needs telling about writes and the
			// exact one does not.
			needsTelling := maintainerOf(d.vectors) != nil
			if needsTelling != (tc.index == "hnsw") {
				t.Errorf("vector.Maintainer satisfied = %v for index %q", needsTelling, tc.index)
			}
		})
	}
}

// A default approximate index keeps read-your-writes: nothing but the index
// name is configured, and a memory is the best match for its own content the
// moment its create returns, among decoys that mean other things. With the
// queue on, that search may miss the memory and say truncated: false, which is
// why the queue is not the default. The first assertion is the discriminating
// one: had the default been left on, d.indexQueue would exist and it fails.
func TestADefaultApproximateIndexKeepsReadYourWrites(t *testing.T) {
	d := mustDepsWith(t, func(c *config.Config) { c.Vector.Index = "hnsw" })
	if d.indexQueue != nil {
		t.Fatal("an approximate index with default settings has an index queue in front of it")
	}
	const tid = tenant.ID("acme")
	ctx := tenant.NewContext(context.Background(), tid)
	if _, err := tenant.Ensure(ctx, d.tenants, tid); err != nil {
		t.Fatal(err)
	}
	for _, decoy := range []string{
		"sourdough starter needs feeding twice a day",
		"the quarterly invoice was paid in full",
		"raft elects a leader by majority vote",
	} {
		if _, err := d.memories.Create(ctx, memory.CreateReq{Content: decoy}); err != nil {
			t.Fatal(err)
		}
	}
	const content = "the lighthouse keeper logged a storm at midnight"
	created, err := d.memories.Create(ctx, memory.CreateReq{Content: content})
	if err != nil {
		t.Fatal(err)
	}

	res, err := d.memories.Search(ctx, memory.SearchReq{Query: content, Type: memory.SearchSemantic, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 1 || res.Results[0].Memory.ID != created.ID {
		t.Fatalf("a search straight after the create did not return it first: %+v", res.Results)
	}
	if res.Truncated {
		t.Error("the search reported truncated over a healthy index")
	}
}

// With the queue on, an operation it cannot apply after the memory committed
// must not leave a tenant that reports healthy while its graph is missing the
// memory. The composition root hands the queue the same repair hook the index
// reports damage through, so the loss surfaces as a degraded index — searches
// say truncated — and as a durable vector.rebuild. A vector of another width is
// the apply failure here because the inner index refuses it whatever the
// queue's timing, so nothing about this test depends on a race.
func TestALostQueueOperationDegradesTheTenantAndQueuesARebuild(t *testing.T) {
	d := mustDepsWith(t, func(c *config.Config) {
		c.Vector.Index = "hnsw"
		c.Vector.AsyncIndexing = true
	})
	const tid = tenant.ID("acme")
	ctx := tenant.NewContext(context.Background(), tid)
	if _, err := tenant.Ensure(ctx, d.tenants, tid); err != nil {
		t.Fatal(err)
	}
	if _, err := d.memories.Create(ctx, memory.CreateReq{Content: "the lighthouse keeper logged a storm"}); err != nil {
		t.Fatal(err)
	}
	if err := d.indexQueue.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if h, err := d.vectors.Health(ctx, tid); err != nil || h.Degraded {
		t.Fatalf("before the loss Health = %+v, %v; want healthy", h, err)
	}

	if err := d.indexQueue.Indexed(ctx, tid, id.New(), []float32{1, 2, 3}); err != nil {
		t.Fatalf("the enqueue itself failed: %v", err)
	}
	if err := d.indexQueue.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := d.memories.Search(ctx, memory.SearchReq{
		Query: "lighthouse storm", Type: memory.SearchSemantic, Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Truncated {
		t.Error("a search over a tenant with a lost index operation reported truncated: false")
	}

	d.repairs.close()
	queued, err := d.queue.List(context.Background(), tid,
		jobs.Filter{Types: []jobs.Type{TypeVectorRebuild}, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 1 {
		t.Fatalf("the lost operation queued %d vector.rebuild jobs, want 1", len(queued))
	}
}

// The bound is one number in two packages. internal/config cannot import
// internal/vector — it is imported by everything and the vector packages pull
// in the storage layer — so the copy is checked here instead of left to drift.
func TestTheQueueDefaultsMatchTheIndexQueuePackage(t *testing.T) {
	if config.DefaultIndexQueueSize != indexqueue.DefaultSize {
		t.Errorf("config.DefaultIndexQueueSize = %d, indexqueue.DefaultSize = %d",
			config.DefaultIndexQueueSize, indexqueue.DefaultSize)
	}
}

// The shutdown drains. A server that stopped with operations still queued
// would leave tenants whose next search is served by a materialisation that
// has to insert them first — correct, and slower than finishing the work while
// the store is still open.
//
// What this must not do is ask the index. hnsw.Stats materialises the tenant,
// and a materialisation inserts every canonical vector that has no node record,
// so it answered "all of them are there" whether the drain had run or not: the
// first version of this test could not fail. The assertion that can is the
// queue's own apply meter, which counts one observation per operation handled
// and which no repair can add to. Measured with the drain removed, it reports
// 19 of 20.
//
// The node-record count is the weaker half and is kept as the end state rather
// than as the discriminator: it is read from the store instead of from the
// index, so the assertion itself repairs nothing — but a materialisation inside
// the queue's *first* apply does insert every canonical vector committed by
// then, which is why it alone could not tell a drained queue from an abandoned
// one.
func TestTheShutdownDrainsTheIndexQueue(t *testing.T) {
	d := mustDepsWith(t, func(c *config.Config) {
		c.Vector.Index = "hnsw"
		c.Vector.AsyncIndexing = true // off by default; this is the queue's test
	})
	const tid = tenant.ID("acme")
	ctx := tenant.NewContext(context.Background(), tid)
	if _, err := tenant.Ensure(ctx, d.tenants, tid); err != nil {
		t.Fatal(err)
	}
	const total = 20
	for n := range total {
		if _, err := d.memories.Create(ctx, memory.CreateReq{
			Content: fmt.Sprintf("a memory the index has not caught up with yet, number %d", n),
		}); err != nil {
			t.Fatal(err)
		}
	}

	d.closeVectors()

	if got := appliedOperations(t, d); got != total {
		t.Errorf("the queue applied %d of %d operations before its workers stopped", got, total)
	}
	if got := nodeRecords(t, d, tid); got != total {
		t.Errorf("the store holds %d node records of %d after the drain", got, total)
	}
}

// appliedOperations is how many index operations the queue handled, read from
// the apply-lag histogram it observes once per operation.
//
// The queue's own count rather than the index's: a materialisation can conjure
// node records, and it cannot conjure applies.
func appliedOperations(t *testing.T, d *deps) int {
	t.Helper()
	fams, err := d.metrics.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, fam := range fams {
		if fam.GetName() != "remem_vector_index_apply_lag_seconds" {
			continue
		}
		for _, met := range fam.Metric {
			total += int(met.Histogram.GetSampleCount())
		}
	}
	return total
}

// nodeRecords counts a tenant's HNSW node records in the store directly.
//
// Read through the iterator rather than through the index, because reading
// through the index is what repairs it.
func nodeRecords(t *testing.T, d *deps, tid tenant.ID) int {
	t.Helper()
	lower, upper := keys.SpaceRange(tid, tenant.DefaultNamespace, keys.SpaceVectorIndex)
	it := d.kv.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return n
}
