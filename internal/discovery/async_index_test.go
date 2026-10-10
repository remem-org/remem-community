package discovery_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector/indexes"
	"github.com/remem-org/remem-go/internal/vector/indexqueue"
)

// Discovery searches an index whose writes are applied off the request path, so
// it has to wait for them.
//
// internal/vector/indexqueue moved the HNSW insert off the write path, which
// widened the window between a memory's commit and its vector becoming
// searchable from microseconds to the queue's lag. Every other reader of the
// index tolerates that window: a user's search runs again. Discovery does not
// run again. It is handed a memory once, and a search over an index that has
// not yet applied the pair's inserts finds no candidates, writes no edges and
// reports success — two memories that qualify, never linked, with nothing
// anywhere saying so. That is the failure this pins.
func TestDiscoveryWaitsForQueuedIndexingBeforeSearching(t *testing.T) {
	w := newAsyncWorld(t)
	a, b := w.storeTwoParaphrasesAndQueueTheirInserts(t)

	must(t, w.runWhileTheQueueIsHeld(t, a, b))
	got := w.edgesBetween(t, a, b)

	// The control, kept from the review that found this: a second run over the
	// same pair, with the queue drained by hand first so that the control holds
	// whatever the run above did. It must add nothing — the first run already
	// linked them — and it is also what says the pair qualifies at all, so a
	// failure above cannot be read as "these two were never similar enough".
	must(t, w.queue.Flush(context.Background()))
	must(t, w.run(t, "acme", a, b))
	after := w.edgesBetween(t, a, b)

	if got != 1 {
		t.Fatalf("a discovery run that reported success left %d edges between two identical "+
			"paraphrases; a rerun over the settled index leaves %d", got, after)
	}
	if after != got {
		t.Fatalf("a second run changed the edge count from %d to %d; "+
			"the first run's edges are not what a rerun finds already written", got, after)
	}
}

// The wait is a real barrier and not a best-effort look, pinned separately
// because the test above would otherwise degrade into a race rather than fail:
// a run that does not wait still links the pair whenever the scheduler happens
// to apply the inserts first.
func TestDiscoveryDoesNotReturnWhileItsTenantsIndexingIsQueued(t *testing.T) {
	w := newAsyncWorld(t)
	a, b := w.storeTwoParaphrasesAndQueueTheirInserts(t)

	ran := w.startRun(t, a, b)
	select {
	case err := <-ran:
		t.Fatalf("the discovery run returned %v with its tenant's inserts still queued: "+
			"it searched an index that had been told nothing", err)
	case <-time.After(settleGrace):
	}

	w.release()
	must(t, w.wait(t, ran))
}

// settleGrace is how long a held queue is given to prove the run is waiting for
// it. It is generous for the work it bounds — two searches over an empty index
// — and it is only ever paid by a test that is about to fail.
const settleGrace = 100 * time.Millisecond

// asyncWorld is discovery over the composition the server runs: the index queue
// in front of the approximate index, with the index's applies held by the test.
//
// The approximate index rather than the harness's `flat` one, because only a
// maintained index has anything to defer and the queue refuses to wrap an exact
// one: flat *is* a scan of the canonical rows, so there is nothing to tell it
// and nothing to wait for. It is built through internal/vector/indexes rather
// than named directly, because internal/arch's rule says no package outside
// internal/vector may name HNSW — a test is still a package.
type asyncWorld struct {
	*harness
	queue *indexqueue.Queue
	block *blockingIndex

	releaseOnce sync.Once
}

func newAsyncWorld(t *testing.T) *asyncWorld {
	t.Helper()
	w := &asyncWorld{block: &blockingIndex{
		started: make(chan struct{}, 1),
		gate:    make(chan struct{}),
	}}
	w.harness = newHarness(t, func(d *discovery.Deps) {
		ix, err := indexes.Open(d.KV, indexes.Config{
			Kind: "hnsw", Metric: "l2", Neighbours: 16,
			BuildEffort: 200, SearchEffort: 64, ResidentBudgetBytes: 64 << 20,
		})
		must(t, err)
		// Warm the resident graph before either write, so that what the test
		// holds is the apply and not a first load behind it.
		_, err = ix.Search(context.Background(), "acme", unit(0), 10, nil)
		must(t, err)
		maintained, ok := ix.(indexqueue.Maintained)
		if !ok {
			t.Fatal("the approximate index does not need maintaining; the queue has nothing to wrap")
		}
		w.block.Maintained = maintained
		w.queue = indexqueue.New(w.block)
		d.Index = w.queue
	})
	// Released by the cleanup as well as by the body: a failure leaves a worker
	// inside the held apply, and a Close that waited for it would turn an
	// informative failure into a hung test.
	t.Cleanup(func() { w.release(); _ = w.queue.Close(context.Background()) })
	return w
}

// storeTwoParaphrasesAndQueueTheirInserts writes two memories with the same
// vector and enqueues both inserts, the second behind a worker already held
// inside the first.
//
// The memories are written through the repository rather than through
// internal/memory, so the queue holds exactly the two operations the test put
// there and the records are canonical either way.
func (w *asyncWorld) storeTwoParaphrasesAndQueueTheirInserts(t *testing.T) (a, b id.ID) {
	t.Helper()
	ctx := context.Background()
	first := w.store(t, "acme", "first paraphrase", unit(0))
	second := w.store(t, "acme", "second paraphrase", unit(0))
	must(t, w.queue.Indexed(ctx, "acme", first.rid, unit(0)))
	<-w.block.started
	must(t, w.queue.Indexed(ctx, "acme", second.rid, unit(0)))
	return first.rid, second.rid
}

// startRun drives the handler over the subjects the way a worker would, in a
// goroutine.
//
// A goroutine because a run that waits for the queue cannot be the thing that
// releases it: driven synchronously, the test would deadlock against its own
// barrier.
func (w *asyncWorld) startRun(t *testing.T, subjects ...id.ID) <-chan error {
	t.Helper()
	ran := make(chan error, 1)
	go func() { ran <- w.run(t, "acme", subjects...) }()
	return ran
}

// runWhileTheQueueIsHeld starts a run with the applies still held, releases
// them a beat later, and returns what the run returned.
//
// The beat is what makes the assertion afterwards mean something: a run that
// searched without waiting has, by the time the applies are released, already
// searched an index holding nothing. A run that waits is merely asleep for it.
func (w *asyncWorld) runWhileTheQueueIsHeld(t *testing.T, subjects ...id.ID) error {
	t.Helper()
	ran := w.startRun(t, subjects...)
	time.Sleep(settleGrace)
	w.release()
	return w.wait(t, ran)
}

// wait collects the run's error, failing rather than hanging when it never
// comes.
func (w *asyncWorld) wait(t *testing.T, ran <-chan error) error {
	t.Helper()
	select {
	case err := <-ran:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the discovery run never returned")
		return nil
	}
}

// release lets every held apply through. It is safe to call twice.
func (w *asyncWorld) release() { w.releaseOnce.Do(func() { close(w.block.gate) }) }

// edgesBetween is how many edges join the two memories, in either direction.
//
// One is the right answer, not two: similar_to is symmetric, and discovery
// checks both directions before it writes. These are the only two memories in
// the store, so their out-edges are the pair's.
func (w *asyncWorld) edgesBetween(t *testing.T, a, b id.ID) int {
	t.Helper()
	return len(w.outEdges(t, "acme", a)) + len(w.outEdges(t, "acme", b))
}

// blockingIndex is a maintained index whose Indexed waits for the test before
// it reaches the real one, which is how a test makes the queue's lag a span it
// controls rather than a microsecond it has to race.
type blockingIndex struct {
	indexqueue.Maintained
	// started reports that a worker is inside the first held apply. Buffered
	// and sent to without blocking: what the test waits for is the first one.
	started chan struct{}
	gate    chan struct{}
}

func (x *blockingIndex) Indexed(ctx context.Context, tid tenant.ID, rid id.ID, v []float32) error {
	select {
	case x.started <- struct{}{}:
	default:
	}
	<-x.gate
	return x.Maintained.Indexed(ctx, tid, rid, v)
}
