package indexqueue_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/indexes"
	"github.com/remem-org/remem-go/internal/vector/indexqueue"
)

const acme = tenant.ID("acme")

// TestARemoveAfterAnInsertLeavesNoNode is the ordering property, against the
// real approximate index: a create followed by a hard delete must not leave a
// searchable node behind. Applied in the wrong order the remove is a no-op —
// its post-condition already holds — and the insert that follows it leaves the
// memory findable for ever.
func TestARemoveAfterAnInsertLeavesNoNode(t *testing.T) {
	t.Parallel()
	q := realQueue(t)
	ctx := context.Background()
	rid := id.New()
	v := []float32{1, 0, 0, 0}

	if err := q.Indexed(ctx, acme, rid, vector.Single(v)); err != nil {
		t.Fatal(err)
	}
	if err := q.Removed(ctx, acme, rid); err != nil {
		t.Fatal(err)
	}
	if err := q.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	hits, err := q.Search(ctx, acme, v, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("the index still answers with %v; the remove was applied before the insert", hits)
	}
}

// The control for the test above: the same index, the same vector, without the
// remove. A queue that dropped everything would pass the ordering test.
func TestAnInsertThroughTheQueueBecomesSearchable(t *testing.T) {
	t.Parallel()
	q := realQueue(t)
	ctx := context.Background()
	rid := id.New()
	v := []float32{1, 0, 0, 0}

	if err := q.Indexed(ctx, acme, rid, vector.Single(v)); err != nil {
		t.Fatal(err)
	}
	if err := q.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	hits, err := q.Search(ctx, acme, v, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].ID != rid {
		t.Fatalf("searching after a flush found %v, want the one record enqueued", hits)
	}
}

// One tenant's operations apply in the order they were enqueued, whatever the
// worker count: a tenant is sharded onto one worker and that worker has one
// FIFO. A per-operation goroutine, or a shard key that is not the tenant,
// fails here.
func TestOneTenantsOperationsApplyInEnqueueOrder(t *testing.T) {
	t.Parallel()
	r := &recorder{}
	q := indexqueue.New(r, indexqueue.WithWorkers(4), indexqueue.WithSize(8))
	t.Cleanup(func() { _ = q.Close(context.Background()) })
	ctx := context.Background()

	var want []string
	for n := range 100 {
		rid := id.New()
		if err := q.Indexed(ctx, acme, rid, vector.Single([]float32{float32(n)})); err != nil {
			t.Fatal(err)
		}
		want = append(want, "index/acme/"+rid.String())
		if err := q.Removed(ctx, acme, rid); err != nil {
			t.Fatal(err)
		}
		want = append(want, "remove/acme/"+rid.String())
	}
	if err := q.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	got := r.snapshot()
	if len(got) != len(want) {
		t.Fatalf("%d operations applied, %d enqueued", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("operation %d applied as %s, enqueued as %s", i, got[i], want[i])
		}
	}
}

// A full queue is backpressure, not loss: the caller waits for a worker.
func TestAFullQueueBlocksUntilAWorkerDrains(t *testing.T) {
	t.Parallel()
	r := &recorder{gate: make(chan struct{})}
	q := indexqueue.New(r, indexqueue.WithWorkers(1), indexqueue.WithSize(1))
	t.Cleanup(func() { r.open(); _ = q.Close(context.Background()) })
	ctx := context.Background()

	// One operation reaches the worker and stops at the gate, a second fills
	// the single buffer slot, and the third has nowhere to go.
	if err := q.Indexed(ctx, acme, id.New(), vector.Single([]float32{1})); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the worker to take the first operation", func() bool { return r.started() >= 1 })
	if err := q.Indexed(ctx, acme, id.New(), vector.Single([]float32{1})); err != nil {
		t.Fatal(err)
	}

	blocked := make(chan error, 1)
	go func() { blocked <- q.Indexed(ctx, acme, id.New(), vector.Single([]float32{1})) }()
	select {
	case err := <-blocked:
		t.Fatalf("the third operation returned %v on a full queue; it must wait for a worker", err)
	case <-time.After(100 * time.Millisecond):
	}

	r.open()
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("the third operation failed once the queue drained: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the third operation never returned after the workers drained")
	}
}

// Backpressure is bounded by the caller's context: a request that is going
// away must not be held by a queue that is not moving.
func TestEnqueueOnAFullQueueRespectsContextCancellation(t *testing.T) {
	t.Parallel()
	r := &recorder{gate: make(chan struct{})}
	q := indexqueue.New(r, indexqueue.WithWorkers(1), indexqueue.WithSize(1))
	t.Cleanup(func() { r.open(); _ = q.Close(context.Background()) })

	bg := context.Background()
	if err := q.Indexed(bg, acme, id.New(), vector.Single([]float32{1})); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the worker to take the first operation", func() bool { return r.started() >= 1 })
	if err := q.Indexed(bg, acme, id.New(), vector.Single([]float32{1})); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(bg)
	blocked := make(chan error, 1)
	go func() { blocked <- q.Indexed(ctx, acme, id.New(), vector.Single([]float32{1})) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-blocked:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("a cancelled enqueue returned %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a cancelled enqueue never returned")
	}
}

// Flush is what makes the queue testable and the shutdown correct: it returns
// only once everything enqueued before it was called has been applied.
func TestFlushReturnsOnlyAfterEverythingEnqueuedIsApplied(t *testing.T) {
	t.Parallel()
	r := &recorder{gate: make(chan struct{})}
	q := indexqueue.New(r, indexqueue.WithWorkers(2), indexqueue.WithSize(32))
	t.Cleanup(func() { r.open(); _ = q.Close(context.Background()) })
	ctx := context.Background()

	const total = 16
	for range total {
		if err := q.Indexed(ctx, acme, id.New(), vector.Single([]float32{1})); err != nil {
			t.Fatal(err)
		}
	}

	flushed := make(chan error, 1)
	go func() { flushed <- q.Flush(ctx) }()
	select {
	case err := <-flushed:
		t.Fatalf("Flush returned %v with %d of %d operations applied", err, r.count(), total)
	case <-time.After(100 * time.Millisecond):
	}

	r.open()
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatalf("Flush: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Flush never returned")
	}
	if got := r.count(); got != total {
		t.Fatalf("Flush returned with %d of %d operations applied", got, total)
	}
}

// A rebuild never races the operations still queued for its tenant: it drains
// them first.
//
// A rebuild replaces the tenant's node records wholesale. An insert that
// arrived before it and applied after it would be an edit to a structure the
// rebuild had already declared finished — survivable, since the canonical
// vector is what membership is read from, and still a graph nobody asked for.
// Draining first makes the rebuild's input the state the caller saw.
func TestARebuildDrainsItsTenantFirst(t *testing.T) {
	t.Parallel()
	r := &recorder{gate: make(chan struct{})}
	q := indexqueue.New(r, indexqueue.WithWorkers(1), indexqueue.WithSize(8))
	// The gate is opened by the cleanup as well as by the body: a failure
	// leaves a worker at the gate, and a Close that waited for it would turn
	// an informative failure into a hung test.
	t.Cleanup(func() { r.open(); _ = q.Close(context.Background()) })
	ctx := context.Background()

	rid := id.New()
	if err := q.Indexed(ctx, acme, rid, vector.Single([]float32{1})); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the worker to take the insert", func() bool { return r.started() >= 1 })

	rebuilt := make(chan error, 1)
	go func() { rebuilt <- q.Rebuild(ctx, acme, nil) }()
	select {
	case err := <-rebuilt:
		t.Fatalf("Rebuild returned %v with the tenant's insert still queued", err)
	case <-time.After(100 * time.Millisecond):
	}
	if got := r.snapshot(); len(got) != 0 {
		t.Fatalf("the index was asked to %v before its queued work was applied", got)
	}

	r.open()
	select {
	case err := <-rebuilt:
		if err != nil {
			t.Fatalf("Rebuild: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Rebuild never returned")
	}
	want := []string{"index/acme/" + rid.String(), "rebuild/acme"}
	got := r.snapshot()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("the index saw %v, want %v", got, want)
	}
}

// An operation that fails is counted and logged and is not the caller's
// problem: the canonical vector is committed either way, and the next
// materialisation inserts the node record this lost.
func TestAFailedOperationIsCountedAndLoggedAndNotReturned(t *testing.T) {
	t.Parallel()
	r := &recorder{fail: errors.New("the node record could not be committed")}
	m := obs.NewMetrics()
	q := indexqueue.New(r, indexqueue.WithWorkers(1), indexqueue.WithMetrics(&m.Vector))
	t.Cleanup(func() { _ = q.Close(context.Background()) })

	logs := &syncBuffer{}
	ctx := obs.WithLogger(context.Background(), obs.LoggerTo(logs))
	rid := id.New()
	if err := q.Indexed(ctx, acme, rid, vector.Single([]float32{1})); err != nil {
		t.Fatalf("the failure surfaced to the caller as %v; a create must not fail over its index", err)
	}
	if err := q.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Pinned to the kind, not summed over them: an operation the index refused
	// and one a cancelled caller or a shutdown never got to apply are
	// different events, and an assertion on the total would pass on either.
	if got := failures(t, m, "apply"); got != 1 {
		t.Errorf("remem_vector_index_failures_total{kind=\"apply\"} = %v, want 1", got)
	}
	if got := familyTotal(t, m, "remem_vector_index_failures_total"); got != 1 {
		t.Errorf("%v failures were counted in all, want the one apply", got)
	}
	line := logs.String()
	for _, want := range []string{"WARN", string(acme), rid.String()} {
		if !strings.Contains(line, want) {
			t.Errorf("the log line does not name %q:\n%s", want, line)
		}
	}
	// The apply lag is observed whatever the operation's outcome: how far
	// behind the queue is running is not a question about failures.
	if got := familyTotal(t, m, "remem_vector_index_apply_lag_seconds"); got != 1 {
		t.Errorf("remem_vector_index_apply_lag_seconds counted %v observations, want 1", got)
	}
	if !hasFamily(t, m, "remem_vector_index_queue_depth") {
		t.Error("remem_vector_index_queue_depth was never set")
	}
}

// Close drains, within the deadline the caller gives it, and stops the workers.
func TestCloseDrainsWithinItsDeadline(t *testing.T) {
	t.Parallel()
	r := &recorder{}
	q := indexqueue.New(r, indexqueue.WithWorkers(2), indexqueue.WithSize(64))
	ctx := context.Background()

	const total = 50
	for range total {
		if err := q.Indexed(ctx, acme, id.New(), vector.Single([]float32{1})); err != nil {
			t.Fatal(err)
		}
	}
	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := q.Close(deadline); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := r.count(); got != total {
		t.Fatalf("Close returned with %d of %d operations applied", got, total)
	}
	// A closed queue refuses rather than blocks: the workers are gone and
	// nothing would ever apply the operation.
	if err := q.Indexed(ctx, acme, id.New(), vector.Single([]float32{1})); !errs.Is(err, errs.Unavailable) {
		t.Fatalf("enqueueing into a closed queue returned %v, want Unavailable", err)
	}
	// Closing twice is not an error: a shutdown path that is entered twice
	// must not panic on a closed channel.
	if err := q.Close(ctx); err != nil {
		t.Fatalf("the second Close: %v", err)
	}
}

// A Close whose deadline has already passed still stops the workers. The
// operations it abandons are node records a materialisation re-inserts.
func TestCloseWithNoTimeLeftStillStopsTheWorkers(t *testing.T) {
	t.Parallel()
	r := &recorder{gate: make(chan struct{})}
	q := indexqueue.New(r, indexqueue.WithWorkers(1), indexqueue.WithSize(8))
	ctx := context.Background()
	for range 4 {
		if err := q.Indexed(ctx, acme, id.New(), vector.Single([]float32{1})); err != nil {
			t.Fatal(err)
		}
	}
	expired, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- q.Close(expired) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Close reported a clean drain although its deadline had passed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close never returned; the workers were not stopped")
	}
	r.open()
}

// --- helpers ----------------------------------------------------------------

// realQueue wraps the approximate index over an in-memory store: the
// composition the server runs, without the server.
func realQueue(t *testing.T) *indexqueue.Queue {
	t.Helper()
	m := obs.NewMetrics()
	q := indexqueue.New(realIndex(t), indexqueue.WithMetrics(&m.Vector))
	t.Cleanup(func() { _ = q.Close(context.Background()) })
	return q
}

// realIndex is that approximate index on its own, for a test that has to wrap
// it in something of its own before the queue wraps it.
func realIndex(t *testing.T) indexqueue.Maintained {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	idx, err := indexes.Open(kv, indexes.Config{
		Kind: "hnsw", Metric: "cosine", Neighbours: 16,
		BuildEffort: 200, SearchEffort: 64, ResidentBudgetBytes: 64 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	maintained, ok := idx.(indexqueue.Maintained)
	if !ok {
		t.Fatal("the approximate index does not need maintaining; the queue has nothing to wrap")
	}
	return maintained
}

// recorder is a maintained index that records what it was told, in order, and
// can be made to block or to fail.
type recorder struct {
	// gate, when not nil, holds every operation until open is called.
	gate chan struct{}
	// fail is what every operation returns.
	fail error

	begun    atomic.Int64
	openOnce sync.Once

	mu   sync.Mutex
	done []string
}

func (r *recorder) Indexed(_ context.Context, t tenant.ID, rid id.ID, _ []vector.Entry) error {
	return r.apply("index", t, rid)
}

func (r *recorder) Removed(_ context.Context, t tenant.ID, rid id.ID) error {
	return r.apply("remove", t, rid)
}

func (r *recorder) apply(what string, t tenant.ID, rid id.ID) error {
	r.begun.Add(1)
	if r.gate != nil {
		<-r.gate
	}
	r.mu.Lock()
	r.done = append(r.done, fmt.Sprintf("%s/%s/%s", what, t, rid))
	r.mu.Unlock()
	return r.fail
}

// open releases everything waiting at the gate, and is safe to call twice: a
// test's cleanup runs whether or not the body already opened it.
func (r *recorder) open() {
	if r.gate == nil {
		return
	}
	r.openOnce.Do(func() { close(r.gate) })
}

func (r *recorder) started() int { return int(r.begun.Load()) }

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.done)
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.done...)
}

func (r *recorder) Insert(context.Context, tenant.ID, id.ID, []vector.Entry) error {
	return nil
}
func (r *recorder) Delete(context.Context, tenant.ID, id.ID) error { return nil }

func (r *recorder) Search(context.Context, tenant.ID, []float32, int, vector.Filter) ([]vector.Hit, error) {
	return nil, nil
}

// Rebuild records itself and is deliberately not gated: what the ordering test
// watches is when the queue lets it reach the index, not how long it takes.
func (r *recorder) Rebuild(_ context.Context, t tenant.ID, _ vector.Source, _ ...vector.RebuildOption) error {
	r.mu.Lock()
	r.done = append(r.done, "rebuild/"+string(t))
	r.mu.Unlock()
	return nil
}

func (r *recorder) Stats(context.Context, tenant.ID) (vector.Stats, error) {
	return vector.Stats{Vectors: r.count()}, nil
}

func (r *recorder) Health(context.Context, tenant.ID) (vector.Health, error) {
	return vector.Health{}, nil
}

// syncBuffer is a strings.Builder a worker goroutine may write to while the
// test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// familyTotal sums every sample in a counter or gauge family, and counts the
// observations in a histogram one. The registry is read directly because
// client_golang's testutil would be a new dependency for one assertion.
func familyTotal(t *testing.T, m *obs.Metrics, name string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, met := range fam.Metric {
			switch {
			case met.Counter != nil:
				total += met.Counter.GetValue()
			case met.Histogram != nil:
				total += float64(met.Histogram.GetSampleCount())
			case met.Gauge != nil:
				total += met.Gauge.GetValue()
			}
		}
	}
	return total
}

func hasFamily(t *testing.T, m *obs.Metrics, name string) bool {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, fam := range families {
		if fam.GetName() == name {
			return true
		}
	}
	return false
}
