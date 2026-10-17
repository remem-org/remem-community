package indexqueue_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/indexqueue"
)

// An operation the queue cannot apply leaves a committed memory out of the
// graph, and nothing would put it back until the tenant is next materialised —
// an eviction or a restart away. So a loss is made visible the way any damaged
// index is: the tenant reports degraded, its searches say truncated, and a
// durable rebuild is scheduled through the same hook the index reports damage
// through. These tests pin each half.

// rebuilds counts what the queue asked to have rebuilt.
type rebuilds struct {
	mu      sync.Mutex
	tenants []tenant.ID
}

func (r *rebuilds) ScheduleRebuild(t tenant.ID, _ string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tenants = append(r.tenants, t)
}

func (r *rebuilds) count(t tenant.ID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, got := range r.tenants {
		if got == t {
			n++
		}
	}
	return n
}

func degraded(t *testing.T, q *indexqueue.Queue, tn tenant.ID) vector.Health {
	t.Helper()
	h, err := q.Health(context.Background(), tn)
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	return h
}

func TestAnApplyFailureDegradesTheTenantAndSchedulesARebuild(t *testing.T) {
	t.Parallel()
	r := &recorder{fail: errors.New("the node record could not be committed")}
	rb := &rebuilds{}
	q := indexqueue.New(r, indexqueue.WithWorkers(1), indexqueue.WithRebuilder(rb))
	t.Cleanup(func() { _ = q.Close(context.Background()) })

	if err := q.Indexed(context.Background(), acme, id.New(), vector.Single([]float32{1})); err != nil {
		t.Fatal(err)
	}
	if err := q.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	h := degraded(t, q, acme)
	if !h.Degraded || h.Reason == "" {
		t.Fatalf("after a lost operation Health = %+v, want degraded with a reason", h)
	}
	if got := rb.count(acme); got != 1 {
		t.Errorf("%d rebuilds were scheduled for the tenant, want 1", got)
	}
	if other := degraded(t, q, "globex"); other.Degraded {
		t.Errorf("another tenant reports degraded over acme's loss: %+v", other)
	}
}

// The case the queue adds: a caller that gives up while its committed write
// waits on a full shard. Its enqueue is refused after the record is stored.
func TestARefusedEnqueueDegradesTheTenantAndSchedulesARebuild(t *testing.T) {
	t.Parallel()
	r := &recorder{gate: make(chan struct{})}
	rb := &rebuilds{}
	q := indexqueue.New(r, indexqueue.WithWorkers(1), indexqueue.WithSize(1), indexqueue.WithRebuilder(rb))
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
	cancel()
	if err := q.Indexed(ctx, acme, id.New(), vector.Single([]float32{1})); err == nil {
		t.Fatal("an enqueue on a full shard with a cancelled caller was accepted")
	}

	if h := degraded(t, q, acme); !h.Degraded {
		t.Fatalf("after a refused enqueue Health = %+v, want degraded", h)
	}
	if got := rb.count(acme); got != 1 {
		t.Errorf("%d rebuilds were scheduled for the tenant, want 1", got)
	}
}

func TestACompletedRebuildClearsTheMark(t *testing.T) {
	t.Parallel()
	r := &recorder{fail: errors.New("refused")}
	q := indexqueue.New(r, indexqueue.WithWorkers(1), indexqueue.WithRebuilder(&rebuilds{}))
	t.Cleanup(func() { _ = q.Close(context.Background()) })

	if err := q.Indexed(context.Background(), acme, id.New(), vector.Single([]float32{1})); err != nil {
		t.Fatal(err)
	}
	if err := q.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !degraded(t, q, acme).Degraded {
		t.Fatal("the loss did not degrade the tenant")
	}
	if err := q.Rebuild(context.Background(), acme, nil); err != nil {
		t.Fatal(err)
	}
	if h := degraded(t, q, acme); h.Degraded {
		t.Fatalf("after a completed rebuild Health = %+v, want healthy", h)
	}
}

// lossDuringRebuild is a maintained index whose rebuild runs a hook in the
// middle, so a test can lose an operation while the repair is in progress.
type lossDuringRebuild struct {
	*recorder
	during func()
}

func (l *lossDuringRebuild) Rebuild(ctx context.Context, t tenant.ID, src vector.Source, opts ...vector.RebuildOption) error {
	l.during()
	return l.recorder.Rebuild(ctx, t, src, opts...)
}

func TestALossDuringARebuildKeepsTheMark(t *testing.T) {
	t.Parallel()
	l := &lossDuringRebuild{recorder: &recorder{fail: errors.New("refused")}}
	q := indexqueue.New(l, indexqueue.WithWorkers(1), indexqueue.WithRebuilder(&rebuilds{}))
	t.Cleanup(func() { _ = q.Close(context.Background()) })
	l.during = func() {
		// The rebuild runs on the caller's goroutine and the worker is free, so
		// this operation applies — and fails — while the repair is underway.
		if err := q.Indexed(context.Background(), acme, id.New(), vector.Single([]float32{1})); err != nil {
			t.Error(err)
		}
		if err := q.Flush(context.Background()); err != nil {
			t.Error(err)
		}
	}

	if err := q.Indexed(context.Background(), acme, id.New(), vector.Single([]float32{1})); err != nil {
		t.Fatal(err)
	}
	if err := q.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := q.Rebuild(context.Background(), acme, nil); err != nil {
		t.Fatal(err)
	}
	if h := degraded(t, q, acme); !h.Degraded {
		t.Fatalf("a loss during the rebuild was cleared by it: Health = %+v", h)
	}
}

// What a shutdown leaves undone is not marked: the mark would not survive the
// process, and the restart's materialisation inserts every canonical vector
// with no node record anyway. Two cases. An enqueue after Close is refused
// deterministically. An operation accepted while Close races it may be
// abandoned by its worker, which only a race can produce — the loop is the one
// TestAnAcceptedOperationIsNeverLostSilentlyWhenCloseRacesIt runs, and the
// assertion holds whenever the race happened.
func TestWhatAShutdownLeavesUndoneDoesNotDegrade(t *testing.T) {
	t.Parallel()

	rb := &rebuilds{}
	q := indexqueue.New(&recorder{}, indexqueue.WithWorkers(1), indexqueue.WithRebuilder(rb))
	if err := q.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := q.Indexed(context.Background(), acme, id.New(), vector.Single([]float32{1})); err == nil {
		t.Fatal("an enqueue after Close was accepted")
	}
	if got := rb.count(acme); got != 0 {
		t.Errorf("an enqueue refused by a stopped queue scheduled %d rebuilds", got)
	}
	if h := degraded(t, q, acme); h.Degraded {
		t.Errorf("an enqueue refused by a stopped queue degraded the tenant: %+v", h)
	}

	m := obs.NewMetrics()
	const iterations, perIteration = 2000, 4
	marked := 0
	for range iterations {
		rb := &rebuilds{}
		q := indexqueue.New(&recorder{}, indexqueue.WithWorkers(1), indexqueue.WithSize(8),
			indexqueue.WithMetrics(&m.Vector), indexqueue.WithRebuilder(rb))
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perIteration {
				_ = q.Indexed(context.Background(), acme, id.New(), vector.Single([]float32{1}))
			}
		}()
		_ = q.Close(context.Background())
		wg.Wait()
		marked += rb.count(acme)
	}
	abandoned := int(failures(t, m, "shutdown"))
	t.Logf("%d operations abandoned by a racing shutdown over %d iterations", abandoned, iterations)
	if marked != 0 {
		t.Errorf("a racing shutdown scheduled %d rebuilds (%d abandoned); none should be", marked, abandoned)
	}
}
