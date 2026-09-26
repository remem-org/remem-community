package jobs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
)

func TestThePoolRunsAJobAndCompletesIt(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 2)

	var ran atomic.Int32
	h.register("vector.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error {
		ran.Add(1)
		return nil
	})
	j := h.submit("vector.rebuild")

	h.pollAndWait(ctx)
	if ran.Load() != 1 {
		t.Fatalf("the handler ran %d times, want 1", ran.Load())
	}
	if got := h.get(ctx, j); got.State != jobs.Completed {
		t.Fatalf("the job ended as %s: %q", got.State, got.LastError)
	}
}

func TestOneHundredJobsAcrossSixteenWorkersRunExactlyOnce(t *testing.T) {
	// The phase's completion criterion, driven through the pool rather than
	// through Claim: no lost work, no duplicated work, under -race.
	ctx := context.Background()
	h := newHarness(t, 16)

	var mu sync.Mutex
	seen := map[string]int{}
	h.register("vector.rebuild", func(_ context.Context, j *jobs.Job, _ jobs.Checkpointer) error {
		mu.Lock()
		seen[j.ID.String()]++
		mu.Unlock()
		return nil
	})
	const total = 100
	for range total {
		h.submit("vector.rebuild")
	}

	for range 200 {
		h.pollAndWait(ctx)
		mu.Lock()
		done := len(seen)
		mu.Unlock()
		if done == total {
			break
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != total {
		t.Fatalf("%d of %d jobs ran", len(seen), total)
	}
	for jid, n := range seen {
		if n != 1 {
			t.Errorf("job %s ran %d times", jid, n)
		}
	}
	if n := h.count(ctx, jobs.Completed); n != total {
		t.Fatalf("%d jobs are completed, want %d", n, total)
	}
}

func TestPanicInHandlerFailsOnlyTheJob(t *testing.T) {
	// A panicking handler must not take the process down with it. The job is
	// what fails, and it says what happened, because a job that vanished with
	// the goroutine is work nothing will ever look at again.
	ctx := context.Background()
	h := newHarness(t, 2)

	h.register("vector.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error {
		panic("the index was not where it said it was")
	})
	var healthy atomic.Bool
	h.register("text.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error {
		healthy.Store(true)
		return nil
	})

	bad := h.submitWith("vector.rebuild", func(j *jobs.Job) { j.MaxAttempts = 1 })
	h.submit("text.rebuild")

	h.pollAndWait(ctx)

	got := h.get(ctx, bad)
	if got.State != jobs.Failed {
		t.Fatalf("the panicking job ended as %s", got.State)
	}
	if got.LastError == "" {
		t.Fatal("the failed job does not record the panic")
	}
	if !healthy.Load() {
		t.Fatal("the pool stopped running other work after a panic")
	}
}

func TestAHandlerThatFailsIsRetriedAndThenGivesUp(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 2)

	var runs atomic.Int32
	h.register("vector.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error {
		runs.Add(1)
		return errors.New("the store was busy")
	})
	j := h.submitWith("vector.rebuild", func(j *jobs.Job) { j.MaxAttempts = 3 })

	for range 3 {
		h.pollAndWait(ctx)
		h.clk.Advance(jobs.RetryCap)
	}
	if runs.Load() != 3 {
		t.Fatalf("the handler ran %d times, want 3", runs.Load())
	}
	got := h.get(ctx, j)
	if got.State != jobs.Failed || got.LastError != "the store was busy" {
		t.Fatalf("the job ended as %s / %q", got.State, got.LastError)
	}
}

func TestTheLeaseIsRenewedUnderALongHandler(t *testing.T) {
	// A handler that outlives its lease would have its job reclaimed and run
	// twice while the first run is still going. Renewal is what keeps a slow
	// job from being duplicated by the framework that is supposed to run it.
	ctx := context.Background()
	h := newHarness(t, 2)

	release := make(chan struct{})
	started := make(chan struct{})
	h.register("text.rebuild", func(hctx context.Context, _ *jobs.Job, _ jobs.Checkpointer) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-hctx.Done():
			return hctx.Err()
		}
	})
	j := h.submit("text.rebuild")

	stop := h.start(ctx)
	<-started

	// Push the clock well past the original lease, letting the renewal ticker
	// fire on the way, then prove nothing reclaimed the job.
	for range 6 {
		h.clk.Advance(h.queue.Lease() / 2)
		h.settle()
	}
	if n, err := h.queue.Reclaim(ctx, acme, 10); err != nil || n != 0 {
		t.Fatalf("a renewed job was reclaimed while its handler was still running: %d, %v", n, err)
	}

	close(release)
	stop()
	if got := h.get(ctx, j); got.State != jobs.Completed {
		t.Fatalf("the long job ended as %s", got.State)
	}
}

func TestShutdownDrainsInFlightJobs(t *testing.T) {
	// Cancellation lets a running job checkpoint before the process exits, and
	// the job goes back to the queue rather than being charged a failure: a
	// process stopping is not a job failing.
	ctx := context.Background()
	h := newHarness(t, 2)

	started := make(chan struct{})
	h.register("text.rebuild", func(hctx context.Context, j *jobs.Job, cp jobs.Checkpointer) error {
		close(started)
		<-hctx.Done()
		// The context is cancelled and the checkpoint must still land, which
		// is the whole point of a drain window.
		if err := cp.Save(context.Background(), []byte("stopped-at-500")); err != nil {
			return err
		}
		return hctx.Err()
	})
	j := h.submitWith("text.rebuild", func(j *jobs.Job) { j.MaxAttempts = 2 })

	stop := h.start(ctx)
	<-started
	stop()

	got := h.get(ctx, j)
	if got.State != jobs.Pending {
		t.Fatalf("a drained job is %s, want pending", got.State)
	}
	if got.Attempts != 0 {
		t.Fatalf("a drained job was charged %d attempts", got.Attempts)
	}
	if string(got.Checkpoint) != "stopped-at-500" {
		t.Fatalf("the drained job's checkpoint is %q", got.Checkpoint)
	}
}

func TestCancellingARunningJobStopsItsHandler(t *testing.T) {
	// Cancellation is cooperative and costs no extra read: the renewal already
	// re-reads the row, so a row somebody has cancelled is seen there.
	ctx := context.Background()
	h := newHarness(t, 2)

	started := make(chan struct{})
	stopped := make(chan struct{})
	h.register("text.rebuild", func(hctx context.Context, _ *jobs.Job, _ jobs.Checkpointer) error {
		close(started)
		<-hctx.Done()
		close(stopped)
		return hctx.Err()
	})
	j := h.submit("text.rebuild")

	stop := h.start(ctx)
	defer stop()
	<-started

	if err := h.queue.Cancel(ctx, acme, j.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	for range 10 {
		h.clk.Advance(h.queue.Lease() / 3)
		h.settle()
		select {
		case <-stopped:
			if got := h.get(ctx, j); got.State != jobs.Cancelled {
				t.Fatalf("the cancelled job is %s", got.State)
			}
			// The cancellation transition leaves the one canonical history
			// row; the worker must not add another after it observes the fence.
			h.pool.Wait()
			page, err := h.queue.RunsPage(ctx, acme, jobs.RunFilter{})
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Runs) != 1 || page.Runs[0].Outcome != jobs.Cancelled {
				t.Fatalf("cancellation left %+v, want one cancelled attempt", page.Runs)
			}
			return
		default:
		}
	}
	t.Fatal("the handler was never told its job had been cancelled")
}

func TestThePoolNeverRunsMoreThanItsWorkers(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3)

	var live, peak atomic.Int32
	release := make(chan struct{})
	h.register("vector.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error {
		n := live.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release
		live.Add(-1)
		return nil
	})
	for range 20 {
		h.submit("vector.rebuild")
	}

	stop := h.start(ctx)
	for range 20 {
		h.settle()
		if live.Load() == 3 {
			break
		}
	}
	if got := live.Load(); got != 3 {
		t.Fatalf("%d handlers are running with three workers", got)
	}
	close(release)
	stop()
	if peak.Load() > 3 {
		t.Fatalf("the pool ran %d handlers at once with three workers", peak.Load())
	}
}

func TestThePoolWalksEveryTenant(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 4)

	var mu sync.Mutex
	seen := map[tenant.ID]bool{}
	h.register("jobs.reap", func(_ context.Context, j *jobs.Job, _ jobs.Checkpointer) error {
		mu.Lock()
		seen[j.Tenant] = true
		mu.Unlock()
		return nil
	})

	tenants := []tenant.ID{"acme", "globex", "initech", "umbrella"}
	for _, tid := range tenants {
		h.provision(ctx, tid)
		if err := h.queue.Submit(ctx, &jobs.Job{Tenant: tid, Type: "jobs.reap"}); err != nil {
			t.Fatal(err)
		}
	}

	for range 20 {
		h.pollAndWait(ctx)
		mu.Lock()
		done := len(seen)
		mu.Unlock()
		if done == len(tenants) {
			return
		}
	}
	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("the pool reached %d of %d tenants: %v", len(seen), len(tenants), seen)
}

func TestAJobWhoseTypeNothingHandlesFailsVisibly(t *testing.T) {
	// The shape of an upgrade that removed a handler while rows of that type
	// were still queued. The job must not look like one that has been done.
	ctx := context.Background()
	h := newHarness(t, 2)
	h.register("vector.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error { return nil })

	orphan := h.submitWith("text.rebuild", func(j *jobs.Job) { j.MaxAttempts = 1 })
	h.pollAndWait(ctx)

	got := h.get(ctx, orphan)
	if got.State != jobs.Failed || got.LastError == "" {
		t.Fatalf("an unhandled job ended as %s / %q", got.State, got.LastError)
	}
}

// --- harness ----------------------------------------------------------------

type harness struct {
	t       *testing.T
	metrics *obs.Metrics
	kv      storage.KV
	clk     *clock.Fake
	queue   *jobs.Queue
	reg     *jobs.Registry
	dir     tenant.Directory
	pool    *jobs.Pool
}

func newHarness(t *testing.T, workers int) *harness {
	t.Helper()
	return newPoolHarness(t, workers, nil)
}

// newHarnessWithMetrics is newHarness with a real registry, for the tests that
// assert on what an operator would see.
func newHarnessWithMetrics(t *testing.T, workers int) *harness {
	t.Helper()
	return newPoolHarness(t, workers, obs.NewMetrics())
}

func newPoolHarness(t *testing.T, workers int, metrics *obs.Metrics) *harness {
	t.Helper()
	kv, clk := newStore(t)
	h := &harness{
		t: t, kv: kv, clk: clk, metrics: metrics,
		queue: jobs.NewQueue(kv, clk),
		reg:   jobs.NewRegistry(),
		dir:   tenantkv.New(kv, clk),
	}
	pool, err := jobs.NewPool(jobs.PoolConfig{
		Queue: h.queue, Registry: h.reg, Tenants: h.dir, Clock: clk,
		Owner: "node-test", Workers: workers,
		PollInterval: time.Second, DrainTimeout: 5 * time.Second,
		Metrics: metrics,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	h.pool = pool
	h.provision(context.Background(), acme)
	return h
}

func (h *harness) provision(ctx context.Context, tid tenant.ID) {
	h.t.Helper()
	if _, err := tenant.Ensure(tenant.NewContext(ctx, tid), h.dir, tid); err != nil {
		h.t.Fatalf("provisioning %s: %v", tid, err)
	}
}

func (h *harness) register(typ jobs.Type, fn jobs.HandlerFunc) {
	h.t.Helper()
	if err := h.reg.Register(jobs.Entry{Type: typ, Handler: fn}); err != nil {
		h.t.Fatalf("Register(%s): %v", typ, err)
	}
}

func (h *harness) submit(typ jobs.Type) *jobs.Job {
	return h.submitWith(typ, nil)
}

func (h *harness) submitWith(typ jobs.Type, shape func(*jobs.Job)) *jobs.Job {
	h.t.Helper()
	j := &jobs.Job{Tenant: acme, Type: typ}
	if shape != nil {
		shape(j)
	}
	if err := h.queue.Submit(context.Background(), j); err != nil {
		h.t.Fatalf("Submit: %v", err)
	}
	return j
}

// pollAndWait runs one polling cycle and waits for what it started.
func (h *harness) pollAndWait(ctx context.Context) {
	h.t.Helper()
	if _, err := h.pool.Poll(ctx); err != nil {
		h.t.Fatalf("Poll: %v", err)
	}
	h.pool.Wait()
}

// start runs the pool in the background and returns the function that stops it
// and waits for the drain.
//
// It also drives the fake clock, because the pool's poll and renewal tickers
// are the clock's and a fake clock does not tick on its own. Time therefore
// flows while the pool is up and stops when it is not, which is what lets a
// test say "the handler started" and "nothing reclaimed it" without sleeping
// for a real lease.
func (h *harness) start(ctx context.Context) func() {
	h.t.Helper()
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- h.pool.Run(runCtx) }()

	ticking := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticking:
				return
			default:
				h.clk.Advance(time.Second)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				close(ticking)
				if err != nil {
					h.t.Errorf("Run: %v", err)
				}
			case <-time.After(30 * time.Second):
				close(ticking)
				h.t.Error("the pool did not stop")
			}
		})
	}
}

// settle gives the pool's goroutines a turn. The fake clock fires its waiters
// synchronously, but the goroutines they wake still have to be scheduled.
func (h *harness) settle() {
	time.Sleep(2 * time.Millisecond)
}

func (h *harness) get(ctx context.Context, j *jobs.Job) *jobs.Job {
	h.t.Helper()
	got, err := h.queue.Get(ctx, j.Tenant, j.ID)
	if err != nil {
		h.t.Fatalf("Get(%s): %v", j.ID, err)
	}
	return got
}

func (h *harness) count(ctx context.Context, state jobs.State) int {
	h.t.Helper()
	list, err := h.queue.List(ctx, acme, jobs.Filter{States: []jobs.State{state}, Limit: jobs.MaxListLimit})
	if err != nil {
		h.t.Fatal(err)
	}
	return len(list)
}

func TestTheQueueDepthGaugesFollowTheQueue(t *testing.T) {
	// Spec §50 asks for pending, running, failures, retries and processing
	// time. The one that needs a test is pending, because a gauge nothing
	// writes keeps its last reading for ever: a backlog that cleared would go
	// on being reported as a backlog, which is how an operator learns to stop
	// trusting the alert.
	ctx := context.Background()
	h := newHarnessWithMetrics(t, 2)

	h.register("vector.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error { return nil })
	for range 3 {
		h.submit("vector.rebuild")
	}

	if err := h.pool.RefreshGauges(ctx); err != nil {
		t.Fatalf("RefreshGauges: %v", err)
	}
	if got := gauge(t, h.metrics.Jobs.Pending, "acme", "vector.rebuild"); got != 3 {
		t.Fatalf("jobs_pending is %v, want 3", got)
	}

	for range 3 {
		h.pollAndWait(ctx)
	}
	if err := h.pool.RefreshGauges(ctx); err != nil {
		t.Fatal(err)
	}
	if got := gauge(t, h.metrics.Jobs.Pending, "acme", "vector.rebuild"); got != 0 {
		t.Fatalf("after the queue drained jobs_pending is %v, want 0", got)
	}
}

func TestFailuresAndRetriesAreCountedApart(t *testing.T) {
	// They are different operational facts: retries say the queue is working
	// through something, failures say it gave up. One counter for both would
	// make a healthy retry loop look like an outage.
	ctx := context.Background()
	h := newHarnessWithMetrics(t, 2)

	h.register("vector.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error {
		return errors.New("still broken")
	})
	h.submitWith("vector.rebuild", func(j *jobs.Job) { j.MaxAttempts = 2 })

	h.pollAndWait(ctx)
	if got := counter(t, h.metrics.Jobs.RetriesTotal, "acme", "vector.rebuild"); got != 1 {
		t.Fatalf("after one failure with an attempt left, retries = %v, want 1", got)
	}
	if got := counter(t, h.metrics.Jobs.FailuresTotal, "acme", "vector.rebuild"); got != 0 {
		t.Fatalf("a job with an attempt left was counted as a failure: %v", got)
	}

	h.clk.Advance(jobs.RetryCap)
	h.pollAndWait(ctx)
	if got := counter(t, h.metrics.Jobs.FailuresTotal, "acme", "vector.rebuild"); got != 1 {
		t.Fatalf("after the last attempt, failures = %v, want 1", got)
	}
}

func gauge(t *testing.T, vec *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	return metricValue(t, vec.WithLabelValues(labels...))
}

func counter(t *testing.T, vec *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	return metricValue(t, vec.WithLabelValues(labels...))
}

func metricValue(t *testing.T, m prometheus.Metric) float64 {
	t.Helper()
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		t.Fatal(err)
	}
	switch {
	case out.Gauge != nil:
		return out.Gauge.GetValue()
	case out.Counter != nil:
		return out.Counter.GetValue()
	}
	t.Fatalf("metric %v is neither a gauge nor a counter", m.Desc())
	return 0
}
