package indexqueue_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/indexqueue"
)

// An operation the queue accepted is never lost without a trace, however
// closely the accept raced the shutdown.
//
// enqueue looks at the stop channel before it sends, so a Close firing between
// the look and the send leaves an accepted operation sitting in a buffer whose
// worker may already have taken the stop branch. It used to be lost there:
// applied by nobody, counted by nothing, and visible only as a depth gauge
// that never came back down. The property pinned here is the honest one —
// every accepted operation is either applied or counted as a failure — because
// which of the two it is depends on the race, and what must never happen is
// neither.
func TestAnAcceptedOperationIsNeverLostSilentlyWhenCloseRacesIt(t *testing.T) {
	t.Parallel()
	m := obs.NewMetrics()
	logs := &syncBuffer{}
	ctx := obs.WithLogger(context.Background(), obs.LoggerTo(logs))

	// One worker and a shallow buffer, so that an accept and the stop are
	// close enough together to collide — and enough iterations that they do.
	const iterations, perIteration = 2000, 4
	accepted, applied := 0, 0
	for range iterations {
		r := &recorder{}
		q := indexqueue.New(r, indexqueue.WithWorkers(1), indexqueue.WithSize(8),
			indexqueue.WithMetrics(&m.Vector))
		var n atomic.Int64
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perIteration {
				if err := q.Indexed(ctx, acme, id.New(), vector.Single([]float32{1})); err == nil {
					n.Add(1)
				}
			}
		}()
		_ = q.Close(ctx)
		wg.Wait()
		accepted += int(n.Load())
		applied += r.count()
	}

	abandoned := int(failures(t, m, "shutdown"))
	// Logged rather than asserted on: that the race happens at all is what
	// the iteration count is for, but how often it happens is the scheduler's
	// business and not a thing to fail a build over.
	t.Logf("%d accepted over %d iterations: %d applied, %d abandoned", accepted, iterations, applied, abandoned)
	if accepted != applied+abandoned {
		t.Fatalf("%d operations were accepted, %d applied and %d counted as abandoned: "+
			"%d vanished with nothing recorded",
			accepted, applied, abandoned, accepted-applied-abandoned)
	}
	if abandoned > 0 && !strings.Contains(logs.String(), "WARN") {
		t.Error("an abandoned operation was counted and never logged")
	}
	// The control: a run that never accepted anything would prove nothing.
	if accepted == 0 {
		t.Fatal("no operation was accepted at all; the race was never run")
	}
}

// Wait is how a caller that is about to close the store the workers write to
// learns that they are gone. Close stops them whether or not they have
// finished, so "stopped" and "returned" are two different questions.
func TestWaitReturnsOnceTheWorkersHaveStopped(t *testing.T) {
	t.Parallel()
	r := &recorder{gate: make(chan struct{})}
	q := indexqueue.New(r, indexqueue.WithWorkers(1), indexqueue.WithSize(8))
	ctx := context.Background()
	for range 4 {
		if err := q.Indexed(ctx, acme, id.New(), vector.Single([]float32{1})); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "the worker to take the first operation", func() bool { return r.started() >= 1 })

	expired, cancelExpired := context.WithTimeout(ctx, time.Millisecond)
	defer cancelExpired()
	time.Sleep(5 * time.Millisecond)
	if err := q.Close(expired); err == nil {
		t.Fatal("Close reported a clean drain although its deadline had passed")
	}
	// The worker is still inside an apply, which is exactly the state that
	// makes closing the store under it a panic.
	short, cancelShort := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelShort()
	if err := q.Wait(short); err == nil {
		t.Fatal("Wait returned with a worker still applying an operation")
	}

	r.open()
	waited, cancelWaited := context.WithTimeout(ctx, 10*time.Second)
	defer cancelWaited()
	if err := q.Wait(waited); err != nil {
		t.Fatalf("Wait never saw the workers stop: %v", err)
	}
	// Once they are gone it is a question with a settled answer, however often
	// it is asked.
	if err := q.Wait(waited); err != nil {
		t.Fatalf("the second Wait: %v", err)
	}
}

// A second Close must not report the shutdown done while the first one's
// workers are still running. Both callers are about to close something the
// workers write to; telling the second one "already stopped" and letting it
// carry on is the same bug as not waiting at all.
func TestASecondCloseWaitsForTheFirstOnesWorkers(t *testing.T) {
	t.Parallel()
	r := &recorder{gate: make(chan struct{})}
	q := indexqueue.New(r, indexqueue.WithWorkers(1), indexqueue.WithSize(8))
	ctx := context.Background()
	for range 4 {
		if err := q.Indexed(ctx, acme, id.New(), vector.Single([]float32{1})); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "the worker to take the first operation", func() bool { return r.started() >= 1 })

	expired, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)
	if err := q.Close(expired); err == nil {
		t.Fatal("the first Close reported a clean drain although its deadline had passed")
	}

	second := make(chan error, 1)
	go func() { second <- q.Close(context.Background()) }()
	select {
	case err := <-second:
		t.Fatalf("the second Close returned %v with the first one's worker still applying", err)
	case <-time.After(100 * time.Millisecond):
	}

	r.open()
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("the second Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second Close never returned after the workers stopped")
	}
}

// failures is remem_vector_index_failures_total for one kind. The kind is the
// point: an operation the index refused and one the shutdown abandoned are
// different events, and a test that summed them would pass on either.
func failures(t *testing.T, m *obs.Metrics, kind string) float64 {
	t.Helper()
	fams, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for _, fam := range fams {
		if fam.GetName() != "remem_vector_index_failures_total" {
			continue
		}
		for _, met := range fam.Metric {
			for _, l := range met.Label {
				if l.GetName() == "kind" && l.GetValue() == kind {
					total += met.Counter.GetValue()
				}
			}
		}
	}
	return total
}
