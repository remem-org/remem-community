package jobs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/jobs"
)

// A handler may read its own job while the renewal ticker renews that job's
// lease, and so may the pool's own bookkeeping around the handler.
//
// A renewal is a transition, and a transition rewrites the whole *Job it is
// given under the running job's mutex. The handler was handed that same
// pointer, and the pool read its tenant and type for the log line and the
// metric labels without the mutex — so every read raced the renewal, and a
// struct copied mid-read can hand back a checkpoint slice header from one
// version and its length from another.
//
// Found by `make cover` in Phase 13, where coverage instrumentation widened the
// window enough for TestEveryDeclaredMetricIsRecorded to hit it. Plain `-race`
// runs had passed that test for weeks. This test does not depend on the window:
// the handler reads continuously across several renewals, which is exactly the
// unsynchronised pair the race detector reports whether or not the two accesses
// overlap in time. Without `-race` it proves only that the job still fails
// cleanly, which is why `make test` runs with it.
func TestAHandlerReadingItsJobDoesNotRaceTheRenewal(t *testing.T) {
	ctx := context.Background()
	h := newHarnessWithMetrics(t, 1)

	release := make(chan struct{})
	h.register("text.rebuild", func(_ context.Context, j *jobs.Job, _ jobs.Checkpointer) error {
		var seen int
		for {
			select {
			case <-release:
				return errors.New("the handler gave up after watching its lease")
			default:
			}
			// Every field a real handler has reason to read: where it got to,
			// what it was asked, and how many attempts it has had.
			seen += len(j.Checkpoint) + len(j.Payload) + int(j.Attempts)
			if j.Lease != nil && j.Lease.ExpiresAt.IsZero() {
				seen++
			}
			time.Sleep(100 * time.Microsecond)
		}
	})
	j := h.submitWith("text.rebuild", func(j *jobs.Job) { j.MaxAttempts = 1 })

	stop := h.start(ctx)
	defer stop()

	// Wait for the stored lease to have moved twice, so the handler's reads
	// span renewals rather than preceding the first one.
	var first time.Time
	renewals := 0
	deadline := time.Now().Add(10 * time.Second)
	for renewals < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("the lease was renewed %d times in ten seconds", renewals)
		}
		got := h.get(ctx, j)
		if got.Lease != nil && !got.Lease.ExpiresAt.Equal(first) {
			if !first.IsZero() {
				renewals++
			}
			first = got.Lease.ExpiresAt
		}
		time.Sleep(time.Millisecond)
	}

	// Wait for the failure to be recorded before stopping: a handler error that
	// arrives during the drain is a shutdown, not a failure, and is refunded.
	close(release)
	for got := h.get(ctx, j); got.State != jobs.Failed; got = h.get(ctx, j) {
		if time.Now().After(deadline) {
			t.Fatalf("a handler that returned an error on its only attempt left its job %s", got.State)
		}
		time.Sleep(time.Millisecond)
	}
	stop()
}
