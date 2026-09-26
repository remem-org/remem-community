package jobs

import (
	"context"
	"math/rand/v2"
	"time"
)

// Retry bounds, and the reasoning behind the numbers.
//
// A job fails for a reason outside itself — a store that was busy, an index
// mid-rebuild, a handler that met a half-written row. What decides whether the
// retry succeeds is whether that reason has passed, so the first delay is a
// second rather than a millisecond: an immediate re-run buys nothing and costs
// an attempt out of the six there are.
//
// The cap exists because an unbounded doubling turns the sixth attempt into
// hours, and a queue that quietly goes that stale is worse than one an operator
// is told about. Ten minutes is the point past which the right escalation is a
// person rather than another doubling.
const (
	// RetryBase is the first retry's window.
	RetryBase = time.Second
	// RetryCap bounds every retry's window.
	RetryCap = 10 * time.Minute
)

// BackoffWindow is the window the given attempt's retry delay is drawn from.
//
// It doubles per attempt from [RetryBase] and stops at [RetryCap]. The shift is
// bounded rather than computed, because an attempt count large enough to shift
// past a duration's width produces zero — a backoff that silently becomes a
// tight loop at exactly the moment things are going worst.
func BackoffWindow(attempt uint32) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	const maxShift = 40 // beyond this the cap has long since applied
	shift := attempt - 1
	if shift > maxShift {
		return RetryCap
	}
	w := RetryBase << shift
	if w > RetryCap || w <= 0 {
		return RetryCap
	}
	return w
}

// BackoffDelay draws the delay before the given attempt's retry.
//
// Half jitter: uniformly from [window/2, window). Full jitter would sometimes
// draw a near-zero delay, which defeats the point of having backed off at all;
// no jitter keeps every worker that failed together failing together, so they
// collide again at each doubling.
//
// It is drawn outside the transaction that stores it, which matters for two
// reasons: a transaction body may run more than once, and Invariant 9 forbids a
// random draw inside a path Phase 14 replicates. The delay is decided here and
// the transaction writes an already-decided value.
func BackoffDelay(attempt uint32) time.Duration {
	w := BackoffWindow(attempt)
	return w/2 + rand.N(w/2)
}

// Fail records that the handler did not succeed.
//
// A job with attempts left goes back to the pending partition in the [Retry]
// state, due after a jittered backoff, keeping its checkpoint — which is what
// makes a retry cheaper than a first run. A job that has used its attempts is
// [Failed], terminal, with the last error kept so that the queue itself says
// why rather than sending an operator to the logs.
func (q *Queue) Fail(ctx context.Context, j *Job, cause error) error {
	return q.FailWithRun(ctx, j, cause, nil)
}

// FailWithRun records the failed attempt atomically with either its retry
// transition or terminal failed state.
func (q *Queue) FailWithRun(ctx context.Context, j *Job, cause error, run *Run) error {
	reason := "the handler failed without saying why"
	if cause != nil {
		reason = cause.Error()
	}

	// Read the attempt count from the job the worker holds. It was set by the
	// claim and cannot have moved since without the fence refusing this call.
	if j.Attempts >= j.MaxAttempts {
		return q.finishWithRun(ctx, j, Failed, reason, run)
	}

	now := q.clk.Now().UTC()
	runAt := now.Add(BackoffDelay(j.Attempts))
	return q.transitionWithRun(ctx, j, func(next *Job) {
		next.State = Retry
		next.Lease = nil
		next.RunAt = runAt
		next.UpdatedAt = now
		next.LastError = reason
	}, run)
}
