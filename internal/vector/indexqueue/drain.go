package indexqueue

import (
	"context"
	"errors"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// Draining and stopping the queue — Flush, Settle, Close, Wait and the barrier
// they share — and what the queue records about itself: depth, apply lag and
// the failures a lost operation is counted and marked by.

// Flush returns once everything enqueued before it was called has been
// applied.
//
// It is what makes "a search issued immediately after a create sees it" a thing
// a test can ask for, what `remem-admin` and the shutdown need, and the reason
// the queue has no other way to be observed: asking for the depth to be zero
// would be a poll with a race in it.
func (q *Queue) Flush(ctx context.Context) error {
	return q.drain(ctx, "indexqueue.Flush", q.shards...)
}

// Settle returns once everything this tenant had enqueued before the call has
// been applied, which is what [vector.Settler] promises.
//
// It is Flush narrowed to one tenant, and the narrowing is the point: a caller
// that needs its own tenant's index current has no business waiting out another
// tenant's backlog, for the reason [Queue.Rebuild] drains one shard rather than
// all of them. internal/discovery is the caller: it reads the index once per
// memory written, so a window it merely tolerated would cost it a relationship
// rather than a retry.
func (q *Queue) Settle(ctx context.Context, t tenant.ID) error {
	return q.drain(ctx, "indexqueue.Settle", q.shardFor(t))
}

// drain waits for the named shards to reach the barrier it puts on each of
// them. Flush names all of them; Settle and Rebuild name the one their tenant
// is on. op is the caller's name, so that a refusal says which wait was
// refused.
func (q *Queue) drain(ctx context.Context, op string, shards ...*shard) error {
	select {
	case <-q.stop:
		return errs.E(errs.Unavailable, op, errStopped)
	default:
	}

	dones := make([]chan struct{}, 0, len(shards))
	for _, s := range shards {
		done := make(chan struct{})
		select {
		case s.ops <- barrier(ctx, done):
			dones = append(dones, done)
		case <-ctx.Done():
			return errs.E(errs.Unavailable, op, ctx.Err())
		case <-q.stop:
			return errs.E(errs.Unavailable, op, errStopped)
		}
	}
	for _, done := range dones {
		select {
		case <-done:
		case <-ctx.Done():
			return errs.E(errs.Unavailable, op, ctx.Err())
		case <-q.stopped:
			// The workers are gone, so this barrier will never come back.
			// Waiting on the caller's deadline instead would be a shutdown
			// hanging on a queue that has already stopped.
			return errs.E(errs.Unavailable, op, errStopped)
		}
	}
	return nil
}

func barrier(ctx context.Context, done chan struct{}) op {
	return op{kind: opBarrier, ctx: context.WithoutCancel(ctx), done: done, enqueued: time.Now()}
}

// Close drains within ctx's deadline and then stops the workers.
//
// The workers are stopped whether or not the drain finished, because a
// shutdown that can be held open by a queue is one an orchestrator turns into a
// kill. What an abandoned operation costs is one insert at the next
// materialisation of that tenant, which is the same cost a crash at this moment
// would have had — and every one of them is logged and counted, by the drain in
// [Queue.abandon], rather than silently dropped.
//
// A worker that is mid-apply when the deadline passes is still running when
// this returns. [Queue.Wait] is how a caller that is about to close the store
// under it asks about that.
//
// It is safe to call twice: a shutdown path entered twice must not panic, and
// the second caller waits for the first one's workers rather than being told
// the queue has stopped while they are still writing.
func (q *Queue) Close(ctx context.Context) error {
	err := q.Flush(ctx)
	if errors.Is(err, errStopped) {
		// Closed already, by a caller that may still be draining. That drain
		// is not this one's to report — but this one must not return while the
		// workers it would have stopped are still running.
		err = nil
	}
	q.stopOnce.Do(func() { close(q.stop) })
	if waited := q.Wait(ctx); waited != nil && err == nil {
		err = waited
	}
	return err
}

// Wait returns once every worker has returned, or when ctx is done.
//
// It is the second half of a shutdown, and it exists because [Queue.Close]
// deliberately answers a different question. Close stops the workers whether or
// not they have finished — a shutdown a slow apply can hold open is one an
// orchestrator turns into a kill — so when its deadline expires a worker may
// still be inside an apply, writing a node record. A caller that is about to
// close the store those records go to has to know that worker is gone: a
// commit into a closed Pebble store panics rather than failing.
func (q *Queue) Wait(ctx context.Context) error {
	const op = "indexqueue.Wait"
	select {
	case <-q.stopped:
		return nil
	case <-ctx.Done():
		return errs.E(errs.Unavailable, op, ctx.Err())
	}
}

func (q *Queue) observeDepth(s *shard) {
	if q.metrics == nil {
		return
	}
	q.metrics.QueueDepth.WithLabelValues(s.label).Set(float64(s.depth.Load()))
}

func (q *Queue) observeLag(o op) {
	if q.metrics == nil {
		return
	}
	q.metrics.ApplyLag.WithLabelValues(string(o.tenant)).Observe(time.Since(o.enqueued).Seconds())
}

// countFailure logs and counts an operation that did not reach the index.
//
// It returns nothing, because there is nobody left to tell: the write this
// belongs to was acknowledged before the operation was queued. Warn rather
// than error, and no retry — see the package comment.
func (q *Queue) countFailure(ctx context.Context, o op, kind string, err error) {
	if lost(err) {
		q.markLost(o.tenant)
	}
	obs.Logger(ctx).Warn("a vector index operation did not reach the index; "+
		"the canonical vector is stored and the next materialisation will index it",
		"tenant", o.tenant.String(), "memory", o.rid.String(), "kind", kind, "error", err)
	if q.metrics == nil {
		return
	}
	q.metrics.FailuresTotal.WithLabelValues(string(o.tenant), kind).Inc()
}
