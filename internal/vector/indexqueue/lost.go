package indexqueue

import (
	"errors"

	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
)

// A lost operation is one the queue accepted responsibility for after its
// memory committed and then could not apply: the inner index refused it, or the
// enqueue was refused because the caller gave up on a full shard or the queue
// had stopped. The memory's canonical vector is stored, but the resident graph
// does not hold it, and nothing would put it back until the tenant is next
// materialised — an eviction or a restart away, and possibly never on a busy
// server. Until then a search would omit it and say nothing.
//
// So a loss is reported the way every other damage to this index is: the tenant
// reads as degraded, which makes its searches report truncated, and a durable
// rebuild is scheduled through the same vector.Rebuilder hook the HNSW index
// reports damage through. A rebuild of the tenant that completes clears the
// mark, unless another operation was lost after it began — which is what the
// sequence number is for: the rebuild remembers the count it started from and
// clears only if nothing has moved it.
//
// An operation a shutdown abandons is not marked, and nor is an enqueue refused
// because the queue has already stopped. Both happen only as the process goes
// down: the mark lives in this process and would not outlive it, and the
// restart's materialisation inserts every canonical vector with no node record,
// which is exactly what either operation would have placed.

// lost reports whether a failure leaves the running process with a memory its
// resident graph will not hold. Both shutdown cases arrive carrying errStopped
// — an abandonment is counted with it, and a refused enqueue returns it — so
// that one test is the whole rule.
func lost(err error) bool {
	return !errors.Is(err, errStopped)
}

// WithRebuilder is where a lost operation's repair is scheduled. Without one, a
// loss still degrades the tenant, and only the scheduling is skipped.
func WithRebuilder(r vector.Rebuilder) Option {
	return func(q *Queue) { q.rebuilder = r }
}

// lostReason is what a degraded tenant's Health says, in a sentence an
// operator can act on.
const lostReason = "an index operation queued after its memory was stored could not be applied; " +
	"a vector.rebuild has been scheduled to restore it"

// markLost records a loss for t and schedules the tenant's repair.
func (q *Queue) markLost(t tenant.ID) {
	q.lossMu.Lock()
	if q.losses == nil {
		q.losses = map[tenant.ID]uint64{}
	}
	q.losses[t]++
	q.lossMu.Unlock()
	if q.rebuilder != nil {
		q.rebuilder.ScheduleRebuild(t, lostReason)
	}
}

// lossSeq is how many losses t has had that no rebuild has cleared. Zero means
// none.
func (q *Queue) lossSeq(t tenant.ID) uint64 {
	q.lossMu.Lock()
	defer q.lossMu.Unlock()
	return q.losses[t]
}

// clearLoss forgets t's losses if their count is still seq, the count a
// completed rebuild started from. A loss during the rebuild moved it, and the
// mark stays for the next repair.
func (q *Queue) clearLoss(t tenant.ID, seq uint64) {
	q.lossMu.Lock()
	defer q.lossMu.Unlock()
	if q.losses[t] == seq {
		delete(q.losses, t)
	}
}

// withLosses folds the queue's own damage into the inner index's report.
func (q *Queue) withLosses(t tenant.ID, h vector.Health) vector.Health {
	if q.lossSeq(t) == 0 || h.Degraded {
		return h
	}
	return vector.Health{Degraded: true, Reason: lostReason}
}
