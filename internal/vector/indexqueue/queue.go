// Package indexqueue takes vector index maintenance off the request path.
//
// docs/architecture/vector.md has said since Phase 7 that the HNSW node record
// is the one derived structure maintained *asynchronously*: an index insert
// that fails never fails a write, and a node record a crash loses is one the
// next materialisation inserts again from the canonical vector. The write path
// did not match the document. A create committed its record batch, flushed it,
// and then called the index inline — the tenant's graph write lock, a
// CPU-heavy insert, and a second commit — with the HTTP response waiting for
// all of it. This package is what makes the sentence true.
//
// # It is a vector.Index, not a new concept
//
// [Queue] wraps a maintained index and satisfies the same two interfaces, so
// the composition root substitutes it for the index and nothing below changes:
// internal/memory still asks the index whether it implements vector.Maintainer
// and still calls Indexed and Removed after the commit. Only Indexed and
// Removed are deferred. Search, Stats, Health, Insert and Delete pass straight
// through, which keeps `Insert`'s contract — after it returns, a search finds
// the vector — exactly as the shared suite in internal/vector/vectortest
// requires. Rebuild passes through too, but only after draining its tenant's
// shard: it replaces the tenant's node records wholesale, so it must not run
// against a backlog that would then edit what it had just finished.
//
// # Ordering is in enqueue order per tenant, and that is why it is sharded
//
// An insert followed by a remove of the same record must apply in that order.
// Applied the other way round the remove is a no-op, its post-condition
// already holding, and the insert that follows it leaves a deleted memory
// findable for ever. A worker pool over one shared channel cannot promise that,
// and a lock per record would serialise the thing this package exists to move
// off the request path. So each tenant hashes to exactly one worker, and that
// worker has one FIFO: order within a tenant is the channel's, and two tenants
// never wait for each other unless they share a worker.
//
// What that promises is worth stating exactly, because it is narrower than it
// sounds: operations apply in the order they were *enqueued*, per tenant. That
// is not a promise about the order their transactions committed. internal/memory
// commits the record and then calls Indexed, and between those two there has
// always been a window — two writers racing inside it reach the index in
// whichever order the scheduler gives them, exactly as they reached the inline
// index before this package existed. The queue neither widens that window nor
// closes it. What it adds is that once an operation is enqueued, nothing
// reorders it against its tenant's other enqueued operations.
//
// # Bounded, and the bound is backpressure rather than loss
//
// Each worker's channel holds as many operations as [WithSize] was given —
// [DefaultSize] when it was not, and vector.index_queue_size in the server.
// When it is full,
// Indexed and Removed block on the caller's context: a create waits for a
// worker rather than returning with its vector unindexed. Rust Remem's
// equivalent did a try_send and counted what it dropped (plan §II.10 row 5);
// dropping is survivable here too, since materialisation repairs it, but
// making it the ordinary response to load would turn a busy server into one
// whose index is quietly incomplete.
//
// # A failure is logged, counted, and goes no further
//
// There is no retry loop. The canonical vector is committed before anything
// reaches this queue, so a lost operation costs the next materialisation one
// insert and costs no memory; a worker retrying in a loop would spend the
// tenant's graph lock on work that the materialisation does for free.
//
// What must not happen is a loss nobody can see, and there are three ways an
// operation ends without reaching the index: the index refused it (`apply`),
// the queue never accepted it (`enqueue`), or a shutdown abandoned it
// (`shutdown`). Every one of the three is logged at warn with its tenant and
// record id and counted under its own kind, including the operation an accept
// that raced [Queue.Close] left in a buffer — [Queue.abandon] is what makes
// the last of those visible rather than silent.
package indexqueue

import (
	"context"
	"errors"
	"hash/fnv"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
)

// DefaultWorkers is how many goroutines apply operations.
//
// Two rather than one because a single worker makes one slow tenant everyone's
// problem, and two rather than GOMAXPROCS because an insert holds its tenant's
// graph write lock: more workers buy parallelism only across tenants, and they
// cost a background thread competing with the request path for the cores that
// serve searches. It is not a configuration setting for that reason — the
// number that matters to an operator is the bound, which is.
const DefaultWorkers = 2

// DefaultSize is how many operations one worker may have waiting.
//
// A thousand creates' worth of node-record work, which is a burst a server
// absorbs without making anybody wait, and about 1.5 MB of queued embeddings at
// 384 dimensions.
const DefaultSize = 1024

// Maintained is what the queue wraps: an index that keeps a structure of its
// own beside the canonical vectors and must therefore be told about writes.
//
// It is this pair rather than vector.Index alone so that wrapping an exact
// index is a compile error instead of a silent lie. `flat` *is* a scan of the
// canonical rows; there is nothing to tell it, and a queue in front of it would
// make internal/memory believe otherwise — vector.Maintainer is what that
// decision is read from.
type Maintained interface {
	vector.Index
	vector.Maintainer
}

// A queue is the index it wraps, which is what lets the composition root
// substitute one for the other. Stated here so that a method lost in a
// refactoring is a compile error in this package rather than a type assertion
// that quietly starts answering false in internal/server and internal/memory.
var (
	_ Maintained        = (*Queue)(nil)
	_ vector.Index      = (*Queue)(nil)
	_ vector.Maintainer = (*Queue)(nil)
	_ vector.Settler    = (*Queue)(nil)
)

// Queue is a [Maintained] index whose Indexed and Removed are applied by
// background workers.
type Queue struct {
	inner   Maintained
	metrics *obs.VectorMetrics
	shards  []*shard
	// size is how deep one shard's channel is. It is settled before the
	// channels are made, which is why WithSize records it here.
	size int

	// stop is closed by Close. It is what stops the workers and what makes a
	// later enqueue refuse rather than wait for a worker that is gone.
	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	// stopped is closed once every worker has returned. New owns the one
	// goroutine that closes it, which is what lets Close bound its wait by the
	// caller's deadline instead of blocking on the WaitGroup — a shutdown that
	// a slow apply can hold open is one an orchestrator turns into a kill.
	stopped chan struct{}

	// rebuilder, lossMu and losses are the lost-operation bookkeeping; see
	// lost.go.
	rebuilder vector.Rebuilder
	lossMu    sync.Mutex
	losses    map[tenant.ID]uint64
}

// shard is one worker and its FIFO.
type shard struct {
	ops   chan op
	label string
	// depth counts operations accepted onto this shard and not yet handled.
	//
	// len(ops) was the obvious gauge and was the wrong one twice over. It
	// misses the operation a worker has taken out of the channel and is
	// applying, which is the one a shutdown has to wait for; and it is read
	// after the send rather than before it, so two senders and a worker
	// reported three different depths for the same moment. This counter is
	// also load-bearing rather than decorative: a sender increments it
	// *before* it looks at the stop channel, which is what lets a worker's
	// shutdown drain tell "nothing left" from "an accept still on its way".
	depth atomic.Int64
}

// opKind is what a queued operation does.
type opKind uint8

const (
	opIndexed opKind = iota
	opRemoved
	// opBarrier carries no work. Flush sends one to every shard and waits for
	// it to come back: a shard is a FIFO, so a barrier is applied after
	// everything enqueued before it and before anything enqueued after.
	opBarrier
)

type op struct {
	kind opKind
	// ctx is the enqueueing caller's context with its cancellation removed.
	// It carries the logger, the tenant and the request id, so a warning about
	// a failed apply names the create that caused it — and carries no deadline,
	// because the request it came from has already been answered.
	ctx      context.Context
	tenant   tenant.ID
	rid      id.ID
	entries  []vector.Entry
	enqueued time.Time
	// done is closed by the worker, for opBarrier only.
	done chan struct{}
}

// Option adjusts a queue.
type Option func(*Queue)

// WithWorkers sets how many goroutines apply operations. Zero or less takes
// [DefaultWorkers].
func WithWorkers(n int) Option {
	return func(q *Queue) {
		if n > 0 {
			q.shards = make([]*shard, n)
		}
	}
}

// WithSize sets how many operations one worker may have waiting. Zero or less
// takes [DefaultSize].
func WithSize(n int) Option {
	return func(q *Queue) {
		if n > 0 {
			q.size = n
		}
	}
}

// WithMetrics records queue depth, apply lag and failures. Without it nothing
// is recorded, which is what a test that is not about metrics wants.
func WithMetrics(m *obs.VectorMetrics) Option {
	return func(q *Queue) { q.metrics = m }
}

// New wraps inner and starts its workers. Close stops them.
func New(inner Maintained, opts ...Option) *Queue {
	q := &Queue{inner: inner, stop: make(chan struct{}), stopped: make(chan struct{})}
	q.shards = make([]*shard, DefaultWorkers)
	q.size = DefaultSize
	for _, o := range opts {
		o(q)
	}
	for i := range q.shards {
		s := &shard{ops: make(chan op, q.size), label: strconv.Itoa(i)}
		q.shards[i] = s
		q.wg.Add(1)
		go q.work(s)
	}
	go func() {
		q.wg.Wait()
		close(q.stopped)
	}()
	return q
}

// Indexed enqueues the insert of an already-committed canonical vector.
//
// It returns when the operation is *queued*, not when it is applied, which is
// the whole point: the response no longer waits for an HNSW insert. The error
// it can return is a full queue the caller's context ran out of patience with,
// and internal/memory drops it for the reason it has always dropped this
// call's error — the memory is stored, and an unindexed canonical vector is one
// the next materialisation indexes.
func (q *Queue) Indexed(ctx context.Context, t tenant.ID, rid id.ID, entries []vector.Entry) error {
	// Copied, because the caller's slices are the embeddings it is about to
	// reuse or release. hnsw.Indexed copies for the same reason; this one has to
	// outlive the request rather than the call.
	own := make([]vector.Entry, len(entries))
	for i, e := range entries {
		own[i] = vector.Entry{Passage: e.Passage, Vector: append([]float32(nil), e.Vector...)}
	}
	return q.enqueue(ctx, op{kind: opIndexed, tenant: t, rid: rid, entries: own})
}

// Removed enqueues the removal of a record whose canonical vector has already
// been deleted.
func (q *Queue) Removed(ctx context.Context, t tenant.ID, rid id.ID) error {
	return q.enqueue(ctx, op{kind: opRemoved, tenant: t, rid: rid})
}

// enqueue puts o on its tenant's shard, waiting for room under ctx.
func (q *Queue) enqueue(ctx context.Context, o op) error {
	o.ctx = context.WithoutCancel(ctx)
	o.enqueued = time.Now()

	s := q.shardFor(o.tenant)
	// Registered before the stop channel is looked at, and read by the
	// worker's shutdown drain afterwards. That order is the whole guarantee:
	// an operation this sender is about to put in the buffer is either refused
	// below or seen by a worker, and never left in a channel nobody reads.
	s.depth.Add(1)

	// Looked at before the channel, because a closed queue has workers that
	// are gone and buffer space that nothing will ever read: an operation
	// accepted here would be lost silently, where a refusal is counted.
	select {
	case <-q.stop:
		return q.refuse(ctx, s, o, errStopped)
	default:
	}

	select {
	case s.ops <- o:
		q.observeDepth(s)
		return nil
	default:
	}
	// The channel is full: this is the backpressure, and it is bounded by the
	// caller rather than by a timeout of ours.
	select {
	case s.ops <- o:
		q.observeDepth(s)
		return nil
	case <-ctx.Done():
		return q.refuse(ctx, s, o, ctx.Err())
	case <-q.stop:
		return q.refuse(ctx, s, o, errStopped)
	}
}

// refuse gives back the depth an enqueue reserved and reports why its
// operation was not accepted.
func (q *Queue) refuse(ctx context.Context, s *shard, o op, err error) error {
	s.depth.Add(-1)
	q.observeDepth(s)
	q.countFailure(ctx, o, "enqueue", err)
	return errs.E(errs.Unavailable, "indexqueue.enqueue", err)
}

var errStopped = errors.New("the vector index queue has stopped; its workers are gone")

// shardFor maps a tenant to its worker. The hash is FNV-1a over the tenant id,
// which is what keeps one tenant on one worker and therefore keeps its
// operations ordered.
func (q *Queue) shardFor(t tenant.ID) *shard {
	if len(q.shards) == 1 {
		return q.shards[0]
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(t))
	return q.shards[int(h.Sum32()%uint32(len(q.shards)))]
}

// work is one worker: apply what is waiting, then wait for more or for Close.
func (q *Queue) work(s *shard) {
	defer q.wg.Done()
	for {
		// Drained before the stop is looked at, so a Close whose Flush
		// succeeded never abandons an operation a select happened to pick the
		// stop branch over.
		select {
		case o := <-s.ops:
			q.handle(s, o)
			continue
		default:
		}
		select {
		case o := <-s.ops:
			q.handle(s, o)
		case <-q.stop:
			q.abandon(s)
			return
		}
	}
}

func (q *Queue) handle(s *shard, o op) {
	if o.kind == opBarrier {
		close(o.done)
		q.observeDepth(s)
		return
	}
	err := q.apply(o)
	q.observeLag(o)
	if err != nil {
		q.countFailure(o.ctx, o, "apply", err)
	}
	// Released after the apply rather than when the operation left the
	// channel: what the depth reports is work the index has not caught up
	// with, and the operation being applied is exactly that.
	s.depth.Add(-1)
	q.observeDepth(s)
}

// abandon counts everything the stop left behind on this shard, so that no
// operation the queue accepted ever disappears without a log line and a
// counter.
//
// Two things get here. One is a buffer whose Close ran out of deadline, which
// is the case [Queue.Close] has always described in prose and used to leave
// invisible. The other is the race this function exists for: enqueue looks at
// the stop channel and then sends, so an accept can land in the buffer after
// the worker has already chosen the stop branch. The first is drained; the
// second is waited for, because a sender reserves its shard's depth before it
// looks at the stop and gives it back only when it is refused.
func (q *Queue) abandon(s *shard) {
	for {
		select {
		case o := <-s.ops:
			q.abandonOne(s, o)
			continue
		default:
		}
		if s.depth.Load() <= 0 {
			// The channel is empty and no sender is on its way: an enqueue
			// that registers from here on finds the stop closed and is
			// refused, so nothing more can arrive.
			return
		}
		// A sender is between its registration and its send. Its send cannot
		// block — the channel is empty — so this is a yield, not a spin.
		runtime.Gosched()
	}
}

// abandonOne logs and counts one operation the shutdown will not apply.
//
// A barrier is released instead of counted: it carries no work, and the Flush
// waiting for it is answered by it either way.
func (q *Queue) abandonOne(s *shard, o op) {
	if o.kind == opBarrier {
		close(o.done)
		return
	}
	q.countFailure(o.ctx, o, "shutdown", errStopped)
	s.depth.Add(-1)
	q.observeDepth(s)
}

func (q *Queue) apply(o op) error {
	if o.kind == opRemoved {
		return q.inner.Removed(o.ctx, o.tenant, o.rid)
	}
	return q.inner.Indexed(o.ctx, o.tenant, o.rid, o.entries)
}

// --- passed straight through -------------------------------------------------

// Insert adds or replaces a vector, synchronously.
//
// It is not the production write path — a memory and its vector commit together
// in one transaction, and Indexed is what tells the index afterwards — and its
// contract is read-your-write: internal/vector/vectortest requires a search
// after Insert to find the vector. Queueing it would break that for the
// admin and rebuild paths that use it, so it is not queued. The consequence,
// stated because it is a real one: Insert and Delete do not order themselves
// against queued work for the same record, where [Queue.Rebuild] does. Nothing
// in the write path calls them, and they address one record where a rebuild
// replaces every one of them.
func (q *Queue) Insert(ctx context.Context, t tenant.ID, rid id.ID, entries []vector.Entry) error {
	return q.inner.Insert(ctx, t, rid, entries)
}

// Delete removes a vector, synchronously. See [Queue.Insert].
func (q *Queue) Delete(ctx context.Context, t tenant.ID, rid id.ID) error {
	return q.inner.Delete(ctx, t, rid)
}

// Search answers from the index as it is. A vector whose insert is still queued
// is not found yet, which is the visibility window
// docs/architecture/vector.md describes.
func (q *Queue) Search(ctx context.Context, t tenant.ID, v []float32, k int, f vector.Filter) ([]vector.Hit, error) {
	return q.inner.Search(ctx, t, v, k, f)
}

// Rebuild replaces the tenant's index from src, after draining the tenant's
// queued operations.
//
// It is the one method that does not simply pass through. A rebuild replaces
// the tenant's node records wholesale, so an operation that arrived before it
// and applied after it would be an edit to a structure the rebuild had already
// declared finished. Draining first makes the rebuild's input the state the
// caller saw. Only this tenant's shard is drained: another tenant's backlog is
// nothing to do with this rebuild, and waiting for it would make one tenant's
// repair wait on another's writes.
//
// A drain that fails because the queue has stopped is not a reason to refuse:
// its workers are gone and its enqueues refuse, so there is nothing left that
// could still race the rebuild.
func (q *Queue) Rebuild(ctx context.Context, t tenant.ID, src vector.Source, opts ...vector.RebuildOption) error {
	// Read before the drain, so a loss the drain itself surfaces is one this
	// rebuild does not get to clear: it may have applied after the rebuild read
	// the canonical vectors.
	seq := q.lossSeq(t)
	if err := q.Settle(ctx, t); err != nil && !errors.Is(err, errStopped) {
		return err
	}
	if err := q.inner.Rebuild(ctx, t, src, opts...); err != nil {
		return err
	}
	q.clearLoss(t, seq)
	return nil
}

// Stats describes the tenant's index as it is, queued work excluded.
func (q *Queue) Stats(ctx context.Context, t tenant.ID) (vector.Stats, error) {
	return q.inner.Stats(ctx, t)
}

// Health reports whether this tenant's searches can be complete.
func (q *Queue) Health(ctx context.Context, t tenant.ID) (vector.Health, error) {
	h, err := q.inner.Health(ctx, t)
	if err != nil {
		return h, err
	}
	return q.withLosses(t, h), nil
}
