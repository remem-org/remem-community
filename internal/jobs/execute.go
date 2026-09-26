package jobs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// What one worker does with one job, from the slot it is launched on to the
// history row its attempt leaves behind.
//
// It is split from worker.go, which is the dispatcher: the pool decides *which*
// jobs run and this decides what happens to one while it does. The two meet at
// launch and nowhere else, and keeping them in one file is how a job framework
// grows the seven-thousand-line engine the file-size rule exists to prevent.

// launch starts one job on a worker slot. The slot is returned when it ends.
func (p *Pool) launch(ctx context.Context, j *Job) {
	// The handler's context is derived from a context that outlives the poll,
	// so a running job is not cancelled by the tick that started it. Shutdown
	// cancels it through rj.cancel instead, which is what makes the drain a
	// decision this pool takes rather than a race with the caller's context.
	hctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	// Everything read from j happens here, before the renewal ticker can see it.
	rj := &runningJob{job: j, cancel: cancel,
		id: j.ID, tenant: j.Tenant, typ: j.Type, handled: j.Clone()}

	p.mu.Lock()
	p.running[j.ID] = rj
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer cancel()
		defer p.give()
		defer func() {
			p.mu.Lock()
			delete(p.running, j.ID)
			p.mu.Unlock()
		}()
		p.execute(hctx, rj)
	}()
}

// execute runs one handler and records what happened to its job.
func (p *Pool) execute(ctx context.Context, rj *runningJob) {
	log := obs.Logger(ctx).With("job_id", rj.id.String(),
		"job_type", rj.typ.String(), "tenant", rj.tenant.String())
	started := p.clk.Now()

	err := p.invoke(ctx, rj)
	elapsed := p.clk.Since(started)

	// The bookkeeping runs on a context the handler's cancellation cannot
	// reach. A drain cancels the handler, and a completion that failed because
	// of that would leave the job running until its lease lapsed — the one
	// outcome the drain exists to avoid.
	book := context.WithoutCancel(ctx)
	if p.metrics != nil {
		p.metrics.Jobs.ProcessingDuration.
			WithLabelValues(rj.tenant.String(), rj.typ.String()).Observe(elapsed.Seconds())
	}

	switch {
	case rj.lost.Load():
		// The row is no longer this worker's to move: it was cancelled, or the
		// lease lapsed and somebody else has it. Reporting an outcome here
		// would overwrite theirs — and that includes the history, which is why
		// nothing is recorded on this branch.
		log.Info("a job ended after its claim was lost", "duration_ms", elapsed.Milliseconds())

	case p.draining.Load() && err != nil:
		// Shutdown is not a failure. The job goes back with its attempt
		// refunded and its checkpoint intact, so the attempt did not happen as
		// far as the queue is concerned and there is nothing to record.
		if rerr := rj.withJob(func(j *Job) error { return p.queue.Release(book, j) }); rerr != nil {
			log.Warn("a drained job could not be returned to the queue", "error", rerr)
		}

	case err != nil:
		var attempts uint32
		var state State
		run := p.attempt(rj, started, 0, Retry, err.Error())
		ferr := rj.withJob(func(j *Job) error {
			p.countFailure(j)
			run.Attempt = j.Attempts
			if j.Attempts >= j.MaxAttempts {
				run.Outcome = Failed
			}
			if ferr := p.queue.FailWithRun(book, j, err, run); ferr != nil {
				return ferr
			}
			attempts, state = j.Attempts, j.State
			return nil
		})
		if ferr != nil {
			log.Warn("a failed job could not be recorded", "error", ferr, "cause", err.Error())
		} else {
			log.Warn("a job failed", "error", err.Error(), "attempt", attempts, "state", state.String())
		}

	default:
		run := p.attempt(rj, started, 0, Completed, "")
		cerr := rj.withJob(func(j *Job) error {
			run.Attempt = j.Attempts
			if cerr := p.queue.CompleteWithRun(book, j, run); cerr != nil {
				return cerr
			}
			return nil
		})
		if cerr != nil {
			log.Warn("a completed job could not be recorded", "error", cerr)
		} else {
			log.Info("a job completed", "duration_ms", elapsed.Milliseconds())
		}
	}
}

// attempt snapshots one handler outcome before the queue transaction begins.
func (p *Pool) attempt(rj *runningJob, started time.Time, number uint32, outcome State, cause string) *Run {
	processed, changed := rj.counts()
	return &Run{
		ID:               id.New(),
		JobID:            rj.id,
		Tenant:           rj.tenant,
		Namespace:        rj.handled.Namespace,
		Type:             rj.typ,
		Attempt:          number,
		StartedAt:        started.UTC(),
		FinishedAt:       p.clk.Now().UTC(),
		Outcome:          outcome,
		Error:            cause,
		Owner:            p.cfg.Owner,
		RecordsProcessed: processed,
		RecordsChanged:   changed,
	}
}

// invoke calls the handler, turning a panic into an ordinary error.
//
// A panicking handler must fail its own job and nothing else. Letting it
// through would take the process down, which turns one bad payload into an
// outage of every background workload — and the job that caused it would come
// back on the next start and do it again.
func (p *Pool) invoke(ctx context.Context, rj *runningJob) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("the handler panicked: %v", v)
		}
	}()

	handler, err := p.reg.Handler(rj.typ)
	if err != nil {
		return err
	}
	return handler.Handle(ctx, rj.handled, checkpointer{queue: p.queue, running: rj})
}

// runningJob is one job a worker holds.
//
// The mutex is what keeps the renewal ticker and the handler's checkpoints off
// each other: both are transitions on one row, and both mutate the same *Job.
// Each transition re-reads the durable row, so they compose rather than
// overwrite — but the struct they share still needs one writer at a time.
//
// Nothing reads job outside the mutex. A transition replaces the whole struct,
// identity included, so even a read of a field that never changes value is a
// read of memory being written. What the pool needs without the lock — the
// identity for logs and labels — is copied out at launch, and the handler is
// given its own clone rather than this pointer. Found by `make cover` in Phase
// 13; TestAHandlerReadingItsJobDoesNotRaceTheRenewal holds it.
type runningJob struct {
	mu     sync.Mutex
	job    *Job
	cancel context.CancelFunc
	lost   atomic.Bool

	id      id.ID
	tenant  tenant.ID
	typ     Type
	handled *Job

	// processed and changed are what the handler has reported for *this*
	// attempt. They are pointers so that "nobody counted" survives as far as
	// the history row: a zero here would be indistinguishable from a handler
	// that examined nothing, and those are different answers.
	//
	// They live under the same mutex as the job, because a handler reporting
	// from one goroutine and the completion reading them from another is the
	// ordinary case rather than a contrived one.
	processed *uint64
	changed   *uint64
}

func (r *runningJob) withJob(fn func(*Job) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fn(r.job)
}

// report sets one of the attempt's counters. The last call wins: a handler
// counting as it goes reports totals, not increments.
func (r *runningJob) report(slot **uint64, n uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := n
	*slot = &v
}

// counts copies what the handler reported, for the history row.
func (r *runningJob) counts() (processed, changed *uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.processed != nil {
		p := *r.processed
		processed = &p
	}
	if r.changed != nil {
		c := *r.changed
		changed = &c
	}
	return processed, changed
}
