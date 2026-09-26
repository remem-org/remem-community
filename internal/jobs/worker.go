package jobs

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// Pool is the worker pool: the thing that turns rows in the queue into work.
//
// # Every goroutine has an owner (spec §57)
//
// Run owns all of them. It starts the dispatcher, the renewal ticker and one
// goroutine per running job, and it returns only when every one of them has
// finished. A pool goroutine that outlived Run would keep a data directory open
// after shutdown reported success.
type Pool struct {
	queue *Queue
	reg   *Registry
	dir   tenant.Directory
	clk   clock.Clock
	cfg   PoolConfig

	// free holds one token per idle worker. The dispatcher takes tokens before
	// it claims, so it never leases work it has no worker for — a claimed job
	// waiting in a channel is a lease held for nothing.
	free chan struct{}

	mu      sync.Mutex
	running map[id.ID]*runningJob
	wg      sync.WaitGroup

	// cursor is where the tenant walk resumes. Restarting at the first tenant
	// in id order on every tick would starve everything after the first tenant
	// that can fill the pool.
	cursor   int
	tenants  []tenant.ID
	tenantAt time.Time

	draining atomic.Bool
	metrics  *obs.Metrics

	// gauges remembers which (tenant, type) label sets currently hold a
	// non-zero value. A pair that empties has to be set back to zero
	// explicitly: a gauge nothing writes keeps its last reading for ever, so a
	// backlog that cleared would go on being reported as a backlog.
	gaugeCursor int
	gauges      map[[3]string]struct{}
}

// PoolConfig is everything a pool needs.
type PoolConfig struct {
	Queue    *Queue
	Registry *Registry
	Tenants  tenant.Directory
	Clock    clock.Clock

	// Owner is this node's identity, recorded on every lease.
	Owner string
	// Workers bounds how many handlers run at once.
	Workers int
	// PollInterval is how often the dispatcher looks for work.
	PollInterval time.Duration
	// DrainTimeout is how long a shutdown waits for in-flight handlers to
	// checkpoint and return before giving up on them.
	DrainTimeout time.Duration
	// Types restricts what this pool claims. Empty means every registered type.
	Types []Type
	// Metrics is optional.
	Metrics *obs.Metrics
}

// Bounds the dispatcher pays regardless of how large a deployment gets.
const (
	// maxTenantsPerPoll bounds the tenant walk, so one tick's cost does not
	// grow with the number of tenants. The cursor makes coverage a matter of
	// how many ticks it takes rather than whether it happens.
	maxTenantsPerPoll = 32
	// tenantListEvery is how long the directory listing is reused. A tenant
	// provisioned in between waits that long for its first job, which is a
	// better trade than a full directory scan every second.
	tenantListEvery = 10 * time.Second
	// reclaimPerPoll bounds how many lapsed leases one tenant gives back per
	// tick.
	reclaimPerPoll = 16
	// gaugeEvery is how often the queue-depth gauges are refreshed. It is far
	// slower than the poll because counting is a scan and "how deep is the
	// queue" is a question nobody asks thirty times a minute.
	gaugeEvery = 30 * time.Second
)

// NewPool builds a pool.
func NewPool(cfg PoolConfig) (*Pool, error) {
	const op = "jobs.NewPool"

	switch {
	case cfg.Queue == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("a pool needs a queue"))
	case cfg.Registry == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("a pool needs a registry"))
	case cfg.Tenants == nil:
		return nil, errs.E(errs.Invalid, op, errors.New(
			"a pool needs the tenant directory: the queue has no cross-tenant range to scan"))
	case cfg.Clock == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("a pool needs a clock"))
	case cfg.Owner == "":
		return nil, errs.E(errs.Invalid, op, errors.New(
			"a pool needs an owner: a lease that names nobody cannot be fenced"))
	case cfg.Workers <= 0:
		return nil, errs.E(errs.Invalid, op, errors.New("a pool needs at least one worker"))
	case cfg.PollInterval <= 0:
		return nil, errs.E(errs.Invalid, op, errors.New("a pool needs a positive poll interval"))
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 15 * time.Second
	}

	p := &Pool{
		queue: cfg.Queue, reg: cfg.Registry, dir: cfg.Tenants, clk: cfg.Clock,
		cfg:     cfg,
		free:    make(chan struct{}, cfg.Workers),
		running: make(map[id.ID]*runningJob),
		metrics: cfg.Metrics,
		gauges:  map[[3]string]struct{}{},
	}
	for range cfg.Workers {
		p.free <- struct{}{}
	}
	return p, nil
}

// Run polls for work until ctx is cancelled, then drains and returns.
//
// It returns nil on a clean shutdown, including one caused by cancellation:
// being asked to stop is not a failure.
func (p *Pool) Run(ctx context.Context) error {
	log := obs.Logger(ctx)

	poll := p.clk.NewTicker(p.cfg.PollInterval)
	defer poll.Stop()
	renew := p.clk.NewTicker(p.renewEvery())
	defer renew.Stop()
	gauges := p.clk.NewTicker(gaugeEvery)
	defer gauges.Stop()

	log.Info("the job pool is running",
		"owner", p.cfg.Owner, "workers", p.cfg.Workers, "poll_interval", p.cfg.PollInterval.String())

	for {
		select {
		case <-ctx.Done():
			p.drain(ctx)
			log.Info("the job pool has stopped")
			return nil
		case <-renew.C:
			p.renewAll(ctx)
		case <-gauges.C:
			if err := p.RefreshGauges(ctx); err != nil {
				log.Warn("the job gauges could not be refreshed", "error", err)
			}
		case <-poll.C:
			if _, err := p.Poll(ctx); err != nil {
				// A failed poll is not a reason to stop: the store may be busy,
				// a tenant may have gone away mid-scan, and the next tick tries
				// again. Stopping would turn a transient read error into an
				// outage of every background workload at once.
				log.Warn("a job poll failed", "error", err)
			}
		}
	}
}

// Poll runs one dispatch cycle and reports how many jobs it started.
//
// It is exported because it is the unit Run repeats, and because a test that
// drives one cycle synchronously is far more legible than one that advances a
// clock and hopes.
func (p *Pool) Poll(ctx context.Context) (int, error) {
	if p.draining.Load() {
		return 0, nil
	}
	slots := p.take()
	if slots == 0 {
		return 0, nil
	}

	started := 0
	defer func() {
		for range slots - started {
			p.give()
		}
	}()

	tenants, err := p.walk(ctx)
	if err != nil {
		return 0, err
	}
	for _, t := range tenants {
		if started == slots {
			break
		}
		// Lapsed leases first: a job whose worker died is work this tenant is
		// already waiting on, and claiming new work ahead of it would let a
		// busy queue starve its own retries.
		if _, err := p.queue.Reclaim(ctx, t, reclaimPerPoll); err != nil {
			return started, err
		}
		claimed, err := p.queue.Claim(ctx, t, p.cfg.Owner, p.cfg.Types, slots-started)
		if err != nil {
			return started, err
		}
		for _, j := range claimed {
			p.launch(ctx, j)
			started++
		}
	}
	return started, nil
}

// Wait blocks until every job this pool has started has finished.
//
// Run calls it as part of shutting down. It is exported because "stop giving me
// work and let what is running finish" is a thing an embedder legitimately
// needs, and because a test that has just polled needs to know when the answer
// is settled.
func (p *Pool) Wait() { p.wg.Wait() }

// Running reports how many handlers are executing right now.
func (p *Pool) Running() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.running)
}

// Cancel stops a queued or running job and snapshots handler totals when the
// attempt is running on this pool.
func (p *Pool) Cancel(ctx context.Context, t tenant.ID, jid id.ID) error {
	p.mu.Lock()
	rj := p.running[jid]
	p.mu.Unlock()
	var processed, changed *uint64
	var leaseToken id.ID
	if rj != nil && rj.tenant == t {
		_ = rj.withJob(func(j *Job) error {
			if j.Lease != nil {
				leaseToken = j.Lease.Token
			}
			return nil
		})
		processed, changed = rj.counts()
	}
	return p.queue.cancelWithCounts(ctx, t, jid, leaseToken, processed, changed)
}

// renewAll extends every running job's lease, and stops the handlers whose jobs
// are no longer theirs. It reports what it did, which is what makes the
// "finished" case testable rather than merely commented.
//
// Cancellation rides on this and costs nothing extra: the renewal already
// re-reads the durable row, so a job somebody has cancelled is discovered here
// rather than by a poll nobody would otherwise make.
func (p *Pool) renewAll(ctx context.Context) (renewed, finished, lost int) {
	p.mu.Lock()
	live := make([]*runningJob, 0, len(p.running))
	for _, rj := range p.running {
		live = append(live, rj)
	}
	p.mu.Unlock()

	for _, rj := range live {
		err := rj.withJob(func(j *Job) error {
			// The handler finished between the snapshot above and this line,
			// and the completion already dropped the lease.
			//
			// Skipping is not tidiness. Renewing here fails with "holds no
			// lease to renew" and logs a warning about a job that did exactly
			// what it was supposed to — one per job that finishes near a
			// renewal tick, which is how an operator learns that this server's
			// warnings do not mean anything. Found by the Phase 9 verification
			// run, in the shutdown log of a perfectly successful reaper.
			//
			// The check is exact rather than hopeful because it is inside the
			// running job's mutex, which is the same lock the completion takes.
			if j.Lease == nil {
				return errJobFinished
			}
			return p.queue.Renew(ctx, j)
		})
		switch {
		case err == nil:
			renewed++
		case errors.Is(err, errJobFinished):
			finished++
		case errs.Is(err, errs.Conflict):
			// The job is not ours any more. Stop the handler: continuing would
			// spend a worker on work somebody else is already redoing.
			lost++
			rj.lost.Store(true)
			rj.cancel()
			obs.Logger(ctx).Info("a running job was taken away from this worker",
				"job_id", rj.id.String(), "reason", err.Error())
		default:
			obs.Logger(ctx).Warn("a lease could not be renewed", "job_id", rj.id.String(), "error", err)
		}
	}
	return renewed, finished, lost
}

// errJobFinished is renewAll's internal signal that a job completed while the
// renewal pass was assembling its list. It never reaches a caller.
var errJobFinished = errors.New("the job finished before its lease was renewed")

// drain stops accepting work and gives the handlers a window to checkpoint.
//
// The window is the point: a handler told to stop needs a moment to record
// where it got to, and a shutdown that cancelled and immediately exited would
// throw that away on every restart.
func (p *Pool) drain(ctx context.Context) {
	p.draining.Store(true)
	log := obs.Logger(ctx)

	p.mu.Lock()
	live := len(p.running)
	for _, rj := range p.running {
		rj.cancel()
	}
	p.mu.Unlock()

	if live == 0 {
		return
	}
	log.Info("waiting for running jobs to checkpoint", "jobs", live,
		"budget", p.cfg.DrainTimeout.String())

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(p.cfg.DrainTimeout):
		// The handlers that did not return keep their leases, which lapse on
		// their own. The next process reclaims them and they resume from
		// whatever they last checkpointed — which is the whole reason a
		// checkpoint is durable.
		log.Warn("some jobs did not stop within the drain budget; their leases will lapse and they will be reclaimed",
			"jobs", p.Running())
	}
}

// walk returns the tenants this poll will visit, resuming from the dispatcher's
// cursor.
func (p *Pool) walk(ctx context.Context) ([]tenant.ID, error) {
	if err := p.refreshTenants(ctx); err != nil {
		return nil, err
	}
	return p.rotate(&p.cursor), nil
}

// walkFrom is walk with a caller's own cursor, so the gauge refresh and the
// dispatcher do not drag each other's position around.
func (p *Pool) walkFrom(ctx context.Context, cursor *int) ([]tenant.ID, error) {
	if err := p.refreshTenants(ctx); err != nil {
		return nil, err
	}
	return p.rotate(cursor), nil
}

// refreshTenants reloads the directory listing when it has gone stale.
//
// The listing is reused for a while rather than read every tick. A tenant
// provisioned in between waits that long for its first job, which is a better
// trade than a full directory scan every second in a deployment with thousands
// of them.
func (p *Pool) refreshTenants(ctx context.Context) error {
	now := p.clk.Now()
	if p.tenants != nil && now.Sub(p.tenantAt) < tenantListEvery {
		return nil
	}
	var all []tenant.ID
	// The single audited cross-tenant path (Invariant 1). internal/jobs is on
	// its allowlist precisely for this: the queue has no range that spans
	// tenants, so a dispatcher must be told which tenants exist.
	if err := p.dir.ForEach(ctx, func(t tenant.ID) error {
		all = append(all, t)
		return nil
	}); err != nil {
		return err
	}
	p.tenants, p.tenantAt = all, now
	return nil
}

// rotate returns the next batch of tenants and advances the caller's cursor.
//
// Bounded at maxTenantsPerPoll so one tick's cost does not grow with the number
// of tenants, and resumed from the cursor rather than restarted, because
// restarting at the first tenant in id order starves everything after the first
// tenant that can fill the pool.
func (p *Pool) rotate(cursor *int) []tenant.ID {
	if len(p.tenants) == 0 {
		return nil
	}
	if *cursor >= len(p.tenants) {
		*cursor = 0
	}
	n := min(maxTenantsPerPoll, len(p.tenants))
	out := make([]tenant.ID, 0, n)
	for i := range n {
		out = append(out, p.tenants[(*cursor+i)%len(p.tenants)])
	}
	*cursor = (*cursor + n) % len(p.tenants)
	return out
}

// take claims as many idle worker slots as are available, without waiting.
func (p *Pool) take() int {
	n := 0
	for {
		select {
		case <-p.free:
			n++
		default:
			return n
		}
	}
}

func (p *Pool) give() {
	select {
	case p.free <- struct{}{}:
	default: // impossible: one token per slot, returned once
	}
}

// renewEvery is how often leases are extended: a third of the lease, so two
// renewals may be missed before one lapses.
func (p *Pool) renewEvery() time.Duration {
	d := p.queue.Lease() / 3
	if d <= 0 {
		d = time.Second
	}
	return d
}
