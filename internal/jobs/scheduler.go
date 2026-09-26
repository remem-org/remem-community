package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// Scheduler enqueues the recurring job types, one job per tenant per interval.
//
// # What it does not do
//
// It does not run anything. It writes rows and the pool runs them, which is
// what makes recurring work identical to work anything else enqueued — same
// retries, same leases, same visibility in the admin surface. Rust ran five
// sweeps on five schedules, each with its own loop and its own idea of failure.
//
// # Its state is in memory, and that is a decision
//
// "When did this type last run" is held here, not on disk, so a restart makes
// every recurring type due at once. For repair-shaped work — which is what
// recurring work is — running it on start-up is right rather than merely
// tolerable, and the outstanding-job check below stops a restart loop from
// piling up copies. A durable schedule belongs with Phase 10's per-tenant
// maintenance, where "this tenant was last swept at T" is a fact about the
// tenant rather than about this process.
type Scheduler struct {
	queue *Queue
	reg   *Registry
	dir   tenant.Directory
	clk   clock.Clock

	interval time.Duration
	next     map[Type]time.Time
}

// SchedulerConfig is everything a scheduler needs.
type SchedulerConfig struct {
	Queue    *Queue
	Registry *Registry
	Tenants  tenant.Directory
	Clock    clock.Clock

	// Interval is how often the scheduler looks at its registrations. It is not
	// how often a job runs — each type carries its own Every — so it only needs
	// to be finer than the shortest interval registered.
	Interval time.Duration
}

// NewScheduler builds a scheduler.
func NewScheduler(cfg SchedulerConfig) (*Scheduler, error) {
	const op = "jobs.NewScheduler"
	switch {
	case cfg.Queue == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("a scheduler needs a queue"))
	case cfg.Registry == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("a scheduler needs a registry"))
	case cfg.Tenants == nil:
		return nil, errs.E(errs.Invalid, op, errors.New(
			"a scheduler needs the tenant directory: recurring work is per tenant"))
	case cfg.Clock == nil:
		return nil, errs.E(errs.Invalid, op, errors.New("a scheduler needs a clock"))
	case cfg.Interval <= 0:
		return nil, errs.E(errs.Invalid, op, errors.New("a scheduler needs a positive interval"))
	}
	return &Scheduler{
		queue: cfg.Queue, reg: cfg.Registry, dir: cfg.Tenants, clk: cfg.Clock,
		interval: cfg.Interval,
		next:     make(map[Type]time.Time),
	}, nil
}

// Run ticks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	log := obs.Logger(ctx)
	ticker := s.clk.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			n, err := s.Tick(ctx)
			if err != nil {
				// The same reasoning as a failed poll: a tenant directory that
				// was briefly unreadable is not a reason to stop scheduling
				// every recurring workload for the rest of the process's life.
				log.Warn("a scheduler tick failed", "error", err)
				continue
			}
			if n > 0 {
				log.Info("recurring jobs enqueued", "jobs", n)
			}
		}
	}
}

// Tick enqueues whatever is due and reports how many jobs it wrote.
//
// It is exported for the same reason Pool.Poll is: it is the unit Run repeats,
// and a test that drives one cycle is far more legible than one that advances a
// clock and hopes.
func (s *Scheduler) Tick(ctx context.Context) (int, error) {
	now := s.clk.Now()

	enqueued := 0
	for _, e := range s.reg.Recurring() {
		due, seen := s.next[e.Type]
		if seen && now.Before(due) {
			continue
		}
		s.next[e.Type] = s.advance(due, seen, now, e.Every)

		// The single audited cross-tenant path (Invariant 1). Recurring work is
		// per tenant by definition — there is no such thing as a maintenance
		// job for everybody — so this is the fan-out, and internal/jobs is on
		// the guard's allowlist for it.
		err := s.dir.ForEach(ctx, func(t tenant.ID) error {
			outstanding, err := s.queue.HasOutstanding(ctx, t, e.Type)
			if err != nil {
				return err
			}
			if outstanding {
				// A recurring job that runs slower than its interval must not
				// queue a second copy behind the first: a tenant permanently
				// behind then accumulates work faster than it can do it, and
				// every copy makes the next one slower.
				return nil
			}
			job := &Job{
				Tenant:      t,
				Namespace:   tenant.DefaultNamespace,
				Type:        e.Type,
				MaxAttempts: e.MaxAttempts,
				Priority:    e.Priority,
				RunAt:       now,
			}
			// SubmitUnlessPaused rather than Submit: an operator may have
			// suppressed this type for this tenant, and the pause is read
			// inside the transaction that writes the row so that pausing
			// during a tick cannot lose the race. Submit stays the manual
			// path — a pause suppresses the schedule, not the operator.
			queued, err := s.queue.SubmitUnlessPaused(ctx, job)
			if err != nil {
				return err
			}
			if queued {
				enqueued++
			}
			return nil
		})
		if err != nil {
			return enqueued, err
		}
	}
	return enqueued, nil
}

// advance decides when a recurring type is next due.
//
// # From the previous due time, not from now
//
// A tick only notices that a job is due; it is not when the job was asked for.
// Setting the next due time to `now + Every` therefore adds however late the
// tick was to every period, and the error accumulates: with an hourly type and
// a fifteen-minute tick, a day of runs ends up hours behind where the operator
// asked for them. Advancing from the previous due time makes the tick a matter
// of phase — a run may be up to one tick late, and the next one is not later
// still.
//
// Phase 10's end-to-end run is what found this. Phase 9 registered one
// recurring type whose interval was derived from the same setting as the
// scheduler's own tick, so the drift had nowhere to show.
//
// # A long stall skips the periods it missed
//
// A process stopped for a week comes back and enqueues one job, not a hundred
// and sixty-eight. Recurring work here is repair-shaped: what matters is that it
// happens now, not that it happens once for every period nobody was running.
func (s *Scheduler) advance(due time.Time, seen bool, now time.Time, every time.Duration) time.Time {
	if !seen {
		return now.Add(every)
	}
	next := due.Add(every)
	if next.Before(now) {
		return now.Add(every)
	}
	return next
}

// HasOutstanding reports whether a job of this type is waiting or running for
// the tenant.
//
// It is a scan of the two live partitions rather than a lookup, for the reason
// Get is: the queue is addressed by due time, not by type. Both partitions are
// bounded by what is actually in flight — the audit trail is not read — so this
// is proportional to the work outstanding rather than to the work ever done.
func (q *Queue) HasOutstanding(ctx context.Context, t tenant.ID, typ Type) (bool, error) {
	j, err := q.Outstanding(ctx, t, typ)
	if err != nil {
		if errs.Is(err, errs.NotFound) {
			return false, nil
		}
		return false, err
	}
	return j != nil, nil
}

// Outstanding returns the waiting or running job of this type, or
// [errs.NotFound].
//
// It is the same scan [HasOutstanding] is, returning the job rather than a
// boolean, because a refusal that says "something of this type is already
// outstanding" sends an operator to a listing to find out what. Naming the job
// and its state makes the next thing they do a lookup rather than a search.
func (q *Queue) Outstanding(ctx context.Context, t tenant.ID, typ Type) (*Job, error) {
	const op = "jobs.Outstanding"
	if t == "" {
		return nil, errs.E(errs.Invalid, op, errors.New(
			"this question requires a tenant: there is no unscoped read path (Invariant 1)"))
	}

	for _, part := range []keys.JobState{keys.JobPending, keys.JobRunning} {
		lower, upper := keys.JobStateRange(t, tenant.DefaultNamespace, part)
		it := q.kv.NewIterator(lower, upper)
		var found *Job
		for ok := it.First(); ok && found == nil; ok = it.Next() {
			j, err := Unmarshal(append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...))
			if err != nil {
				_ = it.Close()
				return nil, err
			}
			if j.Type == typ {
				found = j
			}
		}
		err := it.Error()
		if closeErr := it.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
		if found != nil {
			return found, nil
		}
	}
	return nil, errs.E(errs.NotFound, op, fmt.Errorf(
		"tenant %s has no waiting or running job of type %s", t, typ))
}
