package server

import (
	"time"

	"github.com/remem-org/remem-go/internal/jobs"
)

// The job framework's runners, built by the composition root rather than by
// internal/jobs, which never imports the packages whose work it runs.

// buildJobRunners constructs the pool and the scheduler.
func (d *deps) buildJobRunners() error {
	pool, err := jobs.NewPool(jobs.PoolConfig{
		Queue: d.queue, Registry: d.registry, Tenants: d.tenants, Clock: d.clk,
		Owner:        d.nodeID(),
		Workers:      d.cfg.Jobs.Workers,
		PollInterval: d.cfg.Jobs.PollInterval,
		DrainTimeout: d.cfg.Jobs.DrainTimeout,
		Metrics:      d.metrics,
	})
	if err != nil {
		return err
	}
	d.pool = pool

	interval := schedulerTick(d.registry.Recurring(), d.cfg.Jobs.PollInterval)
	sched, err := jobs.NewScheduler(jobs.SchedulerConfig{
		Queue: d.queue, Registry: d.registry, Tenants: d.tenants, Clock: d.clk,
		Interval: interval,
	})
	if err != nil {
		return err
	}
	d.scheduler = sched
	return nil
}

// schedulerTick is how often the scheduler looks at its registrations.
//
// # It comes from the registrations, not from a setting
//
// It used to be derived from jobs.retention — a Phase 9 shortcut that held only
// because the reaper was the one recurring type and its interval came from the
// same setting. Phase 10 registers lifecycle.maintain at lifecycle.sweep_interval,
// and the end-to-end run found what that produced: a tick of three hours over a
// sweep asked to run hourly. An operator setting a dial and getting something
// else is the class of defect Phase 9's configuration work exists to prevent, so
// the tick is derived from the shortest interval anything actually registered.
//
// # And it is bounded on both sides
//
// A quarter of the shortest interval, so a run is at most 25% of a period late —
// and with the drift fix in jobs.Scheduler.advance, late once rather than later
// every time. Never finer than the pool polls, because a tick that enqueues work
// nothing will look at for another second is a directory walk for nothing. Never
// coarser than a minute, so a long interval does not make start-up recovery slow.
func schedulerTick(recurring []jobs.Entry, poll time.Duration) time.Duration {
	shortest := time.Duration(0)
	for _, e := range recurring {
		if shortest == 0 || e.Every < shortest {
			shortest = e.Every
		}
	}
	if shortest == 0 {
		// Nothing recurring is registered. The scheduler still runs, because a
		// type may be registered by a later phase, and a minute is cheap.
		return time.Minute
	}

	tick := shortest / 4
	if floor := poll; tick < floor {
		tick = floor
	}
	if tick < time.Second {
		tick = time.Second
	}
	if tick > time.Minute {
		tick = time.Minute
	}
	return tick
}
