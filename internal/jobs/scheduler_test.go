package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
)

func TestSchedulerFansOutPerTenant(t *testing.T) {
	ctx := context.Background()
	s := newScheduling(t)

	tenants := s.provisionMany(ctx, 10)
	s.recurring("jobs.reap", time.Hour)

	n, err := s.sched.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if n != 10 {
		t.Fatalf("one tick over ten tenants enqueued %d jobs, want 10", n)
	}
	for _, tid := range tenants {
		list, err := s.queue.List(ctx, tid, jobs.Filter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 1 || list[0].Tenant != tid || list[0].Type != "jobs.reap" {
			t.Fatalf("tenant %s holds %d jobs: %v", tid, len(list), list)
		}
	}
}

func TestARecurringJobDoesNotPileUp(t *testing.T) {
	// A recurring job that runs slower than its interval must not queue a
	// second copy behind the first. Rust's sweeps had no such guard, and the
	// failure it produces is the worst kind: a tenant whose maintenance is
	// permanently behind accumulates work faster than it can do it, and every
	// copy makes the next one slower.
	ctx := context.Background()
	s := newScheduling(t)
	s.provisionMany(ctx, 1)
	s.recurring("jobs.reap", time.Hour)

	if n, _ := s.sched.Tick(ctx); n != 1 {
		t.Fatalf("the first tick enqueued %d jobs", n)
	}
	s.clk.Advance(2 * time.Hour)
	if n, err := s.sched.Tick(ctx); err != nil || n != 0 {
		t.Fatalf("a second tick queued %d jobs behind the outstanding one (%v)", n, err)
	}

	// The same holds while it is running, not only while it is pending.
	claimed, err := s.queue.Claim(ctx, "tenant-0", owner, nil, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim: %d, %v", len(claimed), err)
	}
	s.clk.Advance(2 * time.Hour)
	if n, err := s.sched.Tick(ctx); err != nil || n != 0 {
		t.Fatalf("a tick queued %d jobs behind a running one (%v)", n, err)
	}

	// And once it is done, the next tick schedules the next run.
	if err := s.queue.Complete(ctx, claimed[0]); err != nil {
		t.Fatal(err)
	}
	s.clk.Advance(2 * time.Hour)
	if n, err := s.sched.Tick(ctx); err != nil || n != 1 {
		t.Fatalf("after the run finished the next tick enqueued %d jobs (%v)", n, err)
	}
}

func TestARecurringJobWaitsForItsInterval(t *testing.T) {
	ctx := context.Background()
	s := newScheduling(t)
	s.provisionMany(ctx, 1)
	s.recurring("jobs.reap", time.Hour)

	if n, _ := s.sched.Tick(ctx); n != 1 {
		t.Fatal("the first tick did not enqueue")
	}
	drain(t, s.queue, "tenant-0")

	s.clk.Advance(30 * time.Minute)
	if n, err := s.sched.Tick(ctx); err != nil || n != 0 {
		t.Fatalf("a job on an hourly schedule ran again after thirty minutes: %d, %v", n, err)
	}
	s.clk.Advance(31 * time.Minute)
	if n, err := s.sched.Tick(ctx); err != nil || n != 1 {
		t.Fatalf("an hourly job did not run after an hour: %d, %v", n, err)
	}
}

func TestOnlyRecurringTypesAreScheduled(t *testing.T) {
	ctx := context.Background()
	s := newScheduling(t)
	s.provisionMany(ctx, 3)

	// Registered, handled, and not recurring: something enqueues it, not the
	// clock. A scheduler that ran every registered type would turn every
	// repair into a treadmill.
	if err := s.reg.Register(jobs.Entry{Type: "vector.rebuild", Handler: jobs.HandlerFunc(nop)}); err != nil {
		t.Fatal(err)
	}
	if n, err := s.sched.Tick(ctx); err != nil || n != 0 {
		t.Fatalf("a non-recurring type was scheduled %d times (%v)", n, err)
	}
}

func TestTheScheduledJobCarriesItsTypesPolicy(t *testing.T) {
	ctx := context.Background()
	s := newScheduling(t)
	s.provisionMany(ctx, 1)
	if err := s.reg.Register(jobs.Entry{
		Type: "jobs.reap", Handler: jobs.HandlerFunc(nop),
		Every: time.Hour, MaxAttempts: 2, Priority: -8,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.sched.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	list, err := s.queue.List(ctx, "tenant-0", jobs.Filter{})
	if err != nil || len(list) != 1 {
		t.Fatalf("List: %d, %v", len(list), err)
	}
	if list[0].MaxAttempts != 2 || list[0].Priority != -8 {
		t.Fatalf("the scheduled job did not take its type's policy: %+v", list[0])
	}
}

// --- helpers ----------------------------------------------------------------

type scheduling struct {
	t     *testing.T
	clk   *clock.Fake
	queue *jobs.Queue
	reg   *jobs.Registry
	dir   tenant.Directory
	sched *jobs.Scheduler
}

func newScheduling(t *testing.T) *scheduling {
	t.Helper()
	kv, clk := newStore(t)
	s := &scheduling{
		t: t, clk: clk,
		queue: jobs.NewQueue(kv, clk),
		reg:   jobs.NewRegistry(),
		dir:   tenantkv.New(kv, clk),
	}
	sched, err := jobs.NewScheduler(jobs.SchedulerConfig{
		Queue: s.queue, Registry: s.reg, Tenants: s.dir, Clock: clk,
		Interval: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	s.sched = sched
	return s
}

func (s *scheduling) recurring(typ jobs.Type, every time.Duration) {
	s.t.Helper()
	if err := s.reg.Register(jobs.Entry{Type: typ, Handler: jobs.HandlerFunc(nop), Every: every}); err != nil {
		s.t.Fatal(err)
	}
}

func (s *scheduling) provisionMany(ctx context.Context, n int) []tenant.ID {
	s.t.Helper()
	out := make([]tenant.ID, 0, n)
	for i := range n {
		tid := tenant.ID("tenant-" + string(rune('0'+i)))
		if _, err := tenant.Ensure(tenant.NewContext(ctx, tid), s.dir, tid); err != nil {
			s.t.Fatalf("provisioning %s: %v", tid, err)
		}
		out = append(out, tid)
	}
	return out
}

func drain(t *testing.T, q *jobs.Queue, tid tenant.ID) {
	t.Helper()
	ctx := context.Background()
	claimed, err := q.Claim(ctx, tid, owner, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range claimed {
		if err := q.Complete(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
}

// A recurring type's period must not drift with the scheduler's tick.
//
// The defect this pins was found by Phase 10's end-to-end run. The scheduler
// set the next due time to `now + Every`, where `now` is the tick that noticed
// the job was due — so every period was Every plus however late the tick was,
// and the error accumulated. With an hourly type and a fifteen-minute tick, a
// day's worth of runs lands hours behind where the operator asked for them.
//
// Advancing from the previous *due* time instead makes the tick a matter of
// phase rather than of period: a run may be up to one tick late, and the next
// one is not later still.
func TestARecurringTypeDoesNotDriftWithTheTick(t *testing.T) {
	const every = time.Hour
	// A tick that is deliberately coarse and out of phase with the interval.
	const tick = 25 * time.Minute

	s := newScheduling(t)
	ctx := context.Background()
	s.provisionMany(ctx, 1)
	s.recurring("jobs.reap", every)
	_ = tick

	var fired []time.Time
	for i := 0; i < 200; i++ {
		s.clk.Advance(tick)
		n, err := s.sched.Tick(ctx)
		if err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
		if n > 0 {
			fired = append(fired, s.clk.Now())
			// Finish it, so the outstanding-job guard does not mask the drift
			// this test is about.
			drain(t, s.queue, "tenant-0")
		}
	}

	if len(fired) < 5 {
		t.Fatalf("the type fired %d times in %v", len(fired), time.Duration(200)*tick)
	}
	// The first run fires on the first tick — recurring work is repair-shaped
	// and running it at start-up is right. Every run after it is measured from
	// there, and must be within one tick of where it was asked for. Without the
	// fix the last one is hours behind.
	for i, at := range fired {
		want := fired[0].Add(time.Duration(i) * every)
		if d := at.Sub(want); d < 0 || d >= tick {
			t.Fatalf("run %d fired at %v, %v away from the %v it was scheduled for; "+
				"the period is drifting with the tick", i+1, at, d, want)
		}
	}
}

// A process that was stopped for a week must not enqueue a week of missed runs
// the moment it comes back. Recurring work is repair-shaped: what matters is
// that it happens now, not that it happens once for every period it missed.
func TestALongStallDoesNotEnqueueABurst(t *testing.T) {
	s := newScheduling(t)
	ctx := context.Background()
	s.provisionMany(ctx, 1)
	s.recurring("jobs.reap", time.Hour)

	if n, err := s.sched.Tick(ctx); err != nil || n != 1 {
		t.Fatalf("first run: %d, %v", n, err)
	}
	drain(t, s.queue, "tenant-0")

	// A week passes with nothing running.
	s.clk.Advance(7 * 24 * time.Hour)
	n, err := s.sched.Tick(ctx)
	if err != nil {
		t.Fatalf("tick after the stall: %v", err)
	}
	if n != 1 {
		t.Fatalf("a week-long stall enqueued %d jobs, want 1", n)
	}
	drain(t, s.queue, "tenant-0")

	// And the next one is an hour later, not immediately.
	s.clk.Advance(time.Minute)
	if n, err := s.sched.Tick(ctx); err != nil || n != 0 {
		t.Fatalf("a minute after catching up: %d, %v", n, err)
	}
}

// TestAPauseSuppressesSchedulingForOneTenantOnly is the whole point of a pause:
// it is an operator's decision about one tenant's recurring work, and the same
// type stays schedulable for everybody else.
func TestAPauseSuppressesSchedulingForOneTenantOnly(t *testing.T) {
	ctx := context.Background()
	s := newScheduling(t)
	tenants := s.provisionMany(ctx, 3)
	s.recurring("jobs.reap", time.Hour)

	paused := tenants[1]
	if _, err := s.queue.Pause(ctx, paused, "jobs.reap", s.clk.Now().Add(time.Hour), "operator"); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	n, err := s.sched.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if n != 2 {
		t.Fatalf("a tick over three tenants with one paused enqueued %d jobs, want 2", n)
	}
	for _, tid := range tenants {
		list, err := s.queue.List(ctx, tid, jobs.Filter{})
		if err != nil {
			t.Fatal(err)
		}
		want := 1
		if tid == paused {
			want = 0
		}
		if len(list) != want {
			t.Errorf("tenant %s holds %d jobs, want %d", tid, len(list), want)
		}
	}
}

// TestAPauseSuppressesOnlyItsOwnType: a pause names a type, and the scheduler's
// other registrations keep running. A suppression that took the whole schedule
// with it would be an outage wearing the costume of an operator's decision.
func TestAPauseSuppressesOnlyItsOwnType(t *testing.T) {
	ctx := context.Background()
	s := newScheduling(t)
	s.provisionMany(ctx, 1)
	s.recurring("jobs.reap", time.Hour)
	s.recurring("lifecycle.maintain", time.Hour)

	if _, err := s.queue.Pause(ctx, "tenant-0", "jobs.reap", s.clk.Now().Add(time.Hour), "operator"); err != nil {
		t.Fatal(err)
	}
	if n, err := s.sched.Tick(ctx); err != nil || n != 1 {
		t.Fatalf("a tick with one of two types paused enqueued %d jobs (%v), want 1", n, err)
	}
	list, err := s.queue.List(ctx, "tenant-0", jobs.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Type != "lifecycle.maintain" {
		t.Fatalf("the tick queued %v, want only lifecycle.maintain", list)
	}
}

// TestPausingLeavesQueuedAndRunningWorkAlone: a pause is about the schedule.
// Cancelling what is already outstanding would conflate it with the per-job
// cancellation that exists precisely for that, and would do it without an
// operator naming the job.
func TestPausingLeavesQueuedAndRunningWorkAlone(t *testing.T) {
	ctx := context.Background()
	s := newScheduling(t)
	s.provisionMany(ctx, 1)
	s.recurring("jobs.reap", time.Hour)

	if n, _ := s.sched.Tick(ctx); n != 1 {
		t.Fatalf("the first tick enqueued %d jobs", n)
	}
	claimed, err := s.queue.Claim(ctx, "tenant-0", owner, nil, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim: %d, %v", len(claimed), err)
	}

	if _, err := s.queue.Pause(ctx, "tenant-0", "jobs.reap", s.clk.Now().Add(2*time.Hour), "operator"); err != nil {
		t.Fatal(err)
	}
	list, err := s.queue.List(ctx, "tenant-0", jobs.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].State != jobs.Running {
		t.Fatalf("a pause moved the running job: %v", list)
	}
	if err := s.queue.Complete(ctx, claimed[0]); err != nil {
		t.Fatalf("a paused type's running job could not complete: %v", err)
	}
}

// TestAnExpiredPauseLetsTheScheduleResume: nothing runs to lift a pause, so
// the scheduler's own clock has to be what ends it.
func TestAnExpiredPauseLetsTheScheduleResume(t *testing.T) {
	ctx := context.Background()
	s := newScheduling(t)
	s.provisionMany(ctx, 1)
	s.recurring("jobs.reap", time.Minute)

	if _, err := s.queue.Pause(ctx, "tenant-0", "jobs.reap", s.clk.Now().Add(30*time.Minute), "operator"); err != nil {
		t.Fatal(err)
	}
	if n, err := s.sched.Tick(ctx); err != nil || n != 0 {
		t.Fatalf("a paused type was scheduled: %d (%v)", n, err)
	}

	s.clk.Advance(31 * time.Minute)
	if n, err := s.sched.Tick(ctx); err != nil || n != 1 {
		t.Fatalf("an expired pause still suppressed the schedule: %d (%v)", n, err)
	}
}

// TestAManualRunStillQueuesWhilePaused: pausing the schedule must not take the
// operator's own "run it now" with it — that is the one action that says the
// suppression is deliberate rather than an outage.
func TestAManualRunStillQueuesWhilePaused(t *testing.T) {
	ctx := context.Background()
	s := newScheduling(t)
	s.provisionMany(ctx, 1)
	s.recurring("jobs.reap", time.Hour)

	if _, err := s.queue.Pause(ctx, "tenant-0", "jobs.reap", s.clk.Now().Add(time.Hour), "operator"); err != nil {
		t.Fatal(err)
	}
	if err := s.queue.Submit(ctx, &jobs.Job{Tenant: "tenant-0", Type: "jobs.reap"}); err != nil {
		t.Fatalf("a manual run was refused while the schedule was paused: %v", err)
	}
	list, err := s.queue.List(ctx, "tenant-0", jobs.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("the manual job did not queue: %v", list)
	}
	if _, err := s.queue.PauseOf(ctx, "tenant-0", "jobs.reap"); err != nil {
		t.Fatalf("the manual run lifted the pause: %v", err)
	}
}
