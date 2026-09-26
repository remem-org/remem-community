package jobs_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
)

const operator = "operator@example.com"

func TestAPauseIsScopedToOneTenantAndSurvivesARestart(t *testing.T) {
	ctx := context.Background()
	q, clk, kv := newQueue(t)

	until := clk.Now().Add(time.Hour).UTC()
	p, err := q.Pause(ctx, acme, "lifecycle.maintain", until, operator)
	if err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if !p.ExpiresAt.Equal(until) || p.SetBy != operator || p.Type != "lifecycle.maintain" {
		t.Fatalf("Pause returned %+v", p)
	}

	// A second queue over the same store is what a restart looks like: nothing
	// of the pause is held in this process.
	restarted := jobs.NewQueue(kv, clk)
	got, err := restarted.PauseOf(ctx, acme, "lifecycle.maintain")
	if err != nil {
		t.Fatalf("PauseOf after a restart: %v", err)
	}
	if !got.ExpiresAt.Equal(until) || got.SetBy != operator {
		t.Fatalf("the pause came back as %+v", got)
	}

	// And it says nothing about anybody else's tenant or anybody else's type.
	if _, err := restarted.PauseOf(ctx, "other", "lifecycle.maintain"); !errs.Is(err, errs.NotFound) {
		t.Fatalf("a pause in acme reached tenant other: %v", err)
	}
	if _, err := restarted.PauseOf(ctx, acme, "jobs.reap"); !errs.Is(err, errs.NotFound) {
		t.Fatalf("pausing lifecycle.maintain also paused jobs.reap: %v", err)
	}
}

func TestAPauseNeedsAFutureExpiryWithinTheMaximum(t *testing.T) {
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	cases := []struct {
		name  string
		until time.Time
	}{
		{"in the past", clk.Now().Add(-time.Second)},
		{"now", clk.Now()},
		{"beyond the maximum", clk.Now().Add(jobs.MaxPauseDuration + time.Second)},
		{"never", time.Time{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := q.Pause(ctx, acme, "jobs.reap", c.until, operator); !errs.Is(err, errs.Invalid) {
				t.Fatalf("an expiry %s was accepted: %v", c.name, err)
			}
			// A refused pause writes nothing. A half-written suppression is
			// worse than none: it suppresses, and nothing reports it.
			if _, err := q.PauseOf(ctx, acme, "jobs.reap"); !errs.Is(err, errs.NotFound) {
				t.Fatalf("a refused pause left state behind: %v", err)
			}
		})
	}
}

func TestAPauseExpiresWithoutAnybodyClearingIt(t *testing.T) {
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	if _, err := q.Pause(ctx, acme, "jobs.reap", clk.Now().Add(time.Hour), operator); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	clk.Advance(time.Hour + time.Second)

	if _, err := q.PauseOf(ctx, acme, "jobs.reap"); !errs.Is(err, errs.NotFound) {
		t.Fatalf("an expired pause is still in force: %v", err)
	}
	live, err := q.Pauses(ctx, acme)
	if err != nil {
		t.Fatalf("Pauses: %v", err)
	}
	if len(live) != 0 {
		t.Fatalf("an expired pause is still listed: %+v", live)
	}
}

func TestResumeClearsAPauseAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	if _, err := q.Pause(ctx, acme, "jobs.reap", clk.Now().Add(time.Hour), operator); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if err := q.Resume(ctx, acme, "jobs.reap"); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if _, err := q.PauseOf(ctx, acme, "jobs.reap"); !errs.Is(err, errs.NotFound) {
		t.Fatalf("a resumed type is still paused: %v", err)
	}
	// Resuming what is not paused is a success that writes nothing: the
	// post-condition — this type is not paused — already holds.
	if err := q.Resume(ctx, acme, "jobs.reap"); err != nil {
		t.Fatalf("a second Resume: %v", err)
	}
}

func TestPausesListsOnlyTheLiveOnes(t *testing.T) {
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	if _, err := q.Pause(ctx, acme, "jobs.reap", clk.Now().Add(time.Minute), operator); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Pause(ctx, acme, "lifecycle.maintain", clk.Now().Add(time.Hour), operator); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Minute)

	live, err := q.Pauses(ctx, acme)
	if err != nil {
		t.Fatalf("Pauses: %v", err)
	}
	if len(live) != 1 || live[0].Type != "lifecycle.maintain" {
		t.Fatalf("Pauses returned %+v, want only lifecycle.maintain", live)
	}
}

// TestPausingRefusesAnUnnameableType holds the type name to the same grammar
// every other job surface uses: a pause is stored under the type name, so a
// name that cannot be spelled in a key cannot be paused.
func TestPausingRefusesAnUnnameableType(t *testing.T) {
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	if _, err := q.Pause(ctx, acme, "Vector Rebuild", clk.Now().Add(time.Hour), operator); !errs.Is(err, errs.Invalid) {
		t.Fatalf("a malformed type name was accepted: %v", err)
	}
	if _, err := q.Pause(ctx, "", "jobs.reap", clk.Now().Add(time.Hour), operator); !errs.Is(err, errs.Invalid) {
		t.Fatalf("a pause with no tenant was accepted (Invariant 1): %v", err)
	}
}

// TestSubmitUnlessPausedIsTheSchedulerSeam: the scheduler's enqueue reads the
// pause inside the transaction that writes the job, and expects it absent at
// commit. A pause written between the read and the commit therefore loses the
// job rather than racing it.
func TestSubmitUnlessPausedRefusesWhilePaused(t *testing.T) {
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	if _, err := q.Pause(ctx, acme, "jobs.reap", clk.Now().Add(time.Hour), operator); err != nil {
		t.Fatal(err)
	}
	queued, err := q.SubmitUnlessPaused(ctx, &jobs.Job{
		Tenant: acme, Namespace: tenant.DefaultNamespace, Type: "jobs.reap",
	})
	if err != nil {
		t.Fatalf("SubmitUnlessPaused: %v", err)
	}
	if queued {
		t.Fatal("a paused type was scheduled anyway")
	}
	list, err := q.List(ctx, acme, jobs.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("the queue holds %d jobs after a suppressed schedule", len(list))
	}

	// Unpaused, the same call queues.
	if err := q.Resume(ctx, acme, "jobs.reap"); err != nil {
		t.Fatal(err)
	}
	queued, err = q.SubmitUnlessPaused(ctx, &jobs.Job{
		Tenant: acme, Namespace: tenant.DefaultNamespace, Type: "jobs.reap",
	})
	if err != nil || !queued {
		t.Fatalf("an unpaused type was not queued: %v, %v", queued, err)
	}
}

func TestPausingDoesNotTouchQueuedOrRunningWork(t *testing.T) {
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	queuedJob := &jobs.Job{Tenant: acme, Type: "jobs.reap"}
	if err := q.Submit(ctx, queuedJob); err != nil {
		t.Fatal(err)
	}
	runningJob := &jobs.Job{Tenant: acme, Type: "jobs.reap"}
	if err := q.Submit(ctx, runningJob); err != nil {
		t.Fatal(err)
	}
	claimed, err := q.Claim(ctx, acme, owner, nil, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim: %d, %v", len(claimed), err)
	}

	if _, err := q.Pause(ctx, acme, "jobs.reap", clk.Now().Add(time.Hour), operator); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	list, err := q.List(ctx, acme, jobs.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("a pause moved existing work: the queue holds %d jobs, want 2", len(list))
	}
	var pending, running int
	for _, j := range list {
		switch j.State {
		case jobs.Pending:
			pending++
		case jobs.Running:
			running++
		default:
			t.Fatalf("a pause put a job into %s", j.State)
		}
	}
	if pending != 1 || running != 1 {
		t.Fatalf("after a pause: %d pending, %d running; want 1 and 1", pending, running)
	}

	// And the running one still completes through the ordinary lifecycle.
	if err := q.Complete(ctx, claimed[0]); err != nil {
		t.Fatalf("a paused type's running job could not complete: %v", err)
	}
}

// TestAManualSubmitIgnoresAPause: Submit is the manual path — an operator
// asking for this work now — and a pause suppresses the *schedule*, not the
// operator.
func TestAManualSubmitIgnoresAPause(t *testing.T) {
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	if _, err := q.Pause(ctx, acme, "jobs.reap", clk.Now().Add(time.Hour), operator); err != nil {
		t.Fatal(err)
	}
	if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "jobs.reap"}); err != nil {
		t.Fatalf("a manual submit was refused while the schedule was paused: %v", err)
	}
	list, err := q.List(ctx, acme, jobs.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("the manual job did not queue: %d rows", len(list))
	}
	// The pause is still in force afterwards.
	if _, err := q.PauseOf(ctx, acme, "jobs.reap"); err != nil {
		t.Fatalf("a manual run cleared the pause: %v", err)
	}
}

// TestAPauseWrittenMidTickLosesTheJobRatherThanRacingIt is the risk the design
// names, made deterministic: the pause is written *after* the scheduler read
// "not paused" and *before* its commit lands.
//
// It fails without the `tx.Expect(pauseKey, nil, false)` in
// SubmitUnlessPaused, and that is the point — reading before writing is enough
// only when nobody writes in between, and this is the case where somebody does.
// A per-tenant gate cannot help here and would not help across nodes: it makes
// the window small rather than closed, which is why the correctness lives in
// the conditional commit.
func TestAPauseWrittenMidTickLosesTheJobRatherThanRacingIt(t *testing.T) {
	ctx := context.Background()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	clk := clock.NewFake(clock.FakeStart)

	racing := &pauseRacer{KV: kv}
	q := jobs.NewQueue(racing, clk, jobs.WithoutGate())

	// The operator's pause is committed by the racer, through a queue of its
	// own so the interception below does not capture this write too.
	racing.pause = func() {
		plain := jobs.NewQueue(kv, clk)
		if _, err := plain.Pause(ctx, acme, "jobs.reap", clk.Now().Add(time.Hour), operator); err != nil {
			t.Errorf("the racing Pause failed: %v", err)
		}
	}

	queued, err := q.SubmitUnlessPaused(ctx, &jobs.Job{
		Tenant: acme, Namespace: tenant.DefaultNamespace, Type: "jobs.reap",
	})
	if err != nil {
		t.Fatalf("SubmitUnlessPaused: %v", err)
	}
	if queued {
		t.Fatal("a job was scheduled although the type was paused before the commit landed")
	}

	list, err := jobs.NewQueue(kv, clk).List(ctx, acme, jobs.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("the queue holds %d jobs the operator had already paused", len(list))
	}
}

// pauseRacer commits somebody else's pause between a batch being staged and it
// being applied, which is the only window the race has.
type pauseRacer struct {
	storage.KV
	pause func()
	once  sync.Once
}

func (r *pauseRacer) NewBatch() storage.Batch {
	return &racingBatch{Batch: r.KV.NewBatch(), racer: r}
}

type racingBatch struct {
	storage.Batch
	racer *pauseRacer
}

func (b *racingBatch) Commit(ctx context.Context, sync bool) error {
	b.racer.once.Do(func() {
		if b.racer.pause != nil {
			b.racer.pause()
		}
	})
	return b.Batch.Commit(ctx, sync)
}
