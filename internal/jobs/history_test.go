package jobs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// TestEveryAttemptLeavesItsOwnHistoryRow is the reason history is not the job
// row: the row aggregates an attempt count and a terminal state, and cannot say
// what each retry did. Two failures and a success are three different
// executions and three different answers to "what happened".
func TestEveryAttemptLeavesItsOwnHistoryRow(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)

	var runs atomic.Int32
	h.register("vector.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error {
		if runs.Add(1) <= 2 {
			return errors.New("the index was busy")
		}
		return nil
	})
	j := h.submitWith("vector.rebuild", func(j *jobs.Job) { j.MaxAttempts = 3 })

	for range 3 {
		h.pollAndWait(ctx)
		h.clk.Advance(time.Hour) // past the retry backoff
	}
	if got := h.get(ctx, j); got.State != jobs.Completed {
		t.Fatalf("the job ended as %s: %q", got.State, got.LastError)
	}

	history := h.history(ctx, jobs.RunFilter{})
	if len(history) != 3 {
		t.Fatalf("three attempts left %d history rows: %+v", len(history), history)
	}
	// Newest first: an operator opening a history wants the last thing that
	// happened, not the first.
	wantOutcome := []jobs.State{jobs.Completed, jobs.Retry, jobs.Retry}
	wantAttempt := []uint32{3, 2, 1}
	for i, r := range history {
		if r.JobID != j.ID || r.Type != "vector.rebuild" {
			t.Errorf("row %d belongs to %s/%s", i, r.JobID, r.Type)
		}
		if r.Attempt != wantAttempt[i] {
			t.Errorf("row %d is attempt %d, want %d", i, r.Attempt, wantAttempt[i])
		}
		if r.Outcome != wantOutcome[i] {
			t.Errorf("row %d ended as %s, want %s", i, r.Outcome, wantOutcome[i])
		}
		if r.StartedAt.IsZero() || r.FinishedAt.IsZero() || r.FinishedAt.Before(r.StartedAt) {
			t.Errorf("row %d has no usable timing: %v → %v", i, r.StartedAt, r.FinishedAt)
		}
		if r.Owner == "" {
			t.Errorf("row %d does not say which node ran it", i)
		}
	}
	if history[0].Error != "" {
		t.Errorf("the successful attempt carries an error: %q", history[0].Error)
	}
	for _, r := range history[1:] {
		if r.Error == "" {
			t.Error("a failed attempt does not say why")
		}
	}
}

func TestHistoryPagesContinueAcrossFilteredRowsAndValidateTenant(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)
	base := h.clk.Now().UTC()
	for i, typ := range []jobs.Type{"vector.rebuild", "text.rebuild", "vector.rebuild"} {
		if err := h.queue.RecordRun(ctx, &jobs.Run{
			JobID: id.New(), Tenant: acme, Namespace: tenant.DefaultNamespace,
			Type: typ, Attempt: 1, StartedAt: base.Add(time.Duration(i) * time.Second),
			FinishedAt: base.Add(time.Duration(i) * time.Second), Outcome: jobs.Completed,
		}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := h.queue.RunsPage(ctx, acme, jobs.RunFilter{Types: []jobs.Type{"vector.rebuild"}, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Runs) != 1 || first.Runs[0].Attempt != 1 || !first.HasMore || len(first.NextKey) == 0 {
		t.Fatalf("first filtered page = %+v", first)
	}
	second, err := h.queue.RunsPage(ctx, acme, jobs.RunFilter{
		Types: []jobs.Type{"vector.rebuild"}, Limit: 1, BeforeKey: first.NextKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Runs) != 1 || second.Runs[0].JobID == first.Runs[0].JobID || second.HasMore {
		t.Fatalf("continuation page = %+v", second)
	}
	if _, err := h.queue.RunsPage(ctx, "other", jobs.RunFilter{BeforeKey: first.NextKey}); err == nil {
		t.Fatal("a history cursor was accepted for another tenant")
	}
}

func TestHistoryContinuationCrossesTheFilteredScanBound(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)
	base := h.clk.Now().UTC()
	want := id.New()
	if err := h.queue.RecordRun(ctx, &jobs.Run{
		JobID: want, Tenant: acme, Namespace: tenant.DefaultNamespace,
		Type: "vector.rebuild", Attempt: 1, StartedAt: base, FinishedAt: base,
		Outcome: jobs.Completed,
	}); err != nil {
		t.Fatal(err)
	}
	for i := range 10_000 {
		finished := base.Add(time.Duration(i+1) * time.Second)
		if err := h.queue.RecordRun(ctx, &jobs.Run{
			JobID: id.New(), Tenant: acme, Namespace: tenant.DefaultNamespace,
			Type: "text.rebuild", Attempt: 1, StartedAt: finished, FinishedAt: finished,
			Outcome: jobs.Completed,
		}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := h.queue.RunsPage(ctx, acme, jobs.RunFilter{Types: []jobs.Type{"vector.rebuild"}, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Runs) != 0 || !first.HasMore || len(first.NextKey) == 0 {
		t.Fatalf("the capped filtered page hid the continuation: %+v", first)
	}
	second, err := h.queue.RunsPage(ctx, acme, jobs.RunFilter{
		Types: []jobs.Type{"vector.rebuild"}, Limit: 1, BeforeKey: first.NextKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Runs) != 1 || second.Runs[0].JobID != want {
		t.Fatalf("continuation after the scan cap returned %+v", second)
	}
}

func TestStaleWorkerCountsAreNotAttachedToANewerCancelledAttempt(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	h.register("vector.rebuild", func(_ context.Context, _ *jobs.Job, cp jobs.Checkpointer) error {
		cp.Processed(50)
		cp.Changed(7)
		close(started)
		<-release
		return nil
	})
	j := h.submit("vector.rebuild")
	if _, err := h.pool.Poll(ctx); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	<-started

	// Reclaim the still-running local attempt, then charge a new claim. The
	// old worker remains in this pool's map but no longer owns the lease.
	h.clk.Advance(h.queue.Lease())
	if n, err := h.queue.Reclaim(ctx, acme, 1); err != nil || n != 1 {
		t.Fatalf("Reclaim = %d, %v", n, err)
	}
	claimed, err := h.queue.Claim(ctx, acme, "node-new", nil, 1)
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 2 {
		t.Fatalf("new claim = %+v, %v", claimed, err)
	}
	if err := h.pool.Cancel(ctx, acme, j.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	close(release)
	h.pool.Wait()

	history := h.history(ctx, jobs.RunFilter{})
	if len(history) != 1 || history[0].Outcome != jobs.Cancelled || history[0].Attempt != 2 {
		t.Fatalf("cancelled attempt history = %+v", history)
	}
	if history[0].RecordsProcessed != nil || history[0].RecordsChanged != nil {
		t.Fatalf("stale local counts were attributed to the newer attempt: %+v", history[0])
	}
}

// TestAnAttemptReportsTheRecordsItTouched: the counts come from the handler and
// from nowhere else. Inferring them from a checkpoint, an elapsed time or the
// size of an index afterwards would produce a number that looks exact and is
// not.
func TestAnAttemptReportsTheRecordsItTouched(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)

	h.register("lifecycle.maintain", func(_ context.Context, _ *jobs.Job, cp jobs.Checkpointer) error {
		cp.Processed(400)
		cp.Changed(12)
		// The last report wins: a handler counting as it goes reports totals,
		// not increments.
		cp.Processed(900)
		cp.Changed(31)
		return nil
	})
	h.submit("lifecycle.maintain")
	h.pollAndWait(ctx)

	history := h.history(ctx, jobs.RunFilter{})
	if len(history) != 1 {
		t.Fatalf("one attempt left %d rows", len(history))
	}
	r := history[0]
	if r.RecordsProcessed == nil || *r.RecordsProcessed != 900 {
		t.Errorf("records_processed is %v, want 900", r.RecordsProcessed)
	}
	if r.RecordsChanged == nil || *r.RecordsChanged != 31 {
		t.Errorf("records_changed is %v, want 31", r.RecordsChanged)
	}
}

// TestAFailedAttemptKeepsThePartialCountsItReported: a handler that broke after
// doing half its work did that half, and the history has to say so. Discarding
// the counts because the attempt failed would make a failure look like it did
// nothing.
func TestAFailedAttemptKeepsThePartialCountsItReported(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)

	h.register("lifecycle.maintain", func(_ context.Context, _ *jobs.Job, cp jobs.Checkpointer) error {
		cp.Processed(50)
		cp.Changed(7)
		return errors.New("the store went away")
	})
	h.submitWith("lifecycle.maintain", func(j *jobs.Job) { j.MaxAttempts = 1 })
	h.pollAndWait(ctx)

	history := h.history(ctx, jobs.RunFilter{})
	if len(history) != 1 {
		t.Fatalf("one attempt left %d rows", len(history))
	}
	r := history[0]
	if r.Outcome != jobs.Failed || r.Error == "" {
		t.Fatalf("the attempt reads as %s: %q", r.Outcome, r.Error)
	}
	if r.RecordsProcessed == nil || *r.RecordsProcessed != 50 {
		t.Errorf("a failed attempt lost its processed count: %v", r.RecordsProcessed)
	}
	if r.RecordsChanged == nil || *r.RecordsChanged != 7 {
		t.Errorf("a failed attempt lost its changed count: %v", r.RecordsChanged)
	}
}

// TestACountNobodyReportedIsUnavailableRatherThanZero: "this attempt changed
// nothing" and "nobody counted" are different answers, and a zero that means
// the second is a number an operator will act on.
func TestACountNobodyReportedIsUnavailableRatherThanZero(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)

	h.register("vector.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error {
		return nil // reports nothing at all
	})
	h.register("text.rebuild", func(_ context.Context, _ *jobs.Job, cp jobs.Checkpointer) error {
		cp.Processed(3) // and nothing about what it changed
		return nil
	})
	h.submit("vector.rebuild")
	h.pollAndWait(ctx)
	h.submit("text.rebuild")
	h.pollAndWait(ctx)

	silent := h.historyOf(ctx, "vector.rebuild")
	if len(silent) != 1 {
		t.Fatalf("one attempt left %d rows", len(silent))
	}
	if silent[0].RecordsProcessed != nil || silent[0].RecordsChanged != nil {
		t.Fatalf("a handler that counted nothing was given counts: %v, %v",
			silent[0].RecordsProcessed, silent[0].RecordsChanged)
	}

	half := h.historyOf(ctx, "text.rebuild")
	if len(half) != 1 {
		t.Fatalf("one attempt left %d rows", len(half))
	}
	if half[0].RecordsProcessed == nil || *half[0].RecordsProcessed != 3 {
		t.Errorf("the reported count is %v, want 3", half[0].RecordsProcessed)
	}
	if half[0].RecordsChanged != nil {
		t.Errorf("the unreported count was filled in as %v", *half[0].RecordsChanged)
	}
}

// TestAReportIsScopedToItsOwnAttempt: a retry starts from nothing. Carrying the
// previous attempt's counts forward would double-count the records both visited,
// and at-least-once delivery makes that the ordinary case rather than a rare one.
func TestAReportIsScopedToItsOwnAttempt(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)

	var runs atomic.Int32
	h.register("lifecycle.maintain", func(_ context.Context, _ *jobs.Job, cp jobs.Checkpointer) error {
		if runs.Add(1) == 1 {
			cp.Processed(10)
			cp.Changed(10)
			return errors.New("broke after ten")
		}
		cp.Processed(4)
		return nil
	})
	h.submitWith("lifecycle.maintain", func(j *jobs.Job) { j.MaxAttempts = 2 })
	h.pollAndWait(ctx)
	h.clk.Advance(time.Hour)
	h.pollAndWait(ctx)

	history := h.history(ctx, jobs.RunFilter{})
	if len(history) != 2 {
		t.Fatalf("two attempts left %d rows", len(history))
	}
	second, first := history[0], history[1]
	if second.RecordsProcessed == nil || *second.RecordsProcessed != 4 {
		t.Errorf("the second attempt reports %v processed, want 4", second.RecordsProcessed)
	}
	if second.RecordsChanged != nil {
		t.Errorf("the second attempt inherited a changed count of %v", *second.RecordsChanged)
	}
	if first.RecordsProcessed == nil || *first.RecordsProcessed != 10 {
		t.Errorf("the first attempt reports %v processed, want 10", first.RecordsProcessed)
	}
}

// TestAWorkerThatLostItsJobDoesNotWriteDuplicateHistory verifies fencing while
// preserving the cancellation row written atomically with the operator action.
//
// The mechanism it pins is that history is written only *after* a terminal
// transition that succeeded: this worker's Complete meets a row that has moved
// and fails with a conflict, so nothing is recorded. It was verified to fail
// with recordAttempt hoisted out of that check. The other half — a worker the
// renewal pass has already told to stop — is held by
// TestCancellingARunningJobStopsItsHandler, which drives the renewal.
func TestAWorkerThatLostItsJobWritesNoHistory(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)

	started := make(chan struct{})
	release := make(chan struct{})
	h.register("vector.rebuild", func(hctx context.Context, _ *jobs.Job, cp jobs.Checkpointer) error {
		cp.Processed(50)
		cp.Changed(7)
		close(started)
		select {
		case <-release:
		case <-hctx.Done():
		}
		return nil
	})
	j := h.submit("vector.rebuild")

	if _, err := h.pool.Poll(ctx); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	<-started
	// Cancelled where it stands: the row moves and the worker holding it no
	// longer owns the outcome.
	if err := h.pool.Cancel(ctx, acme, j.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	close(release)
	h.pool.Wait()

	if history := h.history(ctx, jobs.RunFilter{}); len(history) != 1 || history[0].Outcome != jobs.Cancelled {
		t.Fatalf("cancelling a running attempt should leave one cancelled row: %+v", history)
	} else if history[0].RecordsProcessed == nil || *history[0].RecordsProcessed != 50 ||
		history[0].RecordsChanged == nil || *history[0].RecordsChanged != 7 {
		t.Fatalf("cancellation did not preserve reported partial counts: %+v", history[0])
	}
}

func TestHistoryIsScopedToOneTenant(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)
	h.provision(ctx, "other")

	h.register("vector.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error { return nil })
	h.submit("vector.rebuild")
	h.pollAndWait(ctx)

	if len(h.history(ctx, jobs.RunFilter{})) != 1 {
		t.Fatal("the tenant that ran the job has no history")
	}
	elsewhere, err := h.queue.RunsPage(ctx, "other", jobs.RunFilter{})
	if err != nil {
		t.Fatalf("Runs: %v", err)
	}
	if len(elsewhere.Runs) != 0 {
		t.Fatalf("one tenant's history reached another: %+v", elsewhere.Runs)
	}
	if _, err := h.queue.RunsPage(ctx, "", jobs.RunFilter{}); err == nil {
		t.Fatal("an unscoped history read was allowed (Invariant 1)")
	}
}

func TestHistoryFiltersByTypeAndIsBounded(t *testing.T) {
	ctx := context.Background()
	// Two workers, because each poll claims at most one job per idle slot and
	// this wants both of a round's jobs to run before the clock moves.
	h := newHarness(t, 2)

	h.register("vector.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error { return nil })
	h.register("text.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error { return nil })
	for range 3 {
		h.submit("vector.rebuild")
		h.submit("text.rebuild")
		h.pollAndWait(ctx)
		h.clk.Advance(time.Second)
	}

	if all := h.history(ctx, jobs.RunFilter{}); len(all) != 6 {
		t.Fatalf("six attempts left %d rows", len(all))
	}
	only := h.historyOf(ctx, "text.rebuild")
	if len(only) != 3 {
		t.Fatalf("filtering by type returned %d rows, want 3", len(only))
	}
	for _, r := range only {
		if r.Type != "text.rebuild" {
			t.Fatalf("the filter returned a %s row", r.Type)
		}
	}
	bounded, err := h.queue.RunsPage(ctx, acme, jobs.RunFilter{Limit: 2})
	if err != nil || len(bounded.Runs) != 2 || !bounded.HasMore {
		t.Fatalf("a limit of 2 returned %+v, %v", bounded, err)
	}
	if capped := h.history(ctx, jobs.RunFilter{Limit: jobs.MaxRunListLimit + 100}); len(capped) != 6 {
		t.Fatalf("an over-large limit returned %d rows", len(capped))
	}
}

// TestHistoryAgesOutWithTheRetention: the window is the configured job
// retention, and it is the same reaper that bounds the job rows. Unbounded
// history is a second copy of the queue that nothing ever removes.
func TestHistoryAgesOutWithTheRetention(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 1)

	h.register("vector.rebuild", func(context.Context, *jobs.Job, jobs.Checkpointer) error { return nil })
	h.submit("vector.rebuild")
	h.pollAndWait(ctx)
	old := h.clk.Now()

	h.clk.Advance(48 * time.Hour)
	h.submit("vector.rebuild")
	h.pollAndWait(ctx)

	if before := h.history(ctx, jobs.RunFilter{}); len(before) != 2 {
		t.Fatalf("two attempts left %d rows", len(before))
	}
	removed, err := h.queue.ReapRuns(ctx, acme, old.Add(time.Second), 100)
	if err != nil {
		t.Fatalf("ReapRuns: %v", err)
	}
	if removed != 1 {
		t.Fatalf("the reaper removed %d rows, want 1", removed)
	}
	after := h.history(ctx, jobs.RunFilter{})
	if len(after) != 1 {
		t.Fatalf("after reaping, %d rows remain", len(after))
	}
	if after[0].FinishedAt.Before(old.Add(time.Second)) {
		t.Fatal("the reaper removed the wrong end of the history")
	}
}

// history reads the tenant's attempt history through the queue.
func (h *harness) history(ctx context.Context, f jobs.RunFilter) []*jobs.Run {
	h.t.Helper()
	page, err := h.queue.RunsPage(ctx, acme, f)
	if err != nil {
		h.t.Fatalf("Runs: %v", err)
	}
	if page.HasMore {
		h.t.Fatal("history helper cannot discard a continuation page")
	}
	return page.Runs
}

func (h *harness) historyOf(ctx context.Context, typ jobs.Type) []*jobs.Run {
	h.t.Helper()
	return h.history(ctx, jobs.RunFilter{Types: []jobs.Type{typ}})
}
