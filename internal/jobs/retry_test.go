package jobs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
)

func TestRetryBacksOffExponentially(t *testing.T) {
	// Three properties, and each of them fails differently. Windows that do not
	// grow are a tight loop against whatever broke; windows that do not stop
	// growing are a queue that silently goes stale; windows without jitter keep
	// every failing worker in lockstep so they collide at each doubling.
	var last time.Duration
	for attempt := uint32(1); attempt <= 8; attempt++ {
		w := jobs.BackoffWindow(attempt)
		if w <= last && w != jobs.RetryCap {
			t.Fatalf("the window for attempt %d is %s, not longer than %s", attempt, w, last)
		}
		if w > jobs.RetryCap {
			t.Fatalf("the window for attempt %d is %s, past the %s cap", attempt, w, jobs.RetryCap)
		}
		last = w
	}
	if got := jobs.BackoffWindow(64); got != jobs.RetryCap {
		t.Fatalf("a far-out attempt waits %s, want the cap %s", got, jobs.RetryCap)
	}
	// The shift must not overflow into a negative or tiny duration.
	if got := jobs.BackoffWindow(1 << 20); got != jobs.RetryCap {
		t.Fatalf("an absurd attempt count produced %s", got)
	}

	seen := map[time.Duration]bool{}
	for range 200 {
		d := jobs.BackoffDelay(4)
		if w := jobs.BackoffWindow(4); d < w/2 || d >= w {
			t.Fatalf("a drawn delay of %s is outside [%s, %s)", d, w/2, w)
		}
		seen[d] = true
	}
	if len(seen) < 10 {
		t.Fatalf("200 draws produced %d distinct delays; the jitter is not jittering", len(seen))
	}
}

func TestAFailedJobIsQueuedAgainLater(t *testing.T) {
	ctx := context.Background()
	q, clk, kv := newQueue(t)

	j := &jobs.Job{Tenant: acme, Type: "vector.rebuild", MaxAttempts: 3}
	if err := q.Submit(ctx, j); err != nil {
		t.Fatal(err)
	}
	claimed := claimOne(t, q)
	if err := q.Fail(ctx, claimed, errors.New("the model was unreachable")); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	if claimed.State != jobs.Retry {
		t.Fatalf("a failed job with attempts left is %s, want retry", claimed.State)
	}
	if claimed.Lease != nil {
		t.Fatal("a job waiting to retry still holds a lease")
	}
	if claimed.LastError == "" {
		t.Fatal("a failed job does not say why")
	}
	if !claimed.RunAt.After(clk.Now()) {
		t.Fatalf("a retry is due at %v, which is not after now %v", claimed.RunAt, clk.Now())
	}
	assertPartitionCounts(t, kv, acme, map[keys.JobState]int{keys.JobPending: 1})

	// It is not claimable until its backoff has passed, and then it is.
	if got, err := q.Claim(ctx, acme, owner, nil, 1); err != nil || len(got) != 0 {
		t.Fatalf("a backed-off job was claimed immediately: %d, %v", len(got), err)
	}
	clk.Advance(jobs.RetryCap)
	again := claimOne(t, q)
	if again.Attempts != 2 {
		t.Fatalf("the retried job is on attempt %d, want 2", again.Attempts)
	}
	if again.LastError == "" {
		t.Fatal("a retried job forgot what went wrong last time")
	}
}

func TestFailedAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	q, clk, kv := newQueue(t)

	j := &jobs.Job{Tenant: acme, Type: "vector.rebuild", MaxAttempts: 3}
	if err := q.Submit(ctx, j); err != nil {
		t.Fatal(err)
	}

	var last *jobs.Job
	for attempt := 1; attempt <= 3; attempt++ {
		last = claimOne(t, q)
		if int(last.Attempts) != attempt {
			t.Fatalf("attempt %d reports %d", attempt, last.Attempts)
		}
		if err := q.Fail(ctx, last, errors.New("still broken")); err != nil {
			t.Fatalf("Fail on attempt %d: %v", attempt, err)
		}
		clk.Advance(jobs.RetryCap)
	}

	if last.State != jobs.Failed {
		t.Fatalf("after three of three attempts the job is %s, want failed", last.State)
	}
	if last.LastError != "still broken" {
		t.Fatalf("the last error is %q", last.LastError)
	}
	assertPartitionCounts(t, kv, acme, map[keys.JobState]int{keys.JobDone: 1})
	if got, err := q.Claim(ctx, acme, owner, nil, 1); err != nil || len(got) != 0 {
		t.Fatalf("a failed job was claimed again: %d, %v", len(got), err)
	}
}

func TestAFailedJobKeepsItsCheckpoint(t *testing.T) {
	// This is what makes a retry cheaper than a first run: the handler resumes
	// from where it stopped rather than redoing the half it finished.
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "text.rebuild", MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	claimed := claimOne(t, q)
	if err := q.Save(ctx, claimed, []byte("halfway")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := q.Fail(ctx, claimed, errors.New("interrupted")); err != nil {
		t.Fatal(err)
	}

	clk.Advance(jobs.RetryCap)
	again := claimOne(t, q)
	if string(again.Checkpoint) != "halfway" {
		t.Fatalf("the retry starts from %q, want the saved cursor", again.Checkpoint)
	}
}

func TestFailWithoutAReasonStillSaysSomething(t *testing.T) {
	ctx := context.Background()
	q, _, _ := newQueue(t)
	if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "vector.rebuild", MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	claimed := claimOne(t, q)
	if err := q.Fail(ctx, claimed, nil); err != nil {
		t.Fatal(err)
	}
	if claimed.State != jobs.Failed || claimed.LastError == "" {
		t.Fatalf("a job failed with no cause reports %s / %q", claimed.State, claimed.LastError)
	}
}

func claimOne(t *testing.T, q *jobs.Queue) *jobs.Job {
	t.Helper()
	claimed, err := q.Claim(context.Background(), acme, owner, nil, 1)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(claimed))
	}
	return claimed[0]
}
