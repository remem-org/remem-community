package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/storage"
)

func TestCheckpointSurvivesRestart(t *testing.T) {
	// A job interrupted at 50% resumes from its cursor, not from the start.
	//
	// The restart is simulated by abandoning the queue and building a new one
	// over the same store — everything that survives a process restart is what
	// is on disk, and that is exactly what the second queue reads. The genuine
	// kill-and-restart is part of the end-to-end verification, because this
	// package may not import the Pebble adapter.
	ctx := context.Background()
	kv, clk := newStore(t)

	before := jobs.NewQueue(kv, clk)
	if err := before.Submit(ctx, &jobs.Job{Tenant: acme, Type: "text.rebuild", MaxAttempts: 5}); err != nil {
		t.Fatal(err)
	}
	claimed, err := before.Claim(ctx, acme, "node-before", nil, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim: %d, %v", len(claimed), err)
	}
	if err := before.Save(ctx, claimed[0], []byte("record-500-of-1000")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// The process dies here: no completion, no failure, no release.

	after := jobs.NewQueue(kv, clk)
	clk.Advance(after.Lease() + time.Millisecond)
	if n, err := after.Reclaim(ctx, acme, 10); err != nil || n != 1 {
		t.Fatalf("the new process reclaimed %d jobs, want 1 (%v)", n, err)
	}
	resumed, err := after.Claim(ctx, acme, "node-after", nil, 1)
	if err != nil || len(resumed) != 1 {
		t.Fatalf("Claim after restart: %d, %v", len(resumed), err)
	}
	if got := string(resumed[0].Checkpoint); got != "record-500-of-1000" {
		t.Fatalf("the resumed job starts from %q, want the cursor it saved", got)
	}
	if resumed[0].Attempts != 2 {
		t.Fatalf("the resumed job is on attempt %d, want 2", resumed[0].Attempts)
	}
}

func TestTheLatestCheckpointIsTheOneThatSurvives(t *testing.T) {
	ctx := context.Background()
	q, _, _ := newQueue(t)

	if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "text.rebuild"}); err != nil {
		t.Fatal(err)
	}
	j := claimOne(t, q)
	for _, cursor := range []string{"100", "200", "300"} {
		if err := q.Save(ctx, j, []byte(cursor)); err != nil {
			t.Fatalf("Save(%s): %v", cursor, err)
		}
	}
	stored, err := q.Get(ctx, acme, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(stored.Checkpoint); got != "300" {
		t.Fatalf("the stored cursor is %q, want the last one saved", got)
	}
}

func TestACheckpointAndARenewalDoNotOverwriteEachOther(t *testing.T) {
	// Both are read-modify-writes on one row, and the renewal timer runs beside
	// the handler. A transition built from the caller's copy rather than from
	// the durable row would make whichever landed second discard the other —
	// silently, and only under load.
	ctx := context.Background()
	q, clk, _ := newQueue(t)

	if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "text.rebuild"}); err != nil {
		t.Fatal(err)
	}
	j := claimOne(t, q)
	stale := j.Clone() // what a second goroutine holds: correct when it was taken

	if err := q.Save(ctx, j, []byte("halfway")); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)
	if err := q.Renew(ctx, stale); err != nil {
		t.Fatalf("Renew from a copy taken before the save: %v", err)
	}

	stored, err := q.Get(ctx, acme, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored.Checkpoint) != "halfway" {
		t.Fatalf("the renewal discarded the checkpoint: %q", stored.Checkpoint)
	}
	if !stored.Lease.ExpiresAt.After(j.Lease.ExpiresAt) {
		t.Fatal("the renewal did not extend the lease")
	}
}

func TestOnlyTheWorkerHoldingAJobMayRecordItsProgress(t *testing.T) {
	ctx := context.Background()
	q, _, _ := newQueue(t)

	j := &jobs.Job{Tenant: acme, Type: "text.rebuild"}
	if err := q.Submit(ctx, j); err != nil {
		t.Fatal(err)
	}
	if err := q.Save(ctx, j, []byte("cursor")); !errs.Is(err, errs.Invalid) {
		t.Fatalf("progress was recorded against a job nobody is running: %v", err)
	}
}

func TestAnOversizedCheckpointIsRefused(t *testing.T) {
	// A cursor is a position, not a working set. Storing one is a row every
	// claim scan pays to skip.
	ctx := context.Background()
	q, _, _ := newQueue(t)

	if err := q.Submit(ctx, &jobs.Job{Tenant: acme, Type: "text.rebuild"}); err != nil {
		t.Fatal(err)
	}
	j := claimOne(t, q)
	if err := q.Save(ctx, j, make([]byte, jobs.MaxCheckpointBytes+1)); !errs.Is(err, errs.Invalid) {
		t.Fatalf("an oversized checkpoint was stored: %v", err)
	}
}

// newStore returns a store and a clock without a queue over them, for the
// tests that build their own queues to stand in for two processes.
func newStore(t *testing.T) (storage.KV, *clock.Fake) {
	t.Helper()
	_, clk, kv := newQueue(t)
	return kv, clk
}
