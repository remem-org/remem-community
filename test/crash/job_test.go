//go:build crash

package crash

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/txn"
)

const (
	jobItems      = 1000
	jobEvery      = 100
	jobType       = jobs.Type("crash.count")
	jobTenant     = tenant.ID("acme")
	jobKillAfter  = 300
	jobLease      = time.Second
	jobItemPause  = 2 * time.Millisecond
	jobIDKeyPath  = "crash/job-id"
	noCursorLabel = "none"
)

func init() { scenarios["job"] = childJob }

// jobItemMark is the durable per-item counter the handler writes, one synced
// commit per item.
func jobItemMark(i int) []byte {
	var rid id.ID
	binary.BigEndian.PutUint64(rid[8:], uint64(i))
	return keys.Session(jobTenant, tenant.DefaultNamespace, rid)
}

// childJob runs the real queue, registry and worker pool over a real Pebble
// directory. On its first life it submits one job; on every life it runs the
// pool until that job completes.
func childJob(t *testing.T, dir string) {
	ctx := context.Background()

	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatalf("opening %s: %v", dir, err)
	}
	defer func() { _ = kv.Close() }()

	clk := clock.System()
	dirTenants := tenantkv.New(kv, clk)
	if _, err := tenant.Ensure(ctx, dirTenants, jobTenant); err != nil {
		t.Fatalf("ensuring the tenant: %v", err)
	}

	queue := jobs.NewQueue(kv, clk, jobs.WithLease(jobLease), jobs.WithSyncWrites(true))
	registry := jobs.NewRegistry()
	if err := registry.Register(jobs.Entry{
		Type:        jobType,
		Description: "count a thousand items, checkpointing every hundred",
		Handler:     jobs.HandlerFunc(countItems(kv)),
		MaxAttempts: 10,
	}); err != nil {
		t.Fatal(err)
	}

	jid := submitJobOnce(t, ctx, kv, queue, jobTenant, jobType, jobIDKeyPath)
	runJobToCompletion(t, kv, dirTenants, queue, registry, jobTenant, jid)
}

// countItems marks items in order, saving the next item to do every hundred.
//
// Work and checkpoint are two commits, by the framework's design: a handler's
// progress is recorded through its Checkpointer, not inside its own
// transactions. So an item after the last saved cursor may legitimately be done
// twice after a kill; an item before it must not be.
func countItems(kv storage.KV) func(ctx context.Context, j *jobs.Job, cp jobs.Checkpointer) error {
	return func(ctx context.Context, j *jobs.Job, cp jobs.Checkpointer) error {
		start := 0
		if len(j.Checkpoint) == 8 {
			start = int(binary.BigEndian.Uint64(j.Checkpoint))
			fmt.Printf("job resumed from %d\n", start)
		} else {
			fmt.Printf("job resumed from %s\n", noCursorLabel)
		}
		for i := start; i < jobItems; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			tx := txn.New(kv)
			prev, err := tx.Get(jobItemMark(i))
			if err != nil && !errs.Is(err, errs.NotFound) {
				tx.Close()
				return err
			}
			count := byte(0)
			if len(prev) == 1 {
				count = prev[0]
			}
			tx.Set(jobItemMark(i), []byte{count + 1})
			if err := tx.Commit(ctx); err != nil {
				tx.Close()
				return err
			}
			tx.Close()
			time.Sleep(jobItemPause)

			if done := i + 1; done%jobEvery == 0 {
				cursor := binary.BigEndian.AppendUint64(nil, uint64(done))
				if err := cp.Save(ctx, cursor); err != nil {
					return err
				}
				fmt.Printf("job checkpoint %d\n", done)
			}
		}
		return nil
	}
}

// TestCrashDuringJobResumes: kill a worker mid-job, and a second process
// reclaims the job once the dead worker's lease lapses, resumes from the saved
// checkpoint rather than the beginning, and completes it — with no item before
// the checkpoint done twice.
func TestCrashDuringJobResumes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	first := spawn(t, "job", dir)
	if got := first.waitFor("job resumed from ", time.Minute); got != noCursorLabel {
		t.Fatalf("the first attempt resumed from %q; nothing should have been saved yet", got)
	}
	for {
		n, err := strconv.Atoi(first.waitFor("job checkpoint ", time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if n >= jobKillAfter {
			break
		}
	}
	first.kill()

	second := spawn(t, "job", dir)
	started := time.Now()
	resumed := second.waitFor("job resumed from ", time.Minute)
	if resumed == noCursorLabel {
		t.Fatal("the reclaimed attempt saw no checkpoint: it restarted the job rather than resuming it")
	}
	from, err := strconv.Atoi(resumed)
	if err != nil {
		t.Fatal(err)
	}
	if from < jobKillAfter {
		t.Fatalf("resumed from %d; the first attempt had saved at least %d before it was killed", from, jobKillAfter)
	}
	second.waitFor("job completed", 2*time.Minute)
	second.wait(time.Minute)
	t.Logf("resumed from item %d, %s after the restart (lease %s)", from, time.Since(started).Round(time.Millisecond), jobLease)

	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	var twiceBefore, never []string
	redone := 0
	for i := 0; i < jobItems; i++ {
		v, err := kv.Get(context.Background(), jobItemMark(i))
		switch {
		case errs.Is(err, errs.NotFound):
			never = append(never, strconv.Itoa(i))
		case err != nil:
			t.Fatal(err)
		case len(v) == 1 && v[0] > 1 && i < from:
			twiceBefore = append(twiceBefore, strconv.Itoa(i))
		case len(v) == 1 && v[0] > 1:
			redone++
		}
	}
	if len(never) > 0 {
		t.Fatalf("%d items were never processed: %s", len(never), strings.Join(never[:min(10, len(never))], ", "))
	}
	if len(twiceBefore) > 0 {
		t.Fatalf("%d items before the checkpoint at %d were processed again: %s",
			len(twiceBefore), from, strings.Join(twiceBefore[:min(10, len(twiceBefore))], ", "))
	}
	t.Logf("%d items after the checkpoint were done twice, which work-then-checkpoint permits", redone)
}
