package server

import (
	"context"
	"errors"
	"testing"

	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// cancellingCheckpointer saves like the real one and cancels the handler's
// context on its first save — which, for a vector rebuild, is the moment the
// destructive half has committed and nothing has been re-inserted yet.
type cancellingCheckpointer struct {
	job    *jobs.Job
	cancel context.CancelFunc
	saves  int
}

func (c *cancellingCheckpointer) Save(_ context.Context, cursor []byte) error {
	c.saves++
	c.job.Checkpoint = append([]byte(nil), cursor...)
	if c.saves == 1 {
		c.cancel()
	}
	return nil
}

func (*cancellingCheckpointer) Processed(uint64) {}
func (*cancellingCheckpointer) Changed(uint64)   {}

// TestTheVectorRebuildJobResumesFromItsCheckpoint is the wiring half of
// hnsw's TestCrashDuringRebuildResumes: the handler hands the job's checkpoint
// to the index and the index's progress to the job, so a retried attempt
// continues rather than repeating the destructive half.
func TestTheVectorRebuildJobResumesFromItsCheckpoint(t *testing.T) {
	d := mustDepsWith(t, func(c *config.Config) { c.Vector.Index = "hnsw" })
	const tid = tenant.ID("acme")
	seedMemories(t, d, tid)
	ctx := context.Background()

	want, err := d.vectors.Stats(ctx, tid)
	if err != nil {
		t.Fatal(err)
	}
	if want.Vectors == 0 {
		t.Fatal("the fixture indexed no vectors, so a rebuild would prove nothing")
	}

	handler, err := d.registry.Handler(TypeVectorRebuild)
	if err != nil {
		t.Fatal(err)
	}
	j := &jobs.Job{Tenant: tid, Namespace: tenant.DefaultNamespace, Type: TypeVectorRebuild}

	first, cancel := context.WithCancel(ctx)
	cp := &cancellingCheckpointer{job: j, cancel: cancel}
	err = handler.Handle(first, j, cp)
	if err == nil {
		t.Fatal("the first attempt completed although its context was cancelled after the first save")
	}
	if first.Err() == nil || (!errors.Is(err, context.Canceled) && cp.saves == 0) {
		t.Fatalf("the first attempt failed before saving any progress: %v", err)
	}
	if len(j.Checkpoint) == 0 {
		t.Fatal("the handler saved no checkpoint onto the job")
	}

	// The retry: same job, carrying the checkpoint the first attempt saved.
	if err := handler.Handle(ctx, j, noCheckpoint{}); err != nil {
		t.Fatalf("the resumed attempt: %v", err)
	}
	got, err := d.vectors.Stats(ctx, tid)
	if err != nil {
		t.Fatal(err)
	}
	if got.Vectors != want.Vectors || got.Rebuilding {
		t.Fatalf("after resuming: %d vectors (rebuilding=%v), want %d and finished",
			got.Vectors, got.Rebuilding, want.Vectors)
	}
	health, err := d.vectors.Health(ctx, tid)
	if err != nil {
		t.Fatal(err)
	}
	if health.Degraded {
		t.Fatalf("the resumed index reports itself degraded: %s", health.Reason)
	}
}
