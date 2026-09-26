//go:build crash

package crash

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/indexes"
)

const (
	rebuildVectors = 5000
	rebuildDim     = 16
	rebuildBatch   = 500
	rebuildType    = jobs.Type("crash.vector_rebuild")
	rebuildTenant  = tenant.ID("acme")
	rebuildIDPath  = "crash/rebuild-job-id"
	// rebuildKillAt is how many checkpoint lines the first life prints before it
	// is killed: one when the replace commits, then one per 500 inserted — so
	// three is a kill with 1,000 of 5,000 inserted.
	rebuildKillAt = 3
)

func init() { scenarios["rebuild"] = childRebuild }

// rebuildRID and rebuildVector are the corpus, derived from the index alone so
// the parent can regenerate what the child stored.
func rebuildRID(i int) id.ID {
	var rid id.ID
	rid[6] = 0x70 // version 7, so the id reads as a well-formed one
	rid[8] = 0x80
	binary.BigEndian.PutUint32(rid[12:], uint32(i+1))
	return rid
}

func rebuildVector(i int) []float32 {
	r := rand.New(rand.NewPCG(uint64(i), 0x5eed))
	v := make([]float32, rebuildDim)
	var sum float64
	for d := range v {
		v[d] = float32(r.NormFloat64())
		sum += float64(v[d]) * float64(v[d])
	}
	inv := float32(1 / math.Sqrt(sum))
	for d := range v {
		v[d] *= inv
	}
	return v
}

func openRebuildIndex(t *testing.T, kv storage.KV) vector.Index {
	t.Helper()
	// Through indexes, not hnsw: outside internal/vector/hnsw nothing names the
	// approximate index's concepts, and a test is no exception.
	index, err := indexes.Open(kv, indexes.Config{Kind: "hnsw", Metric: "cosine"})
	if err != nil {
		t.Fatalf("opening the approximate index: %v", err)
	}
	return index
}

// insertedByCursor reads the count a rebuild cursor carries.
//
// The layout — a version byte, a phase byte, then a uvarint of records inserted
// by this attempt — is private to the index. This test reads it knowingly, to
// print progress the parent can act on; if the layout changes, this line is
// where the crash test says so.
func insertedByCursor(c []byte) uint64 {
	if len(c) < 3 {
		return 0
	}
	n, _ := binary.Uvarint(c[2:])
	return n
}

// childRebuild seeds canonical vectors on the first life, then runs a vector
// rebuild as a job — through the real queue and pool, with the job's checkpoint
// as the rebuild's cursor, exactly as the server's vector.rebuild handler does —
// until the job completes or the parent kills the process.
func childRebuild(t *testing.T, dir string) {
	ctx := context.Background()
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatalf("opening %s: %v", dir, err)
	}
	defer func() { _ = kv.Close() }()

	clk := clock.System()
	tenants := tenantkv.New(kv, clk)
	if _, err := tenant.Ensure(ctx, tenants, rebuildTenant); err != nil {
		t.Fatal(err)
	}

	store := vector.NewStore(kv)
	if _, err := store.Get(ctx, rebuildTenant, tenant.DefaultNamespace, rebuildRID(0)); errs.Is(err, errs.NotFound) {
		for start := 0; start < rebuildVectors; start += rebuildBatch {
			tx := txn.New(kv)
			for i := start; i < min(start+rebuildBatch, rebuildVectors); i++ {
				if err := store.Stage(tx, rebuildTenant, tenant.DefaultNamespace, rebuildRID(i),
					&vector.Vector{ModelID: "test", Dim: rebuildDim, Values: rebuildVector(i)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			tx.Close()
		}
	} else if err != nil {
		t.Fatal(err)
	}

	index := openRebuildIndex(t, kv)
	queue := jobs.NewQueue(kv, clk, jobs.WithLease(jobLease), jobs.WithSyncWrites(true))
	registry := jobs.NewRegistry()
	if err := registry.Register(jobs.Entry{
		Type:        rebuildType,
		Description: "rebuild the approximate index in batches of 500",
		Handler: jobs.HandlerFunc(func(ctx context.Context, j *jobs.Job, cp jobs.Checkpointer) error {
			if len(j.Checkpoint) == 0 {
				fmt.Println("rebuild resumed from none")
			} else {
				fmt.Printf("rebuild resumed from a cursor of %d bytes\n", len(j.Checkpoint))
			}
			var last uint64
			err := index.Rebuild(ctx, j.Tenant, vector.NewStore(kv),
				vector.WithBatchSize(rebuildBatch),
				vector.WithProgress(j.Checkpoint, func(ctx context.Context, c []byte) error {
					if err := cp.Save(ctx, c); err != nil {
						return err
					}
					last = insertedByCursor(c)
					fmt.Printf("rebuild checkpoint %d\n", last)
					return nil
				}))
			if err == nil {
				fmt.Printf("rebuild finished inserted=%d\n", last)
			}
			return err
		}),
		MaxAttempts: 10,
	}); err != nil {
		t.Fatal(err)
	}

	jid := submitJobOnce(t, ctx, kv, queue, rebuildTenant, rebuildType, rebuildIDPath)
	runJobToCompletion(t, kv, tenants, queue, registry, rebuildTenant, jid)
}

// TestCrashDuringRebuildResumesAcrossAProcess is Task 1.3's property across a
// real kill. hnsw's TestCrashDuringRebuildResumes interrupts a rebuild with a
// cancelled context inside one process; this one kills the process a fifth of
// the way through and lets a second one — which knows nothing but what is on
// disk and in the job row — finish it.
func TestCrashDuringRebuildResumesAcrossAProcess(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	first := spawn(t, "rebuild", dir)
	if got := first.waitFor("rebuild resumed from ", time.Minute); got != "none" {
		t.Fatalf("the first attempt resumed from %q; nothing had been saved", got)
	}
	var killedAt string
	for range rebuildKillAt {
		killedAt = first.waitFor("rebuild checkpoint ", 2*time.Minute)
	}
	first.kill()
	t.Logf("killed the first life at checkpoint %s of %d", killedAt, rebuildVectors)

	second := spawn(t, "rebuild", dir)
	if got := second.waitFor("rebuild resumed from ", time.Minute); got == "none" {
		t.Fatal("the second attempt saw no cursor: it restarted the rebuild rather than resuming it")
	}
	inserted, err := strconv.Atoi(second.waitFor("rebuild finished inserted=", 3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	second.waitFor("job completed", time.Minute)
	second.wait(time.Minute)
	if inserted <= 0 || inserted >= rebuildVectors {
		t.Fatalf("the resumed attempt inserted %d of %d records: it should continue, not restart", inserted, rebuildVectors)
	}
	t.Logf("the second life inserted %d of %d", inserted, rebuildVectors)

	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	index := openRebuildIndex(t, kv)
	ctx := context.Background()

	stats, err := index.Stats(ctx, rebuildTenant)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Vectors != rebuildVectors || stats.Rebuilding {
		t.Fatalf("after the resumed rebuild: %d vectors, rebuilding=%v; want %d and finished",
			stats.Vectors, stats.Rebuilding, rebuildVectors)
	}
	for i := 0; i < rebuildVectors; i += 97 {
		hits, err := index.Search(ctx, rebuildTenant, rebuildVector(i), 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) != 1 || hits[0].ID != rebuildRID(i) {
			t.Fatalf("vector %d does not find itself after the resumed rebuild", i)
		}
	}
}
