package memory_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/discovery"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

// discoveryType is the durable job type the composition root registers. It is
// spelled out here rather than imported from internal/server, because the point
// of the option is that internal/memory does not know the name.
const discoveryType jobs.Type = "discovery.similar"

// failingIndexer aborts the record transaction from inside it, which is the
// only way to test that the enqueue commits with the write rather than beside
// it: an embedder that failed would abort before the transaction exists.
type failingIndexer struct{ err error }

func (f failingIndexer) Stage(context.Context, txn.Tx, tenant.ID, tenant.Namespace, id.ID, *record.Record) error {
	return f.err
}

type discoverySetup struct {
	svc   *memory.Service
	queue *jobs.Queue
}

func newDiscoveringService(t *testing.T, indexers ...record.Indexer) discoverySetup {
	t.Helper()
	clk := clock.NewFake(clock.FakeStart)
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	texts := text.New()
	opts := []record.RepoOption{
		record.WithIndexer(attr.NewIndexer(attr.MustTable())),
		record.WithIndexer(texts),
	}
	for _, ix := range indexers {
		opts = append(opts, record.WithIndexer(ix))
	}
	repo := record.NewRepo(kv, opts...)
	queue := jobs.NewQueue(kv, clk)
	index := flat.New(vector.NewStore(kv), distance.L2)
	svc := memory.New(kv, repo, index, embeddingtest.New(), clk, attr.MustTable(),
		graph.NewService(kv, clk), memory.Config{},
		memory.WithTextIndex(texts),
		memory.WithDiscovery(discovery.NewEnqueuer(queue, discoveryType)))
	t.Cleanup(func() { _ = svc.Close() })
	return discoverySetup{svc: svc, queue: queue}
}

func discoveryJobs(t *testing.T, q *jobs.Queue, tid tenant.ID) []*jobs.Job {
	t.Helper()
	all, err := q.List(context.Background(), tid, jobs.Filter{Limit: jobs.MaxListLimit})
	must(t, err)
	var out []*jobs.Job
	for _, j := range all {
		if j.Type == discoveryType {
			out = append(out, j)
		}
	}
	return out
}

// The completion criterion of Task 1, and the whole reason discovery moved onto
// the job framework: a failed memory write leaves no orphan job.
//
// Rust's enqueue is a try_send after the write, so the two can disagree in both
// directions — a stored memory with no queued discovery when the channel is
// full, and nothing at all to say it was owed any.
func TestDiscoveryJobIsEnqueuedTransactionally(t *testing.T) {
	ctx := acmeCtx()

	t.Run("a successful write queues one", func(t *testing.T) {
		s := newDiscoveringService(t)
		m, err := s.svc.Create(ctx, memory.CreateReq{Content: "the transaction is the enqueue"})
		must(t, err)

		queued := discoveryJobs(t, s.queue, "acme")
		if len(queued) != 1 {
			t.Fatalf("a create queued %d discovery jobs, want exactly one", len(queued))
		}
		subjects, err := discovery.DecodePayload(queued[0].Payload)
		must(t, err)
		if len(subjects) != 1 || subjects[0] != m.ID {
			t.Fatalf("the queued job names %v, want just the memory %s", subjects, m.ID)
		}
		if queued[0].State != jobs.Pending {
			t.Fatalf("the queued job is %s, want pending", queued[0].State)
		}
	})

	t.Run("a failed write queues none", func(t *testing.T) {
		boom := errors.New("the derived index refused this record")
		s := newDiscoveringService(t, failingIndexer{err: boom})

		if _, err := s.svc.Create(ctx, memory.CreateReq{Content: "this write does not land"}); err == nil {
			t.Fatal("the create succeeded; the failing indexer should have aborted its transaction")
		}
		if queued := discoveryJobs(t, s.queue, "acme"); len(queued) != 0 {
			t.Fatalf("a failed write left %d orphan discovery jobs behind", len(queued))
		}
	})
}

// A batch is one job carrying its subjects, not one job per memory: the queue's
// cost is per row and the work is per subject either way. A single create is
// therefore one job with one subject, which is what makes the load test below
// hold as the plan writes it.
func TestABatchIsOneJobCarryingItsSubjects(t *testing.T) {
	s := newDiscoveringService(t)

	reqs := make([]memory.CreateReq, 8)
	for i := range reqs {
		reqs[i] = memory.CreateReq{Content: fmt.Sprintf("batch member %d", i)}
	}
	out, err := s.svc.CreateBatch(acmeCtx(), reqs)
	must(t, err)

	queued := discoveryJobs(t, s.queue, "acme")
	if len(queued) != 1 {
		t.Fatalf("a batch of 8 queued %d discovery jobs, want exactly one", len(queued))
	}
	subjects, err := discovery.DecodePayload(queued[0].Payload)
	must(t, err)
	if len(subjects) != len(out) {
		t.Fatalf("the job names %d subjects for a batch of %d", len(subjects), len(out))
	}
	for i, m := range out {
		if subjects[i] != m.ID {
			t.Fatalf("subject %d is %s, want %s — the order is the write order", i, subjects[i], m.ID)
		}
	}
}

// The Rust channel drops here, silently, and counts the drops. Nothing is
// dropped: the queue's capacity is the disk.
func TestNothingIsDroppedUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("ten thousand creates")
	}
	const n = 10_000
	s := newDiscoveringService(t)
	ctx := acmeCtx()

	want := make(map[id.ID]bool, n)
	for i := 0; i < n; i++ {
		m, err := s.svc.Create(ctx, memory.CreateReq{Content: fmt.Sprintf("memory %d", i)})
		must(t, err)
		want[m.ID] = true
	}

	// Claimed rather than listed, because a listing is capped and the assertion
	// is about every job rather than a page of them. Claiming is also the
	// stronger statement: these are not merely rows, they are work a worker
	// would pick up.
	got := map[id.ID]bool{}
	for {
		batch, err := s.queue.Claim(ctx, "acme", "test", []jobs.Type{discoveryType}, 500)
		must(t, err)
		if len(batch) == 0 {
			break
		}
		for _, j := range batch {
			subjects, err := discovery.DecodePayload(j.Payload)
			must(t, err)
			for _, sub := range subjects {
				if got[sub] {
					t.Fatalf("memory %s was queued for discovery twice", sub)
				}
				got[sub] = true
			}
		}
	}
	if len(got) != n {
		t.Fatalf("%d creates produced discovery jobs for %d of them", n, len(got))
	}
	for rid := range want {
		if !got[rid] {
			t.Fatalf("memory %s was stored and never queued for discovery", rid)
		}
	}
}

// Without the option nothing is enqueued, and nothing fails. A build with
// discovery off is a build whose graph does not fill itself, not a build whose
// writes break.
func TestDiscoveryOffQueuesNothing(t *testing.T) {
	svc := newService(t)
	if _, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "no discovery here"}); err != nil {
		t.Fatalf("a create with no discovery enqueuer failed: %v", err)
	}
}
