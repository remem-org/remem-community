package memory_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/indexes"
	"github.com/remem-org/remem-go/internal/vector/indexqueue"
)

// BenchmarkCreateBatch is what plan item 14 is measured by: what one durable
// create costs the caller, with the HNSW insert on the request path and off it.
//
// The composition is the server's, minus the server: an in-memory store, both
// derived indexes, the deterministic embedder at the real 384-dimensional
// width, and the approximate index with the shipped parameters. The embedder is
// the fake rather than the model because this benchmark is about where the
// index insert happens, and 20 ms of ONNX inference per create would bury it.
//
// Read it with:
//
//	go test -run '^$' -bench BenchmarkCreateBatch -benchtime=2s -count=6 ./internal/memory/ > out.txt
//	benchstat -col /indexing out.txt
//
// A fixed iteration count — `-benchtime=4000x` — is the apples-to-apples form,
// and worth running beside it: an HNSW insert costs more into a larger graph,
// and at a wall-clock benchtime the faster arm runs more iterations and is
// therefore measured against a bigger corpus than the slower one. Either way
// the asynchronous arm skips above [benchQueueSize] iterations, for the reason
// the next paragraph gives.
//
// # What the two reported numbers mean, and why there are two
//
// `ns/op` is the request path: the time CreateBatch took to return, which is
// what an HTTP caller waits for. `drain-ns/op` is the queued index work the
// asynchronous arm still owed when the loop ended, measured by a Flush outside
// the timer and divided by the same number of operations. It is zero in the
// synchronous arm, which has nothing to owe. Together they account for the same
// work, and the split between them *is* the change: the insert did not get
// cheaper, it stopped being something a response waits for.
//
// The queue is sized past the whole run on purpose, so the loop never meets the
// backpressure. A closed loop with no think time offers creates faster than the
// index applies them, and one tenant's inserts are applied by one worker under
// one graph write lock — deliberately, since that is what keeps a record's
// insert and its remove in order — so a saturated single-tenant loop measures
// the insert rate and not the request path. Sized this way the benchmark
// answers the question the item asks.
//
// The size is a constant and the benchmark skips above it, rather than being
// b.N slots deep: a benchmark whose memory grows with the iteration count the
// framework happens to choose is one that can be made to allocate anything at
// all from the command line, and a run that quietly became a measurement of the
// insert rate would be worse than a run that said it was skipped.
//
// Measured on an Apple M1 Pro at 10,000 iterations an arm, before the cap made
// that a skip: 1.45 ms a create on
// the request path synchronously, 338 µs asynchronously (-77%), with 951 µs an
// operation of index work moved behind the response. Bounded to the shipped
// 1024 instead, the same loop saturates and both arms converge on the insert
// rate, which is what one tenant's single worker under one graph lock can do.
func BenchmarkCreateBatch(b *testing.B) {
	for _, tc := range []struct {
		name  string
		async bool
	}{
		{"indexing=sync", false},
		{"indexing=async", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			if tc.async && b.N > benchQueueSize {
				b.Skipf("this benchmark queues every operation it creates and the queue is bounded at %d; "+
					"run it with -benchtime=%dx or less", benchQueueSize, benchQueueSize)
			}
			svc, h := benchService(b, tc.async)
			defer h.close()
			ctx := acmeCtx()
			b.ReportAllocs()
			b.ResetTimer()
			for n := range b.N {
				if _, err := svc.CreateBatch(ctx, []memory.CreateReq{{
					Content: fmt.Sprintf("the deployment pipeline note number %d, about runners and caches", n),
				}}); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			drain := h.drain(b)
			b.ReportMetric(float64(drain.Nanoseconds())/float64(b.N), "drain-ns/op")
		})
	}
}

// benchQueueSize is how deep the asynchronous arm's queue is, and therefore the
// most iterations it will run. Above it the arm skips: see the benchmark's
// comment.
const benchQueueSize = 8192

// harness is the queue-dependent half of the benchmark's fixture: how to wait
// for the index work and how to release everything afterwards.
//
// It releases explicitly rather than through b.Cleanup because a benchmark
// function is called several times on one *testing.B as the framework searches
// for an iteration count, and a background worker left running from an earlier
// attempt would be competing with the attempt being measured.
type harness struct {
	drain func(*testing.B) time.Duration
	close func()
}

func benchService(b *testing.B, async bool) (*memory.Service, harness) {
	b.Helper()
	kv := memkv.New()

	texts := text.New()
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(texts))

	var index vector.Index
	index, err := indexes.Open(kv, indexes.Config{
		Kind: "hnsw", Metric: "cosine", Neighbours: 16,
		BuildEffort: 200, SearchEffort: 64, ResidentBudgetBytes: 512 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}

	h := harness{drain: func(*testing.B) time.Duration { return 0 }}
	release := []func(){func() { _ = kv.Close() }}
	if async {
		// Deep enough that the loop never meets the backpressure, and a fixed
		// number rather than b.N: sizing a channel to the iteration count
		// hands the framework's own ramp-up the power to allocate whatever it
		// likes, and a -benchtime that asked for a million iterations would
		// have allocated a million slots. The benchmark skips above this
		// instead. See the benchmark's comment.
		q := indexqueue.New(index.(indexqueue.Maintained),
			indexqueue.WithSize(benchQueueSize+indexqueue.DefaultWorkers))
		index = q
		h.drain = func(b *testing.B) time.Duration {
			started := time.Now()
			if err := q.Flush(context.Background()); err != nil {
				b.Fatal(err)
			}
			return time.Since(started)
		}
		release = append(release, func() { _ = q.Close(context.Background()) })
	}

	svc := memory.New(kv, repo, index, embeddingtest.New(), clock.NewFake(clock.FakeStart),
		attr.MustTable(), graph.NewService(kv, clock.NewFake(clock.FakeStart)),
		memory.Config{Metric: distance.Cosine}, memory.WithTextIndex(texts))
	release = append(release, func() { _ = svc.Close() })
	h.close = func() {
		for i := len(release) - 1; i >= 0; i-- {
			release[i]()
		}
	}
	return svc, h
}
