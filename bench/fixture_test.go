package bench

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	remtext "github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/indexes"
)

// corpusSize is the fixture size of Rust's vector-retrieval benchmark (RECORDS).
const corpusSize = 200

// recordPrefix is the content of fixture record i, less its number. The fixed
// embedder reads the number back to place the vector.
const recordPrefix = "Remem vector retrieval benchmark record "

// fixedEmbedder reproduces the vectors of Rust's vector-retrieval benchmark:
// record i sits at [1, i·0.001, 0, 0] and every other text at [1, 0, 0, 0]
// (benches/vector_retrieval.rs at pre-go-freeze). It keeps the model out of the
// measurement, as Rust's query-engine benchmark does.
type fixedEmbedder struct{}

func (fixedEmbedder) Dim() int        { return 4 }
func (fixedEmbedder) ModelID() string { return "benchmark-fixed-4d" }

func (fixedEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		v := []float32{1, 0, 0, 0}
		if n, err := strconv.Atoi(strings.TrimPrefix(t, recordPrefix)); err == nil && strings.HasPrefix(t, recordPrefix) {
			v[1] = float32(n) * 0.001
		}
		out[i] = v
	}
	return out, nil
}

func benchCtx() context.Context { return tenant.NewContext(context.Background(), "bench") }

// fixtures holds each benchmark's service for the life of the process. Go calls
// a benchmark function several times while it calibrates b.N, and a service is
// a Pebble directory with a lock on it; building one per call would measure the
// build and leave the rest open. Rust's Criterion builds its fixture once and
// times repeated queries against it, which is the same shape.
var fixtures sync.Map // name → *memory.Service

type serviceOpts struct {
	text bool
}

// service is the named fixture's service, built on first use: real Pebble,
// HNSW at Rust's vector-benchmark parameters (M 8, ef_construction 64,
// ef_search 64, L2), unsynced commits as Rust's fixture writes them, and a
// materialised ranking of one page, since Rust's query engine returns one.
func service(b *testing.B, name string, o serviceOpts, seed func(*memory.Service) error) *memory.Service {
	b.Helper()
	if v, ok := fixtures.Load(name); ok {
		return v.(*memory.Service)
	}

	kv, err := pebble.Open(benchDataDir(b, name), pebble.Options{})
	if err != nil {
		b.Fatalf("opening pebble: %v", err)
	}
	slots, err := attr.Table()
	if err != nil {
		b.Fatal(err)
	}
	clk := clock.System()

	repoOpts := []record.RepoOption{record.WithIndexer(attr.NewIndexer(slots))}
	var memOpts []memory.Option
	if o.text {
		ix := remtext.New()
		repoOpts = append(repoOpts, record.WithIndexer(ix))
		memOpts = append(memOpts, memory.WithTextIndex(ix))
	}
	index, err := indexes.Open(kv, indexes.Config{
		Kind: "hnsw", Metric: "l2",
		Neighbours: 8, BuildEffort: 64, SearchEffort: 64,
		ModelID: fixedEmbedder{}.ModelID(),
	})
	if err != nil {
		b.Fatalf("opening the vector index: %v", err)
	}
	svc := memory.New(kv, record.NewRepo(kv, repoOpts...), index, fixedEmbedder{}, clk, slots,
		graph.NewService(kv, clk), memory.Config{MaxPageDepth: 10, Metric: distance.L2}, memOpts...)

	if err := seed(svc); err != nil {
		b.Fatalf("seeding %s: %v", name, err)
	}
	fixtures.Store(name, svc)
	return svc
}

// seedCorpus stores the 200 fixture memories. longTermEvery > 0 stores every
// nth as long_term (Rust's selective_filter); deleteOthers hard-deletes every
// memory not on that stride (Rust's phantom_heavy leaves one in five).
func seedCorpus(longTermEvery int, deleteOthers bool) func(*memory.Service) error {
	return func(svc *memory.Service) error {
		ctx := benchCtx()
		for i := 0; i < corpusSize; i++ {
			policy := "short_term"
			if longTermEvery > 0 && i%longTermEvery == 0 {
				policy = "long_term"
			}
			m, err := svc.Create(ctx, memory.CreateReq{
				Content: fmt.Sprintf("%s%d", recordPrefix, i),
				Policy:  policy,
				Tags:    []string{"benchmark"},
			})
			if err != nil {
				return err
			}
			if deleteOthers && i%5 != 0 {
				if err := svc.Delete(ctx, m.ID, true); err != nil {
					return err
				}
			}
		}
		return nil
	}
}
