package vector_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
	"github.com/remem-org/remem-go/internal/vector/hnsw"
)

// Recall is measured against `flat`, which is the definition of correct: it
// scans every vector the tenant owns and returns the true nearest neighbours.
// That is the whole reason it stays in the tree permanently rather than being
// replaced by the faster index.
//
// This file lives in internal/vector rather than internal/vector/hnsw because
// it compares two implementations and belongs to neither. The import of hnsw is
// what internal/arch's rule already permits: only internal/vector may name it.

// The real embedding width. Measuring recall at a convenient 64 dimensions
// would measure a different problem: how hard a nearest-neighbour search is
// depends on the dimensionality of the space it runs in, and 384 is the one the
// product runs in.
const recallDim = 384

// The benchmark corpus: 20 topics, each a cloud of points around a centre, with
// queries drawn from the same topics.
//
// This is a choice and it has to be justified, because recall depends on the
// corpus far more than on the parameters — see docs/architecture/vector.md for
// the measurements. Uniformly distributed points on a 384-dimensional sphere are
// the worst case any proximity graph can be given: every pair is
// near-orthogonal, the true nearest neighbour is barely nearer than the
// hundredth, and there is no structure to exploit. Text embeddings are nothing
// like that; they cluster by topic. Gating the product's recall on the
// adversarial corpus would report a number no deployment will ever see.
//
// The uniform case is not dropped. TestTheGraphIsConnected below measures it,
// and gates on the property that actually distinguishes a sound graph from a
// broken one: given enough search effort, it finds everything.
const (
	recallTopics = 20
	recallSpread = 0.6
)

// Recall against the exact index, at the size the everyday suite can afford.
//
// The size is the whole design of this test. The plan's numbers are measured
// over 10,000 and 250,000 vectors; those live in recall_scale_test.go behind the
// `recall` build tag and run as their own CI job, because at 384 dimensions
// under -race and coverage instrumentation they take longer than the package
// timeout and turn `make cover` into a mystery failure — which is exactly what
// they did before this was split. What stays here is small enough to run on
// every commit and large enough that a change halving recall cannot reach the
// nightly job unnoticed.
func TestRecallAgainstFlat(t *testing.T) {
	if testing.Short() {
		t.Skip("800 vectors at the real embedding width")
	}
	// Four topics rather than twenty, so 800 memories are two hundred per topic
	// and an effort of sixteen has real work to do.
	r := measureRecall(t, topicCorpusOf(800, 4), []int{16, 64}, 10, 60)
	t.Logf("recall@10 over 800 vectors in 4 topics: %.4f at ef_search=16, %.4f at 64", r[16], r[64])

	if r[64] < 0.95 {
		t.Fatalf("recall@10 at ef_search=64 is %.4f, want >= 0.95", r[64])
	}
	if r[16] < 0.80 {
		t.Fatalf("recall@10 at ef_search=16 is %.4f, want above 0.80", r[16])
	}
	// The plan's "recall at ef_search=16 is *lower*" is asserted at 10,000
	// vectors and not here. At this size the two efforts measure 0.982 and
	// 1.000, and a gap of under two points is one an honest improvement could
	// close — an assertion that a *better* index fails is worse than none. At
	// 10,000 they measure 0.887 and 0.997, which is a gap worth pinning.
}

// A rebuilt index and an incrementally built one must find the same memories.
//
// They are not byte-identical and are not meant to be: levels come from the
// record id, so those match, but neighbour lists depend on insertion order and
// a rebuild inserts in id order. What has to survive that is the answer.
func TestRebuildMatchesIncrementalConstruction(t *testing.T) {
	ctx := context.Background()
	const n, queries, k = 800, 60, 10

	c := topicCorpus(n)
	ids := make([]id.ID, n)
	vecs := make([][]float32, n)
	for i := range n {
		ids[i] = id.New()
		vecs[i] = c.point()
	}

	incremental, incKV := newHNSW(t, hnsw.Options{Params: hnsw.Params{EfSearch: 64}})
	for i := range n {
		if err := incremental.Insert(ctx, "acme", ids[i], vecs[i]); err != nil {
			t.Fatal(err)
		}
	}

	// The rebuilt index is built from the same canonical vectors, through the
	// rebuild path, into its own store.
	rebuilt, _ := newHNSW(t, hnsw.Options{Params: hnsw.Params{EfSearch: 64}})
	if err := rebuilt.Rebuild(ctx, "acme", vector.NewStore(incKV)); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	agree, total := 0, 0
	firstDiffers := 0
	for q := range queries {
		query := c.queryAt()
		a, err := incremental.Search(ctx, "acme", query, k, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := rebuilt.Search(ctx, "acme", query, k, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(a) != k || len(b) != k {
			t.Fatalf("query %d returned %d and %d hits, want %d each", q, len(a), len(b), k)
		}
		if a[0].ID != b[0].ID {
			firstDiffers++
		}
		in := map[id.ID]bool{}
		for _, h := range a {
			in[h.ID] = true
		}
		for _, h := range b {
			total++
			if in[h.ID] {
				agree++
			}
		}
	}
	overlap := float64(agree) / float64(total)
	t.Logf("top-%d overlap between rebuilt and incremental: %.4f, top-1 disagreements: %d of %d",
		k, overlap, firstDiffers, queries)
	if firstDiffers > 0 {
		t.Fatalf("%d of %d queries disagree on the nearest memory", firstDiffers, queries)
	}
	if overlap < 0.99 {
		t.Fatalf("top-%d overlap is %.4f, want >= 0.99", k, overlap)
	}
}

// --- helpers ---------------------------------------------------------------

// corpus is a generator of points and of queries drawn from the same structure.
// A query unrelated to the corpus measures a regime search is never in.
type corpus struct {
	name    string
	n       int
	point   func() []float32
	queryAt func() []float32
}

// topicCorpus is the benchmark corpus described at the top of this file.
func topicCorpus(n int) corpus { return topicCorpusOf(n, recallTopics) }

// topicCorpusOf is topicCorpus with the topic count stated.
//
// It exists because what makes a nearest-neighbour search hard is how many
// points share a topic, not how many points there are: a search only has to
// tell apart the memories it has already narrowed down to. A small corpus
// spread over twenty topics is forty memories per topic, which an effort of
// sixteen exhausts, and both effort settings then measure 1.0000 — a test that
// cannot see a regression until it is total.
func topicCorpusOf(n, topics int) corpus {
	r := newRecallRNG(3)
	centres := make([][]float32, topics)
	for i := range centres {
		centres[i] = r.unit(recallDim)
	}
	sigma := float32(recallSpread / math.Sqrt(float64(recallDim)))
	near := func(src *recallRNG, c []float32) []float32 {
		v := make([]float32, recallDim)
		var sum float64
		for j := range v {
			v[j] = c[j] + sigma*src.next()
			sum += float64(v[j]) * float64(v[j])
		}
		inv := float32(1 / math.Sqrt(sum))
		for j := range v {
			v[j] *= inv
		}
		return v
	}
	q := newRecallRNG(4)
	i, j := 0, 0
	return corpus{fmt.Sprintf("topics=%d spread=%.1f", topics, recallSpread), n,
		func() []float32 { c := centres[i%topics]; i++; return near(r, c) },
		// A stride of seven so consecutive queries do not walk the topics in
		// the order the corpus was written in.
		func() []float32 { c := centres[(j*7)%topics]; j++; return near(q, c) }}
}

// measureRecall builds one corpus and reports, for each effort level, the
// fraction of the exact top-k that the approximate index found.
//
// It takes every effort level at once rather than being called once per level,
// because the corpus and the graph are what cost minutes and the search that
// walks them costs milliseconds. The graph is built exactly once here: the
// second index over the same store materialises the node records the first one
// wrote, which is the ordinary restart path and is fast.
func measureRecall(t *testing.T, c corpus, efs []int, k, queries int) map[int]float64 {
	t.Helper()
	ctx := context.Background()

	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	store := vector.NewStore(kv)

	for range c.n {
		if err := store.Put(ctx, "acme", tenant.DefaultNamespace, id.New(), &vector.Vector{
			ModelID: "recall-test", Dim: recallDim, Values: c.point(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// The queries and their exact answers, computed once against the oracle.
	exact := flat.New(store, distance.L2)
	qs := make([][]float32, queries)
	want := make([]map[id.ID]bool, queries)
	for i := range qs {
		qs[i] = c.queryAt()
		hits, err := exact.Search(ctx, "acme", qs[i], k, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) != k {
			t.Fatalf("the exact index returned %d of %d for query %d", len(hits), k, i)
		}
		want[i] = make(map[id.ID]bool, k)
		for _, h := range hits {
			want[i][h.ID] = true
		}
	}

	out := make(map[int]float64, len(efs))
	for _, ef := range efs {
		approx, err := hnsw.New(kv, hnsw.Options{
			Metric: distance.L2,
			Params: hnsw.Params{EfSearch: ef},
			// Large enough that the corpus stays resident: measuring recall
			// while the graph is being evicted and re-read would measure the
			// cache instead.
			ResidentBudgetBytes: 4 << 30,
		})
		if err != nil {
			t.Fatal(err)
		}
		found := 0
		for i := range qs {
			got, err := approx.Search(ctx, "acme", qs[i], k, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != k {
				t.Fatalf("ef_search=%d returned %d of %d for query %d", ef, len(got), k, i)
			}
			for _, h := range got {
				if want[i][h.ID] {
					found++
				}
			}
		}
		out[ef] = float64(found) / float64(k*queries)
	}
	return out
}

func newHNSW(t *testing.T, opts hnsw.Options) (*hnsw.Index, storage.KV) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	if opts.Metric == 0 {
		opts.Metric = distance.L2
	}
	if opts.ResidentBudgetBytes == 0 {
		opts.ResidentBudgetBytes = 4 << 30
	}
	idx, err := hnsw.New(kv, opts)
	if err != nil {
		t.Fatalf("hnsw.New: %v", err)
	}
	return idx, kv
}

type recallRNG struct{ s uint64 }

func newRecallRNG(seed uint64) *recallRNG {
	return &recallRNG{s: seed*6364136223846793005 + 1442695040888963407}
}

func (r *recallRNG) next() float32 {
	r.s = r.s*6364136223846793005 + 1442695040888963407
	return float32(int64(r.s>>33))/float32(1<<30) - 1
}

func (r *recallRNG) unit(dim int) []float32 {
	v := make([]float32, dim)
	var sum float64
	for i := range v {
		v[i] = r.next()
		sum += float64(v[i]) * float64(v[i])
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
	return v
}
