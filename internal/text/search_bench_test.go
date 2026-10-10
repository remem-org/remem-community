package text

import (
	"context"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/txn"
)

// benchCorpusDocs is the corpus the keyword benchmarks measure against, and
// benchTermShare is how much of it each of the query's two terms covers.
//
// A quarter each is the case the index is worst at and the one the plan names:
// a term in a quarter of the corpus has a quarter of the corpus as its postings
// and every one of them is read, so the query costs five thousand postings to
// answer with ten hits. A benchmark over a rare term measures the key lookup
// and nothing else.
const (
	benchCorpusDocs = 20_000
	benchTermShare  = 25 // per cent of documents carrying each query term
)

// buildBenchCorpus indexes benchCorpusDocs documents in which "alfa" and
// "bravo" each appear in about a quarter, padded with a vocabulary of rarer
// words so that document lengths — and so BM25's length normalisation — vary.
func buildBenchCorpus(tb testing.TB, ix *Index, kv storage.KV, rng *rand.Rand) {
	tb.Helper()

	ctx := context.Background()
	const perBatch = 500
	for start := 0; start < benchCorpusDocs; start += perBatch {
		err := txn.Do(ctx, kv, func(tx txn.Tx) error {
			for i := start; i < min(start+perBatch, benchCorpusDocs); i++ {
				var sb strings.Builder
				for w := range 4 + rng.IntN(16) {
					if w > 0 {
						sb.WriteByte(' ')
					}
					sb.WriteString(refRare[rng.IntN(len(refRare))])
				}
				if rng.IntN(100) < benchTermShare {
					for range 1 + rng.IntN(3) {
						sb.WriteString(" alfa")
					}
				}
				if rng.IntN(100) < benchTermShare {
					for range 1 + rng.IntN(3) {
						sb.WriteString(" bravo")
					}
				}
				if err := ix.Add(ctx, tx, refTenant, refNS, id.New(), sb.String(), nil); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			tb.Fatalf("building the corpus: %v", err)
		}
	}
}

// documentFrequency counts a term's postings, so a benchmark can state the
// corpus it actually built rather than the one it intended to.
func documentFrequency(tb testing.TB, r Reader, term string) int {
	tb.Helper()

	lower, upper := keys.TextTermRange(refTenant, refNS, term)
	it := r.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	if err := it.Error(); err != nil {
		tb.Fatalf("counting %q: %v", term, err)
	}
	return n
}

// BenchmarkSearchCommonTerms is the measurement the sorted-postings merge was
// made for: twenty thousand documents, a two-term query whose terms are each in
// about five thousand of them, and a page of ten.
//
// It reads through a snapshot, as [query.Executor] does, because memkv's store
// copies its whole index on every iterator it hands out and the benchmark would
// otherwise measure that copy. The snapshot is taken once and shared, which is
// exactly the pinned view a hybrid query reads every step through.
func BenchmarkSearchCommonTerms(b *testing.B) {
	kv := memkv.New()
	b.Cleanup(func() { _ = kv.Close() })
	ix := New()
	ctx := context.Background()

	buildBenchCorpus(b, ix, kv, rand.New(rand.NewPCG(7, 11)))
	snap := kv.NewSnapshot()
	b.Cleanup(func() { _ = snap.Close() })

	req := SearchReq{Optional: []string{"alfa", "bravo"}, Limit: 10}
	hits, err := ix.Search(ctx, snap, refTenant, refNS, req)
	if err != nil || len(hits) != req.Limit {
		b.Fatalf("warm-up search: %d hits, %v", len(hits), err)
	}
	b.Logf("%d documents, %d postings for %q and %d for %q, limit %d",
		benchCorpusDocs,
		documentFrequency(b, snap, "alfa"), "alfa",
		documentFrequency(b, snap, "bravo"), "bravo", req.Limit)

	b.ReportAllocs()
	for b.Loop() {
		if _, err := ix.Search(ctx, snap, refTenant, refNS, req); err != nil {
			b.Fatal(err)
		}
	}
}
