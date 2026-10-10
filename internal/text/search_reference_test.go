package text

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// This file holds the oracle for [Index.Search] and the randomised corpus that
// runs against it.
//
// searchReference is the implementation the merge replaced: one full postings
// list per term, three maps keyed by record id, a candidate slice holding every
// match and one sort over all of it. It is kept because "the merge returns the
// same answer" is the only claim that matters about the change, and a hand-built
// expectation table cannot make it — the thing worth checking is the agreement
// of two algorithms over hundreds of corpora and queries, including the cases
// nobody would think to tabulate: a term repeated in the query, a tie broken on
// the record id, a float64 sum that two orders of accumulation would round
// differently, a limit far below the number of candidates.
//
// It is a test fixture and must not be "tidied" into sharing code with the
// implementation. An oracle that shares the code under test agrees with it by
// construction and proves nothing.

const (
	refTenant tenant.ID        = "oracle"
	refNS     tenant.Namespace = tenant.DefaultNamespace
)

// searchReference is [Index.Search] as it stood before the sorted-postings
// merge: three maps and a full sort. See the file comment.
func searchReference(ctx context.Context, r Reader, t tenant.ID, ns tenant.Namespace,
	req SearchReq) ([]Hit, error) {
	const op = "text.Index.Search"

	if t == "" {
		return nil, errs.E(errs.Invalid, op, errNoTenant)
	}
	if len(req.Required) == 0 && len(req.Optional) == 0 {
		return nil, errs.E(errs.Invalid, op, errors.New("this query produced no searchable terms"))
	}
	if req.Limit <= 0 {
		return nil, errs.E(errs.Invalid, op, errors.New("a search limit must be greater than zero"))
	}

	stats, err := ReadStats(ctx, r, t, ns)
	if err != nil {
		return nil, errs.E(errs.KindOf(err), op, err)
	}

	scores := map[id.ID]float64{}
	present := map[id.ID]int{}
	matched := map[id.ID]int{}

	gateOnly := len(req.Optional) > 0

	for _, term := range req.Required {
		if err := scoreTermReference(ctx, r, t, ns, term, stats, func(rid id.ID, score float64) {
			if !gateOnly {
				scores[rid] += score
			}
			present[rid]++
		}); err != nil {
			return nil, errs.E(errs.KindOf(err), op, err)
		}
	}
	if len(req.Required) > 0 && len(present) == 0 {
		return nil, nil
	}

	for _, term := range req.Optional {
		if err := scoreTermReference(ctx, r, t, ns, term, stats, func(rid id.ID, score float64) {
			if len(req.Required) > 0 && present[rid] < len(req.Required) {
				return
			}
			scores[rid] += score
			matched[rid]++
		}); err != nil {
			return nil, errs.E(errs.KindOf(err), op, err)
		}
	}

	hits := make([]Hit, 0, len(scores))
	for rid, score := range scores {
		if len(req.Required) > 0 && present[rid] < len(req.Required) {
			continue
		}
		if gateOnly && matched[rid] == 0 {
			continue
		}
		hits = append(hits, Hit{ID: rid, Score: float32(score)})
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return bytes.Compare(hits[i].ID[:], hits[j].ID[:]) < 0
	})
	if len(hits) > req.Limit {
		hits = hits[:req.Limit]
	}
	return hits, nil
}

// scoreTermReference is the two-pass per-term scan the reference uses: the whole
// postings list into a slice, the document frequency from its length, then a
// second pass over the same rows to score.
func scoreTermReference(ctx context.Context, r Reader, t tenant.ID, ns tenant.Namespace,
	term string, stats Stats, emit func(id.ID, float64)) error {
	const op = "text.scoreTerm"

	type entry struct {
		rid id.ID
		p   posting
	}

	lower, upper := keys.TextTermRange(t, ns, term)
	it := r.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	var entries []entry
	for ok := it.First(); ok; ok = it.Next() {
		_, _, got, rid, err := keys.ParseText(it.Key())
		if err != nil {
			return err
		}
		if got != term {
			return errs.E(errs.Corruption, op, errors.New(
				"a posting inside one term's range belongs to another term"))
		}
		p, err := decodePosting(it.Value())
		if err != nil {
			return err
		}
		entries = append(entries, entry{rid: rid, p: p})
		if err := ctx.Err(); err != nil {
			return errs.E(errs.Unavailable, op, err)
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	idf := inverseDocumentFrequency(stats.Documents, uint64(len(entries)))
	avg := stats.AvgDocLen()
	for _, e := range entries {
		emit(e.rid, idf*termWeight(float64(e.p.TermFreq), float64(e.p.DocLen), avg))
	}
	return nil
}

// --- the randomised corpus ---------------------------------------------------

// The vocabulary is shaped like a real one, which is the only shape worth
// testing a merge on: three words in about half the documents, a middle band in
// about one in ten, and a long tail in one or two. A query over a rare term
// exercises a one-posting merge and tells us nothing about the common-term path,
// which is where the whole cost of a keyword query is.
var (
	refCommon = []string{"alfa", "bravo", "charlie"}
	refMid    = []string{"delta", "echo", "foxtrot", "golf", "hotel", "india", "juliett", "kilo"}
	refRare   = []string{
		"lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra",
		"tango", "uniform", "victor", "whiskey", "xray", "yankee", "zulu",
		"aurora", "basalt", "cinnabar", "dolomite", "erbium", "feldspar",
	}
	refTags = []string{"atlas", "borealis", "cygnus"}
)

// refWord draws one word with the frequency the band implies.
func refWord(rng *rand.Rand) string {
	switch n := rng.IntN(100); {
	case n < 50:
		return refCommon[rng.IntN(len(refCommon))]
	case n < 80:
		return refMid[rng.IntN(len(refMid))]
	default:
		return refRare[rng.IntN(len(refRare))]
	}
}

// buildRefCorpus indexes docs documents of varying length, so that BM25's
// length normalisation is actually exercised and scores are not all equal.
func buildRefCorpus(tb testing.TB, ix *Index, kv storage.KV, docs int, rng *rand.Rand) {
	tb.Helper()

	ctx := context.Background()
	const perBatch = 200
	for start := 0; start < docs; start += perBatch {
		err := txn.Do(ctx, kv, func(tx txn.Tx) error {
			for i := start; i < min(start+perBatch, docs); i++ {
				var sb strings.Builder
				for w := 0; w < 3+rng.IntN(25); w++ {
					if w > 0 {
						sb.WriteByte(' ')
					}
					sb.WriteString(refWord(rng))
				}
				var tags []string
				for _, tag := range refTags {
					if rng.IntN(100) < 30 {
						tags = append(tags, tag)
					}
				}
				if err := ix.Add(ctx, tx, refTenant, refNS, id.New(), sb.String(), tags); err != nil {
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

// refQueries generates queries covering every shape Search distinguishes:
// optional only, required only, both, a required term nothing carries, a term
// repeated in the query, and limits above and below the candidate count.
func refQueries(rng *rand.Rand, n int) []SearchReq {
	out := make([]SearchReq, 0, n+4)
	limits := []int{1, 2, 3, 7, 10, 25, 100, 1000}
	for range n {
		var req SearchReq
		for range 1 + rng.IntN(6) {
			req.Optional = append(req.Optional, refWord(rng))
		}
		switch rng.IntN(4) {
		case 0: // optional only
		case 1:
			req.Required = []string{TagTerm(refTags[rng.IntN(len(refTags))])}
		case 2:
			req.Required = []string{
				TagTerm(refTags[rng.IntN(len(refTags))]),
				TagTerm(refTags[rng.IntN(len(refTags))]),
			}
		case 3: // required only: a tag search still has to rank
			req.Required = []string{TagTerm(refTags[rng.IntN(len(refTags))])}
			req.Optional = nil
		}
		req.Limit = limits[rng.IntN(len(limits))]
		out = append(out, req)
	}
	// The shapes a generator would reach rarely or never.
	out = append(out,
		// A term repeated in the query scores twice, in both implementations.
		SearchReq{Optional: []string{"alfa", "alfa", "bravo"}, Limit: 10},
		// A required term nothing carries: the early return.
		SearchReq{Required: []string{TagTerm("no-such-tag")}, Optional: []string{"alfa"}, Limit: 10},
		// Every common word at once, limit one.
		SearchReq{Optional: refCommon, Limit: 1},
		// A tag that gates and an optional term nothing carries.
		SearchReq{Required: []string{TagTerm("atlas")}, Optional: []string{"nosuchword"}, Limit: 10},
	)
	return out
}

// TestSearchMatchesTheReferenceImplementation is the acceptance test for the
// sorted-postings merge: the same hits, in the same order, with the same scores
// as the three-map-and-full-sort implementation it replaced, over a randomised
// corpus and two hundred randomised queries.
//
// Scores are compared with == rather than a tolerance, deliberately. The merge
// accumulates a candidate's contributions in a different place from the one the
// maps did, and float64 addition is not associative: the only way to know the
// arithmetic was moved and not changed is to require the same bits.
//
// The whole corpus runs twice, once per candidate-finding strategy. Every query
// a generator with a real vocabulary produces is a handful of terms, which is
// the scan's side of heapMergeMinTerms, so without forcing the threshold down
// the heap would never meet the oracle at all — and two strategies that agree
// with each other only on the queries nobody ran is not agreement.
func TestSearchMatchesTheReferenceImplementation(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := New()
	ctx := context.Background()

	rng := rand.New(rand.NewPCG(0x5eed, 0x1234))
	buildRefCorpus(t, ix, kv, 500, rng)

	snap := kv.NewSnapshot()
	t.Cleanup(func() { _ = snap.Close() })

	queries := refQueries(rng, 200)

	chosen := heapMergeMinTerms
	t.Cleanup(func() { heapMergeMinTerms = chosen })
	for _, strategy := range []struct {
		name      string
		threshold int
	}{
		{"the scan, which every one of these queries' term counts chooses", chosen},
		{"the heap, forced onto all of them", 0},
	} {
		t.Run(strategy.name, func(t *testing.T) {
			heapMergeMinTerms = strategy.threshold

			truncated := 0
			for n, req := range queries {
				want, err := searchReference(ctx, snap, refTenant, refNS, req)
				if err != nil {
					t.Fatalf("query %d: the reference failed: %v", n, err)
				}
				got, err := ix.Search(ctx, snap, refTenant, refNS, req)
				if err != nil {
					t.Fatalf("query %d: %v", n, err)
				}
				if len(got) != len(want) {
					t.Fatalf("query %d %+v: returned %d hits, the reference returned %d",
						n, req, len(got), len(want))
				}
				for i := range want {
					if got[i].ID != want[i].ID || got[i].Score != want[i].Score {
						t.Fatalf("query %d %+v: hit %d is %v scoring %v, the reference has %v scoring %v",
							n, req, i, got[i].ID, got[i].Score, want[i].ID, want[i].Score)
					}
				}

				// A limit that is never reached tests nothing about a bounded
				// selection, so count the queries that actually truncate and
				// insist there are enough of them for the comparison to mean
				// something.
				wide := req
				wide.Limit = 1 << 30
				all, err := searchReference(ctx, snap, refTenant, refNS, wide)
				if err != nil {
					t.Fatalf("query %d: the reference failed on the wide limit: %v", n, err)
				}
				if len(all) > req.Limit {
					truncated++
				}
			}
			if truncated < 50 {
				t.Fatalf("only %d of %d queries had more candidates than their limit, so the "+
					"comparison barely exercised the bounded selection", truncated, len(queries))
			}
			t.Logf("%d queries agreed with the reference, %d of them truncated by their limit",
				len(queries), truncated)
		})
	}
}
