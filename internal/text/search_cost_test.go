package text

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/txn"
)

// This file holds what the merge costs, and it is in package text rather than
// beside the other cost test in package text_test for two reasons: the only
// honest yardstick for a long query is searchReference, the implementation the
// merge replaced, and the merge's own cancellation check cannot be reached
// through [Index.Search] at all. See the two tests.

// buildOneWordCorpus indexes one document per term, each document being that
// term and nothing else.
//
// It is the shape a long query is worst at: every term in the query carries
// exactly one candidate, so all but one of the cursors is exhausted by the time
// the merge is a few candidates in, and an implementation that rescans every
// cursor per candidate pays for all of them anyway.
func buildOneWordCorpus(tb testing.TB, ix *Index, kv storage.KV, terms []string) {
	tb.Helper()

	ctx := context.Background()
	const perBatch = 500
	for start := 0; start < len(terms); start += perBatch {
		err := txn.Do(ctx, kv, func(tx txn.Tx) error {
			for _, term := range terms[start:min(start+perBatch, len(terms))] {
				if err := ix.Add(ctx, tx, refTenant, refNS, id.New(), term, nil); err != nil {
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

// TestKeywordSearchCostDoesNotGrowWithExhaustedTerms is the other half of
// TestKeywordSearchCostIsProportionalToMatches, and it is the test the first
// merge did not have.
//
// A query is bounded at memory.MaxQueryBytes, which is 64 KiB, so "the query's
// terms are a handful" was an assumption about the caller rather than a property
// of the index: a valid 59,999-byte query holds six thousand distinct terms. The
// merge that found its next candidate by scanning every cursor, and then scanned
// every cursor again to score it, answered that query in 134 ms against 6 ms for
// the three-map implementation it had replaced — because the cost was candidates
// times terms and an exhausted term cost the same as a live one.
//
// The bound is relative rather than absolute. An absolute millisecond figure is
// a number that flakes the first time the host is busy; the reference does the
// same reads over the same snapshot, so the ratio holds whatever else the
// machine is doing. Three times is generous — the merge should be at or under
// the reference here — and still an order of magnitude below what the scan cost.
func TestKeywordSearchCostDoesNotGrowWithExhaustedTerms(t *testing.T) {
	const terms = 6_000

	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := New()
	ctx := context.Background()

	query := make([]string, terms)
	for i := range query {
		query[i] = fmt.Sprintf("word%05d", i)
	}
	buildOneWordCorpus(t, ix, kv, query)

	snap := kv.NewSnapshot()
	t.Cleanup(func() { _ = snap.Close() })

	req := SearchReq{Optional: query, Limit: 10}

	// Best of three on each side. One sample measures whatever else the host
	// was doing, and a mean is dragged by the same thing; the fastest run of
	// each is the closest either implementation got to its own cost.
	var best, bestRef time.Duration
	for run := range 3 {
		start := time.Now()
		got, err := ix.Search(ctx, snap, refTenant, refNS, req)
		if err != nil {
			t.Fatalf("searching: %v", err)
		}
		elapsed := time.Since(start)

		start = time.Now()
		want, err := searchReference(ctx, snap, refTenant, refNS, req)
		if err != nil {
			t.Fatalf("the reference failed: %v", err)
		}
		refElapsed := time.Since(start)

		if len(got) != req.Limit {
			t.Fatalf("a %d-term query over %d matching documents returned %d hits, want a full page "+
				"of %d: the measurement below would be of a query that matched nothing",
				terms, terms, len(got), req.Limit)
		}
		if len(got) != len(want) {
			t.Fatalf("returned %d hits, the reference returned %d", len(got), len(want))
		}
		for i := range want {
			if got[i].ID != want[i].ID || got[i].Score != want[i].Score {
				t.Fatalf("hit %d is %v scoring %v, the reference has %v scoring %v",
					i, got[i].ID, got[i].Score, want[i].ID, want[i].Score)
			}
		}

		if run == 0 || elapsed < best {
			best = elapsed
		}
		if run == 0 || refElapsed < bestRef {
			bestRef = refElapsed
		}
		t.Logf("run %d: %d terms, %d hits, merge %v, reference %v", run, terms, len(got), elapsed, refElapsed)
	}

	if best > 3*bestRef {
		t.Fatalf("a %d-term query took %v against the reference's %v: the merge is paying for terms "+
			"it has already exhausted", terms, best, bestRef)
	}
	t.Logf("a %d-term query took %v, the reference %v", terms, best, bestRef)

	// Which strategy each query actually got. The timing above would still pass
	// if the threshold were dropped until every query took the heap, and the
	// +10% that would cost the ordinary query is invisible from here; these two
	// subtests say what the timing cannot.
	t.Run("a six-thousand-term query is found through the heap", func(t *testing.T) {
		hits, heap := mergeOnce(t, snap, req)
		if !heap {
			t.Fatalf("a %d-term query was merged by scanning its cursors", terms)
		}
		assertAgreesWithReference(t, snap, req, hits)
	})

	t.Run("a three-term query is found by scanning", func(t *testing.T) {
		short := SearchReq{Optional: query[:3], Limit: 10}
		hits, heap := mergeOnce(t, snap, short)
		if heap {
			t.Fatalf("a 3-term query was merged through the heap, which measured +10%% on "+
				"BenchmarkSearchCommonTerms; heapMergeMinTerms is %d", heapMergeMinTerms)
		}
		if len(hits) != 3 {
			t.Fatalf("a 3-term query over one matching document each returned %d hits", len(hits))
		}
		assertAgreesWithReference(t, snap, short, hits)
	})
}

// mergeOnce answers one query through the merge directly and reports which
// strategy found its candidates.
//
// Directly rather than through [Index.Search] because the choice is not in the
// result: Search builds the merge, uses it and drops it, and a query that took
// the wrong path returns the right hits anyway, only more slowly. This is the
// observable.
func mergeOnce(tb testing.TB, r Reader, req SearchReq) (hits []Hit, heap bool) {
	tb.Helper()

	ctx := context.Background()
	stats, err := ReadStats(ctx, r, refTenant, refNS)
	if err != nil {
		tb.Fatalf("reading the corpus statistics: %v", err)
	}

	m := newTermMerge(len(req.Required) + len(req.Optional))
	for _, term := range req.Required {
		if err := m.read(ctx, r, refTenant, refNS, term, stats); err != nil {
			tb.Fatalf("reading %q: %v", term, err)
		}
	}
	m.required = len(m.terms)
	for _, term := range req.Optional {
		if err := m.read(ctx, r, refTenant, refNS, term, stats); err != nil {
			tb.Fatalf("reading %q: %v", term, err)
		}
	}

	top := newTopHits(req.Limit)
	if err := m.rank(ctx, len(req.Optional) > 0, stats.AvgDocLen(), &top); err != nil {
		tb.Fatalf("ranking: %v", err)
	}
	return top.ranked(), m.heap
}

// assertAgreesWithReference is the oracle comparison, so that a subtest about
// which strategy ran also says that strategy answered correctly.
func assertAgreesWithReference(tb testing.TB, r Reader, req SearchReq, got []Hit) {
	tb.Helper()

	want, err := searchReference(context.Background(), r, refTenant, refNS, req)
	if err != nil {
		tb.Fatalf("the reference failed: %v", err)
	}
	if len(got) != len(want) {
		tb.Fatalf("returned %d hits, the reference returned %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].Score != want[i].Score {
			tb.Fatalf("hit %d is %v scoring %v, the reference has %v scoring %v",
				i, got[i].ID, got[i].Score, want[i].ID, want[i].Score)
		}
	}
}

// TestKeywordSearchStopsWhenTheContextIsCancelled is the other thing the first
// merge did not do. A query whose caller has gone away, or whose server is
// shutting down, must stop rather than run its candidates out — and a merge over
// a long query's postings is long enough to be worth interrupting.
func TestKeywordSearchStopsWhenTheContextIsCancelled(t *testing.T) {
	const docs = 3_000

	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := New()
	ctx := context.Background()

	err := txn.Do(ctx, kv, func(tx txn.Tx) error {
		for range docs {
			if err := ix.Add(ctx, tx, refTenant, refNS, id.New(), "alfa bravo", nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("building the corpus: %v", err)
	}

	snap := kv.NewSnapshot()
	t.Cleanup(func() { _ = snap.Close() })

	req := SearchReq{Optional: []string{"alfa", "bravo"}, Limit: 10}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	t.Run("a cancelled search is Unavailable and returns nothing", func(t *testing.T) {
		hits, err := ix.Search(cancelled, snap, refTenant, refNS, req)
		if err == nil {
			t.Fatalf("a cancelled search returned %d hits and no error", len(hits))
		}
		if got := errs.KindOf(err); got != errs.Unavailable {
			t.Fatalf("a cancelled search failed with kind %v, want Unavailable: %v", got, err)
		}
		if len(hits) != 0 {
			t.Fatalf("a cancelled search returned %d hits alongside its error", len(hits))
		}
	})

	t.Run("the merge checks, and not only the postings scan", func(t *testing.T) {
		// Reading the postings checks the context too, so a cancelled Search
		// stops before the merge begins and the subtest above cannot tell the
		// two checks apart. This one gives the merge a live context to read
		// with and a cancelled one to rank with, which is the only way to see
		// the merge's own check.
		stats, err := ReadStats(ctx, snap, refTenant, refNS)
		if err != nil {
			t.Fatalf("reading the corpus statistics: %v", err)
		}
		m := newTermMerge(len(req.Optional))
		for _, term := range req.Optional {
			if err := m.read(ctx, snap, refTenant, refNS, term, stats); err != nil {
				t.Fatalf("reading %q: %v", term, err)
			}
		}

		top := newTopHits(req.Limit)
		err = m.rank(cancelled, true, stats.AvgDocLen(), &top)
		if err == nil {
			t.Fatalf("the merge ranked a cancelled query to completion, offering %d hits",
				len(top.ranked()))
		}
		if got := errs.KindOf(err); got != errs.Unavailable {
			t.Fatalf("the merge failed with kind %v, want Unavailable: %v", got, err)
		}
		if hits := top.ranked(); len(hits) != 0 {
			t.Fatalf("the merge offered %d hits before it stopped", len(hits))
		}
	})
}
