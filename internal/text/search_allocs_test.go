package text

import (
	"context"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/txn"
)

// TestKeywordSearchAllocationsDoNotFollowTheCandidateCount is the allocation
// half of TestKeywordSearchCostIsProportionalToMatches, and it is what keeps the
// sorted-postings merge from quietly growing a map again.
//
// Reading the postings is proportional to the matches and always will be. What
// must not be proportional to them is the bookkeeping on top: a map keyed by
// record id, or a candidate slice sorted to return ten hits, allocates with the
// corpus, and the only thing that grows here is the postings buffer, which
// doubles — so eight times the candidates may cost a handful more allocations
// and nothing like eight times as many.
//
// The claim is about allocation counts rather than bytes because bytes are
// genuinely proportional: the postings themselves are held, and holding them is
// the trade [Index.Search] documents.
//
// Both candidate-finding strategies are measured. The two-term query here is on
// the scan's side of heapMergeMinTerms, and the heap is the one of the two that
// would allocate per candidate if its backing array were ever allowed to grow,
// so leaving it unmeasured would leave the claim untested where it matters most.
func TestKeywordSearchAllocationsDoNotFollowTheCandidateCount(t *testing.T) {
	chosen := heapMergeMinTerms
	t.Cleanup(func() { heapMergeMinTerms = chosen })
	for _, strategy := range []struct {
		name      string
		threshold int
	}{
		{"the scan, which a two-term query chooses", chosen},
		{"the heap, forced onto it", 0},
	} {
		t.Run(strategy.name, func(t *testing.T) {
			heapMergeMinTerms = strategy.threshold
			assertAllocationsDoNotFollowCandidates(t)
		})
	}
}

func assertAllocationsDoNotFollowCandidates(t *testing.T) {
	t.Helper()

	measure := func(docs int) float64 {
		t.Helper()

		kv := memkv.New()
		t.Cleanup(func() { _ = kv.Close() })
		ix := New()
		ctx := context.Background()

		err := txn.Do(ctx, kv, func(tx txn.Tx) error {
			for i := range docs {
				content := "alfa bravo " + refRare[i%len(refRare)]
				if err := ix.Add(ctx, tx, refTenant, refNS, id.New(), content, nil); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("building a corpus of %d: %v", docs, err)
		}

		snap := kv.NewSnapshot()
		t.Cleanup(func() { _ = snap.Close() })
		req := SearchReq{Optional: []string{"alfa", "bravo"}, Limit: 10}
		if hits, err := ix.Search(ctx, snap, refTenant, refNS, req); err != nil || len(hits) != req.Limit {
			t.Fatalf("a corpus of %d answered with %d hits, %v", docs, len(hits), err)
		}
		return testing.AllocsPerRun(5, func() {
			if _, err := ix.Search(ctx, snap, refTenant, refNS, req); err != nil {
				t.Fatalf("searching: %v", err)
			}
		})
	}

	const small, large = 250, 2_000
	few, many := measure(small), measure(large)
	// The slack is the postings buffer's three further doublings between the two
	// corpus sizes, with room to spare. It is not room for a per-candidate map.
	const slack = 8
	if many > few+slack {
		t.Fatalf("a page of ten cost %v allocations over %d candidates and %v over %d: "+
			"the bookkeeping is following the candidates", few, small, many, large)
	}
	t.Logf("a page of ten cost %v allocations over %d candidates and %v over %d", few, small, many, large)
}
