package text_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/txn"
)

// write indexes one memory through the write path's own gate and retry.
func write(t *testing.T, ix *text.Index, kv storage.KV, tn tenant.ID, content string, tags ...string) id.ID {
	t.Helper()
	rid := id.New()
	ctx := context.Background()
	err := txn.Do(ctx, kv, func(tx txn.Tx) error {
		return ix.Stage(ctx, tx, tn, ns, rid, memoryRecord(rid, tn, content, tags...))
	})
	if err != nil {
		t.Fatalf("indexing: %v", err)
	}
	return rid
}

func search(t *testing.T, ix *text.Index, r text.Reader, tn tenant.ID, req text.SearchReq) []text.Hit {
	t.Helper()
	hits, err := ix.Search(context.Background(), r, tn, ns, req)
	if err != nil {
		t.Fatalf("searching: %v", err)
	}
	return hits
}

func ids(hits []text.Hit) []id.ID {
	out := make([]id.ID, len(hits))
	for i, h := range hits {
		out[i] = h.ID
	}
	return out
}

// TestKeywordSearchCostIsProportionalToMatches is the plan's test, and the
// whole point of the phase (plan §II.10 row 1, the defect REM-29 never fixed).
// Rust scans every record on every keyword query; here a term is a key prefix,
// so answering a five-match query reads five postings and one statistics row.
//
// It asserts proportionality directly — the same query over a corpus four times
// the size touches the same number of stored entries — rather than comparing
// against a threshold. A threshold is a number somebody eventually raises; "it
// did not grow with the corpus" is the claim itself, and it fails the moment a
// scan reappears anywhere in the path.
//
// The corpus is thousands rather than the plan's hundred thousand because the
// in-memory store inserts into a sorted slice, so building one is quadratic in
// the store rather than linear in anything this package does. The
// hundred-thousand measurement was taken once by hand and is recorded in
// docs/architecture/text.md: six entries, which is the five matches and the
// statistics row.
func TestKeywordSearchCostIsProportionalToMatches(t *testing.T) {
	kv := counting(memkv.New())
	t.Cleanup(func() { _ = kv.Close() })
	ix := text.New()
	ctx := context.Background()

	fill := func(from, to int) {
		t.Helper()
		const perBatch = 100
		for start := from; start < to; start += perBatch {
			err := txn.Do(ctx, kv, func(tx txn.Tx) error {
				for i := start; i < min(start+perBatch, to); i++ {
					rid := id.New()
					content := fmt.Sprintf("routine note number %d about ordinary matters", i)
					// The five matching memories are all in the first
					// thousand, so growing the corpus adds no matches and the
					// only thing that can move the count is the access path.
					if i < 1_000 && i%200 == 0 {
						content += " containing the distinctive word pangolin"
					}
					if err := ix.Stage(ctx, tx, acme, ns, rid, memoryRecord(rid, acme, content)); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatalf("building the corpus: %v", err)
			}
		}
	}

	measure := func(wantMatches int) int64 {
		t.Helper()
		kv.reset()
		hits := search(t, ix, kv, acme, text.SearchReq{Optional: text.QueryTerms("pangolin"), Limit: 10})
		if len(hits) != wantMatches {
			t.Fatalf("got %d matches, want %d", len(hits), wantMatches)
		}
		return kv.touched()
	}

	fill(0, 5_000)
	small := measure(5)

	fill(5_000, 20_000)
	large := measure(5)

	if small != large {
		t.Fatalf("the same five-match query touched %d stored entries over 5,000 memories and %d over "+
			"20,000: the cost is following the corpus rather than the matches", small, large)
	}
	// Five postings and the corpus statistics row. Stated so that a change
	// which keeps the cost constant but makes it constant-and-large is still
	// visible in a diff.
	if small != 6 {
		t.Fatalf("a five-match query touched %d stored entries, want 6 "+
			"(five postings and the statistics row)", small)
	}
	t.Logf("a five-match query touched %d stored entries at both 5,000 and 20,000 memories", small)
}

// TestTagFilterNeedsNoSpecialCase is the plan's test. A 120-byte tag filters
// correctly, where Rust's tag index reports "no matches" for a tag outside its
// length bounds and the caller falls back to scanning the payload.
func TestTagFilterNeedsNoSpecialCase(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := text.New()

	long := "quarterly-planning-and-budget-review-for-the-european-region-including-every-subsidiary-x-x-x-x-x-x-x-x-xy"
	long += "-and-then-some" // 120 bytes
	if len(long) != 120 {
		t.Fatalf("the test's tag is %d bytes, not the 120 it is written about", len(long))
	}

	tagged := write(t, ix, kv, acme, "the quarterly numbers", long)
	write(t, ix, kv, acme, "the quarterly numbers", "short")

	hits := search(t, ix, kv, acme, text.SearchReq{
		Required: []string{text.TagTerm(long)},
		Optional: text.QueryTerms("quarterly numbers"),
		Limit:    10,
	})
	if got := ids(hits); !slices.Equal(got, []id.ID{tagged}) {
		t.Fatalf("filtering on a %d-byte tag returned %v, want just the tagged memory", len(long), got)
	}
}

// A required term narrows; an optional one scores. One index, one key space,
// and the difference is whether a missing term disqualifies a candidate or
// merely ranks it lower.
//
// The case that used to be asserted here — that a memory carrying the tag and
// none of the words comes back, ranked below one carrying both — was wrong, and
// Phase 8's end-to-end run is what showed it. See
// TestARequiredTermGatesAndScoresOnlyWhenNothingElseDoes.
func TestRequiredTermsNarrowAndOptionalTermsScore(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := text.New()

	both := write(t, ix, kv, acme, "postgres replication and failover", "database")
	fewer := write(t, ix, kv, acme, "postgres tuning", "database")
	write(t, ix, kv, acme, "unrelated musings about weather", "database")
	write(t, ix, kv, acme, "postgres replication and failover", "infra")

	hits := search(t, ix, kv, acme, text.SearchReq{
		Required: []string{text.TagTerm("database")},
		Optional: text.QueryTerms("postgres replication failover"),
		Limit:    10,
	})

	got := ids(hits)
	if !slices.Equal(got, []id.ID{both, fewer}) {
		t.Fatalf("got %v, want the two memories carrying the tag *and* at least one word, "+
			"best match first", got)
	}
	if hits[0].Score <= hits[1].Score {
		t.Fatalf("scores %v and %v do not separate a three-word match from a one-word match",
			hits[0].Score, hits[1].Score)
	}
}

// A required term nothing carries returns nothing. It must not fall back to
// ignoring the term, which is how Rust's tag index turned "I cannot answer
// this" into "there are no matches".
func TestARequiredTermNothingCarriesReturnsNothing(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := text.New()

	write(t, ix, kv, acme, "postgres replication", "database")

	hits := search(t, ix, kv, acme, text.SearchReq{
		Required: []string{text.TagTerm("no-such-tag")},
		Optional: text.QueryTerms("postgres"),
		Limit:    10,
	})
	if len(hits) != 0 {
		t.Fatalf("a filter on a tag nobody carries returned %d memories", len(hits))
	}
}

// Rarer terms carry more weight, which is the whole of inverse document
// frequency. Without it a query's common word drowns out its distinctive one.
func TestARareTermOutweighsACommonOne(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := text.New()

	for range 50 {
		write(t, ix, kv, acme, "meeting notes from the weekly sync")
	}
	rare := write(t, ix, kv, acme, "meeting notes about pangolin conservation")

	hits := search(t, ix, kv, acme, text.SearchReq{
		Optional: text.QueryTerms("meeting pangolin"), Limit: 5,
	})
	if len(hits) == 0 || hits[0].ID != rare {
		t.Fatalf("the memory carrying the rare term did not rank first: %v", ids(hits))
	}
}

// TestBM25StatisticsArePerTenant is the plan's test: adding 10,000 documents to
// tenant B does not change tenant A's scores. A global corpus statistic would
// leak one tenant's distribution into another's ranking, and nothing in the
// results would show it.
func TestBM25StatisticsArePerTenant(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := text.New()
	ctx := context.Background()

	write(t, ix, kv, acme, "kubernetes cluster autoscaling notes")
	write(t, ix, kv, acme, "a different memory entirely")

	before := search(t, ix, kv, acme, text.SearchReq{Optional: text.QueryTerms("kubernetes"), Limit: 5})
	if len(before) != 1 {
		t.Fatalf("got %d hits before, want 1", len(before))
	}

	err := txn.Do(ctx, kv, func(tx txn.Tx) error {
		for i := range 10_000 {
			rid := id.New()
			content := fmt.Sprintf("globex memory %d about kubernetes cluster autoscaling", i)
			if err := ix.Stage(ctx, tx, "globex", ns, rid, memoryRecord(rid, "globex", content)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("filling the other tenant: %v", err)
	}

	after := search(t, ix, kv, acme, text.SearchReq{Optional: text.QueryTerms("kubernetes"), Limit: 5})
	if len(after) != 1 || after[0].ID != before[0].ID {
		t.Fatalf("the result changed: %v then %v", ids(before), ids(after))
	}
	if after[0].Score != before[0].Score {
		t.Fatalf("this tenant's score moved from %v to %v because another tenant stored 10,000 memories",
			before[0].Score, after[0].Score)
	}
}

// Ties are broken on the record id, ascending. Without a deterministic
// tiebreak two identical queries return different truncated result *sets* — not
// merely different orders — which is the failure query.Fuse records.
func TestTiesAreBrokenDeterministically(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := text.New()

	for range 8 {
		write(t, ix, kv, acme, "identical content in every respect")
	}

	first := ids(search(t, ix, kv, acme, text.SearchReq{Optional: text.QueryTerms("identical"), Limit: 3}))
	for range 5 {
		got := ids(search(t, ix, kv, acme, text.SearchReq{Optional: text.QueryTerms("identical"), Limit: 3}))
		if !slices.Equal(first, got) {
			t.Fatalf("the same query returned %v then %v", first, got)
		}
	}
	if !slices.IsSortedFunc(first, func(a, b id.ID) int { return slices.Compare(a[:], b[:]) }) {
		t.Fatalf("tied hits are not in id order: %v", first)
	}
}

// A search with no terms is refused rather than answered with the corpus. An
// empty query that returns everything is how a client bug becomes a full scan.
func TestASearchWithNoTermsIsRefused(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	if _, err := text.New().Search(context.Background(), kv, acme, ns, text.SearchReq{Limit: 10}); err == nil {
		t.Fatal("a search with no terms was answered")
	}
}

// One tenant's search must never reach another's postings, whatever the terms.
func TestSearchIsTenantScoped(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := text.New()

	mine := write(t, ix, kv, acme, "shared vocabulary here")
	write(t, ix, kv, "globex", "shared vocabulary here")

	hits := search(t, ix, kv, acme, text.SearchReq{Optional: text.QueryTerms("shared vocabulary"), Limit: 10})
	if got := ids(hits); !slices.Equal(got, []id.ID{mine}) {
		t.Fatalf("got %v, want only this tenant's memory", got)
	}
}

// A required term gates, and scores only when there is nothing else to score by.
//
// The second half is the other cause of the defect Phase 8's end-to-end run
// found. A tag that scores promotes memories carrying the tag and nothing else
// above memories that actually matched the query, so "memories about consensus,
// tagged infra" answers with a bakery. A filter that changes which memories come
// back, and in what order, is not a filter.
//
// A tag-only search still ranks, because "everything tagged finance" is an
// ordinary request and record-id order is not an answer to it.
func TestARequiredTermGatesAndScoresOnlyWhenNothingElseDoes(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	ix := text.New()

	matching := write(t, ix, kv, acme, "kubernetes cluster autoscaling", "infra")
	tagOnly := write(t, ix, kv, acme, "marzipan", "infra")
	untagged := write(t, ix, kv, acme, "kubernetes cluster autoscaling")

	t.Run("a tagged memory matching no word is not a hit", func(t *testing.T) {
		hits := search(t, ix, kv, acme, text.SearchReq{
			Required: []string{text.TagTerm("infra")},
			Optional: text.QueryTerms("kubernetes cluster autoscaling"),
			Limit:    10,
		})
		got := ids(hits)
		if !slices.Equal(got, []id.ID{matching}) {
			t.Fatalf("got %v, want only the memory carrying the tag *and* the words", got)
		}
		if slices.Contains(got, tagOnly) {
			t.Error("a memory carrying the tag and none of the words was returned")
		}
		if slices.Contains(got, untagged) {
			t.Error("a memory matching the words without the tag was returned")
		}
	})

	t.Run("the tag contributes no score when words do", func(t *testing.T) {
		withTag := search(t, ix, kv, acme, text.SearchReq{
			Required: []string{text.TagTerm("infra")},
			Optional: text.QueryTerms("kubernetes cluster autoscaling"),
			Limit:    10,
		})
		// The same memory, reached without the filter. Its score must be the
		// same number: a filter that moves a score is weighing itself.
		plain := search(t, ix, kv, acme, text.SearchReq{
			Optional: text.QueryTerms("kubernetes cluster autoscaling"), Limit: 10,
		})
		var want float32
		for _, h := range plain {
			if h.ID == matching {
				want = h.Score
			}
		}
		if withTag[0].Score != want {
			t.Fatalf("the memory scores %v under a tag filter and %v without one; the tag is "+
				"contributing to the ranking rather than restricting it", withTag[0].Score, want)
		}
	})

	t.Run("a tag-only search still ranks", func(t *testing.T) {
		hits := search(t, ix, kv, acme, text.SearchReq{
			Required: []string{text.TagTerm("infra")}, Limit: 10,
		})
		if len(hits) != 2 {
			t.Fatalf("a tag-only search returned %d memories, want both tagged ones", len(hits))
		}
		if hits[0].Score == 0 || hits[0].Score == hits[1].Score {
			t.Fatalf("a tag-only search produced no ranking: scores %v and %v",
				hits[0].Score, hits[1].Score)
		}
	})
}
