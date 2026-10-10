package text

import (
	"bytes"
	"context"
	"errors"
	"math"
	"slices"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
)

// BM25's two constants, at the values the literature settled on and Rust used.
//
// They are constants for the same reason the tokeniser's rules are: they change
// what a query returns rather than how fast it returns, and an operator turning
// one would be changing results while believing they were tuning performance.
const (
	// BM25K1 is term-frequency saturation: how quickly a word repeated in a
	// document stops adding to its score.
	BM25K1 = 1.2
	// BM25B is length normalisation: how much a long document is discounted
	// for having more chances to contain the word.
	BM25B = 0.75
)

// SearchReq is one keyword query.
//
// The two term lists are the correction to the plan's single `terms []string`,
// and they are what makes "a tag filter narrows exactly like any other term"
// true in mechanism rather than only in intent. A term either disqualifies a
// candidate that lacks it or merely scores one that has it, and no index can
// infer which from the term alone.
type SearchReq struct {
	// Required terms must all be present. Tags become these.
	//
	// They gate, and they score only when there is nothing else to score by.
	// See [Index.Search] for why that distinction is not a detail.
	Required []string
	// Optional terms decide the score. A candidate must carry at least one of
	// them when there are any. Content words become these.
	Optional []string
	// Limit is how many hits to return, best first.
	Limit int
}

// Hit is one scored record.
type Hit struct {
	ID id.ID
	// Score is the BM25 sum over the query's terms. It is unbounded and
	// corpus-dependent: it orders this result set and means nothing beside a
	// cosine or beside another corpus's score. internal/query is where it is
	// squashed for reporting, and it is deliberately not what a hit's
	// user-facing relevance comes from when a vector step also matched.
	Score float32
}

// Search answers a keyword query.
//
// # Cost
//
// One prefix scan per term, plus one read of the corpus statistics. Nothing
// reads a record body and nothing reads an attribute row: term frequency and
// document length are in the posting, so a candidate is scored where it is
// found. The cost is therefore the summed document frequency of the query's
// terms — proportional to the matches, which is plan §II.10 row 1 and the whole
// of what REM-29 never fixed in Rust.
//
// The honest qualification: a term that half the corpus contains has half the
// corpus as its postings, and scanning them is proportional to *its* matches
// rather than to the query's. Stop words remove the worst of that and nothing
// removes all of it; a term that selective is also one that tells the ranking
// almost nothing, which is what inverse document frequency then says about it.
//
// Nothing between the scan and the answer is proportional to the candidates in
// any other way. The postings of the query's terms are merged in record-id
// order — which is the order a term's keys are already stored in — so every
// term that carries a candidate is seen together with it, and whether it is a
// hit and what it scores are settled there and then. There is no map keyed by
// record id, and the ranking is a bounded selection of the best [SearchReq.Limit]
// hits rather than a sort of every candidate. What the query holds at its peak
// is the postings themselves plus the page it will return.
//
// Nor is it proportional to the query's terms in any other way. A candidate
// costs the terms still carrying postings and never the terms that have run
// out, which matters because a 64 KiB query is a valid query and holds thousands
// of terms. How the candidate is found — a scan of the live cursors for a short
// query, a heap of them for a long one — is [termMerge.rank], with the
// measurements that produced the threshold.
//
// Holding the postings is the one thing this cannot stream. The document
// frequency BM25 needs is the length of a term's postings list: it is not in
// [Stats], which holds the corpus totals and nothing per term, so it is known
// only once the list has been read to its end. A counting pre-pass would read
// each posting twice, and "a five-match query touches six stored entries" —
// TestKeywordSearchCostIsProportionalToMatches, and the measurement in
// docs/architecture/text.md — is a promise about exactly that number. So the
// postings are read once and kept, and the peak is the summed document
// frequency of the query's terms rather than the largest single one.
//
// # What a required term does, and what it must not do
//
// It gates. A candidate lacking one is not a hit, whatever else it matches.
//
// When there are optional terms it contributes **no score**, and a candidate
// matching none of them is not a hit either. Both halves were learned from a
// live corpus: a required tag that scored promoted memories carrying the tag and
// nothing else above memories that actually matched the query, so "find me
// memories about consensus, tagged infra" answered with a bakery. A filter that
// changes which memories come back is not a filter.
//
// When there are no optional terms the required ones score, because otherwise a
// tag-only search — "everything tagged finance", a perfectly ordinary request —
// would come back in record-id order with nothing to rank it by.
func (ix *Index) Search(ctx context.Context, r Reader, t tenant.ID, ns tenant.Namespace,
	req SearchReq) ([]Hit, error) {
	const op = "text.Index.Search"

	if t == "" {
		return nil, errs.E(errs.Invalid, op, errNoTenant)
	}
	if len(req.Required) == 0 && len(req.Optional) == 0 {
		// It says "produced no terms" rather than "was empty" because the two
		// are different and only one is the caller's mistake: a query of pure
		// punctuation reaches here with something in it. Refusing is still
		// right — answering "no matches" for a query the index cannot represent
		// is the conflation §II.10 row 2 exists to remove — but a message that
		// describes the wrong cause sends the caller looking in the wrong place.
		return nil, errs.E(errs.Invalid, op, errors.New(
			"this query produced no searchable terms, so there is nothing to look up; "+
				"answering it with the whole corpus would turn a client bug into a full scan"))
	}
	if req.Limit <= 0 {
		return nil, errs.E(errs.Invalid, op, errors.New("a search limit must be greater than zero"))
	}

	stats, err := ReadStats(ctx, r, t, ns)
	if err != nil {
		return nil, errs.E(errs.KindOf(err), op, err)
	}

	m := newTermMerge(len(req.Required) + len(req.Optional))
	for _, term := range req.Required {
		if err := m.read(ctx, r, t, ns, term, stats); err != nil {
			return nil, errs.E(errs.KindOf(err), op, err)
		}
	}
	m.required = len(m.terms)
	// Nothing carries any required term. Returning here is not an
	// optimisation: it is the difference between "no memory has this tag" and
	// Rust's "the tag index cannot answer, so there are no matches" — the
	// conflation plan §II.10 row 2 exists to remove. It also keeps the optional
	// terms unread, which is what makes a filter on a tag nobody carries cost
	// the tag's empty scan and nothing else.
	if len(req.Required) > 0 && len(m.postings) == 0 {
		return nil, nil
	}

	for _, term := range req.Optional {
		if err := m.read(ctx, r, t, ns, term, stats); err != nil {
			return nil, errs.E(errs.KindOf(err), op, err)
		}
	}

	top := newTopHits(req.Limit)
	// Required terms score only when there is nothing else to score by.
	if err := m.rank(ctx, len(req.Optional) > 0, stats.AvgDocLen(), &top); err != nil {
		return nil, errs.E(errs.KindOf(err), op, err)
	}
	return top.ranked(), nil
}

// --- the merge ---------------------------------------------------------------

// termPosting is one posting with the record it names.
//
// It is flat and copied by value: a merge over several terms holds thousands of
// these, and a pointer each would turn one contiguous run through memory into a
// pointer chase per candidate.
type termPosting struct {
	rid id.ID
	p   posting
}

// termCursor is one term's run of postings inside [termMerge.postings], how far
// the merge has walked into it, and the weight every posting in it carries.
//
// The run is identified by index rather than by a slice because the postings of
// all the query's terms live in one buffer that grows as each term is read, and
// a slice taken before a growth would point into the old array.
type termCursor struct {
	at, end int
	// idf is the term's inverse document frequency, which is a function of how
	// many postings the run holds and therefore knowable only once it is read.
	idf float64
}

// weigh is what one of this term's postings contributes to its record's score.
//
// It is one expression in one place because it is the arithmetic the whole
// change had to leave alone: the same two operations on the same operands in the
// same order as the implementation this replaced.
func (c *termCursor) weigh(p posting, avgDocLen float64) float64 {
	return c.idf * termWeight(float64(p.TermFreq), float64(p.DocLen), avgDocLen)
}

// termMerge is the postings of a query's terms, one run per term, in the order
// the terms contribute to a score: every required term first, in the order the
// caller gave them, then every optional one.
//
// That order is not presentation. Each term's contribution is a float64 added
// into a candidate's running sum and float64 addition is not associative, so a
// query whose terms are summed in another order can round differently.
// Required-then-optional is the order the three-map implementation summed in,
// and the order every score this index has ever returned was produced in; it is
// kept because nothing is gained by changing it. The rounding does not in fact
// reach the answer — a candidate's sum is squashed to float32 on its way into a
// [Hit], and reversing the order of the optional terms moves no float32 score in
// TestSearchMatchesTheReferenceImplementation's corpus — but that is a
// measurement, not a guarantee, and it is not one worth depending on.
//
// The order is an invariant the merge states rather than one it can read off
// its own structure. Both strategies in [termMerge.rank] happen to collect a
// candidate's cursors in term order already, for different and separately
// fragile reasons, so carrying is sorted anyway before anything is summed. See
// the sort in rank for why a guard is cheaper than that argument.
type termMerge struct {
	postings []termPosting
	terms    []termCursor
	// required is how many of terms are required terms, which are terms[:required].
	required int

	// active is the cursors that still have a posting, held as indices into
	// terms. Either a min-heap keyed by the record id at each cursor's head or
	// an unordered live set scanned for the lowest, depending on heap; a cursor
	// leaves it when its last posting is consumed, so under both strategies an
	// exhausted term costs the merge nothing from then on.
	active []int
	// carrying is one candidate's cursors, reused across candidates so that
	// the merge allocates nothing per candidate. It is never longer than
	// terms.
	carrying []int
	// heap is which strategy this merge chose, which is [usesHeap] of the
	// query's term count. It is set once, at the top of rank.
	heap bool
}

// newTermMerge is the working set for a query of n terms, allocated once.
//
// Every slice is sized for the whole query up front because the alternative is
// growth inside the merge: the heap and the scratch would double as the merge
// ran, which is an allocation count that follows the candidates rather than the
// query. TestKeywordSearchAllocationsDoNotFollowTheCandidateCount is the claim.
func newTermMerge(n int) termMerge {
	return termMerge{
		terms:    make([]termCursor, 0, n),
		active:   make([]int, 0, n),
		carrying: make([]int, 0, n),
	}
}

// read appends one term's postings, in the record-id order the keys are stored
// in, and records the run as a cursor.
func (m *termMerge) read(ctx context.Context, r Reader, t tenant.ID, ns tenant.Namespace,
	term string, stats Stats) error {
	const op = "text.termMerge.read"

	lower, upper := keys.TextTermRange(t, ns, term)
	it := r.NewIterator(lower, upper)
	defer func() { _ = it.Close() }()

	start := len(m.postings)
	for ok := it.First(); ok; ok = it.Next() {
		// Not keys.ParseText: it would decode the tenant, the namespace and the
		// term into three strings for every posting, all of them values this
		// scan built the range from. See [keys.TextRecordID].
		rid, err := keys.TextRecordID(it.Key(), lower)
		if err != nil {
			return err
		}
		p, err := decodePosting(it.Value())
		if err != nil {
			return err
		}
		m.postings = append(m.postings, termPosting{rid: rid, p: p})
		if err := ctx.Err(); err != nil {
			return errs.E(errs.Unavailable, op, err)
		}
	}
	if err := it.Error(); err != nil {
		return err
	}

	m.terms = append(m.terms, termCursor{
		at: start, end: len(m.postings),
		idf: inverseDocumentFrequency(stats.Documents, uint64(len(m.postings)-start)),
	})
	return nil
}

// rankCancelCheck is how many candidates the merge scores between two looks at
// the context.
//
// A candidate is a handful of comparisons and a float multiply, so checking
// every one would put a context load in the innermost loop for no gain; a
// thousand of them is microseconds, which is sooner than any caller can tell.
const rankCancelCheck = 1024

// heapMergeMinTerms is how many terms a query needs before the merge finds each
// candidate through a heap of the cursors rather than by scanning them.
//
// There is a threshold because neither strategy wins everywhere, and the
// difference is measurable in both directions. For a handful of terms the scan
// is two or three comparisons per candidate and the heap is a pop, a sift and a
// push: the heap alone measured +10% on BenchmarkSearchCommonTerms, which is two
// terms over twenty thousand documents. Past a handful the scan's cost per
// candidate keeps growing with the query while the heap's grows only with its
// logarithm, and the query that made this a defect — six thousand terms, which
// is what 64 KiB of query holds — took 134 ms scanning against 4 ms with the
// heap. Eight is inside the flat part of both curves.
//
// It is a var rather than a const only so that the suite can force the heap onto
// a small query. TestSearchMatchesTheReferenceImplementation and
// TestKeywordSearchAllocationsDoNotFollowTheCandidateCount each run their whole
// corpus through both strategies, which is the only way to know the two agree;
// nothing outside a test writes it.
var heapMergeMinTerms = 8

// usesHeap reports whether a query of n terms merges through the heap. It is the
// one place the choice is made, so a test can assert the choice itself rather
// than infer it.
func usesHeap(n int) bool { return n >= heapMergeMinTerms }

// rank walks the merged postings in record-id order and offers every candidate
// that passes the gate to top.
//
// gateOnly is whether the required terms only gate — true when the query has
// optional terms to score by. See [Index.Search].
//
// # How candidates are found, and why there are two ways
//
// The first version of this merge found its next candidate by scanning every
// cursor and then scanned every cursor again to score it, on the argument that
// the query's terms are a handful and a heap would cost more in bookkeeping than
// it saved in comparisons. Half of that was wrong, and the wrong half was the
// premise: "a handful" was an assumption about the caller, not a property of the
// index. A query is bounded at memory.MaxQueryBytes — 64 KiB — so a valid
// 59,999-byte query holds six thousand distinct terms, the cost was candidates
// times terms, and an exhausted term cost exactly as much as a live one.
// Measured on that query over six thousand matching documents: 134 ms, against
// 5 ms for the three-map implementation this merge had replaced in order to be
// faster, and 4 ms with a heap.
//
// The other half was right, and measuring it is what produced a threshold
// rather than a replacement. For two terms the heap is a pop, a sift and a push
// where the scan is two comparisons, and replacing one with the other measured
// +10% on BenchmarkSearchCommonTerms. So the collection step below is written
// twice and [usesHeap] picks per query: a scan of the live cursors under
// heapMergeMinTerms, where a candidate costs the terms still carrying postings,
// and the heap at or above it, where it costs only the terms carrying that
// candidate times the logarithm of the ones still active.
//
// Everything after the collection — the sort, the scoring, the gate and the
// bounded selection — is shared, which keeps the summation order and the gating
// rules in one place for both.
//
// # What that still costs, and what it bought
//
// The two collection bodies are written out inside the loop rather than behind a
// method each, which is ugly and was not the first attempt. A method per
// strategy is a call per candidate that the compiler will not inline — the
// bodies contain loops — and that alone measured +6 of the +10 points, so
// factoring it the obvious way would have given up most of what the threshold
// was for. With them inline the merge is +3.5% on BenchmarkSearchCommonTerms
// (p=0.002, n=20), which is the price of the intermediate carrying list that the
// shared scoring pass reads; the only way further down is to fuse scoring into
// each collection body, and two copies of the gating rules and the summation
// order is a worse thing to own than 3.5%. docs/architecture/text.md has every
// measurement and how it was taken.
func (m *termMerge) rank(ctx context.Context, gateOnly bool, avgDocLen float64, top *topHits) error {
	const op = "text.termMerge.rank"

	m.heap = usesHeap(len(m.terms))
	m.active = m.active[:0]
	for i := range m.terms {
		if m.terms[i].at >= m.terms[i].end {
			continue
		}
		if m.heap {
			m.pushCursor(i)
			continue
		}
		m.active = append(m.active, i)
	}

	for candidates := 0; ; candidates++ {
		if candidates%rankCancelCheck == 0 {
			if err := ctx.Err(); err != nil {
				return errs.E(errs.Unavailable, op, err)
			}
		}

		// Every cursor whose head is the lowest record id any term still has a
		// posting for, which is every term carrying that candidate. The one step
		// the two strategies do differently, and written out here rather than
		// behind a method each for the measured reason in the doc comment.
		m.carrying = m.carrying[:0]
		var rid id.ID
		if m.heap {
			// The heap has to take a cursor out to read its key, so the ones
			// that still have postings are pushed back after the scoring below.
			if len(m.active) == 0 {
				return nil
			}
			rid = m.postings[m.terms[m.active[0]].at].rid
			for len(m.active) > 0 && m.postings[m.terms[m.active[0]].at].rid == rid {
				m.carrying = append(m.carrying, m.popCursor())
			}
		} else {
			// One pass over the live cursors doing three things that each want a
			// pass of their own: dropping the cursors the previous candidate
			// exhausted, tracking the lowest head record id, and accumulating
			// the cursors carrying it — resetting the accumulation whenever a
			// lower id turns up. Compaction is order-preserving, so active stays
			// ascending and carrying comes out in term order.
			live := 0
			for _, i := range m.active {
				if m.terms[i].at >= m.terms[i].end {
					continue
				}
				m.active[live] = i
				live++

				if len(m.carrying) == 0 {
					m.carrying = append(m.carrying, i)
					continue
				}
				c := bytes.Compare(m.postings[m.terms[i].at].rid[:],
					m.postings[m.terms[m.carrying[0]].at].rid[:])
				if c < 0 {
					m.carrying = append(m.carrying[:0], i)
				} else if c == 0 {
					m.carrying = append(m.carrying, i)
				}
			}
			m.active = m.active[:live]
			if len(m.carrying) == 0 {
				return nil
			}
			rid = m.postings[m.terms[m.carrying[0]].at].rid
		}

		// Into term order, which the score must be summed in: see [termMerge].
		//
		// Both strategies in fact deliver it that way already — the scan keeps
		// active ascending, and successive pops from a min-heap come out in the
		// comparator's order, whose tiebreak is the cursor index — so this sort
		// moves nothing today. It stays because the alternative is a summation
		// order resting on two separate and unrelated properties of two
		// different data structures at a distance, and the thing resting on it
		// is float64 addition that is not associative. Two lines are cheaper
		// than that argument.
		//
		// slices.Sort rather than a hand-written insertion sort, although a
		// handful is what this holds for any query a person will type: it *is*
		// an insertion sort at twelve elements or fewer and an n log n one above
		// that, which is the difference between answering and crawling on a
		// query that asks for every distinct word of a document that has
		// thousands. The length is tested here because most candidates carry
		// exactly one of the query's terms and there is nothing to sort.
		if len(m.carrying) > 1 {
			slices.Sort(m.carrying)
		}

		// In term order, which is why the required count is complete before an
		// optional term is weighed.
		var score float64
		present, matched := 0, 0
		for _, i := range m.carrying {
			c := &m.terms[i]
			p := m.postings[c.at].p
			c.at++

			if i < m.required {
				present++
				if !gateOnly {
					score += c.weigh(p, avgDocLen)
				}
				continue
			}
			if m.required > 0 && present < m.required {
				// Not a candidate: a required term is missing. Scoring it would
				// cost nothing here and would cost a filter pass below.
				continue
			}
			score += c.weigh(p, avgDocLen)
			matched++
		}

		// The heap had to take its cursors out to see their keys, so the ones
		// that still have postings go back in. The scan never took them out,
		// and dropped the ones this candidate exhausted as it collected them.
		if m.heap {
			for _, i := range m.carrying {
				if m.terms[i].at < m.terms[i].end {
					m.pushCursor(i)
				}
			}
		}

		switch {
		case m.required > 0 && present < m.required:
			// A candidate lacking a required term is not a hit, whatever else
			// it matched.
		case gateOnly && matched == 0:
			// A gate is not a source: a candidate carrying every tag and none
			// of the words is not an answer to a query that asked for the words.
		default:
			top.push(Hit{ID: rid, Score: float32(score)})
		}
	}
}

// --- scoring -----------------------------------------------------------------

// inverseDocumentFrequency is how much a term's presence tells us, given how
// many documents contain it.
//
// The +1 inside the logarithm is the standard guard against a negative weight:
// a term in more than half the corpus would otherwise score *against* the
// documents that contain it, and a query for a common word would rank the
// documents without it first.
//
// The document count is floored at the term's own frequency for the same class
// of reason. The two are maintained exactly and cannot disagree, but a corpus
// being repaired from a damaged statistics row can — and a count below the
// frequency makes the numerator negative, which is a ranking silently turned
// inside out rather than an error anybody sees.
func inverseDocumentFrequency(documents, df uint64) float64 {
	if documents < df {
		documents = df
	}
	n, f := float64(documents), float64(df)
	return math.Log(1 + (n-f+0.5)/(f+0.5))
}

// termWeight is BM25's saturating term frequency, normalised by how long the
// document is against the corpus average.
//
// Saturation is why a word repeated fifty times does not score fifty times a
// word used once: past a handful of occurrences the document is about the word,
// and more repetitions say nothing further. Length normalisation is why a long
// document does not win merely by having more room to contain the word.
func termWeight(tf, docLen, avgDocLen float64) float64 {
	norm := BM25K1 * (1 - BM25B + BM25B*docLen/avgDocLen)
	return tf * (BM25K1 + 1) / (tf + norm)
}
