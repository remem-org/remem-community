package text

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sort"

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

	scores := map[id.ID]float64{}
	// present counts, per candidate, how many required terms it carries. A
	// candidate that carries all of them survives; the count is what makes the
	// intersection one pass over each term's postings rather than a set
	// intersection materialised per term.
	present := map[id.ID]int{}
	// matched counts optional terms, and is what keeps a gate from becoming a
	// source: a candidate that carries every tag and none of the words is not
	// an answer to a query that asked for the words.
	matched := map[id.ID]int{}

	// Required terms score only when there is nothing else to score by.
	gateOnly := len(req.Optional) > 0

	for _, term := range req.Required {
		if err := ix.scoreTerm(ctx, r, t, ns, term, stats, func(rid id.ID, score float64) {
			if !gateOnly {
				scores[rid] += score
			}
			present[rid]++
		}); err != nil {
			return nil, errs.E(errs.KindOf(err), op, err)
		}
	}
	// Nothing carries every required term. Returning here is not an
	// optimisation: it is the difference between "no memory has this tag" and
	// Rust's "the tag index cannot answer, so there are no matches" — the
	// conflation plan §II.10 row 2 exists to remove.
	if len(req.Required) > 0 && len(present) == 0 {
		return nil, nil
	}

	for _, term := range req.Optional {
		if err := ix.scoreTerm(ctx, r, t, ns, term, stats, func(rid id.ID, score float64) {
			if len(req.Required) > 0 && present[rid] < len(req.Required) {
				// Not a candidate. Scoring it would cost nothing here and would
				// cost a filter pass below; skipping keeps the map the size of
				// the answer rather than of the terms' union.
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

	// Ties break on the record id, ascending. Without a deterministic tiebreak
	// the order falls back to map iteration, and two identical queries return
	// different truncated result *sets* rather than merely different orders —
	// the failure query.Fuse records for the same reason.
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

// scoreTerm scans one term's postings and reports each record's contribution.
//
// The document frequency is counted by the same scan that scores, which is why
// it costs nothing: BM25 needs to know how many documents hold the term, and
// the postings are that number. The scan runs twice — once to count, once to
// score — because inverse document frequency is a function of the count and
// every posting needs it. Both passes are over the same buffered rows.
func (ix *Index) scoreTerm(ctx context.Context, r Reader, t tenant.ID, ns tenant.Namespace,
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
			// The range is built from the length-prefixed term, so this cannot
			// happen without a key written by something else. Refusing is
			// cheaper than scoring another term's postings into this one's.
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
