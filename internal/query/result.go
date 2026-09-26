package query

import (
	"fmt"

	"github.com/remem-org/remem-go/internal/id"
)

// Source names the index a contribution came from.
//
// The numbers are stable because they travel in `explain` output and in
// metrics labels, and a renumbering would silently re-attribute historical
// dashboards.
type Source uint8

const (
	// SourceUnspecified is the zero value and never appears in a result.
	SourceUnspecified Source = iota
	// SourceAttr is an ordered walk of an attribute slot index: a listing.
	SourceAttr
	// SourceVector is nearest-neighbour search over canonical vectors.
	SourceVector
	// SourceText is the inverted index (Phase 8).
	SourceText
	// SourceGraph is traversal from an anchor record (Phase 6).
	SourceGraph
)

var sourceNames = [...]string{
	SourceUnspecified: "unspecified",
	SourceAttr:        "attr",
	SourceVector:      "vector",
	SourceText:        "text",
	SourceGraph:       "graph",
}

// String returns the stable name used in explain output and metrics.
func (s Source) String() string {
	if int(s) < len(sourceNames) && sourceNames[s] != "" {
		return sourceNames[s]
	}
	return fmt.Sprintf("source(%d)", uint8(s))
}

// Contribution is one index's evidence for one record.
//
// The native score is kept rather than overwritten by the fused value, because
// the two answer different questions: the fused value says why this record is
// at this position, and the native score says how well it actually matched.
// Rust lost that distinction before REM-74 and `score` came to mean four
// different things depending on which branch produced it.
type Contribution struct {
	Source Source  `json:"source"`
	Score  float32 `json:"score"`
	// Distance is the raw metric value for a vector contribution, zero
	// elsewhere.
	Distance float32 `json:"distance,omitempty"`
	// Rank is the position this record held in that index's own list,
	// zero-based. It is what the fusion arithmetic is a function of.
	Rank int `json:"rank"`
}

// Hit is one ranked record.
type Hit struct {
	ID id.ID `json:"id"`

	// Score is relevance in [0, 1], comparable across requests.
	Score float32 `json:"score"`

	// FusedScore is the value the ordering was decided by. It is a function of
	// rank, not of similarity, so it is not comparable across requests — a
	// caller that sorts by it gets the server's order and a caller that reads
	// it as relevance gets nonsense. Both are reported so neither has to be
	// guessed at.
	FusedScore float32 `json:"fused_score"`

	// Distance is the raw metric value behind the hit, for debugging and for
	// the differential harness. It is not shown to users.
	Distance float32 `json:"distance,omitempty"`

	// Sources are the per-index contributions, best-scoring first. Present
	// only when the query asked to explain.
	Sources []Contribution `json:"sources,omitempty"`
}

// Result is one query's answer.
type Result struct {
	Hits []Hit `json:"hits"`

	// Truncated reports that the widening budget was spent before the
	// requested number of matches was found, so matches may exist that were
	// never looked at. It is the difference between "these are all the
	// matches" and "these are all the matches I looked at", and it is never
	// set for any other reason.
	Truncated bool `json:"truncated"`

	// HasMore reports that the access path had not reached its end.
	HasMore bool `json:"has_more"`

	// NextCursor resumes after the last hit returned. Nil when there is
	// nothing to resume into.
	NextCursor *Cursor `json:"next_cursor,omitempty"`

	// Examined is how many candidates the execution looked at. It is what a
	// widening budget is denominated in and what `explain` reports; it is not
	// shown to users.
	Examined int `json:"examined,omitempty"`
}

// deriveScore is relevance from the contributions that produced a hit.
//
// # The sources are ranked, not averaged
//
// A vector contribution wins when there is one, then text, then graph. The
// reason is that the three are not three measurements of the same quantity.
//
// A cosine is calibrated: two unrelated memories sit near zero and two
// near-duplicates near one, on the same scale in every request, which is what
// makes a relevance threshold usable at all (behaviour baseline §1.3, REM-74).
// A BM25 sum is not: it is unbounded, it rises with how rare the query's words
// happen to be in *this* tenant's corpus, and the same memory matching the same
// words scores differently after the corpus grows. It is squashed into [0,1]
// for reporting by [TextRelevance] and it is still not comparable with a cosine,
// so letting it set relevance when a cosine is available would make `score`
// mean two different things depending on which index ranked highest — which is
// exactly the defect §1.1 records Rust having.
//
// Graph proximity is last for the reason the baseline gives: being one hop from
// an anchor memory is context, not evidence that the content matches the query,
// and letting it set relevance reports 0.5 for an unrelated neighbour.
//
// This ordering is also what makes the plan's completion criterion true by
// construction rather than by luck — a memory found by both indexes scores
// identically under `semantic` and `hybrid`, not merely within 0.05.
func deriveScore(sources []Contribution) float32 {
	best := func(want Source) (float32, bool) {
		var out float32
		found := false
		for _, s := range sources {
			if s.Source != want {
				continue
			}
			if !found || s.Score > out {
				out, found = s.Score, true
			}
		}
		return out, found
	}

	// Attr contributes no relevance at all — a listing's order is the answer
	// and it reports a zero score — so it is last among the calibrated sources
	// and only ever reached when it is the only one.
	for _, source := range []Source{SourceVector, SourceText, SourceGraph, SourceAttr} {
		if score, ok := best(source); ok {
			return clamp01(score)
		}
	}
	return 0
}

// TextScoreK is the constant that squashes an unbounded BM25 sum into [0, 1).
//
// It is a reporting transform and nothing more. It is monotonic, so it never
// changes an order, and it is not a similarity: a BM25 of 8 reports 0.5 because
// 8 is the constant, not because the match is half as good as a perfect one.
// The raw sum is what the ordering uses and what `explain` reports as the
// step's own value.
const TextScoreK = 8.0

// TextRelevance maps a BM25 sum onto [0, 1) for reporting.
func TextRelevance(bm25 float32) float32 {
	if bm25 <= 0 {
		return 0
	}
	return bm25 / (bm25 + TextScoreK)
}

func clamp01(v float32) float32 {
	switch {
	case v < 0:
		return 0
	case v > 1:
		return 1
	default:
		return v
	}
}
