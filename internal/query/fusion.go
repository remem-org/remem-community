package query

import (
	"bytes"
	"sort"

	"github.com/remem-org/remem-go/internal/id"
)

// RRFK is the Reciprocal Rank Fusion constant, fixed at 60 (plan §Global
// Constraints). It is a constant rather than a setting because it is a
// compatibility surface with Rust: the differential harness compares orderings,
// and a tuned k would make every comparison a comparison of two tunings.
const RRFK = 60

// RankedItem is one record's position in one index's own ranking.
type RankedItem struct {
	ID id.ID
	// Score is that index's relevance, on the comparable 0..1 scale. Fusion
	// does not read it; it is carried through so a hit can report what
	// actually matched rather than only where it ended up.
	Score float32
	// Ordering is the value this step ordered by, on its own scale. It becomes
	// a hit's FusedScore when there is a single step and nothing is fused —
	// which is the honest answer there, because a rank-derived number over one
	// list would look comparable to a hybrid search's and is not.
	Ordering float32
	// Distance is the raw metric value for a vector step, zero elsewhere. It
	// is reported for debugging and for the differential harness, and is not
	// shown to users.
	Distance float32
}

// RankedList is one index's contribution to a fused ranking.
type RankedList struct {
	Source Source
	Items  []RankedItem
}

// Fuse merges ranked lists by position rather than by score.
//
// # Why by rank and not by score
//
// The lists are not score-comparable and cannot be made so. A vector index
// returns distances, an inverted index returns term frequencies, and a
// traversal returns depths; normalising them onto a common scale means
// inventing a conversion, and every choice of conversion is a silent
// re-weighting of the sources. Rank fusion needs no conversion: a record's
// contribution is 1/(k + rank + 1) from each list it appears in, so a record
// several indexes agree on outranks one only a single index found.
//
// The arithmetic is float32 accumulated in list order, matching Rust exactly
// (engine/query/merge.rs). That is not incidental precision-chasing: the
// differential harness compares orderings between the two implementations, and
// float64 here would put ties in different places.
//
// Ties are broken on the record id, ascending. Without a deterministic
// tiebreak the ordering falls back to map iteration order, and two identical
// queries return different truncated result *sets* — not merely different
// orders — which is the failure Rust's comment records.
func Fuse(lists []RankedList, limit int) []Hit {
	type acc struct {
		fused   float32
		sources []Contribution
	}
	fused := make(map[id.ID]*acc)
	order := make([]id.ID, 0, limit*len(lists))

	for _, list := range lists {
		for rank, item := range list.Items {
			a, seen := fused[item.ID]
			if !seen {
				a = &acc{}
				fused[item.ID] = a
				order = append(order, item.ID)
			}
			a.fused += 1.0 / (float32(RRFK) + float32(rank) + 1.0)
			a.sources = append(a.sources, Contribution{
				Source: list.Source, Score: item.Score, Rank: rank, Distance: item.Distance,
			})
		}
	}

	hits := make([]Hit, 0, len(order))
	for _, rid := range order {
		a := fused[rid]
		// Strongest evidence first, so an explanation leads with the index
		// that actually matched rather than with whichever ran first.
		sort.SliceStable(a.sources, func(i, j int) bool { return a.sources[i].Score > a.sources[j].Score })
		hits = append(hits, Hit{
			ID:         rid,
			Score:      deriveScore(a.sources),
			FusedScore: a.fused,
			Distance:   vectorDistance(a.sources),
			Sources:    a.sources,
		})
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].FusedScore != hits[j].FusedScore {
			return hits[i].FusedScore > hits[j].FusedScore
		}
		return bytes.Compare(hits[i].ID[:], hits[j].ID[:]) < 0
	})

	if limit > 0 && len(hits) > limit {
		hits = hits[:limit]
	}
	return hits
}

// vectorDistance is the raw metric value behind a hit, or zero when no vector
// step contributed to it. There is at most one, so there is nothing to choose
// between.
func vectorDistance(sources []Contribution) float32 {
	for _, s := range sources {
		if s.Source == SourceVector {
			return s.Distance
		}
	}
	return 0
}
