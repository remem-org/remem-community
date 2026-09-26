package http

import (
	"net/http"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/memory"
)

// SearchRequest is the body of a search.
type SearchRequest struct {
	Query string `json:"query"`

	// SearchType is required: "semantic", "keyword" or "hybrid". There is no
	// default, deliberately — see memory.SearchType. Rust defaults to
	// "semantic" here and "hybrid" over MCP, which is the same request answered
	// differently depending on which door it came through.
	SearchType string `json:"search_type"`

	// Tags every returned memory must carry, all of them. They narrow every
	// step of the search rather than only the keyword one.
	Tags []string `json:"tags,omitempty"`

	Limit int `json:"limit,omitempty"`
	// Filters. They conjoin: policy long_term with importance_min 0.9 returns
	// memories that are both.
	Policy        string   `json:"policy,omitempty"`
	ImportanceMin *float32 `json:"importance_min,omitempty"`
	ImportanceMax *float32 `json:"importance_max,omitempty"`

	CreatedAfter  *time.Time `json:"created_after,omitempty"`
	CreatedBefore *time.Time `json:"created_before,omitempty"`

	// RelatedTo anchors a graph step: memories connected to this one are fused
	// into the ranking alongside the content match.
	RelatedTo string `json:"related_to,omitempty"`

	// Cursor resumes a previous page. It is bound to the tenant and to this
	// exact query, and it does not survive a restart.
	Cursor string `json:"cursor,omitempty"`
	// Explain returns the per-index evidence behind each hit.
	Explain         bool `json:"explain,omitempty"`
	IncludeArchived bool `json:"include_archived,omitempty"`
}

// SearchResponse is a ranked answer.
type SearchResponse struct {
	Results    []SearchResult `json:"results"`
	NextCursor string         `json:"next_cursor,omitempty"`
	// HasMore says another page exists. Always present, even when false: a
	// flag that appears only when set is a flag clients forget to check --
	// and before paging existed a short page meant "that is everything".
	HasMore bool `json:"has_more"`
	// Truncated says the search exhausted its candidate budget before filling
	// the limit, so there may be matches it did not see. It is always present,
	// even when false: a flag that appears only when set is a flag clients
	// forget to check.
	Truncated bool `json:"truncated"`
}

// SearchResult is one ranked memory.
type SearchResult struct {
	Memory MemoryResponse `json:"memory"`

	// Score is relevance in [0, 1], comparable across requests.
	Score float32 `json:"score"`

	// FusedScore is the value the ordering was decided by, and it is
	// deliberately not the same number as Score (behaviour baseline §1.1 and
	// §1.4). A client that re-sorts by Score gets a different order than the
	// server intended; a client that shows FusedScore to a user is showing a
	// number that means nothing outside this one response. Both are returned
	// so that neither has to be guessed at.
	FusedScore float32 `json:"fused_score"`

	// Sources is the per-index evidence, best-scoring first. Present only when
	// the request asked to explain, because it is bytes every caller would
	// otherwise pay for on every search.
	Sources []SourceScore `json:"sources,omitempty"`
}

// SourceScore is one index's evidence for one memory.
type SourceScore struct {
	// Source is "vector", "text" or "graph".
	Source string `json:"source"`
	// Score is that index's own relevance, on the 0..1 scale it reports. A
	// text score is a squashed BM25 sum and is not comparable with a cosine;
	// see query.TextRelevance.
	Score float32 `json:"score"`
	// Rank is the position the memory held in that index's own list.
	Rank int `json:"rank"`
	// Distance is the raw metric value for a vector contribution.
	Distance float32 `json:"distance,omitempty"`
}

// searchMemories is POST rather than GET, and the reason is not REST
// aesthetics: a query is user content, and a GET would put it in the URL,
// where it lands in access logs, proxy logs and browser history. A memory
// search query is exactly the kind of text that must not be there.
func (d Deps) searchMemories(w http.ResponseWriter, r *http.Request) {
	var body SearchRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	var relatedTo *id.ID
	if body.RelatedTo != "" {
		rid, err := id.Parse(body.RelatedTo)
		if err != nil {
			WriteMalformed(w, r, "related_to is not a UUID")
			return
		}
		relatedTo = &rid
	}

	res, err := d.Memories.Search(r.Context(), memory.SearchReq{
		Query:           body.Query,
		Type:            memory.SearchType(body.SearchType),
		Tags:            body.Tags,
		Limit:           body.Limit,
		Policy:          body.Policy,
		ImportanceMin:   body.ImportanceMin,
		ImportanceMax:   body.ImportanceMax,
		CreatedAfter:    body.CreatedAfter,
		CreatedBefore:   body.CreatedBefore,
		RelatedTo:       relatedTo,
		Cursor:          body.Cursor,
		Explain:         body.Explain,
		IncludeArchived: body.IncludeArchived,
	})
	if err != nil {
		WriteError(w, r, err)
		return
	}

	out := SearchResponse{
		Results:    make([]SearchResult, len(res.Results)),
		NextCursor: res.NextCursor,
		HasMore:    res.HasMore,
		Truncated:  res.Truncated,
	}
	for i, r := range res.Results {
		out.Results[i] = SearchResult{
			Memory:     toResponse(r.Memory),
			Score:      r.Score,
			FusedScore: r.FusedScore,
			Sources:    toSourceScores(r.Sources),
		}
	}
	writeJSON(w, r, http.StatusOK, out)
}

func toSourceScores(in []memory.SourceScore) []SourceScore {
	if len(in) == 0 {
		return nil
	}
	out := make([]SourceScore, len(in))
	for i, s := range in {
		out[i] = SourceScore{Source: s.Source, Score: s.Score, Rank: s.Rank, Distance: s.Distance}
	}
	return out
}
