package http

import (
	"net/http"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/memory"
)

// RecallRequest asks for as much relevant memory as fits in a budget.
type RecallRequest struct {
	// Context is what the agent is doing.
	Context string `json:"context"`

	// SearchType is required, exactly as on /memories/search: "semantic",
	// "keyword" or "hybrid". A default here where search refuses one would be
	// the split default Phase 8 removed.
	SearchType string `json:"search_type"`

	// TokenBudget is how much room the caller has. Required: a recall without
	// one is a search, and there is one of those already.
	TokenBudget int `json:"token_budget"`

	// AlreadyHave are memories the caller is holding. They never come back.
	AlreadyHave []string `json:"already_have,omitempty"`

	Tags []string `json:"tags,omitempty"`
}

// RecallResponse is what fits, and what did not.
type RecallResponse struct {
	Results []SearchResult `json:"results"`

	// UsedTokens is the estimated cost of what came back. It is an estimate —
	// four bytes to a token — and the field is named for what it is.
	UsedTokens int `json:"used_tokens"`

	// OmittedCount is how many relevant memories did not fit. Always present,
	// even at zero: an agent that cannot tell "there was nothing else" from
	// "there was more and it did not fit" will believe the first.
	OmittedCount int `json:"omitted_count"`

	// Truncated says the search itself stopped early, which is a different
	// fact from OmittedCount — "I did not look" against "I looked and it did
	// not fit".
	Truncated bool `json:"truncated"`
}

func (d Deps) recallMemories(w http.ResponseWriter, r *http.Request) {
	var body RecallRequest
	if !decodeJSON(w, r, &body) {
		return
	}

	req := memory.RecallReq{
		Context:     body.Context,
		Type:        memory.SearchType(body.SearchType),
		TokenBudget: body.TokenBudget,
		Tags:        body.Tags,
	}
	for _, raw := range body.AlreadyHave {
		rid, err := id.Parse(raw)
		if err != nil {
			WriteError(w, r, err)
			return
		}
		req.AlreadyHave = append(req.AlreadyHave, rid)
	}

	got, err := d.Memories.Recall(r.Context(), req)
	if err != nil {
		WriteError(w, r, err)
		return
	}

	out := RecallResponse{
		Results:      make([]SearchResult, 0, len(got.Memories)),
		UsedTokens:   got.UsedTokens,
		OmittedCount: got.OmittedCount,
		Truncated:    got.Truncated,
	}
	for _, m := range got.Memories {
		out.Results = append(out.Results, SearchResult{
			Memory:     toResponse(m.Memory),
			Score:      m.Score,
			FusedScore: m.FusedScore,
			Sources:    toSourceScores(m.Sources),
		})
	}
	writeJSON(w, r, http.StatusOK, out)
}

// HistoryEntry is one thing that happened to a memory.
type HistoryEntry struct {
	At  time.Time `json:"at"`
	Seq uint32    `json:"seq"`

	// Kind is the durable transition name: recalled, promoted, expired,
	// archived, restored or hard_deleted. It is a string rather than an
	// enumeration because a rolling upgrade can produce one this binary has
	// never heard of, and rendering that as blank would hide a transition
	// rather than name one it cannot interpret.
	Kind string `json:"kind"`
	// Actor is what did it: ttl, active_forgetting, cleanup, api.
	Actor string `json:"actor"`
	// Reason is the sentence, written for a person reading one row.
	Reason string `json:"reason,omitempty"`

	Before map[string]string `json:"before,omitempty"`
	After  map[string]string `json:"after,omitempty"`
}

// HistoryResponse is a page of a memory's audit trail, newest first.
type HistoryResponse struct {
	Entries []HistoryEntry `json:"entries"`
	// NextCursor resumes below the last entry. Absent when the page reached the
	// end of the trail.
	NextCursor string `json:"next_cursor,omitempty"`
	HasMore    bool   `json:"has_more"`
}

func (d Deps) memoryHistory(w http.ResponseWriter, r *http.Request) {
	rid, ok := pathID(w, r)
	if !ok {
		return
	}

	limit, ok := intQuery(w, r, "limit")
	if !ok {
		return
	}
	req := memory.HistoryReq{ID: rid, Limit: limit, Cursor: r.URL.Query().Get("cursor")}

	got, err := d.Memories.History(r.Context(), req)
	if err != nil {
		WriteError(w, r, err)
		return
	}

	out := HistoryResponse{
		Entries:    make([]HistoryEntry, 0, len(got.Entries)),
		NextCursor: got.NextCursor,
		HasMore:    got.HasMore,
	}
	for _, e := range got.Entries {
		out.Entries = append(out.Entries, HistoryEntry{
			At: e.At, Seq: e.Seq, Kind: e.Kind, Actor: e.Actor, Reason: e.Reason,
			Before: e.Before, After: e.After,
		})
	}
	writeJSON(w, r, http.StatusOK, out)
}
