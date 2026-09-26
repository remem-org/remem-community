package http

import (
	"net/http"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/memory"
)

// MemoryResponse is one memory as a client sees it.
//
// It is a separate type from memory.Memory rather than a set of json tags on
// it, and that is the whole reason it exists: the wire shape is a compatibility
// surface with clients, and the domain type is not. Adding a field to the
// domain must not silently add it to the API, and renaming one internally must
// not break every client.
type MemoryResponse struct {
	ID       string   `json:"id"`
	Content  string   `json:"content"`
	Tags     []string `json:"tags,omitempty"`
	Source   string   `json:"source,omitempty"`
	Archived bool     `json:"archived,omitempty"`

	// The lifecycle group. It is always present rather than omitted when zero:
	// a health of 0 is the most important number a memory can carry, and
	// `omitempty` would hide exactly the memory that is about to be retired.
	Policy     string  `json:"policy"`
	Importance float32 `json:"importance"`
	Health     float32 `json:"health"`
	Valence    float32 `json:"emotional_valence"`
	Arousal    float32 `json:"arousal"`

	// TTLSeconds is absent when the memory never expires, which is different
	// from expiring in zero seconds.
	TTLSeconds     *uint64    `json:"ttl_seconds,omitempty"`
	ProtectedUntil *time.Time `json:"protected_until,omitempty"`

	// AccessCount and LastRecalledAt are the folded values and lag a recall by
	// at most one sweep. GET /api/v1/memories/{id}/history is immediate.
	AccessCount    uint32     `json:"access_count"`
	AccessedAt     *time.Time `json:"accessed_at,omitempty"`
	LastRecalledAt *time.Time `json:"last_recalled_at,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func toResponse(m *memory.Memory) MemoryResponse {
	out := MemoryResponse{
		ID:       m.ID.String(),
		Content:  m.Content,
		Tags:     m.Tags,
		Source:   m.Source,
		Archived: m.Archived,

		Policy:     m.Policy,
		Importance: m.Importance,
		Health:     m.Health,
		Valence:    m.Valence,
		Arousal:    m.Arousal,

		AccessCount: m.AccessCount,

		CreatedAt: m.CreatedAt,
		UpdatedAt: m.UpdatedAt,
	}
	if m.TTL > 0 {
		secs := uint64(m.TTL / time.Second)
		out.TTLSeconds = &secs
	}
	out.ProtectedUntil = whenSet(m.ProtectedUntil)
	out.AccessedAt = whenSet(m.AccessedAt)
	out.LastRecalledAt = whenSet(m.LastRecalledAt)
	return out
}

// whenSet renders an unset time as absent rather than as the year 1.
func whenSet(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// CreateMemoryRequest is the body of a single store.
type CreateMemoryRequest struct {
	Content string   `json:"content"`
	Tags    []string `json:"tags,omitempty"`
	Source  string   `json:"source,omitempty"`

	// Policy names the retention rules: short_term, long_term or pinned.
	// Absent takes the default.
	Policy string `json:"policy,omitempty"`

	// Pointers, because zero is a value a caller may mean. An importance of 0
	// is "ignore this in ranking", not "I did not say", and a defaulting
	// decoder cannot tell the two apart.
	Importance *float32 `json:"importance,omitempty"`
	Valence    *float32 `json:"emotional_valence,omitempty"`
	// Arousal at or above 0.8 makes the memory long-term whatever policy was
	// asked for, and immune to decay for thirty days. The response reports the
	// policy it got, not the one it asked for.
	Arousal *float32 `json:"arousal,omitempty"`

	// TTLSeconds is how long the memory lives. Absent means it never expires,
	// which is what every built-in policy defaults to.
	TTLSeconds *uint64 `json:"ttl_seconds,omitempty"`
}

func (c CreateMemoryRequest) toReq() memory.CreateReq {
	req := memory.CreateReq{
		Content:    c.Content,
		Tags:       c.Tags,
		Source:     c.Source,
		Policy:     c.Policy,
		Importance: c.Importance,
		Valence:    c.Valence,
		Arousal:    c.Arousal,
	}
	if c.TTLSeconds != nil {
		req.TTL = time.Duration(*c.TTLSeconds) * time.Second
	}
	return req
}

// UpdateMemoryRequest is the body of a partial update.
//
// Every field is a pointer, and absent means "leave this alone" — which is a
// different instruction from "set it to zero" and the reason the route is
// PATCH rather than PUT. A non-pointer field cannot say both.
//
// Tags is a pointer to a slice: absent leaves the tags, and `"tags": []`
// removes every one of them. With a plain slice the second is unsayable.
//
// There is no health. Health is the lifecycle's own number, and decodeJSON
// refuses an unknown field by name rather than ignoring it: a client that wrote
// 100 into it and got a 200 would have cancelled the forgetting curve for that
// memory with nothing recording that it did.
type UpdateMemoryRequest struct {
	Content *string   `json:"content,omitempty"`
	Tags    *[]string `json:"tags,omitempty"`
	Source  *string   `json:"source,omitempty"`

	Policy     *string  `json:"policy,omitempty"`
	Importance *float32 `json:"importance,omitempty"`
	Valence    *float32 `json:"emotional_valence,omitempty"`
	Arousal    *float32 `json:"arousal,omitempty"`

	TTLSeconds *uint64 `json:"ttl_seconds,omitempty"`
}

func (u UpdateMemoryRequest) toReq(rid id.ID) memory.UpdateReq {
	req := memory.UpdateReq{
		ID: rid, Content: u.Content, Tags: u.Tags, Source: u.Source,
		Policy: u.Policy, Importance: u.Importance, Valence: u.Valence, Arousal: u.Arousal,
	}
	if u.TTLSeconds != nil {
		ttl := time.Duration(*u.TTLSeconds) * time.Second
		req.TTL = &ttl
	}
	return req
}

// CreateMemoriesRequest is the body of a batch store.
type CreateMemoriesRequest struct {
	Memories []CreateMemoryRequest `json:"memories"`
}

// CreateMemoriesResponse envelopes a batch result.
//
// Even a list response is an object, so that adding a field later — a count, a
// warning, a partial-failure report — is not a breaking change. A bare JSON
// array has nowhere to put one.
type CreateMemoriesResponse struct {
	Memories []MemoryResponse `json:"memories"`
}

func (d Deps) createMemory(w http.ResponseWriter, r *http.Request) {
	var body CreateMemoryRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	m, err := d.Memories.Create(r.Context(), body.toReq())
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusCreated, toResponse(m))
}

func (d Deps) createMemories(w http.ResponseWriter, r *http.Request) {
	var body CreateMemoriesRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	reqs := make([]memory.CreateReq, len(body.Memories))
	for i, m := range body.Memories {
		reqs[i] = m.toReq()
	}
	created, err := d.Memories.CreateBatch(r.Context(), reqs)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	out := CreateMemoriesResponse{Memories: make([]MemoryResponse, len(created))}
	for i, m := range created {
		out.Memories[i] = toResponse(m)
	}
	writeJSON(w, r, http.StatusCreated, out)
}

func (d Deps) getMemory(w http.ResponseWriter, r *http.Request) {
	rid, ok := pathID(w, r)
	if !ok {
		return
	}
	opts := memory.GetOpts{IncludeArchived: boolQuery(r, "include_archived")}
	m, err := d.Memories.Get(r.Context(), rid, opts)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, toResponse(m))
}

// updateMemory changes an existing memory.
//
// PATCH rather than PUT: the body is partial, and a PUT that dropped every
// unmentioned field would make "correct this one typo" a destructive
// operation. Appendix B classifies this route as a redesign rather than a
// preservation of Rust's PUT, and this is the redesign.
func (d Deps) updateMemory(w http.ResponseWriter, r *http.Request) {
	rid, ok := pathID(w, r)
	if !ok {
		return
	}
	var body UpdateMemoryRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	m, err := d.Memories.Update(r.Context(), body.toReq(rid))
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, toResponse(m))
}

// deleteMemory archives by default and removes only when asked.
//
// `?hard=true` rather than a separate endpoint, because the two are the same
// operation with different retention — and because an endpoint named /purge is
// one somebody eventually calls by mistake while exploring.
func (d Deps) deleteMemory(w http.ResponseWriter, r *http.Request) {
	rid, ok := pathID(w, r)
	if !ok {
		return
	}
	if err := d.Memories.Delete(r.Context(), rid, boolQuery(r, "hard")); err != nil {
		WriteError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// pathID parses the {id} path segment.
//
// A malformed id is 400, not 422: the request line itself is wrong, and there
// is no well-formed request here whose content the server is refusing.
func pathID(w http.ResponseWriter, r *http.Request) (id.ID, bool) {
	raw := r.PathValue("id")
	rid, err := id.Parse(raw)
	if err != nil {
		WriteMalformed(w, r, "the id in the path is not a UUID")
		return id.Zero, false
	}
	return rid, true
}

// boolQuery reads a flag, treating a bare `?hard` as true, which is what a
// person typing a URL expects.
func boolQuery(r *http.Request, name string) bool {
	if !r.URL.Query().Has(name) {
		return false
	}
	switch r.URL.Query().Get(name) {
	case "", "1", "true", "TRUE", "True", "yes":
		return true
	default:
		return false
	}
}
