package http

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/memory"
)

// ConnectionResponse is one relationship as a client sees it.
//
// It names both endpoints rather than only the far one. A connection read in
// the `in` direction has the anchor as its target, and a shape reporting only
// "the other memory" would leave a client unable to tell which way the
// relationship points — which for `caused_by` or `contradicts` is its entire
// meaning.
type ConnectionResponse struct {
	From             string            `json:"from"`
	To               string            `json:"to"`
	RelationshipType string            `json:"relationship_type"`
	Direction        string            `json:"direction"`
	Strength         float32           `json:"strength"`
	Meta             map[string]string `json:"meta,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func toConnectionResponse(c *memory.Connection) ConnectionResponse {
	return ConnectionResponse{
		From: c.From.String(), To: c.To.String(),
		RelationshipType: c.Type, Direction: c.Direction, Strength: c.Strength, Meta: c.Meta,
		CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt,
	}
}

// CreateConnectionRequest is the body of a connect.
type CreateConnectionRequest struct {
	TargetID string `json:"target_id"`
	// RelationshipType is one of the eight names. Empty means related_to, the
	// neutral kind, so a client that only means "these go together" need not
	// choose one.
	RelationshipType string            `json:"relationship_type,omitempty"`
	Strength         float32           `json:"strength"`
	Meta             map[string]string `json:"meta,omitempty"`
}

// UpdateConnectionRequest changes an existing relationship's strength.
type UpdateConnectionRequest struct {
	Strength float32 `json:"strength"`
}

// ConnectionsResponse is a list of relationships.
//
// An object rather than a bare array, for the reason every other list response
// here is one: an array has nowhere to put a field added later.
type ConnectionsResponse struct {
	Connections []ConnectionResponse `json:"connections"`
	NextCursor  string               `json:"next_cursor,omitempty"`
	HasMore     bool                 `json:"has_more"`
}

// RelatedResponse is the memories reachable from one memory, strongest
// connection first.
type RelatedResponse struct {
	Results    []RelatedHit `json:"results"`
	NextCursor string       `json:"next_cursor,omitempty"`

	// HasMore says the materialised traversal holds more memories than this
	// page. NextCursor resumes that ranking in its pinned snapshot, without
	// continuing the walk. It is always present, even when false.
	HasMore bool `json:"has_more"`

	// Truncated says the traversal's node budget or materialisation depth bound
	// was reached before completeness could be established. It is a different
	// fact from has_more, which is about the page.
	Truncated bool `json:"truncated"`
}

// RelatedHit is one related memory and how strongly it is connected.
type RelatedHit struct {
	Memory MemoryResponse `json:"memory"`
	// Score is the strength of the strongest chain of relationships reaching
	// this memory: the product of the edge strengths along it, in [0, 1]. It is
	// comparable across requests, which is what makes a threshold usable.
	Score float32 `json:"score"`
}

func (d Deps) createConnection(w http.ResponseWriter, r *http.Request) {
	from, ok := pathID(w, r)
	if !ok {
		return
	}
	var body CreateConnectionRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	to, err := id.Parse(body.TargetID)
	if err != nil {
		WriteMalformed(w, r, "target_id is not a UUID")
		return
	}

	conn, err := d.Memories.Relate(r.Context(), memory.RelateReq{
		From: from, To: to, Type: body.RelationshipType, Strength: body.Strength, Meta: body.Meta,
	})
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusCreated, toConnectionResponse(conn))
}

func (d Deps) listConnections(w http.ResponseWriter, r *http.Request) {
	rid, ok := pathID(w, r)
	if !ok {
		return
	}
	limit, ok := intQuery(w, r, "limit")
	if !ok {
		return
	}
	minStrength, ok := floatQuery(w, r, "min_strength")
	if !ok {
		return
	}

	conns, err := d.Memories.Connections(r.Context(), memory.ConnectionsReq{
		ID:          rid,
		Direction:   r.URL.Query().Get("direction"),
		Types:       csvQuery(r, "types"),
		MinStrength: minStrength,
		Limit:       limit,
		Cursor:      r.URL.Query().Get("cursor"),
	})
	if err != nil {
		WriteError(w, r, err)
		return
	}
	out := ConnectionsResponse{Connections: make([]ConnectionResponse, len(conns.Connections)), NextCursor: conns.NextCursor, HasMore: conns.HasMore}
	for i, c := range conns.Connections {
		out.Connections[i] = toConnectionResponse(c)
	}
	writeJSON(w, r, http.StatusOK, out)
}

// updateConnection changes one relationship's strength.
//
// `type` is required here where it is optional on delete. Two memories may be
// connected several ways, and "set the strength of the relationship between
// these two" has no answer when there are three of them — where "remove the
// relationships between these two" has an obvious one.
func (d Deps) updateConnection(w http.ResponseWriter, r *http.Request) {
	from, to, ok := pathPair(w, r)
	if !ok {
		return
	}
	typeName := r.URL.Query().Get("type")
	if strings.TrimSpace(typeName) == "" {
		WriteMalformed(w, r, "changing a connection's strength needs ?type=<relationship_type>: "+
			"two memories may be connected several ways, and only one of them is being changed")
		return
	}
	var body UpdateConnectionRequest
	if !decodeJSON(w, r, &body) {
		return
	}

	conn, err := d.Memories.Restrengthen(r.Context(), from, to, typeName, body.Strength)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusOK, toConnectionResponse(conn))
}

// deleteConnection removes relationships between two memories.
//
// Without `?type` it removes every relationship between the pair, because a
// client that says "these two are not related" usually means all of them.
func (d Deps) deleteConnection(w http.ResponseWriter, r *http.Request) {
	from, to, ok := pathPair(w, r)
	if !ok {
		return
	}
	if _, err := d.Memories.Unrelate(r.Context(), from, to, r.URL.Query().Get("type")); err != nil {
		WriteError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// relatedMemories is the traversal: memories connected to this one, ranked by
// the strength of the chain that reaches them rather than by how few hops away
// they are.
func (d Deps) relatedMemories(w http.ResponseWriter, r *http.Request) {
	rid, ok := pathID(w, r)
	if !ok {
		return
	}
	limit, ok := intQuery(w, r, "limit")
	if !ok {
		return
	}
	depth, ok := intQuery(w, r, "depth")
	if !ok {
		return
	}
	minStrength, ok := floatQuery(w, r, "min_strength")
	if !ok {
		return
	}

	res, err := d.Memories.Related(r.Context(), memory.RelatedReq{
		ID:              rid,
		Depth:           depth,
		Types:           csvQuery(r, "types"),
		Direction:       r.URL.Query().Get("direction"),
		MinStrength:     minStrength,
		Limit:           limit,
		Cursor:          r.URL.Query().Get("cursor"),
		IncludeArchived: boolQuery(r, "include_archived"),
	})
	if err != nil {
		WriteError(w, r, err)
		return
	}

	out := RelatedResponse{
		Results:    make([]RelatedHit, len(res.Results)),
		NextCursor: res.NextCursor,
		HasMore:    res.HasMore,
		Truncated:  res.Truncated,
	}
	for i, hit := range res.Results {
		out.Results[i] = RelatedHit{Memory: toResponse(hit.Memory), Score: hit.Score}
	}
	writeJSON(w, r, http.StatusOK, out)
}

// pathPair parses the {id} and {target} segments of a connection path.
func pathPair(w http.ResponseWriter, r *http.Request) (from, to id.ID, ok bool) {
	from, ok = pathID(w, r)
	if !ok {
		return id.Zero, id.Zero, false
	}
	to, err := id.Parse(r.PathValue("target"))
	if err != nil {
		WriteMalformed(w, r, "the target id in the path is not a UUID")
		return id.Zero, id.Zero, false
	}
	return from, to, true
}

// csvQuery reads a comma-separated parameter, dropping empty entries so that a
// trailing comma is not a request for a relationship type named "".
func csvQuery(r *http.Request, name string) []string {
	raw := r.URL.Query().Get(name)
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// floatQuery reads a numeric parameter, refusing a malformed one rather than
// treating it as absent — for the reason intQuery does.
func floatQuery(w http.ResponseWriter, r *http.Request, name string) (float32, bool) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return 0, true
	}
	v, err := strconv.ParseFloat(raw, 32)
	if err != nil {
		WriteMalformed(w, r, name+" must be a number")
		return 0, false
	}
	return float32(v), true
}
