package http

import (
	"net/http"
	"strconv"

	"github.com/remem-org/remem-go/internal/memory"
)

// ListResponse is one page of a listing.
//
// It is an object rather than a bare array for the same reason every other
// list response here is: a bare array has nowhere to put the paging fields,
// and adding them later would be a breaking change to every client.
type ListResponse struct {
	Memories []MemoryResponse `json:"memories"`

	// NextCursor is the token that resumes after the last memory returned. It
	// is opaque: it carries no storage key, it is bound to the tenant and the
	// ordering that issued it, and a token from another tenant or another sort
	// is refused rather than resolved to something plausible.
	NextCursor string `json:"next_cursor,omitempty"`

	// HasMore says another page exists. It is always present, even when false:
	// a flag that appears only when set is a flag clients forget to check.
	HasMore bool `json:"has_more"`

	// Truncated says the search widened as far as it is allowed to and still
	// did not fill the page, so matching memories may exist that were never
	// looked at. It is never a stand-in for "there are none".
	Truncated bool `json:"truncated"`
}

// listMemories is GET, where search is POST.
//
// The asymmetry is deliberate and it is about what ends up in a log. A search
// query is user content and must not appear in a URL; a sort field, a page size
// and an opaque cursor are not.
//
// order_by defaults to created_at and the direction defaults to *descending*,
// because "my memories" means the newest first to everyone who has ever asked
// for it. That default lives here rather than in the service, which does
// exactly what it is told.
func (d Deps) listMemories(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	limit, ok := intQuery(w, r, "limit")
	if !ok {
		return
	}

	desc := true
	if q.Has("desc") {
		desc = boolQuery(r, "desc")
	}

	res, err := d.Memories.List(r.Context(), memory.ListReq{
		OrderBy:         q.Get("order_by"),
		Desc:            desc,
		Limit:           limit,
		Cursor:          q.Get("cursor"),
		IncludeArchived: boolQuery(r, "include_archived"),
	})
	if err != nil {
		WriteError(w, r, err)
		return
	}

	out := ListResponse{
		Memories:   make([]MemoryResponse, len(res.Memories)),
		NextCursor: res.NextCursor,
		HasMore:    res.HasMore,
		Truncated:  res.Truncated,
	}
	for i, m := range res.Memories {
		out.Memories[i] = toResponse(m)
	}
	writeJSON(w, r, http.StatusOK, out)
}

// intQuery reads a numeric parameter, refusing a malformed one rather than
// treating it as absent.
//
// `?limit=lots` silently becoming the default limit is the kind of leniency
// that has a client paging through a corpus ten at a time while its author is
// certain it asked for five hundred.
func intQuery(w http.ResponseWriter, r *http.Request, name string) (int, bool) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return 0, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		WriteMalformed(w, r, name+" must be a number")
		return 0, false
	}
	return n, true
}
