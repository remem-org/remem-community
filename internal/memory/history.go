package memory

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/tenant"
)

// MaxHistoryPage bounds one page of a memory's history.
const MaxHistoryPage = 200

// Entry is one thing that happened to a memory.
type Entry struct {
	At     time.Time
	Seq    uint32
	Kind   string
	Actor  string
	Reason string

	Before, After map[string]string
}

// HistoryReq asks for a memory's audit trail.
type HistoryReq struct {
	ID    id.ID
	Limit int
	// Cursor is an opaque, tenant-bound durable history position.
	Cursor string
	// Before pages: only entries strictly older than this position come back.
	// It remains available for in-package callers that already hold a position.
	Before *HistoryCursor
}

// HistoryCursor is where a page of history stopped.
type HistoryCursor struct {
	At  time.Time
	Seq uint32
}

// HistoryResult is one page of a memory's history.
type HistoryResult struct {
	Entries []Entry

	// NextCursor resumes after the last entry returned. It is empty when there
	// is no later page.
	NextCursor string
	// HasMore says another page exists.
	HasMore bool
}

// encode renders a history position as an opaque token.
//
// Fixed width, so there is nothing to frame: eight bytes of unix milliseconds
// and four of sequence. A history token names a durable position rather than a
// session, so unlike a search cursor it survives a restart and stays valid
// indefinitely.
func (c HistoryCursor) encode(t tenant.ID) string {
	p := make([]byte, 0, 12)
	p = binary.BigEndian.AppendUint64(p, uint64(c.At.UnixMilli()))
	p = binary.BigEndian.AppendUint32(p, c.Seq)
	return codec.EncodeToken(codec.TokenHistory, t, p)
}

func decodeHistoryCursor(token string, t tenant.ID) (*HistoryCursor, error) {
	const op = "memory.History"
	if token == "" {
		return nil, nil
	}
	p, err := codec.DecodeToken(token, codec.TokenHistory, t)
	if err != nil {
		return nil, err
	}
	if len(p) != 12 {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf(
			"this page token ends before its position"))
	}
	return &HistoryCursor{
		At:  time.UnixMilli(int64(binary.BigEndian.Uint64(p[:8]))).UTC(),
		Seq: binary.BigEndian.Uint32(p[8:12]),
	}, nil
}

// History returns what happened to one memory, newest first.
//
// # It is answered from the stream alone
//
// A memory that has been hard-deleted still has exactly one event — the row
// saying it was deleted — and that is the answer to "why did my memory
// disappear", so requiring the record to exist would refuse the one question
// this endpoint is best at. An id with no events at all is [errs.NotFound],
// which is the honest answer for a typo.
//
// # Newest first
//
// The question is nearly always "what just happened to this", and the stream
// has no end: a memory that keeps being recalled keeps growing, so oldest-first
// would page from a fixed point away from what the caller wants.
func (s *Service) History(ctx context.Context, req HistoryReq) (HistoryResult, error) {
	const op = "memory.History"

	t, err := s.requireTenant(ctx)
	if err != nil {
		return HistoryResult{}, err
	}
	if s.events == nil {
		return HistoryResult{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"this build does not record memory history"))
	}
	if req.ID.IsZero() {
		return HistoryResult{}, errs.E(errs.Invalid, op, fmt.Errorf("a history request needs a memory"))
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > MaxHistoryPage {
		return HistoryResult{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"a history page may hold at most %d entries, not %d", MaxHistoryPage, limit))
	}
	if req.Before == nil {
		req.Before, err = decodeHistoryCursor(req.Cursor, t)
		if err != nil {
			return HistoryResult{}, err
		}
	}

	q := events.Query{
		Tenant:    t,
		Namespace: tenant.DefaultNamespace,
		Subject:   req.ID,
		Limit:     limit + 1,
	}
	if req.Before != nil {
		q.Before = &events.Position{At: req.Before.At, Seq: req.Before.Seq}
	}

	got, err := s.events.History(ctx, s.kv, q)
	if err != nil {
		return HistoryResult{}, err
	}
	if len(got) == 0 && req.Before == nil && req.Cursor == "" {
		// An empty trail is ambiguous, and the ambiguity is the one Phase 6
		// found on the connections routes: a memory that exists and nothing has
		// happened to yet, and a memory that does not exist, must not answer
		// the same way — otherwise a typo reads as "nothing has happened".
		//
		// Storing a memory writes no event, so an empty trail is the ordinary
		// state of a memory nobody has recalled. The record settles it, at the
		// cost of one read on a page that returned nothing.
		if _, err := s.repo.Get(ctx, req.ID); err != nil {
			if errs.Is(err, errs.NotFound) {
				return HistoryResult{}, errs.E(errs.NotFound, op, fmt.Errorf("no memory %s", req.ID))
			}
			return HistoryResult{}, err
		}
		return HistoryResult{Entries: []Entry{}}, nil
	}

	out := HistoryResult{Entries: make([]Entry, 0, limit)}
	out.HasMore = len(got) > limit
	if out.HasMore {
		got = got[:limit]
	}
	for _, e := range got {
		out.Entries = append(out.Entries, Entry{
			At: e.At, Seq: e.Seq, Kind: string(e.Kind),
			Actor: e.Actor, Reason: e.Reason,
			Before: e.Before, After: e.After,
		})
	}
	if out.HasMore && len(out.Entries) > 0 {
		last := out.Entries[len(out.Entries)-1]
		out.NextCursor = HistoryCursor{At: last.At, Seq: last.Seq}.encode(t)
	}
	return out, nil
}
