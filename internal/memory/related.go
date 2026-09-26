package memory

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/query"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
)

// Connection is one relationship between two memories, as a user sees it.
//
// It names both endpoints rather than only the far one. A connection read in
// the `in` direction has the anchor as its *target*, and a shape that reported
// only "the other memory" would leave a caller unable to tell which way the
// relationship points — which for `caused_by` or `contradicts` is the whole
// meaning.
type Connection struct {
	From, To  id.ID
	Type      string
	Direction string
	Strength  float32

	CreatedAt, UpdatedAt time.Time
	Meta                 map[string]string
}

func toConnection(e graph.Edge) *Connection {
	return &Connection{
		From: e.From, To: e.To, Type: e.Type.String(), Direction: "out", Strength: e.Strength,
		CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt, Meta: e.Meta,
	}
}

// RelateReq asks for two memories to be connected.
type RelateReq struct {
	From, To id.ID
	// Type is the relationship's name. Empty means "related_to", the neutral
	// kind, so a caller who only means "these go together" need not choose one.
	Type string
	// Strength is the weight the relationship carries, 0..1. It is clamped
	// rather than refused, which is what Rust does and what an import needs.
	Strength float32
	Meta     map[string]string
}

// ConnectionsReq asks for the relationships attached to one memory.
type ConnectionsReq struct {
	ID id.ID
	// Direction is "out", "in" or "both". Empty means "out".
	Direction string
	// Types restricts the read to these relationship names. Empty means every
	// type.
	Types []string
	// MinStrength drops relationships weaker than this.
	MinStrength float32
	Limit       int
	// Cursor resumes strictly after the last relationship returned.
	Cursor string
}

// ConnectionsResult is one durable, tenant-bound page of connections.
type ConnectionsResult struct {
	Connections []*Connection
	NextCursor  string
	HasMore     bool
}

// RelatedReq asks for the memories connected to one memory.
type RelatedReq struct {
	ID    id.ID
	Depth int
	Types []string
	// Direction is "out", "in" or "both". Empty means "out".
	Direction   string
	MinStrength float32
	Limit       int
	// Cursor resumes a materialised traversal in its pinned snapshot.
	Cursor string
	// IncludeArchived widens the walk's results to retired memories. The
	// traversal still crosses them either way — an archived memory is a
	// legitimate stepping stone between two live ones — but by default it is
	// not itself returned.
	IncludeArchived bool
}

// RelatedResult is one page of related memories.
type RelatedResult struct {
	Results    []Result
	NextCursor string

	// HasMore reports that the traversal reached more memories than this page
	// holds, and NextCursor resumes into them.
	//
	// The cursor does not continue the walk — a live traversal has no stable
	// position to continue at, and inventing one would be a promise the walk
	// cannot keep across a write. It resumes a materialised ranking: the
	// traversal ran once over a pinned snapshot, the reached set was ranked by
	// path strength, and each page is a slice of that ranking. So the walk is
	// never resumed, and the promise is one the session can keep.
	//
	// HasMore exists because without it a caller who asked for five of
	// thirty-three related memories gets five, `truncated: false`, and no way
	// to tell that apart from a memory with exactly five neighbours. That
	// distinction was missing until the Phase 6 end-to-end verification found it.
	HasMore bool

	// Truncated reports that the traversal's node budget or materialisation
	// depth bound was reached before completeness could be established.
	// It is a different fact from HasMore,
	// which is about the page, and it is never a stand-in for "there are none".
	Truncated bool
}

// Relate connects two memories.
//
// Both endpoints are checked: a relationship to a memory that does not exist,
// or to one that has been retired, is refused rather than stored. That check is
// Rust's behaviour and it is worth keeping, because an edge is only useful if
// something is on the other end of it — and the graph has no way to notice
// later that there is not.
//
// The check costs two record reads on a write that is not on any hot path.
func (s *Service) Relate(ctx context.Context, req RelateReq) (*Connection, error) {
	const op = "memory.Relate"

	if _, err := s.requireTenant(ctx); err != nil {
		return nil, err
	}
	if s.edges == nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("this build has no graph, so memories cannot be connected"))
	}

	typ := graph.RelatedTo
	if name := strings.TrimSpace(req.Type); name != "" {
		parsed, err := graph.ParseRelationshipType(name)
		if err != nil {
			return nil, err
		}
		typ = parsed
	}

	for _, rid := range []id.ID{req.From, req.To} {
		if _, err := s.Get(ctx, rid, GetOpts{}); err != nil {
			return nil, err
		}
	}

	e := graph.Edge{From: req.From, To: req.To, Type: typ, Strength: req.Strength, Meta: req.Meta}
	tx := txn.New(s.kv, txn.Sync(s.cfg.SyncWrites))
	defer tx.Close()
	if err := s.edges.Add(ctx, tx, e); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	stored, err := s.edges.Get(ctx, req.From, req.To, typ)
	if err != nil {
		return nil, err
	}
	return toConnection(stored), nil
}

// Restrengthen changes an existing relationship's strength.
//
// It is separate from Relate rather than folded into it because the two mean
// different things to a caller: Relate states that a relationship exists, and
// this states that one already does and is worth more or less than it was. A
// relationship that is not there is errs.NotFound.
func (s *Service) Restrengthen(ctx context.Context, from, to id.ID, typeName string, strength float32) (*Connection, error) {
	const op = "memory.Restrengthen"

	if _, err := s.requireTenant(ctx); err != nil {
		return nil, err
	}
	if s.edges == nil {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("this build has no graph"))
	}
	typ, err := graph.ParseRelationshipType(strings.TrimSpace(typeName))
	if err != nil {
		return nil, err
	}

	tx := txn.New(s.kv, txn.Sync(s.cfg.SyncWrites))
	defer tx.Close()
	if err := s.edges.Update(ctx, tx, from, to, typ, strength); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	stored, err := s.edges.Get(ctx, from, to, typ)
	if err != nil {
		return nil, err
	}
	return toConnection(stored), nil
}

// Unrelate removes relationships between two memories.
//
// An empty typeName removes every relationship between the pair, because a
// caller who says "these two are not related" usually means all of them rather
// than one named kind. It reports how many were removed, and removing none is
// not an error: the post-condition holds.
func (s *Service) Unrelate(ctx context.Context, from, to id.ID, typeName string) (int, error) {
	const op = "memory.Unrelate"

	if _, err := s.requireTenant(ctx); err != nil {
		return 0, err
	}
	if s.edges == nil {
		return 0, errs.E(errs.Invalid, op, fmt.Errorf("this build has no graph"))
	}

	tx := txn.New(s.kv, txn.Sync(s.cfg.SyncWrites))
	defer tx.Close()

	removed := 1
	if name := strings.TrimSpace(typeName); name != "" {
		typ, err := graph.ParseRelationshipType(name)
		if err != nil {
			return 0, err
		}
		if err := s.edges.Remove(ctx, tx, from, to, typ); err != nil {
			return 0, err
		}
	} else {
		n, err := s.edges.RemoveAll(ctx, tx, from, to)
		if err != nil {
			return 0, err
		}
		removed = n
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return removed, nil
}

// Connections returns the relationships attached to one memory.
//
// It returns edges, not memories: "what is this connected to, and how" is a
// different question from "which memories are near this one", and answering it
// with record bodies would charge a payload read per relationship for
// information that is entirely in the edge.
func (s *Service) Connections(ctx context.Context, req ConnectionsReq) (ConnectionsResult, error) {
	const op = "memory.Connections"

	t, err := s.requireTenant(ctx)
	if err != nil {
		return ConnectionsResult{}, err
	}
	if s.edges == nil {
		return ConnectionsResult{}, errs.E(errs.Invalid, op, fmt.Errorf("this build has no graph"))
	}

	dir, err := direction(req.Direction)
	if err != nil {
		return ConnectionsResult{}, err
	}
	types, err := relationshipTypes(req.Types)
	if err != nil {
		return ConnectionsResult{}, err
	}
	limit := req.Limit
	if limit <= 0 {
		limit = s.cfg.DefaultLimit
	}
	if limit > s.cfg.MaxLimit {
		return ConnectionsResult{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"a connections query may ask for at most %d results, not %d", s.cfg.MaxLimit, limit))
	}
	cursor, err := decodeConnectionsCursor(req.Cursor, t)
	if err != nil {
		return ConnectionsResult{}, err
	}
	if cursor != nil && !cursor.rangeAllowed(dir) {
		return ConnectionsResult{}, errs.E(errs.Invalid, op, fmt.Errorf("this connections cursor belongs to a different direction"))
	}
	// The anchor must exist, for the reason Related checks it: an empty list is
	// what a memory with no relationships returns, and a caller who cannot tell
	// that from a mistyped id reads the typo as a memory with no connections.
	if _, err := s.Get(ctx, req.ID, GetOpts{IncludeArchived: true}); err != nil {
		return ConnectionsResult{}, err
	}

	read := func(r edgeRange, remaining int) ([]graph.Edge, error) {
		opts := graph.NeighbourOpts{Types: types, MinStrength: req.MinStrength, Limit: remaining + 1}
		if cursor != nil && cursor.Range == r {
			opts.After = &graph.Position{Rel: cursor.Rel, ID: cursor.ID}
		}
		if r == edgeIn {
			return s.edges.In(ctx, req.ID, opts)
		}
		return s.edges.Out(ctx, req.ID, opts)
	}

	var edges []graph.Edge
	var ranges []edgeRange
	appendPage := func(r edgeRange, remaining int) (bool, error) {
		got, err := read(r, remaining)
		if err != nil {
			return false, err
		}
		more := len(got) > remaining
		if more {
			got = got[:remaining]
		}
		for _, e := range got {
			edges, ranges = append(edges, e), append(ranges, r)
		}
		return more, nil
	}

	var hasMore bool
	switch dir {
	case graph.In:
		hasMore, err = appendPage(edgeIn, limit)
	case graph.Both:
		if cursor == nil || cursor.Range == edgeOut {
			hasMore, err = appendPage(edgeOut, limit)
			if err == nil && !hasMore {
				if len(edges) < limit {
					hasMore, err = appendPage(edgeIn, limit-len(edges))
				} else {
					// The out-range ended exactly at the page boundary. Probe the
					// next range so a non-empty in-range still has a next page.
					var incoming []graph.Edge
					incoming, err = read(edgeIn, 0)
					hasMore = len(incoming) > 0
				}
			}
		} else {
			hasMore, err = appendPage(edgeIn, limit)
		}
	default:
		hasMore, err = appendPage(edgeOut, limit)
	}
	if err != nil {
		return ConnectionsResult{}, err
	}

	out := ConnectionsResult{Connections: make([]*Connection, 0, len(edges)), HasMore: hasMore}
	for i, e := range edges {
		c := toConnection(e)
		if ranges[i] == edgeIn {
			c.Direction = "in"
		} else {
			c.Direction = "out"
		}
		out.Connections = append(out.Connections, c)
	}
	if out.HasMore && len(edges) > 0 {
		last := edges[len(edges)-1]
		out.NextCursor = connectionsCursor{Range: ranges[len(ranges)-1], Rel: last.Type, ID: farID(last, ranges[len(ranges)-1])}.encode(t)
	}
	return out, nil
}

type edgeRange byte

const (
	edgeOut edgeRange = iota
	edgeIn
)

type connectionsCursor struct {
	Range edgeRange
	Rel   graph.RelationshipType
	ID    id.ID
}

func (c connectionsCursor) encode(t tenant.ID) string {
	p := make([]byte, 0, 19)
	p = append(p, byte(c.Range))
	p = binary.BigEndian.AppendUint16(p, uint16(c.Rel))
	p = append(p, c.ID[:]...)
	return codec.EncodeToken(codec.TokenEdges, t, p)
}

func decodeConnectionsCursor(token string, t tenant.ID) (*connectionsCursor, error) {
	const op = "memory.Connections"
	if token == "" {
		return nil, nil
	}
	p, err := codec.DecodeToken(token, codec.TokenEdges, t)
	if err != nil {
		return nil, err
	}
	if len(p) != 19 || edgeRange(p[0]) > edgeIn {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("this page token does not contain a connections position"))
	}
	rel := graph.RelationshipType(binary.BigEndian.Uint16(p[1:3]))
	if !rel.Valid() {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("this page token contains an unknown relationship type"))
	}
	var rid id.ID
	copy(rid[:], p[3:])
	return &connectionsCursor{Range: edgeRange(p[0]), Rel: rel, ID: rid}, nil
}

func (c connectionsCursor) rangeAllowed(dir graph.Direction) bool {
	return dir == graph.Both || (dir == graph.Out && c.Range == edgeOut) || (dir == graph.In && c.Range == edgeIn)
}

func farID(e graph.Edge, r edgeRange) id.ID {
	if r == edgeIn {
		return e.From
	}
	return e.To
}

// Related returns the memories connected to one memory, strongest connection
// first.
//
// # What "strongest" means
//
// A path's strength is the product of the edge strengths along it, and each
// memory reports the strongest chain that reaches it. So a memory two hops away
// through two 0.9 relationships (0.81) outranks one directly attached by a 0.1
// relationship — which is the fix for REM-83, where Rust ranks by hop count and
// discards the weight its own edges carry.
//
// The score reported is that path strength, because a graph-only hit has no
// other evidence to take a relevance from. When a semantic query is combined
// with an anchor the two are fused and relevance comes from the content match
// instead: being one hop from a memory is context, not evidence that the
// content matches (see query/result.go).
func (s *Service) Related(ctx context.Context, req RelatedReq) (RelatedResult, error) {
	const op = "memory.Related"

	t, err := s.requireTenant(ctx)
	if err != nil {
		return RelatedResult{}, err
	}
	if s.executor == nil || s.edges == nil {
		return RelatedResult{}, errs.E(errs.Invalid, op,
			fmt.Errorf("this build has no graph, so related memories cannot be found"))
	}

	limit := req.Limit
	if limit <= 0 {
		limit = s.cfg.DefaultLimit
	}
	if limit > s.cfg.MaxLimit {
		return RelatedResult{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"a related-memory query may ask for at most %d results, not %d", s.cfg.MaxLimit, limit))
	}

	dir, err := direction(req.Direction)
	if err != nil {
		return RelatedResult{}, err
	}
	types, err := relationshipTypes(req.Types)
	if err != nil {
		return RelatedResult{}, err
	}

	anchor := req.ID
	q := &query.Query{
		Tenant:             t,
		RelatedTo:          &anchor,
		RelatedDepth:       req.Depth,
		RelatedTypes:       types,
		RelatedDirection:   dir,
		RelatedMinStrength: req.MinStrength,
		Limit:              limit,
	}
	if !req.IncludeArchived {
		q.Preds = attr.Preds{attr.Eq(attr.SlotArchived, attr.Bool(false))}
	}

	cursorSession, offset, err := decodeSearchCursor(req.Cursor, t, op)
	if err != nil {
		return RelatedResult{}, err
	}
	if err := q.Validate(); err != nil {
		return RelatedResult{}, err
	}
	binding := pagingBinding{tenant: t, fingerprint: s.searchFingerprint(q, req.IncludeArchived)}
	session, err := s.paging.acquireRanked(binding, cursorSession, op, "related query")
	if err != nil {
		return RelatedResult{}, err
	}
	finished := false
	defer func() { s.paging.release(session, finished) }()
	snap := session.snapshot

	if cursorSession == nil {
		// Only the creator runs the walk; its token is not published until
		// materialisation completes. Continuations read immutable ranked hits.
		finished = true
		// Check the anchor in the same snapshot as the walk and bodies. An
		// archived anchor is valid; a missing one is not an empty neighbourhood.
		if _, err := s.repo.GetBodyFrom(ctx, snap, anchor); err != nil {
			return RelatedResult{}, err
		}
		deep := *q
		deep.Limit = max(limit, s.cfg.MaxPageDepth)
		plan, err := s.planner.Plan(&deep)
		if err != nil {
			return RelatedResult{}, err
		}
		res, err := s.executor.Run(ctx, snap, &deep, plan)
		if err != nil {
			return RelatedResult{}, err
		}
		session.ranked = res.Hits
		session.rankedTruncated = res.Truncated || len(res.Hits) >= deep.Limit
	}
	if cursorSession != nil && offset >= len(session.ranked) {
		return RelatedResult{}, errs.E(errs.Invalid, op,
			fmt.Errorf("this page token holds a position beyond the ranking; start the related query again"))
	}
	out := RelatedResult{
		Results:   make([]Result, 0, min(limit, len(session.ranked)-offset)),
		Truncated: session.rankedTruncated,
	}
	// Consume ranked positions until the page is full or the ranking ends.
	// Missing bodies consume positions, not page slots; otherwise an orphan
	// can hide every valid neighbour ranked after it.
	end := offset
	for end < len(session.ranked) && len(out.Results) < limit {
		hit := session.ranked[end]
		end++
		rec, err := s.repo.GetBodyFrom(ctx, snap, hit.ID)
		if err != nil {
			if errs.Is(err, errs.NotFound) {
				// An edge pointing at a record that is gone. Traversal reads no
				// bodies by design, so this is where an orphan is dropped — and
				// `remem-admin graph verify` is where it is reported.
				continue
			}
			return RelatedResult{}, err
		}
		out.Results = append(out.Results, Result{
			Memory:     fromRecord(s.peek(ctx, rec)),
			Score:      hit.Score,
			FusedScore: hit.FusedScore,
		})
	}
	out.HasMore = end < len(session.ranked)
	if out.HasMore {
		out.NextCursor = encodeSearchCursor(t, session.id, end)
	}
	finished = out.NextCursor == ""
	return out, nil
}

// direction resolves a caller's direction name, defaulting to out.
func direction(name string) (graph.Direction, error) {
	if strings.TrimSpace(name) == "" {
		return graph.Out, nil
	}
	return graph.ParseDirection(name)
}

// relationshipTypes resolves a caller's relationship names, refusing any that
// this binary does not know rather than silently ignoring it — a filter that
// drops an unrecognised name returns the unfiltered neighbourhood and says
// nothing.
func relationshipTypes(names []string) ([]graph.RelationshipType, error) {
	if len(names) == 0 {
		return nil, nil
	}
	out := make([]graph.RelationshipType, 0, len(names))
	for _, name := range names {
		typ, err := graph.ParseRelationshipType(strings.TrimSpace(name))
		if err != nil {
			return nil, err
		}
		out = append(out, typ)
	}
	return out, nil
}
