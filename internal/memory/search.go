package memory

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/query"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
)

// Search returns the memories matching a query, best first.
//
// # The three modes
//
// `semantic` embeds the query and ranks by meaning. `keyword` looks the query's
// terms up in the inverted index and runs no model at all — which is what makes
// it both the exact mode and the cheap one. `hybrid` does both and fuses them
// by rank, so a memory two independent indexes agree on outranks one either
// found alone.
//
// The mode is required. See [SearchType] for why there is no default.
//
// # How archived memories are excluded, and why that changed
//
// An archived memory keeps its canonical vector and its postings — archiving is
// a retirement, not a deletion, and re-embedding on un-archive would be work
// with no purpose — so both indexes still hold it and something has to exclude
// it.
//
// In Phase 3 that something was the record itself: the search asked the index
// for far more candidates than it needed and read each candidate's *record* to
// find out whether it was archived. The widening budget was the mechanism, and
// the cost was a protobuf decode per rejected candidate.
//
// It is now a predicate settled from the attribute row: tens of bytes, no
// decode, and the same answer. The widening is still there and still bounded at
// 32×, but it has become what it should always have been — an optimisation that
// keeps a filtered search from returning short, rather than the thing that
// makes filtering possible at all.
func (s *Service) Search(ctx context.Context, req SearchReq) (SearchResult, error) {
	return s.search(ctx, req, true)
}

// retainContinuation is false for Recall, whose bounded answer has no paging
// surface. Its snapshot must be released when this page finishes.
func (s *Service) search(ctx context.Context, req SearchReq, retainContinuation bool) (SearchResult, error) {
	const op = "memory.Search"

	t, err := s.requireTenant(ctx)
	if err != nil {
		return SearchResult{}, err
	}
	if !req.Type.Valid() {
		return SearchResult{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"a search must say how to look: %s. %q is not one of them",
			strings.Join(SearchTypes(), ", "), req.Type))
	}
	if strings.TrimSpace(req.Query) == "" && len(req.Tags) == 0 {
		return SearchResult{}, errs.E(errs.Invalid, op, fmt.Errorf("a search needs a query or a tag"))
	}
	if err := boundedQuery(op, "query", req.Query, req.Tags); err != nil {
		return SearchResult{}, err
	}
	if s.executor == nil {
		return SearchResult{}, errs.E(errs.Invalid, op,
			fmt.Errorf("this build has no attribute index, so a search cannot be filtered or answered"))
	}
	preds, err := req.preds()
	if err != nil {
		return SearchResult{}, errs.E(errs.Invalid, op, err)
	}
	if req.RelatedTo != nil && s.edges == nil {
		return SearchResult{}, errs.E(errs.Invalid, op,
			fmt.Errorf("this build has no graph, so related memories cannot be found"))
	}

	limit := req.Limit
	if limit <= 0 {
		limit = s.cfg.DefaultLimit
	}
	if limit > s.cfg.MaxLimit {
		return SearchResult{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"a search may ask for at most %d results, not %d", s.cfg.MaxLimit, limit))
	}
	// Timed from here: after every refusal, so a request turned away is not
	// measured as a search, and before the embedding, which is most of what a
	// semantic search costs.
	started := s.clk.Now()

	q := &query.Query{
		Tenant:  t,
		Text:    req.Query,
		Tags:    req.Tags,
		Limit:   limit,
		Explain: req.Explain,
	}
	if req.Type == SearchSemantic || req.Type == SearchHybrid {
		// The one place a search costs a model run. A keyword search skips it
		// entirely, which is most of why the mode is worth having.
		qs, err := s.embedder.Embed(ctx, []string{req.Query})
		if err != nil {
			return SearchResult{}, err
		}
		q.Vector = qs[0]
	}
	if req.Type == SearchKeyword || req.Type == SearchHybrid {
		q.Keyword = true
	}
	if !req.IncludeArchived {
		q.Preds = attr.Preds{attr.Eq(attr.SlotArchived, attr.Bool(false))}
	}
	q.Preds = append(q.Preds, preds...)
	q.RelatedTo = req.RelatedTo

	cursorSession, offset, err := decodeSearchCursor(req.Cursor, t, op)
	if err != nil {
		return SearchResult{}, err
	}
	if err := q.Validate(); err != nil {
		return SearchResult{}, err
	}
	binding := pagingBinding{tenant: t, fingerprint: s.searchFingerprint(q, req.IncludeArchived)}
	session, err := s.paging.acquireRanked(binding, cursorSession, op, "search")
	if err != nil {
		return SearchResult{}, err
	}
	finished := false
	defer func() { s.paging.release(session, finished) }()
	// One iterator over the tenant's keys answers this request's point reads,
	// every candidate's attribute row and every result's body, where a Get each
	// builds and tears down an iterator of its own. It is this request's alone:
	// continuations of one session read concurrently, and an iterator has one
	// position. Deferred after the release, so it closes before the snapshot.
	lower, upper := keys.TenantRange(t)
	reader := storage.NewSeekReader(session.snapshot, lower, upper)
	defer func() { _ = reader.Close() }()
	var examined int

	if cursorSession == nil {
		// Only the creator materialises a session. Its token is not exposed
		// until this completes; concurrent continuations only read the hits.
		finished = true
		if q.RelatedTo != nil {
			// Check the anchor in the same tenant and pinned snapshot as the
			// ranking. Continuations retain that answer even if the live anchor
			// is subsequently deleted. Archived anchors remain valid, as in Related.
			if _, err := s.repo.GetBodyFrom(ctx, reader, *q.RelatedTo); err != nil {
				return SearchResult{}, err
			}
		}
		deep := *q
		deep.Limit = max(q.Limit, s.cfg.MaxPageDepth)
		deep.Cursor = nil
		plan, perr := s.planner.Plan(&deep)
		if perr != nil {
			return SearchResult{}, perr
		}
		res, rerr := s.executor.Run(ctx, reader, &deep, plan)
		if rerr != nil {
			return SearchResult{}, rerr
		}
		session.ranked = res.Hits
		session.rankedTruncated = res.Truncated || len(res.Hits) >= deep.Limit
		examined = res.Examined
	}
	if cursorSession != nil && offset >= len(session.ranked) {
		return SearchResult{}, errs.E(errs.Invalid, op,
			fmt.Errorf("this page token holds a position beyond the ranking; start the search again"))
	}
	out := SearchResult{
		Results:   make([]Result, 0, min(limit, len(session.ranked)-offset)),
		Truncated: session.rankedTruncated,
	}

	// Missing bodies consume ranked positions, not page slots. Keep walking
	// until the requested page is full or the ranking is exhausted.
	end := offset
	for end < len(session.ranked) && len(out.Results) < limit {
		hit := session.ranked[end]
		end++
		rec, err := s.repo.GetBodyFrom(ctx, reader, hit.ID)
		if err != nil {
			if errs.Is(err, errs.NotFound) {
				// An index entry whose record is gone. It cannot be returned,
				// and it is not a reason to fail the search: the canonical
				// record is the authority on what exists, and the index catches
				// up.
				continue
			}
			return SearchResult{}, err
		}
		out.Results = append(out.Results, Result{
			Memory:     fromRecord(s.peek(ctx, rec)),
			Score:      hit.Score,
			FusedScore: hit.FusedScore,
			Distance:   hit.Distance,
			Sources:    sourcesOf(hit),
		})
	}
	out.HasMore = end < len(session.ranked)
	if out.HasMore && retainContinuation {
		out.NextCursor = encodeSearchCursor(t, session.id, end)
	}
	finished = out.NextCursor == ""
	if cursorSession == nil {
		// Only the request that materialised the ranking was a search; a
		// continuation slices what it computed.
		s.observeSearch(t, req.Type, started, examined, out.Truncated)
	}
	return out, nil
}

// preds turns filters into conditions on the sidecar row. They narrow every
// retrieval source without contributing a ranked list of their own. Naming a
// single slot also lets the planner narrow an ordered walk over that slot.
func (r SearchReq) preds() (attr.Preds, error) {
	var out attr.Preds
	if r.Policy != "" {
		out = append(out, attr.Eq(attr.SlotPolicy, attr.Str(r.Policy)))
	}
	if r.ImportanceMin != nil || r.ImportanceMax != nil {
		for _, bound := range []struct {
			name  string
			value *float32
		}{{"importance_min", r.ImportanceMin}, {"importance_max", r.ImportanceMax}} {
			if v := bound.value; v != nil && (math.IsNaN(float64(*v)) || math.IsInf(float64(*v), 0)) {
				return nil, fmt.Errorf("%s must be finite", bound.name)
			}
		}
		if r.ImportanceMin != nil && r.ImportanceMax != nil && *r.ImportanceMin > *r.ImportanceMax {
			return nil, fmt.Errorf("importance_min %v is above importance_max %v, so nothing can match",
				*r.ImportanceMin, *r.ImportanceMax)
		}
		out = append(out, attr.Range(attr.SlotImportance,
			valueOrNil(r.ImportanceMin, attr.F32), valueOrNil(r.ImportanceMax, attr.F32), true, true))
	}
	if r.CreatedAfter != nil || r.CreatedBefore != nil {
		if r.CreatedAfter != nil && r.CreatedBefore != nil && r.CreatedAfter.After(*r.CreatedBefore) {
			return nil, fmt.Errorf("created_after is later than created_before, so nothing can match")
		}
		out = append(out, attr.Range(attr.SlotCreatedAt,
			timeValueOrNil(r.CreatedAfter), timeValueOrNil(r.CreatedBefore), true, true))
	}
	return out, nil
}

func valueOrNil[T any](v *T, convert func(T) attr.Value) *attr.Value {
	if v == nil {
		return nil
	}
	value := convert(*v)
	return &value
}

func timeValueOrNil(t *time.Time) *attr.Value {
	return valueOrNil(t, func(v time.Time) attr.Value {
		// Match attr.Project: unset and pre-epoch timestamps map to zero.
		if v.IsZero() {
			return attr.U64(0)
		}
		return attr.U64(uint64(max(0, v.UnixMilli())))
	})
}

// searchFingerprint binds all query semantics and the materialised evidence,
// but deliberately excludes page size and position. Quoted strings have an
// unambiguous boundary even when user content contains NULs; float bits retain
// the exact embedding and graph threshold rather than a rounded display value.
func (s *Service) searchFingerprint(q *query.Query, includeArchived bool) [32]byte {
	h := sha256.New()
	fmt.Fprintf(h, "v1 tenant=%q namespace=%q text=%q keyword=%t explain=%t archived=%t order=%d desc=%t\n",
		q.Tenant, q.Namespace, q.Text, q.Keyword, q.Explain, includeArchived, q.OrderBy, q.Desc)
	for _, tag := range q.Tags {
		fmt.Fprintf(h, "tag=%q\n", tag)
	}
	for _, p := range q.Preds {
		fmt.Fprintf(h, "pred=%q\n", p.String())
	}
	fmt.Fprintf(h, "related=%v depth=%d direction=%q strength=%08x\n",
		q.RelatedTo, q.RelatedDepth, q.RelatedDirection, math.Float32bits(q.RelatedMinStrength))
	for _, typ := range q.RelatedTypes {
		fmt.Fprintf(h, "type=%q\n", typ)
	}
	fmt.Fprintf(h, "vector=%d\n", len(q.Vector))
	// One buffer rather than a binary.Write per component: the bytes are the
	// same, and binary.Write allocates on every call — 384 times a search.
	vec := make([]byte, 4*len(q.Vector))
	for i, v := range q.Vector {
		binary.LittleEndian.PutUint32(vec[4*i:], math.Float32bits(v))
	}
	_, _ = h.Write(vec)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func encodeSearchCursor(t tenant.ID, session [16]byte, offset int) string {
	p := append([]byte(nil), session[:]...)
	p = binary.AppendUvarint(p, uint64(offset))
	return codec.EncodeToken(codec.TokenSearch, t, p)
}

// decodeSearchCursor reads a ranked-session token. Search and related both mint
// one, so the caller supplies op and a refusal names the surface it came from.
func decodeSearchCursor(token string, t tenant.ID, op string) (*[16]byte, int, error) {
	if token == "" {
		return nil, 0, nil
	}
	p, err := codec.DecodeToken(token, codec.TokenSearch, t)
	if err != nil {
		return nil, 0, err
	}
	if len(p) < 17 {
		return nil, 0, errs.E(errs.Invalid, op, fmt.Errorf("this page token ends before its position"))
	}
	var session [16]byte
	copy(session[:], p[:16])
	offset, w := binary.Uvarint(p[16:])
	if w <= 0 || 16+w != len(p) || offset == 0 || offset > uint64(math.MaxInt) {
		return nil, 0, errs.E(errs.Invalid, op, fmt.Errorf("this page token holds a malformed position"))
	}
	return &session, int(offset), nil
}

// sourcesOf projects the query layer's per-index evidence for a caller.
//
// It is empty unless the query asked to explain, because the executor clears it
// there — which keeps "did the caller pay for this" a single decision rather
// than one taken again at every surface.
func sourcesOf(hit query.Hit) []SourceScore {
	if len(hit.Sources) == 0 {
		return nil
	}
	out := make([]SourceScore, 0, len(hit.Sources))
	for _, c := range hit.Sources {
		out = append(out, SourceScore{
			Source:   c.Source.String(),
			Score:    c.Score,
			Rank:     c.Rank,
			Distance: c.Distance,
		})
	}
	return out
}
