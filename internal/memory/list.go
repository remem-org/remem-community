package memory

import (
	"context"
	"fmt"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/query"
	"github.com/remem-org/remem-go/internal/storage"
)

// OrderCreatedAt is the ordering a listing takes when the caller names none.
//
// It is creation time descending — newest first — because that is what every
// caller means by "my memories" and because it is the one ordering that pages
// exactly: the slot is immutable, so records written during a listing sort at
// one end and cannot displace what has already been returned.
const OrderCreatedAt = "created_at"

// Orderings are the fields a listing may be ordered by, and they are a product
// decision rather than a storage one.
//
// Every one of them is an indexed slot, but not every indexed slot belongs
// here: next_attention_at is an access path too, and it is the lifecycle
// sweeps' scheduling index. Offering it would mean a caller could sort their
// memories by when a background job intends to look at them next, which is a
// number that means nothing to them and that Phase 10 is free to redefine.
var Orderings = []string{"created_at", "updated_at", "importance", "health", "last_recalled_at"}

func orderingIsOffered(name string) bool {
	for _, o := range Orderings {
		if o == name {
			return true
		}
	}
	return false
}

// ListReq asks for a page of memories.
type ListReq struct {
	// OrderBy names the field to order by. Empty means [OrderCreatedAt]. Only
	// indexed fields are accepted; anything else is refused by name, with the
	// available orderings listed, rather than answered by reading the whole
	// corpus and sorting it.
	OrderBy string
	// Desc orders from the high end down. It defaults to true for time
	// orderings at the API surface, not here: this layer does exactly what it
	// is told.
	Desc bool

	Limit int
	// Cursor resumes a previous page. It is the opaque token the previous page
	// returned, and it is refused if it was issued for another tenant, another
	// ordering, or an older token format.
	Cursor string

	// IncludeArchived widens the listing to retired memories. Off by default:
	// an archived memory is retired *from retrieval*, and a listing is
	// retrieval.
	IncludeArchived bool
}

// ListResult is one page of a listing.
type ListResult struct {
	Memories []*Memory

	// NextCursor resumes after the last memory returned. Empty when there is
	// nothing to resume into.
	NextCursor string
	// HasMore reports that the walk had not reached the end of its range.
	HasMore bool
	// Truncated reports that the widening budget was spent before the page was
	// filled, so matching memories may exist that were never looked at. It is
	// never a stand-in for "there are none".
	Truncated bool
}

// List returns a page of memories in a chosen order.
//
// # What it costs
//
// One attribute row read per candidate examined, and one record body read per
// memory returned. Listing ten memories out of a hundred thousand reads ten
// bodies, at every page including the last — that is the completion criterion
// of this phase and TestListReadsOnlyThePageItReturns measures it rather than
// asserting it.
//
// # What it guarantees across pages
//
// All pages share one bounded, server-owned snapshot. Every ordering is exact
// while its session lives; expired sessions must restart explicitly.
func (s *Service) List(ctx context.Context, req ListReq) (ListResult, error) {
	const op = "memory.List"

	t, err := s.requireTenant(ctx)
	if err != nil {
		return ListResult{}, err
	}
	if s.planner == nil {
		return ListResult{}, errs.E(errs.Invalid, op,
			fmt.Errorf("this build has no attribute index, so memories cannot be listed"))
	}

	limit := req.Limit
	if limit <= 0 {
		limit = s.cfg.DefaultLimit
	}
	if limit > s.cfg.MaxLimit {
		return ListResult{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"a listing may ask for at most %d memories, not %d", s.cfg.MaxLimit, limit))
	}

	orderBy := req.OrderBy
	if orderBy == "" {
		orderBy = OrderCreatedAt
	}
	if !orderingIsOffered(orderBy) {
		return ListResult{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"%q is not a field memories can be ordered by; the orderings are %v", orderBy, Orderings))
	}
	slot, err := s.planner.SlotByName(orderBy)
	if err != nil {
		return ListResult{}, err
	}

	cursor, err := query.DecodeCursor(req.Cursor, t)
	if err != nil {
		return ListResult{}, err
	}

	q := &query.Query{
		Tenant:  t,
		OrderBy: slot,
		Desc:    req.Desc,
		Limit:   limit,
		Cursor:  cursor,
	}
	if !req.IncludeArchived {
		// Pushed down to the row rather than applied after the read. An
		// archived memory keeps its vector and its row — archiving is a
		// retirement, not a deletion — so something has to exclude it, and the
		// row is where that costs a fixed few bytes instead of a record decode.
		q.Preds = attr.Preds{attr.Eq(attr.SlotArchived, attr.Bool(false))}
	}

	plan, err := s.planner.Plan(q)
	if err != nil {
		return ListResult{}, err
	}

	session, err := s.paging.acquire(pagingBinding{tenant: t, slot: slot, desc: req.Desc, archived: req.IncludeArchived}, cursor)
	if err != nil {
		return ListResult{}, err
	}
	finished := true
	defer func() { s.paging.release(session, finished) }()
	snap := session.snapshot

	res, err := s.executor.Run(ctx, snap, q, plan)
	if err != nil {
		return ListResult{}, err
	}

	out := ListResult{
		Memories:  make([]*Memory, 0, len(res.Hits)),
		HasMore:   res.HasMore,
		Truncated: res.Truncated,
	}
	if res.NextCursor != nil {
		res.NextCursor.Session = session.id
		out.NextCursor = res.NextCursor.Encode()
	}

	// The payload reads, and the only ones in this call. They go through the
	// same snapshot as the walk that chose them, so a memory the index selected
	// cannot have vanished by the time its body is read.
	bodies, err := s.readPage(ctx, snap, res.Hits)
	if err != nil {
		return ListResult{}, err
	}
	out.Memories = bodies

	// A page that returned nothing has nothing to resume from either: handing
	// back a cursor there would let a client page forever through emptiness.
	if len(out.Memories) == 0 && !out.Truncated {
		out.NextCursor = ""
		out.HasMore = false
	}
	finished = out.NextCursor == ""
	return out, nil
}

// readPage reads exactly the record bodies the page returns, in the page's
// order.
//
// A hit whose record has gone is skipped rather than failed. The index is
// derived and the record is the authority on what exists, so a body that is
// absent means the row has not caught up — which is a reason to omit one
// memory, not to fail the listing.
func (s *Service) readPage(ctx context.Context, snap storage.Snapshot, hits []query.Hit) ([]*Memory, error) {
	out := make([]*Memory, 0, len(hits))
	for _, hit := range hits {
		rec, err := s.repo.GetBodyFrom(ctx, snap, hit.ID)
		if errs.Is(err, errs.NotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, fromRecord(s.peek(ctx, rec)))
	}
	return out, nil
}
