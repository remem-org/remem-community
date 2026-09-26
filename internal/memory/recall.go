package memory

import (
	"context"
	"fmt"
	"strings"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
)

// TokenBytes is how many bytes of content one token is estimated to be.
//
// It is an estimate and is named so that nothing has to pretend otherwise. Four
// bytes per token is the usual rule of thumb for English text under a
// byte-pair tokenizer; CJK is denser and code is sparser, so a budget filled
// this way is approximate in both directions.
//
// Running the real tokenizer would be exact and would cost a model-adjacent
// call per candidate on a path whose entire purpose is to be cheap enough to
// call before every turn. The honest disposition is an estimate that is
// documented, not a precision nobody asked for.
const TokenBytes = 4

// ResultOverheadTokens is what one result costs beyond its content: the id, the
// tags, the framing an agent puts around it. It is charged so a budget of 100
// does not come back with fifty memories and no room to print them.
const ResultOverheadTokens = 16

// MaxRecallCandidates bounds how many memories the retrieval step considers
// before the budget is filled. It is a multiple of nothing in particular — the
// budget is in tokens and the retrieval is in results, so the two cannot be
// derived from each other — and it is what stops a large budget from turning
// one request into an unbounded search.
const MaxRecallCandidates = 200

// RecallReq asks for as much relevant memory as fits in a budget.
type RecallReq struct {
	// Context is what the agent is doing. It is embedded for the semantic step
	// and analysed for the keyword one, exactly as a search query is.
	Context string

	// Type is required, as it is on every other search surface. The three modes
	// answer materially different questions and the right one is the caller's
	// to know; a default here where Search refuses one would be the split
	// default Phase 8 removed — the same request answered differently
	// depending on which door it came through.
	Type SearchType

	// TokenBudget is how much room the caller has. It is required: a recall
	// with no budget is a search, and there is one of those already.
	TokenBudget int

	// AlreadyHave are memories the caller is holding. They never come back,
	// because spending a context window re-sending what is already in it is
	// the specific waste this endpoint exists to avoid.
	AlreadyHave []id.ID

	// Tags narrow the candidates, as they do everywhere else.
	Tags []string
}

// Recalled is what fits, and what did not.
type Recalled struct {
	// Memories are the chosen memories, most relevant first.
	Memories []Result

	// UsedTokens is the estimated cost of what came back.
	UsedTokens int
	// OmittedCount is how many relevant memories did not fit. It is the whole
	// point of the endpoint: an agent that cannot tell "there was nothing else"
	// from "there was more and it did not fit" will believe the first.
	OmittedCount int
	// Truncated reports that the search itself stopped early, so there may be
	// matches it never scored. It is a different fact from OmittedCount —
	// "I did not look" against "I looked and it did not fit".
	Truncated bool
}

// Recall fills a token budget with the memories most relevant to a context.
//
// # It records no recall
//
// The caller did not name these memories; the ranking chose them. That is the
// same line search sits on (behaviour baseline §3), and drawing it anywhere
// else would make `access_count` a measure of how often an agent asked a
// question rather than of how often a memory was used.
//
// # Greedy by relevance, and it does not reorder
//
// Candidates are taken best-first and each is kept if it fits. A memory that
// does not fit is skipped and counted, and a later smaller one may still be
// taken — which is what makes a budget usable rather than merely a cutoff. The
// order that comes back is relevance order, not packing order.
func (s *Service) Recall(ctx context.Context, req RecallReq) (Recalled, error) {
	const op = "memory.Recall"

	if !req.Type.Valid() {
		return Recalled{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"a recall must say how to look: %s. %q is not one of them",
			strings.Join(SearchTypes(), ", "), req.Type))
	}
	if req.TokenBudget <= 0 {
		return Recalled{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"a recall needs a token budget greater than zero; a recall without one is a search"))
	}
	if strings.TrimSpace(req.Context) == "" && len(req.Tags) == 0 {
		return Recalled{}, errs.E(errs.Invalid, op, fmt.Errorf(
			"a recall needs a context or a tag to choose memories by"))
	}
	if err := boundedQuery(op, "context", req.Context, req.Tags); err != nil {
		return Recalled{}, err
	}

	// Ask for more than the budget can plausibly hold, because the caller's
	// held ids and the per-result overhead both reduce what fits, and a page
	// sized to the budget alone would come back short for a reason the caller
	// could not see.
	limit := req.TokenBudget/(TokenBytes*8) + len(req.AlreadyHave) + 1
	if limit > MaxRecallCandidates {
		limit = MaxRecallCandidates
	}
	if limit > s.cfg.MaxLimit {
		limit = s.cfg.MaxLimit
	}

	found, err := s.search(ctx, SearchReq{
		Query: req.Context,
		Type:  req.Type,
		Tags:  req.Tags,
		Limit: limit,
	}, false)
	if err != nil {
		return Recalled{}, err
	}

	held := make(map[id.ID]bool, len(req.AlreadyHave))
	for _, rid := range req.AlreadyHave {
		held[rid] = true
	}

	out := Recalled{Truncated: found.Truncated, Memories: make([]Result, 0, len(found.Results))}
	for _, r := range found.Results {
		if held[r.Memory.ID] {
			continue
		}
		cost := TokensOf(r.Memory.Content)
		if out.UsedTokens+cost > req.TokenBudget {
			// It did not fit. A later, smaller memory still might, which is
			// what makes this a budget rather than a cutoff.
			out.OmittedCount++
			continue
		}
		out.UsedTokens += cost
		out.Memories = append(out.Memories, r)
	}
	return out, nil
}

// TokensOf estimates what one memory costs an agent's context window.
//
// Exported because the caller sizing a budget and the server filling it must
// agree about the unit, and two estimates of the same thing eventually
// disagree.
func TokensOf(content string) int {
	return (len(content)+TokenBytes-1)/TokenBytes + ResultOverheadTokens
}
