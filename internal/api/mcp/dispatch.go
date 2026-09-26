package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/memory"
)

// callTool runs one tool and renders its answer.
func (h *Handler) callTool(ctx context.Context, name string, raw json.RawMessage) (any, error) {
	switch name {
	case ToolStoreMemory:
		var a storeMemoryArgs
		if err := decodeArgs(raw, &a); err != nil {
			return nil, err
		}
		m, err := h.Memories.Create(ctx, a.toReq())
		if err != nil {
			return nil, err
		}
		return toPayload(m), nil

	case ToolStoreMemories:
		var a storeMemoriesArgs
		if err := decodeArgs(raw, &a); err != nil {
			return nil, err
		}
		reqs := make([]memory.CreateReq, len(a.Memories))
		for i, m := range a.Memories {
			reqs[i] = m.toReq()
		}
		created, err := h.Memories.CreateBatch(ctx, reqs)
		if err != nil {
			return nil, err
		}
		out := make([]memoryPayload, len(created))
		for i, m := range created {
			out[i] = toPayload(m)
		}
		return map[string]any{"memories": out}, nil

	case ToolGetMemory:
		var a getMemoryArgs
		if err := decodeArgs(raw, &a); err != nil {
			return nil, err
		}
		rid, err := id.Parse(a.ID)
		if err != nil {
			return nil, err
		}
		m, err := h.Memories.Get(ctx, rid, memory.GetOpts{IncludeArchived: a.IncludeArchived})
		if err != nil {
			return nil, err
		}
		if a.IncludeConnections {
			connections, more, err := h.inlineConnections(ctx, rid)
			if err != nil {
				return nil, err
			}
			return memoryWithConnections{toPayload(m), connections, more}, nil
		}
		return toPayload(m), nil

	case ToolListMemories:
		a := listArgs{Desc: true}
		if err := decodeArgs(raw, &a); err != nil {
			return nil, err
		}
		res, err := h.Memories.List(ctx, memory.ListReq{
			OrderBy: a.OrderBy, Desc: a.Desc, Limit: a.Limit, Cursor: a.Cursor,
			IncludeArchived: a.IncludeArchived,
		})
		if err != nil {
			return nil, err
		}
		out := listPayload{
			Memories: make([]memoryPayload, len(res.Memories)), NextCursor: res.NextCursor,
			HasMore: res.HasMore, Truncated: res.Truncated,
		}
		for i, m := range res.Memories {
			out.Memories[i] = toPayload(m)
		}
		return out, nil

	case ToolUpdateMemory:
		var a updateMemoryArgs
		if err := decodeArgs(raw, &a); err != nil {
			return nil, err
		}
		rid, err := id.Parse(a.ID)
		if err != nil {
			return nil, err
		}
		m, err := h.Memories.Update(ctx, a.toReq(rid))
		if err != nil {
			return nil, err
		}
		return toPayload(m), nil

	case ToolDeleteMemory:
		var a deleteMemoryArgs
		if err := decodeArgs(raw, &a); err != nil {
			return nil, err
		}
		rid, err := id.Parse(a.ID)
		if err != nil {
			return nil, err
		}
		if err := h.Memories.Delete(ctx, rid, a.Hard); err != nil {
			return nil, err
		}
		verb := "archived"
		if a.Hard {
			verb = "deleted"
		}
		return map[string]any{"id": a.ID, "status": verb}, nil

	case ToolSearchMemories:
		var a searchArgs
		if err := decodeArgs(raw, &a); err != nil {
			return nil, err
		}
		var relatedTo *id.ID
		if a.RelatedTo != "" {
			rid, err := id.Parse(a.RelatedTo)
			if err != nil {
				return nil, err
			}
			relatedTo = &rid
		}
		res, err := h.Memories.Search(ctx, memory.SearchReq{
			Query:         a.Query,
			Type:          memory.SearchType(a.SearchType),
			Tags:          a.Tags,
			Limit:         a.Limit,
			Policy:        a.Policy,
			ImportanceMin: a.ImportanceMin,
			ImportanceMax: a.ImportanceMax,
			CreatedAfter:  a.CreatedAfter,
			CreatedBefore: a.CreatedBefore,
			RelatedTo:     relatedTo,
			Cursor:        a.Cursor,
			// The breakdown is always computed, because `matched` needs it: a
			// model deciding whether to trust a hit is told which indexes found
			// it either way. Only the per-source numbers are gated on explain.
			Explain:         true,
			IncludeArchived: a.IncludeArchived,
		})
		if err != nil {
			return nil, err
		}
		out := searchPayload{Results: make([]searchHit, len(res.Results)), NextCursor: res.NextCursor, HasMore: res.HasMore, Truncated: res.Truncated}
		for i, r := range res.Results {
			hit := searchHit{
				Memory:  toPayload(r.Memory),
				Score:   r.Score,
				Matched: matchedBy(r.Sources),
			}
			if a.Explain {
				hit.Sources = make([]sourceScore, 0, len(r.Sources))
				for _, s := range r.Sources {
					hit.Sources = append(hit.Sources, sourceScore{
						Source:     s.Source,
						Score:      s.Score,
						Rank:       s.Rank,
						FusedScore: r.FusedScore,
						Distance:   s.Distance,
					})
				}
			}
			out.Results[i] = hit
		}
		return out, nil

	case ToolFindRelated:
		var a findRelatedArgs
		if err := decodeArgs(raw, &a); err != nil {
			return nil, err
		}
		rid, err := id.Parse(a.ID)
		if err != nil {
			return nil, err
		}
		res, err := h.Memories.Related(ctx, memory.RelatedReq{
			ID: rid, Depth: a.Depth, Types: a.Types, Direction: a.Direction,
			MinStrength: a.MinStrength, Limit: a.Limit, Cursor: a.Cursor,
			IncludeArchived: a.IncludeArchived,
		})
		if err != nil {
			return nil, err
		}
		out := relatedPayload{
			Memories: make([]relatedHit, len(res.Results)), NextCursor: res.NextCursor,
			HasMore: res.HasMore, Truncated: res.Truncated,
		}
		for i, hit := range res.Results {
			out.Memories[i] = relatedHit{Memory: toPayload(hit.Memory), Score: hit.Score}
		}
		return out, nil

	case ToolRecall:
		var a recallArgs
		if err := decodeArgs(raw, &a); err != nil {
			return nil, err
		}
		req := memory.RecallReq{
			Context:     a.Context,
			Type:        memory.SearchType(a.SearchType),
			TokenBudget: a.TokenBudget,
			Tags:        a.Tags,
		}
		for _, raw := range a.AlreadyHave {
			rid, err := id.Parse(raw)
			if err != nil {
				return nil, err
			}
			req.AlreadyHave = append(req.AlreadyHave, rid)
		}
		got, err := h.Memories.Recall(ctx, req)
		if err != nil {
			return nil, err
		}
		out := recallPayload{
			Memories:     make([]memoryPayload, 0, len(got.Memories)),
			UsedTokens:   got.UsedTokens,
			OmittedCount: got.OmittedCount,
			Truncated:    got.Truncated,
		}
		for _, m := range got.Memories {
			out.Memories = append(out.Memories, toPayload(m.Memory))
		}
		return out, nil

	case ToolMemoryHistory:
		var a historyArgs
		if err := decodeArgs(raw, &a); err != nil {
			return nil, err
		}
		rid, err := id.Parse(a.ID)
		if err != nil {
			return nil, err
		}
		entries, err := h.Memories.History(ctx, memory.HistoryReq{ID: rid, Limit: a.Limit, Cursor: a.Cursor})
		if err != nil {
			return nil, err
		}
		out := historyPayload{
			Entries:    make([]historyEntry, 0, len(entries.Entries)),
			NextCursor: entries.NextCursor,
			HasMore:    entries.HasMore,
		}
		for _, e := range entries.Entries {
			out.Entries = append(out.Entries, historyEntry{
				At: e.At, Kind: e.Kind, Actor: e.Actor, Reason: e.Reason,
			})
		}
		return out, nil

	default:
		return nil, fmt.Errorf("no tool named %q", name)
	}
}

// maxInlineConnections bounds what include_connections returns. Rust returns
// every outgoing connection; a memory can hold thousands, and each one is
// context the model pays for, so this stops at a stated bound and says so.
const maxInlineConnections = 200

// inlineConnections collects a memory's outgoing relationships for get_memory,
// following the connections cursor until the list ends or the bound is met.
//
// It used to take the service's first page and stop, so over the real binary a
// memory with thirty-one relationships reported ten and nothing said there
// were more -- found by the Task 4.2 end-to-end run. The flag is what makes a
// bound honest.
func (h *Handler) inlineConnections(ctx context.Context, rid id.ID) ([]connectionPayload, bool, error) {
	out := make([]connectionPayload, 0)
	cursor := ""
	for {
		page, err := h.Memories.Connections(ctx, memory.ConnectionsReq{ID: rid, Cursor: cursor})
		if err != nil {
			return nil, false, err
		}
		for _, c := range page.Connections {
			if len(out) == maxInlineConnections {
				return out, true, nil
			}
			out = append(out, toConnectionPayload(c))
		}
		if !page.HasMore {
			return out, false, nil
		}
		if page.NextCursor == "" {
			// More exists and nothing resumes into it: report it rather than
			// loop on the first page forever.
			return out, true, nil
		}
		cursor = page.NextCursor
	}
}

// decodeArgs reads a tool's arguments strictly.
//
// Unknown fields are refused for the same reason they are over REST: a model
// that guessed a field name and got a success has stored something other than
// what it meant to, and will not find out.
func decodeArgs(raw json.RawMessage, dst any) error {
	if len(raw) == 0 {
		raw = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytesReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		// Invalid, not an unclassified error. toolErrorText shows the model
		// the message only for the kinds it recognises and replaces the rest
		// with "the server could not complete this request" — so an
		// unclassified decode failure told a model that Remem was broken when
		// what had actually happened was that the model misspelled an
		// argument. That is the one reading that makes it stop trying rather
		// than fix its call, and it applied to every tool.
		return errs.E(errs.Invalid, "mcp.decodeArgs",
			fmt.Errorf("the tool arguments could not be read: %w", err))
	}
	return nil
}
