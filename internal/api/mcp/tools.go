package mcp

import "github.com/remem-org/remem-go/internal/memory"

// Tool names. Phase 3 ships the five that store, fetch, retire and find; the
// rest arrive with the subsystems they need.
//
// This file holds what a model reads — the names and the schemas. What each
// tool decodes and renders is in payloads.go, and the switch in dispatch.go.
const (
	ToolStoreMemory    = "store_memory"
	ToolStoreMemories  = "store_memories"
	ToolGetMemory      = "get_memory"
	ToolDeleteMemory   = "delete_memory"
	ToolSearchMemories = "search_memories"
	ToolListMemories   = "list_memories"
	// ToolFindRelated walks the relationship graph from one memory.
	ToolFindRelated = "find_related"
	// ToolRecall fills a context budget. It is the tool an agent reaches for
	// before a turn, where search is the one it reaches for on a question.
	ToolRecall = "recall"
	// ToolMemoryHistory is what happened to one memory.
	ToolMemoryHistory = "memory_history"
	// ToolUpdateMemory changes a stored memory. It is also how a memory's
	// policy is changed, which is why there is no promote_to_longterm: a
	// second tool onto one field is a second tool description in every
	// context window, and a policy field generalises to pinned and to any
	// policy a tenant defines in a way promote_to_longterm never could.
	ToolUpdateMemory = "update_memory"
)

func intPtr(n int) *int { return &n }

// Tools is the fixed tool set.
//
// Descriptions are written for a model, not for a human reading a reference:
// they say when to use the tool and what it costs, because that is what
// decides whether a model reaches for it at the right moment.
func Tools() []Tool {
	return []Tool{
		{
			Name: ToolStoreMemory,
			Description: "Store one memory. Use it for a fact worth recalling in a later " +
				"conversation, not for something already in the current context.",
			InputSchema: Schema{
				Type: "object",
				Properties: map[string]Property{
					"content": {Type: "string", Description: "The text to remember."},
					"tags": {Type: "array", Description: "Optional labels for later filtering.",
						Items: &Property{Type: "string"}},
					"source": {Type: "string", Description: "Optional note of where this came from."},
					"policy": {Type: "string", Description: "Retention rules: short_term (the " +
						"default, retired as its health decays), long_term (decays slowly), or " +
						"pinned (never decays, never expires)."},
					"importance": {Type: "number", Description: "How much this should weigh in " +
						"ranking, 0 to 1. Defaults to 0.5."},
					"arousal": {Type: "number", Description: "How emotionally charged this is, 0 " +
						"to 1. At 0.8 or above the memory becomes long-term whatever policy was " +
						"asked for, and is immune to decay for thirty days."},
					"emotional_valence": {Type: "number", Description: "How positive or negative " +
						"this is, -1 to 1."},
					"ttl_seconds": {Type: "integer", Description: "How long to keep it. Omit for " +
						"a memory that never expires on the clock."},
				},
				Required: []string{"content"},
			},
		},
		{
			Name: ToolStoreMemories,
			Description: "Store several memories at once. Prefer this over repeated " +
				"store_memory calls: the whole batch is one write and one embedding pass.",
			InputSchema: Schema{
				Type: "object",
				Properties: map[string]Property{
					"memories": {Type: "array", Description: "The memories to store.",
						Items: &Property{Type: "object", Description: "An object with content, and optionally tags and source."}},
				},
				Required: []string{"memories"},
			},
		},
		{
			Name:        ToolGetMemory,
			Description: "Fetch one memory by id.",
			InputSchema: Schema{
				Type: "object",
				Properties: map[string]Property{
					"id": {Type: "string", Description: "The memory's UUID."},
					"include_archived": {Type: "boolean", Default: false,
						Description: "Return the memory even if it has been archived."},
					"include_connections": {Type: "boolean", Default: false,
						Description: "Also return the relationships this memory starts, up to 200; " +
							"connections_has_more says when there were more. Costs a read per relationship; " +
							"leave it off unless you need the graph."},
				},
				Required: []string{"id"},
			},
		},
		{
			Name: ToolListMemories,
			Description: "List this tenant's memories in a chosen order, newest first by " +
				"default. Use it to see what is stored rather than to answer a question -- " +
				"search_memories is for questions. Page with cursor rather than asking for a " +
				"large limit.",
			InputSchema: Schema{
				Type: "object",
				Properties: map[string]Property{
					"order_by": {Type: "string", Enum: memory.Orderings,
						Description: "created_at (the default), updated_at, importance, health or last_recalled_at."},
					"desc":   {Type: "boolean", Default: true, Description: "Order from the high end down."},
					"limit":  {Type: "integer", Description: "How many memories to return.", Default: 10, Minimum: intPtr(1), Maximum: intPtr(200)},
					"cursor": {Type: "string", Description: "Resume after a previous page. Does not survive a server restart."},
					"include_archived": {Type: "boolean", Default: false,
						Description: "Also list memories that have been retired from retrieval."},
				},
			},
		},
		{
			Name: ToolUpdateMemory,
			Description: "Change a stored memory. Send only the fields you are changing; " +
				"anything you leave out stays as it was. Use it to correct a fact you got " +
				"wrong, or to change how long a memory is kept \u2014 set policy to long_term to " +
				"promote it. Changing the content re-reads the memory for connections to " +
				"others, so a rewritten memory finds the neighbours its new wording deserves.",
			InputSchema: Schema{
				Type: "object",
				Properties: map[string]Property{
					"id":      {Type: "string", Description: "The memory's UUID."},
					"content": {Type: "string", Description: "Replacement text. Omit to keep what is there."},
					"tags": {Type: "array", Description: "Replacement labels, all of them. " +
						"An empty list removes every tag; omit the field to leave them alone.",
						Items: &Property{Type: "string"}},
					"source": {Type: "string", Description: "Replacement note of where this came from."},
					"policy": {Type: "string", Description: "Change the retention rules: " +
						"short_term, long_term or pinned. This is how a memory is promoted."},
					"importance": {Type: "number", Description: "How much this should weigh in ranking, 0 to 1."},
					"arousal": {Type: "number", Description: "How emotionally charged this is, 0 to 1. " +
						"At 0.8 or above the memory becomes long-term and is immune to decay for thirty days."},
					"emotional_valence": {Type: "number", Description: "How positive or negative this is, -1 to 1."},
					"ttl_seconds":       {Type: "integer", Description: "How long to keep it from now."},
				},
				Required: []string{"id"},
			},
		},
		{
			Name: ToolDeleteMemory,
			Description: "Retire a memory. By default it is archived: it leaves search but " +
				"remains fetchable. Pass hard to remove it permanently.",
			InputSchema: Schema{
				Type: "object",
				Properties: map[string]Property{
					"id": {Type: "string", Description: "The memory's UUID."},
					"hard": {Type: "boolean", Default: false,
						Description: "Delete permanently instead of archiving. This cannot be undone."},
				},
				Required: []string{"id"},
			},
		},
		{
			Name: ToolSearchMemories,
			Description: "Find memories. Say how to look: semantic ranks by meaning, keyword " +
				"matches exact terms and runs no model, hybrid does both and is usually right. " +
				"Use keyword for an identifier, a code or a name; semantic for a description of " +
				"what you half-remember. Returns matches with a relevance score between 0 and 1.",
			InputSchema: Schema{
				Type: "object",
				Properties: map[string]Property{
					"query": {Type: "string", Description: "What to look for."},
					"search_type": {Type: "string", Enum: memory.SearchTypes(),
						Description: "semantic ranks by meaning; keyword matches exact terms and " +
							"cannot miss a spelling; hybrid fuses both. Required — there is no default, " +
							"because the three answer different questions."},
					"tags": {Type: "array", Description: "Only return memories carrying every one of these tags.",
						Items: &Property{Type: "string"}},
					"limit": {Type: "integer", Description: "How many results to return.",
						Default: 10, Minimum: intPtr(1), Maximum: intPtr(200)},
					"explain": {Type: "boolean", Default: false,
						Description: "Include the per-source score breakdown. Costs context; leave it off unless debugging ranking."},
					"include_archived": {Type: "boolean", Default: false,
						Description: "Also search memories that have been archived."},
					"policy": {Type: "string", Description: "Only return memories under this retention " +
						"policy: short_term, long_term, pinned, or one this tenant defines."},
					"importance_min": {Type: "number", Description: "Only return memories at least this " +
						"important, 0 to 1. Combines with every other filter."},
					"importance_max": {Type: "number", Description: "Only return memories at most this important, 0 to 1."},
					"created_after": {Type: "string", Description: "Only return memories written after this " +
						"RFC 3339 instant. Use it for \"what did I learn last week\"."},
					"created_before": {Type: "string", Description: "Only return memories written before this RFC 3339 instant."},
					"related_to": {Type: "string", Description: "A memory's UUID. Memories connected to it in " +
						"the graph are fused into the ranking alongside the content match, so \"like this one, " +
						"about that\" is one search."},
					"cursor": {Type: "string", Description: "Resume after a previous page: pass the next_cursor " +
						"a previous call returned. It is bound to this exact query and does not survive a " +
						"server restart; on refusal, search again from the first page."},
				},
				Required: []string{"query", "search_type"},
			},
		},
		{
			Name: ToolFindRelated,
			Description: "Find the memories connected to one memory, strongest connection " +
				"first. Strength is the product of the relationships along the path, so a " +
				"memory two hops away through two strong links outranks one directly attached " +
				"by a weak one. Use it when you have a memory and want its context; use " +
				"search_memories when you have a question. Read has_more: it is how you learn " +
				"there were more neighbours than fit.",
			InputSchema: Schema{
				Type: "object",
				Properties: map[string]Property{
					"id":    {Type: "string", Description: "The memory to start from."},
					"depth": {Type: "integer", Description: "How many relationships to follow outward.", Minimum: intPtr(1)},
					"types": {Type: "array", Description: "Only follow these relationship types.",
						Items: &Property{Type: "string"}},
					"direction": {Type: "string", Enum: []string{"out", "in", "both"},
						Description: "out follows this memory's own relationships; in finds what points at it; both does each."},
					"min_strength": {Type: "number", Description: "Ignore relationships weaker than this, 0 to 1."},
					"limit":        {Type: "integer", Description: "How many memories to return.", Default: 10, Minimum: intPtr(1), Maximum: intPtr(200)},
					"cursor":       {Type: "string", Description: "Resume after a previous page. Does not survive a server restart."},
					"include_archived": {Type: "boolean", Default: false,
						Description: "Also return memories that have been archived. The walk crosses them either way."},
				},
				Required: []string{"id"},
			},
		},
		{
			Name: ToolRecall,
			Description: "Fill a context budget with the memories most relevant to what you are " +
				"doing. Use this at the start of a turn, where search_memories is for a specific " +
				"question. Pass the ids you already have so they are not sent twice, and read " +
				"omitted_count: it is how you learn there was more that did not fit.",
			InputSchema: Schema{
				Type: "object",
				Properties: map[string]Property{
					"context": {Type: "string", Description: "What you are working on."},
					"search_type": {Type: "string", Enum: memory.SearchTypes(),
						Description: "semantic ranks by meaning; keyword matches exact terms; " +
							"hybrid fuses both and is usually right here. Required — there is no " +
							"default, for the same reason search_memories has none."},
					"token_budget": {Type: "integer", Minimum: intPtr(1),
						Description: "How much room you have, in tokens. Estimated at four bytes " +
							"to a token, so treat what comes back as approximate."},
					"already_have": {Type: "array", Description: "Memory ids already in your context.",
						Items: &Property{Type: "string"}},
					"tags": {Type: "array", Description: "Only consider memories carrying every one of these tags.",
						Items: &Property{Type: "string"}},
				},
				Required: []string{"context", "search_type", "token_budget"},
			},
		},
		{
			Name: ToolMemoryHistory,
			Description: "What happened to one memory and why: when it was recalled, promoted, " +
				"expired, archived or deleted, and what caused each. Newest first. It is the " +
				"answer to \"why did this memory change\", and it still answers for a memory that " +
				"has been deleted.",
			InputSchema: Schema{
				Type: "object",
				Properties: map[string]Property{
					"id":    {Type: "string", Description: "The memory to trace."},
					"limit": {Type: "integer", Description: "How many entries to return.", Default: 50, Minimum: intPtr(1), Maximum: intPtr(200)},
					"cursor": {Type: "string", Description: "Resume after a previous page. Pass the " +
						"next_cursor a previous call returned. A history cursor stays valid indefinitely."},
				},
				Required: []string{"id"},
			},
		},
	}
}
