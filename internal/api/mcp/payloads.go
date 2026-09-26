package mcp

import (
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/memory"
)

// The arguments each tool decodes and the payloads it renders. The schemas a
// model reads are in tools.go and the switch that joins the two in dispatch.go.

type storeMemoryArgs struct {
	Content string   `json:"content"`
	Tags    []string `json:"tags,omitempty"`
	Source  string   `json:"source,omitempty"`

	Policy string `json:"policy,omitempty"`
	// Pointers, because zero is a value a caller may mean: an importance of 0
	// is "ignore this in ranking", not "I did not say".
	Importance *float32 `json:"importance,omitempty"`
	Valence    *float32 `json:"emotional_valence,omitempty"`
	Arousal    *float32 `json:"arousal,omitempty"`
	TTLSeconds *uint64  `json:"ttl_seconds,omitempty"`
}

// toReq is the one place MCP's payload becomes a create request, so the two
// tools cannot drift into accepting different fields.
func (a storeMemoryArgs) toReq() memory.CreateReq {
	req := memory.CreateReq{
		Content: a.Content, Tags: a.Tags, Source: a.Source,
		Policy: a.Policy, Importance: a.Importance, Valence: a.Valence, Arousal: a.Arousal,
	}
	if a.TTLSeconds != nil {
		req.TTL = time.Duration(*a.TTLSeconds) * time.Second
	}
	return req
}

type updateMemoryArgs struct {
	ID      string    `json:"id"`
	Content *string   `json:"content,omitempty"`
	Tags    *[]string `json:"tags,omitempty"`
	Source  *string   `json:"source,omitempty"`

	// Pointers throughout, and Tags a pointer to a slice: absent means "leave
	// this alone" and an empty list means "remove every tag". A plain slice
	// cannot say both. See memory.UpdateReq.
	Policy     *string  `json:"policy,omitempty"`
	Importance *float32 `json:"importance,omitempty"`
	Valence    *float32 `json:"emotional_valence,omitempty"`
	Arousal    *float32 `json:"arousal,omitempty"`
	TTLSeconds *uint64  `json:"ttl_seconds,omitempty"`
}

// toReq is the one place MCP's payload becomes an update request, for the
// reason storeMemoryArgs.toReq gives: two surfaces must not drift into
// applying the same field differently.
func (a updateMemoryArgs) toReq(rid id.ID) memory.UpdateReq {
	req := memory.UpdateReq{
		ID: rid, Content: a.Content, Tags: a.Tags, Source: a.Source,
		Policy: a.Policy, Importance: a.Importance, Valence: a.Valence, Arousal: a.Arousal,
	}
	if a.TTLSeconds != nil {
		ttl := time.Duration(*a.TTLSeconds) * time.Second
		req.TTL = &ttl
	}
	return req
}

type recallArgs struct {
	Context     string   `json:"context"`
	SearchType  string   `json:"search_type"`
	TokenBudget int      `json:"token_budget"`
	AlreadyHave []string `json:"already_have,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

// recallPayload drops the per-hit scores that searchPayload carries.
//
// A recall is not a ranking a model acts on — it is context to read — and every
// field here is context the model pays for. The scores decided the order; they
// do not need to be shown to justify it.
type recallPayload struct {
	Memories     []memoryPayload `json:"memories"`
	UsedTokens   int             `json:"used_tokens"`
	OmittedCount int             `json:"omitted_count"`
	Truncated    bool            `json:"truncated"`
}

type historyArgs struct {
	ID     string `json:"id"`
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

type historyPayload struct {
	Entries    []historyEntry `json:"entries"`
	NextCursor string         `json:"next_cursor,omitempty"`
	HasMore    bool           `json:"has_more"`
}

// historyEntry drops the before/after maps the REST surface returns. A model
// reading a trail wants the sentence; a tool reconciling state wants the
// fields, and that tool is talking to the REST API.
type historyEntry struct {
	At     time.Time `json:"at"`
	Kind   string    `json:"kind"`
	Actor  string    `json:"actor"`
	Reason string    `json:"reason,omitempty"`
}

type storeMemoriesArgs struct {
	Memories []storeMemoryArgs `json:"memories"`
}

type getMemoryArgs struct {
	ID                 string `json:"id"`
	IncludeArchived    bool   `json:"include_archived,omitempty"`
	IncludeConnections bool   `json:"include_connections,omitempty"`
}

type listArgs struct {
	OrderBy         string `json:"order_by,omitempty"`
	Desc            bool   `json:"desc"`
	Limit           int    `json:"limit,omitempty"`
	Cursor          string `json:"cursor,omitempty"`
	IncludeArchived bool   `json:"include_archived,omitempty"`
}

type listPayload struct {
	Memories   []memoryPayload `json:"memories"`
	NextCursor string          `json:"next_cursor,omitempty"`
	HasMore    bool            `json:"has_more"`
	Truncated  bool            `json:"truncated"`
}

// connectionPayload is one relationship as a model sees it.
//
// A type of its own rather than memory.Connection, for the reason memoryPayload
// is smaller than the REST shape: the domain type is not a wire contract, and a
// field added to it must not silently join what every model pays to read.
// Outgoing only, as Rust's get_memory is, so `from` is always the memory asked
// about and there is no direction to report.
type connectionPayload struct {
	From     string  `json:"from"`
	To       string  `json:"to"`
	Type     string  `json:"relationship_type"`
	Strength float32 `json:"strength"`
}

func toConnectionPayload(c *memory.Connection) connectionPayload {
	return connectionPayload{From: c.From.String(), To: c.To.String(), Type: c.Type, Strength: c.Strength}
}

// memoryWithConnections is get_memory's answer when include_connections is set.
//
// connections_has_more is always present beside the list, for the reason
// has_more is on every paged response: a list cut at a bound is otherwise
// indistinguishable from a memory with exactly that many relationships.
type memoryWithConnections struct {
	memoryPayload
	Connections        []connectionPayload `json:"connections"`
	ConnectionsHasMore bool                `json:"connections_has_more"`
}

type deleteMemoryArgs struct {
	ID   string `json:"id"`
	Hard bool   `json:"hard,omitempty"`
}

type findRelatedArgs struct {
	ID              string   `json:"id"`
	Depth           int      `json:"depth,omitempty"`
	Types           []string `json:"types,omitempty"`
	Direction       string   `json:"direction,omitempty"`
	MinStrength     float32  `json:"min_strength,omitempty"`
	Limit           int      `json:"limit,omitempty"`
	Cursor          string   `json:"cursor,omitempty"`
	IncludeArchived bool     `json:"include_archived,omitempty"`
}

type searchArgs struct {
	Query           string     `json:"query"`
	SearchType      string     `json:"search_type"`
	Tags            []string   `json:"tags,omitempty"`
	Limit           int        `json:"limit,omitempty"`
	Explain         bool       `json:"explain,omitempty"`
	IncludeArchived bool       `json:"include_archived,omitempty"`
	Policy          string     `json:"policy,omitempty"`
	ImportanceMin   *float32   `json:"importance_min,omitempty"`
	ImportanceMax   *float32   `json:"importance_max,omitempty"`
	CreatedAfter    *time.Time `json:"created_after,omitempty"`
	CreatedBefore   *time.Time `json:"created_before,omitempty"`
	RelatedTo       string     `json:"related_to,omitempty"`
	Cursor          string     `json:"cursor,omitempty"`
}

// memoryPayload is a memory as a model sees it.
//
// It is deliberately smaller than the REST shape. Every field here is context a
// model pays for on every call, so what is not useful to it is left out —
// updated_at and the tenant among them.
type memoryPayload struct {
	ID        string   `json:"id"`
	Content   string   `json:"content"`
	Tags      []string `json:"tags,omitempty"`
	Source    string   `json:"source,omitempty"`
	Archived  bool     `json:"archived,omitempty"`
	CreatedAt string   `json:"created_at"`
}

func toPayload(m *memory.Memory) memoryPayload {
	return memoryPayload{
		ID:        m.ID.String(),
		Content:   m.Content,
		Tags:      m.Tags,
		Source:    m.Source,
		Archived:  m.Archived,
		CreatedAt: m.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
}

// searchHit is one result.
//
// `matched` is the compact list of indexes that contributed, and `sources` —
// the per-source score breakdown — is present only when explain is set. That
// is the Rust contract kept deliberately (behaviour baseline §7): every field
// here is context the model pays for on each search, so the default keeps the
// score it can act on and drops the evidence it cannot.
type searchHit struct {
	Memory  memoryPayload `json:"memory"`
	Score   float32       `json:"score"`
	Matched []string      `json:"matched,omitempty"`
	Sources []sourceScore `json:"sources,omitempty"`
}

type sourceScore struct {
	Source string  `json:"source"`
	Score  float32 `json:"score"`
	// Rank is where this index put the memory in its own list, zero-based. It
	// is what the fusion arithmetic is a function of, so it is the number that
	// explains the ordering.
	Rank int `json:"rank"`
	// FusedScore is the hit's overall fused value, repeated on every source
	// because a model reading one source needs to know what it added up to.
	FusedScore float32 `json:"fused_score"`
	Distance   float32 `json:"distance"`
}

// matchedBy is the compact list of indexes that found a memory.
//
// It is present on every hit, not only under explain: it costs a few bytes and
// it is what lets a model tell "this matched the words you asked for" from
// "this is merely similar in meaning" — the one distinction the three search
// types exist to give it.
func matchedBy(sources []memory.SourceScore) []string {
	if len(sources) == 0 {
		return nil
	}
	out := make([]string, 0, len(sources))
	seen := map[string]bool{}
	for _, s := range sources {
		if seen[s.Source] {
			continue
		}
		seen[s.Source] = true
		out = append(out, s.Source)
	}
	return out
}

type searchPayload struct {
	Results    []searchHit `json:"results"`
	NextCursor string      `json:"next_cursor,omitempty"`
	HasMore    bool        `json:"has_more"`
	Truncated  bool        `json:"truncated"`
}

type relatedHit struct {
	Memory memoryPayload `json:"memory"`
	Score  float32       `json:"score"`
}

type relatedPayload struct {
	Memories   []relatedHit `json:"memories"`
	NextCursor string       `json:"next_cursor,omitempty"`
	HasMore    bool         `json:"has_more"`
	Truncated  bool         `json:"truncated"`
}
