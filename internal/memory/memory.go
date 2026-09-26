// Package memory is what a record means to a user.
//
// A record is a row; a memory is a thing an agent stored, can find again, and
// can retire. This package owns the difference: embedding on write, ranking on
// read, and archiving as a retirement that is not a deletion.
//
// # Archiving is not deletion
//
// Plan §II.10 row 8 turns Rust's opt-in hard delete into a rule: a heuristic
// must never destroy user data. An archived memory leaves search and stays
// fetchable, and only an explicit hard delete removes it. "Why did my memory
// disappear" has to have an answer other than "it did not".
package memory

import (
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/tenant"
)

// Memory is a stored memory.
type Memory struct {
	ID      id.ID
	Tenant  tenant.ID
	Content string
	Tags    []string
	Source  string

	Archived   bool
	ArchivedAt time.Time

	// The lifecycle group. It is on the surface from Phase 10 because a caller
	// cannot reason about retention it cannot see: "why did my memory
	// disappear" needs health and policy to be answerable before the memory is
	// gone, not after.
	//
	// AccessCount and LastRecalledAt are the folded values, which lag a recall
	// by at most one sweep interval. The event stream — GET /memories/{id}/history
	// — is immediate, and it is where a caller who needs the current answer
	// should look. Rust's search "peeks" at unflushed recalls so two endpoints
	// do not disagree; here nothing peeks, every surface reports the same
	// stored number, and the immediate answer has its own endpoint.
	Policy     string
	Importance float32
	Health     float32
	Valence    float32
	Arousal    float32

	TTL            time.Duration
	ProtectedUntil time.Time

	AccessCount    uint32
	AccessedAt     time.Time
	LastRecalledAt time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

// CreateReq asks for one memory to be stored.
type CreateReq struct {
	Content string
	Tags    []string
	Source  string

	// Policy names the retention rules. Empty takes the tenant's default.
	Policy string

	// Importance, Valence and Arousal are the caller's opinion of the memory.
	// They are pointers because zero is a value a caller may mean: an
	// importance of 0 is "ignore this in ranking", not "I did not say".
	Importance *float32
	Valence    *float32
	Arousal    *float32

	// TTL is how long the memory lives. Zero means the policy's default, which
	// for every built-in is "never expires".
	TTL time.Duration
}

// UpdateReq changes an existing memory.
//
// Every field is a pointer for the reason CreateReq gives for Importance: zero
// is a value a caller may mean. Nil means "leave this alone", which is a
// different instruction from "set this to its zero value", and a non-pointer
// field cannot say both.
//
// Tags is a pointer to a slice rather than a slice for the same reason taken
// one step further: nil leaves the tags, and an empty slice removes every one
// of them. With a plain []string the second is unsayable.
//
// There is no Health. Health is the lifecycle's own number, computed by decay
// from the record's state; a caller who writes 100 into it has cancelled the
// forgetting curve for that memory with nothing recording that they did. Rust
// accepts it on store and update, and that is a gap in Rust rather than a
// feature to port.
type UpdateReq struct {
	ID id.ID

	Content *string
	Tags    *[]string
	Source  *string

	Policy     *string
	Importance *float32
	Valence    *float32
	Arousal    *float32
	TTL        *time.Duration
}

// GetOpts varies a read.
type GetOpts struct {
	// IncludeArchived returns a memory that has been retired from retrieval.
	// It is off by default so the ordinary read path cannot accidentally
	// resurrect one, and on when a caller is auditing or exporting.
	IncludeArchived bool
}

// SearchType is how a search looks for memories.
//
// # There is no default, and that is deliberate
//
// Rust defaults to `semantic` over REST and `hybrid` over MCP — the same
// request answered differently depending on which door it came through. Go
// requires the field on every surface and refuses a search that omits it.
//
// The three modes answer materially different questions and the right one is
// the caller's to know. `keyword` does not run the embedding model at all;
// `semantic` cannot find an invoice number, because an embedding of an
// identifier is an embedding of nothing in particular; `hybrid` costs both and
// is usually what a person means. A default would make one of those the silent
// answer to "I did not think about it".
type SearchType string

const (
	// SearchSemantic ranks by meaning, over the vector index alone.
	SearchSemantic SearchType = "semantic"
	// SearchKeyword ranks by terms, over the inverted index alone. It runs no
	// model, so it is the cheap mode as well as the exact one.
	SearchKeyword SearchType = "keyword"
	// SearchHybrid runs both and fuses them by rank.
	SearchHybrid SearchType = "hybrid"
)

// Valid reports whether s names a search this build performs.
func (s SearchType) Valid() bool {
	switch s {
	case SearchSemantic, SearchKeyword, SearchHybrid:
		return true
	default:
		return false
	}
}

// SearchTypes are the modes, for an error message and for a schema.
func SearchTypes() []string {
	return []string{string(SearchSemantic), string(SearchKeyword), string(SearchHybrid)}
}

// SearchReq asks for the memories matching a query.
type SearchReq struct {
	Query string
	// Type is required. See [SearchType].
	Type SearchType
	// Tags every returned memory must carry, all of them. They narrow every
	// step rather than only the keyword one — a filter removes candidates.
	Tags  []string
	Limit int
	// Policy narrows to one retention policy, including tenant-defined ones.
	Policy string
	// ImportanceMin and ImportanceMax bound importance inclusively.
	ImportanceMin, ImportanceMax *float32
	// CreatedAfter and CreatedBefore bound creation time inclusively, at the
	// attribute row's millisecond precision.
	CreatedAfter, CreatedBefore *time.Time
	// RelatedTo adds a graph source to the content ranking. The anchor must
	// exist in the caller's tenant; its outgoing neighbours are fused in.
	RelatedTo *id.ID
	// Cursor continues a materialised ranking on its pinned snapshot.
	Cursor string
	// Explain asks for the per-source evidence behind each hit. It is off by
	// default because it is context a caller pays for on every search.
	Explain bool
	// IncludeArchived widens the search to retired memories. Off by default:
	// an archived memory is retired *from retrieval*, which is the whole point
	// of the state.
	IncludeArchived bool
}

// Result is one ranked memory.
type Result struct {
	Memory *Memory

	// Score is relevance in [0, 1], comparable across requests. It is
	// recovered cosine where the metric permits it, which is what makes a
	// threshold usable — see internal/vector/distance and the behaviour
	// baseline §1.3.
	Score float32

	// FusedScore is the value the ordering was decided by. A plain semantic
	// search has a single step and no rank fusion, so this is 1/(1+distance)
	// and sits on a completely different scale from a hybrid search's
	// (behaviour baseline §1.4). It is reported separately from Score rather
	// than instead of it, because a client that sorts by the wrong one gets a
	// different order than the server intended.
	FusedScore float32

	// Distance is the raw metric value, for debugging and for the differential
	// harness. It is not shown to users.
	Distance float32

	// Sources are the indexes that found this memory, best-scoring first.
	// Present only when the request asked to explain.
	Sources []SourceScore
}

// SourceScore is one index's evidence for one memory.
//
// The native score is kept rather than overwritten by the fused value, because
// the two answer different questions: the fused value says why this memory is
// at this position, and the native score says how well it actually matched.
type SourceScore struct {
	// Source is "vector", "text", "graph" or "attr".
	Source string
	// Score is that index's own relevance, on the 0..1 scale it reports.
	Score float32
	// Rank is the position this memory held in that index's list, zero-based.
	// It is what the fusion arithmetic is a function of.
	Rank int
	// Distance is the raw metric value for a vector contribution, zero
	// elsewhere.
	Distance float32
}

// SearchResult is one search's answer.
type SearchResult struct {
	Results []Result
	// HasMore and NextCursor describe remaining materialised hits. They are
	// independent of Truncated, which describes the ranking's completeness.
	HasMore    bool
	NextCursor string

	// Truncated reports that the search reached its materialisation depth or
	// exhausted its candidate budget, so there may be matches it did not see. It
	// is the difference between "these are all the matches" and "these are all
	// the matches I looked at" — a distinction a caller cannot recover alone.
	Truncated bool
}

// fromRecord projects a record into a memory.
func fromRecord(r *record.Record) *Memory {
	f := r.Fields.WithDefaults()
	return &Memory{
		ID:         r.ID,
		Tenant:     r.Tenant,
		Content:    r.Content,
		Tags:       append([]string(nil), f.Tags...),
		Source:     f.Source,
		Archived:   f.Archived,
		ArchivedAt: f.ArchivedAt,

		Policy:     f.Policy,
		Importance: f.Importance,
		Health:     f.Health,
		Valence:    f.Valence,
		Arousal:    f.Arousal,

		TTL:            f.TTL,
		ProtectedUntil: f.ProtectedUntil,

		AccessCount:    f.AccessCount,
		AccessedAt:     f.AccessedAt,
		LastRecalledAt: f.LastRecalledAt,

		CreatedAt: r.CreatedAt,
		UpdatedAt: r.UpdatedAt,
	}
}
