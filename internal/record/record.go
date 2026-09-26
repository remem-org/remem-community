// Package record is Remem's canonical record: the thing a memory is made of,
// and the only data in the system nothing can rebuild.
//
// Spec §8 describes fields as `map[string]Value`. This uses a typed struct with
// an extension map instead, for one reason: a map of Value for fields the code
// already knows about buys nothing and costs every read a type assertion, at
// every call site, forever. The extension map arrives with user-defined fields.
//
// # What is canonical here
//
// The record body and the canonical vector both are (plan §II.4). Neither is
// rebuildable from anything else, so a decode failure on either is
// errs.Corruption naming the key — never a zero value quietly substituted. Every
// index over them is derived and is rebuilt rather than repaired.
package record

import (
	"time"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/vector"
)

// Type distinguishes the kinds of record sharing the record key space. It is
// an alias for the key encoding's byte rather than a parallel enumeration,
// because two enumerations of the same thing eventually disagree.
type Type = keys.RecordType

// TypeMemory is a memory: the only record type the product has.
const TypeMemory = keys.RecordMemory

// VectorContent names the embedding of a record's content. It is the only
// vector key in Phase 3; the map exists because a record with an embedding of
// its summary, or of each of its chunks, is a change of content rather than of
// shape.
const VectorContent = "content"

// Vector is a canonical embedding. It is an alias, not a second type:
// internal/vector owns the vector key space, its framing and its reads, because
// there are two writers of that row — this repository, which stages a vector
// into the same transaction as its record, and a rebuild, which writes outside
// one — and two copies of a durable format are two things that can disagree.
type Vector = vector.Vector

// Default field values for a record nothing has expressed an opinion about.
//
// They match Rust Remem's, including health's 0..100 scale, so an imported
// corpus needs no rescaling and the differential harness compares like with
// like (docs/BEHAVIOUR_BASELINE.md §2).
const (
	DefaultImportance float32 = 0.5
	DefaultHealth     float32 = 100.0
	// DefaultPolicy is the retention policy a memory gets when the caller
	// names none. Rust defaults to MemoryType::ShortTerm and plan §II.6 maps
	// that enum onto named policies without changing behaviour.
	DefaultPolicy = "short_term"
)

// Fields are a record's indexed and user-visible fields.
//
// The lifecycle group — policy, importance, health, the affective pair and the
// recall telemetry — is written from Phase 5 rather than from Phase 10, which
// owns the behaviour that moves it. The reason is the attribute slot table:
// every declared slot is projected from one of these fields, and a slot that
// is declared and never populated is an ordering that silently returns
// nothing. Carrying the field without the behaviour costs nine values in a
// protobuf; declaring a slot without the field costs an empty index.
type Fields struct {
	// Tags are ordinary terms in the text index rather than a special case
	// (plan §II.10 row 2).
	Tags []string
	// Source is where the memory came from. Empty means unrecorded.
	Source string
	// Archived retires the record from retrieval without deleting it. The
	// record survives until cleanup: a heuristic must not destroy user data.
	Archived   bool
	ArchivedAt time.Time

	// Policy is the named retention policy (plan §II.6). Empty is read as
	// [DefaultPolicy] and is never stored.
	Policy string
	// Importance is the record's weight in ranking and decay, 0..1.
	Importance float32
	// Health is the decay budget, 0..100. Zero always archives.
	Health float32
	// Valence (-1..1) and Arousal (0..1) are the affective dimensions.
	Valence float32
	Arousal float32

	// AccessedAt and AccessCount are read telemetry, updated from Phase 10.
	AccessedAt  time.Time
	AccessCount uint32
	// LastRecalledAt is the last genuine recall, zero until one happens.
	LastRecalledAt time.Time
	// NextAttentionAt is when a lifecycle sweep next needs to look at this
	// record. Zero means "due now", which is the safe reading of a record
	// nothing has scheduled: treating it as "never" would drop the record out
	// of maintenance silently and permanently.
	//
	// It is derived, and [Repo.Put] computes it through a [Scheduler] rather
	// than trusting whatever the caller left here. See [WithScheduler].
	NextAttentionAt time.Time

	// TTL is how long this memory lives before it expires. Zero means it never
	// does — which is Rust's behaviour and not plan §II.6's, and it matters
	// because short_term is the default policy and nothing sets a TTL: a
	// default of one hour would archive every memory an hour after it was
	// written. What retires a short-term memory is health, at eight points a
	// day.
	TTL time.Duration

	// ProtectedUntil defers decay and forgetting wholesale while it is in the
	// future. It is Rust's flashbulb_until: a memory stored with arousal >= 0.8
	// is protected for thirty days.
	ProtectedUntil time.Time

	// LastDecayAt and LastHealthCheckAt are when each decay pass last ran.
	// Zero means never, and each clock then starts at CreatedAt. They are
	// separate because the two passes advance on different schedules, and
	// because active forgetting deliberately does not reset the decay clock.
	LastDecayAt       time.Time
	LastHealthCheckAt time.Time
}

// WithDefaults fills in the lifecycle values of a record that carries none.
//
// The signal is the policy, and nothing else. Every record written from Phase 5
// declares one and no record written before it does, so an empty policy means
// the whole group is unset — while a health of 0 in a record that *has* a
// policy is a real health of 0.
//
// That distinction is the reason this is not a field-by-field zero check.
// Health 0 always archives (plan §II.10 row 8), so reading a fully-decayed
// record's 0 as "unset, therefore 100" would silently resurrect it, and an
// importance of 0 is equally a value a caller may set on purpose. A
// zero-checking default is invisible, plausible, and wrong.
func (f Fields) WithDefaults() Fields {
	if f.Policy != "" {
		return f
	}
	f.Policy = DefaultPolicy
	f.Importance = DefaultImportance
	f.Health = DefaultHealth
	return f
}

// Record is a canonical record.
type Record struct {
	// originalBody is the opaque optimistic concurrency token from a read.
	originalBody []byte

	ID     id.ID
	Tenant tenant.ID
	// Namespace subdivides a tenant. It is fixed at tenant.DefaultNamespace
	// until a product decision introduces more; it is in the struct, and in
	// every key, because retrofitting it would mean rewriting every key every
	// customer owns (plan §II.5).
	Namespace tenant.Namespace
	Type      Type
	Content   string
	Fields    Fields

	// SchemaVersion is the tenant's user-schema version this record was
	// written against (spec §19.3, plan §II.5b). It is *not* a storage format
	// version, and the two move independently — a test asserts it.
	//
	// It is carried on the struct rather than stamped at encode time because a
	// record read at version 1 and written back must not silently claim to be
	// version 2, and because it is what [Upgrader] compares against to decide
	// whether a record needs upgrading on read.
	SchemaVersion uint32
	// Vectors are the record's canonical embeddings, keyed by what they embed.
	Vectors map[string]*Vector

	CreatedAt time.Time
	UpdatedAt time.Time
}

// namespace returns the record's namespace, defaulting rather than producing
// an unaddressable record: a zero namespace is a caller that has not been
// updated, not a request for a nameless one.
func (r *Record) namespace() tenant.Namespace {
	if r.Namespace == "" {
		return tenant.DefaultNamespace
	}
	return r.Namespace
}

// Clone returns a deep copy, so a caller cannot mutate what the repository
// holds or what another caller reads.
func (r *Record) Clone() *Record {
	if r == nil {
		return nil
	}
	out := *r
	out.Fields.Tags = append([]string(nil), r.Fields.Tags...)
	if r.Vectors != nil {
		out.Vectors = make(map[string]*Vector, len(r.Vectors))
		for k, v := range r.Vectors {
			out.Vectors[k] = v.Clone()
		}
	}
	return &out
}

// BodyKey is the storage key of a record body.
//
// It is exported so that remem-admin can name a row it is inspecting and so a
// test can damage exactly one. Nothing in the read path calls it from outside
// this package.
func BodyKey(t tenant.ID, ns tenant.Namespace, rid id.ID) []byte {
	return keys.Record(t, ns, TypeMemory, rid)
}

// VectorKey is the storage key of a record's canonical content vector.
func VectorKey(t tenant.ID, ns tenant.Namespace, rid id.ID) []byte {
	return vector.Key(t, ns, rid)
}
