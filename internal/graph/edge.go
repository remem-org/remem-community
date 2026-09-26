// Package graph is Remem's relationship model: edges as canonical data, a
// derived reverse index, and traversal bounded in both depth and node count.
//
// # What is canonical and what is derived
//
// Out-edges are canonical. Nothing else in the system can reconstruct them, so a
// decode failure on one is [errs.Corruption] naming the key rather than a zero
// value quietly substituted (plan §II.4).
//
// In-edges are derived from out-edges. They are maintained inside the same
// transaction as their out-edge, so a reader never observes one without the
// other (spec §12), and they are *rebuilt* rather than repaired when they are
// wrong (Invariant 3). The derived row holds a verbatim copy of the canonical
// value, which is what makes "what points at this memory, and how strongly" one
// iterator instead of one read per neighbour — see [EncodeBody] for the price
// that copy charges.
//
// # Ranking
//
// A path's strength is the product of the edge strengths along it, and traversal
// reports the strongest path that reaches each node. Rust ranks by hop count
// alone and discards the weight its own edges carry, which is REM-83 and plan
// §II.10 row 7; see traverse.go for why the bound makes best-first the only
// honest way to spend it.
package graph

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/codec/pb"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/tenant"
)

// RelationshipType names a kind of relationship.
//
// The numbers are a durable format: two bytes of every edge key are this value,
// so a renumbering re-points every relationship a customer owns, silently and
// irreversibly. A number is never reused for a different meaning, and a retired
// type stays retired — its number is the thing a reader needs in order to skip
// past a relationship it has no name for.
//
// The eight names and their order are Rust Remem's, so an imported corpus needs
// no remapping (plan §Q15).
type RelationshipType keys.RelType

// The eight relationship types. Zero is unspecified and is refused.
const (
	RelatedTo   RelationshipType = 1
	CausedBy    RelationshipType = 2
	PartOf      RelationshipType = 3
	References  RelationshipType = 4
	Contradicts RelationshipType = 5
	Supports    RelationshipType = 6
	SimilarTo   RelationshipType = 7
	DerivedFrom RelationshipType = 8
)

// relationshipNames is the durable number-to-name table. Adding a row is adding
// a durable format; changing one is changing what a stored edge means.
var relationshipNames = map[RelationshipType]string{
	RelatedTo:   "related_to",
	CausedBy:    "caused_by",
	PartOf:      "part_of",
	References:  "references",
	Contradicts: "contradicts",
	Supports:    "supports",
	SimilarTo:   "similar_to",
	DerivedFrom: "derived_from",
}

// RelationshipTypes returns every type in number order. It is what an error
// message lists when a caller names one that does not exist, and what an API
// surface validates against.
func RelationshipTypes() []RelationshipType {
	return []RelationshipType{
		RelatedTo, CausedBy, PartOf, References, Contradicts, Supports, SimilarTo, DerivedFrom,
	}
}

// RelationshipNames returns every type's name in number order.
func RelationshipNames() []string {
	out := make([]string, 0, len(relationshipNames))
	for _, r := range RelationshipTypes() {
		out = append(out, relationshipNames[r])
	}
	return out
}

// String returns the stable snake_case name. The names travel over the API, in
// exports and in metric labels, so they do not change once shipped.
func (r RelationshipType) String() string {
	if n, ok := relationshipNames[r]; ok {
		return n
	}
	return fmt.Sprintf("relationship(%d)", uint16(r))
}

// Valid reports whether r is one this binary knows.
func (r RelationshipType) Valid() bool {
	_, ok := relationshipNames[r]
	return ok
}

// ParseRelationshipType resolves a caller's name.
//
// An unknown name is refused with the available ones listed, rather than
// defaulted to related_to: a caller who misspelled a type and got a success has
// stored a relationship other than the one they meant, and will not find out.
func ParseRelationshipType(name string) (RelationshipType, error) {
	for r, n := range relationshipNames {
		if n == name {
			return r, nil
		}
	}
	return 0, errs.E(errs.Invalid, "graph.ParseRelationshipType", fmt.Errorf(
		"%q is not a relationship type; the types are %s", name, strings.Join(RelationshipNames(), ", ")))
}

// MarshalJSON writes the name, not the number. A query travels as JSON between
// a planner and an executor (and, from Phase 14, between nodes), and a number
// there would be a durable format leaking into a wire format that nobody
// reading a request body could interpret.
func (r RelationshipType) MarshalJSON() ([]byte, error) {
	if !r.Valid() {
		return nil, errs.E(errs.Invalid, "graph.RelationshipType.MarshalJSON",
			fmt.Errorf("relationship type %d is not one this binary knows", uint16(r)))
	}
	return json.Marshal(r.String())
}

// UnmarshalJSON reads a name written by [RelationshipType.MarshalJSON].
func (r *RelationshipType) UnmarshalJSON(b []byte) error {
	var name string
	if err := json.Unmarshal(b, &name); err != nil {
		return errs.E(errs.Invalid, "graph.RelationshipType.UnmarshalJSON", err)
	}
	parsed, err := ParseRelationshipType(name)
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

// Direction is which way a neighbour read or a traversal follows edges.
type Direction uint8

const (
	// Out follows edges away from the anchor: "what does this point at".
	Out Direction = iota
	// In follows edges towards it: "what points at this".
	In
	// Both follows edges in either direction, treating the graph as undirected
	// for the purpose of the walk. The edges themselves stay directed.
	Both
)

var directionNames = [...]string{Out: "out", In: "in", Both: "both"}

// String returns the stable name used over the API and in explain output.
func (d Direction) String() string {
	if int(d) < len(directionNames) {
		return directionNames[d]
	}
	return fmt.Sprintf("direction(%d)", uint8(d))
}

// Valid reports whether d is one of the three directions.
func (d Direction) Valid() bool { return d <= Both }

// ParseDirection resolves a caller's direction name.
func ParseDirection(name string) (Direction, error) {
	for i, n := range directionNames {
		if n == name {
			return Direction(i), nil
		}
	}
	return 0, errs.E(errs.Invalid, "graph.ParseDirection", fmt.Errorf(
		"%q is not a direction; the directions are out, in, both", name))
}

// MarshalJSON writes the direction's name.
func (d Direction) MarshalJSON() ([]byte, error) {
	if !d.Valid() {
		return nil, errs.E(errs.Invalid, "graph.Direction.MarshalJSON",
			fmt.Errorf("direction %d is not one this binary knows", uint8(d)))
	}
	return json.Marshal(d.String())
}

// UnmarshalJSON reads a name written by [Direction.MarshalJSON].
func (d *Direction) UnmarshalJSON(b []byte) error {
	var name string
	if err := json.Unmarshal(b, &name); err != nil {
		return errs.E(errs.Invalid, "graph.Direction.UnmarshalJSON", err)
	}
	parsed, err := ParseDirection(name)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

// Scope is the tenant and namespace an edge operation runs in.
//
// It is a struct rather than two parameters because every function in this
// package takes both, and two adjacent strings are two things a call site can
// swap without the compiler noticing.
type Scope struct {
	Tenant    tenant.ID
	Namespace tenant.Namespace
}

// namespace defaults a scope that named none, rather than producing an
// unaddressable key: a zero namespace is a caller that has not been updated.
func (s Scope) namespace() tenant.Namespace {
	if s.Namespace == "" {
		return tenant.DefaultNamespace
	}
	return s.Namespace
}

// validate refuses a scope with no tenant. There is no unscoped edge read
// (Invariant 1).
func (s Scope) validate(op string) error {
	if s.Tenant == "" {
		return errs.E(errs.Invalid, op, errors.New(
			"an edge operation requires a tenant: there is no unscoped read path (Invariant 1)"))
	}
	return nil
}

// Edge is a relationship between two records.
//
// The triple (From, Type, To) is its identity: two records may be connected by
// several relationships, and each is a separate edge with its own strength.
// There is no edge id, because an edge with an identity of its own is a thing
// that can be pointed at, and nothing in Remem points at one.
type Edge struct {
	From, To id.ID
	Type     RelationshipType

	// Strength is the weight the relationship carries, 0..1. For an
	// auto-discovered edge it is the similarity that discovered it; for a
	// user-created one it is stated confidence.
	Strength float32

	CreatedAt, UpdatedAt time.Time

	// Meta is caller-supplied annotation. Remem does not index it, rank by it
	// or interpret it; it survives a round trip unchanged.
	Meta map[string]string
}

// Clone returns a deep copy, so a caller cannot mutate the metadata map another
// caller is reading.
func (e Edge) Clone() Edge {
	if e.Meta != nil {
		m := make(map[string]string, len(e.Meta))
		for k, v := range e.Meta {
			m[k] = v
		}
		e.Meta = m
	}
	return e
}

// Validate refuses an edge that cannot be stored.
//
// A self-edge is the one worth naming. It is not merely useless: every
// traversal would find the anchor among its own neighbours at every depth, and
// the `pathological` fixture contains them, so an import that accepted one would
// carry the defect forward into a corpus nobody chose to create.
func (e Edge) Validate() error {
	const op = "graph.Edge.Validate"

	bad := func(format string, args ...any) error {
		return errs.E(errs.Invalid, op, fmt.Errorf(format, args...))
	}
	switch {
	case e.From.IsZero():
		return bad("an edge must have a source")
	case e.To.IsZero():
		return bad("an edge must have a target")
	case e.From == e.To:
		return bad("a memory cannot be connected to itself")
	case !e.Type.Valid():
		return bad("%q is not a relationship type; the types are %s",
			e.Type, strings.Join(RelationshipNames(), ", "))
	case math.IsNaN(float64(e.Strength)):
		return bad("an edge strength must be a number: a NaN strength is excluded from every " +
			"bound it is compared against, so the relationship would silently stop being ranked")
	case math.IsInf(float64(e.Strength), 0):
		return bad("an edge strength must be finite")
	}
	return nil
}

// Normalise returns the edge as it will be stored: strength clamped into the
// unit interval.
//
// Clamping rather than refusing is Rust's behaviour, kept deliberately so an
// imported corpus lands unchanged rather than partly rejected. NaN is refused
// by [Edge.Validate] instead, because there is no value to clamp it to that is
// not a guess about what the caller meant.
func (e Edge) Normalise() Edge {
	switch {
	case e.Strength < 0:
		e.Strength = 0
	case e.Strength > 1:
		e.Strength = 1
	}
	return e
}

// EncodeBody frames an edge's value for storage.
//
// Only what the key does not hold is written: the endpoints and the relationship
// type are in the key, and a second copy in the value is a second thing that can
// disagree with the first.
//
// The encoding is deterministic — see codec.MarshalEdge — because the derived
// in-edge index stores this exact byte string and a rebuild has to reproduce it.
func EncodeBody(e Edge) ([]byte, error) {
	e = e.Normalise()
	return codec.MarshalEdge(&pb.Edge{
		Strength:        e.Strength,
		CreatedAtUnixMs: unixMs(e.CreatedAt),
		UpdatedAtUnixMs: unixMs(e.UpdatedAt),
		Meta:            e.Meta,
	})
}

// DecodeBody reads a stored edge value back into an edge, given the identity
// its key carried.
func DecodeBody(value []byte, from, to id.ID, typ RelationshipType) (Edge, error) {
	body, err := codec.UnmarshalEdge(value)
	if err != nil {
		return Edge{}, err
	}
	e := Edge{
		From: from, To: to, Type: typ,
		Strength:  body.GetStrength(),
		CreatedAt: unixMilli(body.GetCreatedAtUnixMs()),
		UpdatedAt: unixMilli(body.GetUpdatedAtUnixMs()),
	}
	if m := body.GetMeta(); len(m) > 0 {
		e.Meta = m
	}
	return e, nil
}

// unixMs encodes a time, with zero meaning unset rather than the epoch.
func unixMs(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixMilli())
}

func unixMilli(ms uint64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(int64(ms)).UTC()
}
