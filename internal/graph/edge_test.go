package graph_test

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
)

// The eight numbers are an address on disk: they sit in every edge key, and a
// renumbering would silently re-point every relationship a customer owns.
func TestRelationshipTypeNumbersAreDurable(t *testing.T) {
	want := map[graph.RelationshipType]string{
		1: "related_to",
		2: "caused_by",
		3: "part_of",
		4: "references",
		5: "contradicts",
		6: "supports",
		7: "similar_to",
		8: "derived_from",
	}
	for n, name := range want {
		if got := n.String(); got != name {
			t.Errorf("relationship type %d is %q, want %q", uint16(n), got, name)
		}
		parsed, err := graph.ParseRelationshipType(name)
		if err != nil {
			t.Errorf("parsing %q: %v", name, err)
			continue
		}
		if parsed != n {
			t.Errorf("%q parsed to %d, want %d", name, uint16(parsed), uint16(n))
		}
	}
	if len(graph.RelationshipTypes()) != len(want) {
		t.Errorf("there are %d relationship types, want %d", len(graph.RelationshipTypes()), len(want))
	}
}

func TestUnknownRelationshipTypeIsRefusedByName(t *testing.T) {
	_, err := graph.ParseRelationshipType("caused-by")
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("parsing an unknown type returned %v, want Invalid", err)
	}
	if got := err.Error(); !contains(got, "related_to") {
		t.Errorf("the refusal does not list the available types: %s", got)
	}
}

func TestRelationshipTypeRoundTripsThroughJSONByName(t *testing.T) {
	b, err := json.Marshal(graph.SimilarTo)
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	if string(b) != `"similar_to"` {
		t.Fatalf("marshalled to %s, want \"similar_to\"", b)
	}
	var back graph.RelationshipType
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshalling: %v", err)
	}
	if back != graph.SimilarTo {
		t.Fatalf("round-tripped to %d, want %d", uint16(back), uint16(graph.SimilarTo))
	}
}

func TestDirectionRoundTripsThroughJSONByName(t *testing.T) {
	for _, d := range []graph.Direction{graph.Out, graph.In, graph.Both} {
		b, err := json.Marshal(d)
		if err != nil {
			t.Fatalf("marshalling %v: %v", d, err)
		}
		var back graph.Direction
		if err := json.Unmarshal(b, &back); err != nil {
			t.Fatalf("unmarshalling %s: %v", b, err)
		}
		if back != d {
			t.Errorf("%s round-tripped to %v, want %v", b, back, d)
		}
	}
}

func TestSelfEdgeIsRejected(t *testing.T) {
	rid := id.New()
	e := graph.Edge{From: rid, To: rid, Type: graph.RelatedTo, Strength: 1}
	if err := e.Validate(); !errs.Is(err, errs.Invalid) {
		t.Fatalf("a self-edge validated with %v, want Invalid", err)
	}
}

func TestAnEdgeNeedsBothEndpointsAndAType(t *testing.T) {
	a, b := id.New(), id.New()
	cases := map[string]graph.Edge{
		"no from": {To: b, Type: graph.RelatedTo},
		"no to":   {From: a, Type: graph.RelatedTo},
		"no type": {From: a, To: b},
	}
	for name, e := range cases {
		if err := e.Validate(); !errs.Is(err, errs.Invalid) {
			t.Errorf("%s: validated with %v, want Invalid", name, err)
		}
	}
}

// A NaN strength would poison every path product it took part in, and the
// poisoning is silent: NaN compares false against every bound, so the node
// simply stops being ranked.
func TestNaNStrengthIsRejected(t *testing.T) {
	e := graph.Edge{From: id.New(), To: id.New(), Type: graph.RelatedTo,
		Strength: float32(math.NaN())}
	if err := e.Validate(); !errs.Is(err, errs.Invalid) {
		t.Fatalf("a NaN strength validated with %v, want Invalid", err)
	}
}

// Rust clamps rather than refusing, and an imported corpus must land unchanged.
func TestStrengthIsClampedToTheUnitInterval(t *testing.T) {
	for _, tc := range []struct{ in, want float32 }{{-1, 0}, {0, 0}, {0.5, 0.5}, {1, 1}, {7, 1}} {
		e := graph.Edge{From: id.New(), To: id.New(), Type: graph.RelatedTo, Strength: tc.in}
		if got := e.Normalise().Strength; got != tc.want {
			t.Errorf("strength %v normalised to %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestEdgeBodyRoundTrips(t *testing.T) {
	now := time.UnixMilli(1_725_000_000_000).UTC()
	e := graph.Edge{
		From: id.New(), To: id.New(), Type: graph.Supports, Strength: 0.75,
		CreatedAt: now, UpdatedAt: now.Add(time.Minute),
		Meta: map[string]string{"by": "discovery", "run": "17"},
	}
	b, err := graph.EncodeBody(e)
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	back, err := graph.DecodeBody(b, e.From, e.To, e.Type)
	if err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if back.Strength != e.Strength || !back.CreatedAt.Equal(e.CreatedAt) || !back.UpdatedAt.Equal(e.UpdatedAt) {
		t.Fatalf("round-tripped to %+v, want %+v", back, e)
	}
	if len(back.Meta) != 2 || back.Meta["by"] != "discovery" || back.Meta["run"] != "17" {
		t.Fatalf("metadata round-tripped to %v", back.Meta)
	}
}

// An edge's stored bytes must be a function of the edge and nothing else: two
// nodes handed the same edge write the same row (Invariants 8 and 9, Phase 14),
// and importing the same snapshot twice produces the same database rather than
// one that merely holds the same edges (Phase 12). Go's protobuf runtime
// randomises map field order unless it is told not to.
func TestEdgeBodyEncodingIsDeterministic(t *testing.T) {
	e := graph.Edge{
		From: id.New(), To: id.New(), Type: graph.RelatedTo, Strength: 0.5,
		CreatedAt: time.UnixMilli(1).UTC(),
		Meta: map[string]string{
			"a": "1", "b": "2", "c": "3", "d": "4", "e": "5",
			"f": "6", "g": "7", "h": "8", "i": "9", "j": "10",
		},
	}
	first, err := graph.EncodeBody(e)
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	for i := 0; i < 200; i++ {
		again, err := graph.EncodeBody(e)
		if err != nil {
			t.Fatalf("encoding: %v", err)
		}
		if string(again) != string(first) {
			t.Fatalf("encoding attempt %d differs from the first; the marshaller is not deterministic", i)
		}
	}
}

// A vector value read out of the edge space is a reader bug, not damage on the
// medium, and it must not decode as a plausible edge.
func TestAValueFromAnotherSpaceIsCorruption(t *testing.T) {
	body, err := codec.MarshalVector(vectorBody())
	if err != nil {
		t.Fatalf("building a vector value: %v", err)
	}
	_, err = graph.DecodeBody(body, id.New(), id.New(), graph.RelatedTo)
	if !errs.Is(err, errs.Corruption) {
		t.Fatalf("decoding a vector value as an edge returned %v, want Corruption", err)
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
