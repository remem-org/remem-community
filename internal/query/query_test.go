package query_test

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/query"
	"github.com/remem-org/remem-go/internal/tenant"
)

// TestQueryRoundTripsThroughJSON is the shardability property: a query holding
// a closure or an index handle could not be planned on one node and executed on
// another, and building that in later means rebuilding the planner.
func TestQueryRoundTripsThroughJSON(t *testing.T) {
	lo, hi := attr.F32(0.25), attr.F32(0.75)
	when := attr.U64(1_756_000_000_000)

	want := query.Query{
		Tenant:    "acme",
		Namespace: tenant.DefaultNamespace,
		Text:      "the deployment runbook",
		Vector:    []float32{0.1, -0.2, 0.3},
		Preds: attr.Preds{
			attr.Eq(attr.SlotArchived, attr.Bool(false)),
			attr.Range(attr.SlotImportance, &lo, &hi, true, false),
			attr.And(
				attr.Range(attr.SlotCreatedAt, &when, nil, true, false),
				attr.Eq(attr.SlotPolicy, attr.Str("long_term")),
			),
			attr.Not(attr.Eq(attr.SlotAccessCount, attr.U32(0))),
		},
		OrderBy: attr.SlotUpdatedAt,
		Desc:    true,
		Limit:   25,
		Cursor: &query.Cursor{
			Tenant: "acme", Slot: attr.SlotUpdatedAt, Desc: true,
			Value: attr.U64(1_756_000_000_001).OrderBytes(), ID: id.New(), Session: [16]byte{1},
		},
		Explain: true,
	}

	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	var got query.Query
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unmarshalling: %v", err)
	}

	if got.Tenant != want.Tenant || got.Text != want.Text || got.OrderBy != want.OrderBy ||
		got.Desc != want.Desc || got.Limit != want.Limit || got.Explain != want.Explain {
		t.Errorf("the scalar fields changed:\n got %+v\nwant %+v", got, want)
	}
	if len(got.Vector) != len(want.Vector) {
		t.Fatalf("the vector came back with %d values, want %d", len(got.Vector), len(want.Vector))
	}
	for i := range want.Vector {
		if got.Vector[i] != want.Vector[i] {
			t.Errorf("vector[%d] = %v, want %v", i, got.Vector[i], want.Vector[i])
		}
	}
	if len(got.Preds) != len(want.Preds) {
		t.Fatalf("%d predicates survived, want %d", len(got.Preds), len(want.Preds))
	}
	for i := range want.Preds {
		if got.Preds[i].String() != want.Preds[i].String() {
			t.Errorf("predicate %d came back as %s, want %s", i, got.Preds[i], want.Preds[i])
		}
	}
	if got.Cursor == nil || got.Cursor.Encode() != want.Cursor.Encode() {
		t.Errorf("the cursor did not survive the round trip")
	}
}

func TestPredicatesSurviveJSONAndStillMatchTheSameRows(t *testing.T) {
	lo := attr.F32(0.5)
	original := attr.Preds{attr.Range(attr.SlotImportance, &lo, nil, true, false)}

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	var got attr.Preds
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unmarshalling: %v", err)
	}

	for _, tc := range []struct {
		importance float32
		want       bool
	}{{0.4, false}, {0.5, true}, {0.9, true}} {
		row := attr.NewRow(attr.SchemaVersion)
		row.Set(attr.SlotImportance, attr.F32(tc.importance))
		if attr.Match(row, got) != tc.want {
			t.Errorf("after a round trip, importance %v matched %t, want %t",
				tc.importance, !tc.want, tc.want)
		}
	}
}

func TestAValueReadAtAnUnknownTypeIsRefused(t *testing.T) {
	var v attr.Value
	err := json.Unmarshal([]byte(`{"t":"decimal","v":1}`), &v)
	if !errs.Is(err, errs.Invalid) {
		t.Errorf("an unknown value type gave %v, want errs.Invalid — a value read at the wrong "+
			"type is a filter that matches the wrong records", err)
	}
}

// TestCursorFromAnotherTenantIsRefused: a token minted in one tenant must
// resolve to nothing in another — not to the first tenant's rows, and not to
// the second's rows at the first's position.
func TestCursorFromAnotherTenantIsRefused(t *testing.T) {
	token := (&query.Cursor{
		Tenant: "acme", Slot: attr.SlotCreatedAt,
		Value: attr.U64(1).OrderBytes(), ID: id.New(), Session: [16]byte{1},
	}).Encode()

	if _, err := query.DecodeCursor(token, "acme"); err != nil {
		t.Fatalf("the issuing tenant could not use its own token: %v", err)
	}
	got, err := query.DecodeCursor(token, "globex")
	if !errs.Is(err, errs.Invalid) {
		t.Errorf("another tenant's token decoded to %+v with error %v, want errs.Invalid", got, err)
	}
}

func TestCursorVersionMismatchIsRefused(t *testing.T) {
	c := &query.Cursor{Tenant: "acme", Slot: attr.SlotCreatedAt,
		Value: attr.U64(1).OrderBytes(), ID: id.New()}
	raw := mustDecodeToken(t, c.Encode())
	raw[0] = codec.TokenVersion + 1

	_, err := query.DecodeCursor(encodeToken(raw), "acme")
	if !errs.Is(err, errs.Invalid) {
		t.Errorf("a token at an unknown version gave %v, want errs.Invalid: the fields after the "+
			"version byte are positional, so misreading one resolves to a real but wrong position", err)
	}
}

// A token is opaque and must stay that way: it carries no physical key, because
// a physical key holds the tenant prefix and could be edited to address another
// tenant's rows (plan §II.9).
func TestACursorCarriesNoPhysicalKey(t *testing.T) {
	rid := id.New()
	c := &query.Cursor{Tenant: "acme", Slot: attr.SlotImportance,
		Value: attr.F32(0.5).OrderBytes(), ID: rid, Session: [16]byte{1}}
	raw := mustDecodeToken(t, c.Encode())

	// The whole token is 1 (version) + 1 (kind) + (1+4) (tenant) + 2 (slot) +
	// 1 (desc) + (1+4) (value) + 16 (id) + 16 (session) bytes. A physical key
	// would be longer than the fields it is built from, and would repeat the
	// tenant inside a length-prefixed scope prefix followed by a space byte;
	// asserting the exact width is what catches one being smuggled in.
	if want := 1 + 1 + 1 + len("acme") + 2 + 1 + 1 + 4 + 16 + 16; len(raw) != want {
		t.Errorf("the token is %d bytes, want %d — something beyond the declared fields is in it",
			len(raw), want)
	}
}

func TestAMalformedTokenIsRefusedRatherThanGuessedAt(t *testing.T) {
	for name, token := range map[string]string{
		"not base64":   "!!!!",
		"version only": encodeToken([]byte{codec.TokenVersion}),
		"truncated":    encodeToken([]byte{codec.TokenVersion, uint8(codec.TokenListing), 4, 'a', 'c'}),
		"no position": encodeToken([]byte{
			codec.TokenVersion, uint8(codec.TokenListing), 4, 'a', 'c', 'm', 'e', 0, 3, 0, 0,
		}),
		"trailing bytes": encodeToken(append(mustDecodeToken(t, sampleToken()), 0xFF)),
	} {
		if _, err := query.DecodeCursor(token, "acme"); !errs.Is(err, errs.Invalid) {
			t.Errorf("%s: got %v, want errs.Invalid", name, err)
		}
	}
}

// A listing cursor is now framed by internal/codec, so a token minted for a
// different kind of walk must not resolve here even with the right tenant.
func TestAListingCursorRefusesASearchToken(t *testing.T) {
	tok := codec.EncodeToken(codec.TokenSearch, tenant.ID("acme"), []byte{1, 2, 3})

	if _, err := query.DecodeCursor(tok, tenant.ID("acme")); !errs.Is(err, errs.Invalid) {
		t.Fatalf("a search token decoded as a listing cursor: %v", err)
	}
}

func TestAnEmptyTokenIsTheStartOfAListing(t *testing.T) {
	c, err := query.DecodeCursor("", "acme")
	if err != nil || c != nil {
		t.Errorf("an empty token gave (%v, %v), want (nil, nil): no cursor is where a listing starts", c, err)
	}
}

// TestFusionMatchesTheRustRanking. RRF is 1/(k + rank + 1) summed over the
// lists a record appears in, k = 60, accumulated in float32 in list order, ties
// broken on the record id ascending — engine/query/merge.rs.
func TestFusionMatchesTheRustRanking(t *testing.T) {
	a := idFromByte(0x0a)
	b := idFromByte(0x0b)
	c := idFromByte(0x0c)

	got := query.Fuse([]query.RankedList{
		{Source: query.SourceVector, Items: []query.RankedItem{
			{ID: a, Score: 0.9}, {ID: b, Score: 0.8}, {ID: c, Score: 0.7},
		}},
		{Source: query.SourceText, Items: []query.RankedItem{
			{ID: c, Score: 0.6}, {ID: a, Score: 0.5},
		}},
	}, 10)

	// a: 1/61 + 1/62; c: 1/63 + 1/61; b: 1/62.
	wantOrder := []id.ID{a, c, b}
	if len(got) != len(wantOrder) {
		t.Fatalf("fused %d hits, want %d", len(got), len(wantOrder))
	}
	for i, want := range wantOrder {
		if got[i].ID != want {
			t.Fatalf("position %d is %s, want %s", i, got[i].ID, want)
		}
	}

	want := float32(1.0/61.0) + float32(1.0/62.0)
	if diff := math.Abs(float64(got[0].FusedScore - want)); diff > 1e-9 {
		t.Errorf("the top hit fused to %v, want %v (difference %v): the arithmetic must be "+
			"float32 accumulated in list order, as Rust's is", got[0].FusedScore, want, diff)
	}
}

func TestFusionBreaksTiesDeterministically(t *testing.T) {
	low, high := idFromByte(0x01), idFromByte(0x02)

	for i := 0; i < 32; i++ {
		got := query.Fuse([]query.RankedList{
			{Source: query.SourceVector, Items: []query.RankedItem{{ID: high, Score: 0.5}}},
			{Source: query.SourceText, Items: []query.RankedItem{{ID: low, Score: 0.5}}},
		}, 1)
		if len(got) != 1 || got[0].ID != low {
			t.Fatalf("run %d truncated to a different record; without a deterministic tiebreak "+
				"identical queries return different result sets, not merely different orders", i)
		}
	}
}

// TestScoreIgnoresGraphSourcesUnlessAlone. Being one hop from an anchor memory
// is context, not evidence that the content matches — letting it set relevance
// reports 0.5 for an unrelated neighbour, and 0.5 is the number a caller puts a
// threshold against.
func TestScoreIgnoresGraphSourcesUnlessAlone(t *testing.T) {
	mixed := idFromByte(0x01)
	graphOnly := idFromByte(0x02)

	got := query.Fuse([]query.RankedList{
		{Source: query.SourceVector, Items: []query.RankedItem{{ID: mixed, Score: 0.42}}},
		{Source: query.SourceGraph, Items: []query.RankedItem{
			{ID: mixed, Score: 0.99}, {ID: graphOnly, Score: 0.5},
		}},
	}, 10)

	byID := map[id.ID]query.Hit{}
	for _, h := range got {
		byID[h.ID] = h
	}
	if s := byID[mixed].Score; s != 0.42 {
		t.Errorf("a hit with both a vector and a graph source scored %v, want the vector's 0.42", s)
	}
	if s := byID[graphOnly].Score; s != 0.5 {
		t.Errorf("a graph-only hit scored %v, want its graph score 0.5", s)
	}
}

func TestScoreIsClampedToTheUnitInterval(t *testing.T) {
	got := query.Fuse([]query.RankedList{
		{Source: query.SourceVector, Items: []query.RankedItem{{ID: idFromByte(1), Score: 1.7}}},
	}, 1)
	if got[0].Score != 1 {
		t.Errorf("a score of 1.7 came back as %v, want 1: relevance is comparable across "+
			"requests only if it is on one scale", got[0].Score)
	}
}

// --- helpers ----------------------------------------------------------------

func idFromByte(b byte) id.ID {
	var out id.ID
	out[0] = b
	return out
}

func sampleToken() string {
	return (&query.Cursor{Tenant: "acme", Slot: 3, Value: attr.U64(1).OrderBytes(), ID: id.New()}).Encode()
}

func mustDecodeToken(t *testing.T, token string) []byte {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		t.Fatalf("decoding a token this test just minted: %v", err)
	}
	return raw
}

func encodeToken(raw []byte) string { return base64.RawURLEncoding.EncodeToString(raw) }
