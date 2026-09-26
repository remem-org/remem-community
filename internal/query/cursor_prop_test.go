package query_test

import (
	"encoding/json"
	"flag"
	"os"
	"testing"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/query"
	"github.com/remem-org/remem-go/internal/tenant"
	"pgregory.net/rapid"
)

// The floor for property tests is 1,000 iterations, raised here rather than
// left to a flag: a criterion that only holds when someone remembers a
// command-line argument is not a criterion.
func TestMain(m *testing.M) {
	if f := flag.Lookup("rapid.checks"); f != nil {
		if err := f.Value.Set("1000"); err != nil {
			panic("raising the rapid check count: " + err.Error())
		}
	}
	os.Exit(m.Run())
}

// A cursor is where a client's next page comes from, and a field that silently
// fails to round-trip resumes the listing somewhere else — which shows up as
// memories missing from the middle of a paged read, not as an error.
func TestACursorRoundTripsForAnyValue(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		want := drawCursor(rt)

		got, err := query.DecodeCursor(want.Encode(), want.Tenant)
		if err != nil {
			rt.Fatalf("decode: %v", err)
		}
		if got == nil {
			rt.Fatal("a token this test just minted decoded to no cursor")
		}
		if got.Encode() != want.Encode() {
			rt.Fatalf("round trip changed the token:\n got %+v\nwant %+v", got, want)
		}
	})
}

func TestACursorRoundTripsThroughJSON(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		want := drawCursor(rt)

		encoded, err := json.Marshal(want)
		if err != nil {
			rt.Fatalf("marshal: %v", err)
		}
		var got query.Cursor
		if err := json.Unmarshal(encoded, &got); err != nil {
			rt.Fatalf("unmarshal: %v", err)
		}
		if got.Encode() != want.Encode() {
			rt.Fatalf("the JSON round trip changed the token:\n got %+v\nwant %+v", got, want)
		}
	})
}

// No token minted for one tenant may ever decode for another. This is the
// property TestCursorFromAnotherTenantIsRefused checks once and this checks
// over every shape a tenant name and an ordering value can take — including the
// ones where a name is a prefix of another, which is the case a length-prefix
// bug would let through.
func TestNoCursorEverDecodesForAnotherTenant(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		want := drawCursor(rt)
		other := tenant.ID(rapid.SampledFrom([]string{
			"a", "ac", "acme", "acmecorp", "acme-2", "globex", "z",
		}).Draw(rt, "other"))
		if other == want.Tenant {
			return
		}

		got, err := query.DecodeCursor(want.Encode(), other)
		if err == nil {
			rt.Fatalf("a token issued for %q decoded for %q as %+v", want.Tenant, other, got)
		}
	})
}

func drawCursor(rt *rapid.T) *query.Cursor {
	return &query.Cursor{
		Tenant: tenant.ID(rapid.SampledFrom([]string{
			"a", "ac", "acme", "acmecorp", "acme-2", "globex", "z",
		}).Draw(rt, "tenant")),
		Slot: rapid.Uint16().Draw(rt, "slot"),
		Desc: rapid.Bool().Draw(rt, "desc"),
		// An ordering value is opaque bytes of any width: eight for a
		// timestamp, four for a float, and unbounded for a string slot.
		Value:   rapid.SliceOfN(rapid.Byte(), 0, 64).Draw(rt, "value"),
		ID:      drawID(rt),
		Session: [16]byte(drawID(rt)),
	}
}

func drawID(rt *rapid.T) id.ID {
	var out id.ID
	copy(out[:], rapid.SliceOfN(rapid.Byte(), 16, 16).Draw(rt, "id"))
	return out
}
