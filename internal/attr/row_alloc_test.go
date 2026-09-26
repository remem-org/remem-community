package attr_test

import (
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
)

// Decoding an attribute row allocates the row, one slice of values, and the
// strings it carries — not a hash map.
//
// Every candidate a filtered search examines has its row decoded to test the
// predicates, and a default semantic search always carries one (not archived),
// so a page of ten decodes at least thirty rows. Profiled in Phase 13, a map of
// every slot per candidate was a large part of the vector-retrieval
// benchmark's 617 allocations a query and 17% of its time in the collector.
// Slot numbers are small and dense, so a slice indexed by slot answers the same
// questions without the map's buckets.
func TestDecodingAFullRowAllocatesTheRowItsValuesAndItsStrings(t *testing.T) {
	slots := attr.MustTable()
	row := attr.NewRow(1)
	row.Set(attr.SlotArchived, attr.Bool(false))
	row.Set(attr.SlotPolicy, attr.Str("short_term"))
	row.Set(attr.SlotImportance, attr.F32(0.5))
	row.Set(attr.SlotCreatedAt, attr.U64(1_725_000_000_000))
	row.Set(attr.SlotAccessedAt, attr.U64(1_725_000_000_001))
	row.Set(attr.SlotHealth, attr.F32(100))
	row.Set(attr.SlotValence, attr.F32(0.1))
	row.Set(attr.SlotArousal, attr.F32(0.2))
	row.Set(attr.SlotNextAttentionAt, attr.U64(1_725_086_400_000))
	row.Set(attr.SlotUpdatedAt, attr.U64(1_725_000_000_002))
	row.Set(attr.SlotLastRecalledAt, attr.U64(1_725_000_000_003))
	encoded, err := row.Encode(slots)
	if err != nil {
		t.Fatal(err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := attr.DecodeRow(encoded, slots); err != nil {
			t.Fatal(err)
		}
	})
	// The row, its value slice, and the one string slot (policy).
	if allocs > 3 {
		t.Fatalf("decoding a row of eleven slots allocated %.0f times; want at most 3 "+
			"(the row, its values, its one string)", allocs)
	}
}
