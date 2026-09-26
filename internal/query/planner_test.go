package query_test

import (
	"testing"

	"github.com/remem-org/remem-go/internal/query"
)

// A listing and a search widen to different bounds, as in Rust
// (config.rs:290-300): search.widen_max_factor 32 and search.list_max_factor
// 128. A listing candidate is one attribute-row read on an index already being
// walked; widening a search re-runs a similarity traversal. Until Phase 13 a
// Go listing used the search's factor and truncated a quarter as far in.
func TestAListingWidensFurtherThanASearch(t *testing.T) {
	slots := slotTable(t)
	planner := query.NewPlanner(slots, 0, 0)

	created, ok := slots.Lookup("created_at")
	if !ok {
		t.Fatal("the slot table has no created_at")
	}
	listing, err := planner.Plan(&query.Query{Tenant: graphTenant, OrderBy: created.Slot, Limit: 5})
	if err != nil {
		t.Fatalf("planning a listing: %v", err)
	}
	if want := 5 * query.ListMaxFactor; listing.MaxBudget != want {
		t.Fatalf("a listing of 5 may examine %d candidates, want %d (list_max_factor %d)",
			listing.MaxBudget, want, query.ListMaxFactor)
	}

	search, err := planner.Plan(&query.Query{Tenant: graphTenant, Vector: []float32{1, 0}, Limit: 5})
	if err != nil {
		t.Fatalf("planning a search: %v", err)
	}
	if want := 5 * query.WidenMaxFactor; search.MaxBudget != want {
		t.Fatalf("a search of 5 may examine %d candidates, want %d (widen_max_factor %d)",
			search.MaxBudget, want, query.WidenMaxFactor)
	}

	// And the setting is honoured, not only the default.
	tuned, err := query.NewPlanner(slots, 0, 200).Plan(&query.Query{Tenant: graphTenant, OrderBy: created.Slot, Limit: 5})
	if err != nil {
		t.Fatalf("planning a listing with list_max_factor 200: %v", err)
	}
	if tuned.MaxBudget != 1000 {
		t.Fatalf("list_max_factor 200 gave a budget of %d for a page of 5, want 1000", tuned.MaxBudget)
	}
}
