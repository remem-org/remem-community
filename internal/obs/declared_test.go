package obs_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/obs"
)

// TestDeclaredNamesListEveryRegisteredFamily: the list the metric-coverage guard
// compares a workload against. It must include families nothing has recorded
// yet — that is the whole point of it — including the three the engine
// collector owns behind one registration.
func TestDeclaredNamesListEveryRegisteredFamily(t *testing.T) {
	names := obs.NewMetrics().DeclaredNames()

	for _, want := range []string{
		"remem_api_requests_total",
		"remem_storage_writes_total",
		"remem_storage_size_bytes",
		"remem_storage_compactions_total",
		"remem_storage_cache_accesses_total",
		"remem_search_truncated_total",
		"remem_migration_records_remaining",
	} {
		if !slices.Contains(names, want) {
			t.Errorf("DeclaredNames lacks %s", want)
		}
	}
	for _, n := range names {
		if !strings.HasPrefix(n, "remem_") {
			t.Errorf("a declared family %q is outside the remem namespace", n)
		}
	}
	if !slices.IsSorted(names) {
		t.Error("DeclaredNames is not sorted")
	}
	// Three API, six storage, three search, five jobs, three lifecycle, four
	// discovery, four migrations. A family added without updating this number
	// is a family the coverage guard must now be told about.
	if len(names) != 28 {
		t.Fatalf("%d families declared, want 28:\n%s", len(names), strings.Join(names, "\n"))
	}
}
