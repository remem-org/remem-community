package memory_test

import (
	"testing"

	"github.com/remem-org/remem-go/internal/memory"
)

// TestOneTenantsAbandonedPagesDoNotRefuseAnother reproduces what
// TestTenantIsolationUnderConcurrency found under load: the paging-session
// budget is one budget for every tenant, and a session whose first page did not
// exhaust its results is held until its idle deadline. So one tenant reading
// first pages and never asking for the second can refuse every other tenant's
// first page.
//
// The existing capacity tests all exhaust the budget and then ask again as the
// same tenant. None asks as a second one.
func TestOneTenantsAbandonedPagesDoNotRefuseAnother(t *testing.T) {
	svc := newService(t)
	seedAs(t, svc, "acme", "acme one", "acme two")
	seedAs(t, svc, "globex", "globex one", "globex two")

	for i := 0; i < 128; i++ {
		page, err := svc.List(tenantCtx("acme"), memory.ListReq{OrderBy: "created_at", Limit: 1})
		must(t, err)
		if page.NextCursor == "" {
			t.Fatalf("acme's page %d retained no session, so it holds nothing of the budget", i)
		}
	}

	if _, err := svc.List(tenantCtx("globex"), memory.ListReq{OrderBy: "created_at", Limit: 1}); err != nil {
		t.Fatalf("globex's first page, after acme abandoned 128 first pages: %v", err)
	}
}
