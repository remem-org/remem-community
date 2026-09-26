package memory_test

import (
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

func TestListSnapshotSurvivesOrderingMutation(t *testing.T) {
	for _, field := range memory.Orderings {
		for _, desc := range []bool{false, true} {
			t.Run(field+map[bool]string{false: "Asc", true: "Desc"}[desc], func(t *testing.T) {
				h := newListHarness(t)
				h.seedGradient(t, 4)
				req := memory.ListReq{OrderBy: field, Desc: desc, Limit: 1}
				first, err := h.svc.List(acmeCtx(), req)
				must(t, err)
				rec, err := h.repo.Get(acmeCtx(), first.Memories[0].ID)
				must(t, err)
				when := h.now().Add(time.Hour)
				val := float32(100)
				if desc {
					when = time.Unix(1, 0)
					val = 0
				}
				rec.CreatedAt = when
				rec.UpdatedAt = when
				rec.Fields.LastRecalledAt = when
				rec.Fields.Importance = val
				rec.Fields.Health = val
				tx := txn.New(h.kv)
				defer tx.Close()
				must(t, h.repo.Put(acmeCtx(), tx, rec))
				must(t, tx.Commit(acmeCtx()))
				seen := map[string]bool{first.Memories[0].ID.String(): true}
				req.Cursor = first.NextCursor
				for req.Cursor != "" {
					page, err := h.svc.List(acmeCtx(), req)
					must(t, err)
					for _, m := range page.Memories {
						if seen[m.ID.String()] {
							t.Fatalf("duplicate memory after ordering mutation")
						}
						seen[m.ID.String()] = true
					}
					req.Cursor = page.NextCursor
				}
				if len(seen) != 4 {
					t.Fatalf("got %d memories", len(seen))
				}
			})
		}
	}
}

func TestPagingSessionExpiryAndRestartAreExplicit(t *testing.T) {
	h := newListHarness(t)
	h.seedGradient(t, 3)
	first, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 1})
	must(t, err)
	if first.NextCursor == "" {
		t.Fatal("first page has no continuation")
	}

	h.clk.Advance(5*time.Minute + time.Second)
	_, err = h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 1, Cursor: first.NextCursor})
	if !errs.Is(err, errs.Invalid) || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired session = %v, want an explicit Invalid error", err)
	}

	other := memory.New(h.kv,
		record.NewRepo(h.kv, record.WithIndexer(attr.NewIndexer(attr.MustTable()))),
		flat.New(vector.NewStore(h.kv), distance.L2), embeddingtest.New(),
		clock.NewFake(clock.FakeStart), attr.MustTable(),
		graph.NewService(h.kv, clock.NewFake(clock.FakeStart)), memory.Config{})
	t.Cleanup(func() { _ = other.Close() })
	_, err = other.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 1, Cursor: first.NextCursor})
	if !errs.Is(err, errs.Invalid) || !strings.Contains(err.Error(), "restart") {
		t.Fatalf("token presented to a restarted service = %v", err)
	}
}

// At capacity a new first page reclaims the least recently used idle session
// rather than being refused. The budget still bounds pinned snapshots; what it
// no longer does is turn away a caller who is here now to protect one who has
// probably gone — which let one tenant's abandoned first pages refuse every
// other tenant's (TestOneTenantsAbandonedPagesDoNotRefuseAnother).
func TestPagingSessionCapacityReclaimsTheOldestIdleSession(t *testing.T) {
	h := newListHarness(t)
	h.seedGradient(t, 2)
	var cursors []string
	for i := 0; i < 128; i++ {
		page, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 1})
		must(t, err)
		if page.NextCursor == "" {
			t.Fatal("session was not retained")
		}
		cursors = append(cursors, page.NextCursor)
	}

	beyond, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 1})
	if err != nil {
		t.Fatalf("a first page beyond capacity = %v, want it served by reclaiming an idle session", err)
	}
	if beyond.NextCursor == "" {
		t.Fatal("the page served beyond capacity retained no session")
	}

	_, err = h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 1, Cursor: cursors[0]})
	if !errs.Is(err, errs.Invalid) || !strings.Contains(err.Error(), "reclaimed") {
		t.Fatalf("the oldest idle session's cursor = %v, want Invalid naming the reclaim", err)
	}
	if _, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 1, Cursor: cursors[1]}); err != nil {
		t.Fatalf("a newer session's cursor was reclaimed too: %v", err)
	}
}
