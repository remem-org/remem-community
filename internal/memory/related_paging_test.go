package memory_test

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

// Stopping after an orphaned slice hides valid weaker neighbours. Advancing
// the cursor by returned bodies instead of consumed positions repeats them.
func TestRelatedPagesSkipDanglingEdgesWithoutLosingNeighbours(t *testing.T) {
	ctx := acmeCtx()
	clk := clock.NewFake(clock.FakeStart)
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	repo := record.NewRepo(kv, record.WithIndexer(attr.NewIndexer(attr.MustTable())))
	svc := memory.New(kv, repo, flat.New(vector.NewStore(kv), distance.L2),
		embeddingtest.New(), clk, attr.MustTable(), graph.NewService(kv, clk), memory.Config{})
	t.Cleanup(func() { _ = svc.Close() })
	ids := seed(t, svc, "anchor", "missing strong", "valid strong", "missing weak", "valid weak")
	for i, strength := range []float32{0.9, 0.7, 0.5, 0.3} {
		_, err := svc.Relate(ctx, memory.RelateReq{From: ids[0], To: ids[i+1], Strength: strength})
		must(t, err)
	}
	// Delete canonical bodies directly to leave dangling graph edges. A normal
	// service delete removes its edges too and cannot reproduce this damage.
	must(t, txn.Do(ctx, kv, func(tx txn.Tx) error {
		if err := repo.Delete(ctx, tx, ids[1]); err != nil {
			return err
		}
		return repo.Delete(ctx, tx, ids[3])
	}))
	// IncludeArchived avoids an attribute predicate dropping the orphan before
	// hydration, so this exercises the missing-body handling in Related itself.
	req := memory.RelatedReq{ID: ids[0], IncludeArchived: true, Limit: 1}
	first, err := svc.Related(ctx, req)
	must(t, err)
	if len(first.Results) != 1 || first.Results[0].Memory.ID != ids[2] || first.Results[0].Score != 0.7 || !first.HasMore || first.NextCursor == "" {
		t.Fatalf("first page lost the valid stronger neighbour: %+v", first)
	}
	req.Cursor = first.NextCursor
	last, err := svc.Related(ctx, req)
	must(t, err)
	if len(last.Results) != 1 || last.Results[0].Memory.ID != ids[4] || last.Results[0].Score != 0.3 || last.HasMore || last.NextCursor != "" {
		t.Fatalf("last page lost or repeated a valid neighbour: %+v", last)
	}
}

// Rewalking with a page-sized limit loses neighbours or repeats page one.
func TestRelatedPagesThroughEveryNeighbour(t *testing.T) {
	svc := newService(t)
	anchor := connectedCluster(t, svc, 33)
	seen := map[id.ID]bool{}
	var cursor string
	for {
		res, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: anchor, Limit: 5, Cursor: cursor})
		must(t, err)
		for _, r := range res.Results {
			if seen[r.Memory.ID] {
				t.Fatalf("memory %s came back on two pages", r.Memory.ID)
			}
			seen[r.Memory.ID] = true
		}
		if res.HasMore != (res.NextCursor != "") {
			t.Fatalf("cursor and has_more disagree: %+v", res)
		}
		if !res.HasMore {
			break
		}
		cursor = res.NextCursor
	}
	if len(seen) != 33 {
		t.Fatalf("paging reached %d neighbours, want 33", len(seen))
	}
}

// Ranking by keys or rescoring between pages breaks this descending sequence.
func TestRelatedPagesStayOrderedByPathStrength(t *testing.T) {
	svc := newService(t)
	ids := seed(t, svc, makeContents(21)...)
	for i, target := range ids[1:] {
		_, err := svc.Relate(acmeCtx(), memory.RelateReq{From: ids[0], To: target, Strength: float32(i+1) / 20})
		must(t, err)
	}
	var scores []float32
	var cursor string
	for {
		res, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: ids[0], Limit: 3, Cursor: cursor})
		must(t, err)
		for _, r := range res.Results {
			scores = append(scores, r.Score)
		}
		if !res.HasMore {
			break
		}
		if res.NextCursor == "" || len(scores) > 20 {
			t.Fatal("paging did not advance")
		}
		cursor = res.NextCursor
	}
	if len(scores) != 20 {
		t.Fatalf("got %d scores, want 20", len(scores))
	}
	for i, score := range scores {
		if want := float32(20-i) / 20; score != want {
			t.Fatalf("score %d = %v, want %v", i, score, want)
		}
	}
}

// A live anchor lookup or a fresh traversal/body snapshot breaks continuation
// after deleting the anchor and every neighbour that remains to be returned.
func TestRelatedPagesKeepTheOriginalSnapshot(t *testing.T) {
	svc := newService(t)
	anchor := connectedCluster(t, svc, 7)
	first, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: anchor, Limit: 2})
	must(t, err)
	if first.NextCursor == "" {
		t.Fatal("first page has no continuation")
	}
	conns, err := svc.Connections(acmeCtx(), memory.ConnectionsReq{ID: anchor})
	must(t, err)
	for _, c := range conns.Connections {
		must(t, svc.Delete(acmeCtx(), c.To, true))
	}
	must(t, svc.Delete(acmeCtx(), anchor, true))
	second, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: anchor, Limit: 10, Cursor: first.NextCursor})
	must(t, err)
	if len(second.Results) != 5 || second.HasMore || second.NextCursor != "" {
		t.Fatalf("remaining snapshot page = %+v", second)
	}
	for _, a := range first.Results {
		for _, b := range second.Results {
			if a.Memory.ID == b.Memory.ID {
				t.Fatal("second page repeated a memory")
			}
		}
	}
}

// Omitting any traversal input from the fingerprint reuses the wrong ranking.
func TestRelatedCursorBindsEveryTraversalParameter(t *testing.T) {
	svc := newService(t)
	anchor := connectedCluster(t, svc, 5)
	base := memory.RelatedReq{ID: anchor, Limit: 1}
	first, err := svc.Related(acmeCtx(), base)
	must(t, err)
	if first.NextCursor == "" {
		t.Fatal("first page has no continuation")
	}
	for name, change := range map[string]func(*memory.RelatedReq){
		"anchor":    func(r *memory.RelatedReq) { r.ID = id.New() },
		"depth":     func(r *memory.RelatedReq) { r.Depth = 2 },
		"types":     func(r *memory.RelatedReq) { r.Types = []string{"supports"} },
		"direction": func(r *memory.RelatedReq) { r.Direction = "both" },
		"strength":  func(r *memory.RelatedReq) { r.MinStrength = 0.4 },
		"archived":  func(r *memory.RelatedReq) { r.IncludeArchived = true },
	} {
		t.Run(name, func(t *testing.T) {
			req := base
			req.Cursor = first.NextCursor
			change(&req)
			_, err := svc.Related(acmeCtx(), req)
			if !errs.Is(err, errs.Invalid) {
				t.Fatalf("changed query returned %v, want Invalid", err)
			}
		})
	}
	base.Cursor = first.NextCursor
	if _, err := svc.Related(tenantCtx("other"), base); !errs.Is(err, errs.Invalid) {
		t.Fatalf("foreign tenant returned %v, want Invalid", err)
	}
	base.Limit = 2
	if _, err := svc.Related(acmeCtx(), base); err != nil {
		t.Fatalf("changed page size refused: %v", err)
	}
}

// Missing session lookup or offset validation accepts stale or forged cursors.
func TestRelatedCursorRefusesMissingSessionAndMalformedPosition(t *testing.T) {
	svc := newService(t)
	anchor := connectedCluster(t, svc, 5)
	first, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: anchor, Limit: 1})
	must(t, err)
	other := newService(t)
	_, err = other.Related(acmeCtx(), memory.RelatedReq{ID: anchor, Limit: 1, Cursor: first.NextCursor})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("cursor without its session returned %v", err)
	}
	payload, err := codec.DecodeToken(first.NextCursor, codec.TokenSearch, tenant.ID("acme"))
	must(t, err)
	for _, cursor := range []string{"bad cursor", codec.EncodeToken(codec.TokenSearch, "acme", binary.AppendUvarint(payload[:16:16], 1000))} {
		_, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: anchor, Limit: 1, Cursor: cursor})
		if !errs.Is(err, errs.Invalid) {
			t.Fatalf("malformed position returned %v", err)
		}
	}
}

// A page reaching the materialisation bound must keep truncated on every page.
func TestRelatedPagesReportBoundAndReleaseExhaustedSessions(t *testing.T) {
	svc := newService(t, withConfig(memory.Config{MaxPageDepth: 3}))
	anchor := connectedCluster(t, svc, 5)
	for i := 0; i < 130; i++ {
		first, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: anchor, Limit: 2})
		must(t, err)
		if !first.Truncated || !first.HasMore || first.NextCursor == "" {
			t.Fatalf("bounded first page = %+v", first)
		}
		last, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: anchor, Limit: 2, Cursor: first.NextCursor})
		must(t, err)
		if !last.Truncated || last.HasMore || last.NextCursor != "" || len(last.Results) != 1 {
			t.Fatalf("bounded last page = %+v", last)
		}
	}
}

// Related shares listing/search's snapshot budget, and at capacity reclaims the
// least recently used idle session rather than refusing a new first page.
func TestRelatedReclaimsTheOldestIdleSessionAtCapacity(t *testing.T) {
	svc := newService(t)
	anchor := connectedCluster(t, svc, 3)
	var oldest string
	for i := 0; i < 128; i++ {
		first, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: anchor, Limit: 1})
		must(t, err)
		if first.NextCursor == "" {
			t.Fatal("no retained session")
		}
		if i == 0 {
			oldest = first.NextCursor
		}
	}
	if _, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: anchor, Limit: 1}); err != nil {
		t.Fatalf("a first page beyond capacity returned %v, want it served", err)
	}
	_, err := svc.Related(acmeCtx(), memory.RelatedReq{ID: anchor, Limit: 1, Cursor: oldest})
	if !errs.Is(err, errs.Invalid) || !strings.Contains(err.Error(), "reclaimed") {
		t.Fatalf("the reclaimed session's cursor returned %v, want Invalid naming the reclaim", err)
	}
}
