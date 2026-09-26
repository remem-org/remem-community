package memory

import (
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
)

// Ignoring RankedTTL, using it for listing, or failing to renew it breaks this.
func TestConfiguredRankedTTLExpiresIdleSearchButPreservesListing(t *testing.T) {
	ctx, svc := newSearchPagingService(t, nil, Config{RankedTTL: 7 * time.Second})
	seedSearchCorpus(t, ctx, svc, 10)
	clk := svc.clk.(*clock.Fake)
	req := SearchReq{Query: "alpha", Type: SearchHybrid, Limit: 1}
	idle, err := svc.Search(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	active, err := svc.Search(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	listed, err := svc.List(ctx, ListReq{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(6 * time.Second)
	req.Cursor = active.NextCursor
	active, err = svc.Search(ctx, req)
	if err != nil {
		t.Fatalf("active continuation: %v", err)
	}
	clk.Advance(time.Second)
	req.Cursor = idle.NextCursor
	if _, err := svc.Search(ctx, req); !errs.Is(err, errs.Invalid) {
		t.Fatalf("idle token must expire at configured TTL: %v", err)
	}
	req.Cursor = active.NextCursor
	if _, err := svc.Search(ctx, req); err != nil {
		t.Fatalf("continuation must renew idle deadline: %v", err)
	}
	if _, err := svc.List(ctx, ListReq{Limit: 1, Cursor: listed.NextCursor}); err != nil {
		t.Fatalf("ranked TTL must not expire listing: %v", err)
	}
}
