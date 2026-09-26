package server

import (
	"context"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/tenant"
)

func TestServerAppliesPagingConfiguration(t *testing.T) {
	cfg := config.Default()
	cfg.Storage.Engine = "memory"
	cfg.Search.MaxPageDepth = 3
	cfg.Paging.RankedTTL = 7 * time.Second
	clk := clock.NewFake(clock.FakeStart)
	d, err := build(cfg, options{embedder: embeddingtest.New(), clk: clk,
		capability: tenant.IdentityScoped})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.close() })
	ctx := tenant.NewContext(context.Background(), "acme")
	for range 5 {
		if _, err := d.memories.Create(ctx, memory.CreateReq{Content: "alpha"}); err != nil {
			t.Fatal(err)
		}
	}
	req := memory.SearchReq{Query: "alpha", Type: memory.SearchKeyword, Limit: 1}
	page, err := d.memories.Search(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !page.Truncated {
		t.Fatal("configured depth must truncate five matches at three")
	}
	count := len(page.Results)
	for page.HasMore {
		req.Cursor = page.NextCursor
		page, err = d.memories.Search(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		count += len(page.Results)
	}
	if count != 3 {
		t.Fatalf("configured depth returned %d results, want 3", count)
	}
	req.Cursor = ""
	page, err = d.memories.Search(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(7 * time.Second)
	req.Cursor = page.NextCursor
	if _, err := d.memories.Search(ctx, req); !errs.Is(err, errs.Invalid) {
		t.Fatalf("configured idle timeout not applied: %v", err)
	}
}
