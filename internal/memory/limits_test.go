package memory_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/memory"
)

// Every input a caller controls is bounded by something smaller than the 8 MiB
// request body, and each bound is enforced at the service, so HTTP and MCP
// refuse alike. Each is checked at the limit and one past it, so a bound off by
// one in either direction fails here.
//
// Found in Phase 13's security review. Tags, a batch's size and a query's
// length were bounded by the body alone. A memory's tags are postings written
// in its own transaction, and a batch is one model run a memory inside one
// request, so either could turn one request into unbounded work.
func TestCallerInputsAreBounded(t *testing.T) {
	svc := newService(t)
	ctx := acmeCtx()

	tags := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("tag-%d", i)
		}
		return out
	}
	batch := func(n int) []memory.CreateReq {
		out := make([]memory.CreateReq, n)
		for i := range out {
			out[i] = memory.CreateReq{Content: fmt.Sprintf("memory %d", i)}
		}
		return out
	}
	query := func(n int) string { return strings.Repeat("q", n) }

	existing, err := svc.Create(ctx, memory.CreateReq{Content: "a memory to update"})
	must(t, err)

	cases := []struct {
		name   string
		limit  func() error
		beyond func() error
	}{
		{"tags on a create", func() error {
			_, err := svc.Create(ctx, memory.CreateReq{Content: "c", Tags: tags(memory.MaxTags)})
			return err
		}, func() error {
			_, err := svc.Create(ctx, memory.CreateReq{Content: "c", Tags: tags(memory.MaxTags + 1)})
			return err
		}},
		{"tags on an update", func() error {
			ts := tags(memory.MaxTags)
			_, err := svc.Update(ctx, memory.UpdateReq{ID: existing.ID, Tags: &ts})
			return err
		}, func() error {
			ts := tags(memory.MaxTags + 1)
			_, err := svc.Update(ctx, memory.UpdateReq{ID: existing.ID, Tags: &ts})
			return err
		}},
		{"memories in a batch", func() error {
			_, err := svc.CreateBatch(ctx, batch(memory.MaxBatch))
			return err
		}, func() error {
			_, err := svc.CreateBatch(ctx, batch(memory.MaxBatch+1))
			return err
		}},
		{"a search query", func() error {
			_, err := svc.Search(ctx, memory.SearchReq{Type: memory.SearchHybrid, Query: query(memory.MaxQueryBytes)})
			return err
		}, func() error {
			_, err := svc.Search(ctx, memory.SearchReq{Type: memory.SearchHybrid, Query: query(memory.MaxQueryBytes + 1)})
			return err
		}},
		{"a search's tag filter", func() error {
			_, err := svc.Search(ctx, memory.SearchReq{Type: memory.SearchKeyword, Tags: tags(memory.MaxTags)})
			return err
		}, func() error {
			_, err := svc.Search(ctx, memory.SearchReq{Type: memory.SearchKeyword, Tags: tags(memory.MaxTags + 1)})
			return err
		}},
		{"a recall context", func() error {
			_, err := svc.Recall(ctx, memory.RecallReq{Type: memory.SearchHybrid, TokenBudget: 100, Context: query(memory.MaxQueryBytes)})
			return err
		}, func() error {
			_, err := svc.Recall(ctx, memory.RecallReq{Type: memory.SearchHybrid, TokenBudget: 100, Context: query(memory.MaxQueryBytes + 1)})
			return err
		}},
		{"a recall's tag filter", func() error {
			_, err := svc.Recall(ctx, memory.RecallReq{Type: memory.SearchKeyword, TokenBudget: 100, Tags: tags(memory.MaxTags)})
			return err
		}, func() error {
			_, err := svc.Recall(ctx, memory.RecallReq{Type: memory.SearchKeyword, TokenBudget: 100, Tags: tags(memory.MaxTags + 1)})
			return err
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.limit(); err != nil {
				t.Fatalf("at the limit: %v", err)
			}
			err := c.beyond()
			if !errs.Is(err, errs.Invalid) {
				t.Fatalf("one past the limit returned %v, want an Invalid refusal", err)
			}
		})
	}
}
