package memory_test

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/memory"
)

func createForSearch(t *testing.T, ctx context.Context, svc *memory.Service, req memory.CreateReq) *memory.Memory {
	t.Helper()
	m, err := svc.Create(ctx, req)
	must(t, err)
	return m
}

// Dropping either predicate admits a decoy, in each retrieval mode.
func TestFiltersCombineWithAnd(t *testing.T) {
	for _, mode := range []memory.SearchType{memory.SearchSemantic, memory.SearchKeyword, memory.SearchHybrid} {
		t.Run(string(mode), func(t *testing.T) {
			ctx, svc := acmeCtx(), newService(t)
			high, low := float32(0.95), float32(0.2)
			wanted := createForSearch(t, ctx, svc, memory.CreateReq{Content: "consensus protocols", Policy: "long_term", Importance: &high})
			createForSearch(t, ctx, svc, memory.CreateReq{Content: "consensus protocols", Policy: "long_term", Importance: &low})
			createForSearch(t, ctx, svc, memory.CreateReq{Content: "consensus protocols", Policy: "short_term", Importance: &high})
			min := float32(0.9)
			res, err := svc.Search(ctx, memory.SearchReq{Query: "consensus", Type: mode, Limit: 10, Policy: "long_term", ImportanceMin: &min})
			must(t, err)
			if len(res.Results) != 1 || res.Results[0].Memory.ID != wanted.ID {
				t.Fatalf("conjoined filters returned %+v, want only %s", res.Results, wanted.ID)
			}
		})
	}
}

// The endpoints are inclusive, and timestamp bounds use the sidecar's milliseconds.
func TestATimeWindowNarrowsASearch(t *testing.T) {
	clk := clock.NewFake(clock.FakeStart)
	ctx, svc := acmeCtx(), newService(t, func(s *setup) { s.clk = clk })
	old := createForSearch(t, ctx, svc, memory.CreateReq{Content: "alpha old"})
	clk.Set(time.Date(2026, 6, 1, 0, 0, 0, 123000000, time.UTC))
	recent := createForSearch(t, ctx, svc, memory.CreateReq{Content: "alpha recent"})
	clk.Advance(time.Millisecond)
	createForSearch(t, ctx, svc, memory.CreateReq{Content: "alpha later"})
	for _, tc := range []struct {
		name          string
		after, before *time.Time
		want          id.ID
	}{
		{"equal endpoints", &recent.CreatedAt, &recent.CreatedAt, recent.ID},
		{"upper endpoint", nil, &old.CreatedAt, old.ID},
		{"bounded window", &old.CreatedAt, &old.CreatedAt, old.ID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := svc.Search(ctx, memory.SearchReq{Query: "alpha", Type: memory.SearchKeyword, Limit: 10, CreatedAfter: tc.after, CreatedBefore: tc.before})
			must(t, err)
			if len(res.Results) != 1 || res.Results[0].Memory.ID != tc.want {
				t.Fatalf("time window returned %+v, want only %s", res.Results, tc.want)
			}
		})
	}
	after := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	res, err := svc.Search(ctx, memory.SearchReq{Query: "alpha", Type: memory.SearchKeyword, Limit: 10, CreatedAfter: &after})
	must(t, err)
	if len(res.Results) != 2 {
		t.Fatalf("lower bound returned %d, want two recent memories", len(res.Results))
	}
}

func TestSearchImportanceBoundsIncludeTheirEndpoints(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)
	lo, mid, hi := float32(0.2), float32(0.5), float32(0.9)
	a := createForSearch(t, ctx, svc, memory.CreateReq{Content: "alpha", Importance: &lo})
	b := createForSearch(t, ctx, svc, memory.CreateReq{Content: "alpha", Importance: &mid})
	createForSearch(t, ctx, svc, memory.CreateReq{Content: "alpha", Importance: &hi})
	for _, tc := range []struct {
		name    string
		min     *float32
		wantIDs []id.ID
	}{
		{"upper only", nil, []id.ID{a.ID, b.ID}},
		{"equal endpoints", &mid, []id.ID{b.ID}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := svc.Search(ctx, memory.SearchReq{Query: "alpha", Type: memory.SearchKeyword, Limit: 10, ImportanceMin: tc.min, ImportanceMax: &mid})
			must(t, err)
			got := map[id.ID]bool{}
			for _, hit := range res.Results {
				got[hit.Memory.ID] = true
			}
			if len(got) != len(tc.wantIDs) {
				t.Fatalf("got %v, want %v", got, tc.wantIDs)
			}
			for _, rid := range tc.wantIDs {
				if !got[rid] {
					t.Fatalf("inclusive endpoint %s missing", rid)
				}
			}
		})
	}
}

// Evidence and scores must stay identical when a filter matches the whole corpus.
// A separate attr source could leave cosine unchanged while corrupting fusion.
func TestAFilterOnlyNarrows(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)
	createForSearch(t, ctx, svc, memory.CreateReq{Content: "consensus and raft", Policy: "long_term"})
	createForSearch(t, ctx, svc, memory.CreateReq{Content: "marzipan recipes from a Bavarian bakery", Policy: "long_term"})
	for _, mode := range []memory.SearchType{memory.SearchSemantic, memory.SearchKeyword, memory.SearchHybrid} {
		req := memory.SearchReq{Query: "distributed consensus", Type: mode, Limit: 10, Explain: true}
		base, err := svc.Search(ctx, req)
		must(t, err)
		req.Policy = "long_term"
		filtered, err := svc.Search(ctx, req)
		must(t, err)
		if !reflect.DeepEqual(base.Results, filtered.Results) {
			t.Fatalf("%s: a matching policy changed candidates, scores, ordering or sources", mode)
		}
	}
}

func TestRelatedToBoostsGraphNeighbours(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)
	anchor := createForSearch(t, ctx, svc, memory.CreateReq{Content: "the anchor"})
	neighbour := createForSearch(t, ctx, svc, memory.CreateReq{Content: "alpha connected", Policy: "long_term"})
	createForSearch(t, ctx, svc, memory.CreateReq{Content: "alpha unconnected"})
	_, err := svc.Relate(ctx, memory.RelateReq{From: anchor.ID, To: neighbour.ID, Type: "related_to", Strength: 0.9})
	must(t, err)
	req := memory.SearchReq{Query: "alpha", Type: memory.SearchHybrid, Limit: 10, RelatedTo: &anchor.ID, Explain: true}
	res, err := svc.Search(ctx, req)
	must(t, err)
	if len(res.Results) < 2 || res.Results[0].Memory.ID != neighbour.ID {
		t.Fatalf("connected memory did not rank first: %+v", res.Results)
	}
	graphFound := false
	for _, source := range res.Results[0].Sources {
		graphFound = graphFound || source.Source == "graph"
	}
	if !graphFound {
		t.Fatal("connected memory has no graph evidence")
	}
	req.Policy = "short_term"
	res, err = svc.Search(ctx, req)
	must(t, err)
	for _, hit := range res.Results {
		if hit.Memory.ID == neighbour.ID {
			t.Fatal("graph source bypassed policy filter")
		}
	}
}

func TestAnImpossibleRangeIsRefusedRatherThanReturningNothing(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)
	lo, hi := float32(0.9), float32(0.1)
	after, before := clock.FakeStart.Add(time.Hour), clock.FakeStart
	nan, inf := float32(math.NaN()), float32(math.Inf(1))
	for name, req := range map[string]memory.SearchReq{
		"importance inverted": {ImportanceMin: &lo, ImportanceMax: &hi},
		"time inverted":       {CreatedAfter: &after, CreatedBefore: &before},
		"nan min":             {ImportanceMin: &nan},
		"nan max":             {ImportanceMax: &nan},
		"infinite min":        {ImportanceMin: &inf},
		"infinite max":        {ImportanceMax: &inf},
	} {
		t.Run(name, func(t *testing.T) {
			req.Query, req.Type, req.Limit = "alpha", memory.SearchKeyword, 10
			if _, err := svc.Search(ctx, req); !errs.Is(err, errs.Invalid) {
				t.Fatalf("invalid range returned %v, want Invalid", err)
			}
		})
	}
}

func TestSearchRelatedAnchorMustExistInTheTenant(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)
	foreign := createForSearch(t, tenantCtx("globex"), svc, memory.CreateReq{Content: "foreign anchor"})
	for _, tc := range []struct {
		name string
		rid  id.ID
		kind errs.Kind
	}{
		{"missing", id.New(), errs.NotFound},
		{"foreign", foreign.ID, errs.NotFound},
		{"zero", id.ID{}, errs.Invalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Search(ctx, memory.SearchReq{Query: "alpha", Type: memory.SearchKeyword, RelatedTo: &tc.rid})
			if !errs.Is(err, tc.kind) {
				t.Fatalf("anchor returned %v, want %v", err, tc.kind)
			}
		})
	}
}

// A changed bound must not resume the cached answer, even if both bounds happen
// to match this corpus. Adjacent float32 values catch precision lost in hashing.
func TestASearchCursorBindsEveryFilterAndAnchor(t *testing.T) {
	for _, field := range []string{"policy", "importance_min", "importance_max", "created_after", "created_before", "related_to"} {
		t.Run(field, func(t *testing.T) {
			ctx, svc := acmeCtx(), newService(t)
			ids := seed(t, svc, "alpha one", "alpha two", "alpha three", "alpha four")
			min, max := float32(0.1), float32(0.9)
			after, before := clock.FakeStart.Add(-time.Hour), clock.FakeStart.Add(time.Hour)
			req := memory.SearchReq{Query: "alpha", Type: memory.SearchHybrid, Limit: 1, Policy: "short_term", ImportanceMin: &min, ImportanceMax: &max, CreatedAfter: &after, CreatedBefore: &before, RelatedTo: &ids[0]}
			first, err := svc.Search(ctx, req)
			must(t, err)
			if first.NextCursor == "" {
				t.Fatal("fixture has no continuation")
			}
			req.Cursor = first.NextCursor
			switch field {
			case "policy":
				req.Policy = "long_term"
			case "importance_min":
				min = math.Nextafter32(min, 1)
			case "importance_max":
				max = math.Nextafter32(max, 0)
			case "created_after":
				after = after.Add(time.Millisecond)
			case "created_before":
				before = before.Add(-time.Millisecond)
			case "related_to":
				req.RelatedTo = &ids[1]
			}
			if _, err := svc.Search(ctx, req); !errs.Is(err, errs.Invalid) {
				t.Fatalf("changed %s resumed cached answer: %v", field, err)
			}
		})
	}
}

// Live writes must not change the predicates, graph evidence, or bodies of a
// materialised ranking, including deleting its anchor between pages.
func TestFilteredGraphSearchKeepsItsSnapshotAcrossPages(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)
	ids := seed(t, svc, "alpha anchor", "alpha one", "alpha two", "alpha three", "alpha four")
	_, err := svc.Relate(ctx, memory.RelateReq{From: ids[0], To: ids[1], Strength: 0.9})
	must(t, err)
	min, max := float32(0.1), float32(0.9)
	after, before := clock.FakeStart.Add(-time.Hour), clock.FakeStart.Add(time.Hour)
	req := memory.SearchReq{Query: "alpha", Type: memory.SearchHybrid, Limit: 10, Explain: true, Policy: "short_term", ImportanceMin: &min, ImportanceMax: &max, CreatedAfter: &after, CreatedBefore: &before, RelatedTo: &ids[0]}
	whole, err := svc.Search(ctx, req)
	must(t, err)
	req.Limit = 1
	first, err := svc.Search(ctx, req)
	must(t, err)
	if !first.HasMore || first.NextCursor == "" {
		t.Fatal("fixture has no continuation")
	}
	must(t, svc.Delete(ctx, ids[0], true))
	policy := "long_term"
	_, err = svc.Update(ctx, memory.UpdateReq{ID: ids[2], Policy: &policy})
	must(t, err)
	seed(t, svc, "alpha newly stored")
	got := first.Results
	req.Cursor, req.Limit = first.NextCursor, 2
	for pages := 0; req.Cursor != ""; pages++ {
		if pages >= len(ids) {
			t.Fatal("pagination did not terminate")
		}
		page, err := svc.Search(ctx, req)
		must(t, err)
		if page.Truncated || page.HasMore != (page.NextCursor != "") {
			t.Fatalf("incorrect page flags: %+v", page)
		}
		got = append(got, page.Results...)
		req.Cursor = page.NextCursor
	}
	if !reflect.DeepEqual(got, whole.Results) {
		t.Fatal("live writes changed the filtered graph ranking or snapshot bodies")
	}
	if _, err := svc.Search(ctx, req); !errs.Is(err, errs.NotFound) {
		t.Fatalf("new search accepted deleted anchor: %v", err)
	}
}

func TestSearchTimeBoundsMatchUnsetAndPreEpochProjection(t *testing.T) {
	ctx, svc := acmeCtx(), newService(t)
	seed(t, svc, "alpha")
	for _, before := range []time.Time{{}, time.Unix(-1, 0)} {
		res, err := svc.Search(ctx, memory.SearchReq{Query: "alpha", Type: memory.SearchKeyword, CreatedBefore: &before})
		must(t, err)
		if len(res.Results) != 0 {
			t.Fatal("pre-epoch upper bound wrapped around and admitted a modern timestamp")
		}
	}
}
