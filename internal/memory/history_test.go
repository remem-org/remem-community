package memory_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	pebblekv "github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

func TestHistoryIsOrderedAndTenantScoped(t *testing.T) {
	svc, clk := newRecordingService(t)
	ids := seed(t, svc, "a memory")

	// Three recalls, each outside the coalescing window, so three events.
	for i := 0; i < 3; i++ {
		// Past the coalescing window each time, so each fetch is its own
		// recall session rather than a repeat of the last.
		clk.Advance(2 * lifecycle.DefaultRecallWindow)
		if _, err := svc.Get(acmeCtx(), ids[0], memory.GetOpts{}); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}

	got, err := svc.History(acmeCtx(), memory.HistoryReq{ID: ids[0]})
	must(t, err)
	if len(got.Entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(got.Entries))
	}
	for i := 1; i < len(got.Entries); i++ {
		if got.Entries[i-1].At.Before(got.Entries[i].At) {
			t.Fatalf("entry %d is older than entry %d — history is newest-first", i-1, i)
		}
	}

	// Another tenant asking for the same id sees nothing of it.
	if _, err := svc.History(tenantCtx("globex"), memory.HistoryReq{ID: ids[0]}); !errs.Is(err, errs.NotFound) {
		t.Fatalf("another tenant's history request is %v, want NotFound", err)
	}
}

// An empty trail and a missing memory must not answer the same way. It is the
// distinction Phase 6 found missing on the connections routes, where listing
// the connections of a memory that did not exist returned an empty list and a
// typo read as "no connections".
func TestAnEmptyTrailAndAMissingMemoryDiffer(t *testing.T) {
	svc, _ := newRecordingService(t)
	ids := seed(t, svc, "never recalled")

	got, err := svc.History(acmeCtx(), memory.HistoryReq{ID: ids[0]})
	must(t, err)
	if len(got.Entries) != 0 {
		t.Fatalf("a memory nothing has touched has %d entries", len(got.Entries))
	}

	if _, err := svc.History(acmeCtx(), memory.HistoryReq{ID: id.New()}); !errs.Is(err, errs.NotFound) {
		t.Fatalf("a memory that does not exist answered %v, want NotFound", err)
	}
}

func TestHistoryRefusesAnUnreasonablePage(t *testing.T) {
	svc, _ := newRecordingService(t)
	ids := seed(t, svc, "a memory")

	_, err := svc.History(acmeCtx(), memory.HistoryReq{ID: ids[0], Limit: memory.MaxHistoryPage + 1})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

// Paging visits every entry once. The hard case is several entries in one
// millisecond, where only the sequence number separates them.
func TestHistoryPagesWithoutSkippingOrRepeating(t *testing.T) {
	svc, clk := newRecordingService(t)
	ids := seed(t, svc, "a memory")

	const recalls = 7
	for i := 0; i < recalls; i++ {
		// Past the coalescing window each time, so each fetch is its own
		// recall session rather than a repeat of the last.
		clk.Advance(2 * lifecycle.DefaultRecallWindow)
		if _, err := svc.Get(acmeCtx(), ids[0], memory.GetOpts{}); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}

	seen := map[string]int{}
	var before *memory.HistoryCursor
	for {
		page, err := svc.History(acmeCtx(), memory.HistoryReq{ID: ids[0], Limit: 2, Before: before})
		must(t, err)
		if len(page.Entries) == 0 {
			break
		}
		for _, e := range page.Entries {
			seen[fmt.Sprintf("%v/%d", e.At, e.Seq)]++
		}
		last := page.Entries[len(page.Entries)-1]
		before = &memory.HistoryCursor{At: last.At, Seq: last.Seq}
	}
	if len(seen) != recalls {
		t.Fatalf("paged over %d distinct entries, want %d", len(seen), recalls)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("entry %s came back %d times", k, n)
		}
	}
}

func TestHistoryRefusesAnUnscopedRead(t *testing.T) {
	svc, _ := newRecordingService(t)
	if _, err := svc.History(tenant.NewContext(context.Background(), ""), memory.HistoryReq{ID: id.New()}); err == nil {
		t.Fatal("an unscoped history read succeeded (Invariant 1)")
	}
}

func TestHistoryPagesThroughEveryEntry(t *testing.T) {
	ctx := acmeCtx()
	svc, clk := newRecordingService(t)
	m := storeAndRecallNTimes(t, ctx, svc, clk, 25)

	var seen []string
	var cursor string
	for {
		res, err := svc.History(ctx, memory.HistoryReq{ID: m, Limit: 10, Cursor: cursor})
		if err != nil {
			t.Fatalf("reading history: %v", err)
		}
		for _, e := range res.Entries {
			seen = append(seen, e.Kind+"@"+e.At.String()+"#"+strconv.Itoa(int(e.Seq)))
		}
		if !res.HasMore {
			if res.NextCursor != "" {
				t.Fatal("a final page handed back a cursor to page forever through emptiness")
			}
			break
		}
		cursor = res.NextCursor
	}

	if len(seen) != 25 {
		t.Fatalf("paging saw %d entries, want 25", len(seen))
	}
	if dup := firstDuplicate(seen); dup != "" {
		t.Fatalf("entry %s came back on two pages", dup)
	}
}

func TestAHistoryCursorSurvivesRestart(t *testing.T) {
	ctx := acmeCtx()
	dir := t.TempDir()
	svc, clk, close := newRecordingServiceOnDisk(t, dir)
	m := storeAndRecallNTimes(t, ctx, svc, clk, 25)
	first, err := svc.History(ctx, memory.HistoryReq{ID: m, Limit: 10})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.NextCursor == "" {
		t.Fatal("the first page of 25 entries handed back no cursor")
	}
	close()

	svc2, _, close2 := newRecordingServiceOnDisk(t, dir)
	defer close2()
	second, err := svc2.History(ctx, memory.HistoryReq{ID: m, Limit: 10, Cursor: first.NextCursor})
	if err != nil {
		t.Fatalf("resuming after a restart: %v", err)
	}
	if len(second.Entries) != 10 {
		t.Fatalf("resumed page has %d entries, want 10", len(second.Entries))
	}
}

func TestAHistoryCursorFromAnotherTenantIsRefused(t *testing.T) {
	ctx := acmeCtx()
	svc, clk := newRecordingService(t)
	m := storeAndRecallNTimes(t, ctx, svc, clk, 5)

	first, err := svc.History(ctx, memory.HistoryReq{ID: m, Limit: 2})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}

	_, err = svc.History(tenantCtx("globex"), memory.HistoryReq{ID: m, Limit: 2, Cursor: first.NextCursor})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("another tenant's cursor returned %v, want Invalid", err)
	}
}

func storeAndRecallNTimes(t *testing.T, ctx context.Context, svc *memory.Service, clk *clock.Fake, n int) id.ID {
	t.Helper()
	m, err := svc.Create(ctx, memory.CreateReq{Content: "a memory"})
	must(t, err)
	for i := 0; i < n; i++ {
		clk.Advance(2 * lifecycle.DefaultRecallWindow)
		if _, err := svc.Get(ctx, m.ID, memory.GetOpts{}); err != nil {
			t.Fatalf("recall %d: %v", i, err)
		}
	}
	return m.ID
}

func firstDuplicate(values []string) string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			return value
		}
		seen[value] = struct{}{}
	}
	return ""
}

func newRecordingServiceOnDisk(t *testing.T, dir string) (*memory.Service, *clock.Fake, func()) {
	t.Helper()
	kv, err := pebblekv.Open(dir, pebblekv.Options{})
	must(t, err)
	clk := clock.NewFake(clock.FakeStart)
	texts := text.New()
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(texts))
	svc := memory.New(kv, repo, flat.New(vector.NewStore(kv), distance.L2), embeddingtest.New(), clk,
		attr.MustTable(), graph.NewService(kv, clk), memory.Config{}, memory.WithTextIndex(texts),
		memory.WithRecall(events.NewStore(kv), 0))
	return svc, clk, func() {
		must(t, svc.Close())
		must(t, kv.Close())
	}
}
