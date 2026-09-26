package memory_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/keys"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/txn"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

// TestListOrdersByEachIndexedSlot is table-driven over the five orderings the
// phase promises, which is plan §II.10 row 3: Rust allows one, because its
// memtable holds a single version per key, and Pebble snapshots remove the
// constraint.
func TestListOrdersByEachIndexedSlot(t *testing.T) {
	for _, field := range []string{"created_at", "updated_at", "importance", "health", "last_recalled_at"} {
		t.Run(field, func(t *testing.T) {
			h := newListHarness(t)
			h.seedGradient(t, 12)

			asc, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: field, Limit: 50})
			must(t, err)
			if len(asc.Memories) != 12 {
				t.Fatalf("ordering by %s returned %d memories, want 12", field, len(asc.Memories))
			}

			desc, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: field, Desc: true, Limit: 50})
			must(t, err)

			for i := range asc.Memories {
				if asc.Memories[i].ID != desc.Memories[len(desc.Memories)-1-i].ID {
					t.Fatalf("ordering by %s: the descending listing is not the reverse of the "+
						"ascending one at position %d", field, i)
				}
			}
			// The gradient makes every slot strictly increasing with the
			// content number, so position i must hold memory i.
			for i, m := range asc.Memories {
				if want := fmt.Sprintf("memory %02d", i); m.Content != want {
					t.Errorf("ordering by %s: position %d holds %q, want %q", field, i, m.Content, want)
				}
			}
		})
	}
}

func TestListRefusesAnOrderingThatIsNotAnAccessPath(t *testing.T) {
	h := newListHarness(t)
	h.seedGradient(t, 3)

	_, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "arousal", Limit: 10})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("ordering by an unindexed field gave %v, want errs.Invalid: answering it would "+
			"mean reading every memory the tenant owns", err)
	}
	if got := err.Error(); !contains(got, "created_at") {
		t.Errorf("the refusal must list the orderings that do work: %v", got)
	}
}

// TestListPagesInBothDirections: ascending and descending walks over the same
// corpus are exact reverses, page by page.
func TestListPagesInBothDirections(t *testing.T) {
	h := newListHarness(t)
	h.seedGradient(t, 25)

	asc := h.pageAll(t, memory.ListReq{OrderBy: "created_at", Limit: 7})
	desc := h.pageAll(t, memory.ListReq{OrderBy: "created_at", Desc: true, Limit: 7})

	if len(asc) != 25 || len(desc) != 25 {
		t.Fatalf("paging returned %d ascending and %d descending, want 25 each", len(asc), len(desc))
	}
	for i := range asc {
		if asc[i] != desc[len(desc)-1-i] {
			t.Fatalf("the paged walks are not reverses of each other at position %d", i)
		}
	}
	seen := map[id.ID]bool{}
	for _, rid := range asc {
		if seen[rid] {
			t.Fatalf("memory %s came back twice in one paged listing", rid)
		}
		seen[rid] = true
	}
}

// TestListIsStableAcrossConcurrentWrites: 1,000 records created during a paged
// listing, and no record appears twice or is skipped.
//
// The guarantee is stated per ordering slot on query.Cursor, and this is the
// exact case it promises: created_at is immutable, so records written during
// the listing sort at one end and cannot displace what has already been
// returned.
func TestListIsStableAcrossConcurrentWrites(t *testing.T) {
	h := newListHarness(t)
	original := h.seedGradient(t, 60)
	want := make(map[id.ID]bool, len(original))
	for _, rid := range original {
		want[rid] = true
	}

	var mu sync.Mutex
	seen := map[id.ID]int{}
	cursor := ""
	writes := 0

	for page := 0; ; page++ {
		res, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 6, Cursor: cursor})
		must(t, err)

		mu.Lock()
		for _, m := range res.Memories {
			seen[m.ID]++
		}
		mu.Unlock()

		// A thousand interleaved creations, spread across the pages. They are
		// newer than everything in the original corpus, so an ascending walk
		// must not reach them and must not lose its place among the originals.
		for i := 0; i < 100 && writes < 1000; i++ {
			h.advance()
			h.write(t, fmt.Sprintf("interleaved %04d", writes), 0.5, 100, h.now())
			writes++
		}

		if !res.HasMore || res.NextCursor == "" {
			break
		}
		cursor = res.NextCursor
		if page > 500 {
			t.Fatal("the listing did not terminate")
		}
	}
	if writes != 1000 {
		t.Fatalf("the test wrote %d records during the listing, want 1000", writes)
	}

	for rid := range want {
		switch seen[rid] {
		case 0:
			t.Fatalf("memory %s was skipped by a listing that ran while records were being written", rid)
		case 1:
		default:
			t.Fatalf("memory %s came back %d times", rid, seen[rid])
		}
	}
}

// TestListReadsOnlyThePageItReturns holds the completion criterion, measured
// rather than asserted: listing ten from a large corpus reads ten record
// bodies, at every page including the last.
func TestListReadsOnlyThePageItReturns(t *testing.T) {
	const corpus = 500
	h := newListHarness(t)
	h.seedGradient(t, corpus)

	cursor := ""
	pages := 0
	for {
		h.counter.reset()
		res, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 10, Cursor: cursor})
		must(t, err)
		pages++

		if got := h.counter.bodies; got > len(res.Memories) {
			t.Fatalf("page %d returned %d memories and read %d record bodies; a listing reads "+
				"exactly the page it returns", pages, len(res.Memories), got)
		}
		if !res.HasMore || res.NextCursor == "" {
			break
		}
		cursor = res.NextCursor
	}
	if pages < corpus/10 {
		t.Fatalf("the listing finished after %d pages, want at least %d", pages, corpus/10)
	}
}

func TestListExcludesArchivedUnlessAsked(t *testing.T) {
	h := newListHarness(t)
	ids := h.seedGradient(t, 6)
	must(t, h.svc.Delete(acmeCtx(), ids[0], false))
	must(t, h.svc.Delete(acmeCtx(), ids[1], false))

	live, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 50})
	must(t, err)
	if len(live.Memories) != 4 {
		t.Errorf("the default listing returned %d memories, want 4: archiving is a retirement "+
			"from retrieval and a listing is retrieval", len(live.Memories))
	}

	all, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 50, IncludeArchived: true})
	must(t, err)
	if len(all.Memories) != 6 {
		t.Errorf("the widened listing returned %d memories, want 6: an archived memory is "+
			"retired, never deleted", len(all.Memories))
	}
}

func TestListRefusesACursorFromAnotherTenant(t *testing.T) {
	h := newListHarness(t)
	h.seedGradient(t, 10)

	first, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 2})
	must(t, err)
	if first.NextCursor == "" {
		t.Fatal("the first page returned no cursor to resume from")
	}

	_, err = h.svc.List(tenantCtx("globex"), memory.ListReq{OrderBy: "created_at", Limit: 2, Cursor: first.NextCursor})
	if !errs.Is(err, errs.Invalid) {
		t.Errorf("another tenant resumed acme's listing and got %v; want errs.Invalid", err)
	}
}

func TestListRefusesACursorFromADifferentOrdering(t *testing.T) {
	h := newListHarness(t)
	h.seedGradient(t, 10)

	first, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 2})
	must(t, err)

	_, err = h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "importance", Limit: 2, Cursor: first.NextCursor})
	if !errs.Is(err, errs.Invalid) {
		t.Errorf("resuming under a different ordering gave %v, want errs.Invalid: the token names "+
			"a position in an index the new ordering does not walk", err)
	}
}

// TestFilteredSearchWidensRatherThanUnderReturning: a filter matching one
// candidate in twenty still returns a full page while inside the budget.
func TestFilteredSearchWidensRatherThanUnderReturning(t *testing.T) {
	h := newListHarness(t)
	ids := h.seedGradient(t, 100)
	for i, rid := range ids {
		if i%20 != 0 {
			must(t, h.svc.Delete(acmeCtx(), rid, false))
		}
	}

	res, err := h.svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "memory", Limit: 5})
	must(t, err)
	if len(res.Results) != 5 {
		t.Errorf("a search over a corpus where 1 in 20 is live returned %d results, want 5: "+
			"without widening it would return whatever survived the first few candidates",
			len(res.Results))
	}
	if res.Truncated {
		t.Error("the search reported truncation while inside its widening budget")
	}
}

// TestTruncatedIsSetAtTheWideningBound and TestTruncatedIsFalseWhenComplete are
// the same distinction from both sides: Truncated must be true exactly when the
// budget ran out, never as a proxy for "no results".
func TestTruncatedIsSetAtTheWideningBound(t *testing.T) {
	// Bound the materialised ranking at ten as well as the page: pagination
	// otherwise searches to its default depth and exhausts this corpus.
	h := newListHarnessWithConfig(t, memory.Config{MaxPageDepth: 10})
	ids := h.seedGradient(t, 400)
	for i, rid := range ids {
		if i != 0 {
			must(t, h.svc.Delete(acmeCtx(), rid, false))
		}
	}

	res, err := h.svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "memory", Limit: 10})
	must(t, err)
	if !res.Truncated {
		t.Error("a search whose filter matched 1 in 400, with a budget of 320, reported no truncation")
	}
	if len(res.Results) >= 10 {
		t.Errorf("the truncated search returned a full page of %d; the point of the flag is that "+
			"the page is short and the caller is told why", len(res.Results))
	}
}

// A listing widens further than a search, as Rust's does (list_max_factor 128
// against widen_max_factor 32, config.rs:290-300). A listing candidate costs one
// attribute-row read on an index already being walked, where widening a search
// re-runs a similarity traversal, so sharing the search's number truncates
// ordinary filtered listings.
//
// Six live memories among 600, one every hundredth: a page of five in creation
// order has to examine 401 candidates. That is past 32 × 5 = 160 and within
// 128 × 5 = 640. Until Phase 13 Go's listing used the search's factor and
// returned two memories marked truncated.
func TestAListingWidensToItsOwnBound(t *testing.T) {
	h := newListHarness(t)
	ids := h.seedGradient(t, 600)
	for i, rid := range ids {
		if i%100 != 0 {
			must(t, h.svc.Delete(acmeCtx(), rid, false))
		}
	}

	res, err := h.svc.List(acmeCtx(), memory.ListReq{OrderBy: "created_at", Limit: 5})
	must(t, err)
	if len(res.Memories) != 5 || res.Truncated {
		t.Fatalf("a listing needing 401 candidates for a page of 5 returned %d memories, truncated=%t; "+
			"want 5 and not truncated, inside list_max_factor 128", len(res.Memories), res.Truncated)
	}
}

func TestTruncatedIsFalseWhenComplete(t *testing.T) {
	h := newListHarness(t)
	h.seedGradient(t, 20)

	res, err := h.svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "memory", Limit: 10})
	must(t, err)
	if res.Truncated {
		t.Error("an unfiltered search over a corpus larger than its limit reported truncation")
	}
	if len(res.Results) != 10 {
		t.Errorf("the search returned %d results, want 10", len(res.Results))
	}
}

func TestTruncatedIsNotSetForAnEmptyCorpus(t *testing.T) {
	h := newListHarness(t)

	res, err := h.svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "anything", Limit: 10})
	must(t, err)
	if res.Truncated {
		t.Error("a search over an empty corpus reported truncation; Truncated must never stand " +
			"in for 'there are none'")
	}

	list, err := h.svc.List(acmeCtx(), memory.ListReq{Limit: 10})
	must(t, err)
	if list.Truncated || list.HasMore || list.NextCursor != "" {
		t.Errorf("an empty listing reported truncated=%t has_more=%t cursor=%q",
			list.Truncated, list.HasMore, list.NextCursor)
	}
}

// --- harness ----------------------------------------------------------------

type listHarness struct {
	svc      *memory.Service
	repo     record.Repo
	kv       storage.KV
	counter  *countingKV
	clk      *clock.Fake
	embedder *embeddingtest.Fake
	tick     time.Duration
}

func newListHarness(t *testing.T) *listHarness {
	t.Helper()
	return newListHarnessWithConfig(t, memory.Config{})
}

func newListHarnessWithConfig(t *testing.T, cfg memory.Config) *listHarness {
	t.Helper()
	base := memkv.New()
	t.Cleanup(func() { _ = base.Close() })

	counter := &countingKV{KV: base}
	clk := clock.NewFake(clock.FakeStart)
	embedder := embeddingtest.New()
	repo := record.NewRepo(counter, record.WithIndexer(attr.NewIndexer(attr.MustTable())))
	svc := memory.New(counter, repo, flat.New(vector.NewStore(counter), distance.L2),
		embedder, clk, attr.MustTable(), graph.NewService(counter, clk), cfg)

	t.Cleanup(func() { _ = svc.Close() })
	return &listHarness{
		svc: svc, repo: repo, kv: counter, counter: counter,
		clk: clk, embedder: embedder, tick: time.Second,
	}
}

func (h *listHarness) now() time.Time { return h.clk.Now().UTC() }
func (h *listHarness) advance()       { h.clk.Advance(h.tick) }

// seedGradient writes n memories whose every indexed slot increases with the
// index, so one corpus exercises all five orderings and each is checkable
// against the content.
func (h *listHarness) seedGradient(t *testing.T, n int) []id.ID {
	t.Helper()
	out := make([]id.ID, n)
	for i := 0; i < n; i++ {
		h.advance()
		when := h.now()
		out[i] = h.write(t,
			fmt.Sprintf("memory %02d", i),
			float32(i+1)/float32(n+1), // importance, strictly increasing
			float32(i+1),              // health, strictly increasing
			when,                      // created/updated/last recalled
		)
	}
	return out
}

// write stores one record directly, so the lifecycle fields can be set. The
// service's Create path does not expose them before Phase 10, and a listing
// ordered by importance needs importances that differ.
func (h *listHarness) write(t *testing.T, content string, importance, health float32, when time.Time) id.ID {
	t.Helper()
	ctx := acmeCtx()
	rec := &record.Record{
		ID:        id.New(),
		Tenant:    "acme",
		Namespace: tenant.DefaultNamespace,
		Type:      record.TypeMemory,
		Content:   content,
		CreatedAt: when,
		UpdatedAt: when,
		Fields: record.Fields{
			Policy:         record.DefaultPolicy,
			Importance:     importance,
			Health:         health,
			LastRecalledAt: when,
		},
		Vectors: map[string]*vector.Vector{
			record.VectorContent: h.embed(t, content),
		},
	}
	tx := txn.New(h.kv)
	defer tx.Close()
	if err := h.repo.Put(ctx, tx, rec); err != nil {
		t.Fatalf("writing a seed record: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("committing a seed record: %v", err)
	}
	return rec.ID
}

// embed produces a seed record's vector through the same fake the service uses,
// so the index compares vectors of one width from one model.
func (h *listHarness) embed(t *testing.T, content string) *vector.Vector {
	t.Helper()
	vs, err := h.embedder.Embed(context.Background(), []string{content})
	if err != nil {
		t.Fatalf("embedding a seed record: %v", err)
	}
	return &vector.Vector{ModelID: h.embedder.ModelID(), Dim: len(vs[0]), Values: vs[0]}
}

// pageAll walks a listing to its end and returns the ids in order.
func (h *listHarness) pageAll(t *testing.T, req memory.ListReq) []id.ID {
	t.Helper()
	var out []id.ID
	for page := 0; ; page++ {
		res, err := h.svc.List(acmeCtx(), req)
		must(t, err)
		for _, m := range res.Memories {
			out = append(out, m.ID)
		}
		if !res.HasMore || res.NextCursor == "" {
			return out
		}
		req.Cursor = res.NextCursor
		if page > 1000 {
			t.Fatal("the listing did not terminate")
		}
	}
}

// countingKV counts reads by key space, so "reads only the page it returns" is
// measured against the record space rather than trusted.
type countingKV struct {
	storage.KV
	mu     sync.Mutex
	rows   int
	bodies int
	// vectors counts canonical vector reads through a snapshot only, which is
	// how a page is materialised; the flat index scans the store itself.
	vectors int
}

func (c *countingKV) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows, c.bodies, c.vectors = 0, 0, 0
}

func (c *countingKV) count(key []byte) {
	_, _, space, err := keys.ParseSpace(key)
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	switch space {
	case keys.SpaceAttrRow:
		c.rows++
	case keys.SpaceRecord:
		c.bodies++
	}
}

func (c *countingKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	c.count(key)
	return c.KV.Get(ctx, key)
}

func (c *countingKV) NewSnapshot() storage.Snapshot {
	return &countingSnapshot{Snapshot: c.KV.NewSnapshot(), kv: c}
}

type countingSnapshot struct {
	storage.Snapshot
	kv *countingKV
}

func (s *countingSnapshot) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.kv.count(key)
	s.kv.countVector(key)
	return s.Snapshot.Get(ctx, key)
}

// NewIterator counts seeks as reads, so a read moved from Get onto an iterator
// is still counted rather than vanishing from the measurement.
func (s *countingSnapshot) NewIterator(lower, upper []byte) storage.Iterator {
	return &countingIterator{Iterator: s.Snapshot.NewIterator(lower, upper), kv: s.kv}
}

type countingIterator struct {
	storage.Iterator
	kv *countingKV
}

func (i *countingIterator) SeekGE(key []byte) bool {
	i.kv.count(key)
	i.kv.countVector(key)
	return i.Iterator.SeekGE(key)
}

func (c *countingKV) countVector(key []byte) {
	if _, _, space, err := keys.ParseSpace(key); err == nil && space == keys.SpaceVector {
		c.mu.Lock()
		c.vectors++
		c.mu.Unlock()
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle ||
		len(needle) == 0 || indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
