package memory_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func acmeCtx() context.Context { return tenantCtx("acme") }
func tenantCtx(t tenant.ID) context.Context {
	return tenant.NewContext(context.Background(), t)
}

type option func(*setup)

type setup struct {
	embedder embedding.Embedder
	clk      *clock.Fake
	cfg      memory.Config
	// extra are service options beyond the ones newService always wires.
	extra []memory.Option
}

func withEmbedder(e embedding.Embedder) option { return func(s *setup) { s.embedder = e } }
func withConfig(c memory.Config) option        { return func(s *setup) { s.cfg = c } }
func withServiceOption(o memory.Option) option {
	return func(s *setup) { s.extra = append(s.extra, o) }
}

func newService(t *testing.T, opts ...option) *memory.Service {
	t.Helper()
	s := &setup{embedder: embeddingtest.New(), clk: clock.NewFake(clock.FakeStart)}
	for _, o := range opts {
		o(s)
	}
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	// Both derived indexes, as the server wires them: a test service that
	// indexed only attributes would let a keyword search pass here and fail in
	// production.
	texts := text.New()
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(texts))
	index := flat.New(vector.NewStore(kv), distance.L2)
	svc := memory.New(kv, repo, index, s.embedder, s.clk, attr.MustTable(),
		graph.NewService(kv, s.clk), s.cfg, append([]memory.Option{memory.WithTextIndex(texts)}, s.extra...)...)
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

// newRecordingService is newService with durable recall recording on, which is
// how the server wires it. It is a separate constructor rather than the default
// so that a test asserting "nothing recorded a recall" is asserting about a
// service that could have.
func newRecordingService(t *testing.T, opts ...option) (*memory.Service, *clock.Fake) {
	t.Helper()
	s := &setup{embedder: embeddingtest.New(), clk: clock.NewFake(clock.FakeStart)}
	for _, o := range opts {
		o(s)
	}
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	texts := text.New()
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(texts))
	index := flat.New(vector.NewStore(kv), distance.L2)
	svc := memory.New(kv, repo, index, s.embedder, s.clk, attr.MustTable(),
		graph.NewService(kv, s.clk), s.cfg,
		memory.WithTextIndex(texts),
		memory.WithRecall(events.NewStore(kv), 0))
	t.Cleanup(func() { _ = svc.Close() })
	return svc, s.clk
}

func seed(t *testing.T, svc *memory.Service, contents ...string) []id.ID {
	t.Helper()
	return seedAs(t, svc, "acme", contents...)
}

func seedAs(t *testing.T, svc *memory.Service, tn tenant.ID, contents ...string) []id.ID {
	t.Helper()
	reqs := make([]memory.CreateReq, len(contents))
	for i, c := range contents {
		reqs[i] = memory.CreateReq{Content: c}
	}
	got, err := svc.CreateBatch(tenantCtx(tn), reqs)
	must(t, err)
	ids := make([]id.ID, len(got))
	for i, m := range got {
		ids[i] = m.ID
	}
	return ids
}

func TestCreateEmbedsOnceAndStoresAtomically(t *testing.T) {
	e := embeddingtest.New()
	svc := newService(t, withEmbedder(e))
	m, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "the sky is blue"})
	must(t, err)
	if e.Calls() != 1 {
		t.Fatalf("embedded %d times, want 1", e.Calls())
	}

	got, err := svc.Get(acmeCtx(), m.ID, memory.GetOpts{})
	must(t, err)
	if got.Content != "the sky is blue" {
		t.Fatalf("got %q", got.Content)
	}
}

func TestCreateBatchEmbedsInOneCall(t *testing.T) {
	e := embeddingtest.New()
	svc := newService(t, withEmbedder(e))
	reqs := make([]memory.CreateReq, 20)
	for i := range reqs {
		reqs[i] = memory.CreateReq{Content: fmt.Sprintf("memory %d", i)}
	}
	got, err := svc.CreateBatch(acmeCtx(), reqs)
	must(t, err)
	if len(got) != 20 {
		t.Fatalf("got %d memories", len(got))
	}
	if e.Calls() != 1 {
		t.Fatalf("embedded in %d calls, want 1 batch", e.Calls())
	}
}

// Ranking, ordering and the score contract, over the deterministic fake
// embedder. What this holds is the plumbing: that results come back nearest
// first, that scores fall with relevance and stay inside [0, 1], and that the
// ordering value is reported separately. The genuinely semantic version of
// this test — "feline biology" finding "cats are mammals" — needs the real
// model and lives in semantic_onnx_test.go, behind the onnx build tag.
func TestSemanticSearchRanksByDistance(t *testing.T) {
	svc := newService(t)
	ids := seed(t, svc,
		"cats are mammals and cats purr",
		"dogs are mammals",
		"quantum chromodynamics",
	)
	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "cats purr mammals", Limit: 3})
	must(t, err)
	if len(res.Results) != 3 {
		t.Fatalf("got %d results", len(res.Results))
	}
	if res.Results[0].Memory.ID != ids[0] {
		t.Fatalf("closest match should rank first, got %v", res.Results[0].Memory.Content)
	}
	if res.Results[0].Score <= res.Results[2].Score {
		t.Fatal("scores must decrease with relevance")
	}
	for _, r := range res.Results {
		if r.Score < 0 || r.Score > 1 {
			t.Fatalf("score %f outside [0,1]", r.Score)
		}
	}
	for i := 1; i < len(res.Results); i++ {
		if res.Results[i-1].Distance > res.Results[i].Distance {
			t.Fatal("results are not ordered by distance")
		}
	}
}

// Behaviour baseline §1.4: a plain semantic search bypasses rank fusion, so
// FusedScore is 1/(1+distance) and is on a different scale from Score. Both
// are reported, because a client that sorts by the wrong one gets a different
// order than the server intended.
func TestScoreAndFusedScoreAreDifferentNumbers(t *testing.T) {
	svc := newService(t)
	seed(t, svc, "the sky is blue")
	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "the sky is blue", Limit: 1})
	must(t, err)

	r := res.Results[0]
	if r.Score < 0.99 {
		t.Fatalf("an exact match should recover a cosine near 1, got %v", r.Score)
	}
	if r.FusedScore != distance.Score(r.Distance) {
		t.Fatalf("FusedScore = %v, want the ordering value %v", r.FusedScore, distance.Score(r.Distance))
	}
}

func TestArchivedMemoriesLeaveTheVectorIndex(t *testing.T) {
	svc := newService(t)
	ids := seed(t, svc, "findable content")
	must(t, svc.Delete(acmeCtx(), ids[0], false)) // soft = archive

	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "findable content", Limit: 10})
	must(t, err)
	if len(res.Results) != 0 {
		t.Fatal("archived memories must not surface in search")
	}

	// But the record survives until cleanup.
	got, err := svc.Get(acmeCtx(), ids[0], memory.GetOpts{IncludeArchived: true})
	if err != nil {
		t.Fatalf("archiving is not deletion: %v", err)
	}
	if !got.Archived || got.ArchivedAt.IsZero() {
		t.Fatalf("the archived state was not recorded: %+v", got)
	}

	// And an ordinary read does not resurrect it.
	if _, err := svc.Get(acmeCtx(), ids[0], memory.GetOpts{}); !errs.Is(err, errs.NotFound) {
		t.Fatalf("an archived memory is visible to an ordinary read: %v", err)
	}
}

func TestSearchIsTenantIsolated(t *testing.T) {
	svc := newService(t)
	seedAs(t, svc, "acme", "acme's private note")
	res, err := svc.Search(tenantCtx("other"), memory.SearchReq{Type: memory.SearchSemantic, Query: "private note", Limit: 10})
	must(t, err)
	if len(res.Results) != 0 {
		t.Fatal("cross-tenant leak")
	}
}

// A memory in another tenant is absent, never forbidden: "you may not see
// this" confirms it exists, which is itself a cross-tenant disclosure.
func TestGetIsTenantIsolatedAndReportsAbsence(t *testing.T) {
	svc := newService(t)
	ids := seedAs(t, svc, "acme", "acme's private note")
	if _, err := svc.Get(tenantCtx("other"), ids[0], memory.GetOpts{}); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
}

func TestAnUnscopedRequestIsRefused(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()
	if _, err := svc.Create(ctx, memory.CreateReq{Content: "x"}); err == nil {
		t.Fatal("Invariant 1: Create without a tenant")
	}
	if _, err := svc.Search(ctx, memory.SearchReq{Type: memory.SearchSemantic, Query: "x"}); err == nil {
		t.Fatal("Invariant 1: Search without a tenant")
	}
	if _, err := svc.Get(ctx, id.New(), memory.GetOpts{}); err == nil {
		t.Fatal("Invariant 1: Get without a tenant")
	}
}

// A hard delete is the only path that destroys user data, which is why it is a
// parameter a caller passes rather than a threshold something crosses.
func TestHardDeleteRemovesTheRecordAndItsVector(t *testing.T) {
	svc := newService(t)
	ids := seed(t, svc, "goodbye")
	must(t, svc.Delete(acmeCtx(), ids[0], true))

	if _, err := svc.Get(acmeCtx(), ids[0], memory.GetOpts{IncludeArchived: true}); !errs.Is(err, errs.NotFound) {
		t.Fatalf("Get after a hard delete = %v", err)
	}
	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "goodbye", Limit: 10})
	must(t, err)
	if len(res.Results) != 0 {
		t.Fatal("a hard-deleted memory still ranks; its vector was orphaned")
	}
}

func TestArchivingTwiceIsNotAnError(t *testing.T) {
	svc := newService(t)
	ids := seed(t, svc, "retire me")
	must(t, svc.Delete(acmeCtx(), ids[0], false))
	must(t, svc.Delete(acmeCtx(), ids[0], false))
}

func TestDeletingSomethingThatIsNotThereIsNotFound(t *testing.T) {
	svc := newService(t)
	if err := svc.Delete(acmeCtx(), id.New(), false); !errs.Is(err, errs.NotFound) {
		t.Fatalf("got %v, want NotFound", err)
	}
}

// Widening is what makes archived memories filterable without a per-candidate
// predicate. With many archived memories in front of the live ones, the search
// must still find the live ones rather than returning an empty page.
func TestSearchWidensPastArchivedCandidates(t *testing.T) {
	svc := newService(t)
	var archived []id.ID
	for range 30 {
		archived = append(archived, seed(t, svc, "the sky is blue")...)
	}
	live := seed(t, svc, "the sky is blue and clear")
	for _, a := range archived {
		must(t, svc.Delete(acmeCtx(), a, false))
	}

	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "the sky is blue", Limit: 5})
	must(t, err)
	if len(res.Results) != 1 || res.Results[0].Memory.ID != live[0] {
		t.Fatalf("widening did not reach past the archived candidates: %d results", len(res.Results))
	}
}

// Truncated is the difference between "these are all the matches" and "these
// are all the matches I looked at". A search that filled its limit has not
// truncated anything.
func TestTruncatedIsFalseWhenTheLimitIsFilled(t *testing.T) {
	svc := newService(t)
	seed(t, svc, "one", "two", "three")
	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "one", Limit: 2})
	must(t, err)
	if res.Truncated {
		t.Fatal("a search that filled its limit reported truncation")
	}
}

func TestSearchDefaultsAndCapsTheLimit(t *testing.T) {
	svc := newService(t, withConfig(memory.Config{DefaultLimit: 2, MaxLimit: 5}))
	seed(t, svc, "a a", "a b", "a c", "a d")

	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "a"})
	must(t, err)
	if len(res.Results) != 2 {
		t.Fatalf("an unspecified limit returned %d results, want the configured default of 2", len(res.Results))
	}

	if _, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "a", Limit: 6}); !errs.Is(err, errs.Invalid) {
		t.Fatalf("a limit above the maximum = %v, want Invalid", err)
	}
}

func TestCreateRefusesEmptyOrOversizedContent(t *testing.T) {
	svc := newService(t)
	for name, req := range map[string]memory.CreateReq{
		"empty":      {Content: ""},
		"whitespace": {Content: "   \n\t "},
		"oversized":  {Content: strings.Repeat("x", memory.MaxContentBytes+1)},
		"blank tag":  {Content: "fine", Tags: []string{"ok", "  "}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := svc.Create(acmeCtx(), req); !errs.Is(err, errs.Invalid) {
				t.Fatalf("got %v, want Invalid", err)
			}
		})
	}
}

// A batch commits or it does not. A partial batch would leave an agent with no
// way to know which of its twenty facts were stored.
func TestABatchWithOneBadMemoryStoresNothing(t *testing.T) {
	svc := newService(t)
	_, err := svc.CreateBatch(acmeCtx(), []memory.CreateReq{
		{Content: "good"},
		{Content: ""},
	})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "good", Limit: 10})
	must(t, err)
	if len(res.Results) != 0 {
		t.Fatal("a rejected batch stored one of its memories anyway")
	}
}

func TestAFailedEmbeddingStoresNothing(t *testing.T) {
	e := embeddingtest.New()
	boom := errors.New("the model fell over")
	e.Fail(boom)
	svc := newService(t, withEmbedder(e))

	if _, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "unstorable"}); !errors.Is(err, boom) {
		t.Fatalf("got %v, want the embedder's error", err)
	}
	e.Fail(nil)
	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "unstorable", Limit: 10})
	must(t, err)
	if len(res.Results) != 0 {
		t.Fatal("a memory whose embedding failed was stored anyway")
	}
}

// Every stored vector carries the model that produced it (plan §II.10 row 10),
// so a later model change is detectable rather than silently corrupting the
// space.
func TestStoredVectorsCarryTheRunningModel(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	repo := record.NewRepo(kv, record.WithIndexer(attr.NewIndexer(attr.MustTable())))
	svc := memory.New(kv, repo, flat.New(vector.NewStore(kv), distance.L2),
		embeddingtest.New(), clock.NewFake(clock.FakeStart), attr.MustTable(), graph.NewService(kv, clock.NewFake(clock.FakeStart)), memory.Config{})
	t.Cleanup(func() { _ = svc.Close() })

	m, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "stamped"})
	must(t, err)

	rec, err := repo.Get(acmeCtx(), m.ID)
	must(t, err)
	v := rec.Vectors[record.VectorContent]
	if v == nil || v.ModelID != embedding.Model {
		t.Fatalf("stored vector = %+v, want model %q", v, embedding.Model)
	}
	if v.Dim != embedding.Dim {
		t.Fatalf("stored vector has %d dimensions", v.Dim)
	}
}

func TestTagsAreNormalisedAndDeduplicated(t *testing.T) {
	svc := newService(t)
	m, err := svc.Create(acmeCtx(), memory.CreateReq{
		Content: "tagged",
		Tags:    []string{"Weather", " weather ", "sky"},
	})
	must(t, err)
	if len(m.Tags) != 2 || m.Tags[0] != "weather" || m.Tags[1] != "sky" {
		t.Fatalf("tags = %v, want [weather sky]", m.Tags)
	}
}

func TestTimestampsComeFromTheInjectedClock(t *testing.T) {
	clk := clock.NewFake(clock.FakeStart)
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	svc := memory.New(kv, record.NewRepo(kv, record.WithIndexer(attr.NewIndexer(attr.MustTable()))), flat.New(vector.NewStore(kv), distance.L2),
		embeddingtest.New(), clk, attr.MustTable(), graph.NewService(kv, clk), memory.Config{})
	t.Cleanup(func() { _ = svc.Close() })

	m, err := svc.Create(acmeCtx(), memory.CreateReq{Content: "timed"})
	must(t, err)
	if !m.CreatedAt.Equal(clock.FakeStart) {
		t.Fatalf("CreatedAt = %v, want the fake clock's time %v", m.CreatedAt, clock.FakeStart)
	}
}

// A vector whose record is gone must not fail a search. The canonical record is
// the authority on what exists, and an index catching up is normal.
func TestAnOrphanedVectorIsSkippedRatherThanFailingTheSearch(t *testing.T) {
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	store := vector.NewStore(kv)
	svc := memory.New(kv, record.NewRepo(kv, record.WithIndexer(attr.NewIndexer(attr.MustTable()))), flat.New(store, distance.L2),
		embeddingtest.New(), clock.NewFake(clock.FakeStart), attr.MustTable(), graph.NewService(kv, clock.NewFake(clock.FakeStart)), memory.Config{})
	t.Cleanup(func() { _ = svc.Close() })

	seed(t, svc, "real memory")
	orphan := id.New()
	must(t, store.Put(acmeCtx(), "acme", tenant.DefaultNamespace, orphan, &vector.Vector{
		ModelID: embedding.Model,
		Dim:     embedding.Dim,
		Values:  make([]float32, embedding.Dim),
	}))

	res, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "real memory", Limit: 10})
	must(t, err)
	for _, r := range res.Results {
		if r.Memory.ID == orphan {
			t.Fatal("an orphaned vector was returned as a memory")
		}
	}
	if len(res.Results) != 1 {
		t.Fatalf("got %d results, want the one real memory", len(res.Results))
	}
}

func TestSearchNeedsAQuery(t *testing.T) {
	svc := newService(t)
	if _, err := svc.Search(acmeCtx(), memory.SearchReq{Type: memory.SearchSemantic, Query: "  "}); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}

func TestAnEmptyBatchIsRefused(t *testing.T) {
	svc := newService(t)
	if _, err := svc.CreateBatch(acmeCtx(), nil); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid", err)
	}
}
