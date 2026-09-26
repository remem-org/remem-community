package memory

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/query"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
	"pgregory.net/rapid"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if f := flag.Lookup("rapid.checks"); f != nil {
		n, err := strconv.Atoi(f.Value.String())
		if err != nil {
			panic(err)
		}
		if n < 1000 {
			if err := f.Value.Set("1000"); err != nil {
				panic(err)
			}
		}
	}
	os.Exit(m.Run())
}

type searchTB interface {
	Helper()
	Fatalf(string, ...any)
	Cleanup(func())
}

func newSearchPagingService(t searchTB, kv storage.KV, cfg Config) (context.Context, *Service) {
	t.Helper()
	if kv == nil {
		kv = memkv.New()
	}
	clk := clock.NewFake(clock.FakeStart)
	texts := text.New()
	repo := record.NewRepo(kv, record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(texts))
	svc := New(kv, repo, flat.New(vector.NewStore(kv), distance.L2), embeddingtest.New(), clk,
		attr.MustTable(), graph.NewService(kv, clk), cfg, WithTextIndex(texts))
	t.Cleanup(func() { _ = svc.Close(); _ = kv.Close() })
	return tenant.NewContext(context.Background(), "acme"), svc
}

func seedSearchCorpus(t searchTB, ctx context.Context, svc *Service, n int) {
	t.Helper()
	reqs := make([]CreateReq, n)
	for i := range reqs {
		reqs[i] = CreateReq{Content: fmt.Sprintf("alpha %s topic %d", strings.Repeat("alpha ", i%5), i), Tags: []string{"one", "two"}}
	}
	if _, err := svc.CreateBatch(ctx, reqs); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// Re-executing at each page's depth changes RRF scores and can repeat or omit hits.
func TestPagingASearchReturnsEveryResultExactlyOnce(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		corpus := rapid.IntRange(1, 60).Draw(rt, "corpus")
		pageSize := rapid.IntRange(1, 12).Draw(rt, "page")
		mode := rapid.SampledFrom([]SearchType{SearchSemantic, SearchKeyword, SearchHybrid}).Draw(rt, "mode")
		ctx, svc := newSearchPagingService(rt, nil, Config{})
		seedSearchCorpus(rt, ctx, svc, corpus)
		whole, err := svc.Search(ctx, SearchReq{Query: "alpha", Type: mode, Limit: corpus, Explain: true})
		if err != nil {
			rt.Fatalf("unpaged search: %v", err)
		}
		if len(whole.Results) != corpus {
			rt.Fatalf("whole got %d, want %d", len(whole.Results), corpus)
		}
		var paged []Result
		var cursor string
		for pages := 0; ; pages++ {
			if pages > corpus {
				rt.Fatalf("pagination did not terminate")
			}
			res, err := svc.Search(ctx, SearchReq{Query: "alpha", Type: mode, Limit: pageSize, Cursor: cursor, Explain: true})
			if err != nil {
				rt.Fatalf("paged search: %v", err)
			}
			paged = append(paged, res.Results...)
			if res.HasMore != (res.NextCursor != "") {
				rt.Fatalf("cursor and has_more disagree: %+v", res)
			}
			if !res.HasMore {
				break
			}
			cursor = res.NextCursor
		}
		if !reflect.DeepEqual(paged, whole.Results) {
			rt.Fatalf("paged results differ from whole ranking: %d vs %d hits", len(paged), len(whole.Results))
		}
	})
}

func TestASearchCursorFromAnotherQueryIsRefused(t *testing.T) {
	for _, field := range []string{"query", "mode", "tags", "archived", "explain"} {
		t.Run(field, func(t *testing.T) {
			ctx, svc := newSearchPagingService(t, nil, Config{})
			seedSearchCorpus(t, ctx, svc, 20)
			req := SearchReq{Query: "alpha", Type: SearchHybrid, Limit: 5}
			first, err := svc.Search(ctx, req)
			if err != nil || first.NextCursor == "" {
				t.Fatalf("first page: %+v, %v", first, err)
			}
			req.Cursor = first.NextCursor
			switch field {
			case "query":
				req.Query = "beta"
			case "mode":
				req.Type = SearchSemantic
			case "tags":
				req.Tags = []string{"one"}
			case "archived":
				req.IncludeArchived = true
			case "explain":
				req.Explain = true
			}
			if _, err := svc.Search(ctx, req); !errs.Is(err, errs.Invalid) {
				t.Fatalf("changed %s returned %v, want Invalid", field, err)
			}
		})
	}
}

// Every normalized query input that affects a materialised answer must bind
// the session. Limit and Cursor alone can vary while consuming that answer.
func TestSearchFingerprintBindsTheWholeQuestion(t *testing.T) {
	_, svc := newSearchPagingService(t, nil, Config{})
	anchor := id.New()
	base := query.Query{Tenant: "acme", Text: "alpha", Keyword: true, Limit: 5}
	want := svc.searchFingerprint(&base, false)
	for name, change := range map[string]func(*query.Query){
		"tenant":     func(q *query.Query) { q.Tenant = "globex" },
		"namespace":  func(q *query.Query) { q.Namespace = "other" },
		"text":       func(q *query.Query) { q.Text = "beta" },
		"keyword":    func(q *query.Query) { q.Keyword = false },
		"vector":     func(q *query.Query) { q.Vector = []float32{0.25, 0.5} },
		"tags":       func(q *query.Query) { q.Tags = []string{"one"} },
		"predicates": func(q *query.Query) { q.Preds = attr.Preds{attr.Eq(attr.SlotArchived, attr.Bool(false))} },
		"related":    func(q *query.Query) { q.RelatedTo = &anchor },
		"depth":      func(q *query.Query) { q.RelatedDepth = 2 },
		"types":      func(q *query.Query) { q.RelatedTypes = []graph.RelationshipType{graph.SimilarTo} },
		"direction":  func(q *query.Query) { q.RelatedDirection = graph.In },
		"strength":   func(q *query.Query) { q.RelatedMinStrength = 0.7 },
		"ordering":   func(q *query.Query) { q.OrderBy = attr.SlotCreatedAt },
		"descending": func(q *query.Query) { q.Desc = true },
		"explain":    func(q *query.Query) { q.Explain = true },
	} {
		t.Run(name, func(t *testing.T) {
			q := base
			change(&q)
			if svc.searchFingerprint(&q, false) == want {
				t.Fatal("changed question reused fingerprint")
			}
		})
	}
	if svc.searchFingerprint(&base, true) == want {
		t.Fatal("archive flag did not bind")
	}
	base.Limit = 99
	base.Cursor = &query.Cursor{}
	if svc.searchFingerprint(&base, false) != want {
		t.Fatal("page position or size changed fingerprint")
	}
	a, b := base, base
	a.Tags = []string{"one\x00tag=two"}
	b.Tags = []string{"one", "two"}
	if svc.searchFingerprint(&a, false) == svc.searchFingerprint(&b, false) {
		t.Fatal("ambiguous string framing")
	}
	a, b = base, base
	a.RelatedTo, b.RelatedTo = &anchor, &anchor
	a.RelatedMinStrength, b.RelatedMinStrength = 0.7000001, 0.7000002
	if svc.searchFingerprint(&a, false) == svc.searchFingerprint(&b, false) {
		t.Fatal("strength precision lost")
	}
}

func TestASearchCursorFromAnotherTenantIsRefused(t *testing.T) {
	ctx, svc := newSearchPagingService(t, nil, Config{})
	seedSearchCorpus(t, ctx, svc, 20)
	first, err := svc.Search(ctx, SearchReq{Query: "alpha", Type: SearchKeyword, Limit: 5})
	if err != nil || first.NextCursor == "" {
		t.Fatalf("first page: %+v, %v", first, err)
	}
	_, err = svc.Search(tenant.NewContext(ctx, "globex"), SearchReq{Query: "alpha", Type: SearchKeyword, Limit: 5, Cursor: first.NextCursor})
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("another tenant: %v, want Invalid", err)
	}
}

func TestASearchCursorIsRefusedAfterRestart(t *testing.T) {
	dir := t.TempDir()
	kv, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, svc := newSearchPagingService(t, kv, Config{SyncWrites: true})
	seedSearchCorpus(t, ctx, svc, 20)
	first, err := svc.Search(ctx, SearchReq{Query: "alpha", Type: SearchKeyword, Limit: 5})
	if err != nil || first.NextCursor == "" {
		t.Fatalf("first page: %+v, %v", first, err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	kv2, err := pebble.Open(dir, pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx2, svc2 := newSearchPagingService(t, kv2, Config{})
	_, err = svc2.Search(ctx2, SearchReq{Query: "alpha", Type: SearchKeyword, Limit: 5, Cursor: first.NextCursor})
	if !errs.Is(err, errs.Invalid) || !strings.Contains(err.Error(), "start the search again") {
		t.Fatalf("restart refusal: %v", err)
	}
}

func TestAnEmptySearchPageHandsBackNoCursor(t *testing.T) {
	ctx, svc := newSearchPagingService(t, nil, Config{})
	seedSearchCorpus(t, ctx, svc, 5)
	res, err := svc.Search(ctx, SearchReq{Query: "nothingmatchesthis", Type: SearchKeyword, Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 0 || res.NextCursor != "" || res.HasMore {
		t.Fatalf("empty page: %+v", res)
	}
}

// Deleting records after page one must not remove their bodies from page two.
func TestSearchPagingPinsBodiesAndAllowsChangingPageSize(t *testing.T) {
	ctx, svc := newSearchPagingService(t, nil, Config{})
	seedSearchCorpus(t, ctx, svc, 12)
	whole, err := svc.Search(ctx, SearchReq{Query: "alpha", Type: SearchHybrid, Limit: 12})
	if err != nil {
		t.Fatal(err)
	}
	first, err := svc.Search(ctx, SearchReq{Query: "alpha", Type: SearchHybrid, Limit: 2})
	if err != nil || first.NextCursor == "" {
		t.Fatalf("first page: %+v, %v", first, err)
	}
	for _, hit := range whole.Results {
		if err := svc.Delete(ctx, hit.Memory.ID, true); err != nil {
			t.Fatal(err)
		}
	}
	second, err := svc.Search(ctx, SearchReq{Query: "alpha", Type: SearchHybrid, Limit: 10, Cursor: first.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second.Results, whole.Results[2:]) || second.HasMore || second.NextCursor != "" {
		t.Fatalf("snapshot continuation returned %+v", second)
	}
}

func TestSearchPagingReportsDepthTruncationOnEveryPage(t *testing.T) {
	ctx, svc := newSearchPagingService(t, nil, Config{MaxPageDepth: 7})
	seedSearchCorpus(t, ctx, svc, 10)
	req := SearchReq{Query: "alpha", Type: SearchKeyword, Limit: 3}
	for page, want := range []int{3, 3, 1} {
		res, err := svc.Search(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Results) != want || !res.Truncated || res.HasMore != (page < 2) || (res.NextCursor != "") != res.HasMore {
			t.Fatalf("page %d: %+v", page, res)
		}
		req.Cursor = res.NextCursor
	}
}

func TestASearchCursorRefusesMalformedPositions(t *testing.T) {
	ctx, svc := newSearchPagingService(t, nil, Config{})
	seedSearchCorpus(t, ctx, svc, 5)
	first, err := svc.Search(ctx, SearchReq{Query: "alpha", Type: SearchKeyword, Limit: 1})
	if err != nil || first.NextCursor == "" {
		t.Fatalf("first page: %+v, %v", first, err)
	}
	p, err := codec.DecodeToken(first.NextCursor, codec.TokenSearch, "acme")
	if err != nil {
		t.Fatal(err)
	}
	for _, pos := range [][]byte{nil, {0x80}, {1, 0}, binary.AppendUvarint(nil, ^uint64(0)), binary.AppendUvarint(nil, 1000), {0}} {
		payload := append(append([]byte(nil), p[:16]...), pos...)
		_, err := svc.Search(ctx, SearchReq{Query: "alpha", Type: SearchKeyword, Limit: 1, Cursor: codec.EncodeToken(codec.TokenSearch, "acme", payload)})
		if !errs.Is(err, errs.Invalid) {
			t.Fatalf("position %x returned %v, want Invalid", pos, err)
		}
	}
}
