package memory

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/codec"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/txn"
)

// Discarding Search's cursor must not leave one inaccessible snapshot per recall.
func TestRecallDoesNotExhaustPagingCapacity(t *testing.T) {
	ctx, svc := newSearchPagingService(t, nil, Config{})
	for i := 0; i < 12; i++ {
		if _, err := svc.Create(ctx, CreateReq{Content: "alpha " + strings.Repeat("x", 90)}); err != nil {
			t.Fatal(err)
		}
	}
	req := RecallReq{Context: "alpha", Type: SearchKeyword, TokenBudget: 100}
	first, err := svc.Recall(ctx, req)
	if err != nil || len(first.Memories) != 2 || first.UsedTokens != 80 || first.OmittedCount != 2 || first.Truncated {
		t.Fatalf("first recall: %+v, %v", first, err)
	}
	for i := 0; i < 130; i++ {
		got, err := svc.Recall(ctx, req)
		if err != nil {
			t.Fatalf("recall %d exhausted paging capacity: %v", i+2, err)
		}
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("recall changed its answer or budget: %+v, want %+v", got, first)
		}
	}
	if _, err := svc.List(ctx, ListReq{Limit: 1}); err != nil {
		t.Fatalf("recalls consumed shared listing capacity: %v", err)
	}
}

// Orphans consume ranked positions, not page slots. The second page catches
// advancing by returned bodies instead of the actual positions consumed.
func TestSearchPagesSkipDanglingGraphBodies(t *testing.T) {
	ctx, svc := newSearchPagingService(t, nil, Config{})
	var ids []id.ID
	for _, content := range []string{"anchor", "missing strong", "valid strong", "missing weak", "valid weak"} {
		m, err := svc.Create(ctx, CreateReq{Content: content})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, m.ID)
	}
	for i, strength := range []float32{0.9, 0.7, 0.5, 0.3} {
		if _, err := svc.Relate(ctx, RelateReq{From: ids[0], To: ids[i+1], Strength: strength}); err != nil {
			t.Fatal(err)
		}
	}
	if err := txn.Do(ctx, svc.kv, func(tx txn.Tx) error {
		if err := svc.repo.Delete(ctx, tx, ids[1]); err != nil {
			return err
		}
		return svc.repo.Delete(ctx, tx, ids[3])
	}); err != nil {
		t.Fatal(err)
	}
	// The text source contributes nothing; the graph still contributes all
	// four neighbours, and no archived predicate prunes the dangling rows.
	req := SearchReq{Query: "nomatchingterm", Type: SearchKeyword, RelatedTo: &ids[0], IncludeArchived: true, Limit: 1}
	first, err := svc.Search(ctx, req)
	if err != nil || len(first.Results) != 1 || first.Results[0].Memory.ID != ids[2] || !first.HasMore || first.NextCursor == "" {
		t.Fatalf("first page lost valid stronger neighbour: %+v, %v", first, err)
	}
	req.Cursor = first.NextCursor
	last, err := svc.Search(ctx, req)
	if err != nil || len(last.Results) != 1 || last.Results[0].Memory.ID != ids[4] || last.HasMore || last.NextCursor != "" {
		t.Fatalf("last page lost or repeated valid neighbour: %+v, %v", last, err)
	}
}

// Real snapshot reads with an injected delay model page work exceeding TTL.
type pageWorkKV struct {
	storage.KV
	onRead func()
}

func (kv *pageWorkKV) NewSnapshot() storage.Snapshot {
	return &pageWorkSnapshot{Snapshot: kv.KV.NewSnapshot(), kv: kv}
}

type pageWorkSnapshot struct {
	storage.Snapshot
	kv *pageWorkKV
}

func (s *pageWorkSnapshot) Get(ctx context.Context, key []byte) ([]byte, error) {
	if f := s.kv.onRead; f != nil {
		s.kv.onRead = nil
		f()
	}
	return s.Snapshot.Get(ctx, key)
}

// Expiring an active reader or renewing only at acquisition breaks the token
// returned by a slow page, even though the client has not been idle at all.
func TestRankedTTLStartsAfterPageWork(t *testing.T) {
	for _, reapDuringWork := range []bool{false, true} {
		name := "release renews deadline"
		if reapDuringWork {
			name = "active reader survives reaper"
		}
		t.Run(name, func(t *testing.T) {
			kv := &pageWorkKV{KV: memkv.New()}
			ctx, svc := newSearchPagingService(t, kv, Config{RankedTTL: 7 * time.Second})
			seedSearchCorpus(t, ctx, svc, 10)
			clk := svc.clk.(*clock.Fake)
			delayPage := func() {
				kv.onRead = func() {
					clk.Advance(8 * time.Second)
					if reapDuringWork {
						svc.paging.mu.Lock()
						svc.paging.expireLocked()
						svc.paging.mu.Unlock()
					}
				}
			}
			req := SearchReq{Query: "alpha", Type: SearchKeyword, Limit: 1}
			delayPage()
			first, err := svc.Search(ctx, req)
			if err != nil || first.NextCursor == "" {
				t.Fatalf("slow first page: %+v, %v", first, err)
			}
			clk.Advance(6 * time.Second)
			req.Cursor = first.NextCursor
			delayPage()
			second, err := svc.Search(ctx, req)
			if err != nil || second.NextCursor == "" {
				t.Fatalf("cursor from slow first page did not resume: %+v, %v", second, err)
			}
			clk.Advance(6 * time.Second)
			req.Cursor = second.NextCursor
			third, err := svc.Search(ctx, req)
			if err != nil || third.NextCursor == "" {
				t.Fatalf("cursor from slow continuation did not resume: %+v, %v", third, err)
			}
			clk.Advance(7 * time.Second)
			req.Cursor = third.NextCursor
			if _, err := svc.Search(ctx, req); !errs.Is(err, errs.Invalid) {
				t.Fatalf("genuinely idle cursor did not expire: %v", err)
			}
		})
	}
}

// An equality offset is past the last consumable hit and must not exhaust the
// session: the legitimate continuation must still work after its rejection.
func TestRankedCursorRejectsEndOffsetWithoutRetiringSession(t *testing.T) {
	for _, related := range []bool{false, true} {
		name := "search"
		if related {
			name = "related"
		}
		t.Run(name, func(t *testing.T) {
			ctx, svc := newSearchPagingService(t, nil, Config{})
			anchor, err := svc.Create(ctx, CreateReq{Content: "anchor"})
			if err != nil {
				t.Fatal(err)
			}
			if related {
				empty, err := svc.Related(ctx, RelatedReq{ID: anchor.ID, Limit: 1})
				if err != nil || len(empty.Results) != 0 || empty.HasMore || empty.NextCursor != "" {
					t.Fatalf("initial empty ranking must remain valid: %+v, %v", empty, err)
				}
			}
			for i := 0; i < 3; i++ {
				m, err := svc.Create(ctx, CreateReq{Content: "alpha"})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := svc.Relate(ctx, RelateReq{From: anchor.ID, To: m.ID, Strength: float32(i+1) / 3}); err != nil {
					t.Fatal(err)
				}
			}
			page := func(cursor string) ([]Result, string, error) {
				if related {
					r, err := svc.Related(ctx, RelatedReq{ID: anchor.ID, Limit: 1, Cursor: cursor})
					return r.Results, r.NextCursor, err
				}
				r, err := svc.Search(ctx, SearchReq{Query: "alpha", Type: SearchKeyword, Limit: 1, Cursor: cursor})
				return r.Results, r.NextCursor, err
			}
			first, cursor, err := page("")
			if err != nil || len(first) != 1 || cursor == "" {
				t.Fatalf("first page: %+v, %q, %v", first, cursor, err)
			}
			payload, err := codec.DecodeToken(cursor, codec.TokenSearch, "acme")
			if err != nil {
				t.Fatal(err)
			}
			forged := codec.EncodeToken(codec.TokenSearch, "acme", append(payload[:16:16], byte(3)))
			if _, _, err := page(forged); !errs.Is(err, errs.Invalid) {
				t.Errorf("end offset was not refused: %v", err)
			}
			second, _, err := page(cursor)
			if err != nil || len(second) != 1 || second[0].Memory.ID == first[0].Memory.ID {
				t.Fatalf("forged end offset retired valid state: %+v, %v", second, err)
			}
		})
	}
}
