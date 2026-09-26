package memory

import (
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/query"
	"github.com/remem-org/remem-go/internal/storage/memkv"
)

func TestARankedSessionIsCreatedOnceAndReused(t *testing.T) {
	p := newPagingRegistry(memkv.New(), clock.NewFake(clock.FakeStart), pagingTTL)
	defer p.close()

	b := pagingBinding{tenant: "acme", fingerprint: [32]byte{1}}
	s1, err := p.acquireRanked(b, nil, "memory.Search", "search")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	s1.ranked = []query.Hit{{ID: id.New()}}
	sid := s1.id
	p.release(s1, false)

	s2, err := p.acquireRanked(b, &sid, "memory.Search", "search")
	if err != nil {
		t.Fatalf("resuming: %v", err)
	}
	if s2 != s1 {
		t.Fatal("resuming minted a second session rather than reusing the first")
	}
	if len(s2.ranked) != 1 {
		t.Fatal("the materialised ranking did not survive the release")
	}
	p.release(s2, true)
}

// A token presented against a different query must be refused. Resuming a
// ranking that was computed for another question would return an arbitrary
// slice of the corpus that looks like a page.
func TestARankedSessionRefusesADifferentQuery(t *testing.T) {
	p := newPagingRegistry(memkv.New(), clock.NewFake(clock.FakeStart), pagingTTL)
	defer p.close()

	s, err := p.acquireRanked(pagingBinding{tenant: "acme", fingerprint: [32]byte{1}}, nil, "memory.Search", "search")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	sid := s.id
	p.release(s, false)

	_, err = p.acquireRanked(pagingBinding{tenant: "acme", fingerprint: [32]byte{2}}, &sid, "memory.Search", "search")
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("a different fingerprint returned %v, want Invalid", err)
	}
}

func TestARankedSessionRefusesADifferentTenant(t *testing.T) {
	p := newPagingRegistry(memkv.New(), clock.NewFake(clock.FakeStart), pagingTTL)
	defer p.close()

	s, err := p.acquireRanked(pagingBinding{tenant: "acme", fingerprint: [32]byte{1}}, nil, "memory.Search", "search")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	sid := s.id
	p.release(s, false)

	_, err = p.acquireRanked(pagingBinding{tenant: "globex", fingerprint: [32]byte{1}}, &sid, "memory.Search", "search")
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("another tenant's session returned %v, want Invalid", err)
	}
}

// Search and listing pin the same scarce thing -- a snapshot -- so they share
// one budget. A listing past a budget full of idle ranked sessions reclaims the
// oldest of them: the budget is shared in what it bounds, not in who it refuses.
func TestRankedAndListingSessionsShareTheCapacityBudget(t *testing.T) {
	p := newPagingRegistry(memkv.New(), clock.NewFake(clock.FakeStart), pagingTTL)
	defer p.close()

	var held []*pagingSession
	for i := 0; i < pagingCapacity; i++ {
		s, err := p.acquireRanked(pagingBinding{tenant: "acme", fingerprint: [32]byte{byte(i)}}, nil, "memory.Search", "search")
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		p.release(s, false)
		held = append(held, s)
	}

	listing, err := p.acquire(pagingBinding{tenant: "acme", slot: 3}, nil)
	if err != nil {
		t.Fatalf("a listing acquire past a budget of idle ranked sessions returned %v", err)
	}
	p.release(listing, false)
	if !held[0].retired || held[1].retired {
		t.Fatalf("reclaimed the wrong session: oldest retired=%v, next retired=%v",
			held[0].retired, held[1].retired)
	}
	if p.pinned != pagingCapacity {
		t.Fatalf("%d snapshots pinned after reclaiming one to open one, want %d", p.pinned, pagingCapacity)
	}
}

// TestReclaimTakesTheLeastRecentlyUsedNotTheFirstOpened: a client paging
// through a long result is using its session, so continuing it must move it to
// the back of the queue — otherwise the one caller actively reading is the one
// a newcomer displaces.
func TestReclaimTakesTheLeastRecentlyUsedNotTheFirstOpened(t *testing.T) {
	p := newPagingRegistry(memkv.New(), clock.NewFake(clock.FakeStart), pagingTTL)
	defer p.close()

	var held []*pagingSession
	for i := 0; i < pagingCapacity; i++ {
		s, err := p.acquireRanked(pagingBinding{tenant: "acme", fingerprint: [32]byte{byte(i), 1}}, nil, "memory.Search", "search")
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		p.release(s, false)
		held = append(held, s)
	}

	// The first-opened session is continued, so the second is now the least
	// recently used.
	again, err := p.acquireRanked(held[0].binding, &held[0].id, "memory.Search", "search")
	if err != nil {
		t.Fatalf("continuing the first session: %v", err)
	}
	p.release(again, false)

	fresh, err := p.acquireRanked(pagingBinding{tenant: "globex", fingerprint: [32]byte{9, 9}}, nil, "memory.Search", "search")
	if err != nil {
		t.Fatalf("a new session at capacity: %v", err)
	}
	p.release(fresh, false)
	if held[0].retired || !held[1].retired {
		t.Fatalf("continued session retired=%v, least recently used retired=%v; want false and true",
			held[0].retired, held[1].retired)
	}
}

// TestAFullBudgetOfActiveReadersStillRefuses: only an idle session can be
// reclaimed. When every pinned snapshot has a request reading through it, the
// memory bound is the thing that must hold, and a new session is refused.
func TestAFullBudgetOfActiveReadersStillRefuses(t *testing.T) {
	p := newPagingRegistry(memkv.New(), clock.NewFake(clock.FakeStart), pagingTTL)

	var reading []*pagingSession
	for i := 0; i < pagingCapacity; i++ {
		s, err := p.acquireRanked(pagingBinding{tenant: "acme", fingerprint: [32]byte{byte(i), 2}}, nil, "memory.Search", "search")
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		reading = append(reading, s) // not released: a request is still reading
	}

	_, err := p.acquire(pagingBinding{tenant: "globex", slot: 3}, nil)
	if !errs.Is(err, errs.Unavailable) {
		t.Fatalf("a new session with every snapshot in use returned %v, want Unavailable", err)
	}
	for _, s := range reading {
		p.release(s, false)
	}
	p.close()
}
