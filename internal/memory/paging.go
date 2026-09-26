package memory

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/query"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/tenant"
)

const pagingTTL = 5 * time.Minute
const pagingCapacity = 128

type pagingBinding struct {
	tenant         tenant.ID
	slot           uint16
	desc, archived bool
	// fingerprint identifies a ranked session's query. It is zero for a
	// listing session, whose identity is the ordering above, so the two kinds
	// can never collide on one binding.
	fingerprint [32]byte
}
type pagingSession struct {
	id       [16]byte
	binding  pagingBinding
	snapshot storage.Snapshot
	expires  time.Time
	readers  int
	retired  bool
	// lastUse orders sessions for reclaiming: the registry's use counter at the
	// session's most recent open, continuation or release. A counter rather than
	// a time, because two uses in one clock reading must still order.
	lastUse uint64

	// ranked is the materialised ranking of a search or traversal, computed
	// once on the first page and sliced by every page after it. Bodies are
	// deliberately not held here: each page reads its own through the pinned
	// snapshot, so a session costs tens of bytes per hit rather than a
	// megabyte and the capacity budget stays meaningful.
	ranked []query.Hit
	// rankedTruncated is whether the materialisation hit its depth bound. It
	// is reported on every page of the session, because the fact is about the
	// ranking rather than about one page of it.
	rankedTruncated bool
}

// pagingRegistry owns snapshots until expiry, exhaustion or service shutdown.
// Its mutex also protects reader leases: retirement never closes a reader's view.
type pagingRegistry struct {
	mu        sync.Mutex
	cond      *sync.Cond
	kv        storage.KV
	clk       clock.Clock
	rankedTTL time.Duration
	sessions  map[[16]byte]*pagingSession
	closed    bool
	active    int
	pinned    int
	useSeq    uint64
	stop      chan struct{}
	done      chan struct{}
}

func newPagingRegistry(kv storage.KV, clk clock.Clock, rankedTTL time.Duration) *pagingRegistry {
	p := &pagingRegistry{kv: kv, clk: clk, rankedTTL: rankedTTL, sessions: make(map[[16]byte]*pagingSession), stop: make(chan struct{}), done: make(chan struct{})}
	p.cond = sync.NewCond(&p.mu)
	ticker := clk.NewTicker(time.Second)
	go func() {
		defer close(p.done)
		defer ticker.Stop()
		for {
			select {
			case <-p.stop:
				return
			case <-ticker.C:
				p.mu.Lock()
				p.expireLocked()
				p.mu.Unlock()
			}
		}
	}()
	return p
}
func (p *pagingRegistry) retireLocked(s *pagingSession) {
	if s.retired {
		return
	}
	s.retired = true
	delete(p.sessions, s.id)
	if s.readers == 0 {
		_ = s.snapshot.Close()
		p.pinned--
	}
}
func (p *pagingRegistry) expireLocked() {
	for _, s := range p.sessions {
		// Ranked TTL bounds client inactivity, not ranking or hydration work.
		// Listing retains its existing independent expiry semantics.
		if s.binding.fingerprint != ([32]byte{}) && s.readers > 0 {
			continue
		}
		if !p.clk.Now().Before(s.expires) {
			p.retireLocked(s)
		}
	}
}

// idlestLocked is the least recently used session no request is reading
// through, or nil when every session has a reader.
func (p *pagingRegistry) idlestLocked() *pagingSession {
	var victim *pagingSession
	for _, s := range p.sessions {
		if s.readers > 0 {
			continue
		}
		if victim == nil || s.lastUse < victim.lastUse {
			victim = s
		}
	}
	return victim
}

func (p *pagingRegistry) acquire(binding pagingBinding, cursor *query.Cursor) (*pagingSession, error) {
	return p.acquireSession(binding, sessionOf(cursor), "memory.List", "listing")
}

func sessionOf(cursor *query.Cursor) *[16]byte {
	if cursor == nil {
		return nil
	}
	return &cursor.Session
}

// acquireRanked resumes or opens a session holding a materialised ranking.
//
// Its query fingerprint identifies both the ranking and the ranked idle TTL;
// listing sessions keep their independent five-minute window.
//
// The caller names itself in op and noun. Search and related share this
// registry, and when both refusals said "start the search again" a caller
// paging related memories was sent to look at the wrong surface.
func (p *pagingRegistry) acquireRanked(b pagingBinding, session *[16]byte, op, noun string) (*pagingSession, error) {
	return p.acquireSession(b, session, op, noun)
}

func (p *pagingRegistry) acquireSession(binding pagingBinding, session *[16]byte, op, noun string) (*pagingSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expireLocked()
	ttl := pagingTTL
	if binding.fingerprint != ([32]byte{}) {
		ttl = p.rankedTTL
	}
	bad := func(msg string) (*pagingSession, error) {
		return nil, errs.E(errs.Invalid, op, fmt.Errorf("%s; start the %s again", msg, noun))
	}
	if p.closed {
		return bad(noun + " service is closed")
	}
	var s *pagingSession
	if session != nil {
		s = p.sessions[*session]
		if s == nil {
			return bad("paging session is missing or expired: it was idle past its deadline, reclaimed for a newer request, or issued before a restart (tokens do not survive restart)")
		}
		if s.binding != binding {
			return bad("page token was issued for a different tenant or query")
		}
		// The bound is on abandonment, not on the total duration of an active
		// session. A client making progress keeps its pinned view alive.
		s.expires = p.clk.Now().Add(ttl)
		p.useSeq++
		s.lastUse = p.useSeq
	} else {
		// At capacity, reclaim the least recently used idle session rather than
		// refuse. The budget exists to bound pinned snapshots, and it still does;
		// what refusing did besides was turn away a caller who is here now to
		// protect one who has most likely gone — most clients read a first page
		// and never ask for the second. And the budget is shared by every tenant,
		// so one tenant's abandoned first pages refused everyone else's.
		// A session with a request reading through it is never reclaimed, so a
		// budget of active readers still refuses: that is the bound doing its job.
		if p.pinned >= pagingCapacity {
			if victim := p.idlestLocked(); victim != nil {
				p.retireLocked(victim)
			}
		}
		if p.pinned >= pagingCapacity {
			return nil, errs.E(errs.Unavailable, op, fmt.Errorf("all %d paging sessions are being read; retry after a %s finishes", pagingCapacity, noun))
		}
		p.useSeq++
		s = &pagingSession{binding: binding, expires: p.clk.Now().Add(ttl), lastUse: p.useSeq}
		for {
			if _, err := rand.Read(s.id[:]); err != nil {
				return nil, errs.E(errs.Unavailable, op, fmt.Errorf("creating paging session: %w", err))
			}
			if _, exists := p.sessions[s.id]; !exists {
				break
			}
		}
		s.snapshot = p.kv.NewSnapshot()
		p.sessions[s.id] = s
		p.pinned++
	}
	s.readers++
	p.active++
	return s, nil
}
func (p *pagingRegistry) release(s *pagingSession, finish bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if finish {
		p.retireLocked(s)
	}
	s.readers--
	p.active--
	p.useSeq++
	s.lastUse = p.useSeq
	if !s.retired && s.readers == 0 && s.binding.fingerprint != ([32]byte{}) {
		// The idle window starts after the last in-flight page finishes.
		s.expires = p.clk.Now().Add(p.rankedTTL)
	}
	if s.retired && s.readers == 0 {
		_ = s.snapshot.Close()
		p.pinned--
	}
	p.cond.Broadcast()
}
func (p *pagingRegistry) close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.stop)
		for _, s := range p.sessions {
			p.retireLocked(s)
		}
	}
	for p.active > 0 {
		p.cond.Wait()
	}
	p.mu.Unlock()
	<-p.done
}

// Close releases paging snapshots and waits for in-flight listing reads before
// returning. The caller must close the service before its underlying KV.
func (s *Service) Close() error {
	if s.paging != nil {
		s.paging.close()
	}
	return nil
}
