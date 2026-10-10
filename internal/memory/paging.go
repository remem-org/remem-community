package memory

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/keys"
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
	// ranked says which kind of session this binding names: a ranked one,
	// holding a materialised ranking, or a listing, whose identity is the
	// ordering above. The two kinds can never collide on one binding because
	// this field is part of it, and it is what selects the ranked idle TTL
	// rather than the listing's independent five-minute window.
	ranked bool
	// fingerprint identifies a ranked session's query. It is unset for a
	// listing session.
	//
	// Sixty-four bits, not a digest: it is compared only with fingerprints
	// drawn from the same process seed (see queryFingerprintSeed), and the cost
	// of the collision it leaves on the table is that one caller resumes their
	// own earlier ranking in their own tenant. At the hundred and twenty-eight
	// sessions this registry holds, that is a one-in-a-quadrillion event.
	fingerprint uint64
}
type pagingSession struct {
	id       [16]byte
	binding  pagingBinding
	snapshot storage.Snapshot
	expires  time.Time
	readers  int
	retired  bool
	// rowGen is the tenant's attribute-row generation as it stood *before* this
	// session's snapshot was taken, and it is what lets every page of the session
	// read rows from the shared cache. Taking it first is the whole of the
	// ordering argument: a write landing between the two leaves the session
	// holding a generation that has moved on, and its reads go to its own
	// snapshot. See attr.RowCache.
	rowGen uint64
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
	// rows is the shared cache of decoded attribute rows, or nil. The registry
	// holds it because opening a session is the one place that can read its
	// generation before the snapshot, which is what makes it safe.
	rows     *attr.RowCache
	sessions map[[16]byte]*pagingSession
	closed   bool
	active   int
	pinned   int
	useSeq   uint64
	stop     chan struct{}
	done     chan struct{}
}

func newPagingRegistry(kv storage.KV, clk clock.Clock, rankedTTL time.Duration, rows *attr.RowCache) *pagingRegistry {
	p := &pagingRegistry{kv: kv, clk: clk, rankedTTL: rankedTTL, rows: rows, sessions: make(map[[16]byte]*pagingSession), stop: make(chan struct{}), done: make(chan struct{})}
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
		if s.binding.ranked && s.readers > 0 {
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
// Its query fingerprint identifies the ranking and its binding takes the ranked
// idle TTL; listing sessions keep their independent five-minute window.
//
// The caller names itself in op and noun. Search and related share this
// registry, and when both refusals said "start the search again" a caller
// paging related memories was sent to look at the wrong surface.
func (p *pagingRegistry) acquireRanked(b pagingBinding, session *[16]byte, op, noun string) (*pagingSession, error) {
	b.ranked = true
	return p.acquireSession(b, session, op, noun)
}

func (p *pagingRegistry) acquireSession(binding pagingBinding, session *[16]byte, op, noun string) (*pagingSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expireLocked()
	ttl := pagingTTL
	if binding.ranked {
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
		// The generation strictly before the snapshot. A write that commits in
		// between moves the generation on, so this session finds it stale and reads
		// every row from its own snapshot — which is the conservative answer, and
		// the reason the two lines are in this order rather than the other.
		s.rowGen = p.rows.Generation(binding.tenant)
		s.snapshot = p.kv.NewSnapshot()
		p.sessions[s.id] = s
		p.pinned++
	}
	s.readers++
	p.active++
	return s, nil
}

// rowReader is what one page reads everything through, and the handle that
// finishes with it.
//
// It is two things stacked. One iterator over the tenant's keys answers every
// point read this request makes — every attribute row the walk settles and every
// body the page returns — where a Get each builds and tears down an iterator of
// its own; and in front of the rows, the shared cache, pinned to the generation
// this session took before its snapshot. It is this request's alone: continuation
// pages of one session read concurrently, and an iterator has one position.
//
// With no cache it returns the iterator itself, and the page is byte for byte the
// request it was before this: `search.row_cache_entries = 0` costs nothing for the
// seam being there, not one allocation and not one indirection.
//
// The handle is a value, so returning it allocates nothing. Defer its finish after
// releasing the session, so the iterator closes before the snapshot it reads.
func (s *Service) rowReader(session *pagingSession, t tenant.ID) (attr.Reader, pageReader) {
	lower, upper := keys.TenantRange(t)
	seek := storage.NewSeekReader(session.snapshot, lower, upper)
	if s.rows == nil {
		return seek, pageReader{seek: seek}
	}
	rows := attr.NewCachedReader(seek, s.rows, s.slots, t, session.rowGen)
	return rows, pageReader{seek: seek, rows: rows}
}

// pageReader owns what one page's reader has to give back: the iterator, and the
// tally the cached reader kept.
type pageReader struct {
	seek *storage.SeekReader
	// rows is nil when the page read through the iterator alone.
	rows *attr.CachedReader
}

func (p pageReader) finish() {
	p.rows.Report()
	_ = p.seek.Close()
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
	if !s.retired && s.readers == 0 && s.binding.ranked {
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
