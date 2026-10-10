package tenant

import (
	"context"
	"sync"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
)

// CacheTTL is how long [Cached] reuses a tenant lookup.
//
// It is the bound on staleness in one direction only: a tenant deleted or
// renamed by another process stays accepted for at most this long. Nothing
// goes stale in the other direction, because an absent tenant is never
// cached, so a tenant created a moment ago — by another node, or by this one's
// auto-provisioning — is visible immediately.
//
// Thirty seconds is chosen against that asymmetry. A deleted tenant keeping
// its access for half a minute is an operational delay; a created tenant being
// refused for half a minute would be a bug in the first request of every new
// deployment.
const CacheTTL = 30 * time.Second

// CacheMaxEntries bounds the cache. Reaching it empties the cache rather than
// evicting a victim: an entry costs a read to rebuild and the bound is far
// above any real tenant count, so the simple answer is the right one.
//
// The bound is generous because entries exist only for tenants that do exist —
// a caller naming tenants that do not cannot grow the map at all — so the real
// limit is the deployment's tenant count. It is here so that "the real limit"
// is never something an operator discovers.
const CacheMaxEntries = 4096

// Cached reuses a directory's successful lookups for ttl.
//
// It exists because tenant existence is checked on the hot path: every
// authenticated request resolves its tenant and then asks the directory
// whether it exists, which is a KV read per request for an answer that changes
// about never. A zero or negative ttl takes [CacheTTL].
//
// Only successful lookups are cached. [errs.NotFound] is deliberately not,
// because a cache that remembers absence would make the auto-provisioning path
// refuse the tenant it has just created, and — the reason that matters — a
// cache entry must never be the thing that decides a tenant exists. A hit
// means the directory said yes within the TTL; a miss asks the directory
// again.
//
// It is a decorator over the interface for the same reason [Confine] is: the
// composition root wraps the directory once and hands the wrapper to
// everything, so no call site has a cache of its own to get wrong. Wrapping
// outside Confine is safe and intended — a confined directory refuses another
// tenant before the cache is consulted, and that refusal is a NotFound, which
// is never cached.
func Cached(d Directory, clk clock.Clock, ttl time.Duration) Directory {
	if ttl <= 0 {
		ttl = CacheTTL
	}
	return &cached{inner: d, clk: clk, ttl: ttl, known: make(map[ID]knownTenant)}
}

type cached struct {
	inner Directory
	clk   clock.Clock
	ttl   time.Duration

	mu    sync.Mutex
	known map[ID]knownTenant
}

type knownTenant struct {
	meta    Meta
	expires time.Time
}

var _ Directory = (*cached)(nil)

func (c *cached) Get(ctx context.Context, id ID) (Meta, error) {
	if m, ok := c.lookup(id); ok {
		return m, nil
	}
	// The inner read is taken outside the lock: a slow store must not serialise
	// every other tenant's requests behind it. Two requests missing on the same
	// tenant at once both read, which costs one extra read and keeps the
	// critical section a map operation.
	m, err := c.inner.Get(ctx, id)
	if err != nil {
		return Meta{}, err
	}
	c.remember(id, m)
	return m, nil
}

func (c *cached) Create(ctx context.Context, id ID, m Meta) error {
	err := c.inner.Create(ctx, id, m)
	// Dropped whatever the outcome: a Create that conflicted proves the entry
	// is older than a write this process has seen, and a Create that succeeded
	// replaces what any entry described.
	c.forget(id)
	return err
}

func (c *cached) List(ctx context.Context) ([]Meta, error) { return c.inner.List(ctx) }

// ForEach is not cached and does not populate the cache. It is the audited
// cross-tenant path (Invariant 1), it runs for maintenance rather than per
// request, and a job that walks every tenant must see the directory as it is.
func (c *cached) ForEach(ctx context.Context, fn func(ID) error) error {
	return c.inner.ForEach(ctx, fn)
}

func (c *cached) lookup(id ID) (Meta, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.known[id]
	if !ok || !c.clk.Now().Before(e.expires) {
		return Meta{}, false
	}
	return e.meta, true
}

func (c *cached) remember(id ID, m Meta) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.known) >= CacheMaxEntries {
		c.known = make(map[ID]knownTenant)
	}
	c.known[id] = knownTenant{meta: m, expires: c.clk.Now().Add(c.ttl)}
}

func (c *cached) forget(id ID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.known, id)
}
