package attr

import (
	"context"
	"sync/atomic"

	"github.com/remem-org/remem-go/internal/id"
	"github.com/remem-org/remem-go/internal/schema"
	"github.com/remem-org/remem-go/internal/tenant"
)

// CachedReader is how a query reads attribute rows through the RowCache:
// served from the cache while the reader's generation holds, and otherwise read
// through its own snapshot and offered to the cache.

// RowSource is a [Reader] that can answer for a decoded attribute row itself,
// rather than being asked for its bytes.
//
// It is optional, and discovered by type assertion in [ReadRow]: a reader with a
// cache behind it implements it, a bare snapshot does not, and no call site has
// to know which it was handed.
//
// The row it returns is shared read-only with every other holder. A caller that
// needs to change one must copy it first; see [RowCache].
type RowSource interface {
	Row(ctx context.Context, t tenant.ID, ns tenant.Namespace, rid id.ID) (*Row, error)
}

// CachedReader is a [Reader] whose attribute-row reads go through a [RowCache],
// pinned to the generation its session took before opening its snapshot.
//
// It is one request's, like the storage.SeekReader it usually wraps: its tally of
// hits and misses is unsynchronised, and continuation pages of one session each
// build their own.
type CachedReader struct {
	// Reader is the reader beneath, which answers everything that is not a row:
	// record bodies, text postings, the index walk's own iterator.
	Reader

	cache  *RowCache
	slots  *schema.Slots
	tenant tenant.ID
	// gen is the tenant generation this reader was opened at, read before its
	// snapshot was taken. See [RowCache].
	gen uint64
	// current is the live counter gen is compared against, resolved once here
	// rather than looked up per candidate. A page settles about fifty of them.
	current *atomic.Uint64

	hits, misses int
}

// NewCachedReader wraps r so that attribute rows come from c while the session's
// generation still holds. A nil or disabled cache makes it a pass-through, which
// is what `search.row_cache_entries = 0` deploys.
func NewCachedReader(r Reader, c *RowCache, slots *schema.Slots, t tenant.ID, gen uint64) *CachedReader {
	cr := &CachedReader{Reader: r, cache: c, slots: slots, tenant: t, gen: gen}
	if c != nil {
		cr.current = c.gens.counter(t)
	}
	return cr
}

// Row returns a record's decoded attribute row: from the cache when the cache may
// answer for it, and from this reader's own snapshot otherwise. A missing or
// undecodable row is absent rather than an error, as everywhere else.
//
// A tenant other than the one this reader was opened for bypasses the cache
// entirely. Nothing should ask — a request reads one tenant — and quietly
// answering from another tenant's generation would be the one bug in here worth
// being paranoid about (Invariant 1).
func (c *CachedReader) Row(ctx context.Context, t tenant.ID, ns tenant.Namespace, rid id.ID) (*Row, error) {
	if ns == "" {
		ns = tenant.DefaultNamespace
	}
	if c.cache == nil || t != c.tenant {
		return decodeRowFrom(ctx, c.Reader, c.slots, t, ns, rid)
	}
	if row, ok := c.cache.lookupAt(c.current, t, ns, rid, c.gen); ok {
		c.hits++
		return row, nil
	}
	c.misses++
	row, err := decodeRowFrom(ctx, c.Reader, c.slots, t, ns, rid)
	if err != nil || row == nil {
		return row, err
	}
	c.cache.storeAt(c.current, t, ns, rid, c.gen, row)
	return row, nil
}

// Report folds this reader's hits and misses into the cache's counters, once the
// request is done with it.
//
// It deliberately does not close the reader beneath: that one belongs to whoever
// opened it, and a Close here would have closed a bare store the day somebody
// wrapped one. A nil reader has nothing to report, so a caller that built none
// needs no branch.
func (c *CachedReader) Report() {
	if c == nil {
		return
	}
	c.cache.report(c.tenant, c.hits, c.misses)
	c.hits, c.misses = 0, 0
}
