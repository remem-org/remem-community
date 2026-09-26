package http

import (
	"container/list"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// RateLimitConfig bounds how fast one tenant may send requests.
//
// # Why per tenant, where Rust limits per client IP
//
// Rust keys its limiter by address (`tower_governor`'s SmartIpKeyExtractor),
// which made sense for a server whose isolation unit was the process. Go's is
// the tenant: two tenants behind one proxy share an address and would starve
// each other, and one tenant spread across many agents would escape the limit
// entirely. The limiter therefore runs *after* authentication and keys by the
// tenant the request resolved to — which also means a flood of bad credentials
// naming a tenant cannot spend that tenant's allowance.
type RateLimitConfig struct {
	// PerSecond is the sustained rate. Zero or less turns limiting off.
	PerSecond float64
	// Burst is how many requests a tenant with a full bucket may send at once.
	Burst int
	// MaxTenants bounds how many tenants' buckets are held. Zero means
	// DefaultRateLimitTenants.
	MaxTenants int
}

// DefaultRateLimitTenants is how many tenants' buckets the limiter remembers.
// Forgetting one hands it a full bucket, which is at most one burst of extra
// allowance; the bound is what keeps a caller-controlled key from growing a map
// without limit.
const DefaultRateLimitTenants = 10_000

// tenantLimiter is a token bucket per tenant, least recently used first out.
//
// It reads the injected clock rather than the wall clock, so a test drives it
// to the millisecond. One mutex over the map: at the rates this is configured
// for, contention on it is not what a request waits on.
type tenantLimiter struct {
	rate  float64
	burst float64
	max   int
	clk   clock.Clock

	mu      sync.Mutex
	buckets map[tenant.ID]*bucket
	lru     *list.List // front is most recently used; values are tenant.ID
}

type bucket struct {
	tokens float64
	last   time.Time
	elem   *list.Element
}

// newTenantLimiter returns nil when limiting is off, which limitRate reads as
// "pass everything through".
func newTenantLimiter(cfg RateLimitConfig, clk clock.Clock) *tenantLimiter {
	if cfg.PerSecond <= 0 {
		return nil
	}
	burst := cfg.Burst
	if burst < 1 {
		burst = 1
	}
	maxTenants := cfg.MaxTenants
	if maxTenants <= 0 {
		maxTenants = DefaultRateLimitTenants
	}
	return &tenantLimiter{
		rate:    cfg.PerSecond,
		burst:   float64(burst),
		max:     maxTenants,
		clk:     clk,
		buckets: map[tenant.ID]*bucket{},
		lru:     list.New(),
	}
}

// Allow spends one token of t's allowance, or reports how long until one is
// available.
func (l *tenantLimiter) Allow(t tenant.ID) (bool, time.Duration) {
	now := l.clk.Now()
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[t]
	if !ok {
		if len(l.buckets) >= l.max {
			oldest := l.lru.Back()
			delete(l.buckets, oldest.Value.(tenant.ID))
			l.lru.Remove(oldest)
		}
		b = &bucket{tokens: l.burst, last: now}
		b.elem = l.lru.PushFront(t)
		l.buckets[t] = b
	} else {
		if elapsed := now.Sub(b.last); elapsed > 0 {
			b.tokens = math.Min(l.burst, b.tokens+elapsed.Seconds()*l.rate)
		}
		b.last = now
		l.lru.MoveToFront(b.elem)
	}

	// The epsilon absorbs float rounding in the refill: a tenant that waited
	// exactly one token's worth of time has earned the token.
	const epsilon = 1e-9
	if b.tokens >= 1-epsilon {
		b.tokens = math.Max(0, b.tokens-1)
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
}

func (l *tenantLimiter) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// limitRate refuses a request whose tenant has spent its allowance.
//
// It must sit inside authenticate: the tenant is what it counts against, and a
// request that has not proved which tenant it acts for has nothing to count.
func limitRate(l *tenantLimiter, next http.Handler) http.Handler {
	if l == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t, ok := tenant.FromContext(r.Context())
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		allowed, wait := l.Allow(t)
		if allowed {
			next.ServeHTTP(w, r)
			return
		}
		// Whole seconds, rounded up: RFC 9110's Retry-After has no fraction,
		// and rounding down would invite a retry that is refused again.
		secs := int(math.Ceil(wait.Seconds()))
		if secs < 1 {
			secs = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		WriteError(w, r, errs.E(errs.RateLimited, "http.limitRate", fmt.Errorf(
			"tenant %s has used its allowance of %g requests a second (burst %g); retry after %ds",
			t, l.rate, l.burst, secs)))
	})
}
