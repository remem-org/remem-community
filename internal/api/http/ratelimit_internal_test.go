package http

import (
	"fmt"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/tenant"
)

// TestTheLimiterStaysBoundedAndForgetsIdleTenants: a limiter keyed by tenant is
// a map a caller controls the keys of, so it is bounded. Forgetting a tenant
// hands it a full bucket — at most one burst of extra allowance, which is the
// price of the bound and is checked here rather than assumed.
func TestTheLimiterStaysBoundedAndForgetsIdleTenants(t *testing.T) {
	clk := clock.NewFake(clock.FakeStart)
	// One token per thousand seconds: nothing in this test refills by time, so
	// a bucket that is full again was forgotten, not refilled.
	l := newTenantLimiter(RateLimitConfig{PerSecond: 0.001, Burst: 1, MaxTenants: 64}, clk)

	if ok, _ := l.Allow("t-0"); !ok {
		t.Fatal("the first request of a fresh tenant was refused")
	}
	if ok, wait := l.Allow("t-0"); ok || wait <= 0 {
		t.Fatalf("a second request inside a burst of one: allowed=%v, retry after %s", ok, wait)
	}

	for i := 1; i <= 1000; i++ {
		clk.Advance(time.Millisecond)
		l.Allow(tenant.ID(fmt.Sprintf("t-%d", i)))
		if n := l.size(); n > 64 {
			t.Fatalf("after %d tenants the limiter holds %d entries, past its bound of 64", i, n)
		}
	}

	if ok, _ := l.Allow("t-0"); !ok {
		t.Fatal("t-0 was evicted a thousand tenants ago and still found its bucket empty")
	}
}

// TestRetryAfterIsTheTimeToTheNextToken: the header is a promise, so it is the
// actual wait rather than a constant.
func TestRetryAfterIsTheTimeToTheNextToken(t *testing.T) {
	clk := clock.NewFake(clock.FakeStart)
	l := newTenantLimiter(RateLimitConfig{PerSecond: 0.5, Burst: 1}, clk)

	l.Allow("a")
	ok, wait := l.Allow("a")
	if ok {
		t.Fatal("a second request inside a burst of one was allowed")
	}
	if wait != 2*time.Second {
		t.Fatalf("at half a token a second an empty bucket waits %s, want 2s", wait)
	}
	clk.Advance(2 * time.Second)
	if ok, _ := l.Allow("a"); !ok {
		t.Fatal("refused after waiting exactly the promised time")
	}
}
