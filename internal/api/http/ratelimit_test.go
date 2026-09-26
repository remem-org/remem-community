package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/lifecycle/policykv"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

// newLimitedServer is newServer with a rate limit, a clock the test drives, and
// a stand-in MCP handler — enough to show the limit covers /mcp without
// building the MCP surface.
func newLimitedServer(t *testing.T, limit remhttp.RateLimitConfig, clk *clock.Fake) http.Handler {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	keyring, err := auth.NewKeyring([]auth.Credential{
		{ID: "operator", Secret: keyRoot, CrossTenant: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	svc := memory.New(kv, record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(text.New())),
		flat.New(vector.NewStore(kv), distance.L2),
		embeddingtest.New(), clk, attr.MustTable(), graph.NewService(kv, clk), memory.Config{},
		memory.WithTextIndex(text.New()))

	return remhttp.NewRouter(remhttp.Deps{
		Memories:      svc,
		Tenants:       tenantkv.New(kv, clk),
		Resolver:      tenant.Resolver{Capability: tenant.IdentityScoped, Default: "default"},
		Auth:          keyring,
		Metrics:       obs.NewMetrics(),
		Clock:         clk,
		Policies:      policykv.New(kv, clk),
		AutoProvision: true,
		Ready:         func() error { return nil },
		RateLimit:     limit,
		MCP: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	})
}

// sendFor issues a request as the operator, acting for one tenant.
func sendFor(t *testing.T, srv http.Handler, key string, tid tenant.ID, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, nil)
	r.Header.Set("Authorization", "Bearer "+key)
	r.Header.Set(remhttp.TenantHeader, string(tid))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	return rr
}

// TestRateLimitBoundsOneTenant: one tenant spending its allowance is refused,
// told when to come back, and recovers at the configured rate — while another
// tenant served by the same process never notices.
func TestRateLimitBoundsOneTenant(t *testing.T) {
	clk := clock.NewFake(clock.FakeStart)
	srv := newLimitedServer(t, remhttp.RateLimitConfig{PerSecond: 10, Burst: 5}, clk)
	list := remhttp.APIPrefix + "/memories"

	for i := range 5 {
		if rr := sendFor(t, srv, keyRoot, "a", "GET", list); rr.Code != http.StatusOK {
			t.Fatalf("tenant a, request %d, within its burst: %d %s", i, rr.Code, rr.Body.String())
		}
	}
	rr := sendFor(t, srv, keyRoot, "a", "GET", list)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("tenant a past its burst: status %d, want 429", rr.Code)
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Fatal("a 429 without Retry-After tells the caller to retry and not when")
	}
	if !strings.Contains(rr.Body.String(), "urn:remem:problem:rate_limited") {
		t.Fatalf("the refusal is not a rate_limited problem document: %s", rr.Body.String())
	}

	for i := range 5 {
		if rr := sendFor(t, srv, keyRoot, "b", "GET", list); rr.Code != http.StatusOK {
			t.Fatalf("tenant b, request %d, while a is limited: %d", i, rr.Code)
		}
	}

	// One token's worth of time at 10 per second.
	clk.Advance(100 * time.Millisecond)
	if rr := sendFor(t, srv, keyRoot, "a", "GET", list); rr.Code != http.StatusOK {
		t.Fatalf("tenant a after one token's worth of time: %d", rr.Code)
	}
	if rr := sendFor(t, srv, keyRoot, "a", "GET", list); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("tenant a spent the one token it earned and was served again: %d", rr.Code)
	}
}

// TestAZeroRateDisablesLimiting: zero is the documented way to turn it off, as
// REMEM_RATE_LIMIT_RPS=0 was for Rust.
func TestAZeroRateDisablesLimiting(t *testing.T) {
	clk := clock.NewFake(clock.FakeStart)
	srv := newLimitedServer(t, remhttp.RateLimitConfig{PerSecond: 0, Burst: 5}, clk)
	for i := range 200 {
		if rr := sendFor(t, srv, keyRoot, "a", "GET", remhttp.APIPrefix+"/memories"); rr.Code != http.StatusOK {
			t.Fatalf("request %d with limiting off: %d", i, rr.Code)
		}
	}
}

// TestARefusedCredentialIsNotChargedToATenant: the limiter runs after
// authentication, so a flood of bad credentials naming a tenant cannot spend
// that tenant's allowance and lock its real clients out.
func TestARefusedCredentialIsNotChargedToATenant(t *testing.T) {
	clk := clock.NewFake(clock.FakeStart)
	srv := newLimitedServer(t, remhttp.RateLimitConfig{PerSecond: 1, Burst: 3}, clk)
	list := remhttp.APIPrefix + "/memories"

	for range 50 {
		if rr := sendFor(t, srv, "not-a-key", "a", "GET", list); rr.Code != http.StatusUnauthorized {
			t.Fatalf("a bad credential: %d, want 401", rr.Code)
		}
	}
	for i := range 3 {
		if rr := sendFor(t, srv, keyRoot, "a", "GET", list); rr.Code != http.StatusOK {
			t.Fatalf("request %d after 50 refused credentials: %d — they were charged to the tenant", i, rr.Code)
		}
	}
}

// TestAnMCPCallIsLimitedLikeAnyOther: /mcp shares the REST surface's
// authentication, so it shares its limit — a model in a loop is exactly the
// load a limit exists for.
func TestAnMCPCallIsLimitedLikeAnyOther(t *testing.T) {
	clk := clock.NewFake(clock.FakeStart)
	srv := newLimitedServer(t, remhttp.RateLimitConfig{PerSecond: 1, Burst: 2}, clk)
	for range 2 {
		if rr := sendFor(t, srv, keyRoot, "a", "POST", "/mcp"); rr.Code != http.StatusOK {
			t.Fatalf("/mcp within the burst: %d", rr.Code)
		}
	}
	if rr := sendFor(t, srv, keyRoot, "a", "POST", "/mcp"); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("/mcp past the burst: %d, want 429", rr.Code)
	}
}
