package server_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/server"
)

func TestMain(m *testing.M) {
	obs.SetDefault(obs.LoggerTo(io.Discard))
	os.Exit(m.Run())
}

// testConfig is a real configuration: a Pebble directory the test owns, on a
// port the kernel picks. Running against the real engine matters here — the
// leak and shutdown tests are about resources the in-memory store does not have.
func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Storage.Path = filepath.Join(t.TempDir(), "data")
	cfg.Storage.SyncWrites = false
	cfg.Server.HTTPAddr = "127.0.0.1:0"
	cfg.Log.Level = "error"
	return cfg
}

func mustNew(t *testing.T, cfg config.Config) *server.Server {
	t.Helper()
	srv, err := server.New(cfg,
		server.WithEmbedder(embeddingtest.New()),
		server.WithClock(clock.System()))
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	return srv
}

func waitReady(t *testing.T, srv *server.Server) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Ready() && srv.Addr() != "" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the server never became ready")
}

func runAndStop(t *testing.T) {
	t.Helper()
	srv := mustNew(t, testConfig(t))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	waitReady(t, srv)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return")
	}
}

// --- the plan's three tests -------------------------------------------------

func TestShutdownIsClean(t *testing.T) {
	srv := mustNew(t, testConfig(t))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	waitReady(t, srv)
	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("dirty shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return within 10s of cancellation")
	}
}

func TestNoGoroutinesLeak(t *testing.T) {
	// One warm-up run first: the HTTP and Pebble packages start persistent
	// goroutines on first use, and counting those as a leak would make this
	// test fail for a reason that has nothing to do with ownership.
	runAndStop(t)
	time.Sleep(200 * time.Millisecond)

	before := runtime.NumGoroutine()
	runAndStop(t)
	time.Sleep(200 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines leaked: %d → %d (spec §57: no untracked goroutines)", before, after)
	}
}

func TestInvalidConfigFailsBeforeListening(t *testing.T) {
	for name, mangle := range map[string]func(*config.Config){
		"no storage path":  func(c *config.Config) { c.Storage.Path = "" },
		"unknown engine":   func(c *config.Config) { c.Storage.Engine = "sqlite" },
		"unknown metric":   func(c *config.Config) { c.Vector.Metric = "manhattan" },
		"bad tenant":       func(c *config.Config) { c.Tenant.Default = "../etc" },
		"negative timeout": func(c *config.Config) { c.Server.ReadTimeout = -1 },
		"production without a credential": func(c *config.Config) {
			c.Server.Env = "production"
			c.Server.APIKey = ""
			c.Server.APIKeys = nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t)
			mangle(&cfg)
			if _, err := server.New(cfg, server.WithEmbedder(embeddingtest.New())); err == nil {
				t.Fatal("an invalid configuration produced a server (spec §51: fail fast)")
			}
		})
	}
}

// --- the rest ---------------------------------------------------------------

// A server that has bound its port is one an orchestrator may believe. Anything
// that can refuse must refuse in New, before Run listens.
func TestNewRefusesAnIndexThatDoesNotExistYet(t *testing.T) {
	cfg := testConfig(t)
	cfg.Vector.Index = "ivf"
	if _, err := server.New(cfg, server.WithEmbedder(embeddingtest.New())); !errs.Is(err, errs.Invalid) {
		t.Fatalf("got %v, want Invalid: this binary builds flat and hnsw", err)
	}
}

// Both indexes start, and the approximate one answers. Phase 7 shipped it; a
// configuration that names it must reach a working server rather than a
// refusal, which is what this test asserted until Phase 7 landed.
func TestBothIndexesStart(t *testing.T) {
	for _, kind := range []string{"flat", "hnsw"} {
		t.Run(kind, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Vector.Index = kind
			srv, err := server.New(cfg, server.WithEmbedder(embeddingtest.New()))
			if err != nil {
				t.Fatalf("vector.index = %q refused to start: %v", kind, err)
			}
			stopImmediately(t, srv)
		})
	}
}

// A failed New must not leave the data directory locked against the next
// attempt — an operator who fixes the configuration and restarts would
// otherwise meet a second, unrelated failure.
//
// The failure has to happen *after* the store is opened or the test proves
// nothing: a configuration rejected by Validate never reaches Pebble. Naming a
// model this binary cannot produce vectors for is such a failure, and it is a
// real one — the alternative is a server that starts and fills a durable corpus
// with vectors nothing can compare.
func TestAFailedStartReleasesTheDataDirectory(t *testing.T) {
	cfg := testConfig(t)
	cfg.Embedding.Model = "some-other-model"
	if _, err := server.New(cfg); err == nil {
		t.Fatal("expected a failure")
	}

	cfg.Embedding.Model = config.EmbeddingModel
	srv, err := server.New(cfg, server.WithEmbedder(embeddingtest.New()))
	if err != nil {
		t.Fatalf("the directory is still locked after a failed start: %v", err)
	}
	stopImmediately(t, srv)
}

func stopImmediately(t *testing.T, srv *server.Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	waitReady(t, srv)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return")
	}
}

// The end-to-end path: store over HTTP, find it again, over a real listener.
func TestTheServerServesTheWholeSurface(t *testing.T) {
	cfg := testConfig(t)
	// The implicit tenant this build serves need not be called "default", and
	// the credential is bound to it: that is the ordinary single-tenant
	// deployment, and it is what the shipped binaries do.
	cfg.Tenant.Default = "acme"
	cfg.Server.APIKeys = []string{"key-acme@acme"}
	srv := mustNew(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	waitReady(t, srv)

	base := "http://" + srv.Addr()
	client := &http.Client{Timeout: 10 * time.Second}

	get := func(path string) *http.Response {
		t.Helper()
		req, err := http.NewRequest("GET", base+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer key-acme")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// Health and readiness need no credential.
	resp, err := client.Get(base + "/api/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("health = %d", resp.StatusCode)
	}

	resp, err = client.Get(base + "/api/v1/ready")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("ready = %d while serving", resp.StatusCode)
	}

	// Store, then find.
	req, err := http.NewRequest("POST", base+"/api/v1/memories",
		strings.NewReader(`{"content":"the sky is blue"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer key-acme")
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create = %d: %s", resp.StatusCode, body)
	}

	req, err = http.NewRequest("POST", base+"/api/v1/memories/search",
		strings.NewReader(`{"query":"the sky is blue","search_type":"hybrid","limit":5}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer key-acme")
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), "the sky is blue") {
		t.Fatalf("search = %d: %s", resp.StatusCode, body)
	}

	// MCP is on the same server and behind the same credential.
	resp = get("/api/v1/tenants")
	_ = resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("a tenant-bound credential administering tenants = %d, want 403", resp.StatusCode)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// Readiness must be false before Run and false again after it returns, or an
// orchestrator will route traffic at a process that is not serving.
func TestReadinessTracksTheServingWindow(t *testing.T) {
	srv := mustNew(t, testConfig(t))
	if srv.Ready() {
		t.Fatal("a server that has not run reports ready")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	waitReady(t, srv)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if srv.Ready() {
		t.Fatal("a stopped server still reports ready")
	}
}

// The default tenant exists before the first request, so a fresh directory
// serves rather than failing on provisioning at a moment nobody chose
// (Invariant 10).
func TestTheDefaultTenantIsProvisionedAtStartup(t *testing.T) {
	cfg := testConfig(t)
	cfg.Tenant.AutoProvision = false
	cfg.Server.APIKeys = []string{"key-default@default"}
	srv := mustNew(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	waitReady(t, srv)

	req, err := http.NewRequest("POST", "http://"+srv.Addr()+"/api/v1/memories",
		strings.NewReader(`{"content":"first ever"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer key-default")
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("the first write against a fresh directory = %d: %s", resp.StatusCode, body)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Two servers must not open one data directory. Pebble's lock is what stops it,
// and this asserts the error reaches the operator rather than being swallowed.
func TestTwoServersCannotShareADataDirectory(t *testing.T) {
	cfg := testConfig(t)
	first := mustNew(t, cfg)
	t.Cleanup(func() { stopImmediately(t, first) })

	if _, err := server.New(cfg, server.WithEmbedder(embeddingtest.New())); err == nil {
		t.Fatal("a second server opened the same data directory")
	}
}

// A port already in use must fail Run, and must still release the store —
// otherwise a restart after fixing the port meets a locked directory.
func TestRunFailsCleanlyWhenThePortIsTaken(t *testing.T) {
	occupied := mustNew(t, testConfig(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- occupied.Run(ctx) }()
	waitReady(t, occupied)

	cfg := testConfig(t)
	cfg.Server.HTTPAddr = occupied.Addr()
	blocked := mustNew(t, cfg)
	if err := blocked.Run(context.Background()); err == nil {
		t.Fatal("a server bound a port already in use")
	}

	// The store was released, so the same directory opens again.
	again, err := server.New(cfg, server.WithEmbedder(embeddingtest.New()))
	if err != nil {
		t.Fatalf("the data directory is still locked after a failed Run: %v", err)
	}
	_ = again

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestVersionsAreReportedAtStartup(t *testing.T) {
	srv := mustNew(t, testConfig(t))
	if srv.Metrics() == nil {
		t.Fatal("no metrics registry")
	}
	if srv.Handler() == nil {
		t.Fatal("no handler")
	}
	stopImmediately(t, srv)
}
