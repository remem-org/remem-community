package server_test

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/server"
)

// With metrics.addr set, the metrics endpoint is served on that listener and
// only there: the main listener no longer has it, and the metrics listener has
// nothing else. It keeps its credential rule on its own listener.
//
// Found in Phase 13's security review: metrics.addr was never read, so an
// operator who moved the endpoint to a private listener to keep it off the
// public one still had it on the public one. Decided with the user: make it
// work.
func TestMetricsCanBeServedOnTheirOwnListener(t *testing.T) {
	const key = "an-operator-key-long-enough-for-this-test"
	cfg := testConfig(t)
	cfg.Server.APIKey = key
	cfg.Metrics.Addr = "127.0.0.1:0"
	srv := mustNew(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	waitReady(t, srv)
	if srv.MetricsAddr() == "" {
		t.Fatal("the metrics listener was not bound")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	get := func(addr, path, bearer string) int {
		t.Helper()
		req, err := http.NewRequest("GET", "http://"+addr+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	for _, c := range []struct {
		name       string
		addr, path string
		bearer     string
		want       int
	}{
		{"the main listener no longer serves metrics", srv.Addr(), "/metrics", key, http.StatusNotFound},
		{"the metrics listener serves them", srv.MetricsAddr(), "/metrics", key, http.StatusOK},
		{"the metrics listener still needs a credential", srv.MetricsAddr(), "/metrics", "", http.StatusUnauthorized},
		{"the metrics listener serves nothing else", srv.MetricsAddr(), "/api/v1/health", key, http.StatusNotFound},
		{"the main listener still serves the API", srv.Addr(), "/api/v1/health", "", http.StatusOK},
	} {
		if got := get(c.addr, c.path, c.bearer); got != c.want {
			t.Errorf("%s: GET %s on %s returned %d, want %d", c.name, c.path, c.addr, got, c.want)
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// A metrics listener that cannot bind fails the start, names the setting, and
// releases the data directory, as a main listener that cannot bind does.
func TestRunFailsCleanlyWhenTheMetricsPortIsTaken(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()

	cfg := testConfig(t)
	cfg.Metrics.Addr = occupied.Addr().String()
	blocked := mustNew(t, cfg)
	err = blocked.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "metrics.addr") {
		t.Fatalf("Run with the metrics port taken returned %v, want a refusal naming metrics.addr", err)
	}
	if _, err := server.New(cfg, server.WithEmbedder(embeddingtest.New())); err != nil {
		t.Fatalf("the data directory is still locked after a failed Run: %v", err)
	}
}
