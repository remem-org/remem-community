// Package server is Remem's composition root.
//
// It is the only place that knows how the parts fit together, and the only
// place that constructs anything concrete: below it, every subsystem takes its
// dependencies as interfaces and has no opinion about which implementation it
// is given. That is what makes the subsystems testable and what keeps the wiring
// readable in one file rather than spread across nine constructors.
//
// # Every goroutine has an owner
//
// Spec §57 requires it, and this package is where it is enforced. Two long-lived
// goroutines exist — the HTTP listener and the session sweeper — and both are
// started by Run, both watch the same context, and both are waited for before
// Run returns. A goroutine that outlived Run would leak a listener across tests
// and, in production, would keep a data directory open after shutdown reported
// success.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/api/mcp"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/version"
)

// Server is one Remem process.
type Server struct {
	deps    *deps
	metrics *obs.Metrics
	handler http.Handler
	// metricsHandler serves the metrics endpoint on a listener of its own, when
	// metrics.addr names one. Nil means the main listener serves it, or nothing.
	metricsHandler http.Handler

	// listener is bound in Run, and Addr reads it. It is behind a mutex
	// because a test asks for the address from another goroutine while Run is
	// still starting.
	mu              sync.Mutex
	listener        net.Listener
	metricsListener net.Listener

	ready atomic.Bool
}

// New builds a server. It does not listen, and it does not start a goroutine.
//
// Everything that can refuse does so here: an invalid configuration, a data
// directory written by a newer Remem, a model that is not present. Spec §51
// asks for a fast failure, and what makes that useful is the converse — a
// server that has bound its port is one an orchestrator may believe.
func New(cfg config.Config, opt ...Option) (*Server, error) {
	var o options
	for _, f := range opt {
		f(&o)
	}

	// The tenant policy is settled before anything is built, and it fails
	// closed: a build declaring a capability nothing parses refuses to start
	// rather than serve under a policy chosen by accident. build needs it,
	// because it is what decides whether this directory can be served at all.
	if o.capability == tenant.NoCapability {
		c, err := tenant.ParseCapability(version.TenantCapability)
		if err != nil {
			return nil, err
		}
		o.capability = c
	}
	capability := o.capability

	d, err := build(cfg, o)
	if err != nil {
		return nil, err
	}

	// The registry is built with the dependencies rather than here, because the
	// migration runner reports through it and runs before anything listens.
	s := &Server{deps: d, metrics: d.metrics}
	// With metrics.addr set, the endpoint moves to a listener of its own and the
	// main one no longer has it: an operator moves it there to keep it off the
	// public listener.
	ownListener := cfg.Metrics.Enabled && cfg.Metrics.Addr != ""
	rd := remhttp.Deps{
		Memories:    d.memories,
		Tenants:     d.tenants,
		Resolver:    tenant.Resolver{Capability: capability, Default: tenant.ID(cfg.Tenant.Default)},
		Capability:  capability,
		Auth:        d.keyring,
		Metrics:     s.metrics,
		MetricsPath: cfg.Metrics.Path,
		// Metrics are always collected, and served only when metrics.enabled
		// says so. The endpoint's labels name every tenant served, and an
		// operator who turned it off must not still have it.
		MetricsDisabled: !cfg.Metrics.Enabled || ownListener,
		Clock:           d.clk,
		MCP: mcp.NewHandler(mcp.Deps{
			Memories:   d.memories,
			Sessions:   d.sessions,
			ServerName: "remem",
		}),
		Jobs: remhttp.JobsDeps{
			Queue:     d.queue,
			Registry:  d.registry,
			Pool:      d.pool,
			Retention: cfg.Jobs.Retention,
		},
		Admin:           adminProvider{d: d},
		Policies:        d.policies,
		PolicyCache:     d.policyCache,
		AutoProvision:   cfg.Tenant.AutoProvision,
		MaxRequestBytes: cfg.Server.MaxRequestBytes,
		RateLimit: remhttp.RateLimitConfig{
			PerSecond: float64(cfg.Server.RateLimitRPS),
			Burst:     cfg.Server.RateLimitBurst,
		},
		Ready: s.readiness,
	}
	s.handler = remhttp.NewRouter(rd)
	if ownListener {
		metricsDeps := rd
		metricsDeps.MetricsDisabled = false
		s.metricsHandler = remhttp.NewMetricsRouter(metricsDeps)
	}
	return s, nil
}

// Metrics exposes the registry, so a caller that embeds a server can scrape it.
func (s *Server) Metrics() *obs.Metrics { return s.metrics }

// Handler exposes the routed surface, so an integration test can drive it
// without binding a port.
func (s *Server) Handler() http.Handler { return s.handler }

// Addr is the address the server is listening on, or "" before Run has bound
// one. A test uses it to reach a server started on port 0.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// MetricsAddr is the address the metrics listener is bound to, or "" when
// metrics.addr is unset or Run has not bound it yet.
func (s *Server) MetricsAddr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metricsListener == nil {
		return ""
	}
	return s.metricsListener.Addr().String()
}

// Ready reports whether the server is serving.
func (s *Server) Ready() bool { return s.ready.Load() }

func (s *Server) readiness() error {
	if !s.ready.Load() {
		return errNotReady
	}
	return nil
}

// Run serves until ctx is cancelled, then shuts down and releases everything.
//
// It returns nil on a clean shutdown, including one caused by cancellation:
// being asked to stop is not a failure, and returning context.Canceled would
// make every caller special-case the ordinary path.
func (s *Server) Run(ctx context.Context) error {
	log := obs.Logger(ctx)

	listener, err := net.Listen("tcp", s.deps.cfg.Server.HTTPAddr)
	if err != nil {
		// Close what New opened: a failed Run must not leave the data
		// directory locked against the next attempt.
		_ = s.deps.close()
		return errs.E(errs.Unavailable, "server.Run",
			fmt.Errorf("listening on %s: %w", s.deps.cfg.Server.HTTPAddr, err))
	}
	var metricsListener net.Listener
	if s.metricsHandler != nil {
		metricsListener, err = net.Listen("tcp", s.deps.cfg.Metrics.Addr)
		if err != nil {
			_ = listener.Close()
			_ = s.deps.close()
			return errs.E(errs.Unavailable, "server.Run",
				fmt.Errorf("listening on metrics.addr %s: %w", s.deps.cfg.Metrics.Addr, err))
		}
	}
	s.mu.Lock()
	s.listener = listener
	s.metricsListener = metricsListener
	s.mu.Unlock()

	if err := s.provisionDefaultTenant(ctx); err != nil {
		_ = listener.Close()
		if metricsListener != nil {
			_ = metricsListener.Close()
		}
		_ = s.deps.close()
		return err
	}

	httpServer := &http.Server{
		Handler:      s.handler,
		ReadTimeout:  s.deps.cfg.Server.ReadTimeout,
		WriteTimeout: s.deps.cfg.Server.WriteTimeout,
		BaseContext:  func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
	}

	var metricsServer *http.Server
	if metricsListener != nil {
		metricsServer = &http.Server{
			Handler:      s.metricsHandler,
			ReadTimeout:  s.deps.cfg.Server.ReadTimeout,
			WriteTimeout: s.deps.cfg.Server.WriteTimeout,
			BaseContext:  func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
		}
	}

	var wg sync.WaitGroup
	serveErr := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()
	if metricsServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := metricsServer.Serve(metricsListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				serveErr <- fmt.Errorf("the metrics listener: %w", err)
			}
		}()
	}

	sweeper := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.sweepSessions(ctx, sweeper)
	}()

	// The background job framework. Both goroutines watch the same context and
	// are waited for below, which is spec §57's rule that every goroutine has
	// an owner. The pool drains on cancellation — running handlers are given a
	// window to checkpoint, and whatever has not finished is released back to
	// the queue rather than charged a failure.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.deps.pool.Run(ctx); err != nil {
			log.Error("the job pool stopped", "error", err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.deps.scheduler.Run(ctx); err != nil {
			log.Error("the job scheduler stopped", "error", err)
		}
	}()

	// Background migrations run alongside serving, not before it. The tail of
	// the plan is whatever start-up deliberately left — see deps.migrate — and
	// it keeps the plan's order.
	//
	// A failure here does not stop the server. The directory is at the version
	// the completed steps left it at, which is a version this binary can serve;
	// taking the process down would turn a migration that needs looking at into
	// an outage. The runner has already recorded why in the migration's state
	// row, and the next start retries from its cursor.
	if len(s.deps.background) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.deps.migrations.Run(ctx, s.deps.background); err != nil {
				log.Error("a background migration did not finish; the directory stays at the version "+
					"the completed steps left it at, and the next start resumes from its cursor", "error", err)
			}
		}()
	}

	s.ready.Store(true)
	serving := []any{"addr", listener.Addr().String(), "engine", s.deps.cfg.Storage.Engine,
		"tenancy", s.deps.capability.String(), "tenant_default", s.deps.cfg.Tenant.Default}
	if metricsListener != nil {
		serving = append(serving, "metrics_addr", metricsListener.Addr().String())
	}
	log.Info("remem is serving", serving...)

	// Wait for a stop signal or for the listener to fail on its own.
	var runErr error
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		runErr = errs.E(errs.Unavailable, "server.Run", err)
	}

	s.ready.Store(false)
	close(sweeper)

	// Graceful first: in-flight requests finish. The budget is the operator's,
	// and when it runs out the connections are closed rather than waited on —
	// a shutdown that can be delayed indefinitely by one slow client is a
	// shutdown an orchestrator will turn into a kill.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.deps.cfg.Server.ShutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("graceful shutdown did not finish in time; closing connections", "error", err)
		_ = httpServer.Close()
	}
	if metricsServer != nil {
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			_ = metricsServer.Close()
		}
	}

	wg.Wait()
	if err := s.deps.close(); err != nil && runErr == nil {
		runErr = err
	}
	log.Info("remem has stopped")
	return runErr
}

// provisionDefaultTenant makes the configured default tenant exist.
//
// Without it the first request against a fresh directory fails, or provisions
// implicitly at a moment nobody chose. Invariant 10 is that a single-node
// `remem start --data-dir ./data` is a complete experience, and a tenant that
// exists before the first request is part of that.
func (s *Server) provisionDefaultTenant(ctx context.Context) error {
	t, err := tenant.Parse(s.deps.cfg.Tenant.Default)
	if err != nil {
		return err
	}
	scoped := tenant.NewContext(ctx, t)
	if _, err := tenant.Ensure(scoped, s.deps.tenants, t); err != nil {
		return err
	}
	return nil
}

// sweepSessions removes expired sessions, across every tenant.
//
// This is the one background job Phase 3 has, and it is here rather than in a
// handler because it belongs to nobody's request. It goes through
// tenant.ForEach, which is the audited cross-tenant path — internal/server is
// on the guard's allowlist for exactly this kind of work.
func (s *Server) sweepSessions(ctx context.Context, stop <-chan struct{}) {
	interval := s.deps.sessions.TTL() / 2
	if interval < time.Minute {
		interval = time.Minute
	}
	ticker := s.deps.clk.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweepOnce(ctx)
		}
	}
}

func (s *Server) sweepOnce(ctx context.Context) {
	log := obs.Logger(ctx)
	total := 0
	err := s.deps.tenants.ForEach(ctx, func(t tenant.ID) error {
		n, err := s.deps.sessions.SweepExpired(ctx, t)
		if err != nil {
			return err
		}
		total += n
		return nil
	})
	if err != nil {
		// A failed sweep is not a reason to stop the server: sessions are
		// expendable, and the next tick tries again.
		log.Warn("the session sweep failed", "error", err)
		return
	}
	if total > 0 {
		log.Info("expired sessions removed", "count", total)
	}
}
