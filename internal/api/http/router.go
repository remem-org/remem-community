package http

import (
	"context"
	"net/http"
	"time"

	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/lifecycle"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// Deps is everything the REST surface needs. It is a struct of interfaces
// rather than a constructor with nine parameters, because a nine-parameter
// constructor is one whose call sites nobody can read.
type Deps struct {
	Memories *memory.Service
	Tenants  tenant.Directory
	Resolver tenant.Resolver
	Auth     auth.Verifier
	Metrics  *obs.Metrics
	Clock    clock.Clock

	// Capability is the tenant policy this surface serves under. It decides
	// whether the routes that name or enumerate a tenant exist at all, which
	// the resolver cannot: the resolver settles which tenant a request is for,
	// and tenant administration is about the set of them.
	Capability tenant.Capability

	// MetricsPath is where the metrics endpoint is mounted, from metrics.path.
	// Empty means /metrics.
	MetricsPath string
	// MetricsDisabled mounts no metrics endpoint, from metrics.enabled = false.
	// Metrics is still recorded into, since every route is timed; it is only no
	// longer served. The zero value serves it, so a router built without an
	// opinion keeps the endpoint.
	MetricsDisabled bool

	// MCP is the Model Context Protocol surface, mounted at /mcp. It is a
	// field on this router rather than a second server so that both surfaces
	// pass through the same authentication and tenant resolution: an MCP
	// client that could reach a tenant the REST API refuses would be an
	// isolation hole with two doors, and one of them unreviewed.
	MCP http.Handler

	// Policies is the durable store of per-tenant retention overrides. It is
	// optional: without it the policy routes answer "this build does not store
	// retention overrides" rather than 404, so "not built" and "wrong URL" stay
	// distinguishable.
	Policies PolicyStore
	// PolicyCache is the scheduler's cached view of those overrides. This is
	// the one surface that changes one, so it is the one that drops the cache.
	PolicyCache interface{ Invalidate(tenant.ID) }

	// Jobs is the background job framework, mounted under /api/v1/admin. It is
	// optional: a process embedding the router without a pool answers those
	// routes with "this process is not running the job framework" rather than
	// 404, so the difference between "not built" and "wrong URL" stays legible.
	Jobs JobsDeps

	// Admin reports index health and migration state, and maps an index name to
	// the jobs that rebuild it. It is optional, and without it those routes
	// answer that the process has no index administration rather than 404.
	Admin AdminDeps

	// AutoProvision creates a tenant on first use rather than refusing.
	AutoProvision bool
	// MaxRequestBytes caps a request body.
	MaxRequestBytes int64
	// RateLimit bounds each tenant's request rate. A zero value turns
	// limiting off, which is what a test that is not about limiting wants.
	RateLimit RateLimitConfig
	// Ready reports whether the server may serve traffic. It is separate from
	// liveness on purpose: a process that is up but still opening its store
	// must fail readiness and pass liveness, or an orchestrator restarts it in
	// a loop while it tries to start.
	Ready func() error
}

// JobsDeps is the background job framework, as the delivery layer sees it.
//
// Two pointers rather than an interface: there is one queue and one registry,
// and an interface here would be a contract with a single implementation whose
// only purpose was to be mocked. The router does not run jobs; it reads and
// writes rows in the same queue the pool claims from.
type JobsDeps struct {
	Queue    *jobs.Queue
	Registry *jobs.Registry
	Pool     *jobs.Pool

	// Retention is jobs.retention: how long a finished job row and its attempt
	// history are kept. The history surface reports it, because a client that
	// does not know the window cannot tell an empty history from a short one.
	Retention time.Duration
}

// PolicyStore is the durable retention-override store, as the delivery layer
// sees it. It is an interface here for the reason JobsDeps is not: the router
// must be constructible without one, and a nil concrete pointer would answer
// with a panic rather than a message.
type PolicyStore interface {
	Get(ctx context.Context, t tenant.ID) (map[string]lifecycle.Policy, error)
	Put(ctx context.Context, t tenant.ID, policies map[string]lifecycle.Policy) error
}

// APIPrefix is where the versioned surface lives. The version is in the path
// rather than a header because a URL is what ends up in a support ticket.
const APIPrefix = "/api/v1"

// route pairs a ServeMux pattern with the label its metrics carry.
//
// The label is the pattern, never the request path: a path holds record ids,
// and one time series per memory would take a metrics store down inside a day.
type route struct {
	pattern string
	label   string
	handler http.HandlerFunc
	// public marks a route served without a credential. Only health, readiness
	// and metrics are, and each is listed here rather than decided inside the
	// handler, so "what can be reached unauthenticated" is answerable by
	// reading one list.
	public bool
}

// NewRouter builds the REST surface.
func NewRouter(d Deps) http.Handler {
	if d.Clock == nil {
		d.Clock = clock.System()
	}
	if d.MaxRequestBytes <= 0 {
		d.MaxRequestBytes = 8 << 20
	}

	routes := []route{
		{"POST " + APIPrefix + "/memories", "create_memory", d.createMemory, false},
		{"POST " + APIPrefix + "/memories:batch", "create_memories", d.createMemories, false},
		{"POST " + APIPrefix + "/memories/search", "search_memories", d.searchMemories, false},
		// Recall is a search with a budget, so it sits beside search rather
		// than under a memory: it does not address one.
		{"POST " + APIPrefix + "/memories/recall", "recall_memories", d.recallMemories, false},
		{"GET " + APIPrefix + "/memories", "list_memories", d.listMemories, false},
		{"GET " + APIPrefix + "/memories/{id}", "get_memory", d.getMemory, false},
		{"PATCH " + APIPrefix + "/memories/{id}", "update_memory", d.updateMemory, false},
		{"DELETE " + APIPrefix + "/memories/{id}", "delete_memory", d.deleteMemory, false},

		{"POST " + APIPrefix + "/memories/{id}/connections", "create_connection", d.createConnection, false},
		{"GET " + APIPrefix + "/memories/{id}/connections", "list_connections", d.listConnections, false},
		{"PATCH " + APIPrefix + "/memories/{id}/connections/{target}", "update_connection", d.updateConnection, false},
		{"DELETE " + APIPrefix + "/memories/{id}/connections/{target}", "delete_connection", d.deleteConnection, false},
		{"GET " + APIPrefix + "/memories/{id}/related", "related_memories", d.relatedMemories, false},
		{"GET " + APIPrefix + "/memories/{id}/history", "memory_history", d.memoryHistory, false},

		// Tenant administration exists only where there is a set of tenants to
		// administer. Under tenant.SingleTenant the two routes stay registered
		// and refuse by name: a 404 would be indistinguishable from a mistyped
		// path, and an operator has to be able to tell "this build does not do
		// that" from "you typed it wrong".
		{"GET " + APIPrefix + "/tenants", "list_tenants", d.listTenants, false},
		{"POST " + APIPrefix + "/tenants", "create_tenant", d.createTenant, false},

		// Retention policies are a property of a tenant, so the tenant is in
		// the path rather than in the X-Remem-Tenant header the job routes
		// take: changing the wrong tenant's decay because a header was left
		// over from the last request is a mistake with no undo.
		{"GET " + APIPrefix + "/tenants/{id}/policies", "list_policies", d.listPolicies, false},
		{"PATCH " + APIPrefix + "/tenants/{id}/policies", "patch_policies", d.patchPolicies, false},

		// Administration. Every one of these needs a credential that is not
		// bound to one tenant, and acts on the tenant the request resolved to
		// — so an operator names it with X-Remem-Tenant exactly as everywhere
		// else. /types is a literal and takes precedence over /{id}.
		{"GET " + APIPrefix + "/admin/jobs", "list_jobs", d.listJobs, false},
		{"GET " + APIPrefix + "/admin/jobs/types", "list_job_types", d.listJobTypes, false},
		{"GET " + APIPrefix + "/admin/jobs/{id}", "get_job", d.getJob, false},
		{"POST " + APIPrefix + "/admin/jobs/{id}/cancel", "cancel_job", d.cancelJob, false},
		{"POST " + APIPrefix + "/admin/jobs/{type}/run", "run_job", d.runJob, false},
		{"POST " + APIPrefix + "/admin/jobs/{type}/pause", "pause_job_type", d.pauseJobType, false},
		{"POST " + APIPrefix + "/admin/jobs/{type}/resume", "resume_job_type", d.resumeJobType, false},
		// A literal, like /types, so it takes precedence over /{id}.
		{"GET " + APIPrefix + "/admin/jobs/history", "job_history", d.listJobHistory, false},
		{"GET " + APIPrefix + "/admin/indexes", "list_indexes", d.listIndexes, false},
		{"POST " + APIPrefix + "/admin/rebuild", "rebuild_indexes", d.rebuildIndexes, false},
		{"GET " + APIPrefix + "/admin/migrations", "list_migrations", d.listMigrations, false},

		{"GET " + APIPrefix + "/health", "health", d.health, true},
		{"GET " + APIPrefix + "/ready", "ready", d.ready, true},
	}

	// One limiter for the whole surface, so a tenant's REST and MCP requests
	// draw on one allowance rather than one each.
	limiter := newTenantLimiter(d.RateLimit, d.Clock)

	mux := http.NewServeMux()
	for _, rt := range routes {
		h := http.Handler(rt.handler)
		if !rt.public {
			h = d.authenticate(limitRate(limiter, h))
		}
		mux.Handle(rt.pattern, d.observe(rt.label, h))
	}

	// MCP is not versioned in its path: the protocol carries its own version in
	// the initialize handshake, which is where a client already looks.
	if d.MCP != nil {
		mux.Handle("/mcp", d.observe("mcp", d.authenticate(limitRate(limiter, d.MCP))))
	}

	// The metrics endpoint is unversioned because /metrics is where every
	// Prometheus scrape config already looks, and metrics.path moves it.
	//
	// It needs a credential that is not bound to one tenant: the tenant label on
	// every per-tenant series makes a scrape a list of every tenant the server has
	// served, which is cross-tenant information whoever asks for it. It is not
	// rate limited, because a scrape refused while the default tenant is busy is
	// a monitoring gap exactly when monitoring matters.
	if !d.MetricsDisabled {
		d.mountMetrics(mux)
	}

	// "/" catches everything unrouted, so a mistyped path gets a problem
	// document rather than ServeMux's bare "404 page not found".
	mux.Handle("/", d.observe("unrouted", notFound()))

	return d.recoverPanics(d.limitBody(mux))
}

// NewMetricsRouter serves the metrics endpoint alone, for a server whose
// metrics.addr gives it a listener of its own. It is built from the same Deps
// as the main router, so the endpoint keeps the same authentication and the
// same credential rule on its own listener, and it answers nothing else: a
// private listener that also served the API would be a second door into it.
func NewMetricsRouter(d Deps) http.Handler {
	mux := http.NewServeMux()
	d.mountMetrics(mux)
	mux.Handle("/", d.observe("unrouted", notFound()))
	return d.recoverPanics(d.limitBody(mux))
}

// mountMetrics mounts the metrics endpoint at its configured path.
func (d Deps) mountMetrics(mux *http.ServeMux) {
	if d.Metrics == nil {
		return
	}
	mux.Handle("GET "+d.metricsPath(), d.observe("metrics", d.authenticate(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !d.crossTenant(w, r) {
				return
			}
			// A scrape names no tenant, so the scope cannot come from the
			// request: it comes from the credential's explicit authorisation,
			// and a credential narrowed to a set of tenants sees that set's
			// series and no others. Without a narrowing the handler is the
			// unfiltered one, which is what an operator credential is for.
			d.Metrics.HandlerFor(metricsScope(r)).ServeHTTP(w, r)
		}))))
}

// routeOf reports the matched pattern for logging. It is the pattern, not the
// path, for the same reason the metric label is.
func routeOf(r *http.Request) string {
	if p := r.Pattern; p != "" {
		return p
	}
	return "unrouted"
}

// metricsPath is where the metrics endpoint is mounted.
func (d Deps) metricsPath() string {
	if d.MetricsPath == "" {
		return "/metrics"
	}
	return d.MetricsPath
}
