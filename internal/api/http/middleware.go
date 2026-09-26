package http

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// TenantHeader names the tenant a cross-tenant credential is acting for.
const TenantHeader = "X-Remem-Tenant"

// RequestIDHeader carries the request id, in and out. A client that supplies
// one has it echoed, so a trace that began upstream stays one trace.
const RequestIDHeader = "X-Request-Id"

// errsAs is errors.As, wrapped so problem.go does not import errors for one
// call.
func errsAs(err error, target any) bool { return errors.As(err, target) }

// recorder captures the status so logging and metrics see what was sent.
type recorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *recorder) WriteHeader(status int) {
	if w.wrote {
		return
	}
	w.wrote = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *recorder) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

// tenantSlot is how the tenant reaches the metrics.
//
// The problem it solves is a real one and easy to miss: the authentication
// middleware resolves the tenant into a *derived* context and passes that to
// the handler, so the observe middleware wrapped around it still holds the
// original and would label every metric with an empty tenant. Invariant 1
// reaches the metrics, not only the keyspace, so an always-empty label is a
// broken guarantee rather than a cosmetic one.
//
// A pointer in the context, filled in by the inner middleware and read by the
// outer one after the handler returns, is the smallest fix that keeps observe
// outside authenticate — which it must be, because a request refused for a bad
// credential still has to be counted.
type tenantSlot struct{ id string }

type tenantSlotKey struct{}

func withTenantSlot(ctx context.Context, slot *tenantSlot) context.Context {
	return context.WithValue(ctx, tenantSlotKey{}, slot)
}

func recordTenant(ctx context.Context, t string) {
	if slot, ok := ctx.Value(tenantSlotKey{}).(*tenantSlot); ok {
		slot.id = t
	}
}

// observe assigns a request id, records metrics, and logs the outcome.
//
// It never logs a request or response body. Memory content is the user's data
// and a log line is not where it lives — internal/obs has a test that enforces
// the rule, and this is the layer most likely to break it.
func (d Deps) observe(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get(RequestIDHeader)
		if reqID == "" {
			reqID = obs.NewRequestID()
		}
		slot := &tenantSlot{}
		ctx := withTenantSlot(obs.WithRequestID(r.Context(), reqID), slot)
		w.Header().Set(RequestIDHeader, reqID)

		rec := &recorder{ResponseWriter: w, status: http.StatusOK}
		start := d.Clock.Now()

		d.Metrics.API.InFlight.WithLabelValues("http").Inc()
		defer d.Metrics.API.InFlight.WithLabelValues("http").Dec()

		next.ServeHTTP(rec, r.WithContext(ctx))

		elapsed := d.Clock.Since(start)
		d.Metrics.API.Duration.WithLabelValues(route, r.Method).Observe(elapsed.Seconds())
		d.Metrics.API.RequestsTotal.
			WithLabelValues(slot.id, route, r.Method, statusClass(rec.status)).Inc()

		obs.Logger(obs.WithTenant(ctx, slot.id)).Info("request",
			"method", r.Method, "route", route, "status", rec.status,
			"duration_ms", elapsed.Milliseconds())
	})
}

// statusClass buckets a status as 2xx, 4xx and so on.
//
// The raw code would be a label with a hundred values, most of them never
// seen, and the question an operator asks is "are we failing", not "how many
// 418s". A dashboard that needs the exact code reads the logs.
func statusClass(status int) string {
	switch {
	case status < 200:
		return "1xx"
	case status < 300:
		return "2xx"
	case status < 400:
		return "3xx"
	case status < 500:
		return "4xx"
	default:
		return "5xx"
	}
}

// recover turns a panic into a 500 rather than a dropped connection.
//
// A panicking handler otherwise closes the connection with no response at all,
// which a client reports as a network error — sending an operator to look at
// the load balancer for a bug that is in this process.
func (d Deps) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				obs.Logger(r.Context()).Error("handler panicked", "panic", v, "path", r.URL.Path)
				WriteProblem(w, r, http.StatusInternalServerError, "Internal error",
					"The server could not complete this request. Quote the request id when reporting it.",
					"urn:remem:problem:panic")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// limitBody caps how much a request may send, so one client cannot spend the
// server's memory.
func (d Deps) limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, d.MaxRequestBytes)
		}
		next.ServeHTTP(w, r)
	})
}

// authenticate verifies the credential and resolves the tenant.
//
// The two are one step because the second depends on the first: which tenant a
// request belongs to is decided by what its credential is bound to (plan
// §II.5), and a handler that could see an authenticated principal without a
// resolved tenant would be a handler that could read across tenants.
func (d Deps) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := d.Auth.Verify(r.Context(), bearer(r))
		if err != nil {
			WriteError(w, r, err)
			return
		}

		ctx := auth.NewContext(r.Context(), principal)
		ctx = tenant.WithClaim(ctx, principal.Claim(r.Header.Get(TenantHeader)))

		t, err := d.Resolver.FromContext(ctx)
		if err != nil {
			WriteError(w, r, err)
			return
		}
		if d.AutoProvision {
			if _, err := tenant.Ensure(ctx, d.Tenants, t); err != nil {
				WriteError(w, r, err)
				return
			}
		} else if _, err := d.Tenants.Get(ctx, t); err != nil {
			WriteError(w, r, err)
			return
		}

		ctx = tenant.NewContext(ctx, t)
		ctx = obs.WithTenant(ctx, string(t))
		recordTenant(ctx, string(t))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// bearer extracts the credential from the Authorization header, accepting the
// bare token as well.
//
// The bare form exists because it is what curl users and the MCP stdio client
// send, and rejecting it would make the first five minutes with Remem an
// argument about header syntax.
func bearer(r *http.Request) string {
	h := strings.TrimSpace(r.Header.Get("Authorization"))
	if h == "" {
		return ""
	}
	if scheme, token, ok := strings.Cut(h, " "); ok && strings.EqualFold(scheme, "Bearer") {
		return strings.TrimSpace(token)
	}
	return h
}

// notFound answers an unrouted path with a problem document, so that a typo in
// a URL produces the same shape as every other error.
func notFound() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		WriteError(w, r, errs.E(errs.NotFound, "http", errors.New("no such endpoint")))
	})
}

// timeoutOf is the clock helper the observe middleware needs, kept here so
// Deps can be constructed with a fake clock in tests.
var _ = time.Duration(0)
