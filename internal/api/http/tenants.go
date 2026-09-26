package http

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/tenant"
)

// TenantResponse is one tenant as an operator sees it.
type TenantResponse struct {
	ID            string    `json:"id"`
	DisplayName   string    `json:"display_name,omitempty"`
	SchemaVersion uint32    `json:"schema_version"`
	CreatedAt     time.Time `json:"created_at"`
}

// TenantsResponse envelopes a list.
type TenantsResponse struct {
	Tenants []TenantResponse `json:"tenants"`
}

// CreateTenantRequest asks for a tenant to be provisioned.
type CreateTenantRequest struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name,omitempty"`
}

// listTenants shows every tenant, and is available only to a credential
// authorised across tenants.
//
// A tenant-bound credential gets 403 rather than a list containing only its
// own tenant. A filtered list would be the more forgiving design and the wrong
// one: it teaches a client that this endpoint returns "the tenants", and the
// first integration written against that assumption breaks the day someone is
// given an operator key.
func (d Deps) listTenants(w http.ResponseWriter, r *http.Request) {
	if !d.multiTenant(w, r, "listing the tenants") {
		return
	}
	if !d.crossTenant(w, r) {
		return
	}
	metas, err := d.Tenants.List(r.Context())
	if err != nil {
		WriteError(w, r, err)
		return
	}
	scope, narrowed := authorizedTenants(r)
	out := TenantsResponse{Tenants: make([]TenantResponse, 0, len(metas))}
	for _, m := range metas {
		if narrowed && !scope[m.ID] {
			continue
		}
		out.Tenants = append(out.Tenants, TenantResponse{
			ID:            string(m.ID),
			DisplayName:   m.DisplayName,
			SchemaVersion: m.SchemaVersion,
			CreatedAt:     m.CreatedAt,
		})
	}
	writeJSON(w, r, http.StatusOK, out)
}

func (d Deps) createTenant(w http.ResponseWriter, r *http.Request) {
	if !d.multiTenant(w, r, "creating a tenant") {
		return
	}
	if !d.crossTenant(w, r) {
		return
	}
	var body CreateTenantRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	tid, err := tenant.Parse(body.ID)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	// A credential with an explicit scope may not create a tenant outside it.
	// Provisioning is the one cross-tenant operation that *widens* the set, so
	// a narrowed credential that could do it would have narrowed nothing.
	if scope, narrowed := authorizedTenants(r); narrowed && !scope[tid] {
		WriteError(w, r, errs.E(errs.Forbidden, "http.tenants", fmt.Errorf(
			"this credential is authorized for an explicit set of tenants, and %q is not one of them", tid)))
		return
	}
	if err := d.Tenants.Create(r.Context(), tid, tenant.Meta{DisplayName: body.DisplayName}); err != nil {
		WriteError(w, r, err)
		return
	}
	m, err := d.Tenants.Get(r.Context(), tid)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, r, http.StatusCreated, TenantResponse{
		ID:            string(m.ID),
		DisplayName:   m.DisplayName,
		SchemaVersion: m.SchemaVersion,
		CreatedAt:     m.CreatedAt,
	})
}

// multiTenant reports whether this build administers a set of tenants at all,
// writing the refusal itself when it does not.
//
// It is checked before crossTenant, because the two answer different questions
// and the order decides which one an operator is told. "This build serves one
// tenant" is the fact; "your credential is bound" is advice about a credential
// that would not help even if it were unbound. Phase 9's lesson was that a
// refusal naming the wrong subject sends somebody to look at the wrong thing.
func (d Deps) multiTenant(w http.ResponseWriter, r *http.Request, action string) bool {
	if d.Capability != tenant.SingleTenant {
		return true
	}
	WriteError(w, r, errs.E(errs.Forbidden, "http.tenants", fmt.Errorf(
		"this build serves one tenant, so %s is not something it does. A deployment that administers "+
			"several tenants resolves them from an authenticated identity; docs/EDITION_BOUNDARY.md "+
			"describes the two", action)))
	return false
}

// crossTenant reports whether the caller may use an administration endpoint,
// writing the refusal itself when not.
//
// 403 here, not 404: unlike a memory in another tenant, the existence of an
// administration endpoint is not a secret, and hiding it would only make a
// misconfigured operator key look like a routing bug.
//
// The refusal names *what the caller asked for* rather than a fixed subject.
// The Phase 9 verification run hit the earlier version, which told an operator
// asking about background jobs that "administering tenants requires…" — a
// sentence that sends somebody to look at the wrong thing, and the sort of
// mistake that only shows up when a helper written for one surface is reused by
// a second.
func (d Deps) crossTenant(w http.ResponseWriter, r *http.Request) bool {
	p, ok := auth.FromContext(r.Context())
	if ok && p.CrossTenant {
		return true
	}
	WriteError(w, r, errs.E(errs.Forbidden, "http."+adminSubject(r, d.metricsPath()), fmt.Errorf(
		"%s requires a credential that is not bound to one tenant", adminAction(r, d.metricsPath()))))
	return false
}

// adminSubject and adminAction describe the endpoint in the words an operator
// used to reach it.
func adminSubject(r *http.Request, metrics string) string {
	switch {
	case r.URL.Path == metrics:
		return "metrics"
	case strings.Contains(r.URL.Path, "/admin/indexes"), strings.Contains(r.URL.Path, "/admin/rebuild"):
		return "admin.indexes"
	case strings.Contains(r.URL.Path, "/admin/migrations"):
		return "admin.migrations"
	case strings.Contains(r.URL.Path, "/admin/jobs"):
		return "admin.jobs"
	case strings.Contains(r.URL.Path, "/policies"):
		return "tenants.policies"
	default:
		return "tenants"
	}
}

func adminAction(r *http.Request, metrics string) string {
	switch {
	case r.URL.Path == metrics:
		return "reading the metrics, whose labels name every tenant served,"
	case strings.Contains(r.URL.Path, "/admin/indexes"), strings.Contains(r.URL.Path, "/admin/rebuild"):
		return "administering indexes"
	case strings.Contains(r.URL.Path, "/admin/migrations"):
		return "reading migration state"
	case strings.Contains(r.URL.Path, "/admin/jobs"):
		return "administering background jobs"
	case strings.Contains(r.URL.Path, "/policies"):
		return "reading or changing a tenant's retention policies"
	default:
		return "administering tenants"
	}
}

// metricsScope is the tenant filter a scrape is allowed, from the credential's
// explicit authorisation. A credential configuration did not narrow gets nil,
// which is the unfiltered endpoint.
//
// This is the one surface where the scope cannot come from the request, because
// a scrape names no tenant. Everywhere else the resolver settles it and the
// handler never sees a choice.
func metricsScope(r *http.Request) func(string) bool {
	p, ok := auth.FromContext(r.Context())
	if !ok || len(p.Authorized) == 0 {
		return nil
	}
	allowed := make(map[string]bool, len(p.Authorized))
	for _, t := range p.Authorized {
		allowed[string(t)] = true
	}
	return func(t string) bool { return allowed[t] }
}

// authorizedTenants narrows a cross-tenant listing to the credential's explicit
// scope, and reports whether it narrowed anything.
//
// A cross-tenant administrative operation acts on the scope configuration
// stated, rather than on every tenant that happens to exist: an unbound
// credential is not, on its own, authorisation for a tenant created tomorrow.
// A credential with no explicit set is the operator credential, and sees every
// tenant — which is a decision a configuration file made, not one inferred here.
func authorizedTenants(r *http.Request) (map[tenant.ID]bool, bool) {
	p, ok := auth.FromContext(r.Context())
	if !ok || len(p.Authorized) == 0 {
		return nil, false
	}
	allowed := make(map[tenant.ID]bool, len(p.Authorized))
	for _, t := range p.Authorized {
		allowed[t] = true
	}
	return allowed, true
}
