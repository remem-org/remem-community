package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/api/mcp"
	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/events"
	"github.com/remem-org/remem-go/internal/graph"
	"github.com/remem-org/remem-go/internal/lifecycle/policykv"
	"github.com/remem-org/remem-go/internal/memory"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/record"
	"github.com/remem-org/remem-go/internal/session"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
	"github.com/remem-org/remem-go/internal/text"
	"github.com/remem-org/remem-go/internal/vector"
	"github.com/remem-org/remem-go/internal/vector/distance"
	"github.com/remem-org/remem-go/internal/vector/flat"
)

// The implicit tenant a single-tenant deployment serves. It is deliberately not
// "default": the implicit tenant is whatever tenant.default names, and a test
// that used the fallback would pass over an implementation that ignored the
// setting.
const implicitTenant = tenant.ID("solo")

const (
	keySolo      = "key-solo"
	keyElsewhere = "key-elsewhere"
	keyOperator  = "key-operator"
)

// newSingleTenantServer is the surface an officially supported Community or
// Business build serves: the resolver under tenant.SingleTenant, and a tenant
// directory confined to the implicit tenant — which is what the composition
// root wires, and what makes the surfaces that read the directory rather than
// the request single-tenant too.
//
// The underlying directory holds a second tenant with memories in it, because a
// confinement tested against a store that has only one tenant is a confinement
// tested against nothing.
func newSingleTenantServer(t *testing.T) http.Handler {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })

	keyring, err := auth.NewKeyring([]auth.Credential{
		{ID: "solo-agent", Secret: keySolo, Tenant: implicitTenant},
		{ID: "elsewhere-agent", Secret: keyElsewhere, Tenant: "elsewhere"},
		{ID: "operator", Secret: keyOperator, CrossTenant: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	clk := clock.NewFake(clock.FakeStart)
	repo := record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(text.New()))
	svc := memory.New(kv, repo,
		flat.New(vector.NewStore(kv), distance.L2),
		embeddingtest.New(), clk, attr.MustTable(), graph.NewService(kv, clk), memory.Config{},
		memory.WithTextIndex(text.New()), memory.WithRecall(events.NewStore(kv), 0))
	t.Cleanup(func() { _ = svc.Close() })

	// Both tenants exist on disk, and the second one has a memory in it.
	raw := tenantkv.New(kv, clk)
	for _, id := range []tenant.ID{implicitTenant, "elsewhere"} {
		if _, err := tenant.Ensure(tenant.NewContext(t.Context(), id), raw, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.Create(tenant.NewContext(t.Context(), "elsewhere"),
		memory.CreateReq{Content: "a memory belonging to the other tenant"}); err != nil {
		t.Fatal(err)
	}

	return remhttp.NewRouter(remhttp.Deps{
		Memories:   svc,
		Tenants:    tenant.Confine(raw, implicitTenant),
		Resolver:   tenant.Resolver{Capability: tenant.SingleTenant, Default: implicitTenant},
		Capability: tenant.SingleTenant,
		Auth:       keyring,
		Metrics:    obs.NewMetrics(),
		Clock:      clk,
		Policies:   policykv.New(kv, clk),
		Ready:      func() error { return nil },
		MCP: mcp.NewHandler(mcp.Deps{
			Memories: svc,
			Sessions: session.New(kv, clk, 0),
		}),
	})
}

// The ordinary path: no selector anywhere, and every operation lands in the
// implicit tenant.
func TestSingleTenantRequestsUseTheImplicitTenant(t *testing.T) {
	srv := newSingleTenantServer(t)

	rr := send(t, srv, keySolo, "POST", "/api/v1/memories",
		strings.NewReader(`{"content":"the invoice was paid on Tuesday"}`))
	if rr.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", rr.Code, rr.Body.String())
	}

	rr = send(t, srv, keySolo, "GET", "/api/v1/memories?limit=10", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("list = %d: %s", rr.Code, rr.Body.String())
	}
	// The other tenant's memory is in the same store and must not be here.
	if strings.Contains(rr.Body.String(), "belonging to the other tenant") {
		t.Fatalf("the listing reached another tenant:\n%s", rr.Body.String())
	}

	rr = send(t, srv, keySolo, "POST", "/api/v1/memories/search",
		strings.NewReader(`{"query":"invoice","search_type":"keyword","limit":10}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("search = %d: %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "belonging to the other tenant") {
		t.Fatalf("the search reached another tenant:\n%s", rr.Body.String())
	}
}

// Every caller-controlled selector is refused, on every surface that takes one.
// The refusal is what matters rather than the confinement: a server that
// ignored the header would answer a request for another tenant's memories with
// this tenant's, and the caller could not tell.
func TestSingleTenantRefusesTheTenantHeaderOnEverySurface(t *testing.T) {
	srv := newSingleTenantServer(t)
	for _, tc := range []struct{ method, path, body string }{
		{"POST", "/api/v1/memories", `{"content":"x"}`},
		{"GET", "/api/v1/memories?limit=5", ""},
		{"POST", "/api/v1/memories/search", `{"query":"x","search_type":"keyword"}`},
		{"POST", "/api/v1/memories/recall", `{"context":"x","search_type":"keyword","token_budget":100}`},
		{"GET", "/api/v1/memories/0192b1f0-0000-7000-8000-000000000000", ""},
		{"GET", "/api/v1/admin/jobs", ""},
		{"GET", "/api/v1/admin/indexes", ""},
		{"POST", "/mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			r.Header.Set("Authorization", "Bearer "+keyOperator)
			r.Header.Set(remhttp.TenantHeader, "elsewhere")
			if tc.body != "" {
				r.Header.Set("Content-Type", "application/json")
			}
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, r)

			if rr.Code != http.StatusForbidden {
				t.Fatalf("a tenant header was answered %d: %s", rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), "serves one tenant") {
				t.Fatalf("the refusal does not say why: %s", rr.Body.String())
			}
			if strings.Contains(rr.Body.String(), "belonging to the other tenant") {
				t.Fatalf("the refusal leaked the other tenant's data: %s", rr.Body.String())
			}
		})
	}
}

// Even the implicit tenant's own name is refused as a header. A client that
// gets away with sending it against one deployment sends it against the next,
// where it names something else.
func TestSingleTenantRefusesItsOwnTenantAsAHeader(t *testing.T) {
	srv := newSingleTenantServer(t)
	r := httptest.NewRequest("GET", "/api/v1/memories?limit=5", nil)
	r.Header.Set("Authorization", "Bearer "+keyOperator)
	r.Header.Set(remhttp.TenantHeader, string(implicitTenant))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, r)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("the implicit tenant's own name as a header was answered %d: %s", rr.Code, rr.Body.String())
	}
}

// Tenant enumeration and provisioning are not what this build does, and it says
// so rather than 404ing: an operator has to be able to tell that from a typo.
func TestSingleTenantRefusesTenantAdministrationByName(t *testing.T) {
	srv := newSingleTenantServer(t)

	rr := send(t, srv, keyOperator, "GET", "/api/v1/tenants", nil)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("GET /tenants = %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "serves one tenant") {
		t.Fatalf("the refusal does not say why: %s", rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "elsewhere") {
		t.Fatalf("the refusal named another tenant: %s", rr.Body.String())
	}

	rr = send(t, srv, keyOperator, "POST", "/api/v1/tenants", strings.NewReader(`{"id":"elsewhere"}`))
	if rr.Code != http.StatusForbidden {
		t.Fatalf("POST /tenants = %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "serves one tenant") {
		t.Fatalf("the refusal does not say why: %s", rr.Body.String())
	}
}

// A credential cut for another tenant is refused rather than quietly served the
// implicit tenant's memories.
func TestSingleTenantRefusesACredentialBoundElsewhere(t *testing.T) {
	srv := newSingleTenantServer(t)
	rr := send(t, srv, keyElsewhere, "GET", "/api/v1/memories?limit=5", nil)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("a credential bound to another tenant was answered %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "elsewhere") ||
		!strings.Contains(rr.Body.String(), string(implicitTenant)) {
		t.Fatalf("the refusal names neither tenant: %s", rr.Body.String())
	}
}

// The retention-policy routes name a tenant in the path rather than a header,
// so they are a second selector and need the same answer. They get it from the
// confined directory: another tenant is simply not there.
func TestSingleTenantPolicyRoutesReachOnlyTheImplicitTenant(t *testing.T) {
	srv := newSingleTenantServer(t)

	rr := send(t, srv, keyOperator, "GET", "/api/v1/tenants/"+string(implicitTenant)+"/policies", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("the implicit tenant's policies = %d: %s", rr.Code, rr.Body.String())
	}

	if rr := send(t, srv, keyOperator, "GET",
		"/api/v1/tenants/elsewhere/policies", nil); rr.Code != http.StatusNotFound {
		t.Fatalf("reading another tenant's policies = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := send(t, srv, keyOperator, "PATCH", "/api/v1/tenants/elsewhere/policies",
		strings.NewReader(`{"policies":{"long_term":{"importance_decay":0.999}}}`)); rr.Code != http.StatusNotFound {
		t.Fatalf("changing another tenant's policies = %d: %s", rr.Code, rr.Body.String())
	}
}

// Administration still works, for the one tenant there is. A build that served
// one tenant and could not administer it would be worse than one that refused
// to start.
func TestSingleTenantAdministrationWorksForTheImplicitTenant(t *testing.T) {
	srv := newSingleTenantServer(t)
	for _, path := range []string{"/api/v1/admin/jobs", "/api/v1/admin/indexes", "/api/v1/admin/migrations"} {
		rr := send(t, srv, keyOperator, "GET", path, nil)
		// Without a job framework these answer by name rather than 404; what
		// matters here is that neither is a tenancy refusal.
		if rr.Code == http.StatusForbidden {
			t.Fatalf("%s was refused for the implicit tenant: %s", path, rr.Body.String())
		}
	}
}

// The health endpoint reports the capability, so a client can ask whether it
// may name a tenant instead of finding out from a 403.
func TestHealthReportsTheTenantCapability(t *testing.T) {
	rr := send(t, newSingleTenantServer(t), "", "GET", "/api/v1/health", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("health = %d", rr.Code)
	}
	if got := jsonBody(t, rr)["tenancy"]; got != "single-tenant" {
		t.Fatalf("health reports tenancy %v", got)
	}
}

// Metrics label every tenant the server served, which is why they need an
// operator credential. Under one tenant there is only ever one label to leak,
// and the other tenant in the store must not appear.
func TestSingleTenantMetricsExposeOneTenant(t *testing.T) {
	srv := newSingleTenantServer(t)
	if rr := send(t, srv, keySolo, "GET", "/api/v1/memories?limit=5", nil); rr.Code != http.StatusOK {
		t.Fatalf("list = %d: %s", rr.Code, rr.Body.String())
	}
	rr := send(t, srv, keyOperator, "GET", "/metrics", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("metrics = %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `tenant="`+string(implicitTenant)+`"`) {
		t.Fatalf("no metric carries the implicit tenant's label:\n%s", rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), `tenant="elsewhere"`) {
		t.Fatalf("the metrics name another tenant:\n%s", rr.Body.String())
	}
}
