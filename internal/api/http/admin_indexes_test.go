package http_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/tenant"
	"github.com/remem-org/remem-go/internal/tenant/tenantkv"
)

// fakeAdmin is the composition root's side of the index routes, as a table.
type fakeAdmin struct {
	mu        sync.Mutex
	askedFor  []tenant.ID
	indexes   []remhttp.IndexStatus
	rebuilds  map[string][]jobs.Type
	migration []remhttp.MigrationStatus
}

func (f *fakeAdmin) Indexes(_ context.Context, t tenant.ID) ([]remhttp.IndexStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.askedFor = append(f.askedFor, t)
	return f.indexes, nil
}

func (f *fakeAdmin) RebuildTypes(index string) ([]jobs.Type, error) {
	types, ok := f.rebuilds[index]
	if !ok {
		return nil, errs.E(errs.Invalid, "fake.RebuildTypes",
			errors.New(`"`+index+`" is not a derived index; the indexes are vector, text, or all`))
	}
	return types, nil
}

func (f *fakeAdmin) Migrations(context.Context) ([]remhttp.MigrationStatus, error) {
	return f.migration, nil
}

func newAdminServer(t *testing.T) (http.Handler, *fakeAdmin) {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	keyring, err := auth.NewKeyring([]auth.Credential{
		{ID: "acme-agent", Secret: keyAcme, Tenant: "acme"},
		{ID: "operator", Secret: keyRoot, CrossTenant: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFake(clock.FakeStart)
	reg := jobs.NewRegistry()
	for _, typ := range []jobs.Type{"vector.rebuild", "text.rebuild"} {
		if err := reg.Register(jobs.Entry{Type: typ, Description: "a rebuild", Handler: noopHandler{}}); err != nil {
			t.Fatal(err)
		}
	}
	count := int64(42)
	admin := &fakeAdmin{
		indexes: []remhttp.IndexStatus{
			{Space: "vector_index", Index: "vector", RebuildJob: "vector.rebuild", State: remhttp.IndexOK, Count: &count},
			{Space: "attr_row", Index: "attr", RebuildJob: "attr.rebuild", State: remhttp.IndexUnmonitored},
		},
		rebuilds: map[string][]jobs.Type{
			"vector": {"vector.rebuild"},
			"text":   {"text.rebuild"},
			"all":    {"vector.rebuild", "text.rebuild"},
		},
		migration: []remhttp.MigrationStatus{{
			ID: "0001-attribute-rows", State: "done", Processed: 250,
			StartedAt: clock.FakeStart, LastProgressAt: clock.FakeStart.Add(time.Second),
		}},
	}
	srv := remhttp.NewRouter(remhttp.Deps{
		Tenants:       tenantkv.New(kv, clk),
		Resolver:      tenant.Resolver{Capability: tenant.IdentityScoped, Default: "default"},
		Auth:          keyring,
		Metrics:       obs.NewMetrics(),
		Clock:         clk,
		Jobs:          remhttp.JobsDeps{Queue: jobs.NewQueue(kv, clk), Registry: reg},
		Admin:         admin,
		AutoProvision: true,
		Ready:         func() error { return nil },
	})
	return srv, admin
}

func TestIndexHealthIsReportedForTheTenantTheRequestNamed(t *testing.T) {
	srv, admin := newAdminServer(t)
	rr := sendFor(t, srv, keyRoot, "globex", "GET", remhttp.APIPrefix+"/admin/indexes")
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /admin/indexes: %d %s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, want := range []string{`"tenant":"globex"`, `"space":"vector_index"`, `"state":"ok"`,
		`"count":42`, `"state":"unmonitored"`} {
		if !strings.Contains(body, want) {
			t.Errorf("the response lacks %s: %s", want, body)
		}
	}
	if !slices.Equal(admin.askedFor, []tenant.ID{"globex"}) {
		t.Fatalf("the provider was asked about %v, want the header's tenant", admin.askedFor)
	}
}

// Index health names every tenant's derived data and a rebuild spends a
// tenant's compute; migration rows are untenanted. All three are the
// administration surface's, and a refusal says which one was asked for.
func TestIndexAndMigrationRoutesNeedACrossTenantCredential(t *testing.T) {
	srv, _ := newAdminServer(t)
	for _, c := range []struct {
		method, path, body, names string
	}{
		{"GET", "/admin/indexes", "", "indexes"},
		{"POST", "/admin/rebuild", `{"index":"vector"}`, "index"},
		{"GET", "/admin/migrations", "", "migration"},
	} {
		rr := send(t, srv, keyAcme, c.method, remhttp.APIPrefix+c.path, bodyOf(c.body))
		if rr.Code != http.StatusForbidden {
			t.Errorf("%s %s with a tenant-bound key: %d, want 403", c.method, c.path, rr.Code)
			continue
		}
		if !strings.Contains(rr.Body.String(), c.names) {
			t.Errorf("%s %s refused without naming %q: %s", c.method, c.path, c.names, rr.Body.String())
		}
	}
}

func TestARebuildQueuesItsJobAndRefusesASecondWhileOutstanding(t *testing.T) {
	srv, _ := newAdminServer(t)
	rr := send(t, srv, keyRoot, "POST", remhttp.APIPrefix+"/admin/rebuild", bodyOf(`{"index":"text"}`))
	if rr.Code != http.StatusAccepted {
		t.Fatalf("the first rebuild: %d %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"type":"text.rebuild"`) {
		t.Fatalf("the response does not show the queued job: %s", rr.Body.String())
	}

	rr = send(t, srv, keyRoot, "POST", remhttp.APIPrefix+"/admin/rebuild", bodyOf(`{"index":"text"}`))
	if rr.Code != http.StatusConflict {
		t.Fatalf("a second rebuild while the first is outstanding: %d, want 409: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "text.rebuild") {
		t.Fatalf("the refusal does not name the outstanding job: %s", rr.Body.String())
	}
}

// "all" queues what it can: an index already being rebuilt is reported, not
// queued twice, and not a reason to refuse the others.
func TestRebuildAllQueuesOnlyWhatIsNotOutstanding(t *testing.T) {
	srv, _ := newAdminServer(t)
	if rr := send(t, srv, keyRoot, "POST", remhttp.APIPrefix+"/admin/rebuild", bodyOf(`{"index":"text"}`)); rr.Code != http.StatusAccepted {
		t.Fatalf("queueing text first: %d", rr.Code)
	}
	rr := send(t, srv, keyRoot, "POST", remhttp.APIPrefix+"/admin/rebuild", bodyOf(`{"index":"all"}`))
	if rr.Code != http.StatusAccepted {
		t.Fatalf("rebuild all with one outstanding: %d %s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"type":"vector.rebuild"`) {
		t.Errorf("rebuild all did not queue the vector rebuild: %s", body)
	}
	if !strings.Contains(body, `"already_outstanding":["text.rebuild"]`) {
		t.Errorf("rebuild all did not report the outstanding text rebuild: %s", body)
	}
	if strings.Count(body, `"type":"text.rebuild"`) != 0 {
		t.Errorf("rebuild all queued a second text rebuild: %s", body)
	}
}

func TestARebuildOfAnUnknownIndexIsRefusedByName(t *testing.T) {
	srv, _ := newAdminServer(t)
	rr := send(t, srv, keyRoot, "POST", remhttp.APIPrefix+"/admin/rebuild", bodyOf(`{"index":"postings"}`))
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("an unknown index: %d, want 422: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "postings") || !strings.Contains(rr.Body.String(), "vector") {
		t.Fatalf("the refusal must name the index it got and the ones that exist: %s", rr.Body.String())
	}
}

func TestMigrationStateIsListed(t *testing.T) {
	srv, _ := newAdminServer(t)
	rr := send(t, srv, keyRoot, "GET", remhttp.APIPrefix+"/admin/migrations", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /admin/migrations: %d %s", rr.Code, rr.Body.String())
	}
	for _, want := range []string{`"id":"0001-attribute-rows"`, `"state":"done"`, `"processed":250`} {
		if !strings.Contains(rr.Body.String(), want) {
			t.Errorf("the listing lacks %s: %s", want, rr.Body.String())
		}
	}
}

// bodyOf is nil for an empty body — an untyped nil io.Reader. The first version
// returned *strings.Reader, whose nil is a non-nil interface once passed as an
// io.Reader: the request read from it and panicked, which aborted the test
// binary and hid four tests behind the one that crashed.
func bodyOf(s string) io.Reader {
	if s == "" {
		return nil
	}
	return strings.NewReader(s)
}
