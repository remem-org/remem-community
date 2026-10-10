package http_test

import (
	"net/http"
	"strings"
	"testing"

	remhttp "github.com/remem-org/remem-go/internal/api/http"
	"github.com/remem-org/remem-go/internal/attr"
	"github.com/remem-org/remem-go/internal/auth"
	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/embedding"
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

// newExternalServer is newServer for a server whose vectors come from the
// caller: four dimensions, cosine, and no model.
func newExternalServer(t *testing.T) http.Handler {
	t.Helper()
	kv := memkv.New()
	t.Cleanup(func() { _ = kv.Close() })
	keyring, err := auth.NewKeyring([]auth.Credential{{ID: "acme-agent", Secret: keyAcme, Tenant: "acme"}})
	if err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFake(clock.FakeStart)
	texts := text.New()
	svc := memory.New(kv, record.NewRepo(kv,
		record.WithIndexer(attr.NewIndexer(attr.MustTable())), record.WithIndexer(texts)),
		flat.New(vector.NewStore(kv), distance.Cosine),
		embedding.NewExternal("cohere-embed-english-v3.0", 4), clk, attr.MustTable(), graph.NewService(kv, clk),
		memory.Config{Metric: distance.Cosine}, memory.WithTextIndex(texts))
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
	})
}

func wantRefusal(t *testing.T, rr interface {
	Result() *http.Response
}, code int, body string, words ...string) {
	t.Helper()
	if got := rr.Result().StatusCode; got != code {
		t.Fatalf("status = %d, want %d: %s", got, code, body)
	}
	for _, w := range words {
		if !strings.Contains(body, w) {
			t.Errorf("refusal %s does not name %q", body, w)
		}
	}
}

func TestAVectorSentToAComputingServerIsRefusedOverHTTP(t *testing.T) {
	srv := newServer(t)
	rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories",
		strings.NewReader(`{"content":"hello","embedding":[1,0,0,0]}`))
	wantRefusal(t, rr, http.StatusUnprocessableEntity, rr.Body.String(), "embedding", "computes its own")

	createAs(t, srv, keyAcme, "hello")
	rr = send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories/search",
		strings.NewReader(`{"query":"hello","search_type":"semantic","query_vector":[1,0,0,0]}`))
	wantRefusal(t, rr, http.StatusUnprocessableEntity, rr.Body.String(), "query_vector", "computes its own")
}

func TestAnExternalServerOverHTTP(t *testing.T) {
	srv := newExternalServer(t)

	t.Run("a create without a vector names the field and the width", func(t *testing.T) {
		rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories", strings.NewReader(`{"content":"x"}`))
		wantRefusal(t, rr, http.StatusUnprocessableEntity, rr.Body.String(), "embedding", "4 dimensions")
	})
	t.Run("a vector of the wrong width is refused", func(t *testing.T) {
		rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories",
			strings.NewReader(`{"content":"x","embedding":[1,0,0]}`))
		wantRefusal(t, rr, http.StatusUnprocessableEntity, rr.Body.String(), "embedding", "3 components", "have 4")
	})

	rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories:batch", strings.NewReader(`{"memories":[
		{"content":"0","source":"vdbb:0","embedding":[1,0,0,0]},
		{"content":"1","source":"vdbb:1","embedding":[0,1,0,0]},
		{"content":"2","source":"vdbb:2","embedding":[0,0,1,0]},
		{"content":"3","source":"vdbb:3","embedding":[0,0,0,1]}]}`))
	if rr.Code != http.StatusCreated {
		t.Fatalf("batch create = %d: %s", rr.Code, rr.Body.String())
	}
	firstID := jsonBody(t, rr)["memories"].([]any)[0].(map[string]any)["id"].(string)

	t.Run("a semantic search by vector alone ranks by it", func(t *testing.T) {
		rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories/search",
			strings.NewReader(`{"search_type":"semantic","query_vector":[0,0.1,2,0],"limit":2}`))
		if rr.Code != http.StatusOK {
			t.Fatalf("search = %d: %s", rr.Code, rr.Body.String())
		}
		results := jsonBody(t, rr)["results"].([]any)
		first := results[0].(map[string]any)["memory"].(map[string]any)
		if first["source"] != "vdbb:2" {
			t.Fatalf("first result is %v, want the memory stored along axis 2", first["source"])
		}
	})
	t.Run("hybrid without a vector is refused", func(t *testing.T) {
		rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories/search",
			strings.NewReader(`{"query":"2","search_type":"hybrid"}`))
		wantRefusal(t, rr, http.StatusUnprocessableEntity, rr.Body.String(), "query_vector")
	})
	t.Run("recall ranks by the supplied vector", func(t *testing.T) {
		rr := send(t, srv, keyAcme, "POST", remhttp.APIPrefix+"/memories/recall",
			strings.NewReader(`{"search_type":"semantic","query_vector":[0,0,0,1],"token_budget":500}`))
		if rr.Code != http.StatusOK {
			t.Fatalf("recall = %d: %s", rr.Code, rr.Body.String())
		}
		first := jsonBody(t, rr)["results"].([]any)[0].(map[string]any)["memory"].(map[string]any)
		if first["source"] != "vdbb:3" {
			t.Fatalf("first recalled is %v, want vdbb:3", first["source"])
		}
	})
	t.Run("an update to content carries its vector", func(t *testing.T) {
		path := remhttp.APIPrefix + "/memories/" + firstID
		rr := send(t, srv, keyAcme, "PATCH", path, strings.NewReader(`{"content":"moved"}`))
		wantRefusal(t, rr, http.StatusUnprocessableEntity, rr.Body.String(), "embedding")

		rr = send(t, srv, keyAcme, "PATCH", path, strings.NewReader(`{"content":"moved","embedding":[0,0,1,1]}`))
		if rr.Code != http.StatusOK {
			t.Fatalf("update = %d: %s", rr.Code, rr.Body.String())
		}
	})
}
