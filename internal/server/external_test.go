package server_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/server"
)

// externalConfig is a server whose vectors come from the caller, at a width no
// built-in model produces.
func externalConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := testConfig(t)
	cfg.Embedding.Provider = config.ProviderExternal
	cfg.Embedding.ExternalModelID = "cohere-embed-english-v3.0"
	cfg.Vector.Dimension = 768
	// A path that holds no model: an external server must never look.
	cfg.Embedding.ModelPath = t.TempDir()
	return cfg
}

func TestAnExternalVectorServerStartsWithoutAModel(t *testing.T) {
	// No WithEmbedder, and this test binary is built without the onnx tag: the
	// only way New succeeds is by not loading a model at all.
	srv, err := server.New(externalConfig(t), server.WithClock(clock.System()))
	if err != nil {
		t.Fatalf("an external-vector server needs no model, but New refused: %v", err)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("health = %d: %s", rec.Code, rec.Body)
	}
	stopImmediately(t, srv)
}

func TestAServerWithoutAnEmbedderRefusesToStartInONNXMode(t *testing.T) {
	cfg := testConfig(t)
	cfg.Embedding.ModelPath = t.TempDir()
	_, err := server.New(cfg, server.WithClock(clock.System()))
	if err == nil {
		t.Fatal("an onnx server with no model must refuse to start rather than produce meaningless vectors")
	}
	if errs.KindOf(err) == errs.Unclassified {
		t.Fatalf("the refusal should be classified, got %v", err)
	}
}

// Both indexes, because each refuses on its own: the approximate one when it
// materialises a tenant, the exact one as it scans, and flat is the default.
func TestACorpusWrittenByTheModelIsRefusedByAnExternalServer(t *testing.T) {
	for _, kind := range []string{"flat", "hnsw"} {
		t.Run(kind, func(t *testing.T) { refusesAnotherModelsCorpus(t, kind) })
	}
}

func refusesAnotherModelsCorpus(t *testing.T, kind string) {
	cfg := testConfig(t)
	cfg.Vector.Index = kind

	// Written by the model, the ordinary way.
	onnxSrv := mustNew(t, cfg)
	rr := httptest.NewRecorder()
	onnxSrv.Handler().ServeHTTP(rr, apiRequest(t, http.MethodPost, "/api/v1/memories", `{"content":"written by the model"}`))
	if rr.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", rr.Code, rr.Body)
	}
	stopImmediately(t, onnxSrv)

	// The same directory, served as external at the same width, so only the
	// model id can tell the two apart.
	cfg.Embedding.Provider = config.ProviderExternal
	cfg.Embedding.ExternalModelID = "cohere-embed-english-v3.0"
	cfg.Vector.Dimension = 384
	extSrv, err := server.New(cfg, server.WithClock(clock.System()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer stopImmediately(t, extSrv)

	query := `{"search_type":"semantic","query_vector":[` + strings.TrimSuffix(strings.Repeat("0.05,", 384), ",") + `]}`
	rr = httptest.NewRecorder()
	extSrv.Handler().ServeHTTP(rr, apiRequest(t, http.MethodPost, "/api/v1/memories/search", query))
	if rr.Code == http.StatusOK {
		t.Fatalf("a corpus embedded by another model was searched: %s", rr.Body)
	}
	for _, w := range []string{embedding.Model, "cohere-embed-english-v3.0"} {
		if !strings.Contains(rr.Body.String(), w) {
			t.Errorf("the refusal does not name %q: %s", w, rr.Body)
		}
	}
}

func apiRequest(t *testing.T, method, path, body string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}
