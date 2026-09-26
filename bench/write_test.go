//go:build onnx

package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/server"
)

// benchKey is the credential the benchmark server accepts. It is not a secret:
// the server exists for the life of one benchmark process and listens on
// nothing.
const benchKey = "remem-benchmark-credential-0001"

var (
	writeHandler     http.Handler
	writeHandlerOnce sync.Once
	writeSequence    atomic.Uint64
)

// BenchmarkWritePath mirrors Rust's write_path Criterion benchmark
// (crates/remem-server/benches/write_path.rs at pre-go-freeze): one operation
// is REMEM_BENCHMARK_TOTAL_WRITES creates (1,000 by default) in synchronised
// waves of 50, through the in-process HTTP handler, with every commit synced and
// the real embedding model computing every vector.
//
// Discovery is enqueued and never executed, as Rust drains its discovery tasks
// without running them. That is not a special mode: the job pool is started by
// Server.Run, and the benchmark drives Server.Handler without calling it, so
// the discovery job lands in the write's own transaction — a cost measured —
// and no worker ever claims it.
//
// One warm-up operation runs before the timer, as in Rust. It also provisions
// the default tenant, which Server.Run would otherwise have done.
func BenchmarkWritePath(b *testing.B) {
	total, err := parseTotalWrites(os.Getenv("REMEM_BENCHMARK_TOTAL_WRITES"))
	if err != nil {
		b.Fatal(err)
	}
	handler := writeServer(b)

	var failed atomic.Value
	operation := func(lat *latencies) {
		for wave := 0; wave < total/writeConcurrency; wave++ {
			var wg sync.WaitGroup
			for req := 0; req < writeConcurrency; req++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					seq := writeSequence.Add(1)
					body, _ := json.Marshal(map[string]any{
						"content": fmt.Sprintf(
							"Remem write-path benchmark memory wave=%d request=%d sequence=%d", wave, req, seq),
						"policy":     "short_term",
						"importance": 0.5,
						"tags":       []string{"benchmark", "write-path"},
					})
					r := httptest.NewRequest(http.MethodPost, "/api/v1/memories", bytes.NewReader(body))
					r.Header.Set("Content-Type", "application/json")
					r.Header.Set("Authorization", "Bearer "+benchKey)
					w := httptest.NewRecorder()
					start := time.Now()
					handler.ServeHTTP(w, r)
					if lat != nil {
						lat.add(time.Since(start))
					}
					if w.Code != http.StatusCreated {
						failed.Store(fmt.Sprintf("create answered %d: %s", w.Code, w.Body.String()))
					}
				}()
			}
			wg.Wait()
			if msg := failed.Load(); msg != nil {
				b.Fatal(msg)
			}
		}
	}

	operation(nil) // warm-up, untimed

	var lat latencies
	b.ResetTimer()
	start := time.Now()
	for range b.N {
		operation(&lat)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	b.ReportMetric(float64(total*b.N)/elapsed.Seconds(), "writes/s")
	lat.report(b, "request-ms")
}

// writeServer builds the server once per process: a Pebble directory under
// .benchmark-data/, synced commits, the HNSW index, and the real model.
func writeServer(b *testing.B) http.Handler {
	b.Helper()
	model := envOr("REMEM_EMBEDDING_MODEL_PATH", filepath.Join("..", ".models", "all-MiniLM-L6-v2"))
	library := envOr("REMEM_ONNX_LIBRARY_PATH", filepath.Join("..", ".tools", "onnxruntime", "lib", "libonnxruntime.so"))
	for _, p := range []string{model, library} {
		if _, err := os.Stat(p); err != nil {
			b.Skipf("the write-path benchmark needs the real model and runtime (%s): %v; see make model", p, err)
		}
	}

	var buildErr error
	writeHandlerOnce.Do(func() {
		cfg := config.Default()
		cfg.Storage.Path = benchDataDir(b, "write-path")
		cfg.Storage.SyncWrites = true
		cfg.Vector.Index = "hnsw"
		cfg.Tenant.AutoProvision = true
		cfg.Server.APIKey = benchKey
		cfg.Server.RateLimitRPS = 0
		// Warn, not the default info. At info the server writes one request
		// line per create, so a timed operation would also be timing 1,000 JSON
		// log writes that Rust's benchmark never makes: it installs no tracing
		// subscriber. The first measured runs paid exactly that, and their
		// output was unreadable besides.
		cfg.Log.Level = "warn"
		cfg.Embedding.ModelPath, _ = filepath.Abs(model)
		cfg.Embedding.ONNXLibraryPath, _ = filepath.Abs(library)
		srv, err := server.New(cfg)
		if err != nil {
			buildErr = err
			return
		}
		writeHandler = srv.Handler()
	})
	if buildErr != nil {
		b.Fatalf("building the benchmark server: %v", buildErr)
	}
	if writeHandler == nil {
		b.Fatal("the benchmark server was not built")
	}
	return writeHandler
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
