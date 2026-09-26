package obs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/obs"
)

func TestLoggerCarriesContextFields(t *testing.T) {
	var buf bytes.Buffer
	ctx := obs.WithRequestID(obs.WithTenant(context.Background(), "acme"), "req-1")
	obs.LoggerTo(&buf).InfoContext(ctx, "stored")

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("logs must be machine-readable: %v (%s)", err, buf.String())
	}
	if got["tenant"] != "acme" || got["request_id"] != "req-1" {
		t.Fatalf("context fields missing: %v", got)
	}
}

func TestLoggerCarriesEveryIdentifierSpec50Requires(t *testing.T) {
	var buf bytes.Buffer
	ctx := context.Background()
	ctx = obs.WithTenant(ctx, "acme")
	ctx = obs.WithRequestID(ctx, "req-1")
	ctx = obs.WithNode(ctx, "node-a")
	ctx = obs.WithShard(ctx, "shard-3")
	ctx = obs.WithJobID(ctx, "job-9")
	ctx = obs.WithMigrationID(ctx, "0004_attr_slots")
	obs.LoggerTo(&buf).InfoContext(ctx, "swept")

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]string{
		"tenant": "acme", "request_id": "req-1", "node": "node-a",
		"shard": "shard-3", "job_id": "job-9", "migration_id": "0004_attr_slots",
	} {
		if got[k] != want {
			t.Errorf("%s = %v, want %q", k, got[k], want)
		}
	}
}

func TestAbsentContextFieldsAreOmitted(t *testing.T) {
	// A log line for a background sweep has no request id. Emitting an empty
	// one would make every field's presence meaningless.
	var buf bytes.Buffer
	obs.LoggerTo(&buf).InfoContext(context.Background(), "started")

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"tenant", "request_id", "node", "shard", "job_id", "migration_id"} {
		if _, present := got[k]; present {
			t.Errorf("%s must be absent when it is not in the context, got %v", k, got[k])
		}
	}
}

func TestLoggerFromContextIsTheOneStoredThere(t *testing.T) {
	var buf bytes.Buffer
	l := obs.LoggerTo(&buf)
	ctx := obs.WithLogger(context.Background(), l)
	if obs.Logger(ctx) != l {
		t.Fatal("Logger(ctx) must return the logger the context carries")
	}
	if obs.Logger(context.Background()) == nil {
		t.Fatal("Logger must always return a usable logger")
	}
}

func TestNewLoggerHonoursLevelAndFormat(t *testing.T) {
	var buf bytes.Buffer
	l := obs.NewLogger(obs.LogConfig{Level: slog.LevelWarn, Format: obs.FormatJSON, Output: &buf})
	l.Info("suppressed")
	if buf.Len() != 0 {
		t.Fatalf("info must be below the warn threshold, got %s", buf.String())
	}
	l.Warn("emitted")
	if !strings.Contains(buf.String(), "emitted") {
		t.Fatalf("warn must pass the threshold, got %s", buf.String())
	}

	buf.Reset()
	text := obs.NewLogger(obs.LogConfig{Level: slog.LevelInfo, Format: obs.FormatText, Output: &buf})
	text.InfoContext(obs.WithTenant(context.Background(), "acme"), "hello")
	if !strings.Contains(buf.String(), "tenant=acme") {
		t.Fatalf("the text format must carry context fields too: %s", buf.String())
	}
}

func TestNewLoggerRejectsAnUnknownFormat(t *testing.T) {
	// Spec §51: fail fast rather than silently pick a default nobody chose.
	defer func() {
		if recover() == nil {
			t.Fatal("an unknown log format must not be silently accepted")
		}
	}()
	obs.NewLogger(obs.LogConfig{Format: "yaml", Output: &bytes.Buffer{}})
}

func TestRequestIDsAreUniqueAndReadable(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for range 1000 {
		v := obs.NewRequestID()
		if v == "" {
			t.Fatal("an empty request id is not traceable")
		}
		if seen[v] {
			t.Fatalf("duplicate request id %s", v)
		}
		seen[v] = true
	}
	ctx := obs.WithRequestID(context.Background(), "req-7")
	if got := obs.RequestID(ctx); got != "req-7" {
		t.Fatalf("RequestID = %q, want req-7", got)
	}
	if obs.RequestID(context.Background()) != "" {
		t.Fatal("a context with no request id reports none")
	}
	if obs.Tenant(obs.WithTenant(context.Background(), "acme")) != "acme" {
		t.Fatal("Tenant must read back what WithTenant stored")
	}
}

func TestMetricsCoverEveryDomainSpec50Requires(t *testing.T) {
	m := obs.NewMetrics()

	m.Storage.WritesTotal.WithLabelValues("acme", "record").Inc()
	m.Storage.ReadsTotal.WithLabelValues("acme", "record").Inc()
	m.Storage.ScanDuration.WithLabelValues("acme", "record").Observe(0.01)
	m.Storage.SizeBytes.WithLabelValues("pebble").Set(1024)
	m.Storage.CompactionsTotal.WithLabelValues("pebble").Inc()
	m.Storage.CacheAccessesTotal.WithLabelValues("pebble", "hit").Inc()

	m.Search.Duration.WithLabelValues("acme", "vector").Observe(0.02)
	m.Search.Duration.WithLabelValues("acme", "keyword").Observe(0.02)
	m.Search.Duration.WithLabelValues("acme", "hybrid").Observe(0.02)
	m.Search.CandidatesConsidered.WithLabelValues("acme", "vector").Observe(128)
	m.Search.TruncatedTotal.WithLabelValues("acme", "hybrid").Inc()

	m.Jobs.Pending.WithLabelValues("acme", "decay").Set(3)
	m.Jobs.Running.WithLabelValues("acme", "decay").Set(1)
	m.Jobs.FailuresTotal.WithLabelValues("acme", "decay").Inc()
	m.Jobs.RetriesTotal.WithLabelValues("acme", "decay").Inc()
	m.Jobs.ProcessingDuration.WithLabelValues("acme", "decay").Observe(0.5)

	m.Lifecycle.TransitionsTotal.WithLabelValues("acme", "archived").Inc()
	m.Lifecycle.SweepDuration.WithLabelValues("acme").Observe(1.5)
	m.Lifecycle.DueBacklog.WithLabelValues("acme").Set(0)

	m.Migrations.Running.WithLabelValues("0004_attr_slots").Set(1)
	m.Migrations.Progress.WithLabelValues("0004_attr_slots").Set(0.5)
	m.Migrations.FailuresTotal.WithLabelValues("0004_attr_slots").Inc()
	m.Migrations.RecordsRemaining.WithLabelValues("0004_attr_slots").Set(42)

	body := scrape(t, m.Handler())
	for _, want := range []string{
		"remem_storage_writes_total",
		"remem_storage_reads_total",
		"remem_storage_scan_duration_seconds",
		"remem_storage_size_bytes",
		"remem_storage_compactions_total",
		"remem_storage_cache_accesses_total",
		"remem_search_duration_seconds",
		"remem_search_candidates_considered",
		"remem_search_truncated_total",
		"remem_jobs_pending",
		"remem_jobs_running",
		"remem_jobs_failures_total",
		"remem_jobs_retries_total",
		"remem_jobs_processing_duration_seconds",
		"remem_lifecycle_transitions_total",
		"remem_lifecycle_sweep_duration_seconds",
		"remem_lifecycle_due_backlog",
		"remem_migration_running",
		"remem_migration_progress_ratio",
		"remem_migration_failures_total",
		"remem_migration_records_remaining",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics does not expose %s", want)
		}
	}
}

func TestUserFacingMetricsAreTenantLabelled(t *testing.T) {
	// Invariant 1 reaches the metrics: work done on a tenant's behalf is
	// attributable to that tenant.
	m := obs.NewMetrics()
	m.Storage.WritesTotal.WithLabelValues("acme", "record").Inc()
	m.Jobs.FailuresTotal.WithLabelValues("acme", "decay").Inc()

	body := scrape(t, m.Handler())
	for _, want := range []string{
		`remem_storage_writes_total{op="record",tenant="acme"}`,
		`remem_jobs_failures_total{kind="decay",tenant="acme"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing tenant-labelled series %s in:\n%s", want, body)
		}
	}
}

func TestMetricsIncludeRuntimeCollectors(t *testing.T) {
	body := scrape(t, obs.NewMetrics().Handler())
	if !strings.Contains(body, "go_goroutines") {
		t.Error("the Go runtime collector is not registered")
	}
}

func TestEachMetricsInstanceIsIndependent(t *testing.T) {
	// Two registries in one process: a test must not have to unregister
	// collectors a previous test left behind on a package-level global.
	a, b := obs.NewMetrics(), obs.NewMetrics()
	a.Storage.WritesTotal.WithLabelValues("acme", "record").Inc()
	if strings.Contains(scrape(t, b.Handler()), `tenant="acme"`) {
		t.Fatal("metrics leaked between registries")
	}
}

// spec §50: "Avoid logging memory content by default."
func TestContentIsNeverLogged(t *testing.T) {
	for _, f := range readAllGoFilesUnder(t, "..") {
		if strings.Contains(f.text, `"content"`) && strings.Contains(f.text, "slog") {
			t.Errorf("%s appears to log memory content", f.path)
		}
	}
}

// Phase 1 completion criterion: no package outside internal/obs constructs a
// logger, so there is exactly one place where the format, the level and the
// redaction rules are decided.
func TestOnlyObsConstructsLoggers(t *testing.T) {
	constructors := []string{"slog.New(", "slog.NewJSONHandler(", "slog.NewTextHandler(", "slog.Default()"}
	for _, f := range readAllGoFilesUnder(t, "..") {
		if strings.HasPrefix(filepath.ToSlash(f.path), "../obs/") {
			continue
		}
		for _, c := range constructors {
			if strings.Contains(f.text, c) {
				t.Errorf("%s calls %s — only internal/obs may construct a logger", f.path, c)
			}
		}
	}
}

type goFile struct {
	path string
	text string
}

// readAllGoFilesUnder returns every non-test .go file below root. Test files
// are excluded: this file names the very patterns the scans look for.
func readAllGoFilesUnder(t *testing.T, root string) []goFile {
	t.Helper()
	var out []goFile
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out = append(out, goFile{path: path, text: string(b)})
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	if len(out) == 0 {
		t.Fatalf("no Go files found under %s — the scan is looking in the wrong place", root)
	}
	return out
}

func scrape(t *testing.T, h http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(rec, req) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the metrics handler did not respond")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics returned %d", rec.Code)
	}
	return rec.Body.String()
}
