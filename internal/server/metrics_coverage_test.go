package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/clock"
	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/embedding/embeddingtest"
	"github.com/remem-org/remem-go/internal/jobs"
	"github.com/remem-org/remem-go/internal/schema"
	pebblekv "github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/tenant"
)

const coverageKey = "coverage-operator-key-00000000"

// notRecordedByAnOrdinaryWorkload are the declared families this test does not
// require, each with the reason. An entry here is a claim that the family
// cannot be produced without breaking something, and the test fails if one of
// them turns up after all — an allowlist nobody prunes becomes a list of
// excuses.
var notRecordedByAnOrdinaryWorkload = map[string]string{
	"remem_migration_failures_total": "it needs a migration step to fail, which is a broken store rather " +
		"than a workload; schema's runner tests drive the failure path",
}

// TestEveryDeclaredMetricIsRecorded is the metric equivalent of
// TestEveryMCPToolHasATest.
//
// The storage and search collectors were registered in Phase 3 and scraped
// empty for ten phases, and nothing noticed, because a family with no samples
// does not appear in the exposition at all: "declared and never recorded" is
// invisible from /metrics. This test runs a real server over a real Pebble
// directory through an ordinary workload and requires every declared family to
// have a sample.
func TestEveryDeclaredMetricIsRecorded(t *testing.T) {
	cfg := config.Default()
	cfg.Storage.Path = filepath.Join(t.TempDir(), "data")
	cfg.Storage.SyncWrites = false
	cfg.Server.HTTPAddr = "127.0.0.1:0"
	cfg.Server.APIKey = coverageKey
	cfg.Server.RateLimitRPS = 0
	cfg.Log.Level = "error"
	// Every search over three or more matches reaches this bound and reports
	// truncated, which is how remem_search_truncated_total gets a sample.
	cfg.Search.MaxPageDepth = 2
	cfg.Lifecycle.SweepInterval = 500 * time.Millisecond
	cfg.Jobs.PollInterval = 50 * time.Millisecond

	client := &http.Client{Timeout: 10 * time.Second}

	// Life one seeds a corpus, so the migration life two runs has records to
	// report progress over.
	{
		srv := newCoverageServer(t, cfg)
		stop := runCoverageServer(t, srv)
		base := "http://" + srv.Addr()
		var batch strings.Builder
		batch.WriteString(`{"memories":[`)
		for n := range 30 {
			if n > 0 {
				batch.WriteByte(',')
			}
			fmt.Fprintf(&batch, `{"content":"seeded raft note %d"}`, n)
		}
		batch.WriteString(`]}`)
		mustStatus(t, client, "POST", base+"/api/v1/memories:batch", batch.String(), http.StatusCreated)
		stop()
	}
	windAttrSchemaBack(t, cfg.Storage.Path)

	// Life two runs the backfill at start-up, then serves the workload.
	srv := newCoverageServer(t, cfg)
	release := make(chan struct{})
	for _, e := range []jobs.Entry{
		{Type: "coverage.fail_once", MaxAttempts: 2, Handler: jobs.HandlerFunc(
			func(_ context.Context, j *jobs.Job, _ jobs.Checkpointer) error {
				if j.Attempts < 2 {
					return errors.New("the first attempt fails on purpose")
				}
				return nil
			})},
		{Type: "coverage.always_fail", MaxAttempts: 1, Handler: jobs.HandlerFunc(
			func(context.Context, *jobs.Job, jobs.Checkpointer) error {
				return errors.New("this job fails on purpose")
			})},
		{Type: "coverage.hold", MaxAttempts: 1, Handler: jobs.HandlerFunc(
			func(ctx context.Context, _ *jobs.Job, _ jobs.Checkpointer) error {
				select {
				case <-release:
				case <-ctx.Done():
				}
				return nil
			})},
		{Type: "coverage.later", MaxAttempts: 1, Handler: jobs.HandlerFunc(
			func(context.Context, *jobs.Job, jobs.Checkpointer) error { return nil })},
	} {
		e.Description = "a metric-coverage test job"
		if err := srv.deps.registry.Register(e); err != nil {
			t.Fatal(err)
		}
	}
	stop := runCoverageServer(t, srv)
	defer stop()
	defer close(release)
	base := "http://" + srv.Addr()
	ctx := context.Background()

	driveWorkload(t, client, base)

	// The job families: a retry, a failure, one job held running and one due in
	// an hour, both present when the gauges are refreshed.
	const def = tenant.ID("default")
	submit := func(typ jobs.Type, runAt time.Time) *jobs.Job {
		j := &jobs.Job{Tenant: def, Namespace: tenant.DefaultNamespace, Type: typ, RunAt: runAt}
		if err := srv.deps.queue.Submit(ctx, j); err != nil {
			t.Fatalf("submitting %s: %v", typ, err)
		}
		return j
	}
	submit("coverage.fail_once", time.Time{})
	submit("coverage.always_fail", time.Time{})
	submit("coverage.later", time.Now().Add(time.Hour))
	held := submit("coverage.hold", time.Time{})
	waitFor(t, 20*time.Second, "the held job to be running", func() bool {
		j, err := srv.deps.queue.Get(ctx, def, held.ID)
		return err == nil && j.State == jobs.Running
	})
	// The pool refreshes these every thirty seconds; the test asks now rather
	// than wait for the tick.
	if err := srv.deps.pool.RefreshGauges(ctx); err != nil {
		t.Fatal(err)
	}

	declared := srv.Metrics().DeclaredNames()
	var missing []string
	deadline := time.Now().Add(30 * time.Second)
	for {
		present := scrapeFamilies(t, client, base, declared)
		missing = missing[:0]
		for _, name := range declared {
			if _, excused := notRecordedByAnOrdinaryWorkload[name]; excused {
				if present[name] {
					t.Errorf("%s is allowlisted as unrecordable and was recorded; remove its entry", name)
				}
				continue
			}
			if !present[name] {
				missing = append(missing, name)
			}
		}
		if len(missing) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if len(missing) > 0 {
		t.Fatalf("%d declared metric families have no sample after an ordinary workload:\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}
}

func newCoverageServer(t *testing.T, cfg config.Config) *Server {
	t.Helper()
	srv, err := New(cfg, WithEmbedder(embeddingtest.New()), WithClock(clock.System()))
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	return srv
}

// runCoverageServer runs srv until the returned function is called, which waits
// for it to stop.
func runCoverageServer(t *testing.T, srv *Server) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	waitFor(t, 30*time.Second, "the server to be ready", func() bool { return srv.Ready() && srv.Addr() != "" })
	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}
}

// windAttrSchemaBack stamps the directory one attribute-schema version behind,
// so the next start runs the shipped backfill over the corpus already there.
func windAttrSchemaBack(t *testing.T, dir string) {
	t.Helper()
	kv, err := pebblekv.Open(dir, pebblekv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	f, err := schema.ReadFormat(context.Background(), kv)
	if err != nil {
		t.Fatal(err)
	}
	f.Subsystems["attr_schema"] = schema.Subsystem{Current: 1, MinReader: 1, MinWriter: 1}
	if err := schema.WriteFormat(context.Background(), kv, f); err != nil {
		t.Fatal(err)
	}
}

// driveWorkload is what an ordinary deployment does in its first minute.
func driveWorkload(t *testing.T, client *http.Client, base string) {
	t.Helper()
	var ids []string
	for _, body := range []string{
		`{"content":"raft elects a leader by majority vote"}`,
		`{"content":"raft replicates its log to every follower"}`,
		`{"content":"raft needs a quorum to commit an entry"}`,
		// Identical content, identical vectors under the fake embedder: discovery
		// links them, which is how an edge gets created.
		`{"content":"duplicate invoice INV-2024-8871 for the office lease"}`,
		`{"content":"duplicate invoice INV-2024-8871 for the office lease"}`,
		// Expires within a sweep or two, which is a lifecycle transition.
		`{"content":"a note that lasts one second","ttl_seconds":1}`,
	} {
		out := mustStatus(t, client, "POST", base+"/api/v1/memories", body, http.StatusCreated)
		ids = append(ids, jsonField(t, out, "id"))
	}
	mustStatus(t, client, "GET", base+"/api/v1/memories/"+ids[0], "", http.StatusOK)
	mustStatus(t, client, "PATCH", base+"/api/v1/memories/"+ids[1],
		`{"content":"raft replicates its log to a majority of followers"}`, http.StatusOK)
	for _, mode := range []string{"semantic", "keyword", "hybrid"} {
		mustStatus(t, client, "POST", base+"/api/v1/memories/search",
			fmt.Sprintf(`{"query":"raft","search_type":%q,"limit":1}`, mode), http.StatusOK)
	}
	mustStatus(t, client, "POST", base+"/api/v1/memories/"+ids[0]+"/connections",
		fmt.Sprintf(`{"target_id":%q,"relationship_type":"supports","strength":0.8}`, ids[2]), http.StatusCreated)
	mustStatus(t, client, "GET", base+"/api/v1/memories/"+ids[0]+"/related", "", http.StatusOK)
	mustStatus(t, client, "GET", base+"/api/v1/memories/"+ids[0]+"/history", "", http.StatusOK)
	mustStatus(t, client, "POST", base+"/api/v1/memories/recall",
		`{"context":"raft","search_type":"hybrid","token_budget":500}`, http.StatusOK)
	mustStatus(t, client, "POST", base+"/api/v1/admin/jobs/text.rebuild/run", "", http.StatusAccepted)
	mustStatus(t, client, "DELETE", base+"/api/v1/memories/"+ids[2], "", http.StatusNoContent)
}

func mustStatus(t *testing.T, client *http.Client, method, url, body string, want int) []byte {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+coverageKey)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		t.Fatalf("%s %s: %d, want %d: %.300s", method, url, resp.StatusCode, want, out)
	}
	return out
}

func jsonField(t *testing.T, body []byte, field string) string {
	t.Helper()
	marker := `"` + field + `":"`
	i := strings.Index(string(body), marker)
	if i < 0 {
		t.Fatalf("no %q in %.200s", field, body)
	}
	rest := string(body)[i+len(marker):]
	return rest[:strings.IndexByte(rest, '"')]
}

// scrapeFamilies reads /metrics and reports which declared families have at
// least one sample. A histogram's samples are named with _bucket, _sum and
// _count; they are credited to the family.
func scrapeFamilies(t *testing.T, client *http.Client, base string, declared []string) map[string]bool {
	t.Helper()
	body := mustStatus(t, client, "GET", base+"/metrics", "", http.StatusOK)
	present := map[string]bool{}
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name := line
		if i := strings.IndexAny(line, "{ "); i >= 0 {
			name = line[:i]
		}
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			if base, ok := strings.CutSuffix(name, suffix); ok && slices.Contains(declared, base) {
				name = base
				break
			}
		}
		present[name] = true
	}
	return present
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
