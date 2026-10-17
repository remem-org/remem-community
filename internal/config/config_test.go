package config_test

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/obs"
)

func TestValidationRejectsContradictions(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*config.Config)
		want string
	}{
		{"zero vector dimension", func(c *config.Config) { c.Vector.Dimension = 0 }, "vector.dimension"},
		{"auth disabled in production", func(c *config.Config) {
			c.Server.Env = "production"
			c.Server.APIKey = ""
		}, "authentication"},
		{"tenant id invalid", func(c *config.Config) { c.Tenant.Default = "Acme Corp!" }, "tenant.default"},
		{"widen factor below one", func(c *config.Config) { c.Search.WidenMaxFactor = 0 }, "widen_max_factor"},
		{"list factor below one", func(c *config.Config) { c.Search.ListMaxFactor = 0 }, "list_max_factor"},
		{"unknown storage engine", func(c *config.Config) { c.Storage.Engine = "sqlite" }, "storage.engine"},
		{"empty storage path", func(c *config.Config) { c.Storage.Path = "" }, "storage.path"},
		{"negative intra-op threads", func(c *config.Config) { c.Embedding.IntraOpThreads = -1 },
			"embedding.intra_op_threads"},
		// Negative is refused whether or not the queue is on: unlike zero or a
		// size past the bound, it is never a value someone means.
		{"negative index queue size with the queue off", func(c *config.Config) {
			c.Vector.AsyncIndexing = false
			c.Vector.IndexQueueSize = -1
		}, "vector.index_queue_size"},
		{"negative row cache", func(c *config.Config) { c.Search.RowCacheEntries = -1 }, "search.row_cache_entries"},
		{"unknown vector index", func(c *config.Config) { c.Vector.Index = "ivf" }, "vector.index"},
		{"dimension contradicts the model", func(c *config.Config) { c.Vector.Dimension = 768 }, "vector.dimension"},
		{"max limit below default", func(c *config.Config) {
			c.Search.DefaultLimit = 100
			c.Search.MaxLimit = 10
		}, "search.max_limit"},
		{"zero rrf k", func(c *config.Config) { c.Search.RRFK = 0 }, "search.rrf_k"},
		{"no job workers", func(c *config.Config) { c.Jobs.Workers = 0 }, "jobs.workers"},
		{"zero job lease", func(c *config.Config) { c.Jobs.LeaseDuration = 0 }, "jobs.lease_duration"},
		{"zero sweep interval", func(c *config.Config) { c.Lifecycle.SweepInterval = 0 }, "lifecycle.sweep_interval"},
		{"unknown log level", func(c *config.Config) { c.Log.Level = "chatty" }, "log.level"},
		{"unknown log format", func(c *config.Config) { c.Log.Format = "yaml" }, "log.format"},
		{"metrics path without a slash", func(c *config.Config) { c.Metrics.Path = "metrics" }, "metrics.path"},
		{"metrics path at the root", func(c *config.Config) { c.Metrics.Path = "/" }, "metrics.path"},
		{"metrics path under the API", func(c *config.Config) { c.Metrics.Path = "/api/v1/stats" }, "metrics.path"},
		{"metrics path at MCP", func(c *config.Config) { c.Metrics.Path = "/mcp" }, "metrics.path"},
		{"metrics path under MCP", func(c *config.Config) { c.Metrics.Path = "/mcp/stats" }, "metrics.path"},
		{"metrics path with pattern syntax", func(c *config.Config) { c.Metrics.Path = "/scrape/{tenant}" }, "metrics.path"},
		{"metrics path with a space", func(c *config.Config) { c.Metrics.Path = "/ops scrape" }, "metrics.path"},
		{"metrics address malformed", func(c *config.Config) { c.Metrics.Addr = "nonsense" }, "metrics.addr"},
		{"metrics address is the main listener", func(c *config.Config) {
			c.Server.HTTPAddr = "127.0.0.1:9000"
			c.Metrics.Addr = "127.0.0.1:9000"
		}, "metrics.addr"},
		{"unknown server env", func(c *config.Config) { c.Server.Env = "staging-ish" }, "server.env"},
		{"memory engine in production", func(c *config.Config) {
			c.Server.Env = "production"
			c.Server.APIKey = "a-real-looking-key-of-sufficient-length"
			c.Storage.Engine = "memory"
		}, "storage.engine"},
		{"gc percent below minus one", func(c *config.Config) { c.Runtime.GCPercent = -2 }, "runtime.gc_percent"},
		{"negative memory limit", func(c *config.Config) { c.Runtime.MemoryLimitBytes = -1 }, "runtime.memory_limit_bytes"},
		{"negative memtable size", func(c *config.Config) { c.Storage.MemTableSizeBytes = -1 }, "storage.memtable_size_bytes"},
		{"negative bloom bits", func(c *config.Config) { c.Storage.BloomBitsPerKey = -1 }, "storage.bloom_bits_per_key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := config.Default()
			tc.mut(&c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an error naming %q, got %v", tc.want, err)
			}
			if !errs.Is(err, errs.Invalid) {
				t.Fatalf("invalid configuration is caller error, got kind %v", errs.KindOf(err))
			}
		})
	}
}

func TestValidationReportsEveryProblemAtOnce(t *testing.T) {
	// Fail fast (spec §51), but not one problem per restart.
	c := config.Default()
	c.Vector.Dimension = 0
	c.Jobs.Workers = -1
	err := c.Validate()
	if err == nil {
		t.Fatal("want errors")
	}
	for _, want := range []string{"vector.dimension", "jobs.workers"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}

func TestRedactedNeverContainsSecrets(t *testing.T) {
	c := config.Default()
	c.Server.APIKey = "sk-do-not-print-me"
	if strings.Contains(c.Redacted(), "sk-do-not-print-me") {
		t.Fatal("Redacted() leaked the API key")
	}
	if !strings.Contains(c.Redacted(), "server.api_key") {
		t.Fatal("Redacted() must still show that the key is set, just not its value")
	}
	if !strings.Contains(c.Redacted(), "storage.path") {
		t.Fatal("Redacted() is the inspectable rendering: non-secret keys stay visible")
	}
}

func TestLoadPrecedenceIsFlagsThenEnvThenFileThenDefaults(t *testing.T) {
	path := writeTOML(t, `
[server]
http_addr = "127.0.0.1:1111"
env = "production"
api_key = "from-file-long-enough"

[jobs]
workers = 3
`)
	// File alone.
	c, err := config.Load([]string{path}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.HTTPAddr != "127.0.0.1:1111" || c.Jobs.Workers != 3 {
		t.Fatalf("file values not applied: %+v", c.Server)
	}
	if c.Vector.Dimension != config.EmbeddingDimension {
		t.Fatal("unset keys must keep their defaults")
	}

	// Environment beats the file.
	env := []string{"REMEM_SERVER_HTTP_ADDR=127.0.0.1:2222", "REMEM_JOBS_WORKERS=7"}
	c, err = config.Load([]string{path}, env, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.HTTPAddr != "127.0.0.1:2222" || c.Jobs.Workers != 7 {
		t.Fatalf("environment did not override the file: %+v", c.Server)
	}

	// Flags beat the environment.
	args := []string{"--server.http_addr=127.0.0.1:3333", "--jobs.workers", "9"}
	c, err = config.Load([]string{path}, env, args)
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.HTTPAddr != "127.0.0.1:3333" || c.Jobs.Workers != 9 {
		t.Fatalf("flags did not override the environment: %+v", c.Server)
	}
	if c.Server.APIKey != "from-file-long-enough" {
		t.Fatal("a key nobody overrode must survive from the file")
	}
}

func TestLoadParsesEveryScalarKind(t *testing.T) {
	path := writeTOML(t, `
[jobs]
lease_duration = "45s"

[storage]
sync_writes = false

[search]
similarity_threshold = 0.42
`)
	c, err := config.Load([]string{path}, []string{
		"REMEM_LIFECYCLE_SWEEP_INTERVAL=2h",
		"REMEM_METRICS_ENABLED=false",
		"REMEM_SEARCH_MAX_LIMIT=250",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Jobs.LeaseDuration != 45*time.Second {
		t.Errorf("lease_duration = %v", c.Jobs.LeaseDuration)
	}
	if c.Storage.SyncWrites {
		t.Error("sync_writes = true, want false from the file")
	}
	if c.Search.SimilarityThreshold != 0.42 {
		t.Errorf("similarity_threshold = %v", c.Search.SimilarityThreshold)
	}
	if c.Lifecycle.SweepInterval != 2*time.Hour {
		t.Errorf("sweep_interval = %v", c.Lifecycle.SweepInterval)
	}
	if c.Metrics.Enabled {
		t.Error("metrics.enabled = true, want false from the environment")
	}
	if c.Search.MaxLimit != 250 {
		t.Errorf("max_limit = %d", c.Search.MaxLimit)
	}
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	// A typo in a config file must fail loudly, not be ignored into a default.
	path := writeTOML(t, "[server]\nhttp_adress = \"127.0.0.1:9999\"\n")
	_, err := config.Load([]string{path}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "http_adress") {
		t.Fatalf("want an error naming the unknown key, got %v", err)
	}
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("kind = %v, want Invalid", errs.KindOf(err))
	}
}

func TestLoadRejectsUnknownEnvAndFlagKeys(t *testing.T) {
	if _, err := config.Load(nil, []string{"REMEM_SERVER_HTTP_ADRESS=x"}, nil); err == nil ||
		!strings.Contains(err.Error(), "REMEM_SERVER_HTTP_ADRESS") {
		t.Fatalf("want an error naming the unknown variable, got %v", err)
	}
	if _, err := config.Load(nil, nil, []string{"--server.http_adress=x"}); err == nil ||
		!strings.Contains(err.Error(), "server.http_adress") {
		t.Fatalf("want an error naming the unknown flag, got %v", err)
	}
	// A variable that is not ours is not our business.
	if _, err := config.Load(nil, []string{"PATH=/usr/bin", "HOME=/root"}, nil); err != nil {
		t.Fatalf("non-REMEM environment variables must be ignored: %v", err)
	}
}

func TestLoadRejectsUnparseableValues(t *testing.T) {
	_, err := config.Load(nil, []string{"REMEM_JOBS_WORKERS=lots"}, nil)
	if err == nil || !strings.Contains(err.Error(), "jobs.workers") {
		t.Fatalf("want an error naming jobs.workers, got %v", err)
	}
	_, err = config.Load(nil, []string{"REMEM_JOBS_LEASE_DURATION=soon"}, nil)
	if err == nil || !strings.Contains(err.Error(), "jobs.lease_duration") {
		t.Fatalf("want an error naming jobs.lease_duration, got %v", err)
	}
}

func TestLoadSkipsAbsentSearchPathsButNotAnExplicitOne(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.toml")
	if _, err := config.Load([]string{missing}, nil, nil); err != nil {
		t.Fatalf("a search path that does not exist is not an error: %v", err)
	}
	_, err := config.Load(nil, nil, []string{"--config=" + missing})
	if err == nil || !strings.Contains(err.Error(), missing) {
		t.Fatalf("a file the operator named must exist, got %v", err)
	}
}

func TestExplicitConfigFlagBeatsTheSearchPaths(t *testing.T) {
	searched := writeTOML(t, "[server]\nhttp_addr = \"127.0.0.1:1111\"\n")
	explicit := writeTOML(t, "[server]\nhttp_addr = \"127.0.0.1:2222\"\n")
	c, err := config.Load([]string{searched}, nil, []string{"--config", explicit})
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.HTTPAddr != "127.0.0.1:2222" {
		t.Fatalf("http_addr = %s, want the explicitly named file to win", c.Server.HTTPAddr)
	}
}

func TestLoadValidates(t *testing.T) {
	// Startup validation (spec §51) is part of loading, not something a caller
	// may forget.
	_, err := config.Load(nil, []string{"REMEM_VECTOR_DIMENSION=0"}, nil)
	if err == nil || !strings.Contains(err.Error(), "vector.dimension") {
		t.Fatalf("Load must validate, got %v", err)
	}
}

func TestShippedConfigFileLoadsAndValidates(t *testing.T) {
	// config/remem.toml is documentation that has to keep compiling.
	path := filepath.Join("..", "..", "config", "remem.toml")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the shipped example config is missing: %v", err)
	}
	if _, err := config.Load([]string{path}, nil, nil); err != nil {
		t.Fatalf("the shipped example config does not load: %v", err)
	}
}

func TestLogConfigConversion(t *testing.T) {
	c := config.Default()
	c.Log.Level = "debug"
	c.Log.Format = "text"
	got := c.LogConfig()
	if got.Level != slog.LevelDebug {
		t.Errorf("level = %v, want debug", got.Level)
	}
	if got.Format != obs.FormatText {
		t.Errorf("format = %v, want text", got.Format)
	}
}

func writeTOML(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "remem.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// `remem start --data-dir ./data` is the command in the specification and in
// every piece of documentation, so it has to work.
func TestDataDirIsAnAliasForStoragePath(t *testing.T) {
	c, err := config.Load(nil, nil, []string{"--data-dir", "/srv/remem"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Storage.Path != "/srv/remem" {
		t.Fatalf("storage.path = %q", c.Storage.Path)
	}
	if c, err := config.Load(nil, nil, []string{"--data-dir=/srv/other"}); err != nil {
		t.Fatalf("Load: %v", err)
	} else if c.Storage.Path != "/srv/other" {
		t.Fatalf("storage.path = %q", c.Storage.Path)
	}
}

// A metrics path of the operator's choosing is accepted, and so is turning the
// endpoint off with a path that would otherwise be refused: a disabled endpoint
// mounts nothing, so its path collides with nothing.
func TestAMetricsPathOfTheOperatorsChoosingIsAccepted(t *testing.T) {
	c := config.Default()
	c.Metrics.Path = "/ops/scrape"
	if err := c.Validate(); err != nil {
		t.Fatalf("a metrics path of /ops/scrape was refused: %v", err)
	}
	c.Metrics.Enabled = false
	c.Metrics.Path = "/api/v1/stats"
	if err := c.Validate(); err != nil {
		t.Fatalf("a disabled metrics endpoint was refused for its path: %v", err)
	}
}

func TestExternalEmbeddingProviderIsValidatedByName(t *testing.T) {
	external := func(c *config.Config) {
		c.Embedding.Provider = config.ProviderExternal
		c.Embedding.ExternalModelID = "cohere-embed-english-v3.0"
		c.Vector.Dimension = 768
	}
	for _, tc := range []struct {
		name string
		mut  func(*config.Config)
		want string
	}{
		{"unknown provider", func(c *config.Config) { c.Embedding.Provider = "openai" }, "embedding.provider"},
		{"external without a model id", func(c *config.Config) {
			external(c)
			c.Embedding.ExternalModelID = ""
		}, "embedding.external_model_id"},
		{"external claiming the built-in model", func(c *config.Config) {
			external(c)
			c.Embedding.ExternalModelID = config.EmbeddingModel
		}, "embedding.external_model_id"},
		{"external above the widest dimension", func(c *config.Config) {
			external(c)
			c.Vector.Dimension = 4097
		}, "vector.dimension"},
		{"external with a zero dimension", func(c *config.Config) {
			external(c)
			c.Vector.Dimension = 0
		}, "vector.dimension"},
		{"onnx with an external model id", func(c *config.Config) {
			c.Embedding.ExternalModelID = "cohere-embed-english-v3.0"
		}, "embedding.external_model_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := config.Default()
			tc.mut(&c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an error naming %q, got %v", tc.want, err)
			}
			if !errs.Is(err, errs.Invalid) {
				t.Fatalf("invalid configuration is caller error, got kind %v", errs.KindOf(err))
			}
		})
	}

	t.Run("a valid external configuration", func(t *testing.T) {
		c := config.Default()
		external(&c)
		if err := c.Validate(); err != nil {
			t.Fatalf("an external provider at 768 dimensions is valid: %v", err)
		}
	})
}
