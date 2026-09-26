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

func TestDefaultsAreValid(t *testing.T) {
	// A development default that does not pass its own validation is a trap.
	if err := config.Default().Validate(); err != nil {
		t.Fatalf("the defaults must validate: %v", err)
	}
}

func TestFixedConstantsAreTheDefaults(t *testing.T) {
	c := config.Default()
	if c.Embedding.Model != "all-MiniLM-L6-v2" {
		t.Errorf("embedding model = %q", c.Embedding.Model)
	}
	if c.Vector.Dimension != 384 {
		t.Errorf("vector dimension = %d, want 384", c.Vector.Dimension)
	}
	if c.Search.RRFK != 60 {
		t.Errorf("rrf k = %d, want 60", c.Search.RRFK)
	}
	if c.Search.WidenMaxFactor != 32 {
		t.Errorf("widen_max_factor = %d, want 32", c.Search.WidenMaxFactor)
	}
	// Rust's config.rs:300. Deliberately larger than the search bound: a
	// listing candidate is far cheaper than a widened search's.
	if c.Search.ListMaxFactor != 128 {
		t.Errorf("list_max_factor = %d, want 128", c.Search.ListMaxFactor)
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
	if c.Vector.Dimension != 384 {
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

// An upgrade from Rust Remem must not be a rewrite of every deployment
// manifest: REMEM_API_KEY and REMEM_DATA_DIR are in every existing
// environment.
func TestLegacyEnvironmentVariablesStillWork(t *testing.T) {
	c, err := config.Load(nil, []string{
		"REMEM_API_KEY=from-the-old-deployment",
		"REMEM_DATA_DIR=/srv/remem",
		"REMEM_ENV=development",
	}, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Server.APIKey != "from-the-old-deployment" {
		t.Errorf("server.api_key = %q", c.Server.APIKey)
	}
	if c.Storage.Path != "/srv/remem" {
		t.Errorf("storage.path = %q", c.Storage.Path)
	}
}

// Rust's rate-limit variables were refused while Go had no limiter. Now they
// are aliases again — same numbers, counted per tenant rather than per address.
func TestRateLimitVariablesAreAliasesAndOnByDefault(t *testing.T) {
	def := config.Default()
	if def.Server.RateLimitRPS != 100 || def.Server.RateLimitBurst != 50 {
		t.Fatalf("defaults are %d rps, burst %d; Rust shipped 100 and 50, on",
			def.Server.RateLimitRPS, def.Server.RateLimitBurst)
	}

	c, err := config.Load(nil, []string{
		"REMEM_RATE_LIMIT_RPS=7",
		"REMEM_RATE_LIMIT_BURST=3",
	}, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Server.RateLimitRPS != 7 || c.Server.RateLimitBurst != 3 {
		t.Fatalf("server.rate_limit_rps = %d, server.rate_limit_burst = %d; want 7 and 3",
			c.Server.RateLimitRPS, c.Server.RateLimitBurst)
	}
}

func TestAnUnservableRateLimitIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
		key  string
	}{
		{"negative rate", []string{"REMEM_SERVER_RATE_LIMIT_RPS=-1"}, "server.rate_limit_rps"},
		{"no burst while limiting", []string{"REMEM_SERVER_RATE_LIMIT_BURST=0"}, "server.rate_limit_burst"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Load(nil, tc.env, nil)
			if err == nil {
				t.Fatalf("%v was accepted", tc.env)
			}
			if !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("the refusal does not name %s: %v", tc.key, err)
			}
		})
	}

	// A burst of zero is fine when limiting is off: nothing reads it.
	if _, err := config.Load(nil, []string{
		"REMEM_SERVER_RATE_LIMIT_RPS=0", "REMEM_SERVER_RATE_LIMIT_BURST=0",
	}, nil); err != nil {
		t.Fatalf("limiting off with a zero burst was refused: %v", err)
	}
}

// A setting that is gone must say so, not be ignored. Each of these was
// security-relevant, and an operator whose CORS restriction silently stopped
// being enforced would find out from an incident.
func TestRetiredEnvironmentVariablesRefuseWithAnExplanation(t *testing.T) {
	for _, name := range []string{
		"REMEM_ALLOW_AUTH_DISABLED=true",
		"REMEM_CORS_ORIGINS=https://example.com",
		"REMEM_TRUST_PROXY_HEADERS=true",
		"REMEM_PORT=9000",
	} {
		_, err := config.Load(nil, []string{name}, nil)
		if err == nil {
			t.Errorf("%s was ignored", name)
			continue
		}
		key, _, _ := strings.Cut(name, "=")
		if !strings.Contains(err.Error(), key) {
			t.Errorf("the error does not name %s: %v", key, err)
		}
	}
}

// The three tokeniser dials were removed because they decided what was written
// to disk rather than how fast a query ran. A config file still carrying one is
// refused by name and told why — an operator whose setting silently stopped
// being read would find out from a support ticket instead.
func TestRetiredTokeniserSettingsRefuseWithAnExplanation(t *testing.T) {
	for _, env := range []string{
		"REMEM_TEXT_MIN_TOKEN_LENGTH=3",
		"REMEM_TEXT_MAX_TOKEN_LENGTH=100",
		"REMEM_TEXT_REMOVE_STOP_WORDS=false",
	} {
		_, err := config.Load(nil, []string{env}, nil)
		if err == nil {
			t.Errorf("%s was ignored", env)
			continue
		}
		if !strings.Contains(err.Error(), "no longer") {
			t.Errorf("%s was refused as an unknown setting rather than a retired one: %v", env, err)
		}
	}

	// text.enabled stays: turning the index off is a deployment decision, not
	// a change to what a query means.
	cfg, err := config.Load(nil, []string{"REMEM_TEXT_ENABLED=false"}, nil)
	if err != nil {
		t.Fatalf("text.enabled was refused: %v", err)
	}
	if cfg.Text.Enabled {
		t.Fatal("text.enabled=false did not take effect")
	}
}

// remem-mcp is routinely launched from the same shell as the server it talks
// to, so its variables must not stop the server from starting.
func TestTheBridgesVariablesAreIgnoredByTheServer(t *testing.T) {
	if _, err := config.Load(nil, []string{
		"REMEM_MCP_URL=http://localhost:4545/mcp",
		"REMEM_MCP_API_KEY=secret",
		"REMEM_MCP_TENANT=acme",
	}, nil); err != nil {
		t.Fatalf("the bridge's variables stopped the server: %v", err)
	}
}

// The strict rule still holds for everything else: a typo silently dropped is
// a setting an operator believes is in force and is not.
func TestATypoInAnEnvironmentVariableIsStillRefused(t *testing.T) {
	if _, err := config.Load(nil, []string{"REMEM_SERVER_HTTP_ADDRESS=1.2.3.4:80"}, nil); err == nil {
		t.Fatal("a misspelled setting was ignored")
	}
}

// A secret list nobody configured must read as unset, not as "(set,
// redacted)": telling an operator they have credentials they do not have is
// worse than saying nothing.
func TestAnUnsetSecretListReadsAsUnset(t *testing.T) {
	c := config.Default()
	if !strings.Contains(c.Redacted(), "server.api_keys = (unset)") {
		t.Fatalf("api_keys is not reported as unset:\n%s", c.Redacted())
	}
	c.Server.APIKeys = []string{"key@acme"}
	if !strings.Contains(c.Redacted(), "server.api_keys = (set, redacted)") {
		t.Fatalf("a configured api_keys is not reported as set:\n%s", c.Redacted())
	}
}

func TestTwoNamesForOneSettingAreRefusedRatherThanRaced(t *testing.T) {
	// A legacy variable and its canonical name are two spellings of one
	// setting, and os.Environ() has no defined order, so the winner would
	// otherwise be whichever the operating system handed over second.
	//
	// Found by the Phase 9 verification run: a REMEM_API_KEY left over in the
	// shell silently overrode the REMEM_SERVER_API_KEY the run had just
	// exported, and the server rejected its own operator credential with
	// "the presented credential is not one this server issued". Nothing said
	// anything, and both variables were plainly visible in the environment.
	env := []string{
		"REMEM_SERVER_API_KEY=the-one-i-just-set-000000",
		"REMEM_API_KEY=the-one-left-over-from-before",
	}
	_, err := config.Load(nil, env, nil)
	if !errs.Is(err, errs.Invalid) {
		t.Fatalf("two names for one setting were resolved silently: %v", err)
	}
	for _, want := range []string{"REMEM_API_KEY", "REMEM_SERVER_API_KEY", "server.api_key"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %s: %v", want, err)
		}
	}

	// Reversing the order must refuse identically. A rule that depends on
	// which came first is the same coin flip wearing a different hat.
	if _, err := config.Load(nil, []string{env[1], env[0]}, nil); !errs.Is(err, errs.Invalid) {
		t.Fatalf("the refusal depends on the order of the environment: %v", err)
	}

	// The same value under both names is not a conflict: it is a deployment
	// that set the belt and the braces, and refusing it would break exactly
	// the upgrade the legacy names exist to protect.
	agreeing := []string{
		"REMEM_SERVER_API_KEY=the-same-value-under-both-000",
		"REMEM_API_KEY=the-same-value-under-both-000",
	}
	cfg, err := config.Load(nil, agreeing, nil)
	if err != nil {
		t.Fatalf("two names agreeing on one value were refused: %v", err)
	}
	if cfg.Server.APIKey != "the-same-value-under-both-000" {
		t.Fatalf("the agreed value did not land: %q", cfg.Server.APIKey)
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
