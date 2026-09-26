// Package config is Remem's configuration: the whole of it, in one struct, in
// one place.
//
// Spec §51 asks for configuration that is explicit and inspectable, validated
// at startup, failing fast on invalid values, with no behaviour hidden behind
// environment-specific defaults. Three rules follow:
//
//   - Every default is a named constant in this file, so "what does it do if I
//     do not set it" is answered by reading one page.
//   - Nothing reads the environment except [Load]. A subsystem that consults
//     os.Getenv is a behaviour no configuration file can describe.
//   - The whole configuration is validated before the first byte is written,
//     and every message names the key at fault.
//
// Precedence is flags, then environment, then file, then defaults.
package config

import (
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/remem-org/remem-go/internal/obs"
)

// Config is the complete configuration of a Remem process, in the domains
// spec §51 names.
type Config struct {
	Node      NodeConfig      `toml:"node"`
	Storage   StorageConfig   `toml:"storage"`
	Server    ServerConfig    `toml:"server"`
	Tenant    TenantConfig    `toml:"tenant"`
	Embedding EmbeddingConfig `toml:"embedding"`
	Vector    VectorConfig    `toml:"vector"`
	Text      TextConfig      `toml:"text"`
	Search    SearchConfig    `toml:"search"`
	Paging    PagingConfig    `toml:"paging"`
	Lifecycle LifecycleConfig `toml:"lifecycle"`
	Discovery DiscoveryConfig `toml:"discovery"`
	Jobs      JobsConfig      `toml:"jobs"`
	Log       LogConfig       `toml:"log"`
	Metrics   MetricsConfig   `toml:"metrics"`
}

// NodeConfig identifies this process. The cluster (Phase 14) gives these
// meaning; single-node runs still log them, so a log line from a laptop and
// one from a cluster member have the same shape.
type NodeConfig struct {
	// ID is this node's stable identity. Empty means "derive from the
	// hostname at startup" — the one derived default, because a node id that
	// changes on restart is worse than a hostname.
	ID string `toml:"id"`
}

// StorageConfig covers the durable store.
type StorageConfig struct {
	// Path is the data directory. Remem owns everything under it.
	Path string `toml:"path"`
	// Engine selects the storage adapter: "pebble" durably, "memory" for
	// tests and ephemeral runs.
	Engine string `toml:"engine"`
	// CacheSizeBytes is the engine's block cache budget.
	CacheSizeBytes int64 `toml:"cache_size_bytes"`
	// SyncWrites fsyncs each commit. Off trades durability of the last
	// writes for throughput, and is a decision an operator must make
	// explicitly rather than discover.
	SyncWrites bool `toml:"sync_writes"`
}

// ServerConfig covers the delivery surfaces.
type ServerConfig struct {
	// Env is "development" or "production". It changes nothing implicitly; it
	// is what validation uses to refuse an unauthenticated production server.
	Env string `toml:"env"`
	// HTTPAddr is the REST listener address.
	HTTPAddr string `toml:"http_addr"`
	// APIKey authenticates HTTP and MCP callers. Required in production. It is
	// the operator's key: bound to no tenant, and therefore able to name one
	// per request with X-Remem-Tenant.
	APIKey string `toml:"api_key" secret:"true"`
	// APIKeys are additional credentials, each written as "<secret>@<tenant>"
	// to bind it to one tenant, or "<secret>" alone for another unbound
	// operator key.
	//
	// A bound key is an isolation boundary rather than a suggestion: no header
	// can talk it out of its tenant, so a leaked agent key exposes exactly the
	// one tenant it was cut for. That is the whole reason this is a list and
	// not a single string.
	APIKeys []string `toml:"api_keys" secret:"true"`
	// ReadTimeout, WriteTimeout and ShutdownTimeout bound a request and a
	// graceful stop.
	ReadTimeout     time.Duration `toml:"read_timeout"`
	WriteTimeout    time.Duration `toml:"write_timeout"`
	ShutdownTimeout time.Duration `toml:"shutdown_timeout"`
	// MaxRequestBytes caps a request body.
	MaxRequestBytes int64 `toml:"max_request_bytes"`
	// RateLimitRPS is each tenant's sustained request rate. Zero turns
	// limiting off. On by default, as Rust's was: embedding makes a write
	// expensive, and a fresh deployment should not be open to cheap CPU
	// exhaustion.
	RateLimitRPS int `toml:"rate_limit_rps"`
	// RateLimitBurst is how many requests a tenant may send at once.
	RateLimitBurst int `toml:"rate_limit_burst"`
}

// TenantConfig covers identity. Invariant 1: all user-owned data belongs to an
// explicit tenant, so there is always a default one rather than an implicit
// unscoped space.
type TenantConfig struct {
	// Default is the tenant a request without one belongs to.
	Default string `toml:"default"`
	// AutoProvision creates a tenant on first use instead of refusing.
	AutoProvision bool `toml:"auto_provision"`
}

// EmbeddingConfig covers the embedder. The model and its dimension are fixed
// constants of the system, not tuning knobs: a vector produced by another
// model is not comparable with the ones already stored, which is why every
// vector records the model that produced it.
type EmbeddingConfig struct {
	Model string `toml:"model"`
	// ModelPath is the directory holding model.onnx and tokenizer.json,
	// populated by scripts/fetch-model.sh. Nothing is downloaded at runtime: a
	// server that fetched a model on first request would have a cold start
	// measured in minutes and a hard dependency on Hugging Face being
	// reachable from production.
	ModelPath string `toml:"model_path"`
	// ONNXLibraryPath names the ONNX Runtime shared library. Empty uses the
	// loader path, which is what a container with ldconfig run gets.
	ONNXLibraryPath string `toml:"onnx_library_path"`
	BatchSize       int    `toml:"batch_size"`
	// MaxSequenceTokens is the model's input limit; longer text is truncated.
	MaxSequenceTokens int `toml:"max_sequence_tokens"`
	// CacheSize is how many vectors the embedding LRU remembers.
	CacheSize int `toml:"cache_size"`
	// Workers bounds concurrent model runs.
	Workers int `toml:"workers"`
	// FillWindow is how long a partly-filled batch waits for company.
	FillWindow time.Duration `toml:"fill_window"`
}

// VectorConfig covers the vector index.
type VectorConfig struct {
	Dimension int    `toml:"dimension"`
	Index     string `toml:"index"`  // "flat" or "hnsw"
	Metric    string `toml:"metric"` // "cosine", "dot" or "l2"
	// HNSW parameters, used only when Index is "hnsw".
	HNSWM              int `toml:"hnsw_m"`
	HNSWEfConstruction int `toml:"hnsw_ef_construction"`
	HNSWEfSearch       int `toml:"hnsw_ef_search"`
	// ResidentBudgetMB bounds the memory the approximate index may hold across
	// every tenant it has materialised.
	//
	// It is a budget rather than a limit on tenants, because tenants are not
	// the same size: a process serving one corpus of a million memories and
	// nine hundred of a thousand each should spend its memory on the one that
	// needs it. A tenant whose graph is evicted is re-read from its node
	// records on the next search, which costs a scan and never an answer.
	ResidentBudgetMB int `toml:"resident_budget_mb"`
}

// TextConfig covers the inverted index behind keyword search.
//
// # Only one setting, and deliberately
//
// This section used to declare min_token_length, max_token_length and
// remove_stop_words. They were not performance dials: each one decides which
// terms are written into the postings key space, so changing one on a live
// corpus produces memories written before the change that are findable by a
// term and memories written after that are not — silently, with nothing in the
// results to show it.
//
// Tokenisation is therefore fixed in internal/text, alongside RRF's k and
// widen_max_factor, for the reason those are: they change what a query returns
// rather than how fast it returns. The three names are listed in
// retiredSettings so a config file still carrying one is refused by name with
// that explanation, rather than ignored — an operator whose dial silently
// stopped working is worse off than one who has to make an edit.
type TextConfig struct {
	// Enabled turns keyword and hybrid search on. With it off the index is not
	// maintained and those modes are refused by name.
	Enabled bool `toml:"enabled"`
}

// SearchConfig covers query execution and fusion.
type SearchConfig struct {
	DefaultLimit int `toml:"default_limit"`
	MaxLimit     int `toml:"max_limit"`
	// MaxPageDepth bounds a session's ranking beyond its first page. A larger
	// requested page size takes precedence, up to MaxLimit.
	MaxPageDepth int `toml:"max_page_depth"`
	// RRFK is reciprocal-rank fusion's k. Fixed at 60.
	RRFK int `toml:"rrf_k"`
	// WidenMaxFactor bounds how far a filtered search widens its candidate
	// set before giving up. Fixed at 32.
	WidenMaxFactor int `toml:"widen_max_factor"`
	// ListMaxFactor is the same bound for a listing, and larger on purpose: a
	// listing candidate is one attribute-row read, where a widened search
	// re-runs a similarity walk. Rust's number, 128 (config.rs:290-300).
	ListMaxFactor       int     `toml:"list_max_factor"`
	SimilarityThreshold float64 `toml:"similarity_threshold"`
}

// PagingConfig bounds how long abandoned ranked searches pin snapshots.
// Listing retains its independent five-minute idle window.
type PagingConfig struct {
	RankedTTL time.Duration `toml:"ranked_ttl"`
}

// LifecycleConfig covers decay, promotion, TTL and archival.
//
// # Three settings were retired here, and the reason is the same for all three
//
// decay_half_life, promotion_threshold and archive_at_health each described a
// model that named retention policies replace (plan §II.6). Decay is a per-day
// multiplier per policy, not a half-life; promotion is at a recall count, not
// at an importance; and the health that archives is a field on the policy. Each
// is now per tenant and per policy, set through
// PATCH /api/v1/tenants/{id}/policies.
//
// Keeping them would leave a global dial that silently loses to a policy — the
// "two names, one setting" defect Phase 9's end-to-end run found, where two
// environment variables set one value and `os.Environ()` order decided which
// won. They are in retiredSettings, so a file naming one is refused by name
// with that explanation.
type LifecycleConfig struct {
	// Enabled turns the maintenance job and durable recall recording on. With
	// it off nothing decays, nothing expires and a fetch records nothing.
	Enabled bool `toml:"enabled"`

	// SweepInterval is how often the per-tenant maintenance job is scheduled.
	// It is not how fast a backlog drains: a run that spends its budget
	// enqueues its own continuation, so a corpus that is entirely due catches
	// up at worker speed rather than at this interval.
	SweepInterval time.Duration `toml:"sweep_interval"`

	// RunBudget is how many memories one run acts on before leaving the rest
	// for its continuation. It bounds a single run, never the work.
	RunBudget int `toml:"run_budget"`

	// RecallWindow is how close together two fetches of one memory have to be
	// for the second to be free. It is what makes access_count count recall
	// sessions rather than round trips, and unlike Rust's flush timer it is
	// decided against the durable stream, so it survives a restart.
	RecallWindow time.Duration `toml:"recall_window"`

	// EventRetention is how long an audit row is kept. It is enforced per
	// memory by the sweep that was visiting it anyway, so it costs no walk of
	// its own.
	EventRetention time.Duration `toml:"event_retention"`
}

// DiscoveryConfig covers automatic relationship discovery.
//
// # Why these two stay configurable when Phase 8's tokeniser settings did not
//
// A tokeniser setting decides what is written into the postings key space, so a
// corpus written under one and queried under another answers differently for
// older and newer memories — which is why those were retired. A discovery
// threshold decides which edges are created *next*. The edges already on disk
// stay true statements about the corpus, so lowering it adds relationships
// rather than invalidating any.
type DiscoveryConfig struct {
	// Enabled makes a memory write enqueue relationship discovery, in the same
	// transaction. With it off the graph does not fill itself and nothing else
	// changes: a memory is still stored, and connections can still be made by
	// hand.
	Enabled bool `toml:"enabled"`

	// Threshold is the cosine at or above which two memories are linked,
	// compared inclusively.
	//
	// The number is Rust's `auto_discovery_threshold`; the quantity is not.
	// Rust compares 1/(1+d) against it, which over unit vectors is cosine
	// >= 0.786, and Rust's own source records why that is unusable as a
	// threshold (REM-74). See docs/architecture/discovery.md.
	Threshold float32 `toml:"threshold"`

	// TopK bounds how many relationships one memory gains per run.
	TopK int `toml:"top_k"`

	// MaxCandidates bounds the union of everything the sources propose for one
	// subject, so a memory in a densely connected neighbourhood costs no more
	// than one in a sparse one.
	MaxCandidates int `toml:"max_candidates"`
}

// JobsConfig covers the background job framework.
//
// # queue_capacity is gone, and deliberately
//
// It bounded an in-memory channel. A durable queue does not have a capacity —
// its capacity is the disk — and the bounded channel that silently dropped its
// overflow is the specific defect this framework replaces. Keeping the name
// would leave an operator with a dial that cannot do anything, which is the
// failure the three retired tokeniser settings exist to warn about. It is in
// retiredSettings, so a configuration file still naming it is refused by name.
type JobsConfig struct {
	// Workers bounds how many handlers run at once in this process.
	Workers int `toml:"workers"`
	// LeaseDuration is how long a claim lasts before another worker may
	// reclaim the job. It is renewed at a third of this while a handler runs.
	LeaseDuration time.Duration `toml:"lease_duration"`
	// MaxRetries is how many times a failed job is tried again, so a job runs
	// at most MaxRetries+1 times.
	MaxRetries int `toml:"max_retries"`
	// PollInterval is how often a worker looks for due work.
	PollInterval time.Duration `toml:"poll_interval"`
	// Retention is how long a finished job row is kept for audit before the
	// reaper removes it. It is what keeps "what happened to my rebuild"
	// answerable and what keeps the queue's scans bounded.
	Retention time.Duration `toml:"retention"`
	// DrainTimeout is how long a shutdown waits for running handlers to
	// checkpoint and return before their jobs are released back to the queue.
	DrainTimeout time.Duration `toml:"drain_timeout"`
}

// LogConfig covers structured logging. internal/obs owns the logger; this is
// the user-facing description of it.
type LogConfig struct {
	Level     string `toml:"level"`  // debug, info, warn, error
	Format    string `toml:"format"` // json, text
	AddSource bool   `toml:"add_source"`
}

// MetricsConfig covers the Prometheus surface.
type MetricsConfig struct {
	Enabled bool   `toml:"enabled"`
	Addr    string `toml:"addr"` // empty serves on the main HTTP listener
	Path    string `toml:"path"`
}

// The defaults. Every one of them is here, named, so that the behaviour of an
// unconfigured Remem is readable in one place (spec §51).
const (
	DefaultStoragePath      = "./data"
	DefaultStorageEngine    = "pebble"
	DefaultCacheSizeBytes   = 256 << 20 // 256 MiB
	DefaultSyncWrites       = true
	DefaultServerEnv        = "development"
	DefaultHTTPAddr         = "127.0.0.1:4545"
	DefaultReadTimeout      = 30 * time.Second
	DefaultWriteTimeout     = 30 * time.Second
	DefaultShutdownTimeout  = 30 * time.Second
	DefaultMaxRequestBytes  = 8 << 20 // 8 MiB
	DefaultRateLimitRPS     = 100     // Rust's default, per tenant rather than per address
	DefaultRateLimitBurst   = 50
	DefaultTenant           = "default"
	DefaultAutoProvision    = true
	DefaultEmbeddingBatch   = 32
	DefaultMaxSeqTokens     = 256
	DefaultEmbeddingCache   = 4096
	DefaultEmbeddingWorkers = 2
	DefaultEmbeddingFill    = 5 * time.Millisecond
	DefaultVectorIndex      = "flat"
	DefaultVectorMetric     = "cosine"
	DefaultHNSWM            = 16
	DefaultHNSWEfConstruct  = 200
	DefaultHNSWEfSearch     = 64
	DefaultResidentBudget   = 512
	DefaultTextEnabled      = true
	DefaultSearchLimit      = 10
	DefaultSearchMaxLimit   = 200
	DefaultMaxPageDepth     = 2000
	DefaultRankedTTL        = time.Minute
	DefaultSimilarity       = 0.0
	DefaultLifecycleEnabled = true
	DefaultSweepInterval    = time.Hour
	DefaultRunBudget        = 10_000
	DefaultRecallWindow     = 30 * time.Second
	DefaultEventRetention   = 90 * 24 * time.Hour
	DefaultDiscoveryEnabled = true
	// The three discovery numbers are repeated here rather than imported from
	// internal/discovery, because internal/config is imported by everything and
	// discovery pulls in the graph, the record repository and the job
	// framework. TestConfigDefaultsMatchTheDiscoveryPackage in internal/server
	// is what stops the two copies drifting.
	DefaultDiscoveryThreshold     = 0.7
	DefaultDiscoveryTopK          = 5
	DefaultDiscoveryMaxCandidates = 64

	DefaultJobWorkers      = 4
	DefaultJobLease        = time.Minute
	DefaultJobMaxRetries   = 5
	DefaultJobPollInterval = time.Second
	DefaultJobRetention    = 24 * time.Hour
	DefaultJobDrainTimeout = 15 * time.Second
	DefaultLogLevel        = "info"
	DefaultLogFormat       = "json"
	DefaultMetricsEnabled  = true
	DefaultMetricsPath     = "/metrics"
)

// Fixed constants of the system. These are defaults in the sense that they can
// be written in a file, and constants in the sense that validation refuses the
// combinations that would make stored data incomparable.
const (
	// EmbeddingModel is the only model whose vectors are comparable with the
	// ones already stored.
	EmbeddingModel = "all-MiniLM-L6-v2"
	// EmbeddingDimension is that model's output width: 384, mean-pooled and
	// L2-normalised.
	EmbeddingDimension = 384
	// RRFK is reciprocal-rank fusion's k.
	RRFK = 60
	// WidenMaxFactor bounds candidate-set widening.
	WidenMaxFactor = 32
	// ListMaxFactor bounds a listing's widening.
	ListMaxFactor = 128
)

// Default returns the configuration of an unconfigured Remem: a development
// server on localhost, storing under ./data, with no API key.
//
// It validates. A default that does not pass its own validation is a trap for
// the first person who runs the binary without a config file.
func Default() Config {
	return Config{
		Storage: StorageConfig{
			Path:           DefaultStoragePath,
			Engine:         DefaultStorageEngine,
			CacheSizeBytes: DefaultCacheSizeBytes,
			SyncWrites:     DefaultSyncWrites,
		},
		Server: ServerConfig{
			Env:             DefaultServerEnv,
			HTTPAddr:        DefaultHTTPAddr,
			ReadTimeout:     DefaultReadTimeout,
			WriteTimeout:    DefaultWriteTimeout,
			ShutdownTimeout: DefaultShutdownTimeout,
			MaxRequestBytes: DefaultMaxRequestBytes,
			RateLimitRPS:    DefaultRateLimitRPS,
			RateLimitBurst:  DefaultRateLimitBurst,
		},
		Tenant: TenantConfig{
			Default:       DefaultTenant,
			AutoProvision: DefaultAutoProvision,
		},
		Embedding: EmbeddingConfig{
			Model:             EmbeddingModel,
			BatchSize:         DefaultEmbeddingBatch,
			MaxSequenceTokens: DefaultMaxSeqTokens,
			CacheSize:         DefaultEmbeddingCache,
			Workers:           DefaultEmbeddingWorkers,
			FillWindow:        DefaultEmbeddingFill,
		},
		Vector: VectorConfig{
			Dimension:          EmbeddingDimension,
			Index:              DefaultVectorIndex,
			Metric:             DefaultVectorMetric,
			HNSWM:              DefaultHNSWM,
			HNSWEfConstruction: DefaultHNSWEfConstruct,
			HNSWEfSearch:       DefaultHNSWEfSearch,
			ResidentBudgetMB:   DefaultResidentBudget,
		},
		Text: TextConfig{Enabled: DefaultTextEnabled},
		Search: SearchConfig{
			DefaultLimit:        DefaultSearchLimit,
			MaxLimit:            DefaultSearchMaxLimit,
			MaxPageDepth:        DefaultMaxPageDepth,
			RRFK:                RRFK,
			WidenMaxFactor:      WidenMaxFactor,
			ListMaxFactor:       ListMaxFactor,
			SimilarityThreshold: DefaultSimilarity,
		},
		Paging: PagingConfig{RankedTTL: DefaultRankedTTL},
		Lifecycle: LifecycleConfig{
			Enabled:        DefaultLifecycleEnabled,
			SweepInterval:  DefaultSweepInterval,
			RunBudget:      DefaultRunBudget,
			RecallWindow:   DefaultRecallWindow,
			EventRetention: DefaultEventRetention,
		},
		Discovery: DiscoveryConfig{
			Enabled:       DefaultDiscoveryEnabled,
			Threshold:     DefaultDiscoveryThreshold,
			TopK:          DefaultDiscoveryTopK,
			MaxCandidates: DefaultDiscoveryMaxCandidates,
		},
		Jobs: JobsConfig{
			Workers:       DefaultJobWorkers,
			LeaseDuration: DefaultJobLease,
			MaxRetries:    DefaultJobMaxRetries,
			PollInterval:  DefaultJobPollInterval,
			Retention:     DefaultJobRetention,
			DrainTimeout:  DefaultJobDrainTimeout,
		},
		Log: LogConfig{
			Level:  DefaultLogLevel,
			Format: DefaultLogFormat,
		},
		Metrics: MetricsConfig{
			Enabled: DefaultMetricsEnabled,
			Path:    DefaultMetricsPath,
		},
	}
}

// LogConfig converts the user-facing logging settings into the form
// internal/obs constructs a logger from. It is called after [Config.Validate],
// so the level and format are known good.
func (c Config) LogConfig() obs.LogConfig {
	return obs.LogConfig{
		Level:     logLevels[c.Log.Level],
		Format:    obs.Format(c.Log.Format),
		AddSource: c.Log.AddSource,
	}
}

var logLevels = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

// Redacted renders the whole configuration as sorted key = value lines, with
// secrets replaced by a placeholder.
//
// This is the "inspectable" half of spec §51: a server logs it at startup so
// that "what was this process actually running with" is answerable from the
// logs, without the API key being one of the answers.
func (c Config) Redacted() string {
	var b strings.Builder
	fields := walk(reflect.ValueOf(&c).Elem())
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f := fields[name]
		value := formatValue(f.value)
		if f.secret {
			if value == "" {
				value = "(unset)"
			} else {
				value = "(set, redacted)"
			}
		}
		fmt.Fprintf(&b, "%s = %s\n", name, value)
	}
	return b.String()
}

func formatValue(v reflect.Value) string {
	if d, ok := v.Interface().(time.Duration); ok {
		return d.String()
	}
	if v.Kind() == reflect.String {
		return v.String()
	}
	// An empty list renders as "" rather than "[]", so that a secret list
	// nobody configured is reported as unset instead of "(set, redacted)" —
	// which would tell an operator they have credentials they do not have.
	if v.Kind() == reflect.Slice && v.Len() == 0 {
		return ""
	}
	return fmt.Sprintf("%v", v.Interface())
}
