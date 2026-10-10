package config

import (
	"time"
)

// The defaults, and the fixed constants that are written as defaults but
// validated as constants. They are kept together and apart from the types in
// config.go so that the behaviour of an unconfigured Remem reads in one place
// (spec §51).

// The defaults. Every one of them is here, named, so that the behaviour of an
// unconfigured Remem is readable in one place (spec §51).
const (
	// DefaultGCPercent is GOGC. Go's own default is 100; 200 halves how often
	// the collector runs and costs about half as much resident memory again.
	// The performance review measured the collector and page release at over
	// 40% of CPU in the search benchmark under 100.
	DefaultGCPercent = 200
	// DefaultMemoryLimitBytes is GOMEMLIMIT: no limit, because a soft limit a
	// server did not ask for turns a memory spike into a collector that will
	// not stop running. An operator who bounds the process bounds this too.
	DefaultMemoryLimitBytes = 0

	DefaultStoragePath    = "./data"
	DefaultStorageEngine  = "pebble"
	DefaultCacheSizeBytes = 256 << 20 // 256 MiB
	// DefaultMemTableSizeBytes is sixteen times Pebble's own 4 MiB, which is
	// sized for a short-lived test instance. Up to two buffers of this size
	// may be queued, so it is up to 128 MiB of write buffer.
	DefaultMemTableSizeBytes = 64 << 20 // 64 MiB
	// DefaultBloomBitsPerKey is the usual bloom size: about a 1%
	// false-positive rate for about 1.25 bytes of filter per key. Pebble
	// writes no filter unless asked.
	DefaultBloomBitsPerKey  = 10
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

	// Zero derives the count from the CPUs this process may use. It is
	// separate from Workers: the model runs one inference at a time.
	DefaultEmbeddingIntraOpThreads = 0

	DefaultEmbeddingFill   = 5 * time.Millisecond
	DefaultVectorIndex     = "flat"
	DefaultVectorMetric    = "cosine"
	DefaultHNSWM           = 16
	DefaultHNSWEfConstruct = 200
	DefaultHNSWEfSearch    = 64
	DefaultResidentBudget  = 512
	DefaultAsyncIndexing   = false
	// DefaultIndexQueueSize is indexqueue.DefaultSize. It is repeated here
	// rather than imported for the reason the discovery numbers are:
	// internal/config is imported by everything, and internal/vector pulls in
	// the storage layer. TestTheQueueDefaultsMatchTheIndexQueuePackage in
	// internal/server is what stops the two copies drifting.
	DefaultIndexQueueSize = 1024
	DefaultTextEnabled    = true
	DefaultSearchLimit    = 10
	DefaultSearchMaxLimit = 200
	DefaultMaxPageDepth   = 2000
	DefaultRankedTTL      = time.Minute
	DefaultSimilarity     = 0.0
	// DefaultRowCacheEntries is sixteen thousand decoded attribute rows, about
	// 11 MB at the twelve slots the memory table declares and the measured 689
	// bytes an entry costs. It is sized for the working set a busy tenant's
	// queries touch rather than for a corpus: a miss costs exactly the read the
	// cache was there to save.
	DefaultRowCacheEntries  = 16384
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

	// ProviderONNX embeds text in this process with EmbeddingModel.
	ProviderONNX = "onnx"
	// ProviderExternal takes every vector from the caller and embeds nothing.
	ProviderExternal = "external"
	// MaxExternalDimension bounds a caller-declared width. 4096 covers every
	// published text embedding model this was written against, and a bound
	// keeps one request's vector inside max_request_bytes with room to spare.
	MaxExternalDimension = 4096
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
		Runtime: RuntimeConfig{
			GCPercent:        DefaultGCPercent,
			MemoryLimitBytes: DefaultMemoryLimitBytes,
		},
		Storage: StorageConfig{
			Path:           DefaultStoragePath,
			Engine:         DefaultStorageEngine,
			CacheSizeBytes: DefaultCacheSizeBytes,
			SyncWrites:     DefaultSyncWrites,
			// Pebble's own defaults here are a 4 MiB write buffer and no
			// filter at all, both of which suit a test instance rather than a
			// server.
			MemTableSizeBytes: DefaultMemTableSizeBytes,
			BloomBitsPerKey:   DefaultBloomBitsPerKey,
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
			Provider:          ProviderONNX,
			Model:             EmbeddingModel,
			BatchSize:         DefaultEmbeddingBatch,
			MaxSequenceTokens: DefaultMaxSeqTokens,
			CacheSize:         DefaultEmbeddingCache,
			Workers:           DefaultEmbeddingWorkers,
			IntraOpThreads:    DefaultEmbeddingIntraOpThreads,
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
			AsyncIndexing:      DefaultAsyncIndexing,
			IndexQueueSize:     DefaultIndexQueueSize,
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
			RowCacheEntries:     DefaultRowCacheEntries,
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
