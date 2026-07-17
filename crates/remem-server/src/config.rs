use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;

/// remem-server configuration loaded from TOML file + CLI/env overrides.
#[derive(Debug, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub vector: VectorConfig,
    pub embedding: EmbeddingConfig,
    pub connections: ConnectionConfig,
    pub tasks: TaskConfig,
}

/// Deployment environment, controlling whether dev-only conveniences
/// (auth disabled, permissive CORS) are permitted. Defaults to
/// `Development` so the out-of-box quickstart keeps working; production
/// deployments must set `REMEM_ENV=production` explicitly, which then
/// enforces the guards checked by `validate_production_config`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    Development,
    Production,
}

impl Environment {
    fn from_env() -> Self {
        match std::env::var("REMEM_ENV") {
            Ok(v) if v.eq_ignore_ascii_case("production") || v.eq_ignore_ascii_case("prod") => {
                Environment::Production
            }
            _ => Environment::Development,
        }
    }

    pub fn is_production(&self) -> bool {
        matches!(self, Environment::Production)
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Empty string means auth disabled (development mode).
    pub api_key: String,
    /// Optional secondary key accepted alongside `api_key`, so a key can be
    /// rotated by adding the new key here first, updating callers, then
    /// promoting it to `api_key` — no downtime window with zero valid keys.
    pub api_key_secondary: String,
    /// When true, an empty api_key is allowed (unauthenticated access).
    /// Must be explicitly opted-in via REMEM_ALLOW_AUTH_DISABLED=true.
    /// Never set this in production — enforced by `validate_production_config`
    /// when `env` is `Production`.
    pub allow_auth_disabled: bool,
    /// Comma-separated allowed CORS origins. Empty = permissive (dev mode).
    pub allowed_origins: Vec<String>,
    /// Global request rate limit in requests-per-second (0 = disabled).
    pub rate_limit_rps: u32,
    /// Burst allowance on top of the per-second rate.
    pub rate_limit_burst: u32,
    /// Deployment environment (`REMEM_ENV`); gates dev-only conveniences.
    pub env: Environment,
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub sync_writes: bool,
    pub checkpoint_interval_secs: u64,
    pub max_wal_size_mb: u64,
}

#[derive(Debug, Clone)]
pub struct VectorConfig {
    pub dimension: usize,
    pub hnsw_m: usize,
    pub hnsw_ef_construction: usize,
    pub hnsw_ef_search: usize,
}

#[derive(Debug, Clone)]
pub struct EmbeddingConfig {
    /// Number of embeddings to keep in the in-memory LRU cache.
    pub cache_size: usize,
}

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Cosine similarity threshold for auto-discovery.
    pub auto_discovery_threshold: f32,
    /// Maximum connections created per auto-discovery run.
    pub auto_discovery_top_k: usize,
}

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub expire_short_term_secs: u64,
    pub apply_importance_decay_secs: u64,
    pub active_forgetting_secs: u64,
    pub consolidate_similar_secs: u64,
    pub cleanup_archived_secs: u64,
    pub discover_connections_secs: u64,
    pub discovery_workers: usize,
    pub discovery_queue_size: usize,
}

// ─── TOML file schema ───────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct FileConfig {
    #[serde(default)]
    server: FileServerConfig,
    #[serde(default)]
    storage: FileStorageConfig,
    #[serde(default)]
    vector: FileVectorConfig,
    #[serde(default)]
    embedding: FileEmbeddingConfig,
    #[serde(default)]
    connections: FileConnectionConfig,
    #[serde(default)]
    tasks: FileTaskConfig,
}

#[derive(Deserialize)]
struct FileServerConfig {
    host: String,
    port: u16,
    api_key: String,
}

impl Default for FileServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".into(),
            port: 4545,
            api_key: String::new(),
        }
    }
}

#[derive(Deserialize)]
struct FileStorageConfig {
    data_dir: String,
    sync_writes: bool,
    checkpoint_interval_secs: u64,
    max_wal_size_mb: u64,
}

impl Default for FileStorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "/var/lib/remem".into(),
            sync_writes: true,
            checkpoint_interval_secs: 300,
            max_wal_size_mb: 256,
        }
    }
}

#[derive(Deserialize)]
struct FileVectorConfig {
    dimension: usize,
    hnsw_m: usize,
    hnsw_ef_construction: usize,
    hnsw_ef_search: usize,
}

impl Default for FileVectorConfig {
    fn default() -> Self {
        Self {
            dimension: 384,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 50,
        }
    }
}

#[derive(Deserialize)]
struct FileEmbeddingConfig {
    cache_size: usize,
}

impl Default for FileEmbeddingConfig {
    fn default() -> Self {
        Self { cache_size: 10_000 }
    }
}

#[derive(Deserialize)]
struct FileConnectionConfig {
    auto_discovery_threshold: f32,
    auto_discovery_top_k: usize,
}

impl Default for FileConnectionConfig {
    fn default() -> Self {
        Self {
            auto_discovery_threshold: 0.7,
            auto_discovery_top_k: 5,
        }
    }
}

#[derive(Deserialize)]
struct FileTaskConfig {
    expire_short_term_secs: u64,
    apply_importance_decay_secs: u64,
    active_forgetting_secs: u64,
    consolidate_similar_secs: u64,
    cleanup_archived_secs: u64,
    discover_connections_secs: u64,
    discovery_workers: usize,
    discovery_queue_size: usize,
}

impl Default for FileTaskConfig {
    fn default() -> Self {
        Self {
            expire_short_term_secs: 5 * 60,
            apply_importance_decay_secs: 24 * 3600,
            active_forgetting_secs: 24 * 3600,
            consolidate_similar_secs: 7 * 24 * 3600,
            cleanup_archived_secs: 30 * 24 * 3600,
            discover_connections_secs: 60 * 60,
            discovery_workers: 2,
            discovery_queue_size: 10_000,
        }
    }
}

// ─── CLI args ────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "remem-server", about = "Remem backend server")]
pub struct Args {
    /// Path to TOML config file.
    #[arg(long, env = "REMEM_CONFIG")]
    pub config: Option<PathBuf>,

    /// Override storage data directory.
    #[arg(long, env = "REMEM_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// Override server port.
    #[arg(long, env = "REMEM_PORT")]
    pub port: Option<u16>,

    /// Override API key (empty = disabled).
    #[arg(long, env = "REMEM_API_KEY")]
    pub api_key: Option<String>,

    /// Optional secondary API key, accepted alongside the primary key during
    /// rotation. Set the new key here, roll out callers, then promote it to
    /// --api-key and drop this.
    #[arg(long, env = "REMEM_API_KEY_SECONDARY")]
    pub api_key_secondary: Option<String>,
}

// ─── Loader ─────────────────────────────────────────────────────────────────

#[cfg(test)]
pub fn args_default() -> Args {
    Args { config: None, data_dir: None, port: None, api_key: None, api_key_secondary: None }
}

pub fn load(args: &Args) -> anyhow::Result<Config> {
    // Start with defaults.
    let mut file = FileConfig::default();

    // Overlay with TOML file if provided.
    if let Some(path) = &args.config {
        let text = std::fs::read_to_string(path)?;
        file = toml::from_str(&text)?;
    }

    // CLI/env overrides.
    if let Some(d) = &args.data_dir {
        file.storage.data_dir = d.to_string_lossy().into();
    }
    if let Some(p) = args.port {
        file.server.port = p;
    }
    if let Some(k) = &args.api_key {
        file.server.api_key = k.clone();
    }
    let api_key_secondary = args
        .api_key_secondary
        .clone()
        .or_else(|| std::env::var("REMEM_API_KEY_SECONDARY").ok())
        .unwrap_or_default();

    // REMEM_ALLOW_AUTH_DISABLED is intentionally env-only (not TOML) so it
    // cannot be accidentally committed into a config file.
    let allow_auth_disabled = std::env::var("REMEM_ALLOW_AUTH_DISABLED")
        .map(|v| matches!(v.to_lowercase().as_str(), "true" | "1"))
        .unwrap_or(false);

    let allowed_origins = std::env::var("REMEM_CORS_ORIGINS")
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    // Rate limiting defaults ON (100 rps, burst 50) so a fresh deployment
    // isn't wide open to cheap CPU exhaustion via the embedding endpoints.
    // Set REMEM_RATE_LIMIT_RPS=0 to explicitly disable.
    let rate_limit_rps = std::env::var("REMEM_RATE_LIMIT_RPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(100);

    let rate_limit_burst = std::env::var("REMEM_RATE_LIMIT_BURST")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(50);

    let env = Environment::from_env();

    Ok(Config {
        server: ServerConfig {
            host: file.server.host,
            port: file.server.port,
            api_key: file.server.api_key,
            api_key_secondary,
            allow_auth_disabled,
            allowed_origins,
            rate_limit_rps,
            rate_limit_burst,
            env,
        },
        storage: StorageConfig {
            data_dir: PathBuf::from(file.storage.data_dir),
            sync_writes: file.storage.sync_writes,
            checkpoint_interval_secs: file.storage.checkpoint_interval_secs,
            max_wal_size_mb: file.storage.max_wal_size_mb,
        },
        vector: VectorConfig {
            dimension: file.vector.dimension,
            hnsw_m: file.vector.hnsw_m,
            hnsw_ef_construction: file.vector.hnsw_ef_construction,
            hnsw_ef_search: file.vector.hnsw_ef_search,
        },
        embedding: EmbeddingConfig {
            cache_size: file.embedding.cache_size,
        },
        connections: ConnectionConfig {
            auto_discovery_threshold: file.connections.auto_discovery_threshold,
            auto_discovery_top_k: file.connections.auto_discovery_top_k,
        },
        tasks: TaskConfig {
            expire_short_term_secs: file.tasks.expire_short_term_secs,
            apply_importance_decay_secs: file.tasks.apply_importance_decay_secs,
            active_forgetting_secs: file.tasks.active_forgetting_secs,
            consolidate_similar_secs: file.tasks.consolidate_similar_secs,
            cleanup_archived_secs: file.tasks.cleanup_archived_secs,
            discover_connections_secs: file.tasks.discover_connections_secs,
            discovery_workers: file.tasks.discovery_workers,
            discovery_queue_size: file.tasks.discovery_queue_size,
        },
    })
}

/// Known placeholder secrets that must never survive into a production
/// deployment. Anyone can read these out of `.env.example` or the compose
/// files, so treat them as public.
const PLACEHOLDER_API_KEYS: &[&str] = &["change-this-secret-key-in-production", "dev", "test"];

/// Validate `cfg` against the production security bar. Returns a list of
/// human-readable violations; empty means the config is safe to boot with
/// `REMEM_ENV=production`. Called from `main` — any violation refuses startup
/// rather than degrading silently, since these are exactly the defaults a
/// copied `.env.example` ships with.
pub fn validate_production_config(cfg: &Config) -> Vec<String> {
    let mut violations = Vec::new();

    if cfg.server.allow_auth_disabled {
        violations.push(
            "REMEM_ALLOW_AUTH_DISABLED=true is not permitted with REMEM_ENV=production".into(),
        );
    }
    if cfg.server.api_key.is_empty() {
        violations.push("REMEM_API_KEY must be set with REMEM_ENV=production".into());
    } else if cfg.server.api_key.len() < 16
        || PLACEHOLDER_API_KEYS.contains(&cfg.server.api_key.as_str())
    {
        violations.push(
            "REMEM_API_KEY looks like a placeholder or is too short (need >= 16 chars) for \
             REMEM_ENV=production"
                .into(),
        );
    }
    if cfg.server.allowed_origins.is_empty() {
        violations.push(
            "REMEM_CORS_ORIGINS must be set (permissive CORS is not permitted) with \
             REMEM_ENV=production"
                .into(),
        );
    }

    violations
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use tempfile::NamedTempFile;

    fn default_args() -> Args {
        Args { config: None, data_dir: None, port: None, api_key: None, api_key_secondary: None }
    }

    // ── Defaults ──────────────────────────────────────────────────────────────

    #[test]
    fn default_server_config() {
        let cfg = load(&default_args()).unwrap();
        assert_eq!(cfg.server.host, "0.0.0.0");
        assert_eq!(cfg.server.port, 4545);
        assert_eq!(cfg.server.api_key, "");
    }

    #[test]
    fn default_storage_config() {
        let cfg = load(&default_args()).unwrap();
        assert_eq!(cfg.storage.data_dir, PathBuf::from("/var/lib/remem"));
        assert!(cfg.storage.sync_writes);
        assert_eq!(cfg.storage.checkpoint_interval_secs, 300);
        assert_eq!(cfg.storage.max_wal_size_mb, 256);
    }

    #[test]
    fn default_vector_config() {
        let cfg = load(&default_args()).unwrap();
        assert_eq!(cfg.vector.dimension, 384);
        assert_eq!(cfg.vector.hnsw_m, 16);
        assert_eq!(cfg.vector.hnsw_ef_construction, 200);
        assert_eq!(cfg.vector.hnsw_ef_search, 50);
    }

    #[test]
    fn default_embedding_config() {
        let cfg = load(&default_args()).unwrap();
        assert_eq!(cfg.embedding.cache_size, 10_000);
    }

    #[test]
    fn default_connections_config() {
        let cfg = load(&default_args()).unwrap();
        assert!((cfg.connections.auto_discovery_threshold - 0.7).abs() < f32::EPSILON);
        assert_eq!(cfg.connections.auto_discovery_top_k, 5);
    }

    #[test]
    fn default_task_config() {
        let cfg = load(&default_args()).unwrap();
        assert_eq!(cfg.tasks.expire_short_term_secs, 5 * 60);
        assert_eq!(cfg.tasks.apply_importance_decay_secs, 24 * 3600);
        assert_eq!(cfg.tasks.discover_connections_secs, 60 * 60);
        assert_eq!(cfg.tasks.discovery_workers, 2);
        assert_eq!(cfg.tasks.discovery_queue_size, 10_000);
        assert_eq!(cfg.tasks.active_forgetting_secs, 24 * 3600);
        assert_eq!(cfg.tasks.consolidate_similar_secs, 7 * 24 * 3600);
        assert_eq!(cfg.tasks.cleanup_archived_secs, 30 * 24 * 3600);
    }

    #[test]
    #[allow(deprecated)]
    fn allow_auth_disabled_defaults_to_false_when_env_unset() {
        // Remove the variable if it happens to be set in the test environment
        std::env::remove_var("REMEM_ALLOW_AUTH_DISABLED");
        let cfg = load(&default_args()).unwrap();
        assert!(!cfg.server.allow_auth_disabled);
    }

    #[test]
    #[allow(deprecated)]
    fn allow_auth_disabled_true_when_env_var_set() {
        std::env::set_var("REMEM_ALLOW_AUTH_DISABLED", "true");
        let cfg = load(&default_args()).unwrap();
        let result = cfg.server.allow_auth_disabled;
        // Clean up before asserting so a test failure doesn't poison other tests
        std::env::remove_var("REMEM_ALLOW_AUTH_DISABLED");
        assert!(result);
    }

    // ── CLI overrides ─────────────────────────────────────────────────────────

    #[test]
    fn cli_port_override() {
        let args = Args { port: Some(9999), ..default_args() };
        let cfg = load(&args).unwrap();
        assert_eq!(cfg.server.port, 9999);
    }

    #[test]
    fn cli_api_key_override() {
        let args = Args { api_key: Some("secret".into()), ..default_args() };
        let cfg = load(&args).unwrap();
        assert_eq!(cfg.server.api_key, "secret");
    }

    #[test]
    fn cli_data_dir_override() {
        let args = Args {
            data_dir: Some(PathBuf::from("/tmp/remem-test")),
            ..default_args()
        };
        let cfg = load(&args).unwrap();
        assert_eq!(cfg.storage.data_dir, PathBuf::from("/tmp/remem-test"));
    }

    // ── TOML loading ──────────────────────────────────────────────────────────

    fn write_toml(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    #[test]
    fn toml_file_overrides_defaults() {
        let toml = r#"
[server]
host = "127.0.0.1"
port = 7777
api_key = "toml-key"

[storage]
data_dir = "/data/remem"
sync_writes = false
checkpoint_interval_secs = 600
max_wal_size_mb = 512

[vector]
dimension = 768
hnsw_m = 32
hnsw_ef_construction = 400
hnsw_ef_search = 100

[embedding]
cache_size = 5000

[connections]
auto_discovery_threshold = 0.8
auto_discovery_top_k = 10
"#;
        let f = write_toml(toml);
        let args = Args { config: Some(f.path().to_path_buf()), ..default_args() };
        let cfg = load(&args).unwrap();

        assert_eq!(cfg.server.host, "127.0.0.1");
        assert_eq!(cfg.server.port, 7777);
        assert_eq!(cfg.server.api_key, "toml-key");
        assert_eq!(cfg.storage.data_dir, PathBuf::from("/data/remem"));
        assert!(!cfg.storage.sync_writes);
        assert_eq!(cfg.storage.checkpoint_interval_secs, 600);
        assert_eq!(cfg.storage.max_wal_size_mb, 512);
        assert_eq!(cfg.vector.dimension, 768);
        assert_eq!(cfg.vector.hnsw_m, 32);
        assert_eq!(cfg.vector.hnsw_ef_construction, 400);
        assert_eq!(cfg.vector.hnsw_ef_search, 100);
        assert_eq!(cfg.embedding.cache_size, 5000);
        assert!((cfg.connections.auto_discovery_threshold - 0.8).abs() < f32::EPSILON);
        assert_eq!(cfg.connections.auto_discovery_top_k, 10);
    }

    #[test]
    fn cli_overrides_toml() {
        let toml = r#"
[server]
host = "0.0.0.0"
port = 7777
api_key = "toml-key"

[storage]
data_dir = "/data/remem"
sync_writes = true
checkpoint_interval_secs = 300
max_wal_size_mb = 256

[vector]
dimension = 384
hnsw_m = 16
hnsw_ef_construction = 200
hnsw_ef_search = 50

[embedding]
cache_size = 10000

[connections]
auto_discovery_threshold = 0.7
auto_discovery_top_k = 5
"#;
        let f = write_toml(toml);
        let args = Args {
            config: Some(f.path().to_path_buf()),
            port: Some(8888),
            api_key: Some("cli-key".into()),
            data_dir: None,
            api_key_secondary: None,
        };
        let cfg = load(&args).unwrap();

        // CLI wins
        assert_eq!(cfg.server.port, 8888);
        assert_eq!(cfg.server.api_key, "cli-key");
        // TOML value for non-overridden fields
        assert_eq!(cfg.storage.data_dir, PathBuf::from("/data/remem"));
    }

    #[test]
    fn partial_toml_uses_defaults_for_missing_sections() {
        // Only override server section; other sections should use defaults
        let toml = r#"
[server]
host = "127.0.0.1"
port = 9000
api_key = ""
"#;
        let f = write_toml(toml);
        let args = Args { config: Some(f.path().to_path_buf()), ..default_args() };
        let cfg = load(&args).unwrap();

        assert_eq!(cfg.server.port, 9000);
        // Storage should fall back to defaults
        assert_eq!(cfg.storage.data_dir, PathBuf::from("/var/lib/remem"));
        assert_eq!(cfg.vector.dimension, 384);
    }

    // ── Error cases ───────────────────────────────────────────────────────────

    #[test]
    fn invalid_toml_returns_error() {
        let f = write_toml("not valid toml !!!");
        let args = Args { config: Some(f.path().to_path_buf()), ..default_args() };
        assert!(load(&args).is_err());
    }

    #[test]
    fn nonexistent_config_file_returns_error() {
        let args = Args {
            config: Some(PathBuf::from("/nonexistent/path/config.toml")),
            ..default_args()
        };
        assert!(load(&args).is_err());
    }

    #[test]
    #[allow(deprecated)]
    fn cors_origins_empty_when_env_unset() {
        std::env::remove_var("REMEM_CORS_ORIGINS");
        let cfg = load(&default_args()).unwrap();
        assert!(cfg.server.allowed_origins.is_empty());
    }

    #[test]
    #[allow(deprecated)]
    fn cors_origins_parsed_from_env() {
        std::env::set_var("REMEM_CORS_ORIGINS", "http://localhost:3000, https://app.example.com");
        let cfg = load(&default_args()).unwrap();
        std::env::remove_var("REMEM_CORS_ORIGINS");
        assert_eq!(cfg.server.allowed_origins, vec![
            "http://localhost:3000",
            "https://app.example.com",
        ]);
    }

    #[test]
    #[allow(deprecated)]
    fn default_rate_limit_enabled() {
        std::env::remove_var("REMEM_RATE_LIMIT_RPS");
        std::env::remove_var("REMEM_RATE_LIMIT_BURST");
        let cfg = load(&default_args()).unwrap();
        assert_eq!(cfg.server.rate_limit_rps, 100);
        assert_eq!(cfg.server.rate_limit_burst, 50);
    }

    #[test]
    #[allow(deprecated)]
    fn rate_limit_can_be_disabled_explicitly() {
        std::env::set_var("REMEM_RATE_LIMIT_RPS", "0");
        let cfg = load(&default_args()).unwrap();
        std::env::remove_var("REMEM_RATE_LIMIT_RPS");
        assert_eq!(cfg.server.rate_limit_rps, 0);
    }

    #[test]
    #[allow(deprecated)]
    fn rate_limit_from_env() {
        std::env::set_var("REMEM_RATE_LIMIT_RPS", "200");
        std::env::set_var("REMEM_RATE_LIMIT_BURST", "100");
        let cfg = load(&default_args()).unwrap();
        std::env::remove_var("REMEM_RATE_LIMIT_RPS");
        std::env::remove_var("REMEM_RATE_LIMIT_BURST");
        assert_eq!(cfg.server.rate_limit_rps, 200);
        assert_eq!(cfg.server.rate_limit_burst, 100);
    }

    // ── Environment gate / production validation ────────────────────────────

    #[test]
    #[allow(deprecated)]
    fn env_defaults_to_development() {
        std::env::remove_var("REMEM_ENV");
        let cfg = load(&default_args()).unwrap();
        assert!(!cfg.server.env.is_production());
    }

    #[test]
    #[allow(deprecated)]
    fn env_production_recognised() {
        std::env::set_var("REMEM_ENV", "production");
        let cfg = load(&default_args()).unwrap();
        std::env::remove_var("REMEM_ENV");
        assert!(cfg.server.env.is_production());
    }

    #[test]
    #[allow(deprecated)]
    fn validate_production_config_flags_all_dev_defaults() {
        std::env::remove_var("REMEM_CORS_ORIGINS");
        std::env::remove_var("REMEM_ALLOW_AUTH_DISABLED");
        let cfg = load(&default_args()).unwrap();
        let violations = validate_production_config(&cfg);
        // Empty api_key, empty origins, both flagged.
        assert!(violations.iter().any(|v| v.contains("REMEM_API_KEY")));
        assert!(violations.iter().any(|v| v.contains("REMEM_CORS_ORIGINS")));
    }

    #[test]
    #[allow(deprecated)]
    fn validate_production_config_flags_placeholder_key() {
        let args = Args {
            api_key: Some("change-this-secret-key-in-production".into()),
            ..default_args()
        };
        std::env::remove_var("REMEM_CORS_ORIGINS");
        let cfg = load(&args).unwrap();
        let violations = validate_production_config(&cfg);
        assert!(violations.iter().any(|v| v.contains("REMEM_API_KEY")));
    }

    #[test]
    #[allow(deprecated)]
    fn validate_production_config_flags_auth_disabled() {
        std::env::set_var("REMEM_ALLOW_AUTH_DISABLED", "true");
        let cfg = load(&default_args()).unwrap();
        std::env::remove_var("REMEM_ALLOW_AUTH_DISABLED");
        let violations = validate_production_config(&cfg);
        assert!(violations.iter().any(|v| v.contains("REMEM_ALLOW_AUTH_DISABLED")));
    }

    #[test]
    #[allow(deprecated)]
    fn validate_production_config_passes_with_proper_secrets() {
        std::env::set_var("REMEM_CORS_ORIGINS", "https://app.example.com");
        let args = Args {
            api_key: Some("a-sufficiently-long-random-production-key".into()),
            ..default_args()
        };
        let cfg = load(&args).unwrap();
        std::env::remove_var("REMEM_CORS_ORIGINS");
        assert!(validate_production_config(&cfg).is_empty());
    }
}
