//! Shared server setup for the write-path Criterion benchmark and its focused tests.

use std::{path::PathBuf, sync::Arc};

use crate::{
    api::{build_router, AppState},
    config::{
        Config, ConnectionConfig, EmbeddingConfig, Environment, SearchConfig, ServerConfig,
        StorageConfig as CfgStorage, TaskConfig, VectorConfig as CfgVector,
    },
    engine::{
        storage::{
            engine::{
                EngineConfig, GraphIndexConfig, TagIndexConfig, TimeSeriesConfig,
                VectorConfig as EngineVector,
            },
            partition::PartitionId,
        },
        util::DistanceMetric,
        StorageEngine,
    },
    services::{self, create_services},
};

pub struct WriteBenchmarkApp {
    router: axum::Router,
    sync_writes: bool,
}

impl WriteBenchmarkApp {
    pub fn router(&self) -> axum::Router {
        self.router.clone()
    }

    pub fn sync_writes(&self) -> bool {
        self.sync_writes
    }
}

/// Builds the same in-process HTTP stack used by the server, configured for durable
/// writes and with background discovery left unstarted so it is not timed.
pub async fn build_write_benchmark_app(data_dir: PathBuf) -> anyhow::Result<WriteBenchmarkApp> {
    let default_partition = PartitionId::default_legacy();
    let sync_writes = true;
    let engine_cfg = EngineConfig {
        data_dir: data_dir.clone(),
        sync_writes,
        default_partition: default_partition.clone(),
        vector: EngineVector {
            enabled: true,
            dimension: 384,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 50,
            metric: DistanceMetric::L2,
            hnsw_resident_budget_bytes: None,
        },
        graph: GraphIndexConfig {
            enabled: true,
            directed: true,
        },
        time_series: TimeSeriesConfig { enabled: true },
        tag_index: TagIndexConfig {
            enabled: true,
            lowercase: true,
            min_token_length: 1,
        },
        attr_schema: Some(services::attrs::memory_schema()),
        attr_project: Some(Arc::new(|bytes: &[u8]| {
            serde_json::from_slice::<services::types::StoredMemory>(bytes)
                .ok()
                .map(|stored| services::attrs::project(&stored))
        })),
        ..EngineConfig::default()
    };
    let engine = Arc::new(StorageEngine::new(engine_cfg).await?);

    let cfg = Config {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port: 4545,
            api_key: String::new(),
            api_key_secondary: String::new(),
            allow_auth_disabled: true,
            allowed_origins: vec![],
            rate_limit_rps: 0,
            rate_limit_burst: 50,
            trust_proxy_headers: false,
            env: Environment::Development,
        },
        storage: CfgStorage {
            data_dir,
            sync_writes,
            checkpoint_interval_secs: 300,
            max_wal_size_mb: 256,
            text_field: "content".to_string(),
            default_partition,
        },
        vector: CfgVector {
            hnsw_resident_budget_mb: None,
            dimension: 384,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 50,
        },
        embedding: EmbeddingConfig { cache_size: 100 },
        connections: ConnectionConfig {
            auto_discovery_threshold: 0.7,
            auto_discovery_top_k: 5,
        },
        search: SearchConfig {
            widen_max_factor: 32,
            list_max_factor: 128,
        },
        tasks: TaskConfig {
            expire_short_term_secs: 300,
            apply_importance_decay_secs: 86_400,
            active_forgetting_secs: 86_400,
            consolidate_similar_secs: 604_800,
            cleanup_archived_secs: 2_592_000,
            discover_connections_secs: 3_600,
            flush_recall_secs: 30,
            discovery_workers: 2,
            discovery_queue_size: 10_000,
            active_forgetting_hard_delete: false,
        },
    };
    let services = create_services(engine, &cfg).await?;
    let discovery_rx = Arc::clone(&services.discovery_rx);
    tokio::spawn(async move {
        while discovery_rx.lock().await.recv().await.is_some() {
            // The benchmark measures create requests through the API boundary. Drain
            // queued discovery work without executing it so that it never accumulates
            // or joins the timed critical path.
        }
    });
    let state = AppState {
        services,
        config: Arc::new(cfg),
    };

    Ok(WriteBenchmarkApp {
        router: build_router(state),
        sync_writes,
    })
}
