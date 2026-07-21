mod api;
mod config;
mod embedding;
mod engine;
mod error;
#[cfg(feature = "business")]
mod business;
mod services;
mod tasks;

use std::sync::Arc;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use engine::{
    storage::engine::{EngineConfig, GraphIndexConfig, TagIndexConfig, TimeSeriesConfig, VectorConfig},
    StorageEngine,
};

use api::{build_router, AppState};
use config::Args;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Logging ──────────────────────────────────────────────────────────────
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // ── Config ───────────────────────────────────────────────────────────────
    let args = Args::parse();
    let cfg = config::load(&args)?;
    tracing::info!(
        port = cfg.server.port,
        data_dir = %cfg.storage.data_dir.display(),
        "remem-server starting"
    );
    if cfg.server.api_key.is_empty() {
        if cfg.server.allow_auth_disabled {
            tracing::warn!("SECURITY: auth is disabled (REMEM_ALLOW_AUTH_DISABLED=true). Do not use in production.");
        } else {
            tracing::error!("REMEM_API_KEY is empty and REMEM_ALLOW_AUTH_DISABLED is not set. All non-health requests will return 500.");
        }
    }

    // REMEM_ENV=production enforces the security bar that dev-friendly
    // defaults (auth disabled, permissive CORS, placeholder keys) would
    // otherwise silently violate — refuse to boot rather than serve
    // insecurely, since these are exactly the defaults .env.example ships.
    if cfg.server.env.is_production() {
        let violations = config::validate_production_config(&cfg);
        if !violations.is_empty() {
            for v in &violations {
                tracing::error!("REMEM_ENV=production refused to start: {v}");
            }
            anyhow::bail!(
                "{} production security violation(s); see errors above. \
                 Fix the configuration or unset REMEM_ENV to run in development mode.",
                violations.len()
            );
        }
        tracing::info!("REMEM_ENV=production: security validation passed");
    }

    // ── Storage engine ───────────────────────────────────────────────────────
    let engine_cfg = EngineConfig {
        data_dir: cfg.storage.data_dir.clone(),
        sync_writes: cfg.storage.sync_writes,
        checkpoint_interval: std::time::Duration::from_secs(
            cfg.storage.checkpoint_interval_secs,
        ),
        max_wal_size: cfg.storage.max_wal_size_mb * 1024 * 1024,
        vector: VectorConfig {
            enabled: true,
            dimension: cfg.vector.dimension,
            hnsw_m: cfg.vector.hnsw_m,
            hnsw_ef_construction: cfg.vector.hnsw_ef_construction,
            hnsw_ef_search: cfg.vector.hnsw_ef_search,
            metric: engine::util::DistanceMetric::L2,
        },
        graph: GraphIndexConfig {
            enabled: true,
            ..Default::default()
        },
        time_series: TimeSeriesConfig { enabled: true },
        tag_index: TagIndexConfig {
            enabled: true,
            lowercase: true,
            ..Default::default()
        },
        ..Default::default()
    };

    let engine = Arc::new(StorageEngine::new(engine_cfg).await?);
    tracing::info!("storage engine ready");

    // ── Services ─────────────────────────────────────────────────────────────
    let mut services = services::create_services(Arc::clone(&engine), &cfg).await?;
    tracing::info!("embedding model loaded");

    // ── Background tasks ─────────────────────────────────────────────────────
    let token = CancellationToken::new();
    let mut supervisor = tasks::supervisor::TaskSupervisor::new(token.clone());
    supervisor.spawn_lifecycle(
        Arc::clone(&services.lifecycle),
        Arc::clone(&services.task_registry),
        Arc::clone(&engine),
        cfg.tasks.clone(),
    );
    // Spawn discovery workers with restart-on-panic (max 5 retries each).
    // Worker count and queue size come from [tasks] config.
    // Worker state Arcs are pre-wired into AppServices so routes can read
    // health counters without a reference to the supervisor.
    let worker_state = tasks::supervisor::DiscoveryWorkerState::new(cfg.tasks.discovery_workers);
    supervisor.spawn_discovery_workers(
        cfg.tasks.discovery_workers,
        Arc::clone(&services.discovery_rx),
        Arc::clone(&services.connection),
        &worker_state,
    );
    services.discovery_worker_state = worker_state;

    // ── HTTP server ───────────────────────────────────────────────────────────
    let state = AppState {
        services,
        config: Arc::new(cfg.clone()),
    };
    let router = build_router(state);
    let addr = format!("{}:{}", cfg.server.host, cfg.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(addr, "listening");

    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    // ── Graceful shutdown ─────────────────────────────────────────────────────
    tracing::info!("HTTP server stopped, shutting down background tasks…");
    supervisor.shutdown().await;

    tracing::info!("flushing storage engine…");
    engine.graceful_shutdown().await?;
    tracing::info!("remem-server stopped");

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
