use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::config::TaskConfig;
use crate::services::LifecycleManager;
use crate::tasks::TaskRegistry;

/// Run all lifecycle background tasks until `token` is cancelled.
pub async fn run(
    lifecycle: Arc<LifecycleManager>,
    registry: Arc<TaskRegistry>,
    engine: Arc<crate::engine::StorageEngine>,
    cfg: TaskConfig,
    token: CancellationToken,
) {
    let mut expiry_interval     = tokio::time::interval(Duration::from_secs(cfg.expire_short_term_secs));
    let mut decay_interval      = tokio::time::interval(Duration::from_secs(cfg.apply_importance_decay_secs));
    let mut forgetting_interval = tokio::time::interval(Duration::from_secs(cfg.active_forgetting_secs));
    let mut consolidate_interval = tokio::time::interval(Duration::from_secs(cfg.consolidate_similar_secs));
    let mut cleanup_interval    = tokio::time::interval(Duration::from_secs(cfg.cleanup_archived_secs));
    let mut discover_interval   = tokio::time::interval(Duration::from_secs(cfg.discover_connections_secs));

    // Skip the immediate first tick so tasks don't fire on startup
    expiry_interval.tick().await;
    decay_interval.tick().await;
    forgetting_interval.tick().await;
    consolidate_interval.tick().await;
    cleanup_interval.tick().await;
    discover_interval.tick().await;

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("lifecycle tasks: shutdown signal received");
                break;
            }
            _ = expiry_interval.tick() => {
                if registry.is_paused("expire_short_term") {
                    tracing::debug!(task = "expire_short_term", "skipped — paused");
                } else {
                    run_task("expire_short_term", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine)).await;
                }
            }
            _ = decay_interval.tick() => {
                if registry.is_paused("apply_importance_decay") {
                    tracing::debug!(task = "apply_importance_decay", "skipped — paused");
                } else {
                    run_task("apply_importance_decay", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine)).await;
                }
            }
            _ = forgetting_interval.tick() => {
                if registry.is_paused("active_forgetting") {
                    tracing::debug!(task = "active_forgetting", "skipped — paused");
                } else {
                    run_task("active_forgetting", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine)).await;
                }
            }
            _ = consolidate_interval.tick() => {
                if registry.is_paused("consolidate_similar") {
                    tracing::debug!(task = "consolidate_similar", "skipped — paused");
                } else {
                    run_task("consolidate_similar", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine)).await;
                }
            }
            _ = cleanup_interval.tick() => {
                if registry.is_paused("cleanup_archived") {
                    tracing::debug!(task = "cleanup_archived", "skipped — paused");
                } else {
                    run_task("cleanup_archived", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine)).await;
                }
            }
            _ = discover_interval.tick() => {
                if registry.is_paused("discover_connections") {
                    tracing::debug!(task = "discover_connections", "skipped — paused");
                } else {
                    run_task("discover_connections", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine)).await;
                }
            }
        }
    }
}

pub async fn run_task(
    name: &str,
    lifecycle: Arc<LifecycleManager>,
    registry: Arc<TaskRegistry>,
    engine: Arc<crate::engine::StorageEngine>,
) {
    registry.set_running(name);
    let result: crate::error::Result<usize> = match name {
        "expire_short_term" => lifecycle.expire_short_term().await,
        "apply_importance_decay" => lifecycle.apply_importance_decay().await,
        "active_forgetting" => lifecycle.active_forgetting().await,
        "consolidate_similar" => lifecycle.consolidate_similar().await,
        "cleanup_archived" => lifecycle.cleanup_archived(30).await,
        "discover_connections" => lifecycle.discover_connections(0.7, 5).await,
        "checkpoint" => engine.checkpoint().await.map(|_| 0usize).map_err(Into::into),
        _ => Err(crate::error::AppError::Validation(format!("unknown task: {name}"))),
    };
    match result {
        Ok(n) => {
            tracing::debug!(task = name, count = n, "task completed");
            registry.record_result(name, n as i64, None);
        }
        Err(e) => {
            tracing::error!(task = name, error = %e, "task failed");
            registry.record_result(name, -1, Some(e.to_string()));
        }
    }
}
