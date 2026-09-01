use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::config::TaskConfig;
use crate::engine::storage::partition::{PartitionBinding, PartitionScope};
use crate::services::LifecycleManager;
use crate::tasks::TaskRegistry;

#[derive(Debug, Clone)]
pub struct TaskPartitionSet {
    read_scope: PartitionScope,
    write_target: PartitionBinding,
}

impl TaskPartitionSet {
    pub fn from_lifecycle(lifecycle: &LifecycleManager) -> Self {
        Self {
            read_scope: lifecycle.read_scope().clone(),
            write_target: lifecycle.write_target().clone(),
        }
    }

    pub fn read_scope(&self) -> &PartitionScope {
        &self.read_scope
    }

    pub fn write_target(&self) -> &PartitionBinding {
        &self.write_target
    }
}

/// Run all lifecycle background tasks until `token` is cancelled.
pub async fn run(
    lifecycle: Arc<LifecycleManager>,
    registry: Arc<TaskRegistry>,
    engine: Arc<crate::engine::StorageEngine>,
    cfg: TaskConfig,
    token: CancellationToken,
) {
    let partition_set = TaskPartitionSet::from_lifecycle(&lifecycle);
    let mut expiry_interval =
        tokio::time::interval(Duration::from_secs(cfg.expire_short_term_secs));
    let mut decay_interval =
        tokio::time::interval(Duration::from_secs(cfg.apply_importance_decay_secs));
    let mut forgetting_interval =
        tokio::time::interval(Duration::from_secs(cfg.active_forgetting_secs));
    let mut consolidate_interval =
        tokio::time::interval(Duration::from_secs(cfg.consolidate_similar_secs));
    let mut cleanup_interval =
        tokio::time::interval(Duration::from_secs(cfg.cleanup_archived_secs));
    let mut discover_interval =
        tokio::time::interval(Duration::from_secs(cfg.discover_connections_secs));
    let mut recall_interval = tokio::time::interval(Duration::from_secs(cfg.flush_recall_secs));

    // Skip the immediate first tick so tasks don't fire on startup
    expiry_interval.tick().await;
    decay_interval.tick().await;
    forgetting_interval.tick().await;
    consolidate_interval.tick().await;
    cleanup_interval.tick().await;
    discover_interval.tick().await;
    recall_interval.tick().await;

    // The flush task's own accumulator, so the early-wake arm below can
    // await it without reaching back through the service graph each tick.
    let recall_log = Arc::clone(lifecycle.recall_log());

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("lifecycle tasks: shutdown signal received");
                // Recall is allowed to be up to one interval behind on a
                // crash, but a clean shutdown has no such excuse -- persist
                // what is pending before the process goes away.
                match lifecycle.flush_recall().await {
                    Ok(n) if n > 0 => tracing::info!(count = n, "flushed pending recall on shutdown"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "failed to flush pending recall on shutdown"),
                }
                break;
            }
            _ = expiry_interval.tick() => {
                if registry.is_paused("expire_short_term") {
                    tracing::debug!(task = "expire_short_term", "skipped — paused");
                } else {
                    run_task("expire_short_term", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine), partition_set.clone()).await;
                }
            }
            _ = decay_interval.tick() => {
                if registry.is_paused("apply_importance_decay") {
                    tracing::debug!(task = "apply_importance_decay", "skipped — paused");
                } else {
                    run_task("apply_importance_decay", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine), partition_set.clone()).await;
                }
            }
            _ = forgetting_interval.tick() => {
                if registry.is_paused("active_forgetting") {
                    tracing::debug!(task = "active_forgetting", "skipped — paused");
                } else {
                    run_task("active_forgetting", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine), partition_set.clone()).await;
                }
            }
            _ = consolidate_interval.tick() => {
                if registry.is_paused("consolidate_similar") {
                    tracing::debug!(task = "consolidate_similar", "skipped — paused");
                } else {
                    run_task("consolidate_similar", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine), partition_set.clone()).await;
                }
            }
            _ = cleanup_interval.tick() => {
                if registry.is_paused("cleanup_archived") {
                    tracing::debug!(task = "cleanup_archived", "skipped — paused");
                } else {
                    run_task("cleanup_archived", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine), partition_set.clone()).await;
                }
            }
            _ = recall_interval.tick() => {
                if registry.is_paused("flush_recall") {
                    tracing::debug!(task = "flush_recall", "skipped — paused");
                } else {
                    run_task("flush_recall", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine), partition_set.clone()).await;
                }
            }
            // Not a schedule: the accumulator has outgrown its cap, so drain
            // it now rather than letting it keep growing until the next tick.
            _ = recall_log.over_capacity() => {
                if !registry.is_paused("flush_recall") {
                    run_task("flush_recall", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine), partition_set.clone()).await;
                }
            }
            _ = discover_interval.tick() => {
                if registry.is_paused("discover_connections") {
                    tracing::debug!(task = "discover_connections", "skipped — paused");
                } else {
                    run_task("discover_connections", Arc::clone(&lifecycle), Arc::clone(&registry), Arc::clone(&engine), partition_set.clone()).await;
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
    partition_set: TaskPartitionSet,
) {
    debug_assert_eq!(partition_set.read_scope(), lifecycle.read_scope());
    debug_assert_eq!(partition_set.write_target(), lifecycle.write_target());
    registry.set_running(name);
    let result: crate::error::Result<usize> = match name {
        "expire_short_term" => lifecycle.expire_short_term().await,
        "apply_importance_decay" => lifecycle.apply_importance_decay().await,
        "active_forgetting" => lifecycle.active_forgetting().await,
        "consolidate_similar" => lifecycle.consolidate_similar().await,
        "cleanup_archived" => lifecycle.cleanup_archived(30).await,
        "discover_connections" => lifecycle.discover_connections(0.7, 5).await,
        "flush_recall" => lifecycle.flush_recall().await,
        "checkpoint" => engine
            .checkpoint()
            .await
            .map(|_| 0usize)
            .map_err(Into::into),
        _ => Err(crate::error::AppError::Validation(format!(
            "unknown task: {name}"
        ))),
    };
    match result {
        Ok(n) => {
            tracing::debug!(
                task = name,
                tenant = %partition_set.read_scope().tenant(),
                partitions = ?partition_set.read_scope().partitions(),
                write_partition = %partition_set.write_target().partition(),
                count = n,
                "task completed"
            );
            registry.record_result(name, n as i64, None);
        }
        Err(e) => {
            tracing::error!(
                task = name,
                tenant = %partition_set.read_scope().tenant(),
                partitions = ?partition_set.read_scope().partitions(),
                write_partition = %partition_set.write_target().partition(),
                error = %e,
                "task failed"
            );
            registry.record_result(name, -1, Some(e.to_string()));
        }
    }
}
