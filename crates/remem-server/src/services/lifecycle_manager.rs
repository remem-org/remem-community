use std::sync::Arc;
use uuid::Uuid;

use crate::embedding::EmbeddingService;
use crate::engine::storage::partition::{PartitionBinding, PartitionScope};
use crate::error::{AppError, Result};
use crate::services::connection_manager::ConnectionManager;
use crate::services::repository::MemoryRepository;
use crate::services::types::{memory_key, now_ms, parse_memory_id, MemoryType, StoredMemory};

pub struct LifecycleManager {
    repo: Arc<MemoryRepository>,
    connection: Arc<ConnectionManager>,
    embedding: Arc<EmbeddingService>,
    /// Access-count threshold above which an expired short-term memory is promoted.
    promote_threshold: u32,
    /// When true, active_forgetting hard-deletes at health=0 instead of
    /// archiving. Defaults to false at the config layer (see TaskConfig).
    hard_delete_on_forgetting: bool,
    /// Where the last `discover_connections` run stopped.
    ///
    /// In memory rather than on disk: losing it costs one repeated pass over
    /// a prefix that is idempotent anyway (re-discovering a connection that
    /// already exists changes nothing), which is not worth a durable write on
    /// every run.
    discovery_resume: tokio::sync::Mutex<Option<crate::engine::index::IndexPosition>>,
    /// How many memories one run of a sweep may handle.
    ///
    /// A bound on a single run, not on the work: whatever a run does not
    /// reach stays due and is picked up by the next one. Without it a large
    /// backlog -- a long outage, or a first run after the upgrade -- would be
    /// one unbounded pass competing with live traffic.
    run_budget: usize,
}

/// How many memories a sweep handles in one run before leaving the rest for
/// the next.
pub const DEFAULT_RUN_BUDGET: usize = 10_000;

/// How many candidates a sweep may settle per memory it is allowed to handle.
///
/// One shared attention time means a sweep is woken for memories another
/// sweep owns; this bounds how many of those it will settle and discard
/// before giving up for this run.
const SWEEP_EFFORT_FACTOR: usize = 8;

impl LifecycleManager {
    pub fn new(
        repo: Arc<MemoryRepository>,
        connection: Arc<ConnectionManager>,
        embedding: Arc<EmbeddingService>,
        hard_delete_on_forgetting: bool,
    ) -> Self {
        Self {
            repo,
            connection,
            embedding,
            promote_threshold: 3,
            hard_delete_on_forgetting,
            discovery_resume: tokio::sync::Mutex::new(None),
            run_budget: DEFAULT_RUN_BUDGET,
        }
    }

    /// Override the per-run budget. Tests use it to force a run to stop
    /// part-way and prove the next one continues.
    #[cfg(test)]
    pub fn with_run_budget(mut self, budget: usize) -> Self {
        self.run_budget = budget;
        self
    }

    /// Walk the memories currently due, handing each to `handle`.
    ///
    /// The shape every selective sweep shares: page through the due-time
    /// index, stop at the run budget, and report how many were acted on.
    /// `handle` returns whether it did anything -- a sweep woken for a memory
    /// another sweep owns says `false`, and that memory does not count
    /// against the budget, because the budget bounds work done rather than
    /// candidates seen.
    ///
    /// `due_before` is usually now. Cleanup passes something else: memories
    /// are scheduled against the retention the projection assumes, so a run
    /// asked for a *different* retention shifts the window rather than
    /// silently ignoring what it was asked for.
    async fn sweep_due<F, Fut>(&self, due_before: u64, mut handle: F) -> Result<usize>
    where
        F: FnMut(bytes::Bytes) -> Fut,
        Fut: std::future::Future<Output = Result<bool>>,
    {
        const BATCH: usize = 256;

        let mut handled = 0usize;
        let mut after = None;
        let mut examined = 0usize;
        let effort = self.run_budget.saturating_mul(SWEEP_EFFORT_FACTOR);

        while handled < self.run_budget {
            let page = self
                .repo
                .select_due(
                    due_before,
                    after,
                    BATCH.min(self.run_budget - handled),
                    effort.saturating_sub(examined).max(1),
                )
                .await?;
            after = page.next.clone();
            examined += page.examined;
            let exhausted = page.exhausted;
            let stopped_early = page.truncated;

            for key in page.keys {
                if handle(key).await? {
                    handled += 1;
                    if handled >= self.run_budget {
                        break;
                    }
                }
            }

            if exhausted || stopped_early || examined >= effort {
                break;
            }
        }
        Ok(handled)
    }

    pub fn read_scope(&self) -> &PartitionScope {
        self.repo.read_scope()
    }

    pub fn write_target(&self) -> &PartitionBinding {
        self.repo.write_target()
    }

    /// The recall accumulator this manager flushes from.
    pub fn recall_log(&self) -> &Arc<crate::services::recall::RecallLog> {
        self.repo.recall()
    }

    /// Promote a short-term memory to long-term. Returns the updated memory.
    pub async fn promote(&self, id: Uuid) -> Result<StoredMemory> {
        let _guard = self.repo.lock(id).await;

        let (binding, mut stored) = self
            .repo
            .load_bound(id)
            .await?
            .ok_or(AppError::NotFound(id))?;

        if stored.archived {
            return Err(AppError::NotFound(id));
        }
        if stored.memory_type == MemoryType::LongTerm {
            return Ok(stored); // Already long-term.
        }

        stored.memory_type = MemoryType::LongTerm;
        stored.metadata.ttl = None;
        stored.metadata.updated_at = now_ms();

        let key = memory_key(id);
        let physical_key = self.repo.physical_key_in(&binding, key.as_bytes())?;
        let mut index_tags = stored.metadata.tags.clone();
        index_tags.push("__type:long_term".to_owned());
        self.repo.engine.set_tags(physical_key, &index_tags).await?;

        self.repo.store_in(&binding, &mut stored).await?;
        Ok(stored)
    }

    /// Archive or promote expired short-term memories. Returns count handled.
    /// Pending recall is flushed first, for the same reason
    /// `active_forgetting` drains before it sweeps: this decision reads
    /// `access_count` to choose between promoting a memory and archiving it
    /// as unused, and an unflushed recall is a memory someone used that still
    /// looks untouched. Folding it during the sweep's own write would arrive
    /// after the choice was already made.
    pub async fn expire_short_term(&self) -> Result<usize> {
        self.flush_recall().await?;

        let now = now_ms();
        let handled = self
            .sweep_due(now, |key_bytes| async move {
                let Some(id) = parse_memory_id(&key_bytes) else {
                    return Ok(false);
                };
                let guard = self.repo.lock(id).await;

                let Some((binding, mut stored)) = self.repo.load_bound_by_key(&key_bytes).await?
                else {
                    return Ok(false);
                };
                // Woken by the shared attention time for a sweep that is not
                // this one. Settled without acting; the sweep that owns this
                // memory will move it on.
                if stored.archived || stored.memory_type != MemoryType::ShortTerm {
                    return Ok(false);
                }
                if !stored.is_expired() {
                    return Ok(false);
                }

                if stored.metadata.access_count >= self.promote_threshold {
                    // Release our lock before calling promote(), which acquires
                    // its own lock on this same id -- holding both would
                    // deadlock (tokio::sync::Mutex is not reentrant).
                    drop(guard);
                    let _ = self.promote(stored.id).await;
                } else {
                    // Archive low-value expired memories
                    stored.archived = true;
                    stored.metadata.updated_at = now;
                    let _ = self.repo.store_in(&binding, &mut stored).await;
                    let key = memory_key(stored.id);
                    if let Ok(physical_key) = self.repo.physical_key_in(&binding, key.as_bytes()) {
                        let _ = self
                            .repo
                            .engine
                            .add_tags(physical_key, &["__archived__".to_owned()])
                            .await;
                    }
                    // Retire only after the record is archived -- see the same
                    // ordering note in `MemoryManager::delete`.
                    if let Err(e) = self
                        .repo
                        .retire_from_retrieval_in(&binding, stored.id)
                        .await
                    {
                        tracing::warn!(
                            memory_id = %stored.id,
                            error = %e,
                            "failed to retire expired memory from search and browsing; it keeps \
                             costing retrieval work until cleanup"
                        );
                    }
                }
                Ok(true)
            })
            .await?;

        tracing::info!(handled, "expire_short_term completed");
        Ok(handled)
    }

    /// Apply importance decay to long-term memories. Returns count updated.
    pub async fn apply_importance_decay(&self) -> Result<usize> {
        let now = now_ms();
        let day_ms: u64 = 86_400_000;
        let decay_factor: f32 = 0.995; // ~0.5% per day

        let updated = self
            .sweep_due(now, |key_bytes| async move {
                let Some(id) = parse_memory_id(&key_bytes) else {
                    return Ok(false);
                };
                let _guard = self.repo.lock(id).await;

                let Some((binding, mut stored)) = self.repo.load_bound_by_key(&key_bytes).await?
                else {
                    return Ok(false);
                };
                if stored.archived || stored.memory_type != MemoryType::LongTerm {
                    return Ok(false);
                }
                if stored
                    .metadata
                    .flashbulb_until
                    .is_some_and(|until| until > now)
                {
                    return Ok(false);
                }

                let last_decay = stored
                    .metadata
                    .last_decay_at
                    .unwrap_or(stored.metadata.created_at);
                let age_days = (now.saturating_sub(last_decay)) / day_ms;
                if age_days == 0 {
                    return Ok(false);
                }

                let new_importance =
                    stored.metadata.importance * decay_factor.powi(age_days as i32);
                stored.metadata.importance = new_importance.max(0.0);
                stored.metadata.last_decay_at = Some(now);

                let _ = self.repo.store_in(&binding, &mut stored).await;
                Ok(true)
            })
            .await?;

        tracing::info!(updated, "apply_importance_decay completed");
        Ok(updated)
    }

    /// Persist every recall recorded since the last flush. Returns the
    /// number of memories written.
    ///
    /// Recall is deliberately not durable at the moment it happens -- see
    /// `services::recall`. This is what bounds how long it stays that way.
    /// The write itself does not apply the delta: `store_in` folds it, so a
    /// record that was also updated in the meantime has already consumed
    /// its recall and is simply absent from the drain.
    pub async fn flush_recall(&self) -> Result<usize> {
        let pending = self.repo.recall().drain();
        if pending.is_empty() {
            return Ok(0);
        }

        let mut written = 0usize;
        let mut retry = Vec::new();
        let mut last_error = None;

        for ((binding, id), delta) in pending {
            let _guard = self.repo.lock(id).await;

            let loaded = match self.repo.load_bound(id).await {
                Ok(loaded) => loaded,
                Err(e) => {
                    tracing::warn!(memory_id = %id, error = %e, "flush_recall: load failed");
                    retry.push(((binding, id), delta));
                    last_error = Some(e);
                    continue;
                }
            };
            let Some((actual_binding, mut stored)) = loaded else {
                // The memory was deleted while its recall was pending.
                // There is nothing to write it to, and retrying would never
                // succeed -- dropping it is the only terminating choice.
                continue;
            };

            // Hand the delta back keyed by the partition the record actually
            // lives in: `store_in` folds under the binding it writes to, and
            // a record can be read out of a partition other than the one the
            // recall was recorded against.
            self.repo
                .recall()
                .restore(vec![((actual_binding.clone(), id), delta)]);

            if let Err(e) = self.repo.store_in(&actual_binding, &mut stored).await {
                // `store_in` already returned the delta to the log on
                // failure, so the recall is still pending for the next run.
                tracing::warn!(memory_id = %id, error = %e, "flush_recall: write failed");
                last_error = Some(e);
                continue;
            }
            written += 1;
        }

        if !retry.is_empty() {
            self.repo.recall().restore(retry);
        }

        // Report a failure the way every other lifecycle task does, but only
        // when nothing at all got through: a partial flush is progress, and
        // whatever failed is still pending for the next run.
        match last_error {
            Some(e) if written == 0 => Err(e),
            _ => Ok(written),
        }
    }

    /// Decay memory health and archive memories whose health reaches zero.
    /// Only hard-deletes when `hard_delete_on_forgetting` is explicitly set
    /// (see `TaskConfig.active_forgetting_hard_delete`).
    ///
    /// Pending recall is flushed first. This sweep *reads* health to decide
    /// what to archive, and an unflushed recall is a memory someone used
    /// that still looks untouched -- folding it during the sweep's own write
    /// would arrive after the decision was already made. Draining up front
    /// is one ordering statement instead of an invariant spread across the
    /// loop below.
    pub async fn active_forgetting(&self) -> Result<usize> {
        self.flush_recall().await?;

        let now = now_ms();
        let day_ms: u64 = 86_400_000;

        let handled = self
            .sweep_due(now, |key_bytes| async move {
                let Some(id) = parse_memory_id(&key_bytes) else {
                    return Ok(false);
                };
                let _guard = self.repo.lock(id).await;

                let Some((binding, mut stored)) = self.repo.load_bound_by_key(&key_bytes).await?
                else {
                    return Ok(false);
                };
                if stored.archived {
                    return Ok(false);
                }
                if stored
                    .metadata
                    .flashbulb_until
                    .is_some_and(|until| until > now)
                {
                    return Ok(false);
                }

                // Genuine reinforcement signals only -- deliberately excludes
                // updated_at, which apply_importance_decay (and any future
                // lifecycle task) touches on its own periodic schedule, not
                // because the memory was actually recalled or edited.
                let last_reinforced = stored
                    .metadata
                    .last_recalled_at
                    .unwrap_or(stored.metadata.accessed_at);
                let last_checked = stored
                    .metadata
                    .last_health_check_at
                    .unwrap_or(stored.metadata.created_at);
                let age_days = (now.saturating_sub(last_reinforced.max(last_checked))) / day_ms;
                if age_days == 0 {
                    return Ok(false);
                }

                let daily_decay = match stored.memory_type {
                    MemoryType::ShortTerm => 8.0,
                    MemoryType::LongTerm => 2.0,
                };
                stored.metadata.health =
                    (stored.metadata.health - daily_decay * age_days as f32).clamp(0.0, 100.0);
                stored.metadata.last_health_check_at = Some(now);

                if stored.metadata.health <= 0.0 {
                    if self.hard_delete_on_forgetting {
                        self.repo.delete_in(&binding, stored.id).await?;
                    } else {
                        stored.archived = true;
                        stored.metadata.updated_at = now;
                        self.repo.store_in(&binding, &mut stored).await?;
                        let key = memory_key(stored.id);
                        let physical_key = self.repo.physical_key_in(&binding, key.as_bytes())?;
                        let _ = self
                            .repo
                            .engine
                            .add_tags(physical_key, &["__archived__".to_owned()])
                            .await;
                        // Archive branch only: the hard-delete branch above
                        // already clears every index. Retire after the write --
                        // see the ordering note in `MemoryManager::delete`.
                        if let Err(e) = self
                            .repo
                            .retire_from_retrieval_in(&binding, stored.id)
                            .await
                        {
                            tracing::warn!(
                                memory_id = %stored.id,
                                error = %e,
                                "failed to retire forgotten memory from search and browsing; it \
                                 keeps costing retrieval work until cleanup"
                            );
                        }
                    }
                } else {
                    self.repo.store_in(&binding, &mut stored).await?;
                }
                Ok(true)
            })
            .await?;

        tracing::info!(handled, "active_forgetting completed");
        Ok(handled)
    }

    /// Consolidate highly similar memories by archiving duplicates. Returns count archived.
    pub async fn consolidate_similar(&self) -> Result<usize> {
        // Simple implementation: scan all non-archived memories and group by content similarity.
        // This is a placeholder — a production implementation would use vector search.
        tracing::info!("consolidate_similar: placeholder implementation, no-op");
        Ok(0)
    }

    /// Permanently delete memories archived more than `max_age_days` ago.
    /// Returns count deleted.
    pub async fn cleanup_archived(&self, max_age_days: u64) -> Result<usize> {
        let now = now_ms();
        let day_ms: u64 = 86_400_000;
        let cutoff = now.saturating_sub(max_age_days * day_ms);

        // Archiving scheduled this memory for `updated_at + CLEANUP_AGE_DAYS`.
        // Eligibility for *this* run is `updated_at <= now - max_age_days`, so
        // the scheduled times that satisfy it are those at or before
        // `now + CLEANUP_AGE_DAYS - max_age_days`. Deriving the bound rather
        // than assuming `now` is what keeps the retention this run was asked
        // for meaningful: at the default the two are the same instant, and a
        // shorter retention widens the window instead of quietly finding
        // nothing.
        let due_before = now
            .saturating_add(crate::services::attrs::CLEANUP_AGE_DAYS.saturating_mul(day_ms))
            .saturating_sub(max_age_days.saturating_mul(day_ms));

        let deleted = self
            .sweep_due(due_before, |key_bytes| async move {
                let Some(id) = parse_memory_id(&key_bytes) else {
                    return Ok(false);
                };
                let _guard = self.repo.lock(id).await;

                let Some((binding, stored)) = self.repo.load_bound_by_key(&key_bytes).await? else {
                    return Ok(false);
                };
                if !stored.archived {
                    return Ok(false);
                }
                // The row schedules, the record decides. Selection is exact
                // when the retention matches what archiving assumed; when the
                // window was widened for a shorter retention, this is what
                // holds the line at the age actually asked for.
                if stored.metadata.updated_at > cutoff {
                    return Ok(false);
                }

                let _ = self.repo.delete_in(&binding, stored.id).await;
                Ok(true)
            })
            .await?;

        tracing::info!(deleted, "cleanup_archived completed");
        Ok(deleted)
    }

    /// Re-discover connections for all non-archived memories. Returns total new connections found.
    pub async fn discover_connections(&self, threshold: f32, top_k: usize) -> Result<usize> {
        const CONCURRENCY: usize = 16;

        // The one sweep with no due time to narrow by: re-discovering
        // connections is about every live memory, not about memories that
        // have become due for something. So it takes the other half of the
        // treatment -- a bounded, resumable walk of the whole collection,
        // ordered by creation so a run that stops part-way can say where.
        let mut after = self.discovery_resume.lock().await;
        let page = self
            .repo
            .select_live_page(after.clone(), self.run_budget)
            .await?;
        // A run that reached the end starts the next one from the beginning;
        // one that stopped at its budget continues from where it stopped.
        *after = if page.exhausted {
            None
        } else {
            page.next.clone()
        };
        drop(after);

        let semaphore = Arc::new(tokio::sync::Semaphore::new(CONCURRENCY));
        let mut join_set = tokio::task::JoinSet::new();
        let mut total = 0usize;

        for key_bytes in page.keys.into_iter() {
            let Some(stored) = self.repo.load_by_key(&key_bytes).await? else {
                continue;
            };
            if stored.archived {
                continue;
            }

            // Use stored HNSW vector — avoids re-embedding (major speedup).
            // Fall back to re-embed only for legacy records without a stored vector.
            let embedding = if let Some(vec) = self.repo.engine.get_vector(key_bytes.as_ref()) {
                vec
            } else {
                match self.embedding.embed(&stored.content).await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(
                            memory_id = %stored.id,
                            error = %e,
                            "discover_connections: embed fallback failed"
                        );
                        continue;
                    }
                }
            };

            let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
            let conn = Arc::clone(&self.connection);
            let id = stored.id;
            let read_scope = self.repo.read_scope().clone();

            join_set.spawn(async move {
                let _permit = permit; // dropped at end of task, releasing permit
                match conn
                    .auto_discover_in_scope(key_bytes, &embedding, threshold, top_k, &read_scope)
                    .await
                {
                    Ok(conns) => conns.len(),
                    Err(e) => {
                        tracing::warn!(
                            memory_id = %id,
                            error = %e,
                            "discover_connections: auto_discover failed"
                        );
                        0
                    }
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            if let Ok(count) = result {
                total += count;
            }
        }

        tracing::info!(total, "discover_connections completed");
        Ok(total)
    }
}

#[cfg(test)]
mod active_forgetting_tests {
    use super::*;
    use crate::engine::storage::partition::{PartitionId, TenantId};
    use crate::services::connection_manager::ConnectionManager;
    use crate::services::repository::MemoryRepository;
    use crate::services::types::{MemoryType, StoredMetadata};

    async fn test_engine() -> Arc<crate::engine::StorageEngine> {
        Arc::new(
            crate::engine::storage::engine::StorageEngine::new(
                crate::engine::storage::engine::EngineConfig {
                    data_dir: tempfile::tempdir().unwrap().keep(),
                    sync_writes: false,
                    // The sweeps select through the due-time index, so a
                    // fixture without a schema has no access path at all.
                    attr_schema: Some(crate::services::attrs::memory_schema()),
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        )
    }

    fn expired_memory(id: Uuid, content: &str) -> StoredMemory {
        StoredMemory {
            id,
            content: content.into(),
            memory_type: MemoryType::ShortTerm,
            metadata: StoredMetadata {
                created_at: 0,
                updated_at: 0,
                accessed_at: 0,
                access_count: 0,
                source: None,
                tags: vec![],
                importance: 0.5,
                emotional_valence: 0.0,
                arousal: 0.0,
                health: 100.0,
                last_recalled_at: None,
                flashbulb_until: None,
                ttl: Some(1),
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: false,
        }
    }

    #[tokio::test]
    async fn expire_short_term_scans_only_the_repository_partition_scope() {
        let engine = test_engine().await;
        let tenant = TenantId::new("acme").unwrap();
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        let finance = PartitionBinding::new(tenant, PartitionId::new("finance").unwrap());
        let product_repo = Arc::new(MemoryRepository::with_partition_scope(
            Arc::clone(&engine),
            PartitionScope::single(product.clone()),
            product,
        ));
        let finance_repo = Arc::new(MemoryRepository::with_partition_scope(
            Arc::clone(&engine),
            PartitionScope::single(finance.clone()),
            finance,
        ));
        let connection = Arc::new(ConnectionManager::new(Arc::clone(&product_repo)));
        let lifecycle = LifecycleManager::new(
            Arc::clone(&product_repo),
            connection,
            Arc::new(EmbeddingService::new_for_test()),
            false,
        );
        let product_id = Uuid::new_v4();
        let finance_id = Uuid::new_v4();
        let mut product_memory = expired_memory(product_id, "product");
        let mut finance_memory = expired_memory(finance_id, "finance");

        product_repo.store(&mut product_memory).await.unwrap();
        finance_repo.store(&mut finance_memory).await.unwrap();
        engine
            .add_timestamp(product_repo.physical_memory_key(product_id).unwrap(), 0)
            .await
            .unwrap();
        engine
            .add_timestamp(finance_repo.physical_memory_key(finance_id).unwrap(), 0)
            .await
            .unwrap();

        assert_eq!(lifecycle.expire_short_term().await.unwrap(), 1);
        assert!(
            product_repo
                .load(product_id)
                .await
                .unwrap()
                .unwrap()
                .archived
        );
        assert!(
            !finance_repo
                .load(finance_id)
                .await
                .unwrap()
                .unwrap()
                .archived,
            "product-scoped lifecycle scan must not handle finance records"
        );
    }

    #[tokio::test]
    async fn active_forgetting_archives_by_default_instead_of_hard_deleting() {
        let engine = test_engine().await;
        let repo = Arc::new(MemoryRepository::new(Arc::clone(&engine)));
        let connection = Arc::new(ConnectionManager::new(Arc::clone(&repo)));
        let embedding = Arc::new(EmbeddingService::new_for_test());
        let lifecycle = LifecycleManager::new(
            Arc::clone(&repo),
            connection,
            embedding,
            /* hard_delete_on_forgetting */ false,
        );

        let id = Uuid::new_v4();
        let mut stored = StoredMemory {
            id,
            content: "old memory".into(),
            memory_type: MemoryType::ShortTerm,
            metadata: StoredMetadata {
                created_at: 0,
                updated_at: 0,
                accessed_at: 0,
                access_count: 0,
                source: None,
                tags: vec![],
                importance: 0.5,
                emotional_valence: 0.0,
                arousal: 0.0,
                health: 1.0, // one decay tick away from zero
                last_recalled_at: None,
                flashbulb_until: None,
                ttl: None,
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: false,
        };
        repo.store(&mut stored).await.unwrap();
        engine.add_timestamp(memory_key(id), 0).await.unwrap();

        lifecycle.active_forgetting().await.unwrap();

        let after = repo.load(id).await.unwrap();
        assert!(
            after.is_some(),
            "memory must still exist (archived, not hard-deleted)"
        );
        assert!(
            after.unwrap().archived,
            "memory must be archived when health reaches zero"
        );
    }

    #[tokio::test]
    async fn active_forgetting_hard_deletes_when_flag_is_set() {
        let engine = test_engine().await;
        let repo = Arc::new(MemoryRepository::new(Arc::clone(&engine)));
        let connection = Arc::new(ConnectionManager::new(Arc::clone(&repo)));
        let embedding = Arc::new(EmbeddingService::new_for_test());
        let lifecycle = LifecycleManager::new(
            Arc::clone(&repo),
            connection,
            embedding,
            /* hard_delete_on_forgetting */ true,
        );

        let id = Uuid::new_v4();
        let mut stored = StoredMemory {
            id,
            content: "old memory".into(),
            memory_type: MemoryType::ShortTerm,
            metadata: StoredMetadata {
                created_at: 0,
                updated_at: 0,
                accessed_at: 0,
                access_count: 0,
                source: None,
                tags: vec![],
                importance: 0.5,
                emotional_valence: 0.0,
                arousal: 0.0,
                health: 1.0,
                last_recalled_at: None,
                flashbulb_until: None,
                ttl: None,
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: false,
        };
        repo.store(&mut stored).await.unwrap();
        engine.add_timestamp(memory_key(id), 0).await.unwrap();

        lifecycle.active_forgetting().await.unwrap();

        let after = repo.load(id).await.unwrap();
        assert!(
            after.is_none(),
            "memory must be hard-deleted when the opt-in flag is set"
        );
    }

    /// Regression test for the Phase 1 Task 8 deadlock: `expire_short_term`
    /// holds this id's per-memory lock while scanning, and its promote
    /// branch calls `self.promote(id)`, which re-acquires the *same*
    /// non-reentrant `tokio::sync::Mutex`. The fix is the `drop(guard)`
    /// right before that call in `expire_short_term`; a future refactor
    /// that silently drops it hangs the task forever instead of returning
    /// an error, which would otherwise surface only as a CI timeout. The
    /// `tokio::time::timeout` here turns that hang into a fast,
    /// bisectable assertion failure.
    #[tokio::test]
    async fn expire_short_term_promoting_an_expired_memory_does_not_deadlock() {
        let engine = test_engine().await;
        let repo = Arc::new(MemoryRepository::new(Arc::clone(&engine)));
        let connection = Arc::new(ConnectionManager::new(Arc::clone(&repo)));
        let embedding = Arc::new(EmbeddingService::new_for_test());
        let lifecycle = LifecycleManager::new(Arc::clone(&repo), connection, embedding, false);

        let id = Uuid::new_v4();
        let mut stored = StoredMemory {
            id,
            content: "frequently accessed short-term memory".into(),
            memory_type: MemoryType::ShortTerm,
            metadata: StoredMetadata {
                created_at: 0,
                updated_at: 0,
                accessed_at: 0,
                access_count: 5, // >= promote_threshold (3): must take the promote branch
                source: None,
                tags: vec![],
                importance: 0.5,
                emotional_valence: 0.0,
                arousal: 0.0,
                health: 100.0,
                last_recalled_at: None,
                flashbulb_until: None,
                ttl: Some(1), // created_at 0 + ttl 1s: already expired
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: false,
        };
        repo.store(&mut stored).await.unwrap();
        engine.add_timestamp(memory_key(id), 0).await.unwrap();

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            lifecycle.expire_short_term(),
        )
        .await;

        assert!(
            result.is_ok(),
            "expire_short_term deadlocked promoting an expired, frequently-accessed memory \
             -- the per-memory lock was held across the reentrant promote() call"
        );
        result.unwrap().unwrap();

        let after = repo.load(id).await.unwrap().unwrap();
        assert_eq!(
            after.memory_type,
            MemoryType::LongTerm,
            "expired memory at/above the access-count threshold must be promoted, not archived"
        );
    }

    #[tokio::test]
    async fn importance_decay_does_not_reset_active_forgetting_clock() {
        let engine = test_engine().await;
        let repo = Arc::new(MemoryRepository::new(Arc::clone(&engine)));
        let connection = Arc::new(ConnectionManager::new(Arc::clone(&repo)));
        let embedding = Arc::new(EmbeddingService::new_for_test());
        let lifecycle = LifecycleManager::new(Arc::clone(&repo), connection, embedding, false);

        let id = Uuid::new_v4();
        let long_ago = 0u64;
        let mut stored = StoredMemory {
            id,
            content: "long-term memory".into(),
            memory_type: MemoryType::LongTerm,
            metadata: StoredMetadata {
                created_at: long_ago,
                updated_at: long_ago,
                accessed_at: long_ago,
                access_count: 0,
                source: None,
                tags: vec![],
                importance: 0.9,
                emotional_valence: 0.0,
                arousal: 0.0,
                health: 100.0,
                last_recalled_at: None,
                flashbulb_until: None,
                ttl: None,
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: false,
        };
        repo.store(&mut stored).await.unwrap();
        engine
            .add_timestamp(memory_key(id), long_ago)
            .await
            .unwrap();

        // Run importance decay first -- it should NOT reset the clock
        // active_forgetting uses.
        lifecycle.apply_importance_decay().await.unwrap();
        lifecycle.active_forgetting().await.unwrap();

        let after = repo.load(id).await.unwrap().unwrap();
        assert!(
            after.metadata.health < 100.0,
            "health should have decayed based on real age, not been reset by the decay task \
             touching updated_at (health = {})",
            after.metadata.health
        );
    }

    /// A manager over one repository, for the cost and budget tests.
    async fn simple_lifecycle() -> (Arc<MemoryRepository>, LifecycleManager) {
        let engine = test_engine().await;
        let repo = Arc::new(MemoryRepository::new(engine));
        let connection = Arc::new(ConnectionManager::new(Arc::clone(&repo)));
        let lifecycle = LifecycleManager::new(
            Arc::clone(&repo),
            connection,
            Arc::new(EmbeddingService::new_for_test()),
            false,
        );
        (repo, lifecycle)
    }

    /// A long-term memory whose attention is a long way off: nothing is due
    /// for it, so no sweep should look at its content.
    fn settled_memory(id: Uuid) -> StoredMemory {
        let now = now_ms();
        let mut m = expired_memory(id, "settled");
        m.memory_type = MemoryType::LongTerm;
        m.metadata.ttl = None;
        m.metadata.created_at = now;
        m.metadata.updated_at = now;
        m.metadata.accessed_at = now;
        m.metadata.last_decay_at = Some(now);
        m.metadata.last_health_check_at = Some(now);
        m
    }

    /// With nothing due, a sweep must cost nothing: no record read, no write.
    /// This is the whole promise of scheduling attention rather than scanning.
    #[tokio::test]
    async fn a_sweep_with_nothing_due_reads_and_writes_nothing() {
        let (repo, lifecycle) = simple_lifecycle().await;
        for _ in 0..50 {
            let mut m = settled_memory(Uuid::new_v4());
            repo.store(&mut m).await.unwrap();
        }

        repo.engine.reset_payload_reads();
        let handled = lifecycle.expire_short_term().await.unwrap();
        let decayed = lifecycle.apply_importance_decay().await.unwrap();
        let forgotten = lifecycle.active_forgetting().await.unwrap();

        assert_eq!((handled, decayed, forgotten), (0, 0, 0));
        assert_eq!(
            repo.engine.payload_reads(),
            0,
            "nothing was due, so no memory's content should have been read"
        );
    }

    /// The proportionality promise: sweep cost tracks what is due, not how
    /// much the deployment holds.
    #[tokio::test]
    async fn sweep_cost_tracks_what_is_due_not_the_size_of_the_store() {
        let mut reads = Vec::new();
        for settled in [20usize, 200] {
            let (repo, lifecycle) = simple_lifecycle().await;
            for _ in 0..settled {
                let mut m = settled_memory(Uuid::new_v4());
                repo.store(&mut m).await.unwrap();
            }
            // The same three memories are due in both stores.
            for _ in 0..3 {
                let mut m = expired_memory(Uuid::new_v4(), "due");
                repo.store(&mut m).await.unwrap();
            }

            repo.engine.reset_payload_reads();
            let handled = lifecycle.expire_short_term().await.unwrap();
            assert_eq!(handled, 3, "the three due memories are handled either way");
            reads.push(repo.engine.payload_reads());
        }

        assert_eq!(
            reads[0], reads[1],
            "a store ten times larger must not cost ten times the sweep"
        );
    }

    /// A run stops at its budget and the next one continues -- without a
    /// stored cursor, because a memory a sweep handles is written, and that
    /// write moves it out of the due range.
    #[tokio::test]
    async fn a_budgeted_run_leaves_the_rest_for_the_next_one() {
        let engine = test_engine().await;
        let repo = Arc::new(MemoryRepository::new(engine));
        let connection = Arc::new(ConnectionManager::new(Arc::clone(&repo)));
        let lifecycle = LifecycleManager::new(
            Arc::clone(&repo),
            connection,
            Arc::new(EmbeddingService::new_for_test()),
            false,
        )
        .with_run_budget(4);

        let mut ids = Vec::new();
        for _ in 0..10 {
            let id = Uuid::new_v4();
            let mut m = expired_memory(id, "due");
            repo.store(&mut m).await.unwrap();
            ids.push(id);
        }

        let mut handled_per_run = Vec::new();
        for _ in 0..4 {
            handled_per_run.push(lifecycle.expire_short_term().await.unwrap());
        }

        assert_eq!(
            handled_per_run,
            vec![4, 4, 2, 0],
            "each run takes its budget and the next continues; nothing is done twice"
        );
        for id in ids {
            let stored = repo.load(id).await.unwrap().expect("record still present");
            assert!(stored.archived, "every memory that was due got handled");
        }
    }

    /// The retention a cleanup run is dispatched with has to match the one
    /// archiving schedules against, or cleanup selects the wrong window.
    /// They are coupled through one constant; this is what says so.
    #[test]
    fn the_cleanup_age_matches_what_the_projection_assumes() {
        let dispatched = include_str!("../tasks/lifecycle.rs");
        let expected = format!(
            "cleanup_archived({})",
            crate::services::attrs::CLEANUP_AGE_DAYS
        );
        assert!(
            dispatched.contains(&expected),
            "the lifecycle task must dispatch cleanup with CLEANUP_AGE_DAYS ({}), \
             which is the retention `next_attention_at` schedules archived memories against",
            crate::services::attrs::CLEANUP_AGE_DAYS
        );
    }
}
