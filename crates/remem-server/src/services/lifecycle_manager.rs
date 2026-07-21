use std::sync::Arc;
use uuid::Uuid;

use crate::embedding::EmbeddingService;
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
}

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
        }
    }

    /// Promote a short-term memory to long-term. Returns the updated memory.
    pub async fn promote(&self, id: Uuid) -> Result<StoredMemory> {
        let _guard = self.repo.lock(id).await;

        let mut stored = self
            .repo
            .load(id)
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
        let mut index_tags = stored.metadata.tags.clone();
        index_tags.push("__type:long_term".to_owned());
        self.repo.engine.set_tags(key, &index_tags)?;

        self.repo.store(&stored).await?;
        Ok(stored)
    }

    /// Archive or promote expired short-term memories. Returns count handled.
    pub async fn expire_short_term(&self) -> Result<usize> {
        let now = now_ms();
        let entries = self.repo.engine.time_range_query(0, now, None)?;
        let mut handled = 0usize;

        for (_ts, key_bytes) in entries {
            let Some(id) = parse_memory_id(&key_bytes) else { continue };
            let guard = self.repo.lock(id).await;

            let Some(mut stored) = self.repo.load_by_key(&key_bytes).await? else {
                continue;
            };
            if stored.archived || stored.memory_type != MemoryType::ShortTerm {
                continue;
            }
            if !stored.is_expired() {
                continue;
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
                let _ = self.repo.store(&stored).await;
                let key = memory_key(stored.id);
                let _ = self.repo.engine.add_tags(key, &["__archived__".to_owned()]);
            }
            handled += 1;
        }

        tracing::info!(handled, "expire_short_term completed");
        Ok(handled)
    }

    /// Apply importance decay to long-term memories. Returns count updated.
    pub async fn apply_importance_decay(&self) -> Result<usize> {
        let now = now_ms();
        let day_ms: u64 = 86_400_000;
        let decay_factor: f32 = 0.995; // ~0.5% per day

        let entries = self.repo.engine.time_range_query(0, now, None)?;
        let mut updated = 0usize;

        for (_ts, key_bytes) in entries {
            let Some(id) = parse_memory_id(&key_bytes) else { continue };
            let _guard = self.repo.lock(id).await;

            let Some(mut stored) = self.repo.load_by_key(&key_bytes).await? else {
                continue;
            };
            if stored.archived || stored.memory_type != MemoryType::LongTerm {
                continue;
            }
            if stored.metadata.flashbulb_until.is_some_and(|until| until > now) {
                continue;
            }

            let last_decay = stored
                .metadata
                .last_decay_at
                .unwrap_or(stored.metadata.created_at);
            let age_days = (now.saturating_sub(last_decay)) / day_ms;
            if age_days == 0 {
                continue;
            }

            let new_importance =
                stored.metadata.importance * decay_factor.powi(age_days as i32);
            stored.metadata.importance = new_importance.max(0.0);
            stored.metadata.last_decay_at = Some(now);

            let _ = self.repo.store(&stored).await;
            updated += 1;
        }

        tracing::info!(updated, "apply_importance_decay completed");
        Ok(updated)
    }

    /// Decay memory health and archive memories whose health reaches zero.
    /// Only hard-deletes when `hard_delete_on_forgetting` is explicitly set
    /// (see `TaskConfig.active_forgetting_hard_delete`).
    pub async fn active_forgetting(&self) -> Result<usize> {
        let now = now_ms();
        let day_ms: u64 = 86_400_000;
        let entries = self.repo.engine.time_range_query(0, now, None)?;
        let mut handled = 0usize;

        for (_ts, key_bytes) in entries {
            let Some(id) = parse_memory_id(&key_bytes) else { continue };
            let _guard = self.repo.lock(id).await;

            let Some(mut stored) = self.repo.load_by_key(&key_bytes).await? else {
                continue;
            };
            if stored.archived {
                continue;
            }
            if stored.metadata.flashbulb_until.is_some_and(|until| until > now) {
                continue;
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
                continue;
            }

            let daily_decay = match stored.memory_type {
                MemoryType::ShortTerm => 8.0,
                MemoryType::LongTerm => 2.0,
            };
            stored.metadata.health = (stored.metadata.health - daily_decay * age_days as f32)
                .clamp(0.0, 100.0);
            stored.metadata.last_health_check_at = Some(now);

            if stored.metadata.health <= 0.0 {
                if self.hard_delete_on_forgetting {
                    self.repo.delete(stored.id).await?;
                } else {
                    stored.archived = true;
                    stored.metadata.updated_at = now;
                    self.repo.store(&stored).await?;
                    let key = memory_key(stored.id);
                    let _ = self.repo.engine.add_tags(key, &["__archived__".to_owned()]);
                }
            } else {
                self.repo.store(&stored).await?;
            }
            handled += 1;
        }

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
        let cutoff = now.saturating_sub(max_age_days * 86_400_000);
        let entries = self.repo.engine.time_range_query(0, u64::MAX, None)?;
        let mut deleted = 0usize;

        for (_ts, key_bytes) in entries {
            let Some(id) = parse_memory_id(&key_bytes) else { continue };
            let _guard = self.repo.lock(id).await;

            let Some(stored) = self.repo.load_by_key(&key_bytes).await? else {
                continue;
            };
            if !stored.archived {
                continue;
            }
            if stored.metadata.updated_at > cutoff {
                continue;
            }

            let _ = self.repo.delete(stored.id).await;
            deleted += 1;
        }

        tracing::info!(deleted, "cleanup_archived completed");
        Ok(deleted)
    }

    /// Re-discover connections for all non-archived memories. Returns total new connections found.
    pub async fn discover_connections(
        &self,
        threshold: f32,
        top_k: usize,
    ) -> Result<usize> {
        const CONCURRENCY: usize = 16;

        let now = now_ms();
        let entries = self.repo.engine.time_range_query(0, now, None)?;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(CONCURRENCY));
        let mut join_set = tokio::task::JoinSet::new();
        let mut total = 0usize;

        for (_ts, key_bytes) in entries {
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

            join_set.spawn(async move {
                let _permit = permit; // dropped at end of task, releasing permit
                match conn.auto_discover(id, &embedding, threshold, top_k).await {
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
    use crate::services::connection_manager::ConnectionManager;
    use crate::services::repository::MemoryRepository;
    use crate::services::types::{MemoryType, StoredMetadata};

    async fn test_engine() -> Arc<crate::engine::StorageEngine> {
        Arc::new(
            crate::engine::storage::engine::StorageEngine::new(
                crate::engine::storage::engine::EngineConfig {
                    data_dir: tempfile::tempdir().unwrap().keep(),
                    sync_writes: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        )
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
        let stored = StoredMemory {
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
        repo.store(&stored).await.unwrap();
        engine.add_timestamp(memory_key(id), 0).unwrap();

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
        let stored = StoredMemory {
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
        repo.store(&stored).await.unwrap();
        engine.add_timestamp(memory_key(id), 0).unwrap();

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
        let stored = StoredMemory {
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
        repo.store(&stored).await.unwrap();
        engine.add_timestamp(memory_key(id), 0).unwrap();

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
        let stored = StoredMemory {
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
        repo.store(&stored).await.unwrap();
        engine.add_timestamp(memory_key(id), long_ago).unwrap();

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
}
