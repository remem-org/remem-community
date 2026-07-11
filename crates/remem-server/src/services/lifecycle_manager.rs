use std::sync::Arc;
use uuid::Uuid;

use crate::embedding::EmbeddingService;
use crate::error::{AppError, Result};
use crate::services::connection_manager::ConnectionManager;
use crate::services::repository::MemoryRepository;
use crate::services::types::{memory_key, now_ms, MemoryType, StoredMemory};

pub struct LifecycleManager {
    repo: Arc<MemoryRepository>,
    connection: Arc<ConnectionManager>,
    embedding: Arc<EmbeddingService>,
    /// Access-count threshold above which an expired short-term memory is promoted.
    promote_threshold: u32,
}

impl LifecycleManager {
    pub fn new(
        repo: Arc<MemoryRepository>,
        connection: Arc<ConnectionManager>,
        embedding: Arc<EmbeddingService>,
    ) -> Self {
        Self {
            repo,
            connection,
            embedding,
            promote_threshold: 3,
        }
    }

    /// Promote a short-term memory to long-term. Returns the updated memory.
    pub async fn promote(&self, id: Uuid) -> Result<StoredMemory> {
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
                // Promote high-access expired memories
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
            let Some(mut stored) = self.repo.load_by_key(&key_bytes).await? else {
                continue;
            };
            if stored.archived || stored.memory_type != MemoryType::LongTerm {
                continue;
            }
            if stored.metadata.flashbulb_until.is_some_and(|until| until > now) {
                continue;
            }

            let age_days = (now.saturating_sub(stored.metadata.updated_at)) / day_ms;
            if age_days == 0 {
                continue;
            }

            let new_importance =
                stored.metadata.importance * decay_factor.powi(age_days as i32);
            stored.metadata.importance = new_importance.max(0.0);
            stored.metadata.updated_at = now;

            let _ = self.repo.store(&stored).await;
            updated += 1;
        }

        tracing::info!(updated, "apply_importance_decay completed");
        Ok(updated)
    }

    /// Decay memory health and permanently remove memories whose health reaches zero.
    pub async fn active_forgetting(&self) -> Result<usize> {
        let now = now_ms();
        let day_ms: u64 = 86_400_000;
        let entries = self.repo.engine.time_range_query(0, now, None)?;
        let mut handled = 0usize;

        for (_ts, key_bytes) in entries {
            let Some(mut stored) = self.repo.load_by_key(&key_bytes).await? else {
                continue;
            };
            if stored.archived {
                continue;
            }
            if stored.metadata.flashbulb_until.is_some_and(|until| until > now) {
                continue;
            }

            let last_reinforced = stored
                .metadata
                .last_recalled_at
                .unwrap_or(stored.metadata.accessed_at)
                .max(stored.metadata.updated_at);
            let age_days = (now.saturating_sub(last_reinforced)) / day_ms;
            if age_days == 0 {
                continue;
            }

            let daily_decay = match stored.memory_type {
                MemoryType::ShortTerm => 8.0,
                MemoryType::LongTerm => 2.0,
            };
            stored.metadata.health = (stored.metadata.health - daily_decay * age_days as f32)
                .clamp(0.0, 100.0);

            if stored.metadata.health <= 0.0 {
                self.repo.delete(stored.id).await?;
            } else {
                stored.metadata.updated_at = now;
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
