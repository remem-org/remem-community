use std::sync::Arc;
use uuid::Uuid;

use tokio::sync::mpsc;

use crate::embedding::EmbeddingService;
use crate::error::{AppError, Result};
use crate::services::connection_manager::DiscoveryTask;
use crate::services::repository::MemoryRepository;
use crate::services::types::{
    memory_key, now_ms, Connection, Memory, MemoryFilters, MemoryType, StoredMemory,
    StoredMetadata,
};

// serde_json is used directly in create() to pass the serialized bytes to
// store_memory_core, which combines KV + timestamp + tag writes in one WAL lock.
use serde_json;

pub struct MemoryManager {
    repo: Arc<MemoryRepository>,
    embedding: Arc<EmbeddingService>,
    discovery_tx: mpsc::Sender<DiscoveryTask>,
}

pub struct CreateOpts {
    pub memory_type: MemoryType,
    pub tags: Vec<String>,
    pub importance: f32,
    pub emotional_valence: f32,
    pub arousal: f32,
    pub health: Option<f32>,
    pub ttl: Option<u64>,
    pub source: Option<String>,
}

impl Default for CreateOpts {
    fn default() -> Self {
        Self {
            memory_type: MemoryType::ShortTerm,
            tags: Vec::new(),
            importance: 0.5,
            emotional_valence: 0.0,
            arousal: 0.0,
            health: None,
            ttl: Some(3600),
            source: None,
        }
    }
}

pub struct UpdatePatch {
    pub content: Option<String>,
    pub tags: Option<Vec<String>>,
    pub importance: Option<f32>,
    pub emotional_valence: Option<f32>,
    pub arousal: Option<f32>,
    pub health: Option<f32>,
    pub source: Option<String>,
}

impl MemoryManager {
    pub fn new(
        repo: Arc<MemoryRepository>,
        embedding: Arc<EmbeddingService>,
        discovery_tx: mpsc::Sender<DiscoveryTask>,
    ) -> Self {
        Self { repo, embedding, discovery_tx }
    }

    pub async fn create(&self, content: &str, opts: CreateOpts) -> Result<(Memory, Vec<f32>)> {
        let id = Uuid::new_v4();
        let now = now_ms();
        const FLASHBULB_AROUSAL_THRESHOLD: f32 = 0.8;
        const FLASHBULB_PROTECTION_MS: u64 = 30 * 86_400_000;

        let arousal = opts.arousal.clamp(0.0, 1.0);
        let emotional_valence = opts.emotional_valence.clamp(-1.0, 1.0);
        let is_flashbulb = arousal >= FLASHBULB_AROUSAL_THRESHOLD;
        let memory_type = if is_flashbulb {
            MemoryType::LongTerm
        } else {
            opts.memory_type.clone()
        };
        let flashbulb_until = is_flashbulb.then_some(now + FLASHBULB_PROTECTION_MS);

        let stored = StoredMemory {
            id,
            content: content.to_owned(),
            memory_type: memory_type.clone(),
            metadata: StoredMetadata {
                created_at: now,
                updated_at: now,
                accessed_at: now,
                access_count: 0,
                source: opts.source,
                tags: opts.tags.clone(),
                importance: if is_flashbulb {
                    opts.importance.clamp(0.0, 1.0).max(0.9)
                } else {
                    opts.importance.clamp(0.0, 1.0)
                },
                emotional_valence,
                arousal,
                health: opts.health.unwrap_or(100.0).clamp(0.0, 100.0),
                last_recalled_at: None,
                flashbulb_until,
                ttl: match memory_type {
                    MemoryType::ShortTerm => opts.ttl.or(Some(3600)),
                    MemoryType::LongTerm => None,
                },
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: false,
        };

        let embedding = self.embedding.embed(content).await?;
        let json = serde_json::to_vec(&stored)?;
        let key = memory_key(id);

        let mut index_tags = opts.tags.clone();
        index_tags.push(format!("__type:{}", memory_type));
        if is_flashbulb {
            index_tags.push("__flashbulb__".to_owned());
        }

        self.repo
            .engine
            .store_memory_core(
                key,
                json,
                Some(embedding.clone()),
                now,
                &index_tags,
            )
            .await?;

        Ok((stored.into_api(Vec::new()), embedding))
    }

    pub async fn get(&self, id: Uuid) -> Result<Memory> {
        let _guard = self.repo.lock(id).await;

        let mut stored = self
            .repo
            .load(id)
            .await?
            .ok_or(AppError::NotFound(id))?;

        if stored.archived {
            return Err(AppError::NotFound(id));
        }

        // Update access metadata
        let now = now_ms();
        stored.metadata.accessed_at = now;
        stored.metadata.access_count += 1;
        stored.metadata.last_recalled_at = Some(now);
        stored.metadata.health = (stored.metadata.health + 10.0).min(100.0);
        self.repo.store(&stored).await?;

        Ok(stored.into_api(Vec::new()))
    }

    pub async fn update(&self, id: Uuid, patch: UpdatePatch) -> Result<Memory> {
        let _guard = self.repo.lock(id).await;

        let mut stored = self
            .repo
            .load(id)
            .await?
            .ok_or(AppError::NotFound(id))?;

        if stored.archived {
            return Err(AppError::NotFound(id));
        }

        let content_changed = patch.content.is_some();
        let tags_changed = patch.tags.is_some();
        if let Some(c) = patch.content {
            stored.content = c;
        }
        if let Some(t) = patch.tags {
            stored.metadata.tags = t;
        }
        if let Some(i) = patch.importance {
            stored.metadata.importance = i.clamp(0.0, 1.0);
        }
        if let Some(v) = patch.emotional_valence {
            stored.metadata.emotional_valence = v.clamp(-1.0, 1.0);
        }
        if let Some(a) = patch.arousal {
            stored.metadata.arousal = a.clamp(0.0, 1.0);
        }
        if let Some(h) = patch.health {
            stored.metadata.health = h.clamp(0.0, 100.0);
        }
        if let Some(s) = patch.source {
            stored.metadata.source = Some(s);
        }
        stored.metadata.updated_at = now_ms();

        if content_changed {
            let embedding = self.embedding.embed(&stored.content).await?;
            self.repo.store_with_embedding(&stored, embedding.clone()).await?;

            // Remove stale connections — new ones will be discovered asynchronously.
            let key = memory_key(id);
            let _ = self.repo.engine.remove_all_edges(key.as_bytes());
            let _ = self.discovery_tx.try_send(DiscoveryTask {
                memory_id: id,
                embedding,
                threshold: 0.7,
                top_k: 5,
            });
        } else {
            self.repo.store(&stored).await?;
        }

        // Replace the inverted index entries when tags change, so stale tags are removed.
        if tags_changed {
            let key = memory_key(id);
            let mut index_tags = stored.metadata.tags.clone();
            index_tags.push(format!("__type:{}", stored.memory_type));
            self.repo.engine.set_tags(key, &index_tags)?;
        }

        Ok(stored.into_api(Vec::new()))
    }

    pub async fn delete(&self, id: Uuid, hard: bool) -> Result<()> {
        let _guard = self.repo.lock(id).await;

        if hard {
            self.repo.delete(id).await?;
        } else {
            let mut stored = self
                .repo
                .load(id)
                .await?
                .ok_or(AppError::NotFound(id))?;
            stored.archived = true;
            stored.metadata.updated_at = now_ms();
            self.repo.store(&stored).await?;
            // Keep timestamp/tag index entries so cleanup_archived can find the
            // archived record later; user-facing reads filter `archived=true`.
            let key = memory_key(id);
            self.repo.engine.add_tags(key, &["__archived__".to_owned()])?;
        }
        Ok(())
    }

    /// List memories ordered by creation time (newest first), with optional filters.
    pub async fn list(
        &self,
        filters: &MemoryFilters,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<Memory>, usize)> {
        // Fetch all BTree entries — no limit, so older entries (e.g. short-term memories
        // with earlier timestamps) are never silently truncated. time_series_count can
        // temporarily lag behind inserts, making the old time_latest(count+64) pattern
        // miss entries that sit at the tail of the newest-first sort order.
        let entries = self.repo.engine.time_range_query(0, u64::MAX, None)?;

        let mut memories: Vec<Memory> = Vec::new();
        let mut total = 0usize;
        let mut seen = 0usize;

        for (_ts, key_bytes) in entries {
            let key = key_bytes.as_ref();
            let Some(stored) = self.repo.load_by_key(key).await? else {
                continue;
            };
            if stored.archived {
                continue;
            }
            if !matches_filters(&stored, filters) {
                continue;
            }

            total += 1;
            seen += 1;
            if seen <= offset {
                continue;
            }
            if memories.len() < limit {
                memories.push(stored.into_api(Vec::new()));
            }
        }

        Ok((memories, total))
    }

    /// Load a StoredMemory without updating access counts (for internal use).
    #[allow(dead_code)]
    pub async fn load_stored(&self, id: Uuid) -> Result<StoredMemory> {
        self.repo.load(id).await?.ok_or(AppError::NotFound(id))
    }

    /// Persist a StoredMemory back (without re-indexing tags/timestamps).
    #[allow(dead_code)]
    pub async fn save_stored(&self, stored: &StoredMemory) -> Result<()> {
        self.repo.store(stored).await
    }

    pub async fn fetch_connections(&self, id: Uuid) -> Result<Vec<Connection>> {
        let key = memory_key(id);
        let neighbors = self.repo.engine.get_neighbors(key.as_bytes())?;

        let mut connections = Vec::new();
        for (target_bytes, edge_type_str, weight) in neighbors {
            let target_str = String::from_utf8_lossy(&target_bytes);
            // Key format is "memory:{uuid}"
            if let Some(uuid_str) = target_str.strip_prefix("memory:") {
                if let Ok(target_id) = uuid_str.parse::<Uuid>() {
                    if let Ok(rel) =
                        crate::services::types::RelationshipType::try_from(edge_type_str.as_str())
                    {
                        connections.push(Connection {
                            target_id,
                            relationship_type: rel,
                            strength: weight,
                            created_at: crate::services::types::ms_to_dt(now_ms()),
                        });
                    }
                }
            }
        }
        Ok(connections)
    }
}

/// Public for use by other service modules.
pub(crate) fn matches_filters_pub(stored: &StoredMemory, filters: &MemoryFilters) -> bool {
    matches_filters(stored, filters)
}

fn matches_filters(stored: &StoredMemory, filters: &MemoryFilters) -> bool {
    if let Some(ref mt) = filters.memory_type {
        if &stored.memory_type != mt {
            return false;
        }
    }
    if let Some(min) = filters.min_importance {
        if stored.metadata.importance < min {
            return false;
        }
    }
    if let Some(max) = filters.max_importance {
        if stored.metadata.importance > max {
            return false;
        }
    }
    if let Some(after) = filters.created_after {
        if stored.metadata.created_at < after {
            return false;
        }
    }
    if let Some(before) = filters.created_before {
        if stored.metadata.created_at > before {
            return false;
        }
    }
    if !filters.tags.is_empty() {
        let tag_set: std::collections::HashSet<&str> =
            stored.metadata.tags.iter().map(|s| s.as_str()).collect();
        if !filters.tags.iter().all(|t| tag_set.contains(t.as_str())) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::types::{MemoryType, StoredMemory, StoredMetadata};

    fn make_stored(
        memory_type: MemoryType,
        importance: f32,
        tags: Vec<String>,
        created_at: u64,
    ) -> StoredMemory {
        StoredMemory {
            id: uuid::Uuid::new_v4(),
            content: "test content".into(),
            memory_type,
            metadata: StoredMetadata {
                created_at,
                updated_at: created_at,
                accessed_at: created_at,
                access_count: 0,
                source: None,
                tags,
                importance,
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
        }
    }

    fn no_filters() -> MemoryFilters {
        MemoryFilters::default()
    }

    #[test]
    fn no_filters_matches_everything() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 1000);
        assert!(matches_filters_pub(&m, &no_filters()));

        let m2 = make_stored(MemoryType::LongTerm, 0.9, vec!["rust".into()], 9999);
        assert!(matches_filters_pub(&m2, &no_filters()));
    }

    #[test]
    fn memory_type_filter_match() {
        let m = make_stored(MemoryType::LongTerm, 0.5, vec![], 1000);
        let f = MemoryFilters { memory_type: Some(MemoryType::LongTerm), ..Default::default() };
        assert!(matches_filters_pub(&m, &f));
    }

    #[test]
    fn memory_type_filter_no_match() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 1000);
        let f = MemoryFilters { memory_type: Some(MemoryType::LongTerm), ..Default::default() };
        assert!(!matches_filters_pub(&m, &f));
    }

    #[test]
    fn min_importance_boundary() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 1000);

        let pass = MemoryFilters { min_importance: Some(0.5), ..Default::default() };
        assert!(matches_filters_pub(&m, &pass));

        let fail = MemoryFilters { min_importance: Some(0.51), ..Default::default() };
        assert!(!matches_filters_pub(&m, &fail));
    }

    #[test]
    fn max_importance_boundary() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 1000);

        let pass = MemoryFilters { max_importance: Some(0.5), ..Default::default() };
        assert!(matches_filters_pub(&m, &pass));

        let fail = MemoryFilters { max_importance: Some(0.49), ..Default::default() };
        assert!(!matches_filters_pub(&m, &fail));
    }

    #[test]
    fn importance_range_filter() {
        let m = make_stored(MemoryType::ShortTerm, 0.6, vec![], 1000);
        let f = MemoryFilters {
            min_importance: Some(0.5),
            max_importance: Some(0.7),
            ..Default::default()
        };
        assert!(matches_filters_pub(&m, &f));

        let low = make_stored(MemoryType::ShortTerm, 0.4, vec![], 1000);
        assert!(!matches_filters_pub(&low, &f));

        let high = make_stored(MemoryType::ShortTerm, 0.8, vec![], 1000);
        assert!(!matches_filters_pub(&high, &f));
    }

    #[test]
    fn created_after_filter() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 2000);

        let pass = MemoryFilters { created_after: Some(1000), ..Default::default() };
        assert!(matches_filters_pub(&m, &pass));

        let fail = MemoryFilters { created_after: Some(3000), ..Default::default() };
        assert!(!matches_filters_pub(&m, &fail));
    }

    #[test]
    fn created_before_filter() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 2000);

        let pass = MemoryFilters { created_before: Some(3000), ..Default::default() };
        assert!(matches_filters_pub(&m, &pass));

        let fail = MemoryFilters { created_before: Some(1000), ..Default::default() };
        assert!(!matches_filters_pub(&m, &fail));
    }

    #[test]
    fn tags_single_match() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec!["rust".into(), "test".into()], 1000);

        let pass = MemoryFilters { tags: vec!["rust".into()], ..Default::default() };
        assert!(matches_filters_pub(&m, &pass));

        let fail = MemoryFilters { tags: vec!["python".into()], ..Default::default() };
        assert!(!matches_filters_pub(&m, &fail));
    }

    #[test]
    fn tags_all_must_match() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec!["rust".into(), "test".into()], 1000);

        // Both present → passes
        let both = MemoryFilters {
            tags: vec!["rust".into(), "test".into()],
            ..Default::default()
        };
        assert!(matches_filters_pub(&m, &both));

        // One present, one missing → fails
        let partial = MemoryFilters {
            tags: vec!["rust".into(), "python".into()],
            ..Default::default()
        };
        assert!(!matches_filters_pub(&m, &partial));
    }

    #[test]
    fn empty_tags_filter_matches_all() {
        let m = make_stored(MemoryType::ShortTerm, 0.5, vec![], 1000);
        let f = MemoryFilters { tags: vec![], ..Default::default() };
        assert!(matches_filters_pub(&m, &f));
    }

    #[test]
    fn combined_filters_all_conditions() {
        let m = make_stored(
            MemoryType::LongTerm,
            0.8,
            vec!["important".into()],
            5000,
        );

        let pass = MemoryFilters {
            memory_type: Some(MemoryType::LongTerm),
            min_importance: Some(0.7),
            max_importance: Some(0.9),
            tags: vec!["important".into()],
            created_after: Some(1000),
            created_before: Some(9000),
        };
        assert!(matches_filters_pub(&m, &pass));

        // Flip one condition — wrong type
        let fail = MemoryFilters {
            memory_type: Some(MemoryType::ShortTerm),
            ..pass.clone()
        };
        assert!(!matches_filters_pub(&m, &fail));
    }
}
