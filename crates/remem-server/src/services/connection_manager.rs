use bytes::Bytes;
use std::sync::Arc;
use uuid::Uuid;

use crate::error::{AppError, Result};
use crate::services::repository::MemoryRepository;
use crate::services::types::{
    distance_to_score, memory_key, ms_to_dt, now_ms, Connection, Memory, RelationshipType,
};

pub struct ConnectionManager {
    repo: Arc<MemoryRepository>,
}

impl ConnectionManager {
    pub fn new(repo: Arc<MemoryRepository>) -> Self {
        Self { repo }
    }

    pub async fn create(
        &self,
        source_id: Uuid,
        target_id: Uuid,
        rel: RelationshipType,
        strength: f32,
    ) -> Result<Connection> {
        if source_id == target_id {
            return Err(AppError::Validation("source_id and target_id must differ".into()));
        }

        let source = self
            .repo
            .load(source_id)
            .await?
            .ok_or(AppError::NotFound(source_id))?;
        if source.archived {
            return Err(AppError::NotFound(source_id));
        }

        let target = self
            .repo
            .load(target_id)
            .await?
            .ok_or(AppError::NotFound(target_id))?;
        if target.archived {
            return Err(AppError::NotFound(target_id));
        }

        let src_key = memory_key(source_id);
        let dst_key = memory_key(target_id);

        self.repo.engine.add_edge(
            src_key,
            dst_key,
            Some(rel.to_string()),
            Some(strength.clamp(0.0, 1.0)),
        )?;

        Ok(Connection {
            target_id,
            relationship_type: rel,
            strength: strength.clamp(0.0, 1.0),
            created_at: ms_to_dt(now_ms()),
        })
    }

    pub fn delete(&self, source_id: Uuid, target_id: Uuid) -> Result<()> {
        let src_key = memory_key(source_id);
        let dst_key = memory_key(target_id);
        self.repo.engine.remove_edge(src_key, dst_key)?;
        Ok(())
    }

    /// Create SimilarTo connections for the given memory using a pre-computed embedding.
    ///
    /// The embedding must be the same vector that was stored for `id` — callers
    /// must pass the embedding returned by `memory_manager.create()` rather than
    /// re-computing it here.
    pub async fn auto_discover(
        &self,
        id: Uuid,
        embedding: &[f32],
        threshold: f32,
        top_k: usize,
    ) -> Result<Vec<Connection>> {
        let key = memory_key(id);

        // Search for similar memories (fetch top_k+1 to exclude self)
        let raw = self.repo.engine.vector_search(embedding, top_k + 1).await?;

        // Collect candidates first — no WAL writes yet
        let mut candidates: Vec<(Uuid, f32)> = Vec::new();
        for item in raw {
            let candidate_key = String::from_utf8_lossy(&item.key);
            if candidate_key == key {
                continue;
            }
            let score = distance_to_score(item.distance);
            if score < threshold {
                continue;
            }
            if let Some(uuid_str) = candidate_key.strip_prefix("memory:") {
                if let Ok(target_id) = uuid_str.parse::<Uuid>() {
                    candidates.push((target_id, score.clamp(0.0, 1.0)));
                    if candidates.len() >= top_k {
                        break;
                    }
                }
            }
        }

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // Filter out pairs that are already connected to avoid duplicate edges.
        let existing_targets: std::collections::HashSet<Bytes> = self
            .repo
            .engine
            .get_neighbors(key.as_bytes())
            .unwrap_or_default()
            .into_iter()
            .map(|(target_key, _, _)| target_key)
            .collect();

        let candidates: Vec<(Uuid, f32)> = candidates
            .into_iter()
            .filter(|(tid, _)| {
                let k = Bytes::from(memory_key(*tid));
                !existing_targets.contains(&k)
            })
            .collect();

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // Single WAL write for all edges
        let src_key_bytes = Bytes::from(key.clone());
        let edges: Vec<(Bytes, Bytes, Option<String>, Option<f32>)> = candidates
            .iter()
            .map(|(tid, score)| {
                (
                    src_key_bytes.clone(),
                    Bytes::from(memory_key(*tid)),
                    Some(RelationshipType::SimilarTo.to_string()),
                    Some(*score),
                )
            })
            .collect();

        self.repo.engine.add_edges_batch(edges)?;

        let now = ms_to_dt(now_ms());
        Ok(candidates
            .into_iter()
            .map(|(target_id, score)| Connection {
                target_id,
                relationship_type: RelationshipType::SimilarTo,
                strength: score,
                created_at: now,
            })
            .collect())
    }

    /// Traverse the graph from `id` up to `depth` hops, filtered by relationship types.
    pub async fn find_related(
        &self,
        id: Uuid,
        depth: usize,
        types: &[RelationshipType],
    ) -> Result<Vec<(Memory, Connection)>> {
        let key = memory_key(id);
        let type_strings: Option<Vec<String>> = if types.is_empty() {
            None
        } else {
            Some(types.iter().map(|r| r.to_string()).collect())
        };

        let traversal = self
            .repo
            .engine
            .traverse_graph(key.as_bytes(), depth, type_strings.as_deref())?;

        let mut results = Vec::new();
        for node in traversal {
            let node_key = String::from_utf8_lossy(&node.node_id);
            if node_key == key {
                continue; // Skip start node
            }
            let Some(stored) = self.repo.load_by_key(node.node_id.as_ref()).await? else {
                continue;
            };
            if stored.archived {
                continue;
            }

            let (rel, strength) = if let Some(meta) = node.edge_metadata {
                let rel = RelationshipType::try_from(meta.edge_type.as_str())
                    .unwrap_or(RelationshipType::RelatedTo);
                (rel, meta.weight)
            } else {
                (RelationshipType::RelatedTo, 1.0)
            };

            let conn = Connection {
                target_id: stored.id,
                relationship_type: rel,
                strength,
                created_at: ms_to_dt(now_ms()),
            };
            let memory = stored.into_api(Vec::new());
            results.push((memory, conn));
        }

        Ok(results)
    }

    /// List connections in the graph with pagination (for the backoffice connections endpoint).
    ///
    /// Only connections where both source and target exist in KV (non-archived) are returned.
    /// Returns `(page_items, total_valid_edge_count)`.
    pub async fn list_all(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<(Uuid, Connection)>, usize)> {
        let entries = self.repo.engine.time_range_query(0, u64::MAX, None)?;

        let mut all: Vec<(Uuid, Connection)> = Vec::new();

        for (_ts, key_bytes) in entries {
            let key = key_bytes.as_ref();

            // Verify source exists in KV.
            let Ok(key_str) = std::str::from_utf8(key) else { continue };
            let Some(uuid_str) = key_str.strip_prefix("memory:") else { continue };
            let Ok(source_id) = Uuid::parse_str(uuid_str) else { continue };
            let Some(src_stored) = self.repo.load_by_key(key).await? else { continue };
            if src_stored.archived { continue; }

            let neighbors = self.repo.engine.get_neighbors(key)?;
            for (target_key, rel_type, strength) in neighbors {
                // Verify target exists in KV.
                let Some(tgt_stored) = self.repo.load_by_key(target_key.as_ref()).await? else {
                    continue;
                };
                if tgt_stored.archived { continue; }

                let target_str = String::from_utf8_lossy(&target_key);
                let Some(target_uuid_str) = target_str.strip_prefix("memory:") else { continue };
                let Ok(target_id) = Uuid::parse_str(target_uuid_str) else { continue };
                let relationship_type = RelationshipType::try_from(rel_type.as_str())
                    .unwrap_or(RelationshipType::RelatedTo);
                all.push((
                    source_id,
                    Connection {
                        target_id,
                        relationship_type,
                        strength,
                        created_at: ms_to_dt(now_ms()),
                    },
                ));
            }
        }

        let total = all.len();
        let page = all.into_iter().skip(offset).take(limit).collect();
        Ok((page, total))
    }
}

/// A pending auto-discovery job submitted via the background channel.
#[derive(Debug)]
pub struct DiscoveryTask {
    pub memory_id: Uuid,
    pub embedding: Vec<f32>,
    pub threshold: f32,
    pub top_k: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ensures auto_discover accepts &[f32] (compile-time check)
    fn _assert_auto_discover_takes_embedding_slice(_mgr: &ConnectionManager) {
        let _: std::pin::Pin<Box<dyn std::future::Future<Output = _>>> =
            Box::pin(_mgr.auto_discover(uuid::Uuid::nil(), &[0.0f32; 384], 0.7, 5));
    }

    // Ensures DiscoveryTask is Send (required for mpsc channel)
    fn _assert_discovery_task_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<DiscoveryTask>();
    }

    #[test]
    fn candidates_filtered_against_existing_neighbors() {
        use std::collections::HashSet;

        let id_b = Uuid::new_v4();
        let id_c = Uuid::new_v4();

        // Simulate: memory A already has edge to B.
        // auto_discover proposes [B, C]. After filtering, only C is new.
        let existing_targets: HashSet<Bytes> = [Bytes::from(memory_key(id_b))].into_iter().collect();

        let candidates: Vec<(Uuid, f32)> = vec![(id_b, 0.9f32), (id_c, 0.85f32)];

        let new_candidates: Vec<_> = candidates
            .into_iter()
            .filter(|(tid, _)| {
                let k = Bytes::from(memory_key(*tid));
                !existing_targets.contains(&k)
            })
            .collect();

        assert_eq!(new_candidates.len(), 1);
        assert_eq!(new_candidates[0].0, id_c);
    }
}
