use bytes::Bytes;
use std::sync::Arc;
use uuid::Uuid;

use crate::engine::storage::partition::{PartitionBinding, PartitionScope};
use crate::error::{AppError, Result};
use crate::services::cursor::{ConnectionCursor, ListCursor};
use crate::services::repository::MemoryRepository;
use crate::services::types::{
    distance_to_score, memory_key, ms_to_dt, now_ms, parse_memory_id, Connection, Memory,
    RelationshipType,
};

pub struct ConnectionManager {
    repo: Arc<MemoryRepository>,
}

/// One page of connections, and what the walk learned about the rest.
pub struct ConnectionPage {
    pub connections: Vec<(Uuid, Connection)>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub truncated: bool,
}

/// The key immediately before `key` in byte order.
///
/// Used to make a resume position inclusive of the record it names: the
/// ordered walk resumes *after* the position it is given, and a connection
/// cursor needs the walk to revisit its own source to reach the edges it had
/// not yet returned.
fn previous_key(key: Bytes) -> Bytes {
    let mut bytes = key.to_vec();
    match bytes.last_mut() {
        // Trimming the last byte yields a strictly smaller key with no
        // shorter key between them, which is what "immediately before" needs
        // to mean here.
        Some(0) => {
            bytes.pop();
        }
        Some(last) => *last -= 1,
        None => {}
    }
    Bytes::from(bytes)
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
            return Err(AppError::Validation(
                "source_id and target_id must differ".into(),
            ));
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

        let src_key = self.repo.physical_memory_key(source_id)?;
        let dst_key = self.repo.physical_memory_key(target_id)?;

        let created_at_ms = now_ms();
        self.repo
            .engine
            .add_edge(
                src_key,
                dst_key,
                Some(rel.to_string()),
                Some(strength.clamp(0.0, 1.0)),
                created_at_ms,
            )
            .await?;

        Ok(Connection {
            target_id,
            relationship_type: rel,
            strength: strength.clamp(0.0, 1.0),
            created_at: ms_to_dt(created_at_ms),
        })
    }

    pub async fn delete(&self, source_id: Uuid, target_id: Uuid) -> Result<()> {
        let src_key = self.repo.physical_memory_key(source_id)?;
        let dst_key = self.repo.physical_memory_key(target_id)?;
        self.repo.engine.remove_edge(src_key, dst_key).await?;
        Ok(())
    }

    /// Create SimilarTo connections for the given memory using a pre-computed embedding.
    ///
    /// The embedding must be the same vector that was stored for `id` — callers
    /// must pass the embedding returned by `memory_manager.create()` rather than
    /// re-computing it here.
    pub async fn auto_discover_in_scope(
        &self,
        key: Bytes,
        embedding: &[f32],
        threshold: f32,
        top_k: usize,
        read_scope: &PartitionScope,
    ) -> Result<Vec<Connection>> {
        // Search for similar memories (fetch top_k+1 to exclude self)
        let raw = self
            .repo
            .engine
            .vector_search_partitioned(read_scope, embedding, top_k + 1, None)
            .await?;

        // Collect candidates first — no WAL writes yet
        let mut candidates: Vec<(Uuid, Bytes, f32)> = Vec::new();
        for item in raw {
            if item.key == key {
                continue;
            }
            let score = distance_to_score(item.distance);
            if score < threshold {
                continue;
            }
            if let Some(target_id) = parse_memory_id(item.key.as_ref()) {
                candidates.push((target_id, item.key, score.clamp(0.0, 1.0)));
                if candidates.len() >= top_k {
                    break;
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
            .get_neighbors_partitioned(read_scope, key.as_ref())
            .unwrap_or_default()
            .into_iter()
            .map(|(target_key, _, _, _)| target_key)
            .collect();

        let candidates: Vec<(Uuid, Bytes, f32)> = candidates
            .into_iter()
            .filter(|(_, target_key, _)| !existing_targets.contains(target_key))
            .collect();

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // Single WAL write for all edges
        let src_key_bytes = key.clone();
        let edges: Vec<(Bytes, Bytes, Option<String>, Option<f32>)> = candidates
            .iter()
            .map(|(_, target_key, score)| -> Result<_> {
                Ok((
                    src_key_bytes.clone(),
                    target_key.clone(),
                    Some(RelationshipType::SimilarTo.to_string()),
                    Some(*score),
                ))
            })
            .collect::<Result<_>>()?;

        let created_at_ms = now_ms();
        self.repo
            .engine
            .add_edges_batch(edges, created_at_ms)
            .await?;

        let now = ms_to_dt(created_at_ms);
        Ok(candidates
            .into_iter()
            .map(|(target_id, _, score)| Connection {
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
        // See `MemoryManager::fetch_connections`: the traversal start must be
        // the record's real partition, not this repository's write target.
        let Some(binding) = self.repo.resolve_binding(id).await? else {
            return Ok(Vec::new());
        };
        let logical_key = memory_key(id);
        let key = self
            .repo
            .physical_key_in(&binding, logical_key.as_bytes())?;
        let type_strings: Option<Vec<String>> = if types.is_empty() {
            None
        } else {
            Some(types.iter().map(|r| r.to_string()).collect())
        };

        let traversal = self.repo.engine.traverse_graph_partitioned(
            self.repo.read_scope(),
            key.as_ref(),
            depth,
            type_strings.as_deref(),
        )?;

        let mut results = Vec::new();
        for node in traversal {
            if node.node_id == key {
                continue; // Skip start node
            }
            let Some(stored) = self.repo.load_by_key(node.node_id.as_ref()).await? else {
                continue;
            };
            if stored.archived {
                continue;
            }

            let (rel, strength, edge_ts) = if let Some(meta) = node.edge_metadata {
                let rel = RelationshipType::try_from(meta.edge_type.as_str())
                    .unwrap_or(RelationshipType::RelatedTo);
                (rel, meta.weight, meta.timestamp)
            } else {
                (RelationshipType::RelatedTo, 1.0, now_ms())
            };

            let conn = Connection {
                target_id: stored.id,
                relationship_type: rel,
                strength,
                created_at: ms_to_dt(edge_ts),
            };
            let memory = stored.into_api(Vec::new());
            results.push((memory, conn));
        }

        Ok(results)
    }

    /// One bounded page of the connections in scope.
    ///
    /// Connections are walked in the order of the memories they start from,
    /// which is the only stable order available: an edge has no identity of
    /// its own, and the graph makes no promise about the order it returns a
    /// memory's neighbours in across compaction. Within one source the edges
    /// are ordered by their target, so a boundary landing part-way through a
    /// well-connected memory can be resumed exactly.
    ///
    /// No total. Counting every connection in scope means assembling every
    /// connection in scope, which is the cost this exists not to pay -- and
    /// on a corpus that concurrent writes are changing, a total taken at one
    /// instant describes nothing the caller can rely on.
    ///
    /// Both ends are checked for archived state from their attribute rows.
    /// The previous form read and deserialized both memories in full to look
    /// at one boolean each, twice per edge.
    pub async fn list_page(
        &self,
        limit: usize,
        start: Option<ConnectionCursor>,
        effort_factor: usize,
    ) -> Result<ConnectionPage> {
        use crate::engine::attr::select::{AttrPred, OrderedSelect};
        use crate::engine::attr::value::AttrValue;
        use crate::services::attrs::{SLOT_ARCHIVED, SLOT_CREATED_AT};

        let mut page = ConnectionPage {
            connections: Vec::with_capacity(limit.min(1024)),
            next_cursor: None,
            has_more: true,
            truncated: false,
        };
        if limit == 0 {
            return Ok(page);
        }

        // Sources that are archived are excluded by the walk itself, so an
        // archived memory's edges cost nothing to skip.
        let preds = [AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false))];
        let mut after = match &start {
            Some(cursor) => Some(self.source_position(cursor.source).await?),
            None => None,
        };
        // Only meaningful for the first source visited: the cursor names a
        // point inside that source's edges, and every later source starts
        // from its first edge.
        let mut resume_after_target = start.map(|c| c.target);

        let effort = limit.saturating_mul(effort_factor).max(limit);
        let mut examined = 0usize;

        while page.connections.len() < limit {
            let batch = self
                .repo
                .engine
                .select_page_partitioned(
                    self.repo.read_scope(),
                    OrderedSelect {
                        preds: &preds,
                        order_slot: SLOT_CREATED_AT,
                        after: after.clone(),
                        descending: false,
                        limit: limit.saturating_sub(page.connections.len()).max(1),
                        effort: effort.saturating_sub(examined).max(1),
                    },
                )
                .await?;

            let exhausted = batch.exhausted;
            let stopped_early = batch.truncated;
            after = batch.next.clone();
            examined += batch.examined;

            for source_key in batch.keys {
                let Some(source_id) = parse_memory_id(source_key.as_ref()) else {
                    continue;
                };
                let Some(source_order) = self.order_of(source_key.as_ref()).await? else {
                    continue;
                };

                let mut neighbors = self
                    .repo
                    .engine
                    .get_neighbors_partitioned(self.repo.read_scope(), source_key.as_ref())?;
                // The graph returns neighbours in whatever order it holds
                // them, which compaction may change. Sorting by target key
                // gives the walk an order a boundary can be expressed in.
                neighbors.sort_by(|a, b| a.0.cmp(&b.0));

                for (target_key, rel_type, strength, edge_ts) in neighbors {
                    let Some(target_id) = parse_memory_id(target_key.as_ref()) else {
                        continue;
                    };
                    if let Some(boundary) = resume_after_target {
                        if target_id <= boundary {
                            continue;
                        }
                    }
                    if self.is_archived(target_key.as_ref()).await? {
                        continue;
                    }

                    let relationship_type = RelationshipType::try_from(rel_type.as_str())
                        .unwrap_or(RelationshipType::RelatedTo);
                    page.connections.push((
                        source_id,
                        Connection {
                            target_id,
                            relationship_type,
                            strength,
                            created_at: ms_to_dt(edge_ts),
                        },
                    ));
                    if page.connections.len() == limit {
                        page.next_cursor = Some(
                            ConnectionCursor::new(
                                ListCursor::new(source_order, source_id),
                                target_id,
                            )
                            .encode(),
                        );
                        return Ok(page);
                    }
                }

                // Past the first source, there is no partial edge list to
                // resume into.
                resume_after_target = None;
            }

            if exhausted {
                page.has_more = false;
                break;
            }
            if stopped_early || examined >= effort {
                page.truncated = true;
                break;
            }
        }

        if !page.has_more {
            page.next_cursor = None;
        }
        Ok(page)
    }

    /// Whether the record at `key` is archived, read from its attribute row.
    ///
    /// One row read rather than a payload read and a JSON parse, for one
    /// boolean -- and this runs twice per edge.
    async fn is_archived(&self, key: &[u8]) -> Result<bool> {
        use crate::engine::attr::value::AttrValue;
        use crate::services::attrs::SLOT_ARCHIVED;

        let Some(row) = self.repo.engine.get_attrs(key).await? else {
            // No row means no record: treat it as gone rather than as live,
            // so a dangling edge does not surface a connection to nothing.
            return Ok(true);
        };
        Ok(matches!(
            row.get(SLOT_ARCHIVED),
            Some(AttrValue::Bool(true)) | None
        ))
    }

    /// The ordering value a source memory sits at, for building a cursor.
    async fn order_of(&self, key: &[u8]) -> Result<Option<u64>> {
        use crate::services::attrs::SLOT_CREATED_AT;

        Ok(self
            .repo
            .engine
            .get_attrs(key)
            .await?
            .and_then(|row| row.get(SLOT_CREATED_AT))
            .map(|v| v.order_key()))
    }

    /// Rebuild the walk position a listing cursor names, inside this
    /// repository's own read scope -- see `MemoryManager::list_page`.
    async fn source_position(
        &self,
        cursor: ListCursor,
    ) -> Result<crate::engine::index::IndexPosition> {
        let binding = match self.repo.resolve_binding(cursor.id).await? {
            Some(binding) => binding,
            None => self.repo.write_target().clone(),
        };
        let key = memory_key(cursor.id);
        let physical = self.repo.physical_key_in(&binding, key.as_bytes())?;
        // Inclusive of the source itself: the cursor names a point *inside*
        // this source's edges, so the walk has to visit it again to reach the
        // rest of them. `resume_after_target` is what stops the edges already
        // returned from coming back.
        Ok((cursor.order, previous_key(physical)))
    }
}

/// A pending auto-discovery job submitted via the background channel.
#[derive(Debug)]
pub struct DiscoveryTask {
    pub memory_id: Uuid,
    pub source_key: Bytes,
    pub embedding: Vec<f32>,
    pub threshold: f32,
    pub top_k: usize,
    pub read_scope: PartitionScope,
    pub write_target: PartitionBinding,
}

impl DiscoveryTask {
    pub fn new(
        repo: &MemoryRepository,
        memory_id: Uuid,
        embedding: Vec<f32>,
        threshold: f32,
        top_k: usize,
    ) -> Result<Self> {
        Ok(Self {
            memory_id,
            source_key: repo.physical_memory_key(memory_id)?,
            embedding,
            threshold,
            top_k,
            read_scope: repo.read_scope().clone(),
            write_target: repo.write_target().clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ensures auto_discover_in_scope accepts &[f32] (compile-time check).
    // The future is deliberately never awaited — only its type is asserted. It
    // is bound to a name rather than `_` so it is not dropped mid-statement,
    // which is also what keeps `clippy::let_underscore_future` quiet.
    fn _assert_auto_discover_in_scope_takes_embedding_slice(_mgr: &ConnectionManager) {
        let scope = crate::engine::storage::partition::PartitionScope::legacy_default();
        let _fut: std::pin::Pin<Box<dyn std::future::Future<Output = _>>> = Box::pin(
            _mgr.auto_discover_in_scope(Bytes::from("memory:demo"), &[0.0f32; 384], 0.7, 5, &scope),
        );
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
        let existing_targets: HashSet<Bytes> =
            [Bytes::from(memory_key(id_b))].into_iter().collect();

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

    use crate::services::types::{MemoryType, StoredMemory, StoredMetadata};

    async fn test_connection_manager() -> (Arc<MemoryRepository>, ConnectionManager) {
        let engine = Arc::new(
            crate::engine::storage::engine::StorageEngine::new(
                crate::engine::storage::engine::EngineConfig {
                    data_dir: tempfile::tempdir().unwrap().keep(),
                    sync_writes: false,
                    // The connection walk selects through the attribute
                    // store, so a fixture without a schema has no access
                    // path at all.
                    attr_schema: Some(crate::services::attrs::memory_schema()),
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
        let repo = Arc::new(MemoryRepository::new(Arc::clone(&engine)));
        let mgr = ConnectionManager::new(Arc::clone(&repo));
        (repo, mgr)
    }

    async fn partitioned_vector_engine() -> Arc<crate::engine::storage::StorageEngine> {
        Arc::new(
            crate::engine::storage::engine::StorageEngine::new(
                crate::engine::storage::engine::EngineConfig {
                    data_dir: tempfile::tempdir().unwrap().keep(),
                    sync_writes: false,
                    vector: crate::engine::storage::engine::VectorConfig {
                        enabled: true,
                        dimension: 4,
                        hnsw_m: 4,
                        hnsw_ef_construction: 10,
                        hnsw_ef_search: 4,
                        metric: crate::engine::util::DistanceMetric::L2,
                        hnsw_resident_budget_bytes: None,
                    },
                    // As in `test_connection_manager`: the connection walk
                    // has no access path without a schema.
                    attr_schema: Some(crate::services::attrs::memory_schema()),
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        )
    }

    async fn store_test_memory(repo: &MemoryRepository, content: &str) -> Uuid {
        let id = Uuid::new_v4();
        let created = now_ms();
        let mut stored = StoredMemory {
            id,
            content: content.into(),
            memory_type: MemoryType::ShortTerm,
            metadata: StoredMetadata {
                created_at: created,
                updated_at: created,
                accessed_at: created,
                access_count: 0,
                source: None,
                tags: vec![],
                importance: 0.5,
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
        repo.engine
            .add_timestamp(repo.physical_memory_key(id).unwrap(), created)
            .await
            .unwrap();
        id
    }

    async fn store_test_memory_with_embedding(
        repo: &MemoryRepository,
        content: &str,
        embedding: Vec<f32>,
    ) -> Uuid {
        let id = Uuid::new_v4();
        let created = now_ms();
        let mut stored = StoredMemory {
            id,
            content: content.into(),
            memory_type: MemoryType::ShortTerm,
            metadata: StoredMetadata {
                created_at: created,
                updated_at: created,
                accessed_at: created,
                access_count: 0,
                source: None,
                tags: vec![],
                importance: 0.5,
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
        repo.store_with_embedding(&mut stored, embedding)
            .await
            .unwrap();
        repo.engine
            .add_timestamp(repo.physical_memory_key(id).unwrap(), created)
            .await
            .unwrap();
        id
    }

    /// A multi-partition reader must be able to traverse from a memory in
    /// any authorized partition, not just the one it happens to write to.
    /// The start key used to be built from the write target unconditionally,
    /// so traversal from a memory living elsewhere in scope addressed a key
    /// that does not exist and reported "no connections" for a memory that
    /// has them. The existing REM-99 coverage misses this because it only
    /// ever traverses from a memory that sits in the write target.
    #[tokio::test]
    async fn traversal_starts_from_a_memory_outside_the_write_target() {
        use crate::engine::storage::partition::{PartitionId, TenantId};

        let engine = partitioned_vector_engine().await;
        let tenant = TenantId::new("acme").unwrap();
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let broad_scope = PartitionScope::new(
            tenant,
            [
                PartitionId::new("product").unwrap(),
                PartitionId::new("finance").unwrap(),
            ],
        )
        .unwrap();

        let finance_repo = MemoryRepository::with_partition_scope(
            Arc::clone(&engine),
            PartitionScope::single(finance.clone()),
            finance.clone(),
        );
        // Both records live in `finance`; the reader below writes to `product`.
        let source_id =
            store_test_memory_with_embedding(&finance_repo, "source", vec![1.0, 0.0, 0.0, 0.0])
                .await;
        let target_id =
            store_test_memory_with_embedding(&finance_repo, "target", vec![0.9, 0.1, 0.0, 0.0])
                .await;
        engine
            .add_edge(
                finance_repo.physical_memory_key(source_id).unwrap(),
                finance_repo.physical_memory_key(target_id).unwrap(),
                Some("related_to".to_string()),
                Some(0.9),
                now_ms(),
            )
            .await
            .unwrap();

        let broad_repo = Arc::new(MemoryRepository::with_partition_scope(
            engine,
            broad_scope,
            product,
        ));
        let mgr = ConnectionManager::new(broad_repo);

        let related = mgr.find_related(source_id, 1, &[]).await.unwrap();

        assert_eq!(related.len(), 1);
        assert_eq!(related[0].0.id, target_id);
    }

    #[tokio::test]
    async fn broad_auto_discovery_creates_cross_partition_edges_but_scoped_reads_hide_them() {
        use crate::engine::storage::partition::{PartitionId, TenantId};

        let engine = partitioned_vector_engine().await;
        let tenant = TenantId::new("acme").unwrap();
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let broad_scope = PartitionScope::new(
            tenant,
            [
                PartitionId::new("product").unwrap(),
                PartitionId::new("finance").unwrap(),
            ],
        )
        .unwrap();
        let product_repo = Arc::new(MemoryRepository::with_partition_scope(
            Arc::clone(&engine),
            PartitionScope::single(product.clone()),
            product,
        ));
        let finance_repo = MemoryRepository::with_partition_scope(
            Arc::clone(&engine),
            PartitionScope::single(finance.clone()),
            finance,
        );
        let broad_repo = Arc::new(MemoryRepository::with_partition_scope(
            Arc::clone(&engine),
            broad_scope.clone(),
            product_repo.write_target().clone(),
        ));

        let source_id =
            store_test_memory_with_embedding(&product_repo, "product", vec![1.0, 0.0, 0.0, 0.0])
                .await;
        let target_id =
            store_test_memory_with_embedding(&finance_repo, "finance", vec![0.95, 0.05, 0.0, 0.0])
                .await;
        let source_key = product_repo.physical_memory_key(source_id).unwrap();
        let broad_mgr = ConnectionManager::new(Arc::clone(&broad_repo));

        let created = broad_mgr
            .auto_discover_in_scope(source_key, &[1.0, 0.0, 0.0, 0.0], 0.5, 5, &broad_scope)
            .await
            .unwrap();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].target_id, target_id);

        let product_mgr = ConnectionManager::new(Arc::clone(&product_repo));
        assert!(
            product_mgr
                .find_related(source_id, 1, &[])
                .await
                .unwrap()
                .is_empty(),
            "product-only readers must not see the finance endpoint"
        );
        let broad_related = broad_mgr.find_related(source_id, 1, &[]).await.unwrap();
        assert_eq!(broad_related.len(), 1);
        assert_eq!(broad_related[0].0.id, target_id);

        let product_page = product_mgr.list_page(10, None, 32).await.unwrap();
        assert!(product_page.connections.is_empty());

        let broad_page = broad_mgr.list_page(10, None, 32).await.unwrap();
        assert_eq!(broad_page.connections.len(), 1);
        assert_eq!(broad_page.connections[0].0, source_id);
        assert_eq!(broad_page.connections[0].1.target_id, target_id);
    }

    #[tokio::test]
    async fn a_listed_connection_reports_real_edge_creation_time_not_read_time() {
        let (repo, mgr) = test_connection_manager().await;
        let a = store_test_memory(&repo, "a").await;
        let b = store_test_memory(&repo, "b").await;

        let before_create_ms = now_ms();
        mgr.create(a, b, RelationshipType::RelatedTo, 0.9)
            .await
            .unwrap();

        // Simulate time passing between edge creation and this read.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let page = mgr.list_page(10, None, 32).await.unwrap();
        let conn = page.connections.iter().find(|(src, _)| *src == a).unwrap();
        let reported_ms = conn.1.created_at.timestamp_millis() as u64;

        assert!(
            reported_ms >= before_create_ms && reported_ms < now_ms(),
            "created_at must be the edge's actual creation time, not now() at read time"
        );
    }

    /// One page of connections costs the page, not the graph.
    #[tokio::test]
    async fn a_connection_page_costs_the_page_not_the_graph() {
        let (repo, mgr) = test_connection_manager().await;
        let mut ids = Vec::new();
        for i in 0..40 {
            ids.push(store_test_memory(&repo, &format!("m{i}")).await);
        }
        // A well-connected graph: every memory points at the next three.
        for (i, source) in ids.iter().enumerate() {
            for offset in 1..=3 {
                let target = ids[(i + offset) % ids.len()];
                if *source != target {
                    mgr.create(*source, target, RelationshipType::RelatedTo, 0.5)
                        .await
                        .unwrap();
                }
            }
        }

        repo.engine.reset_payload_reads();
        repo.engine.reset_attr_reads();
        let page = mgr.list_page(5, None, 32).await.unwrap();

        assert_eq!(page.connections.len(), 5);
        assert!(page.has_more, "the graph holds far more than one page");
        assert_eq!(
            repo.engine.payload_reads(),
            0,
            "archived state comes from attribute rows; no memory is deserialized"
        );
        let reads = repo.engine.attr_reads();
        assert!(
            reads < 30,
            "a page of 5 must not settle the whole graph; read {reads} rows"
        );
    }

    /// The continuation contract, on a boundary that lands part-way through
    /// one memory's edges.
    #[tokio::test]
    async fn a_connection_sequence_returns_every_edge_exactly_once() {
        let (repo, mgr) = test_connection_manager().await;
        let mut ids = Vec::new();
        for i in 0..12 {
            ids.push(store_test_memory(&repo, &format!("m{i}")).await);
        }
        let mut expected = std::collections::HashSet::new();
        for (i, source) in ids.iter().enumerate() {
            for offset in 1..=3 {
                let target = ids[(i + offset) % ids.len()];
                if *source != target {
                    mgr.create(*source, target, RelationshipType::RelatedTo, 0.5)
                        .await
                        .unwrap();
                    expected.insert((*source, target));
                }
            }
        }

        // A page size that does not divide the per-source edge count, so a
        // boundary has to land inside one memory's edge list.
        let mut seen: Vec<(Uuid, Uuid)> = Vec::new();
        let mut start = None;
        loop {
            let page = mgr.list_page(2, start, 32).await.unwrap();
            assert!(!page.truncated, "nothing here should exhaust the bound");
            seen.extend(
                page.connections
                    .iter()
                    .map(|(src, conn)| (*src, conn.target_id)),
            );
            match page.next_cursor {
                Some(token) if page.has_more => {
                    start = Some(ConnectionCursor::decode(&token).unwrap());
                }
                _ => break,
            }
            assert!(seen.len() < 1_000, "paging failed to terminate");
        }

        let unique: std::collections::HashSet<(Uuid, Uuid)> = seen.iter().copied().collect();
        assert_eq!(seen.len(), unique.len(), "no edge is returned twice");
        assert_eq!(unique, expected, "every edge is returned exactly once");
    }

    /// An edge whose far end has been archived is not a connection anyone can
    /// follow, and must not spend a slot in the page.
    #[tokio::test]
    async fn a_connection_to_an_archived_memory_is_left_out() {
        let (repo, mgr) = test_connection_manager().await;
        let a = store_test_memory(&repo, "a").await;
        let live = store_test_memory(&repo, "live").await;
        let gone = store_test_memory(&repo, "gone").await;
        mgr.create(a, live, RelationshipType::RelatedTo, 0.5)
            .await
            .unwrap();
        mgr.create(a, gone, RelationshipType::RelatedTo, 0.5)
            .await
            .unwrap();

        let (binding, mut stored) = repo.load_bound(gone).await.unwrap().unwrap();
        stored.archived = true;
        repo.store_in(&binding, &mut stored).await.unwrap();

        let page = mgr.list_page(10, None, 32).await.unwrap();
        let targets: Vec<Uuid> = page.connections.iter().map(|(_, c)| c.target_id).collect();

        assert!(targets.contains(&live));
        assert!(
            !targets.contains(&gone),
            "an edge to an archived memory is not a connection"
        );
    }
}
