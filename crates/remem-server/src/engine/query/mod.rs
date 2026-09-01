//! Query engine for the embedded storage engine
//!
//! Exposes a single access path: a [`HybridQuery`] fans out to whichever of the
//! vector, tag and graph indexes are enabled, and the resulting ranked lists are
//! fused by rank (RRF).
//!
//! An earlier version of this module carried standalone per-index query types, a
//! `Filter`/`Predicate` DSL applied to results *after* retrieval, several merge
//! strategies and a cost model — none of it reachable. It was removed rather
//! than finished (REM-72): predicates evaluated after retrieval cannot be pushed
//! into storage or across a network boundary, and the predicate language could
//! not express the attributes the product actually filters on. See
//! `docs/QUERY_LAYER_AND_SCALE_FOUNDATION.md` for the full reasoning, and REM-84
//! for the design intent that a future planner should be built on.

pub mod executor;
pub mod merge;
pub mod planner;
pub mod types;

pub use executor::QueryExecutor;
pub use planner::QueryPlanner;
pub use types::{BooleanMode, Evidence, FusedItem, HybridQuery, QueryResult, SourceKind};

use std::sync::Arc;

use crate::engine::error::Result;
use crate::engine::storage::partition::PartitionScope;
use crate::engine::storage::StorageEngine;

/// Configuration for the query engine
#[derive(Debug, Clone)]
pub struct QueryEngineConfig {
    /// RRF k parameter used when fusing results from multiple indexes
    pub rrf_k: usize,

    /// Multiple of a query's result target that bounds how far a step will
    /// widen when attribute predicates reject candidates (REM-78).
    ///
    /// The vector step opens at three times the target, so the default of 32
    /// is between four and five doublings — enough for a predicate that keeps
    /// roughly one candidate in thirty, which covers `memory_type` and
    /// ordinary importance and date ranges. Past that a query is asking for
    /// something the corpus barely holds, and reading the whole scope to
    /// prove it is the unbounded work this bound exists to prevent: the
    /// result comes back marked truncated instead.
    pub widen_max_factor: usize,
}

impl Default for QueryEngineConfig {
    fn default() -> Self {
        Self {
            rrf_k: 60,
            widen_max_factor: 32,
        }
    }
}

/// The main query engine that orchestrates query planning and execution
pub struct QueryEngine {
    /// Reference to the storage engine
    engine: Arc<StorageEngine>,

    /// Query planner
    planner: QueryPlanner,

    /// Query executor
    executor: QueryExecutor,
}

impl QueryEngine {
    /// Create a new query engine
    pub fn new(engine: Arc<StorageEngine>, config: QueryEngineConfig) -> Self {
        let planner = QueryPlanner::new(config.rrf_k, config.widen_max_factor);
        let executor = QueryExecutor::new(Arc::clone(&engine));

        Self {
            engine,
            planner,
            executor,
        }
    }

    /// Plan and execute a hybrid query
    #[allow(dead_code)]
    pub(crate) async fn execute(&self, query: HybridQuery) -> Result<QueryResult> {
        let plan = self.planner.plan(&query, &self.engine)?;
        self.executor.execute(plan).await
    }

    /// Plan and execute a query under an authorized partition scope.
    pub async fn execute_partitioned(
        &self,
        query: HybridQuery,
        scope: &PartitionScope,
    ) -> Result<QueryResult> {
        let plan = self.planner.plan(&query, &self.engine)?;
        self.executor.execute_partitioned(plan, scope).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default_rrf_k() {
        assert_eq!(QueryEngineConfig::default().rrf_k, 60);
    }

    use crate::engine::storage::engine::EngineConfig;
    use bytes::Bytes;
    use std::time::Duration;
    use tempfile::TempDir;

    async fn engine_with(dir: &TempDir) -> Arc<StorageEngine> {
        Arc::new(
            StorageEngine::new(EngineConfig {
                data_dir: dir.path().to_path_buf(),
                sync_writes: false,
                checkpoint_interval: Duration::from_secs(86400),
                vector: crate::engine::storage::engine::VectorConfig {
                    enabled: true,
                    dimension: 4,
                    hnsw_m: 4,
                    hnsw_ef_construction: 10,
                    hnsw_ef_search: 4,
                    metric: crate::engine::util::DistanceMetric::L2,
                    hnsw_resident_budget_bytes: None,
                },
                ..Default::default()
            })
            .await
            .unwrap(),
        )
    }

    async fn put_memory(engine: &StorageEngine, key: &str, content: &str, tags: &[&str], ts: u64) {
        let value = serde_json::json!({ "content": content, "tags": tags }).to_string();
        engine.put(key.to_string(), value).await.unwrap();
        engine.add_timestamp(key.to_string(), ts).await.unwrap();
        let owned: Vec<String> = tags.iter().map(|t| t.to_string()).collect();
        if !owned.is_empty() {
            engine.add_tags(key.to_string(), &owned).await.unwrap();
        }
    }

    #[tokio::test]
    async fn keyword_query_fuses_tag_hits_and_content_hits() {
        let dir = TempDir::new().unwrap();
        let engine = engine_with(&dir).await;

        // Tagged and matching in text — both sources should contribute.
        put_memory(&engine, "memory:a", "rust ownership", &["rust"], 1).await;
        // Text-only match: invisible to keyword search before REM-88 whenever
        // the tag index returned enough on its own.
        put_memory(&engine, "memory:b", "rust borrow checker", &[], 2).await;
        put_memory(&engine, "memory:c", "python decorators", &[], 3).await;

        let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
        let qr = qe
            .execute(HybridQuery::keyword(
                vec!["rust".to_string()],
                BooleanMode::Or,
                10,
            ))
            .await
            .unwrap();

        let keys: Vec<Bytes> = qr.items.iter().map(|i| i.key.clone()).collect();
        assert!(keys.contains(&Bytes::from("memory:a")));
        assert!(
            keys.contains(&Bytes::from("memory:b")),
            "a text-only match must surface"
        );
        assert!(!keys.contains(&Bytes::from("memory:c")));

        let a = qr.items.iter().find(|i| i.key == "memory:a").unwrap();
        let kinds: Vec<SourceKind> = a.sources.iter().map(|s| s.kind).collect();
        assert!(kinds.contains(&SourceKind::Tag));
        assert!(kinds.contains(&SourceKind::Content));

        let b = qr.items.iter().find(|i| i.key == "memory:b").unwrap();
        assert_eq!(
            b.sources.iter().map(|s| s.kind).collect::<Vec<_>>(),
            vec![SourceKind::Content]
        );
    }

    #[tokio::test]
    async fn keyword_query_ranks_a_two_source_hit_above_a_one_source_hit() {
        // RRF's defining property: an item found by several indexes outranks
        // one found by a single index. This replaces the old 0.5/0.5 blend.
        let dir = TempDir::new().unwrap();
        let engine = engine_with(&dir).await;

        // Timestamps are deliberately reversed relative to name order: both
        // records score 1.0 on the content scan (single-token query), and
        // content_scan's sort is stable over timestamp-ascending input, so
        // whichever record has the *lower* timestamp wins content-only rank.
        // Giving memory:text (the content-only hit) the lower timestamp means
        // content alone would rank it above memory:both — so memory:both can
        // only win the fused ranking below by virtue of its tag hit. Do not
        // "tidy" these back to match name order; that would let this test
        // pass even with cross-source fusion completely broken.
        put_memory(&engine, "memory:text", "rust borrow checker", &[], 1).await;
        put_memory(&engine, "memory:both", "rust ownership", &["rust"], 2).await;

        let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
        let qr = qe
            .execute(HybridQuery::keyword(
                vec!["rust".to_string()],
                BooleanMode::Or,
                10,
            ))
            .await
            .unwrap();

        assert_eq!(qr.items[0].key, Bytes::from("memory:both"));
    }

    async fn put_vector_memory(engine: &StorageEngine, key: &str, embedding: Vec<f32>) {
        let value = serde_json::json!({ "content": key, "tags": [] }).to_string();
        engine
            .put_with_embedding(key.to_string(), value, Some(embedding))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn partitioned_vector_query_searches_only_authorized_scope() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = TempDir::new().unwrap();
        let engine = engine_with(&dir).await;
        let tenant = TenantId::new("acme").unwrap();
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());

        let finance_key = engine
            .put_with_embedding_partitioned(
                &finance,
                b"memory:finance",
                serde_json::json!({ "content": "finance", "tags": [] }).to_string(),
                Some(vec![1.0, 0.0, 0.0, 0.0]),
            )
            .await
            .unwrap();
        let product_key = engine
            .put_with_embedding_partitioned(
                &product,
                b"memory:product",
                serde_json::json!({ "content": "product", "tags": [] }).to_string(),
                Some(vec![0.9, 0.1, 0.0, 0.0]),
            )
            .await
            .unwrap();

        let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
        let qr = qe
            .execute_partitioned(
                HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10),
                &PartitionScope::single(product),
            )
            .await
            .unwrap();

        let keys: Vec<Bytes> = qr.items.iter().map(|i| i.key.clone()).collect();
        assert_eq!(keys, vec![product_key]);
        assert!(!keys.contains(&finance_key));
    }

    #[tokio::test]
    async fn partitioned_keyword_query_searches_only_authorized_scope() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = TempDir::new().unwrap();
        let engine = engine_with(&dir).await;
        let tenant = TenantId::new("acme").unwrap();
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());

        let finance_key = engine
            .store_memory_core_partitioned(
                &finance,
                b"memory:finance",
                serde_json::json!({ "content": "rust finance", "tags": ["rust"] }).to_string(),
                None,
                1,
                &["rust".to_string()],
                None,
            )
            .await
            .unwrap();
        let product_key = engine
            .store_memory_core_partitioned(
                &product,
                b"memory:product",
                serde_json::json!({ "content": "rust product", "tags": ["rust"] }).to_string(),
                None,
                2,
                &["rust".to_string()],
                None,
            )
            .await
            .unwrap();

        let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
        let qr = qe
            .execute_partitioned(
                HybridQuery::keyword(vec!["rust".to_string()], BooleanMode::Or, 10),
                &PartitionScope::single(product),
            )
            .await
            .unwrap();

        let keys: Vec<Bytes> = qr.items.iter().map(|i| i.key.clone()).collect();
        assert_eq!(keys, vec![product_key]);
        assert!(!keys.contains(&finance_key));
    }

    #[tokio::test]
    async fn partitioned_hybrid_query_merges_only_authorized_partitions_deterministically() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = TempDir::new().unwrap();
        let engine = engine_with(&dir).await;
        let tenant = TenantId::new("acme").unwrap();
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        let tech = PartitionBinding::new(tenant.clone(), PartitionId::new("tech").unwrap());
        let tags = vec!["rust".to_string()];

        let finance_key = engine
            .store_memory_core_partitioned(
                &finance,
                b"memory:finance",
                serde_json::json!({ "content": "rust finance", "tags": ["rust"] }).to_string(),
                Some(vec![1.0, 0.0, 0.0, 0.0]),
                1,
                &tags,
                None,
            )
            .await
            .unwrap();
        let product_key = engine
            .store_memory_core_partitioned(
                &product,
                b"memory:product",
                serde_json::json!({ "content": "rust product", "tags": ["rust"] }).to_string(),
                Some(vec![0.9, 0.1, 0.0, 0.0]),
                2,
                &tags,
                None,
            )
            .await
            .unwrap();
        let tech_key = engine
            .store_memory_core_partitioned(
                &tech,
                b"memory:tech",
                serde_json::json!({ "content": "rust tech", "tags": ["rust"] }).to_string(),
                Some(vec![0.95, 0.05, 0.0, 0.0]),
                3,
                &tags,
                None,
            )
            .await
            .unwrap();
        let scope = PartitionScope::new(
            tenant,
            [
                PartitionId::new("finance").unwrap(),
                PartitionId::new("product").unwrap(),
            ],
        )
        .unwrap();
        let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
        let query = HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10)
            .with_tags(vec!["rust".to_string()], BooleanMode::Or);

        let first = qe.execute_partitioned(query.clone(), &scope).await.unwrap();
        let second = qe.execute_partitioned(query, &scope).await.unwrap();
        let first_keys: Vec<Bytes> = first.items.iter().map(|i| i.key.clone()).collect();
        let second_keys: Vec<Bytes> = second.items.iter().map(|i| i.key.clone()).collect();

        assert_eq!(first_keys, second_keys);
        assert!(first_keys.contains(&finance_key));
        assert!(first_keys.contains(&product_key));
        assert!(!first_keys.contains(&tech_key));
    }

    #[tokio::test]
    async fn partitioned_graph_context_search_hides_unauthorized_endpoints() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = TempDir::new().unwrap();
        let engine = engine_with(&dir).await;
        let tenant = TenantId::new("acme").unwrap();
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());

        let product_key = engine
            .store_memory_core_partitioned(
                &product,
                b"memory:product",
                serde_json::json!({ "content": "product", "tags": [] }).to_string(),
                None,
                1,
                &[],
                None,
            )
            .await
            .unwrap();
        let finance_key = engine
            .store_memory_core_partitioned(
                &finance,
                b"memory:finance",
                serde_json::json!({ "content": "finance", "tags": [] }).to_string(),
                None,
                2,
                &[],
                None,
            )
            .await
            .unwrap();
        engine
            .add_edge(
                product_key.clone(),
                finance_key.clone(),
                Some("related_to".to_string()),
                Some(0.9),
                3,
            )
            .await
            .unwrap();
        let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
        let graph_query = HybridQuery::new(vec![0.0, 0.0, 0.0, 0.0], 10)
            .with_graph_context(product_key.clone(), 1);

        let product_only = qe
            .execute_partitioned(graph_query.clone(), &PartitionScope::single(product))
            .await
            .unwrap();
        assert!(
            product_only.items.is_empty(),
            "product-only graph-context search must hide the finance endpoint"
        );

        let broad_scope = PartitionScope::new(
            tenant,
            [
                PartitionId::new("product").unwrap(),
                PartitionId::new("finance").unwrap(),
            ],
        )
        .unwrap();
        let broad = qe
            .execute_partitioned(graph_query, &broad_scope)
            .await
            .unwrap();
        assert_eq!(
            broad
                .items
                .iter()
                .map(|item| item.key.clone())
                .collect::<Vec<_>>(),
            vec![finance_key]
        );
    }

    #[tokio::test]
    async fn vector_search_drops_phantom_entries_left_by_deletes() {
        // HNSW cannot delete, so a removed memory stays in the vector index.
        // Before REM-88 only the semantic fast path filtered these out; now
        // every path does, because it happens in the executor.
        let dir = TempDir::new().unwrap();
        let engine = engine_with(&dir).await;

        put_vector_memory(&engine, "memory:a", vec![1.0, 0.0, 0.0, 0.0]).await;
        put_vector_memory(&engine, "memory:b", vec![0.9, 0.1, 0.0, 0.0]).await;
        put_vector_memory(&engine, "memory:c", vec![0.0, 1.0, 0.0, 0.0]).await;

        engine.delete("memory:b".to_string()).await.unwrap();

        let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
        let qr = qe
            .execute(HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10))
            .await
            .unwrap();

        let keys: Vec<Bytes> = qr.items.iter().map(|i| i.key.clone()).collect();
        assert!(
            !keys.contains(&Bytes::from("memory:b")),
            "a deleted memory must not surface as a phantom"
        );
        assert!(keys.contains(&Bytes::from("memory:a")));
        assert!(keys.contains(&Bytes::from("memory:c")));
    }

    #[tokio::test]
    async fn widening_terminates_against_partitions_that_were_never_resident() {
        // The widening loop bounds itself on the scoped node count. Once
        // partitions are materialized on demand, that figure has to come from
        // the catalog rather than from whatever happens to be loaded — if it
        // read zero for an unloaded partition, `k_actual` would be clamped to
        // 1 and retrieval would silently return almost nothing.
        let dir = TempDir::new().unwrap();
        let count_before;

        {
            let engine = engine_with(&dir).await;
            for i in 0..20 {
                let offset = i as f32 / 100.0;
                put_vector_memory(
                    &engine,
                    &format!("memory:{i}"),
                    vec![1.0 - offset, offset, 0.0, 0.0],
                )
                .await;
            }
            for i in 0..18 {
                engine.delete(format!("memory:{i}")).await.unwrap();
            }
            engine.checkpoint().await.unwrap();
            count_before = engine.vector_node_count();
            engine.graceful_shutdown().await.unwrap();
        }

        // Reopened: nothing is resident, so every figure the loop relies on has
        // to be answered from disk-derived accounting. `delete` leaves the
        // vector in the index as a phantom, so the figure covers all 20 — the
        // property under test is that it survives the restart, not its value.
        let engine = engine_with(&dir).await;
        assert_eq!(
            engine.vector_node_count(),
            count_before,
            "the widening bound changed across a restart: it is being derived \
             from what is loaded rather than from what exists"
        );
        assert!(count_before > 0, "fixture produced no vectors");

        let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
        let qr = qe
            .execute(HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 5))
            .await
            .unwrap();

        assert_eq!(qr.items.len(), 2, "only the live records survive");
        let keys: Vec<Bytes> = qr.items.iter().map(|i| i.key.clone()).collect();
        assert!(keys.contains(&Bytes::from("memory:18")));
        assert!(keys.contains(&Bytes::from("memory:19")));
    }

    #[tokio::test]
    async fn vector_search_widens_and_terminates_when_most_entries_are_phantoms() {
        // The test above never enters the doubling branch: k (10) already
        // exceeds the node count (3), so the loop exits on the first pass. This
        // one forces the retry path and pins its termination bound, which is
        // the only place an infinite loop could hide.
        let dir = TempDir::new().unwrap();
        let engine = engine_with(&dir).await;

        for i in 0..20 {
            let offset = i as f32 / 100.0;
            put_vector_memory(
                &engine,
                &format!("memory:{i}"),
                vec![1.0 - offset, offset, 0.0, 0.0],
            )
            .await;
        }
        // Leave only two live records, so the first pass at k = 5 comes up
        // short and the loop must widen.
        for i in 0..18 {
            engine.delete(format!("memory:{i}")).await.unwrap();
        }

        let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
        let qr = qe
            .execute(HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 5))
            .await
            .unwrap();

        assert_eq!(qr.items.len(), 2, "only the live records survive");
        let keys: Vec<Bytes> = qr.items.iter().map(|i| i.key.clone()).collect();
        assert!(keys.contains(&Bytes::from("memory:18")));
        assert!(keys.contains(&Bytes::from("memory:19")));
    }

    // ─── Filter pushdown (REM-78) ────────────────────────────────────────────
    //
    // These run without the ONNX model: the embeddings are synthetic, which is
    // all the vector index needs. That matters — the service-level and REST
    // tests for the same behaviour need the model and so are `#[ignore]`d,
    // which means CI never runs them. The core guarantee is pinned here
    // instead, where it always runs.
    mod filter_pushdown {
        use super::*;
        use crate::engine::attr::select::AttrPred;
        use crate::engine::attr::value::AttrValue;
        use crate::services::attrs::{
            memory_schema, project, SLOT_ARCHIVED, SLOT_IMPORTANCE, SLOT_MEMORY_TYPE,
        };
        use crate::services::types::{MemoryType, StoredMemory, StoredMetadata};

        async fn engine_with_attrs(dir: &TempDir) -> Arc<StorageEngine> {
            Arc::new(
                StorageEngine::new(EngineConfig {
                    data_dir: dir.path().to_path_buf(),
                    sync_writes: false,
                    checkpoint_interval: Duration::from_secs(86400),
                    vector: crate::engine::storage::engine::VectorConfig {
                        enabled: true,
                        dimension: 4,
                        hnsw_m: 8,
                        hnsw_ef_construction: 64,
                        hnsw_ef_search: 64,
                        metric: crate::engine::util::DistanceMetric::L2,
                        hnsw_resident_budget_bytes: None,
                    },
                    attr_schema: Some(memory_schema()),
                    ..Default::default()
                })
                .await
                .unwrap(),
            )
        }

        fn record(i: u32, long_term: bool, importance: f32, archived: bool) -> StoredMemory {
            StoredMemory {
                id: uuid::Uuid::from_u128(i as u128 + 1),
                content: format!("rust record {i}"),
                memory_type: if long_term {
                    MemoryType::LongTerm
                } else {
                    MemoryType::ShortTerm
                },
                metadata: StoredMetadata {
                    created_at: 1_700_000_000_000 + i as u64,
                    updated_at: 1_700_000_000_000 + i as u64,
                    accessed_at: 1_700_000_000_000 + i as u64,
                    access_count: 0,
                    source: None,
                    tags: vec!["rust".to_string()],
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
                archived,
            }
        }

        /// Records fan out along one axis, close together and in index order.
        ///
        /// Deliberately not all-identical: HNSW builds its neighbour lists
        /// from distances, so a corpus of one repeated vector produces a
        /// degenerate graph whose traversal reaches only a handful of nodes.
        /// That would make these tests fail for a reason that has nothing to
        /// do with filtering. Spread thinly instead, so the graph is
        /// well-formed and the ranking is still decided by the filter.
        async fn store(engine: &StorageEngine, s: &StoredMemory, i: u32) {
            let key = format!("memory:{}", s.id);
            engine
                .store_memory_core(
                    key,
                    serde_json::to_vec(s).unwrap(),
                    Some(vec![1.0, i as f32 * 0.001, 0.0, 0.0]),
                    s.metadata.created_at,
                    &s.metadata.tags,
                    Some(&project(s)),
                )
                .await
                .unwrap();
        }

        fn long_term_only() -> Vec<AttrPred> {
            vec![
                AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false)),
                AttrPred::Eq(SLOT_MEMORY_TYPE, AttrValue::U8(1)),
            ]
        }

        /// The headline bug. One record in five is long-term, so a request for
        /// 10 that fetches a fixed 30 candidates keeps ~6 and returns short.
        /// Widening has to make up the difference.
        #[tokio::test]
        async fn a_filtered_search_returns_a_full_page() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with_attrs(&dir).await;
            for i in 0..200u32 {
                store(&engine, &record(i, i % 5 == 0, 0.5, false), i).await;
            }

            let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
            let qr = qe
                .execute(
                    HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10)
                        .with_limit(10)
                        .with_preds(long_term_only()),
                )
                .await
                .unwrap();

            assert_eq!(
                qr.items.len(),
                10,
                "40 long-term records exist; a page of 10 must come back full"
            );
            assert!(!qr.truncated, "the page filled, so nothing was cut short");
        }

        #[tokio::test]
        async fn widening_reuses_the_hnsw_floor_window_and_admissions() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with_attrs(&dir).await;
            for i in 0..200u32 {
                store(&engine, &record(i, i % 3 == 0, 0.5, false), i).await;
            }

            let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
            engine.reset_attr_reads();
            engine.reset_vector_searches();
            engine.reset_vector_distance_evaluations();
            let qr = qe
                .execute(
                    HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10)
                        .with_limit(10)
                        .with_preds(long_term_only()),
                )
                .await
                .unwrap();

            assert_eq!(qr.items.len(), 10);
            assert_eq!(
                engine.vector_searches(),
                1,
                "the first 64-candidate HNSW window contains the needed page, so widening \
                 must reuse it rather than repeat the same traversal at 10, 20, and 40"
            );
            assert!(
                engine.attr_reads() <= 64,
                "a full page must settle only the cached window entries it needs, not reread \
                 candidates from repeated widening attempts"
            );

            let cached_work = engine.vector_distance_evaluations();
            engine.reset_vector_distance_evaluations();
            for candidate_count in [10, 20, 40] {
                engine
                    .vector_search(&[1.0, 0.0, 0.0, 0.0], candidate_count)
                    .await
                    .unwrap();
            }
            let repeated_work = engine.vector_distance_evaluations();
            assert!(
                cached_work < repeated_work,
                "one cached 64-candidate window must do less HNSW work ({cached_work}) than \
                 repeating the identical floor traversal ({repeated_work})"
            );
        }

        /// Short because the corpus is short, not because the search gave up.
        /// The caller must be able to tell the difference.
        #[tokio::test]
        async fn exhausting_the_corpus_is_not_truncation() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with_attrs(&dir).await;
            for i in 0..40u32 {
                store(&engine, &record(i, i < 3, 0.5, false), i).await;
            }

            let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
            let qr = qe
                .execute(
                    HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10)
                        .with_limit(10)
                        .with_preds(long_term_only()),
                )
                .await
                .unwrap();

            assert_eq!(qr.items.len(), 3, "only three records match");
            assert!(
                !qr.truncated,
                "every matching record was returned, so the answer is complete"
            );
        }

        /// The bound doing its job: rather than walking the whole scope to
        /// satisfy a filter almost nothing matches, the search stops and says
        /// so.
        #[tokio::test]
        async fn a_filter_nothing_matches_truncates_instead_of_scanning() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with_attrs(&dir).await;
            for i in 0..300u32 {
                store(&engine, &record(i, false, 0.1, false), i).await;
            }

            // A tight effort bound stands in for a corpus large enough that
            // the default would bite; the mechanism under test is the same.
            let qe = QueryEngine::new(
                Arc::clone(&engine),
                QueryEngineConfig {
                    widen_max_factor: 2,
                    ..Default::default()
                },
            );
            let qr = qe
                .execute(
                    HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10)
                        .with_limit(10)
                        .with_preds(vec![
                            AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false)),
                            AttrPred::Range(
                                SLOT_IMPORTANCE,
                                std::ops::Bound::Included(AttrValue::F32(0.99)),
                                std::ops::Bound::Unbounded,
                            ),
                        ]),
                )
                .await
                .unwrap();

            assert!(qr.items.is_empty(), "nothing in the corpus clears 0.99");
            assert!(
                qr.truncated,
                "the search stopped at its bound, so it cannot claim the corpus holds no matches"
            );
            assert!(
                engine.search_widen_cap_hits() >= 1,
                "hitting the bound must be visible in telemetry (REM-103's gate)"
            );
        }

        /// Archived records used to cost a full payload read each just to be
        /// discarded. Now they are rejected from the row.
        #[tokio::test]
        async fn archived_records_are_rejected_without_reading_their_payloads() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with_attrs(&dir).await;
            for i in 0..60u32 {
                store(&engine, &record(i, true, 0.5, i % 2 == 0), i).await;
            }

            let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
            engine.reset_payload_reads();
            let qr = qe
                .execute(
                    HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10)
                        .with_limit(10)
                        .with_preds(long_term_only()),
                )
                .await
                .unwrap();

            assert_eq!(qr.items.len(), 10);
            assert_eq!(
                engine.payload_reads(),
                0,
                "the query engine settles candidates from attribute rows; \
                 payloads are read by the caller, only for records it returns"
            );
            assert!(engine.attr_reads() > 0, "rows are what it read instead");
        }

        fn not_archived() -> Vec<AttrPred> {
            vec![AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false))]
        }

        /// Retires a stored record's vector the way archiving does.
        async fn retire(engine: &StorageEngine, s: &StoredMemory) {
            engine
                .retire_vector(format!("memory:{}", s.id).as_bytes())
                .await
                .unwrap();
        }

        /// The `archived` predicate keeps archived records out of the results.
        /// Retiring their vectors keeps them out of the *candidates*, which is
        /// what stops them displacing live matches from the page.
        #[tokio::test]
        async fn retired_records_do_not_occupy_slots_in_the_page() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with_attrs(&dir).await;

            let mut archived_ids = Vec::new();
            for i in 0..80u32 {
                let is_archived = i % 2 == 0;
                let rec = record(i, false, 0.5, is_archived);
                store(&engine, &rec, i).await;
                if is_archived {
                    retire(&engine, &rec).await;
                    archived_ids.push(format!("memory:{}", rec.id));
                }
            }

            let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
            let qr = qe
                .execute(
                    HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10)
                        .with_limit(10)
                        .with_preds(not_archived()),
                )
                .await
                .unwrap();

            assert_eq!(qr.items.len(), 10, "the page must come back full");
            assert!(!qr.truncated);
            for item in &qr.items {
                let key = String::from_utf8_lossy(item.key.as_ref()).into_owned();
                assert!(
                    !archived_ids.contains(&key),
                    "an archived record occupied a slot in the page"
                );
            }
            assert_eq!(
                engine.search_widen_cap_hits(),
                0,
                "no archived record should have driven the search to its bound"
            );
        }

        /// The business claim, pinned: a corpus does not get more expensive to
        /// search the more of it has been deleted.
        ///
        /// Measured as work, not as page shape. REM-78's widening loop already
        /// keeps the *page* full over an archived corpus -- it just re-runs the
        /// search until it is, settling every archived candidate from its row
        /// on the way. That is the cost this change removes: a retired record
        /// is dropped inside the index and never reaches the settling step at
        /// all, so it costs no row lookup.
        #[tokio::test]
        async fn search_work_stops_growing_with_archived_volume_once_retired() {
            const LIVE: u32 = 40;

            /// One live record every `total / LIVE`, so the live ones are
            /// spread across the distance range rather than clustered nearest
            /// the query -- otherwise they win the page regardless of what
            /// sits behind them, and this measures nothing.
            async fn work_for(total: u32, retire_archived: bool) -> usize {
                let stride = total / LIVE;
                let dir = TempDir::new().unwrap();
                let engine = engine_with_attrs(&dir).await;
                for i in 0..total {
                    let is_archived = i % stride != 0;
                    let rec = record(i, false, 0.5, is_archived);
                    store(&engine, &rec, i).await;
                    if is_archived && retire_archived {
                        retire(&engine, &rec).await;
                    }
                }

                let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
                let before = engine.attr_reads();
                let qr = qe
                    .execute(
                        HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10)
                            .with_limit(10)
                            .with_preds(not_archived()),
                    )
                    .await
                    .unwrap();
                assert_eq!(qr.items.len(), 10, "the page comes back full either way");
                engine.attr_reads() - before
            }

            // Same 40 live records both times; only the archived volume differs.
            let kept_small = work_for(80, false).await;
            let kept_large = work_for(400, false).await;
            let retired_small = work_for(80, true).await;
            let retired_large = work_for(400, true).await;

            assert!(
                kept_large >= kept_small * 3,
                "baseline: without retirement the work must grow with archived \
                 volume (small={kept_small}, large={kept_large}) -- if it stops \
                 growing, this test no longer measures what it claims"
            );
            assert!(
                retired_large < retired_small * 2,
                "retiring archived records must keep the work flat as archived \
                 volume grows (small={retired_small}, large={retired_large})"
            );
            assert!(
                retired_large < kept_large,
                "retirement must cost less work than leaving the records in \
                 (retired={retired_large}, kept={kept_large})"
            );
        }

        /// Retirement is allowed to fail, and legacy records archived before
        /// it existed never had it applied. Either way the predicate still
        /// has to exclude them -- from the row, without reading the payload.
        #[tokio::test]
        async fn the_archived_predicate_still_excludes_a_record_that_was_never_retired() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with_attrs(&dir).await;

            let mut archived_ids = Vec::new();
            for i in 0..40u32 {
                let is_archived = i % 2 == 0;
                let rec = record(i, false, 0.5, is_archived);
                store(&engine, &rec, i).await;
                if is_archived {
                    // Deliberately not retired.
                    archived_ids.push(format!("memory:{}", rec.id));
                }
            }

            let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
            engine.reset_payload_reads();
            let qr = qe
                .execute(
                    HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10)
                        .with_limit(10)
                        .with_preds(not_archived()),
                )
                .await
                .unwrap();

            for item in &qr.items {
                let key = String::from_utf8_lossy(item.key.as_ref()).into_owned();
                assert!(
                    !archived_ids.contains(&key),
                    "an un-retired archived record reached the results"
                );
            }
            assert_eq!(
                engine.payload_reads(),
                0,
                "the backstop must stay a row-level rejection, not a payload read"
            );
        }

        /// Retirement shrinks the live vector count, which is also the bound
        /// the widening loop uses to decide it has seen everything. A short
        /// page that exhausted the corpus is complete; one that ran out of
        /// effort is truncated. Shrinking the count must not blur the two.
        #[tokio::test]
        async fn a_short_page_over_a_retired_corpus_is_complete_not_truncated() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with_attrs(&dir).await;

            // Interleaved: writing the five live records first would make them
            // the nearest to the query, and the search would find them without
            // ever traversing the retired ones -- which is the whole thing
            // this test needs it to do.
            for i in 0..55u32 {
                let is_archived = i % 11 != 0;
                let rec = record(i, false, 0.5, is_archived);
                store(&engine, &rec, i).await;
                if is_archived {
                    retire(&engine, &rec).await;
                }
            }

            let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
            let qr = qe
                .execute(
                    HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10)
                        .with_limit(10)
                        .with_preds(not_archived()),
                )
                .await
                .unwrap();

            assert_eq!(qr.items.len(), 5, "only five live records exist");
            assert!(
                !qr.truncated,
                "the scope was exhausted, so the short page is complete --                  marking it truncated would claim more may exist when none does"
            );
            assert_eq!(engine.search_widen_cap_hits(), 0);
        }

        /// A keyword query loses candidates at three separate truncation
        /// points, none of which the vector path has. The filter has to be
        /// applied before each cut, not after.
        #[tokio::test]
        async fn a_filtered_keyword_search_returns_a_full_page() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with_attrs(&dir).await;
            for i in 0..200u32 {
                store(&engine, &record(i, i % 5 == 0, 0.5, false), i).await;
            }

            let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
            let qr = qe
                .execute(
                    HybridQuery::keyword(vec!["rust".to_string()], BooleanMode::Or, 10)
                        .with_preds(long_term_only()),
                )
                .await
                .unwrap();

            assert_eq!(
                qr.items.len(),
                10,
                "the tag step, the content scan and the merge each truncate; \
                 filtering has to happen before all three"
            );
        }

        /// Vector and tag steps together, fused, with the filter applied
        /// inside each before the merge sees them.
        #[tokio::test]
        async fn a_filtered_hybrid_search_returns_a_full_page() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with_attrs(&dir).await;
            for i in 0..200u32 {
                store(&engine, &record(i, i % 5 == 0, 0.5, false), i).await;
            }

            let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
            let qr = qe
                .execute(
                    HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10)
                        .with_limit(10)
                        .with_tags(vec!["rust".to_string()], BooleanMode::Or)
                        .with_preds(long_term_only()),
                )
                .await
                .unwrap();

            assert_eq!(qr.items.len(), 10);
            for item in &qr.items {
                assert!(
                    !item.sources.is_empty(),
                    "every fused item keeps the contributions that produced it"
                );
            }
        }

        /// With no attribute schema and no projector there is no way to
        /// evaluate a predicate. Returning unfiltered results would leak
        /// archived records; returning none would claim the corpus is empty.
        /// Both are lies, so the query is refused.
        #[tokio::test]
        async fn predicates_without_attribute_support_are_refused_not_ignored() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with(&dir).await;
            put_memory(&engine, "memory:a", "rust", &["rust"], 1).await;

            let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
            let err = qe
                .execute(
                    HybridQuery::keyword(vec!["rust".to_string()], BooleanMode::Or, 10)
                        .with_preds(long_term_only()),
                )
                .await
                .unwrap_err();

            assert!(
                matches!(err, crate::engine::error::StorageError::InvalidArgument(_)),
                "got {err:?}"
            );
        }

        /// The same query without predicates still works in that
        /// configuration — attribute support being off must not break search
        /// itself.
        #[tokio::test]
        async fn unfiltered_search_still_works_without_attribute_support() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with(&dir).await;
            put_memory(&engine, "memory:a", "rust ownership", &["rust"], 1).await;

            let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
            let qr = qe
                .execute(HybridQuery::keyword(
                    vec!["rust".to_string()],
                    BooleanMode::Or,
                    10,
                ))
                .await
                .unwrap();

            assert_eq!(qr.items.len(), 1);
            assert!(!qr.truncated);
        }

        /// A tag filter is a filter, not a ranking hint: it has to narrow the
        /// candidate set before truncation, or it shrinks the page after the
        /// engine has stopped widening.
        #[tokio::test]
        async fn a_tag_filtered_search_returns_a_full_page() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with_attrs(&dir).await;
            for i in 0..200u32 {
                let mut s = record(i, true, 0.5, false);
                // One record in five carries the tag being filtered on.
                s.metadata.tags = if i % 5 == 0 {
                    vec!["rust".to_string(), "wanted".to_string()]
                } else {
                    vec!["rust".to_string()]
                };
                store(&engine, &s, i).await;
            }

            let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
            let qr = qe
                .execute(
                    HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10)
                        .with_limit(10)
                        .with_preds(vec![AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false))])
                        .with_tag_filter(vec!["wanted".to_string()]),
                )
                .await
                .unwrap();

            assert_eq!(
                qr.items.len(),
                10,
                "40 records carry the tag; the page must come back full"
            );
            assert!(!qr.tag_filter_deferred, "the tag index could answer");
        }

        /// A tag the index has no posting list for reports "no matches",
        /// which is indistinguishable from a real miss. Trusting it would
        /// turn an answerable query into an empty page, so the filter is
        /// handed back to the caller instead.
        #[tokio::test]
        async fn a_tag_the_index_cannot_answer_for_is_deferred_not_treated_as_no_match() {
            let dir = TempDir::new().unwrap();
            let engine = engine_with_attrs(&dir).await;
            for i in 0..20u32 {
                store(&engine, &record(i, true, 0.5, false), i).await;
            }

            // Longer than the tag index's `max_token_length`, so it has no
            // posting list of its own.
            let overlong = "x".repeat(200);

            let qe = QueryEngine::new(Arc::clone(&engine), QueryEngineConfig::default());
            let qr = qe
                .execute(
                    HybridQuery::new(vec![1.0, 0.0, 0.0, 0.0], 10)
                        .with_limit(10)
                        .with_preds(vec![AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false))])
                        .with_tag_filter(vec![overlong]),
                )
                .await
                .unwrap();

            assert!(
                qr.tag_filter_deferred,
                "the engine must say it could not settle the tag filter"
            );
            assert!(
                !qr.items.is_empty(),
                "deferring must not be reported as an empty result set"
            );
        }
    }
}
