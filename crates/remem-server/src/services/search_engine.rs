use std::sync::Arc;

use uuid::Uuid;

use crate::engine::QueryEngine;

use crate::embedding::EmbeddingService;
use crate::error::Result;
use crate::services::repository::MemoryRepository;
use crate::services::types::{
    distance_to_score, memory_key, MemoryFilters, SearchResult, SearchType,
};

pub struct SearchEngine {
    repo: Arc<MemoryRepository>,
    query_engine: Arc<QueryEngine>,
    embedding: Arc<EmbeddingService>,
}

pub struct SearchQuery {
    pub query: String,
    pub search_type: SearchType,
    pub filters: MemoryFilters,
    pub limit: usize,
    /// When set, graph neighbours of this memory are boosted in results.
    pub related_to: Option<Uuid>,
}

impl SearchEngine {
    pub fn new(
        repo: Arc<MemoryRepository>,
        query_engine: Arc<QueryEngine>,
        embedding: Arc<EmbeddingService>,
    ) -> Self {
        Self {
            repo,
            query_engine,
            embedding,
        }
    }

    pub async fn search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>> {
        match query.search_type {
            SearchType::Semantic => self.semantic_search(query).await,
            SearchType::Keyword => self.keyword_search(query).await,
            SearchType::Hybrid => self.hybrid_search(query).await,
        }
    }

    async fn semantic_search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>> {
        let embedding = self.embedding.embed(&query.query).await?;
        let k = (query.limit * 3).max(20);

        // When a graph context is requested, route through the QueryEngine so that
        // connected memories are boosted via RRF alongside the vector results.
        if let Some(related_id) = query.related_to {
            use crate::engine::query::{MergeStrategyType};
            use crate::engine::{HybridQuery, Query};

            let node_key = memory_key(related_id);
            let hybrid = HybridQuery::vector(embedding, k)
                .with_graph_context(node_key, 2)
                .with_merge_strategy(MergeStrategyType::Rrf)
                .with_limit(k);

            let qr = self.query_engine.execute(Query::Hybrid(hybrid)).await?;
            return self.collect_results(qr.items, query).await;
        }

        // Fast path: direct HNSW call when no graph context needed.
        // HNSW doesn't support deletion, so phantom entries (deleted from KV but still in the
        // vector index) must be skipped. We double k on each retry until we collect enough valid
        // results or exhaust the index.
        let vector_count = self.repo.engine.vector_count().max(1);
        let mut k_actual = k;
        let mut results = Vec::new();
        loop {
            let raw = self.repo.engine.vector_search(&embedding, k_actual).await?;
            results.clear();
            for item in raw {
                let Some(stored) = self.repo.load_by_key(&item.key).await? else {
                    continue;
                };
                if stored.archived {
                    continue;
                }
                if !crate::services::memory_manager::matches_filters_pub(&stored, &query.filters) {
                    continue;
                }
                results.push(SearchResult {
                    score: distance_to_score(item.distance),
                    memory: stored.into_api(Vec::new()),
                });
                if results.len() >= query.limit {
                    break;
                }
            }
            // Stop if we have enough results or already scanning the full index
            if results.len() >= query.limit || k_actual >= vector_count {
                break;
            }
            k_actual = (k_actual * 2).min(vector_count);
        }
        Ok(results)
    }

    /// Shared post-processing: load stored memories for QueryEngine result items,
    /// apply filters, and truncate to the requested limit.
    async fn collect_results(
        &self,
        items: Vec<crate::engine::query::ResultItem>,
        query: &SearchQuery,
    ) -> Result<Vec<SearchResult>> {
        let mut results = Vec::new();
        for item in items {
            let key = String::from_utf8_lossy(&item.key);
            if !key.starts_with("memory:") {
                continue;
            }
            let Some(stored) = self.repo.load_by_key(item.key.as_ref()).await? else {
                continue;
            };
            if stored.archived {
                continue;
            }
            if !crate::services::memory_manager::matches_filters_pub(&stored, &query.filters) {
                continue;
            }
            results.push(SearchResult {
                score: item.score,
                memory: stored.into_api(Vec::new()),
            });
            if results.len() >= query.limit {
                break;
            }
        }
        Ok(results)
    }

    async fn keyword_search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>> {
        let tokens: Vec<String> = query
            .query
            .split_whitespace()
            .map(|s| s.to_lowercase())
            .collect();

        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let token_refs: Vec<&str> = tokens.iter().map(|s| s.as_str()).collect();
        let tag_hits = self.repo.engine.tag_search_scored(&token_refs)?;

        // Build a set of memory keys found via tag index to avoid duplicates in content scan.
        let mut seen_keys: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut results = Vec::new();

        for (key_bytes, score) in &tag_hits {
            let key = String::from_utf8_lossy(key_bytes);
            if !key.starts_with("memory:") {
                continue;
            }
            let Some(stored) = self.repo.load_by_key(key_bytes.as_ref()).await? else {
                continue;
            };
            if stored.archived {
                continue;
            }
            if !crate::services::memory_manager::matches_filters_pub(&stored, &query.filters) {
                continue;
            }
            let content_lower = stored.content.to_lowercase();
            let content_matches = tokens
                .iter()
                .filter(|t| content_lower.contains(t.as_str()))
                .count() as f32;
            let content_score = content_matches / tokens.len() as f32;
            let combined_score = 0.5 * score + 0.5 * content_score;
            seen_keys.insert(key_bytes.to_vec());
            results.push(SearchResult {
                score: combined_score,
                memory: stored.into_api(Vec::new()),
            });
            if results.len() >= query.limit * 3 {
                break;
            }
        }

        // Fallback content scan: if tag search didn't find enough results, scan all memories
        // for content matches. This handles memories with no tags or unindexed words.
        if results.len() < query.limit {
            let entries = self.repo.engine.time_range_query(0, u64::MAX, None)?;
            for (_ts, key_bytes) in entries {
                if seen_keys.contains(key_bytes.as_ref()) {
                    continue;
                }
                let Some(stored) = self.repo.load_by_key(key_bytes.as_ref()).await? else {
                    continue;
                };
                if stored.archived {
                    continue;
                }
                if !crate::services::memory_manager::matches_filters_pub(&stored, &query.filters) {
                    continue;
                }
                let content_lower = stored.content.to_lowercase();
                let content_matches = tokens
                    .iter()
                    .filter(|t| content_lower.contains(t.as_str()))
                    .count() as f32;
                if content_matches == 0.0 {
                    continue;
                }
                let content_score = content_matches / tokens.len() as f32;
                results.push(SearchResult {
                    score: content_score,
                    memory: stored.into_api(Vec::new()),
                });
                if results.len() >= query.limit * 3 {
                    break;
                }
            }
        }

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(query.limit);
        Ok(results)
    }

    async fn hybrid_search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>> {
        use crate::engine::query::{BooleanMode, MergeStrategyType};
        use crate::engine::{HybridQuery, Query};

        let embedding = self.embedding.embed(&query.query).await?;
        let tokens: Vec<String> = query
            .query
            .split_whitespace()
            .map(|s| s.to_lowercase())
            .collect();

        let k = (query.limit * 3).max(20);

        let mut hybrid = HybridQuery::vector(embedding, k)
            .with_merge_strategy(MergeStrategyType::Rrf)
            .with_limit(k);

        if !tokens.is_empty() {
            hybrid = hybrid.with_tags(tokens, BooleanMode::Or);
        }

        if let Some(related_id) = query.related_to {
            hybrid = hybrid.with_graph_context(memory_key(related_id), 2);
        }

        let qr = self.query_engine.execute(Query::Hybrid(hybrid)).await?;
        self.collect_results(qr.items, query).await
    }
}

