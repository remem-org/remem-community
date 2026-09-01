use std::sync::Arc;

use uuid::Uuid;

use crate::engine::query::{BooleanMode, Evidence};
use crate::engine::{HybridQuery, QueryEngine};

use crate::embedding::EmbeddingService;
use crate::error::Result;
use crate::services::filters::to_attr_preds;
use crate::services::repository::MemoryRepository;
use crate::services::types::{
    memory_key, parse_memory_id, vector_relevance, MemoryFilters, ResultSource, SearchResult,
    SearchType, SourceName,
};

pub struct SearchEngine {
    repo: Arc<MemoryRepository>,
    query_engine: Arc<QueryEngine>,
    embedding: Arc<EmbeddingService>,
}

/// What a search returns: the page, and whether it is the whole answer.
///
/// `truncated` is the difference between "these are all the memories that
/// match" and "this is as far as the search looked". A filtered search used to
/// return the second while looking like the first (REM-78).
pub struct SearchOutcome {
    pub results: Vec<SearchResult>,
    pub truncated: bool,
}

pub struct SearchQuery {
    pub query: String,
    pub search_type: SearchType,
    pub filters: MemoryFilters,
    pub limit: usize,
    /// When set, graph neighbours of this memory are boosted in results.
    pub related_to: Option<Uuid>,
}

/// How many candidates to ask the indexes for, to yield `limit` results.
///
/// Filtered queries over-fetch; unfiltered ones do not. The margin exists
/// because predicates reject candidates, and widening a query costs a second
/// probe that re-walks the index from its entry point — so a query that is
/// *likely* to lose candidates is cheaper fetching ahead once than probing
/// twice, while one that cannot lose any is simply paying for candidates it
/// will throw away.
///
/// This replaces an inherited `max(limit * 3, 20)` whose only recorded
/// rationale was over-fetching past archived records, and whose floor gave the
/// smallest requests a twentyfold margin and the largest threefold — inverted
/// from need. It is a latency choice now, not a correctness one: widening
/// covers any shortfall either way (REM-78, design D9).
fn candidate_target(limit: usize, filters: &MemoryFilters) -> usize {
    if filters.narrows_nothing() {
        limit
    } else {
        limit.saturating_mul(3)
    }
}

/// Split a query into lowercased search tokens.
fn tokenize(query: &str) -> Vec<String> {
    query.split_whitespace().map(|s| s.to_lowercase()).collect()
}

/// Query tokens that match one of the memory's tags, reported as the evidence
/// behind a tag-index hit. The inverted index returns scores but not the terms
/// that produced them, so the intersection is recomputed here.
fn matching_tags(tokens: &[String], tags: &[String]) -> Vec<String> {
    tags.iter()
        .filter(|tag| tokens.contains(&tag.to_lowercase()))
        .cloned()
        .collect()
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

    pub async fn search(&self, query: &SearchQuery) -> Result<SearchOutcome> {
        match query.search_type {
            SearchType::Semantic => self.semantic_search(query).await,
            SearchType::Keyword => self.keyword_search(query).await,
            SearchType::Hybrid => self.hybrid_search(query).await,
        }
    }

    async fn semantic_search(&self, query: &SearchQuery) -> Result<SearchOutcome> {
        let embedding = self.embedding.embed(&query.query).await?;
        let k = candidate_target(query.limit, &query.filters);

        let mut hybrid = HybridQuery::new(embedding, k)
            .with_limit(k)
            .with_preds(to_attr_preds(&query.filters))
            .with_tag_filter(query.filters.tags.clone());

        // When a graph context is requested, connected memories are boosted
        // via RRF alongside the vector results.
        if let Some(related_id) = query.related_to {
            hybrid = hybrid.with_graph_context(memory_key(related_id), 2);
        }

        let qr = self
            .query_engine
            .execute_partitioned(hybrid, self.repo.read_scope())
            .await?;
        // Invariant: without `related_to` this plan has a single step
        // (vector only), which bypasses RRF entirely — `fused_score` is then
        // exactly `1/(1+distance)`, not a rank-based value. Adding a second
        // step to this path (e.g. a future tag or content step) would
        // silently convert `fused_score` to RRF's rank-based scale.
        // No tag step in this plan, so no tokens are needed for evidence.
        let (truncated, deferred) = (qr.truncated, qr.tag_filter_deferred);
        self.collect_results(qr.items, query, &[], truncated, deferred)
            .await
    }

    /// Shared post-processing: load the stored memory behind each result item
    /// and shape it for the API.
    ///
    /// No filtering happens here. Every item reaching this point has already
    /// been settled against its attribute row inside the query engine, so a
    /// payload is read only for a record that is going to be returned — which
    /// is the whole point of pushing predicates down (REM-78). Tag conditions
    /// are the documented exception and are settled below, from the payload,
    /// because tags carry no attribute slot.
    ///
    /// `tokens` are the query terms, used to report which tags a tag-index hit
    /// actually matched — the index returns scores but not the matching terms,
    /// and the memory is already loaded here, so the intersection is free.
    async fn collect_results(
        &self,
        items: Vec<crate::engine::query::FusedItem>,
        query: &SearchQuery,
        tokens: &[String],
        truncated: bool,
        tag_filter_deferred: bool,
    ) -> Result<SearchOutcome> {
        let metric = self.repo.engine.vector_metric();
        let mut results = Vec::new();
        // Only meaningful when the tag filter was deferred: a record dropped
        // here was dropped after the engine stopped widening, so a page that
        // ends up short may not be a complete answer.
        let mut dropped_by_deferred_tags = false;
        for item in items {
            if parse_memory_id(item.key.as_ref()).is_none() {
                continue;
            }
            let Some((binding, mut stored)) =
                self.repo.load_bound_by_key(item.key.as_ref()).await?
            else {
                continue;
            };
            // Search discovers memories rather than addressing them, so it
            // records no recall of its own -- but it must still report the
            // recall other operations have recorded and not yet written, or
            // the same memory shows a different use count depending on which
            // endpoint asked. `peek`, not `take`: a read must never consume a
            // recall no write has applied.
            if let Some(delta) = self.repo.recall().peek(&binding, stored.id) {
                delta.apply(&mut stored.metadata);
            }
            // The tag index already settled this filter unless it could not
            // answer for every requested tag, in which case the payload — in
            // hand now — decides. This is the one place a rejected record is
            // materialized (REM-78, design D6).
            if tag_filter_deferred && !query.filters.tags.is_empty() {
                let tag_set: std::collections::HashSet<&str> =
                    stored.metadata.tags.iter().map(|s| s.as_str()).collect();
                if !query
                    .filters
                    .tags
                    .iter()
                    .all(|t| tag_set.contains(t.as_str()))
                {
                    dropped_by_deferred_tags = true;
                    continue;
                }
            }

            let sources =
                item.sources
                    .iter()
                    .map(|c| {
                        let source = SourceName::from(c.kind);
                        // Vector contributions carry a raw distance, which is only
                        // meaningful once converted under the configured metric.
                        let base = ResultSource::new(
                            source,
                            match c.evidence {
                                Evidence::Vector { distance } => vector_relevance(metric, distance),
                                _ => c.score,
                            },
                            c.rank,
                        );
                        match c.evidence {
                            Evidence::Vector { distance } => {
                                base.with_vector_evidence(metric, distance)
                            }
                            Evidence::Graph { depth } => base.with_depth(depth),
                            Evidence::None if source == SourceName::Tag => base
                                .with_matching_tags(matching_tags(tokens, &stored.metadata.tags)),
                            Evidence::None => base,
                        }
                    })
                    .collect();

            results.push(SearchResult::from_sources(
                stored.into_api(Vec::new()),
                sources,
                item.fused_score,
            ));
            if results.len() >= query.limit {
                break;
            }
        }
        // A deferred tag filter can shrink the page below the limit with
        // nothing left to widen against, which is exactly the shape of
        // incompleteness `truncated` exists to report.
        let truncated = truncated || (dropped_by_deferred_tags && results.len() < query.limit);
        Ok(SearchOutcome { results, truncated })
    }

    async fn keyword_search(&self, query: &SearchQuery) -> Result<SearchOutcome> {
        let tokens = tokenize(&query.query);

        if tokens.is_empty() {
            return Ok(SearchOutcome {
                results: Vec::new(),
                truncated: false,
            });
        }

        let k = candidate_target(query.limit, &query.filters);
        let hybrid = HybridQuery::keyword(tokens.clone(), BooleanMode::Or, k)
            .with_preds(to_attr_preds(&query.filters))
            .with_tag_filter(query.filters.tags.clone());

        let qr = self
            .query_engine
            .execute_partitioned(hybrid, self.repo.read_scope())
            .await?;
        let (truncated, deferred) = (qr.truncated, qr.tag_filter_deferred);
        self.collect_results(qr.items, query, &tokens, truncated, deferred)
            .await
    }

    async fn hybrid_search(&self, query: &SearchQuery) -> Result<SearchOutcome> {
        let embedding = self.embedding.embed(&query.query).await?;
        let tokens = tokenize(&query.query);

        let k = candidate_target(query.limit, &query.filters);

        let mut hybrid = HybridQuery::new(embedding, k)
            .with_limit(k)
            .with_preds(to_attr_preds(&query.filters))
            .with_tag_filter(query.filters.tags.clone());

        if !tokens.is_empty() {
            hybrid = hybrid.with_tags(tokens.clone(), BooleanMode::Or);
        }

        if let Some(related_id) = query.related_to {
            hybrid = hybrid.with_graph_context(memory_key(related_id), 2);
        }

        let qr = self
            .query_engine
            .execute_partitioned(hybrid, self.repo.read_scope())
            .await?;
        let (truncated, deferred) = (qr.truncated, qr.tag_filter_deferred);
        self.collect_results(qr.items, query, &tokens, truncated, deferred)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Constructing a SearchEngine needs a storage engine and the ONNX embedding
    // model, so the search methods themselves are covered by the (#[ignore]d)
    // integration tests in api/tests.rs. What is unit-testable here is the
    // evidence derivation the result contract depends on.

    #[test]
    fn tokenize_lowercases_and_splits_on_whitespace() {
        assert_eq!(
            tokenize("Rust  Ownership\tModel"),
            vec!["rust", "ownership", "model"]
        );
    }

    #[test]
    fn tokenize_empty_query_yields_no_tokens() {
        assert!(tokenize("   ").is_empty());
    }

    #[test]
    fn matching_tags_reports_the_intersection_case_insensitively() {
        let tokens = tokenize("Rust Borrow");
        let tags = vec!["Rust".to_string(), "async".to_string()];
        assert_eq!(matching_tags(&tokens, &tags), vec!["Rust".to_string()]);
    }

    #[test]
    fn matching_tags_preserves_the_stored_casing() {
        // The reported evidence should be the tag as stored, not as queried.
        let tokens = tokenize("rust");
        let tags = vec!["Rust".to_string()];
        assert_eq!(matching_tags(&tokens, &tags), vec!["Rust".to_string()]);
    }

    #[test]
    fn matching_tags_is_empty_when_nothing_overlaps() {
        let tokens = tokenize("python");
        let tags = vec!["rust".to_string()];
        assert!(matching_tags(&tokens, &tags).is_empty());
    }

    #[test]
    fn matching_tags_does_not_match_on_substrings() {
        // "rust" must not be reported as evidence for the tag "trust".
        let tokens = tokenize("rust");
        let tags = vec!["trust".to_string()];
        assert!(matching_tags(&tokens, &tags).is_empty());
    }
}
