//! Query Executor that runs execution plans against the storage engine
#![allow(dead_code)]
//!
//! The executor takes an execution plan and:
//! - Executes each step against the appropriate index
//! - Applies filters
//! - Merges results from multiple sources

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;

use crate::engine::error::{Result, StorageError};
use crate::engine::storage::StorageEngine;

use super::merge::{IntersectionMerger, RrfMerger, ScoreNormalizer, UnionMerger, WeightedMerger};
use super::planner::{ExecutionPlan, ExecutionStep, MergeStep};
use super::types::*;
use super::QueryEngineConfig;

/// Query executor that runs execution plans
pub struct QueryExecutor {
    /// Storage engine reference
    engine: Arc<StorageEngine>,

    /// Configuration
    config: QueryEngineConfig,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(engine: Arc<StorageEngine>, config: QueryEngineConfig) -> Self {
        Self { engine, config }
    }

    /// Execute a plan and return results
    pub async fn execute(&self, plan: ExecutionPlan) -> Result<QueryResult> {
        let start = Instant::now();

        let items = if plan.is_hybrid() {
            self.execute_hybrid(plan).await?
        } else {
            self.execute_single(plan).await?
        };

        let execution_time_ms = start.elapsed().as_millis() as u64;

        Ok(QueryResult {
            items,
            total_count: None,
            execution_time_ms,
            debug_info: None,
        })
    }

    /// Execute a single-step plan
    async fn execute_single(&self, plan: ExecutionPlan) -> Result<Vec<ResultItem>> {
        let step = plan
            .steps
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::InvalidArgument("Empty execution plan".to_string()))?;

        let mut results = self.execute_step(step).await?;

        // Apply limit
        if let Some(limit) = plan.final_limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    /// Execute a hybrid (multi-step) plan with merging
    async fn execute_hybrid(&self, plan: ExecutionPlan) -> Result<Vec<ResultItem>> {
        let mut all_results: Vec<(ResultSource, Vec<ResultItem>)> = Vec::new();

        // Execute each step
        for step in plan.steps {
            let source = step_source(&step);
            let results = self.execute_step(step).await?;
            all_results.push((source, results));
        }

        // Merge results
        let merged = if let Some(merge) = plan.merge {
            self.merge_results(all_results, merge)?
        } else {
            // No merge specified, just concatenate
            all_results
                .into_iter()
                .flat_map(|(_, items)| items)
                .collect()
        };

        // Apply final limit
        let mut final_results = merged;
        if let Some(limit) = plan.final_limit {
            final_results.truncate(limit);
        }

        Ok(final_results)
    }

    /// Execute a single step
    async fn execute_step(&self, step: ExecutionStep) -> Result<Vec<ResultItem>> {
        match step {
            ExecutionStep::VectorSearch {
                embedding,
                k,
                ef,
                include_values,
            } => {
                self.execute_vector_search(embedding, k, ef, include_values)
                    .await
            }

            ExecutionStep::GraphTraversal {
                start,
                max_depth,
                edge_types,
                limit,
            } => self.execute_graph_traversal(start, max_depth, edge_types, limit),

            ExecutionStep::TimeRangeScan {
                start,
                end,
                limit,
                descending,
            } => self.execute_time_range(start, end, limit, descending),

            ExecutionStep::TagSearch {
                tokens,
                mode,
                limit,
            } => self.execute_tag_search(tokens, mode, limit),

            ExecutionStep::Filter { predicates: _predicates } => {
                // Filter step should be applied to previous results
                // This is handled differently - predicates are applied inline
                Err(StorageError::InvalidArgument(
                    "Filter step should be applied to results, not executed standalone".to_string(),
                ))
            }

            ExecutionStep::FetchValues { keys } => self.execute_fetch_values(keys).await,
        }
    }

    /// Execute vector search
    async fn execute_vector_search(
        &self,
        embedding: Vec<f32>,
        k: usize,
        ef: Option<usize>,
        include_values: bool,
    ) -> Result<Vec<ResultItem>> {
        let search_results = if include_values {
            self.engine.vector_search_with_values(&embedding, k).await?
        } else {
            self.engine
                .vector_search_with_ef(&embedding, k, ef)
                .await?
                .into_iter()
                .map(|r| crate::engine::storage::VectorSearchResult {
                    key: r.key,
                    distance: r.distance,
                    value: None,
                })
                .collect()
        };

        let items: Vec<ResultItem> = search_results
            .into_iter()
            .map(|r| {
                let score = ScoreNormalizer::normalize_vector_distance(r.distance);
                let mut item = ResultItem::new(r.key, score).with_source(ResultSource::Vector);
                if let Some(value) = r.value {
                    item = item.with_value(value);
                }
                item.extra = Some(ResultExtra::Vector {
                    distance: r.distance,
                });
                item
            })
            .collect();

        Ok(items)
    }

    /// Execute graph traversal
    fn execute_graph_traversal(
        &self,
        start: Bytes,
        max_depth: usize,
        edge_types: Option<Vec<String>>,
        limit: Option<usize>,
    ) -> Result<Vec<ResultItem>> {
        let traversal_results =
            self.engine
                .traverse_graph(&start, max_depth, edge_types.as_deref())?;

        let mut items: Vec<ResultItem> = traversal_results
            .into_iter()
            .filter(|r| r.node_id != start) // Exclude start node
            .map(|r| {
                let score = ScoreNormalizer::normalize_graph_depth(r.depth);
                let mut item = ResultItem::new(r.node_id, score).with_source(ResultSource::Graph);
                item.extra = Some(ResultExtra::Graph {
                    depth: r.depth,
                    edge_type: r.edge_metadata.map(|m| m.edge_type),
                });
                item
            })
            .collect();

        if let Some(limit) = limit {
            items.truncate(limit);
        }

        Ok(items)
    }

    /// Execute time range query
    fn execute_time_range(
        &self,
        start: u64,
        end: u64,
        limit: Option<usize>,
        descending: bool,
    ) -> Result<Vec<ResultItem>> {
        let mut results = self.engine.time_range_query(start, end, limit)?;

        if descending {
            results.reverse();
        }

        let items: Vec<ResultItem> = results
            .into_iter()
            .enumerate()
            .map(|(i, (timestamp, key))| {
                // Score based on recency (for descending) or position
                let score = if descending {
                    1.0 / (1.0 + i as f32)
                } else {
                    1.0 / (1.0 + i as f32)
                };
                let mut item = ResultItem::new(key, score).with_source(ResultSource::TimeSeries);
                item.extra = Some(ResultExtra::TimeSeries { timestamp });
                item
            })
            .collect();

        Ok(items)
    }

    /// Execute tag search
    fn execute_tag_search(
        &self,
        tokens: Vec<String>,
        mode: BooleanMode,
        limit: Option<usize>,
    ) -> Result<Vec<ResultItem>> {
        let token_refs: Vec<&str> = tokens.iter().map(|s| s.as_str()).collect();

        let results = match mode {
            BooleanMode::And => {
                let keys = self.engine.tag_search_and(&token_refs)?;
                keys.into_iter().map(|k| (k, 1.0)).collect::<Vec<_>>()
            }
            BooleanMode::Or => self.engine.tag_search_scored(&token_refs)?,
        };

        let mut items: Vec<ResultItem> = results
            .into_iter()
            .map(|(key, score)| {
                let mut item = ResultItem::new(key.clone(), score).with_source(ResultSource::Tag);
                // Get matching tags for this key
                if let Ok(tags) = self.engine.get_tags(&key) {
                    let matching: Vec<String> =
                        tags.into_iter().filter(|t| tokens.contains(t)).collect();
                    item.extra = Some(ResultExtra::Tag {
                        matching_tags: matching,
                    });
                }
                item
            })
            .collect();

        // Normalize tag scores
        ScoreNormalizer::normalize_tag_scores(&mut items);

        if let Some(limit) = limit {
            items.truncate(limit);
        }

        Ok(items)
    }

    /// Fetch values for a list of keys
    async fn execute_fetch_values(&self, keys: Vec<Bytes>) -> Result<Vec<ResultItem>> {
        let mut items = Vec::with_capacity(keys.len());

        for key in keys {
            if let Some(value) = self.engine.get(&key).await? {
                items.push(ResultItem::new(key, 1.0).with_value(value));
            }
        }

        Ok(items)
    }

    /// Merge results from multiple steps
    fn merge_results(
        &self,
        results: Vec<(ResultSource, Vec<ResultItem>)>,
        merge: MergeStep,
    ) -> Result<Vec<ResultItem>> {
        match merge.strategy {
            MergeStrategyType::Rrf => {
                let merger = RrfMerger::new(merge.rrf_k);
                Ok(merger.merge_with_sources(results, merge.limit))
            }
            MergeStrategyType::WeightedSum => {
                let weights = merge.weights.unwrap_or_default();
                let merger = WeightedMerger::new(weights);
                Ok(merger.merge(results, merge.limit))
            }
            MergeStrategyType::Intersection => {
                let lists: Vec<Vec<ResultItem>> =
                    results.into_iter().map(|(_, items)| items).collect();
                Ok(IntersectionMerger::merge(lists, merge.limit))
            }
            MergeStrategyType::Union => {
                let lists: Vec<Vec<ResultItem>> =
                    results.into_iter().map(|(_, items)| items).collect();
                Ok(UnionMerger::merge(lists, merge.limit))
            }
        }
    }

    /// Apply filter predicates to results
    pub fn apply_filter(&self, items: Vec<ResultItem>, filter: &Filter) -> Vec<ResultItem> {
        if filter.is_empty() {
            return items;
        }

        items
            .into_iter()
            .filter(|item| self.matches_filter(item, filter))
            .collect()
    }

    /// Check if an item matches all filter predicates
    fn matches_filter(&self, item: &ResultItem, filter: &Filter) -> bool {
        for predicate in &filter.predicates {
            if !self.matches_predicate(item, predicate) {
                return false;
            }
        }
        true
    }

    /// Check if an item matches a single predicate
    fn matches_predicate(&self, item: &ResultItem, predicate: &Predicate) -> bool {
        match predicate {
            Predicate::HasTags(required_tags) => {
                if let Ok(item_tags) = self.engine.get_tags(&item.key) {
                    let tag_set: HashSet<_> = item_tags.iter().collect();
                    required_tags.iter().all(|t| tag_set.contains(t))
                } else {
                    false
                }
            }

            Predicate::HasAnyTag(any_tags) => {
                if let Ok(item_tags) = self.engine.get_tags(&item.key) {
                    let tag_set: HashSet<_> = item_tags.iter().collect();
                    any_tags.iter().any(|t| tag_set.contains(t))
                } else {
                    false
                }
            }

            Predicate::TimeRange { start, end } => {
                // Check if item's timestamp is in range
                if let Some(ResultExtra::TimeSeries { timestamp }) = &item.extra {
                    *timestamp >= *start && *timestamp <= *end
                } else {
                    // Can't filter by time if no timestamp info
                    true
                }
            }

            Predicate::RelatedTo { node, max_depth } => {
                // Check if item is reachable from node
                if let Ok(related) = self.engine.traverse_graph(node, *max_depth, None) {
                    related.iter().any(|r| r.node_id == item.key)
                } else {
                    false
                }
            }

            Predicate::KeyPrefix(prefix) => item.key.starts_with(prefix.as_ref()),

            Predicate::Metadata { key: _, value: _ } => {
                // Metadata filtering not yet implemented
                true
            }
        }
    }
}

/// Determine the result source for an execution step
fn step_source(step: &ExecutionStep) -> ResultSource {
    match step {
        ExecutionStep::VectorSearch { .. } => ResultSource::Vector,
        ExecutionStep::GraphTraversal { .. } => ResultSource::Graph,
        ExecutionStep::TimeRangeScan { .. } => ResultSource::TimeSeries,
        ExecutionStep::TagSearch { .. } => ResultSource::Tag,
        ExecutionStep::Filter { .. } => ResultSource::Unknown,
        ExecutionStep::FetchValues { .. } => ResultSource::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_step_source() {
        let vector_step = ExecutionStep::VectorSearch {
            embedding: vec![0.1],
            k: 10,
            ef: None,
            include_values: true,
        };
        assert_eq!(step_source(&vector_step), ResultSource::Vector);

        let graph_step = ExecutionStep::GraphTraversal {
            start: Bytes::from("node"),
            max_depth: 2,
            edge_types: None,
            limit: None,
        };
        assert_eq!(step_source(&graph_step), ResultSource::Graph);
    }
}
