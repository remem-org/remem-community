//! Query executor that runs execution plans against the storage engine
//!
//! Each step runs against one index and produces an independently ranked list;
//! when there is more than one, the lists are fused by rank (RRF).

use std::sync::Arc;

use bytes::Bytes;

use crate::engine::error::{Result, StorageError};
use crate::engine::storage::StorageEngine;

use super::merge::{RrfMerger, ScoreNormalizer};
use super::planner::{ExecutionPlan, ExecutionStep};
use super::types::{BooleanMode, QueryResult, ResultItem};

/// Query executor that runs execution plans
pub struct QueryExecutor {
    /// Storage engine reference
    engine: Arc<StorageEngine>,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }

    /// Execute a plan and return results
    pub async fn execute(&self, plan: ExecutionPlan) -> Result<QueryResult> {
        let items = if plan.is_hybrid() {
            self.execute_hybrid(plan).await?
        } else {
            self.execute_single(plan).await?
        };

        Ok(QueryResult { items })
    }

    /// Execute a single-step plan
    async fn execute_single(&self, plan: ExecutionPlan) -> Result<Vec<ResultItem>> {
        let step = plan
            .steps
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::InvalidArgument("Empty execution plan".to_string()))?;

        let mut results = self.execute_step(step).await?;
        results.truncate(plan.final_limit);

        Ok(results)
    }

    /// Execute a multi-step plan, fusing the per-index rankings
    async fn execute_hybrid(&self, plan: ExecutionPlan) -> Result<Vec<ResultItem>> {
        let mut all_results: Vec<Vec<ResultItem>> = Vec::with_capacity(plan.steps.len());

        for step in plan.steps {
            all_results.push(self.execute_step(step).await?);
        }

        let mut final_results = match plan.merge {
            Some(merge) => RrfMerger::new(merge.rrf_k).merge(all_results, merge.limit),
            // No merge step: a plan with multiple steps always carries one, but
            // concatenating is the honest fallback rather than dropping results.
            None => all_results.into_iter().flatten().collect(),
        };

        final_results.truncate(plan.final_limit);
        Ok(final_results)
    }

    /// Execute a single step
    async fn execute_step(&self, step: ExecutionStep) -> Result<Vec<ResultItem>> {
        match step {
            ExecutionStep::VectorSearch { embedding, k } => {
                self.execute_vector_search(embedding, k).await
            }

            ExecutionStep::GraphTraversal {
                start,
                max_depth,
                limit,
            } => self.execute_graph_traversal(start, max_depth, limit),

            ExecutionStep::TagSearch {
                tokens,
                mode,
                limit,
            } => self.execute_tag_search(tokens, mode, limit),
        }
    }

    /// Execute vector search
    async fn execute_vector_search(
        &self,
        embedding: Vec<f32>,
        k: usize,
    ) -> Result<Vec<ResultItem>> {
        let search_results = self.engine.vector_search(&embedding, k).await?;

        Ok(search_results
            .into_iter()
            .map(|r| {
                ResultItem::new(
                    r.key,
                    ScoreNormalizer::normalize_vector_distance(r.distance),
                )
            })
            .collect())
    }

    /// Execute graph traversal
    fn execute_graph_traversal(
        &self,
        start: Bytes,
        max_depth: usize,
        limit: usize,
    ) -> Result<Vec<ResultItem>> {
        let traversal_results = self.engine.traverse_graph(&start, max_depth, None)?;

        let mut items: Vec<ResultItem> = traversal_results
            .into_iter()
            .filter(|r| r.node_id != start) // Exclude start node
            .map(|r| ResultItem::new(r.node_id, ScoreNormalizer::normalize_graph_depth(r.depth)))
            .collect();

        items.truncate(limit);

        Ok(items)
    }

    /// Execute tag search
    fn execute_tag_search(
        &self,
        tokens: Vec<String>,
        mode: BooleanMode,
        limit: usize,
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
            .map(|(key, score)| ResultItem::new(key, score))
            .collect();

        // Normalize tag scores before they are handed to rank fusion
        ScoreNormalizer::normalize_tag_scores(&mut items);

        items.truncate(limit);

        Ok(items)
    }
}
