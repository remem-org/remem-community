//! Query Planner for optimizing and planning query execution
#![allow(dead_code)]
//!
//! The planner analyzes queries and creates execution plans that:
//! - Choose the best index to use
//! - Determine filter application order
//! - Plan multi-index query execution

use std::collections::HashSet;

use bytes::Bytes;

use crate::engine::error::{Result, StorageError};
use crate::engine::storage::StorageEngine;

use super::types::*;

/// Execution plan for a query
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    /// Steps to execute in order
    pub steps: Vec<ExecutionStep>,

    /// Final merge strategy (for multi-step plans)
    pub merge: Option<MergeStep>,

    /// Estimated cost (lower is better)
    pub estimated_cost: f64,

    /// Limit to apply at the end
    pub final_limit: Option<usize>,
}

impl ExecutionPlan {
    /// Create a simple single-step plan
    pub fn single(step: ExecutionStep) -> Self {
        let cost = step.estimated_cost();
        Self {
            steps: vec![step],
            merge: None,
            estimated_cost: cost,
            final_limit: None,
        }
    }

    /// Create a multi-step plan with merging
    pub fn multi(steps: Vec<ExecutionStep>, merge: MergeStep) -> Self {
        let cost: f64 = steps.iter().map(|s| s.estimated_cost()).sum();
        Self {
            steps,
            merge: Some(merge),
            estimated_cost: cost,
            final_limit: None,
        }
    }

    /// Set the final result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.final_limit = Some(limit);
        self
    }

    /// Check if this is a hybrid (multi-step) plan
    pub fn is_hybrid(&self) -> bool {
        self.steps.len() > 1
    }
}

/// A single execution step
#[derive(Debug, Clone)]
pub enum ExecutionStep {
    /// Vector similarity search
    VectorSearch {
        embedding: Vec<f32>,
        k: usize,
        ef: Option<usize>,
        include_values: bool,
    },

    /// Graph traversal
    GraphTraversal {
        start: Bytes,
        max_depth: usize,
        edge_types: Option<Vec<String>>,
        limit: Option<usize>,
    },

    /// Time range scan
    TimeRangeScan {
        start: u64,
        end: u64,
        limit: Option<usize>,
        descending: bool,
    },

    /// Tag search
    TagSearch {
        tokens: Vec<String>,
        mode: BooleanMode,
        limit: Option<usize>,
    },

    /// Filter step (applies predicates to results from previous step)
    Filter { predicates: Vec<Predicate> },

    /// Fetch values for keys
    FetchValues { keys: Vec<Bytes> },
}

impl ExecutionStep {
    /// Estimate the cost of this step (lower is better)
    fn estimated_cost(&self) -> f64 {
        match self {
            ExecutionStep::VectorSearch { k, .. } => {
                // HNSW is O(log n) but we estimate based on k
                10.0 + (*k as f64) * 0.5
            }
            ExecutionStep::GraphTraversal { max_depth, .. } => {
                // BFS cost grows exponentially with depth
                20.0 * (2.0_f64.powi(*max_depth as i32))
            }
            ExecutionStep::TimeRangeScan { start, end, .. } => {
                // Cost depends on range size (estimated)
                5.0 + ((end - start) as f64 / 1000.0).min(100.0)
            }
            ExecutionStep::TagSearch { tokens, mode, .. } => {
                // AND is faster (intersection), OR is slower (union)
                let base = tokens.len() as f64 * 5.0;
                match mode {
                    BooleanMode::And => base,
                    BooleanMode::Or => base * 2.0,
                }
            }
            ExecutionStep::Filter { predicates } => {
                // Filter is cheap
                1.0 + predicates.len() as f64 * 0.1
            }
            ExecutionStep::FetchValues { keys } => {
                // Fetch is proportional to key count
                keys.len() as f64 * 0.5
            }
        }
    }
}

/// Merge step for combining results from multiple execution steps
#[derive(Debug, Clone)]
pub struct MergeStep {
    /// Strategy for merging
    pub strategy: MergeStrategyType,

    /// RRF k parameter
    pub rrf_k: usize,

    /// Component weights (for weighted sum)
    pub weights: Option<ComponentWeights>,

    /// Limit after merging
    pub limit: usize,
}

impl MergeStep {
    /// Create an RRF merge step
    pub fn rrf(k: usize, limit: usize) -> Self {
        Self {
            strategy: MergeStrategyType::Rrf,
            rrf_k: k,
            weights: None,
            limit,
        }
    }

    /// Create a weighted sum merge step
    pub fn weighted(weights: ComponentWeights, limit: usize) -> Self {
        Self {
            strategy: MergeStrategyType::WeightedSum,
            rrf_k: 60,
            weights: Some(weights),
            limit,
        }
    }

    /// Create an intersection merge step
    pub fn intersection(limit: usize) -> Self {
        Self {
            strategy: MergeStrategyType::Intersection,
            rrf_k: 60,
            weights: None,
            limit,
        }
    }
}

/// Query planner that creates execution plans from queries
pub struct QueryPlanner {
    /// Default RRF k parameter
    default_rrf_k: usize,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new() -> Self {
        Self { default_rrf_k: 60 }
    }

    /// Create a new planner with custom RRF k
    pub fn with_rrf_k(mut self, k: usize) -> Self {
        self.default_rrf_k = k;
        self
    }

    /// Plan a query execution
    pub fn plan(&self, query: &Query, engine: &StorageEngine) -> Result<ExecutionPlan> {
        match query {
            Query::Vector(vq) => self.plan_vector(vq, engine),
            Query::Graph(gq) => self.plan_graph(gq, engine),
            Query::TimeRange(tq) => self.plan_time_range(tq, engine),
            Query::Tag(tq) => self.plan_tag(tq, engine),
            Query::Hybrid(hq) => self.plan_hybrid(hq, engine),
        }
    }

    /// Plan a vector search query
    fn plan_vector(&self, query: &VectorQuery, engine: &StorageEngine) -> Result<ExecutionPlan> {
        // Check if vector search is available
        if !engine.vector_enabled() {
            return Err(StorageError::IndexNotEnabled("vector".to_string()));
        }

        let mut steps = vec![ExecutionStep::VectorSearch {
            embedding: query.embedding.clone(),
            k: query.k,
            ef: query.ef,
            include_values: query.include_values,
        }];

        // Add filter step if needed
        if let Some(filter) = &query.filter {
            if !filter.is_empty() {
                steps.push(ExecutionStep::Filter {
                    predicates: filter.predicates.clone(),
                });
            }
        }

        Ok(ExecutionPlan::single(steps.remove(0)).with_limit(query.k))
    }

    /// Plan a graph traversal query
    fn plan_graph(&self, query: &GraphQuery, engine: &StorageEngine) -> Result<ExecutionPlan> {
        // Check if graph is available
        if !engine.graph_enabled() {
            return Err(StorageError::IndexNotEnabled("graph".to_string()));
        }

        let step = ExecutionStep::GraphTraversal {
            start: query.start_node.clone(),
            max_depth: query.max_depth,
            edge_types: query.edge_types.clone(),
            limit: query.limit,
        };

        let mut plan = ExecutionPlan::single(step);

        if let Some(limit) = query.limit {
            plan = plan.with_limit(limit);
        }

        Ok(plan)
    }

    /// Plan a time range query
    fn plan_time_range(
        &self,
        query: &TimeRangeQuery,
        engine: &StorageEngine,
    ) -> Result<ExecutionPlan> {
        // Check if time series is available
        if !engine.time_series_enabled() {
            return Err(StorageError::IndexNotEnabled("time_series".to_string()));
        }

        let step = ExecutionStep::TimeRangeScan {
            start: query.start,
            end: query.end,
            limit: query.limit,
            descending: query.order == SortOrder::Descending,
        };

        let mut plan = ExecutionPlan::single(step);

        if let Some(limit) = query.limit {
            plan = plan.with_limit(limit);
        }

        Ok(plan)
    }

    /// Plan a tag search query
    fn plan_tag(&self, query: &TagQuery, engine: &StorageEngine) -> Result<ExecutionPlan> {
        // Check if tag index is available
        if !engine.tag_enabled() {
            return Err(StorageError::IndexNotEnabled("tag".to_string()));
        }

        let step = ExecutionStep::TagSearch {
            tokens: query.tokens.clone(),
            mode: query.mode,
            limit: query.limit,
        };

        let mut plan = ExecutionPlan::single(step);

        if let Some(limit) = query.limit {
            plan = plan.with_limit(limit);
        }

        Ok(plan)
    }

    /// Plan a hybrid query
    fn plan_hybrid(&self, query: &HybridQuery, engine: &StorageEngine) -> Result<ExecutionPlan> {
        let mut steps = Vec::new();
        let mut available_sources = HashSet::new();

        // Plan vector search component
        if let Some(vq) = &query.vector {
            if engine.vector_enabled() {
                steps.push(ExecutionStep::VectorSearch {
                    embedding: vq.embedding.clone(),
                    k: vq.k,
                    ef: vq.ef,
                    include_values: vq.include_values,
                });
                available_sources.insert("vector");
            }
        }

        // Plan tag search component
        if let Some(tq) = &query.tag {
            if engine.tag_enabled() {
                steps.push(ExecutionStep::TagSearch {
                    tokens: tq.tokens.clone(),
                    mode: tq.mode,
                    limit: Some(query.limit * 2), // Get more candidates for merging
                });
                available_sources.insert("tag");
            }
        }

        // Plan graph context component
        if let Some(gc) = &query.graph_context {
            if engine.graph_enabled() {
                steps.push(ExecutionStep::GraphTraversal {
                    start: gc.node.clone(),
                    max_depth: gc.max_depth,
                    edge_types: gc.edge_types.clone(),
                    limit: Some(query.limit * 2),
                });
                available_sources.insert("graph");
            }
        }

        // Add time range scan if specified
        if let Some((start, end)) = query.time_range {
            if engine.time_series_enabled() {
                steps.push(ExecutionStep::TimeRangeScan {
                    start,
                    end,
                    limit: Some(query.limit * 2),
                    descending: true,
                });
                available_sources.insert("time");
            }
        }

        // Ensure we have at least one source
        if steps.is_empty() {
            return Err(StorageError::InvalidArgument(
                "Hybrid query requires at least one valid search component".to_string(),
            ));
        }

        // If only one step, no merging needed
        if steps.len() == 1 {
            return Ok(ExecutionPlan::single(steps.remove(0)).with_limit(query.limit));
        }

        // Create merge step based on strategy
        let merge = match query.merge_strategy {
            MergeStrategyType::Rrf => MergeStep::rrf(self.default_rrf_k, query.limit),
            MergeStrategyType::WeightedSum => {
                let weights = query.weights.clone().unwrap_or_default();
                MergeStep::weighted(weights, query.limit)
            }
            MergeStrategyType::Intersection => MergeStep::intersection(query.limit),
            MergeStrategyType::Union => MergeStep {
                strategy: MergeStrategyType::Union,
                rrf_k: self.default_rrf_k,
                weights: None,
                limit: query.limit,
            },
        };

        Ok(ExecutionPlan::multi(steps, merge).with_limit(query.limit))
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_step_cost() {
        let vector_step = ExecutionStep::VectorSearch {
            embedding: vec![0.1; 384],
            k: 10,
            ef: None,
            include_values: true,
        };
        assert!(vector_step.estimated_cost() > 0.0);

        let graph_step = ExecutionStep::GraphTraversal {
            start: Bytes::from("node1"),
            max_depth: 3,
            edge_types: None,
            limit: None,
        };
        // Depth 3 should be more expensive than depth 1
        let graph_step_shallow = ExecutionStep::GraphTraversal {
            start: Bytes::from("node1"),
            max_depth: 1,
            edge_types: None,
            limit: None,
        };
        assert!(graph_step.estimated_cost() > graph_step_shallow.estimated_cost());
    }

    #[test]
    fn test_plan_creation() {
        let step = ExecutionStep::VectorSearch {
            embedding: vec![0.1; 10],
            k: 5,
            ef: None,
            include_values: true,
        };

        let plan = ExecutionPlan::single(step).with_limit(5);
        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.final_limit, Some(5));
        assert!(!plan.is_hybrid());
    }

    #[test]
    fn test_merge_step() {
        let merge = MergeStep::rrf(60, 10);
        assert_eq!(merge.strategy, MergeStrategyType::Rrf);
        assert_eq!(merge.rrf_k, 60);
        assert_eq!(merge.limit, 10);
    }
}
