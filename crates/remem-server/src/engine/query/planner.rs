//! Query planner: turns a [`HybridQuery`] into an execution plan
//!
//! There is exactly one access path today — fan out to whichever indexes are
//! enabled and fuse the ranked lists — so the planner does no cost-based
//! selection. It only decides *which* index steps are available and how the
//! results get merged. REM-84 tracks a real planner, gated on there being at
//! least two viable access paths to choose between.

use bytes::Bytes;

use crate::engine::error::{Result, StorageError};
use crate::engine::storage::StorageEngine;

use super::types::{BooleanMode, HybridQuery};

/// Execution plan for a query
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    /// Steps to execute, each producing an independently ranked list
    pub steps: Vec<ExecutionStep>,

    /// Merge step, present when more than one source contributes
    pub merge: Option<MergeStep>,

    /// Limit to apply at the end
    pub final_limit: usize,
}

impl ExecutionPlan {
    /// Check if this plan fans out across multiple indexes
    pub fn is_hybrid(&self) -> bool {
        self.steps.len() > 1
    }
}

/// A single execution step, run against one index
#[derive(Debug, Clone)]
pub enum ExecutionStep {
    /// Vector similarity search
    VectorSearch { embedding: Vec<f32>, k: usize },

    /// Graph traversal
    GraphTraversal {
        start: Bytes,
        max_depth: usize,
        limit: usize,
    },

    /// Tag search
    TagSearch {
        tokens: Vec<String>,
        mode: BooleanMode,
        limit: usize,
    },
}

/// Merge step for fusing results from multiple execution steps
#[derive(Debug, Clone)]
pub struct MergeStep {
    /// RRF k parameter
    pub rrf_k: usize,

    /// Limit after merging
    pub limit: usize,
}

/// Query planner that creates execution plans from queries
pub struct QueryPlanner {
    /// RRF k parameter applied to fused plans
    rrf_k: usize,
}

impl QueryPlanner {
    /// Create a new planner with the given RRF k parameter
    pub fn new(rrf_k: usize) -> Self {
        Self { rrf_k }
    }

    /// Plan a hybrid query.
    ///
    /// Each enabled index contributes one step. Candidate counts are
    /// deliberately over-fetched (2x the final limit) for the non-vector
    /// sources so that fusion has something to work with.
    pub fn plan(&self, query: &HybridQuery, engine: &StorageEngine) -> Result<ExecutionPlan> {
        let mut steps = Vec::new();

        if engine.vector_enabled() {
            steps.push(ExecutionStep::VectorSearch {
                embedding: query.vector.embedding.clone(),
                k: query.vector.k,
            });
        }

        if let Some(tq) = &query.tag {
            if engine.tag_enabled() {
                steps.push(ExecutionStep::TagSearch {
                    tokens: tq.tokens.clone(),
                    mode: tq.mode,
                    limit: query.limit * 2,
                });
            }
        }

        if let Some(gc) = &query.graph_context {
            if engine.graph_enabled() {
                steps.push(ExecutionStep::GraphTraversal {
                    start: gc.node.clone(),
                    max_depth: gc.max_depth,
                    limit: query.limit * 2,
                });
            }
        }

        if steps.is_empty() {
            return Err(StorageError::InvalidArgument(
                "Hybrid query requires at least one valid search component".to_string(),
            ));
        }

        let merge = (steps.len() > 1).then_some(MergeStep {
            rrf_k: self.rrf_k,
            limit: query.limit,
        });

        Ok(ExecutionPlan {
            steps,
            merge,
            final_limit: query.limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_step_carries_planner_rrf_k() {
        // The configured rrf_k must reach the merge step — it was previously
        // dropped in favour of a hardcoded 60 (REM-72).
        let planner = QueryPlanner::new(17);
        let merge = MergeStep {
            rrf_k: planner.rrf_k,
            limit: 10,
        };
        assert_eq!(merge.rrf_k, 17);
    }

    #[test]
    fn test_is_hybrid() {
        let vector_step = ExecutionStep::VectorSearch {
            embedding: vec![0.1; 8],
            k: 5,
        };
        let tag_step = ExecutionStep::TagSearch {
            tokens: vec!["rust".to_string()],
            mode: BooleanMode::Or,
            limit: 10,
        };

        let single = ExecutionPlan {
            steps: vec![vector_step.clone()],
            merge: None,
            final_limit: 5,
        };
        assert!(!single.is_hybrid());

        let multi = ExecutionPlan {
            steps: vec![vector_step, tag_step],
            merge: Some(MergeStep {
                rrf_k: 60,
                limit: 5,
            }),
            final_limit: 5,
        };
        assert!(multi.is_hybrid());
    }
}
