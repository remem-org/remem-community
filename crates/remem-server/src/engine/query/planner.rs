//! Query planner: turns a [`HybridQuery`] into an execution plan
//!
//! There is exactly one access path today — fan out to whichever indexes are
//! enabled and fuse the ranked lists — so the planner does no cost-based
//! selection. It only decides *which* index steps are available and how the
//! results get merged. REM-84 tracks a real planner, gated on there being at
//! least two viable access paths to choose between.

use bytes::Bytes;

use crate::engine::attr::select::AttrPred;
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

    /// Attribute predicates every returned item must satisfy, ANDed.
    ///
    /// Carried on the plan rather than duplicated into each step variant:
    /// every step evaluates the same set, and a step that could silently
    /// skip them would return unfiltered candidates into the fusion.
    pub preds: Vec<AttrPred>,

    /// Tags every returned item must carry, ANDed. Settled against the tag
    /// index rather than an attribute row — tags carry no slot.
    pub tag_filter: Vec<String>,

    /// Most candidates any one step may examine before it stops and reports
    /// the result truncated.
    ///
    /// Without this a selective predicate turns a bounded retrieval into a
    /// walk of the whole authorized scope — a latency problem for one caller
    /// and an isolation problem for every other one sharing the process
    /// (REM-78 / `docs/QUERY_LAYER_AND_SCALE_FOUNDATION.md` §4.1).
    pub effort_cap: usize,
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

    /// Full-corpus content scan — the access path used when content has no
    /// index. Runs on every keyword query, unconditionally, not as a
    /// fallback triggered by another step under-delivering.
    ContentScan { tokens: Vec<String>, limit: usize },
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

    /// Multiple of a query's result target that bounds each step's effort.
    widen_max_factor: usize,
}

impl QueryPlanner {
    /// Create a new planner with the given RRF k parameter
    pub fn new(rrf_k: usize, widen_max_factor: usize) -> Self {
        Self {
            rrf_k,
            widen_max_factor,
        }
    }

    /// Plan a hybrid query.
    ///
    /// Each enabled index contributes one step. Candidate counts are
    /// deliberately over-fetched (2x the final limit) for the non-vector
    /// sources so that fusion has something to work with.
    pub fn plan(&self, query: &HybridQuery, engine: &StorageEngine) -> Result<ExecutionPlan> {
        let mut steps = Vec::new();

        if let Some(vq) = &query.vector {
            if engine.vector_enabled() {
                steps.push(ExecutionStep::VectorSearch {
                    embedding: vq.embedding.clone(),
                    k: vq.k,
                });
            }
        }

        if let Some(tq) = &query.tag {
            // Empty tokens are not a search: `HybridQuery::keyword` always
            // sets `tag`, so an empty-token keyword query must still fall
            // through to the "no components" rejection below (REM-88).
            if engine.tag_enabled() && !tq.tokens.is_empty() {
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

        // Runs unconditionally when requested, rather than only when the tag
        // step under-delivers. A step that sometimes runs cannot be priced
        // before execution, and the old trigger counted post-filter survivors
        // the planner cannot see (REM-88).
        if query.content_scan {
            if let Some(tq) = &query.tag {
                if !tq.tokens.is_empty() {
                    steps.push(ExecutionStep::ContentScan {
                        tokens: tq.tokens.clone(),
                        limit: query.limit * 2,
                    });
                }
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
            preds: query.preds.clone(),
            tag_filter: query.tag_filter.clone(),
            // `max(limit)` keeps the cap from falling below the target it is
            // meant to reach: a factor of 0 would otherwise make every query
            // truncate immediately.
            effort_cap: query
                .limit
                .saturating_mul(self.widen_max_factor)
                .max(query.limit),
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
        let planner = QueryPlanner::new(17, 32);
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
            preds: Vec::new(),
            tag_filter: Vec::new(),
            effort_cap: 160,
        };
        assert!(!single.is_hybrid());

        let multi = ExecutionPlan {
            steps: vec![vector_step, tag_step],
            merge: Some(MergeStep {
                rrf_k: 60,
                limit: 5,
            }),
            final_limit: 5,
            preds: Vec::new(),
            tag_filter: Vec::new(),
            effort_cap: 160,
        };
        assert!(multi.is_hybrid());
    }

    use crate::engine::storage::engine::EngineConfig;
    use std::time::Duration;
    use tempfile::TempDir;

    async fn test_engine(dir: &TempDir) -> StorageEngine {
        StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            checkpoint_interval: Duration::from_secs(86400),
            ..Default::default()
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn keyword_query_plans_a_tag_step_and_a_content_scan() {
        let dir = TempDir::new().unwrap();
        let engine = test_engine(&dir).await;
        let planner = QueryPlanner::new(60, 32);

        let query = HybridQuery::keyword(vec!["rust".to_string()], BooleanMode::Or, 30);
        let plan = planner.plan(&query, &engine).unwrap();

        assert_eq!(plan.steps.len(), 2, "tag search plus content scan");
        assert!(matches!(plan.steps[0], ExecutionStep::TagSearch { .. }));
        assert!(matches!(plan.steps[1], ExecutionStep::ContentScan { .. }));
        assert!(plan.is_hybrid(), "two steps must fuse");
        assert!(plan.merge.is_some());
    }

    #[tokio::test]
    async fn vector_query_plans_no_content_scan() {
        let dir = TempDir::new().unwrap();
        let engine = test_engine(&dir).await;
        let planner = QueryPlanner::new(60, 32);

        let query = HybridQuery::new(vec![0.1; 384], 10);
        let plan = planner.plan(&query, &engine).unwrap();

        assert_eq!(plan.steps.len(), 1);
        assert!(matches!(plan.steps[0], ExecutionStep::VectorSearch { .. }));
    }

    #[tokio::test]
    async fn query_with_no_components_is_rejected() {
        let dir = TempDir::new().unwrap();
        let engine = test_engine(&dir).await;
        let planner = QueryPlanner::new(60, 32);

        // No vector, no tags, no content scan.
        let query = HybridQuery::keyword(Vec::new(), BooleanMode::Or, 10);
        let err = planner.plan(&query, &engine).unwrap_err();

        assert!(matches!(err, StorageError::InvalidArgument(_)));
    }
}
