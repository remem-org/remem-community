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
pub use types::{BooleanMode, HybridQuery, QueryResult, ResultItem};

use std::sync::Arc;

use crate::engine::error::Result;
use crate::engine::storage::StorageEngine;

/// Configuration for the query engine
#[derive(Debug, Clone)]
pub struct QueryEngineConfig {
    /// RRF k parameter used when fusing results from multiple indexes
    pub rrf_k: usize,
}

impl Default for QueryEngineConfig {
    fn default() -> Self {
        Self { rrf_k: 60 }
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
        let planner = QueryPlanner::new(config.rrf_k);
        let executor = QueryExecutor::new(Arc::clone(&engine));

        Self {
            engine,
            planner,
            executor,
        }
    }

    /// Plan and execute a hybrid query
    pub async fn execute(&self, query: HybridQuery) -> Result<QueryResult> {
        let plan = self.planner.plan(&query, &self.engine)?;
        self.executor.execute(plan).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default_rrf_k() {
        assert_eq!(QueryEngineConfig::default().rrf_k, 60);
    }
}
