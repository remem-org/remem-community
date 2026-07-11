//! Query Engine for remem-storage
#![allow(unused_imports, dead_code)]
//!
//! This module provides a unified query interface that supports:
//! - Vector similarity search
//! - Graph traversal
//! - Time-series range queries
//! - Tag/text search
//! - Hybrid queries combining multiple index types
//! - RRF (Reciprocal Rank Fusion) for score normalization

pub mod executor;
pub mod merge;
pub mod planner;
pub mod types;

pub use executor::QueryExecutor;
pub use merge::{MergeStrategy, RrfMerger, ScoreNormalizer};
pub use planner::{ExecutionPlan, QueryPlanner};
pub use types::{
    BooleanMode, ComponentWeights, Filter, GraphQuery, HybridQuery, MergeStrategyType, Predicate,
    Query, QueryResult, ResultExtra, ResultItem, ResultSource, TagQuery, TimeRangeQuery,
    VectorQuery,
};

use std::sync::Arc;

use crate::engine::error::Result;
use crate::engine::storage::StorageEngine;

/// Configuration for the query engine
#[derive(Debug, Clone)]
pub struct QueryEngineConfig {
    /// Default number of results for vector search
    pub default_vector_k: usize,

    /// Default ef parameter for HNSW search
    pub default_ef: usize,

    /// Default RRF k parameter
    pub rrf_k: usize,

    /// Maximum results to return
    pub max_results: usize,

    /// Enable query caching
    pub cache_enabled: bool,

    /// Cache size in entries
    pub cache_size: usize,
}

impl Default for QueryEngineConfig {
    fn default() -> Self {
        Self {
            default_vector_k: 10,
            default_ef: 50,
            rrf_k: 60,
            max_results: 1000,
            cache_enabled: false,
            cache_size: 1000,
        }
    }
}

impl QueryEngineConfig {
    /// Create a new configuration with specified vector k
    pub fn with_default_k(mut self, k: usize) -> Self {
        self.default_vector_k = k;
        self
    }

    /// Create a new configuration with specified RRF k
    pub fn with_rrf_k(mut self, k: usize) -> Self {
        self.rrf_k = k;
        self
    }

    /// Create a new configuration with caching enabled
    pub fn with_cache(mut self, enabled: bool, size: usize) -> Self {
        self.cache_enabled = enabled;
        self.cache_size = size;
        self
    }
}

/// The main Query Engine that orchestrates query planning and execution
pub struct QueryEngine {
    /// Reference to the storage engine
    engine: Arc<StorageEngine>,

    /// Query planner
    planner: QueryPlanner,

    /// Query executor
    executor: QueryExecutor,

    /// Configuration
    config: QueryEngineConfig,
}

impl QueryEngine {
    /// Create a new query engine
    pub fn new(engine: Arc<StorageEngine>, config: QueryEngineConfig) -> Self {
        let planner = QueryPlanner::new();
        let executor = QueryExecutor::new(Arc::clone(&engine), config.clone());

        Self {
            engine,
            planner,
            executor,
            config,
        }
    }

    /// Execute a query and return results
    pub async fn execute(&self, query: Query) -> Result<QueryResult> {
        // Plan the query
        let plan = self.planner.plan(&query, &self.engine)?;

        // Execute the plan
        self.executor.execute(plan).await
    }

    /// Execute a vector search query
    pub async fn vector_search(&self, query: VectorQuery) -> Result<QueryResult> {
        self.execute(Query::Vector(query)).await
    }

    /// Execute a hybrid search query (vector + filters)
    pub async fn hybrid_search(&self, query: HybridQuery) -> Result<QueryResult> {
        self.execute(Query::Hybrid(query)).await
    }

    /// Execute a graph traversal query
    pub async fn graph_query(&self, query: GraphQuery) -> Result<QueryResult> {
        self.execute(Query::Graph(query)).await
    }

    /// Execute a time-range query
    pub async fn time_range(&self, query: TimeRangeQuery) -> Result<QueryResult> {
        self.execute(Query::TimeRange(query)).await
    }

    /// Execute a tag search query
    pub async fn tag_search(&self, query: TagQuery) -> Result<QueryResult> {
        self.execute(Query::Tag(query)).await
    }

    /// Get the configuration
    pub fn config(&self) -> &QueryEngineConfig {
        &self.config
    }

    /// Get statistics about the underlying indexes
    pub fn stats(&self) -> QueryEngineStats {
        let storage_stats = self.engine.stats();
        QueryEngineStats {
            vector_enabled: storage_stats.vector_enabled,
            vector_count: storage_stats.vector_count,
            graph_enabled: storage_stats.graph_enabled,
            graph_node_count: storage_stats.graph_node_count,
            graph_edge_count: storage_stats.graph_edge_count,
            time_series_enabled: storage_stats.time_series_enabled,
            time_series_count: storage_stats.time_series_count,
            tag_enabled: storage_stats.tag_enabled,
            tag_doc_count: storage_stats.tag_doc_count,
        }
    }
}

/// Statistics about the query engine and underlying indexes
#[derive(Debug, Clone)]
pub struct QueryEngineStats {
    pub vector_enabled: bool,
    pub vector_count: usize,
    pub graph_enabled: bool,
    pub graph_node_count: usize,
    pub graph_edge_count: usize,
    pub time_series_enabled: bool,
    pub time_series_count: usize,
    pub tag_enabled: bool,
    pub tag_doc_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = QueryEngineConfig::default();
        assert_eq!(config.default_vector_k, 10);
        assert_eq!(config.rrf_k, 60);
        assert!(!config.cache_enabled);
    }

    #[test]
    fn test_config_builder() {
        let config = QueryEngineConfig::default()
            .with_default_k(20)
            .with_rrf_k(100)
            .with_cache(true, 5000);

        assert_eq!(config.default_vector_k, 20);
        assert_eq!(config.rrf_k, 100);
        assert!(config.cache_enabled);
        assert_eq!(config.cache_size, 5000);
    }
}
