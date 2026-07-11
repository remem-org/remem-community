//! Query types and filter DSL for the query engine
#![allow(dead_code)]
//!
//! This module defines the query language for remem-storage, including:
//! - Query variants (Vector, Graph, TimeRange, Tag, Hybrid)
//! - Filter predicates for narrowing results
//! - Result types

use bytes::Bytes;
use std::collections::HashMap;

/// The main query type that represents all possible queries
#[derive(Debug, Clone)]
pub enum Query {
    /// Vector similarity search
    Vector(VectorQuery),

    /// Graph traversal query
    Graph(GraphQuery),

    /// Time-series range query
    TimeRange(TimeRangeQuery),

    /// Tag/text search query
    Tag(TagQuery),

    /// Hybrid query combining multiple search types
    Hybrid(HybridQuery),
}

/// Vector similarity search query
#[derive(Debug, Clone)]
pub struct VectorQuery {
    /// Query embedding vector
    pub embedding: Vec<f32>,

    /// Number of results to return
    pub k: usize,

    /// Optional ef parameter for HNSW search quality
    pub ef: Option<usize>,

    /// Optional filters to apply post-search
    pub filter: Option<Filter>,

    /// Whether to include values in results
    pub include_values: bool,

    /// Whether to include metadata in results
    pub include_metadata: bool,
}

impl VectorQuery {
    /// Create a new vector query
    pub fn new(embedding: Vec<f32>, k: usize) -> Self {
        Self {
            embedding,
            k,
            ef: None,
            filter: None,
            include_values: true,
            include_metadata: false,
        }
    }

    /// Set the ef parameter for search quality
    pub fn with_ef(mut self, ef: usize) -> Self {
        self.ef = Some(ef);
        self
    }

    /// Add a filter to the query
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Include values in results
    pub fn with_values(mut self, include: bool) -> Self {
        self.include_values = include;
        self
    }
}

/// Graph traversal query
#[derive(Debug, Clone)]
pub struct GraphQuery {
    /// Starting node key
    pub start_node: Bytes,

    /// Maximum traversal depth
    pub max_depth: usize,

    /// Edge types to follow (None = all types)
    pub edge_types: Option<Vec<String>>,

    /// Direction of traversal
    pub direction: GraphDirection,

    /// Maximum number of results
    pub limit: Option<usize>,

    /// Optional filter for nodes
    pub filter: Option<Filter>,
}

impl GraphQuery {
    /// Create a new graph query
    pub fn new(start_node: impl Into<Bytes>, max_depth: usize) -> Self {
        Self {
            start_node: start_node.into(),
            max_depth,
            edge_types: None,
            direction: GraphDirection::Outgoing,
            limit: None,
            filter: None,
        }
    }

    /// Filter by edge types
    pub fn with_edge_types(mut self, types: Vec<String>) -> Self {
        self.edge_types = Some(types);
        self
    }

    /// Set traversal direction
    pub fn with_direction(mut self, direction: GraphDirection) -> Self {
        self.direction = direction;
        self
    }

    /// Limit results
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Direction for graph traversal
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphDirection {
    /// Follow outgoing edges
    Outgoing,
    /// Follow incoming edges (requires bidirectional index)
    Incoming,
    /// Follow both directions
    Both,
}

/// Time-series range query
#[derive(Debug, Clone)]
pub struct TimeRangeQuery {
    /// Start timestamp (inclusive)
    pub start: u64,

    /// End timestamp (inclusive)
    pub end: u64,

    /// Maximum number of results
    pub limit: Option<usize>,

    /// Sort order
    pub order: SortOrder,

    /// Optional filter for results
    pub filter: Option<Filter>,

    /// Whether to include values in results
    pub include_values: bool,
}

impl TimeRangeQuery {
    /// Create a new time-range query
    pub fn new(start: u64, end: u64) -> Self {
        Self {
            start,
            end,
            limit: None,
            order: SortOrder::Ascending,
            filter: None,
            include_values: false,
        }
    }

    /// Limit results
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set sort order
    pub fn with_order(mut self, order: SortOrder) -> Self {
        self.order = order;
        self
    }

    /// Include values in results
    pub fn with_values(mut self, include: bool) -> Self {
        self.include_values = include;
        self
    }
}

/// Sort order for results
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    /// Oldest first
    Ascending,
    /// Newest first
    Descending,
}

/// Tag/text search query
#[derive(Debug, Clone)]
pub struct TagQuery {
    /// Tags or text tokens to search for
    pub tokens: Vec<String>,

    /// Boolean mode for combining tokens
    pub mode: BooleanMode,

    /// Maximum number of results
    pub limit: Option<usize>,

    /// Whether to include scores
    pub include_scores: bool,

    /// Optional additional filter
    pub filter: Option<Filter>,
}

impl TagQuery {
    /// Create a new tag query with AND mode
    pub fn and(tokens: Vec<String>) -> Self {
        Self {
            tokens,
            mode: BooleanMode::And,
            limit: None,
            include_scores: true,
            filter: None,
        }
    }

    /// Create a new tag query with OR mode
    pub fn or(tokens: Vec<String>) -> Self {
        Self {
            tokens,
            mode: BooleanMode::Or,
            limit: None,
            include_scores: true,
            filter: None,
        }
    }

    /// Limit results
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Boolean mode for combining search terms
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BooleanMode {
    /// All tokens must match
    And,
    /// Any token can match
    Or,
}

/// Hybrid query combining multiple search types with score fusion
#[derive(Debug, Clone)]
pub struct HybridQuery {
    /// Vector search component (optional)
    pub vector: Option<VectorQuery>,

    /// Tag search component (optional)
    pub tag: Option<TagQuery>,

    /// Time range filter (optional)
    pub time_range: Option<(u64, u64)>,

    /// Graph context (optional): find items related to this node
    pub graph_context: Option<GraphContext>,

    /// Strategy for merging results
    pub merge_strategy: MergeStrategyType,

    /// Maximum number of final results
    pub limit: usize,

    /// Weights for different components (for weighted merging)
    pub weights: Option<ComponentWeights>,
}

impl HybridQuery {
    /// Create a new hybrid query with vector search
    pub fn vector(embedding: Vec<f32>, k: usize) -> Self {
        Self {
            vector: Some(VectorQuery::new(embedding, k)),
            tag: None,
            time_range: None,
            graph_context: None,
            merge_strategy: MergeStrategyType::Rrf,
            limit: k,
            weights: None,
        }
    }

    /// Add tag search component
    pub fn with_tags(mut self, tokens: Vec<String>, mode: BooleanMode) -> Self {
        self.tag = Some(if mode == BooleanMode::And {
            TagQuery::and(tokens)
        } else {
            TagQuery::or(tokens)
        });
        self
    }

    /// Add time range filter
    pub fn with_time_range(mut self, start: u64, end: u64) -> Self {
        self.time_range = Some((start, end));
        self
    }

    /// Add graph context
    pub fn with_graph_context(mut self, node: impl Into<Bytes>, max_depth: usize) -> Self {
        self.graph_context = Some(GraphContext {
            node: node.into(),
            max_depth,
            edge_types: None,
        });
        self
    }

    /// Set merge strategy
    pub fn with_merge_strategy(mut self, strategy: MergeStrategyType) -> Self {
        self.merge_strategy = strategy;
        self
    }

    /// Set result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Set component weights
    pub fn with_weights(mut self, weights: ComponentWeights) -> Self {
        self.weights = Some(weights);
        self
    }
}

/// Graph context for hybrid queries
#[derive(Debug, Clone)]
pub struct GraphContext {
    /// Node to find related items from
    pub node: Bytes,
    /// Maximum depth for relationship
    pub max_depth: usize,
    /// Edge types to follow
    pub edge_types: Option<Vec<String>>,
}

/// Strategy for merging results from multiple sources
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeStrategyType {
    /// Reciprocal Rank Fusion
    Rrf,
    /// Weighted combination of normalized scores
    WeightedSum,
    /// Intersection (only items in all results)
    Intersection,
    /// Union (all items from all results)
    Union,
}

/// Weights for different query components in hybrid search
#[derive(Debug, Clone)]
pub struct ComponentWeights {
    /// Weight for vector search results
    pub vector: f32,
    /// Weight for tag search results
    pub tag: f32,
    /// Weight for graph context results
    pub graph: f32,
}

impl Default for ComponentWeights {
    fn default() -> Self {
        Self {
            vector: 1.0,
            tag: 1.0,
            graph: 0.5,
        }
    }
}

/// Filter for narrowing query results
#[derive(Debug, Clone)]
pub struct Filter {
    /// Predicates combined with AND
    pub predicates: Vec<Predicate>,
}

impl Filter {
    /// Create a new empty filter
    pub fn new() -> Self {
        Self {
            predicates: Vec::new(),
        }
    }

    /// Add a predicate to the filter
    pub fn and(mut self, predicate: Predicate) -> Self {
        self.predicates.push(predicate);
        self
    }

    /// Create a filter requiring specific tags
    pub fn has_tags(tags: Vec<String>) -> Self {
        Self {
            predicates: vec![Predicate::HasTags(tags)],
        }
    }

    /// Create a filter for time range
    pub fn time_range(start: u64, end: u64) -> Self {
        Self {
            predicates: vec![Predicate::TimeRange { start, end }],
        }
    }

    /// Create a filter for graph relationship
    pub fn related_to(node: impl Into<Bytes>, max_depth: usize) -> Self {
        Self {
            predicates: vec![Predicate::RelatedTo {
                node: node.into(),
                max_depth,
            }],
        }
    }

    /// Check if the filter is empty
    pub fn is_empty(&self) -> bool {
        self.predicates.is_empty()
    }
}

impl Default for Filter {
    fn default() -> Self {
        Self::new()
    }
}

/// A single filter predicate
#[derive(Debug, Clone)]
pub enum Predicate {
    /// Must have all specified tags
    HasTags(Vec<String>),

    /// Must have any of the specified tags
    HasAnyTag(Vec<String>),

    /// Timestamp must be in range
    TimeRange { start: u64, end: u64 },

    /// Must be related to node within depth
    RelatedTo { node: Bytes, max_depth: usize },

    /// Key must match pattern (simple prefix matching)
    KeyPrefix(Bytes),

    /// Custom metadata filter
    Metadata { key: String, value: String },
}

/// Result of a query execution
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Result items
    pub items: Vec<ResultItem>,

    /// Total count (may be more than items if limited)
    pub total_count: Option<usize>,

    /// Execution time in milliseconds
    pub execution_time_ms: u64,

    /// Debug information about query execution
    pub debug_info: Option<QueryDebugInfo>,
}

impl QueryResult {
    /// Create a new query result
    pub fn new(items: Vec<ResultItem>) -> Self {
        Self {
            items,
            total_count: None,
            execution_time_ms: 0,
            debug_info: None,
        }
    }

    /// Create an empty result
    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Check if results are empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get keys from results
    pub fn keys(&self) -> Vec<Bytes> {
        self.items.iter().map(|item| item.key.clone()).collect()
    }
}

/// A single result item
#[derive(Debug, Clone)]
pub struct ResultItem {
    /// Key of the item
    pub key: Bytes,

    /// Relevance score (higher is better, normalized to 0-1)
    pub score: f32,

    /// Optional value
    pub value: Option<Bytes>,

    /// Optional metadata
    pub metadata: Option<HashMap<String, String>>,

    /// Source of this result (for hybrid queries)
    pub source: ResultSource,

    /// Additional information based on query type
    pub extra: Option<ResultExtra>,
}

impl ResultItem {
    /// Create a new result item
    pub fn new(key: Bytes, score: f32) -> Self {
        Self {
            key,
            score,
            value: None,
            metadata: None,
            source: ResultSource::Unknown,
            extra: None,
        }
    }

    /// Set the value
    pub fn with_value(mut self, value: Bytes) -> Self {
        self.value = Some(value);
        self
    }

    /// Set the source
    pub fn with_source(mut self, source: ResultSource) -> Self {
        self.source = source;
        self
    }
}

/// Source of a result item
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultSource {
    /// From vector search
    Vector,
    /// From graph traversal
    Graph,
    /// From time-series index
    TimeSeries,
    /// From tag/text search
    Tag,
    /// From multiple sources (hybrid)
    Hybrid,
    /// Unknown source
    Unknown,
}

/// Extra information for specific query types
#[derive(Debug, Clone)]
pub enum ResultExtra {
    /// Vector search extra info
    Vector {
        /// Raw distance from HNSW
        distance: f32,
    },

    /// Graph traversal extra info
    Graph {
        /// Depth from start node
        depth: usize,
        /// Edge type used to reach this node
        edge_type: Option<String>,
    },

    /// Time-series extra info
    TimeSeries {
        /// Timestamp of the record
        timestamp: u64,
    },

    /// Tag search extra info
    Tag {
        /// Matching tags
        matching_tags: Vec<String>,
    },
}

/// Debug information about query execution
#[derive(Debug, Clone)]
pub struct QueryDebugInfo {
    /// Execution plan used
    pub plan: String,

    /// Number of candidates from each source
    pub candidates_per_source: HashMap<String, usize>,

    /// Time spent in each phase (ms)
    pub phase_times: HashMap<String, u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_query_builder() {
        let query = VectorQuery::new(vec![0.1, 0.2, 0.3], 10)
            .with_ef(100)
            .with_values(true);

        assert_eq!(query.k, 10);
        assert_eq!(query.ef, Some(100));
        assert!(query.include_values);
    }

    #[test]
    fn test_hybrid_query_builder() {
        let query = HybridQuery::vector(vec![0.1, 0.2], 10)
            .with_tags(vec!["rust".to_string()], BooleanMode::And)
            .with_time_range(1000, 2000)
            .with_limit(20);

        assert!(query.vector.is_some());
        assert!(query.tag.is_some());
        assert_eq!(query.time_range, Some((1000, 2000)));
        assert_eq!(query.limit, 20);
    }

    #[test]
    fn test_filter_builder() {
        let filter = Filter::new()
            .and(Predicate::HasTags(vec!["rust".to_string()]))
            .and(Predicate::TimeRange {
                start: 1000,
                end: 2000,
            });

        assert_eq!(filter.predicates.len(), 2);
    }

    #[test]
    fn test_tag_query() {
        let query = TagQuery::and(vec!["rust".to_string(), "async".to_string()]).with_limit(10);

        assert_eq!(query.mode, BooleanMode::And);
        assert_eq!(query.tokens.len(), 2);
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_result_item() {
        let item = ResultItem::new(Bytes::from("key1"), 0.95)
            .with_value(Bytes::from("value1"))
            .with_source(ResultSource::Vector);

        assert_eq!(item.key.as_ref(), b"key1");
        assert_eq!(item.score, 0.95);
        assert_eq!(item.source, ResultSource::Vector);
    }
}
