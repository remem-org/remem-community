//! Query types for the multi-index fusion path
//!
//! Remem's query surface is deliberately narrow: a [`HybridQuery`] fans out to
//! the vector, tag and graph indexes, and the resulting ranked lists are fused
//! by rank (RRF). There is no predicate DSL and no standalone per-index query
//! type — see `docs/QUERY_LAYER_AND_SCALE_FOUNDATION.md` for why the earlier
//! post-retrieval filter model was removed rather than finished (REM-72), and
//! REM-84 for the design intent a future planner should be built on.

use bytes::Bytes;

/// Vector similarity component of a hybrid query
#[derive(Debug, Clone)]
pub struct VectorQuery {
    /// Query embedding vector
    pub embedding: Vec<f32>,

    /// Number of candidates to retrieve from the vector index
    pub k: usize,
}

/// Tag/text search component of a hybrid query
#[derive(Debug, Clone)]
pub struct TagQuery {
    /// Tags or text tokens to search for
    pub tokens: Vec<String>,

    /// Boolean mode for combining tokens
    pub mode: BooleanMode,
}

impl TagQuery {
    /// Create a new tag query with AND mode
    pub fn and(tokens: Vec<String>) -> Self {
        Self {
            tokens,
            mode: BooleanMode::And,
        }
    }

    /// Create a new tag query with OR mode
    pub fn or(tokens: Vec<String>) -> Self {
        Self {
            tokens,
            mode: BooleanMode::Or,
        }
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

/// Graph context for hybrid queries: neighbours of `node` are fused into the
/// ranking alongside the vector and tag hits.
#[derive(Debug, Clone)]
pub struct GraphContext {
    /// Node to find related items from
    pub node: Bytes,
    /// Maximum traversal depth
    pub max_depth: usize,
}

/// Hybrid query: fans out to multiple indexes and fuses the ranked lists.
///
/// The vector component is mandatory — every caller starts from an embedding.
/// Tag and graph context are optional refinements.
#[derive(Debug, Clone)]
pub struct HybridQuery {
    /// Vector search component
    pub vector: VectorQuery,

    /// Tag search component (optional)
    pub tag: Option<TagQuery>,

    /// Graph context (optional): boost items related to this node
    pub graph_context: Option<GraphContext>,

    /// Maximum number of final results
    pub limit: usize,
}

impl HybridQuery {
    /// Create a new hybrid query seeded with a vector search
    pub fn new(embedding: Vec<f32>, k: usize) -> Self {
        Self {
            vector: VectorQuery { embedding, k },
            tag: None,
            graph_context: None,
            limit: k,
        }
    }

    /// Add a tag search component
    pub fn with_tags(mut self, tokens: Vec<String>, mode: BooleanMode) -> Self {
        self.tag = Some(if mode == BooleanMode::And {
            TagQuery::and(tokens)
        } else {
            TagQuery::or(tokens)
        });
        self
    }

    /// Add graph context
    pub fn with_graph_context(mut self, node: impl Into<Bytes>, max_depth: usize) -> Self {
        self.graph_context = Some(GraphContext {
            node: node.into(),
            max_depth,
        });
        self
    }

    /// Set the final result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

/// Result of a query execution
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Result items, ranked best-first
    pub items: Vec<ResultItem>,
}

/// A single result item
#[derive(Debug, Clone)]
pub struct ResultItem {
    /// Key of the item
    pub key: Bytes,

    /// Relevance score (higher is better)
    pub score: f32,
}

impl ResultItem {
    /// Create a new result item
    pub fn new(key: Bytes, score: f32) -> Self {
        Self { key, score }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hybrid_query_builder() {
        let query = HybridQuery::new(vec![0.1, 0.2], 10)
            .with_tags(vec!["rust".to_string()], BooleanMode::And)
            .with_graph_context(Bytes::from("memory:abc"), 2)
            .with_limit(20);

        assert_eq!(query.vector.k, 10);
        assert!(query.tag.is_some());
        assert_eq!(query.tag.unwrap().mode, BooleanMode::And);
        assert!(query.graph_context.is_some());
        assert_eq!(query.limit, 20);
    }

    #[test]
    fn test_hybrid_query_defaults_limit_to_k() {
        let query = HybridQuery::new(vec![0.1, 0.2], 7);
        assert_eq!(query.limit, 7);
        assert!(query.tag.is_none());
        assert!(query.graph_context.is_none());
    }

    #[test]
    fn test_tag_query_modes() {
        assert_eq!(TagQuery::and(vec!["a".to_string()]).mode, BooleanMode::And);
        assert_eq!(TagQuery::or(vec!["a".to_string()]).mode, BooleanMode::Or);
    }

    #[test]
    fn test_result_item() {
        let item = ResultItem::new(Bytes::from("key1"), 0.95);
        assert_eq!(item.key.as_ref(), b"key1");
        assert_eq!(item.score, 0.95);
    }
}
