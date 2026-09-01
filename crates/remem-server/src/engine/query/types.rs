//! Query types for the multi-index fusion path
//!
//! Remem's query surface is deliberately narrow: a [`HybridQuery`] fans out to
//! the vector, tag and graph indexes, and the resulting ranked lists are fused
//! by rank (RRF). There is no predicate DSL and no standalone per-index query
//! type — see `docs/QUERY_LAYER_AND_SCALE_FOUNDATION.md` for why the earlier
//! post-retrieval filter model was removed rather than finished (REM-72), and
//! REM-84 for the design intent a future planner should be built on.

use bytes::Bytes;

use crate::engine::attr::select::AttrPred;

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
/// Every component is optional, but the plan must end up with at least one
/// step. Vector-seeded queries use [`HybridQuery::new`]; the keyword path has
/// no embedding and uses [`HybridQuery::keyword`].
#[derive(Debug, Clone)]
pub struct HybridQuery {
    /// Vector search component; absent on the keyword path
    pub vector: Option<VectorQuery>,

    /// Tag search component (optional)
    pub tag: Option<TagQuery>,

    /// Graph context (optional): boost items related to this node
    pub graph_context: Option<GraphContext>,

    /// Whether to add a full-corpus content scan.
    ///
    /// Opt-in rather than inferred from "tags present, vector absent":
    /// inference happens to be correct today because keyword is the only
    /// vector-less query, but a future tag-only browse would silently inherit
    /// a scan of the entire corpus (REM-88).
    pub content_scan: bool,

    /// Maximum number of final results
    pub limit: usize,

    /// Attribute predicates every returned item must satisfy, ANDed.
    ///
    /// These travel *into* retrieval rather than being applied to its output:
    /// a candidate is settled from its attribute row before its payload is
    /// read, and a step that loses candidates to them widens rather than
    /// returning short (REM-78). Empty means "no filtering", which is what
    /// every pre-REM-78 construction of this type meant.
    ///
    /// Conditions with no attribute slot — tags above all — are deliberately
    /// absent here. They carry no predicate and are settled by the tag index
    /// or the payload; see `services/filters.rs::to_attr_preds`.
    pub preds: Vec<AttrPred>,

    /// Tags every returned item must carry, ANDed.
    ///
    /// Separate from `tag`, which is a *ranking source* — a query token that
    /// contributes score when it matches. This is a *filter*: an item lacking
    /// one of these is not a weaker result, it is not a result.
    ///
    /// Kept out of `preds` because tags have no attribute slot. They are
    /// settled by intersecting against the tag index, which is why they must
    /// travel with the query rather than being applied to its output: applied
    /// afterwards they shrink the page below the requested limit with nothing
    /// left to widen against (REM-78, design D6).
    pub tag_filter: Vec<String>,
}

impl HybridQuery {
    /// Create a new hybrid query seeded with a vector search
    pub fn new(embedding: Vec<f32>, k: usize) -> Self {
        Self {
            vector: Some(VectorQuery { embedding, k }),
            tag: None,
            graph_context: None,
            content_scan: false,
            limit: k,
            preds: Vec::new(),
            tag_filter: Vec::new(),
        }
    }

    /// Create a keyword query: tag index plus a content scan, no embedding.
    ///
    /// Called from `services/search_engine.rs::keyword_search` (REM-88).
    pub fn keyword(tokens: Vec<String>, mode: BooleanMode, limit: usize) -> Self {
        Self {
            vector: None,
            tag: Some(if mode == BooleanMode::And {
                TagQuery::and(tokens)
            } else {
                TagQuery::or(tokens)
            }),
            graph_context: None,
            content_scan: true,
            limit,
            preds: Vec::new(),
            tag_filter: Vec::new(),
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

    /// Constrain results to items whose attributes satisfy every predicate.
    pub fn with_preds(mut self, preds: Vec<AttrPred>) -> Self {
        self.preds = preds;
        self
    }

    /// Constrain results to items carrying every one of these tags.
    pub fn with_tag_filter(mut self, tags: Vec<String>) -> Self {
        self.tag_filter = tags;
        self
    }
}

/// Result of a query execution
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Result items, ranked best-first
    pub items: Vec<FusedItem>,

    /// Whether a step stopped at its effort bound with room left in the
    /// result target.
    ///
    /// `false` means the retrieval ran to exhaustion: these are all the items
    /// that match. `true` means it gave up early and more may exist — the
    /// distinction a caller needs in order to tell a complete page from a
    /// short one, and the reason a filtered search may not simply return
    /// fewer results and say nothing (REM-78).
    pub truncated: bool,

    /// A tag filter the tag index could not settle, left for the caller to
    /// apply against the payloads it loads.
    ///
    /// The caller has to know: applying it costs a payload read per candidate,
    /// and it can shrink the page after the engine has already stopped
    /// widening, so a short page in this state may not be a complete one
    /// (REM-78, design D6).
    pub tag_filter_deferred: bool,
}

/// A single result item, as produced by one index before fusion
#[derive(Debug, Clone)]
pub struct ResultItem {
    /// Key of the item
    pub key: Bytes,

    /// Relevance score (higher is better)
    pub score: f32,

    /// Index-specific evidence for why this item matched
    pub evidence: Evidence,
}

impl ResultItem {
    /// Create a new result item carrying no evidence beyond its score
    pub fn new(key: Bytes, score: f32) -> Self {
        Self {
            key,
            score,
            evidence: Evidence::None,
        }
    }

    /// Create a new result item with index-specific evidence
    pub fn with_evidence(key: Bytes, score: f32, evidence: Evidence) -> Self {
        Self {
            key,
            score,
            evidence,
        }
    }
}

/// Which index produced a result.
///
/// Kept separate from the API-facing source enum in `services::types` so the
/// engine stays free of serialization concerns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    /// Vector similarity (HNSW)
    Vector,
    /// Tag / token match (inverted index)
    Tag,
    /// Graph proximity (CSR traversal)
    Graph,
    /// Full-corpus text match (no index).
    Content,
}

/// Index-specific evidence for a match, preserved through fusion so callers can
/// explain a ranking rather than just consume it (REM-74).
#[derive(Debug, Clone, PartialEq)]
pub enum Evidence {
    /// No evidence beyond the score itself
    None,
    /// Raw distance from the vector index, in whatever metric it is configured for
    Vector { distance: f32 },
    /// Hop count from the graph traversal start node
    Graph { depth: usize },
}

/// One index's contribution to a fused result.
///
/// `rank` is retained alongside `score` because RRF fuses by *rank*: a
/// coordinator re-fusing results across shards needs the position an item held
/// in each node's list, which cannot be recovered from the score alone.
#[derive(Debug, Clone)]
pub struct SourceContribution {
    /// Which index contributed
    pub kind: SourceKind,

    /// That index's own relevance score, normalized to 0-1
    pub score: f32,

    /// Zero-based rank the item held within that index's ranked list
    pub rank: usize,

    /// Index-specific evidence
    pub evidence: Evidence,
}

/// A result after fusion, retaining every contribution that produced it.
///
/// Fusion used to overwrite each item's score with the RRF value, which
/// destroyed the per-index relevance scores — the ordering survived but the
/// evidence did not (REM-74).
#[derive(Debug, Clone)]
pub struct FusedItem {
    /// Key of the item
    pub key: Bytes,

    /// Rank-fusion score. Determines ordering only; it is a function of rank
    /// position, not similarity, and is not comparable across requests.
    pub fused_score: f32,

    /// Per-index contributions, best-scoring first
    pub sources: Vec<SourceContribution>,
}

impl FusedItem {
    /// Wrap a single index's result as an unfused item, so single-step plans
    /// emit the same shape as fused ones.
    pub fn single(kind: SourceKind, rank: usize, item: ResultItem) -> Self {
        Self {
            key: item.key,
            fused_score: item.score,
            sources: vec![SourceContribution {
                kind,
                score: item.score,
                rank,
                evidence: item.evidence,
            }],
        }
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

        assert_eq!(query.vector.unwrap().k, 10);
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

    #[test]
    fn test_keyword_query_has_no_vector_component() {
        let query = HybridQuery::keyword(vec!["rust".to_string()], BooleanMode::Or, 30);

        assert!(query.vector.is_none(), "keyword search has no embedding");
        assert!(query.content_scan, "keyword search always scans content");
        assert_eq!(query.tag.unwrap().mode, BooleanMode::Or);
        assert_eq!(query.limit, 30);
    }

    #[test]
    fn test_vector_query_does_not_request_a_content_scan() {
        // Only the keyword constructor opts in. A future tag-only browse must
        // not inherit a full corpus scan by accident (REM-88).
        let query = HybridQuery::new(vec![0.1, 0.2], 10)
            .with_tags(vec!["rust".to_string()], BooleanMode::Or);

        assert!(!query.content_scan);
    }
}
