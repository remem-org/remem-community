use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::engine::query::SourceKind;
use crate::engine::storage::partition::decode_record_key;
use crate::engine::util::DistanceMetric;

// ─── Enums ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MemoryType {
    ShortTerm,
    LongTerm,
}

impl std::fmt::Display for MemoryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryType::ShortTerm => write!(f, "short_term"),
            MemoryType::LongTerm => write!(f, "long_term"),
        }
    }
}

impl TryFrom<&str> for MemoryType {
    type Error = String;
    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        match s {
            "short_term" => Ok(MemoryType::ShortTerm),
            "long_term" => Ok(MemoryType::LongTerm),
            other => Err(format!("unknown memory_type: {other}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipType {
    RelatedTo,
    CausedBy,
    PartOf,
    References,
    Contradicts,
    Supports,
    SimilarTo,
    DerivedFrom,
}

impl std::fmt::Display for RelationshipType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RelationshipType::RelatedTo => "related_to",
            RelationshipType::CausedBy => "caused_by",
            RelationshipType::PartOf => "part_of",
            RelationshipType::References => "references",
            RelationshipType::Contradicts => "contradicts",
            RelationshipType::Supports => "supports",
            RelationshipType::SimilarTo => "similar_to",
            RelationshipType::DerivedFrom => "derived_from",
        };
        write!(f, "{s}")
    }
}

impl TryFrom<&str> for RelationshipType {
    type Error = String;
    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        match s {
            "related_to" => Ok(RelationshipType::RelatedTo),
            "caused_by" => Ok(RelationshipType::CausedBy),
            "part_of" => Ok(RelationshipType::PartOf),
            "references" => Ok(RelationshipType::References),
            "contradicts" => Ok(RelationshipType::Contradicts),
            "supports" => Ok(RelationshipType::Supports),
            "similar_to" => Ok(RelationshipType::SimilarTo),
            "derived_from" => Ok(RelationshipType::DerivedFrom),
            other => Err(format!("unknown relationship_type: {other}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SearchType {
    Semantic,
    Keyword,
    Hybrid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
/// How a listing is ordered.
///
/// One variant, deliberately. An ordering key has to be immutable, or a
/// record can move between one page and the next and be returned twice or
/// skipped entirely; `created_at` is written once and never reassigned.
/// Ordering by recency of retrieval was removed for exactly that reason --
/// reading a memory changed where it sat in the list (REM-79).
pub enum SortBy {
    #[default]
    CreatedAt,
}

/// Which end of the ordering key a listing starts from.
///
/// Separate from [`SortBy`] on purpose: the key and the direction are
/// independent choices, and leaving the direction implicit is what let
/// `list_recent_memories` return the *oldest* memories for as long as it did.
/// Callers state it, so the answer is visible at the call site.
///
/// `Ascending` stays the default because it is what stable offset pagination
/// wants: records are appended in `created_at` order, so an ascending page
/// boundary keeps its meaning as the corpus grows, while a descending one
/// shifts by one for every record written after the first page was read.
/// A caller that wants newest-first and pages through results has to accept
/// that drift -- or ask for a single unpaged page, as `list_recent_memories`
/// does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    #[default]
    Ascending,
    Descending,
}

impl TryFrom<&str> for SortOrder {
    type Error = String;

    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        match s {
            "asc" => Ok(SortOrder::Ascending),
            "desc" => Ok(SortOrder::Descending),
            other => Err(format!("unknown order: {other}; use asc or desc")),
        }
    }
}

impl TryFrom<&str> for SortBy {
    type Error = String;

    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        match s {
            "created_at" => Ok(SortBy::CreatedAt),
            other => Err(format!("unknown sort_by: {other}; use created_at")),
        }
    }
}

#[cfg(test)]
mod sort_order_tests {
    use super::SortOrder;

    #[test]
    fn parses_known_values() {
        assert_eq!(SortOrder::try_from("asc").unwrap(), SortOrder::Ascending);
        assert_eq!(SortOrder::try_from("desc").unwrap(), SortOrder::Descending);
    }

    #[test]
    fn rejects_unknown_value() {
        let err = SortOrder::try_from("sideways").unwrap_err();
        assert!(
            err.contains("sideways"),
            "the error names what was asked for: {err}"
        );
        assert!(err.contains("asc"), "and what is available: {err}");
    }

    /// Ascending is what a paged listing needs, so it is what a caller that
    /// says nothing gets.
    #[test]
    fn default_is_ascending() {
        assert_eq!(SortOrder::default(), SortOrder::Ascending);
    }
}

#[cfg(test)]
mod sort_by_tests {
    use super::SortBy;

    #[test]
    fn parses_known_values() {
        assert_eq!(SortBy::try_from("created_at").unwrap(), SortBy::CreatedAt);
    }

    #[test]
    fn rejects_unknown_value() {
        assert!(SortBy::try_from("popularity").is_err());
    }

    /// Ordering by recency of retrieval is gone, and a caller asking for it
    /// is told so rather than quietly served a different order.
    #[test]
    fn rejects_ordering_by_access_time() {
        let err = SortBy::try_from("accessed_at").unwrap_err();
        assert!(
            err.contains("accessed_at"),
            "the error names what was asked for: {err}"
        );
        assert!(err.contains("created_at"), "and what is available: {err}");
    }

    #[test]
    fn default_is_created_at() {
        assert_eq!(SortBy::default(), SortBy::CreatedAt);
    }
}

// ─── Core data types ─────────────────────────────────────────────────────────

/// What gets stored in the KV store under "memory:{uuid}".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMemory {
    pub id: Uuid,
    pub content: String,
    pub memory_type: MemoryType,
    pub metadata: StoredMetadata,
    /// Set to true by soft-delete; never un-set.
    pub archived: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMetadata {
    /// Unix timestamp in milliseconds.
    pub created_at: u64,
    pub updated_at: u64,
    pub accessed_at: u64,
    pub access_count: u32,
    pub source: Option<String>,
    /// User-specified tags (not internal index tags).
    pub tags: Vec<String>,
    /// 0.0–1.0
    pub importance: f32,
    /// Emotional valence in the range -1.0..1.0.
    #[serde(default)]
    pub emotional_valence: f32,
    /// Emotional arousal in the range 0.0..1.0. High arousal creates flashbulb memories.
    #[serde(default)]
    pub arousal: f32,
    /// Active-forgetting health in the range 0.0..100.0.
    #[serde(default = "default_memory_health")]
    pub health: f32,
    /// Last recall timestamp in Unix milliseconds.
    #[serde(default)]
    pub last_recalled_at: Option<u64>,
    /// Timestamp until which this memory is protected from normal decay.
    #[serde(default)]
    pub flashbulb_until: Option<u64>,
    /// TTL in seconds. None for long-term.
    pub ttl: Option<u64>,
    /// Last time apply_importance_decay touched this memory. Kept separate
    /// from `updated_at` so the decay task's own periodic touch doesn't
    /// masquerade as a content edit or a reinforcement signal.
    #[serde(default)]
    pub last_decay_at: Option<u64>,
    /// Last time active_forgetting touched this memory's health. Kept
    /// separate from `updated_at` for the same reason.
    #[serde(default)]
    pub last_health_check_at: Option<u64>,
}

/// A memory as returned to API callers — timestamps converted to ISO-8601.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct Memory {
    pub id: Uuid,
    pub content: String,
    pub memory_type: MemoryType,
    pub metadata: Metadata,
    pub connections: Vec<Connection>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct Metadata {
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub accessed_at: DateTime<Utc>,
    pub access_count: u32,
    pub source: Option<String>,
    pub tags: Vec<String>,
    pub importance: f32,
    pub emotional_valence: f32,
    pub arousal: f32,
    pub health: f32,
    pub last_recalled_at: Option<DateTime<Utc>>,
    pub flashbulb_until: Option<DateTime<Utc>>,
    pub ttl: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Connection {
    pub target_id: Uuid,
    pub relationship_type: RelationshipType,
    pub strength: f32,
    pub created_at: DateTime<Utc>,
}

/// Which index a result surfaced from.
///
/// `Content` is the full-corpus text scan: the query engine plans it as an
/// explicit `ExecutionStep::ContentScan` step (`engine/query/planner.rs`)
/// and executes it via `StorageEngine::content_scan`
/// (`engine/storage/engine.rs`) on every keyword query, unconditionally
/// alongside the tag search rather than only when tags under-deliver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceName {
    Vector,
    Tag,
    Graph,
    Content,
}

impl From<SourceKind> for SourceName {
    fn from(kind: SourceKind) -> Self {
        match kind {
            SourceKind::Vector => SourceName::Vector,
            SourceKind::Tag => SourceName::Tag,
            SourceKind::Graph => SourceName::Graph,
            SourceKind::Content => SourceName::Content,
        }
    }
}

/// One index's contribution to a search result.
///
/// `rank` is reported alongside `score` because rank fusion works on positions,
/// not scores: re-fusing results across shards needs the rank an item held in
/// each node's list, which cannot be recovered from the score.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ResultSource {
    /// Index that produced this contribution
    pub source: SourceName,

    /// That index's own relevance score, normalized to 0-1
    pub score: f32,

    /// Zero-based rank the result held within this index's ranked list
    pub rank: usize,

    /// Cosine similarity, when the vector index reported a convertible distance
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cosine: Option<f32>,

    /// Raw distance from the vector index, in its configured metric
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance: Option<f32>,

    /// Hop count from the graph traversal start node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth: Option<usize>,

    /// Query tokens that matched this memory's tags
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matching_tags: Option<Vec<String>>,
}

impl ResultSource {
    /// Base constructor; evidence fields are filled in by the `with_*` helpers.
    pub fn new(source: SourceName, score: f32, rank: usize) -> Self {
        Self {
            source,
            score,
            rank,
            cosine: None,
            distance: None,
            depth: None,
            matching_tags: None,
        }
    }

    /// Attach vector evidence, converting the raw distance under `metric`.
    pub fn with_vector_evidence(mut self, metric: DistanceMetric, distance: f32) -> Self {
        self.distance = Some(distance);
        self.cosine = cosine_from_distance(metric, distance);
        self
    }

    /// Attach graph traversal depth
    pub fn with_depth(mut self, depth: usize) -> Self {
        self.depth = Some(depth);
        self
    }

    /// Attach the query tokens that matched this memory's tags
    pub fn with_matching_tags(mut self, tags: Vec<String>) -> Self {
        self.matching_tags = Some(tags);
        self
    }
}

/// A search result, carrying both the ordering and the evidence behind it.
///
/// Construct via [`SearchResult::from_sources`] — the fields are derived from
/// the contributions rather than set independently, so no search path can
/// report a score that its sources do not support. Before REM-74 each path
/// assembled this struct itself and `score` drifted into meaning four different
/// things depending on which branch produced it.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct SearchResult {
    pub memory: Memory,

    /// Relevance, 0-1, comparable across search types.
    ///
    /// Note this is deliberately *not* monotonic with response order: results
    /// are ordered by `fused_score`.
    pub score: f32,

    /// Rank-fusion value that determined ordering. A function of rank position,
    /// not similarity — not comparable across requests.
    pub fused_score: f32,

    /// Per-index contributions, best-scoring first. Omitted entirely when the
    /// caller did not request an explanation.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<ResultSource>,
}

impl SearchResult {
    /// Build a result from the contributions that produced it, deriving
    /// `score` from them.
    ///
    /// Relevance is the best content-matching source's score. Graph proximity
    /// is deliberately excluded unless it is the only contribution: being one
    /// hop from the anchor memory is context, not evidence that the content
    /// matches the query, and letting it set relevance would report 0.5 for an
    /// unrelated neighbour.
    pub fn from_sources(memory: Memory, sources: Vec<ResultSource>, fused_score: f32) -> Self {
        let best = |filter: &dyn Fn(&ResultSource) -> bool| -> Option<f32> {
            sources
                .iter()
                .filter(|s| filter(s))
                .map(|s| s.score)
                .fold(None, |acc: Option<f32>, s| {
                    Some(acc.map_or(s, |a| a.max(s)))
                })
        };

        let score = best(&|s: &ResultSource| s.source != SourceName::Graph)
            .or_else(|| best(&|_: &ResultSource| true))
            .unwrap_or(0.0)
            .clamp(0.0, 1.0);

        Self {
            memory,
            score,
            fused_score,
            sources,
        }
    }

    /// Drop the per-source detail, keeping the scores.
    ///
    /// Used by the MCP surface, where every field is context the model pays for
    /// on each search; callers that want the evidence pass `explain`.
    pub fn without_sources(mut self) -> Self {
        self.sources.clear();
        self
    }
}

// ─── Query / filter types ────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct MemoryFilters {
    pub memory_type: Option<MemoryType>,
    pub tags: Vec<String>,
    pub min_importance: Option<f32>,
    pub max_importance: Option<f32>,
    pub created_after: Option<u64>,  // Unix ms
    pub created_before: Option<u64>, // Unix ms
}

impl MemoryFilters {
    /// Whether the caller asked to narrow anything.
    ///
    /// Note what this does *not* count: excluding archived memories is not a
    /// caller's filter, it is what every user-facing read does, so a search
    /// with no filters at all still carries the `archived` predicate. This
    /// only decides how far ahead retrieval fetches — a request that narrows
    /// nothing has nothing to lose candidates to, so it need not over-fetch
    /// against that possibility (REM-78, design D9).
    pub fn narrows_nothing(&self) -> bool {
        self.memory_type.is_none()
            && self.tags.is_empty()
            && self.min_importance.is_none()
            && self.max_importance.is_none()
            && self.created_after.is_none()
            && self.created_before.is_none()
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

pub fn now_ms() -> u64 {
    Utc::now().timestamp_millis() as u64
}

pub fn ms_to_dt(ms: u64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms as i64).unwrap_or_else(Utc::now)
}

pub fn memory_key(id: Uuid) -> String {
    format!("memory:{id}")
}

/// Inverse of `memory_key` — parses the memory id out of a raw KV key. Used
/// by lifecycle scan loops to lock by id *before* loading the record, so
/// there's no window between "index scan finds this key" and "we know
/// which id to lock."
pub fn parse_memory_id(key: &[u8]) -> Option<Uuid> {
    let logical_key = match decode_record_key(key).ok()? {
        Some(decoded) => decoded.logical_key().to_vec(),
        None => key.to_vec(),
    };
    std::str::from_utf8(&logical_key)
        .ok()?
        .strip_prefix("memory:")?
        .parse()
        .ok()
}

pub fn default_memory_health() -> f32 {
    100.0
}

/// Convert HNSW distance (any metric) to a [0,1] relevance score.
///
/// Monotonic in distance, so it orders correctly under every metric — but it is
/// an ordering value, not a calibrated similarity. Use
/// [`cosine_from_distance`] for a number that means the same thing across
/// search types.
pub fn distance_to_score(distance: f32) -> f32 {
    1.0 / (1.0 + distance)
}

/// Recover cosine similarity from a raw index distance, when the configured
/// metric permits it.
///
/// Embeddings are L2-normalized on the way in (`embedding::l2_normalize`), so
/// for unit vectors `squared_l2 = 2 - 2·cos` and the conversion is exact.
/// Returns `None` for metrics where no bounded similarity can be recovered.
///
/// This is preferred over [`distance_to_score`] as the reported relevance
/// because `1/(1+d)` floors at 1/3 for orthogonal vectors and never approaches
/// zero, which makes it a poor threshold (REM-74).
pub fn cosine_from_distance(metric: DistanceMetric, distance: f32) -> Option<f32> {
    match metric {
        // Squared L2 over unit vectors: d = 2 - 2·cos
        DistanceMetric::L2 => Some((1.0 - distance / 2.0).clamp(0.0, 1.0)),
        // Already 1 - cos
        DistanceMetric::Cosine => Some((1.0 - distance).clamp(0.0, 1.0)),
        // Negated dot product; equals cosine for unit vectors, but the index
        // does not guarantee normalization, so it is not reported as cosine.
        DistanceMetric::DotProduct => None,
    }
}

/// Relevance score for a vector hit, preferring cosine where recoverable.
pub fn vector_relevance(metric: DistanceMetric, distance: f32) -> f32 {
    cosine_from_distance(metric, distance).unwrap_or_else(|| distance_to_score(distance))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── MemoryType ────────────────────────────────────────────────────────────

    #[test]
    fn memory_type_display() {
        assert_eq!(MemoryType::ShortTerm.to_string(), "short_term");
        assert_eq!(MemoryType::LongTerm.to_string(), "long_term");
    }

    #[test]
    fn memory_type_try_from_valid() {
        assert_eq!(
            MemoryType::try_from("short_term"),
            Ok(MemoryType::ShortTerm)
        );
        assert_eq!(MemoryType::try_from("long_term"), Ok(MemoryType::LongTerm));
    }

    #[test]
    fn memory_type_try_from_invalid() {
        assert!(MemoryType::try_from("ShortTerm").is_err());
        assert!(MemoryType::try_from("SHORT_TERM").is_err());
        assert!(MemoryType::try_from("").is_err());
        assert!(MemoryType::try_from("unknown").is_err());
    }

    #[test]
    fn memory_type_serde_roundtrip() {
        let short = MemoryType::ShortTerm;
        let json = serde_json::to_string(&short).unwrap();
        assert_eq!(json, r#""short_term""#);

        let long: MemoryType = serde_json::from_str(r#""long_term""#).unwrap();
        assert_eq!(long, MemoryType::LongTerm);
    }

    // ── RelationshipType ──────────────────────────────────────────────────────

    #[test]
    fn relationship_type_display() {
        assert_eq!(RelationshipType::RelatedTo.to_string(), "related_to");
        assert_eq!(RelationshipType::CausedBy.to_string(), "caused_by");
        assert_eq!(RelationshipType::PartOf.to_string(), "part_of");
        assert_eq!(RelationshipType::References.to_string(), "references");
        assert_eq!(RelationshipType::Contradicts.to_string(), "contradicts");
        assert_eq!(RelationshipType::Supports.to_string(), "supports");
        assert_eq!(RelationshipType::SimilarTo.to_string(), "similar_to");
        assert_eq!(RelationshipType::DerivedFrom.to_string(), "derived_from");
    }

    #[test]
    fn relationship_type_try_from_all_variants() {
        let pairs = [
            ("related_to", RelationshipType::RelatedTo),
            ("caused_by", RelationshipType::CausedBy),
            ("part_of", RelationshipType::PartOf),
            ("references", RelationshipType::References),
            ("contradicts", RelationshipType::Contradicts),
            ("supports", RelationshipType::Supports),
            ("similar_to", RelationshipType::SimilarTo),
            ("derived_from", RelationshipType::DerivedFrom),
        ];
        for (s, expected) in pairs {
            assert_eq!(
                RelationshipType::try_from(s),
                Ok(expected),
                "failed for {s}"
            );
        }
    }

    #[test]
    fn relationship_type_try_from_invalid() {
        // "follows" and "precedes" existed in the old Python model but not in Rust
        assert!(RelationshipType::try_from("follows").is_err());
        assert!(RelationshipType::try_from("precedes").is_err());
        assert!(RelationshipType::try_from("RELATED_TO").is_err());
        assert!(RelationshipType::try_from("").is_err());
    }

    #[test]
    fn relationship_type_serde_roundtrip() {
        let rt = RelationshipType::SimilarTo;
        let json = serde_json::to_string(&rt).unwrap();
        assert_eq!(json, r#""similar_to""#);

        let rt2: RelationshipType = serde_json::from_str(r#""derived_from""#).unwrap();
        assert_eq!(rt2, RelationshipType::DerivedFrom);
    }

    // ── SearchType ────────────────────────────────────────────────────────────

    #[test]
    fn search_type_serde() {
        let semantic: SearchType = serde_json::from_str(r#""semantic""#).unwrap();
        assert_eq!(serde_json::to_string(&semantic).unwrap(), r#""semantic""#);

        let keyword: SearchType = serde_json::from_str(r#""keyword""#).unwrap();
        assert_eq!(serde_json::to_string(&keyword).unwrap(), r#""keyword""#);

        let hybrid: SearchType = serde_json::from_str(r#""hybrid""#).unwrap();
        assert_eq!(serde_json::to_string(&hybrid).unwrap(), r#""hybrid""#);
    }

    // ── Helper functions ──────────────────────────────────────────────────────

    #[test]
    fn memory_key_format() {
        let id = uuid::Uuid::nil();
        assert_eq!(
            memory_key(id),
            "memory:00000000-0000-0000-0000-000000000000"
        );
    }

    #[test]
    fn memory_key_contains_uuid() {
        let id = uuid::Uuid::new_v4();
        let key = memory_key(id);
        assert!(key.starts_with("memory:"));
        assert!(key.contains(&id.to_string()));
    }

    #[test]
    fn distance_to_score_zero_distance() {
        // Distance 0 → score 1.0
        assert!((distance_to_score(0.0) - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn distance_to_score_monotone_decreasing() {
        assert!(distance_to_score(0.5) > distance_to_score(1.0));
        assert!(distance_to_score(1.0) > distance_to_score(10.0));
        assert!(distance_to_score(10.0) > distance_to_score(100.0));
    }

    #[test]
    fn distance_to_score_always_positive() {
        for d in [0.0f32, 0.5, 1.0, 10.0, 100.0, f32::MAX / 2.0] {
            let score = distance_to_score(d);
            assert!(score > 0.0, "score for distance {d} must be > 0");
            assert!(score <= 1.0, "score for distance {d} must be <= 1");
        }
    }

    #[test]
    fn ms_to_dt_roundtrip() {
        let ms: u64 = 1_700_000_000_000;
        let dt = ms_to_dt(ms);
        assert_eq!(dt.timestamp_millis() as u64, ms);
    }

    #[test]
    fn now_ms_after_2024() {
        let ms = now_ms();
        let jan_2024_ms: u64 = 1_704_067_200_000;
        assert!(
            ms > jan_2024_ms,
            "now_ms() returned a timestamp before 2024"
        );
    }

    // ── REM-74: search result contract ────────────────────────────────────────

    fn make_memory() -> Memory {
        let now = Utc::now();
        Memory {
            id: Uuid::nil(),
            content: "content".to_string(),
            memory_type: MemoryType::LongTerm,
            metadata: Metadata {
                created_at: now,
                updated_at: now,
                accessed_at: now,
                access_count: 0,
                source: None,
                tags: Vec::new(),
                importance: 0.5,
                emotional_valence: 0.0,
                arousal: 0.0,
                health: 100.0,
                last_recalled_at: None,
                flashbulb_until: None,
                ttl: None,
            },
            connections: Vec::new(),
        }
    }

    fn source(name: SourceName, score: f32) -> ResultSource {
        ResultSource::new(name, score, 0)
    }

    #[test]
    fn cosine_from_l2_matches_the_normalized_identity() {
        // Embeddings are unit vectors, so squared L2 d = 2 - 2·cos.
        let cases: [(f32, f32); 5] = [
            (0.0, 1.0),  // identical
            (0.4, 0.8),  // cos 0.8
            (1.0, 0.5),  // cos 0.5
            (2.0, 0.0),  // orthogonal
            (4.0, -1.0), // opposite, clamped to 0
        ];
        for (distance, expected_cos) in cases {
            let got = cosine_from_distance(DistanceMetric::L2, distance).unwrap();
            let want = expected_cos.max(0.0_f32);
            assert!(
                (got - want).abs() < 1e-6,
                "distance {distance} → cosine {got}, expected {want}"
            );
        }
    }

    #[test]
    fn cosine_is_not_reported_for_dot_product() {
        // The index does not guarantee normalized vectors under this metric, so
        // no bounded similarity can be recovered.
        assert!(cosine_from_distance(DistanceMetric::DotProduct, 0.5).is_none());
    }

    #[test]
    fn vector_relevance_falls_back_when_cosine_is_unrecoverable() {
        // Falls back to the monotonic ordering score rather than inventing one.
        assert_eq!(
            vector_relevance(DistanceMetric::DotProduct, 1.0),
            distance_to_score(1.0)
        );
    }

    #[test]
    fn cosine_discriminates_where_distance_to_score_floors() {
        // The reason cosine is the reported relevance: 1/(1+d) never drops
        // below 1/3 for orthogonal-or-worse vectors, so it cannot express
        // "this is a bad match".
        assert!(distance_to_score(2.0) > 0.33);
        assert_eq!(cosine_from_distance(DistanceMetric::L2, 2.0).unwrap(), 0.0);
    }

    #[test]
    fn score_is_the_best_content_matching_source() {
        let result = SearchResult::from_sources(
            make_memory(),
            vec![
                source(SourceName::Vector, 0.80),
                source(SourceName::Tag, 0.50),
            ],
            0.0328,
        );
        assert_eq!(result.score, 0.80);
        assert_eq!(result.fused_score, 0.0328);
    }

    #[test]
    fn graph_proximity_does_not_set_relevance() {
        // Being one hop from the anchor is context, not evidence that the
        // content matches — otherwise an unrelated neighbour reports 0.5.
        let result = SearchResult::from_sources(
            make_memory(),
            vec![
                source(SourceName::Vector, 0.20),
                source(SourceName::Graph, 0.50),
            ],
            0.0328,
        );
        assert_eq!(result.score, 0.20);
    }

    #[test]
    fn graph_only_hit_falls_back_to_graph_score() {
        // With nothing else to report, the graph score is the honest answer.
        let result =
            SearchResult::from_sources(make_memory(), vec![source(SourceName::Graph, 0.5)], 0.0164);
        assert_eq!(result.score, 0.5);
    }

    #[test]
    fn score_is_comparable_across_search_types() {
        // The bug this contract exists to fix: the same memory matching equally
        // well must report the same relevance whether it was fused or not.
        let semantic =
            SearchResult::from_sources(make_memory(), vec![source(SourceName::Vector, 0.8)], 0.8);
        let hybrid = SearchResult::from_sources(
            make_memory(),
            vec![
                source(SourceName::Vector, 0.8),
                source(SourceName::Tag, 0.5),
            ],
            0.0328,
        );

        assert_eq!(semantic.score, hybrid.score);
        // ...while the ordering values remain wildly different, as they should.
        assert!(semantic.fused_score > hybrid.fused_score * 10.0);
    }

    #[test]
    fn score_is_clamped_to_unit_range() {
        let result = SearchResult::from_sources(
            make_memory(),
            vec![
                source(SourceName::Tag, 4.0),
                source(SourceName::Content, -1.0),
            ],
            1.0,
        );
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn sourceless_result_scores_zero_rather_than_panicking() {
        let result = SearchResult::from_sources(make_memory(), Vec::new(), 0.0);
        assert_eq!(result.score, 0.0);
    }

    #[test]
    fn without_sources_keeps_both_scores() {
        let result = SearchResult::from_sources(
            make_memory(),
            vec![source(SourceName::Vector, 0.8)],
            0.0164,
        )
        .without_sources();

        assert!(result.sources.is_empty());
        assert_eq!(result.score, 0.8);
        assert_eq!(result.fused_score, 0.0164);
    }

    #[test]
    fn evidence_fields_are_omitted_when_absent() {
        // Keeps the payload lean, and matters most on the MCP surface where
        // every field costs the model context.
        let json = serde_json::to_value(source(SourceName::Tag, 0.5)).unwrap();
        assert!(json.get("cosine").is_none());
        assert!(json.get("distance").is_none());
        assert!(json.get("depth").is_none());
        assert!(json.get("matching_tags").is_none());

        let vector = serde_json::to_value(
            source(SourceName::Vector, 0.8).with_vector_evidence(DistanceMetric::L2, 0.4),
        )
        .unwrap();
        assert!((vector["distance"].as_f64().unwrap() - 0.4).abs() < 1e-6);
        assert!((vector["cosine"].as_f64().unwrap() - 0.8).abs() < 1e-6);
    }

    #[test]
    fn source_names_serialize_as_snake_case() {
        // The frontend and both MCP copies match on these strings.
        let names = serde_json::to_value(vec![
            SourceName::Vector,
            SourceName::Tag,
            SourceName::Graph,
            SourceName::Content,
        ])
        .unwrap();
        assert_eq!(
            names,
            serde_json::json!(["vector", "tag", "graph", "content"])
        );
    }

    // ── StoredMemory::is_expired ──────────────────────────────────────────────

    fn make_stored(memory_type: MemoryType, ttl: Option<u64>, created_ms_ago: u64) -> StoredMemory {
        let now = now_ms();
        StoredMemory {
            id: uuid::Uuid::new_v4(),
            content: "test content".into(),
            memory_type,
            metadata: StoredMetadata {
                created_at: now.saturating_sub(created_ms_ago),
                updated_at: now,
                accessed_at: now,
                access_count: 0,
                source: None,
                tags: vec![],
                importance: 0.5,
                emotional_valence: 0.0,
                arousal: 0.0,
                health: 100.0,
                last_recalled_at: None,
                flashbulb_until: None,
                ttl,
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: false,
        }
    }

    #[test]
    fn long_term_memory_never_expires() {
        // Long-term with no TTL: should never expire regardless of age
        let m = make_stored(MemoryType::LongTerm, None, 365 * 24 * 3600 * 1000);
        assert!(!m.is_expired());
    }

    #[test]
    fn long_term_memory_with_ttl_never_expires() {
        // Even if someone sets a TTL on a long-term memory, the type gate prevents expiry
        let m = make_stored(MemoryType::LongTerm, Some(1), 999_999_999);
        assert!(!m.is_expired());
    }

    #[test]
    fn short_term_memory_not_yet_expired() {
        // Created just now, TTL 1 hour → not expired
        let m = make_stored(MemoryType::ShortTerm, Some(3600), 0);
        assert!(!m.is_expired());
    }

    #[test]
    fn short_term_memory_expired() {
        // Created 2 hours ago, TTL 1 hour (3_600_000 ms) → expired
        let m = make_stored(MemoryType::ShortTerm, Some(3600), 7_200_000);
        assert!(m.is_expired());
    }

    #[test]
    fn short_term_memory_no_ttl_never_expires() {
        // Short-term with no TTL should not expire
        let m = make_stored(MemoryType::ShortTerm, None, 999_999_999_000);
        assert!(!m.is_expired());
    }

    // ── StoredMemory::into_api ────────────────────────────────────────────────

    #[test]
    fn into_api_maps_all_fields() {
        let id = uuid::Uuid::new_v4();
        let target_id = uuid::Uuid::new_v4();
        let now = now_ms();

        let stored = StoredMemory {
            id,
            content: "Hello world".into(),
            memory_type: MemoryType::LongTerm,
            metadata: StoredMetadata {
                created_at: now,
                updated_at: now,
                accessed_at: now,
                access_count: 7,
                source: Some("unit-test".into()),
                tags: vec!["tag1".into(), "tag2".into()],
                importance: 0.8,
                emotional_valence: 0.25,
                arousal: 0.4,
                health: 88.0,
                last_recalled_at: Some(now),
                flashbulb_until: None,
                ttl: None,
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: false,
        };

        let conn = Connection {
            target_id,
            relationship_type: RelationshipType::SimilarTo,
            strength: 0.9,
            created_at: ms_to_dt(now),
        };

        let api = stored.into_api(vec![conn]);

        assert_eq!(api.id, id);
        assert_eq!(api.content, "Hello world");
        assert_eq!(api.memory_type, MemoryType::LongTerm);
        assert_eq!(api.metadata.access_count, 7);
        assert_eq!(api.metadata.source.as_deref(), Some("unit-test"));
        assert_eq!(api.metadata.tags, ["tag1", "tag2"]);
        assert!((api.metadata.importance - 0.8).abs() < f32::EPSILON);
        assert!(api.metadata.ttl.is_none());
        assert_eq!(api.connections.len(), 1);
        assert_eq!(api.connections[0].target_id, target_id);
        assert_eq!(
            api.connections[0].relationship_type,
            RelationshipType::SimilarTo
        );
    }

    #[test]
    fn into_api_empty_connections() {
        let now = now_ms();
        let stored = StoredMemory {
            id: uuid::Uuid::new_v4(),
            content: "No connections".into(),
            memory_type: MemoryType::ShortTerm,
            metadata: StoredMetadata {
                created_at: now,
                updated_at: now,
                accessed_at: now,
                access_count: 0,
                source: None,
                tags: vec![],
                importance: 0.5,
                emotional_valence: 0.0,
                arousal: 0.0,
                health: 100.0,
                last_recalled_at: None,
                flashbulb_until: None,
                ttl: Some(3600),
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: false,
        };

        let api = stored.into_api(vec![]);
        assert!(api.connections.is_empty());
        assert_eq!(api.metadata.ttl, Some(3600));
    }

    // ── parse_memory_id ──────────────────────────────────────────────────────

    #[test]
    fn parse_memory_id_roundtrips_with_memory_key() {
        let id = Uuid::new_v4();
        let key = memory_key(id);
        assert_eq!(parse_memory_id(key.as_bytes()), Some(id));
    }

    #[test]
    fn parse_memory_id_accepts_partitioned_record_keys() {
        let id = Uuid::new_v4();
        let binding = crate::engine::storage::partition::PartitionBinding::legacy_default();
        let key =
            crate::engine::storage::partition::encode_record_key(&binding, memory_key(id)).unwrap();

        assert_eq!(parse_memory_id(key.as_ref()), Some(id));
    }

    #[test]
    fn parse_memory_id_rejects_wrong_prefix() {
        assert_eq!(parse_memory_id(b"tag:abc"), None);
        assert_eq!(parse_memory_id(b"memory:not-a-uuid"), None);
        assert_eq!(parse_memory_id(b""), None);
    }
}

impl StoredMemory {
    pub fn into_api(self, connections: Vec<Connection>) -> Memory {
        Memory {
            id: self.id,
            content: self.content,
            memory_type: self.memory_type,
            metadata: Metadata {
                created_at: ms_to_dt(self.metadata.created_at),
                updated_at: ms_to_dt(self.metadata.updated_at),
                accessed_at: ms_to_dt(self.metadata.accessed_at),
                access_count: self.metadata.access_count,
                source: self.metadata.source,
                tags: self.metadata.tags,
                importance: self.metadata.importance,
                emotional_valence: self.metadata.emotional_valence,
                arousal: self.metadata.arousal,
                health: self.metadata.health,
                last_recalled_at: self.metadata.last_recalled_at.map(ms_to_dt),
                flashbulb_until: self.metadata.flashbulb_until.map(ms_to_dt),
                ttl: self.metadata.ttl,
            },
            connections,
        }
    }

    pub fn is_expired(&self) -> bool {
        if self.memory_type != MemoryType::ShortTerm {
            return false;
        }
        if let Some(ttl_secs) = self.metadata.ttl {
            let expiry_ms = self.metadata.created_at + ttl_secs * 1000;
            now_ms() >= expiry_ms
        } else {
            false
        }
    }
}
