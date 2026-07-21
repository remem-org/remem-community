use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct SearchResult {
    pub memory: Memory,
    pub score: f32,
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
    std::str::from_utf8(key).ok()?.strip_prefix("memory:")?.parse().ok()
}

pub fn default_memory_health() -> f32 {
    100.0
}

/// Convert HNSW distance (any metric) to a [0,1] relevance score.
pub fn distance_to_score(distance: f32) -> f32 {
    1.0 / (1.0 + distance)
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
        assert_eq!(MemoryType::try_from("short_term"), Ok(MemoryType::ShortTerm));
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
            assert_eq!(RelationshipType::try_from(s), Ok(expected), "failed for {s}");
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
        assert_eq!(memory_key(id), "memory:00000000-0000-0000-0000-000000000000");
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
        assert!(ms > jan_2024_ms, "now_ms() returned a timestamp before 2024");
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
        assert_eq!(api.connections[0].relationship_type, RelationshipType::SimilarTo);
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
