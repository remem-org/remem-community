use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::api::AppState;
use crate::error::{AppError, ErrorResponse, Result};
use crate::services::types::{Memory, MemoryFilters, MemoryType, RelationshipType};
use crate::services::memory_manager::CreateOpts;

const MAX_CONTENT_BYTES: usize = 100_000;
const MAX_TAGS: usize = 50;
const MAX_GRAPH_ENTITIES: usize = 100;
const MAX_GRAPH_RELATIONSHIPS: usize = 200;

// ─── Serde helpers for query-string deserialization ───────────────────────────

/// Deserialize a comma-separated string into `Vec<String>`.
/// Missing field → empty vec; `tags=` (empty) → empty vec.
fn deserialize_comma_list<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Vec<String>, D::Error> {
    let s = String::deserialize(d)?;
    Ok(if s.is_empty() {
        Vec::new()
    } else {
        s.split(',').map(|t| t.trim().to_owned()).filter(|t| !t.is_empty()).collect()
    })
}

/// Deserialize an optional `MemoryType` from a string, returning a clear error
/// on unknown values instead of propagating a generic 500.
fn deserialize_opt_memory_type<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Option<MemoryType>, D::Error> {
    let s: Option<String> = Option::deserialize(d)?;
    match s {
        None => Ok(None),
        Some(s) => MemoryType::try_from(s.as_str()).map(Some).map_err(serde::de::Error::custom),
    }
}

/// Deserialize an optional RFC-3339 timestamp string into milliseconds since epoch.
/// Accepts RFC-3339 with timezone (e.g. `2024-01-01T00:00:00Z`) or timezone-naive
/// ISO 8601 (e.g. `2024-01-01T00:00:00.123456`), treating the latter as UTC.
fn deserialize_opt_rfc3339_ms<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Option<u64>, D::Error> {
    let s: Option<String> = Option::deserialize(d)?;
    match s {
        None => Ok(None),
        Some(s) if s.is_empty() => Ok(None),
        Some(s) => {
            let with_z = format!("{s}Z");
            chrono::DateTime::parse_from_rfc3339(&s)
                .or_else(|_| chrono::DateTime::parse_from_rfc3339(&with_z))
                .map(|dt| Some(dt.timestamp_millis() as u64))
                .map_err(|_| serde::de::Error::custom(format!("invalid datetime: {s}")))
        }
    }
}

// ─── Request / Response types ─────────────────────────────────────────────────

#[derive(Deserialize, utoipa::ToSchema)]
pub struct CreateMemoryRequest {
    pub content: String,
    pub memory_type: Option<String>,
    pub tags: Option<Vec<String>>,
    pub importance: Option<f32>,
    pub emotional_valence: Option<f32>,
    pub arousal: Option<f32>,
    pub health: Option<f32>,
    pub ttl: Option<u64>,
    pub source: Option<String>,
    pub graph_extraction: Option<GraphExtraction>,
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct UpdateMemoryRequest {
    pub content: Option<String>,
    pub tags: Option<Vec<String>>,
    pub importance: Option<f32>,
    pub emotional_valence: Option<f32>,
    pub arousal: Option<f32>,
    pub health: Option<f32>,
    pub source: Option<String>,
}

#[derive(Clone, Deserialize, utoipa::ToSchema)]
pub struct GraphExtraction {
    #[serde(default)]
    pub entities: Vec<ExtractedEntity>,
    #[serde(default)]
    pub relationships: Vec<ExtractedRelationship>,
}

#[derive(Clone, Deserialize, utoipa::ToSchema)]
pub struct ExtractedEntity {
    pub name: String,
    #[serde(default)]
    pub entity_type: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Clone, Deserialize, utoipa::ToSchema)]
pub struct ExtractedRelationship {
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub relationship_type: Option<String>,
    #[serde(default)]
    pub strength: Option<f32>,
}

#[derive(Deserialize)]
pub struct ListQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_opt_memory_type")]
    pub memory_type: Option<MemoryType>,
    /// Comma-separated tag list, e.g. `tags=rust,async`.
    #[serde(default, deserialize_with = "deserialize_comma_list")]
    pub tags: Vec<String>,
    pub min_importance: Option<f32>,
    pub max_importance: Option<f32>,
    /// RFC-3339 timestamp, e.g. `2024-01-01T00:00:00Z`. Parsed to ms-since-epoch at extraction.
    #[serde(default, deserialize_with = "deserialize_opt_rfc3339_ms")]
    pub created_after: Option<u64>,
    /// RFC-3339 timestamp. Parsed to ms-since-epoch at extraction.
    #[serde(default, deserialize_with = "deserialize_opt_rfc3339_ms")]
    pub created_before: Option<u64>,
    pub include_connections: Option<bool>,
}

#[derive(Deserialize)]
pub struct GetQuery {
    pub include_connections: Option<bool>,
}

#[derive(Deserialize)]
pub struct DeleteQuery {
    pub hard: Option<bool>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct MemoryListResponse {
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
    pub memories: Vec<Memory>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct DeleteResponse {
    pub success: bool,
    pub message: &'static str,
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

#[utoipa::path(
    post,
    path = "/api/v1/memories",
    request_body = CreateMemoryRequest,
    responses(
        (status = 201, description = "Memory created", body = Memory),
        (status = 422, description = "Validation error", body = ErrorResponse),
        (status = 500, description = "Embedding or storage error", body = ErrorResponse),
    ),
    tag = "memories"
)]
pub async fn create_memory(
    State(state): State<AppState>,
    Json(body): Json<CreateMemoryRequest>,
) -> Result<(StatusCode, Json<Memory>)> {
    if body.content.trim().is_empty() {
        return Err(AppError::Validation("content must not be empty".into()));
    }
    if body.content.len() > MAX_CONTENT_BYTES {
        return Err(AppError::Validation(format!(
            "content exceeds maximum length of {MAX_CONTENT_BYTES} bytes"
        )));
    }
    if let Some(tags) = &body.tags {
        if tags.len() > MAX_TAGS {
            return Err(AppError::Validation(format!(
                "too many tags: maximum is {MAX_TAGS}, got {}", tags.len()
            )));
        }
    }
    if let Some(ge) = &body.graph_extraction {
        if ge.entities.len() > MAX_GRAPH_ENTITIES {
            return Err(AppError::Validation(format!(
                "too many graph entities: maximum is {MAX_GRAPH_ENTITIES}"
            )));
        }
        if ge.relationships.len() > MAX_GRAPH_RELATIONSHIPS {
            return Err(AppError::Validation(format!(
                "too many graph relationships: maximum is {MAX_GRAPH_RELATIONSHIPS}"
            )));
        }
    }

    let memory_type = body
        .memory_type
        .as_deref()
        .map(MemoryType::try_from)
        .transpose()
        .map_err(AppError::Validation)?
        .unwrap_or(MemoryType::ShortTerm);

    let opts = CreateOpts {
        memory_type,
        tags: body.tags.unwrap_or_default(),
        importance: body.importance.unwrap_or(0.5),
        emotional_valence: body.emotional_valence.unwrap_or(0.0),
        arousal: body.arousal.unwrap_or(0.0),
        health: body.health,
        ttl: body.ttl,
        source: body.source.clone(),
    };

    let graph_extraction = body.graph_extraction.clone();
    let (memory, embedding) = state.services.memory.create(&body.content, opts).await?;

    if let Some(extraction) = graph_extraction {
        if let Err(e) = process_graph_extraction(&state, memory.id, &extraction).await {
            tracing::warn!(memory_id = %memory.id, error = %e, "graph extraction processing failed");
        }
    }

    // Fire-and-forget: auto-discovery runs in the background.
    // If the channel is full, discovery is skipped for this memory (not an error).
    let threshold = state.config.connections.auto_discovery_threshold;
    let top_k = state.config.connections.auto_discovery_top_k;
    if let Err(e) = state.services.discovery_tx.try_send(
        crate::services::connection_manager::DiscoveryTask {
            memory_id: memory.id,
            embedding,
            threshold,
            top_k,
        },
    ) {
        match e {
            tokio::sync::mpsc::error::TrySendError::Full(_) => {
                state.services.dropped_discovery_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(memory_id = %memory.id, "discovery queue full; skipping auto_discover");
            }
            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                tracing::error!(memory_id = %memory.id, "discovery channel closed; workers may have exited");
            }
        }
    }

    Ok((StatusCode::CREATED, Json(memory)))
}

#[utoipa::path(
    get,
    path = "/api/v1/memories/{id}",
    params(
        ("id" = Uuid, Path, description = "Memory UUID"),
        ("include_connections" = Option<bool>, Query, description = "Include connection list"),
    ),
    responses(
        (status = 200, description = "Memory object", body = Memory),
        (status = 404, description = "Not found", body = ErrorResponse),
    ),
    tag = "memories"
)]
pub async fn get_memory(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(q): Query<GetQuery>,
) -> Result<Json<Memory>> {
    let mut memory = state.services.memory.get(id).await?;
    if q.include_connections.unwrap_or(false) {
        memory.connections = state.services.memory.fetch_connections(id).await?;
    }
    Ok(Json(memory))
}

#[utoipa::path(
    put,
    path = "/api/v1/memories/{id}",
    params(("id" = Uuid, Path, description = "Memory UUID")),
    request_body = UpdateMemoryRequest,
    responses(
        (status = 200, description = "Updated memory", body = Memory),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 422, description = "Validation error", body = ErrorResponse),
    ),
    tag = "memories"
)]
pub async fn update_memory(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateMemoryRequest>,
) -> Result<Json<Memory>> {
    if let Some(content) = &body.content {
        if content.len() > MAX_CONTENT_BYTES {
            return Err(AppError::Validation(format!(
                "content exceeds maximum length of {MAX_CONTENT_BYTES} bytes"
            )));
        }
    }
    if let Some(tags) = &body.tags {
        if tags.len() > MAX_TAGS {
            return Err(AppError::Validation(format!(
                "too many tags: maximum is {MAX_TAGS}, got {}", tags.len()
            )));
        }
    }
    use crate::services::memory_manager::UpdatePatch;
    let patch = UpdatePatch {
        content: body.content,
        tags: body.tags,
        importance: body.importance,
        emotional_valence: body.emotional_valence,
        arousal: body.arousal,
        health: body.health,
        source: body.source,
    };
    let memory = state.services.memory.update(id, patch).await?;
    Ok(Json(memory))
}

#[utoipa::path(
    delete,
    path = "/api/v1/memories/{id}",
    params(
        ("id" = Uuid, Path, description = "Memory UUID"),
        ("hard" = Option<bool>, Query, description = "Permanently remove (default false = soft delete)"),
    ),
    responses(
        (status = 200, description = "Deleted", body = DeleteResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
    ),
    tag = "memories"
)]
pub async fn delete_memory(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(q): Query<DeleteQuery>,
) -> Result<Json<DeleteResponse>> {
    state
        .services
        .memory
        .delete(id, q.hard.unwrap_or(false))
        .await?;
    Ok(Json(DeleteResponse {
        success: true,
        message: "memory deleted",
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/memories",
    params(
        ("limit" = Option<usize>, Query, description = "Max results (default 10, max 100)"),
        ("offset" = Option<usize>, Query, description = "Pagination offset"),
        ("memory_type" = Option<String>, Query, description = "Filter: short_term or long_term"),
        ("tags" = Option<String>, Query, description = "Comma-separated tag filter"),
        ("min_importance" = Option<f32>, Query, description = "Minimum importance 0.0–1.0"),
        ("max_importance" = Option<f32>, Query, description = "Maximum importance 0.0–1.0"),
        ("created_after" = Option<String>, Query, description = "RFC-3339 timestamp lower bound"),
        ("created_before" = Option<String>, Query, description = "RFC-3339 timestamp upper bound"),
        ("include_connections" = Option<bool>, Query, description = "Include connection lists"),
    ),
    responses(
        (status = 200, description = "Paginated memory list", body = MemoryListResponse),
    ),
    tag = "memories"
)]
pub async fn list_memories(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<MemoryListResponse>> {
    let limit = q.limit.unwrap_or(10).min(100);
    let offset = q.offset.unwrap_or(0);

    let filters = build_filters(&q);
    let include_connections = q.include_connections.unwrap_or(false);

    let (mut memories, total) = state.services.memory.list(&filters, limit, offset).await?;

    if include_connections {
        for mem in &mut memories {
            mem.connections = state
                .services
                .memory
                .fetch_connections(mem.id)
                .await
                .unwrap_or_default();
        }
    }

    Ok(Json(MemoryListResponse {
        total,
        limit,
        offset,
        memories,
    }))
}

fn build_filters(q: &ListQuery) -> MemoryFilters {
    MemoryFilters {
        memory_type: q.memory_type.clone(),
        tags: q.tags.clone(),
        min_importance: q.min_importance,
        max_importance: q.max_importance,
        created_after: q.created_after,
        created_before: q.created_before,
    }
}

async fn process_graph_extraction(
    state: &AppState,
    memory_id: Uuid,
    extraction: &GraphExtraction,
) -> Result<()> {
    let mut entity_ids: HashMap<String, Uuid> = HashMap::new();

    for entity in extraction.entities.iter().take(MAX_GRAPH_ENTITIES) {
        let name = entity.name.trim();
        if name.is_empty() {
            continue;
        }

        let entity_type = entity.entity_type.as_deref().unwrap_or("entity").trim();
        let description = entity.description.as_deref().unwrap_or("").trim();
        let content = if description.is_empty() {
            format!("Entity: {name}\nType: {entity_type}")
        } else {
            format!("Entity: {name}\nType: {entity_type}\nDescription: {description}")
        };

        let (entity_memory, _) = state
            .services
            .memory
            .create(
                &content,
                CreateOpts {
                    memory_type: MemoryType::LongTerm,
                    tags: vec![
                        "__entity__".to_owned(),
                        format!("entity:{entity_type}"),
                    ],
                    importance: 0.6,
                    emotional_valence: 0.0,
                    arousal: 0.0,
                    health: Some(100.0),
                    ttl: None,
                    source: Some("graph_extraction".to_owned()),
                },
            )
            .await?;

        entity_ids.insert(name.to_lowercase(), entity_memory.id);

        let _ = state
            .services
            .connection
            .create(memory_id, entity_memory.id, RelationshipType::References, 1.0)
            .await;
    }

    for relation in extraction.relationships.iter().take(MAX_GRAPH_RELATIONSHIPS) {
        let Some(source_id) = entity_ids.get(&relation.source.trim().to_lowercase()).copied() else {
            continue;
        };
        let Some(target_id) = entity_ids.get(&relation.target.trim().to_lowercase()).copied() else {
            continue;
        };
        let rel = relation
            .relationship_type
            .as_deref()
            .and_then(|s| RelationshipType::try_from(s).ok())
            .unwrap_or(RelationshipType::RelatedTo);
        let strength = relation.strength.unwrap_or(0.8);
        let _ = state
            .services
            .connection
            .create(source_id, target_id, rel, strength)
            .await;
    }

    Ok(())
}
