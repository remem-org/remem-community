use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::AppState;
use crate::error::{AppError, ErrorResponse, Result};
use crate::services::search_engine::SearchQuery;
use crate::services::types::{MemoryFilters, MemoryType, SearchResult, SearchType};

const MAX_SEARCH_LIMIT: usize = 500;

#[derive(Deserialize, utoipa::ToSchema)]
pub struct SearchRequest {
    pub query: String,
    /// One of: semantic (default), keyword, hybrid
    pub search_type: Option<String>,
    pub limit: Option<usize>,
    pub memory_type: Option<String>,
    pub tags: Option<Vec<String>>,
    pub min_importance: Option<f32>,
    pub max_importance: Option<f32>,
    /// When set, memories connected to this ID in the graph are boosted in results.
    pub related_to: Option<Uuid>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct SearchResponse {
    pub results: Vec<SearchResult>,
    pub total: usize,
}

#[utoipa::path(
    post,
    path = "/api/v1/memories/search",
    request_body = SearchRequest,
    responses(
        (status = 200, description = "Search results ranked by relevance", body = SearchResponse),
        (status = 422, description = "Validation error (empty query, unknown search_type)", body = ErrorResponse),
        (status = 500, description = "Embedding or storage error", body = ErrorResponse),
    ),
    tag = "memories"
)]
pub async fn search_memories(
    State(state): State<AppState>,
    Json(body): Json<SearchRequest>,
) -> Result<Json<SearchResponse>> {
    if body.query.trim().is_empty() {
        return Err(AppError::Validation("query must not be empty".into()));
    }
    if let Some(limit) = body.limit {
        if limit > MAX_SEARCH_LIMIT {
            return Err(AppError::Validation(format!(
                "limit exceeds maximum of {MAX_SEARCH_LIMIT}"
            )));
        }
    }

    let search_type = match body.search_type.as_deref().unwrap_or("semantic") {
        "semantic" => SearchType::Semantic,
        "keyword" => SearchType::Keyword,
        "hybrid" => SearchType::Hybrid,
        other => {
            return Err(AppError::Validation(format!(
                "unknown search_type: {other}; use semantic, keyword, or hybrid"
            )))
        }
    };

    let memory_type = body
        .memory_type
        .as_deref()
        .map(MemoryType::try_from)
        .transpose()
        .map_err(AppError::Validation)?;

    let filters = MemoryFilters {
        memory_type,
        tags: body.tags.unwrap_or_default(),
        min_importance: body.min_importance,
        max_importance: body.max_importance,
        created_after: None,
        created_before: None,
    };

    let query = SearchQuery {
        query: body.query,
        search_type,
        filters,
        limit: body.limit.unwrap_or(10).min(100),
        related_to: body.related_to,
    };

    let results = state.services.search.search(&query).await?;
    let total = results.len();
    Ok(Json(SearchResponse { results, total }))
}
