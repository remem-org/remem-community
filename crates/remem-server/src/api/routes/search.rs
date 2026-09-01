use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::AppState;
use crate::error::{AppError, Result};
use crate::services::search_engine::SearchQuery;
use crate::services::types::{MemoryFilters, MemoryType, SearchResult, SearchType};

const MAX_SEARCH_LIMIT: usize = 100;

#[derive(Deserialize, utoipa::ToSchema)]
pub struct SearchRequest {
    pub query: String,
    /// One of: semantic (default), keyword, hybrid
    pub search_type: Option<String>,
    pub limit: Option<usize>,
    /// Zero-based result offset for pagination.
    pub offset: Option<usize>,
    pub memory_type: Option<String>,
    pub tags: Option<Vec<String>>,
    pub min_importance: Option<f32>,
    pub max_importance: Option<f32>,
    /// When set, memories connected to this ID in the graph are boosted in results.
    pub related_to: Option<Uuid>,
    /// Include the per-source breakdown behind each result's ranking.
    /// Defaults to true; set false for a leaner payload.
    pub explain: Option<bool>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct SearchResponse {
    pub results: Vec<SearchResult>,
    /// Number of ranked results traversed through the end of this page.
    /// This is not a full match count when `has_more` is true.
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
    pub has_more: bool,
    /// Whether the search stopped at its effort bound before it had found
    /// everything it was asked for.
    ///
    /// `false` with `has_more: false` means the result set is exhausted —
    /// these are all the matches. `true` means the search gave up early and
    /// further matches may exist; it cannot know. The two used to be
    /// indistinguishable, and a selective filter produced the second while
    /// reporting the first (REM-78).
    pub truncated: bool,
}

fn paginate<T>(results: Vec<T>, offset: usize, limit: usize) -> (Vec<T>, bool) {
    let has_more = results.len() > offset + limit;
    let page = results.into_iter().skip(offset).take(limit).collect();
    (page, has_more)
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

    let limit = body.limit.unwrap_or(10);
    let offset = body.offset.unwrap_or(0);
    let window = offset
        .checked_add(limit)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| AppError::Validation("offset and limit are too large".into()))?;

    let query = SearchQuery {
        query: body.query,
        search_type,
        filters,
        // Fetch one extra ranked result so callers can page without computing
        // the complete (potentially very large) result set.
        limit: window,
        related_to: body.related_to,
    };

    let outcome = state.services.search.search(&query).await?;
    let truncated = outcome.truncated;
    let (results, has_more) = paginate(outcome.results, offset, limit);
    let results = if body.explain.unwrap_or(true) {
        results
    } else {
        results
            .into_iter()
            .map(SearchResult::without_sources)
            .collect()
    };
    let total = offset + results.len();
    Ok(Json(SearchResponse {
        results,
        total,
        limit,
        offset,
        has_more,
        truncated,
    }))
}

#[cfg(test)]
mod tests {
    use super::paginate;

    #[test]
    fn pagination_returns_later_pages_and_detects_more_results() {
        let results: Vec<_> = (0..76).collect();

        let (page, has_more) = paginate(results, 50, 25);

        assert_eq!(page, (50..75).collect::<Vec<_>>());
        assert!(has_more);
    }

    #[test]
    fn pagination_marks_the_final_page() {
        let results: Vec<_> = (0..61).collect();

        let (page, has_more) = paginate(results, 50, 25);

        assert_eq!(page, (50..61).collect::<Vec<_>>());
        assert!(!has_more);
    }
}
