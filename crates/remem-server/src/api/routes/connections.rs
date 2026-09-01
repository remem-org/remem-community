use axum::{
    extract::{Path, State},
    Json,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::extract::ValidatedQuery;
use crate::api::AppState;
use crate::error::{AppError, Result};
use crate::services::cursor::ConnectionCursor;
use crate::services::types::{Connection, Memory, RelationshipType};

#[derive(Deserialize, utoipa::ToSchema)]
pub struct CreateConnectionRequest {
    pub source_id: Uuid,
    pub target_id: Uuid,
    pub relationship_type: Option<String>,
    pub strength: Option<f32>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct ConnectionResponse {
    pub source_id: Uuid,
    pub connection: Connection,
}

#[derive(Deserialize)]
pub struct ListConnectionsQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    /// Continuation from a previous response's `next_cursor`.
    pub cursor: Option<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct ListConnectionsResponse {
    pub connections: Vec<ConnectionResponse>,
    pub limit: usize,
    /// Pass back as `cursor` to continue. Absent once the listing is
    /// exhausted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Whether further connections exist beyond this page.
    pub has_more: bool,
    /// Whether the walk stopped at its effort bound before it could fill the
    /// page or establish that nothing remains. Same meaning as on the memory
    /// listing and the search response.
    pub truncated: bool,
}

#[derive(Deserialize)]
pub struct RelatedQuery {
    pub depth: Option<usize>,
    pub relationship_types: Option<String>, // comma-separated
    pub limit: Option<usize>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct RelatedResponse {
    pub memory_id: Uuid,
    /// The memory the traversal started from.
    ///
    /// Present so a caller rendering a graph has everything it needs from
    /// one request. Fetching the centre separately would work, but fetching
    /// a memory by id is a recall -- drawing a graph would then reinforce
    /// the very memories it is only displaying. `None` when the memory does
    /// not exist, which is also when `related` is empty.
    pub memory: Option<Memory>,
    pub related: Vec<RelatedItem>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct RelatedItem {
    pub memory: Memory,
    pub connection: Connection,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct DeleteConnectionResponse {
    pub success: bool,
    pub message: &'static str,
}

#[utoipa::path(
    get,
    path = "/api/v1/connections",
    params(
        ("limit" = Option<usize>, Query, description = "Max results (default 50, max 500)"),
        ("cursor" = Option<String>, Query, description = "Continuation from a previous response's next_cursor"),
    ),
    responses(
        (status = 200, description = "Paginated connection list", body = ListConnectionsResponse),
    ),
    tag = "connections"
)]
pub async fn list_connections(
    State(state): State<AppState>,
    ValidatedQuery(q): ValidatedQuery<ListConnectionsQuery>,
) -> Result<Json<ListConnectionsResponse>> {
    let limit = q.limit.unwrap_or(50).min(500);
    if q.offset.is_some_and(|o| o > 0) {
        return Err(AppError::Validation(
            "connections are paged with cursor, not offset: pass the next_cursor from the \
             previous page"
                .into(),
        ));
    }
    let start = match q.cursor.as_deref() {
        Some(token) => Some(
            ConnectionCursor::decode(token)
                .map_err(|_| AppError::Validation("cursor is not valid for this request".into()))?,
        ),
        None => None,
    };

    let page = state
        .services
        .connection
        .list_page(limit, start, state.config.search.list_max_factor)
        .await?;
    let connections: Vec<ConnectionResponse> = page
        .connections
        .into_iter()
        .map(|(src, conn)| ConnectionResponse {
            source_id: src,
            connection: conn,
        })
        .collect();
    Ok(Json(ListConnectionsResponse {
        connections,
        limit,
        next_cursor: page.next_cursor,
        has_more: page.has_more,
        truncated: page.truncated,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/connections",
    request_body = CreateConnectionRequest,
    responses(
        (status = 200, description = "Connection created", body = ConnectionResponse),
        (status = 422, description = "Unknown relationship_type", body = ErrorResponse),
        (status = 500, description = "Storage error", body = ErrorResponse),
    ),
    tag = "connections"
)]
pub async fn create_connection(
    State(state): State<AppState>,
    Json(body): Json<CreateConnectionRequest>,
) -> Result<Json<ConnectionResponse>> {
    let rel = body
        .relationship_type
        .as_deref()
        .map(RelationshipType::try_from)
        .transpose()
        .map_err(AppError::Validation)?
        .unwrap_or(RelationshipType::RelatedTo);

    let connection = state
        .services
        .connection
        .create(
            body.source_id,
            body.target_id,
            rel,
            body.strength.unwrap_or(1.0),
        )
        .await?;

    Ok(Json(ConnectionResponse {
        source_id: body.source_id,
        connection,
    }))
}

#[utoipa::path(
    delete,
    path = "/api/v1/connections/{source_id}/{target_id}",
    params(
        ("source_id" = Uuid, Path, description = "Source memory UUID"),
        ("target_id" = Uuid, Path, description = "Target memory UUID"),
    ),
    responses(
        (status = 200, description = "Connection removed", body = DeleteConnectionResponse),
        (status = 404, description = "Connection not found", body = ErrorResponse),
    ),
    tag = "connections"
)]
pub async fn delete_connection(
    State(state): State<AppState>,
    Path((source_id, target_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<DeleteConnectionResponse>> {
    state
        .services
        .connection
        .delete(source_id, target_id)
        .await?;
    Ok(Json(DeleteConnectionResponse {
        success: true,
        message: "connection removed",
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/memories/{id}/related",
    params(
        ("id" = Uuid, Path, description = "Memory UUID"),
        ("depth" = Option<usize>, Query, description = "Traversal depth (default 1, max 5)"),
        ("relationship_types" = Option<String>, Query, description = "Comma-separated relationship type filter"),
        ("limit" = Option<usize>, Query, description = "Max results (default 20, max 100)"),
    ),
    responses(
        (status = 200, description = "The centre memory plus related memories with connection metadata. Traversal records no recall — neither for the centre nor for the memories reached.", body = RelatedResponse),
        (status = 404, description = "Memory not found", body = ErrorResponse),
    ),
    tag = "memories"
)]
pub async fn find_related(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    ValidatedQuery(q): ValidatedQuery<RelatedQuery>,
) -> Result<Json<RelatedResponse>> {
    let depth = q.depth.unwrap_or(1).min(5);
    let types: Vec<RelationshipType> = q
        .relationship_types
        .as_deref()
        .map(|s| {
            s.split(',')
                .filter(|t| !t.trim().is_empty())
                .map(|t| RelationshipType::try_from(t.trim()).map_err(AppError::Validation))
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();

    let pairs = state
        .services
        .connection
        .find_related(id, depth, &types)
        .await?;

    let limit = q.limit.unwrap_or(20).min(100);
    let related: Vec<RelatedItem> = pairs
        .into_iter()
        .take(limit)
        .map(|(memory, connection)| RelatedItem { memory, connection })
        .collect();

    // Read through the repository rather than `MemoryManager::get`: this is
    // a traversal, and a traversal records no recall -- for the centre any
    // more than for the memories it reaches.
    let memory = state
        .services
        .repo
        .load(id)
        .await?
        .filter(|stored| !stored.archived)
        .map(|stored| stored.into_api(Vec::new()));

    Ok(Json(RelatedResponse {
        memory_id: id,
        memory,
        related,
    }))
}
