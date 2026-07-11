use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::AppState;
use crate::error::{AppError, ErrorResponse, Result};
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
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct ListConnectionsResponse {
    pub connections: Vec<ConnectionResponse>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
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
        ("offset" = Option<usize>, Query, description = "Pagination offset"),
    ),
    responses(
        (status = 200, description = "Paginated connection list", body = ListConnectionsResponse),
    ),
    tag = "connections"
)]
pub async fn list_connections(
    State(state): State<AppState>,
    Query(q): Query<ListConnectionsQuery>,
) -> Result<Json<ListConnectionsResponse>> {
    let limit = q.limit.unwrap_or(50).min(500);
    let offset = q.offset.unwrap_or(0);
    let (pairs, total) = state.services.connection.list_all(limit, offset).await?;
    let connections: Vec<ConnectionResponse> = pairs
        .into_iter()
        .map(|(src, conn)| ConnectionResponse { source_id: src, connection: conn })
        .collect();
    Ok(Json(ListConnectionsResponse { connections, total, limit, offset }))
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

    Ok(Json(ConnectionResponse { source_id: body.source_id, connection }))
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
    state.services.connection.delete(source_id, target_id)?;
    Ok(Json(DeleteConnectionResponse { success: true, message: "connection removed" }))
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
        (status = 200, description = "Related memories with connection metadata", body = RelatedResponse),
        (status = 404, description = "Memory not found", body = ErrorResponse),
    ),
    tag = "memories"
)]
pub async fn find_related(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(q): Query<RelatedQuery>,
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

    Ok(Json(RelatedResponse { memory_id: id, related }))
}
