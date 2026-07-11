use axum::{
    extract::{Path, State},
    Json,
};
use uuid::Uuid;

use crate::api::AppState;
use crate::error::{ErrorResponse, Result};
use crate::services::types::Memory;

#[utoipa::path(
    post,
    path = "/api/v1/memories/{id}/promote",
    params(("id" = Uuid, Path, description = "Memory ID")),
    responses(
        (status = 200, description = "Memory promoted to long-term", body = Memory),
        (status = 404, description = "Memory not found", body = ErrorResponse),
        (status = 500, description = "Storage error", body = ErrorResponse),
    ),
    tag = "memories"
)]
pub async fn promote_memory(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Memory>> {
    let stored = state.services.lifecycle.promote(id).await?;
    Ok(Json(stored.into_api(Vec::new())))
}
