use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

/// Standardised error body returned by all failing API responses.
#[derive(Serialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub detail: String,
}

#[derive(Error, Debug)]
pub enum AppError {
    #[error("memory not found: {0}")]
    NotFound(Uuid),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[allow(dead_code)]
    #[error("unauthorized")]
    Unauthorized,

    #[error("embedding error: {0}")]
    Embedding(String),

    #[error("storage error: {0}")]
    Storage(#[from] crate::engine::StorageError),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
            AppError::Conflict(_) => StatusCode::CONFLICT,
            AppError::Unauthorized => StatusCode::UNAUTHORIZED,
            AppError::Embedding(_)
            | AppError::Storage(_)
            | AppError::Serialization(_)
            | AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        // 4xx variants are client-facing by design (not found / bad input /
        // conflict) and carry no internal detail, so it's safe to return
        // `self.to_string()` verbatim. 5xx variants can wrap storage paths,
        // io errors, and serde messages (e.g. StorageError::Corruption{file}) —
        // those are logged server-side only; the client gets a generic body.
        let detail = if status.is_server_error() {
            tracing::error!(error = %self, status = %status, "internal error");
            "internal server error".to_string()
        } else {
            self.to_string()
        };

        let body = Json(ErrorResponse { detail });
        (status, body).into_response()
    }
}

pub type Result<T> = std::result::Result<T, AppError>;
