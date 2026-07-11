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
        let body = Json(ErrorResponse { detail: self.to_string() });
        (status, body).into_response()
    }
}

pub type Result<T> = std::result::Result<T, AppError>;
