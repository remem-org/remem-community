use axum::{
    body::Body,
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};

use crate::api::AppState;
use crate::error::ErrorResponse;

pub async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    // Health, readiness, metrics, and OpenAPI spec endpoints are always public
    if req.uri().path() == "/api/v1/health"
        || req.uri().path() == "/api/v1/ready"
        || req.uri().path() == "/api/v1/health/deep"
        || req.uri().path() == "/api/v1/metrics"
        || req.uri().path() == "/api/v1/openapi.json"
    {
        return next.run(req).await;
    }
    // Auth disabled when api_key is empty — require explicit opt-in
    if state.config.server.api_key.is_empty() {
        if state.config.server.allow_auth_disabled {
            return next.run(req).await;
        }
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                detail: "server misconfigured: REMEM_API_KEY is not set. Set REMEM_ALLOW_AUTH_DISABLED=true to allow unauthenticated access (development only).".to_string(),
            }),
        )
            .into_response();
    }

    let x_api_key = req
        .headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let bearer = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");

    if x_api_key == state.config.server.api_key || bearer == state.config.server.api_key {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse { detail: "invalid or missing API key".to_string() }),
        )
            .into_response()
    }
}
