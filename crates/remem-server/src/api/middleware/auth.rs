use axum::{
    body::Body,
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use subtle::ConstantTimeEq;

use crate::api::AppState;
use crate::error::ErrorResponse;

/// Constant-time string comparison — avoids leaking key length/prefix via
/// response-time side channels. `==` on `&str` short-circuits on the first
/// mismatched byte, which is enough to time-attack an API key over the network.
fn secure_eq(a: &str, b: &str) -> bool {
    // ConstantTimeEq requires equal-length inputs; unequal lengths are not a
    // secret worth hiding (the attacker can already infer it from HTTP framing),
    // so it's safe to branch on length before the constant-time byte compare.
    a.len() == b.len() && a.as_bytes().ct_eq(b.as_bytes()).into()
}

/// True if `provided` matches the primary key, or the secondary key when one
/// is configured (rotation window: add the new key as secondary, roll out
/// callers, then promote it to primary and clear secondary — no downtime
/// window with zero valid keys).
fn key_matches(provided: &str, primary: &str, secondary: &str) -> bool {
    if provided.is_empty() {
        return false;
    }
    secure_eq(provided, primary) || (!secondary.is_empty() && secure_eq(provided, secondary))
}

pub async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    // Only basic liveness and the API spec are public. /health/deep and
    // /metrics disclose internal state (storage paths, error strings, request
    // rates) and must be authenticated like everything else.
    if req.uri().path() == "/api/v1/health"
        || req.uri().path() == "/api/v1/ready"
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

    let primary = state.config.server.api_key.as_str();
    let secondary = state.config.server.api_key_secondary.as_str();

    if key_matches(x_api_key, primary, secondary) || key_matches(bearer, primary, secondary) {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse { detail: "invalid or missing API key".to_string() }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secure_eq_matches_equal_strings() {
        assert!(secure_eq("secret-key", "secret-key"));
    }

    #[test]
    fn secure_eq_rejects_different_strings() {
        assert!(!secure_eq("secret-key", "other-key!"));
    }

    #[test]
    fn secure_eq_rejects_different_lengths() {
        assert!(!secure_eq("short", "much-longer-value"));
    }

    #[test]
    fn key_matches_primary() {
        assert!(key_matches("abc", "abc", ""));
    }

    #[test]
    fn key_matches_secondary_during_rotation() {
        assert!(key_matches("new-key", "old-key", "new-key"));
    }

    #[test]
    fn key_matches_rejects_empty_provided_even_if_secondary_empty() {
        assert!(!key_matches("", "", ""));
    }

    #[test]
    fn key_matches_does_not_accept_empty_against_empty_secondary() {
        // secondary unset ("") must never be treated as a valid credential
        assert!(!key_matches("", "primary-key", ""));
    }

    #[test]
    fn key_matches_rejects_unknown_key() {
        assert!(!key_matches("guess", "primary-key", "secondary-key"));
    }
}
