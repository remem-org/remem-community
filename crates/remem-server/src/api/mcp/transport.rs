use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::{Extension, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use parking_lot::Mutex;
use uuid::Uuid;

use crate::api::mcp::{handler, protocol};
use crate::api::AppState;

const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(1800);
const SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const SESSION_HEADER: &str = "Mcp-Session-Id";
// Matches the cap the old (now-deleted) sse.rs enforced via MCP_MAX_SSE_SESSIONS.
// /mcp sits behind auth, so this is a defensive bound on a trusted-but-fallible
// caller, not a pre-auth DoS mitigation.
const MAX_SESSIONS: usize = 1000;

#[derive(Clone, Default)]
struct Sessions(Arc<Mutex<HashMap<Uuid, Instant>>>);

impl Sessions {
    /// Returns `None` if the session cap has been reached (caller should
    /// reject the request rather than growing the map unboundedly).
    fn try_create(&self) -> Option<Uuid> {
        let mut guard = self.0.lock();
        if guard.len() >= MAX_SESSIONS {
            return None;
        }
        let id = Uuid::new_v4();
        guard.insert(id, Instant::now());
        Some(id)
    }

    /// Bump last-seen if `id` is a known session; returns false if unknown
    /// (caller should reject the request).
    fn touch(&self, id: Uuid) -> bool {
        let mut guard = self.0.lock();
        match guard.get_mut(&id) {
            Some(seen) => {
                *seen = Instant::now();
                true
            }
            None => false,
        }
    }

    fn remove(&self, id: Uuid) {
        self.0.lock().remove(&id);
    }

    fn sweep(&self, idle_timeout: Duration) {
        let now = Instant::now();
        self.0
            .lock()
            .retain(|_, last_seen| now.duration_since(*last_seen) < idle_timeout);
    }
}

/// Mount the MCP Streamable HTTP endpoint at `/mcp`. The caller must merge
/// this into the same `api` router that has the auth middleware layer
/// applied, and merge it in BEFORE that `.layer(...)` call — see
/// `crate::api::build_router`.
pub fn router() -> Router<AppState> {
    let sessions = Sessions::default();

    let sweep_sessions = sessions.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(SESSION_SWEEP_INTERVAL);
        loop {
            ticker.tick().await;
            sweep_sessions.sweep(SESSION_IDLE_TIMEOUT);
        }
    });

    Router::new()
        .route(
            "/mcp",
            post(handle_post).get(handle_get).delete(handle_delete),
        )
        .layer(Extension(sessions))
}

async fn handle_post(
    State(app): State<AppState>,
    Extension(sessions): Extension<Sessions>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let req: protocol::JsonRpcRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {"code": protocol::PARSE_ERROR, "message": e.to_string()}
                })),
            )
                .into_response();
        }
    };

    let is_initialize = req.method == "initialize";
    if !is_initialize {
        let valid_session = headers
            .get(SESSION_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| Uuid::parse_str(v).ok())
            .map(|id| sessions.touch(id))
            .unwrap_or(false);
        if !valid_session {
            return (
                StatusCode::BAD_REQUEST,
                format!("missing or unknown {SESSION_HEADER} header"),
            )
                .into_response();
        }
    }

    // Reserve the session slot up front for `initialize` so a full session
    // table is rejected before doing any request handling work.
    let reserved_id = if is_initialize {
        match sessions.try_create() {
            Some(id) => Some(id),
            None => {
                tracing::warn!(
                    max_sessions = MAX_SESSIONS,
                    "MCP session cap reached, refusing new session"
                );
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "too many active MCP sessions",
                )
                    .into_response();
            }
        }
    } else {
        None
    };

    match handler::handle(&req, &app).await {
        None => StatusCode::ACCEPTED.into_response(),
        Some(resp) => {
            let mut http_resp = (StatusCode::OK, Json(resp)).into_response();
            if let Some(new_id) = reserved_id {
                if let Ok(value) = HeaderValue::from_str(&new_id.to_string()) {
                    http_resp.headers_mut().insert(SESSION_HEADER, value);
                }
            }
            http_resp
        }
    }
}

/// We don't support server-initiated push streams — the spec explicitly
/// allows returning 405 here.
async fn handle_get() -> StatusCode {
    StatusCode::METHOD_NOT_ALLOWED
}

async fn handle_delete(headers: HeaderMap, Extension(sessions): Extension<Sessions>) -> StatusCode {
    if let Some(id) = headers
        .get(SESSION_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| Uuid::parse_str(v).ok())
    {
        sessions.remove(id);
    }
    StatusCode::NO_CONTENT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_lifecycle() {
        let sessions = Sessions::default();
        let id = sessions.try_create().unwrap();
        assert!(sessions.touch(id));
        sessions.remove(id);
        assert!(!sessions.touch(id));
    }

    #[test]
    fn touch_unknown_session_returns_false() {
        let sessions = Sessions::default();
        assert!(!sessions.touch(Uuid::new_v4()));
    }

    #[test]
    fn sweep_evicts_idle_sessions() {
        let sessions = Sessions::default();
        let id = sessions.try_create().unwrap();
        // Force the "last seen" instant into the past by sweeping with a
        // zero-duration timeout — anything is "older than zero".
        sessions.sweep(Duration::from_secs(0));
        assert!(!sessions.touch(id));
    }

    #[test]
    fn try_create_returns_none_at_capacity() {
        let sessions = Sessions::default();
        for _ in 0..MAX_SESSIONS {
            sessions.try_create().expect("under cap");
        }
        assert!(sessions.try_create().is_none());
    }
}
