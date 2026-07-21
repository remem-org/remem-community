use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Json, Response,
    },
    routing::{get, post},
    Router,
};
use futures::{stream, StreamExt};
use parking_lot::Mutex;
use serde_json::json;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::client::RememClient;
use crate::{handler, protocol::JsonRpcRequest};

/// Per-session outbound channel capacity. A reader that falls this far
/// behind is treated as stuck and its session is evicted.
const CHANNEL_CAPACITY: usize = 32;
/// How often the idle-session sweep runs.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

struct SessionEntry {
    tx: mpsc::Sender<String>,
    /// Timestamp of the last `/messages` POST for this session — used by
    /// the idle sweep. Deliberately *not* updated by SSE keep-alive pings,
    /// which only prove the TCP connection is open, not that the client is
    /// actively using the session.
    last_active: Instant,
}

type Sessions = Arc<Mutex<HashMap<Uuid, SessionEntry>>>;

#[derive(Clone)]
struct AppState {
    sessions: Sessions,
    client: Arc<RememClient>,
    max_sessions: usize,
}

pub async fn run(
    client: Arc<RememClient>,
    host: &str,
    port: u16,
    max_sessions: usize,
    idle_timeout: Duration,
) -> anyhow::Result<()> {
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(idle_sweep(Arc::clone(&sessions), idle_timeout));

    let state = AppState {
        sessions,
        client,
        max_sessions,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/sse", get(sse_connect))
        .route("/messages", post(sse_message))
        .with_state(state);

    let addr = format!("{host}:{port}");
    tracing::info!(addr, max_sessions, ?idle_timeout, "SSE transport listening");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/// Periodically evicts sessions with no `/messages` activity for
/// `idle_timeout`. Dropping an evicted `SessionEntry` drops its `Sender`,
/// which ends that session's SSE stream (`rx.recv()` returns `None`),
/// forcing the client to reconnect if it's still around.
///
/// This is the backstop for connections that stay open at the TCP level
/// (keep-alive pings keep succeeding) but never send a real request again —
/// a case the disconnect-triggered `SessionGuard` cleanup in `sse_connect`
/// cannot catch, since nothing is wrong at the transport level.
async fn idle_sweep(sessions: Sessions, idle_timeout: Duration) {
    let mut ticker = tokio::time::interval(SWEEP_INTERVAL);
    loop {
        ticker.tick().await;
        let now = Instant::now();
        let mut sessions = sessions.lock();
        let before = sessions.len();
        sessions.retain(|_, entry| now.duration_since(entry.last_active) < idle_timeout);
        let evicted = before - sessions.len();
        if evicted > 0 {
            tracing::info!(evicted, remaining = sessions.len(), "evicted idle SSE sessions");
        }
    }
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({"status": "healthy", "server": "remem-mcp"}))
}

/// Removes its session from the map when dropped — fires whenever the SSE
/// response stream is dropped for any reason, in particular when the
/// client disconnects (the next keep-alive write to a dead socket fails,
/// hyper drops the response body, and this guard drops with it). Detection
/// latency is therefore bounded by the SSE keep-alive interval (Axum's
/// `KeepAlive::default()`, ~15s), not instantaneous.
struct SessionGuard {
    id: Uuid,
    sessions: Sessions,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        if self.sessions.lock().remove(&self.id).is_some() {
            tracing::info!(session_id = %self.id, "SSE session cleaned up (client disconnected)");
        }
    }
}

async fn sse_connect(State(state): State<AppState>) -> Response {
    if state.sessions.lock().len() >= state.max_sessions {
        tracing::warn!(max_sessions = state.max_sessions, "SSE session cap reached, refusing connection");
        return (StatusCode::SERVICE_UNAVAILABLE, "too many active SSE sessions").into_response();
    }

    let session_id = Uuid::new_v4();
    let (tx, rx) = mpsc::channel::<String>(CHANNEL_CAPACITY);
    state.sessions.lock().insert(
        session_id,
        SessionEntry {
            tx,
            last_active: Instant::now(),
        },
    );
    tracing::info!(%session_id, "SSE client connected");

    let endpoint_url = format!("/messages?sessionId={session_id}");

    // Send the endpoint event first, then relay channel messages.
    let endpoint_event = stream::once(async move {
        Ok::<Event, Infallible>(Event::default().event("endpoint").data(endpoint_url))
    });

    // `guard` is dropped (and cleans up `sessions`) whenever this stream is
    // dropped, whether it runs to completion (rx closed) or is cancelled
    // early by the caller (client disconnect) — see `stream::unfold` docs:
    // the closure's captured state is owned by the returned stream.
    let guard = SessionGuard {
        id: session_id,
        sessions: Arc::clone(&state.sessions),
    };
    let msg_stream = stream::unfold((guard, ReceiverStream::new(rx)), |(guard, mut rx)| async move {
        rx.next()
            .await
            .map(|msg| (Ok::<Event, Infallible>(Event::default().data(msg)), (guard, rx)))
    });

    Sse::new(endpoint_event.chain(msg_stream))
        .keep_alive(KeepAlive::default())
        .into_response()
}

#[derive(serde::Deserialize)]
struct SessionQuery {
    #[serde(rename = "sessionId")]
    session_id: Uuid,
}

async fn sse_message(
    State(state): State<AppState>,
    Query(q): Query<SessionQuery>,
    body: String,
) -> StatusCode {
    let tx = {
        let mut sessions = state.sessions.lock();
        match sessions.get_mut(&q.session_id) {
            Some(entry) => {
                entry.last_active = Instant::now();
                entry.tx.clone()
            }
            None => {
                tracing::warn!(session_id = %q.session_id, "POST to unknown session");
                return StatusCode::NOT_FOUND;
            }
        }
    };

    let req: JsonRpcRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("bad JSON-RPC request: {e}");
            return StatusCode::BAD_REQUEST;
        }
    };

    let client = Arc::clone(&state.client);
    let sessions = Arc::clone(&state.sessions);
    let session_id = q.session_id;

    tokio::spawn(async move {
        if let Some(resp) = handler::handle(&req, &client).await {
            match serde_json::to_string(&resp) {
                Ok(json) => {
                    if let Err(e) = tx.try_send(json) {
                        let reason = match e {
                            TrySendError::Full(_) => "channel full (slow reader)",
                            TrySendError::Closed(_) => "receiver dropped",
                        };
                        tracing::info!(%session_id, reason, "removing SSE session");
                        sessions.lock().remove(&session_id);
                    }
                }
                Err(e) => tracing::error!("failed to serialize response: {e}"),
            }
        }
    });

    StatusCode::ACCEPTED
}
