use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Json,
    },
    routing::{get, post},
    Router,
};
use futures::{stream, StreamExt};
use parking_lot::Mutex;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

use crate::client::RememClient;
use crate::{handler, protocol::JsonRpcRequest};

type Sessions = Arc<Mutex<HashMap<Uuid, mpsc::UnboundedSender<String>>>>;

#[derive(Clone)]
struct AppState {
    sessions: Sessions,
    client: Arc<RememClient>,
}

pub async fn run(client: Arc<RememClient>, host: &str, port: u16) -> anyhow::Result<()> {
    let state = AppState {
        sessions: Arc::new(Mutex::new(HashMap::new())),
        client,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/sse", get(sse_connect))
        .route("/messages", post(sse_message))
        .with_state(state);

    let addr = format!("{host}:{port}");
    tracing::info!(addr, "SSE transport listening");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({"status": "healthy", "server": "remem-mcp"}))
}

async fn sse_connect(State(state): State<AppState>) -> impl IntoResponse {
    let session_id = Uuid::new_v4();
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    state.sessions.lock().insert(session_id, tx);
    tracing::info!(%session_id, "SSE client connected");

    let endpoint_url = format!("/messages?sessionId={session_id}");

    // Send the endpoint event first, then relay channel messages.
    let endpoint_event = stream::once(async move {
        Ok::<Event, Infallible>(Event::default().event("endpoint").data(endpoint_url))
    });
    let msg_stream = UnboundedReceiverStream::new(rx)
        .map(|msg| Ok::<Event, Infallible>(Event::default().data(msg)));

    Sse::new(endpoint_event.chain(msg_stream)).keep_alive(KeepAlive::default())
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
        let sessions = state.sessions.lock();
        sessions.get(&q.session_id).cloned()
    };
    let tx = match tx {
        Some(t) => t,
        None => {
            tracing::warn!(session_id = %q.session_id, "POST to unknown session");
            return StatusCode::NOT_FOUND;
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
                    if tx.send(json).is_err() {
                        // Receiver dropped — SSE connection closed.
                        tracing::info!(%session_id, "SSE client gone, removing session");
                        sessions.lock().remove(&session_id);
                    }
                }
                Err(e) => tracing::error!("failed to serialize response: {e}"),
            }
        }
    });

    StatusCode::ACCEPTED
}
