use serde_json::{json, Value};
use uuid::Uuid;

use crate::client::RememClient;
use crate::protocol::{JsonRpcRequest, JsonRpcResponse, INTERNAL_ERROR, METHOD_NOT_FOUND};
use crate::{resources, tools};

/// Dispatch a JSON-RPC request.
/// Returns `None` for notifications (no response must be sent).
pub async fn handle(req: &JsonRpcRequest, client: &RememClient) -> Option<JsonRpcResponse> {
    if req.is_notification() {
        tracing::debug!(method = %req.method, "notification — no response");
        return None;
    }

    let request_id = Uuid::new_v4().to_string();
    let id = req.id.clone();
    let result: anyhow::Result<Value> = match req.method.as_str() {
        "initialize" => Ok(json!({
            "protocolVersion": "2024-11-05",
            "capabilities": { "tools": {}, "resources": {} },
            "serverInfo": { "name": "remem", "version": env!("CARGO_PKG_VERSION") }
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(tools::list()),
        "tools/call" => tools::call(&req.params, client, &request_id).await,
        "resources/list" => Ok(resources::list()),
        "resources/read" => resources::read(&req.params, client, &request_id).await,
        other => {
            return Some(JsonRpcResponse::err(
                id,
                METHOD_NOT_FOUND,
                format!("method not found: {other}"),
            ));
        }
    };

    Some(match result {
        Ok(v) => JsonRpcResponse::ok(id, v),
        Err(e) => JsonRpcResponse::err(id, INTERNAL_ERROR, e.to_string()),
    })
}
