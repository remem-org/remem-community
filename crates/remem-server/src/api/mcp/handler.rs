use serde_json::{json, Value};

use crate::api::mcp::protocol::{
    JsonRpcRequest, JsonRpcResponse, INTERNAL_ERROR, METHOD_NOT_FOUND,
};
use crate::api::mcp::{resources, tools};
use crate::api::partition::EffectivePartition;
use crate::api::AppState;

/// Dispatch a JSON-RPC request against `AppState` directly (no HTTP hop to
/// remem-server, because we ARE remem-server). Returns `None` for
/// notifications (no response must be sent).
pub async fn handle(
    req: &JsonRpcRequest,
    state: &AppState,
    partition: &EffectivePartition,
) -> Option<JsonRpcResponse> {
    if req.is_notification() {
        tracing::debug!(method = %req.method, "notification — no response");
        return None;
    }

    let id = req.id.clone();
    let result: anyhow::Result<Value> = match req.method.as_str() {
        "initialize" => Ok(json!({
            "protocolVersion": "2025-03-26",
            "capabilities": { "tools": {}, "resources": {} },
            "serverInfo": { "name": "remem", "version": env!("CARGO_PKG_VERSION") }
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(tools::list()),
        "tools/call" => tools::call(&req.params, state, partition).await,
        "resources/list" => Ok(resources::list()),
        "resources/read" => resources::read(&req.params, state).await,
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
