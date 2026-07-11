use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A JSON-RPC 2.0 request or notification.
/// Notifications have no `id` field (deserialises as `None`).
#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    #[allow(dead_code)]
    pub jsonrpc: String,
    /// `None` when the message is a notification (no response expected).
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

impl JsonRpcRequest {
    pub fn is_notification(&self) -> bool {
        self.id.is_none()
    }
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl JsonRpcResponse {
    pub fn ok(id: Option<Value>, result: Value) -> Self {
        Self { jsonrpc: "2.0", id, result: Some(result), error: None }
    }

    pub fn err(id: Option<Value>, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(RpcError { code, message: message.into() }),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

pub const PARSE_ERROR: i32 = -32700;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INTERNAL_ERROR: i32 = -32603;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── JsonRpcRequest ────────────────────────────────────────────────────────

    #[test]
    fn request_with_id_is_not_notification() {
        let raw = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {}
        });
        let req: JsonRpcRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.jsonrpc, "2.0");
        assert_eq!(req.method, "initialize");
        assert!(!req.is_notification());
        assert!(req.id.is_some());
    }

    #[test]
    fn request_with_string_id() {
        let raw = json!({
            "jsonrpc": "2.0",
            "id": "abc-123",
            "method": "tools/list",
            "params": {}
        });
        let req: JsonRpcRequest = serde_json::from_value(raw).unwrap();
        assert!(!req.is_notification());
        assert_eq!(req.id, Some(json!("abc-123")));
    }

    #[test]
    fn notification_has_no_id() {
        let raw = json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        });
        let req: JsonRpcRequest = serde_json::from_value(raw).unwrap();
        assert!(req.is_notification());
        assert!(req.id.is_none());
    }

    #[test]
    fn request_params_default_to_null() {
        let raw = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list"
        });
        let req: JsonRpcRequest = serde_json::from_value(raw).unwrap();
        // params has #[serde(default)] so missing field becomes Value::Null
        assert_eq!(req.params, Value::Null);
    }

    #[test]
    fn request_with_nested_params() {
        let raw = json!({
            "jsonrpc": "2.0",
            "id": 42,
            "method": "tools/call",
            "params": {
                "name": "store_memory",
                "arguments": { "content": "hello" }
            }
        });
        let req: JsonRpcRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.params["name"], "store_memory");
        assert_eq!(req.params["arguments"]["content"], "hello");
    }

    // ── JsonRpcResponse::ok ───────────────────────────────────────────────────

    #[test]
    fn ok_response_has_result_no_error() {
        let resp = JsonRpcResponse::ok(Some(json!(1)), json!({"status": "ok"}));
        let v = serde_json::to_value(&resp).unwrap();

        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 1);
        assert_eq!(v["result"]["status"], "ok");
        assert!(v.get("error").is_none() || v["error"].is_null());
    }

    #[test]
    fn ok_response_with_null_id() {
        let resp = JsonRpcResponse::ok(None, json!(42));
        let v = serde_json::to_value(&resp).unwrap();
        // id is None → skip_serializing_if omits it
        assert!(!v.as_object().unwrap().contains_key("id"));
        assert_eq!(v["result"], 42);
    }

    // ── JsonRpcResponse::err ──────────────────────────────────────────────────

    #[test]
    fn error_response_has_error_no_result() {
        let resp = JsonRpcResponse::err(Some(json!(99)), -32601, "method not found");
        let v = serde_json::to_value(&resp).unwrap();

        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 99);
        assert_eq!(v["error"]["code"], -32601);
        assert_eq!(v["error"]["message"], "method not found");
        assert!(!v.as_object().unwrap().contains_key("result"));
    }

    // ── Error code constants ──────────────────────────────────────────────────

    #[test]
    fn error_codes_match_json_rpc_spec() {
        assert_eq!(PARSE_ERROR, -32700);
        assert_eq!(METHOD_NOT_FOUND, -32601);
        assert_eq!(INTERNAL_ERROR, -32603);
    }

    // ── MCP tool listing ──────────────────────────────────────────────────────

    #[test]
    fn tools_list_contains_all_eight_tools() {
        let list = crate::tools::list();
        let tools = list["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter()
            .filter_map(|t| t["name"].as_str())
            .collect();

        let expected = [
            "store_memory",
            "search_memories",
            "get_memory",
            "update_memory",
            "delete_memory",
            "find_related",
            "promote_to_longterm",
            "list_recent_memories",
        ];

        for name in expected {
            assert!(names.contains(&name), "missing tool: {name}");
        }
        assert_eq!(tools.len(), expected.len(), "unexpected number of tools");
    }

    #[test]
    fn each_tool_has_required_fields() {
        let list = crate::tools::list();
        let tools = list["tools"].as_array().unwrap();
        for tool in tools {
            let name = tool["name"].as_str().unwrap_or("?");
            assert!(tool.get("name").is_some(), "{name}: missing name");
            assert!(tool.get("description").is_some(), "{name}: missing description");
            assert!(tool.get("inputSchema").is_some(), "{name}: missing inputSchema");
            assert_eq!(
                tool["inputSchema"]["type"],
                "object",
                "{name}: inputSchema must be object type"
            );
        }
    }

    #[test]
    fn store_memory_requires_content() {
        let list = crate::tools::list();
        let tools = list["tools"].as_array().unwrap();
        let store = tools.iter().find(|t| t["name"] == "store_memory").unwrap();
        let required = store["inputSchema"]["required"].as_array().unwrap();
        assert!(
            required.iter().any(|r| r == "content"),
            "store_memory must require 'content'"
        );
    }
}
