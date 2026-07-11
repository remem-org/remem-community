use anyhow::anyhow;
use serde_json::{json, Value};

use crate::client::RememClient;

/// Return the MCP `resources/list` result value.
pub fn list() -> Value {
    json!({
        "resources": [
            {
                "uri": "memory://stats",
                "name": "System Statistics",
                "description": "Memory system statistics (counts, connections, avg importance)",
                "mimeType": "application/json"
            },
            {
                "uri": "memory://collections/recent",
                "name": "Recent Memories",
                "description": "Recently created memories (supports ?limit=N&offset=N)",
                "mimeType": "application/json"
            },
            {
                "uri": "memory://collections/important",
                "name": "Important Memories",
                "description": "Memories sorted by importance (supports ?limit=N&offset=N)",
                "mimeType": "application/json"
            }
        ]
    })
}

/// Dispatch a `resources/read` request.
pub async fn read(params: &Value, client: &RememClient, request_id: &str) -> anyhow::Result<Value> {
    let uri = params["uri"].as_str().ok_or_else(|| anyhow!("missing uri"))?;

    // Strip "memory://" scheme
    let rest = uri
        .strip_prefix("memory://")
        .ok_or_else(|| anyhow!("unsupported URI scheme in '{uri}'"))?;

    let (path, query_str) = rest.split_once('?').unwrap_or((rest, ""));

    let parse_int = |key: &str, default: i64| -> i64 {
        query_str
            .split('&')
            .find_map(|p| {
                p.strip_prefix(&format!("{key}="))
                    .and_then(|v| v.parse().ok())
            })
            .unwrap_or(default)
    };

    let limit = parse_int("limit", 10);
    let offset = parse_int("offset", 0);

    let text = match path {
        "stats" => {
            let data = client.get_stats(request_id).await?;
            serde_json::to_string_pretty(&data)?
        }

        "collections/recent" => {
            let data = client.list_memories(limit, offset, None, request_id).await?;
            let empty = vec![];
            let memories = data["memories"].as_array().unwrap_or(&empty);
            let count = memories.len();
            let result = json!({
                "memories": memories.iter().map(summarise).collect::<Vec<_>>(),
                "pagination": {"limit": limit, "offset": offset, "count": count, "total": data["total"]}
            });
            serde_json::to_string_pretty(&result)?
        }

        "collections/important" => {
            let data = client.list_memories(200, 0, None, request_id).await?;
            let empty = vec![];
            let mut all: Vec<&Value> = data["memories"].as_array().unwrap_or(&empty).iter().collect();
            all.sort_by(|a, b| {
                let ia = a["metadata"]["importance"].as_f64().unwrap_or(0.0);
                let ib = b["metadata"]["importance"].as_f64().unwrap_or(0.0);
                ib.partial_cmp(&ia).unwrap_or(std::cmp::Ordering::Equal)
            });
            let page: Vec<_> = all
                .into_iter()
                .skip(offset as usize)
                .take(limit as usize)
                .map(summarise)
                .collect();
            let count = page.len();
            let result = json!({
                "memories": page,
                "pagination": {"limit": limit, "offset": offset, "count": count}
            });
            serde_json::to_string_pretty(&result)?
        }

        p if p.starts_with("graph/") => {
            let memory_id = &p["graph/".len()..];
            let depth = parse_int("depth", 2);
            let data = client.find_related(memory_id, depth, 50, request_id).await?;
            serde_json::to_string_pretty(&data)?
        }

        _ => return Err(anyhow!("unknown resource path: {path}")),
    };

    Ok(json!({ "contents": [{"uri": uri, "mimeType": "application/json", "text": text}] }))
}

fn summarise(m: &Value) -> Value {
    let content = m["content"].as_str().unwrap_or("");
    let preview = if content.len() > 100 {
        format!("{}…", &content[..100])
    } else {
        content.to_string()
    };
    json!({
        "id": m["id"],
        "content_preview": preview,
        "type": m["memory_type"],
        "importance": m["metadata"]["importance"],
        "tags": m["metadata"]["tags"],
        "created_at": m["metadata"]["created_at"]
    })
}
