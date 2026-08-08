use anyhow::anyhow;
use serde_json::{json, Value};

use crate::api::routes::health::compute_stats;
use crate::api::AppState;
use crate::services::types::{Memory, MemoryFilters, SortBy};

pub fn list() -> Value {
    json!({
        "resources": [
            {"uri": "memory://stats", "name": "System Statistics", "description": "Memory system statistics (counts, connections, avg importance)", "mimeType": "application/json"},
            {"uri": "memory://collections/recent", "name": "Recent Memories", "description": "Recently created memories (supports ?limit=N&offset=N)", "mimeType": "application/json"},
            {"uri": "memory://collections/important", "name": "Important Memories", "description": "Memories sorted by importance (supports ?limit=N&offset=N)", "mimeType": "application/json"}
        ]
    })
}

pub async fn read(params: &Value, state: &AppState) -> anyhow::Result<Value> {
    let uri = params["uri"]
        .as_str()
        .ok_or_else(|| anyhow!("missing uri"))?;
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
    let limit = (parse_int("limit", 10).max(0) as usize).min(100);
    let offset = parse_int("offset", 0).max(0) as usize;

    let text = match path {
        "stats" => {
            let stats = compute_stats(state).await?;
            serde_json::to_string_pretty(&stats)?
        }

        "collections/recent" => {
            let (memories, total) = state
                .services
                .memory
                .list(&MemoryFilters::default(), SortBy::CreatedAt, limit, offset)
                .await?;
            let count = memories.len();
            let result = json!({
                "memories": memories.iter().map(summarise).collect::<Vec<_>>(),
                "pagination": {"limit": limit, "offset": offset, "count": count, "total": total}
            });
            serde_json::to_string_pretty(&result)?
        }

        "collections/important" => {
            let (mut memories, _) = state
                .services
                .memory
                .list(&MemoryFilters::default(), SortBy::CreatedAt, 200, 0)
                .await?;
            memories.sort_by(|a, b| {
                b.metadata
                    .importance
                    .partial_cmp(&a.metadata.importance)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let page: Vec<Value> = memories
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|m| summarise(&m))
                .collect();
            let count = page.len();
            let result = json!({
                "memories": page,
                "pagination": {"limit": limit, "offset": offset, "count": count}
            });
            serde_json::to_string_pretty(&result)?
        }

        p if p.starts_with("graph/") => {
            let memory_id: uuid::Uuid = p["graph/".len()..]
                .parse()
                .map_err(|e| anyhow!("invalid memory id: {e}"))?;
            let depth = (parse_int("depth", 2).max(0) as usize).min(5);
            let pairs = state
                .services
                .connection
                .find_related(memory_id, depth, &[])
                .await?;
            let related: Vec<Value> = pairs
                .into_iter()
                .take(50)
                .map(|(m, c)| json!({"memory": m, "connection": c}))
                .collect();
            serde_json::to_string_pretty(&json!({"memory_id": memory_id, "related": related}))?
        }

        _ => return Err(anyhow!("unknown resource path: {path}")),
    };

    Ok(json!({ "contents": [{"uri": uri, "mimeType": "application/json", "text": text}] }))
}

fn truncate_preview(content: &str) -> String {
    if content.len() <= 100 {
        return content.to_string();
    }
    let cut = (0..=100)
        .rfind(|&i| content.is_char_boundary(i))
        .unwrap_or(0);
    format!("{}…", &content[..cut])
}

fn summarise(m: &Memory) -> Value {
    let preview = truncate_preview(&m.content);
    json!({
        "id": m.id,
        "content_preview": preview,
        "type": m.memory_type.to_string(),
        "importance": m.metadata.importance,
        "tags": m.metadata.tags,
        "created_at": m.metadata.created_at
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lists_three_resources() {
        let l = list();
        let resources = l["resources"].as_array().unwrap();
        assert_eq!(resources.len(), 3);
        let uris: Vec<&str> = resources.iter().filter_map(|r| r["uri"].as_str()).collect();
        assert!(uris.contains(&"memory://stats"));
        assert!(uris.contains(&"memory://collections/recent"));
        assert!(uris.contains(&"memory://collections/important"));
    }

    #[test]
    fn truncate_preview_does_not_split_multibyte_char_at_boundary() {
        // "é" is 2 bytes in UTF-8; place one straddling the byte-100 cut point.
        let content = format!("{}éé rest of the content", "a".repeat(99));
        // Should not panic, and should produce valid UTF-8.
        let preview = truncate_preview(&content);
        assert!(preview.ends_with('…'));
    }

    #[test]
    fn truncate_preview_leaves_short_content_untouched() {
        assert_eq!(truncate_preview("short"), "short");
    }
}
