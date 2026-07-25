use anyhow::anyhow;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::api::routes::memories::{
    create_memory_core, validate_create_memory, CreateMemoryRequest, UpdateMemoryRequest,
    MAX_CONTENT_BYTES, MAX_TAGS,
};
use crate::api::AppState;
use crate::services::memory_manager::UpdatePatch;
use crate::services::search_engine::SearchQuery;
use crate::services::types::{MemoryFilters, MemoryType, RelationshipType, SearchType, SortBy};

/// Return the MCP `tools/list` result value. Schema is identical to
/// `crates/remem-mcp/src/tools.rs::list()` except `update_memory` now
/// includes `emotional_valence`/`arousal`/`health` (REM-24 fix).
pub fn list() -> Value {
    json!({
        "tools": [
            {
                "name": "store_memory",
                "description": "Store a new memory. Automatically discovers similar memories. When possible, estimate emotional_valence (-1.0 negative to 1.0 positive), arousal (0.0 calm to 1.0 intense), and provide graph_extraction with key entities and relationships from the full conversation context.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "content": {"type": "string"},
                        "memory_type": {"type": "string", "enum": ["short_term", "long_term"]},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "importance": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                        "emotional_valence": {"type": "number", "minimum": -1.0, "maximum": 1.0, "description": "Estimated emotional valence for the user: -1 negative, 0 neutral, 1 positive"},
                        "arousal": {"type": "number", "minimum": 0.0, "maximum": 1.0, "description": "Estimated emotional intensity. Values >= 0.8 create protected long-term flashbulb memories."},
                        "health": {"type": "number", "minimum": 0.0, "maximum": 100.0, "description": "Optional active-forgetting health score. Defaults to 100."},
                        "ttl": {"type": "integer", "description": "TTL in seconds (short_term only)"},
                        "source": {"type": "string"},
                        "graph_extraction": {
                            "type": "object",
                            "description": "Extract key entities and relationships from the memory. Use stable names from conversation context.",
                            "properties": {
                                "entities": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string"},
                                            "entity_type": {"type": "string"},
                                            "description": {"type": "string"}
                                        },
                                        "required": ["name"]
                                    }
                                },
                                "relationships": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "source": {"type": "string"},
                                            "target": {"type": "string"},
                                            "relationship_type": {"type": "string", "description": "Prefer existing Remem relationship types such as related_to, references, supports, part_of, caused_by, similar_to."},
                                            "strength": {"type": "number", "minimum": 0.0, "maximum": 1.0}
                                        },
                                        "required": ["source", "target"]
                                    }
                                }
                            }
                        }
                    },
                    "required": ["content"]
                }
            },
            {
                "name": "search_memories",
                "description": "Search memories using semantic, keyword, or hybrid search. When related_to is provided, memories connected in the graph to that memory ID are boosted in results.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "search_type": {"type": "string", "enum": ["semantic", "keyword", "hybrid"]},
                        "limit": {"type": "integer"},
                        "related_to": {"type": "string", "description": "UUID of a memory whose graph neighbours should be boosted in results"},
                        "filters": {
                            "type": "object",
                            "properties": {
                                "memory_type": {"type": "string", "enum": ["short_term", "long_term"]},
                                "tags": {"type": "array", "items": {"type": "string"}},
                                "importance_min": {"type": "number"},
                                "importance_max": {"type": "number"}
                            }
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "get_memory",
                "description": "Retrieve a specific memory by ID.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "memory_id": {"type": "string"},
                        "include_connections": {"type": "boolean"}
                    },
                    "required": ["memory_id"]
                }
            },
            {
                "name": "update_memory",
                "description": "Update an existing memory's content, tags, importance, or emotional metadata.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "memory_id": {"type": "string"},
                        "content": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "importance": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                        "emotional_valence": {"type": "number", "minimum": -1.0, "maximum": 1.0},
                        "arousal": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                        "health": {"type": "number", "minimum": 0.0, "maximum": 100.0},
                        "source": {"type": "string"}
                    },
                    "required": ["memory_id"]
                }
            },
            {
                "name": "delete_memory",
                "description": "Delete a memory (soft archive by default, or hard delete).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "memory_id": {"type": "string"},
                        "hard_delete": {"type": "boolean"}
                    },
                    "required": ["memory_id"]
                }
            },
            {
                "name": "find_related",
                "description": "Find memories related to a given memory via the connection graph.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "memory_id": {"type": "string"},
                        "depth": {"type": "integer"},
                        "limit": {"type": "integer"}
                    },
                    "required": ["memory_id"]
                }
            },
            {
                "name": "promote_to_longterm",
                "description": "Promote a short-term memory to long-term storage.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "memory_id": {"type": "string"}
                    },
                    "required": ["memory_id"]
                }
            },
            {
                "name": "list_recent_memories",
                "description": "List recently created or accessed memories.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer"},
                        "memory_type": {"type": "string", "enum": ["short_term", "long_term"]},
                        "sort_by": {"type": "string", "enum": ["created_at", "accessed_at"]}
                    }
                }
            }
        ]
    })
}

/// Dispatch a `tools/call` request against `AppServices` directly (no HTTP hop).
pub async fn call(params: &Value, state: &AppState) -> anyhow::Result<Value> {
    let name = params["name"].as_str().ok_or_else(|| anyhow!("missing tool name"))?;
    let args = &params["arguments"];

    let result = match name {
        "store_memory" => store_memory(state, args).await,
        "search_memories" => search_memories(state, args).await,
        "get_memory" => get_memory(state, args).await,
        "update_memory" => update_memory(state, args).await,
        "delete_memory" => delete_memory(state, args).await,
        "find_related" => find_related(state, args).await,
        "promote_to_longterm" => promote_to_longterm(state, args).await,
        "list_recent_memories" => list_recent_memories(state, args).await,
        other => Err(anyhow!("unknown tool: {other}")),
    };

    Ok(text_content(match result {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(tool = name, error = %e, "tool call failed");
            json!({"success": false, "error": "TOOL_ERROR", "message": e.to_string()})
        }
    }))
}

fn text_content(data: Value) -> Value {
    json!({ "content": [{"type": "text", "text": data.to_string()}] })
}

fn parse_id(args: &Value) -> anyhow::Result<Uuid> {
    args["memory_id"]
        .as_str()
        .ok_or_else(|| anyhow!("memory_id is required"))?
        .parse::<Uuid>()
        .map_err(|e| anyhow!("invalid memory_id: {e}"))
}

async fn store_memory(state: &AppState, args: &Value) -> anyhow::Result<Value> {
    let body: CreateMemoryRequest = serde_json::from_value(args.clone())
        .map_err(|e| anyhow!("invalid store_memory arguments: {e}"))?;
    validate_create_memory(&body)?;
    let memory = create_memory_core(state, body).await?;
    Ok(json!({
        "success": true,
        "memory_id": memory.id,
        "type": memory.memory_type.to_string(),
        "summary": format!("Stored memory {} ({})", memory.id, memory.memory_type)
    }))
}

async fn search_memories(state: &AppState, args: &Value) -> anyhow::Result<Value> {
    let query_text = args["query"].as_str().ok_or_else(|| anyhow!("query is required"))?;
    let search_type = match args.get("search_type").and_then(|v| v.as_str()).unwrap_or("hybrid") {
        "semantic" => SearchType::Semantic,
        "keyword" => SearchType::Keyword,
        "hybrid" => SearchType::Hybrid,
        other => return Err(anyhow!("unknown search_type: {other}; use semantic, keyword, or hybrid")),
    };

    let f = args.get("filters");
    let filters = MemoryFilters {
        memory_type: f
            .and_then(|f| f.get("memory_type"))
            .and_then(|v| v.as_str())
            .map(MemoryType::try_from)
            .transpose()
            .map_err(|e| anyhow!(e))?,
        tags: f
            .and_then(|f| f.get("tags"))
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|t| t.as_str().map(String::from)).collect())
            .unwrap_or_default(),
        min_importance: f.and_then(|f| f.get("importance_min")).and_then(|v| v.as_f64()).map(|v| v as f32),
        max_importance: f.and_then(|f| f.get("importance_max")).and_then(|v| v.as_f64()).map(|v| v as f32),
        created_after: None,
        created_before: None,
    };

    let related_to = args
        .get("related_to")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Uuid>().ok());

    let query = SearchQuery {
        query: query_text.to_string(),
        search_type,
        filters,
        limit: args.get("limit").and_then(|v| v.as_u64()).unwrap_or(10).min(100) as usize,
        related_to,
    };

    let results = state.services.search.search(&query).await?;
    let count = results.len();
    Ok(json!({
        "success": true,
        "query": query_text,
        "results_count": count,
        "results": results,
        "summary": if count > 0 {
            format!("Found {count} memories for '{query_text}'")
        } else {
            format!("No memories found for '{query_text}'")
        }
    }))
}

async fn get_memory(state: &AppState, args: &Value) -> anyhow::Result<Value> {
    let id = parse_id(args)?;
    let include_conn = args.get("include_connections").and_then(|v| v.as_bool()).unwrap_or(false);
    let mut memory = state.services.memory.get(id).await?;
    if include_conn {
        memory.connections = state.services.memory.fetch_connections(id).await?;
    }
    Ok(json!({"success": true, "memory": memory}))
}

async fn update_memory(state: &AppState, args: &Value) -> anyhow::Result<Value> {
    let id = parse_id(args)?;
    let body: UpdateMemoryRequest = serde_json::from_value(args.clone())
        .map_err(|e| anyhow!("invalid update_memory arguments: {e}"))?;

    if let Some(content) = &body.content {
        if content.len() > MAX_CONTENT_BYTES {
            return Err(anyhow!("content exceeds maximum length of {MAX_CONTENT_BYTES} bytes"));
        }
    }
    if let Some(tags) = &body.tags {
        if tags.len() > MAX_TAGS {
            return Err(anyhow!("too many tags: maximum is {MAX_TAGS}, got {}", tags.len()));
        }
    }

    let updated_fields: Vec<&'static str> = [
        ("content", body.content.is_some()),
        ("tags", body.tags.is_some()),
        ("importance", body.importance.is_some()),
        ("emotional_valence", body.emotional_valence.is_some()),
        ("arousal", body.arousal.is_some()),
        ("health", body.health.is_some()),
        ("source", body.source.is_some()),
    ]
    .into_iter()
    .filter_map(|(name, present)| present.then_some(name))
    .collect();

    if updated_fields.is_empty() {
        return Err(anyhow!("at least one field to update is required"));
    }

    let patch = UpdatePatch {
        content: body.content,
        tags: body.tags,
        importance: body.importance,
        emotional_valence: body.emotional_valence,
        arousal: body.arousal,
        health: body.health,
        source: body.source,
    };
    state.services.memory.update(id, patch).await?;
    Ok(json!({"success": true, "memory_id": id, "updated_fields": updated_fields}))
}

async fn delete_memory(state: &AppState, args: &Value) -> anyhow::Result<Value> {
    let id = parse_id(args)?;
    let hard = args.get("hard_delete").and_then(|v| v.as_bool()).unwrap_or(false);
    state.services.memory.delete(id, hard).await?;
    let action = if hard { "permanently deleted" } else { "archived" };
    Ok(json!({"success": true, "memory_id": id, "summary": format!("Memory {id} {action}")}))
}

async fn find_related(state: &AppState, args: &Value) -> anyhow::Result<Value> {
    let id = parse_id(args)?;
    let depth = (args.get("depth").and_then(|v| v.as_u64()).unwrap_or(1) as usize).min(5);
    let limit = (args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20) as usize).min(100);

    let types: Vec<RelationshipType> = Vec::new(); // no filter — matches existing MCP tool behavior
    let pairs = state.services.connection.find_related(id, depth, &types).await?;
    let related: Vec<Value> = pairs
        .into_iter()
        .take(limit)
        .map(|(memory, connection)| json!({"memory": memory, "connection": connection}))
        .collect();
    let count = related.len();
    Ok(json!({
        "success": true,
        "source_memory_id": id,
        "results_count": count,
        "related_memories": related
    }))
}

async fn promote_to_longterm(state: &AppState, args: &Value) -> anyhow::Result<Value> {
    let id = parse_id(args)?;
    state.services.lifecycle.promote(id).await?;
    Ok(json!({"success": true, "memory_id": id, "summary": format!("Memory {id} promoted to long_term")}))
}

async fn list_recent_memories(state: &AppState, args: &Value) -> anyhow::Result<Value> {
    let limit = (args.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize).min(100);
    let memory_type = args
        .get("memory_type")
        .and_then(|v| v.as_str())
        .map(MemoryType::try_from)
        .transpose()
        .map_err(|e| anyhow!(e))?;
    let sort_by = args
        .get("sort_by")
        .and_then(|v| v.as_str())
        .map(SortBy::try_from)
        .transpose()
        .map_err(|e| anyhow!(e))?
        .unwrap_or_default();

    let filters = MemoryFilters { memory_type, ..Default::default() };
    let (memories, total) = state.services.memory.list(&filters, sort_by, limit, 0).await?;
    let count = memories.len();
    Ok(json!({"success": true, "count": count, "total": total, "memories": memories}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tools_list_contains_all_eight_tools() {
        let list = list();
        let tools = list["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
        let expected = [
            "store_memory", "search_memories", "get_memory", "update_memory",
            "delete_memory", "find_related", "promote_to_longterm", "list_recent_memories",
        ];
        for name in expected {
            assert!(names.contains(&name), "missing tool: {name}");
        }
        assert_eq!(tools.len(), expected.len());
    }

    #[test]
    fn update_memory_schema_includes_emotional_fields() {
        let list = list();
        let tools = list["tools"].as_array().unwrap();
        let update = tools.iter().find(|t| t["name"] == "update_memory").unwrap();
        let props = &update["inputSchema"]["properties"];
        assert!(props.get("emotional_valence").is_some());
        assert!(props.get("arousal").is_some());
        assert!(props.get("health").is_some());
    }

    #[test]
    fn list_recent_memories_schema_advertises_sort_by() {
        let list = list();
        let tools = list["tools"].as_array().unwrap();
        let lrm = tools.iter().find(|t| t["name"] == "list_recent_memories").unwrap();
        let sort_by_enum = lrm["inputSchema"]["properties"]["sort_by"]["enum"].as_array().unwrap();
        assert!(sort_by_enum.contains(&json!("created_at")));
        assert!(sort_by_enum.contains(&json!("accessed_at")));
    }

    #[test]
    fn parse_id_rejects_non_uuid() {
        assert!(parse_id(&json!({"memory_id": "not-a-uuid"})).is_err());
    }

    #[test]
    fn parse_id_requires_memory_id_field() {
        assert!(parse_id(&json!({})).is_err());
    }
}
