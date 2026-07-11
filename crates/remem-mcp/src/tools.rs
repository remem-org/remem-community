use anyhow::anyhow;
use serde_json::{json, Value};

use crate::client::RememClient;

/// Return the MCP `tools/list` result value.
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
                "description": "Update an existing memory's content, tags, or importance.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "memory_id": {"type": "string"},
                        "content": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "importance": {"type": "number", "minimum": 0.0, "maximum": 1.0},
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

/// Dispatch a `tools/call` request.
pub async fn call(params: &Value, client: &RememClient, request_id: &str) -> anyhow::Result<Value> {
    let name = params["name"].as_str().ok_or_else(|| anyhow!("missing tool name"))?;
    let args = &params["arguments"];

    let result = match name {
        "store_memory" => store_memory(client, args, request_id).await,
        "search_memories" => search_memories(client, args, request_id).await,
        "get_memory" => get_memory(client, args, request_id).await,
        "update_memory" => update_memory(client, args, request_id).await,
        "delete_memory" => delete_memory(client, args, request_id).await,
        "find_related" => find_related(client, args, request_id).await,
        "promote_to_longterm" => promote_to_longterm(client, args, request_id).await,
        "list_recent_memories" => list_recent_memories(client, args, request_id).await,
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

async fn store_memory(client: &RememClient, args: &Value, request_id: &str) -> anyhow::Result<Value> {
    let content = args["content"].as_str().ok_or_else(|| anyhow!("content is required"))?;

    let mut body = json!({
        "content": content,
        "memory_type": args.get("memory_type").and_then(|v| v.as_str()).unwrap_or("short_term"),
        "tags": args.get("tags").cloned().unwrap_or(json!([])),
        "importance": args.get("importance").and_then(|v| v.as_f64()).unwrap_or(0.5),
    });
    for field in ["emotional_valence", "arousal", "health", "ttl", "source", "graph_extraction"] {
        if let Some(value) = args.get(field) {
            body[field] = value.clone();
        }
    }

    let data = client.store_memory(body, request_id).await?;
    Ok(json!({
        "success": true,
        "memory_id": data["id"],
        "type": data["memory_type"],
        "summary": format!("Stored memory {} ({})", data["id"], data["memory_type"])
    }))
}

async fn search_memories(client: &RememClient, args: &Value, request_id: &str) -> anyhow::Result<Value> {
    let query = args["query"].as_str().ok_or_else(|| anyhow!("query is required"))?;

    let mut body = json!({
        "query": query,
        "search_type": args.get("search_type").and_then(|v| v.as_str()).unwrap_or("hybrid"),
        "limit": args.get("limit").and_then(|v| v.as_i64()).unwrap_or(10),
    });
    if let Some(f) = args.get("filters") {
        body["filters"] = f.clone();
    }
    if let Some(id) = args.get("related_to").and_then(|v| v.as_str()) {
        body["related_to"] = json!(id);
    }

    let data = client.search_memories(body, request_id).await?;
    let count = data["results"].as_array().map(|a| a.len()).unwrap_or(0);
    Ok(json!({
        "success": true,
        "query": query,
        "results_count": count,
        "results": data["results"],
        "summary": if count > 0 {
            format!("Found {count} memories for '{query}'")
        } else {
            format!("No memories found for '{query}'")
        }
    }))
}

async fn get_memory(client: &RememClient, args: &Value, request_id: &str) -> anyhow::Result<Value> {
    let id = args["memory_id"].as_str().ok_or_else(|| anyhow!("memory_id is required"))?;
    let include_conn = args.get("include_connections").and_then(|v| v.as_bool()).unwrap_or(false);
    let data = client.get_memory(id, include_conn, request_id).await?;
    Ok(json!({"success": true, "memory": data}))
}

async fn update_memory(client: &RememClient, args: &Value, request_id: &str) -> anyhow::Result<Value> {
    let id = args["memory_id"].as_str().ok_or_else(|| anyhow!("memory_id is required"))?;

    let mut body = serde_json::Map::new();
    for field in ["content", "tags", "importance", "source"] {
        if let Some(v) = args.get(field) {
            body.insert(field.to_string(), v.clone());
        }
    }
    if body.is_empty() {
        return Err(anyhow!("at least one field to update is required"));
    }

    client.update_memory(id, Value::Object(body.clone()), request_id).await?;
    let updated: Vec<&String> = body.keys().collect();
    Ok(json!({"success": true, "memory_id": id, "updated_fields": updated}))
}

async fn delete_memory(client: &RememClient, args: &Value, request_id: &str) -> anyhow::Result<Value> {
    let id = args["memory_id"].as_str().ok_or_else(|| anyhow!("memory_id is required"))?;
    let hard = args.get("hard_delete").and_then(|v| v.as_bool()).unwrap_or(false);
    client.delete_memory(id, hard, request_id).await?;
    let action = if hard { "permanently deleted" } else { "archived" };
    Ok(json!({"success": true, "memory_id": id, "summary": format!("Memory {id} {action}")}))
}

async fn find_related(client: &RememClient, args: &Value, request_id: &str) -> anyhow::Result<Value> {
    let id = args["memory_id"].as_str().ok_or_else(|| anyhow!("memory_id is required"))?;
    let depth = args.get("depth").and_then(|v| v.as_i64()).unwrap_or(1);
    let limit = args.get("limit").and_then(|v| v.as_i64()).unwrap_or(10);
    let data = client.find_related(id, depth, limit, request_id).await?;
    let count = data["related"].as_array().map(|a| a.len()).unwrap_or(0);
    Ok(json!({"success": true, "source_memory_id": id, "results_count": count, "related_memories": data["related"]}))
}

async fn promote_to_longterm(client: &RememClient, args: &Value, request_id: &str) -> anyhow::Result<Value> {
    let id = args["memory_id"].as_str().ok_or_else(|| anyhow!("memory_id is required"))?;
    client.promote_to_longterm(id, request_id).await?;
    Ok(json!({"success": true, "memory_id": id, "summary": format!("Memory {id} promoted to long_term")}))
}

async fn list_recent_memories(client: &RememClient, args: &Value, request_id: &str) -> anyhow::Result<Value> {
    let limit = args.get("limit").and_then(|v| v.as_i64()).unwrap_or(10);
    let memory_type = args.get("memory_type").and_then(|v| v.as_str());
    let data = client.list_memories(limit, 0, memory_type, request_id).await?;
    let count = data["memories"].as_array().map(|a| a.len()).unwrap_or(0);
    Ok(json!({"success": true, "count": count, "total": data["total"], "memories": data["memories"]}))
}
