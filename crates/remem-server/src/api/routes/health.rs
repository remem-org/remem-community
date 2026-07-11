use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;

use crate::api::AppState;
use crate::error::{ErrorResponse, Result};
use crate::services::types::MemoryType;

#[derive(Serialize, utoipa::ToSchema)]
pub struct DeepHealthCheck {
    pub ok: bool,
    pub detail: Option<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct DeepHealthResponse {
    pub status: &'static str,
    pub storage_read: DeepHealthCheck,
    pub vector_search: DeepHealthCheck,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct HealthResponse {
    status: &'static str,
    message: &'static str,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct ReadyResponse {
    status: &'static str,
    storage: StorageReadiness,
    embedding: bool,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct StorageReadiness {
    ok: bool,
    vector_count: usize,
    vector_enabled: bool,
    graph_node_count: usize,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct StatsResponse {
    success: bool,
    stats: Stats,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct Stats {
    total_memories: usize,
    short_term_memories: usize,
    long_term_memories: usize,
    total_connections: usize,
    avg_importance: f32,
}

#[utoipa::path(
    get,
    path = "/api/v1/health",
    responses(
        (status = 200, description = "Server is healthy", body = HealthResponse),
    ),
    tag = "system"
)]
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy",
        message: "remem-server is running",
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/ready",
    responses(
        (status = 200, description = "All dependencies ready", body = ReadyResponse),
        (status = 503, description = "One or more dependencies not ready", body = ReadyResponse),
    ),
    tag = "system"
)]
pub async fn ready(
    State(state): State<AppState>,
) -> (StatusCode, Json<ReadyResponse>) {
    let storage_stats = state.services.engine.stats();
    let storage = StorageReadiness {
        ok: true,
        vector_count: storage_stats.vector_count,
        vector_enabled: storage_stats.vector_enabled,
        graph_node_count: storage_stats.graph_node_count,
    };
    (
        StatusCode::OK,
        Json(ReadyResponse {
            status: "ready",
            storage,
            embedding: true,
        }),
    )
}

#[utoipa::path(
    get,
    path = "/api/v1/stats",
    responses(
        (status = 200, description = "Memory and connection counts", body = StatsResponse),
        (status = 500, description = "Storage error", body = ErrorResponse),
    ),
    tag = "system"
)]
pub async fn stats(State(state): State<AppState>) -> Result<Json<StatsResponse>> {
    // Walk all BTree entries and verify each against KV. time_range_query with no
    // limit returns every entry regardless of count, avoiding the truncation bug
    // that time_latest(btree_count + 64) introduced when btree_count lagged the
    // actual number of entries (e.g. older short-term memories were silently dropped).
    let entries = state.services.engine.time_range_query(0, u64::MAX, None)?;

    let mut total = 0usize;
    let mut long_term = 0usize;
    let mut importance_sum = 0.0f32;
    let mut active_entry_keys: Vec<Vec<u8>> = Vec::new();

    for (_ts, key_bytes) in &entries {
        let Some(stored) = state.services.repo.load_by_key(key_bytes.as_ref()).await? else {
            continue;
        };
        if stored.archived {
            continue;
        }
        total += 1;
        if stored.memory_type == MemoryType::LongTerm {
            long_term += 1;
        }
        importance_sum += stored.metadata.importance;
        active_entry_keys.push(key_bytes.to_vec());
    }

    let short_term = total - long_term;
    let avg_importance = if total > 0 { importance_sum / total as f32 } else { 0.0 };

    // Count edges where both source and target are non-archived in KV.
    // Verify targets via KV (same as connection_manager::list_all) rather than
    // checking the BTree set — the two indexes can diverge after WAL replay
    // resurrects KV entries whose BTree timestamps were durably removed.
    let mut total_connections = 0usize;
    for key_bytes in &active_entry_keys {
        let neighbors = state.services.engine.get_neighbors(key_bytes)?;
        for (target, _, _) in neighbors {
            let Ok(tgt) = state.services.repo.load_by_key(target.as_ref()).await else {
                continue;
            };
            if let Some(tgt_stored) = tgt {
                if !tgt_stored.archived {
                    total_connections += 1;
                }
            }
        }
    }

    Ok(Json(StatsResponse {
        success: true,
        stats: Stats {
            total_memories: total,
            short_term_memories: short_term,
            long_term_memories: long_term,
            total_connections,
            avg_importance,
        },
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/health/deep",
    responses(
        (status = 200, description = "All deep checks passed", body = DeepHealthResponse),
        (status = 503, description = "One or more deep checks failed", body = DeepHealthResponse),
    ),
    tag = "system"
)]
pub async fn deep_health(State(state): State<AppState>) -> (StatusCode, Json<DeepHealthResponse>) {
    // Check 1: KV layer is readable
    let storage_read = match state.services.engine.get(b"__deep_health_check__" as &[u8]).await {
        Ok(_) => DeepHealthCheck { ok: true, detail: None },
        Err(e) => DeepHealthCheck { ok: false, detail: Some(e.to_string()) },
    };

    // Check 2: vector index is queryable (only if vectors exist)
    let vector_search = if state.services.engine.stats().vector_count == 0 {
        DeepHealthCheck { ok: true, detail: None }
    } else {
        let zero_vec = vec![0.0f32; 384];
        match state.services.engine.vector_search(&zero_vec, 1).await {
            Ok(_) => DeepHealthCheck { ok: true, detail: None },
            Err(e) => DeepHealthCheck { ok: false, detail: Some(e.to_string()) },
        }
    };

    let all_ok = storage_read.ok && vector_search.ok;
    let http_status = if all_ok { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };

    (
        http_status,
        Json(DeepHealthResponse {
            status: if all_ok { "healthy" } else { "degraded" },
            storage_read,
            vector_search,
        }),
    )
}
