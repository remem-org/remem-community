//! API integration tests.
//!
//! These tests spin up the full Axum router with a real storage engine and
//! embedding service.  They are marked `#[ignore]` because they require the
//! fastembed ONNX model to be present on disk (either downloaded at build time
//! or pre-baked into the Docker image via `FASTEMBED_CACHE_PATH`).
//!
//! Run with:
//!   cargo test --test-threads=1 -- --ignored
//!
//! or individually:
//!   cargo test api_integration::health -- --ignored

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt as _;
use serde_json::Value;
use tower::ServiceExt as _;

use crate::{
    api::{build_router, AppState},
    config::{
        Config, ConnectionConfig, EmbeddingConfig, Environment, SearchConfig, ServerConfig,
        StorageConfig as CfgStorage, TaskConfig, VectorConfig as CfgVector,
    },
    engine::storage::{
        engine::{
            EngineConfig, GraphIndexConfig, TagIndexConfig, TimeSeriesConfig,
            VectorConfig as EngineVector,
        },
        partition::PartitionId,
    },
    engine::{util::DistanceMetric, StorageEngine},
    services::{create_services, types::memory_key},
};

// ── Test helpers ──────────────────────────────────────────────────────────────

async fn make_test_app() -> (axum::Router, AppState, tempfile::TempDir) {
    make_test_app_with_default_partition(PartitionId::default_legacy()).await
}

async fn make_test_app_with_default_partition(
    default_partition: PartitionId,
) -> (axum::Router, AppState, tempfile::TempDir) {
    let tmpdir = tempfile::tempdir().expect("tempdir");

    let engine_cfg = EngineConfig {
        data_dir: tmpdir.path().to_path_buf(),
        sync_writes: false,
        default_partition: default_partition.clone(),
        vector: EngineVector {
            enabled: true,
            dimension: 384,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 50,
            metric: DistanceMetric::L2,
            hnsw_resident_budget_bytes: None,
        },
        graph: GraphIndexConfig {
            enabled: true,
            directed: true,
        },
        time_series: TimeSeriesConfig { enabled: true },
        tag_index: TagIndexConfig {
            enabled: true,
            lowercase: true,
            min_token_length: 1,
        },
        // Mirrors `main.rs`. Without these the engine has no attribute store,
        // and every path that reaches it fails: `MemoryManager::list` walks an
        // ordered attribute index (REM-96) and errors when no slot is indexed,
        // and filtered search cannot settle its predicates (REM-78). A harness
        // that omits them is not exercising the server these tests describe.
        attr_schema: Some(crate::services::attrs::memory_schema()),
        attr_project: Some(std::sync::Arc::new(|bytes: &[u8]| {
            serde_json::from_slice::<crate::services::types::StoredMemory>(bytes)
                .ok()
                .map(|stored| crate::services::attrs::project(&stored))
        })),
        ..EngineConfig::default()
    };

    let engine = Arc::new(
        StorageEngine::new(engine_cfg)
            .await
            .expect("storage engine"),
    );

    let cfg = Config {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port: 4545,
            api_key: String::new(), // auth disabled
            api_key_secondary: String::new(),
            allow_auth_disabled: true,
            allowed_origins: vec![],
            rate_limit_rps: 0,
            rate_limit_burst: 50,
            trust_proxy_headers: false,
            env: Environment::Development,
        },
        storage: CfgStorage {
            data_dir: tmpdir.path().to_path_buf(),
            sync_writes: false,
            checkpoint_interval_secs: 300,
            max_wal_size_mb: 256,
            text_field: "content".to_string(),
            default_partition,
        },
        vector: CfgVector {
            hnsw_resident_budget_mb: None,
            dimension: 384,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 50,
        },
        embedding: EmbeddingConfig { cache_size: 100 },
        connections: ConnectionConfig {
            auto_discovery_threshold: 0.7,
            auto_discovery_top_k: 5,
        },
        search: SearchConfig {
            widen_max_factor: 32,
            list_max_factor: 128,
        },
        tasks: TaskConfig {
            expire_short_term_secs: 300,
            apply_importance_decay_secs: 86400,
            active_forgetting_secs: 86400,
            consolidate_similar_secs: 604800,
            cleanup_archived_secs: 2592000,
            discover_connections_secs: 3600,
            flush_recall_secs: 30,
            discovery_workers: 2,
            discovery_queue_size: 10_000,
            active_forgetting_hard_delete: false,
        },
    };

    let services = create_services(Arc::clone(&engine), &cfg)
        .await
        .expect("services (requires embedding model — run with FASTEMBED_CACHE_PATH set)");

    let state = AppState {
        services,
        config: Arc::new(cfg),
    };
    (build_router(state.clone()), state, tmpdir)
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn post_json(uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

fn delete(uri: &str) -> Request<Body> {
    Request::builder()
        .method("DELETE")
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

// ── Health (no auth) ──────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn health_returns_healthy() {
    let (app, _state, _dir) = make_test_app().await;
    let resp = app.oneshot(get("/api/v1/health")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["status"], "healthy");
}

// ── Stats ─────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn stats_returns_counts() {
    let (app, _state, _dir) = make_test_app().await;
    let resp = app.oneshot(get("/api/v1/stats")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert!(json["stats"]["total_memories"].is_number());
    assert!(json["stats"]["total_connections"].is_number());
}

// ── Memories CRUD ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn create_memory_returns_memory_object() {
    let (app, _state, _dir) = make_test_app().await;

    let resp = app
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({
                "content": "Rust is a systems programming language",
                "memory_type": "short_term",
                "tags": ["rust", "programming"],
                "importance": 0.8
            }),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let json = body_json(resp).await;
    assert!(json["id"].is_string());
    assert_eq!(json["content"], "Rust is a systems programming language");
    assert_eq!(json["memory_type"], "short_term");

    // Validate UUID format
    let id = json["id"].as_str().unwrap();
    uuid::Uuid::parse_str(id).expect("id must be a valid UUID");
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn rest_create_memory_writes_to_configured_default_partition() {
    let (app, state, _dir) =
        make_test_app_with_default_partition(PartitionId::new("finance").unwrap()).await;

    let resp = app
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({"content": "finance default partition memory"}),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let json = body_json(resp).await;
    let id = uuid::Uuid::parse_str(json["id"].as_str().unwrap()).unwrap();
    let physical_key = state.services.repo.physical_memory_key(id).unwrap();

    assert!(state
        .services
        .engine
        .get(physical_key.as_ref())
        .await
        .unwrap()
        .is_some());
    assert!(state
        .services
        .engine
        .get(memory_key(id).as_bytes())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn create_long_term_memory() {
    let (app, _state, _dir) = make_test_app().await;

    let resp = app
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({
                "content": "Important long-term fact",
                "memory_type": "long_term",
                "importance": 0.9
            }),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let json = body_json(resp).await;
    assert_eq!(json["memory_type"], "long_term");
    // Long-term memories have no TTL
    assert!(json["metadata"]["ttl"].is_null());
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn get_memory_by_id() {
    let (app, _state, _dir) = make_test_app().await;

    // Create
    let create_resp = app
        .clone()
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({"content": "Memory to retrieve"}),
        ))
        .await
        .unwrap();
    let created = body_json(create_resp).await;
    let id = created["id"].as_str().unwrap().to_owned();

    // Get
    let resp = app
        .oneshot(get(&format!("/api/v1/memories/{id}")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["id"], id);
    assert_eq!(json["content"], "Memory to retrieve");
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn get_nonexistent_memory_returns_404() {
    let (app, _state, _dir) = make_test_app().await;
    let fake = "00000000-0000-0000-0000-000000000000";
    let resp = app
        .oneshot(get(&format!("/api/v1/memories/{fake}")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn update_memory_content_and_importance() {
    let (app, _state, _dir) = make_test_app().await;

    // Create
    let create_resp = app
        .clone()
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({"content": "Original content"}),
        ))
        .await
        .unwrap();
    let id = body_json(create_resp).await["id"]
        .as_str()
        .unwrap()
        .to_owned();

    // Update
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/v1/memories/{id}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "content": "Updated content",
                        "importance": 0.95
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["content"], "Updated content");
    assert!((json["metadata"]["importance"].as_f64().unwrap() - 0.95).abs() < 0.01);
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn soft_delete_memory() {
    let (app, _state, _dir) = make_test_app().await;

    // Create
    let create_resp = app
        .clone()
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({"content": "Memory to delete"}),
        ))
        .await
        .unwrap();
    let id = body_json(create_resp).await["id"]
        .as_str()
        .unwrap()
        .to_owned();

    // Soft delete
    let del_resp = app
        .clone()
        .oneshot(delete(&format!("/api/v1/memories/{id}")))
        .await
        .unwrap();
    assert_eq!(del_resp.status(), StatusCode::OK);

    // Verify deleted — should now 404
    let get_resp = app
        .oneshot(get(&format!("/api/v1/memories/{id}")))
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn list_memories_returns_created_memories() {
    let (app, _state, _dir) = make_test_app().await;

    // Create 3 memories
    for i in 0..3 {
        app.clone()
            .oneshot(post_json(
                "/api/v1/memories",
                serde_json::json!({"content": format!("List test memory {i}")}),
            ))
            .await
            .unwrap();
    }

    let resp = app
        .oneshot(get("/api/v1/memories?limit=10&offset=0"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    let memories = json["memories"].as_array().unwrap();
    assert!(memories.len() >= 3);
    assert!(
        json.get("total").is_none(),
        "the listing reports the page it served, not a count of everything that matches"
    );
    assert_eq!(json["limit"], 10);
    assert_eq!(json["offset"], 0);
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn list_memories_rejects_sort_by_accessed_at() {
    // Ordering by recency of retrieval is gone: reading a memory changed
    // where it sat in the list, so paging could return the same memory twice
    // and skip another. A caller asking for it is refused rather than served
    // a different order silently.
    let (app, _state, _dir) = make_test_app().await;
    let resp = app
        .oneshot(get("/api/v1/memories?limit=10&sort_by=accessed_at"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn list_memories_are_ordered_by_creation_time() {
    let (app, _state, _dir) = make_test_app().await;

    let mut created = Vec::new();
    for content in ["first memory", "second memory", "third memory"] {
        let resp = app
            .clone()
            .oneshot(post_json(
                "/api/v1/memories",
                serde_json::json!({ "content": content }),
            ))
            .await
            .unwrap();
        created.push(body_json(resp).await["id"].as_str().unwrap().to_owned());
    }

    let resp = app
        .oneshot(get("/api/v1/memories?limit=10&sort_by=created_at"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    let memories = json["memories"].as_array().unwrap();

    let returned: Vec<&str> = memories.iter().filter_map(|m| m["id"].as_str()).collect();
    let positions: Vec<usize> = created
        .iter()
        .map(|id| {
            returned
                .iter()
                .position(|r| r == id)
                .unwrap_or_else(|| panic!("memory {id} not found in response"))
        })
        .collect();
    let mut ascending = positions.clone();
    ascending.sort_unstable();
    assert_eq!(
        positions, ascending,
        "memories must come back in the order they were created"
    );
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn list_memories_rejects_unknown_sort_by() {
    let (app, _state, _dir) = make_test_app().await;
    let resp = app
        .oneshot(get("/api/v1/memories?sort_by=popularity"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

// ── In-process MCP tools ────────────────────────────────────────────────────

fn default_partition(state: &AppState) -> crate::api::partition::EffectivePartition {
    crate::api::partition::EffectivePartition::legacy_default(state)
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn mcp_store_memory_tool_creates_a_memory() {
    let (_app, state, _dir) = make_test_app().await;
    let partition = default_partition(&state);
    let result = crate::api::mcp::tools::call(
        &serde_json::json!({
            "name": "store_memory",
            "arguments": {"content": "MCP-created memory"}
        }),
        &state,
        &partition,
    )
    .await
    .unwrap();
    let text = result["content"][0]["text"].as_str().unwrap();
    let data: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(data["success"], true);
    assert!(data["memory_id"].is_string());
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn mcp_store_memory_tool_writes_to_configured_default_partition() {
    let (_app, state, _dir) =
        make_test_app_with_default_partition(PartitionId::new("finance").unwrap()).await;
    let partition = default_partition(&state);

    let result = crate::api::mcp::tools::call(
        &serde_json::json!({
            "name": "store_memory",
            "arguments": {"content": "MCP finance default partition memory"}
        }),
        &state,
        &partition,
    )
    .await
    .unwrap();

    let text = result["content"][0]["text"].as_str().unwrap();
    let data: serde_json::Value = serde_json::from_str(text).unwrap();
    let id = uuid::Uuid::parse_str(data["memory_id"].as_str().unwrap()).unwrap();
    let physical_key = state.services.repo.physical_memory_key(id).unwrap();

    assert!(state
        .services
        .engine
        .get(physical_key.as_ref())
        .await
        .unwrap()
        .is_some());
    assert!(state
        .services
        .engine
        .get(memory_key(id).as_bytes())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn mcp_update_memory_tool_accepts_emotional_fields() {
    let (_app, state, _dir) = make_test_app().await;
    let partition = default_partition(&state);
    let store_result = crate::api::mcp::tools::call(
        &serde_json::json!({"name": "store_memory", "arguments": {"content": "to update"}}),
        &state,
        &partition,
    )
    .await
    .unwrap();
    let store_text = store_result["content"][0]["text"].as_str().unwrap();
    let store_data: serde_json::Value = serde_json::from_str(store_text).unwrap();
    let id = store_data["memory_id"].as_str().unwrap();

    let update_result = crate::api::mcp::tools::call(
        &serde_json::json!({
            "name": "update_memory",
            "arguments": {"memory_id": id, "emotional_valence": 0.5, "arousal": 0.9, "health": 80.0}
        }),
        &state,
        &partition,
    )
    .await
    .unwrap();
    let update_text = update_result["content"][0]["text"].as_str().unwrap();
    let update_data: serde_json::Value = serde_json::from_str(update_text).unwrap();
    assert_eq!(update_data["success"], true);
    let updated_fields = update_data["updated_fields"].as_array().unwrap();
    let names: Vec<&str> = updated_fields.iter().filter_map(|v| v.as_str()).collect();
    assert!(names.contains(&"emotional_valence"));
    assert!(names.contains(&"arousal"));
    assert!(names.contains(&"health"));
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn mcp_list_recent_memories_tool_rejects_ordering_by_access_time() {
    let (_app, state, _dir) = make_test_app().await;
    let partition = default_partition(&state);
    crate::api::mcp::tools::call(
        &serde_json::json!({"name": "store_memory", "arguments": {"content": "one"}}),
        &state,
        &partition,
    )
    .await
    .unwrap();

    // Ordering by recency of retrieval is gone: reading a memory moved it in
    // the list, so paging could return it twice and skip another. A caller
    // asking for it is refused rather than served creation order silently.
    let refused = crate::api::mcp::tools::call(
        &serde_json::json!({"name": "list_recent_memories", "arguments": {"limit": 5, "sort_by": "accessed_at"}}),
        &state,
        &partition,
    )
    .await
    .unwrap();
    let refused: serde_json::Value =
        serde_json::from_str(refused["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(
        refused["success"], false,
        "accessed_at must be refused, not silently served in creation order"
    );
    assert!(
        refused["message"].as_str().unwrap().contains("accessed_at"),
        "the refusal must name what was asked for: {refused}"
    );

    let result = crate::api::mcp::tools::call(
        &serde_json::json!({"name": "list_recent_memories", "arguments": {"limit": 5, "sort_by": "created_at"}}),
        &state,
        &partition,
    )
    .await
    .unwrap();
    let text = result["content"][0]["text"].as_str().unwrap();
    let data: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(data["success"], true);
    assert!(!data["memories"].as_array().unwrap().is_empty());
    assert!(
        data.get("total").is_none(),
        "the tool reports the page it has, not a count of everything that matches"
    );
}

// ── In-process MCP resources ────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn mcp_resource_stats_returns_json() {
    let (_app, state, _dir) = make_test_app().await;
    let result =
        crate::api::mcp::resources::read(&serde_json::json!({"uri": "memory://stats"}), &state)
            .await
            .unwrap();
    let text = result["contents"][0]["text"].as_str().unwrap();
    let data: serde_json::Value = serde_json::from_str(text).unwrap();
    assert!(data["total_memories"].is_number());
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn mcp_resource_unknown_path_errors() {
    let (_app, state, _dir) = make_test_app().await;
    let result =
        crate::api::mcp::resources::read(&serde_json::json!({"uri": "memory://nonsense"}), &state)
            .await;
    assert!(result.is_err());
}

// ── MCP transport (Streamable HTTP, mounted at /mcp) ────────────────────────────

fn mcp_post(body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn mcp_endpoint_requires_auth_when_api_key_set() {
    let tmpdir = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        StorageEngine::new(EngineConfig {
            data_dir: tmpdir.path().to_path_buf(),
            sync_writes: false,
            ..EngineConfig::default()
        })
        .await
        .unwrap(),
    );
    let cfg = Config {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port: 4545,
            api_key: "test-secret".into(),
            api_key_secondary: String::new(),
            allow_auth_disabled: false,
            allowed_origins: vec![],
            rate_limit_rps: 0,
            rate_limit_burst: 50,
            trust_proxy_headers: false,
            env: Environment::Development,
        },
        storage: CfgStorage {
            data_dir: tmpdir.path().to_path_buf(),
            sync_writes: false,
            checkpoint_interval_secs: 300,
            max_wal_size_mb: 256,
            text_field: "content".to_string(),
            default_partition: PartitionId::default_legacy(),
        },
        vector: CfgVector {
            hnsw_resident_budget_mb: None,
            dimension: 384,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 50,
        },
        embedding: EmbeddingConfig { cache_size: 100 },
        connections: ConnectionConfig {
            auto_discovery_threshold: 0.7,
            auto_discovery_top_k: 5,
        },
        search: SearchConfig {
            widen_max_factor: 32,
            list_max_factor: 128,
        },
        tasks: TaskConfig {
            expire_short_term_secs: 300,
            apply_importance_decay_secs: 86400,
            active_forgetting_secs: 86400,
            consolidate_similar_secs: 604800,
            cleanup_archived_secs: 2592000,
            discover_connections_secs: 3600,
            flush_recall_secs: 30,
            discovery_workers: 2,
            discovery_queue_size: 10_000,
            active_forgetting_hard_delete: false,
        },
    };
    let services = create_services(Arc::clone(&engine), &cfg).await.unwrap();
    let app = build_router(AppState {
        services,
        config: Arc::new(cfg),
    });

    let resp = app
        .oneshot(mcp_post(serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}
        })))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn mcp_initialize_then_tools_call_round_trip() {
    let (app, _state, _dir) = make_test_app().await;

    let init_resp = app
        .clone()
        .oneshot(mcp_post(serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}
        })))
        .await
        .unwrap();
    assert_eq!(init_resp.status(), StatusCode::OK);
    let session_id = init_resp
        .headers()
        .get("Mcp-Session-Id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let init_json = body_json(init_resp).await;
    assert_eq!(init_json["result"]["protocolVersion"], "2025-03-26");

    let call_req = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .header("Mcp-Session-Id", &session_id)
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "store_memory", "arguments": {"content": "via /mcp"}}
            }))
            .unwrap(),
        ))
        .unwrap();
    let call_resp = app.clone().oneshot(call_req).await.unwrap();
    assert_eq!(call_resp.status(), StatusCode::OK);
    let call_json = body_json(call_resp).await;
    let text = call_json["result"]["content"][0]["text"].as_str().unwrap();
    let data: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(data["success"], true);

    // GET is not supported — 405.
    let get_resp = app.clone().oneshot(get("/mcp")).await.unwrap();
    assert_eq!(get_resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    // A call without a valid session id (post-initialize) is rejected.
    let no_session_req = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({
                "jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {}
            }))
            .unwrap(),
        ))
        .unwrap();
    let no_session_resp = app.clone().oneshot(no_session_req).await.unwrap();
    assert_eq!(no_session_resp.status(), StatusCode::BAD_REQUEST);

    // DELETE terminates the session.
    let del_req = Request::builder()
        .method("DELETE")
        .uri("/mcp")
        .header("Mcp-Session-Id", &session_id)
        .body(Body::empty())
        .unwrap();
    let del_resp = app.oneshot(del_req).await.unwrap();
    assert_eq!(del_resp.status(), StatusCode::NO_CONTENT);
}

// ── Search ────────────────────────────────────────────────────────────────────

/// `has_more` used to lie whenever a filter was selective (REM-78).
///
/// REST search asks the engine for `offset + limit + 1` ranked results and
/// reports `has_more` from whether that extra sentinel came back. A filter
/// rejecting candidates starved the sentinel, so a search with plenty of
/// further matches reported `has_more: false` — a short page labelled as the
/// last one, which a caller paginating correctly would stop at.
#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn filtered_search_reports_further_pages() {
    let (app, _state, _dir) = make_test_app().await;

    // Enough long-term matches to fill several pages, diluted by short-term
    // records so the filter has real work to do.
    for i in 0..40 {
        let long_term = i % 2 == 0;
        app.clone()
            .oneshot(post_json(
                "/api/v1/memories",
                serde_json::json!({
                    "content": format!("rust ownership and borrowing note {i}"),
                    "memory_type": if long_term { "long_term" } else { "short_term" },
                }),
            ))
            .await
            .unwrap();
    }

    let resp = app
        .oneshot(post_json(
            "/api/v1/memories/search",
            serde_json::json!({
                "query": "rust ownership borrowing",
                "search_type": "hybrid",
                "limit": 5,
                "memory_type": "long_term"
            }),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(
        json["results"].as_array().unwrap().len(),
        5,
        "20 long-term records match; a page of 5 must come back full"
    );
    assert_eq!(
        json["has_more"], true,
        "further matches exist, so pagination must not be told to stop"
    );
    assert_eq!(
        json["truncated"], false,
        "the page filled without reaching the effort bound"
    );
}

/// The three states of a search response are distinguishable, and the fourth
/// combination is never produced.
#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn search_distinguishes_exhausted_from_truncated() {
    let (app, _state, _dir) = make_test_app().await;

    for i in 0..6 {
        app.clone()
            .oneshot(post_json(
                "/api/v1/memories",
                serde_json::json!({
                    "content": format!("rust ownership note {i}"),
                    "memory_type": "long_term",
                }),
            ))
            .await
            .unwrap();
    }

    // Exhausted: fewer matches than asked for, but every one of them returned.
    let resp = app
        .clone()
        .oneshot(post_json(
            "/api/v1/memories/search",
            serde_json::json!({
                "query": "rust ownership",
                "search_type": "hybrid",
                "limit": 50,
                "memory_type": "long_term"
            }),
        ))
        .await
        .unwrap();
    let json = body_json(resp).await;
    assert_eq!(json["has_more"], false);
    assert_eq!(
        json["truncated"], false,
        "a short page is only honest if it says whether it is complete"
    );

    // A filter nothing satisfies is still an exhausted answer, not a
    // truncated one: the search looked and there was nothing there.
    let resp = app
        .oneshot(post_json(
            "/api/v1/memories/search",
            serde_json::json!({
                "query": "rust ownership",
                "search_type": "hybrid",
                "limit": 5,
                "memory_type": "short_term"
            }),
        ))
        .await
        .unwrap();
    let json = body_json(resp).await;
    assert_eq!(json["results"].as_array().unwrap().len(), 0);
    assert_eq!(json["has_more"], false);
    assert!(
        json["truncated"].is_boolean(),
        "the field is always present so a caller never has to infer it"
    );
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn semantic_search_returns_results() {
    let (app, _state, _dir) = make_test_app().await;

    // Store a memory
    app.clone()
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({
                "content": "Rust is a memory-safe systems programming language"
            }),
        ))
        .await
        .unwrap();

    // Search
    let resp = app
        .oneshot(post_json(
            "/api/v1/memories/search",
            serde_json::json!({
                "query": "programming language memory safety",
                "search_type": "semantic",
                "limit": 5
            }),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert!(json["results"].is_array());
    assert!(json["total"].is_number());
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn keyword_search_returns_results() {
    let (app, _state, _dir) = make_test_app().await;

    app.clone()
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({"content": "Machine learning with neural networks"}),
        ))
        .await
        .unwrap();

    let resp = app
        .oneshot(post_json(
            "/api/v1/memories/search",
            serde_json::json!({
                "query": "neural networks",
                "search_type": "keyword",
                "limit": 10
            }),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn hybrid_search_returns_results() {
    let (app, _state, _dir) = make_test_app().await;

    app.clone()
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({"content": "Python pandas dataframe operations"}),
        ))
        .await
        .unwrap();

    let resp = app
        .oneshot(post_json(
            "/api/v1/memories/search",
            serde_json::json!({
                "query": "pandas dataframe",
                "search_type": "hybrid",
                "limit": 10
            }),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn search_empty_query_returns_error() {
    let (app, _state, _dir) = make_test_app().await;
    let resp = app
        .oneshot(post_json(
            "/api/v1/memories/search",
            serde_json::json!({"query": "  ", "search_type": "semantic"}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn create_memory_empty_content_returns_error() {
    let (app, _state, _dir) = make_test_app().await;
    let resp = app
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({"content": ""}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn create_memory_invalid_type_returns_error() {
    let (app, _state, _dir) = make_test_app().await;
    let resp = app
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({
                "content": "Valid content",
                "memory_type": "invalid_type"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

// ── Authentication ────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn auth_required_when_api_key_set() {
    let tmpdir = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        StorageEngine::new(EngineConfig {
            data_dir: tmpdir.path().to_path_buf(),
            sync_writes: false,
            ..EngineConfig::default()
        })
        .await
        .unwrap(),
    );

    let cfg = Config {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port: 4545,
            api_key: "test-secret".into(), // auth enabled
            api_key_secondary: String::new(),
            allow_auth_disabled: false,
            allowed_origins: vec![],
            rate_limit_rps: 0,
            rate_limit_burst: 50,
            trust_proxy_headers: false,
            env: Environment::Development,
        },
        storage: CfgStorage {
            data_dir: tmpdir.path().to_path_buf(),
            sync_writes: false,
            checkpoint_interval_secs: 300,
            max_wal_size_mb: 256,
            text_field: "content".to_string(),
            default_partition: PartitionId::default_legacy(),
        },
        vector: CfgVector {
            hnsw_resident_budget_mb: None,
            dimension: 384,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 50,
        },
        embedding: EmbeddingConfig { cache_size: 100 },
        connections: ConnectionConfig {
            auto_discovery_threshold: 0.7,
            auto_discovery_top_k: 5,
        },
        search: SearchConfig {
            widen_max_factor: 32,
            list_max_factor: 128,
        },
        tasks: TaskConfig {
            expire_short_term_secs: 300,
            apply_importance_decay_secs: 86400,
            active_forgetting_secs: 86400,
            consolidate_similar_secs: 604800,
            cleanup_archived_secs: 2592000,
            discover_connections_secs: 3600,
            flush_recall_secs: 30,
            discovery_workers: 2,
            discovery_queue_size: 10_000,
            active_forgetting_hard_delete: false,
        },
    };

    let services = create_services(Arc::clone(&engine), &cfg).await.unwrap();
    let app = build_router(AppState {
        services,
        config: Arc::new(cfg),
    });

    // No key → 401
    let no_key = app
        .clone()
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({"content": "test"}),
        ))
        .await
        .unwrap();
    assert_eq!(no_key.status(), StatusCode::UNAUTHORIZED);

    // Wrong key → 401
    let wrong_key = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/memories")
                .header("content-type", "application/json")
                .header("x-api-key", "wrong")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({"content": "test"})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong_key.status(), StatusCode::UNAUTHORIZED);

    // Correct key → 201 Created
    let good_key = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/memories")
                .header("content-type", "application/json")
                .header("x-api-key", "test-secret")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "content": "auth test memory"
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(good_key.status(), StatusCode::CREATED);
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn misconfigured_server_returns_500() {
    let tmpdir = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        StorageEngine::new(EngineConfig {
            data_dir: tmpdir.path().to_path_buf(),
            sync_writes: false,
            ..EngineConfig::default()
        })
        .await
        .unwrap(),
    );

    // api_key empty AND allow_auth_disabled false → misconfiguration
    let cfg = Config {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port: 4545,
            api_key: String::new(),
            api_key_secondary: String::new(),
            allow_auth_disabled: false,
            allowed_origins: vec![],
            rate_limit_rps: 0,
            rate_limit_burst: 50,
            trust_proxy_headers: false,
            env: Environment::Development,
        },
        storage: CfgStorage {
            data_dir: tmpdir.path().to_path_buf(),
            sync_writes: false,
            checkpoint_interval_secs: 300,
            max_wal_size_mb: 256,
            text_field: "content".to_string(),
            default_partition: PartitionId::default_legacy(),
        },
        vector: CfgVector {
            hnsw_resident_budget_mb: None,
            dimension: 384,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 50,
        },
        embedding: EmbeddingConfig { cache_size: 100 },
        connections: ConnectionConfig {
            auto_discovery_threshold: 0.7,
            auto_discovery_top_k: 5,
        },
        search: SearchConfig {
            widen_max_factor: 32,
            list_max_factor: 128,
        },
        tasks: TaskConfig {
            expire_short_term_secs: 300,
            apply_importance_decay_secs: 86400,
            active_forgetting_secs: 86400,
            consolidate_similar_secs: 604800,
            cleanup_archived_secs: 2592000,
            discover_connections_secs: 3600,
            flush_recall_secs: 30,
            discovery_workers: 2,
            discovery_queue_size: 10_000,
            active_forgetting_hard_delete: false,
        },
    };

    let services = create_services(Arc::clone(&engine), &cfg).await.unwrap();
    let app = build_router(AppState {
        services,
        config: Arc::new(cfg),
    });

    // Health must remain available even when misconfigured
    let health_resp = app.clone().oneshot(get("/api/v1/health")).await.unwrap();
    assert_eq!(health_resp.status(), StatusCode::OK);

    // Non-health GET must return 500
    let list_resp = app.clone().oneshot(get("/api/v1/memories")).await.unwrap();
    assert_eq!(list_resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

    // Non-health POST must return 500
    let create_resp = app
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({"content": "test"}),
        ))
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ── Lifecycle ─────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn promote_memory_to_long_term() {
    let (app, _state, _dir) = make_test_app().await;

    // Create short-term
    let create_resp = app
        .clone()
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({
                "content": "Memory to promote",
                "memory_type": "short_term"
            }),
        ))
        .await
        .unwrap();
    let id = body_json(create_resp).await["id"]
        .as_str()
        .unwrap()
        .to_owned();

    // Promote
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/memories/{id}/promote"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify now long-term
    let get_resp = app
        .oneshot(get(&format!("/api/v1/memories/{id}")))
        .await
        .unwrap();
    let json = body_json(get_resp).await;
    assert_eq!(json["memory_type"], "long_term");
    assert!(json["metadata"]["ttl"].is_null());
}

// ── Connections ───────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn find_related_returns_structure() {
    let (app, _state, _dir) = make_test_app().await;

    let create_resp = app
        .clone()
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({"content": "Connection test memory"}),
        ))
        .await
        .unwrap();
    let id = body_json(create_resp).await["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let resp = app
        .oneshot(get(&format!("/api/v1/memories/{id}/related?depth=1")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert!(json["memory_id"].is_string());
    assert!(json["related"].is_array());

    // The centre memory travels with the traversal so a caller drawing a
    // graph needs no second request per node -- which is what kept the
    // graph view from silently recalling everything it displayed.
    assert_eq!(json["memory"]["id"], serde_json::json!(id));
    assert_eq!(json["memory"]["content"], "Connection test memory");
    assert!(json["memory"]["metadata"].is_object());
}

/// Rendering a graph is discovery, not recall: neither the memory the
/// traversal starts from nor the ones it reaches were addressed by the
/// caller.
#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn find_related_records_no_recall() {
    let (app, state, _dir) = make_test_app().await;

    let create_resp = app
        .clone()
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({"content": "Traversal centre"}),
        ))
        .await
        .unwrap();
    let id = body_json(create_resp).await["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let resp = app
        .oneshot(get(&format!("/api/v1/memories/{id}/related?depth=1")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    assert!(
        state.services.repo.recall().is_empty(),
        "graph traversal must not record a recall"
    );
}

/// The counterpart: opening a node from the graph view is an ordinary fetch,
/// and that does count.
#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn fetching_a_memory_records_a_recall() {
    let (app, state, _dir) = make_test_app().await;

    let create_resp = app
        .clone()
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({"content": "Opened from the graph"}),
        ))
        .await
        .unwrap();
    let id = body_json(create_resp).await["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let resp = app
        .oneshot(get(&format!("/api/v1/memories/{id}")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["metadata"]["access_count"], 1);

    assert!(
        !state.services.repo.recall().is_empty(),
        "fetching a memory by id must record a recall"
    );
}

// ── Backup ────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn backup_endpoint_returns_a_nonempty_gzip_archive() {
    let (app, _state, _dir) = make_test_app().await;

    // Seed one memory so the backup has something in it.
    let create_resp = app
        .clone()
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({"content": "Memory to back up"}),
        ))
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let backup_resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/backup")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(backup_resp.status(), StatusCode::OK);
    assert_eq!(
        backup_resp.headers().get("content-type").unwrap(),
        "application/gzip"
    );
    assert_eq!(
        backup_resp.headers().get("content-disposition").unwrap(),
        "attachment; filename=\"remem-backup.tar.gz\""
    );

    let body = backup_resp.into_body().collect().await.unwrap().to_bytes();
    assert!(!body.is_empty());
    // A gzip stream starts with the magic bytes 0x1f 0x8b.
    assert_eq!(&body[0..2], &[0x1f, 0x8b]);
}

// ── Input limit validation ─────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn create_memory_rejects_oversized_content() {
    let (app, _state, _dir) = make_test_app().await;
    let content = "x".repeat(100_001);
    let resp = app
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({ "content": content }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn create_memory_rejects_too_many_tags() {
    let (app, _state, _dir) = make_test_app().await;
    let tags: Vec<String> = (0..51).map(|i| format!("tag{i}")).collect();
    let resp = app
        .oneshot(post_json(
            "/api/v1/memories",
            serde_json::json!({
                "content": "hello world",
                "tags": tags
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}
