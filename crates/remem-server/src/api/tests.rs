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
        Config, ConnectionConfig, EmbeddingConfig, ServerConfig, TaskConfig,
        StorageConfig as CfgStorage, VectorConfig as CfgVector,
    },
    engine::storage::engine::{
        EngineConfig, GraphIndexConfig, TagIndexConfig, TimeSeriesConfig,
        VectorConfig as EngineVector,
    },
    engine::{util::DistanceMetric, StorageEngine},
    services::create_services,
};

// ── Test helpers ──────────────────────────────────────────────────────────────

async fn make_test_app() -> (axum::Router, tempfile::TempDir) {
    let tmpdir = tempfile::tempdir().expect("tempdir");

    let engine_cfg = EngineConfig {
        data_dir: tmpdir.path().to_path_buf(),
        sync_writes: false,
        vector: EngineVector {
            enabled: true,
            dimension: 384,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 50,
            metric: DistanceMetric::L2,
        },
        graph: GraphIndexConfig { enabled: true, directed: true },
        time_series: TimeSeriesConfig { enabled: true },
        tag_index: TagIndexConfig { enabled: true, lowercase: true, min_token_length: 1 },
        ..EngineConfig::default()
    };

    let engine = Arc::new(
        StorageEngine::new(engine_cfg).await.expect("storage engine"),
    );

    let cfg = Config {
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port: 4545,
            api_key: String::new(), // auth disabled
            allow_auth_disabled: true,
            allowed_origins: vec![],
            rate_limit_rps: 0,
            rate_limit_burst: 50,
        },
        storage: CfgStorage {
            data_dir: tmpdir.path().to_path_buf(),
            sync_writes: false,
            checkpoint_interval_secs: 300,
            max_wal_size_mb: 256,
        },
        vector: CfgVector {
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
        tasks: TaskConfig {
            expire_short_term_secs: 300,
            apply_importance_decay_secs: 86400,
            active_forgetting_secs: 86400,
            consolidate_similar_secs: 604800,
            cleanup_archived_secs: 2592000,
            discover_connections_secs: 3600,
            discovery_workers: 2,
            discovery_queue_size: 10_000,
        },
    };

    let services = create_services(Arc::clone(&engine), &cfg)
        .await
        .expect("services (requires embedding model — run with FASTEMBED_CACHE_PATH set)");

    let state = AppState { services, config: Arc::new(cfg) };
    (build_router(state), tmpdir)
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
    let (app, _dir) = make_test_app().await;
    let resp = app.oneshot(get("/api/v1/health")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["status"], "healthy");
}

// ── Stats ─────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn stats_returns_counts() {
    let (app, _dir) = make_test_app().await;
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
    let (app, _dir) = make_test_app().await;

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
async fn create_long_term_memory() {
    let (app, _dir) = make_test_app().await;

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
    let (app, _dir) = make_test_app().await;

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
    let (app, _dir) = make_test_app().await;
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
    let (app, _dir) = make_test_app().await;

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
                .uri(&format!("/api/v1/memories/{id}"))
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
    let (app, _dir) = make_test_app().await;

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
    let (app, _dir) = make_test_app().await;

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
}

// ── Search ────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn semantic_search_returns_results() {
    let (app, _dir) = make_test_app().await;

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
    let (app, _dir) = make_test_app().await;

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
    let (app, _dir) = make_test_app().await;

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
    let (app, _dir) = make_test_app().await;
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
    let (app, _dir) = make_test_app().await;
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
    let (app, _dir) = make_test_app().await;
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
            allow_auth_disabled: false,
            allowed_origins: vec![],
            rate_limit_rps: 0,
            rate_limit_burst: 50,
        },
        storage: CfgStorage {
            data_dir: tmpdir.path().to_path_buf(),
            sync_writes: false,
            checkpoint_interval_secs: 300,
            max_wal_size_mb: 256,
        },
        vector: CfgVector { dimension: 384, hnsw_m: 16, hnsw_ef_construction: 200, hnsw_ef_search: 50 },
        embedding: EmbeddingConfig { cache_size: 100 },
        connections: ConnectionConfig { auto_discovery_threshold: 0.7, auto_discovery_top_k: 5 },
        tasks: TaskConfig {
            expire_short_term_secs: 300,
            apply_importance_decay_secs: 86400,
            active_forgetting_secs: 86400,
            consolidate_similar_secs: 604800,
            cleanup_archived_secs: 2592000,
            discover_connections_secs: 3600,
            discovery_workers: 2,
            discovery_queue_size: 10_000,
        },
    };

    let services = create_services(Arc::clone(&engine), &cfg).await.unwrap();
    let app = build_router(AppState { services, config: Arc::new(cfg) });

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
            allow_auth_disabled: false,
            allowed_origins: vec![],
            rate_limit_rps: 0,
            rate_limit_burst: 50,
        },
        storage: CfgStorage {
            data_dir: tmpdir.path().to_path_buf(),
            sync_writes: false,
            checkpoint_interval_secs: 300,
            max_wal_size_mb: 256,
        },
        vector: CfgVector { dimension: 384, hnsw_m: 16, hnsw_ef_construction: 200, hnsw_ef_search: 50 },
        embedding: EmbeddingConfig { cache_size: 100 },
        connections: ConnectionConfig { auto_discovery_threshold: 0.7, auto_discovery_top_k: 5 },
        tasks: TaskConfig {
            expire_short_term_secs: 300,
            apply_importance_decay_secs: 86400,
            active_forgetting_secs: 86400,
            consolidate_similar_secs: 604800,
            cleanup_archived_secs: 2592000,
            discover_connections_secs: 3600,
            discovery_workers: 2,
            discovery_queue_size: 10_000,
        },
    };

    let services = create_services(Arc::clone(&engine), &cfg).await.unwrap();
    let app = build_router(AppState { services, config: Arc::new(cfg) });

    // Health must remain available even when misconfigured
    let health_resp = app
        .clone()
        .oneshot(get("/api/v1/health"))
        .await
        .unwrap();
    assert_eq!(health_resp.status(), StatusCode::OK);

    // Non-health GET must return 500
    let list_resp = app
        .clone()
        .oneshot(get("/api/v1/memories"))
        .await
        .unwrap();
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
    let (app, _dir) = make_test_app().await;

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
                .uri(&format!("/api/v1/memories/{id}/promote"))
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
    let (app, _dir) = make_test_app().await;

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
}

// ── Input limit validation ─────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn create_memory_rejects_oversized_content() {
    let (app, _dir) = make_test_app().await;
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
    let (app, _dir) = make_test_app().await;
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
