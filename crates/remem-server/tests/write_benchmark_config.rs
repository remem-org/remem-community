use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt as _;
use remem_server::{benchmark::WriteBenchmarkConfig, benchmark_support::build_write_benchmark_app};
use tower::ServiceExt as _;

#[test]
fn defaults_to_one_thousand_writes_at_fifty_concurrent_requests() {
    let config = WriteBenchmarkConfig::from_total_writes(None).unwrap();

    assert_eq!(config.total_writes(), 1_000);
    assert_eq!(config.concurrency(), 50);
    assert_eq!(config.waves(), 20);
}

#[test]
fn accepts_an_override_that_contains_whole_waves() {
    let config = WriteBenchmarkConfig::from_total_writes(Some("2000")).unwrap();

    assert_eq!(config.total_writes(), 2_000);
    assert_eq!(config.waves(), 40);
}

#[test]
fn rejects_a_write_count_that_cannot_fill_a_wave() {
    let error = WriteBenchmarkConfig::from_total_writes(Some("999")).unwrap_err();

    assert!(
        error.contains("multiple of 50"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[ignore = "requires fastembed ONNX model"]
async fn durable_benchmark_app_serves_the_memory_create_route() {
    let dir = tempfile::tempdir().unwrap();
    let app = build_write_benchmark_app(dir.path().to_path_buf())
        .await
        .unwrap();

    assert!(app.sync_writes());
    let response = app
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/memories")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"content": "benchmark fixture"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"].is_string());
}
