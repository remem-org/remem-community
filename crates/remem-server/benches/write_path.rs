use std::{
    env,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use axum::{body::Body, http::Request};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use remem_server::{benchmark::WriteBenchmarkConfig, benchmark_support::build_write_benchmark_app};
use tokio::task::JoinSet;
use tower::ServiceExt as _;

static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn benchmark_data_dir() -> PathBuf {
    env::var_os("REMEM_BENCHMARK_DATA_DIR")
        .map(PathBuf::from)
        .expect("REMEM_BENCHMARK_DATA_DIR must name a persistent benchmark data directory")
}

async fn run_operation(router: axum::Router, config: WriteBenchmarkConfig) {
    for wave in 0..config.waves() {
        let mut requests = JoinSet::new();
        for request in 0..config.concurrency() {
            let router = router.clone();
            let sequence = REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            requests.spawn(async move {
                let body = serde_json::json!({
                    "content": format!(
                        "Remem write-path benchmark memory wave={wave} request={request} sequence={sequence}"
                    ),
                    "memory_type": "short_term",
                    "importance": 0.5,
                    "tags": ["benchmark", "write-path"]
                });
                let response = router
                    .oneshot(
                        Request::builder()
                            .method("POST")
                            .uri("/api/v1/memories")
                            .header("content-type", "application/json")
                            .body(Body::from(serde_json::to_vec(&body).unwrap()))
                            .unwrap(),
                    )
                    .await
                    .expect("benchmark router must return a response");
                assert!(
                    response.status().is_success(),
                    "benchmark create request failed with {}",
                    response.status()
                );
            });
        }
        while let Some(result) = requests.join_next().await {
            result.expect("benchmark request task must not panic");
        }
    }
}

fn write_path_benchmark(criterion: &mut Criterion) {
    let config = WriteBenchmarkConfig::from_environment()
        .unwrap_or_else(|error| panic!("invalid write benchmark configuration: {error}"));
    let revision = env::var("REMEM_BENCHMARK_REVISION").unwrap_or_else(|_| "unknown".to_owned());
    eprintln!(
        "remem write-path benchmark revision={revision} sync_writes=true concurrency={} total_writes={} waves={} execution=in-process-axum",
        config.concurrency(),
        config.total_writes(),
        config.waves(),
    );

    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime");
    let app = runtime
        .block_on(build_write_benchmark_app(benchmark_data_dir()))
        .expect("benchmark app initialization");
    runtime.block_on(run_operation(app.router(), config));

    let mut group = criterion.benchmark_group("write_path");
    group.throughput(Throughput::Elements(config.total_writes() as u64));
    group.bench_function("durable_api_create", |bencher| {
        bencher
            .to_async(&runtime)
            .iter(|| run_operation(app.router(), config));
    });
    group.finish();
}

criterion_group!(benches, write_path_benchmark);
criterion_main!(benches);
