pub mod middleware;
pub mod openapi;
pub mod routes;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use axum::{
    middleware as axum_middleware,
    routing::{delete, get, post, put},
    Router,
};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::config::Config;
use crate::services::AppServices;

/// Shared application state threaded through all Axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub services: AppServices,
    pub config: Arc<Config>,
}

fn build_cors(allowed_origins: &[String]) -> CorsLayer {
    if allowed_origins.is_empty() {
        return CorsLayer::permissive();
    }
    let parsed: Vec<axum::http::HeaderValue> = allowed_origins
        .iter()
        .filter_map(|o| o.parse().ok())
        .collect();
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(parsed))
        .allow_headers(Any)
        .allow_methods(Any)
}

pub fn build_router(state: AppState) -> Router {
    let cors = build_cors(&state.config.server.allowed_origins);
    let rate_limit_rps = state.config.server.rate_limit_rps;
    let rate_limit_burst = state.config.server.rate_limit_burst;

    #[cfg(feature = "business")]
    let (prometheus_layer, metric_handle) = axum_prometheus::PrometheusMetricLayer::pair();
    #[cfg(feature = "business")]
    let metrics_services = state.services.clone();

    let api = Router::new()
        // Health, readiness, and OpenAPI spec (no auth)
        .route("/api/v1/health", get(routes::health::health))
        .route("/api/v1/ready", get(routes::health::ready))
        .route("/api/v1/health/deep", get(routes::health::deep_health))
        .route("/api/v1/openapi.json", get(openapi::openapi_json))
        // Stats
        .route("/api/v1/stats", get(routes::health::stats))
        // Memories CRUD
        .route("/api/v1/memories", post(routes::memories::create_memory))
        .route("/api/v1/memories", get(routes::memories::list_memories))
        .route("/api/v1/memories/:id", get(routes::memories::get_memory))
        .route("/api/v1/memories/:id", put(routes::memories::update_memory))
        .route(
            "/api/v1/memories/:id",
            delete(routes::memories::delete_memory),
        )
        // Search
        .route(
            "/api/v1/memories/search",
            post(routes::search::search_memories),
        )
        // Related
        .route(
            "/api/v1/memories/:id/related",
            get(routes::connections::find_related),
        )
        // Lifecycle
        .route(
            "/api/v1/memories/:id/promote",
            post(routes::lifecycle::promote_memory),
        )
        // Connections
        .route(
            "/api/v1/connections",
            get(routes::connections::list_connections),
        )
        .route(
            "/api/v1/connections",
            post(routes::connections::create_connection),
        )
        .route(
            "/api/v1/connections/:src/:dst",
            delete(routes::connections::delete_connection),
        )
        // Background tasks
        .route("/api/v1/tasks", get(routes::tasks::list_tasks))
        .route("/api/v1/tasks/:name/run", post(routes::tasks::run_task))
        .route("/api/v1/tasks/:name/history", get(routes::tasks::get_task_history))
        .route("/api/v1/tasks/:name/pause", post(routes::tasks::pause_task))
        .route("/api/v1/tasks/:name/resume", post(routes::tasks::resume_task));

    // Business: Prometheus metrics endpoint (no auth — scraped by Prometheus server)
    #[cfg(feature = "business")]
    let api = api.route("/api/v1/metrics", get({
        let services = metrics_services.clone();
        let handle = metric_handle.clone();
        move || {
            let services = services.clone();
            let handle = handle.clone();
            async move {
                crate::business::monitoring::update_from_services(&services);
                handle.render()
            }
        }
    }));

    let api = api
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            middleware::auth::auth_middleware,
        ))
        .with_state(state);

    let router = Router::new().merge(api);

    // Business: wrap all routes with the Prometheus HTTP instrumentation layer
    // (records per-route latency histograms and request counts)
    #[cfg(feature = "business")]
    let router = router.layer(prometheus_layer);

    let router = router
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<axum::body::Body>| {
                let request_id = request
                    .headers()
                    .get("X-Request-ID")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-");
                tracing::info_span!(
                    "http_request",
                    method = %request.method(),
                    path = %request.uri().path(),
                    request_id = %request_id,
                )
            }),
        )
        .layer(cors);

    middleware::rate_limit::apply_if_configured(router, rate_limit_rps, rate_limit_burst)
}
