use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;

use crate::api::AppState;
use crate::error::{AppError, ErrorResponse, Result};
use crate::tasks::registry::{RunLog, TaskStatus};

#[derive(Serialize, utoipa::ToSchema)]
pub struct TaskListResponse {
    pub tasks: Vec<TaskStatus>,
    pub uptime_ms: u64,
    /// Number of auto-discovery tasks currently waiting in the channel.
    pub discovery_queue_depth: usize,
    /// Cumulative count of memories skipped because the discovery channel was full.
    pub discovery_dropped: u64,
    /// Number of discovery worker goroutines currently alive.
    pub discovery_workers_alive: usize,
    /// Total discovery workers that were originally spawned.
    pub discovery_workers_total: usize,
    /// Cumulative restart count across all discovery worker slots (each panic = +1).
    pub discovery_worker_restarts: u32,
    /// Panic message from the most recent discovery worker crash, if any.
    pub discovery_worker_last_panic: Option<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct RunTaskResponse {
    pub task: String,
    pub count: i64,
    pub error: Option<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct TaskHistoryResponse {
    pub task: String,
    pub history: Vec<RunLog>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct PauseTaskResponse {
    pub task: String,
    pub paused: bool,
}

#[utoipa::path(
    get,
    path = "/api/v1/tasks",
    responses(
        (status = 200, description = "Background task status and queue metrics", body = TaskListResponse),
    ),
    tag = "tasks"
)]
pub async fn list_tasks(State(state): State<AppState>) -> Json<TaskListResponse> {
    let sender = &state.services.discovery_tx;
    let queue_depth = sender.max_capacity() - sender.capacity();
    let dropped = state.services.dropped_discovery_count
        .load(std::sync::atomic::Ordering::Relaxed);
    let ws = &state.services.discovery_worker_state;

    Json(TaskListResponse {
        tasks: state.services.task_registry.list(),
        uptime_ms: state.services.task_registry.uptime_ms(),
        discovery_queue_depth: queue_depth,
        discovery_dropped: dropped,
        discovery_workers_alive: ws.alive.load(std::sync::atomic::Ordering::Relaxed),
        discovery_workers_total: ws.total,
        discovery_worker_restarts: ws.restart_count.load(std::sync::atomic::Ordering::Relaxed),
        discovery_worker_last_panic: ws.last_panic.lock().ok().and_then(|g| g.clone()),
    })
}

#[utoipa::path(
    post,
    path = "/api/v1/tasks/{name}/run",
    params(("name" = String, Path, description = "Task name")),
    responses(
        (status = 202, description = "Task dispatched", body = RunTaskResponse),
        (status = 409, description = "Task already running", body = ErrorResponse),
        (status = 422, description = "Unknown task name", body = ErrorResponse),
    ),
    tag = "tasks"
)]
pub async fn run_task(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<RunTaskResponse>)> {
    if !state.services.task_registry.is_known(&name) {
        return Err(AppError::Validation(format!("unknown task: {name}")));
    }

    // try_set_running atomically checks-and-sets under one lock — no TOCTOU.
    // run_task will call set_running again (harmless double-set for background loop compat).
    if !state.services.task_registry.try_set_running(&name) {
        return Err(AppError::Conflict(format!("task '{name}' is already running")));
    }

    // Spawn the task in the background so the HTTP response returns immediately.
    // Long-running tasks (e.g. discover_connections over 50k memories) would
    // otherwise exceed the client timeout. The registry records the result when
    // the task finishes; the history endpoint reflects the outcome.
    let lifecycle = std::sync::Arc::clone(&state.services.lifecycle);
    let registry = std::sync::Arc::clone(&state.services.task_registry);
    let engine = std::sync::Arc::clone(&state.services.engine);
    let task_name = name.clone();
    tokio::spawn(async move {
        crate::tasks::lifecycle::run_task(&task_name, lifecycle, registry, engine).await;
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(RunTaskResponse { task: name, count: 0, error: None }),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/tasks/{name}/history",
    params(("name" = String, Path, description = "Task name")),
    responses(
        (status = 200, description = "Task run history", body = TaskHistoryResponse),
        (status = 422, description = "Unknown task name", body = ErrorResponse),
    ),
    tag = "tasks"
)]
pub async fn get_task_history(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<TaskHistoryResponse>> {
    if !state.services.task_registry.is_known(&name) {
        return Err(AppError::Validation(format!("unknown task: {name}")));
    }
    let history = state.services.task_registry.get_history(&name);
    Ok(Json(TaskHistoryResponse { task: name, history }))
}

#[utoipa::path(
    post,
    path = "/api/v1/tasks/{name}/pause",
    params(("name" = String, Path, description = "Task name")),
    responses(
        (status = 200, description = "Task paused", body = PauseTaskResponse),
        (status = 422, description = "Unknown task name", body = ErrorResponse),
    ),
    tag = "tasks"
)]
pub async fn pause_task(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<PauseTaskResponse>> {
    if !state.services.task_registry.is_known(&name) {
        return Err(AppError::Validation(format!("unknown task: {name}")));
    }
    state.services.task_registry.pause(&name);
    Ok(Json(PauseTaskResponse { task: name, paused: true }))
}

#[utoipa::path(
    post,
    path = "/api/v1/tasks/{name}/resume",
    params(("name" = String, Path, description = "Task name")),
    responses(
        (status = 200, description = "Task resumed", body = PauseTaskResponse),
        (status = 422, description = "Unknown task name", body = ErrorResponse),
    ),
    tag = "tasks"
)]
pub async fn resume_task(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<PauseTaskResponse>> {
    if !state.services.task_registry.is_known(&name) {
        return Err(AppError::Validation(format!("unknown task: {name}")));
    }
    state.services.task_registry.resume(&name);
    Ok(Json(PauseTaskResponse { task: name, paused: false }))
}
