use utoipa::OpenApi;

use crate::api::routes::{
    connections::{
        ConnectionResponse, CreateConnectionRequest, DeleteConnectionResponse,
        ListConnectionsResponse, RelatedItem, RelatedResponse,
    },
    health::{DeepHealthCheck, DeepHealthResponse, HealthResponse, ReadyResponse, StorageReadiness, Stats, StatsResponse},
    memories::{
        CreateMemoryRequest, DeleteResponse, ExtractedEntity, ExtractedRelationship,
        GraphExtraction, MemoryListResponse, UpdateMemoryRequest,
    },
    search::{SearchRequest, SearchResponse},
    tasks::{PauseTaskResponse, RunTaskResponse, TaskHistoryResponse, TaskListResponse},
};
use crate::error::ErrorResponse;
use crate::services::types::{
    Connection, Memory, Metadata, MemoryType, RelationshipType, SearchResult,
};
use crate::tasks::registry::{RunLog, TaskStatus};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "remem-server API",
        version = "1.0.0",
        description = "Persistent memory system for LLMs and AI agents."
    ),
    paths(
        crate::api::routes::health::health,
        crate::api::routes::health::ready,
        crate::api::routes::health::deep_health,
        crate::api::routes::health::stats,
        crate::api::routes::memories::create_memory,
        crate::api::routes::memories::get_memory,
        crate::api::routes::memories::update_memory,
        crate::api::routes::memories::delete_memory,
        crate::api::routes::memories::list_memories,
        crate::api::routes::search::search_memories,
        crate::api::routes::connections::list_connections,
        crate::api::routes::connections::create_connection,
        crate::api::routes::connections::delete_connection,
        crate::api::routes::connections::find_related,
        crate::api::routes::lifecycle::promote_memory,
        crate::api::routes::tasks::list_tasks,
        crate::api::routes::tasks::run_task,
        crate::api::routes::tasks::get_task_history,
        crate::api::routes::tasks::pause_task,
        crate::api::routes::tasks::resume_task,
    ),
    components(schemas(
        Memory, Metadata, Connection, MemoryType, RelationshipType, SearchResult,
        CreateMemoryRequest, UpdateMemoryRequest, DeleteResponse,
        GraphExtraction, ExtractedEntity, ExtractedRelationship,
        MemoryListResponse,
        SearchRequest, SearchResponse,
        CreateConnectionRequest, ConnectionResponse, ListConnectionsResponse,
        RelatedResponse, RelatedItem, DeleteConnectionResponse,
        HealthResponse, ReadyResponse, StorageReadiness, StatsResponse, Stats, DeepHealthResponse, DeepHealthCheck,
        TaskListResponse, RunTaskResponse, TaskHistoryResponse, PauseTaskResponse,
        TaskStatus, RunLog,
        ErrorResponse,
    )),
    tags(
        (name = "memories", description = "Memory CRUD, search, and lifecycle"),
        (name = "connections", description = "Connection management and graph traversal"),
        (name = "tasks", description = "Background task monitoring and control"),
        (name = "system", description = "Health, stats, and system information"),
    ),
)]
pub struct ApiDoc;

/// Serve the OpenAPI 3 spec as JSON.
pub async fn openapi_json() -> axum::Json<utoipa::openapi::OpenApi> {
    axum::Json(ApiDoc::openapi())
}
