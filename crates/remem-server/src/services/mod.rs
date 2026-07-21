pub mod connection_manager;
pub mod lifecycle_manager;
pub mod memory_manager;
pub mod repository;
pub mod search_engine;
pub mod types;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc;

use crate::engine::{QueryEngine, QueryEngineConfig, StorageEngine};
use crate::services::connection_manager::DiscoveryTask;

use crate::config::Config;
use crate::embedding::EmbeddingService;
use crate::error::Result;
use crate::tasks::TaskRegistry;

pub use connection_manager::ConnectionManager;
pub use lifecycle_manager::LifecycleManager;
pub use memory_manager::MemoryManager;
pub use repository::MemoryRepository;
pub use search_engine::SearchEngine;
pub use types::*;

/// All services bundled together and shared via Arc.
#[derive(Clone)]
pub struct AppServices {
    pub memory: Arc<MemoryManager>,
    pub search: Arc<SearchEngine>,
    pub connection: Arc<ConnectionManager>,
    pub lifecycle: Arc<LifecycleManager>,
    #[allow(dead_code)]
    pub embedding: Arc<EmbeddingService>,
    pub engine: Arc<StorageEngine>,
    /// Typed repository — exposes `load_by_key` / `load` for callers that need
    /// to scan raw storage without going through `MemoryManager` (e.g. stats).
    pub repo: Arc<MemoryRepository>,
    pub task_registry: Arc<TaskRegistry>,
    /// Channel for fire-and-forget auto-discovery tasks.
    pub discovery_tx: mpsc::Sender<DiscoveryTask>,
    /// Receiver end — handed to TaskSupervisor to spawn discovery workers.
    pub discovery_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<DiscoveryTask>>>,
    /// Counts how many discovery tasks were dropped because the channel was full.
    pub dropped_discovery_count: Arc<AtomicU64>,
    /// Live health counters updated by the TaskSupervisor's discovery worker slots.
    pub discovery_worker_state: crate::tasks::supervisor::DiscoveryWorkerState,
}

pub async fn create_services(engine: Arc<StorageEngine>, cfg: &Config) -> Result<AppServices> {
    let query_engine = Arc::new(QueryEngine::new(
        Arc::clone(&engine),
        QueryEngineConfig::default(),
    ));

    let embedding = Arc::new(EmbeddingService::new(cfg.embedding.cache_size)?);
    let repo = Arc::new(MemoryRepository::new(Arc::clone(&engine)));

    // Create discovery channel FIRST so MemoryManager can hold a sender.
    let (discovery_tx, discovery_rx) = mpsc::channel::<DiscoveryTask>(cfg.tasks.discovery_queue_size);
    let dropped_discovery_count = Arc::new(AtomicU64::new(0));
    let discovery_rx = Arc::new(tokio::sync::Mutex::new(discovery_rx));

    let memory = Arc::new(MemoryManager::new(
        Arc::clone(&repo),
        Arc::clone(&embedding),
        discovery_tx.clone(),
    ));
    let search = Arc::new(SearchEngine::new(
        Arc::clone(&repo),
        Arc::clone(&query_engine),
        Arc::clone(&embedding),
    ));
    let connection = Arc::new(ConnectionManager::new(Arc::clone(&repo)));
    let lifecycle = Arc::new(LifecycleManager::new(
        Arc::clone(&repo),
        Arc::clone(&connection),
        Arc::clone(&embedding),
        cfg.tasks.active_forgetting_hard_delete,
    ));
    let task_registry = Arc::new(TaskRegistry::new(&cfg.storage.data_dir));

    // Discovery workers are spawned by TaskSupervisor in main.rs,
    // not here, so their handles can be properly tracked and awaited.

    Ok(AppServices {
        memory,
        search,
        connection,
        lifecycle,
        embedding,
        engine,
        repo,
        task_registry,
        discovery_tx,
        discovery_rx,
        dropped_discovery_count,
        discovery_worker_state: crate::tasks::supervisor::DiscoveryWorkerState::new(0),
    })
}
