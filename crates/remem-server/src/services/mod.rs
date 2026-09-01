pub mod attrs;
pub mod connection_manager;
pub mod cursor;
pub mod filters;
pub mod lifecycle_manager;
pub mod memory_manager;
pub mod recall;
pub mod repository;
pub mod search_engine;
pub mod types;

/// `MemoryManager::list` coverage. A sibling file rather than a module inside
/// `memory_manager.rs` because the equivalence oracle below is a full
/// deserializing scan, and the listing-path guard in this file inspects
/// `memory_manager.rs` as text — an oracle living there would match it.
#[cfg(test)]
mod memory_manager_list_tests;

/// Recall semantics (REM-27): which operations count as a recall, which do
/// not, and the coalescing rule. A sibling file for the same reason as the
/// listing tests above — the guards below inspect `memory_manager.rs` as text.
#[cfg(test)]
mod memory_manager_recall_tests;

/// Archiving retires a memory from the similarity index, at every path that
/// archives. A sibling file for the same reason as the tests above.
#[cfg(test)]
mod retire_archived_vectors_tests;
#[cfg(test)]
mod upgrade_rehearsal_tests;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
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

/// All services bundled together and shared via Arc.
#[derive(Clone)]
pub struct AppServices {
    pub memory: Arc<MemoryManager>,
    pub search: Arc<SearchEngine>,
    pub connection: Arc<ConnectionManager>,
    pub lifecycle: Arc<LifecycleManager>,
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
        QueryEngineConfig {
            widen_max_factor: cfg.search.widen_max_factor,
            ..QueryEngineConfig::default()
        },
    ));

    let embedding = Arc::new(EmbeddingService::new(cfg.embedding.cache_size)?);
    let repo = Arc::new(MemoryRepository::new(Arc::clone(&engine)));

    // Create discovery channel FIRST so MemoryManager can hold a sender.
    let (discovery_tx, discovery_rx) =
        mpsc::channel::<DiscoveryTask>(cfg.tasks.discovery_queue_size);
    let dropped_discovery_count = Arc::new(AtomicU64::new(0));
    let discovery_rx = Arc::new(tokio::sync::Mutex::new(discovery_rx));

    let memory = Arc::new(MemoryManager::new(
        Arc::clone(&repo),
        Arc::clone(&embedding),
        discovery_tx.clone(),
        cfg.search.list_max_factor,
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
        engine,
        repo,
        task_registry,
        discovery_tx,
        discovery_rx,
        dropped_discovery_count,
        discovery_worker_state: crate::tasks::supervisor::DiscoveryWorkerState::new(0),
    })
}

#[cfg(test)]
mod search_path_guard {
    /// Every retrieval on the search path goes through `QueryEngine`, so that
    /// tenancy (REM-76), per-partition indexes (REM-77), cost estimation
    /// (REM-80), read snapshots (REM-79) and shard fan-out are each defined
    /// once instead of threaded by hand through every call site. A missed site
    /// under REM-76 is a cross-tenant read, not a bug.
    ///
    /// This test lives here rather than in `search_engine.rs` because a guard
    /// inside the file it inspects would match its own string literals.
    ///
    /// `engine.vector_metric()` is deliberately absent from the list: it reads
    /// configuration, it does not retrieve data.
    #[test]
    fn no_direct_storage_engine_retrieval_remains_in_search_engine() {
        const FORBIDDEN: &[&str] = &[
            "engine.vector_search",
            "engine.vector_count",
            "engine.tag_search_scored",
            "engine.tag_search_and",
            "engine.time_range_query",
            "engine.traverse_graph",
            "engine.content_scan",
            "engine.get(",
        ];

        let source = include_str!("search_engine.rs");

        for needle in FORBIDDEN {
            assert!(
                !source.contains(needle),
                "`{needle}` reappeared in services/search_engine.rs — \
                 route it through QueryEngine instead (REM-88)"
            );
        }
    }
}

#[cfg(test)]
mod listing_path_guard {
    /// The listing path reads through the attribute store: one ordered index
    /// walk that filters from sidecar rows, rather than a scan of the time
    /// index that deserializes every record to decide. Reintroducing either
    /// call would restore the full-corpus read this path exists not to do.
    ///
    /// This test lives here rather than in `memory_manager.rs` because a
    /// guard inside the file it inspects would match its own string literals.
    #[test]
    fn no_full_scan_or_in_memory_filter_remains_in_the_listing_path() {
        const FORBIDDEN: &[&str] = &["matches_filters", "time_range_query"];

        let source = include_str!("memory_manager.rs");

        for needle in FORBIDDEN {
            assert!(
                !source.contains(needle),
                "`{needle}` reappeared in services/memory_manager.rs — \
                 the listing path selects through the attribute store (REM-96)"
            );
        }
    }

    /// The listing walks a page at a time. `select_ordered_partitioned` takes
    /// an `Option<usize>` limit and will happily accept `None`, which is the
    /// unbounded walk this change replaced -- one page request settling every
    /// candidate in the store. Only the paged form belongs here.
    #[test]
    fn the_listing_path_uses_only_the_bounded_walk() {
        let source = include_str!("memory_manager.rs");

        assert!(
            !source.contains("select_ordered"),
            "the listing path must select through `select_page_partitioned`; \
             `select_ordered` is the unbounded walk REM-28 replaced"
        );
        assert!(
            source.contains("select_page_partitioned"),
            "this guard is only meaningful while the listing still selects \
             through the paged walk -- if that call moved, move the guard"
        );
    }

    /// The sweeps select what is due. Reintroducing a corpus walk here is the
    /// regression that made five background tasks each read every record in
    /// the store, every run, to find the handful that needed anything.
    #[test]
    fn no_full_scan_remains_in_the_lifecycle_sweeps() {
        const FORBIDDEN: &[&str] = &["time_range_query", "select_ordered"];

        let source = include_str!("lifecycle_manager.rs");

        for needle in FORBIDDEN {
            assert!(
                !source.contains(needle),
                "`{needle}` reappeared in services/lifecycle_manager.rs — \
                 sweeps select the memories that are due (REM-28)"
            );
        }
        assert!(
            source.contains("select_due"),
            "the sweeps must reach records through the due-time index"
        );
    }

    /// The connection listing is the third path that used to walk everything.
    #[test]
    fn no_full_scan_remains_in_the_connection_listing() {
        const FORBIDDEN: &[&str] = &["time_range_query", "select_ordered"];

        let source = include_str!("connection_manager.rs");

        for needle in FORBIDDEN {
            assert!(
                !source.contains(needle),
                "`{needle}` reappeared in services/connection_manager.rs — \
                 the connection listing is a bounded traversal (REM-28)"
            );
        }
    }
}

#[cfg(test)]
mod partition_scope_guard {
    /// Substring guards, so they only see the files they are handed. The
    /// list below must name every service file: a new one added without a
    /// line here is unguarded, which `every_service_file_is_guarded` catches
    /// by walking the directory and comparing.
    const FILES: &[(&str, &str)] = &[
        ("attrs.rs", include_str!("attrs.rs")),
        (
            "connection_manager.rs",
            include_str!("connection_manager.rs"),
        ),
        ("cursor.rs", include_str!("cursor.rs")),
        ("filters.rs", include_str!("filters.rs")),
        ("lifecycle_manager.rs", include_str!("lifecycle_manager.rs")),
        ("memory_manager.rs", include_str!("memory_manager.rs")),
        ("recall.rs", include_str!("recall.rs")),
        ("repository.rs", include_str!("repository.rs")),
        ("search_engine.rs", include_str!("search_engine.rs")),
        ("types.rs", include_str!("types.rs")),
    ];

    #[test]
    fn services_do_not_use_unscoped_secondary_index_reads() {
        const FORBIDDEN: &[&str] = &[
            ".time_range_query(",
            ".tag_search_and(",
            ".tag_search_scored(",
            ".get_neighbors(",
            ".traverse_graph(",
            ".content_scan(",
            ".select_ordered(",
            ".select(",
        ];

        for (path, source) in FILES {
            for needle in FORBIDDEN {
                assert!(
                    !source.contains(needle),
                    "`{needle}` reappeared in services/{path}; use a partition-scoped or \
                     explicit maintenance API for secondary-index reads"
                );
            }
        }
    }

    /// The guard above is only as good as its file list. A service module
    /// that never gets added to `FILES` is silently exempt from every check
    /// in this file, which is precisely how an unscoped read gets
    /// reintroduced after the reviewer who wrote the guard has moved on.
    #[test]
    fn every_service_file_is_guarded() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/services");
        let mut unguarded = Vec::new();

        for entry in std::fs::read_dir(&dir).expect("services directory must be readable") {
            let entry = entry.expect("readable directory entry");
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".rs") || name == "mod.rs" {
                continue;
            }
            // Test-only siblings hold oracles that deliberately scan.
            if name.ends_with("_tests.rs") {
                continue;
            }
            if !FILES.iter().any(|(guarded, _)| *guarded == name) {
                unguarded.push(name);
            }
        }

        assert!(
            unguarded.is_empty(),
            "services/{unguarded:?} are not listed in the partition-scope guard; \
             add them to FILES so their secondary-index reads are checked"
        );
    }
}
