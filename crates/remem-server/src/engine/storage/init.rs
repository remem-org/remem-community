//! Engine initialization helpers.
//!
//! Splits the `StorageEngine::new()` setup into two focused steps:
//!
//! 1. [`open_kv_layer`] — creates the block cache, compaction manager, WAL, and MemTable.
//! 2. [`open_indexes`] — loads (or creates fresh) the four secondary indexes with graceful
//!    corruption fallback.
//!
//! Both functions are called from `StorageEngine::new()`, keeping that method as a thin
//! coordinator that wires the pieces together.

use parking_lot::{Mutex, RwLock};
use std::path::PathBuf;
use std::sync::Arc;

use crate::engine::error::Result;
use crate::engine::index::{
    BTreeConfig, BTreeIndex, CsrGraph, GraphConfig, HnswIndex, InvertedIndexConfig,
    SegmentedBTreeIndex, SegmentedCsrGraph, SegmentedInvertedIndex,
};
#[cfg(feature = "kuzu")]
use crate::engine::index::KuzuGraphIndex;
use super::compaction::CompactionManager;
use super::engine::EngineConfig;
use super::memtable::MemTable;
use super::sstable::BlockCache;
use super::wal::WAL;

// ── Output types ──────────────────────────────────────────────────────────────

/// The KV-layer components produced by [`open_kv_layer`].
pub(super) struct KvLayer {
    pub cache: Arc<BlockCache>,
    pub compaction: Arc<CompactionManager>,
    pub wal: Arc<Mutex<WAL>>,
    /// Absolute path to the WAL file (needed by the WAL-replay step).
    pub wal_path: PathBuf,
    pub memtable: Arc<RwLock<MemTable>>,
}

/// The four optional secondary indexes produced by [`open_indexes`].
pub(super) struct Indexes {
    pub hnsw: Option<Arc<HnswIndex>>,
    pub graph: Option<Arc<parking_lot::RwLock<crate::engine::index::GraphIndex>>>,
    pub time_series: Option<Arc<parking_lot::RwLock<SegmentedBTreeIndex>>>,
    pub tag: Option<Arc<parking_lot::RwLock<SegmentedInvertedIndex>>>,
}

// ── KV layer ──────────────────────────────────────────────────────────────────

/// Initialise the block cache, compaction manager, WAL, and MemTable.
///
/// Does not touch any secondary index.
pub(super) fn open_kv_layer(config: &EngineConfig) -> Result<KvLayer> {
    let cache = Arc::new(BlockCache::new(config.block_cache_size));

    let compaction = Arc::new(CompactionManager::new(
        config.data_dir.join("sstables"),
        config.compaction.clone(),
        Arc::clone(&cache),
    )?);
    compaction.load_existing()?;

    let wal_path = config.data_dir.join("wal").join("current.wal");
    let wal = if wal_path.exists() {
        WAL::open(&wal_path)?
    } else {
        WAL::create(&wal_path)?
    };
    let wal = Arc::new(Mutex::new(wal));

    let memtable = Arc::new(RwLock::new(MemTable::with_capacity(config.memtable_size)));

    Ok(KvLayer { cache, compaction, wal, wal_path, memtable })
}

// ── Secondary indexes ─────────────────────────────────────────────────────────

/// Load (or create fresh) all four secondary indexes.
///
/// Each index is loaded with a graceful corruption fallback: if the index file
/// exists but fails to parse, a warning is logged and a brand-new empty index
/// is returned instead. WAL replay will then rebuild whatever entries were
/// written since the last successful checkpoint.
pub(super) fn open_indexes(config: &EngineConfig) -> Result<Indexes> {
    let hnsw = open_hnsw(config)?;
    let graph = open_graph(config)?;
    let time_series = open_time_series(config)?;
    let tag = open_tag(config)?;
    Ok(Indexes { hnsw, graph, time_series, tag })
}

fn open_hnsw(config: &EngineConfig) -> Result<Option<Arc<HnswIndex>>> {
    if !config.vector.enabled {
        return Ok(None);
    }
    let index_dir = config.data_dir.join("index");
    std::fs::create_dir_all(&index_dir)?;

    let hnsw_config = config.vector.to_hnsw_config();
    let legacy_path = index_dir.join("hnsw.idx");
    let manifest_path = index_dir.join("hnsw.manifest");

    // If both legacy file and manifest exist, manifest wins; warn about leftover.
    if manifest_path.exists() && legacy_path.exists() {
        tracing::warn!(
            "Both legacy hnsw.idx and hnsw.manifest exist; manifest takes precedence. \
             Remove {:?} to suppress this warning.",
            legacy_path
        );
    }

    // Try loading from chunked manifest first (new format)
    if manifest_path.exists() {
        match HnswIndex::load_chunked(&index_dir, hnsw_config.clone()) {
            Ok(Some(idx)) => {
                tracing::info!("Loaded HNSW index from segmented chunks ({} nodes)", idx.len());
                return Ok(Some(Arc::new(idx)));
            }
            Ok(None) => {
                tracing::info!("HNSW manifest exists but empty; starting fresh");
            }
            Err(e) => {
                tracing::warn!(
                    "HNSW segmented index corrupt ({}); falling back to legacy or fresh",
                    e
                );
            }
        }
    }

    // Fall back to legacy single-file format (migration)
    let index = if legacy_path.exists() {
        match HnswIndex::load(&legacy_path) {
            Ok(idx) => {
                tracing::info!(
                    "Loaded legacy HNSW index from {:?} ({} nodes); migrating to segmented format",
                    legacy_path,
                    idx.len()
                );
                // Save as chunked immediately
                if let Err(e) = idx.save_dirty_chunks(&index_dir) {
                    tracing::warn!("Failed to migrate HNSW to segmented format: {}", e);
                } else {
                    let _ = std::fs::remove_file(&legacy_path);
                    tracing::info!("Migrated legacy hnsw.idx to segmented format");
                }
                idx
            }
            Err(e) => {
                tracing::warn!(
                    "Legacy HNSW index at {:?} is corrupt ({}); starting fresh",
                    legacy_path,
                    e
                );
                HnswIndex::new(hnsw_config)
            }
        }
    } else {
        tracing::info!("Creating new HNSW index with dimension {}", config.vector.dimension);
        HnswIndex::new(hnsw_config)
    };

    Ok(Some(Arc::new(index)))
}

fn open_graph(
    config: &EngineConfig,
) -> Result<Option<Arc<parking_lot::RwLock<crate::engine::index::GraphIndex>>>> {
    if !config.graph.enabled {
        return Ok(None);
    }
    let index_dir = config.data_dir.join("index");
    std::fs::create_dir_all(&index_dir)?;

    let graph_config = GraphConfig::default().directed(config.graph.directed);
    let legacy_path = index_dir.join("graph.idx");
    let manifest_path = index_dir.join("graph.manifest");

    if legacy_path.exists() && manifest_path.exists() {
        tracing::warn!(
            "Both legacy graph.idx and graph.manifest exist; manifest takes precedence. \
             Remove {:?} to suppress this warning.",
            legacy_path
        );
    }

    let index = if legacy_path.exists() && !manifest_path.exists() {
        // Migrate: load legacy single-file graph, resave as segmented chunk.
        // (Only supported for CSR; Kuzu starts fresh)
        #[cfg(not(feature = "kuzu"))]
        {
            tracing::info!(
                "Found legacy graph index at {:?}; migrating to segmented format",
                legacy_path
            );
            match SegmentedCsrGraph::migrate_from_legacy(&legacy_path, graph_config.clone(), index_dir.clone()) {
                Ok(seg_idx) => {
                    let _ = std::fs::remove_file(&legacy_path);
                    tracing::info!("Migrated legacy graph.idx to segmented format");
                    seg_idx as crate::engine::index::GraphIndex
                }
                Err(e) => {
                    tracing::warn!(
                        "Legacy graph index at {:?} is corrupt ({}); starting fresh",
                        legacy_path,
                        e
                    );
                    crate::engine::index::new_graph_index(graph_config, index_dir)?
                }
            }
        }
        #[cfg(feature = "kuzu")]
        {
            tracing::info!(
                "Found legacy graph.idx at {:?}, but Kuzu starts fresh (not imported)",
                legacy_path
            );
            crate::engine::index::new_graph_index(graph_config, index_dir)?
        }
    } else {
        // Load from manifest/dir or create fresh
        crate::engine::index::load_graph_index(graph_config, index_dir)?
    };

    Ok(Some(Arc::new(parking_lot::RwLock::new(index))))
}

fn open_time_series(
    config: &EngineConfig,
) -> Result<Option<Arc<parking_lot::RwLock<SegmentedBTreeIndex>>>> {
    if !config.time_series.enabled {
        return Ok(None);
    }
    let index_dir = config.data_dir.join("index");
    std::fs::create_dir_all(&index_dir)?;

    let legacy_path = index_dir.join("timeseries.idx");
    let manifest_path = index_dir.join("timeseries.manifest");

    if legacy_path.exists() && manifest_path.exists() {
        tracing::warn!(
            "Both legacy timeseries.idx and timeseries.manifest exist; manifest takes precedence. \
             Remove {:?} to suppress this warning.",
            legacy_path
        );
    }

    let index = if legacy_path.exists() && !manifest_path.exists() {
        // Migrate: load legacy single-file BTree, resave as segmented
        tracing::info!(
            "Found legacy time-series index at {:?}; migrating to segmented format",
            legacy_path
        );
        match BTreeIndex::load(&legacy_path) {
            Ok(legacy_idx) => {
                let mut seg_idx = SegmentedBTreeIndex::new(BTreeConfig::default(), index_dir.clone());
                // Re-insert all entries from legacy index
                for (ts, key) in legacy_idx.range(0, u64::MAX) {
                    let _ = seg_idx.insert(ts, key);
                }
                // Seal to disk and remove legacy file
                match seg_idx.seal_growing() {
                    Ok(()) => {
                        let _ = std::fs::remove_file(&legacy_path);
                        tracing::info!(
                            "Migrated legacy timeseries.idx to segmented format ({} entries)",
                            seg_idx.len()
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to seal migrated time-series index: {}; continuing with in-memory only",
                            e
                        );
                    }
                }
                seg_idx
            }
            Err(e) => {
                tracing::warn!(
                    "Legacy time-series index at {:?} is corrupt ({}); starting fresh segmented index",
                    legacy_path,
                    e
                );
                SegmentedBTreeIndex::new(BTreeConfig::default(), index_dir)
            }
        }
    } else {
        // Load from segmented manifest (or create fresh if no manifest)
        SegmentedBTreeIndex::load_from_dir(BTreeConfig::default(), index_dir)
    };

    Ok(Some(Arc::new(parking_lot::RwLock::new(index))))
}

fn open_tag(
    config: &EngineConfig,
) -> Result<Option<Arc<parking_lot::RwLock<SegmentedInvertedIndex>>>> {
    if !config.tag_index.enabled {
        return Ok(None);
    }
    let index_dir = config.data_dir.join("index");
    std::fs::create_dir_all(&index_dir)?;

    let cfg = InvertedIndexConfig::default()
        .lowercase(config.tag_index.lowercase)
        .min_token_length(config.tag_index.min_token_length);

    // Check for legacy single-file format and migrate if needed
    let legacy_path = index_dir.join("tags.idx");
    let manifest_path = index_dir.join("tags.manifest");

    let index = if legacy_path.exists() && !manifest_path.exists() {
        // Migrate: load legacy, then save as segmented
        tracing::info!(
            "Found legacy tag index at {:?}; migrating to segmented format",
            legacy_path
        );
        match crate::engine::index::InvertedIndex::load(&legacy_path) {
            Ok(legacy_idx) => {
                let mut seg_idx = SegmentedInvertedIndex::new(cfg.clone(), index_dir.clone());
                // Re-add all docs from legacy index into the growing segment
                let all_tokens = legacy_idx.all_tokens();
                // Collect all keys from all posting lists
                let mut key_set: std::collections::HashSet<bytes::Bytes> =
                    std::collections::HashSet::new();
                for token in &all_tokens {
                    for key in legacy_idx.search(token) {
                        key_set.insert(key);
                    }
                }
                for key in key_set {
                    let tags = legacy_idx.get_tokens(&key);
                    if !tags.is_empty() {
                        let _ = seg_idx.add_tags(key, &tags);
                    }
                }
                // Seal to disk and remove legacy file
                match seg_idx.seal_growing() {
                    Ok(()) => {
                        let _ = std::fs::remove_file(&legacy_path);
                        let doc_count = seg_idx.len();
                        tracing::info!(
                            "Migrated legacy tags.idx to segmented format ({} docs)",
                            doc_count
                        );
                    }
                    Err(e) => {
                        tracing::warn!("Failed to seal migrated tag index: {}; continuing with in-memory only", e);
                    }
                }
                seg_idx
            }
            Err(e) => {
                tracing::warn!(
                    "Legacy tag index at {:?} is corrupt ({}); starting fresh segmented index",
                    legacy_path,
                    e
                );
                SegmentedInvertedIndex::new(cfg, index_dir)
            }
        }
    } else {
        // Load from segmented manifest (or create fresh if no manifest)
        SegmentedInvertedIndex::load_from_dir(cfg, index_dir)
    };

    Ok(Some(Arc::new(parking_lot::RwLock::new(index))))
}
