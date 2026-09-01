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

use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use super::compaction::CompactionManager;
use super::engine::EngineConfig;
use super::memtable::MemTable;
use super::partition::PartitionBinding;
use super::partitioned_hnsw::PartitionedHnswIndexes;
use super::partitioned_indexes::{PartitionedTagIndex, PartitionedTimeSeriesIndex};
use super::sstable::BlockCache;
use super::wal::WAL;
use crate::engine::error::Result;
#[cfg(feature = "kuzu")]
use crate::engine::index::KuzuGraphIndex;
use crate::engine::index::{
    on_index_parse_failure, BTreeConfig, GraphConfig, HnswIndex, InvertedIndexConfig,
    SegmentedCsrGraph,
};

// ── Output types ──────────────────────────────────────────────────────────────

/// The KV-layer components produced by [`open_kv_layer`].
pub(super) struct KvLayer {
    pub compaction: Arc<CompactionManager>,
    pub wal: WAL,
    /// Absolute path to the WAL file (needed by the WAL-replay step).
    pub wal_path: PathBuf,
    pub memtable: Arc<RwLock<MemTable>>,
    /// The store's record-version counter, shared with `memtable` and with
    /// every memtable that later replaces it.
    pub sequence: Arc<AtomicU64>,
}

/// The four optional secondary indexes produced by [`open_indexes`].
pub(super) struct Indexes {
    pub hnsw: Option<Arc<PartitionedHnswIndexes>>,
    pub graph: Option<Arc<parking_lot::RwLock<crate::engine::index::GraphIndex>>>,
    pub time_series: Option<Arc<parking_lot::RwLock<PartitionedTimeSeriesIndex>>>,
    pub tag: Option<Arc<parking_lot::RwLock<PartitionedTagIndex>>>,
    pub attrs: Option<Arc<crate::engine::attr::index::AttrIndexes>>,
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
        cache,
    )?);
    compaction.load_existing()?;

    let wal_path = config.data_dir.join("wal").join("current.wal");
    let wal = if wal_path.exists() {
        WAL::open(&wal_path)?
    } else {
        WAL::create(&wal_path)?
    };
    // Seeded in `StorageEngine::new` once the WAL has been replayed and any
    // persisted high-water mark read; starting at 1 here only covers a
    // directory with neither.
    let sequence = Arc::new(AtomicU64::new(1));
    let memtable = Arc::new(RwLock::new(MemTable::with_sequence(
        config.memtable_size,
        Arc::clone(&sequence),
    )));

    Ok(KvLayer {
        compaction,
        wal,
        wal_path,
        memtable,
        sequence,
    })
}

// ── Secondary indexes ─────────────────────────────────────────────────────────

/// Load (or create fresh) all four secondary indexes.
///
/// If an index file exists but fails to parse, startup refuses by default
/// (see [`crate::engine::index::on_index_parse_failure`]): rebuilding from
/// empty and relying on WAL replay is lossy, since index files are written
/// at checkpoint and the WAL is truncated at checkpoint. Set
/// `REMEM_ALLOW_INDEX_REBUILD=1` to accept that loss and start anyway.
pub(super) fn open_indexes(config: &EngineConfig) -> Result<Indexes> {
    let hnsw = open_hnsw(config)?;
    let graph = open_graph(config)?;
    let time_series = open_time_series(config)?;
    let tag = open_tag(config)?;
    let attrs = open_attrs(config)?;
    Ok(Indexes {
        hnsw,
        graph,
        time_series,
        tag,
        attrs,
    })
}

/// Validate the registered attribute schema against the persisted one, then
/// open an ordered index per indexed slot.
///
/// Validation happens here, inside engine open, so there is never a window in
/// which the engine is running but cannot answer an attribute query.
fn open_attrs(
    config: &EngineConfig,
) -> Result<Option<Arc<crate::engine::attr::index::AttrIndexes>>> {
    let Some(schema) = &config.attr_schema else {
        return Ok(None);
    };
    schema.check_encodable()?;
    let dir = config.data_dir.join("index").join("attr");
    std::fs::create_dir_all(&dir)?;

    if let Some(persisted) = crate::engine::attr::schema::AttrSchema::read(&dir)? {
        schema.validate_against(&persisted)?;
    }
    schema.write_atomic(&dir)?;

    let default = PartitionBinding::legacy_tenant(config.default_partition.clone());
    Ok(Some(Arc::new(
        crate::engine::attr::index::AttrIndexes::open_partitioned(dir, schema, default)?,
    )))
}

fn open_hnsw(config: &EngineConfig) -> Result<Option<Arc<PartitionedHnswIndexes>>> {
    if !config.vector.enabled {
        return Ok(None);
    }
    let index_dir = config.data_dir.join("index");
    std::fs::create_dir_all(&index_dir)?;

    let hnsw_config = config.vector.to_hnsw_config();
    let root = index_dir.join("hnsw-partitions");
    let default_binding = PartitionBinding::legacy_tenant(config.default_partition.clone());
    let legacy_path = index_dir.join("hnsw.idx");
    let legacy_manifest = index_dir.join("hnsw.manifest");

    let indexes = Arc::new(PartitionedHnswIndexes::open(
        root,
        hnsw_config.clone(),
        default_binding.clone(),
        config.vector.hnsw_resident_budget_bytes,
    )?);

    if let Some(legacy) = load_legacy_hnsw(&index_dir, &hnsw_config)? {
        let migrated = indexes.migrate_legacy_index(&default_binding, &legacy)?;
        indexes.save_dirty()?;
        cleanup_legacy_hnsw_files(&index_dir)?;
        tracing::warn!(
            "Migrated {} vectors from legacy shared HNSW files under {:?} into partition {:?}",
            migrated,
            index_dir,
            default_binding
        );
    }

    if legacy_path.exists() || legacy_manifest.exists() {
        tracing::warn!(
            "Found legacy shared HNSW files under {:?}, but they contained no vectors to migrate",
            index_dir
        );
    }

    Ok(Some(indexes))
}

fn load_legacy_hnsw(
    index_dir: &std::path::Path,
    hnsw_config: &crate::engine::index::HnswConfig,
) -> Result<Option<HnswIndex>> {
    let legacy_path = index_dir.join("hnsw.idx");
    let manifest_path = index_dir.join("hnsw.manifest");

    if manifest_path.exists() {
        match HnswIndex::load_chunked(index_dir, hnsw_config.clone()) {
            Ok(index) => return Ok(index),
            Err(err) => {
                on_index_parse_failure(&manifest_path, &err)?;
                return Ok(None);
            }
        }
    }

    if legacy_path.exists() {
        match HnswIndex::load(&legacy_path) {
            Ok(index) => return Ok(Some(index)),
            Err(err) => {
                on_index_parse_failure(&legacy_path, &err)?;
                return Ok(None);
            }
        }
    }

    Ok(None)
}

fn cleanup_legacy_hnsw_files(index_dir: &std::path::Path) -> Result<()> {
    for filename in ["hnsw.idx", "hnsw.manifest", "deleted_nodes.bin"] {
        let path = index_dir.join(filename);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
    }
    for entry in std::fs::read_dir(index_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("hnsw_") && name.ends_with(".seg") {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
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
            match SegmentedCsrGraph::migrate_from_legacy(
                &legacy_path,
                graph_config.clone(),
                index_dir.clone(),
            ) {
                Ok(seg_idx) => {
                    let _ = std::fs::remove_file(&legacy_path);
                    tracing::info!("Migrated legacy graph.idx to segmented format");
                    seg_idx as crate::engine::index::GraphIndex
                }
                Err(e) => {
                    on_index_parse_failure(&legacy_path, &e)?;
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
) -> Result<Option<Arc<parking_lot::RwLock<PartitionedTimeSeriesIndex>>>> {
    if !config.time_series.enabled {
        return Ok(None);
    }
    let root = config.data_dir.join("index").join("timeseries");
    let default = PartitionBinding::legacy_tenant(config.default_partition.clone());
    let index = PartitionedTimeSeriesIndex::open(root, BTreeConfig::default(), default)?;
    Ok(Some(Arc::new(parking_lot::RwLock::new(index))))
}

fn open_tag(
    config: &EngineConfig,
) -> Result<Option<Arc<parking_lot::RwLock<PartitionedTagIndex>>>> {
    if !config.tag_index.enabled {
        return Ok(None);
    }
    let index_dir = config.data_dir.join("index").join("tags");
    let cfg = InvertedIndexConfig::default()
        .lowercase(config.tag_index.lowercase)
        .min_token_length(config.tag_index.min_token_length);

    let default = PartitionBinding::legacy_tenant(config.default_partition.clone());
    let index = PartitionedTagIndex::open(index_dir, cfg, default)?;
    Ok(Some(Arc::new(parking_lot::RwLock::new(index))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::index::HnswConfig;
    use bytes::Bytes;
    use tempfile::tempdir;

    fn config_for(dir: &std::path::Path) -> EngineConfig {
        EngineConfig {
            data_dir: dir.to_path_buf(),
            ..Default::default()
        }
    }

    /// A corrupt partitioned segmented index must refuse startup.
    #[test]
    fn a_corrupt_hnsw_manifest_refuses_even_with_a_legacy_file_present() {
        let _lock = crate::engine::index::index_rebuild_env_guard();
        let dir = tempdir().unwrap();
        let hnsw_dir = dir
            .path()
            .join("index")
            .join("hnsw-partitions")
            .join("default")
            .join("default");
        std::fs::create_dir_all(&hnsw_dir).unwrap();
        std::fs::write(hnsw_dir.join("hnsw.manifest"), b"not a manifest").unwrap();

        // `Indexes` deliberately does not derive `Debug` (its fields wrap
        // index types that don't either), so `.unwrap_err()` isn't available
        // here — match instead.
        let err = match open_indexes(&config_for(dir.path())) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected a corrupt hnsw.manifest to refuse startup"),
        };
        assert!(err.contains("hnsw.manifest"), "{err}");
        assert!(err.contains("REMEM_ALLOW_INDEX_REBUILD"), "{err}");
    }

    /// With `REMEM_ALLOW_INDEX_REBUILD=1`, a corrupt partition manifest must
    /// produce an empty partitioned HNSW manager rather than serving stale
    /// vectors from unreadable chunks.
    #[test]
    fn allow_index_rebuild_starts_empty_even_with_a_valid_legacy_file_present() {
        let _lock = crate::engine::index::index_rebuild_env_guard();
        let dir = tempdir().unwrap();
        let root = dir.path().join("index").join("hnsw-partitions");
        let hnsw_config = HnswConfig::with_dim(4);
        let indexes = super::super::partitioned_hnsw::PartitionedHnswIndexes::open(
            root,
            hnsw_config,
            super::super::partition::PartitionBinding::legacy_default(),
            None,
        )
        .unwrap();
        indexes
            .insert(
                &super::super::partition::PartitionBinding::legacy_default(),
                Bytes::from_static(b"stale-vector"),
                vec![1.0, 2.0, 3.0, 4.0],
            )
            .unwrap();
        indexes.save_dirty().unwrap();
        let manifest = dir
            .path()
            .join("index")
            .join("hnsw-partitions")
            .join("default")
            .join("default")
            .join("hnsw.manifest");
        std::fs::write(manifest, b"not a manifest").unwrap();

        std::env::set_var("REMEM_ALLOW_INDEX_REBUILD", "1");
        let result = open_indexes(&config_for(dir.path()));
        std::env::remove_var("REMEM_ALLOW_INDEX_REBUILD");

        let indexes = result.expect("REMEM_ALLOW_INDEX_REBUILD=1 must allow startup");
        let hnsw = indexes.hnsw.expect("vector index is enabled by default");
        assert_eq!(
            hnsw.len(),
            0,
            "a corrupt manifest under REMEM_ALLOW_INDEX_REBUILD=1 must rebuild empty, \
             not fall through to whatever the legacy file holds"
        );
        assert!(
            hnsw.get_vector_by_key(b"stale-vector").is_none(),
            "the stale legacy vector must not be served"
        );
    }

    #[test]
    fn legacy_shared_hnsw_vectors_migrate_to_configured_default_partition() {
        let dir = tempdir().unwrap();
        let index_dir = dir.path().join("index");
        std::fs::create_dir_all(&index_dir).unwrap();
        let hnsw_config = HnswConfig::with_dim(4);
        let legacy = HnswIndex::new(hnsw_config.clone());
        legacy
            .insert(
                Bytes::from_static(b"memory:legacy"),
                vec![1.0, 0.0, 0.0, 0.0],
            )
            .unwrap();
        legacy.save_dirty_chunks(&index_dir).unwrap();
        legacy.save_deleted_nodes(&index_dir).unwrap();

        let mut config = config_for(dir.path());
        config.vector.dimension = 4;
        config.default_partition = super::super::partition::PartitionId::new("finance").unwrap();

        let indexes = open_indexes(&config).unwrap();
        let hnsw = indexes.hnsw.expect("vector index is enabled by default");
        let binding =
            super::super::partition::PartitionBinding::legacy_tenant(config.default_partition);
        let migrated_key =
            super::super::partition::encode_record_key(&binding, b"memory:legacy").unwrap();

        assert!(hnsw.get_vector_by_key(&migrated_key).is_some());
        assert!(
            hnsw.get_vector_by_key(b"memory:legacy").is_none(),
            "legacy vector IDs must be rewritten to the migrated primary-record key"
        );
        assert!(!index_dir.join("hnsw.manifest").exists());
        assert!(index_dir
            .join("hnsw-partitions")
            .join("default")
            .join("finance")
            .join("hnsw.manifest")
            .exists());
    }

    /// The refusal above must not have made ordinary startup fail.
    #[test]
    fn a_directory_with_no_index_files_opens_fresh() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("index")).unwrap();

        open_indexes(&config_for(dir.path())).unwrap();
    }
}
