//! Storage engine: The main interface to the LSM-tree storage
//!
//! The storage engine coordinates:
//! - WAL for durability
//! - MemTable for recent writes
//! - SSTables for persistent storage
//! - Compaction for space reclamation

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::compaction::{CompactionConfig, CompactionManager};
use super::memtable::{ImmutableMemTable, MemTable};
use super::partition::{
    encode_record_key, PartitionBinding, PartitionId, PartitionScope, PartitionedRecordKey,
    TenantId,
};
use super::partitioned_hnsw::PartitionedHnswIndexes;
use super::partitioned_indexes::{PartitionedTagIndex, PartitionedTimeSeriesIndex};
use super::wal::WalRecord;
use super::wal_commit::{WalCommitCoordinator, WalCommitOptions};
use crate::engine::error::{Result, StorageError};
use crate::engine::index::{EdgeMetadata, GraphIndex, HnswConfig, TraversalResult};
use crate::engine::util::DistanceMetric;

/// Configuration for vector search
#[derive(Debug, Clone)]
pub struct VectorConfig {
    /// Enable vector search
    pub enabled: bool,
    /// Vector dimension
    pub dimension: usize,
    /// HNSW M parameter (connections per node)
    pub hnsw_m: usize,
    /// HNSW ef_construction parameter
    pub hnsw_ef_construction: usize,
    /// HNSW ef_search parameter (default search quality)
    pub hnsw_ef_search: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Ceiling, in bytes, on the partition graphs held in memory at once.
    ///
    /// `None` — the default — means partitions are never released to reclaim
    /// memory, which is how every deployment behaved before this existed and
    /// is the right answer while a deployment has one partition. Set it once
    /// partition count is large enough that holding every graph is a problem.
    ///
    /// It is a target, not an admission limit: a partition a request needs is
    /// always served, and the budget is restored afterwards by releasing the
    /// least recently used partitions. Refusing work belongs to admission
    /// control, where it can be charged against a tenant.
    pub hnsw_resident_budget_bytes: Option<usize>,
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dimension: 384, // Common embedding dimension (e.g., all-MiniLM-L6-v2)
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 50,
            metric: DistanceMetric::L2,
            hnsw_resident_budget_bytes: None,
        }
    }
}

impl VectorConfig {
    /// Convert to HNSW config
    pub(super) fn to_hnsw_config(&self) -> HnswConfig {
        HnswConfig::with_dim(self.dimension)
            .m(self.hnsw_m)
            .ef_construction(self.hnsw_ef_construction)
            .ef_search(self.hnsw_ef_search)
            .metric(self.metric)
    }
}

/// Configuration for graph index
#[derive(Debug, Clone)]
pub struct GraphIndexConfig {
    /// Enable graph index
    pub enabled: bool,
    /// Whether the graph is directed
    pub directed: bool,
}

impl Default for GraphIndexConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            directed: true,
        }
    }
}

/// Configuration for time-series index
#[derive(Debug, Clone)]
pub struct TimeSeriesConfig {
    /// Enable time-series index
    pub enabled: bool,
}

impl Default for TimeSeriesConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// Configuration for tag/text index
#[derive(Debug, Clone)]
pub struct TagIndexConfig {
    /// Enable tag/text index
    pub enabled: bool,
    /// Whether to normalize tokens to lowercase
    pub lowercase: bool,
    /// Minimum token length
    pub min_token_length: usize,
}

impl Default for TagIndexConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            lowercase: true,
            min_token_length: 1,
        }
    }
}

/// Turns a stored payload's raw bytes into its attribute row, or `None` when
/// the payload cannot be projected under the registered schema (e.g. it
/// isn't valid JSON, or isn't the record shape the caller's schema expects).
///
/// An opaque function rather than a concrete record type: the engine's
/// sidecar rows are schema-generic (slot numbers and fixed-width types
/// only, never field names or record types -- see `engine::attr`'s module
/// docs), and importing a type like `services::types::StoredMemory` here
/// would defeat that. Mirrors `EngineConfig::text_field` above, which lets
/// `content_scan` interpret opaque JSON by field *name* for the same
/// reason.
pub type AttrProjectFn = dyn Fn(&[u8]) -> Option<crate::engine::attr::row::AttrRow> + Send + Sync;

/// Which indexed slots a record must carry *no* ordering entry for, given its
/// row.
///
/// The engine has no idea what any slot means, so it cannot decide that (say)
/// a retired record should leave the browsing order — only the domain can.
/// Supplied as an opaque function for the same reason `AttrProjectFn` is:
/// the engine stays schema-generic, and the one caller that needs the
/// judgement asks for it rather than encoding it.
///
/// Consulted only by the post-open backfill. Steady-state writes retire
/// through [`StorageEngine::retire_ordering_entry`] at the moment the record
/// changes state, which is durable in its own right; the backfill needs this
/// because it is re-deriving rows for records whose state changed before the
/// index existed to record it.
pub type AttrIndexExemptFn = dyn Fn(&crate::engine::attr::row::AttrRow) -> Vec<u16> + Send + Sync;

/// Configuration for the storage engine
#[derive(Clone)]
pub struct EngineConfig {
    /// Base directory for all data files
    pub data_dir: PathBuf,
    /// Maximum MemTable size in bytes before flushing
    pub memtable_size: usize,
    /// Block cache size in bytes
    pub block_cache_size: usize,
    /// Whether to sync WAL after each write
    pub sync_writes: bool,
    /// Compaction configuration
    pub compaction: CompactionConfig,
    /// Vector search configuration
    pub vector: VectorConfig,
    /// Graph index configuration
    pub graph: GraphIndexConfig,
    /// Time-series index configuration
    pub time_series: TimeSeriesConfig,
    /// Tag/text index configuration
    pub tag_index: TagIndexConfig,
    /// Checkpoint interval
    pub checkpoint_interval: std::time::Duration,
    /// Max WAL size in bytes before forcing checkpoint
    pub max_wal_size: u64,
    /// Name of the record field holding searchable text.
    ///
    /// The engine stores records as opaque bytes and has no knowledge of the
    /// memory data model; `content_scan` needs one field name to match
    /// against. REM-29 will index this same field, so it is named once here
    /// rather than hardcoded in two places.
    ///
    /// Read by `StorageEngine::content_scan` (REM-88 Task 2).
    pub text_field: String,
    /// Partition used for legacy single-tenant operation and migrations until
    /// request-authored partition scopes are available.
    pub default_partition: PartitionId,
    /// Attribute schema to register. `None` disables attribute storage
    /// entirely, which keeps the engine usable by tests and tools that have
    /// no schema of their own.
    pub attr_schema: Option<crate::engine::attr::schema::AttrSchema>,
    /// Projects a stored payload's bytes into its attribute row.
    ///
    /// Used only by `StorageEngine::new`'s post-open backfill (see
    /// `backfill_attrs_if_marked`) to turn pre-existing records into rows
    /// when a directory upgrade left the `attrs-v1` marker behind. `None`
    /// leaves any pending backfill marker untouched for a future open that
    /// does supply one -- ordinary reads and writes never call this.
    pub attr_project: Option<Arc<AttrProjectFn>>,
    /// Slots a record must hold no ordering entry for. See [`AttrIndexExemptFn`].
    pub attr_index_exempt: Option<Arc<AttrIndexExemptFn>>,
}

impl std::fmt::Debug for EngineConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineConfig")
            .field("data_dir", &self.data_dir)
            .field("memtable_size", &self.memtable_size)
            .field("block_cache_size", &self.block_cache_size)
            .field("sync_writes", &self.sync_writes)
            .field("compaction", &self.compaction)
            .field("vector", &self.vector)
            .field("graph", &self.graph)
            .field("time_series", &self.time_series)
            .field("tag_index", &self.tag_index)
            .field("checkpoint_interval", &self.checkpoint_interval)
            .field("max_wal_size", &self.max_wal_size)
            .field("text_field", &self.text_field)
            .field("default_partition", &self.default_partition)
            .field("attr_schema", &self.attr_schema)
            // `dyn Fn` implements neither `Debug` nor `Clone`'s-cousin
            // introspection, so this is a presence marker, not a value.
            .field("attr_project", &self.attr_project.as_ref().map(|_| "<fn>"))
            .field(
                "attr_index_exempt",
                &self.attr_index_exempt.as_ref().map(|_| "<fn>"),
            )
            .finish()
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            memtable_size: 256 * 1024 * 1024,   // 256 MB
            block_cache_size: 64 * 1024 * 1024, // 64 MB
            sync_writes: true, // matches FileStorageConfig::default() and config/remem-server.toml
            compaction: CompactionConfig::default(),
            vector: VectorConfig::default(),
            graph: GraphIndexConfig::default(),
            time_series: TimeSeriesConfig::default(),
            tag_index: TagIndexConfig::default(),
            checkpoint_interval: std::time::Duration::from_secs(300), // 5 minutes
            max_wal_size: 1024 * 1024 * 1024,                         // 1 GB
            text_field: "content".to_string(),
            default_partition: PartitionId::default_legacy(),
            attr_schema: None,
            attr_project: None,
            attr_index_exempt: None,
        }
    }
}

/// Maximum number of immutable memtables allowed to queue for flush before
/// new writes are rejected. Without this cap, a stuck flush loop (disk
/// full, permissions) lets rotated memtables accumulate in RAM
/// indefinitely while writes keep being accepted.
const MAX_PENDING_IMMUTABLE_MEMTABLES: usize = 4;

/// The main storage engine
pub struct StorageEngine {
    /// Configuration
    config: EngineConfig,
    /// Active MemTable for writes
    memtable: Arc<RwLock<MemTable>>,
    /// The store's record-version counter. Held here, not inside the memtable
    /// alone, because it has to outlive any single memtable: a rotation builds
    /// the replacement from this handle, and startup seeds it above every
    /// version the directory already holds.
    sequence: Arc<AtomicU64>,
    /// Immutable MemTables waiting to be flushed
    immutable_memtables: Arc<RwLock<Vec<Arc<ImmutableMemTable>>>>,
    /// Write-ahead log
    wal: Arc<WalCommitCoordinator>,
    write_admission: Arc<tokio::sync::RwLock<()>>,
    write_sequence: Arc<tokio::sync::Mutex<()>>,
    #[cfg_attr(not(test), allow(dead_code))]
    checkpoint_attempts: Arc<AtomicUsize>,
    /// Compaction manager (handles SSTable levels)
    compaction: Arc<CompactionManager>,
    /// Per-partition HNSW vector indexes (optional)
    hnsw_index: Option<Arc<PartitionedHnswIndexes>>,
    /// Graph index (CSR or Kuzu, depending on feature flag)
    graph_index: Option<Arc<RwLock<GraphIndex>>>,
    /// Segmented B+Tree time-series index (optional)
    time_series_index: Option<Arc<RwLock<PartitionedTimeSeriesIndex>>>,
    /// Segmented inverted index for tags/text (optional)
    tag_index: Option<Arc<RwLock<PartitionedTagIndex>>>,
    /// Per-slot ordered attribute indexes (optional)
    attr_indexes: Option<Arc<crate::engine::attr::index::AttrIndexes>>,
    /// The registered attribute schema (optional)
    attr_schema: Option<Arc<crate::engine::attr::schema::AttrSchema>>,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
    /// Background flush task handle
    flush_handle: Mutex<Option<JoinHandle<()>>>,
    /// Background compaction task handle
    compaction_handle: Mutex<Option<JoinHandle<()>>>,
    /// Background checkpoint task handle
    checkpoint_handle: Mutex<Option<JoinHandle<()>>>,
    /// Channel to trigger flushes
    flush_tx: mpsc::Sender<()>,
    /// Counts `get()` calls whose key starts with `memory:` (payload reads),
    /// so a test can assert that `select()` decides from sidecar rows alone
    /// and never deserializes a non-matching record. Test-only.
    #[cfg(test)]
    payload_reads: Arc<std::sync::atomic::AtomicUsize>,
    /// Counts WAL append batches, so a test can assert that a path performs
    /// no durable write at all -- the acceptance criterion for taking recall
    /// tracking off the read path (REM-27). Test-only.
    #[cfg(test)]
    wal_appends: Arc<std::sync::atomic::AtomicUsize>,
    /// Counts `get_attrs()` calls (attribute-row lookups), so a test can
    /// assert that access-path selection narrows the candidate set rather
    /// than silently falling through to a full-index enumeration that would
    /// still verify every row and return an identical answer. Test-only.
    #[cfg(test)]
    attr_reads: Arc<std::sync::atomic::AtomicUsize>,
    /// Counts vector index traversals, so query tests can prove widening
    /// reuses a cached candidate window instead of repeating the same HNSW
    /// search below its configured effort floor. Test-only.
    #[cfg(test)]
    vector_searches: Arc<std::sync::atomic::AtomicUsize>,
    /// Counts filtered retrievals that stopped at their effort bound with
    /// room still left in the result target.
    ///
    /// Not test-only: this is the signal REM-103 is gated on. Filter-aware
    /// widening is the cheap answer to a selective filter and it degrades as
    /// selectivity falls, so the question "is it degrading in practice?" has
    /// to be answerable from a running deployment rather than argued from
    /// first principles.
    search_widen_cap_hits: Arc<std::sync::atomic::AtomicU64>,
}

#[cfg(test)]
static GRAPH_ADJACENCY_VISITS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Storage access bound to one authorized read scope and one write target.
#[allow(dead_code)]
pub struct PartitionBoundStorage<'a> {
    engine: &'a StorageEngine,
    read_scope: PartitionScope,
    write_target: PartitionBinding,
}

#[allow(dead_code)]
impl PartitionBoundStorage<'_> {
    pub fn read_scope(&self) -> &PartitionScope {
        &self.read_scope
    }

    pub fn write_target(&self) -> &PartitionBinding {
        &self.write_target
    }

    pub async fn get(
        &self,
        logical_key: impl AsRef<[u8]>,
    ) -> Result<Option<(PartitionedRecordKey, Bytes)>> {
        self.engine
            .get_partitioned(&self.read_scope, logical_key)
            .await
    }

    pub async fn put(
        &self,
        logical_key: impl AsRef<[u8]>,
        value: impl Into<Bytes>,
    ) -> Result<Bytes> {
        self.engine
            .put_partitioned(&self.write_target, logical_key, value)
            .await
    }

    pub async fn delete(&self, logical_key: impl AsRef<[u8]>) -> Result<Bytes> {
        self.engine
            .delete_partitioned(&self.write_target, logical_key)
            .await
    }

    pub async fn put_with_embedding(
        &self,
        logical_key: impl AsRef<[u8]>,
        value: impl Into<Bytes>,
        embedding: Option<Vec<f32>>,
    ) -> Result<Bytes> {
        self.engine
            .put_with_embedding_partitioned(&self.write_target, logical_key, value, embedding)
            .await
    }

    pub(crate) async fn vector_search(
        &self,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<VectorSearchResult>> {
        self.engine
            .vector_search_partitioned(&self.read_scope, query, k, None)
            .await
    }

    pub(crate) fn time_range_query(
        &self,
        start: u64,
        end: u64,
        limit: Option<usize>,
    ) -> Result<Vec<(u64, Bytes)>> {
        self.engine
            .time_range_query_partitioned(&self.read_scope, start, end, limit)
    }
}

impl StorageEngine {
    /// Create a new storage engine
    pub async fn new(config: EngineConfig) -> Result<Self> {
        // Create data directories
        std::fs::create_dir_all(&config.data_dir)?;
        std::fs::create_dir_all(config.data_dir.join("wal"))?;
        std::fs::create_dir_all(config.data_dir.join("sstables"))?;
        std::fs::create_dir_all(config.data_dir.join("index"))?;

        // Refuse to open a data directory this binary does not understand,
        // then close any version gap, before anything reads a file. Both must
        // happen before the tmp sweep below: refusing after mutating would
        // modify a directory we are about to reject.
        let mut format = super::format::FormatManifest::read(&config.data_dir)?;
        format.check()?;
        let had_data = super::migrations::data_dir_has_data(&config.data_dir);
        super::migrations::run(&config, &mut format)?;
        super::migrations::verify_current(&format)?;
        // After migrations, so the partition recorded is the one legacy data
        // was actually assigned to. Before anything opens, so a mismatched
        // `default_partition` is refused rather than silently presenting an
        // empty corpus.
        super::format::bind_default_partition(
            &config.data_dir,
            &mut format,
            TenantId::default_legacy().as_str(),
            config.default_partition.as_str(),
            had_data,
        )?;

        // Remove any `.tmp` files left behind by a crash mid-write in a
        // previous run (manifest, segment writer, HNSW deleted-nodes file,
        // SSTable writer -- see `tmp_sweep`). Must run before the indexes
        // and SSTables below are opened so a fresh writer never collides
        // with a leftover tmp file of the same name.
        super::tmp_sweep::sweep_orphaned_tmp_files(&config.data_dir.join("index"));
        super::tmp_sweep::sweep_orphaned_tmp_files(&config.data_dir.join("sstables"));

        // Open KV layer (cache, compaction, WAL, memtable)
        let kv = super::init::open_kv_layer(&config)?;

        // Open secondary indexes (HNSW, graph, time-series, tag)
        // Done before WAL replay so the replay can populate them.
        let idx = super::init::open_indexes(&config)?;

        // Replay WAL to recover state since the last checkpoint
        let replayed_version = super::recovery::replay_wal(
            &kv.wal_path,
            &kv.memtable,
            idx.hnsw.as_ref(),
            idx.time_series.as_ref(),
            idx.tag.as_ref(),
            idx.graph.as_ref(),
            idx.attrs.as_ref(),
            config.attr_schema.as_ref(),
        )?;

        // Seed the record-version counter above every version this directory
        // already holds, so a version issued from here on sorts after all of
        // them. Two sources cover the whole store: the persisted mark accounts
        // for everything up to the last checkpoint, and the WAL accounts for
        // everything after it.
        let highest_stored = match super::sequence::read(&config.data_dir) {
            Some(mark) => {
                tracing::debug!(mark, "read persisted record-version mark");
                mark
            }
            None => {
                // No mark: an upgrade, or one that went missing. Recover it by
                // reading the versions already stored, then persist so this
                // happens once for this directory.
                let scanned = kv.compaction.max_record_version()?;
                tracing::info!(
                    scanned,
                    "no persisted record-version mark; recovered it by scanning stored versions"
                );
                super::sequence::write(&config.data_dir, scanned)?;
                scanned
            }
        };
        let seed = highest_stored.max(replayed_version) + 1;
        // `fetch_max`, not `store`: replaying the WAL already advanced this
        // counter through `insert_with_timestamp`, and the seed must never
        // lower it.
        kv.sequence.fetch_max(seed, Ordering::SeqCst);
        tracing::info!(
            next_record_version = kv.sequence.load(Ordering::SeqCst),
            highest_stored,
            replayed_version,
            "record-version counter seeded"
        );

        let wal = Arc::new(WalCommitCoordinator::start(
            kv.wal,
            WalCommitOptions::new(config.sync_writes),
        ));
        let write_admission = Arc::new(tokio::sync::RwLock::new(()));
        let write_sequence = Arc::new(tokio::sync::Mutex::new(()));
        let checkpoint_attempts = Arc::new(AtomicUsize::new(0));
        let immutable_memtables = Arc::new(RwLock::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (flush_tx, flush_rx) = mpsc::channel(16);

        // Start background flush / compaction / checkpoint tasks
        let (flush_handle, compaction_handle, checkpoint_handle) =
            super::tasks::start_background_tasks(
                Arc::clone(&immutable_memtables),
                Arc::clone(&kv.memtable),
                Arc::clone(&kv.compaction),
                Arc::clone(&wal),
                Arc::clone(&write_admission),
                Arc::clone(&checkpoint_attempts),
                idx.hnsw.clone(),
                idx.graph.clone(),
                idx.time_series.clone(),
                idx.tag.clone(),
                idx.attrs.clone(),
                Arc::clone(&shutdown),
                config.clone(),
                flush_rx,
            );

        // Cloned before `config` moves into the struct below.
        let attr_schema = config.attr_schema.clone().map(Arc::new);

        let engine = Self {
            config,
            memtable: kv.memtable,
            sequence: kv.sequence,
            immutable_memtables,
            wal,
            write_admission,
            write_sequence,
            checkpoint_attempts,
            compaction: kv.compaction,
            hnsw_index: idx.hnsw,
            graph_index: idx.graph,
            time_series_index: idx.time_series,
            tag_index: idx.tag,
            attr_indexes: idx.attrs,
            attr_schema,
            shutdown,
            flush_handle: Mutex::new(Some(flush_handle)),
            compaction_handle: Mutex::new(Some(compaction_handle)),
            checkpoint_handle: Mutex::new(Some(checkpoint_handle)),
            flush_tx,
            #[cfg(test)]
            payload_reads: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            wal_appends: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            attr_reads: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            vector_searches: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            search_widen_cap_hits: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };

        // The `attrs-v1` migration (`migrations.rs`) only decides whether
        // existing records are owed a sidecar row and drops a marker for
        // it; the actual backfill happens here, now that the engine is
        // fully open and `get`/`time_range_query` merge the memtable,
        // SSTables and replayed WAL the normal way. It is an explicit
        // maintenance path over encoded physical keys, so partitioned records
        // keep their stored partition and legacy records are assigned by the
        // partition-layout migration/default binding. A schema-less open
        // (`attr_indexes` is `None`) leaves the marker for a future open that
        // does register one.
        engine.backfill_attrs_if_marked().await?;

        Ok(engine)
    }

    /// Backfill sidecar attribute rows for records written before the
    /// attribute store existed in this data directory.
    ///
    /// Cheap on every ordinary boot: a single `Path::exists` when the
    /// marker (`migrations::ATTR_BACKFILL_MARKER`) is absent, which is the
    /// steady-state case once a directory has been backfilled once. Only
    /// runs when a schema *and* a projection function
    /// (`EngineConfig::attr_project`) are both registered -- with either
    /// missing there is nowhere to write rows into (no schema) or no way
    /// to derive one (no projector), and the marker is left for a later
    /// open that supplies both.
    ///
    /// Idempotent and resumable: the marker is removed only after every
    /// record has been projected and its row written, so a crash mid-way
    /// simply reruns the whole backfill on the next boot. Re-writing a row
    /// that already exists is a harmless overwrite, not a duplicate.
    ///
    /// **Reaches exactly what the time index holds.** Records are found by
    /// walking that index, so a record with no timestamp entry is not
    /// backfilled — and once a schema adds a slot that background work
    /// *selects on*, a record missing from this walk is a record no sweep can
    /// ever reach again. Nothing errors; maintenance simply stops for it.
    /// Every production creation path writes the timestamp entry alongside
    /// the payload (`store_memory_core`), which is what makes this walk
    /// complete; a future write path that skips it would silently narrow this
    /// backfill too.
    async fn backfill_attrs_if_marked(&self) -> Result<()> {
        let marker = self
            .config
            .data_dir
            .join(super::migrations::ATTR_BACKFILL_MARKER);
        if !marker.exists() {
            return Ok(());
        }
        let (Some(schema), Some(indexes), Some(project)) = (
            &self.attr_schema,
            &self.attr_indexes,
            &self.config.attr_project,
        ) else {
            return Ok(());
        };

        let mut migrated = 0usize;
        let mut retired = 0usize;
        for (_ts, key) in self.maintenance_time_range_query(0, u64::MAX, None)? {
            let Some(bytes) = self.get(&key).await? else {
                continue;
            };
            let Some(row) = project(&bytes) else {
                continue;
            };
            self.put(
                crate::engine::attr::attr_key(&key),
                Bytes::from(row.encode(schema)),
            )
            .await?;
            indexes.apply_row(&key, &row)?;

            // `apply_row` indexes every slot the row carries. A record whose
            // state says it should not be reachable by one of those access
            // paths has to be taken back out -- otherwise a directory
            // upgraded into this schema would carry ordering entries for
            // records that were retired before the entry existed, and every
            // later page would pay for them.
            if let Some(exempt) = &self.config.attr_index_exempt {
                for slot in exempt(&row) {
                    indexes.retire_slot_entry(&key, slot)?;
                    retired += 1;
                }
            }
            migrated += 1;
        }

        indexes.save_if_dirty()?;
        std::fs::remove_file(&marker)?;
        tracing::info!(
            "attribute backfill: wrote {migrated} attribute row(s), \
             retired {retired} ordering entr(ies)"
        );
        Ok(())
    }

    /// Insert a key-value pair
    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let key = key.into();
        let value = value.into();
        self.write_with_retry(
            |ts| vec![WalRecord::insert(key.clone(), value.clone(), ts)],
            |memtable, ts| memtable.insert_with_timestamp(key.clone(), value.clone(), ts),
        )
        .await?;
        Ok(())
    }

    /// Insert a primary record under a single partition target.
    #[allow(dead_code)]
    pub async fn put_partitioned(
        &self,
        target: &PartitionBinding,
        logical_key: impl AsRef<[u8]>,
        value: impl Into<Bytes>,
    ) -> Result<Bytes> {
        let key = encode_record_key(target, logical_key)
            .map_err(|err| StorageError::InvalidArgument(err.to_string()))?;
        self.put(key.clone(), value).await?;
        Ok(key)
    }

    /// Delete a key
    pub async fn delete(&self, key: impl Into<Bytes>) -> Result<()> {
        let key = key.into();
        self.write_with_retry(
            |ts| vec![WalRecord::delete(key.clone(), ts)],
            |memtable, ts| memtable.delete_with_timestamp(key.clone(), ts),
        )
        .await?;
        Ok(())
    }

    /// Delete a primary record from a single partition target.
    #[allow(dead_code)]
    pub async fn delete_partitioned(
        &self,
        target: &PartitionBinding,
        logical_key: impl AsRef<[u8]>,
    ) -> Result<Bytes> {
        let key = encode_record_key(target, logical_key)
            .map_err(|err| StorageError::InvalidArgument(err.to_string()))?;
        self.delete(key.clone()).await?;
        Ok(key)
    }

    /// Get a value by key
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let key = key.as_ref();

        #[cfg(test)]
        {
            let is_memory_payload = key.starts_with(b"memory:")
                || super::partition::decode_record_key(key)
                    .ok()
                    .flatten()
                    .is_some_and(|decoded| decoded.logical_key().starts_with(b"memory:"));
            if is_memory_payload {
                self.payload_reads.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Check active MemTable
        {
            let memtable = self.memtable.read();
            if let Some(entry) = memtable.get(key) {
                return Ok(entry.value);
            }
        }

        // Check immutable MemTables (newest first)
        {
            let immutable = self.immutable_memtables.read();
            for imm in immutable.iter().rev() {
                if let Some(entry) = imm.get(key) {
                    return Ok(entry.value);
                }
            }
        }

        // Check SSTables via compaction manager
        if let Some(record) = self.compaction.get(key)? {
            if record.is_tombstone() {
                return Ok(None);
            }
            return Ok(record.value);
        }

        Ok(None)
    }

    /// Get a primary record through a non-empty authorized partition scope.
    pub async fn get_partitioned(
        &self,
        scope: &PartitionScope,
        logical_key: impl AsRef<[u8]>,
    ) -> Result<Option<(PartitionedRecordKey, Bytes)>> {
        let logical_key = logical_key.as_ref();
        for partition in scope.partitions() {
            let binding = PartitionBinding::new(scope.tenant().clone(), partition.clone());
            let key = encode_record_key(&binding, logical_key)
                .map_err(|err| StorageError::InvalidArgument(err.to_string()))?;
            if let Some(value) = self.get(&key).await? {
                let decoded = super::partition::decode_record_key(&key)
                    .map_err(|err| StorageError::InvalidArgument(err.to_string()))?
                    .ok_or_else(|| {
                        StorageError::InvalidArgument(
                            "encoded partitioned record key lost its prefix".to_string(),
                        )
                    })?;
                return Ok(Some((decoded, value)));
            }
        }
        Ok(None)
    }

    /// Rotate the active MemTable (make it immutable and create a new one)
    ///
    /// # Invariant: safe to race a writer's reserve/apply gap (Phase 3: keep this true)
    ///
    /// `put`/`delete`/`store_memory_core` reserve a timestamp via
    /// `self.memtable.read().reserve_timestamp()` *inside* the write sequencer,
    /// submit the WAL record, release the sequencer, and only then apply the
    /// insert/delete to `self.memtable` via a fresh, unlocked
    /// `self.memtable.read()`. `rotate_memtable` doesn't take the sequencer,
    /// so it can swap `self.memtable` for a brand-new, empty one in that gap
    /// -- the writer's later apply step lands in a *different* `MemTable`
    /// instance than the one that handed out its reserved timestamp.
    ///
    /// This is safe today only because `MemTable::insert_with_timestamp`
    /// (and `delete_with_timestamp`) CAS-bump the *target* memtable's own
    /// `next_timestamp` counter up to `timestamp + 1` whenever an inserted
    /// timestamp is `>=` the counter's current value -- see the "Update
    /// next_timestamp if needed" loop in `memtable.rs`. So even though the
    /// timestamp was reserved against the old memtable's counter, applying
    /// it to the new one still advances the new memtable's counter past it,
    /// and the WAL-record-order-vs-apply-order invariant (every WAL record
    /// and its corresponding memtable entry carry the same timestamp, and
    /// no later reservation can undercut an earlier one) holds across the
    /// swap.
    ///
    /// `rotate_memtable` not taking the sequencer is deliberate (unlike
    /// `checkpoint()`'s exclusive admission span); full WAL
    /// segmentation (Phase 3, see `docs/PROJECT_REVIEW.md` §8) will
    /// restructure this. Whatever replaces this function must preserve the
    /// property above: a
    /// reservation's timestamp must never be reused, and no later
    /// reservation (on any memtable) may be numerically smaller.
    async fn rotate_memtable(&self) -> Result<()> {
        if self.immutable_memtables.read().len() >= MAX_PENDING_IMMUTABLE_MEMTABLES {
            return Err(StorageError::Io(std::io::Error::other(format!(
                "too many pending memtable flushes ({} queued); rejecting write until \
                 the flush backlog drains (disk full or flush stalled?)",
                MAX_PENDING_IMMUTABLE_MEMTABLES
            ))));
        }

        let old_memtable = {
            let mut memtable = self.memtable.write();

            std::mem::replace(
                &mut *memtable,
                MemTable::with_sequence(self.config.memtable_size, Arc::clone(&self.sequence)),
            )
        };

        {
            let mut immutable = self.immutable_memtables.write();
            immutable.push(Arc::new(ImmutableMemTable::from_memtable(old_memtable)));
        }

        let _ = self.flush_tx.send(()).await;

        Ok(())
    }

    /// Reserve a timestamp under the write sequencer, submit the record(s) `build_records`
    /// produces from it, then apply `apply` to the active memtable — retrying
    /// against a freshly rotated memtable if it reports `MemTableFull`. Returns
    /// the timestamp that was ultimately committed.
    ///
    /// Shared by `put`, `delete`, `put_with_embedding`, and `store_memory_core`,
    /// which previously each hand-rolled this loop (REM-35).
    ///
    /// # Invariant: reserve-and-submit under one sequencer acquisition
    ///
    /// The timestamp reservation and the WAL append below happen under the
    /// *same* sequencer acquisition, so a concurrent writer can't have its WAL
    /// record land in one order while its memtable insert lands in another.
    /// The old, pre-refactor code peeked `current_timestamp()` here, then let
    /// `memtable.insert()` assign the *real* timestamp later, unlocked — that
    /// was a real bug (WAL order and memtable order could disagree under
    /// concurrency) that this lock scope fixes. Don't split the reserve and
    /// the submission back across two sequencer acquisitions.
    async fn write_with_retry(
        &self,
        mut build_records: impl FnMut(u64) -> Vec<WalRecord>,
        mut apply: impl FnMut(&MemTable, u64) -> std::result::Result<(), StorageError>,
    ) -> Result<u64> {
        self.ensure_accepting_writes()?;
        let _admission = self.write_admission.read().await;
        self.ensure_accepting_writes()?;
        loop {
            let (timestamp, pending) = {
                let _sequence = self.write_sequence.lock().await;
                let ts = self.memtable.read().reserve_timestamp();
                let records = build_records(ts);
                #[cfg(test)]
                self.wal_appends.fetch_add(1, Ordering::Relaxed);
                let pending = self.wal.enqueue(records).await?;
                (ts, pending)
            };
            pending.wait().await?;

            let result = {
                let memtable = self.memtable.read();
                apply(&memtable, timestamp)
            };

            match result {
                Ok(()) => {
                    if self.memtable.read().is_full() {
                        self.rotate_memtable().await?;
                    }
                    return Ok(timestamp);
                }
                Err(StorageError::MemTableFull { .. }) => {
                    // MemTable is full: rotate and retry. The retry reserves
                    // a fresh timestamp + WAL record against the new
                    // (post-rotation) memtable, so the two always agree.
                    self.rotate_memtable().await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Returns `index.as_ref()`, or `StorageError::InvalidArgument("{label} is
    /// not enabled")` if it's `None`. Replaces 23 duplicated
    /// `let Some(index) = &self.<field> else { return Err(...) }` preambles
    /// across the vector/graph/time-series/tag sections (REM-35).
    fn require_index<'a, T>(index: &'a Option<T>, label: &str) -> Result<&'a T> {
        index
            .as_ref()
            .ok_or_else(|| StorageError::InvalidArgument(format!("{label} is not enabled")))
    }

    fn ensure_accepting_writes(&self) -> Result<()> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "storage engine is shutting down",
            )));
        }
        Ok(())
    }

    /// Flush all data to disk
    #[allow(dead_code)]
    pub async fn flush(&self) -> Result<()> {
        let _admission = self.write_admission.write().await;
        self.wal.barrier().await?;
        // Rotate current memtable to immutable
        self.rotate_memtable().await?;

        // Flush all immutable memtables synchronously
        loop {
            let to_flush: Option<Arc<ImmutableMemTable>> = {
                let imm = self.immutable_memtables.read();
                imm.first().cloned()
            };

            let Some(imm_memtable) = to_flush else {
                break;
            };

            // Flush this memtable
            super::tasks::flush_memtable(&imm_memtable, &self.compaction, &self.config)?;

            // Remove from immutable list
            {
                let mut imm = self.immutable_memtables.write();
                imm.retain(|m| !Arc::ptr_eq(m, &imm_memtable));
            }

            // NOTE: We do NOT truncate WAL here anymore.
            // WAL truncation is now handled by the checkpoint process (background task).
        }

        Ok(())
    }

    /// Get storage statistics
    pub fn stats(&self) -> StorageStats {
        let memtable_size = self.memtable.read().size();
        let vector_count = self.hnsw_index.as_ref().map(|idx| idx.len()).unwrap_or(0);
        let vector_resident_bytes = self
            .hnsw_index
            .as_ref()
            .map(|idx| idx.catalog().resident_bytes())
            .unwrap_or(0);
        let vector_resident_partitions = self
            .hnsw_index
            .as_ref()
            .map(|idx| idx.catalog().resident_count())
            .unwrap_or(0);
        let vector_loads = self.hnsw_index.as_ref().map(|idx| idx.loads()).unwrap_or(0);
        let vector_evictions = self
            .hnsw_index
            .as_ref()
            .map(|idx| idx.evictions())
            .unwrap_or(0);
        let graph_node_count = self
            .graph_index
            .as_ref()
            .map(|idx| idx.read().node_count())
            .unwrap_or(0);
        let graph_edge_count = self
            .graph_index
            .as_ref()
            .map(|idx| idx.read().edge_count())
            .unwrap_or(0);
        let time_series_count = self
            .time_series_index
            .as_ref()
            .map(|idx| idx.read().len())
            .unwrap_or(0);
        let tag_doc_count = self
            .tag_index
            .as_ref()
            .map(|idx| idx.read().len())
            .unwrap_or(0);

        StorageStats {
            memtable_size,
            vector_count,
            vector_resident_bytes,
            vector_resident_partitions,
            vector_loads,
            vector_evictions,
            vector_enabled: self.hnsw_index.is_some(),
            graph_node_count,
            graph_edge_count,
            time_series_count,
            tag_doc_count,
        }
    }

    pub fn partition_vector_counts(&self) -> Vec<(PartitionBinding, usize)> {
        self.hnsw_index
            .as_ref()
            .map(|index| index.partition_counts())
            .unwrap_or_default()
    }

    // ==================== Vector Operations ====================

    pub fn default_partition_binding(&self) -> PartitionBinding {
        PartitionBinding::legacy_tenant(self.config.default_partition.clone())
    }

    pub fn default_partition_scope(&self) -> PartitionScope {
        PartitionScope::single(self.default_partition_binding())
    }

    fn key_is_visible_to_scope(&self, scope: &PartitionScope, key: &[u8]) -> Result<bool> {
        if let Some(partitioned) = super::partition::decode_record_key(key)
            .map_err(|err| StorageError::InvalidArgument(err.to_string()))?
        {
            return Ok(scope.contains_binding(partitioned.binding()));
        }

        Ok(
            key.starts_with(b"memory:")
                && scope.contains_binding(&self.default_partition_binding()),
        )
    }

    #[allow(dead_code)]
    pub fn bind_partition_scope(
        &self,
        read_scope: PartitionScope,
        write_target: PartitionBinding,
    ) -> Result<PartitionBoundStorage<'_>> {
        if read_scope.tenant() != write_target.tenant() {
            return Err(StorageError::InvalidArgument(
                "write target tenant must match read scope tenant".to_string(),
            ));
        }
        if !read_scope.contains(write_target.partition()) {
            return Err(StorageError::InvalidArgument(
                "write target partition must be included in read scope".to_string(),
            ));
        }
        Ok(PartitionBoundStorage {
            engine: self,
            read_scope,
            write_target,
        })
    }

    /// Insert a key-value pair with an optional embedding vector
    ///
    /// If an embedding is provided and vector search is enabled, the vector
    /// will be indexed in the HNSW index for similarity search.
    /// The embedding is also stored in the WAL for durability/recovery.
    pub async fn put_with_embedding(
        &self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
        embedding: Option<Vec<f32>>,
    ) -> Result<()> {
        let key = key.into();
        let value = value.into();

        self.write_with_retry(
            |ts| {
                let record = if let Some(ref emb) = embedding {
                    WalRecord::insert_with_embedding(key.clone(), value.clone(), ts, emb.clone())
                } else {
                    WalRecord::insert(key.clone(), value.clone(), ts)
                };
                vec![record]
            },
            |memtable, ts| memtable.insert_with_timestamp(key.clone(), value.clone(), ts),
        )
        .await?;

        if let (Some(embedding), Some(index)) = (embedding, &self.hnsw_index) {
            index.insert_for_key(key, embedding)?;
        }

        Ok(())
    }

    /// Insert a key-value pair and route its vector to exactly one partition.
    #[allow(dead_code)]
    pub async fn put_with_embedding_partitioned(
        &self,
        target: &PartitionBinding,
        logical_key: impl AsRef<[u8]>,
        value: impl Into<Bytes>,
        embedding: Option<Vec<f32>>,
    ) -> Result<Bytes> {
        let key = encode_record_key(target, logical_key)
            .map_err(|err| StorageError::InvalidArgument(err.to_string()))?;
        self.put_with_embedding(key.clone(), value, None).await?;
        if let Some(embedding) = embedding {
            self.insert_vector_partitioned(target, key.clone(), embedding)?;
        }
        Ok(key)
    }

    /// Insert a key-value pair with an optional embedding vector, and store
    /// its attribute row alongside it under the same WAL-lock acquisition.
    ///
    /// Mirrors `put_with_attrs` with the embedding branch of `put_with_embedding`
    /// folded in, rather than routing through `store_memory_core`: that would
    /// add a timestamp-index entry on every call. `BTreeIndex::insert` (what
    /// the time-series index's growing segment writes into) does dedupe a
    /// re-inserted key in place via its `key_to_timestamp` reverse map — but
    /// only while it is still the same, still-open growing segment. Once a
    /// `seal_growing()` boundary falls between two writes for the same key,
    /// or the key ends up split across two already-sealed chunks, that
    /// reverse map no longer spans both writes, so a second call for the
    /// same key — e.g. a content update — can leave a genuine duplicate
    /// entry rather than replacing the first, and `remove` only strips the
    /// first match it finds. This method exists to add sidecar-row
    /// maintenance to a content update without adding a second
    /// timestamp-index write and risking that duplication.
    /// Falls back to a plain `put_with_embedding` when no attribute schema is
    /// registered.
    pub async fn put_with_embedding_and_attrs(
        &self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
        embedding: Option<Vec<f32>>,
        row: &crate::engine::attr::row::AttrRow,
    ) -> Result<()> {
        let key: Bytes = key.into();
        let value: Bytes = value.into();

        let Some(schema) = &self.attr_schema else {
            return self.put_with_embedding(key, value, embedding).await;
        };
        // Encoded once, outside the WAL closure, which may run more than
        // once on a memtable-full retry.
        let encoded = Bytes::from(row.encode(schema));
        let akey = crate::engine::attr::attr_key(&key);

        self.write_with_retry(
            |ts| {
                let kv_record = if let Some(ref emb) = embedding {
                    WalRecord::insert_with_embedding(key.clone(), value.clone(), ts, emb.clone())
                } else {
                    WalRecord::insert(key.clone(), value.clone(), ts)
                };
                vec![
                    kv_record,
                    WalRecord::put_attrs(key.clone(), encoded.clone(), ts),
                ]
            },
            |memtable, ts| {
                memtable.insert_with_timestamp(key.clone(), value.clone(), ts)?;
                memtable.insert_with_timestamp(akey.clone(), encoded.clone(), ts)
            },
        )
        .await?;

        if let (Some(embedding), Some(index)) = (embedding, &self.hnsw_index) {
            index.insert_for_key(key.clone(), embedding)?;
        }

        if let Some(indexes) = &self.attr_indexes {
            indexes.apply_row(&key, row)?;
        }

        Ok(())
    }

    pub async fn put_with_embedding_and_attrs_partitioned(
        &self,
        target: &PartitionBinding,
        logical_key: impl AsRef<[u8]>,
        value: impl Into<Bytes>,
        embedding: Option<Vec<f32>>,
        row: &crate::engine::attr::row::AttrRow,
    ) -> Result<Bytes> {
        let key = encode_record_key(target, logical_key)
            .map_err(|err| StorageError::InvalidArgument(err.to_string()))?;
        self.put_with_embedding_and_attrs(key.clone(), value, embedding, row)
            .await?;
        Ok(key)
    }

    /// Store a memory atomically: KV insert + timestamp index + tag index in one
    /// WAL lock acquisition and one fsync.
    ///
    /// Reduces WAL lock acquisitions from 3 to 1 and fsyncs from 3 to 1 compared
    /// to calling `put_with_embedding` + `add_timestamp` + `add_tags` separately.
    /// The HNSW insert (CPU-bound) runs on the blocking thread pool while timestamp
    /// and tag writes (fast in-memory O(log N)) run inline.
    pub async fn store_memory_core(
        &self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
        embedding: Option<Vec<f32>>,
        timestamp: u64,
        tags: &[String],
        attrs: Option<&crate::engine::attr::row::AttrRow>,
    ) -> Result<()> {
        let key: Bytes = key.into();
        let value: Bytes = value.into();

        // Encoded once, outside the WAL closure, which may run more than once
        // on a memtable-full retry.
        let encoded_attrs = match (attrs, &self.attr_schema) {
            (Some(row), Some(schema)) => Some(Bytes::from(row.encode(schema))),
            _ => None,
        };
        // Only allocated when there is actually a row to key it for --
        // every attribute-less write (still the common case until a later
        // task wires a real caller) would otherwise pay for a sidecar key
        // it never uses.
        let akey = encoded_attrs
            .as_ref()
            .map(|_| crate::engine::attr::attr_key(&key));

        // ── Step 1+2: reserve ts under the WAL lock, apply with that ts ──────
        self.write_with_retry(
            |ts| {
                let kv_record = if let Some(ref emb) = embedding {
                    WalRecord::insert_with_embedding(key.clone(), value.clone(), ts, emb.clone())
                } else {
                    WalRecord::insert(key.clone(), value.clone(), ts)
                };
                let mut records = vec![
                    kv_record,
                    WalRecord::set_timestamp(key.clone(), timestamp, ts),
                    WalRecord::add_tags(key.clone(), tags.to_vec(), ts),
                ];
                if let Some(ref enc) = encoded_attrs {
                    records.push(WalRecord::put_attrs(key.clone(), enc.clone(), ts));
                }
                records
            },
            |memtable, ts| {
                memtable.insert_with_timestamp(key.clone(), value.clone(), ts)?;
                if let (Some(akey), Some(enc)) = (akey.as_ref(), encoded_attrs.as_ref()) {
                    memtable.insert_with_timestamp(akey.clone(), enc.clone(), ts)?;
                }
                Ok(())
            },
        )
        .await?;

        if let (Some(indexes), Some(row)) = (&self.attr_indexes, attrs) {
            indexes.apply_row(&key, row)?;
        }

        // ── Step 3: secondary indexes ─────────────────────────────────────────
        // HNSW insert is CPU-bound (ANN graph traversal); dispatch to blocking thread pool.
        // Timestamp and tag inserts are fast in-memory; run them inline first, then await HNSW.
        //
        // NOTE: time_series_index and tag_index use Arc<RwLock<T>> where T uses interior
        // mutability for its write methods (&self, not &mut self).  Acquiring a read lock is
        // correct here — a write lock would deadlock if anything else holds a read guard.
        let hnsw_task = if let (Some(emb), Some(hnsw)) = (embedding, self.hnsw_index.clone()) {
            let key = key.clone();
            Some(tokio::task::spawn_blocking(move || {
                hnsw.insert_for_key(key, emb)?;
                Ok::<_, StorageError>(())
            }))
        } else {
            None
        };

        // Run timestamp + tag inline (fast)
        if let Some(index) = &self.time_series_index {
            index.write().insert(timestamp, key.clone())?;
        }
        if let Some(index) = &self.tag_index {
            index.write().add_tags(key.clone(), tags)?;
        }

        // Await HNSW (was running concurrently on thread pool)
        if let Some(task) = hnsw_task {
            task.await
                .map_err(|e| StorageError::Io(std::io::Error::other(e)))??;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn store_memory_core_partitioned(
        &self,
        target: &PartitionBinding,
        logical_key: impl AsRef<[u8]>,
        value: impl Into<Bytes>,
        embedding: Option<Vec<f32>>,
        timestamp: u64,
        tags: &[String],
        attrs: Option<&crate::engine::attr::row::AttrRow>,
    ) -> Result<Bytes> {
        let key = encode_record_key(target, logical_key)
            .map_err(|err| StorageError::InvalidArgument(err.to_string()))?;
        self.store_memory_core(key.clone(), value, embedding, timestamp, tags, attrs)
            .await?;
        Ok(key)
    }

    /// Search for similar vectors
    ///
    /// Returns a list of (key, similarity_score) pairs sorted by similarity.
    /// The score interpretation depends on the distance metric:
    /// - L2: Lower is more similar
    /// - Cosine: Lower distance means higher similarity (cosine distance = 1 - cosine similarity)
    /// - DotProduct: Negated, so lower is higher dot product
    pub async fn vector_search(&self, query: &[f32], k: usize) -> Result<Vec<VectorSearchResult>> {
        self.vector_search_partitioned(&self.default_partition_scope(), query, k, None)
            .await
    }

    /// The minimum candidate window an ordinary vector search explores.
    pub(crate) fn vector_search_floor(&self) -> Result<usize> {
        let index = Self::require_index(&self.hnsw_index, "Vector search")?;
        Ok(index.search_floor())
    }

    /// Search for similar vectors with custom ef parameter
    ///
    /// The ef parameter controls the search quality/speed tradeoff.
    /// Higher ef = better recall but slower search.
    #[allow(dead_code)] // compatibility wrapper until QueryEngine carries partition scopes.
    pub(crate) async fn vector_search_with_ef(
        &self,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<VectorSearchResult>> {
        self.vector_search_partitioned(&self.default_partition_scope(), query, k, ef)
            .await
    }

    /// Search only HNSW instances included in the authorized partition scope.
    pub async fn vector_search_partitioned(
        &self,
        scope: &PartitionScope,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<VectorSearchResult>> {
        #[cfg(test)]
        self.vector_searches.fetch_add(1, Ordering::Relaxed);
        let index = Self::require_index(&self.hnsw_index, "Vector search")?;
        let results = index.search(scope, query, k, ef)?;

        let mut search_results = Vec::with_capacity(results.len());
        for (key, distance) in results {
            search_results.push(VectorSearchResult { key, distance });
        }

        Ok(search_results)
    }

    /// Get the number of vectors in the index
    #[allow(dead_code)]
    pub fn vector_count(&self) -> usize {
        self.hnsw_index.as_ref().map(|idx| idx.len()).unwrap_or(0)
    }

    /// Vectors held by the partitions in `scope`.
    ///
    /// The widening retry in `QueryExecutor::execute_vector_search` bounds
    /// itself on this: bounding on the *global* count makes a small
    /// partition's search retry against its own index once per doubling all
    /// the way up to the size of the whole corpus, so a tenant's query cost
    /// grows with its neighbours' data — the coupling partitioning exists to
    /// remove.
    ///
    /// The lookup itself is scope-sized for the same reason. Deriving it by
    /// listing every partition in the deployment and filtering would reinstate
    /// that coupling in the accounting even after removing it from retrieval.
    #[allow(dead_code)]
    pub fn scoped_vector_count(&self, scope: &PartitionScope) -> usize {
        self.hnsw_index
            .as_ref()
            .map(|idx| idx.scoped_len(scope))
            .unwrap_or(0)
    }

    /// Candidate slots the partitions in `scope` can yield, retired ones
    /// included.
    ///
    /// [`Self::scoped_vector_count`] reports what is *live*, which is the
    /// right figure for stats and the wrong one for deciding a search has
    /// seen everything: retiring a record leaves its slot in the graph, so a
    /// traversal still steps over it. Bounding the widening loop on the live
    /// figure makes it stop once it has asked for the live count -- which, in
    /// a corpus where most records are archived, happens long before the
    /// traversal has reached the live ones. The page comes back short and,
    /// worse, marked complete.
    pub fn scoped_vector_node_count(&self, scope: &PartitionScope) -> usize {
        self.hnsw_index
            .as_ref()
            .map(|idx| idx.scoped_node_count(scope))
            .unwrap_or(0)
    }

    /// Candidate slots the whole index can yield, retired ones included.
    pub fn vector_node_count(&self) -> usize {
        self.hnsw_index
            .as_ref()
            .map(|idx| idx.node_count())
            .unwrap_or(0)
    }

    /// Check if vector search is enabled
    pub fn vector_enabled(&self) -> bool {
        self.hnsw_index.is_some()
    }

    /// Distance metric the vector index is configured with.
    ///
    /// Callers need this to interpret the raw distances reported in search
    /// results: the same number means different things under L2, cosine and
    /// dot product (REM-74).
    pub fn vector_metric(&self) -> DistanceMetric {
        self.config.vector.metric
    }

    /// Save the HNSW index to disk
    pub fn save_vector_index(&self) -> Result<()> {
        if let Some(index) = &self.hnsw_index {
            if index.is_dirty() {
                index.save_dirty()?;
                tracing::info!("Saved partitioned HNSW indexes ({} vectors)", index.len());
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn insert_vector_partitioned(
        &self,
        target: &PartitionBinding,
        key: Bytes,
        embedding: Vec<f32>,
    ) -> Result<()> {
        if let Some(index) = &self.hnsw_index {
            index.insert(target, key, embedding)?;
        }
        Ok(())
    }

    // ==================== Graph Operations ====================

    /// Add an edge, stamped with the caller-supplied real-world creation time.
    ///
    /// `created_at_ms` is an epoch-millisecond wall-clock value (e.g. `now_ms()`),
    /// not an internal MVCC sequence number — unlike `store_memory_core`'s KV
    /// writes, edges never claim a slot in the memtable's MVCC counter (WAL
    /// replay for `AddEdge`/`RemoveEdge` only touches the graph index, never the
    /// memtable), so this value is written into the WAL record and the graph
    /// index's `EdgeMetadata.timestamp` verbatim, with no MVCC bookkeeping.
    pub async fn add_edge(
        &self,
        source: impl Into<Bytes>,
        target: impl Into<Bytes>,
        edge_type: Option<String>,
        weight: Option<f32>,
        created_at_ms: u64,
    ) -> Result<()> {
        let index = Self::require_index(&self.graph_index, "Graph index")?;

        let source: Bytes = source.into();
        let target: Bytes = target.into();

        self.ensure_accepting_writes()?;
        let _admission = self.write_admission.read().await;
        self.ensure_accepting_writes()?;
        let pending = {
            let _sequence = self.write_sequence.lock().await;
            let record = WalRecord::add_edge(
                source.clone(),
                target.clone(),
                edge_type.clone(),
                weight,
                created_at_ms,
            );
            self.wal.enqueue(vec![record]).await?
        };
        pending.wait().await?;

        // Then add to graph index
        let mut metadata = EdgeMetadata::default();
        if let Some(et) = edge_type {
            metadata = EdgeMetadata::with_type(et);
        }
        if let Some(w) = weight {
            metadata = metadata.weight(w);
        }
        metadata = metadata.timestamp(created_at_ms);

        index.read().add_edge(source, target, metadata)
    }

    /// Add multiple edges in a single WAL lock acquisition.
    ///
    /// Semantically equivalent to calling `add_edge` N times, but acquires the
    /// WAL mutex once and calls `sync` once (when `sync_writes = true`), reducing
    /// WAL serialisation from O(N) locks to O(1).
    ///
    /// `created_at_ms` is a single epoch-millisecond wall-clock value shared by
    /// every edge in the batch — correct, since all edges in one atomic batch
    /// share one creation instant (see `add_edge`'s doc comment for why this is
    /// a real timestamp rather than an MVCC sequence number).
    pub async fn add_edges_batch(
        &self,
        edges: Vec<(Bytes, Bytes, Option<String>, Option<f32>)>,
        created_at_ms: u64,
    ) -> Result<()> {
        let index = Self::require_index(&self.graph_index, "Graph index")?;

        if edges.is_empty() {
            return Ok(());
        }

        self.ensure_accepting_writes()?;
        let _admission = self.write_admission.read().await;
        self.ensure_accepting_writes()?;
        let pending = {
            let _sequence = self.write_sequence.lock().await;
            let records: Vec<WalRecord> = edges
                .iter()
                .map(|(src, dst, et, w)| {
                    WalRecord::add_edge(src.clone(), dst.clone(), et.clone(), *w, created_at_ms)
                })
                .collect();
            self.wal.enqueue(records).await?
        };
        pending.wait().await?;

        // Apply all edges to graph index
        let guard = index.read();
        for (src, dst, et, w) in edges {
            let mut metadata = EdgeMetadata::default();
            if let Some(et) = et {
                metadata = EdgeMetadata::with_type(et);
            }
            if let Some(w) = w {
                metadata = metadata.weight(w);
            }
            metadata = metadata.timestamp(created_at_ms);
            guard.add_edge(src, dst, metadata)?;
        }

        Ok(())
    }

    /// Remove all edges from `source` to `target` in the graph index.
    ///
    /// Writes to WAL first for durability, then removes the edge from the
    /// graph index. Logs at debug level if no matching edge existed.
    pub async fn remove_edge(
        &self,
        source: impl Into<Bytes>,
        target: impl Into<Bytes>,
    ) -> Result<()> {
        let index = Self::require_index(&self.graph_index, "Graph index")?;

        let source: Bytes = source.into();
        let target: Bytes = target.into();

        self.ensure_accepting_writes()?;
        let _admission = self.write_admission.read().await;
        self.ensure_accepting_writes()?;
        let pending = {
            let _sequence = self.write_sequence.lock().await;
            let ts = self.memtable.read().current_timestamp();
            self.wal
                .enqueue(vec![WalRecord::remove_edge(
                    source.clone(),
                    target.clone(),
                    ts,
                )])
                .await?
        };
        pending.wait().await?;

        let removed = index.read().remove_edge(&source, &target)?;

        if !removed {
            tracing::debug!(
                "remove_edge: no edge found from {:?} to {:?}",
                source,
                target
            );
        }

        Ok(())
    }

    /// Get neighbors of a node
    pub fn get_neighbors(&self, node: &[u8]) -> Result<Vec<(Bytes, String, f32, u64)>> {
        let index = Self::require_index(&self.graph_index, "Graph index")?;

        let neighbors = index.read().get_neighbors(node)?;
        #[cfg(test)]
        GRAPH_ADJACENCY_VISITS.fetch_add(neighbors.len(), Ordering::Relaxed);
        Ok(neighbors
            .into_iter()
            .map(|(key, meta)| (key, meta.edge_type, meta.weight, meta.timestamp))
            .collect())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn reset_graph_adjacency_visit_count() {
        GRAPH_ADJACENCY_VISITS.store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn graph_adjacency_visit_count() -> usize {
        GRAPH_ADJACENCY_VISITS.load(Ordering::Relaxed)
    }

    pub fn maintenance_get_neighbors(&self, node: &[u8]) -> Result<Vec<(Bytes, String, f32, u64)>> {
        self.get_neighbors(node)
    }

    pub fn get_neighbors_partitioned(
        &self,
        scope: &PartitionScope,
        node: &[u8],
    ) -> Result<Vec<(Bytes, String, f32, u64)>> {
        if !self.key_is_visible_to_scope(scope, node)? {
            return Ok(Vec::new());
        }

        self.get_neighbors(node)?
            .into_iter()
            .filter_map(|(key, rel, weight, ts)| {
                match self.key_is_visible_to_scope(scope, key.as_ref()) {
                    Ok(true) => Some(Ok((key, rel, weight, ts))),
                    Ok(false) => None,
                    Err(err) => Some(Err(err)),
                }
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Traverse the graph using BFS
    pub fn traverse_graph(
        &self,
        start: &[u8],
        max_depth: usize,
        edge_types: Option<&[String]>,
    ) -> Result<Vec<TraversalResult>> {
        let index = Self::require_index(&self.graph_index, "Graph index")?;

        if let Some(edge_types) = edge_types {
            index
                .read()
                .traverse_bfs_with_type(start, max_depth, edge_types)
        } else {
            index.read().traverse_bfs(start, max_depth)
        }
    }

    pub fn traverse_graph_partitioned(
        &self,
        scope: &PartitionScope,
        start: &[u8],
        max_depth: usize,
        edge_types: Option<&[String]>,
    ) -> Result<Vec<TraversalResult>> {
        if !self.key_is_visible_to_scope(scope, start)? {
            return Ok(Vec::new());
        }

        let allowed_types =
            edge_types.map(|types| types.iter().map(String::as_str).collect::<HashSet<_>>());
        let mut seen = HashSet::new();
        let mut queue = VecDeque::new();
        let start = Bytes::copy_from_slice(start);
        let mut out = vec![TraversalResult {
            node_id: start.clone(),
            depth: 0,
            edge_metadata: None,
        }];

        seen.insert(start.clone());
        queue.push_back((start, 0usize));

        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            for (target, rel, weight, ts) in self.get_neighbors_partitioned(scope, node.as_ref())? {
                if let Some(allowed) = &allowed_types {
                    if !allowed.contains(rel.as_str()) {
                        continue;
                    }
                }
                if !seen.insert(target.clone()) {
                    continue;
                }

                let metadata = EdgeMetadata::with_type(rel).weight(weight).timestamp(ts);
                out.push(TraversalResult {
                    node_id: target.clone(),
                    depth: depth + 1,
                    edge_metadata: Some(metadata),
                });
                queue.push_back((target, depth + 1));
            }
        }

        Ok(out)
    }

    /// Check if the graph index is enabled
    pub fn graph_enabled(&self) -> bool {
        self.graph_index.is_some()
    }

    /// Save the graph index to disk
    pub fn save_graph_index(&self) -> Result<()> {
        if let Some(index) = &self.graph_index {
            let is_dirty = index.read().is_dirty();
            if is_dirty {
                let node_count = index.read().node_count();
                let edge_count = index.read().edge_count();
                index.write().save_if_dirty()?;
                tracing::info!(
                    "Saved graph index ({} nodes, {} edges)",
                    node_count,
                    edge_count
                );
            }
        }
        Ok(())
    }

    // ==================== Time-Series Operations ====================

    /// Add a timestamp for a key
    ///
    /// Test-only: used by fixture helpers in `services::connection_manager::tests`
    /// and `services::lifecycle_manager::tests` to backdate a stored memory's
    /// time-index entry for decay/lifecycle tests.
    #[cfg(test)]
    pub async fn add_timestamp(&self, key: impl Into<Bytes>, timestamp: u64) -> Result<()> {
        let index = Self::require_index(&self.time_series_index, "Time-series index")?;

        let key = key.into();

        // Write to WAL first for durability
        self.ensure_accepting_writes()?;
        let _admission = self.write_admission.read().await;
        self.ensure_accepting_writes()?;
        let pending = {
            let _sequence = self.write_sequence.lock().await;
            let ts = self.memtable.read().current_timestamp();
            self.wal
                .enqueue(vec![WalRecord::set_timestamp(key.clone(), timestamp, ts)])
                .await?
        };
        pending.wait().await?;

        index.write().insert(timestamp, key)
    }

    /// Query a range of timestamps
    pub fn time_range_query(
        &self,
        start: u64,
        end: u64,
        limit: Option<usize>,
    ) -> Result<Vec<(u64, Bytes)>> {
        let index = Self::require_index(&self.time_series_index, "Time-series index")?;

        if let Some(limit) = limit {
            Ok(index.read().range_limit(start, end, limit))
        } else {
            Ok(index.read().range(start, end))
        }
    }

    pub fn maintenance_time_range_query(
        &self,
        start: u64,
        end: u64,
        limit: Option<usize>,
    ) -> Result<Vec<(u64, Bytes)>> {
        self.time_range_query(start, end, limit)
    }

    pub fn time_range_query_partitioned(
        &self,
        scope: &PartitionScope,
        start: u64,
        end: u64,
        limit: Option<usize>,
    ) -> Result<Vec<(u64, Bytes)>> {
        let index = Self::require_index(&self.time_series_index, "Time-series index")?;
        Ok(index.read().range_partitioned(scope, start, end, limit))
    }

    /// Remove a key from all mutable indexes: time-series, tag, and graph edges
    /// (both outgoing and incoming). Does not touch the KV store — call `delete()`
    /// for that. Returns Ok(()) even if the key was not present in any index.
    pub async fn remove_from_indexes(&self, key: &[u8]) -> Result<()> {
        // Write the WAL record(s) FIRST, matching every other write path
        // (put/add_edge/add_edges_batch). The previous order mutated the
        // in-memory indexes before logging, so a crash in between left an
        // index mutation that was never durably recorded.
        //
        // Graph edges: we don't know which edges exist until we look, so we
        // peek them read-only *before* taking the WAL lock. This is a
        // separate, coarse-grained lock (`adjacency: RwLock<Vec<..>>` in
        // graph.rs) from the WAL lock — taking it here would either force a
        // lock-ordering rule between the two locks or hold the graph lock
        // across WAL I/O (fsync), which we don't want. The peek->WAL->remove
        // split does open a window where a concurrent `add_edge` or
        // `remove_edge` on this node can interleave: a concurrent add is
        // simply left alone (not part of this removal, correct), and a
        // concurrent remove of the same edge makes our later `remove_edge`
        // call a harmless no-op (idempotent). No corruption path exists, so
        // this is an accepted tradeoff to keep WAL fsync out of the graph
        // lock's critical section.
        let peeked_edges = if let Some(index) = &self.graph_index {
            index.read().peek_node_edges(key)?
        } else {
            Vec::new()
        };

        // Reserve the timestamp and build+append the WAL records inside the
        // same WAL-lock critical section, so the WAL's on-disk order for
        // this key matches the order the timestamp implies — see
        // `MemTable::reserve_timestamp`'s contract.
        self.ensure_accepting_writes()?;
        let _admission = self.write_admission.read().await;
        self.ensure_accepting_writes()?;
        let pending = {
            let _sequence = self.write_sequence.lock().await;
            let mut wal_records: Vec<WalRecord> = Vec::new();
            let ts = self.memtable.read().reserve_timestamp();

            if self.hnsw_index.is_some() {
                wal_records.push(WalRecord::remove_vector(Bytes::copy_from_slice(key), ts));
            }
            if self.time_series_index.is_some() {
                wal_records.push(WalRecord::remove_timestamp(Bytes::copy_from_slice(key), ts));
            }
            if self.tag_index.is_some() {
                wal_records.push(WalRecord::remove_tags(Bytes::copy_from_slice(key), ts));
            }
            for (src, dst) in &peeked_edges {
                wal_records.push(WalRecord::remove_edge(src.clone(), dst.clone(), ts));
            }

            if wal_records.is_empty() {
                None
            } else {
                Some(self.wal.enqueue(wal_records).await?)
            }
        };
        if let Some(pending) = pending {
            pending.wait().await?;
        }

        // Now apply the mutations the WAL already durably recorded.
        if let Some(index) = &self.hnsw_index {
            index.remove(key)?;
        }
        if let Some(index) = &self.time_series_index {
            let _ = index.write().remove(key);
        }
        if let Some(index) = &self.tag_index {
            let _ = index.write().remove(key);
        }
        if let Some(index) = &self.graph_index {
            for (src, dst) in &peeked_edges {
                let _ = index.read().remove_edge(src, dst);
            }
        }

        Ok(())
    }

    /// Retire a key's vector from the similarity index, leaving every other
    /// index entry in place.
    ///
    /// [`Self::remove_from_indexes`] is the wrong tool for a caller that must
    /// keep the record findable: it also drops the timestamp entry, the tags
    /// and the graph edges. Archiving needs exactly the opposite split -- the
    /// record stops being a search candidate, but `cleanup_archived` still
    /// finds it afterwards by walking the time index. Clearing those entries
    /// on archive would strand every archived record permanently.
    ///
    /// WAL first, then apply, matching `remove_from_indexes` and every other
    /// write path: a crash in between replays the record on recovery, so the
    /// retirement survives a restart that precedes the next checkpoint.
    ///
    /// Returns `Ok(())` when there is nothing to retire -- no similarity index
    /// is configured, the key has no vector, or it was already retired -- so a
    /// caller may issue this unconditionally without probing first.
    pub async fn retire_vector(&self, key: &[u8]) -> Result<()> {
        let Some(index) = &self.hnsw_index else {
            return Ok(());
        };

        // Reserve the timestamp and append inside the same WAL-lock critical
        // section, so the WAL's on-disk order for this key matches the order
        // the timestamp implies -- see `MemTable::reserve_timestamp`.
        self.ensure_accepting_writes()?;
        let _admission = self.write_admission.read().await;
        self.ensure_accepting_writes()?;
        let pending = {
            let _sequence = self.write_sequence.lock().await;
            let ts = self.memtable.read().reserve_timestamp();
            self.wal
                .enqueue(vec![WalRecord::remove_vector(
                    Bytes::copy_from_slice(key),
                    ts,
                )])
                .await?
        };
        pending.wait().await?;

        // `false` means the key carried no vector, or was already retired.
        // Both are the idempotent case this method promises to tolerate, not
        // an error -- and the index's own removal only counts a tombstone
        // that it actually inserted, so a repeat cannot skew the live count.
        index.remove(key)?;
        Ok(())
    }

    /// Retire a record's entry from one attribute ordering index, leaving the
    /// record, its attribute row, and its other index entries in place.
    ///
    /// The ordering counterpart to [`Self::retire_vector`], and for the same
    /// reason: a record can need to leave one access path while staying
    /// reachable by another. Archiving takes a memory out of the browsing
    /// order — so a listing page costs the live records it returns rather than
    /// every record ever archived beside them — while cleanup must still find
    /// it later. `remove_from_indexes` would take away both.
    ///
    /// The row is deliberately untouched. It is what a walk verifies a
    /// candidate against, and what the sweeps settle "nothing to do here"
    /// from without reading content; clearing it would trade one full scan
    /// for another.
    ///
    /// WAL first, then apply, like every other index mutation, so a crash
    /// between the two replays the retirement rather than losing it.
    ///
    /// Returns `Ok(())` when there is nothing to retire — no attribute indexes
    /// are configured, the slot carries no index, the record has no entry, or
    /// it was already retired — so a caller may issue it unconditionally.
    pub async fn retire_ordering_entry(&self, key: &[u8], slot: u16) -> Result<()> {
        let Some(indexes) = &self.attr_indexes else {
            return Ok(());
        };

        // Reserve the timestamp and append inside one WAL-lock critical
        // section, so the WAL's on-disk order for this key matches the order
        // its timestamp implies -- same rule as `retire_vector`.
        self.ensure_accepting_writes()?;
        let _admission = self.write_admission.read().await;
        self.ensure_accepting_writes()?;
        let pending = {
            let _sequence = self.write_sequence.lock().await;
            let ts = self.memtable.read().reserve_timestamp();
            self.wal
                .enqueue(vec![WalRecord::remove_attr_index_entry(
                    Bytes::copy_from_slice(key),
                    slot,
                    ts,
                )])
                .await?
        };
        pending.wait().await?;

        indexes.retire_slot_entry(key, slot)
    }

    /// Remove all graph edges (outgoing and incoming) for a key and WAL-record them.
    /// Does not touch KV, time-series, or tag indexes.
    /// Used when a memory's content changes and all connections must be refreshed.
    pub async fn remove_all_edges(&self, key: &[u8]) -> Result<()> {
        let Some(index) = &self.graph_index else {
            return Ok(());
        };

        // Peek edges read-only before the WAL lock — see the comment in
        // `remove_from_indexes` for why the graph lock isn't held across
        // WAL I/O and why the resulting peek/remove split is safe.
        let edges = index.read().peek_node_edges(key)?;
        if edges.is_empty() {
            return Ok(());
        }

        self.ensure_accepting_writes()?;
        let _admission = self.write_admission.read().await;
        self.ensure_accepting_writes()?;
        let pending = {
            let _sequence = self.write_sequence.lock().await;
            let ts = self.memtable.read().reserve_timestamp();
            let wal_records: Vec<WalRecord> = edges
                .iter()
                .map(|(src, dst)| WalRecord::remove_edge(src.clone(), dst.clone(), ts))
                .collect();
            self.wal.enqueue(wal_records).await?
        };
        pending.wait().await?;

        for (src, dst) in &edges {
            let _ = index.read().remove_edge(src, dst);
        }

        Ok(())
    }

    /// Return the stored embedding vector for a key, or None if not present or deleted.
    pub fn get_vector(&self, key: &[u8]) -> Option<Vec<f32>> {
        self.hnsw_index.as_ref()?.get_vector_by_key(key)
    }

    /// Save the time-series index to disk
    pub fn save_time_series_index(&self) -> Result<()> {
        if let Some(index) = &self.time_series_index {
            let is_dirty = index.read().is_dirty();
            if is_dirty {
                let len = index.read().len();
                index.write().save_if_dirty()?;
                tracing::info!("Saved time-series index ({} entries)", len);
            }
        }
        Ok(())
    }

    // ==================== Tag/Text Operations ====================

    /// Add tags to a document
    pub async fn add_tags(&self, key: impl Into<Bytes>, tags: &[String]) -> Result<()> {
        let index = Self::require_index(&self.tag_index, "Tag index")?;

        let key = key.into();

        // Write to WAL first for durability
        self.ensure_accepting_writes()?;
        let _admission = self.write_admission.read().await;
        self.ensure_accepting_writes()?;
        let pending = {
            let _sequence = self.write_sequence.lock().await;
            let ts = self.memtable.read().current_timestamp();
            self.wal
                .enqueue(vec![WalRecord::add_tags(key.clone(), tags.to_vec(), ts)])
                .await?
        };
        pending.wait().await?;

        index.write().add_tags(key, tags)
    }

    /// Replace all tags for a document (removes old tags, then sets new ones)
    pub async fn set_tags(&self, key: impl Into<Bytes>, tags: &[String]) -> Result<()> {
        let index = Self::require_index(&self.tag_index, "Tag index")?;

        let key = key.into();

        // Write to WAL first for durability
        self.ensure_accepting_writes()?;
        let _admission = self.write_admission.read().await;
        self.ensure_accepting_writes()?;
        let pending = {
            let _sequence = self.write_sequence.lock().await;
            let ts = self.memtable.read().current_timestamp();
            self.wal
                .enqueue(vec![WalRecord::set_tags(key.clone(), tags.to_vec(), ts)])
                .await?
        };
        pending.wait().await?;

        index.write().set_tags(key, tags)
    }

    // The next seven methods are the engine-level surface for attribute
    // storage (REM-75). `put_with_attrs`, `get_attrs` and
    // `delete_with_attrs` are wired into live call sites in
    // `services/repository.rs` and `services/memory_manager.rs`, and
    // `select_ordered` backs `MemoryManager::list` (REM-96); `select`'s
    // unordered form is still test-only, because search reaches predicates
    // through `QueryEngine` rather than by enumerating an attribute index
    // (REM-78, design D1).
    /// The registered attribute schema, if any.
    pub fn attr_schema(&self) -> Option<&crate::engine::attr::schema::AttrSchema> {
        self.attr_schema.as_deref()
    }

    /// The registered payload-to-row projector, if any.
    ///
    /// Read by `QueryEngine` to settle predicates in the one configuration
    /// that has a projector but no schema: without a schema there are no
    /// stored rows to read, but a payload can still be projected on the fly.
    /// That is slower than reading a row and is not the path any production
    /// deployment takes — `main.rs` registers both — but it is what keeps a
    /// filtered search *correct* rather than unfiltered when attribute
    /// storage is switched off (REM-78, design D3).
    pub fn attr_project(&self) -> Option<&Arc<AttrProjectFn>> {
        self.config.attr_project.as_ref()
    }

    /// Per-slot ordered attribute indexes, if any.
    ///
    /// Only exercised by `#[cfg(test)]` code (this file's own test module) —
    /// unused in a non-test build, hence the lint allowance, same reasoning
    /// as `attr_schema()` above.
    #[allow(dead_code)]
    pub fn attr_indexes(&self) -> Option<&Arc<crate::engine::attr::index::AttrIndexes>> {
        self.attr_indexes.as_ref()
    }

    /// Store a payload and its attribute row.
    ///
    /// Both records are written under one WAL-lock acquisition and covered
    /// by one `sync()` call (when `sync_writes` is on), so no other writer's
    /// record can land between them and each record is independently
    /// CRC-checked on replay. That is not the same as a single atomic
    /// commit: `append_batch` writes the two records sequentially into the
    /// WAL's `BufWriter`, with no batch marker, so a crash or power loss
    /// between them (mid-flush or mid-fsync) can still leave a torn tail —
    /// replay's torn-tail handling truncates at the last valid record,
    /// which can strand a payload with no sidecar row. This exposure is
    /// pre-existing and applies to every multi-record WAL batch in this
    /// engine, not something specific to attribute rows.
    /// Falls back to a plain `put` when no attribute schema is registered.
    pub async fn put_with_attrs(
        &self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
        row: &crate::engine::attr::row::AttrRow,
    ) -> Result<()> {
        let key: Bytes = key.into();
        let value: Bytes = value.into();

        let Some(schema) = &self.attr_schema else {
            return self.put(key, value).await;
        };
        // Encoded once, outside the WAL closure, which may run more than
        // once on a memtable-full retry.
        let encoded = Bytes::from(row.encode(schema));
        let akey = crate::engine::attr::attr_key(&key);

        self.write_with_retry(
            |ts| {
                vec![
                    WalRecord::insert(key.clone(), value.clone(), ts),
                    WalRecord::put_attrs(key.clone(), encoded.clone(), ts),
                ]
            },
            |memtable, ts| {
                memtable.insert_with_timestamp(key.clone(), value.clone(), ts)?;
                memtable.insert_with_timestamp(akey.clone(), encoded.clone(), ts)
            },
        )
        .await?;

        if let Some(indexes) = &self.attr_indexes {
            indexes.apply_row(&key, row)?;
        }
        Ok(())
    }

    pub async fn put_with_attrs_partitioned(
        &self,
        target: &PartitionBinding,
        logical_key: impl AsRef<[u8]>,
        value: impl Into<Bytes>,
        row: &crate::engine::attr::row::AttrRow,
    ) -> Result<Bytes> {
        let key = encode_record_key(target, logical_key)
            .map_err(|err| StorageError::InvalidArgument(err.to_string()))?;
        self.put_with_attrs(key.clone(), value, row).await?;
        Ok(key)
    }

    /// Read a record's attribute row without touching its payload.
    ///
    /// Returns `None` when no schema is registered or the record has no row.
    pub async fn get_attrs(&self, key: &[u8]) -> Result<Option<crate::engine::attr::row::AttrRow>> {
        #[cfg(test)]
        self.attr_reads.fetch_add(1, Ordering::Relaxed);

        let Some(schema) = &self.attr_schema else {
            return Ok(None);
        };
        let Some(bytes) = self.get(crate::engine::attr::attr_key(key)).await? else {
            return Ok(None);
        };
        Ok(Some(crate::engine::attr::row::AttrRow::decode(
            &bytes, schema,
        )?))
    }

    /// Number of payload (`memory:`-keyed) reads since the engine was
    /// created or last reset. Test-only instrument backing the acceptance
    /// criterion that `select()` never deserializes a non-matching record.
    #[cfg(test)]
    pub fn payload_reads(&self) -> usize {
        self.payload_reads.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn reset_payload_reads(&self) {
        self.payload_reads.store(0, Ordering::Relaxed);
    }

    /// Number of WAL append batches since the engine was created or last
    /// reset. Test-only instrument backing the acceptance criterion that a
    /// read performs no durable write.
    #[cfg(test)]
    pub fn wal_appends(&self) -> usize {
        self.wal_appends.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn reset_wal_appends(&self) {
        self.wal_appends.store(0, Ordering::Relaxed);
    }

    /// Record that a filtered retrieval gave up at its effort bound.
    pub fn note_search_widen_cap_hit(&self) {
        self.search_widen_cap_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// How many filtered retrievals have stopped at their effort bound.
    ///
    /// Read by the business build's Prometheus gauge and by tests; the
    /// community build records the number but has nowhere to publish it.
    #[cfg_attr(not(feature = "business"), allow(dead_code))]
    pub fn search_widen_cap_hits(&self) -> u64 {
        self.search_widen_cap_hits.load(Ordering::Relaxed)
    }

    /// Number of `get_attrs()` calls (attribute-row lookups) since the
    /// engine was created or last reset. Test-only instrument for
    /// discriminating access-path selection (tier 2) from a full-index
    /// enumeration fallback (tier 3) that would return the same answer at a
    /// higher row-lookup cost.
    #[cfg(test)]
    pub fn attr_reads(&self) -> usize {
        self.attr_reads.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn reset_attr_reads(&self) {
        self.attr_reads.store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub fn vector_searches(&self) -> usize {
        self.vector_searches.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn reset_vector_searches(&self) {
        self.vector_searches.store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub fn reset_vector_distance_evaluations(&self) {
        if let Some(index) = &self.hnsw_index {
            index.reset_distance_evaluations();
        }
    }

    #[cfg(test)]
    pub fn vector_distance_evaluations(&self) -> usize {
        self.hnsw_index
            .as_ref()
            .map_or(0, |index| index.distance_evaluations())
    }

    /// Seal the tag index's growing segment.
    ///
    /// Sealing is otherwise reached only at `TAGS_CHUNK_SIZE` documents,
    /// which is too large to stage in a unit test. A sealed segment is the
    /// state in which a replaced tag posting survives its replacement, so
    /// this is what lets a caller's tag verification be tested at all.
    #[cfg(test)]
    pub fn seal_tag_index_for_test(&self) -> Result<()> {
        if let Some(index) = &self.tag_index {
            index.write().seal_growing()?;
        }
        Ok(())
    }

    /// Select record keys whose attributes satisfy every predicate.
    ///
    /// Predicates are ANDed. Returns keys, never payloads: a non-matching
    /// record is never deserialized, which is the point of the sidecar row.
    ///
    /// Access path: the first `Range` predicate on an indexed slot; failing
    /// that, the first predicate on any indexed slot; failing that, the
    /// first indexed slot's full range, which is a total index over every
    /// record and so doubles as a key enumerator. Choosing between paths by
    /// selectivity is REM-84's job — this always takes the first eligible
    /// one. Every candidate the chosen path produces is verified against its
    /// row before being returned, so a stale index entry costs a wasted
    /// lookup rather than a wrong answer, and a slot with no index at all
    /// (like `archived` above) is still evaluated for free during that same
    /// verification. Nothing here compensates for a *missing* index entry —
    /// a record that was never indexed simply never becomes a candidate.
    ///
    /// Returns `Ok(vec![])` when attribute support is off entirely (no
    /// schema or no indexes registered). Returns `Err` when attribute
    /// support is on but the registered schema has no indexed slot at all,
    /// so there is no access path left to enumerate candidates through — an
    /// empty result there would misrepresent "cannot be answered" as "no
    /// matches".
    #[allow(dead_code)]
    pub async fn select(
        &self,
        preds: &[crate::engine::attr::select::AttrPred],
        limit: Option<usize>,
    ) -> Result<Vec<Bytes>> {
        self.select_inner(preds, None, limit, None).await
    }

    #[allow(dead_code)]
    pub async fn select_partitioned(
        &self,
        scope: &PartitionScope,
        preds: &[crate::engine::attr::select::AttrPred],
        limit: Option<usize>,
    ) -> Result<Vec<Bytes>> {
        self.select_inner(preds, None, limit, Some(scope)).await
    }

    /// Select record keys satisfying every predicate, ascending by
    /// `order_slot`'s value.
    ///
    /// The caller names the access path because the output order is part of
    /// the contract: only `order_slot`'s index can deliver it, so no cost
    /// model would be free to substitute another (REM-84 chooses among paths
    /// that are interchangeable; this one is not). A predicate on
    /// `order_slot` narrows the walk; otherwise the slot's full range doubles
    /// as an ordered enumerator over every record.
    ///
    /// Ordered mode verifies each candidate's *position* as well as its
    /// values: an entry whose order key disagrees with its row was
    /// superseded, and honouring it would sort a live record by a value it no
    /// longer holds. That entry is skipped, not the record — the record's
    /// current entry sits elsewhere in the same walk.
    ///
    /// Returns `Err` when `order_slot` has no ordered index, including when
    /// attribute support is off entirely. Unlike `select`, there is no
    /// alternative path to fall back to, and an empty result would
    /// misrepresent "cannot be answered" as "no matches".
    #[allow(dead_code)]
    pub async fn select_ordered(
        &self,
        preds: &[crate::engine::attr::select::AttrPred],
        order_slot: u16,
        limit: Option<usize>,
    ) -> Result<Vec<Bytes>> {
        self.select_inner(preds, Some(order_slot), limit, None)
            .await
    }

    pub async fn select_ordered_partitioned(
        &self,
        scope: &PartitionScope,
        preds: &[crate::engine::attr::select::AttrPred],
        order_slot: u16,
        limit: Option<usize>,
    ) -> Result<Vec<Bytes>> {
        self.select_inner(preds, Some(order_slot), limit, Some(scope))
            .await
    }

    async fn select_inner(
        &self,
        preds: &[crate::engine::attr::select::AttrPred],
        order_slot: Option<u16>,
        limit: Option<usize>,
        scope: Option<&PartitionScope>,
    ) -> Result<Vec<Bytes>> {
        use crate::engine::attr::select::{matches, AttrPred};

        // Matches content_scan: `get` resolves on first poll, so without an
        // explicit yield a large walk holds a worker thread for its duration.
        const YIELD_INTERVAL: usize = 1000;

        if let Some(slot) = order_slot {
            let indexed = self
                .attr_indexes
                .as_ref()
                .is_some_and(|indexes| indexes.has_slot(slot));
            if !indexed {
                return Err(StorageError::InvalidArgument(format!(
                    "select_ordered: slot {slot} has no ordered index to walk"
                )));
            }
        }

        let Some(indexes) = &self.attr_indexes else {
            return Ok(Vec::new());
        };
        let Some(schema) = &self.attr_schema else {
            return Ok(Vec::new());
        };

        let range = |slot: u16, lo: u64, hi: u64| {
            scope
                .map_or_else(
                    || indexes.range(slot, lo, hi),
                    |scope| indexes.range_partitioned(scope, slot, lo, hi, limit),
                )
                .unwrap_or_default()
        };

        let candidates = match order_slot {
            Some(slot) => {
                let (lo, hi) = preds
                    .iter()
                    .find(|p| p.slot() == slot)
                    .map_or((0, u64::MAX), |p| p.order_bounds());
                range(slot, lo, hi)
            }
            None => {
                let path = preds
                    .iter()
                    .find(|p| matches!(p, AttrPred::Range(..)) && indexes.has_slot(p.slot()))
                    .or_else(|| preds.iter().find(|p| indexes.has_slot(p.slot())));

                match path {
                    Some(pred) => {
                        let (lo, hi) = pred.order_bounds();
                        range(pred.slot(), lo, hi)
                    }
                    None => {
                        let Some(first) = schema.indexed_slots().next() else {
                            // Attribute support is on (schema + indexes both
                            // exist), but the registered schema indexes
                            // nothing, so there is no access path left to
                            // enumerate candidates through — unlike "no
                            // schema" / "no indexes" (attribute support
                            // switched off entirely), this is a query that
                            // cannot be answered, and an empty result would
                            // silently claim "no matches" when the true
                            // answer is "unknowable".
                            return Err(StorageError::InvalidArgument(
                                "select: schema has no indexed slot to enumerate candidates through"
                                    .to_string(),
                            ));
                        };
                        range(first.slot, 0, u64::MAX)
                    }
                }
            }
        };
        let mut candidates = candidates;
        candidates.sort();

        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for (i, (order, key)) in candidates.into_iter().enumerate() {
            if i > 0 && i % YIELD_INTERVAL == 0 {
                tokio::task::yield_now().await;
            }
            // Membership is checked here but recorded only once a candidate
            // is accepted: a skipped entry must not suppress a later, live
            // entry for the same key.
            if seen.contains(&key) {
                continue;
            }
            if let Some(scope) = scope {
                if !self.key_is_visible_to_scope(scope, key.as_ref())? {
                    continue;
                }
            }
            // The row is truth: a candidate whose row is gone was deleted,
            // and one whose values moved no longer matches.
            let Some(row) = self.get_attrs(key.as_ref()).await? else {
                continue;
            };
            if let Some(slot) = order_slot {
                if row.get(slot).map(|v| v.order_key()) != Some(order) {
                    continue;
                }
            }
            if !matches(&row, preds) {
                continue;
            }
            seen.insert(key.clone());
            out.push(key);
            if let Some(n) = limit {
                if out.len() >= n {
                    break;
                }
            }
        }

        Ok(out)
    }

    /// One bounded page of an ordered selection, resumable from a position.
    ///
    /// The unbounded `select_ordered_partitioned` above answers "every key
    /// matching these predicates in this order". This answers "the next `limit`
    /// of them after this position, and whether there are more" — which is the
    /// question a caller serving one page actually has, and the only one whose
    /// cost is bounded by the answer rather than by the corpus.
    ///
    /// Why this cannot be `limit` passed to the index walk alone: a candidate
    /// can still be rejected after the index offers it — its row may have moved
    /// on, its key may fall outside the scope, or a predicate on a slot that is
    /// not the one being walked may not hold. So the walk pulls candidates in
    /// batches and keeps pulling until it has a full page. `effort` bounds that
    /// loop; reaching it is reported as `truncated` rather than dressed up as
    /// an exhausted range, because a short page that is short for the two
    /// reasons is two different answers (REM-78's distinction, applied here).
    pub async fn select_page_partitioned(
        &self,
        scope: &PartitionScope,
        req: crate::engine::attr::select::OrderedSelect<'_>,
    ) -> Result<crate::engine::attr::select::OrderedPage> {
        use crate::engine::attr::select::matches;

        const YIELD_INTERVAL: usize = 1000;

        let indexed = self
            .attr_indexes
            .as_ref()
            .is_some_and(|indexes| indexes.has_slot(req.order_slot));
        if !indexed {
            return Err(StorageError::InvalidArgument(format!(
                "select_page: slot {} has no ordered index to walk",
                req.order_slot
            )));
        }
        let Some(indexes) = &self.attr_indexes else {
            return Ok(crate::engine::attr::select::OrderedPage::empty());
        };
        if self.attr_schema.is_none() {
            return Ok(crate::engine::attr::select::OrderedPage::empty());
        }

        let (lo, hi) = req
            .preds
            .iter()
            .find(|p| p.slot() == req.order_slot)
            .map_or((0, u64::MAX), |p| p.order_bounds());

        // Ask the index for more than the page: some candidates will be
        // rejected below, and a batch sized to the page would need a round trip
        // per rejection.
        let batch_size = req.limit.saturating_mul(2).clamp(32, 1024);

        let mut page = crate::engine::attr::select::OrderedPage {
            keys: Vec::with_capacity(req.limit.min(1024)),
            examined: 0,
            next: req.after.clone(),
            exhausted: false,
            truncated: false,
        };
        let mut seen: std::collections::HashSet<Bytes> = std::collections::HashSet::new();
        let mut examined = 0usize;
        let mut until_yield = YIELD_INTERVAL;

        if req.limit == 0 {
            return Ok(page);
        }

        loop {
            let batch = indexes
                .range_page_partitioned(
                    scope,
                    req.order_slot,
                    crate::engine::attr::index::SlotPage {
                        lo,
                        hi,
                        after: page.next.as_ref(),
                        descending: req.descending,
                        limit: batch_size,
                    },
                )
                .unwrap_or_default();
            let batch_len = batch.len();

            for (order, key) in batch {
                page.next = Some((order, key.clone()));
                examined += 1;
                page.examined = examined;
                // Counted down rather than tested with `%`: the modulo form
                // draws a newer clippy's `is_multiple_of` suggestion, and
                // that method is unstable on the toolchain CI builds with.
                until_yield -= 1;
                if until_yield == 0 {
                    tokio::task::yield_now().await;
                    until_yield = YIELD_INTERVAL;
                }

                // Membership is checked but recorded only once a candidate is
                // accepted, so a skipped entry cannot suppress a later live
                // entry for the same key — same rule as the unbounded walk.
                if seen.contains(&key) {
                    continue;
                }
                if !self.key_is_visible_to_scope(scope, key.as_ref())? {
                    continue;
                }
                let Some(row) = self.get_attrs(key.as_ref()).await? else {
                    continue;
                };
                if row.get(req.order_slot).map(|v| v.order_key()) != Some(order) {
                    continue;
                }
                if !matches(&row, req.preds) {
                    continue;
                }
                seen.insert(key.clone());
                page.keys.push(key);
                if page.keys.len() >= req.limit {
                    return Ok(page);
                }
            }

            // A short batch means the index had nothing more to offer in
            // range: the range is exhausted, and a short page here is the
            // complete answer rather than an interrupted one.
            if batch_len < batch_size {
                page.exhausted = true;
                return Ok(page);
            }
            if examined >= req.effort {
                page.truncated = true;
                return Ok(page);
            }
        }
    }

    /// Delete a record and its attribute row.
    ///
    /// Both `Delete` records are written under one WAL-lock acquisition and
    /// covered by one `sync()` call (when `sync_writes` is on) rather than
    /// as two separate calls that could interleave with another writer or
    /// be split across a memtable rotation. As with `put_with_attrs`, this
    /// is not a single atomic commit — see that method's doc comment for
    /// the torn-tail exposure a crash mid-batch can still produce. Falls
    /// back to a plain `delete` when no attribute schema is registered.
    pub async fn delete_with_attrs(&self, key: impl Into<Bytes>) -> Result<()> {
        let key: Bytes = key.into();
        if self.attr_schema.is_none() {
            return self.delete(key).await;
        }
        // Already prefixed here, unlike the bare record key `PutAttrs`
        // carries on the write side (replay derives the sidecar key itself
        // via `attr_key`). Both are correct; don't double-prefix this one.
        let akey = crate::engine::attr::attr_key(&key);

        self.write_with_retry(
            |ts| {
                vec![
                    WalRecord::delete(key.clone(), ts),
                    WalRecord::delete(akey.clone(), ts),
                ]
            },
            |memtable, ts| {
                memtable.delete_with_timestamp(key.clone(), ts)?;
                memtable.delete_with_timestamp(akey.clone(), ts)
            },
        )
        .await?;

        if let Some(indexes) = &self.attr_indexes {
            indexes.remove_key(key.as_ref())?;
        }
        Ok(())
    }

    pub async fn delete_with_attrs_partitioned(
        &self,
        target: &PartitionBinding,
        logical_key: impl AsRef<[u8]>,
    ) -> Result<Bytes> {
        let key = encode_record_key(target, logical_key)
            .map_err(|err| StorageError::InvalidArgument(err.to_string()))?;
        self.delete_with_attrs(key.clone()).await?;
        Ok(key)
    }

    /// Search for documents with specific tags (AND query)
    pub fn tag_search_and(&self, tags: &[&str]) -> Result<Vec<Bytes>> {
        let index = Self::require_index(&self.tag_index, "Tag index")?;

        Ok(index.read().search_and(tags))
    }

    pub fn tag_search_and_partitioned(
        &self,
        scope: &PartitionScope,
        tags: &[&str],
    ) -> Result<Vec<Bytes>> {
        let index = Self::require_index(&self.tag_index, "Tag index")?;
        Ok(index.read().search_and_partitioned(scope, tags))
    }

    /// Whether the tag index can answer a query for every one of `tags`.
    ///
    /// False when the tag index is switched off, and false when any tag falls
    /// outside the index's token-length bounds and so has no posting list.
    /// `tag_search_and` reports both cases as "no matches", which a caller
    /// cannot distinguish from a real one — so a caller that narrows through
    /// the tag index must check this first and fall back to its own authority.
    pub fn tag_index_can_answer(&self, tags: &[&str]) -> bool {
        let Some(index) = &self.tag_index else {
            return false;
        };
        index.read().can_answer(tags)
    }

    /// Search with scoring (returns results sorted by relevance)
    pub fn tag_search_scored(&self, tags: &[&str]) -> Result<Vec<(Bytes, f32)>> {
        let index = Self::require_index(&self.tag_index, "Tag index")?;

        Ok(index.read().search_or_scored(tags))
    }

    pub fn tag_search_scored_partitioned(
        &self,
        scope: &PartitionScope,
        tags: &[&str],
    ) -> Result<Vec<(Bytes, f32)>> {
        let index = Self::require_index(&self.tag_index, "Tag index")?;
        Ok(index.read().search_or_scored_partitioned(scope, tags))
    }

    /// Scan every record's configured text field for the query tokens.
    ///
    /// This is the access path used when content is not indexed: it reads the
    /// whole corpus on every call. REM-29 replaces it with an index step, at
    /// which point this method and its execution step go away. Keeping it
    /// here rather than in the service is what makes it visible to the
    /// planner, and therefore priceable by REM-80.
    ///
    /// Scoring is the fraction of query tokens present in the text field.
    /// Tokens are expected to arrive already lowercased; the text is
    /// lowercased here. Records scoring zero are omitted.
    ///
    /// Ties keep timestamp order, since the sort is stable and entries arrive
    /// timestamp-ascending (`SegmentedBTreeIndex::range`). On the common
    /// single-token query every match scores exactly `1.0`, so a tie is the
    /// norm rather than the exception: `truncate(limit)` then keeps the
    /// *oldest* matches, and on a large corpus the newest matching memories
    /// are unreachable through this step. REM-29's content index removes
    /// this limitation by making the scan unnecessary.
    pub async fn content_scan(&self, tokens: &[&str], limit: usize) -> Result<Vec<(Bytes, f32)>> {
        self.content_scan_inner(tokens, limit, None).await
    }

    pub async fn content_scan_partitioned(
        &self,
        scope: &PartitionScope,
        tokens: &[&str],
        limit: usize,
    ) -> Result<Vec<(Bytes, f32)>> {
        self.content_scan_inner(tokens, limit, Some(scope)).await
    }

    async fn content_scan_inner(
        &self,
        tokens: &[&str],
        limit: usize,
        scope: Option<&PartitionScope>,
    ) -> Result<Vec<(Bytes, f32)>> {
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        // Yield to the Tokio runtime every this many records. `self.get()`
        // completes on first poll (no internal `.await` point actually
        // suspends), so without an explicit yield this loop would run to
        // completion in a single poll and occupy a worker thread for the
        // duration of a keyword query over a large corpus.
        const YIELD_INTERVAL: usize = 1000;

        // The raw-bytes pre-filter below assumes a token's characters appear
        // contiguous in the serialized bytes exactly as they do in the
        // parsed field value. JSON escaping breaks that assumption: a `"`,
        // a `\`, or a control character (U+0000..U+001F) inside a field
        // value is serialized as a multi-character escape sequence (e.g.
        // `"` becomes `\"`), so a token containing one of those characters
        // can be a contiguous substring of the *parsed* field while never
        // appearing contiguous in the *raw* bytes — the pre-filter would
        // then drop a record the field-scoped match should have accepted.
        // The token set is fixed for the whole scan, so this is decided
        // once per call, not per record.
        let prefilter_is_safe = tokens.iter().all(|t| {
            !t.chars()
                .any(|c| c == '"' || c == '\\' || (c as u32) < 0x20)
        });

        let entries = if let Some(scope) = scope {
            self.time_range_query_partitioned(scope, 0, u64::MAX, None)?
        } else {
            self.time_range_query(0, u64::MAX, None)?
        };
        let mut scored: Vec<(Bytes, f32)> = Vec::new();
        // Tracks whether `EngineConfig.text_field` is finding its target: a
        // typo'd or renamed field extracts from zero records even though
        // records were actually examined, which should be visible rather
        // than silently degrading keyword search to tag-only.
        let mut examined = 0usize;
        let mut extracted = 0usize;

        for (i, (_ts, key)) in entries.into_iter().enumerate() {
            if i > 0 && i % YIELD_INTERVAL == 0 {
                tokio::task::yield_now().await;
            }

            let Some(bytes) = self.get(&key).await? else {
                continue;
            };

            // Cheap pre-filter before paying for a JSON parse: reject
            // records where none of the query tokens appear anywhere in the
            // raw bytes. This cannot change results for a `prefilter_is_safe`
            // token set — if no token is a substring of the whole record,
            // none can be a substring of one of its fields either, so the
            // field-scoped match below still decides the score for every
            // record that survives this check. Skipped entirely otherwise,
            // falling through to the plain extract-and-match path.
            if prefilter_is_safe {
                let raw_lower = String::from_utf8_lossy(&bytes).to_lowercase();
                if !tokens.iter().any(|t| raw_lower.contains(*t)) {
                    continue;
                }
            }
            examined += 1;

            let Some(text) = Self::extract_text_field(&bytes, &self.config.text_field) else {
                continue;
            };
            extracted += 1;
            let text = text.to_lowercase();

            let matched = tokens.iter().filter(|t| text.contains(**t)).count();
            if matched == 0 {
                continue;
            }

            scored.push((key, matched as f32 / tokens.len() as f32));
        }

        if examined > 0 && extracted == 0 {
            tracing::warn!(
                text_field = %self.config.text_field,
                examined,
                "content_scan found the query tokens in records but never in the configured \
                 text field — check EngineConfig.text_field for a typo or a stale field name; \
                 keyword search is silently degrading to tag-only"
            );
        }

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(limit);

        Ok(scored)
    }

    /// Pull one named string field out of a serialized record.
    ///
    /// Deliberately generic: the engine deserializes to `serde_json::Value`
    /// rather than to any domain type, so it stays free of knowledge about
    /// what a memory is. Returns `None` for records that are not JSON objects
    /// or lack the field — a corrupt record is skipped, not fatal, matching
    /// `MemoryRepository::load_by_key`.
    fn extract_text_field(bytes: &[u8], field: &str) -> Option<String> {
        let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
        value.get(field)?.as_str().map(str::to_string)
    }

    /// Check if tag index is enabled
    pub fn tag_enabled(&self) -> bool {
        self.tag_index.is_some()
    }

    /// Save the tag index to disk
    pub fn save_tag_index(&self) -> Result<()> {
        if let Some(index) = &self.tag_index {
            let is_dirty = index.read().is_dirty();
            if is_dirty {
                let doc_count = index.read().len();
                index.write().save_if_dirty()?;
                tracing::info!("Saved segmented tag index ({} docs)", doc_count);
            }
        }
        Ok(())
    }

    pub fn save_all_indexes(&self) -> Result<()> {
        self.save_vector_index()?;
        self.save_graph_index()?;
        self.save_time_series_index()?;
        self.save_tag_index()?;
        if let Some(indexes) = &self.attr_indexes {
            indexes.save_if_dirty()?;
            indexes.compact_if_needed()?;
        }
        Ok(())
    }

    /// Returns the configured data directory. `config` is private, so callers
    /// that need the on-disk root (e.g. the backup endpoint, which tars it up
    /// right after a `checkpoint()`) go through this accessor instead.
    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }

    /// Snapshot internal group-commit counters for diagnostics and benchmarks.
    #[allow(dead_code)]
    pub(crate) fn wal_commit_stats(&self) -> super::wal_commit::WalCommitStats {
        self.wal.stats()
    }

    /// Checkpoint the storage engine.
    ///
    /// Saves all indexes, flushes memtables to SSTables, then truncates the
    /// WAL -- all three under exclusive write admission, so no write's WAL
    /// record can be appended (and then erased by truncate) without its data being
    /// safely on disk in either a flushed SSTable or the still-un-truncated
    /// WAL. Should be called periodically or when the WAL grows too large.
    ///
    /// Memtable persistence runs on the blocking thread pool. Exclusive
    /// admission drains accepted writers through in-memory application, and
    /// the coordinator barrier establishes the durability boundary before
    /// persistence and truncation.
    /// Persist the highest record version issued so far.
    ///
    /// Must run *before* the WAL is truncated, never after. The WAL is what
    /// covers versions issued since the last checkpoint; truncating it first
    /// and crashing before this write would leave a stale mark with nothing to
    /// recover the difference from, and startup would seed below versions
    /// already on disk -- exactly the state this whole change removes.
    fn persist_sequence_mark(&self) -> Result<()> {
        // `sequence` holds the next version to issue, so the highest issued is
        // one less.
        let highest_issued = self.sequence.load(Ordering::SeqCst).saturating_sub(1);
        super::sequence::write(&self.config.data_dir, highest_issued)
    }

    pub async fn checkpoint(&self) -> Result<()> {
        tracing::info!("Starting checkpoint...");
        let _admission = self.write_admission.write().await;
        self.wal.barrier().await?;

        // Any index-save failure aborts before we touch the WAL.
        self.save_all_indexes()?;

        let memtable = Arc::clone(&self.memtable);
        let immutable_memtables = Arc::clone(&self.immutable_memtables);
        let compaction = Arc::clone(&self.compaction);
        let config = self.config.clone();

        tokio::task::spawn_blocking(move || {
            super::tasks::flush_memtables_for_checkpoint(
                &memtable,
                &immutable_memtables,
                &compaction,
                &config,
            )
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))??;
        self.persist_sequence_mark()?;
        self.wal.truncate().await?;

        tracing::info!("Checkpoint complete (indexes saved, memtables flushed, WAL truncated)");
        Ok(())
    }

    // ==================== Lifecycle ====================

    /// Graceful shutdown that works with `&self` (for use from signal handlers with `Arc<StorageEngine>`)
    ///
    /// This flushes the memtable, saves all indexes, syncs the WAL, and waits for
    /// background tasks to finish.
    pub async fn graceful_shutdown(&self) -> Result<()> {
        tracing::info!("Starting graceful shutdown...");

        // Signal shutdown to background tasks
        self.shutdown.store(true, Ordering::SeqCst);

        let persistence_result = async {
            let _admission = self.write_admission.write().await;
            self.wal.barrier().await?;
            self.save_all_indexes()?;
            let memtable = Arc::clone(&self.memtable);
            let immutable_memtables = Arc::clone(&self.immutable_memtables);
            let compaction = Arc::clone(&self.compaction);
            let config = self.config.clone();
            tokio::task::spawn_blocking(move || {
                super::tasks::flush_memtables_for_checkpoint(
                    &memtable,
                    &immutable_memtables,
                    &compaction,
                    &config,
                )
            })
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))??;
            self.persist_sequence_mark()?;
            self.wal.truncate().await
        }
        .await;

        // Wait for background tasks (take handles out of mutex before awaiting)
        let flush_handle = self.flush_handle.lock().take();
        if let Some(handle) = flush_handle {
            let _ = handle.await;
        }
        let compaction_handle = self.compaction_handle.lock().take();
        if let Some(handle) = compaction_handle {
            let _ = handle.await;
        }
        let checkpoint_handle = self.checkpoint_handle.lock().take();
        if let Some(handle) = checkpoint_handle {
            let _ = handle.await;
        }
        let coordinator_result = self.wal.shutdown().await;
        persistence_result?;
        coordinator_result?;

        tracing::info!("Graceful shutdown complete");
        Ok(())
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        for slot in [
            &self.flush_handle,
            &self.compaction_handle,
            &self.checkpoint_handle,
        ] {
            if let Some(handle) = slot.lock().take() {
                handle.abort();
            }
        }
    }
}

/// Result from a vector similarity search
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    /// The key of the matching record
    pub key: Bytes,
    /// Distance to the query vector (interpretation depends on metric)
    pub distance: f32,
}

/// Storage statistics
#[derive(Debug)]
pub struct StorageStats {
    /// Bytes of partition vector indexes currently held in memory.
    ///
    /// Business-only reader (`business::monitoring`), as `memtable_size` below.
    #[cfg_attr(not(feature = "business"), allow(dead_code))]
    pub vector_resident_bytes: usize,
    /// Partition vector indexes currently held in memory.
    #[cfg_attr(not(feature = "business"), allow(dead_code))]
    pub vector_resident_partitions: usize,
    /// Partition vector indexes restored from disk since startup.
    ///
    /// Read together with `vector_evictions`: both climbing together is the
    /// signature of a resident budget smaller than the working set, which
    /// degrades latency rather than failing requests and so has no other
    /// symptom.
    #[cfg_attr(not(feature = "business"), allow(dead_code))]
    pub vector_loads: u64,
    /// Partition vector indexes released from memory since startup.
    #[cfg_attr(not(feature = "business"), allow(dead_code))]
    pub vector_evictions: u64,
    /// Current MemTable size in bytes
    ///
    /// Only read by `business::monitoring::update_from_services` (Prometheus
    /// gauge export, gated behind the `business` Cargo feature) — dead in a
    /// default build.
    #[cfg_attr(not(feature = "business"), allow(dead_code))]
    pub memtable_size: usize,
    /// Number of vectors in the HNSW index
    pub vector_count: usize,
    /// Whether vector search is enabled
    pub vector_enabled: bool,
    /// Number of nodes in the graph index
    pub graph_node_count: usize,
    /// Number of edges in the graph index
    ///
    /// Business-only, as `memtable_size` above: the last default-build reader
    /// was `query::QueryEngineStats`, deleted with the unreachable planner
    /// (REM-72).
    #[cfg_attr(not(feature = "business"), allow(dead_code))]
    pub graph_edge_count: usize,
    /// Number of entries in time-series index
    ///
    /// Business-only — see `graph_edge_count`.
    #[cfg_attr(not(feature = "business"), allow(dead_code))]
    pub time_series_count: usize,
    /// Number of documents in tag index
    ///
    /// Business-only — see `graph_edge_count`.
    #[cfg_attr(not(feature = "business"), allow(dead_code))]
    pub tag_doc_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_put_and_get() {
        let dir = tempdir().unwrap();
        let config = EngineConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024, // 1 MB for testing
            ..Default::default()
        };

        let engine = StorageEngine::new(config).await.unwrap();

        engine.put("key1", "value1").await.unwrap();
        engine.put("key2", "value2").await.unwrap();

        let value1 = engine.get("key1").await.unwrap();
        assert_eq!(value1, Some(Bytes::from("value1")));

        let value2 = engine.get("key2").await.unwrap();
        assert_eq!(value2, Some(Bytes::from("value2")));

        let missing = engine.get("key3").await.unwrap();
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn partitioned_put_and_get_use_bound_scope() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = tempdir().unwrap();
        let engine = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            ..Default::default()
        })
        .await
        .unwrap();
        let tenant = TenantId::new("acme").unwrap();
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        let logical = b"memory:00000000-0000-0000-0000-000000000001";

        let physical = engine
            .put_partitioned(&finance, logical, Bytes::from_static(b"finance-value"))
            .await
            .unwrap();
        assert!(physical.starts_with(crate::engine::storage::partition::PARTITIONED_RECORD_PREFIX));
        assert!(engine.get(logical).await.unwrap().is_none());

        let product_scope = PartitionScope::single(product);
        assert!(engine
            .get_partitioned(&product_scope, logical)
            .await
            .unwrap()
            .is_none());

        let finance_scope = PartitionScope::single(finance.clone());
        let (found_key, value) = engine
            .get_partitioned(&finance_scope, logical)
            .await
            .unwrap()
            .expect("finance scope should see its partitioned record");
        assert_eq!(value, Bytes::from_static(b"finance-value"));
        assert_eq!(found_key.binding().partition().as_str(), "finance");
        assert_eq!(found_key.logical_key(), logical);
    }

    #[tokio::test]
    async fn bound_storage_rejects_write_target_outside_read_scope() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = tempdir().unwrap();
        let engine = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            ..Default::default()
        })
        .await
        .unwrap();
        let tenant = TenantId::new("acme").unwrap();
        let scope = PartitionScope::single(PartitionBinding::new(
            tenant.clone(),
            PartitionId::new("product").unwrap(),
        ));
        let finance = PartitionBinding::new(tenant, PartitionId::new("finance").unwrap());

        let err = match engine.bind_partition_scope(scope, finance) {
            Ok(_) => panic!("expected out-of-scope write target to be rejected"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("write target partition"), "{err}");
    }

    #[tokio::test]
    async fn bound_storage_routes_logical_records_and_vectors() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = tempdir().unwrap();
        let engine = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            vector: VectorConfig {
                enabled: true,
                dimension: 4,
                hnsw_m: 4,
                hnsw_ef_construction: 10,
                hnsw_ef_search: 4,
                metric: crate::engine::util::DistanceMetric::L2,
                hnsw_resident_budget_bytes: None,
            },
            ..Default::default()
        })
        .await
        .unwrap();
        let tenant = TenantId::new("acme").unwrap();
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        let scope = PartitionScope::new(
            tenant,
            [
                PartitionId::new("finance").unwrap(),
                PartitionId::new("product").unwrap(),
            ],
        )
        .unwrap();
        let bound = engine
            .bind_partition_scope(scope, finance.clone())
            .expect("finance target is in the read scope");
        assert_eq!(bound.read_scope().tenant().as_str(), "acme");
        assert_eq!(bound.write_target().partition().as_str(), "finance");

        let logical = b"memory:00000000-0000-0000-0000-000000000010";
        let physical = bound
            .put_with_embedding(
                logical,
                Bytes::from_static(b"finance-vector"),
                Some(vec![1.0, 0.0, 0.0, 0.0]),
            )
            .await
            .unwrap();
        assert!(engine.get(logical).await.unwrap().is_none());

        let (_key, value) = bound
            .get(logical)
            .await
            .unwrap()
            .expect("bound read scope includes finance");
        assert_eq!(value, Bytes::from_static(b"finance-vector"));

        let product_only =
            match engine.bind_partition_scope(PartitionScope::single(product), finance) {
                Ok(_) => panic!("expected out-of-scope write target to be rejected"),
                Err(err) => err.to_string(),
            };
        assert!(product_only.contains("write target partition"));

        let hits = bound
            .vector_search(&[1.0, 0.0, 0.0, 0.0], 10)
            .await
            .unwrap();
        assert_eq!(
            hits.iter().map(|hit| hit.key.clone()).collect::<Vec<_>>(),
            vec![physical]
        );

        let plain_logical = b"memory:00000000-0000-0000-0000-000000000011";
        bound
            .put(plain_logical, Bytes::from_static(b"plain"))
            .await
            .unwrap();
        assert!(bound.get(plain_logical).await.unwrap().is_some());
        bound.delete(plain_logical).await.unwrap();
        assert!(bound.get(plain_logical).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn partitioned_records_survive_checkpoint_and_reopen() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = tempdir().unwrap();
        let tenant = TenantId::new("acme").unwrap();
        let binding = PartitionBinding::new(tenant.clone(), PartitionId::new("tech").unwrap());
        let scope = PartitionScope::single(binding.clone());
        let logical = b"memory:00000000-0000-0000-0000-000000000002";

        {
            let engine = StorageEngine::new(EngineConfig {
                data_dir: dir.path().to_path_buf(),
                sync_writes: false,
                ..Default::default()
            })
            .await
            .unwrap();
            engine
                .put_partitioned(&binding, logical, Bytes::from_static(b"tech-value"))
                .await
                .unwrap();
            engine.checkpoint().await.unwrap();
            engine.graceful_shutdown().await.unwrap();
        }

        let engine = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            ..Default::default()
        })
        .await
        .unwrap();

        let (_key, value) = engine
            .get_partitioned(&scope, logical)
            .await
            .unwrap()
            .expect("partitioned record should be read from SSTable after reopen");
        assert_eq!(value, Bytes::from_static(b"tech-value"));
    }

    #[tokio::test]
    async fn partitioned_delete_writes_a_scoped_tombstone() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = tempdir().unwrap();
        let binding = PartitionBinding::new(
            TenantId::new("acme").unwrap(),
            PartitionId::new("finance").unwrap(),
        );
        let scope = PartitionScope::single(binding.clone());
        let logical = b"memory:00000000-0000-0000-0000-000000000003";

        {
            let engine = StorageEngine::new(EngineConfig {
                data_dir: dir.path().to_path_buf(),
                sync_writes: false,
                ..Default::default()
            })
            .await
            .unwrap();
            engine
                .put_partitioned(&binding, logical, Bytes::from_static(b"secret"))
                .await
                .unwrap();
            engine.delete_partitioned(&binding, logical).await.unwrap();
            assert!(engine
                .get_partitioned(&scope, logical)
                .await
                .unwrap()
                .is_none());
            engine.checkpoint().await.unwrap();
            engine.graceful_shutdown().await.unwrap();
        }

        let engine = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            ..Default::default()
        })
        .await
        .unwrap();
        assert!(engine
            .get_partitioned(&scope, logical)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn legacy_primary_records_migrate_to_configured_default_partition() {
        use crate::engine::storage::partition::{
            encode_record_key, PartitionBinding, PartitionId, PartitionScope, TenantId,
        };
        use crate::engine::storage::sstable::{Compression, SSTableWriter};
        use crate::engine::storage::wal::{WalRecord, WAL};
        use crate::engine::{
            attr::attr_key,
            index::{
                BTreeConfig, EdgeMetadata, GraphConfig, InvertedIndexConfig, SegmentedBTreeIndex,
                SegmentedCsrGraph, SegmentedInvertedIndex,
            },
        };

        let dir = tempdir().unwrap();
        let data_dir = dir.path();
        std::fs::create_dir_all(data_dir.join("wal")).unwrap();
        std::fs::create_dir_all(data_dir.join("sstables").join("level0")).unwrap();
        std::fs::create_dir_all(data_dir.join("index")).unwrap();
        std::fs::create_dir_all(data_dir.join("index").join("attr").join("ord.2")).unwrap();

        let mut manifest = crate::engine::storage::format::FormatManifest::current();
        manifest.set("partition.layout", 0);
        // Model the pre-partition format produced by the previous release.
        manifest.set("index.timeseries", 1);
        manifest.set("index.tags", 2);
        manifest.set("index.attr", 1);
        manifest.set("attr", 1);
        manifest.write_atomic(data_dir).unwrap();

        let sstable_path = data_dir.join("sstables").join("level0").join("000001.sst");
        {
            let mut writer =
                SSTableWriter::with_level(&sstable_path, Compression::None, 0).unwrap();
            writer
                .add(
                    Bytes::from_static(b"memory:checkpointed"),
                    Some(Bytes::from_static(b"from-sstable")),
                    10,
                )
                .unwrap();
            writer
                .add(
                    attr_key(b"memory:attrs"),
                    Some(Bytes::from_static(b"attrs-row")),
                    12,
                )
                .unwrap();
            writer.finish().unwrap();
        }
        {
            let mut time = SegmentedBTreeIndex::new(BTreeConfig::default(), data_dir.join("index"));
            time.insert(123, Bytes::from_static(b"memory:unflushed"))
                .unwrap();
            time.seal_growing().unwrap();

            let mut tags =
                SegmentedInvertedIndex::new(InvertedIndexConfig::default(), data_dir.join("index"));
            tags.add_tags(
                Bytes::from_static(b"memory:unflushed"),
                &["rust".to_string()],
            )
            .unwrap();
            tags.seal_growing().unwrap();

            let mut graph = SegmentedCsrGraph::new(GraphConfig::default(), data_dir.join("index"));
            graph
                .add_edge(
                    Bytes::from_static(b"memory:unflushed"),
                    Bytes::from_static(b"memory:checkpointed"),
                    EdgeMetadata::with_type("related_to")
                        .weight(0.9)
                        .timestamp(125),
                )
                .unwrap();
            graph.save_if_dirty().unwrap();

            let mut attrs = SegmentedBTreeIndex::new(
                BTreeConfig::default(),
                data_dir.join("index").join("attr").join("ord.2"),
            );
            attrs
                .insert(900, Bytes::from_static(b"memory:attrs"))
                .unwrap();
            attrs.seal_growing().unwrap();
        }
        {
            let mut wal = WAL::create(data_dir.join("wal").join("current.wal")).unwrap();
            wal.append(&WalRecord::insert(
                Bytes::from_static(b"memory:unflushed"),
                Bytes::from_static(b"from-wal"),
                11,
            ))
            .unwrap();
            wal.sync().unwrap();
        }

        let finance = PartitionBinding::new(
            TenantId::default_legacy(),
            PartitionId::new("finance").unwrap(),
        );
        let product = PartitionBinding::new(
            TenantId::default_legacy(),
            PartitionId::new("product").unwrap(),
        );
        let engine = StorageEngine::new(EngineConfig {
            data_dir: data_dir.to_path_buf(),
            default_partition: PartitionId::new("finance").unwrap(),
            sync_writes: false,
            ..Default::default()
        })
        .await
        .unwrap();

        let finance_scope = PartitionScope::single(finance.clone());
        let product_scope = PartitionScope::single(product);

        assert_eq!(
            engine
                .get_partitioned(&finance_scope, b"memory:checkpointed")
                .await
                .unwrap()
                .map(|(_key, value)| value),
            Some(Bytes::from_static(b"from-sstable"))
        );
        assert_eq!(
            engine
                .get_partitioned(&finance_scope, b"memory:unflushed")
                .await
                .unwrap()
                .map(|(_key, value)| value),
            Some(Bytes::from_static(b"from-wal"))
        );
        assert!(engine
            .get_partitioned(&product_scope, b"memory:checkpointed")
            .await
            .unwrap()
            .is_none());
        assert!(engine.get(b"memory:checkpointed").await.unwrap().is_none());
        assert!(engine.get(b"memory:unflushed").await.unwrap().is_none());

        let physical_unflushed = encode_record_key(&finance, b"memory:unflushed").unwrap();
        let physical_checkpointed = encode_record_key(&finance, b"memory:checkpointed").unwrap();
        let physical_attrs = encode_record_key(&finance, b"memory:attrs").unwrap();

        let times = engine
            .time_range_query_partitioned(&finance_scope, 123, 123, None)
            .unwrap();
        assert_eq!(times, vec![(123, physical_unflushed.clone())]);
        assert!(engine
            .time_range_query_partitioned(&product_scope, 123, 123, None)
            .unwrap()
            .is_empty());

        assert_eq!(
            engine
                .tag_search_and_partitioned(&finance_scope, &["rust"])
                .unwrap(),
            vec![physical_unflushed.clone()]
        );
        assert!(engine
            .tag_search_and_partitioned(&product_scope, &["rust"])
            .unwrap()
            .is_empty());

        let neighbors = engine
            .get_neighbors_partitioned(&finance_scope, physical_unflushed.as_ref())
            .unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, physical_checkpointed);
        assert!(engine
            .get_neighbors_partitioned(&product_scope, physical_unflushed.as_ref())
            .unwrap()
            .is_empty());

        assert_eq!(
            engine
                .get(&attr_key(physical_attrs.as_ref()))
                .await
                .unwrap(),
            Some(Bytes::from_static(b"attrs-row"))
        );
        assert!(engine
            .get(&attr_key(b"memory:attrs"))
            .await
            .unwrap()
            .is_none());

        let attr_index = SegmentedBTreeIndex::load_from_dir(
            BTreeConfig::default(),
            data_dir.join("index").join("attr").join("ord.2"),
        )
        .unwrap();
        assert_eq!(
            attr_index.range(900, 900),
            vec![(900, physical_attrs.clone())]
        );

        let manifest = crate::engine::storage::format::FormatManifest::read(data_dir).unwrap();
        assert_eq!(manifest.version_of("partition.layout"), 1);

        drop(engine);
        let mut manifest = crate::engine::storage::format::FormatManifest::read(data_dir).unwrap();
        manifest.set("partition.layout", 0);
        manifest.write_atomic(data_dir).unwrap();

        let engine = StorageEngine::new(EngineConfig {
            data_dir: data_dir.to_path_buf(),
            default_partition: PartitionId::new("finance").unwrap(),
            sync_writes: false,
            ..Default::default()
        })
        .await
        .unwrap();
        assert!(engine.get(b"memory:unflushed").await.unwrap().is_none());
        assert_eq!(
            engine
                .time_range_query_partitioned(&finance_scope, 123, 123, None)
                .unwrap(),
            vec![(123, physical_unflushed.clone())]
        );
        assert_eq!(
            engine
                .get_neighbors_partitioned(&finance_scope, physical_unflushed.as_ref())
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            engine
                .get(&attr_key(physical_attrs.as_ref()))
                .await
                .unwrap(),
            Some(Bytes::from_static(b"attrs-row"))
        );

        let manifest = crate::engine::storage::format::FormatManifest::read(data_dir).unwrap();
        assert_eq!(manifest.version_of("partition.layout"), 1);
    }

    /// `default_partition` is baked into every key on disk, so changing it
    /// after the fact does not re-target anything — it points the engine at a
    /// prefix nothing was ever written under. Without the recorded marker the
    /// symptom is an empty corpus and no error at all, which reads as total
    /// data loss.
    #[tokio::test]
    async fn reopening_under_a_different_default_partition_is_refused() {
        use crate::engine::storage::partition::PartitionId;

        let dir = tempdir().unwrap();
        {
            let engine = StorageEngine::new(EngineConfig {
                data_dir: dir.path().to_path_buf(),
                default_partition: PartitionId::new("finance").unwrap(),
                sync_writes: false,
                ..Default::default()
            })
            .await
            .unwrap();
            engine
                .put_with_embedding_partitioned(
                    &engine.default_partition_binding(),
                    b"memory:one",
                    Bytes::from_static(b"payload"),
                    None,
                )
                .await
                .unwrap();
            engine.checkpoint().await.unwrap();
            engine.graceful_shutdown().await.unwrap();
        }

        let result = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            default_partition: PartitionId::new("product").unwrap(),
            sync_writes: false,
            ..Default::default()
        })
        .await;

        let Err(err) = result else {
            panic!("a directory bound to one partition must not open under another");
        };
        let msg = err.to_string();
        assert!(msg.contains("finance"), "{msg}");
        assert!(msg.contains("product"), "{msg}");
    }

    #[tokio::test]
    async fn reopening_under_the_same_default_partition_is_allowed() {
        use crate::engine::storage::partition::PartitionId;

        let dir = tempdir().unwrap();
        let config = || EngineConfig {
            data_dir: dir.path().to_path_buf(),
            default_partition: PartitionId::new("finance").unwrap(),
            sync_writes: false,
            ..Default::default()
        };

        let engine = StorageEngine::new(config()).await.unwrap();
        engine.graceful_shutdown().await.unwrap();
        drop(engine);

        StorageEngine::new(config())
            .await
            .expect("an unchanged default partition must reopen cleanly");
    }

    #[tokio::test]
    async fn vector_search_fans_out_only_to_authorized_hnsw_partitions() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = tempdir().unwrap();
        let engine = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            vector: VectorConfig {
                enabled: true,
                dimension: 4,
                hnsw_m: 4,
                hnsw_ef_construction: 10,
                hnsw_ef_search: 4,
                metric: crate::engine::util::DistanceMetric::L2,
                hnsw_resident_budget_bytes: None,
            },
            ..Default::default()
        })
        .await
        .unwrap();

        let tenant = TenantId::new("acme").unwrap();
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        let tech = PartitionBinding::new(tenant.clone(), PartitionId::new("tech").unwrap());

        let finance_key = engine
            .put_with_embedding_partitioned(
                &finance,
                b"memory:finance",
                Bytes::from_static(b"finance"),
                Some(vec![1.0, 0.0, 0.0, 0.0]),
            )
            .await
            .unwrap();
        let product_key = engine
            .put_with_embedding_partitioned(
                &product,
                b"memory:product",
                Bytes::from_static(b"product"),
                Some(vec![0.9, 0.1, 0.0, 0.0]),
            )
            .await
            .unwrap();
        let tech_key = engine
            .put_with_embedding_partitioned(
                &tech,
                b"memory:tech",
                Bytes::from_static(b"tech"),
                Some(vec![0.0, 1.0, 0.0, 0.0]),
            )
            .await
            .unwrap();

        let product_scope = PartitionScope::single(product);
        let product_hits = engine
            .vector_search_partitioned(&product_scope, &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert_eq!(
            product_hits
                .iter()
                .map(|hit| hit.key.clone())
                .collect::<Vec<_>>(),
            vec![product_key.clone()]
        );

        let multi_scope = PartitionScope::new(
            tenant,
            [
                PartitionId::new("finance").unwrap(),
                PartitionId::new("product").unwrap(),
            ],
        )
        .unwrap();
        let multi_hits = engine
            .vector_search_partitioned(&multi_scope, &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        let keys = multi_hits
            .iter()
            .map(|hit| hit.key.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![finance_key, product_key],
            "authorized partition hits must keep the distance-then-key merge order"
        );
        assert!(!multi_hits.iter().any(|hit| hit.key == tech_key));
    }

    #[tokio::test]
    async fn partitioned_tag_and_time_queries_return_only_authorized_keys() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = tempdir().unwrap();
        let engine = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            ..Default::default()
        })
        .await
        .unwrap();
        let tenant = TenantId::new("acme").unwrap();
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        let finance_key = engine
            .store_memory_core_partitioned(
                &finance,
                b"memory:finance",
                Bytes::from_static(b"{\"content\":\"rust finance\"}"),
                None,
                1,
                &["rust".to_string()],
                None,
            )
            .await
            .unwrap();
        let product_key = engine
            .store_memory_core_partitioned(
                &product,
                b"memory:product",
                Bytes::from_static(b"{\"content\":\"rust product\"}"),
                None,
                2,
                &["rust".to_string()],
                None,
            )
            .await
            .unwrap();

        let product_scope = PartitionScope::single(product);
        assert_eq!(
            engine
                .tag_search_and_partitioned(&product_scope, &["rust"])
                .unwrap(),
            vec![product_key.clone()]
        );
        assert_eq!(
            engine
                .time_range_query_partitioned(&product_scope, 0, u64::MAX, None)
                .unwrap()
                .into_iter()
                .map(|(_ts, key)| key)
                .collect::<Vec<_>>(),
            vec![product_key]
        );
        assert!(!engine
            .tag_search_and_partitioned(&product_scope, &["rust"])
            .unwrap()
            .contains(&finance_key));
    }

    #[tokio::test]
    async fn partitioned_graph_traversal_hides_unauthorized_cross_partition_edges() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = tempdir().unwrap();
        let engine = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            ..Default::default()
        })
        .await
        .unwrap();
        let tenant = TenantId::new("acme").unwrap();
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let tech = PartitionBinding::new(tenant.clone(), PartitionId::new("tech").unwrap());
        let product_a = encode_record_key(&product, b"memory:product-a").unwrap();
        let product_b = encode_record_key(&product, b"memory:product-b").unwrap();
        let finance_b = encode_record_key(&finance, b"memory:finance-b").unwrap();
        let tech_c = encode_record_key(&tech, b"memory:tech-c").unwrap();

        engine
            .add_edge(
                product_a.clone(),
                product_b.clone(),
                Some("related_to".to_string()),
                Some(0.8),
                1,
            )
            .await
            .unwrap();
        engine
            .add_edge(
                product_a.clone(),
                finance_b.clone(),
                Some("related_to".to_string()),
                Some(0.9),
                2,
            )
            .await
            .unwrap();
        engine
            .add_edge(
                finance_b.clone(),
                tech_c.clone(),
                Some("related_to".to_string()),
                Some(0.7),
                3,
            )
            .await
            .unwrap();

        let product_scope = PartitionScope::single(product);
        let product_nodes = engine
            .traverse_graph_partitioned(&product_scope, product_a.as_ref(), 2, None)
            .unwrap()
            .into_iter()
            .map(|node| node.node_id)
            .collect::<Vec<_>>();
        assert_eq!(product_nodes, vec![product_a.clone(), product_b.clone()]);
        assert!(!product_nodes.contains(&finance_b));
        assert!(!product_nodes.contains(&tech_c));

        let all_domain_scope = PartitionScope::new(
            tenant,
            [
                PartitionId::new("product").unwrap(),
                PartitionId::new("finance").unwrap(),
            ],
        )
        .unwrap();
        let all_domain_nodes = engine
            .traverse_graph_partitioned(&all_domain_scope, product_a.as_ref(), 2, None)
            .unwrap()
            .into_iter()
            .map(|node| node.node_id)
            .collect::<Vec<_>>();
        assert!(all_domain_nodes.contains(&product_b));
        assert!(all_domain_nodes.contains(&finance_b));
        assert!(
            !all_domain_nodes.contains(&tech_c),
            "finance may be traversed under all-domain access, but tech remains outside the reader scope"
        );
    }

    #[tokio::test]
    async fn an_eviction_flush_neither_loses_nor_duplicates_a_vector_on_replay() {
        // Eviction persists a partition outside the checkpoint cycle. That is
        // only ever *more* current than the checkpoint would have left it, but
        // "only ever more current" is an argument, and this is the test that
        // makes it a fact: no checkpoint runs here, so the WAL replays inserts
        // over chunks an eviction already wrote.
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = tempdir().unwrap();
        let tenant = TenantId::new("acme").unwrap();
        let first = PartitionBinding::new(tenant.clone(), PartitionId::new("first").unwrap());
        let second = PartitionBinding::new(tenant.clone(), PartitionId::new("second").unwrap());
        let first_scope = PartitionScope::single(first.clone());

        // A budget no graph can fit under, so every restore evicts the other
        // partition and flushes it on the way out.
        let engine_config = |dir: &std::path::Path| EngineConfig {
            data_dir: dir.to_path_buf(),
            sync_writes: false,
            vector: VectorConfig {
                enabled: true,
                dimension: 4,
                hnsw_m: 4,
                hnsw_ef_construction: 10,
                hnsw_ef_search: 4,
                metric: crate::engine::util::DistanceMetric::L2,
                hnsw_resident_budget_bytes: Some(1),
            },
            ..Default::default()
        };

        let first_key;
        {
            let engine = StorageEngine::new(engine_config(dir.path())).await.unwrap();

            first_key = engine
                .put_with_embedding_partitioned(
                    &first,
                    b"memory:first",
                    Bytes::from_static(b"first"),
                    Some(vec![1.0, 0.0, 0.0, 0.0]),
                )
                .await
                .unwrap();

            // Restoring `second` pushes `first` out and flushes it.
            engine
                .put_with_embedding_partitioned(
                    &second,
                    b"memory:second",
                    Bytes::from_static(b"second"),
                    Some(vec![0.0, 1.0, 0.0, 0.0]),
                )
                .await
                .unwrap();

            assert_eq!(
                engine.vector_count(),
                2,
                "eviction lost a vector before the process even ended"
            );

            // Deliberately no checkpoint: the WAL still holds both inserts.
            engine.graceful_shutdown().await.unwrap();
        }

        let engine = StorageEngine::new(engine_config(dir.path())).await.unwrap();

        assert_eq!(
            engine.vector_count(),
            2,
            "replaying the WAL over eviction-flushed chunks changed the vector \
             count: an insert was either lost or applied twice"
        );

        let hits = engine
            .vector_search_partitioned(&first_scope, &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert_eq!(
            hits.iter().map(|hit| hit.key.clone()).collect::<Vec<_>>(),
            vec![first_key],
            "the evicted-then-replayed partition did not return exactly its one vector"
        );
    }

    #[tokio::test]
    async fn partitioned_hnsw_vectors_survive_checkpoint_and_reopen() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = tempdir().unwrap();
        let tenant = TenantId::new("acme").unwrap();
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        let finance_scope = PartitionScope::single(finance.clone());
        let product_scope = PartitionScope::single(product);
        let key;

        {
            let engine = StorageEngine::new(EngineConfig {
                data_dir: dir.path().to_path_buf(),
                sync_writes: false,
                vector: VectorConfig {
                    enabled: true,
                    dimension: 4,
                    hnsw_m: 4,
                    hnsw_ef_construction: 10,
                    hnsw_ef_search: 4,
                    metric: crate::engine::util::DistanceMetric::L2,
                    hnsw_resident_budget_bytes: None,
                },
                ..Default::default()
            })
            .await
            .unwrap();
            key = engine
                .put_with_embedding_partitioned(
                    &finance,
                    b"memory:finance",
                    Bytes::from_static(b"finance"),
                    Some(vec![1.0, 0.0, 0.0, 0.0]),
                )
                .await
                .unwrap();
            engine.checkpoint().await.unwrap();
            engine.graceful_shutdown().await.unwrap();
        }

        let engine = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            vector: VectorConfig {
                enabled: true,
                dimension: 4,
                hnsw_m: 4,
                hnsw_ef_construction: 10,
                hnsw_ef_search: 4,
                metric: crate::engine::util::DistanceMetric::L2,
                hnsw_resident_budget_bytes: None,
            },
            ..Default::default()
        })
        .await
        .unwrap();

        let finance_hits = engine
            .vector_search_partitioned(&finance_scope, &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert_eq!(
            finance_hits
                .iter()
                .map(|hit| hit.key.clone())
                .collect::<Vec<_>>(),
            vec![key]
        );
        assert!(engine
            .vector_search_partitioned(&product_scope, &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn test_new_sweeps_orphaned_tmp_files_left_by_a_crash() {
        let dir = tempdir().unwrap();
        let index_dir = dir.path().join("index");
        let sstables_dir = dir.path().join("sstables");
        std::fs::create_dir_all(&index_dir).unwrap();
        std::fs::create_dir_all(&sstables_dir).unwrap();

        // Simulate a crash mid-write at each of the four tmp+rename sites.
        std::fs::write(index_dir.join("hnsw.manifest.tmp"), b"stale").unwrap();
        std::fs::write(index_dir.join("nodes_0_100.seg.tmp"), b"stale").unwrap();
        std::fs::write(index_dir.join("deleted_nodes.bin.tmp"), b"stale").unwrap();
        std::fs::write(sstables_dir.join("000123.sst.tmp"), b"stale").unwrap();

        let config = EngineConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let _engine = StorageEngine::new(config).await.unwrap();

        assert!(!index_dir.join("hnsw.manifest.tmp").exists());
        assert!(!index_dir.join("nodes_0_100.seg.tmp").exists());
        assert!(!index_dir.join("deleted_nodes.bin.tmp").exists());
        assert!(!sstables_dir.join("000123.sst.tmp").exists());
    }

    #[tokio::test]
    async fn test_delete() {
        let dir = tempdir().unwrap();
        let config = EngineConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let engine = StorageEngine::new(config).await.unwrap();

        engine.put("key1", "value1").await.unwrap();
        assert!(engine.get("key1").await.unwrap().is_some());

        engine.delete("key1").await.unwrap();
        assert!(engine.get("key1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_update() {
        let dir = tempdir().unwrap();
        let config = EngineConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let engine = StorageEngine::new(config).await.unwrap();

        engine.put("key1", "value1").await.unwrap();
        assert_eq!(
            engine.get("key1").await.unwrap(),
            Some(Bytes::from("value1"))
        );

        engine.put("key1", "value2").await.unwrap();
        assert_eq!(
            engine.get("key1").await.unwrap(),
            Some(Bytes::from("value2"))
        );
    }

    #[tokio::test]
    async fn test_flush() {
        let dir = tempdir().unwrap();
        let config = EngineConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 10 * 1024 * 1024, // 10 MB to accommodate all writes without hitting backlog cap
            ..Default::default()
        };

        let engine = StorageEngine::new(config).await.unwrap();

        // Write enough data to verify flushing works
        for i in 0..100 {
            engine
                .put(format!("key{:05}", i), format!("value{}", i))
                .await
                .unwrap();
        }

        engine.flush().await.unwrap();

        // Data should still be readable
        let value = engine.get("key00050").await.unwrap();
        assert_eq!(value, Some(Bytes::from("value50")));
    }

    #[tokio::test]
    async fn test_stats() {
        let dir = tempdir().unwrap();
        let config = EngineConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let engine = StorageEngine::new(config).await.unwrap();

        engine.put("key1", "value1").await.unwrap();

        let stats = engine.stats();
        assert!(stats.memtable_size > 0);
    }

    #[tokio::test]
    async fn add_edges_batch_inserts_all_edges() {
        let dir = tempdir().unwrap();
        let config = EngineConfig {
            data_dir: dir.path().to_path_buf(),
            vector: VectorConfig {
                enabled: false,
                ..VectorConfig::default()
            },
            ..EngineConfig::default()
        };
        let engine = StorageEngine::new(config).await.unwrap();

        let edges = vec![
            (
                Bytes::from("node:a"),
                Bytes::from("node:b"),
                Some("similar_to".to_string()),
                Some(0.9f32),
            ),
            (
                Bytes::from("node:a"),
                Bytes::from("node:c"),
                Some("similar_to".to_string()),
                Some(0.8f32),
            ),
        ];

        engine.add_edges_batch(edges, 1_000).await.unwrap();

        let neighbors = engine.get_neighbors(b"node:a").unwrap();
        assert_eq!(neighbors.len(), 2);
        let targets: Vec<String> = neighbors
            .iter()
            .map(|(k, _, _, _)| String::from_utf8_lossy(k).to_string())
            .collect();
        assert!(targets.contains(&"node:b".to_string()));
        assert!(targets.contains(&"node:c".to_string()));
    }

    #[tokio::test]
    async fn store_memory_core_writes_kv_and_indexes() {
        let dir = tempdir().unwrap();
        let config = EngineConfig {
            data_dir: dir.path().to_path_buf(),
            vector: VectorConfig {
                enabled: false,
                ..VectorConfig::default()
            },
            ..EngineConfig::default()
        };
        let engine = StorageEngine::new(config).await.unwrap();

        let key = Bytes::from("memory:aaaaaaaa-0000-0000-0000-000000000001");
        let value = Bytes::from(r#"{"id":"test","content":"hello"}"#);
        let ts = 1_700_000_000_000u64;
        let tags = vec!["rust".to_string(), "__type:short_term".to_string()];

        engine
            .store_memory_core(key.clone(), value.clone(), None, ts, &tags, None)
            .await
            .unwrap();

        // KV readable
        let got = engine.get(&key).await.unwrap();
        assert_eq!(got, Some(value));

        // Timestamp indexed
        let ts_entries = engine.time_range_query(ts, ts, None).unwrap();
        assert_eq!(ts_entries.len(), 1);
        assert_eq!(ts_entries[0].0, ts);

        // Tags indexed
        let tag_results = engine.tag_search_and(&["rust"]).unwrap();
        assert!(
            tag_results.iter().any(|k| k == &key),
            "tag index should contain the stored key"
        );
    }

    #[tokio::test]
    async fn tag_index_can_answer_is_true_for_ordinary_tags() {
        let dir = tempdir().unwrap();
        let config = EngineConfig {
            data_dir: dir.path().to_path_buf(),
            vector: VectorConfig {
                enabled: false,
                ..VectorConfig::default()
            },
            ..EngineConfig::default()
        };
        let engine = StorageEngine::new(config).await.unwrap();

        assert!(engine.tag_index_can_answer(&["rust", "programming"]));
    }

    #[tokio::test]
    async fn tag_index_can_answer_is_false_when_any_tag_is_over_length() {
        let dir = tempdir().unwrap();
        let config = EngineConfig {
            data_dir: dir.path().to_path_buf(),
            vector: VectorConfig {
                enabled: false,
                ..VectorConfig::default()
            },
            ..EngineConfig::default()
        };
        let engine = StorageEngine::new(config).await.unwrap();
        let too_long = "a".repeat(101);

        assert!(!engine.tag_index_can_answer(&["rust", &too_long]));
    }

    #[tokio::test]
    async fn tag_index_can_answer_is_false_when_the_tag_index_is_disabled() {
        let dir = tempdir().unwrap();
        let config = EngineConfig {
            data_dir: dir.path().to_path_buf(),
            vector: VectorConfig {
                enabled: false,
                ..VectorConfig::default()
            },
            tag_index: TagIndexConfig {
                enabled: false,
                ..TagIndexConfig::default()
            },
            ..EngineConfig::default()
        };
        let engine = StorageEngine::new(config).await.unwrap();

        assert!(!engine.tag_index_can_answer(&["rust"]));
    }

    fn format_test_config(dir: &std::path::Path) -> EngineConfig {
        EngineConfig {
            data_dir: dir.to_path_buf(),
            memtable_size: 1024 * 1024,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn new_writes_a_format_manifest() {
        let dir = tempdir().unwrap();
        let _engine = StorageEngine::new(format_test_config(dir.path()))
            .await
            .unwrap();

        let manifest = crate::engine::storage::format::FormatManifest::read(dir.path()).unwrap();
        for (name, version) in crate::engine::storage::format::SUPPORTED {
            assert_eq!(manifest.version_of(name), *version, "subsystem {name}");
        }
        assert_eq!(manifest.version_of("partition.layout"), 1);
    }

    #[tokio::test]
    async fn new_accepts_a_current_partition_layout_manifest() {
        let dir = tempdir().unwrap();
        crate::engine::storage::format::FormatManifest::current()
            .write_atomic(dir.path())
            .unwrap();

        let _engine = StorageEngine::new(format_test_config(dir.path()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn new_refuses_a_directory_from_the_future() {
        let dir = tempdir().unwrap();
        let mut manifest = crate::engine::storage::format::FormatManifest::current();
        manifest.set("index.hnsw", 99);
        manifest.write_atomic(dir.path()).unwrap();

        let err = StorageEngine::new(format_test_config(dir.path()))
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("index.hnsw"), "{err}");
        assert!(err.contains("99"), "{err}");
    }

    #[tokio::test]
    async fn new_refuses_a_newer_partition_layout_version() {
        let dir = tempdir().unwrap();
        let mut manifest = crate::engine::storage::format::FormatManifest::current();
        manifest.set("partition.layout", 2);
        manifest.write_atomic(dir.path()).unwrap();

        let err = StorageEngine::new(format_test_config(dir.path()))
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("partition.layout"), "{err}");
        assert!(err.contains("2"), "{err}");
    }

    #[tokio::test]
    async fn new_refuses_an_unknown_subsystem() {
        let dir = tempdir().unwrap();
        let mut manifest = crate::engine::storage::format::FormatManifest::current();
        manifest.set("index.attributes", 1);
        manifest.write_atomic(dir.path()).unwrap();

        let err = StorageEngine::new(format_test_config(dir.path()))
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("index.attributes"), "{err}");
    }

    #[tokio::test]
    async fn new_adopts_a_directory_written_before_the_manifest_existed() {
        let dir = tempdir().unwrap();

        // Build a real data directory, then remove FORMAT so it looks like
        // one written before this ticket.
        {
            let engine = StorageEngine::new(format_test_config(dir.path()))
                .await
                .unwrap();
            engine.put("adopted-key", "value").await.unwrap();
            engine.checkpoint().await.unwrap();
            engine.put("post-checkpoint-key", "value2").await.unwrap();
        }
        std::fs::remove_file(dir.path().join("FORMAT")).unwrap();

        let engine = StorageEngine::new(format_test_config(dir.path()))
            .await
            .unwrap();

        assert_eq!(
            engine.get("adopted-key").await.unwrap(),
            Some(Bytes::from("value")),
            "adoption must not lose checkpointed data"
        );
        assert_eq!(
            engine.get("post-checkpoint-key").await.unwrap(),
            Some(Bytes::from("value2")),
            "adoption must preserve WAL records written since the last checkpoint"
        );
        assert_eq!(
            crate::engine::storage::format::FormatManifest::read(dir.path())
                .unwrap()
                .version_of("wal"),
            1
        );
        assert_eq!(
            crate::engine::storage::format::FormatManifest::read(dir.path())
                .unwrap()
                .version_of("partition.layout"),
            1,
            "absent manifests migrate to the current partition-layout version"
        );
    }
}

#[cfg(test)]
mod storage_recovery_tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;
    use tempfile::TempDir;

    fn test_cfg(data_dir: std::path::PathBuf) -> EngineConfig {
        EngineConfig {
            data_dir,
            sync_writes: true,
            checkpoint_interval: Duration::from_secs(86400), // no auto-checkpoint
            vector: VectorConfig {
                enabled: true,
                dimension: 4,
                hnsw_m: 4,
                hnsw_ef_construction: 10,
                hnsw_ef_search: 4,
                metric: crate::engine::util::DistanceMetric::L2,
                hnsw_resident_budget_bytes: None,
            },
            ..Default::default()
        }
    }

    #[test]
    fn engine_config_default_matches_production_toml_default() {
        // config/remem-server.toml and FileStorageConfig::default() both
        // ship sync_writes = true; EngineConfig::default() must agree, or
        // any code path constructing it directly (StorageEngine::open,
        // tests using ..Default::default()) silently runs without
        // durability.
        assert!(EngineConfig::default().sync_writes);
    }

    #[tokio::test]
    async fn checkpoint_drains_accepted_write_and_excludes_later_write() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(
            StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap(),
        );
        engine.wal.pause_next_sync();

        let accepted = {
            let engine = Arc::clone(&engine);
            tokio::spawn(async move { engine.put("accepted", "before").await })
        };
        engine.wal.wait_until_sync_blocked().await;

        let checkpoint = {
            let engine = Arc::clone(&engine);
            tokio::spawn(async move { engine.checkpoint().await })
        };
        tokio::task::yield_now().await;
        let later = {
            let engine = Arc::clone(&engine);
            tokio::spawn(async move { engine.put("later", "after").await })
        };
        tokio::task::yield_now().await;
        assert!(!checkpoint.is_finished());
        assert!(!later.is_finished());

        engine.wal.release_sync();
        accepted.await.unwrap().unwrap();
        checkpoint.await.unwrap().unwrap();
        later.await.unwrap().unwrap();

        assert_eq!(
            engine.get("accepted").await.unwrap(),
            Some(Bytes::from("before"))
        );
        assert_eq!(
            engine.get("later").await.unwrap(),
            Some(Bytes::from("after"))
        );
        assert!(engine.wal.size().await.unwrap() > super::super::wal::WAL_HEADER_LEN);
    }

    #[tokio::test]
    async fn concurrent_mvcc_timestamps_follow_wal_submission_order() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(
            StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap(),
        );
        let wal_path = dir.path().join("wal/current.wal");
        engine.wal.pause_next_sync();
        let first = {
            let engine = Arc::clone(&engine);
            tokio::spawn(async move { engine.put("first", "1").await })
        };
        engine.wal.wait_until_sync_blocked().await;
        let second = {
            let engine = Arc::clone(&engine);
            tokio::spawn(async move { engine.put("second", "2").await })
        };
        engine.wal.release_sync();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();

        let records: Vec<_> = super::super::wal::WAL::open(&wal_path)
            .unwrap()
            .iter()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, Bytes::from("first"));
        assert_eq!(records[1].key, Bytes::from("second"));
        assert!(records[0].timestamp < records[1].timestamp);
    }

    #[tokio::test]
    async fn shutdown_drains_accepted_write_and_rejects_new_mutations() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(
            StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap(),
        );
        engine.wal.pause_next_sync();
        let accepted = {
            let engine = Arc::clone(&engine);
            tokio::spawn(async move { engine.put("accepted", "value").await })
        };
        engine.wal.wait_until_sync_blocked().await;
        let shutdown = {
            let engine = Arc::clone(&engine);
            tokio::spawn(async move { engine.graceful_shutdown().await })
        };
        while !engine.shutdown.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        assert!(engine.put("rejected", "value").await.is_err());
        engine.wal.release_sync();
        accepted.await.unwrap().unwrap();
        shutdown.await.unwrap().unwrap();
        assert_eq!(
            engine.get("accepted").await.unwrap(),
            Some(Bytes::from("value"))
        );

        drop(engine);
        let reopened = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        assert_eq!(
            reopened.get("accepted").await.unwrap(),
            Some(Bytes::from("value"))
        );
        assert!(reopened.get("rejected").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn shutdown_failure_is_propagated_and_retains_wal_for_restart() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        engine.put("recoverable", "value").await.unwrap();
        engine.wal.fail_next_sync();

        let error = engine.graceful_shutdown().await.unwrap_err().to_string();
        assert!(error.contains("injected WAL sync failure"), "{error}");
        assert!(engine.put("rejected", "value").await.is_err());
        assert!(
            std::fs::metadata(dir.path().join("wal/current.wal"))
                .unwrap()
                .len()
                > super::super::wal::WAL_HEADER_LEN
        );

        drop(engine);
        let reopened = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        assert_eq!(
            reopened.get("recoverable").await.unwrap(),
            Some(Bytes::from("value"))
        );
    }

    #[tokio::test]
    async fn remove_all_edges_clears_graph() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        let src = Bytes::from("memory:src");
        let dst = Bytes::from("memory:dst");

        engine
            .add_edge(
                src.clone(),
                dst.clone(),
                Some("related_to".to_string()),
                Some(0.9),
                1_000,
            )
            .await
            .unwrap();

        let neighbors_before = engine.get_neighbors(src.as_ref()).unwrap();
        assert_eq!(neighbors_before.len(), 1);

        engine.remove_all_edges(src.as_ref()).await.unwrap();

        let neighbors_after = engine.get_neighbors(src.as_ref()).unwrap();
        assert!(neighbors_after.is_empty(), "all edges must be removed");
    }

    #[tokio::test]
    async fn edge_creation_timestamp_survives_wal_replay() {
        let dir = TempDir::new().unwrap();
        let src = Bytes::from("memory:src-ts");
        let dst = Bytes::from("memory:dst-ts");
        // A distinctive real-world epoch-ms value, not an MVCC-sequence-sized
        // number like 0/1/2/3 -- picking something in that range would make
        // this test pass by coincidence even if the bug (metadata.timestamp
        // holding an MVCC counter instead of `created_at_ms`) were reintroduced.
        let created_at_ms = 1_700_000_000_123_u64;

        // Phase 1: create the edge with a known real timestamp — no checkpoint.
        {
            let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap();
            engine
                .add_edge(
                    src.clone(),
                    dst.clone(),
                    Some("related_to".to_string()),
                    Some(0.9),
                    created_at_ms,
                )
                .await
                .unwrap();
        } // engine dropped here — no checkpoint, only WAL

        // Phase 2: restart via WAL replay; the edge's timestamp must survive
        // exactly, not reset to 0 and not be re-stamped with a fresh now().
        {
            let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap();

            let neighbors = engine.get_neighbors(src.as_ref()).unwrap();
            assert_eq!(neighbors.len(), 1, "edge must survive WAL replay");
            let (target, edge_type, weight, replayed_ts) = &neighbors[0];
            assert_eq!(target.as_ref(), dst.as_ref());
            assert_eq!(edge_type, "related_to");
            assert_eq!(*weight, 0.9);
            assert_eq!(
                *replayed_ts, created_at_ms,
                "edge creation timestamp must survive WAL replay unchanged, \
                 not reset to 0 or re-stamped with a fresh now()"
            );
        }
    }

    #[tokio::test]
    async fn hard_delete_survives_wal_replay() {
        let dir = TempDir::new().unwrap();
        let key = Bytes::from("memory:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        let val = Bytes::from(r#"{"id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","content":"test"}"#);
        let embedding = vec![1.0f32, 0.0, 0.0, 0.0];

        // Phase 1: store + hard delete — no checkpoint
        {
            let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap();
            engine
                .store_memory_core(
                    key.clone(),
                    val.clone(),
                    Some(embedding.clone()),
                    1_000_u64,
                    &["__type:short_term".to_string()],
                    None,
                )
                .await
                .unwrap();

            engine.remove_from_indexes(key.as_ref()).await.unwrap();
            engine.delete(key.clone()).await.unwrap();
        } // engine dropped here — no checkpoint, only WAL

        // Phase 2: restart via WAL replay
        {
            let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap();

            assert!(
                engine.get(key.as_ref()).await.unwrap().is_none(),
                "hard-deleted KV must not reappear after WAL replay"
            );
            assert_eq!(
                engine.time_range_query(0, u64::MAX, None).unwrap().len(),
                0,
                "time-series must not contain hard-deleted key after WAL replay"
            );
            assert!(
                engine.get_vector(key.as_ref()).is_none(),
                "HNSW must not contain hard-deleted key after WAL replay"
            );
        }
    }

    #[tokio::test]
    async fn remove_from_indexes_wal_record_present_immediately() {
        let dir = TempDir::new().unwrap();
        let key = Bytes::from_static(b"memory:will-be-removed");

        {
            let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap();
            engine
                .store_memory_core(
                    key.clone(),
                    Bytes::from_static(b"{}"),
                    Some(vec![1.0, 0.0, 0.0, 0.0]),
                    1,
                    &[],
                    None,
                )
                .await
                .unwrap();
            engine.remove_from_indexes(key.as_ref()).await.unwrap();
            // No checkpoint — only the WAL should record this removal.
        }

        // Fresh replay must reflect the removal (vector gone from search,
        // not just from the KV layer, which `delete()` handles separately).
        let engine2 = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        let results = engine2
            .vector_search(&[1.0, 0.0, 0.0, 0.0], 5)
            .await
            .unwrap();
        assert!(
            results.iter().all(|r| r.key.as_ref() != key.as_ref()),
            "removed vector should not resurrect after WAL replay"
        );
    }

    #[tokio::test]
    async fn hard_deleted_vector_does_not_resurrect_after_checkpoint() {
        let dir = TempDir::new().unwrap();
        let key = Bytes::from_static(b"memory:vec-to-delete");

        {
            let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap();
            engine
                .store_memory_core(
                    key.clone(),
                    Bytes::from_static(b"{}"),
                    Some(vec![1.0, 0.0, 0.0, 0.0]),
                    1,
                    &[],
                    None,
                )
                .await
                .unwrap();
            engine.remove_from_indexes(key.as_ref()).await.unwrap();
            engine.checkpoint().await.unwrap(); // saves chunks + deleted_nodes, truncates WAL
        }

        let engine2 = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        let results = engine2
            .vector_search(&[1.0, 0.0, 0.0, 0.0], 5)
            .await
            .unwrap();
        assert!(
            results.iter().all(|r| r.key.as_ref() != key.as_ref()),
            "hard-deleted vector resurrected as a phantom after checkpoint + restart: {:?}",
            results
        );
    }

    #[tokio::test]
    async fn hard_deleted_vector_does_not_resurrect_after_delete_only_checkpoint() {
        // Regression test for the `is_dirty()` gating gap: insert+checkpoint
        // first (clears the dirty flag), THEN remove with no further insert
        // before the next checkpoint. Before `remove()` was fixed to mark
        // the index dirty, this second checkpoint would see `is_dirty() ==
        // false`, skip saving `deleted_nodes` entirely, and still truncate
        // the WAL -- silently losing the only durable record of the delete.
        let dir = TempDir::new().unwrap();
        let key = Bytes::from_static(b"memory:vec-to-delete-2");

        {
            let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap();
            engine
                .store_memory_core(
                    key.clone(),
                    Bytes::from_static(b"{}"),
                    Some(vec![1.0, 0.0, 0.0, 0.0]),
                    1,
                    &[],
                    None,
                )
                .await
                .unwrap();
            engine.checkpoint().await.unwrap(); // first checkpoint: clears the dirty flag

            engine.remove_from_indexes(key.as_ref()).await.unwrap();
            engine.checkpoint().await.unwrap(); // delete-only checkpoint
        }

        let engine2 = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        let results = engine2
            .vector_search(&[1.0, 0.0, 0.0, 0.0], 5)
            .await
            .unwrap();
        assert!(
            results.iter().all(|r| r.key.as_ref() != key.as_ref()),
            "hard-deleted vector resurrected after a delete-only checkpoint cycle: {:?}",
            results
        );
    }

    #[tokio::test]
    async fn wal_record_timestamp_matches_applied_memtable_timestamp() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        engine
            .put(Bytes::from_static(b"a"), Bytes::from_static(b"1"))
            .await
            .unwrap();
        engine
            .put(Bytes::from_static(b"a"), Bytes::from_static(b"2"))
            .await
            .unwrap();

        // Replay-from-scratch must land on the LAST write ("2"), proving the
        // WAL's on-disk order for this key matches the order the memtable
        // actually applied them in (both writes reserved+logged their
        // timestamp atomically, so replay can't reorder them).
        drop(engine);
        let engine2 = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        assert_eq!(
            engine2.get(b"a".as_slice()).await.unwrap(),
            Some(Bytes::from_static(b"2")),
        );
    }

    // NOTE: the brief's original version of this test made the HNSW index
    // directory read-only via `set_readonly(true)` to simulate a disk-full /
    // permission error during `save_dirty_chunks`. Verified flaky (in fact,
    // deterministically *not* failing) in the dev container: it runs as
    // root, and root ignores directory write-permission bits on most
    // filesystems, so `save_dirty_chunks` still succeeded and `checkpoint()`
    // returned `Ok`. Run 5x in a loop to confirm: 5/5 failed the
    // `result.is_err()` assertion.
    //
    // Falls back to the deterministic injection the brief suggested:
    // `HnswIndex::save_dirty_chunks` calls `std::fs::create_dir_all(dir)`
    // where `dir` is `data_dir/index`. `StorageEngine::new` already creates
    // that directory during startup, so after construction we swap it out
    // for a regular *file* at the same path. `create_dir_all` fails with
    // `AlreadyExists`/`NotADirectory` when the final path component exists
    // as a non-directory, regardless of uid — this fails the save the same
    // way for root and non-root alike.
    #[tokio::test]
    async fn checkpoint_skips_wal_truncation_when_an_index_save_fails() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        engine
            .store_memory_core(
                Bytes::from_static(b"memory:x"),
                Bytes::from_static(b"{}"),
                Some(vec![1.0, 0.0, 0.0, 0.0]),
                1,
                &[],
                None,
            )
            .await
            .unwrap();

        // Replace the HNSW index directory with a regular file so
        // `save_dirty_chunks`'s `create_dir_all` fails deterministically,
        // simulating a disk-full/permission error during checkpoint --
        // without relying on permission bits that root ignores.
        let hnsw_dir = dir.path().join("index");
        std::fs::remove_dir_all(&hnsw_dir).unwrap();
        std::fs::write(&hnsw_dir, b"not a directory").unwrap();

        // checkpoint() should surface the index-save error rather than
        // silently truncating the WAL anyway.
        let result = engine.checkpoint().await;

        // Restore the directory so TempDir can clean up (and so a fresh
        // engine can load/create indexes normally below).
        std::fs::remove_file(&hnsw_dir).unwrap();
        std::fs::create_dir_all(&hnsw_dir).unwrap();

        assert!(
            result.is_err(),
            "checkpoint must fail (and skip WAL truncation) when an index save fails"
        );

        // The WAL must still contain the write — replay after this failed
        // checkpoint must recover it.
        drop(engine);
        let engine2 = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        assert!(engine2.get(b"memory:x".as_slice()).await.unwrap().is_some());
    }

    /// The other half of the write-barrier defect: even when every index
    /// save succeeds, the pre-fix `checkpoint()` truncated the WAL without
    /// ever flushing the (in-memory-only) memtable to an SSTable first. A
    /// write that landed in the memtable but was never flushed would have
    /// its only durable record (the WAL entry) erased by `truncate()`, then
    /// vanish for good once the in-memory memtable is lost (e.g. on
    /// restart). This is the scenario the single-index-failure test above
    /// does not exercise, since that one fails before ever reaching the WAL
    /// truncation step.
    #[tokio::test]
    async fn checkpoint_flushes_memtable_before_truncating_wal() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        engine
            .store_memory_core(
                Bytes::from_static(b"memory:y"),
                Bytes::from_static(b"{}"),
                Some(vec![0.0, 1.0, 0.0, 0.0]),
                1,
                &[],
                None,
            )
            .await
            .unwrap();

        // No index-save failure this time -- checkpoint should succeed
        // outright, but must flush the memtable (to an SSTable) as part of
        // that success, not just truncate the WAL.
        engine.checkpoint().await.unwrap();

        // Drop the engine, discarding the in-memory memtable. If the write
        // was flushed to an SSTable before the WAL was truncated, the data
        // survives. If checkpoint() only truncated the WAL, the write is
        // gone: no WAL record to replay, no SSTable entry either.
        drop(engine);
        let engine2 = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        assert!(
            engine2.get(b"memory:y".as_slice()).await.unwrap().is_some(),
            "checkpoint() must flush the memtable to an SSTable before truncating the WAL, \
             or an in-memtable-only write is lost once the WAL is truncated"
        );
    }

    /// `checkpoint()`'s WAL-locked flush+truncate span does blocking file
    /// I/O (SSTable writes + fsync). Run directly in an `async fn` with no
    /// `.await` inside that span, this executes start-to-finish on
    /// whichever executor thread polls `checkpoint()`, without ever
    /// yielding -- so unrelated async work sharing that thread (e.g. a
    /// `current_thread` runtime, or a busy worker on a multi-thread one)
    /// is blocked alongside the writers waiting on the WAL lock, not just
    /// the writers themselves. Dispatching the span to `spawn_blocking`
    /// fixes this: awaiting the returned `JoinHandle` always yields at
    /// least once (the blocking closure runs on a separate thread and
    /// wakes the awaiting task via a channel, which cannot resolve
    /// synchronously within a single poll), giving the executor a chance
    /// to run other ready tasks while the flush is in flight.
    ///
    /// This test proves that yield happens: it races a task that
    /// increments a counter and calls `yield_now()` in a loop against
    /// `checkpoint()` on a `current_thread` runtime (`#[tokio::test]`'s
    /// default). If `checkpoint()` never yields, the ticker task cannot be
    /// scheduled even once before `checkpoint()` returns, since a
    /// single-threaded runtime only switches tasks at yield points.
    #[tokio::test]
    async fn checkpoint_flush_does_not_monopolize_the_executor_thread() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        engine
            .store_memory_core(
                Bytes::from_static(b"memory:yield"),
                Bytes::from_static(b"{}"),
                Some(vec![1.0, 1.0, 0.0, 0.0]),
                1,
                &[],
                None,
            )
            .await
            .unwrap();

        let yields = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let yields_task = Arc::clone(&yields);
        let ticker = tokio::spawn(async move {
            loop {
                yields_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
        });

        engine.checkpoint().await.unwrap();
        let yields_during_checkpoint = yields.load(std::sync::atomic::Ordering::SeqCst);
        ticker.abort();

        assert!(
            yields_during_checkpoint > 0,
            "checkpoint() ran its WAL-locked flush span start-to-finish without \
             yielding to the executor -- a concurrently-spawned task never got \
             scheduled even once, meaning the flush is blocking I/O running \
             directly on the async runtime thread instead of via spawn_blocking"
        );
    }

    /// Same defect as `checkpoint_skips_wal_truncation_when_an_index_save_fails`,
    /// but exercised through the actual *background* checkpoint loop
    /// (`start_background_tasks` in tasks.rs) rather than the manually
    /// triggered `checkpoint()` -- these are two independent code paths with
    /// separately duplicated logic, so passing the manual-checkpoint test
    /// does not prove the background loop's `all_saves_ok` gating works.
    ///
    /// Uses `start_paused = true` + `tokio::time::advance` (same idiom as
    /// `tasks::supervisor::tests`) to fast-forward past the loop's
    /// hard-coded 10s poll interval without a real-time wait.
    #[tokio::test(start_paused = true)]
    async fn background_checkpoint_loop_skips_wal_truncation_when_index_save_fails() {
        let dir = TempDir::new().unwrap();
        // Keep one task runnable while engine startup and the coordinator's
        // OS thread complete. Otherwise Tokio's paused clock may auto-advance
        // the checkpoint timer before this test installs its failure fixture.
        let fixture_ready = Arc::new(AtomicBool::new(false));
        let fixture_ready_task = Arc::clone(&fixture_ready);
        let clock_guard = tokio::spawn(async move {
            while !fixture_ready_task.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        });
        let mut cfg = test_cfg(dir.path().to_path_buf());
        // Force `should_checkpoint` true on the loop's very first tick,
        // independent of `checkpoint_interval`.
        cfg.max_wal_size = 1;
        let engine = StorageEngine::new(cfg).await.unwrap();

        engine
            .store_memory_core(
                Bytes::from_static(b"memory:bg"),
                Bytes::from_static(b"{}"),
                Some(vec![0.0, 0.0, 1.0, 0.0]),
                1,
                &[],
                None,
            )
            .await
            .unwrap();

        // Corrupt the HNSW index directory so the background loop's
        // save_dirty_chunks fails deterministically on its first tick,
        // regardless of uid (see the note on the sibling test above).
        let hnsw_dir = dir.path().join("index");
        std::fs::remove_dir_all(&hnsw_dir).unwrap();
        std::fs::write(&hnsw_dir, b"not a directory").unwrap();
        fixture_ready.store(true, Ordering::Release);
        clock_guard.await.unwrap();

        // Let the background task observe the corrupted directory, then
        // advance virtual time past its 10s poll interval so it runs.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        let attempts_before = engine.checkpoint_attempts.load(Ordering::Acquire);
        tokio::time::advance(std::time::Duration::from_secs(11)).await;
        for _ in 0..1_000 {
            if engine.checkpoint_attempts.load(Ordering::Acquire) > attempts_before {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(engine.checkpoint_attempts.load(Ordering::Acquire) > attempts_before);

        // Stop every background owner while the failure is still present.
        // This must fail without truncating the WAL, but it deterministically
        // joins the coordinator and maintenance tasks before the reopen.
        assert!(engine.graceful_shutdown().await.is_err());
        let wal_path = dir.path().join("wal/current.wal");
        assert!(std::fs::metadata(&wal_path).unwrap().len() > super::super::wal::WAL_HEADER_LEN);
        let wal_records: Vec<_> = super::super::wal::WAL::open(&wal_path)
            .unwrap()
            .iter()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(wal_records
            .iter()
            .any(|record| { record.key.as_ref() == b"memory:bg" && record.embedding.is_some() }));

        // Restore the directory so a fresh engine can load/create indexes.
        std::fs::remove_file(&hnsw_dir).unwrap();
        std::fs::create_dir_all(&hnsw_dir).unwrap();

        drop(engine);
        let engine2 = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        let results = engine2
            .vector_search(&[0.0, 0.0, 1.0, 0.0], 5)
            .await
            .unwrap();
        assert!(
            results
                .iter()
                .any(|r| r.key.as_ref() == b"memory:bg".as_slice()),
            "background checkpoint loop must not truncate the WAL when an index save fails -- \
             otherwise the vector is lost for good once the un-persisted HNSW chunk and the \
             truncated WAL both vanish, with nothing left to replay on restart"
        );
    }

    /// Regression test for REM-14 (`docs/PROJECT_REVIEW.md` §2.4 item 2):
    /// the background checkpoint loop stamps `last_checkpoint` on *every*
    /// attempt, success or failure. While the underlying problem persists
    /// that's harmless (the loop just retries on the same cadence), but it
    /// means a transient failure that clears up between ticks still has to
    /// wait a full `checkpoint_interval` for the next attempt, rather than
    /// retrying on the very next 10s poll -- the interval-based trigger
    /// "silently stops mattering" as a *prompt* retry mechanism once
    /// something has failed once.
    ///
    /// Isolates the time-based trigger from the WAL-size trigger (a huge
    /// `max_wal_size` never fires) and picks a `checkpoint_interval` (25s)
    /// that doesn't land exactly on the loop's fixed 10s poll tick, so a
    /// bug here is distinguishable: with the bug, the checkpoint attempt at
    /// t=30s (first tick where elapsed > 25s) that fails resets the clock,
    /// so the next attempt doesn't come until t=60s (elapsed > 25s again
    /// from t=30); fixed, the failed attempt leaves the clock alone, so the
    /// next attempt comes at the very next tick, t=40s (elapsed=40 > 25,
    /// still measured from t=0).
    ///
    /// Advances the paused clock in exact 10s increments (the loop's own
    /// poll granularity), yielding between each -- advancing past several
    /// tick boundaries in one jump isn't a reliable way to observe each
    /// individual tick's own timer registration (see the identical idiom
    /// in `tasks::supervisor::tests`).
    #[tokio::test(start_paused = true)]
    async fn background_checkpoint_loop_retries_promptly_after_a_failed_attempt() {
        let dir = TempDir::new().unwrap();
        let fixture_ready = Arc::new(AtomicBool::new(false));
        let fixture_ready_task = Arc::clone(&fixture_ready);
        let clock_guard = tokio::spawn(async move {
            while !fixture_ready_task.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        });
        let mut cfg = test_cfg(dir.path().to_path_buf());
        cfg.checkpoint_interval = Duration::from_secs(25);
        // Default max_wal_size (1 GB) never fires for this test's one tiny
        // write -- only the time-based trigger can cause a checkpoint here.
        let engine = StorageEngine::new(cfg).await.unwrap();

        engine
            .store_memory_core(
                Bytes::from_static(b"memory:retry"),
                Bytes::from_static(b"{}"),
                Some(vec![0.0, 1.0, 1.0, 0.0]),
                1,
                &[],
                None,
            )
            .await
            .unwrap();

        let hnsw_dir = dir.path().join("index");
        std::fs::remove_dir_all(&hnsw_dir).unwrap();
        std::fs::write(&hnsw_dir, b"not a directory").unwrap();
        fixture_ready.store(true, Ordering::Release);
        clock_guard.await.unwrap();

        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        // t=10s, t=20s: elapsed (10s, 20s) <= 25s interval -- no trigger yet.
        for _ in 0..2 {
            tokio::time::advance(Duration::from_secs(10)).await;
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
        }
        // t=30s: elapsed=30s > 25s -- first attempt fires and fails (index
        // dir is corrupted), since nothing has ever succeeded yet.
        tokio::time::advance(Duration::from_secs(10)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(
            engine.wal.size().await.unwrap() >= crate::engine::storage::wal::WAL_HEADER_LEN,
            "sanity check: the first attempt must fail without truncating the WAL"
        );

        // The underlying problem clears up between ticks.
        std::fs::remove_file(&hnsw_dir).unwrap();
        std::fs::create_dir_all(&hnsw_dir).unwrap();

        // t=40s: only one more 10s tick past the failed attempt at t=30s --
        // short of a full second 25s interval measured from t=30s (which
        // wouldn't elapse until t=55s), but enough for one more poll tick.
        // A prompt retry must pick this up now, not wait until t=55s+.
        tokio::time::advance(Duration::from_secs(10)).await;
        let mut observed_wal_size = engine.wal.size().await.unwrap();
        for _ in 0..1_000 {
            if observed_wal_size == crate::engine::storage::wal::WAL_HEADER_LEN {
                break;
            }
            tokio::task::yield_now().await;
            observed_wal_size = engine.wal.size().await.unwrap();
        }

        assert_eq!(
            observed_wal_size,
            crate::engine::storage::wal::WAL_HEADER_LEN,
            "background checkpoint loop must retry on the very next poll after a failed \
             attempt, not wait out a full checkpoint_interval from that failed attempt -- \
             last_checkpoint must only reset on success"
        );
    }

    // ── Record version ordering ──────────────────────────────────────────
    //
    // A record's version has to order writes across a memtable rotation and
    // across a restart, because that is what every duplicate-resolution site
    // relies on to tell a newer copy from an older one.

    /// Raise the version counter so the first copy of the key under test is
    /// written at a high number, which is what a restart-reset counter has to
    /// beat.
    async fn climb_the_version_counter(engine: &StorageEngine) {
        for i in 0..50 {
            engine
                .put(format!("filler:{i:03}"), Bytes::from_static(b"x"))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn a_rewrite_after_a_rotation_survives_compaction() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        climb_the_version_counter(&engine).await;
        engine.put("k", "first").await.unwrap();
        engine.flush().await.unwrap();

        // The rotation above is where the counter used to restart, so this
        // write carried a smaller number than the copy it supersedes.
        engine.put("k", "second").await.unwrap();
        engine.flush().await.unwrap();

        engine.compaction.compact_level(0).unwrap();

        assert_eq!(
            engine.get("k").await.unwrap(),
            Some(Bytes::from_static(b"second")),
            "compaction kept the copy written before the rotation"
        );
    }

    #[tokio::test]
    async fn a_rewrite_after_a_restart_survives_compaction() {
        let dir = TempDir::new().unwrap();

        {
            let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap();
            climb_the_version_counter(&engine).await;
            engine.put("k", "first").await.unwrap();
            // Checkpoint, not just flush: it truncates the WAL, so the reopened
            // engine cannot learn the old numbering by replaying it.
            engine.checkpoint().await.unwrap();
        }

        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        engine.put("k", "second").await.unwrap();
        engine.flush().await.unwrap();

        engine.compaction.compact_level(0).unwrap();

        assert_eq!(
            engine.get("k").await.unwrap(),
            Some(Bytes::from_static(b"second")),
            "compaction kept the copy written before the restart"
        );
    }

    #[tokio::test]
    async fn a_replayed_rewrite_beats_the_copy_it_supersedes() {
        // Both writes stay in the WAL (flush does not truncate it), so recovery
        // replays them in order. Replay admits a record only if its version
        // exceeds the one already held, so the later write is kept only when
        // the numbering actually orders the two.
        let dir = TempDir::new().unwrap();

        {
            let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap();
            climb_the_version_counter(&engine).await;
            engine.put("k", "first").await.unwrap();
            engine.flush().await.unwrap();
            engine.put("k", "second").await.unwrap();
        }

        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        assert_eq!(
            engine.get("k").await.unwrap(),
            Some(Bytes::from_static(b"second")),
            "replay discarded the later write because its version was smaller"
        );
    }

    #[tokio::test]
    async fn rotate_memtable_rejects_once_flush_backlog_is_full() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        // Manually saturate the pending-immutable-memtable backlog to
        // simulate a stalled flush loop (e.g. disk full) without needing
        // to actually fill up gigabytes of memtable data.
        for i in 0..MAX_PENDING_IMMUTABLE_MEMTABLES {
            let mt = MemTable::with_capacity(1024);
            let ts = mt.reserve_timestamp();
            mt.insert_with_timestamp(Bytes::from(format!("k{i}")), Bytes::from_static(b"v"), ts)
                .unwrap();
            engine
                .immutable_memtables
                .write()
                .push(Arc::new(ImmutableMemTable::from_memtable(mt)));
        }

        let result = engine.rotate_memtable().await;
        assert!(
            result.is_err(),
            "rotate_memtable should reject writes once the flush backlog is full"
        );
    }

    #[test]
    fn engine_config_default_text_field_is_content() {
        // The scan needs a field name; "content" matches StoredMemory's text
        // field, which is what REM-29 will index.
        assert_eq!(EngineConfig::default().text_field, "content");
    }

    /// Retiring a vector must be surgical: the record's other index entries
    /// are what `cleanup_archived` later uses to find it, so clearing them
    /// here would strand it permanently.
    #[tokio::test]
    async fn retire_vector_leaves_every_other_index_entry_intact() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        let key = Bytes::from_static(b"memory:to-retire");
        let neighbor = Bytes::from_static(b"memory:neighbor");
        engine
            .store_memory_core(
                key.clone(),
                Bytes::from_static(br#"{"content":"retire me"}"#),
                Some(vec![1.0, 0.0, 0.0, 0.0]),
                1_000,
                &["__archived__".to_string(), "keep-me".to_string()],
                None,
            )
            .await
            .unwrap();
        engine
            .add_edge(key.clone(), neighbor.clone(), None, None, 1_000)
            .await
            .unwrap();

        assert!(engine.get_vector(key.as_ref()).is_some());

        engine.retire_vector(key.as_ref()).await.unwrap();

        assert!(
            engine.get_vector(key.as_ref()).is_none(),
            "retired vector must be gone from the similarity index"
        );
        let hits = engine
            .vector_search(&[1.0, 0.0, 0.0, 0.0], 10)
            .await
            .unwrap();
        assert!(
            hits.iter().all(|hit| hit.key.as_ref() != key.as_ref()),
            "retired record must not be returned by similarity search"
        );

        assert!(
            engine.get(key.as_ref()).await.unwrap().is_some(),
            "the record itself must still be readable by key"
        );
        assert!(
            engine
                .time_range_query(0, u64::MAX, None)
                .unwrap()
                .iter()
                .any(|(_ts, k)| k.as_ref() == key.as_ref()),
            "the timestamp entry must survive -- cleanup_archived walks it"
        );
        assert_eq!(
            engine.tag_search_and(&["__archived__"]).unwrap(),
            vec![key.clone()],
            "tags must survive retirement"
        );
        assert_eq!(
            engine.get_neighbors(key.as_ref()).unwrap().len(),
            1,
            "graph edges must survive retirement"
        );
    }

    /// A caller archives without probing first, so retirement has to tolerate
    /// "nothing to retire" -- and a repeat must not skew the live count that
    /// bounds the search widening loop.
    #[tokio::test]
    async fn retire_vector_is_idempotent_and_tolerates_a_missing_vector() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        let key = Bytes::from_static(b"memory:retired-twice");
        engine
            .store_memory_core(
                key.clone(),
                Bytes::from_static(b"{}"),
                Some(vec![1.0, 0.0, 0.0, 0.0]),
                1_000,
                &[],
                None,
            )
            .await
            .unwrap();
        engine
            .store_memory_core(
                Bytes::from_static(b"memory:never-had-a-vector"),
                Bytes::from_static(b"{}"),
                None,
                1_000,
                &[],
                None,
            )
            .await
            .unwrap();

        engine.retire_vector(key.as_ref()).await.unwrap();
        let after_first = engine.vector_count();

        engine.retire_vector(key.as_ref()).await.unwrap();
        engine
            .retire_vector(b"memory:never-had-a-vector")
            .await
            .unwrap();
        engine
            .retire_vector(b"memory:no-such-record")
            .await
            .unwrap();

        assert_eq!(
            engine.vector_count(),
            after_first,
            "repeating a retirement must not change the live vector count"
        );
    }

    /// Retirement is written before it is applied, so a crash before the next
    /// checkpoint still leaves the record out of the index after replay.
    #[tokio::test]
    async fn retire_vector_survives_a_restart_without_a_checkpoint() {
        let dir = TempDir::new().unwrap();
        let key = Bytes::from_static(b"memory:retired-before-crash");

        {
            let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap();
            engine
                .store_memory_core(
                    key.clone(),
                    Bytes::from_static(b"{}"),
                    Some(vec![1.0, 0.0, 0.0, 0.0]),
                    1_000,
                    &[],
                    None,
                )
                .await
                .unwrap();
            engine.retire_vector(key.as_ref()).await.unwrap();
        } // dropped without a checkpoint -- only the WAL carries the retirement

        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        let hits = engine
            .vector_search(&[1.0, 0.0, 0.0, 0.0], 10)
            .await
            .unwrap();
        assert!(
            hits.iter().all(|hit| hit.key.as_ref() != key.as_ref()),
            "a retired vector must not resurrect after WAL replay"
        );
        assert!(
            engine.get(key.as_ref()).await.unwrap().is_some(),
            "replaying the retirement must not remove the record itself"
        );
    }

    /// The physical key carries its binding, so retirement routes to one
    /// partition's graph the same way insertion does.
    #[tokio::test]
    async fn retire_vector_affects_only_the_records_own_partition() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        let tenant = TenantId::new("acme").unwrap();
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());

        let finance_key = engine
            .put_with_embedding_partitioned(
                &finance,
                b"memory:finance",
                Bytes::from_static(b"finance"),
                Some(vec![1.0, 0.0, 0.0, 0.0]),
            )
            .await
            .unwrap();
        let product_key = engine
            .put_with_embedding_partitioned(
                &product,
                b"memory:product",
                Bytes::from_static(b"product"),
                Some(vec![1.0, 0.0, 0.0, 0.0]),
            )
            .await
            .unwrap();

        engine.retire_vector(finance_key.as_ref()).await.unwrap();

        let finance_hits = engine
            .vector_search_partitioned(
                &PartitionScope::single(finance),
                &[1.0, 0.0, 0.0, 0.0],
                10,
                None,
            )
            .await
            .unwrap();
        assert!(
            finance_hits.is_empty(),
            "the retired record's own partition must no longer return it"
        );

        let product_hits = engine
            .vector_search_partitioned(
                &PartitionScope::single(product),
                &[1.0, 0.0, 0.0, 0.0],
                10,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            product_hits
                .iter()
                .map(|hit| hit.key.clone())
                .collect::<Vec<_>>(),
            vec![product_key],
            "an unrelated partition must be unaffected"
        );
    }
}

#[cfg(test)]
mod content_scan_tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;
    use tempfile::TempDir;

    fn scan_cfg(data_dir: std::path::PathBuf, text_field: &str) -> EngineConfig {
        EngineConfig {
            data_dir,
            sync_writes: false,
            checkpoint_interval: Duration::from_secs(86400), // no auto-checkpoint
            text_field: text_field.to_string(),
            ..Default::default()
        }
    }

    /// Store a record shaped like a `StoredMemory` as far as the scan cares:
    /// a JSON object with a text field and a tags array.
    async fn put_record(engine: &StorageEngine, key: &str, content: &str, tags: &[&str], ts: u64) {
        let value = serde_json::json!({
            "content": content,
            "body": content,
            "tags": tags,
        })
        .to_string();
        engine.put(key.to_string(), value).await.unwrap();
        engine.add_timestamp(key.to_string(), ts).await.unwrap();
    }

    #[tokio::test]
    async fn scores_by_the_fraction_of_query_tokens_present() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(scan_cfg(dir.path().to_path_buf(), "content"))
            .await
            .unwrap();

        put_record(&engine, "memory:a", "rust ownership and borrowing", &[], 1).await;
        put_record(&engine, "memory:b", "rust async runtimes", &[], 2).await;
        put_record(&engine, "memory:c", "python decorators", &[], 3).await;

        let hits = engine
            .content_scan(&["rust", "ownership"], 10)
            .await
            .unwrap();

        assert_eq!(hits.len(), 2, "the non-matching record must be dropped");
        assert_eq!(hits[0].0, Bytes::from("memory:a"));
        assert_eq!(hits[0].1, 1.0);
        assert_eq!(hits[1].0, Bytes::from("memory:b"));
        assert_eq!(hits[1].1, 0.5);
    }

    #[tokio::test]
    async fn matches_only_the_configured_field() {
        // A token appearing in tags but not in the text field is a tag-index
        // hit, not a content hit. Matching the whole serialized record would
        // conflate the two.
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(scan_cfg(dir.path().to_path_buf(), "content"))
            .await
            .unwrap();

        put_record(&engine, "memory:a", "python decorators", &["rust"], 1).await;

        let hits = engine.content_scan(&["rust"], 10).await.unwrap();

        assert!(hits.is_empty());
    }

    #[tokio::test]
    async fn honours_a_non_default_text_field() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(scan_cfg(dir.path().to_path_buf(), "body"))
            .await
            .unwrap();

        put_record(&engine, "memory:a", "rust ownership", &[], 1).await;

        let hits = engine.content_scan(&["ownership"], 10).await.unwrap();

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, Bytes::from("memory:a"));
    }

    #[tokio::test]
    async fn matching_is_case_insensitive() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(scan_cfg(dir.path().to_path_buf(), "content"))
            .await
            .unwrap();

        put_record(&engine, "memory:a", "Rust Ownership", &[], 1).await;

        // Callers lowercase their tokens (services::search_engine::tokenize).
        let hits = engine.content_scan(&["rust"], 10).await.unwrap();

        assert_eq!(hits.len(), 1);
    }

    #[tokio::test]
    async fn truncates_to_the_limit() {
        // Distinct, deliberate timestamps and a single-token query so every
        // match ties at score 1.0 — this pins the tie-order behavior
        // documented on `content_scan`: truncation keeps the oldest matches,
        // not merely `hits.len() == 2` (which would pass for any two).
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(scan_cfg(dir.path().to_path_buf(), "content"))
            .await
            .unwrap();

        for i in 0..5 {
            put_record(&engine, &format!("memory:{i}"), "rust", &[], i as u64 + 1).await;
        }

        let hits = engine.content_scan(&["rust"], 2).await.unwrap();

        assert_eq!(hits.len(), 2);
        assert_eq!(
            hits[0].0,
            Bytes::from("memory:0"),
            "the oldest match survives truncation"
        );
        assert_eq!(
            hits[1].0,
            Bytes::from("memory:1"),
            "the second-oldest match survives truncation"
        );
    }

    #[tokio::test]
    async fn returns_nothing_for_an_empty_token_list() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(scan_cfg(dir.path().to_path_buf(), "content"))
            .await
            .unwrap();

        put_record(&engine, "memory:a", "rust", &[], 1).await;

        let hits = engine.content_scan(&[], 10).await.unwrap();

        assert!(
            hits.is_empty(),
            "no tokens means no match, not every record"
        );
    }

    #[tokio::test]
    async fn skips_records_that_are_not_json_objects() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(scan_cfg(dir.path().to_path_buf(), "content"))
            .await
            .unwrap();

        engine
            .put("memory:bad".to_string(), "not json")
            .await
            .unwrap();
        engine
            .add_timestamp("memory:bad".to_string(), 1)
            .await
            .unwrap();
        put_record(&engine, "memory:a", "rust", &[], 2).await;

        let hits = engine.content_scan(&["rust"], 10).await.unwrap();

        assert_eq!(hits.len(), 1, "a corrupt record must be skipped, not fatal");
        assert_eq!(hits[0].0, Bytes::from("memory:a"));
    }

    #[tokio::test]
    async fn empty_corpus_returns_no_hits() {
        // Structurally guarantees the "field never extracted" warning cannot
        // fire on an empty corpus: with no timestamp entries the scan loop
        // never runs, so the "examined" counter that gates the warning stays
        // zero.
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(scan_cfg(dir.path().to_path_buf(), "content"))
            .await
            .unwrap();

        let hits = engine.content_scan(&["rust"], 10).await.unwrap();

        assert!(hits.is_empty());
    }

    #[tokio::test]
    async fn warns_when_the_configured_text_field_never_matches() {
        // Mirrors `MemoryRepository`'s
        // `load_by_key_logs_and_returns_none_on_corrupt_json` pattern:
        // install a test-scoped subscriber so the `tracing::warn!` is
        // visible under `--nocapture`. A typo'd `text_field` means every
        // record's raw bytes still contain the query token (it is present
        // under the real field name), so the cheap pre-filter lets the
        // record through, but `extract_text_field` never finds it under the
        // configured name — exactly the condition the warning exists for.
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(scan_cfg(dir.path().to_path_buf(), "cotnent"))
            .await
            .unwrap();

        put_record(&engine, "memory:a", "rust ownership", &[], 1).await;

        let hits = engine.content_scan(&["rust"], 10).await.unwrap();

        assert!(
            hits.is_empty(),
            "a misconfigured text_field still degrades to no content matches, not an error"
        );
    }

    #[tokio::test]
    async fn matches_a_token_containing_a_literal_quote() {
        // Regression test: a token containing `"` is a contiguous substring
        // of the *parsed* field value, but JSON serializes that `"` as the
        // two-byte escape `\"`, so it is NOT a contiguous substring of the
        // *raw* bytes. Without the `prefilter_is_safe` guard, the cheap
        // raw-bytes pre-filter drops this record even though the
        // field-scoped match (on the parsed, un-escaped text) would accept
        // it — a real result, not just an ordering change.
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(scan_cfg(dir.path().to_path_buf(), "content"))
            .await
            .unwrap();

        put_record(&engine, "memory:a", "value a\"b end", &[], 1).await;

        let hits = engine.content_scan(&["a\"b"], 10).await.unwrap();

        assert_eq!(
            hits.len(),
            1,
            "a token containing a literal quote must still match the field it appears in"
        );
        assert_eq!(hits[0].0, Bytes::from("memory:a"));
    }
}

#[cfg(test)]
mod attr_engine_tests {
    use super::*;
    use crate::engine::attr::row::AttrRow;
    use crate::engine::attr::schema::{AttrSchema, SlotDef};
    use crate::engine::attr::value::{AttrType, AttrValue};

    fn schema() -> AttrSchema {
        AttrSchema {
            version: 1,
            slots: vec![
                SlotDef {
                    slot: 0,
                    name: "archived".into(),
                    ty: AttrType::Bool,
                    indexed: false,
                    retired: false,
                },
                SlotDef {
                    slot: 2,
                    name: "importance".into(),
                    ty: AttrType::F32,
                    indexed: true,
                    retired: false,
                },
            ],
        }
    }

    fn row(archived: bool, importance: f32) -> AttrRow {
        let mut r = AttrRow::new(1);
        r.set(0, AttrValue::Bool(archived));
        r.set(2, AttrValue::F32(importance));
        r
    }

    fn cfg(data_dir: std::path::PathBuf) -> EngineConfig {
        EngineConfig {
            data_dir,
            // `rows_survive_a_wal_replay` reopens the engine against the same
            // WAL file without a checkpoint. With `sync_writes: false` the
            // WAL's BufWriter is never guaranteed to reach disk before that
            // reopen: a background task still holds an `Arc` to the same
            // `Mutex<WAL>`, so the writer's flush-on-drop does not fire when
            // this scope's `engine` binding drops. `sync_writes: true`
            // matches the pattern used elsewhere in this file for
            // restart-across-WAL-replay tests (see `test_cfg` above).
            sync_writes: true,
            attr_schema: Some(schema()),
            ..Default::default()
        }
    }

    fn default_scope() -> crate::engine::storage::partition::PartitionScope {
        crate::engine::storage::partition::PartitionScope::legacy_default()
    }

    fn page_request(
        preds: &[crate::engine::attr::select::AttrPred],
        after: Option<crate::engine::index::IndexPosition>,
        descending: bool,
        limit: usize,
    ) -> crate::engine::attr::select::OrderedSelect<'_> {
        crate::engine::attr::select::OrderedSelect {
            preds,
            order_slot: 2,
            after,
            descending,
            limit,
            effort: 10_000,
        }
    }

    async fn engine_with_rows(count: usize) -> (tempfile::TempDir, StorageEngine) {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        for i in 0..count {
            engine
                .put_with_attrs(
                    Bytes::from(format!("memory:{i:04}")),
                    Bytes::from("payload"),
                    &row(false, i as f32),
                )
                .await
                .unwrap();
        }
        (dir, engine)
    }

    fn key_names(keys: &[Bytes]) -> Vec<String> {
        keys.iter()
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .collect()
    }

    #[tokio::test]
    async fn a_bounded_page_reads_rows_in_proportion_to_the_page_not_the_corpus() {
        // The claim this change exists to make, stated as a count rather than
        // a duration: the same page over a corpus four times the size costs
        // the same row reads.
        let (_small_dir, small) = engine_with_rows(50).await;
        let (_large_dir, large) = engine_with_rows(200).await;

        small.reset_attr_reads();
        let small_page = small
            .select_page_partitioned(&default_scope(), page_request(&[], None, false, 10))
            .await
            .unwrap();
        let small_reads = small.attr_reads();

        large.reset_attr_reads();
        let large_page = large
            .select_page_partitioned(&default_scope(), page_request(&[], None, false, 10))
            .await
            .unwrap();
        let large_reads = large.attr_reads();

        assert_eq!(small_page.keys.len(), 10);
        assert_eq!(large_page.keys.len(), 10);
        assert_eq!(
            small_reads, large_reads,
            "row reads must not grow with the corpus"
        );
        assert!(
            large_reads <= 20,
            "a page of 10 read {large_reads} rows; it should read about the page"
        );
    }

    #[tokio::test]
    async fn paging_a_whole_range_returns_every_record_exactly_once() {
        let (_dir, engine) = engine_with_rows(37).await;

        let mut seen: Vec<String> = Vec::new();
        let mut after = None;
        loop {
            let page = engine
                .select_page_partitioned(&default_scope(), page_request(&[], after, false, 7))
                .await
                .unwrap();
            seen.extend(key_names(&page.keys));
            after = page.next.clone();
            if page.exhausted {
                break;
            }
        }

        let unique: std::collections::HashSet<&String> = seen.iter().collect();
        assert_eq!(seen.len(), 37, "every record is returned");
        assert_eq!(unique.len(), 37, "no record is returned twice");
    }

    #[tokio::test]
    async fn a_descending_page_walk_is_the_ascending_one_reversed() {
        let (_dir, engine) = engine_with_rows(20).await;

        let mut ascending: Vec<String> = Vec::new();
        let mut after = None;
        loop {
            let page = engine
                .select_page_partitioned(&default_scope(), page_request(&[], after, false, 6))
                .await
                .unwrap();
            ascending.extend(key_names(&page.keys));
            after = page.next.clone();
            if page.exhausted {
                break;
            }
        }

        let mut descending: Vec<String> = Vec::new();
        let mut after = None;
        loop {
            let page = engine
                .select_page_partitioned(&default_scope(), page_request(&[], after, true, 6))
                .await
                .unwrap();
            descending.extend(key_names(&page.keys));
            after = page.next.clone();
            if page.exhausted {
                break;
            }
        }

        ascending.reverse();
        assert_eq!(descending, ascending);
    }

    #[tokio::test]
    async fn an_exhausted_range_and_a_bounded_walk_report_differently() {
        let (_dir, engine) = engine_with_rows(5).await;

        // Fewer records than asked for, and nothing left: complete.
        let exhausted = engine
            .select_page_partitioned(&default_scope(), page_request(&[], None, false, 50))
            .await
            .unwrap();
        assert_eq!(exhausted.keys.len(), 5);
        assert!(exhausted.exhausted);
        assert!(!exhausted.truncated);
        assert!(!exhausted.has_more());

        // A full page with more behind it: not exhausted, not truncated.
        let (_dir2, big) = engine_with_rows(50).await;
        let more = big
            .select_page_partitioned(&default_scope(), page_request(&[], None, false, 10))
            .await
            .unwrap();
        assert_eq!(more.keys.len(), 10);
        assert!(!more.exhausted);
        assert!(!more.truncated);
        assert!(more.has_more());
    }

    #[tokio::test]
    async fn a_walk_that_gives_up_at_its_effort_bound_says_so() {
        // Every record archived, and `archived` carries no index: the walk
        // examines candidates it can never return. Without the effort bound it
        // would walk the corpus; with it, the short page is reported as
        // truncated rather than as the complete answer.
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        for i in 0..300 {
            engine
                .put_with_attrs(
                    Bytes::from(format!("memory:{i:04}")),
                    Bytes::from("payload"),
                    &row(true, i as f32),
                )
                .await
                .unwrap();
        }

        let preds = [crate::engine::attr::select::AttrPred::Eq(
            0,
            AttrValue::Bool(false),
        )];
        let page = engine
            .select_page_partitioned(
                &default_scope(),
                crate::engine::attr::select::OrderedSelect {
                    preds: &preds,
                    order_slot: 2,
                    after: None,
                    descending: false,
                    limit: 10,
                    effort: 64,
                },
            )
            .await
            .unwrap();

        assert!(page.keys.is_empty());
        assert!(page.truncated, "it stopped early and must say so");
        assert!(
            !page.exhausted,
            "a truncated walk has not established exhaustion"
        );
    }

    /// A config that can re-derive rows on open and knows that an "archived"
    /// record (slot 0) must hold no ordering entry (slot 2).
    fn cfg_with_backfill(data_dir: std::path::PathBuf) -> EngineConfig {
        EngineConfig {
            attr_project: Some(Arc::new(|bytes: &[u8]| {
                // Payload is `archived:importance`, enough to stand in for a
                // domain record without dragging one into the engine's tests.
                let text = String::from_utf8_lossy(bytes).into_owned();
                let (archived, importance) = text.split_once(':')?;
                Some(row(archived == "1", importance.parse().ok()?))
            })),
            attr_index_exempt: Some(Arc::new(|r: &AttrRow| {
                if matches!(r.get(0), Some(AttrValue::Bool(true))) {
                    vec![2]
                } else {
                    Vec::new()
                }
            })),
            ..cfg(data_dir)
        }
    }

    async fn seed_for_backfill(engine: &StorageEngine) {
        for (i, archived) in [false, true, false, true, false].into_iter().enumerate() {
            let key = format!("memory:{i:04}");
            engine
                .put_with_attrs(
                    Bytes::from(key.clone()),
                    Bytes::from(format!("{}:{}", u8::from(archived), i)),
                    &row(archived, i as f32),
                )
                .await
                .unwrap();
            engine
                .add_timestamp(Bytes::from(key), i as u64)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn the_backfill_re_derives_every_row_and_retires_the_records_that_must_not_be_ordered() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(cfg_with_backfill(dir.path().to_path_buf()))
            .await
            .unwrap();
        seed_for_backfill(&engine).await;

        // Before: every record is in the ordering index, archived included --
        // this is the state an upgrade inherits.
        let before = engine
            .select_page_partitioned(&default_scope(), page_request(&[], None, false, 100))
            .await
            .unwrap();
        assert_eq!(before.keys.len(), 5);
        drop(engine);

        // A pending marker is what an upgrade leaves behind.
        std::fs::write(
            dir.path()
                .join(super::super::migrations::ATTR_BACKFILL_MARKER),
            b"",
        )
        .unwrap();

        let engine = StorageEngine::new(cfg_with_backfill(dir.path().to_path_buf()))
            .await
            .unwrap();
        let after = engine
            .select_page_partitioned(&default_scope(), page_request(&[], None, false, 100))
            .await
            .unwrap();

        assert_eq!(
            key_names(&after.keys),
            vec!["memory:0000", "memory:0002", "memory:0004"],
            "records the domain says must not be ordered are taken back out"
        );
        for i in [1usize, 3] {
            let key = format!("memory:{i:04}");
            assert!(
                engine.get_attrs(key.as_bytes()).await.unwrap().is_some(),
                "the row stays -- only the ordering entry goes"
            );
            assert!(
                engine.get(key.as_bytes()).await.unwrap().is_some(),
                "and the record itself stays readable"
            );
        }
        assert!(
            !dir.path()
                .join(super::super::migrations::ATTR_BACKFILL_MARKER)
                .exists(),
            "a completed backfill clears its marker"
        );
    }

    #[tokio::test]
    async fn a_repeated_backfill_reaches_the_same_state() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(cfg_with_backfill(dir.path().to_path_buf()))
            .await
            .unwrap();
        seed_for_backfill(&engine).await;
        drop(engine);

        // An upgrade interrupted before it could clear the marker runs the
        // whole thing again on the next boot; twice must equal once.
        let mut results = Vec::new();
        for _ in 0..2 {
            std::fs::write(
                dir.path()
                    .join(super::super::migrations::ATTR_BACKFILL_MARKER),
                b"",
            )
            .unwrap();
            let engine = StorageEngine::new(cfg_with_backfill(dir.path().to_path_buf()))
                .await
                .unwrap();
            let page = engine
                .select_page_partitioned(&default_scope(), page_request(&[], None, false, 100))
                .await
                .unwrap();
            results.push(key_names(&page.keys));
        }

        assert_eq!(results[0], results[1]);
        assert_eq!(
            results[1],
            vec!["memory:0000", "memory:0002", "memory:0004"]
        );
    }

    #[tokio::test]
    async fn retiring_an_ordering_entry_leaves_the_record_reachable_by_every_other_means() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        engine
            .put_with_attrs(
                Bytes::from("memory:keep"),
                Bytes::from("payload"),
                &row(false, 0.5),
            )
            .await
            .unwrap();
        engine
            .add_timestamp(Bytes::from("memory:keep"), 1234)
            .await
            .unwrap();
        engine
            .add_tags(Bytes::from("memory:keep"), &["alpha".to_string()])
            .await
            .unwrap();

        engine
            .retire_ordering_entry(b"memory:keep", 2)
            .await
            .unwrap();

        let page = engine
            .select_page_partitioned(&default_scope(), page_request(&[], None, false, 10))
            .await
            .unwrap();
        assert!(
            page.keys.is_empty(),
            "a retired record must not be offered by the ordered walk"
        );

        assert_eq!(
            engine.get(b"memory:keep").await.unwrap().unwrap(),
            Bytes::from("payload"),
            "the record itself stays readable"
        );
        assert!(
            engine.get_attrs(b"memory:keep").await.unwrap().is_some(),
            "the row stays intact -- it is what a sweep settles from"
        );
        assert_eq!(
            engine.time_range_query(0, u64::MAX, None).unwrap().len(),
            1,
            "maintenance must still be able to reach the record"
        );
        assert!(
            !engine.tag_search_and(&["alpha"]).unwrap().is_empty(),
            "tags are untouched"
        );
    }

    #[tokio::test]
    async fn retiring_an_ordering_entry_is_idempotent_and_tolerates_a_missing_entry() {
        let (_dir, engine) = engine_with_rows(3).await;

        engine
            .retire_ordering_entry(b"memory:0001", 2)
            .await
            .unwrap();
        engine
            .retire_ordering_entry(b"memory:0001", 2)
            .await
            .unwrap();
        engine
            .retire_ordering_entry(b"memory:no-such-record", 2)
            .await
            .unwrap();
        // A slot that carries no index at all: `archived` is slot 0.
        engine
            .retire_ordering_entry(b"memory:0002", 0)
            .await
            .unwrap();

        let page = engine
            .select_page_partitioned(&default_scope(), page_request(&[], None, false, 10))
            .await
            .unwrap();
        assert_eq!(
            key_names(&page.keys),
            vec!["memory:0000", "memory:0002"],
            "repeats and misses change nothing beyond the one real retirement"
        );
    }

    #[tokio::test]
    async fn retiring_an_ordering_entry_survives_a_restart_without_a_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
                .await
                .unwrap();
            for i in 0..3 {
                engine
                    .put_with_attrs(
                        Bytes::from(format!("memory:{i:04}")),
                        Bytes::from("payload"),
                        &row(false, i as f32),
                    )
                    .await
                    .unwrap();
            }
            engine
                .retire_ordering_entry(b"memory:0001", 2)
                .await
                .unwrap();
            // Deliberately no checkpoint: recovery must replay the retirement.
        }

        let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        let page = engine
            .select_page_partitioned(&default_scope(), page_request(&[], None, false, 10))
            .await
            .unwrap();

        assert_eq!(
            key_names(&page.keys),
            vec!["memory:0000", "memory:0002"],
            "the retirement must not come back on replay"
        );
    }

    #[tokio::test]
    async fn retiring_an_ordering_entry_affects_only_the_records_own_partition() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        let tenant = TenantId::new("acme").unwrap();
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());

        let finance_key = engine
            .put_with_attrs_partitioned(
                &finance,
                b"memory:shared-name",
                Bytes::from_static(b"finance"),
                &row(false, 0.5),
            )
            .await
            .unwrap();
        let product_key = engine
            .put_with_attrs_partitioned(
                &product,
                b"memory:shared-name",
                Bytes::from_static(b"product"),
                &row(false, 0.5),
            )
            .await
            .unwrap();

        engine
            .retire_ordering_entry(finance_key.as_ref(), 2)
            .await
            .unwrap();

        let product_page = engine
            .select_page_partitioned(
                &PartitionScope::single(product),
                page_request(&[], None, false, 10),
            )
            .await
            .unwrap();
        let finance_page = engine
            .select_page_partitioned(
                &PartitionScope::single(finance),
                page_request(&[], None, false, 10),
            )
            .await
            .unwrap();

        assert_eq!(product_page.keys, vec![product_key]);
        assert!(finance_page.keys.is_empty());
    }

    #[tokio::test]
    async fn payload_and_row_are_written_together() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        engine
            .put_with_attrs(
                Bytes::from("memory:a"),
                Bytes::from("payload"),
                &row(false, 0.7),
            )
            .await
            .unwrap();

        let back = engine.get_attrs(b"memory:a").await.unwrap().unwrap();
        assert_eq!(back.get(2), Some(AttrValue::F32(0.7)));
        assert_eq!(
            engine.get(b"memory:a").await.unwrap().unwrap(),
            Bytes::from("payload")
        );
    }

    #[tokio::test]
    async fn rows_survive_a_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
                .await
                .unwrap();
            engine
                .put_with_attrs(
                    Bytes::from("memory:b"),
                    Bytes::from("payload"),
                    &row(false, 0.3),
                )
                .await
                .unwrap();
            // Deliberately no checkpoint: recovery must rebuild from the WAL.
        }
        let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        let back = engine.get_attrs(b"memory:b").await.unwrap().unwrap();
        assert_eq!(back.get(2), Some(AttrValue::F32(0.3)));

        let hits = engine
            .attr_indexes()
            .unwrap()
            .range(
                2,
                AttrValue::F32(0.2).order_key(),
                AttrValue::F32(0.4).order_key(),
            )
            .unwrap();
        assert_eq!(
            hits.len(),
            1,
            "replay must rebuild the ordered index, not just the row"
        );
    }

    #[tokio::test]
    async fn partitioned_ordered_attribute_select_returns_only_authorized_keys() {
        use crate::engine::storage::partition::{
            PartitionBinding, PartitionId, PartitionScope, TenantId,
        };

        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        let tenant = TenantId::new("acme").unwrap();
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        let finance_key = engine
            .put_with_attrs_partitioned(
                &finance,
                b"memory:finance",
                Bytes::from_static(b"finance"),
                &row(false, 0.9),
            )
            .await
            .unwrap();
        let product_key = engine
            .put_with_attrs_partitioned(
                &product,
                b"memory:product",
                Bytes::from_static(b"product"),
                &row(false, 0.8),
            )
            .await
            .unwrap();
        let scope = PartitionScope::single(product);

        let hits = engine
            .select_ordered_partitioned(&scope, &[], 2, None)
            .await
            .unwrap();

        assert_eq!(hits, vec![product_key]);
        assert!(!hits.contains(&finance_key));
    }

    /// The background checkpoint loop (`tasks::start_background_tasks`) must
    /// save the attribute indexes before it truncates the WAL, exactly as
    /// the foreground `checkpoint()`/`save_all_indexes()` path already does.
    /// Without that, an un-persisted ordered-index entry has nothing left to
    /// rebuild from once the WAL that could replay it is gone -- a silent,
    /// permanent loss that only shows up as false negatives on a later
    /// range query.
    ///
    /// Drives the real background loop rather than asserting on its
    /// plumbing: `max_wal_size = 1` forces `should_checkpoint` true on the
    /// loop's first tick, `start_paused` + `tokio::time::advance` fast-
    /// forward past its hard-coded 10s poll interval (same idiom as
    /// `storage_recovery_tests::background_checkpoint_loop_skips_wal_truncation_when_index_save_fails`),
    /// and the assertion reopens a fresh engine afterwards -- with the WAL
    /// truncated, the only way the range hit can still be there is if the
    /// checkpoint actually wrote the attribute index to disk.
    #[tokio::test(start_paused = true)]
    async fn background_checkpoint_persists_attribute_indexes_before_truncating_wal() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = cfg(dir.path().to_path_buf());
        config.max_wal_size = 1;
        let engine = StorageEngine::new(config).await.unwrap();

        engine
            .put_with_attrs(
                Bytes::from("memory:bg-attr"),
                Bytes::from("payload"),
                &row(false, 0.42),
            )
            .await
            .unwrap();

        // Let the background checkpoint loop observe the write, then
        // advance virtual time past its 10s poll interval so it runs.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(std::time::Duration::from_secs(11)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        drop(engine);

        // Reopen against a WAL the checkpoint above truncated: any entry
        // the range query finds now came from the on-disk attribute index
        // files, not from WAL replay.
        let engine2 = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        let hits = engine2
            .attr_indexes()
            .unwrap()
            .range(
                2,
                AttrValue::F32(0.4).order_key(),
                AttrValue::F32(0.5).order_key(),
            )
            .unwrap();
        assert_eq!(
            hits.len(),
            1,
            "background checkpoint must persist the attribute indexes before truncating the \
             WAL, or an un-persisted ordered-index entry is lost for good once the WAL is gone"
        );
    }

    #[tokio::test]
    async fn attribute_compaction_keeps_engine_usable_for_memory_operations() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        engine
            .put_with_attrs(
                Bytes::from("memory:hot"),
                Bytes::from("payload-v1"),
                &row(false, 0.1),
            )
            .await
            .unwrap();
        engine.save_all_indexes().unwrap();

        engine
            .put_with_attrs(
                Bytes::from("memory:hot"),
                Bytes::from("payload-v2"),
                &row(false, 0.9),
            )
            .await
            .unwrap();
        engine.save_all_indexes().unwrap();

        assert_eq!(
            engine.get(b"memory:hot").await.unwrap(),
            Some(Bytes::from("payload-v2")),
            "ordinary reads must remain available after attr-index compaction"
        );
        engine
            .put(Bytes::from("memory:after-compaction"), Bytes::from("ok"))
            .await
            .unwrap();
        assert_eq!(
            engine.get(b"memory:after-compaction").await.unwrap(),
            Some(Bytes::from("ok")),
            "ordinary writes must not be rejected because attr-index compaction ran"
        );
    }

    #[tokio::test]
    async fn graceful_shutdown_does_not_publish_partial_attribute_compaction() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
                .await
                .unwrap();
            engine
                .put_with_attrs(
                    Bytes::from("memory:shutdown"),
                    Bytes::from("payload-v1"),
                    &row(false, 0.1),
                )
                .await
                .unwrap();
            engine.save_all_indexes().unwrap();

            engine
                .put_with_attrs(
                    Bytes::from("memory:shutdown"),
                    Bytes::from("payload-v2"),
                    &row(false, 0.9),
                )
                .await
                .unwrap();
            engine.graceful_shutdown().await.unwrap();
        }

        let reopened = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();
        let hits = reopened
            .attr_indexes()
            .unwrap()
            .range(2, 0, u64::MAX)
            .unwrap();
        assert_eq!(
            hits,
            vec![(
                AttrValue::F32(0.9).order_key(),
                Bytes::from_static(b"memory:shutdown")
            )],
            "shutdown must leave either the old manifest or a complete compacted manifest, never a partial one"
        );
    }

    /// A candidate the index still nominates but whose row is gone must be
    /// omitted, not returned half-resolved and not raised as an error. This
    /// is the window `delete_with_attrs` leaves open on purpose -- the index
    /// entry is dropped after the row's tombstone, so a surplus entry is the
    /// only failure mode (design D2).
    #[tokio::test]
    async fn a_candidate_whose_row_is_gone_is_omitted() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        for (key, importance) in [("memory:kept", 0.5f32), ("memory:orphaned", 0.6)] {
            engine
                .put_with_attrs(
                    Bytes::from(key),
                    Bytes::from("payload"),
                    &row(false, importance),
                )
                .await
                .unwrap();
        }

        // Tombstone one record's row while leaving its index entry standing --
        // the state a walk sees between the tombstone and `remove_key`.
        let orphaned = Bytes::from("memory:orphaned");
        let akey = crate::engine::attr::attr_key(&orphaned);
        {
            let ts = engine.memtable.read().reserve_timestamp();
            let memtable = engine.memtable.read();
            memtable.delete_with_timestamp(akey, ts).unwrap();
        }

        let found = engine.select(&[], None).await.unwrap();
        assert_eq!(
            found,
            vec![Bytes::from("memory:kept")],
            "a candidate with no row must drop out of the results silently"
        );
    }

    /// Deletion takes the record out of candidacy as well as out of storage.
    #[tokio::test]
    async fn a_deleted_record_stops_being_a_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        for key in ["memory:x", "memory:y"] {
            engine
                .put_with_attrs(Bytes::from(key), Bytes::from("payload"), &row(false, 0.5))
                .await
                .unwrap();
        }
        assert_eq!(engine.select(&[], None).await.unwrap().len(), 2);

        engine
            .delete_with_attrs(Bytes::from("memory:x"))
            .await
            .unwrap();

        assert_eq!(
            engine.select(&[], None).await.unwrap(),
            vec![Bytes::from("memory:y")],
            "a deleted record must not be nominated by the index it was removed from"
        );
        assert!(engine.get_attrs(b"memory:x").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_with_attrs_removes_both_halves() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
            .await
            .unwrap();

        engine
            .put_with_attrs(
                Bytes::from("memory:c"),
                Bytes::from("payload"),
                &row(false, 0.5),
            )
            .await
            .unwrap();
        engine
            .delete_with_attrs(Bytes::from("memory:c"))
            .await
            .unwrap();

        assert!(engine.get(b"memory:c").await.unwrap().is_none());
        assert!(
            engine.get_attrs(b"memory:c").await.unwrap().is_none(),
            "an orphan row would surface in select() as a key with no payload"
        );
    }

    /// Enforcement test for `open_attrs`'s `check_encodable()` gate
    /// (`init.rs`): deleting that call leaves this test passing against a
    /// schema that would otherwise silently truncate `bitmap_len` on cast
    /// and corrupt every row written with it.
    #[tokio::test]
    async fn a_schema_past_the_encodable_limit_refuses_to_open() {
        let dir = tempfile::tempdir().unwrap();
        let unencodable = AttrSchema {
            version: 1,
            slots: vec![SlotDef {
                slot: crate::engine::attr::row::MAX_ENCODABLE_SLOT + 1,
                name: "too_high".into(),
                ty: AttrType::U32,
                indexed: false,
                retired: false,
            }],
        };

        let result = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            attr_schema: Some(unencodable),
            ..Default::default()
        })
        .await;

        assert!(
            result.is_err(),
            "a schema past MAX_ENCODABLE_SLOT must refuse to open, not corrupt rows on write"
        );
    }

    /// Enforcement test for `open_attrs`'s `validate_against()` gate
    /// (`init.rs`): deleting that call leaves this test passing against a
    /// schema that would silently misparse every row already encoded
    /// against the persisted definition.
    #[tokio::test]
    async fn a_schema_that_changes_a_persisted_slot_type_refuses_to_open() {
        let dir = tempfile::tempdir().unwrap();

        // First open registers and persists `schema()` (slot 2, importance,
        // as F32).
        {
            let engine = StorageEngine::new(cfg(dir.path().to_path_buf()))
                .await
                .unwrap();
            drop(engine);
        }

        // Reopen with that same slot re-typed.
        let mut changed = schema();
        changed.slots[1].ty = AttrType::U32;
        let result = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            attr_schema: Some(changed),
            ..Default::default()
        })
        .await;

        assert!(
            result.is_err(),
            "a changed persisted slot type must refuse to open, matching \
             AttrSchema::validate_against"
        );
    }

    #[tokio::test]
    async fn an_engine_without_a_schema_still_works() {
        // Attribute support is optional; nothing may start requiring it.
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            ..Default::default()
        })
        .await
        .unwrap();
        engine
            .put(Bytes::from("k"), Bytes::from("v"))
            .await
            .unwrap();
        assert!(engine.get_attrs(b"k").await.unwrap().is_none());
        assert!(engine.attr_indexes().is_none());

        // `put_with_attrs` falls back to a plain `put` when no schema is
        // registered -- the caller's row is silently dropped, by design.
        // Assert this so the drop stays a deliberate, documented choice
        // rather than something a future "fix" accidentally removes.
        engine
            .put_with_attrs(Bytes::from("k2"), Bytes::from("v2"), &row(false, 0.1))
            .await
            .unwrap();
        assert_eq!(
            engine.get(b"k2").await.unwrap(),
            Some(Bytes::from("v2")),
            "the payload must still be stored via the put fallback"
        );
        assert!(
            engine.get_attrs(b"k2").await.unwrap().is_none(),
            "the row must be silently dropped when no schema is registered"
        );
    }
}

// The acceptance and scan-equivalence coverage below lives in-crate rather
// than in `tests/attr_acceptance.rs` because `payload_reads`/`attr_reads`
// and their accessors are `#[cfg(test)]`: an integration test in `tests/`
// is compiled as a separate crate against the library built *without* the
// `test` cfg, so those items would not exist there to call. This module has
// no production caller of its own (spec §11.1) -- it exists to keep the
// substrate honest.
#[cfg(test)]
mod attr_acceptance_tests {
    use super::*;
    use crate::engine::attr::select::AttrPred;
    use crate::engine::attr::value::AttrValue;
    use crate::services::attrs::{
        memory_schema, project, SLOT_ARCHIVED, SLOT_CREATED_AT, SLOT_IMPORTANCE,
    };
    use crate::services::types::{MemoryType, StoredMemory, StoredMetadata};
    use std::ops::Bound;

    fn stored(i: u32, importance: f32, archived: bool) -> StoredMemory {
        StoredMemory {
            id: uuid::Uuid::from_u128(i as u128),
            content: format!("record {i}"),
            memory_type: MemoryType::ShortTerm,
            metadata: StoredMetadata {
                created_at: i as u64,
                updated_at: i as u64,
                accessed_at: i as u64,
                access_count: 0,
                source: None,
                tags: vec![],
                importance,
                emotional_valence: 0.0,
                arousal: 0.0,
                health: 100.0,
                last_recalled_at: None,
                flashbulb_until: None,
                ttl: None,
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived,
        }
    }

    async fn corpus(n: u32) -> (tempfile::TempDir, StorageEngine, Vec<StoredMemory>) {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: false,
            attr_schema: Some(memory_schema()),
            ..Default::default()
        })
        .await
        .unwrap();

        let mut all = Vec::new();
        for i in 0..n {
            // A spread of importances, and every 10th record archived.
            let s = stored(i, (i % 100) as f32 / 100.0, i % 10 == 0);
            let key = format!("memory:{}", s.id);
            engine
                .store_memory_core(
                    key,
                    serde_json::to_vec(&s).unwrap(),
                    None,
                    s.metadata.created_at,
                    &[],
                    Some(&project(&s)),
                )
                .await
                .unwrap();
            all.push(s);
        }
        (dir, engine, all)
    }

    #[tokio::test]
    async fn selection_never_deserializes_a_non_matching_record() {
        // The acceptance criterion, made executable.
        let (_d, engine, _all) = corpus(500).await;
        engine.reset_payload_reads();

        let hits = engine
            .select(
                &[
                    AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false)),
                    AttrPred::Range(
                        SLOT_IMPORTANCE,
                        Bound::Included(AttrValue::F32(0.90)),
                        Bound::Unbounded,
                    ),
                ],
                None,
            )
            .await
            .unwrap();

        assert!(!hits.is_empty(), "the fixture must produce some matches");
        assert_eq!(
            engine.payload_reads(),
            0,
            "select() read {} payloads; it must decide from rows alone",
            engine.payload_reads()
        );
    }

    #[tokio::test]
    async fn select_agrees_with_a_full_deserializing_scan() {
        // The anti-rot mechanism: with no production caller, this is what
        // keeps the substrate honest against real semantics (spec §11.1).
        let (_d, engine, all) = corpus(500).await;

        let expected: std::collections::HashSet<String> = all
            .iter()
            .filter(|s| !s.archived && s.metadata.importance >= 0.90)
            .map(|s| format!("memory:{}", s.id))
            .collect();

        let hits = engine
            .select(
                &[
                    AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false)),
                    AttrPred::Range(
                        SLOT_IMPORTANCE,
                        Bound::Included(AttrValue::F32(0.90)),
                        Bound::Unbounded,
                    ),
                ],
                None,
            )
            .await
            .unwrap();

        let actual: std::collections::HashSet<String> = hits
            .iter()
            .map(|k| String::from_utf8_lossy(k).to_string())
            .collect();

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn selection_survives_a_checkpoint_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = EngineConfig {
            data_dir: dir.path().to_path_buf(),
            sync_writes: true,
            attr_schema: Some(memory_schema()),
            ..Default::default()
        };
        let expected_key;
        {
            let engine = StorageEngine::new(cfg.clone()).await.unwrap();
            let s = stored(1, 0.95, false);
            expected_key = format!("memory:{}", s.id);
            engine
                .store_memory_core(
                    expected_key.clone(),
                    serde_json::to_vec(&s).unwrap(),
                    None,
                    1,
                    &[],
                    Some(&project(&s)),
                )
                .await
                .unwrap();
            engine.checkpoint().await.unwrap();
            engine.graceful_shutdown().await.unwrap();
        }

        let engine = StorageEngine::new(cfg).await.unwrap();
        let hits = engine
            .select(
                &[AttrPred::Range(
                    SLOT_IMPORTANCE,
                    Bound::Included(AttrValue::F32(0.9)),
                    Bound::Unbounded,
                )],
                None,
            )
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(String::from_utf8_lossy(&hits[0]), expected_key);
    }

    #[tokio::test]
    async fn an_eq_predicate_on_a_non_first_indexed_slot_narrows_row_lookups() {
        // Carry-forward from Task 8's review: the access-path tests in
        // `select.rs` can only assert on *output*, and output alone cannot
        // tell a genuine tier-2 (targeted) selection apart from a
        // completely broken one that falls through to tier-3 (the first
        // indexed slot's full range, used as a key enumerator) -- every
        // candidate either path produces is verified against its row before
        // being returned, so both would still return the same, correct
        // answer. `attr_reads` (get_attrs calls) is what makes that
        // difference observable: the number of rows *verified*, not just
        // the rows returned.
        //
        // SLOT_IMPORTANCE is the schema's first indexed slot -- exactly what
        // `schema.indexed_slots().next()` walks in the tier-3 fallback. This
        // test targets SLOT_CREATED_AT instead, which every record here
        // holds a distinct value for, so a correct tier-2 selection walks
        // the index down to a single order_key and verifies exactly one
        // row, while a tier-3 fallback would enumerate and verify the whole
        // corpus through SLOT_IMPORTANCE.
        const N: u32 = 2000;
        let (_d, engine, all) = corpus(N).await;
        let target = &all[777];
        engine.reset_attr_reads();

        let hits = engine
            .select(
                &[AttrPred::Eq(
                    SLOT_CREATED_AT,
                    AttrValue::U64(target.metadata.created_at),
                )],
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            hits,
            vec![Bytes::from(format!("memory:{}", target.id))],
            "the fixture's created_at values are unique, so exactly one record must match"
        );

        // A tier-3 fallback walking the whole SLOT_IMPORTANCE index would
        // verify on the order of N rows (2000 here). A genuine tier-2
        // selection on a unique-valued slot verifies exactly one. N / 20
        // sits an order of magnitude above the real count and comfortably
        // below what a full-index fallback would produce, so this bound
        // catches a broken access path without pinning an exact count.
        let reads = engine.attr_reads();
        assert!(
            reads < (N / 20) as usize,
            "select() performed {reads} attribute-row lookups for a unique-valued Eq \
             predicate; a targeted access path should need only a handful, not something \
             approaching the corpus size ({N}) -- this is what a tier-3 fallback would look \
             like"
        );
    }
}
