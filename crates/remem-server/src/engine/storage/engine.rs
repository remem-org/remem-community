//! Storage engine: The main interface to the LSM-tree storage
#![allow(dead_code)]
//!
//! The storage engine coordinates:
//! - WAL for durability
//! - MemTable for recent writes
//! - SSTables for persistent storage
//! - Compaction for space reclamation

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::compaction::{CompactionConfig, CompactionManager};
use super::memtable::{ImmutableMemTable, MemTable};
use super::sstable::BlockCache;
use super::wal::{WalRecord, WAL};
use crate::engine::error::{Result, StorageError};
use crate::engine::index::{
    BTreeIndex, CsrGraph, EdgeMetadata, GraphIndex, HnswConfig, HnswIndex, InvertedIndex,
    SegmentedBTreeIndex, SegmentedCsrGraph, SegmentedInvertedIndex, TraversalResult,
};
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
        }
    }
}

impl VectorConfig {
    /// Create a new vector config with custom dimension
    pub fn with_dimension(dim: usize) -> Self {
        Self {
            dimension: dim,
            ..Default::default()
        }
    }

    /// Set the distance metric
    pub fn metric(mut self, metric: DistanceMetric) -> Self {
        self.metric = metric;
        self
    }

    /// Enable or disable vector search
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

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

impl GraphIndexConfig {
    /// Enable or disable graph index
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set whether the graph is directed
    pub fn directed(mut self, directed: bool) -> Self {
        self.directed = directed;
        self
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

impl TimeSeriesConfig {
    /// Enable or disable time-series index
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
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

impl TagIndexConfig {
    /// Enable or disable tag index
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set case sensitivity
    pub fn lowercase(mut self, lowercase: bool) -> Self {
        self.lowercase = lowercase;
        self
    }
}

/// Configuration for the storage engine
#[derive(Debug, Clone)]
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
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            memtable_size: 256 * 1024 * 1024,   // 256 MB
            block_cache_size: 64 * 1024 * 1024, // 64 MB
            sync_writes: false,                 // Batch syncs for performance
            compaction: CompactionConfig::default(),
            vector: VectorConfig::default(),
            graph: GraphIndexConfig::default(),
            time_series: TimeSeriesConfig::default(),
            tag_index: TagIndexConfig::default(),
            checkpoint_interval: std::time::Duration::from_secs(300), // 5 minutes
            max_wal_size: 1024 * 1024 * 1024,                         // 1 GB
        }
    }
}

/// The main storage engine
pub struct StorageEngine {
    /// Configuration
    config: EngineConfig,
    /// Active MemTable for writes
    memtable: Arc<RwLock<MemTable>>,
    /// Immutable MemTables waiting to be flushed
    immutable_memtables: Arc<RwLock<Vec<Arc<ImmutableMemTable>>>>,
    /// Write-ahead log
    wal: Arc<Mutex<WAL>>,
    /// Compaction manager (handles SSTable levels)
    compaction: Arc<CompactionManager>,
    /// Block cache
    cache: Arc<BlockCache>,
    /// HNSW vector index (optional)
    hnsw_index: Option<Arc<HnswIndex>>,
    /// Graph index (CSR or Kuzu, depending on feature flag)
    graph_index: Option<Arc<RwLock<GraphIndex>>>,
    /// Segmented B+Tree time-series index (optional)
    time_series_index: Option<Arc<RwLock<SegmentedBTreeIndex>>>,
    /// Segmented inverted index for tags/text (optional)
    tag_index: Option<Arc<RwLock<SegmentedInvertedIndex>>>,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
    /// Background flush task handle
    flush_handle: Mutex<Option<JoinHandle<()>>>,
    /// Background compaction task handle
    compaction_handle: Mutex<Option<JoinHandle<()>>>,
    /// Channel to trigger flushes
    flush_tx: mpsc::Sender<()>,
}

impl StorageEngine {
    /// Create a new storage engine
    pub async fn new(config: EngineConfig) -> Result<Self> {
        // Create data directories
        std::fs::create_dir_all(&config.data_dir)?;
        std::fs::create_dir_all(config.data_dir.join("wal"))?;
        std::fs::create_dir_all(config.data_dir.join("sstables"))?;
        std::fs::create_dir_all(config.data_dir.join("index"))?;

        // Open KV layer (cache, compaction, WAL, memtable)
        let kv = super::init::open_kv_layer(&config)?;

        // Open secondary indexes (HNSW, graph, time-series, tag)
        // Done before WAL replay so the replay can populate them.
        let idx = super::init::open_indexes(&config)?;

        // Replay WAL to recover state since the last checkpoint
        super::recovery::replay_wal(
            &kv.wal_path,
            &kv.memtable,
            idx.hnsw.as_ref(),
            idx.time_series.as_ref(),
            idx.tag.as_ref(),
            idx.graph.as_ref(),
        )?;

        let immutable_memtables = Arc::new(RwLock::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (flush_tx, flush_rx) = mpsc::channel(16);

        // Start background flush / compaction / checkpoint tasks
        let (flush_handle, compaction_handle) = super::tasks::start_background_tasks(
            Arc::clone(&immutable_memtables),
            Arc::clone(&kv.memtable),
            Arc::clone(&kv.compaction),
            Arc::clone(&kv.wal),
            idx.hnsw.clone(),
            idx.graph.clone(),
            idx.time_series.clone(),
            idx.tag.clone(),
            Arc::clone(&shutdown),
            config.clone(),
            flush_rx,
        );

        Ok(Self {
            config,
            memtable: kv.memtable,
            immutable_memtables,
            wal: kv.wal,
            compaction: kv.compaction,
            cache: kv.cache,
            hnsw_index: idx.hnsw,
            graph_index: idx.graph,
            time_series_index: idx.time_series,
            tag_index: idx.tag,
            shutdown,
            flush_handle: Mutex::new(Some(flush_handle)),
            compaction_handle: Mutex::new(Some(compaction_handle)),
            flush_tx,
        })
    }

    /// Open an existing storage engine or create a new one
    pub async fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let config = EngineConfig {
            data_dir: data_dir.as_ref().to_path_buf(),
            ..Default::default()
        };
        Self::new(config).await
    }

    /// Insert a key-value pair
    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let key = key.into();
        let value = value.into();

        // Write to WAL first
        let _timestamp = {
            let mut wal = self.wal.lock();
            let memtable = self.memtable.read();
            let ts = memtable.current_timestamp();
            wal.append(&WalRecord::insert(key.clone(), value.clone(), ts))?;
            if self.config.sync_writes {
                wal.sync()?;
            }
            ts
        };

        // Insert into MemTable, retrying after rotation if full
        loop {
            let result = {
                let memtable = self.memtable.read();
                memtable.insert(key.clone(), value.clone())
            };

            match result {
                Ok(_) => {
                    // Check if we should flush after successful insert
                    let should_flush = self.memtable.read().is_full();
                    if should_flush {
                        self.rotate_memtable().await?;
                    }
                    return Ok(());
                }
                Err(StorageError::MemTableFull { .. }) => {
                    // MemTable is full, rotate and retry
                    self.rotate_memtable().await?;
                    // Loop will retry the insert with the new memtable
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Delete a key
    pub async fn delete(&self, key: impl Into<Bytes>) -> Result<()> {
        let key = key.into();

        // Write to WAL first
        {
            let mut wal = self.wal.lock();
            let memtable = self.memtable.read();
            let ts = memtable.current_timestamp();
            wal.append(&WalRecord::delete(key.clone(), ts))?;
            if self.config.sync_writes {
                wal.sync()?;
            }
        }

        // Insert tombstone into MemTable, retrying after rotation if full
        loop {
            let result = {
                let memtable = self.memtable.read();
                memtable.delete(key.clone())
            };

            match result {
                Ok(_) => {
                    // Check if we should flush after successful delete
                    let should_flush = self.memtable.read().is_full();
                    if should_flush {
                        self.rotate_memtable().await?;
                    }
                    return Ok(());
                }
                Err(StorageError::MemTableFull { .. }) => {
                    // Memtable is full, rotate and retry
                    self.rotate_memtable().await?;
                    // Loop will retry the delete with the new memtable
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Get a value by key
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let key = key.as_ref();

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

    /// Rotate the active MemTable (make it immutable and create a new one)
    async fn rotate_memtable(&self) -> Result<()> {
        let old_memtable = {
            let mut memtable = self.memtable.write();
            let old = std::mem::replace(
                &mut *memtable,
                MemTable::with_capacity(self.config.memtable_size),
            );
            old
        };

        // Add to immutable list
        {
            let mut immutable = self.immutable_memtables.write();
            immutable.push(Arc::new(ImmutableMemTable::from_memtable(old_memtable)));
        }

        // Trigger background flush
        let _ = self.flush_tx.send(()).await;

        Ok(())
    }

    /// Flush all data to disk
    pub async fn flush(&self) -> Result<()> {
        // Rotate current memtable to immutable
        self.rotate_memtable().await?;

        // Sync WAL first
        {
            let mut wal = self.wal.lock();
            wal.sync()?;
        }

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

    /// Force compaction
    pub async fn compact(&self) -> Result<()> {
        while let Some(level) = self.compaction.needs_compaction() {
            self.compaction.compact_level(level)?;
        }
        Ok(())
    }

    /// Get storage statistics
    pub fn stats(&self) -> StorageStats {
        let memtable_size = self.memtable.read().size();
        let immutable_count = self.immutable_memtables.read().len();
        let level_stats = self.compaction.stats();
        let cache_stats = self.cache.stats();
        let vector_count = self.hnsw_index.as_ref().map(|idx| idx.len()).unwrap_or(0);
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
        let tag_token_count = self
            .tag_index
            .as_ref()
            .map(|idx| idx.read().all_tokens().len())
            .unwrap_or(0);

        StorageStats {
            memtable_size,
            immutable_memtables: immutable_count,
            level_stats,
            cache_entries: cache_stats.entries,
            cache_size: cache_stats.size_bytes,
            vector_count,
            vector_enabled: self.hnsw_index.is_some(),
            graph_node_count,
            graph_edge_count,
            graph_enabled: self.graph_index.is_some(),
            time_series_count,
            time_series_enabled: self.time_series_index.is_some(),
            tag_doc_count,
            tag_token_count,
            tag_enabled: self.tag_index.is_some(),
        }
    }

    // ==================== Vector Operations ====================

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

        // Write to WAL first (with embedding if present)
        let _timestamp = {
            let mut wal = self.wal.lock();
            let memtable = self.memtable.read();
            let ts = memtable.current_timestamp();

            let record = if let Some(ref emb) = embedding {
                WalRecord::insert_with_embedding(key.clone(), value.clone(), ts, emb.clone())
            } else {
                WalRecord::insert(key.clone(), value.clone(), ts)
            };

            wal.append(&record)?;
            if self.config.sync_writes {
                wal.sync()?;
            }
            ts
        };

        // Insert into MemTable, retrying after rotation if full
        loop {
            let result = {
                let memtable = self.memtable.read();
                memtable.insert(key.clone(), value.clone())
            };

            match result {
                Ok(_) => {
                    // Check if we should flush after successful insert
                    let should_flush = self.memtable.read().is_full();
                    if should_flush {
                        self.rotate_memtable().await?;
                    }
                    break;
                }
                Err(StorageError::MemTableFull { .. }) => {
                    // Memtable is full, rotate and retry
                    self.rotate_memtable().await?;
                    // Loop will retry the insert with the new memtable
                }
                Err(e) => return Err(e),
            }
        }

        // Index embedding if provided and HNSW is enabled
        if let (Some(embedding), Some(index)) = (embedding, &self.hnsw_index) {
            index.insert(key, embedding)?;
        }

        Ok(())
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
    ) -> Result<()> {
        let key: Bytes = key.into();
        let value: Bytes = value.into();

        // ── Step 1: single WAL lock, single fsync ────────────────────────────
        {
            let mut wal = self.wal.lock();
            let ts = self.memtable.read().current_timestamp();

            let kv_record = if let Some(ref emb) = embedding {
                WalRecord::insert_with_embedding(key.clone(), value.clone(), ts, emb.clone())
            } else {
                WalRecord::insert(key.clone(), value.clone(), ts)
            };

            let records = [
                kv_record,
                WalRecord::set_timestamp(key.clone(), timestamp, ts),
                WalRecord::add_tags(key.clone(), tags.to_vec(), ts),
            ];
            wal.append_batch(&records)?;
            if self.config.sync_writes {
                wal.sync()?;
            }
        }

        // ── Step 2: apply to KV memtable ─────────────────────────────────────
        loop {
            let result = {
                let memtable = self.memtable.read();
                memtable.insert(key.clone(), value.clone())
            };
            match result {
                Ok(_) => {
                    if self.memtable.read().is_full() {
                        self.rotate_memtable().await?;
                    }
                    break;
                }
                Err(StorageError::MemTableFull { .. }) => {
                    self.rotate_memtable().await?;
                }
                Err(e) => return Err(e),
            }
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
                hnsw.insert(key, emb)?;
                Ok::<_, StorageError>(())
            }))
        } else {
            None
        };

        // Run timestamp + tag inline (fast)
        if let Some(index) = &self.time_series_index {
            index.read().insert(timestamp, key.clone())?;
        }
        if let Some(index) = &self.tag_index {
            index.read().add_tags(key.clone(), tags)?;
        }

        // Await HNSW (was running concurrently on thread pool)
        if let Some(task) = hnsw_task {
            task.await
                .map_err(|e| StorageError::Io(std::io::Error::other(e)))??;
        }

        Ok(())
    }

    /// Search for similar vectors
    ///
    /// Returns a list of (key, similarity_score) pairs sorted by similarity.
    /// The score interpretation depends on the distance metric:
    /// - L2: Lower is more similar
    /// - Cosine: Lower distance means higher similarity (cosine distance = 1 - cosine similarity)
    /// - DotProduct: Negated, so lower is higher dot product
    pub async fn vector_search(&self, query: &[f32], k: usize) -> Result<Vec<VectorSearchResult>> {
        self.vector_search_with_ef(query, k, None).await
    }

    /// Search for similar vectors with custom ef parameter
    ///
    /// The ef parameter controls the search quality/speed tradeoff.
    /// Higher ef = better recall but slower search.
    pub async fn vector_search_with_ef(
        &self,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<VectorSearchResult>> {
        let Some(index) = &self.hnsw_index else {
            return Err(StorageError::InvalidArgument(
                "Vector search is not enabled".into(),
            ));
        };

        let results = if let Some(ef) = ef {
            index.search_with_ef(query, k, ef)?
        } else {
            index.search(query, k)?
        };

        // Optionally fetch values from storage
        let mut search_results = Vec::with_capacity(results.len());
        for (key, distance) in results {
            search_results.push(VectorSearchResult {
                key,
                distance,
                value: None, // Don't fetch by default for performance
            });
        }

        Ok(search_results)
    }

    /// Search for similar vectors and fetch their values
    pub async fn vector_search_with_values(
        &self,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<VectorSearchResult>> {
        let mut results = self.vector_search(query, k).await?;

        // Fetch values for each result
        for result in &mut results {
            result.value = self.get(&result.key).await?;
        }

        Ok(results)
    }

    /// Get the number of vectors in the index
    pub fn vector_count(&self) -> usize {
        self.hnsw_index.as_ref().map(|idx| idx.len()).unwrap_or(0)
    }

    /// Check if vector search is enabled
    pub fn vector_enabled(&self) -> bool {
        self.hnsw_index.is_some()
    }

    /// Save the HNSW index to disk
    pub fn save_vector_index(&self) -> Result<()> {
        if let Some(index) = &self.hnsw_index {
            if index.is_dirty() {
                let index_dir = self.config.data_dir.join("index");
                index.save_dirty_chunks(&index_dir)?;
                tracing::info!("Saved HNSW index chunks ({} vectors)", index.len());
            }
        }
        Ok(())
    }

    // ==================== Graph Operations ====================

    /// Add an edge between two nodes
    ///
    /// Writes to WAL first for durability, then adds to graph index.
    /// If an edge with the same (source, target, edge_type) already exists,
    /// the weight is updated instead of creating a duplicate.
    pub fn add_edge(
        &self,
        source: impl Into<Bytes>,
        target: impl Into<Bytes>,
        edge_type: Option<String>,
        weight: Option<f32>,
    ) -> Result<()> {
        let Some(index) = &self.graph_index else {
            return Err(StorageError::InvalidArgument(
                "Graph index is not enabled".into(),
            ));
        };

        let source: Bytes = source.into();
        let target: Bytes = target.into();

        // Write to WAL first for durability
        {
            let mut wal = self.wal.lock();
            let memtable = self.memtable.read();
            let ts = memtable.current_timestamp();
            let record = WalRecord::add_edge(
                source.clone(),
                target.clone(),
                edge_type.clone(),
                weight,
                ts,
            );
            wal.append(&record)?;
            if self.config.sync_writes {
                wal.sync()?;
            }
        }

        // Then add to graph index
        let mut metadata = EdgeMetadata::default();
        if let Some(et) = edge_type {
            metadata = EdgeMetadata::with_type(et);
        }
        if let Some(w) = weight {
            metadata = metadata.weight(w);
        }

        index.read().add_edge(source, target, metadata)
    }

    /// Add multiple edges in a single WAL lock acquisition.
    ///
    /// Semantically equivalent to calling `add_edge` N times, but acquires the
    /// WAL mutex once and calls `sync` once (when `sync_writes = true`), reducing
    /// WAL serialisation from O(N) locks to O(1).
    pub fn add_edges_batch(
        &self,
        edges: Vec<(Bytes, Bytes, Option<String>, Option<f32>)>,
    ) -> Result<()> {
        let Some(index) = &self.graph_index else {
            return Err(StorageError::InvalidArgument(
                "Graph index is not enabled".into(),
            ));
        };

        if edges.is_empty() {
            return Ok(());
        }

        // Single WAL lock for all edges
        {
            let mut wal = self.wal.lock();
            let ts = self.memtable.read().current_timestamp();
            let records: Vec<WalRecord> = edges
                .iter()
                .map(|(src, dst, et, w)| {
                    WalRecord::add_edge(src.clone(), dst.clone(), et.clone(), *w, ts)
                })
                .collect();
            wal.append_batch(&records)?;
            if self.config.sync_writes {
                wal.sync()?;
            }
        }

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
            guard.add_edge(src, dst, metadata)?;
        }

        Ok(())
    }

    /// Remove all edges from `source` to `target` in the graph index.
    ///
    /// Unfinalizes the graph before removal and refinalizes afterwards so the
    /// CSR read path remains consistent. Logs a warning if no edge existed.
    pub fn remove_edge(
        &self,
        source: impl Into<Bytes>,
        target: impl Into<Bytes>,
    ) -> Result<()> {
        let Some(index) = &self.graph_index else {
            return Err(StorageError::InvalidArgument(
                "Graph index is not enabled".into(),
            ));
        };

        let source: Bytes = source.into();
        let target: Bytes = target.into();

        // Write to WAL first for durability
        {
            let mut wal = self.wal.lock();
            let memtable = self.memtable.read();
            let ts = memtable.current_timestamp();
            wal.append(&WalRecord::remove_edge(source.clone(), target.clone(), ts))?;
            if self.config.sync_writes {
                wal.sync()?;
            }
        }

        let removed = index.read().remove_edge(&source, &target)?;

        if !removed {
            tracing::debug!("remove_edge: no edge found from {:?} to {:?}", source, target);
        }

        Ok(())
    }

    /// Get neighbors of a node
    pub fn get_neighbors(&self, node: &[u8]) -> Result<Vec<(Bytes, String, f32)>> {
        let Some(index) = &self.graph_index else {
            return Err(StorageError::InvalidArgument(
                "Graph index is not enabled".into(),
            ));
        };

        let neighbors = index.read().get_neighbors(node)?;
        Ok(neighbors
            .into_iter()
            .map(|(key, meta)| (key, meta.edge_type, meta.weight))
            .collect())
    }

    /// Get neighbors filtered by edge type
    pub fn get_neighbors_by_type(&self, node: &[u8], edge_type: &str) -> Result<Vec<Bytes>> {
        let Some(index) = &self.graph_index else {
            return Err(StorageError::InvalidArgument(
                "Graph index is not enabled".into(),
            ));
        };

        let neighbors = index.read().get_neighbors_by_type(node, edge_type)?;
        Ok(neighbors.into_iter().map(|(key, _)| key).collect())
    }

    /// Traverse the graph using BFS
    pub fn traverse_graph(
        &self,
        start: &[u8],
        max_depth: usize,
        edge_types: Option<&[String]>,
    ) -> Result<Vec<TraversalResult>> {
        let Some(index) = &self.graph_index else {
            return Err(StorageError::InvalidArgument(
                "Graph index is not enabled".into(),
            ));
        };

        if let Some(edge_types) = edge_types {
            index.read().traverse_bfs_with_type(start, max_depth, edge_types)
        } else {
            index.read().traverse_bfs(start, max_depth)
        }
    }

    /// Finalize the graph index for faster traversal
    pub fn finalize_graph(&self) -> Result<()> {
        let Some(index) = &self.graph_index else {
            return Err(StorageError::InvalidArgument(
                "Graph index is not enabled".into(),
            ));
        };

        index.read().finalize()
    }

    pub fn unfinalize_graph(&self) -> Result<()> {
        let Some(index) = &self.graph_index else {
            return Err(StorageError::InvalidArgument(
                "Graph index is not enabled".into(),
            ));
        };

        index.read().unfinalize();
        Ok(())
    }

    /// Check if the graph index is enabled
    pub fn graph_enabled(&self) -> bool {
        self.graph_index.is_some()
    }

    /// Get graph statistics
    pub fn graph_stats(&self) -> (usize, usize) {
        self.graph_index
            .as_ref()
            .map(|idx| (idx.read().node_count(), idx.read().edge_count()))
            .unwrap_or((0, 0))
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
    pub fn add_timestamp(&self, key: impl Into<Bytes>, timestamp: u64) -> Result<()> {
        let Some(index) = &self.time_series_index else {
            return Err(StorageError::InvalidArgument(
                "Time-series index is not enabled".into(),
            ));
        };

        let key = key.into();

        // Write to WAL first for durability
        {
            let mut wal = self.wal.lock();
            let memtable = self.memtable.read();
            let ts = memtable.current_timestamp();
            wal.append(&WalRecord::set_timestamp(key.clone(), timestamp, ts))?;
            if self.config.sync_writes {
                wal.sync()?;
            }
        }

        index.read().insert(timestamp, key)
    }

    /// Get the timestamp for a key
    pub fn get_timestamp(&self, key: &[u8]) -> Result<Option<u64>> {
        let Some(index) = &self.time_series_index else {
            return Err(StorageError::InvalidArgument(
                "Time-series index is not enabled".into(),
            ));
        };

        Ok(index.read().get_timestamp(key))
    }

    /// Query a range of timestamps
    pub fn time_range_query(
        &self,
        start: u64,
        end: u64,
        limit: Option<usize>,
    ) -> Result<Vec<(u64, Bytes)>> {
        let Some(index) = &self.time_series_index else {
            return Err(StorageError::InvalidArgument(
                "Time-series index is not enabled".into(),
            ));
        };

        if let Some(limit) = limit {
            Ok(index.read().range_limit(start, end, limit))
        } else {
            Ok(index.read().range(start, end))
        }
    }

    /// Get records before a timestamp
    pub fn time_before(&self, timestamp: u64, limit: usize) -> Result<Vec<(u64, Bytes)>> {
        let Some(index) = &self.time_series_index else {
            return Err(StorageError::InvalidArgument(
                "Time-series index is not enabled".into(),
            ));
        };

        Ok(index.read().before(timestamp, limit))
    }

    /// Get records after a timestamp
    pub fn time_after(&self, timestamp: u64, limit: usize) -> Result<Vec<(u64, Bytes)>> {
        let Some(index) = &self.time_series_index else {
            return Err(StorageError::InvalidArgument(
                "Time-series index is not enabled".into(),
            ));
        };

        Ok(index.read().after(timestamp, limit))
    }

    /// Get the most recent records
    pub fn time_latest(&self, limit: usize) -> Result<Vec<(u64, Bytes)>> {
        let Some(index) = &self.time_series_index else {
            return Err(StorageError::InvalidArgument(
                "Time-series index is not enabled".into(),
            ));
        };

        Ok(index.read().latest(limit))
    }

    /// Add multiple timestamps in a single batch operation
    ///
    /// More efficient than individual `add_timestamp` calls because it acquires
    /// the WAL lock only once for the entire batch.
    pub fn add_timestamps_batch(&self, items: Vec<(Bytes, u64)>) -> Result<usize> {
        let Some(index) = &self.time_series_index else {
            return Err(StorageError::InvalidArgument(
                "Time-series index is not enabled".into(),
            ));
        };

        let count = items.len();

        // Write all records to WAL in a single lock acquisition
        {
            let mut wal = self.wal.lock();
            let memtable = self.memtable.read();
            let ts = memtable.current_timestamp();
            for (key, timestamp) in &items {
                wal.append(&WalRecord::set_timestamp(key.clone(), *timestamp, ts))?;
            }
            if self.config.sync_writes {
                wal.sync()?;
            }
        }

        // Insert into time-series index
        for (key, timestamp) in items {
            index.read().insert(timestamp, key)?;
        }

        Ok(count)
    }

    /// Remove a key from all mutable indexes: time-series, tag, and graph edges
    /// (both outgoing and incoming). Does not touch the KV store — call `delete()`
    /// for that. Returns Ok(()) even if the key was not present in any index.
    pub fn remove_from_indexes(&self, key: &[u8]) -> Result<()> {
        let ts = self.memtable.read().current_timestamp();
        let mut wal_records: Vec<WalRecord> = Vec::new();

        // HNSW vector index
        if let Some(index) = &self.hnsw_index {
            index.remove(key);
            wal_records.push(WalRecord::remove_vector(Bytes::copy_from_slice(key), ts));
        }

        // Time-series index
        if let Some(index) = &self.time_series_index {
            let _ = index.read().remove(key);
            wal_records.push(WalRecord::remove_timestamp(Bytes::copy_from_slice(key), ts));
        }

        // Tag index
        if let Some(index) = &self.tag_index {
            let _ = index.read().remove(key);
            wal_records.push(WalRecord::remove_tags(Bytes::copy_from_slice(key), ts));
        }

        // Graph edges (outgoing + incoming) — each removed pair becomes a RemoveEdge record
        if let Some(index) = &self.graph_index {
            let removed = index.read().remove_node_edges(key)?;
            for (src, dst) in removed {
                wal_records.push(WalRecord::remove_edge(src, dst, ts));
            }
        }

        // Single WAL write for all index cleanups
        if !wal_records.is_empty() {
            let mut wal = self.wal.lock();
            wal.append_batch(&wal_records)?;
            if self.config.sync_writes {
                wal.sync()?;
            }
        }

        Ok(())
    }

    /// Remove all graph edges (outgoing and incoming) for a key and WAL-record them.
    /// Does not touch KV, time-series, or tag indexes.
    /// Used when a memory's content changes and all connections must be refreshed.
    pub fn remove_all_edges(&self, key: &[u8]) -> Result<()> {
        let Some(index) = &self.graph_index else {
            return Ok(());
        };
        let removed = index.read().remove_node_edges(key)?;
        if removed.is_empty() {
            return Ok(());
        }
        let ts = self.memtable.read().current_timestamp();
        let records: Vec<WalRecord> = removed
            .into_iter()
            .map(|(src, dst)| WalRecord::remove_edge(src, dst, ts))
            .collect();
        let mut wal = self.wal.lock();
        wal.append_batch(&records)?;
        if self.config.sync_writes {
            wal.sync()?;
        }
        Ok(())
    }

    /// Return the stored embedding vector for a key, or None if not present or deleted.
    pub fn get_vector(&self, key: &[u8]) -> Option<Vec<f32>> {
        self.hnsw_index.as_ref()?.get_vector_by_key(key)
    }

    /// Check if time-series index is enabled
    pub fn time_series_enabled(&self) -> bool {
        self.time_series_index.is_some()
    }

    /// Save the time-series index to disk
    pub fn save_time_series_index(&self) -> Result<()> {
        if let Some(index) = &self.time_series_index {
            let is_dirty = index.read().is_dirty();
            if is_dirty {
                let len = index.read().len();
                index.write().save_if_dirty()?;
                tracing::info!(
                    "Saved time-series index ({} entries)",
                    len
                );
            }
        }
        Ok(())
    }

    // ==================== Tag/Text Operations ====================

    /// Add tags to a document
    pub fn add_tags(&self, key: impl Into<Bytes>, tags: &[String]) -> Result<()> {
        let Some(index) = &self.tag_index else {
            return Err(StorageError::InvalidArgument(
                "Tag index is not enabled".into(),
            ));
        };

        let key = key.into();

        // Write to WAL first for durability
        {
            let mut wal = self.wal.lock();
            let memtable = self.memtable.read();
            let ts = memtable.current_timestamp();
            wal.append(&WalRecord::add_tags(key.clone(), tags.to_vec(), ts))?;
            if self.config.sync_writes {
                wal.sync()?;
            }
        }

        index.read().add_tags(key, tags)
    }

    /// Replace all tags for a document (removes old tags, then sets new ones)
    pub fn set_tags(&self, key: impl Into<Bytes>, tags: &[String]) -> Result<()> {
        let Some(index) = &self.tag_index else {
            return Err(StorageError::InvalidArgument(
                "Tag index is not enabled".into(),
            ));
        };

        let key = key.into();

        // Write to WAL first for durability
        {
            let mut wal = self.wal.lock();
            let memtable = self.memtable.read();
            let ts = memtable.current_timestamp();
            wal.append(&WalRecord::set_tags(key.clone(), tags.to_vec(), ts))?;
            if self.config.sync_writes {
                wal.sync()?;
            }
        }

        index.read().set_tags(key, tags)
    }

    /// Index text content for a document
    ///
    /// Note: Text is tokenized and stored as tags. The WAL stores the tokens,
    /// not the original text, to ensure recovery produces the same index state.
    pub fn index_text(&self, key: impl Into<Bytes>, text: &str) -> Result<()> {
        let Some(index) = &self.tag_index else {
            return Err(StorageError::InvalidArgument(
                "Tag index is not enabled".into(),
            ));
        };

        let key = key.into();

        // Tokenize the text to get the tags that will be stored
        let min_len = index.read().min_token_length();
        let tokens: Vec<String> = text
            .split_whitespace()
            .map(|s| s.to_lowercase())
            .filter(|s| s.len() >= min_len)
            .collect();

        // Write to WAL first for durability (store tokens, not raw text)
        if !tokens.is_empty() {
            let mut wal = self.wal.lock();
            let memtable = self.memtable.read();
            let ts = memtable.current_timestamp();
            wal.append(&WalRecord::add_tags(key.clone(), tokens, ts))?;
            if self.config.sync_writes {
                wal.sync()?;
            }
        }

        index.read().index_text(key, text)
    }

    /// Search for documents with specific tags (AND query)
    pub fn tag_search_and(&self, tags: &[&str]) -> Result<Vec<Bytes>> {
        let Some(index) = &self.tag_index else {
            return Err(StorageError::InvalidArgument(
                "Tag index is not enabled".into(),
            ));
        };

        Ok(index.read().search_and(tags))
    }

    /// Search for documents with any of the given tags (OR query)
    pub fn tag_search_or(&self, tags: &[&str]) -> Result<Vec<Bytes>> {
        let Some(index) = &self.tag_index else {
            return Err(StorageError::InvalidArgument(
                "Tag index is not enabled".into(),
            ));
        };

        Ok(index.read().search_or(tags))
    }

    /// Search with scoring (returns results sorted by relevance)
    pub fn tag_search_scored(&self, tags: &[&str]) -> Result<Vec<(Bytes, f32)>> {
        let Some(index) = &self.tag_index else {
            return Err(StorageError::InvalidArgument(
                "Tag index is not enabled".into(),
            ));
        };

        Ok(index.read().search_or_scored(tags))
    }

    /// Get tags for a document
    pub fn get_tags(&self, key: &[u8]) -> Result<Vec<String>> {
        let Some(index) = &self.tag_index else {
            return Err(StorageError::InvalidArgument(
                "Tag index is not enabled".into(),
            ));
        };

        Ok(index.read().get_tokens(key))
    }

    /// Check if a key exists in the tag index
    pub fn tag_has_key(&self, key: &[u8]) -> bool {
        self.tag_index
            .as_ref()
            .map(|index| index.read().contains_key(key))
            .unwrap_or(false)
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
        Ok(())
    }

    /// Checkpoint the storage engine
    ///
    /// This saves all indexes to disk and truncates the WAL.
    /// It should be called periodically or when WAL grows too large.
    pub async fn checkpoint(&self) -> Result<()> {
        tracing::info!("Starting checkpoint...");

        // Save all indexes
        // Note: we can't use save_all_indexes() directly here if we wanted to be async,
        // but the save methods are blocking/sync currently (std::fs).
        // For now, we keep it simple. Index saving is blocking.
        self.save_all_indexes()?;

        // Truncate WAL
        {
            let mut wal = self.wal.lock();
            let _ = wal.truncate()?;
        }

        tracing::info!("Checkpoint complete (Indexes saved, WAL truncated)");
        Ok(())
    }

    // ==================== Lifecycle ====================

    /// Shutdown the storage engine (requires mutable reference)
    pub async fn shutdown(&mut self) -> Result<()> {
        self.graceful_shutdown().await
    }

    /// Graceful shutdown that works with `&self` (for use from signal handlers with `Arc<StorageEngine>`)
    ///
    /// This flushes the memtable, saves all indexes, syncs the WAL, and waits for
    /// background tasks to finish.
    pub async fn graceful_shutdown(&self) -> Result<()> {
        tracing::info!("Starting graceful shutdown...");

        // Signal shutdown to background tasks
        self.shutdown.store(true, Ordering::SeqCst);

        // Flush remaining data
        self.flush().await?;

        // Save all indexes
        self.save_all_indexes()?;

        // All KV data is in SSTables and all indexes are on disk — safe to
        // truncate the WAL so that restart skips replay and loads cleanly.
        {
            let mut wal = self.wal.lock();
            wal.sync()?;
            if let Err(e) = wal.truncate() {
                tracing::warn!("Failed to truncate WAL on shutdown: {}", e);
            }
        }

        // Wait for background tasks (take handles out of mutex before awaiting)
        let flush_handle = self.flush_handle.lock().take();
        if let Some(handle) = flush_handle {
            let _ = handle.await;
        }
        let compaction_handle = self.compaction_handle.lock().take();
        if let Some(handle) = compaction_handle {
            let _ = handle.await;
        }

        tracing::info!("Graceful shutdown complete");
        Ok(())
    }
}

/// Result from a vector similarity search
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    /// The key of the matching record
    pub key: Bytes,
    /// Distance to the query vector (interpretation depends on metric)
    pub distance: f32,
    /// The value, if fetched
    pub value: Option<Bytes>,
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

/// Storage statistics
#[derive(Debug)]
pub struct StorageStats {
    /// Current MemTable size in bytes
    pub memtable_size: usize,
    /// Number of immutable MemTables waiting to be flushed
    pub immutable_memtables: usize,
    /// Statistics per level
    pub level_stats: Vec<super::compaction::LevelStats>,
    /// Number of entries in block cache
    pub cache_entries: usize,
    /// Block cache size in bytes
    pub cache_size: usize,
    /// Number of vectors in the HNSW index
    pub vector_count: usize,
    /// Whether vector search is enabled
    pub vector_enabled: bool,
    /// Number of nodes in the graph index
    pub graph_node_count: usize,
    /// Number of edges in the graph index
    pub graph_edge_count: usize,
    /// Whether graph index is enabled
    pub graph_enabled: bool,
    /// Number of entries in time-series index
    pub time_series_count: usize,
    /// Whether time-series index is enabled
    pub time_series_enabled: bool,
    /// Number of documents in tag index
    pub tag_doc_count: usize,
    /// Number of unique tokens in tag index
    pub tag_token_count: usize,
    /// Whether tag index is enabled
    pub tag_enabled: bool,
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
            memtable_size: 1024, // Very small for testing
            ..Default::default()
        };

        let engine = StorageEngine::new(config).await.unwrap();

        // Write enough data to trigger flush
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

        engine.add_edges_batch(edges).unwrap();

        let neighbors = engine.get_neighbors(b"node:a").unwrap();
        assert_eq!(neighbors.len(), 2);
        let targets: Vec<String> = neighbors
            .iter()
            .map(|(k, _, _)| String::from_utf8_lossy(k).to_string())
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
            .store_memory_core(key.clone(), value.clone(), None, ts, &tags)
            .await
            .unwrap();

        // KV readable
        let got = engine.get(&key).await.unwrap();
        assert_eq!(got, Some(value));

        // Timestamp indexed
        let ts_entries = engine.time_latest(1).unwrap();
        assert_eq!(ts_entries.len(), 1);
        assert_eq!(ts_entries[0].0, ts);

        // Tags indexed
        let tag_results = engine.tag_search_and(&["rust"]).unwrap();
        assert!(
            tag_results.iter().any(|k| k == &key),
            "tag index should contain the stored key"
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
            },
            ..Default::default()
        }
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
            .add_edge(src.clone(), dst.clone(), Some("related_to".to_string()), Some(0.9))
            .unwrap();

        let neighbors_before = engine.get_neighbors(src.as_ref()).unwrap();
        assert_eq!(neighbors_before.len(), 1);

        engine.remove_all_edges(src.as_ref()).unwrap();

        let neighbors_after = engine.get_neighbors(src.as_ref()).unwrap();
        assert!(neighbors_after.is_empty(), "all edges must be removed");
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
                )
                .await
                .unwrap();

            engine.remove_from_indexes(key.as_ref()).unwrap();
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
}
