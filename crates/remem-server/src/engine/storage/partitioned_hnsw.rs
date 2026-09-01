//! Per-partition HNSW index manager.
//!
//! One `HnswIndex` is still one graph. This wrapper keeps a separate graph per
//! `(TenantId, PartitionId)` so semantic search never traverses unauthorized
//! partition nodes.
//!
//! It also decides which of those graphs are in memory. A graph is materialized
//! when something needs it and, when a resident budget is configured, released
//! again once it becomes the least recently used — otherwise memory would grow
//! with the number of partitions a deployment has ever written rather than with
//! the ones it is actually serving. Counts and sizes come from
//! [`PartitionCatalog`] instead of from whatever happens to be loaded, so a
//! partition reports the same figures either way.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use super::partition::{
    decode_record_key, encode_record_key, PartitionBinding, PartitionId, PartitionScope, TenantId,
};
use super::partition_catalog::PartitionCatalog;
use crate::engine::error::{Result, StorageError};
use crate::engine::index::{
    on_index_parse_failure, segment_io::SegmentReader, HnswConfig, HnswIndex, SegmentManifest,
};

pub struct PartitionedHnswIndexes {
    root: PathBuf,
    config: HnswConfig,
    legacy_binding: PartitionBinding,
    indexes: RwLock<BTreeMap<PartitionBinding, Arc<HnswIndex>>>,
    /// One slot per partition currently being restored from disk.
    ///
    /// Restores used to happen once in a partition's lifetime, so doing the
    /// read while holding `indexes` for write cost nothing. Under on-demand
    /// residency they are routine, and that lock is shared by every partition —
    /// one tenant's cold restore would stall every other tenant's lookup, which
    /// is the isolation this layout exists to provide, broken one level down.
    /// Callers for the same partition wait on its slot; callers for any other
    /// partition are unaffected.
    loading: Mutex<BTreeMap<PartitionBinding, Arc<LoadSlot>>>,
    /// Counts and sizes for every known partition, resident or not.
    ///
    /// Deliberately not derived from `indexes`: a partition absent from that
    /// map is one that is not loaded, not one that holds no vectors.
    catalog: PartitionCatalog,
    /// Ceiling on resident graph bytes; `None` disables releasing entirely.
    budget_bytes: Option<usize>,
    /// Lets a test hold a restore open, to prove unrelated partitions are not
    /// waiting behind it.
    #[cfg(test)]
    load_gate: Mutex<Option<Arc<std::sync::Barrier>>>,
    /// Partitions restored from disk since startup.
    loads: AtomicU64,
    /// Partitions released from memory since startup.
    evictions: AtomicU64,
}

/// A single partition's in-flight restore.
struct LoadSlot {
    /// `Some` once the restore finishes. Guarded rather than atomic because
    /// waiters need to block on it, not spin.
    loaded: Mutex<Option<Arc<HnswIndex>>>,
}

impl PartitionedHnswIndexes {
    /// Open the partition set. `budget_bytes` of `None` means graphs are never
    /// released to reclaim memory.
    pub fn open(
        root: PathBuf,
        config: HnswConfig,
        legacy_binding: PartitionBinding,
        budget_bytes: Option<usize>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&root)?;
        let catalog = PartitionCatalog::new();
        survey_partitions(&root, &catalog)?;
        Ok(Self {
            root,
            config,
            legacy_binding,
            // Starts empty by design: a partition is materialized when
            // something needs it, not because it exists.
            indexes: RwLock::new(BTreeMap::new()),
            loading: Mutex::new(BTreeMap::new()),
            catalog,
            budget_bytes,
            #[cfg(test)]
            load_gate: Mutex::new(None),
            loads: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        })
    }

    /// Accounting for every known partition, whether or not it is resident.
    pub fn catalog(&self) -> &PartitionCatalog {
        &self.catalog
    }

    pub fn insert(
        &self,
        binding: &PartitionBinding,
        key: impl Into<Bytes>,
        vector: Vec<f32>,
    ) -> Result<u32> {
        let index = self.get_or_create(binding)?;
        let node_id = index.insert(key, vector)?;
        self.catalog
            .sync_resident(binding, index.len(), index.node_count());
        Ok(node_id)
    }

    pub fn insert_for_key(&self, key: Bytes, vector: Vec<f32>) -> Result<u32> {
        let binding = self.binding_for_physical_key(&key)?;
        self.insert(&binding, key, vector)
    }

    pub fn search(
        &self,
        scope: &PartitionScope,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<(Bytes, f32)>> {
        let mut out = Vec::new();
        for partition in scope.partitions() {
            let binding = PartitionBinding::new(scope.tenant().clone(), partition.clone());
            // `resident_or_load` records the use; no second touch needed.
            let Some(index) = self.resident_or_load(&binding)? else {
                continue;
            };
            let mut hits = if let Some(ef) = ef {
                index.search_with_ef(query, k, ef)?
            } else {
                index.search(query, k)?
            };
            out.append(&mut hits);
        }
        out.sort_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.as_ref().cmp(right.0.as_ref()))
        });
        out.truncate(k);
        Ok(out)
    }

    /// The minimum candidate window used by an ordinary partitioned search.
    pub(crate) fn search_floor(&self) -> usize {
        self.config.ef_search
    }

    #[cfg(test)]
    pub(crate) fn reset_distance_evaluations(&self) {
        if let Some(index) = self.indexes.read().values().next() {
            index.reset_distance_evaluations();
        }
    }

    #[cfg(test)]
    pub(crate) fn distance_evaluations(&self) -> usize {
        self.indexes
            .read()
            .values()
            .next()
            .map_or(0, |index| index.distance_evaluations())
    }

    pub fn remove(&self, key: &[u8]) -> Result<bool> {
        let binding = self.binding_for_physical_key(key)?;
        // Loads the partition if it is not resident: skipping the removal
        // because the graph happens to be unloaded would leave the vector in
        // the index and resurrect it as a phantom on the next search.
        let Some(index) = self.resident_or_load(&binding)? else {
            return Ok(false);
        };
        let removed = index.remove(key);
        if removed {
            self.catalog
                .sync_resident(&binding, index.len(), index.node_count());
        }
        Ok(removed)
    }

    pub fn get_vector_by_key(&self, key: &[u8]) -> Option<Vec<f32>> {
        let binding = self.binding_for_physical_key(key).ok()?;
        self.resident_or_load(&binding)
            .ok()??
            .get_vector_by_key(key)
    }

    pub fn migrate_legacy_index(
        &self,
        binding: &PartitionBinding,
        legacy: &HnswIndex,
    ) -> Result<usize> {
        let mut migrated = 0;
        for (key, vector) in legacy.vectors() {
            let physical_key = if decode_record_key(&key)
                .map_err(|err| StorageError::InvalidArgument(err.to_string()))?
                .is_some()
            {
                key
            } else {
                encode_record_key(binding, &key)
                    .map_err(|err| StorageError::InvalidArgument(err.to_string()))?
            };
            self.insert(binding, physical_key, vector)?;
            migrated += 1;
        }
        Ok(migrated)
    }

    pub fn len(&self) -> usize {
        self.catalog.total_live()
    }

    /// Whether no partition holds a live vector.
    ///
    /// Read from the catalog like `len`, not from the resident map: a
    /// partition missing from that map is unloaded, not empty (REM-77).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Node slots across every known partition, tombstoned ones included.
    pub fn node_count(&self) -> usize {
        self.catalog.total_nodes()
    }

    pub fn partition_counts(&self) -> Vec<(PartitionBinding, usize)> {
        self.catalog.counts()
    }

    /// Live vectors held by the partitions in `scope`.
    #[allow(dead_code)]
    pub fn scoped_len(&self, scope: &PartitionScope) -> usize {
        self.catalog.scoped_live(scope)
    }

    /// Node slots the partitions in `scope` hold, tombstoned ones included.
    pub fn scoped_node_count(&self, scope: &PartitionScope) -> usize {
        self.catalog.scoped_nodes(scope)
    }

    pub fn is_dirty(&self) -> bool {
        self.indexes.read().values().any(|index| index.is_dirty())
    }

    pub fn save_dirty(&self) -> Result<()> {
        for (binding, index) in self.indexes.read().iter() {
            if !index.is_dirty() {
                continue;
            }
            let dir = self.partition_dir(binding);
            index.save_dirty_chunks(&dir)?;
            index.save_deleted_nodes(&dir)?;
        }
        Ok(())
    }

    /// The partition's graph, materializing it if it is known but not resident.
    ///
    /// `Ok(None)` means the partition holds nothing worth loading — either the
    /// catalog has never heard of it, or every vector it held has been
    /// tombstoned. Both cases would search to the same empty result, so paying
    /// a load to prove it would be waste.
    fn resident_or_load(&self, binding: &PartitionBinding) -> Result<Option<Arc<HnswIndex>>> {
        if let Some(index) = self.indexes.read().get(binding).cloned() {
            self.catalog.touch(binding);
            return Ok(Some(index));
        }
        if self.catalog.live_count(binding) == 0 {
            return Ok(None);
        }
        self.get_or_create(binding).map(Some)
    }

    fn get_or_create(&self, binding: &PartitionBinding) -> Result<Arc<HnswIndex>> {
        if let Some(index) = self.indexes.read().get(binding).cloned() {
            self.catalog.touch(binding);
            return Ok(index);
        }

        // Claim (or join) this partition's restore. The map lock is held only
        // long enough to hand out the slot, never across the read below.
        let slot = {
            let mut loading = self.loading.lock();
            Arc::clone(loading.entry(binding.clone()).or_insert_with(|| {
                Arc::new(LoadSlot {
                    loaded: Mutex::new(None),
                })
            }))
        };

        let mut loaded = slot.loaded.lock();

        // Either another caller finished this restore while we queued for the
        // slot, or it landed in the map by some other path.
        if let Some(index) = loaded.clone() {
            self.catalog.touch(binding);
            return Ok(index);
        }
        if let Some(index) = self.indexes.read().get(binding).cloned() {
            self.catalog.touch(binding);
            return Ok(index);
        }

        let index = self.restore(binding)?;

        self.indexes
            .write()
            .insert(binding.clone(), Arc::clone(&index));
        *loaded = Some(Arc::clone(&index));

        self.catalog.mark_resident(binding, index.resident_bytes());
        self.catalog
            .sync_resident(binding, index.len(), index.node_count());
        self.catalog.touch(binding);
        self.loads.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            tenant = %binding.tenant().as_str(),
            partition = %binding.partition().as_str(),
            vectors = index.len(),
            index_bytes = index.resident_bytes(),
            resident_bytes = self.catalog.resident_bytes(),
            resident_partitions = self.catalog.resident_count(),
            "restored partition vector index"
        );

        // The slot has served its purpose; leaving it would grow a map with
        // every partition ever touched. Waiters hold their own `Arc` and will
        // read the value it now carries.
        self.loading.lock().remove(binding);

        self.enforce_budget(binding)?;

        Ok(index)
    }

    /// Release least-recently-used partitions until resident bytes are back
    /// within the budget.
    ///
    /// Runs *after* the restore that pushed us over, never before it: the
    /// partition a caller is waiting on is always served, and `keep` is exempt
    /// so a partition larger than the whole budget is not released the instant
    /// it arrives — which would evict it and reload it forever without ever
    /// answering anything.
    fn enforce_budget(&self, keep: &PartitionBinding) -> Result<()> {
        let Some(budget) = self.budget_bytes else {
            return Ok(());
        };

        for (binding, _) in self.catalog.lru_resident() {
            if self.catalog.resident_bytes() <= budget {
                break;
            }
            if &binding == keep {
                continue;
            }
            self.release(&binding)?;
        }

        Ok(())
    }

    /// Persist a partition if it has unsaved work, then drop it from memory.
    ///
    /// Dropping the map's `Arc` does not free anything an in-flight search is
    /// still traversing — that caller cloned its own handle before it started,
    /// and the memory is reclaimed when the last one finishes.
    ///
    /// Flushing here writes chunks outside the checkpoint cycle, which is only
    /// ever *more* current than the checkpoint would have left them. The WAL
    /// still holds those records until a checkpoint truncates it, and replaying
    /// an insert whose key is already present updates in place, so an early
    /// flush can neither lose a write nor duplicate one.
    fn release(&self, binding: &PartitionBinding) -> Result<()> {
        let index = { self.indexes.write().remove(binding) };
        let Some(index) = index else {
            return Ok(());
        };

        if index.is_dirty() {
            let dir = self.partition_dir(binding);
            index.save_dirty_chunks(&dir)?;
            index.save_deleted_nodes(&dir)?;
        }

        self.catalog.mark_evicted(binding);
        self.evictions.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            partition = %binding.partition().as_str(),
            tenant = %binding.tenant().as_str(),
            resident_bytes = self.catalog.resident_bytes(),
            "released partition vector index to stay within the resident budget"
        );
        Ok(())
    }

    /// Read one partition's graph from disk. Holds no shared lock.
    fn restore(&self, binding: &PartitionBinding) -> Result<Arc<HnswIndex>> {
        #[cfg(test)]
        {
            let gate = self.load_gate.lock().clone();
            if let Some(barrier) = gate {
                barrier.wait();
            }
        }

        let dir = self.partition_dir(binding);
        std::fs::create_dir_all(&dir)?;
        let index = match HnswIndex::load_chunked(&dir, self.config.clone()) {
            Ok(Some(index)) => Arc::new(index),
            Ok(None) => Arc::new(HnswIndex::new(self.config.clone())),
            Err(err) => {
                on_index_parse_failure(&dir.join("hnsw.manifest"), &err)?;
                Arc::new(HnswIndex::new(self.config.clone()))
            }
        };
        Ok(index)
    }

    /// Partitions restored from disk since startup.
    pub fn loads(&self) -> u64 {
        self.loads.load(Ordering::Relaxed)
    }

    /// Partitions released from memory since startup.
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    fn partition_dir(&self, binding: &PartitionBinding) -> PathBuf {
        self.root
            .join(binding.tenant().as_str())
            .join(binding.partition().as_str())
    }

    fn binding_for_physical_key(&self, key: &[u8]) -> Result<PartitionBinding> {
        match decode_record_key(key)
            .map_err(|err| StorageError::InvalidArgument(err.to_string()))?
        {
            Some(decoded) => Ok(decoded.binding().clone()),
            None => Ok(self.legacy_binding.clone()),
        }
    }
}

/// Survey every partition on disk without materializing a single graph.
///
/// Two jobs, both of which must happen before the server serves anything:
///
/// 1. **Catalog every partition.** Counts and sizes come from each partition's
///    manifest and tombstone file, so a partition that is never touched still
///    reports its true size. Without this, `scoped_vector_count` would read
///    zero for an unloaded partition and under-bound retrieval.
/// 2. **Verify what is there.** Graphs are no longer parsed at startup, so
///    parsing is no longer what would surface a corrupt chunk. Checking the
///    stored CRC of every chunk keeps the guarantee that corruption fails
///    startup rather than being discovered later, or silently dropped.
///
/// Verification streams a block at a time, so its peak memory is the same for a
/// deployment with one partition as for one with thousands.
fn survey_partitions(root: &Path, catalog: &PartitionCatalog) -> Result<()> {
    for tenant in read_dirs(root)? {
        let tenant_id = TenantId::new(tenant.file_name().to_string_lossy().into_owned())
            .map_err(|err| StorageError::InvalidArgument(err.to_string()))?;

        for partition in read_dirs(&tenant.path())? {
            let partition_id =
                PartitionId::new(partition.file_name().to_string_lossy().into_owned())
                    .map_err(|err| StorageError::InvalidArgument(err.to_string()))?;
            let binding = PartitionBinding::new(tenant_id.clone(), partition_id);
            let dir = partition.path();

            let manifest = match SegmentManifest::load(&dir, "hnsw") {
                Ok(Some(manifest)) => manifest,
                // No manifest: a partition directory that was created but never
                // saved. Nothing to catalog and nothing to verify.
                Ok(None) => continue,
                Err(err) => {
                    on_index_parse_failure(&dir.join("hnsw.manifest"), &err)?;
                    continue;
                }
            };

            verify_chunks(&dir, &manifest)?;

            if let Some((live, nodes, disk_bytes)) = PartitionCatalog::derive_from_disk(&dir)? {
                catalog.record_from_disk(&binding, live, nodes, disk_bytes);
            }
        }
    }
    Ok(())
}

/// Check the stored CRC of every chunk the manifest claims.
///
/// A chunk the manifest lists but that is absent keeps the pre-existing
/// warn-and-continue handling: this is about detecting bytes that changed
/// underneath us, not about reinterpreting a missing file.
fn verify_chunks(dir: &Path, manifest: &SegmentManifest) -> Result<()> {
    for chunk in &manifest.chunks {
        let seg_path = dir.join(&chunk.filename);
        if !seg_path.exists() {
            tracing::warn!("HNSW chunk {:?} missing; skipping", seg_path);
            continue;
        }
        if let Err(err) = SegmentReader::verify(&seg_path) {
            on_index_parse_failure(&seg_path, &err)?;
        }
    }
    Ok(())
}

fn read_dirs(path: &Path) -> Result<Vec<std::fs::DirEntry>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            out.push(entry);
        }
    }
    out.sort_by_key(|entry| entry.path());
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::index::index_rebuild_env_guard;

    fn config() -> HnswConfig {
        HnswConfig::with_dim(4)
    }

    fn binding(partition: &str) -> PartitionBinding {
        PartitionBinding::new(
            TenantId::new("acme").unwrap(),
            PartitionId::new(partition).unwrap(),
        )
    }

    fn scope_of(partition: &str) -> PartitionScope {
        PartitionScope::single(binding(partition))
    }

    /// Populate `root` with `partitions`, each holding one vector, then drop
    /// the handle so the next `open` starts from disk alone.
    fn seed(root: &Path, partitions: &[&str]) {
        let indexes =
            PartitionedHnswIndexes::open(root.to_path_buf(), config(), binding("legacy"), None)
                .unwrap();
        for (i, partition) in partitions.iter().enumerate() {
            let bind = binding(partition);
            let key = encode_record_key(&bind, format!("memory:{partition}").as_bytes()).unwrap();
            let mut vector = vec![0.0; 4];
            vector[i % 4] = 1.0;
            indexes.insert(&bind, key, vector).unwrap();
        }
        indexes.save_dirty().unwrap();
    }

    #[test]
    fn opening_a_populated_directory_materializes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["product", "finance", "tech"]);

        let indexes = PartitionedHnswIndexes::open(
            dir.path().to_path_buf(),
            config(),
            binding("legacy"),
            None,
        )
        .unwrap();

        assert_eq!(
            indexes.catalog().resident_count(),
            0,
            "opening the directory loaded partition graphs that nothing had asked for"
        );
        assert_eq!(
            indexes.len(),
            3,
            "counts must come from the catalog, not from what happens to be loaded"
        );
    }

    #[test]
    fn searching_one_partition_materializes_only_that_partition() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["product", "finance", "tech"]);

        let indexes = PartitionedHnswIndexes::open(
            dir.path().to_path_buf(),
            config(),
            binding("legacy"),
            None,
        )
        .unwrap();

        let hits = indexes
            .search(&scope_of("product"), &[1.0, 0.0, 0.0, 0.0], 10, None)
            .unwrap();
        assert_eq!(hits.len(), 1, "the searched partition returned nothing");

        assert_eq!(
            indexes.catalog().resident_count(),
            1,
            "a search bound to one partition made other partitions resident"
        );
        assert!(
            indexes
                .catalog()
                .stats(&binding("product"))
                .unwrap()
                .is_resident(),
            "the searched partition is not the one that became resident"
        );
        for untouched in ["finance", "tech"] {
            assert!(
                !indexes
                    .catalog()
                    .stats(&binding(untouched))
                    .unwrap()
                    .is_resident(),
                "{untouched} became resident without being searched"
            );
        }
    }

    #[test]
    fn counts_are_reported_for_partitions_that_were_never_loaded() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["product", "finance"]);

        let indexes = PartitionedHnswIndexes::open(
            dir.path().to_path_buf(),
            config(),
            binding("legacy"),
            None,
        )
        .unwrap();

        assert_eq!(indexes.scoped_len(&scope_of("finance")), 1);
        assert_eq!(
            indexes.catalog().resident_count(),
            0,
            "asking for a count materialized a graph"
        );
    }

    #[test]
    fn concurrent_first_use_restores_a_partition_once() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["product"]);

        let indexes = Arc::new(
            PartitionedHnswIndexes::open(
                dir.path().to_path_buf(),
                config(),
                binding("legacy"),
                None,
            )
            .unwrap(),
        );

        // Both threads meet inside the restore, so neither can finish before
        // the other has entered — the interleaving the slot has to survive.
        let barrier = Arc::new(std::sync::Barrier::new(2));
        *indexes.load_gate.lock() = Some(Arc::clone(&barrier));

        let handles: Vec<_> = (0..2)
            .map(|_| {
                let indexes = Arc::clone(&indexes);
                std::thread::spawn(move || {
                    indexes
                        .search(&scope_of("product"), &[1.0, 0.0, 0.0, 0.0], 10, None)
                        .unwrap()
                })
            })
            .collect();

        // Only one thread reaches the gate; release it so the loser of the race
        // proceeds through the slot instead.
        barrier.wait();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results[0].len(), 1);
        assert_eq!(results[1].len(), 1);
        assert_eq!(
            indexes.loads(),
            1,
            "the same partition was restored from disk more than once"
        );
        assert_eq!(indexes.catalog().resident_count(), 1);
    }

    #[test]
    fn a_restore_does_not_block_an_unrelated_partition() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["cold", "warm"]);

        let indexes = Arc::new(
            PartitionedHnswIndexes::open(
                dir.path().to_path_buf(),
                config(),
                binding("legacy"),
                None,
            )
            .unwrap(),
        );

        // Make `warm` resident up front; `cold` is the one that will be stuck
        // mid-restore.
        indexes
            .search(&scope_of("warm"), &[0.0, 1.0, 0.0, 0.0], 10, None)
            .unwrap();

        let gate = Arc::new(std::sync::Barrier::new(2));
        *indexes.load_gate.lock() = Some(Arc::clone(&gate));

        let restoring = {
            let indexes = Arc::clone(&indexes);
            std::thread::spawn(move || {
                indexes
                    .search(&scope_of("cold"), &[1.0, 0.0, 0.0, 0.0], 10, None)
                    .unwrap()
            })
        };

        // Give the restoring thread time to reach the gate and park there.
        std::thread::sleep(std::time::Duration::from_millis(100));

        let (tx, rx) = std::sync::mpsc::channel();
        {
            let indexes = Arc::clone(&indexes);
            std::thread::spawn(move || {
                let hits = indexes
                    .search(&scope_of("warm"), &[0.0, 1.0, 0.0, 0.0], 10, None)
                    .unwrap();
                let _ = tx.send(hits.len());
            });
        }

        // If the restore held the shared index map, this search could not run
        // until the gate opened — and the gate is not open yet.
        let served = rx.recv_timeout(std::time::Duration::from_secs(5)).expect(
            "a search on a resident partition waited on an unrelated \
                 partition's restore: restore I/O is holding the shared lock",
        );
        assert_eq!(served, 1);

        gate.wait();
        assert_eq!(restoring.join().unwrap().len(), 1);
    }

    /// Open with a budget sized to `graphs` typical partition graphs.
    fn open_with_room_for(dir: &Path, graphs: usize) -> PartitionedHnswIndexes {
        let sized =
            PartitionedHnswIndexes::open(dir.to_path_buf(), config(), binding("legacy"), None)
                .unwrap();
        sized
            .search(&scope_of("p0"), &[1.0, 0.0, 0.0, 0.0], 1, None)
            .unwrap();
        let one_graph = sized.catalog().resident_bytes();
        assert!(one_graph > 0, "a resident graph measured as zero bytes");
        drop(sized);

        PartitionedHnswIndexes::open(
            dir.to_path_buf(),
            config(),
            binding("legacy"),
            Some(one_graph * graphs),
        )
        .unwrap()
    }

    #[test]
    fn residency_stays_within_the_budget_and_results_stay_correct() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["p0", "p1", "p2"]);

        let indexes = open_with_room_for(dir.path(), 1);

        for (i, partition) in ["p0", "p1", "p2"].iter().enumerate() {
            let mut query = vec![0.0; 4];
            query[i % 4] = 1.0;
            let hits = indexes
                .search(&scope_of(partition), &query, 10, None)
                .unwrap();
            assert_eq!(
                hits.len(),
                1,
                "{partition} returned nothing once eviction was in play"
            );
            // Residency is asserted in partitions rather than bytes on purpose.
            // The budget is sized from one partition, but partitions are not
            // all exactly one size — HNSW assigns node layers randomly, so
            // neighbour capacity varies — and the partition being served is
            // exempt from release by design. "Everything except the partition
            // in use was released" is the guarantee; "resident bytes are under
            // the budget" is not one the soft budget makes.
            assert!(
                indexes.catalog().resident_count() <= 1,
                "{} partitions stayed resident after touching {partition}; \
                 everything but the one being served should have been released",
                indexes.catalog().resident_count()
            );
        }

        assert!(
            indexes.evictions() > 0,
            "three partitions fit in a one-partition budget without any eviction"
        );
    }

    #[test]
    fn a_partition_larger_than_the_budget_is_still_served() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["p0"]);

        // A budget of one byte: no partition can fit.
        let indexes = PartitionedHnswIndexes::open(
            dir.path().to_path_buf(),
            config(),
            binding("legacy"),
            Some(1),
        )
        .unwrap();

        let hits = indexes
            .search(&scope_of("p0"), &[1.0, 0.0, 0.0, 0.0], 10, None)
            .unwrap();
        assert_eq!(
            hits.len(),
            1,
            "a partition larger than the budget was not served"
        );
        assert_eq!(
            indexes.catalog().resident_count(),
            1,
            "the partition being served was released out from under the request"
        );
    }

    #[test]
    fn no_budget_means_no_eviction() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["p0", "p1", "p2"]);

        let indexes = PartitionedHnswIndexes::open(
            dir.path().to_path_buf(),
            config(),
            binding("legacy"),
            None,
        )
        .unwrap();

        for (i, partition) in ["p0", "p1", "p2"].iter().enumerate() {
            let mut query = vec![0.0; 4];
            query[i % 4] = 1.0;
            indexes
                .search(&scope_of(partition), &query, 10, None)
                .unwrap();
        }

        assert_eq!(
            indexes.evictions(),
            0,
            "partitions were released although no budget was configured"
        );
        assert_eq!(indexes.catalog().resident_count(), 3);
    }

    #[test]
    fn an_evicted_partition_keeps_its_unsaved_writes() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["p0", "p1"]);

        let indexes = open_with_room_for(dir.path(), 1);

        // Write into p0 without saving, then force it out by touching p1.
        let bind = binding("p0");
        let key = encode_record_key(&bind, b"memory:unsaved").unwrap();
        indexes
            .insert(&bind, key.clone(), vec![0.0, 0.0, 1.0, 0.0])
            .unwrap();
        indexes
            .search(&scope_of("p1"), &[0.0, 1.0, 0.0, 0.0], 10, None)
            .unwrap();
        assert!(
            indexes.evictions() > 0,
            "p0 was never evicted, so this proves nothing"
        );

        let hits = indexes
            .search(&scope_of("p0"), &[0.0, 0.0, 1.0, 0.0], 10, None)
            .unwrap();
        assert!(
            hits.iter().any(|(k, _)| k == &key),
            "a vector written before eviction did not survive the reload"
        );
    }

    #[test]
    fn a_deletion_survives_eviction_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["p0", "p1"]);

        let indexes = open_with_room_for(dir.path(), 1);

        let bind = binding("p0");
        let gone = encode_record_key(&bind, b"memory:gone").unwrap();
        indexes
            .insert(&bind, gone.clone(), vec![0.0, 0.0, 1.0, 0.0])
            .unwrap();
        assert!(indexes.remove(&gone).unwrap());
        let live_after_delete = indexes.scoped_len(&scope_of("p0"));

        // Force p0 out, then bring it back.
        indexes
            .search(&scope_of("p1"), &[0.0, 1.0, 0.0, 0.0], 10, None)
            .unwrap();
        let hits = indexes
            .search(&scope_of("p0"), &[0.0, 0.0, 1.0, 0.0], 10, None)
            .unwrap();

        assert!(
            !hits.iter().any(|(k, _)| k == &gone),
            "a deleted vector came back as a phantom after an evict/reload cycle"
        );
        assert_eq!(
            indexes.scoped_len(&scope_of("p0")),
            live_after_delete,
            "the reported live count changed across an evict/reload cycle"
        );
    }

    #[test]
    fn an_in_flight_search_survives_concurrent_eviction() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["p0", "p1"]);

        let indexes = Arc::new(open_with_room_for(dir.path(), 1));

        // p0 resident and in use.
        let held = indexes.resident_or_load(&binding("p0")).unwrap().unwrap();

        // Touching p1 under a one-graph budget releases p0 from the map.
        indexes
            .search(&scope_of("p1"), &[0.0, 1.0, 0.0, 0.0], 10, None)
            .unwrap();
        assert!(indexes.evictions() > 0, "p0 was not evicted");

        // The handle taken before the eviction is still a live, correct graph.
        let hits = held.search(&[1.0, 0.0, 0.0, 0.0], 10).unwrap();
        assert_eq!(
            hits.len(),
            1,
            "a search holding a partition lost its results when the partition \
             was evicted underneath it"
        );
    }

    #[test]
    fn thrash_is_visible_as_loads_and_evictions_climbing_together() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["p0", "p1"]);

        let indexes = open_with_room_for(dir.path(), 1);

        // Alternate between two partitions that cannot both be resident: the
        // working set exceeds the budget, which is exactly the condition an
        // operator needs to be able to see without inspecting process memory.
        for round in 0..3 {
            for (i, partition) in ["p0", "p1"].iter().enumerate() {
                let mut query = vec![0.0; 4];
                query[i % 4] = 1.0;
                indexes
                    .search(&scope_of(partition), &query, 10, None)
                    .unwrap();
            }
            assert!(
                indexes.loads() > round && indexes.evictions() > round,
                "round {round}: loads ({}) and evictions ({}) are not both \
                 climbing, so repeated restore/release cycles would be invisible",
                indexes.loads(),
                indexes.evictions()
            );
        }

        assert!(
            indexes.catalog().resident_count() <= 1,
            "the budget stopped being enforced during the thrash"
        );
    }

    #[test]
    fn steady_state_does_not_move_the_counters() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["p0", "p1"]);

        let indexes = open_with_room_for(dir.path(), 4);
        for _ in 0..5 {
            indexes
                .search(&scope_of("p0"), &[1.0, 0.0, 0.0, 0.0], 10, None)
                .unwrap();
        }

        assert_eq!(
            (indexes.loads(), indexes.evictions()),
            (1, 0),
            "a working set that fits produced restore/release churn, which \
             would make the thrash signal meaningless"
        );
    }

    #[test]
    fn a_corrupt_chunk_refuses_startup_by_default() {
        let _guard = index_rebuild_env_guard();
        std::env::remove_var("REMEM_ALLOW_INDEX_REBUILD");

        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["product"]);

        let seg = dir
            .path()
            .join("acme")
            .join("product")
            .join("hnsw_0000.seg");
        let mut bytes = std::fs::read(&seg).unwrap();
        let data_start = 36;
        bytes[data_start] ^= 0xFF;
        std::fs::write(&seg, &bytes).unwrap();

        let err = match PartitionedHnswIndexes::open(
            dir.path().to_path_buf(),
            config(),
            binding("legacy"),
            None,
        ) {
            Ok(_) => panic!("a corrupt chunk must refuse startup"),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("CRC32") || err.contains("Refusing to start"),
            "expected a corruption refusal, got: {err}"
        );
    }

    #[test]
    fn the_rebuild_opt_in_still_permits_startup() {
        let _guard = index_rebuild_env_guard();

        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), &["product"]);

        let seg = dir
            .path()
            .join("acme")
            .join("product")
            .join("hnsw_0000.seg");
        let mut bytes = std::fs::read(&seg).unwrap();
        bytes[36] ^= 0xFF;
        std::fs::write(&seg, &bytes).unwrap();

        std::env::set_var("REMEM_ALLOW_INDEX_REBUILD", "1");
        let opened = PartitionedHnswIndexes::open(
            dir.path().to_path_buf(),
            config(),
            binding("legacy"),
            None,
        );
        std::env::remove_var("REMEM_ALLOW_INDEX_REBUILD");

        assert!(
            opened.is_ok(),
            "the documented operator opt-in no longer permits starting with a corrupt index"
        );
    }
}
