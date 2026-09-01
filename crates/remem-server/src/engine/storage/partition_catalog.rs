//! Per-partition vector index accounting that does not require residency.
//!
//! Every figure here is derivable from files already on disk, so a partition's
//! live vector count and size can be answered without materializing its graph:
//!
//! * live count — `Σ chunk.entry_count` from the partition's `SegmentManifest`,
//!   minus the cardinality of its `deleted_nodes.bin`. `HnswIndex` keeps node
//!   slots and tombstones as separate counters for exactly this reason, so the
//!   derived figure equals `HnswIndex::len()` rather than approximating it.
//! * on-disk bytes — `Σ chunk.file_size` from the same manifest.
//!
//! That equality is what lets residency become optional. Without it, the counts
//! `StorageEngine::scoped_vector_count` feeds to the widening loop in
//! `query::executor` would read zero for any partition that happens not to be
//! loaded, silently under-bounding retrieval.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use parking_lot::RwLock;

use super::partition::{PartitionBinding, PartitionScope};
use crate::engine::error::Result;
use crate::engine::index::{HnswIndex, SegmentManifest};

/// What the catalog knows about one partition.
#[derive(Debug, Clone, Copy, Default)]
pub struct PartitionStats {
    /// Live vectors, excluding tombstoned ones.
    pub live_count: usize,
    /// Node slots, tombstoned ones included -- what a traversal steps over.
    pub node_count: usize,
    /// Bytes this partition's persisted chunks occupy.
    pub disk_bytes: u64,
    /// Bytes its graph occupies in memory, `None` when not resident.
    pub resident_bytes: Option<usize>,
    /// Monotonic tick of last use, for LRU ordering.
    pub last_used: u64,
}

impl PartitionStats {
    pub fn is_resident(&self) -> bool {
        self.resident_bytes.is_some()
    }
}

/// Accounting for every partition the engine knows about, resident or not.
pub struct PartitionCatalog {
    entries: RwLock<BTreeMap<PartitionBinding, PartitionStats>>,
    /// Source of LRU ordering. A counter rather than a clock: it is only ever
    /// compared against itself, and a monotonic counter cannot go backwards the
    /// way a wall clock can.
    tick: AtomicU64,
    resident_bytes: AtomicUsize,
    /// Entry lookups performed by `scoped_nodes`, so a test can assert the
    /// lookup is scope-sized rather than deployment-sized. The requirement is
    /// about cost, and a returned value alone cannot demonstrate cost.
    ///
    /// Counted on `scoped_nodes` rather than `scoped_live` because the widening
    /// loop reads that one: a probe left on the figure the loop no longer
    /// consults would guard nothing.
    #[cfg(test)]
    scoped_probes: AtomicUsize,
}

impl PartitionCatalog {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
            tick: AtomicU64::new(0),
            resident_bytes: AtomicUsize::new(0),
            #[cfg(test)]
            scoped_probes: AtomicUsize::new(0),
        }
    }

    /// Read one partition's live count and on-disk size from its persisted
    /// metadata, without opening a single `.seg` file.
    ///
    /// Returns `None` when the directory holds no manifest — a partition that
    /// has never been saved.
    pub fn derive_from_disk(dir: &Path) -> Result<Option<(usize, usize, u64)>> {
        let Some(manifest) = SegmentManifest::load(dir, "hnsw")? else {
            return Ok(None);
        };

        let mut slots: u64 = 0;
        let mut disk_bytes: u64 = 0;
        for chunk in &manifest.chunks {
            slots += u64::from(chunk.entry_count);
            disk_bytes += chunk.file_size;
        }

        let tombstoned = HnswIndex::load_deleted_nodes(dir)?.len() as u64;
        let live = slots.saturating_sub(tombstoned) as usize;

        Ok(Some((live, slots as usize, disk_bytes)))
    }

    /// Record what disk says about a partition, for a partition that is not
    /// resident.
    pub fn record_from_disk(
        &self,
        binding: &PartitionBinding,
        live_count: usize,
        node_count: usize,
        disk_bytes: u64,
    ) {
        let mut entries = self.entries.write();
        let entry = entries.entry(binding.clone()).or_default();
        entry.live_count = live_count;
        entry.node_count = node_count;
        entry.disk_bytes = disk_bytes;
    }

    /// Bring the catalog's count back in step with a resident graph.
    ///
    /// Called after any mutation rather than incrementing per operation:
    /// `HnswIndex::insert` updates in place for a key it already holds, so the
    /// caller cannot tell a new vector from an overwrite, and `len()` is free
    /// and exact while the graph is loaded.
    pub fn sync_resident(&self, binding: &PartitionBinding, live_count: usize, node_count: usize) {
        let mut entries = self.entries.write();
        let entry = entries.entry(binding.clone()).or_default();
        entry.live_count = live_count;
        entry.node_count = node_count;
    }

    /// Note that a partition was just used, for LRU ordering.
    pub fn touch(&self, binding: &PartitionBinding) {
        let tick = self.tick.fetch_add(1, Ordering::Relaxed);
        let mut entries = self.entries.write();
        entries.entry(binding.clone()).or_default().last_used = tick;
    }

    /// Record that a partition's graph is now in memory, occupying `bytes`.
    pub fn mark_resident(&self, binding: &PartitionBinding, bytes: usize) {
        let mut entries = self.entries.write();
        let entry = entries.entry(binding.clone()).or_default();
        if let Some(previous) = entry.resident_bytes.replace(bytes) {
            self.resident_bytes.fetch_sub(previous, Ordering::Relaxed);
        }
        self.resident_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record that a partition's graph has left memory. Its counts stay: they
    /// describe the partition, not the copy of it that was loaded.
    pub fn mark_evicted(&self, binding: &PartitionBinding) {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.get_mut(binding) {
            if let Some(bytes) = entry.resident_bytes.take() {
                self.resident_bytes.fetch_sub(bytes, Ordering::Relaxed);
            }
        }
    }

    pub fn live_count(&self, binding: &PartitionBinding) -> usize {
        self.entries
            .read()
            .get(binding)
            .map(|entry| entry.live_count)
            .unwrap_or(0)
    }

    /// Live vectors across every known partition.
    /// Node slots across every known partition, tombstoned ones included.
    pub fn total_nodes(&self) -> usize {
        self.entries
            .read()
            .values()
            .map(|entry| entry.node_count)
            .sum()
    }

    pub fn total_live(&self) -> usize {
        self.entries
            .read()
            .values()
            .map(|entry| entry.live_count)
            .sum()
    }

    /// Live vectors held by the partitions in `scope`.
    ///
    /// Proportional to the scope, not to the deployment: a caller bound to one
    /// partition performs one lookup however many partitions exist alongside it.
    /// Node slots held by the partitions in `scope`, tombstoned ones included.
    ///
    /// The widening retry bounds itself on this rather than on `scoped_live`.
    /// The two diverge exactly when records are retired from the index, and
    /// the live figure is the wrong one for that decision: the traversal steps
    /// over tombstones, so a loop that stops once it has asked for the live
    /// count concludes the scope is exhausted while most of the graph is still
    /// unvisited -- returning a short page and reporting it as complete.
    pub fn scoped_nodes(&self, scope: &PartitionScope) -> usize {
        let entries = self.entries.read();
        scope
            .partitions()
            .iter()
            .map(|partition| {
                let binding = PartitionBinding::new(scope.tenant().clone(), partition.clone());
                #[cfg(test)]
                self.scoped_probes.fetch_add(1, Ordering::Relaxed);
                entries
                    .get(&binding)
                    .map(|entry| entry.node_count)
                    .unwrap_or(0)
            })
            .sum()
    }

    #[allow(dead_code)]
    pub fn scoped_live(&self, scope: &PartitionScope) -> usize {
        let entries = self.entries.read();
        scope
            .partitions()
            .iter()
            .map(|partition| {
                let binding = PartitionBinding::new(scope.tenant().clone(), partition.clone());
                entries
                    .get(&binding)
                    .map(|entry| entry.live_count)
                    .unwrap_or(0)
            })
            .sum()
    }

    pub fn counts(&self) -> Vec<(PartitionBinding, usize)> {
        self.entries
            .read()
            .iter()
            .map(|(binding, entry)| (binding.clone(), entry.live_count))
            .collect()
    }

    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes.load(Ordering::Relaxed)
    }

    pub fn resident_count(&self) -> usize {
        self.entries
            .read()
            .values()
            .filter(|entry| entry.is_resident())
            .count()
    }

    /// Resident partitions, least recently used first.
    pub fn lru_resident(&self) -> Vec<(PartitionBinding, usize)> {
        let mut resident: Vec<_> = self
            .entries
            .read()
            .iter()
            .filter_map(|(binding, entry)| {
                entry
                    .resident_bytes
                    .map(|bytes| (binding.clone(), bytes, entry.last_used))
            })
            .collect();
        resident.sort_by_key(|(_, _, last_used)| *last_used);
        resident
            .into_iter()
            .map(|(binding, bytes, _)| (binding, bytes))
            .collect()
    }

    #[cfg(test)]
    pub fn stats(&self, binding: &PartitionBinding) -> Option<PartitionStats> {
        self.entries.read().get(binding).copied()
    }

    #[cfg(test)]
    pub fn take_scoped_probes(&self) -> usize {
        self.scoped_probes.swap(0, Ordering::Relaxed)
    }
}

impl Default for PartitionCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::index::HnswConfig;
    use std::path::PathBuf;

    /// Build a partition on disk, returning its directory and the live count
    /// the index itself reports before it is dropped.
    fn persisted_partition(
        dir: PathBuf,
        insert: &[&[u8]],
        remove: &[&[u8]],
        save_between: bool,
    ) -> (PathBuf, usize) {
        let config = HnswConfig::with_dim(4);
        let index = HnswIndex::new(config);

        for (i, key) in insert.iter().enumerate() {
            let mut vector = vec![0.0; 4];
            vector[i % 4] = 1.0 + i as f32;
            index.insert(key.to_vec(), vector).unwrap();
        }

        if save_between {
            // Pins the case where tombstones are written with no insert after
            // them — the shape `deleted_nodes_persist_across_reload_without_
            // intervening_insert` already covers for the index itself.
            index.save_dirty_chunks(&dir).unwrap();
            index.save_deleted_nodes(&dir).unwrap();
        }

        for key in remove {
            assert!(index.remove(key), "test fixture removed a missing key");
        }

        index.save_dirty_chunks(&dir).unwrap();
        index.save_deleted_nodes(&dir).unwrap();

        let live = index.len();
        (dir, live)
    }

    #[test]
    fn derived_live_count_matches_the_loaded_index() {
        let root = tempfile::tempdir().unwrap();

        struct Case {
            name: &'static str,
            insert: Vec<&'static [u8]>,
            remove: Vec<&'static [u8]>,
            save_between: bool,
        }

        let cases = vec![
            Case {
                name: "no_deletions",
                insert: vec![b"a", b"b", b"c"],
                remove: vec![],
                save_between: false,
            },
            Case {
                name: "with_deletions",
                insert: vec![b"a", b"b", b"c"],
                remove: vec![b"b"],
                save_between: false,
            },
            Case {
                name: "deletions_saved_without_intervening_insert",
                insert: vec![b"a", b"b", b"c"],
                remove: vec![b"a", b"c"],
                save_between: true,
            },
        ];

        for Case {
            name,
            insert,
            remove,
            save_between,
        } in cases
        {
            let dir = root.path().join(name);
            std::fs::create_dir_all(&dir).unwrap();
            let (dir, live_before) = persisted_partition(dir, &insert, &remove, save_between);

            let (derived, derived_nodes, disk_bytes) = PartitionCatalog::derive_from_disk(&dir)
                .unwrap()
                .unwrap_or_else(|| panic!("{name}: no manifest was written"));

            let reloaded = HnswIndex::load_chunked(&dir, HnswConfig::with_dim(4))
                .unwrap()
                .unwrap_or_else(|| panic!("{name}: index did not reload"));

            assert_eq!(
                derived,
                reloaded.len(),
                "{name}: count derived from disk disagrees with the loaded index — \
                 residency cannot be made optional while these two differ"
            );
            assert_eq!(
                derived_nodes,
                reloaded.node_count(),
                "{name}: node slots derived from disk disagree with the loaded \
                 index — the widening bound reads this figure for partitions \
                 that are not resident"
            );
            assert_eq!(
                derived, live_before,
                "{name}: derived count disagrees with what the index reported \
                 before it was persisted"
            );
            assert!(
                disk_bytes > 0,
                "{name}: persisted chunks reported zero bytes"
            );
        }
    }

    #[test]
    fn derive_reports_nothing_for_a_partition_never_saved() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            PartitionCatalog::derive_from_disk(dir.path())
                .unwrap()
                .is_none(),
            "a directory with no manifest must be reported as absent, not as empty"
        );
    }

    #[test]
    fn scoped_node_lookup_cost_does_not_grow_with_unrelated_partitions() {
        use crate::engine::storage::partition::{PartitionId, TenantId};

        let catalog = PartitionCatalog::new();
        let tenant = TenantId::new("acme").unwrap();
        let bound = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        catalog.record_from_disk(&bound, 3, 3, 0);
        let scope = PartitionScope::single(bound);

        catalog.take_scoped_probes();
        let small_deployment = catalog.scoped_nodes(&scope);
        let probes_before = catalog.take_scoped_probes();

        for i in 0..200 {
            let binding = PartitionBinding::new(
                tenant.clone(),
                PartitionId::new(format!("neighbour-{i}")).unwrap(),
            );
            catalog.record_from_disk(&binding, 5_000, 5_000, 0);
        }

        let large_deployment = catalog.scoped_nodes(&scope);
        let probes_after = catalog.take_scoped_probes();

        assert_eq!(
            small_deployment, large_deployment,
            "an unrelated partition changed a bound scope's vector count"
        );
        assert_eq!(
            probes_before, probes_after,
            "the bound scope's accounting did more work once 200 unrelated \
             partitions existed: cost is still coupled to the deployment"
        );
        assert_eq!(
            probes_after,
            scope.partitions().len(),
            "accounting probed more entries than the scope holds"
        );
    }

    #[test]
    fn scoped_live_counts_only_the_bound_partitions() {
        use crate::engine::storage::partition::{PartitionId, TenantId};

        let catalog = PartitionCatalog::new();
        let tenant = TenantId::new("acme").unwrap();
        for (partition, count) in [("product", 3), ("finance", 97)] {
            let binding =
                PartitionBinding::new(tenant.clone(), PartitionId::new(partition).unwrap());
            catalog.record_from_disk(&binding, count, count, 0);
        }

        let scope = PartitionScope::single(PartitionBinding::new(
            tenant,
            PartitionId::new("product").unwrap(),
        ));
        assert_eq!(catalog.scoped_live(&scope), 3);
        assert_eq!(catalog.total_live(), 100);
    }
}
