//! One ordered index per indexed slot.
//!
//! Each is an ordinary `SegmentedBTreeIndex` — already a crash-safe
//! `u64 -> Bytes` ordered map with sealed segments, compaction and the
//! REM-46 parse-failure policy. Keying it by an order-encoded attribute
//! value instead of a timestamp is the entire adaptation.
//!
//! These are derived state. Every candidate they produce is verified against
//! the sidecar row before a caller sees it, so a stale or missing entry costs
//! a lookup and can never produce a wrong answer.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use super::row::AttrRow;
use super::schema::AttrSchema;
use crate::engine::error::Result;
use crate::engine::index::{BTreeConfig, IndexPosition, SegmentedBTreeIndex};
use crate::engine::storage::partition::{decode_record_key, PartitionBinding, PartitionScope};

type SlotIndexes = BTreeMap<u16, Arc<RwLock<SegmentedBTreeIndex>>>;

/// Which slice of one slot's order to read, and from where.
///
/// Bundled rather than passed loose because the five travel together
/// everywhere and mean nothing apart: a bound without a direction, or a
/// position without the range it sits in, is not a page of anything.
pub struct SlotPage<'a> {
    /// Inclusive lower bound on the order-encoded value.
    pub lo: u64,
    /// Inclusive upper bound.
    pub hi: u64,
    /// Resume strictly after this position.
    pub after: Option<&'a IndexPosition>,
    pub descending: bool,
    pub limit: usize,
}

pub struct AttrIndexes {
    root: PathBuf,
    schema: AttrSchema,
    default: PartitionBinding,
    ordered: RwLock<BTreeMap<PartitionBinding, SlotIndexes>>,
}

impl AttrIndexes {
    /// Open (or create) an index per indexed slot under `dir`.
    pub fn open(dir: PathBuf, schema: &AttrSchema) -> Result<Self> {
        Self::open_partitioned(dir, schema, PartitionBinding::legacy_default())
    }

    pub fn open_partitioned(
        dir: PathBuf,
        schema: &AttrSchema,
        default: PartitionBinding,
    ) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let mut ordered = BTreeMap::new();
        let default_child = dir
            .join(default.tenant().as_str())
            .join(default.partition().as_str());
        let old_layout = !default_child.exists()
            && schema
                .indexed_slots()
                .any(|def| dir.join(format!("ord.{}", def.slot)).exists());

        if old_layout {
            ordered.insert(default.clone(), Self::open_slots(&dir, schema)?);
        }

        for tenant in std::fs::read_dir(&dir)? {
            let tenant = tenant?;
            if !tenant.file_type()?.is_dir() {
                continue;
            }
            let tenant_id = crate::engine::storage::partition::TenantId::new(
                tenant.file_name().to_string_lossy().into_owned(),
            )
            .map_err(|err| crate::engine::error::StorageError::InvalidArgument(err.to_string()))?;
            for partition in std::fs::read_dir(tenant.path())? {
                let partition = partition?;
                if !partition.file_type()?.is_dir() {
                    continue;
                }
                let partition_id = crate::engine::storage::partition::PartitionId::new(
                    partition.file_name().to_string_lossy().into_owned(),
                )
                .map_err(|err| {
                    crate::engine::error::StorageError::InvalidArgument(err.to_string())
                })?;
                let binding = PartitionBinding::new(tenant_id.clone(), partition_id);
                ordered
                    .entry(binding)
                    .or_insert(Self::open_slots(&partition.path(), schema)?);
            }
        }

        if ordered.is_empty() {
            let dir = dir
                .join(default.tenant().as_str())
                .join(default.partition().as_str());
            ordered.insert(default.clone(), Self::open_slots(&dir, schema)?);
        }
        Ok(Self {
            root: dir,
            schema: schema.clone(),
            default,
            ordered: RwLock::new(ordered),
        })
    }

    fn open_slots(
        dir: &std::path::Path,
        schema: &AttrSchema,
    ) -> Result<BTreeMap<u16, Arc<RwLock<SegmentedBTreeIndex>>>> {
        let mut ordered = BTreeMap::new();
        for def in schema.indexed_slots() {
            let slot_dir = dir.join(format!("ord.{}", def.slot));
            std::fs::create_dir_all(&slot_dir)?;
            let index = SegmentedBTreeIndex::load_from_dir(BTreeConfig::default(), slot_dir)?;
            ordered.insert(def.slot, Arc::new(RwLock::new(index)));
        }
        Self::sweep_unindexed_slot_dirs(dir, schema);
        Ok(ordered)
    }

    /// Delete the segment directories of slots that used to carry an index
    /// and no longer do.
    ///
    /// A slot that stops being indexed leaves its `ord.<slot>` directory
    /// behind. Nothing opens it, so it is inert -- but it is a full copy of
    /// an index for a value the engine no longer has any access path for,
    /// and on a large corpus that is real disk. Best-effort by design: a
    /// removal that fails wastes space, and space is not worth refusing to
    /// start over.
    ///
    /// Only slots the *current* schema declares unindexed are swept. A
    /// directory for a slot the schema does not mention at all is left
    /// alone: the schema gate already refuses a data directory from a newer
    /// build, so an unrecognised slot means something this code does not
    /// understand, and deleting it would be the wrong guess.
    fn sweep_unindexed_slot_dirs(dir: &std::path::Path, schema: &AttrSchema) {
        for def in &schema.slots {
            if def.indexed && !def.retired {
                continue;
            }
            let slot_dir = dir.join(format!("ord.{}", def.slot));
            if !slot_dir.is_dir() {
                continue;
            }
            match std::fs::remove_dir_all(&slot_dir) {
                Ok(()) => tracing::info!(
                    slot = def.slot,
                    name = %def.name,
                    "removed the segment directory of a slot that no longer carries an index"
                ),
                Err(e) => tracing::warn!(
                    slot = def.slot,
                    error = %e,
                    "failed to remove an unindexed slot's segment directory; \
                     it is inert, only wasted disk"
                ),
            }
        }
    }

    fn binding_for_key(&self, key: &[u8]) -> Result<PartitionBinding> {
        Ok(decode_record_key(key)
            .map_err(|err| crate::engine::error::StorageError::InvalidArgument(err.to_string()))?
            .map(|key| key.binding().clone())
            .unwrap_or_else(|| self.default.clone()))
    }

    fn ensure_binding(&self, binding: &PartitionBinding) -> Result<()> {
        if !self.ordered.read().contains_key(binding) {
            let dir = self
                .root
                .join(binding.tenant().as_str())
                .join(binding.partition().as_str());
            self.ordered
                .write()
                .insert(binding.clone(), Self::open_slots(&dir, &self.schema)?);
        }
        Ok(())
    }

    pub fn has_slot(&self, slot: u16) -> bool {
        self.ordered
            .read()
            .values()
            .any(|indexes| indexes.contains_key(&slot))
    }

    /// Index every indexed slot present in `row` for `record_key`.
    ///
    /// The prior entry is removed first, so an updated value does not leave a
    /// second entry behind. Correctness does not depend on this — verification
    /// would reject the stale one — but it keeps the indexes from growing
    /// without bound under repeated updates.
    pub fn apply_row(&self, record_key: &Bytes, row: &AttrRow) -> Result<()> {
        let binding = self.binding_for_key(record_key)?;
        self.ensure_binding(&binding)?;
        let guard = self.ordered.read();
        let Some(indexes) = guard.get(&binding) else {
            return Ok(());
        };
        for (slot, index) in indexes {
            let Some(value) = row.get(*slot) else {
                continue;
            };
            let index_guard = index.write();
            index_guard.remove(record_key.as_ref())?;
            index_guard.insert(value.order_key(), record_key.clone())?;
        }
        Ok(())
    }

    /// Best-effort removal from every slot index.
    ///
    /// Not WAL-recorded: these are derived, and a leftover entry is caught by
    /// verification. This is hygiene, not correctness.
    pub fn remove_key(&self, record_key: &[u8]) -> Result<()> {
        let binding = self.binding_for_key(record_key)?;
        if let Some(indexes) = self.ordered.read().get(&binding) {
            for index in indexes.values() {
                index.write().remove(record_key)?;
            }
        }
        Ok(())
    }

    /// Retire a record's entry from one slot's index, leaving every other
    /// slot -- and the record's row -- untouched.
    ///
    /// The narrow counterpart to `remove_key`, which clears every slot. A
    /// caller that must keep a record findable by one access path while
    /// removing it from another has no use for the broad form: archiving takes
    /// a memory out of the browsing order while cleanup must still be able to
    /// reach it.
    ///
    /// Succeeds when the slot carries no index, when the record has no entry
    /// in it, and when the entry was already retired: all three are the
    /// idempotent case, not an error.
    pub fn retire_slot_entry(&self, record_key: &[u8], slot: u16) -> Result<()> {
        let binding = self.binding_for_key(record_key)?;
        if let Some(indexes) = self.ordered.read().get(&binding) {
            if let Some(index) = indexes.get(&slot) {
                index.write().remove(record_key)?;
            }
        }
        Ok(())
    }

    /// Inclusive range over one slot's index, or `None` if the slot has none.
    pub fn range(&self, slot: u16, lo: u64, hi: u64) -> Option<Vec<(u64, Bytes)>> {
        if !self.has_slot(slot) {
            return None;
        }
        Some(
            self.ordered
                .read()
                .values()
                .filter_map(|indexes| indexes.get(&slot))
                .flat_map(|index| index.read().range(lo, hi))
                .collect(),
        )
    }

    pub fn range_partitioned(
        &self,
        scope: &PartitionScope,
        slot: u16,
        lo: u64,
        hi: u64,
        per_partition_limit: Option<usize>,
    ) -> Option<Vec<(u64, Bytes)>> {
        if !self.has_slot(slot) {
            return None;
        }
        let guard = self.ordered.read();
        let mut entries = scope
            .partitions()
            .iter()
            .flat_map(|partition| {
                let binding = PartitionBinding::new(scope.tenant().clone(), partition.clone());
                guard.get(&binding).into_iter().flat_map(|indexes| {
                    indexes.get(&slot).into_iter().flat_map(|index| {
                        let index = index.read();
                        per_partition_limit.map_or_else(
                            || index.range(lo, hi),
                            |limit| index.range_limit(lo, hi, limit),
                        )
                    })
                })
            })
            .collect::<Vec<_>>();
        entries.sort();
        Some(entries)
    }

    /// One page of a slot's total order, across every partition in `scope`.
    ///
    /// Each partition is asked for its own first `limit` entries past the
    /// position, so no partition walks further than the page it might
    /// contribute — the property that keeps a scoped read's cost independent
    /// of what unauthorized partitions hold (REM-76).
    pub fn range_page_partitioned(
        &self,
        scope: &PartitionScope,
        slot: u16,
        page: SlotPage<'_>,
    ) -> Option<Vec<IndexPosition>> {
        let SlotPage {
            lo,
            hi,
            after,
            descending,
            limit,
        } = page;
        if !self.has_slot(slot) {
            return None;
        }
        if limit == 0 {
            return Some(Vec::new());
        }
        let guard = self.ordered.read();
        let mut entries = scope
            .partitions()
            .iter()
            .flat_map(|partition| {
                let binding = PartitionBinding::new(scope.tenant().clone(), partition.clone());
                guard.get(&binding).into_iter().flat_map(|indexes| {
                    indexes
                        .get(&slot)
                        .into_iter()
                        .flat_map(|index| index.read().range_page(lo, hi, after, descending, limit))
                })
            })
            .collect::<Vec<_>>();
        entries.sort();
        entries.dedup();
        if descending {
            entries.reverse();
        }
        entries.truncate(limit);
        Some(entries)
    }

    /// Insert an index entry directly, bypassing `apply_row`.
    ///
    /// The only way to stage a *superseded* entry — one whose order key no
    /// longer matches its row — which `apply_row` exists to prevent and
    /// `select_ordered` exists to survive. Test-only: production writes go
    /// through `apply_row` so the two stay in step.
    ///
    /// To stage a genuinely duplicate entry for a key that is already
    /// indexed, the live entry must first be moved into a sealed segment via
    /// `save_if_dirty()`. The underlying `BTreeIndex::insert` deduplicates by
    /// key within the growing segment — inserting the same key again there
    /// moves it to the new order instead of adding a second entry — and that
    /// reverse-index tracking does not span a `save_if_dirty()` boundary, so
    /// sealing first is what lets this call land as a second, independent
    /// entry rather than relocating the one already staged.
    #[cfg(test)]
    pub fn insert_raw(&self, slot: u16, order: u64, key: Bytes) -> Result<()> {
        if let Some(index) = self
            .ordered
            .read()
            .values()
            .find_map(|indexes| indexes.get(&slot))
        {
            index
                .read()
                .insert_raw_without_retiring_sealed(order, key)?;
        }
        Ok(())
    }

    pub fn save_if_dirty(&self) -> Result<()> {
        for indexes in self.ordered.read().values() {
            for index in indexes.values() {
                index.write().save_if_dirty()?;
            }
        }
        Ok(())
    }

    /// Compact every ordered slot index that needs it.
    ///
    /// Each slot is compacted independently under that slot's write lock.
    /// Readers of other slots and unrelated server work can continue while a
    /// single slot is being merged.
    pub fn compact_if_needed(&self) -> Result<()> {
        for indexes in self.ordered.read().values() {
            for index in indexes.values() {
                if index.read().needs_compaction() {
                    index.write().compact()?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::attr::schema::SlotDef;
    use crate::engine::attr::value::{AttrType, AttrValue};
    use crate::engine::storage::partition::{
        PartitionBinding, PartitionId, PartitionScope, TenantId,
    };

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

    /// A slot that stops being indexed must leave no index behind. The row
    /// keeps the value; only the access path goes away.
    #[test]
    fn un_indexing_a_slot_removes_its_segment_directory() {
        let dir = tempfile::tempdir().unwrap();

        // Open once with the slot indexed, so its directory exists.
        let indexed = schema();
        AttrIndexes::open(dir.path().to_path_buf(), &indexed).unwrap();
        let existing = find_slot_dir(dir.path(), 2)
            .expect("premise: the indexed slot must get a directory somewhere under the root");

        // Reopen with the same slot unindexed.
        let mut unindexed = schema();
        unindexed.slots[1].indexed = false;
        AttrIndexes::open(dir.path().to_path_buf(), &unindexed).unwrap();

        assert!(
            !existing.is_dir(),
            "an unindexed slot must not keep a full index for a value nothing can look up by"
        );
    }

    /// Locate the `ord.<slot>` directory wherever the partition layout put it.
    fn find_slot_dir(root: &std::path::Path, slot: u16) -> Option<PathBuf> {
        let name = format!("ord.{slot}");
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).ok()? {
                let path = entry.ok()?.path();
                if !path.is_dir() {
                    continue;
                }
                if path.file_name().is_some_and(|n| n == name.as_str()) {
                    return Some(path);
                }
                stack.push(path);
            }
        }
        None
    }

    /// A directory for a slot the schema does not mention is left alone --
    /// deleting it would be a guess about data this build does not model.
    #[test]
    fn a_slot_the_schema_does_not_mention_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        AttrIndexes::open(dir.path().to_path_buf(), &schema()).unwrap();

        let stranger = find_slot_dir(dir.path(), 2)
            .expect("premise: an indexed slot directory must exist to sit beside")
            .parent()
            .unwrap()
            .join("ord.99");
        std::fs::create_dir_all(&stranger).unwrap();

        AttrIndexes::open(dir.path().to_path_buf(), &schema()).unwrap();
        assert!(
            stranger.is_dir(),
            "an unrecognised slot must survive the sweep"
        );
    }

    fn row(importance: f32) -> AttrRow {
        let mut r = AttrRow::new(1);
        r.set(0, AttrValue::Bool(false));
        r.set(2, AttrValue::F32(importance));
        r
    }

    #[test]
    fn only_indexed_slots_get_an_index() {
        let dir = tempfile::tempdir().unwrap();
        let idx = AttrIndexes::open(dir.path().to_path_buf(), &schema()).unwrap();
        assert!(idx.has_slot(2));
        assert!(
            !idx.has_slot(0),
            "low-cardinality slots carry no index (spec §3.1)"
        );
    }

    #[test]
    fn range_returns_keys_within_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let idx = AttrIndexes::open(dir.path().to_path_buf(), &schema()).unwrap();

        idx.apply_row(&Bytes::from("memory:low"), &row(0.1))
            .unwrap();
        idx.apply_row(&Bytes::from("memory:mid"), &row(0.5))
            .unwrap();
        idx.apply_row(&Bytes::from("memory:high"), &row(0.9))
            .unwrap();

        let lo = AttrValue::F32(0.4).order_key();
        let hi = AttrValue::F32(1.0).order_key();
        let hits = idx.range(2, lo, hi).unwrap();
        let keys: Vec<&[u8]> = hits.iter().map(|(_, k)| k.as_ref()).collect();

        assert!(keys.contains(&b"memory:mid".as_slice()));
        assert!(keys.contains(&b"memory:high".as_slice()));
        assert!(!keys.contains(&b"memory:low".as_slice()));
    }

    #[test]
    fn unauthorized_partition_growth_does_not_change_scoped_attribute_visits() {
        let dir = tempfile::tempdir().unwrap();
        let product = PartitionBinding::new(
            TenantId::default_legacy(),
            PartitionId::new("product").unwrap(),
        );
        let finance = PartitionBinding::new(
            TenantId::default_legacy(),
            PartitionId::new("finance").unwrap(),
        );
        let idx =
            AttrIndexes::open_partitioned(dir.path().to_path_buf(), &schema(), product.clone())
                .unwrap();
        let product_key =
            crate::engine::storage::partition::encode_record_key(&product, b"memory:product")
                .unwrap();
        idx.apply_row(&product_key, &row(0.5)).unwrap();
        let scope = PartitionScope::single(product);
        crate::engine::index::BTreeIndex::reset_range_visit_count();
        let _ = idx.range_partitioned(&scope, 2, 0, u64::MAX, Some(1));
        let before = crate::engine::index::BTreeIndex::range_visit_count();
        for n in 0..100 {
            let key = crate::engine::storage::partition::encode_record_key(
                &finance,
                format!("memory:finance-{n}").as_bytes(),
            )
            .unwrap();
            idx.apply_row(&key, &row(n as f32 / 100.0)).unwrap();
        }
        crate::engine::index::BTreeIndex::reset_range_visit_count();
        let _ = idx.range_partitioned(&scope, 2, 0, u64::MAX, Some(1));
        assert_eq!(
            before,
            crate::engine::index::BTreeIndex::range_visit_count()
        );
    }

    #[test]
    fn range_orders_negative_values_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = schema();
        s.slots[1].name = "valence".into();
        let idx = AttrIndexes::open(dir.path().to_path_buf(), &s).unwrap();

        idx.apply_row(&Bytes::from("memory:neg"), &row(-0.8))
            .unwrap();
        idx.apply_row(&Bytes::from("memory:pos"), &row(0.8))
            .unwrap();

        let hits = idx
            .range(
                2,
                AttrValue::F32(-1.0).order_key(),
                AttrValue::F32(0.0).order_key(),
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].1.as_ref(), b"memory:neg");
    }

    #[test]
    fn re_applying_a_row_replaces_the_old_entry() {
        let dir = tempfile::tempdir().unwrap();
        let idx = AttrIndexes::open(dir.path().to_path_buf(), &schema()).unwrap();
        let key = Bytes::from("memory:x");

        idx.apply_row(&key, &row(0.1)).unwrap();
        idx.apply_row(&key, &row(0.9)).unwrap();

        let low = idx
            .range(
                2,
                AttrValue::F32(0.0).order_key(),
                AttrValue::F32(0.2).order_key(),
            )
            .unwrap();
        assert!(low.is_empty(), "the superseded entry must not linger");

        let high = idx
            .range(
                2,
                AttrValue::F32(0.8).order_key(),
                AttrValue::F32(1.0).order_key(),
            )
            .unwrap();
        assert_eq!(high.len(), 1);
    }

    #[test]
    fn indexes_survive_save_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let idx = AttrIndexes::open(dir.path().to_path_buf(), &schema()).unwrap();
            idx.apply_row(&Bytes::from("memory:kept"), &row(0.7))
                .unwrap();
            idx.save_if_dirty().unwrap();
        }
        let reopened = AttrIndexes::open(dir.path().to_path_buf(), &schema()).unwrap();
        let hits = reopened
            .range(
                2,
                AttrValue::F32(0.6).order_key(),
                AttrValue::F32(0.8).order_key(),
            )
            .unwrap();
        assert_eq!(hits.len(), 1, "entries must survive a checkpoint");
    }

    #[test]
    fn remove_key_drops_entries_from_every_slot() {
        let dir = tempfile::tempdir().unwrap();
        let idx = AttrIndexes::open(dir.path().to_path_buf(), &schema()).unwrap();
        let key = Bytes::from("memory:gone");
        idx.apply_row(&key, &row(0.5)).unwrap();
        idx.remove_key(key.as_ref()).unwrap();
        let hits = idx.range(2, 0, u64::MAX).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn compact_drops_stale_sealed_attribute_entries_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let key = Bytes::from("memory:x");
        {
            let idx = AttrIndexes::open(dir.path().to_path_buf(), &schema()).unwrap();
            idx.apply_row(&key, &row(0.1)).unwrap();
            idx.save_if_dirty().unwrap();

            idx.apply_row(&key, &row(0.9)).unwrap();
            idx.save_if_dirty().unwrap();
            idx.compact_if_needed().unwrap();

            let hits = idx.range(2, 0, u64::MAX).unwrap();
            assert_eq!(hits, vec![(AttrValue::F32(0.9).order_key(), key.clone())]);
        }

        let reopened = AttrIndexes::open(dir.path().to_path_buf(), &schema()).unwrap();
        let hits = reopened.range(2, 0, u64::MAX).unwrap();
        assert_eq!(hits, vec![(AttrValue::F32(0.9).order_key(), key)]);
    }
}
