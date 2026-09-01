//! Partition-owned exact secondary indexes.
//!
//! Each owner keeps one segmented index per physical partition. The outer
//! owner is deliberately small: it routes writes from the encoded record key
//! and provides merge helpers for scoped reads, while each segmented index
//! retains its existing checkpoint, deletion, and compaction behavior.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::partition::{decode_record_key, PartitionBinding, PartitionScope};
use crate::engine::error::{Result, StorageError};
use crate::engine::index::{
    BTreeConfig, InvertedIndexConfig, SegmentedBTreeIndex, SegmentedInvertedIndex,
};

#[cfg(test)]
static TAG_POSTING_VISITS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn partition_dir(root: &Path, binding: &PartitionBinding) -> PathBuf {
    root.join(binding.tenant().as_str())
        .join(binding.partition().as_str())
}

fn binding_for_key(key: &[u8], default: &PartitionBinding) -> Result<PartitionBinding> {
    Ok(decode_record_key(key)
        .map_err(|err| StorageError::InvalidArgument(err.to_string()))?
        .map(|key| key.binding().clone())
        .unwrap_or_else(|| default.clone()))
}

fn discover_bindings(root: &Path) -> Result<Vec<PartitionBinding>> {
    let mut bindings = Vec::new();
    if !root.exists() {
        return Ok(bindings);
    }
    for tenant in std::fs::read_dir(root)? {
        let tenant = tenant?;
        if !tenant.file_type()?.is_dir() {
            continue;
        }
        let tenant_id = crate::engine::storage::partition::TenantId::new(
            tenant.file_name().to_string_lossy().into_owned(),
        )
        .map_err(|err| StorageError::InvalidArgument(err.to_string()))?;
        for partition in std::fs::read_dir(tenant.path())? {
            let partition = partition?;
            if !partition.file_type()?.is_dir() {
                continue;
            }
            let partition_id = crate::engine::storage::partition::PartitionId::new(
                partition.file_name().to_string_lossy().into_owned(),
            )
            .map_err(|err| StorageError::InvalidArgument(err.to_string()))?;
            bindings.push(PartitionBinding::new(tenant_id.clone(), partition_id));
        }
    }
    Ok(bindings)
}

pub struct PartitionedTimeSeriesIndex {
    root: PathBuf,
    config: BTreeConfig,
    default: PartitionBinding,
    indexes: BTreeMap<PartitionBinding, SegmentedBTreeIndex>,
}

impl PartitionedTimeSeriesIndex {
    pub fn open(root: PathBuf, config: BTreeConfig, default: PartitionBinding) -> Result<Self> {
        std::fs::create_dir_all(&root)?;
        let mut indexes = BTreeMap::new();

        // A pre-partition layout stores the shared index directly at root.
        // Keep it addressable as the configured default until its migration
        // rewrites it into child directories.
        for binding in discover_bindings(&root)? {
            indexes
                .entry(binding.clone())
                .or_insert(SegmentedBTreeIndex::load_from_dir(
                    config.clone(),
                    partition_dir(&root, &binding),
                )?);
        }
        if !indexes.contains_key(&default)
            && (root.join("timeseries.manifest").exists() || root.join("timeseries.idx").exists())
        {
            indexes.insert(
                default.clone(),
                SegmentedBTreeIndex::load_from_dir(config.clone(), root.clone())?,
            );
        }
        if indexes.is_empty() {
            let dir = partition_dir(&root, &default);
            std::fs::create_dir_all(&dir)?;
            indexes.insert(
                default.clone(),
                SegmentedBTreeIndex::load_from_dir(config.clone(), dir)?,
            );
        }
        Ok(Self {
            root,
            config,
            default,
            indexes,
        })
    }

    fn index_mut(&mut self, binding: &PartitionBinding) -> Result<&mut SegmentedBTreeIndex> {
        if !self.indexes.contains_key(binding) {
            let dir = partition_dir(&self.root, binding);
            std::fs::create_dir_all(&dir)?;
            let index = SegmentedBTreeIndex::load_from_dir(self.config.clone(), dir)?;
            self.indexes.insert(binding.clone(), index);
        }
        self.indexes.get_mut(binding).ok_or_else(|| {
            StorageError::InvalidArgument("partition time-series index was not opened".into())
        })
    }

    pub fn insert(&mut self, timestamp: u64, key: impl Into<Bytes>) -> Result<()> {
        let key = key.into();
        let binding = binding_for_key(&key, &self.default)?;
        self.index_mut(&binding)?.insert(timestamp, key)
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<bool> {
        let binding = binding_for_key(key, &self.default)?;
        self.index_mut(&binding)?.remove(key)
    }

    pub fn range(&self, start: u64, end: u64) -> Vec<(u64, Bytes)> {
        let mut out = self
            .indexes
            .values()
            .flat_map(|index| index.range(start, end))
            .collect::<Vec<_>>();
        out.sort();
        out
    }

    pub fn range_limit(&self, start: u64, end: u64, limit: usize) -> Vec<(u64, Bytes)> {
        let mut out = self
            .indexes
            .values()
            .flat_map(|index| index.range_limit(start, end, limit))
            .collect::<Vec<_>>();
        out.sort();
        out.truncate(limit);
        out
    }

    pub fn range_partitioned(
        &self,
        scope: &PartitionScope,
        start: u64,
        end: u64,
        limit: Option<usize>,
    ) -> Vec<(u64, Bytes)> {
        let mut out = Vec::new();
        for partition in scope.partitions() {
            let binding = PartitionBinding::new(scope.tenant().clone(), partition.clone());
            let Some(index) = self.indexes.get(&binding) else {
                continue;
            };
            let entries = limit.map_or_else(
                || index.range(start, end),
                |n| index.range_limit(start, end, n),
            );
            out.extend(entries);
        }
        out.sort();
        if let Some(limit) = limit {
            out.truncate(limit);
        }
        out
    }

    pub fn len(&self) -> usize {
        self.indexes.values().map(SegmentedBTreeIndex::len).sum()
    }
    pub fn is_dirty(&self) -> bool {
        self.indexes.values().any(SegmentedBTreeIndex::is_dirty)
    }
    pub fn save_if_dirty(&mut self) -> Result<()> {
        for index in self.indexes.values_mut() {
            index.save_if_dirty()?;
        }
        Ok(())
    }
    pub fn needs_compaction(&self) -> bool {
        self.indexes
            .values()
            .any(SegmentedBTreeIndex::needs_compaction)
    }
    pub fn compact(&mut self) -> Result<bool> {
        let mut changed = false;
        for index in self.indexes.values_mut() {
            changed |= index.compact()?;
        }
        Ok(changed)
    }
}

pub struct PartitionedTagIndex {
    root: PathBuf,
    config: InvertedIndexConfig,
    default: PartitionBinding,
    indexes: BTreeMap<PartitionBinding, SegmentedInvertedIndex>,
}

impl PartitionedTagIndex {
    pub fn open(
        root: PathBuf,
        config: InvertedIndexConfig,
        default: PartitionBinding,
    ) -> Result<Self> {
        std::fs::create_dir_all(&root)?;
        let mut indexes = BTreeMap::new();
        for binding in discover_bindings(&root)? {
            indexes
                .entry(binding.clone())
                .or_insert(SegmentedInvertedIndex::load_from_dir(
                    config.clone(),
                    partition_dir(&root, &binding),
                )?);
        }
        if !indexes.contains_key(&default)
            && (root.join("tags.manifest").exists() || root.join("tags.idx").exists())
        {
            indexes.insert(
                default.clone(),
                SegmentedInvertedIndex::load_from_dir(config.clone(), root.clone())?,
            );
        }
        if indexes.is_empty() {
            let dir = partition_dir(&root, &default);
            std::fs::create_dir_all(&dir)?;
            indexes.insert(
                default.clone(),
                SegmentedInvertedIndex::load_from_dir(config.clone(), dir)?,
            );
        }
        Ok(Self {
            root,
            config,
            default,
            indexes,
        })
    }

    fn index_mut(&mut self, binding: &PartitionBinding) -> Result<&mut SegmentedInvertedIndex> {
        if !self.indexes.contains_key(binding) {
            let dir = partition_dir(&self.root, binding);
            std::fs::create_dir_all(&dir)?;
            let index = SegmentedInvertedIndex::load_from_dir(self.config.clone(), dir)?;
            self.indexes.insert(binding.clone(), index);
        }
        self.indexes.get_mut(binding).ok_or_else(|| {
            StorageError::InvalidArgument("partition tag index was not opened".into())
        })
    }

    pub fn add_tags(&mut self, key: impl Into<Bytes>, tags: &[String]) -> Result<()> {
        let key = key.into();
        let binding = binding_for_key(&key, &self.default)?;
        self.index_mut(&binding)?.add_tags(key, tags)
    }
    pub fn set_tags(&mut self, key: impl Into<Bytes>, tags: &[String]) -> Result<()> {
        let key = key.into();
        let binding = binding_for_key(&key, &self.default)?;
        self.index_mut(&binding)?.set_tags(key, tags)
    }
    pub fn remove(&mut self, key: &[u8]) -> Result<bool> {
        let binding = binding_for_key(key, &self.default)?;
        self.index_mut(&binding)?.remove(key)
    }
    pub fn search_and(&self, tags: &[&str]) -> Vec<Bytes> {
        self.indexes
            .values()
            .flat_map(|index| index.search_and(tags))
            .collect()
    }
    pub fn search_and_partitioned(&self, scope: &PartitionScope, tags: &[&str]) -> Vec<Bytes> {
        let out = scope
            .partitions()
            .iter()
            .flat_map(|partition| {
                let binding = PartitionBinding::new(scope.tenant().clone(), partition.clone());
                self.indexes
                    .get(&binding)
                    .into_iter()
                    .flat_map(|index| index.search_and(tags))
            })
            .collect::<Vec<_>>();
        #[cfg(test)]
        TAG_POSTING_VISITS.fetch_add(out.len(), std::sync::atomic::Ordering::Relaxed);
        out
    }
    pub fn search_or_scored(&self, tags: &[&str]) -> Vec<(Bytes, f32)> {
        self.indexes
            .values()
            .flat_map(|index| index.search_or_scored(tags))
            .collect()
    }
    pub fn search_or_scored_partitioned(
        &self,
        scope: &PartitionScope,
        tags: &[&str],
    ) -> Vec<(Bytes, f32)> {
        let mut out = scope
            .partitions()
            .iter()
            .flat_map(|partition| {
                let binding = PartitionBinding::new(scope.tenant().clone(), partition.clone());
                self.indexes
                    .get(&binding)
                    .into_iter()
                    .flat_map(|index| index.search_or_scored(tags))
            })
            .collect::<Vec<_>>();
        #[cfg(test)]
        TAG_POSTING_VISITS.fetch_add(out.len(), std::sync::atomic::Ordering::Relaxed);
        out.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        out
    }
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn reset_posting_visit_count() {
        TAG_POSTING_VISITS.store(0, std::sync::atomic::Ordering::Relaxed);
    }
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn posting_visit_count() -> usize {
        TAG_POSTING_VISITS.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn can_answer(&self, tags: &[&str]) -> bool {
        self.indexes
            .values()
            .any(|index| tags.iter().all(|tag| index.can_represent(tag)))
    }
    pub fn len(&self) -> usize {
        self.indexes.values().map(SegmentedInvertedIndex::len).sum()
    }
    pub fn is_dirty(&self) -> bool {
        self.indexes.values().any(SegmentedInvertedIndex::is_dirty)
    }
    pub fn save_if_dirty(&mut self) -> Result<()> {
        for index in self.indexes.values_mut() {
            index.save_if_dirty()?;
        }
        Ok(())
    }
    #[cfg(test)]
    pub fn seal_growing(&mut self) -> Result<()> {
        self.save_if_dirty()
    }
    pub fn needs_compaction(&self) -> bool {
        self.indexes
            .values()
            .any(SegmentedInvertedIndex::needs_compaction)
    }
    pub fn compact(&mut self) -> Result<bool> {
        let mut changed = false;
        for index in self.indexes.values_mut() {
            changed |= index.compact()?;
        }
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::storage::partition::{PartitionId, TenantId};
    use tempfile::tempdir;

    fn binding(partition: &str) -> PartitionBinding {
        PartitionBinding::new(
            TenantId::new("acme").unwrap(),
            PartitionId::new(partition).unwrap(),
        )
    }

    #[test]
    fn time_series_reads_only_authorized_partition_instances() {
        let dir = tempdir().unwrap();
        let default = binding("product");
        let finance = binding("finance");
        let mut indexes = PartitionedTimeSeriesIndex::open(
            dir.path().to_path_buf(),
            BTreeConfig::default(),
            default.clone(),
        )
        .unwrap();
        let product_key =
            crate::engine::storage::partition::encode_record_key(&default, b"memory:product")
                .unwrap();
        let finance_key =
            crate::engine::storage::partition::encode_record_key(&finance, b"memory:finance")
                .unwrap();
        indexes.insert(10, product_key.clone()).unwrap();
        indexes.insert(20, finance_key).unwrap();
        let scope = PartitionScope::single(default);
        assert_eq!(
            indexes.range_partitioned(&scope, 0, u64::MAX, None),
            vec![(10, product_key)]
        );
    }

    #[test]
    fn tag_reads_only_authorized_partition_instances() {
        let dir = tempdir().unwrap();
        let product = binding("product");
        let finance = binding("finance");
        let mut indexes = PartitionedTagIndex::open(
            dir.path().to_path_buf(),
            InvertedIndexConfig::default(),
            product.clone(),
        )
        .unwrap();
        let product_key =
            crate::engine::storage::partition::encode_record_key(&product, b"memory:product")
                .unwrap();
        let finance_key =
            crate::engine::storage::partition::encode_record_key(&finance, b"memory:finance")
                .unwrap();
        indexes
            .add_tags(product_key.clone(), &["rust".into()])
            .unwrap();
        indexes.add_tags(finance_key, &["rust".into()]).unwrap();
        let scope = PartitionScope::single(product);
        assert_eq!(
            indexes.search_and_partitioned(&scope, &["rust"]),
            vec![product_key]
        );
    }

    #[test]
    fn multi_partition_time_merge_is_ordered_and_limited_after_merge() {
        let dir = tempdir().unwrap();
        let product = binding("product");
        let finance = binding("finance");
        let mut indexes = PartitionedTimeSeriesIndex::open(
            dir.path().to_path_buf(),
            BTreeConfig::default(),
            product.clone(),
        )
        .unwrap();
        let product_key =
            crate::engine::storage::partition::encode_record_key(&product, b"memory:product")
                .unwrap();
        let finance_key =
            crate::engine::storage::partition::encode_record_key(&finance, b"memory:finance")
                .unwrap();
        indexes.insert(20, product_key.clone()).unwrap();
        indexes.insert(10, finance_key.clone()).unwrap();
        let scope = PartitionScope::new(
            product.tenant().clone(),
            [product.partition().clone(), finance.partition().clone()],
        )
        .unwrap();
        assert_eq!(
            indexes.range_partitioned(&scope, 0, u64::MAX, Some(1)),
            vec![(10, finance_key)]
        );
        assert_eq!(
            indexes.range_partitioned(&scope, 0, u64::MAX, None),
            vec![
                (
                    10,
                    crate::engine::storage::partition::encode_record_key(
                        &finance,
                        b"memory:finance"
                    )
                    .unwrap()
                ),
                (20, product_key)
            ]
        );
    }

    #[test]
    fn unauthorized_time_growth_does_not_change_scoped_range_visits() {
        let dir = tempdir().unwrap();
        let product = binding("product");
        let finance = binding("finance");
        let mut indexes = PartitionedTimeSeriesIndex::open(
            dir.path().to_path_buf(),
            BTreeConfig::default(),
            product.clone(),
        )
        .unwrap();
        let product_key =
            crate::engine::storage::partition::encode_record_key(&product, b"memory:product")
                .unwrap();
        indexes.insert(10, product_key).unwrap();
        let scope = PartitionScope::single(product);
        crate::engine::index::BTreeIndex::reset_range_visit_count();
        let _ = indexes.range_partitioned(&scope, 0, u64::MAX, Some(1));
        let before = crate::engine::index::BTreeIndex::range_visit_count();
        for n in 0..100 {
            let key = crate::engine::storage::partition::encode_record_key(
                &finance,
                format!("memory:finance-{n}").as_bytes(),
            )
            .unwrap();
            indexes.insert(n, key).unwrap();
        }
        crate::engine::index::BTreeIndex::reset_range_visit_count();
        let _ = indexes.range_partitioned(&scope, 0, u64::MAX, Some(1));
        assert_eq!(
            before,
            crate::engine::index::BTreeIndex::range_visit_count()
        );
    }

    #[test]
    fn unauthorized_tag_growth_does_not_change_scoped_posting_visits() {
        let dir = tempdir().unwrap();
        let product = binding("product");
        let finance = binding("finance");
        let mut indexes = PartitionedTagIndex::open(
            dir.path().to_path_buf(),
            InvertedIndexConfig::default(),
            product.clone(),
        )
        .unwrap();
        let product_key =
            crate::engine::storage::partition::encode_record_key(&product, b"memory:product")
                .unwrap();
        indexes.add_tags(product_key, &["rust".into()]).unwrap();
        let scope = PartitionScope::single(product);
        PartitionedTagIndex::reset_posting_visit_count();
        let _ = indexes.search_and_partitioned(&scope, &["rust"]);
        let before = PartitionedTagIndex::posting_visit_count();
        for n in 0..100 {
            let key = crate::engine::storage::partition::encode_record_key(
                &finance,
                format!("memory:finance-{n}").as_bytes(),
            )
            .unwrap();
            indexes.add_tags(key, &["rust".into()]).unwrap();
        }
        PartitionedTagIndex::reset_posting_visit_count();
        let _ = indexes.search_and_partitioned(&scope, &["rust"]);
        assert_eq!(before, PartitionedTagIndex::posting_visit_count());
    }
}
