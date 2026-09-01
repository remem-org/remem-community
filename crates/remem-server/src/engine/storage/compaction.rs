//! Compaction manager for LSM-tree
//!
//! Compaction merges SSTables to:
//! - Reclaim space from deleted records (tombstones)
//! - Reduce read amplification by merging overlapping tables
//! - Maintain the LSM-tree level structure
//!
//! We use leveled compaction similar to RocksDB:
//! - L0: May have overlapping key ranges (up to 4 files)
//! - L1+: Non-overlapping key ranges, 10x size ratio between levels

use bytes::Bytes;
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::sstable::{BlockCache, Compression, Record, SSTableReader, SSTableWriter};
use crate::engine::error::{Result, StorageError};

/// Configuration for compaction
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Maximum number of L0 files before triggering compaction
    pub l0_compaction_trigger: usize,
    /// Size multiplier between levels (e.g., 10 means L1 is 10x L0)
    pub level_size_multiplier: usize,
    /// Target file size for output SSTables (64 MB)
    pub target_file_size: u64,
    /// Maximum number of levels
    pub max_levels: usize,
    /// Base level size in bytes (L1 target size)
    pub base_level_size: u64,
    /// Compression for output files
    pub compression: Compression,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_compaction_trigger: 4,
            level_size_multiplier: 10,
            target_file_size: 64 * 1024 * 1024, // 64 MB
            max_levels: 7,
            base_level_size: 256 * 1024 * 1024, // 256 MB
            compression: Compression::Zstd,
        }
    }
}

/// A level in the LSM tree
#[derive(Debug)]
pub struct Level {
    /// SSTables in this level
    pub sstables: Vec<Arc<SSTableReader>>,
    /// Total size in bytes
    pub total_size: u64,
}

impl Default for Level {
    fn default() -> Self {
        Self::new()
    }
}

impl Level {
    /// Create a new empty level
    pub fn new() -> Self {
        Self {
            sstables: Vec::new(),
            total_size: 0,
        }
    }

    /// Add an SSTable to this level
    pub fn add_sstable(&mut self, sstable: Arc<SSTableReader>) {
        self.total_size += sstable.meta().file_size;
        self.sstables.push(sstable);
    }

    /// Remove an SSTable from this level
    pub fn remove_sstable(&mut self, path: &Path) {
        if let Some(idx) = self.sstables.iter().position(|s| s.path() == path) {
            let removed = self.sstables.remove(idx);
            self.total_size = self.total_size.saturating_sub(removed.meta().file_size);
        }
    }

    /// Get SSTables that overlap with the given key range
    pub fn get_overlapping(&self, min_key: &[u8], max_key: &[u8]) -> Vec<Arc<SSTableReader>> {
        self.sstables
            .iter()
            .filter(|sst| {
                let meta = sst.meta();
                // Check if ranges overlap
                meta.min_key.as_ref() <= max_key && meta.max_key.as_ref() >= min_key
            })
            .cloned()
            .collect()
    }
}

/// Compaction manager
pub struct CompactionManager {
    /// Base directory for SSTable files
    base_path: PathBuf,
    /// Levels in the LSM tree
    levels: RwLock<Vec<Level>>,
    /// Configuration
    config: CompactionConfig,
    /// Block cache for reading
    cache: Arc<BlockCache>,
    /// Counter for generating unique file names
    file_counter: std::sync::atomic::AtomicU64,
}

impl CompactionManager {
    /// Create a new compaction manager
    pub fn new(
        base_path: impl AsRef<Path>,
        config: CompactionConfig,
        cache: Arc<BlockCache>,
    ) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();

        // Create level directories
        for level in 0..config.max_levels {
            let level_path = base_path.join(format!("level{}", level));
            std::fs::create_dir_all(&level_path)?;
        }

        let mut levels = Vec::new();
        for _ in 0..config.max_levels {
            levels.push(Level::new());
        }

        Ok(Self {
            base_path,
            levels: RwLock::new(levels),
            config,
            cache,
            file_counter: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Load existing SSTables from disk
    pub fn load_existing(&self) -> Result<()> {
        let mut levels = self.levels.write();

        for level in 0..self.config.max_levels {
            let level_path = self.base_path.join(format!("level{}", level));
            if !level_path.exists() {
                continue;
            }

            for entry in std::fs::read_dir(&level_path)? {
                let entry = entry?;
                let path = entry.path();

                if path.extension().map(|e| e == "sst").unwrap_or(false) {
                    let reader =
                        SSTableReader::open_with_cache(&path, Some(Arc::clone(&self.cache)), level)
                            .map_err(|e| {
                                tracing::error!("Failed to load SSTable {:?}: {}", path, e);
                                e
                            })?;
                    levels[level].add_sstable(Arc::new(reader));
                }
            }
        }

        Ok(())
    }

    /// The highest record version held in any SSTable.
    ///
    /// Read once, when opening a directory that carries no persisted
    /// high-water mark — an upgrade, or a mark that went missing. It walks
    /// record versions rather than payload content, and the result is
    /// persisted so no later open repeats it.
    pub fn max_record_version(&self) -> Result<u64> {
        let levels = self.levels.read();
        let mut highest = 0u64;
        for level in levels.iter() {
            for sst in &level.sstables {
                for record in sst.iter() {
                    highest = highest.max(record?.timestamp);
                }
            }
        }
        Ok(highest)
    }

    /// Add a new SSTable to level 0
    pub fn add_l0_sstable(&self, sstable: Arc<SSTableReader>) {
        let mut levels = self.levels.write();
        levels[0].add_sstable(sstable);
    }

    /// Check if compaction is needed and return the level to compact
    pub fn needs_compaction(&self) -> Option<usize> {
        let levels = self.levels.read();

        // Check L0 first
        if levels[0].sstables.len() >= self.config.l0_compaction_trigger {
            return Some(0);
        }

        // Check other levels
        for level in 1..self.config.max_levels - 1 {
            let target_size = self.target_size_for_level(level);
            if levels[level].total_size > target_size {
                return Some(level);
            }
        }

        None
    }

    /// Get the target size for a level
    fn target_size_for_level(&self, level: usize) -> u64 {
        if level == 0 {
            return u64::MAX; // L0 is controlled by file count
        }

        let multiplier = self.config.level_size_multiplier.pow(level as u32 - 1);
        self.config.base_level_size * multiplier as u64
    }

    /// Generate a new SSTable file path
    fn new_sstable_path(&self, level: usize) -> PathBuf {
        let counter = self
            .file_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        self.base_path
            .join(format!("level{}", level))
            .join(format!("{}_{}.sst", timestamp, counter))
    }

    /// Compact a level
    pub fn compact_level(&self, level: usize) -> Result<CompactionResult> {
        if level >= self.config.max_levels - 1 {
            return Err(StorageError::Compaction(format!(
                "Cannot compact level {} (max is {})",
                level,
                self.config.max_levels - 1
            )));
        }

        let (inputs, output_level) = {
            let levels = self.levels.read();

            if level == 0 {
                // L0: Compact all L0 files into L1
                let l0_files = levels[0].sstables.clone();
                if l0_files.is_empty() {
                    return Ok(CompactionResult::default());
                }

                // Find overlapping L1 files
                let (min_key, max_key) = self.find_key_range(&l0_files);
                let l1_overlapping = levels[1].get_overlapping(&min_key, &max_key);

                let mut inputs = l0_files;
                inputs.extend(l1_overlapping);
                (inputs, 1)
            } else {
                // Ln: Pick one file and compact with overlapping Ln+1 files
                if levels[level].sstables.is_empty() {
                    return Ok(CompactionResult::default());
                }

                let file_to_compact = levels[level].sstables[0].clone();
                let meta = file_to_compact.meta();

                let next_level_overlapping =
                    levels[level + 1].get_overlapping(&meta.min_key, &meta.max_key);

                let mut inputs = vec![file_to_compact];
                inputs.extend(next_level_overlapping);
                (inputs, level + 1)
            }
        };

        if inputs.is_empty() {
            return Ok(CompactionResult::default());
        }

        // Perform the merge
        let new_sstables = self.merge_sstables(&inputs, output_level)?;

        // Update levels atomically
        {
            let mut levels = self.levels.write();

            // Remove input files
            for input in &inputs {
                let input_level = input.level();
                levels[input_level].remove_sstable(input.path());
            }

            // Add output files
            for sstable in &new_sstables {
                levels[output_level].add_sstable(Arc::clone(sstable));
            }
        }

        // Delete old files
        for input in &inputs {
            let _ = std::fs::remove_file(input.path());
        }

        Ok(CompactionResult {
            input_count: inputs.len(),
            output_count: new_sstables.len(),
            input_level: level,
            output_level,
        })
    }

    /// Find the key range covered by a set of SSTables
    fn find_key_range(&self, sstables: &[Arc<SSTableReader>]) -> (Bytes, Bytes) {
        let mut min_key = Bytes::from(vec![0xFFu8; 16]);
        let mut max_key = Bytes::new();

        for sst in sstables {
            let meta = sst.meta();
            if meta.min_key < min_key {
                min_key = meta.min_key.clone();
            }
            if meta.max_key > max_key {
                max_key = meta.max_key.clone();
            }
        }

        (min_key, max_key)
    }

    /// Merge multiple SSTables into new SSTables
    fn merge_sstables(
        &self,
        inputs: &[Arc<SSTableReader>],
        output_level: usize,
    ) -> Result<Vec<Arc<SSTableReader>>> {
        // Create merge iterator
        let mut merge_iter = MergeIterator::new(inputs)?;

        let mut outputs = Vec::new();
        let mut current_writer: Option<SSTableWriter> = None;
        let mut last_key: Option<Bytes> = None;

        while let Some(record) = merge_iter.next()? {
            // Skip duplicate keys (keep the one with higher timestamp)
            if let Some(ref last) = last_key {
                if last == &record.key {
                    continue;
                }
            }
            last_key = Some(record.key.clone());

            // Start new SSTable if needed
            if current_writer.is_none() {
                let path = self.new_sstable_path(output_level);
                current_writer = Some(SSTableWriter::with_level(
                    path,
                    self.config.compression,
                    output_level,
                )?);
            }

            // Add record
            let writer = current_writer.as_mut().unwrap();
            writer.add(record.key, record.value, record.timestamp)?;

            // Check if we should start a new file
            if writer.estimated_size() >= self.config.target_file_size {
                let writer = current_writer.take().unwrap();
                let meta = writer.finish()?;
                let reader = SSTableReader::open_with_cache(
                    &meta.path,
                    Some(Arc::clone(&self.cache)),
                    output_level,
                )?;
                outputs.push(Arc::new(reader));
            }
        }

        // Finish the last SSTable
        if let Some(writer) = current_writer {
            if writer.record_count() > 0 {
                let meta = writer.finish()?;
                let reader = SSTableReader::open_with_cache(
                    &meta.path,
                    Some(Arc::clone(&self.cache)),
                    output_level,
                )?;
                outputs.push(Arc::new(reader));
            }
        }

        Ok(outputs)
    }

    /// Get a record from all levels
    pub fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        let levels = self.levels.read();

        // Search L0 (all files, newest first)
        for sst in levels[0].sstables.iter().rev() {
            if let Some(record) = sst.get(key)? {
                return Ok(Some(record));
            }
        }

        // Search L1+ (at most one file per level)
        for level in &levels[1..] {
            for sst in &level.sstables {
                if sst.meta().may_contain(key) {
                    if let Some(record) = sst.get(key)? {
                        return Ok(Some(record));
                    }
                }
            }
        }

        Ok(None)
    }
}

/// Result of a compaction operation
#[derive(Debug, Default)]
pub struct CompactionResult {
    /// Number of input files
    pub input_count: usize,
    /// Number of output files
    pub output_count: usize,
    /// Input level
    pub input_level: usize,
    /// Output level
    pub output_level: usize,
}

/// Merge iterator for compaction.
///
/// Pre-collects all records from every input SSTable into a min-heap ordered
/// by (key asc, timestamp desc).  The old design stored only the first record
/// from each SSTable — every compaction silently discarded all but one entry
/// per file.  Pre-collection avoids the self-referential lifetime problem of
/// storing `SSTableIterator<'_>` alongside its owning `Arc<SSTableReader>`.
struct MergeIterator {
    heap: BinaryHeap<IteratorEntry>,
}

struct IteratorEntry {
    record: Record,
}

impl PartialEq for IteratorEntry {
    fn eq(&self, other: &Self) -> bool {
        self.record.key == other.record.key && self.record.timestamp == other.record.timestamp
    }
}

impl Eq for IteratorEntry {}

impl PartialOrd for IteratorEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IteratorEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse key ordering → min-heap pops smallest key first.
        // For the same key, higher timestamp wins (keep newest version).
        match other.record.key.cmp(&self.record.key) {
            Ordering::Equal => self.record.timestamp.cmp(&other.record.timestamp),
            other => other,
        }
    }
}

impl MergeIterator {
    fn new(sstables: &[Arc<SSTableReader>]) -> Result<Self> {
        let mut heap = BinaryHeap::new();

        // Eagerly drain every SSTable iterator.  This avoids the self-referential
        // lifetime problem (SSTableIterator<'_> borrows from SSTableReader) while
        // keeping the correct heap-ordered merge semantics.
        for sst in sstables {
            for result in sst.iter() {
                let record = result?;
                heap.push(IteratorEntry { record });
            }
        }

        Ok(Self { heap })
    }

    fn next(&mut self) -> Result<Option<Record>> {
        Ok(self.heap.pop().map(|e| e.record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::storage::sstable::format::HEADER_SIZE;
    use crate::engine::storage::sstable::SSTableWriter;
    use tempfile::tempdir;

    fn create_sstable(
        dir: &Path,
        level: usize,
        records: Vec<(&str, &str, u64)>,
    ) -> Arc<SSTableReader> {
        let path = dir.join(format!("level{}", level)).join(format!(
            "test_{}.sst",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        let mut writer = SSTableWriter::with_level(&path, Compression::None, level).unwrap();
        for (key, value, ts) in records {
            writer
                .add(
                    Bytes::from(key.to_string()),
                    Some(Bytes::from(value.to_string())),
                    ts,
                )
                .unwrap();
        }
        writer.finish().unwrap();

        Arc::new(SSTableReader::open_with_cache(&path, None, level).unwrap())
    }

    #[test]
    fn test_compaction_manager_new() {
        let dir = tempdir().unwrap();
        let cache = Arc::new(BlockCache::new(1024 * 1024));

        let manager =
            CompactionManager::new(dir.path(), CompactionConfig::default(), cache).unwrap();

        assert!(manager.needs_compaction().is_none());
    }

    #[test]
    fn test_add_l0_sstable() {
        let dir = tempdir().unwrap();
        let cache = Arc::new(BlockCache::new(1024 * 1024));

        let manager =
            CompactionManager::new(dir.path(), CompactionConfig::default(), Arc::clone(&cache))
                .unwrap();

        let sst = create_sstable(dir.path(), 0, vec![("key1", "value1", 1)]);

        manager.add_l0_sstable(sst);

        let record = manager
            .get(b"key1")
            .unwrap()
            .expect("key1 should be findable after add_l0_sstable");
        assert_eq!(record.value, Some(Bytes::from("value1")));
    }

    #[test]
    fn load_existing_refuses_corrupt_sstable() {
        let dir = tempdir().unwrap();
        let cache = Arc::new(BlockCache::new(1024 * 1024));
        let sst = create_sstable(dir.path(), 0, vec![("key1", "value1", 1)]);
        let path = sst.path().to_path_buf();
        drop(sst);

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[HEADER_SIZE + 1] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();

        let manager =
            CompactionManager::new(dir.path(), CompactionConfig::default(), Arc::clone(&cache))
                .unwrap();

        match manager.load_existing() {
            Err(StorageError::ChecksumMismatch { file, .. }) => assert_eq!(file, path),
            other => panic!("expected checksum mismatch, got {other:?}"),
        }
    }

    /// The header stores only a 16-byte *prefix* of a file's min/max key, so a
    /// lookup that compares a full key against those bounds as if they were
    /// exact excludes every key that shares the prefix but runs longer.
    ///
    /// Harmless while keys differed early -- `memory:<uuid>` diverges by byte 8
    /// -- and catastrophic once every key carries the same partition prefix:
    /// `partition:default:default:memory:<uuid>` is identical for the first 16
    /// bytes, so every payload lookup in L1+ was refused and every stored
    /// memory read back as missing.
    #[test]
    fn get_finds_l1_keys_that_share_the_truncated_bound_prefix() {
        let dir = tempdir().unwrap();
        let cache = Arc::new(BlockCache::new(1024 * 1024));

        // All three share "partition:defaul" -- exactly 16 bytes.
        let sst = create_sstable(
            dir.path(),
            1,
            vec![
                ("partition:default:default:memory:aaa", "va", 1),
                ("partition:default:default:memory:bbb", "vb", 2),
                ("partition:default:default:memory:ccc", "vc", 3),
            ],
        );
        drop(sst);

        let manager =
            CompactionManager::new(dir.path(), CompactionConfig::default(), Arc::clone(&cache))
                .unwrap();
        manager.load_existing().unwrap();

        let record = manager
            .get(b"partition:default:default:memory:bbb")
            .unwrap()
            .expect("an L1 record must be findable when every key shares the stored bound prefix");
        assert_eq!(record.value, Some(Bytes::from("vb")));
    }

    /// A key genuinely outside the file's range must still be answered without
    /// reading it, so the prefix comparison must not degrade into "always look".
    #[test]
    fn get_still_skips_l1_files_whose_prefix_range_excludes_the_key() {
        let dir = tempdir().unwrap();
        let cache = Arc::new(BlockCache::new(1024 * 1024));

        let sst = create_sstable(dir.path(), 1, vec![("bbbb", "vb", 1), ("cccc", "vc", 2)]);
        drop(sst);

        let manager =
            CompactionManager::new(dir.path(), CompactionConfig::default(), Arc::clone(&cache))
                .unwrap();
        manager.load_existing().unwrap();

        assert!(manager.get(b"zzzz").unwrap().is_none());
        assert!(manager.get(b"aaaa").unwrap().is_none());
        assert_eq!(
            manager.get(b"cccc").unwrap().unwrap().value,
            Some(Bytes::from("vc"))
        );
    }

    /// Merging resolves two copies of a key by keeping the higher version, and
    /// that rule is left exactly as it was: the fix for reverted writes was to
    /// make the numbers order writes, not to teach this site a special case.
    ///
    /// Worth pinning, because the rule is only correct while versions are
    /// issued from one counter. They used to restart at 1 on every memtable
    /// rotation, so a value written later carried a smaller number and this
    /// merge discarded it -- `expire_short_term` re-archived the same ~19,189
    /// memories on every run from May onward for exactly that reason. The
    /// counter is now shared across rotations and seeded above the directory's
    /// existing versions, so "higher" means "later" again.
    #[test]
    fn compaction_keeps_the_higher_version_of_a_duplicate_key() {
        let dir = tempdir().unwrap();
        let cache = Arc::new(BlockCache::new(1024 * 1024));

        let manager =
            CompactionManager::new(dir.path(), CompactionConfig::default(), Arc::clone(&cache))
                .unwrap();

        // Deliberately added in the order that makes insertion order and
        // version order disagree: the file added second holds the lower
        // version, so a merge that trusted arrival order would pick it.
        manager.add_l0_sstable(create_sstable(dir.path(), 0, vec![("k", "higher", 5000)]));
        manager.add_l0_sstable(create_sstable(dir.path(), 0, vec![("k", "lower", 3)]));

        manager.compact_level(0).unwrap();

        assert_eq!(
            manager.get(b"k").unwrap().unwrap().value,
            Some(Bytes::from("higher")),
            "merge must resolve a duplicate key by version, not by arrival order"
        );
    }

    #[test]
    fn test_needs_compaction() {
        let dir = tempdir().unwrap();
        let cache = Arc::new(BlockCache::new(1024 * 1024));

        let config = CompactionConfig {
            l0_compaction_trigger: 2,
            ..Default::default()
        };

        let manager = CompactionManager::new(dir.path(), config, Arc::clone(&cache)).unwrap();

        // Add files until compaction is needed
        manager.add_l0_sstable(create_sstable(dir.path(), 0, vec![("a", "1", 1)]));
        assert!(manager.needs_compaction().is_none());

        manager.add_l0_sstable(create_sstable(dir.path(), 0, vec![("b", "2", 2)]));
        assert_eq!(manager.needs_compaction(), Some(0));
    }

    #[test]
    fn test_get_from_levels() {
        let dir = tempdir().unwrap();
        let cache = Arc::new(BlockCache::new(1024 * 1024));

        let manager =
            CompactionManager::new(dir.path(), CompactionConfig::default(), Arc::clone(&cache))
                .unwrap();

        manager.add_l0_sstable(create_sstable(
            dir.path(),
            0,
            vec![("key1", "value1", 1), ("key2", "value2", 1)],
        ));

        let record = manager.get(b"key1").unwrap().unwrap();
        assert_eq!(record.value.unwrap(), Bytes::from("value1"));

        assert!(manager.get(b"nonexistent").unwrap().is_none());
    }
}
