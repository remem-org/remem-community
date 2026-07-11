//! Segmented BTree index — time-windowed sealed chunks + a growing in-memory segment.
//!
//! Each sealed chunk covers a contiguous time window bounded by entry count.
//! Range queries use manifest's `first_id`/`last_id` (timestamps) to skip irrelevant chunks.
#![allow(dead_code)]

use bytes::Bytes;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::engine::error::{Result, StorageError};
use crate::engine::index::manifest::{ChunkMeta, SegmentManifest};
use crate::engine::index::segment_io::{SegmentHeader, SegmentReader, SegmentWriter, INDEX_TYPE_BTREE};
use crate::engine::index::{BTreeConfig, BTreeIndex, BTREE_CHUNK_SIZE};

// ── Sealed chunk ───────────────────────────────────────────────────────────────

struct SealedBTreeChunk {
    seq_no: u32,
    /// Min timestamp in this chunk (from manifest).
    first_ts: u64,
    /// Max timestamp in this chunk (from manifest).
    last_ts: u64,
    /// Loaded BTree index for this chunk.
    index: BTreeIndex,
}

impl SealedBTreeChunk {
    fn overlaps(&self, start: u64, end: u64) -> bool {
        self.first_ts <= end && self.last_ts >= start
    }

    fn entry_count(&self) -> usize {
        self.index.len()
    }
}

// ── SegmentedBTreeIndex ────────────────────────────────────────────────────────

/// Segmented BTree index: sealed time-window chunks + a growing mutable segment.
pub struct SegmentedBTreeIndex {
    config: BTreeConfig,
    /// Sealed chunks ordered by timestamp range.
    sealed: Vec<SealedBTreeChunk>,
    /// Currently growing segment.
    growing: BTreeIndex,
    /// Index directory.
    dir: PathBuf,
    /// Manifest tracking all sealed chunks.
    manifest: SegmentManifest,
    /// Whether there are unsaved changes.
    dirty: AtomicBool,
}

impl SegmentedBTreeIndex {
    const INDEX_NAME: &'static str = "timeseries";

    /// Create a new empty segmented BTree index writing to `dir`.
    pub fn new(config: BTreeConfig, dir: PathBuf) -> Self {
        let growing = BTreeIndex::new(config.clone());
        Self {
            config,
            sealed: Vec::new(),
            growing,
            manifest: SegmentManifest::new(Self::INDEX_NAME),
            dir,
            dirty: AtomicBool::new(false),
        }
    }

    /// Load from manifest + segment files in `dir`. Returns fresh index on any error.
    pub fn load_from_dir(config: BTreeConfig, dir: PathBuf) -> Self {
        match Self::try_load_from_dir(config.clone(), &dir) {
            Ok(idx) => idx,
            Err(e) => {
                tracing::warn!(
                    "Failed to load segmented BTree index from {:?}: {}; starting fresh",
                    dir,
                    e
                );
                Self::new(config, dir)
            }
        }
    }

    fn try_load_from_dir(config: BTreeConfig, dir: &Path) -> Result<Self> {
        let manifest = match SegmentManifest::load(dir, Self::INDEX_NAME)? {
            Some(m) => m,
            None => {
                return Ok(Self::new(config, dir.to_path_buf()));
            }
        };

        let mut sealed = Vec::with_capacity(manifest.chunks.len());

        for chunk_meta in &manifest.chunks {
            let seg_path = dir.join(&chunk_meta.filename);
            match Self::load_sealed_chunk(&seg_path, chunk_meta) {
                Ok(chunk) => {
                    sealed.push(chunk);
                }
                Err(e) => {
                    tracing::warn!(
                        "Corrupt BTree segment {:?}: {}; skipping (WAL will rebuild)",
                        seg_path,
                        e
                    );
                }
            }
        }

        let total: usize = sealed.iter().map(|c| c.entry_count()).sum();
        tracing::info!(
            "Loaded segmented BTree index: {} sealed chunks, {} total entries",
            sealed.len(),
            total
        );

        Ok(Self {
            config: config.clone(),
            sealed,
            growing: BTreeIndex::new(config),
            manifest,
            dir: dir.to_path_buf(),
            dirty: AtomicBool::new(false),
        })
    }

    fn load_sealed_chunk(path: &Path, meta: &ChunkMeta) -> Result<SealedBTreeChunk> {
        let reader = SegmentReader::open(path)?;
        let mut cursor = reader.data_cursor();
        let index = deserialize_btree_index(&mut cursor)?;

        Ok(SealedBTreeChunk {
            seq_no: meta.seq_no,
            first_ts: meta.first_id,
            last_ts: meta.last_id,
            index,
        })
    }

    /// Seal the growing segment to disk if it has entries.
    pub fn seal_growing(&mut self) -> Result<()> {
        if self.growing.is_empty() {
            return Ok(());
        }

        std::fs::create_dir_all(&self.dir).map_err(StorageError::Io)?;

        let seq_no = self.manifest.next_seq_no();
        let filename = format!("{}_{:04}.seg", Self::INDEX_NAME, seq_no);
        let seg_path = self.dir.join(&filename);

        let first_ts = self.growing.min_timestamp().unwrap_or(0);
        let last_ts = self.growing.max_timestamp().unwrap_or(0);
        let entry_count = self.growing.len() as u32;

        // Serialize the growing index
        let mut data_buf = Vec::new();
        serialize_btree_index(&self.growing, &mut data_buf)?;

        let header = SegmentHeader::new(
            *b"BTIX_SEG",
            INDEX_TYPE_BTREE,
            seq_no,
            entry_count,
            first_ts,
            last_ts,
        );

        let mut writer = SegmentWriter::create(&seg_path, header)?;
        writer.write_bytes(&data_buf)?;
        let crc32 = writer.finish()?;

        let file_size = seg_path
            .metadata()
            .map(|m| m.len())
            .unwrap_or(data_buf.len() as u64);

        self.manifest.chunks.push(ChunkMeta {
            seq_no,
            filename,
            entry_count,
            file_size,
            first_id: first_ts,
            last_id: last_ts,
            crc32,
            sealed: true,
            has_deletions: false,
        });
        self.manifest.commit(&self.dir)?;

        let new_config = self.config.clone();
        let old_growing = std::mem::replace(&mut self.growing, BTreeIndex::new(new_config));

        self.sealed.push(SealedBTreeChunk {
            seq_no,
            first_ts,
            last_ts,
            index: old_growing,
        });

        tracing::info!(
            "Sealed BTree segment {} ({} entries, ts {}-{})",
            seg_path.display(),
            entry_count,
            first_ts,
            last_ts
        );

        Ok(())
    }

    /// Checkpoint: seal the growing segment to disk if it has any entries.
    ///
    /// The chunk-size threshold (`BTREE_CHUNK_SIZE`) governs mid-run segment
    /// rotation, not whether to persist at all.  On every checkpoint or graceful
    /// shutdown we must flush whatever is in the growing segment — even if it
    /// hasn't reached the rotation threshold — otherwise entries that have never
    /// been sealed are silently lost on restart.
    pub fn save_if_dirty(&mut self) -> Result<()> {
        if !self.growing.is_empty() {
            self.seal_growing()?;
        }
        self.dirty.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Whether there are unsaved changes.
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed) || self.growing.is_dirty()
    }

    pub fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Relaxed);
        self.growing.mark_clean();
    }

    // ── Write operations ───────────────────────────────────────────────────────

    /// Insert a timestamp-key pair.
    pub fn insert(&self, timestamp: u64, key: impl Into<Bytes>) -> Result<()> {
        let result = self.growing.insert(timestamp, key);
        if result.is_ok() {
            self.dirty.store(true, Ordering::Relaxed);
        }
        result
    }

    /// Remove a key from the index.
    pub fn remove(&self, key: &[u8]) -> Result<bool> {
        let in_growing = self.growing.remove(key)?;
        // Also try sealed chunks (they have interior-mutability for remove)
        let mut in_sealed = false;
        for chunk in &self.sealed {
            if chunk.index.remove(key)? {
                in_sealed = true;
                break;
            }
        }
        if in_growing || in_sealed {
            self.dirty.store(true, Ordering::Relaxed);
        }
        Ok(in_growing || in_sealed)
    }

    // ── Read operations ────────────────────────────────────────────────────────

    /// Get the timestamp for a key.
    pub fn get_timestamp(&self, key: &[u8]) -> Option<u64> {
        if let Some(ts) = self.growing.get_timestamp(key) {
            return Some(ts);
        }
        for chunk in &self.sealed {
            if let Some(ts) = chunk.index.get_timestamp(key) {
                return Some(ts);
            }
        }
        None
    }

    /// Query a range of timestamps (inclusive).
    pub fn range(&self, start: u64, end: u64) -> Vec<(u64, Bytes)> {
        let mut results = Vec::new();
        for chunk in &self.sealed {
            if chunk.overlaps(start, end) {
                results.extend(chunk.index.range(start, end));
            }
        }
        results.extend(self.growing.range(start, end));
        results.sort_by_key(|(ts, _)| *ts);
        results
    }

    /// Query a range with limit.
    pub fn range_limit(&self, start: u64, end: u64, limit: usize) -> Vec<(u64, Bytes)> {
        let all = self.range(start, end);
        all.into_iter().take(limit).collect()
    }

    /// Records before a timestamp (exclusive), newest first.
    pub fn before(&self, timestamp: u64, limit: usize) -> Vec<(u64, Bytes)> {
        let mut results = Vec::new();
        for chunk in &self.sealed {
            if chunk.first_ts < timestamp {
                results.extend(chunk.index.before(timestamp, limit));
            }
        }
        results.extend(self.growing.before(timestamp, limit));
        // Sort descending (newest first) and deduplicate
        results.sort_by(|a, b| b.0.cmp(&a.0));
        results.dedup_by_key(|(ts, k)| (*ts, k.clone()));
        results.truncate(limit);
        results
    }

    /// Records after a timestamp (exclusive), oldest first.
    pub fn after(&self, timestamp: u64, limit: usize) -> Vec<(u64, Bytes)> {
        let mut results = Vec::new();
        for chunk in &self.sealed {
            if chunk.last_ts > timestamp {
                results.extend(chunk.index.after(timestamp, limit));
            }
        }
        results.extend(self.growing.after(timestamp, limit));
        results.sort_by_key(|(ts, _)| *ts);
        results.dedup_by_key(|(ts, k)| (*ts, k.clone()));
        results.truncate(limit);
        results
    }

    /// Most recent entries.
    pub fn latest(&self, limit: usize) -> Vec<(u64, Bytes)> {
        let all_ts_max = self
            .sealed
            .iter()
            .map(|c| c.last_ts)
            .max()
            .unwrap_or(0)
            .max(self.growing.max_timestamp().unwrap_or(0));

        // Use before with a timestamp just past the max
        let mut results = self.before(all_ts_max + 1, limit * 2);
        // before() returns newest first
        results.truncate(limit);
        results
    }

    /// Oldest entries.
    pub fn oldest(&self, limit: usize) -> Vec<(u64, Bytes)> {
        let mut results = self.after(0, limit * 2);
        results.truncate(limit);
        results
    }

    /// Total number of entries.
    pub fn len(&self) -> usize {
        let sealed_count: usize = self.sealed.iter().map(|c| c.entry_count()).sum();
        sealed_count + self.growing.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        if self.growing.contains_key(key) {
            return true;
        }
        self.sealed.iter().any(|c| c.index.contains_key(key))
    }

    /// Min timestamp across all chunks.
    pub fn min_timestamp(&self) -> Option<u64> {
        let sealed_min = self.sealed.iter().map(|c| c.first_ts).min();
        let growing_min = self.growing.min_timestamp();
        match (sealed_min, growing_min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    /// Max timestamp across all chunks.
    pub fn max_timestamp(&self) -> Option<u64> {
        let sealed_max = self.sealed.iter().map(|c| c.last_ts).max();
        let growing_max = self.growing.max_timestamp();
        match (sealed_max, growing_max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    /// Number of sealed chunks.
    pub fn sealed_chunk_count(&self) -> usize {
        self.sealed.len()
    }

    /// Whether size-tiered compaction should be triggered.
    pub fn needs_compaction(&self) -> bool {
        // Trigger if more than 10 sealed chunks with small sizes
        self.sealed.len() > 10
    }

    /// Compact: merge the two smallest sealed chunks into one.
    ///
    /// Returns `true` if compaction was performed.
    pub fn compact(&mut self) -> Result<bool> {
        if self.sealed.len() < 2 {
            return Ok(false);
        }

        // Pick the two smallest chunks by entry count
        let mut by_size: Vec<usize> = (0..self.sealed.len()).collect();
        by_size.sort_by_key(|&i| self.sealed[i].entry_count());

        let a_idx = by_size[0];
        let b_idx = by_size[1];
        let (a_idx, b_idx) = if a_idx < b_idx { (a_idx, b_idx) } else { (b_idx, a_idx) };

        // Merge: collect all entries from both chunks
        let merged = BTreeIndex::new(self.config.clone());
        for &si in &[a_idx, b_idx] {
            let chunk = &self.sealed[si];
            for (ts, key) in chunk.index.range(0, u64::MAX) {
                let _ = merged.insert(ts, key);
            }
        }

        // Write merged chunk to disk
        std::fs::create_dir_all(&self.dir)?;
        let seq_no = self.manifest.next_seq_no();
        let filename = format!("timeseries_{:04}.seg", seq_no);
        let seg_path = self.dir.join(&filename);

        let mut data_buf = Vec::new();
        serialize_btree_index(&merged, &mut data_buf)?;

        let entry_count = merged.len() as u32;
        let first_ts = merged.min_timestamp().unwrap_or(0);
        let last_ts = merged.max_timestamp().unwrap_or(0);

        let header = SegmentHeader::new(
            *b"BTIX_SEG",
            INDEX_TYPE_BTREE,
            seq_no,
            entry_count,
            first_ts,
            last_ts,
        );
        let mut writer = SegmentWriter::create(&seg_path, header)?;
        writer.write_bytes(&data_buf)?;
        let crc32 = writer.finish()?;

        let file_size = seg_path.metadata().map(|m| m.len()).unwrap_or(0);

        // Remove old files
        let old_filenames: Vec<String> = vec![
            format!("timeseries_{:04}.seg", self.sealed[a_idx].seq_no),
            format!("timeseries_{:04}.seg", self.sealed[b_idx].seq_no),
        ];
        for fname in &old_filenames {
            let _ = std::fs::remove_file(self.dir.join(fname));
        }
        self.manifest.chunks.retain(|c| !old_filenames.contains(&c.filename));

        // Add new merged chunk to manifest
        self.manifest.chunks.push(ChunkMeta {
            seq_no,
            filename: filename.clone(),
            entry_count,
            file_size,
            first_id: first_ts,
            last_id: last_ts,
            crc32,
            sealed: true,
            has_deletions: false,
        });
        self.manifest.commit(&self.dir)?;

        // Update in-memory sealed list
        self.sealed.remove(b_idx);
        self.sealed.remove(a_idx);

        self.sealed.push(SealedBTreeChunk {
            seq_no,
            first_ts,
            last_ts,
            index: merged,
        });

        tracing::info!(
            "BTree index compaction: merged 2 chunks into seq_no={} ({} entries)",
            seq_no,
            entry_count
        );
        Ok(true)
    }
}

// ── Serialization helpers ──────────────────────────────────────────────────────

/// Serialize a `BTreeIndex` to a byte buffer using the existing wire format.
fn serialize_btree_index(index: &BTreeIndex, buf: &mut Vec<u8>) -> Result<()> {
    use std::io::Write as IoWrite;

    let mut w = std::io::BufWriter::new(buf);

    // Use the BTree's existing format: BTIX header
    w.write_all(b"BTIX").map_err(StorageError::Io)?;
    w.write_all(&1u32.to_le_bytes()).map_err(StorageError::Io)?;

    // Config
    let max_entries = 10_000_000u64;
    w.write_all(&max_entries.to_le_bytes())
        .map_err(StorageError::Io)?;

    // Snapshot the tree data
    let entry_count = index.len() as u64;
    w.write_all(&entry_count.to_le_bytes())
        .map_err(StorageError::Io)?;

    // Collect (timestamp, keys[]) pairs
    let min_ts = index.min_timestamp().unwrap_or(0);
    let max_ts = index.max_timestamp().unwrap_or(0);
    let snapshot = index.range(min_ts, max_ts);

    // Group by timestamp
    let mut by_ts: std::collections::BTreeMap<u64, Vec<Bytes>> = std::collections::BTreeMap::new();
    for (ts, key) in snapshot {
        by_ts.entry(ts).or_default().push(key);
    }

    w.write_all(&(by_ts.len() as u64).to_le_bytes())
        .map_err(StorageError::Io)?;
    for (timestamp, keys) in &by_ts {
        w.write_all(&timestamp.to_le_bytes()).map_err(StorageError::Io)?;
        w.write_all(&(keys.len() as u32).to_le_bytes())
            .map_err(StorageError::Io)?;
        for key in keys {
            w.write_all(&(key.len() as u32).to_le_bytes())
                .map_err(StorageError::Io)?;
            w.write_all(key).map_err(StorageError::Io)?;
        }
    }

    w.flush().map_err(StorageError::Io)?;
    Ok(())
}

fn deserialize_btree_index(cursor: &mut impl Read) -> Result<BTreeIndex> {
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic)?;
    if &magic != b"BTIX" {
        return Err(StorageError::Serialization(
            "Invalid BTree magic in segment".into(),
        ));
    }

    let mut buf4 = [0u8; 4];
    let mut buf8 = [0u8; 8];

    cursor.read_exact(&mut buf4)?;
    let version = u32::from_le_bytes(buf4);
    if version != 1 {
        return Err(StorageError::Serialization(format!(
            "Unsupported BTree version: {version}"
        )));
    }

    cursor.read_exact(&mut buf8)?;
    let max_entries = u64::from_le_bytes(buf8) as usize;

    cursor.read_exact(&mut buf8)?;
    let entry_count = u64::from_le_bytes(buf8) as usize;

    cursor.read_exact(&mut buf8)?;
    let timestamp_count = u64::from_le_bytes(buf8) as usize;

    let index = BTreeIndex::new(BTreeConfig { max_entries });
    let mut _total = 0usize;

    for _ in 0..timestamp_count {
        cursor.read_exact(&mut buf8)?;
        let timestamp = u64::from_le_bytes(buf8);

        cursor.read_exact(&mut buf4)?;
        let key_count = u32::from_le_bytes(buf4) as usize;

        for _ in 0..key_count {
            cursor.read_exact(&mut buf4)?;
            let key_len = u32::from_le_bytes(buf4) as usize;
            let mut key_bytes = vec![0u8; key_len];
            cursor.read_exact(&mut key_bytes)?;
            index.insert(timestamp, key_bytes)?;
            _total += 1;
        }
    }

    let _ = entry_count; // suppress unused warning
    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_basic_insert_and_range() {
        let dir = tempdir().unwrap();
        let idx = SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());

        for i in 0u64..10 {
            idx.insert(i * 100, format!("key{}", i)).unwrap();
        }

        let results = idx.range(200, 500);
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_seal_and_reload() {
        let dir = tempdir().unwrap();
        let mut idx = SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());

        for i in 0u64..5 {
            idx.insert(i * 100, format!("key{i}")).unwrap();
        }
        idx.seal_growing().unwrap();
        assert_eq!(idx.sealed.len(), 1);

        idx.insert(1000, "late_key".to_string()).unwrap();

        let reloaded =
            SegmentedBTreeIndex::load_from_dir(BTreeConfig::default(), dir.path().to_path_buf());

        assert_eq!(reloaded.sealed.len(), 1);
        let results = reloaded.range(0, 400);
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_latest() {
        let dir = tempdir().unwrap();
        let idx = SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());

        for i in 0u64..100 {
            idx.insert(i, format!("key{i}")).unwrap();
        }

        let latest = idx.latest(5);
        assert_eq!(latest.len(), 5);
        assert_eq!(latest[0].0, 99);
    }
}
