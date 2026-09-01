//! Segmented BTree index — time-windowed sealed chunks + a growing in-memory segment.
//!
//! Each sealed chunk covers a contiguous time window bounded by entry count.
//! Range queries use manifest's `first_id`/`last_id` (timestamps) to skip irrelevant chunks.

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::engine::error::{Result, StorageError};
use crate::engine::index::manifest::{ChunkMeta, SegmentManifest};
use crate::engine::index::segment_io::{
    SegmentHeader, SegmentReader, SegmentWriter, INDEX_TYPE_BTREE,
};
use crate::engine::index::{BTreeConfig, BTreeIndex, IndexPosition};
use crate::engine::storage::durable_rename::durable_rename;

// ── Sealed chunk ───────────────────────────────────────────────────────────────

struct SealedBTreeChunk {
    seq_no: u32,
    /// Min timestamp in this chunk (from manifest).
    first_ts: u64,
    /// Max timestamp in this chunk (from manifest).
    last_ts: u64,
    /// Loaded BTree index for this chunk.
    index: BTreeIndex,
    /// Local doc ID -> external key. This is the source of truth for deletion bit positions.
    keys_by_doc_id: Vec<Bytes>,
    /// External key -> local doc ID.
    doc_id_by_key: HashMap<Bytes, usize>,
    /// Deletion bitset — `deleted[i]` is true if local doc `i` is deleted.
    deleted: RwLock<Vec<bool>>,
    /// Whether the deletion bitset has changed since last save.
    deletions_dirty: AtomicBool,
    /// Absolute path to the `.del` file for this chunk.
    del_path: PathBuf,
}

impl SealedBTreeChunk {
    fn overlaps(&self, start: u64, end: u64) -> bool {
        self.first_ts <= end && self.last_ts >= start
    }

    fn entry_count(&self) -> usize {
        self.keys_by_doc_id.len() - self.deletion_count()
    }

    fn doc_count(&self) -> usize {
        self.keys_by_doc_id.len()
    }

    fn deletion_count(&self) -> usize {
        self.deleted.read().iter().filter(|&&d| d).count()
    }

    fn filename(&self, index_name: &str) -> String {
        format!("{}_{:04}.seg", index_name, self.seq_no)
    }

    fn is_key_deleted(&self, key: &[u8]) -> bool {
        let Some(&doc_id) = self.doc_id_by_key.get(key) else {
            return false;
        };
        self.deleted.read().get(doc_id).copied().unwrap_or(false)
    }

    fn mark_deleted(&self, key: &[u8]) -> bool {
        let Some(&doc_id) = self.doc_id_by_key.get(key) else {
            return false;
        };
        let mut deleted = self.deleted.write();
        if deleted.get(doc_id).copied().unwrap_or(false) {
            return false;
        }
        deleted[doc_id] = true;
        self.deletions_dirty.store(true, Ordering::Relaxed);
        true
    }

    fn live_entries(&self, start: u64, end: u64) -> Vec<(u64, Bytes)> {
        self.index
            .range(start, end)
            .into_iter()
            .filter(|(_, key)| !self.is_key_deleted(key))
            .collect()
    }

    fn live_entries_limit(&self, start: u64, end: u64, limit: usize) -> Vec<(u64, Bytes)> {
        if limit == 0 {
            return Vec::new();
        }

        let mut results = Vec::with_capacity(limit);
        self.index.visit_range(start, end, |timestamp, key| {
            if !self.is_key_deleted(key) {
                results.push((timestamp, key.clone()));
            }
            results.len() < limit
        });
        results
    }

    fn live_entries_page(
        &self,
        start: u64,
        end: u64,
        after: Option<&IndexPosition>,
        descending: bool,
        limit: usize,
    ) -> Vec<IndexPosition> {
        if limit == 0 {
            return Vec::new();
        }

        let mut results = Vec::with_capacity(limit.min(1024));
        self.index
            .visit_range_ordered(start, end, after, descending, |timestamp, key| {
                if !self.is_key_deleted(key) {
                    results.push((timestamp, key.clone()));
                }
                results.len() < limit
            });
        results
    }

    fn load_deletions(del_path: &Path, doc_count: usize) -> Result<Vec<bool>> {
        if !del_path.exists() {
            return Ok(vec![false; doc_count]);
        }
        let bytes = std::fs::read(del_path).map_err(StorageError::Io)?;
        let expected_len = doc_count.div_ceil(8);
        if bytes.len() != expected_len {
            return Err(StorageError::invalid_format(
                del_path,
                format!(
                    "BTree deletion bitset has {} bytes for {doc_count} entries; expected {expected_len}",
                    bytes.len()
                ),
            ));
        }

        let mut deleted = vec![false; doc_count];
        for (i, &byte) in bytes.iter().enumerate() {
            for bit in 0..8 {
                let idx = i * 8 + bit;
                if idx >= doc_count {
                    break;
                }
                deleted[idx] = (byte >> bit) & 1 == 1;
            }
        }
        Ok(deleted)
    }

    fn save_deletions_if_dirty(&self) -> Result<()> {
        if !self.deletions_dirty.load(Ordering::Relaxed) {
            return Ok(());
        }
        write_deletions(&self.del_path, &self.deleted.read())?;
        self.deletions_dirty.store(false, Ordering::Relaxed);
        Ok(())
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

    /// Load from manifest + segment files in `dir`.
    ///
    /// A parse failure is refused by default rather than silently rebuilt
    /// from empty — see [`crate::engine::index::on_index_parse_failure`].
    pub fn load_from_dir(config: BTreeConfig, dir: PathBuf) -> Result<Self> {
        // An index rekey (REM-99) that was interrupted mid-swap left the
        // previous generation in a backup directory. Repair it before
        // reading, so neither a migration rerun nor a normal startup can
        // observe the half-published state.
        crate::engine::index::rekey::recover_interrupted_publish(&dir, &Self::rekey_artifacts())?;
        match Self::try_load_from_dir(config.clone(), &dir) {
            Ok(idx) => Ok(idx),
            Err(e) => {
                crate::engine::index::on_index_parse_failure(&dir, &e)?;
                Ok(Self::new(config, dir))
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
            sealed.push(Self::load_sealed_chunk(&seg_path, chunk_meta, dir)?);
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

    fn load_sealed_chunk(path: &Path, meta: &ChunkMeta, dir: &Path) -> Result<SealedBTreeChunk> {
        let reader = SegmentReader::open(path)?;
        let mut cursor = reader.data_cursor();
        let index = deserialize_btree_index(&mut cursor)?;
        let keys_by_doc_id = key_directory_for_btree(&index);
        if keys_by_doc_id.len() != meta.entry_count as usize {
            return Err(StorageError::invalid_format(
                path,
                format!(
                    "BTree segment key directory has {} entries; manifest records {}",
                    keys_by_doc_id.len(),
                    meta.entry_count
                ),
            ));
        }
        let doc_id_by_key = build_doc_id_by_key(&keys_by_doc_id)?;
        let del_path = del_file_path(dir, Self::INDEX_NAME, meta.seq_no);
        let deleted = SealedBTreeChunk::load_deletions(&del_path, keys_by_doc_id.len())?;

        Ok(SealedBTreeChunk {
            seq_no: meta.seq_no,
            first_ts: meta.first_id,
            last_ts: meta.last_id,
            index,
            keys_by_doc_id,
            doc_id_by_key,
            deleted: RwLock::new(deleted),
            deletions_dirty: AtomicBool::new(false),
            del_path,
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
        let keys_by_doc_id = key_directory_for_btree(&old_growing);
        let doc_id_by_key = build_doc_id_by_key(&keys_by_doc_id)?;
        let doc_count = keys_by_doc_id.len();
        let del_path = del_file_path(&self.dir, Self::INDEX_NAME, seq_no);

        self.sealed.push(SealedBTreeChunk {
            seq_no,
            first_ts,
            last_ts,
            index: old_growing,
            keys_by_doc_id,
            doc_id_by_key,
            deleted: RwLock::new(vec![false; doc_count]),
            deletions_dirty: AtomicBool::new(false),
            del_path,
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
    /// On every checkpoint or graceful shutdown we must flush whatever is in
    /// the growing segment, regardless of its size — otherwise entries that
    /// have never been sealed are silently lost on restart.
    pub fn save_if_dirty(&mut self) -> Result<()> {
        if !self.growing.is_empty() {
            self.seal_growing()?;
        }

        for chunk in &self.sealed {
            chunk.save_deletions_if_dirty()?;
        }
        self.dirty.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Whether there are unsaved changes.
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
            || self.growing.is_dirty()
            || self
                .sealed
                .iter()
                .any(|s| s.deletions_dirty.load(Ordering::Relaxed))
    }

    // ── Write operations ───────────────────────────────────────────────────────

    /// Insert a timestamp-key pair.
    pub fn insert(&self, timestamp: u64, key: impl Into<Bytes>) -> Result<()> {
        let key = key.into();
        let removed_from_sealed = self.mark_deleted_in_sealed(&key);
        let result = self.growing.insert(timestamp, key);
        if result.is_ok() || removed_from_sealed {
            self.dirty.store(true, Ordering::Relaxed);
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn insert_raw_without_retiring_sealed(
        &self,
        timestamp: u64,
        key: impl Into<Bytes>,
    ) -> Result<()> {
        let result = self.growing.insert(timestamp, key);
        if result.is_ok() {
            self.dirty.store(true, Ordering::Relaxed);
        }
        result
    }

    /// Remove a key from the index.
    pub fn remove(&self, key: &[u8]) -> Result<bool> {
        let in_growing = self.growing.remove(key)?;
        let in_sealed = self.mark_deleted_in_sealed(key);
        if in_growing || in_sealed {
            self.dirty.store(true, Ordering::Relaxed);
        }
        Ok(in_growing || in_sealed)
    }

    // ── Read operations ────────────────────────────────────────────────────────

    /// Query a range of timestamps (inclusive).
    pub fn range(&self, start: u64, end: u64) -> Vec<(u64, Bytes)> {
        let mut results = Vec::new();
        for chunk in &self.sealed {
            if chunk.overlaps(start, end) {
                results.extend(chunk.live_entries(start, end));
            }
        }
        results.extend(self.growing.range(start, end));
        results.sort_by_key(|(ts, _)| *ts);
        results
    }

    /// Query a range with limit.
    pub fn range_limit(&self, start: u64, end: u64, limit: usize) -> Vec<(u64, Bytes)> {
        if limit == 0 {
            return Vec::new();
        }

        let mut results = Vec::new();
        for chunk in &self.sealed {
            if chunk.overlaps(start, end) {
                results.extend(chunk.live_entries_limit(start, end, limit));
            }
        }
        results.extend(self.growing.range_limit(start, end, limit));
        results.sort_by_key(|(ts, _)| *ts);
        results.truncate(limit);
        results
    }

    /// One page of the index's total order: at most `limit` live entries from
    /// `start..=end`, resuming after `after`, in the requested direction.
    ///
    /// Every source is asked for its own first `limit` entries past the
    /// position. The globally first `limit` are necessarily among that union,
    /// so merging and truncating is exact -- and no source walks further than
    /// the page it might contribute.
    pub fn range_page(
        &self,
        start: u64,
        end: u64,
        after: Option<&IndexPosition>,
        descending: bool,
        limit: usize,
    ) -> Vec<IndexPosition> {
        if limit == 0 {
            return Vec::new();
        }

        let mut results = Vec::new();
        for chunk in &self.sealed {
            if chunk.overlaps(start, end) {
                results.extend(chunk.live_entries_page(start, end, after, descending, limit));
            }
        }
        results.extend(
            self.growing
                .range_page(start, end, after, descending, limit),
        );

        // Sorted by the total order the position names, so the merge boundary
        // is the same order a caller resumes from. A key can appear in more
        // than one source; an exact duplicate would otherwise spend two slots
        // of one page on one entry.
        results.sort();
        results.dedup();
        if descending {
            results.reverse();
        }
        results.truncate(limit);
        results
    }

    /// Total number of entries.
    pub fn len(&self) -> usize {
        let sealed_count: usize = self.sealed.iter().map(|c| c.entry_count()).sum();
        sealed_count + self.growing.len()
    }

    /// Whether the index holds no live entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether size-tiered compaction should be triggered.
    pub fn needs_compaction(&self) -> bool {
        let total_docs: usize = self.sealed.iter().map(SealedBTreeChunk::doc_count).sum();
        let deleted: usize = self
            .sealed
            .iter()
            .map(SealedBTreeChunk::deletion_count)
            .sum();
        let deletion_ratio = if total_docs == 0 {
            0.0
        } else {
            deleted as f64 / total_docs as f64
        };
        deletion_ratio > crate::engine::index::COMPACTION_DELETION_RATIO || self.sealed.len() > 10
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
        let (a_idx, b_idx) = if a_idx < b_idx {
            (a_idx, b_idx)
        } else {
            (b_idx, a_idx)
        };

        // Merge: collect all live entries from both chunks. Later sequence
        // numbers win for duplicate keys so stale values cannot suppress the
        // current value.
        let mut live_by_key: HashMap<Bytes, u64> = HashMap::new();
        let mut selected = [a_idx, b_idx];
        selected.sort_by_key(|&i| self.sealed[i].seq_no);
        for &si in &selected {
            let chunk = &self.sealed[si];
            for (ts, key) in chunk.live_entries(0, u64::MAX) {
                live_by_key.insert(key, ts);
            }
        }

        let merged = BTreeIndex::new(self.config.clone());
        for (key, ts) in live_by_key {
            let _ = merged.insert(ts, key);
        }

        // Write merged chunk to disk
        std::fs::create_dir_all(&self.dir)?;
        let seq_no = self.manifest.next_seq_no();
        let filename = format!("{}_{:04}.seg", Self::INDEX_NAME, seq_no);
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

        let old_filenames: Vec<String> = vec![
            self.sealed[a_idx].filename(Self::INDEX_NAME),
            self.sealed[b_idx].filename(Self::INDEX_NAME),
        ];

        self.manifest
            .chunks
            .retain(|c| !old_filenames.contains(&c.filename));

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

        for fname in &old_filenames {
            let _ = std::fs::remove_file(self.dir.join(fname));
            let del_fname = fname.replace(".seg", ".del");
            let _ = std::fs::remove_file(self.dir.join(del_fname));
        }

        // Update in-memory sealed list
        self.sealed.remove(b_idx);
        self.sealed.remove(a_idx);

        let keys_by_doc_id = key_directory_for_btree(&merged);
        let doc_id_by_key = build_doc_id_by_key(&keys_by_doc_id)?;
        let doc_count = keys_by_doc_id.len();
        let del_path = del_file_path(&self.dir, Self::INDEX_NAME, seq_no);

        self.sealed.push(SealedBTreeChunk {
            seq_no,
            first_ts,
            last_ts,
            index: merged,
            keys_by_doc_id,
            doc_id_by_key,
            deleted: RwLock::new(vec![false; doc_count]),
            deletions_dirty: AtomicBool::new(false),
            del_path,
        });

        tracing::info!(
            "BTree index compaction: merged 2 chunks into seq_no={} ({} entries)",
            seq_no,
            entry_count
        );
        Ok(true)
    }

    fn mark_deleted_in_sealed(&self, key: &[u8]) -> bool {
        let mut removed = false;
        for chunk in &self.sealed {
            removed |= chunk.mark_deleted(key);
        }
        removed
    }

    pub(crate) fn rewrite_keys_in_dir<F>(
        config: BTreeConfig,
        dir: &Path,
        mut rewrite_key: F,
    ) -> Result<bool>
    where
        F: FnMut(&Bytes) -> Result<Bytes>,
    {
        let existing = Self::load_from_dir(config.clone(), dir.to_path_buf())?;
        let entries = existing.range(0, u64::MAX);
        let mut changed = false;
        let tmp_dir = dir.join(".timeseries-rekey.tmp");
        if tmp_dir.exists() {
            std::fs::remove_dir_all(&tmp_dir)?;
        }
        std::fs::create_dir_all(&tmp_dir)?;

        let mut rebuilt = Self::new(config, tmp_dir.clone());
        for (timestamp, key) in entries {
            let rewritten = rewrite_key(&key)?;
            changed |= rewritten != key;
            rebuilt.insert(timestamp, rewritten)?;
        }

        if !changed {
            std::fs::remove_dir_all(&tmp_dir)?;
            return Ok(false);
        }

        rebuilt.seal_growing()?;
        crate::engine::index::rekey::publish_rebuilt_index(
            dir,
            &tmp_dir,
            &Self::rekey_artifacts(),
        )?;
        Ok(true)
    }

    fn rekey_artifacts() -> crate::engine::index::rekey::IndexArtifacts {
        crate::engine::index::rekey::IndexArtifacts::new(Self::INDEX_NAME)
    }

    /// Convert a pre-segmented `timeseries.idx` in `dir` into segment files.
    ///
    /// `init.rs` does this on open, but the partition migration (REM-99) has
    /// to run *before* the indexes are opened, and it can only rekey entries
    /// it can see through the manifest. Without this, a data directory old
    /// enough to still hold `.idx` files would have its payload keys
    /// partitioned while its index keys stayed unpartitioned — and because
    /// `partition.layout` advances regardless, they would never be rekeyed
    /// again. Converting here lets the migration rekey them; `init.rs` then
    /// finds a manifest and takes its normal path.
    ///
    /// Returns whether a conversion happened. A legacy file that fails to
    /// load is left in place for `init.rs` to report through
    /// `on_index_parse_failure`.
    pub(crate) fn convert_legacy_file_in_dir(config: BTreeConfig, dir: &Path) -> Result<bool> {
        let legacy_path = dir.join(format!("{}.idx", Self::INDEX_NAME));
        let manifest_path = dir.join(format!("{}.manifest", Self::INDEX_NAME));
        if !legacy_path.exists() || manifest_path.exists() {
            return Ok(false);
        }

        let Ok(legacy) = BTreeIndex::load(&legacy_path) else {
            return Ok(false);
        };
        let mut rebuilt = Self::new(config, dir.to_path_buf());
        for (ts, key) in legacy.range(0, u64::MAX) {
            rebuilt.insert(ts, key)?;
        }
        rebuilt.seal_growing()?;
        std::fs::remove_file(&legacy_path)?;
        tracing::info!(
            "Converted legacy {:?} to segmented format before partition rekey",
            legacy_path
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
        w.write_all(&timestamp.to_le_bytes())
            .map_err(StorageError::Io)?;
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

    // Skip legacy config bytes (max_entries — no longer stored on BTreeConfig).
    cursor.read_exact(&mut buf8)?;

    cursor.read_exact(&mut buf8)?;
    let entry_count = u64::from_le_bytes(buf8) as usize;

    cursor.read_exact(&mut buf8)?;
    let timestamp_count = u64::from_le_bytes(buf8) as usize;

    let index = BTreeIndex::new(BTreeConfig::default());
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

fn key_directory_for_btree(index: &BTreeIndex) -> Vec<Bytes> {
    index
        .range(
            index.min_timestamp().unwrap_or(0),
            index.max_timestamp().unwrap_or(0),
        )
        .into_iter()
        .map(|(_, key)| key)
        .collect()
}

fn build_doc_id_by_key(keys_by_doc_id: &[Bytes]) -> Result<HashMap<Bytes, usize>> {
    let mut doc_id_by_key = HashMap::with_capacity(keys_by_doc_id.len());
    for (doc_id, key) in keys_by_doc_id.iter().enumerate() {
        if doc_id_by_key.insert(key.clone(), doc_id).is_some() {
            return Err(StorageError::Serialization(format!(
                "Duplicate key in BTree segment directory: {:?}",
                String::from_utf8_lossy(key)
            )));
        }
    }
    Ok(doc_id_by_key)
}

fn del_file_path(dir: &Path, index_name: &str, seq_no: u32) -> PathBuf {
    dir.join(format!("{}_{:04}.del", index_name, seq_no))
}

fn write_deletions(del_path: &Path, deleted: &[bool]) -> Result<()> {
    let doc_count = deleted.len();
    let byte_count = doc_count.div_ceil(8);
    let mut bytes = vec![0u8; byte_count];
    for (i, &del) in deleted.iter().enumerate() {
        if del {
            bytes[i / 8] |= 1 << (i % 8);
        }
    }
    let tmp = del_path.with_extension("del.tmp");
    std::fs::write(&tmp, &bytes).map_err(StorageError::Io)?;
    durable_rename(&tmp, del_path)?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// The whole point of routing the rekey publish through a backup
    /// directory: a crash between "old generation moved aside" and "new
    /// manifest committed" must not cost the index. Before this, the publish
    /// deleted the live artifacts first, so the interrupted state was
    /// indistinguishable from an empty index — and the migration's own rerun
    /// would rebuild *from* that empty index, report success, and let the
    /// format version advance over the loss.
    #[test]
    fn an_interrupted_rekey_publish_is_rolled_back_on_the_next_load() {
        let dir = tempdir().unwrap();
        {
            let mut idx =
                SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());
            for i in 0u64..5 {
                idx.insert(i * 100, format!("memory:{i}")).unwrap();
            }
            idx.seal_growing().unwrap();
        }

        // Simulate the crash window: the publish moved the live generation
        // into the backup directory and died before any new manifest landed.
        let backup = dir.path().join(".rekey-backup-timeseries");
        std::fs::create_dir_all(&backup).unwrap();
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            if !entry.file_type().unwrap().is_file() {
                continue;
            }
            std::fs::rename(entry.path(), backup.join(entry.file_name())).unwrap();
        }

        let recovered =
            SegmentedBTreeIndex::load_from_dir(BTreeConfig::default(), dir.path().to_path_buf())
                .unwrap();

        assert_eq!(
            recovered.range(0, u64::MAX).len(),
            5,
            "an interrupted publish must restore the previous generation, not read as empty"
        );
        assert!(!backup.exists(), "recovery must clear the backup directory");
    }

    /// A directory old enough to still hold `timeseries.idx` has to be
    /// converted before the partition migration can rekey it — the rekey
    /// only sees entries reachable through a manifest, and the format
    /// version advances whether or not anything was rekeyed.
    #[test]
    fn a_legacy_idx_file_is_converted_to_segments() {
        let dir = tempdir().unwrap();
        let legacy_path = dir.path().join("timeseries.idx");
        {
            let legacy = BTreeIndex::new(BTreeConfig::default());
            legacy
                .insert(100, Bytes::from_static(b"memory:legacy"))
                .unwrap();
            let mut buf = Vec::new();
            serialize_btree_index(&legacy, &mut buf).unwrap();
            std::fs::write(&legacy_path, &buf).unwrap();
        }

        let converted =
            SegmentedBTreeIndex::convert_legacy_file_in_dir(BTreeConfig::default(), dir.path())
                .unwrap();

        assert!(converted);
        assert!(!legacy_path.exists(), "the legacy file must be consumed");
        let idx =
            SegmentedBTreeIndex::load_from_dir(BTreeConfig::default(), dir.path().to_path_buf())
                .unwrap();
        assert_eq!(
            idx.range(0, u64::MAX),
            vec![(100, Bytes::from_static(b"memory:legacy"))]
        );
    }

    #[test]
    fn converting_a_legacy_file_is_skipped_when_segments_already_exist() {
        let dir = tempdir().unwrap();
        {
            let mut idx =
                SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());
            idx.insert(1, Bytes::from_static(b"memory:current"))
                .unwrap();
            idx.seal_growing().unwrap();
        }
        std::fs::write(dir.path().join("timeseries.idx"), b"stale").unwrap();

        let converted =
            SegmentedBTreeIndex::convert_legacy_file_in_dir(BTreeConfig::default(), dir.path())
                .unwrap();

        assert!(
            !converted,
            "a manifest takes precedence; the stale legacy file must not overwrite it"
        );
    }

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
            SegmentedBTreeIndex::load_from_dir(BTreeConfig::default(), dir.path().to_path_buf())
                .unwrap();

        assert_eq!(reloaded.sealed.len(), 1);
        let results = reloaded.range(0, 400);
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn sealed_removal_survives_reload() {
        let dir = tempdir().unwrap();
        {
            let mut idx =
                SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());
            idx.insert(100, "stale").unwrap();
            idx.seal_growing().unwrap();

            assert!(idx.remove(b"stale").unwrap());
            idx.save_if_dirty().unwrap();
            assert!(idx.range(0, u64::MAX).is_empty());
        }

        let reloaded =
            SegmentedBTreeIndex::load_from_dir(BTreeConfig::default(), dir.path().to_path_buf())
                .unwrap();
        assert!(
            reloaded.range(0, u64::MAX).is_empty(),
            "sealed removal must persist across reload"
        );
    }

    #[test]
    fn replacement_insert_retires_older_sealed_value() {
        let dir = tempdir().unwrap();
        {
            let mut idx =
                SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());
            idx.insert(100, "memory:x").unwrap();
            idx.seal_growing().unwrap();

            idx.insert(900, "memory:x").unwrap();
            idx.save_if_dirty().unwrap();
        }

        let reloaded =
            SegmentedBTreeIndex::load_from_dir(BTreeConfig::default(), dir.path().to_path_buf())
                .unwrap();
        assert_eq!(
            reloaded.range(0, u64::MAX),
            vec![(900, Bytes::from_static(b"memory:x"))]
        );
    }

    #[test]
    fn compaction_keeps_newest_live_duplicate() {
        let dir = tempdir().unwrap();
        let mut idx = SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());

        idx.insert(100, "memory:x").unwrap();
        idx.seal_growing().unwrap();

        // Bypass `SegmentedBTreeIndex::insert` to stage the historical bug:
        // two sealed chunks both contain the same key, and compaction must
        // keep the newest one rather than whichever is merged first.
        idx.growing.insert(900, "memory:x").unwrap();
        idx.seal_growing().unwrap();

        assert!(idx.compact().unwrap());
        assert_eq!(
            idx.range(0, u64::MAX),
            vec![(900, Bytes::from_static(b"memory:x"))]
        );
    }

    #[test]
    fn invalid_deletion_sidecar_is_refused() {
        let dir = tempdir().unwrap();
        {
            let mut idx =
                SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());
            idx.insert(100, "memory:x").unwrap();
            idx.seal_growing().unwrap();
        }
        std::fs::write(dir.path().join("timeseries_0000.del"), [0, 0]).unwrap();

        let err = match SegmentedBTreeIndex::load_from_dir(
            BTreeConfig::default(),
            dir.path().to_path_buf(),
        ) {
            Ok(_) => panic!("invalid deletion sidecar should be refused"),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("Deletion bitset") || err.contains("deletion bitset"),
            "error should name the invalid deletion sidecar: {err}"
        );
    }

    #[test]
    fn bounded_range_skips_deleted_entries_and_stops_after_live_limit() {
        let dir = tempdir().unwrap();
        let mut idx = SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());

        idx.insert(100, "deleted-early").unwrap();
        idx.insert(200, "kept-1").unwrap();
        idx.insert(300, "kept-2").unwrap();
        idx.insert(400, "not-visited").unwrap();
        idx.seal_growing().unwrap();
        assert!(idx.remove(b"deleted-early").unwrap());

        assert_eq!(
            idx.range_limit(0, u64::MAX, 2),
            vec![
                (200, Bytes::from_static(b"kept-1")),
                (300, Bytes::from_static(b"kept-2")),
            ]
        );
    }

    #[test]
    fn bounded_range_merges_sealed_and_growing_entries_in_order() {
        let dir = tempdir().unwrap();
        let mut idx = SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());

        idx.insert(200, "sealed").unwrap();
        idx.seal_growing().unwrap();
        idx.insert(100, "growing-early").unwrap();
        idx.insert(300, "growing-late").unwrap();

        assert_eq!(
            idx.range_limit(0, u64::MAX, 2),
            vec![
                (100, Bytes::from_static(b"growing-early")),
                (200, Bytes::from_static(b"sealed")),
            ]
        );
    }

    /// Walk the whole index one page at a time, resuming from each page's
    /// last position.
    fn walk_by_pages(
        idx: &SegmentedBTreeIndex,
        descending: bool,
        page: usize,
    ) -> Vec<(u64, Bytes)> {
        let mut seen = Vec::new();
        let mut after: Option<(u64, Bytes)> = None;
        loop {
            let batch = idx.range_page(0, u64::MAX, after.as_ref(), descending, page);
            if batch.is_empty() {
                return seen;
            }
            after = batch.last().cloned();
            seen.extend(batch);
        }
    }

    #[test]
    fn a_page_resumes_across_the_boundary_between_sealed_and_growing_entries() {
        let dir = tempdir().unwrap();
        let mut idx = SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());

        // Interleave the two sources across the ordering, so a page boundary
        // has to cross between them rather than merely follow one and then
        // the other.
        for i in 0..10u64 {
            idx.insert(i * 10, format!("sealed{i:02}")).unwrap();
        }
        idx.seal_growing().unwrap();
        for i in 0..10u64 {
            idx.insert(i * 10 + 5, format!("growing{i:02}")).unwrap();
        }

        let whole = idx.range_page(0, u64::MAX, None, false, 100);
        let paged = walk_by_pages(&idx, false, 3);

        assert_eq!(paged.len(), 20);
        assert_eq!(paged, whole, "paging reconstructs the merged walk exactly");
    }

    #[test]
    fn a_descending_page_walk_is_the_ascending_walk_reversed() {
        let dir = tempdir().unwrap();
        let mut idx = SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());

        for i in 0..6u64 {
            idx.insert(i * 10, format!("sealed{i}")).unwrap();
        }
        idx.seal_growing().unwrap();
        for i in 0..6u64 {
            idx.insert(i * 10, format!("growing{i}")).unwrap();
        }

        let mut ascending = walk_by_pages(&idx, false, 4);
        let descending = walk_by_pages(&idx, true, 4);
        ascending.reverse();

        assert_eq!(descending, ascending);
    }

    #[test]
    fn a_page_skips_deleted_entries_without_spending_the_page_on_them() {
        let dir = tempdir().unwrap();
        let mut idx = SegmentedBTreeIndex::new(BTreeConfig::default(), dir.path().to_path_buf());

        for i in 0..10u64 {
            idx.insert(i * 10, format!("key{i:02}")).unwrap();
        }
        idx.seal_growing().unwrap();
        for i in 0..5u64 {
            assert!(idx.remove(format!("key{i:02}").as_bytes()).unwrap());
        }

        let page = idx.range_page(0, u64::MAX, None, false, 3);
        let keys: Vec<String> = page
            .iter()
            .map(|(_, k)| String::from_utf8_lossy(k).into_owned())
            .collect();

        assert_eq!(keys, vec!["key05", "key06", "key07"]);
    }
}
