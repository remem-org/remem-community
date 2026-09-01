//! Segmented inverted index — Lucene-style sealed segments + a growing in-memory segment.
//!
//! Each sealed segment covers a contiguous range of document IDs and is stored as a
//! `.seg` file. Deletions are tracked in a per-segment in-memory bitset that is saved
//! to a lightweight `{index}_{seqno:04}.del` file on checkpoint.

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::engine::error::{Result, StorageError};
use crate::engine::index::manifest::{ChunkMeta, SegmentManifest};
use crate::engine::index::segment_io::{
    SegmentHeader, SegmentReader, SegmentWriter, INDEX_TYPE_INVERTED,
};
use crate::engine::index::{InvertedIndex, InvertedIndexConfig, TAGS_CHUNK_SIZE};
use crate::engine::storage::durable_rename::durable_rename;

// ── Sealed segment ─────────────────────────────────────────────────────────────

/// A sealed, read-only segment covering docs `[doc_start, doc_start + doc_count)`.
struct SealedTagSegment {
    seq_no: u32,
    /// The loaded inverted index (already populated).
    index: InvertedIndex,
    /// Local doc ID -> external key. This is the source of truth for deletion bit positions.
    keys_by_doc_id: Vec<Bytes>,
    /// External key -> local doc ID.
    doc_id_by_key: HashMap<Bytes, usize>,
    /// Deletion bitset — `deleted[i]` is true if local doc `i` is deleted.
    deleted: RwLock<Vec<bool>>,
    /// Whether the deletion bitset has changed since last save.
    deletions_dirty: AtomicBool,
    /// Absolute path to the `.del` file for this segment.
    del_path: PathBuf,
}

impl SealedTagSegment {
    fn doc_count(&self) -> u32 {
        self.keys_by_doc_id.len() as u32
    }

    fn deletion_count(&self) -> usize {
        self.deleted.read().iter().filter(|&&d| d).count()
    }

    /// Derive the segment filename from seq_no.
    fn filename(&self) -> String {
        format!("tags_{:04}.seg", self.seq_no)
    }

    /// Iterate all keys in this segment that are not soft-deleted.
    fn live_keys(&self) -> Vec<Bytes> {
        let deleted = self.deleted.read();
        self.keys_by_doc_id
            .iter()
            .enumerate()
            .filter(|(doc_id, _)| !deleted[*doc_id])
            .map(|(_, key)| key.clone())
            .collect()
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

    /// Load the deletion bitset from disk (`.del` file). Missing = all alive.
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
                    "Deletion bitset has {} bytes for {doc_count} docs; expected {expected_len}",
                    bytes.len()
                ),
            ));
        }
        // Bit-packed: byte[i] bit j => doc i*8+j deleted
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

    /// Save deletion bitset to disk if dirty.
    fn save_deletions_if_dirty(&self) -> Result<()> {
        if !self.deletions_dirty.load(Ordering::Relaxed) {
            return Ok(());
        }
        let deleted = self.deleted.read();
        let doc_count = deleted.len();
        let byte_count = doc_count.div_ceil(8);
        let mut bytes = vec![0u8; byte_count];
        for (i, &del) in deleted.iter().enumerate() {
            if del {
                bytes[i / 8] |= 1 << (i % 8);
            }
        }
        drop(deleted);
        let tmp = self.del_path.with_extension("del.tmp");
        std::fs::write(&tmp, &bytes).map_err(StorageError::Io)?;
        durable_rename(&tmp, &self.del_path)?;
        self.deletions_dirty.store(false, Ordering::Relaxed);
        Ok(())
    }
}

// ── SegmentedInvertedIndex ─────────────────────────────────────────────────────

/// Segmented inverted index: sealed read-only segments + a growing mutable segment.
///
/// Wrapped in `Arc<RwLock<SegmentedInvertedIndex>>` by `StorageEngine`.
pub struct SegmentedInvertedIndex {
    config: InvertedIndexConfig,
    /// Sealed segments, ordered by doc_start.
    sealed: Vec<SealedTagSegment>,
    /// Currently growing segment (takes all new writes).
    growing: InvertedIndex,
    /// Index directory (for segment files and manifest).
    dir: PathBuf,
    /// Manifest tracking all sealed chunks.
    manifest: SegmentManifest,
    /// Whether the growing segment has unflushed changes.
    dirty: AtomicBool,
}

impl SegmentedInvertedIndex {
    const INDEX_NAME: &'static str = "tags";

    /// Create a new empty segmented index writing to `dir`.
    pub fn new(config: InvertedIndexConfig, dir: PathBuf) -> Self {
        let growing = InvertedIndex::new(config.clone());
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
    pub fn load_from_dir(config: InvertedIndexConfig, dir: PathBuf) -> Result<Self> {
        // See `SegmentedBTreeIndex::load_from_dir` -- repair an interrupted
        // rekey swap before reading anything.
        crate::engine::index::rekey::recover_interrupted_publish(&dir, &Self::rekey_artifacts())?;
        match Self::try_load_from_dir(config.clone(), &dir) {
            Ok(idx) => Ok(idx),
            Err(e) => {
                crate::engine::index::on_index_parse_failure(&dir, &e)?;
                Ok(Self::new(config, dir))
            }
        }
    }

    fn try_load_from_dir(config: InvertedIndexConfig, dir: &Path) -> Result<Self> {
        let manifest = match SegmentManifest::load(dir, Self::INDEX_NAME)? {
            Some(m) => m,
            None => {
                return Ok(Self::new(config, dir.to_path_buf()));
            }
        };

        let mut sealed = Vec::with_capacity(manifest.chunks.len());
        let mut next_doc_start = 0u32;

        for chunk_meta in &manifest.chunks {
            let seg_path = dir.join(&chunk_meta.filename);
            match Self::load_sealed_segment(&seg_path, chunk_meta, dir) {
                Ok(seg) => {
                    next_doc_start += seg.doc_count();
                    sealed.push(seg);
                }
                Err(e) => {
                    tracing::warn!(
                        "Corrupt tag segment {:?}: {}; skipping (WAL will rebuild)",
                        seg_path,
                        e
                    );
                    // Still advance doc_start by the claimed entry_count so IDs stay consistent
                    next_doc_start += chunk_meta.entry_count;
                }
            }
        }

        tracing::info!(
            "Loaded segmented tag index: {} sealed segments, {} total docs",
            sealed.len(),
            next_doc_start
        );

        Ok(Self {
            config: config.clone(),
            sealed,
            growing: InvertedIndex::new(config),
            manifest,
            dir: dir.to_path_buf(),
            dirty: AtomicBool::new(false),
        })
    }

    fn load_sealed_segment(path: &Path, meta: &ChunkMeta, dir: &Path) -> Result<SealedTagSegment> {
        let reader = SegmentReader::open(path)?;
        let (index, keys_by_doc_id) = deserialize_tag_segment_v2(reader.data())?;
        let doc_count = keys_by_doc_id.len();
        if doc_count != meta.entry_count as usize {
            return Err(StorageError::invalid_format(
                path,
                format!(
                    "Tag segment key directory has {doc_count} docs; manifest records {}",
                    meta.entry_count
                ),
            ));
        }
        let doc_id_by_key = build_doc_id_by_key(&keys_by_doc_id)?;

        let del_path = del_file_path(dir, Self::INDEX_NAME, meta.seq_no);
        let deleted = SealedTagSegment::load_deletions(&del_path, doc_count)?;

        Ok(SealedTagSegment {
            seq_no: meta.seq_no,
            index,
            keys_by_doc_id,
            doc_id_by_key,
            deleted: RwLock::new(deleted),
            deletions_dirty: AtomicBool::new(false),
            del_path,
        })
    }

    /// Whether the index can represent `token`. See `InvertedIndex::can_represent`.
    pub fn can_represent(&self, token: &str) -> bool {
        self.growing.can_represent(token)
    }

    /// Seal the growing segment to disk and add it to the manifest.
    ///
    /// After sealing, `growing` is replaced with a fresh empty index.
    /// No-op if `growing` is empty.
    pub fn seal_growing(&mut self) -> Result<()> {
        if self.growing.is_empty() {
            return Ok(());
        }

        std::fs::create_dir_all(&self.dir).map_err(StorageError::Io)?;

        let seq_no = self.manifest.next_seq_no();
        let filename = format!("{}_{:04}.seg", Self::INDEX_NAME, seq_no);
        let seg_path = self.dir.join(&filename);

        // Compute doc_start for this new segment
        let doc_start: u32 = self.sealed.iter().map(|s| s.doc_count()).sum();

        // Serialize the growing index
        let mut data_buf = Vec::new();
        let keys_by_doc_id = serialize_tag_segment_v2(&self.growing, &mut data_buf)?;

        let entry_count = self.growing.len() as u32;
        let header = SegmentHeader::new(
            *b"TAGS_SEG",
            INDEX_TYPE_INVERTED,
            seq_no,
            entry_count,
            doc_start as u64,
            (doc_start + entry_count.saturating_sub(1)) as u64,
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
            first_id: doc_start as u64,
            last_id: (doc_start + entry_count.saturating_sub(1)) as u64,
            crc32,
            sealed: true,
            has_deletions: false,
        });
        self.manifest.commit(&self.dir)?;

        // Build a fresh InvertedIndex from the serialized data so the sealed
        // segment holds its own copy (not shared with the now-discarded growing).
        let new_config = self.config.clone();
        let old_growing = std::mem::replace(&mut self.growing, InvertedIndex::new(new_config));

        let del_path = del_file_path(&self.dir, Self::INDEX_NAME, seq_no);
        let doc_count = old_growing.len();
        let doc_id_by_key = build_doc_id_by_key(&keys_by_doc_id)?;

        self.sealed.push(SealedTagSegment {
            seq_no,
            index: old_growing,
            keys_by_doc_id,
            doc_id_by_key,
            deleted: RwLock::new(vec![false; doc_count]),
            deletions_dirty: AtomicBool::new(false),
            del_path,
        });

        tracing::info!(
            "Sealed tag segment {} ({} docs, seq_no={})",
            seg_path.display(),
            entry_count,
            seq_no
        );

        Ok(())
    }

    /// Checkpoint: seal if growing >= threshold, then save dirty deletion bitsets.
    pub fn save_if_dirty(&mut self) -> Result<()> {
        if self.growing.len() as u32 >= TAGS_CHUNK_SIZE {
            self.seal_growing()?;
        }

        // Save dirty deletion bitsets
        for seg in &self.sealed {
            if let Err(e) = seg.save_deletions_if_dirty() {
                tracing::error!(
                    "Failed to save deletion bitset for tag segment {}: {}",
                    seg.seq_no,
                    e
                );
            }
        }

        // Save growing if dirty
        if self.growing.is_dirty() || self.dirty.load(Ordering::Relaxed) {
            self.dirty.store(false, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Whether there are any unsaved changes.
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
            || self.growing.is_dirty()
            || self
                .sealed
                .iter()
                .any(|s| s.deletions_dirty.load(Ordering::Relaxed))
    }

    // ── Write operations ───────────────────────────────────────────────────────

    /// Add tags to a document key (preserves existing tags, writes to growing).
    pub fn add_tags(&self, key: impl Into<Bytes>, tags: &[String]) -> Result<()> {
        let result = self.growing.add_tags(key, tags);
        if result.is_ok() {
            self.dirty.store(true, Ordering::Relaxed);
        }
        result
    }

    /// Replace all tags for a document key.
    pub fn set_tags(&self, key: impl Into<Bytes>, tags: &[String]) -> Result<()> {
        let key = key.into();
        let removed_from_sealed = self.mark_deleted_in_sealed(&key);
        let result = self.growing.set_tags(key, tags);
        if result.is_ok() || removed_from_sealed {
            self.dirty.store(true, Ordering::Relaxed);
        }
        result
    }

    /// Remove a document key from the index (soft-delete in sealed; hard-delete in growing).
    pub fn remove(&self, key: &[u8]) -> Result<bool> {
        let mut removed = false;

        removed |= self.mark_deleted_in_sealed(key);

        // Hard-remove from growing
        let in_growing = self.growing.remove(key)?;
        if in_growing {
            removed = true;
        }

        if removed {
            self.dirty.store(true, Ordering::Relaxed);
        }

        Ok(removed)
    }

    // ── Read operations ────────────────────────────────────────────────────────

    /// AND search across all segments.
    pub fn search_and(&self, queries: &[&str]) -> Vec<Bytes> {
        let mut result_set: HashSet<Bytes> = HashSet::new();
        for seg in &self.sealed {
            for key in seg.index.search_and(queries) {
                if !seg.is_key_deleted(&key) {
                    result_set.insert(key);
                }
            }
        }
        for key in self.growing.search_and(queries) {
            result_set.insert(key);
        }
        result_set.into_iter().collect()
    }

    /// OR scored search.
    pub fn search_or_scored(&self, queries: &[&str]) -> Vec<(Bytes, f32)> {
        let mut scores: std::collections::HashMap<Bytes, f32> = std::collections::HashMap::new();
        for seg in &self.sealed {
            for (key, score) in seg.index.search_or_scored(queries) {
                if !seg.is_key_deleted(&key) {
                    *scores.entry(key).or_insert(0.0) += score;
                }
            }
        }
        for (key, score) in self.growing.search_or_scored(queries) {
            *scores.entry(key).or_insert(0.0) += score;
        }
        let mut results: Vec<(Bytes, f32)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Total number of indexed documents (across all segments, excluding soft-deletes).
    pub fn len(&self) -> usize {
        let sealed_count: usize = self
            .sealed
            .iter()
            .map(|s| s.index.len() - s.deletion_count())
            .sum();
        sealed_count + self.growing.len()
    }

    /// Whether the index holds no live documents.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn rewrite_keys_in_dir<F>(
        config: InvertedIndexConfig,
        dir: &Path,
        mut rewrite_key: F,
    ) -> Result<bool>
    where
        F: FnMut(&Bytes) -> Result<Bytes>,
    {
        let existing = Self::load_from_dir(config.clone(), dir.to_path_buf())?;
        let mut docs: HashMap<Bytes, HashSet<String>> = HashMap::new();

        for seg in &existing.sealed {
            for key in seg.live_keys() {
                docs.entry(key.clone())
                    .or_default()
                    .extend(seg.index.get_tokens(&key));
            }
        }
        for key in key_directory(&existing.growing) {
            docs.entry(key.clone())
                .or_default()
                .extend(existing.growing.get_tokens(&key));
        }

        let tmp_dir = dir.join(".tags-rekey.tmp");
        if tmp_dir.exists() {
            std::fs::remove_dir_all(&tmp_dir)?;
        }
        std::fs::create_dir_all(&tmp_dir)?;

        let mut changed = false;
        let mut rewritten_docs: HashMap<Bytes, HashSet<String>> = HashMap::new();
        for (key, tokens) in docs {
            let rewritten = rewrite_key(&key)?;
            changed |= rewritten != key;
            rewritten_docs.entry(rewritten).or_default().extend(tokens);
        }

        if !changed {
            std::fs::remove_dir_all(&tmp_dir)?;
            return Ok(false);
        }

        let mut rebuilt = Self::new(config, tmp_dir.clone());
        for (key, tokens) in rewritten_docs {
            let mut tags: Vec<String> = tokens.into_iter().collect();
            tags.sort();
            if !tags.is_empty() {
                rebuilt.add_tags(key, &tags)?;
            }
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

    /// Convert a pre-segmented `tags.idx` in `dir` into segment files.
    ///
    /// See `SegmentedBTreeIndex::convert_legacy_file_in_dir` for why the
    /// partition migration needs this before it can rekey.
    pub(crate) fn convert_legacy_file_in_dir(
        config: InvertedIndexConfig,
        dir: &Path,
    ) -> Result<bool> {
        let legacy_path = dir.join(format!("{}.idx", Self::INDEX_NAME));
        let manifest_path = dir.join(format!("{}.manifest", Self::INDEX_NAME));
        if !legacy_path.exists() || manifest_path.exists() {
            return Ok(false);
        }

        let Ok(legacy) = InvertedIndex::load(&legacy_path) else {
            return Ok(false);
        };
        let mut keys: HashSet<Bytes> = HashSet::new();
        for token in legacy.all_tokens() {
            keys.extend(legacy.search(&token));
        }

        let mut rebuilt = Self::new(config, dir.to_path_buf());
        for key in keys {
            let tags = legacy.get_tokens(&key);
            if !tags.is_empty() {
                rebuilt.add_tags(key, &tags)?;
            }
        }
        rebuilt.seal_growing()?;
        std::fs::remove_file(&legacy_path)?;
        tracing::info!(
            "Converted legacy {:?} to segmented format before partition rekey",
            legacy_path
        );
        Ok(true)
    }

    /// Whether compaction should be triggered.
    pub fn needs_compaction(&self) -> bool {
        let total_docs: usize = self.sealed.iter().map(|s| s.doc_count() as usize).sum();
        let deleted: usize = self.sealed.iter().map(|s| s.deletion_count()).sum();
        let ratio = if total_docs == 0 {
            0.0
        } else {
            deleted as f64 / total_docs as f64
        };
        ratio > crate::engine::index::COMPACTION_DELETION_RATIO
            || self.sealed.len() > crate::engine::index::MAX_TAG_SEGMENTS
    }

    /// Compact sealed segments: merge the two smallest segments into one,
    /// dropping soft-deleted documents.
    ///
    /// Returns `true` if compaction was performed.
    pub fn compact(&mut self) -> Result<bool> {
        if self.sealed.len() < 2 {
            return Ok(false);
        }

        // Pick the two smallest segments (by entry count) as merge candidates
        let mut by_size: Vec<usize> = (0..self.sealed.len()).collect();
        by_size.sort_by_key(|&i| self.sealed[i].doc_count());

        let a_idx = by_size[0];
        let b_idx = by_size[1];
        let (a_idx, b_idx) = if a_idx < b_idx {
            (a_idx, b_idx)
        } else {
            (b_idx, a_idx)
        };

        // Build merged InvertedIndex from both segments, skipping deleted keys
        let merged_config = self.config.clone();
        let merged = InvertedIndex::new(merged_config);

        for &seg_idx in &[a_idx, b_idx] {
            let seg = &self.sealed[seg_idx];
            for key in seg.live_keys() {
                let tags = seg.index.get_tokens(&key);
                if !tags.is_empty() {
                    let _ = merged.add_tags(key, &tags);
                }
            }
        }

        // Write the merged segment to disk
        std::fs::create_dir_all(&self.dir).map_err(StorageError::Io)?;
        let seq_no = self.manifest.next_seq_no();
        let filename = format!("{}_{:04}.seg", Self::INDEX_NAME, seq_no);
        let seg_path = self.dir.join(&filename);

        let doc_start: u32 = self.sealed[..a_idx.min(b_idx)]
            .iter()
            .map(|s| s.doc_count())
            .sum();

        let mut data_buf = Vec::new();
        let keys_by_doc_id = serialize_tag_segment_v2(&merged, &mut data_buf)?;

        let entry_count = merged.len() as u32;
        let header = SegmentHeader::new(
            *b"TAGS_SEG",
            INDEX_TYPE_INVERTED,
            seq_no,
            entry_count,
            doc_start as u64,
            (doc_start + entry_count.saturating_sub(1)) as u64,
        );
        let mut writer = SegmentWriter::create(&seg_path, header)?;
        writer.write_bytes(&data_buf)?;
        let crc32 = writer.finish()?;

        let file_size = seg_path.metadata().map(|m| m.len()).unwrap_or(0);

        // Remove old seg files
        let old_filenames: Vec<String> =
            vec![self.sealed[a_idx].filename(), self.sealed[b_idx].filename()];
        for fname in &old_filenames {
            let _ = std::fs::remove_file(self.dir.join(fname));
            let del_fname = fname.replace(".seg", ".del");
            let _ = std::fs::remove_file(self.dir.join(del_fname));
        }

        // Remove old chunks from manifest
        self.manifest
            .chunks
            .retain(|c| !old_filenames.contains(&c.filename));

        // Add new merged chunk
        self.manifest.chunks.push(ChunkMeta {
            seq_no,
            filename: filename.clone(),
            entry_count,
            file_size,
            first_id: doc_start as u64,
            last_id: (doc_start + entry_count.saturating_sub(1)) as u64,
            crc32,
            sealed: true,
            has_deletions: false,
        });
        self.manifest.commit(&self.dir)?;

        // Remove old segments from in-memory vec (remove higher index first)
        self.sealed.remove(b_idx);
        self.sealed.remove(a_idx);

        // Add new merged segment
        let del_path = del_file_path(&self.dir, Self::INDEX_NAME, seq_no);
        let doc_count = merged.len();
        let doc_id_by_key = build_doc_id_by_key(&keys_by_doc_id)?;
        self.sealed.push(SealedTagSegment {
            seq_no,
            index: merged,
            keys_by_doc_id,
            doc_id_by_key,
            deleted: RwLock::new(vec![false; doc_count]),
            del_path,
            deletions_dirty: AtomicBool::new(false),
        });

        tracing::info!(
            "Tag index compaction: merged 2 segments into seq_no={} ({} docs)",
            seq_no,
            entry_count
        );
        Ok(true)
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    fn mark_deleted_in_sealed(&self, key: &[u8]) -> bool {
        let mut removed = false;
        for seg in &self.sealed {
            removed |= seg.mark_deleted(key);
        }
        if removed {
            self.dirty.store(true, Ordering::Relaxed);
        }
        removed
    }
}

// ── Serialization helpers ──────────────────────────────────────────────────────

const TAG_SEGMENT_V2_MAGIC: &[u8; 8] = b"TAGSIDX2";
const TAG_SEGMENT_V2_VERSION: u32 = 2;

/// Rewrite every legacy tag segment under `dir` into the v2 payload format.
///
/// `dir` is the shared storage index directory containing `tags.manifest`.
pub fn migrate_segments_to_v2(dir: &Path) -> Result<()> {
    let Some(mut manifest) = SegmentManifest::load(dir, SegmentedInvertedIndex::INDEX_NAME)? else {
        return Ok(());
    };

    let mut changed = false;
    for chunk in &mut manifest.chunks {
        let path = dir.join(&chunk.filename);
        let reader = SegmentReader::open(&path)?;
        if is_tag_segment_v2(reader.data()) {
            continue;
        }

        let (index, keys_by_doc_id) =
            deserialize_legacy_inverted_index_with_keys(&mut reader.data_cursor())?;
        if keys_by_doc_id.len() != chunk.entry_count as usize {
            return Err(StorageError::invalid_format(
                &path,
                format!(
                    "Legacy tag segment has {} keys; manifest records {}",
                    keys_by_doc_id.len(),
                    chunk.entry_count
                ),
            ));
        }

        let del_path = del_file_path(dir, SegmentedInvertedIndex::INDEX_NAME, chunk.seq_no);
        let deleted = SealedTagSegment::load_deletions(&del_path, keys_by_doc_id.len())?;

        let crc32 = write_v2_segment_file(
            &path,
            chunk.seq_no,
            chunk.first_id,
            chunk.last_id,
            &index,
            Some(&keys_by_doc_id),
        )?;

        if del_path.exists() {
            write_deletions(&del_path, &deleted)?;
        }
        chunk.crc32 = crc32;
        chunk.file_size = path.metadata().map(|m| m.len()).unwrap_or(0);
        chunk.entry_count = keys_by_doc_id.len() as u32;
        chunk.has_deletions = deleted.iter().any(|&d| d);
        changed = true;
    }

    if changed {
        manifest.commit(dir)?;
    }

    Ok(())
}

fn is_tag_segment_v2(data: &[u8]) -> bool {
    data.len() >= TAG_SEGMENT_V2_MAGIC.len()
        && &data[..TAG_SEGMENT_V2_MAGIC.len()] == TAG_SEGMENT_V2_MAGIC
}

fn serialize_tag_segment_v2(index: &InvertedIndex, buf: &mut Vec<u8>) -> Result<Vec<Bytes>> {
    let keys_by_doc_id = key_directory(index);
    serialize_tag_segment_v2_with_keys(index, &keys_by_doc_id, buf)?;
    Ok(keys_by_doc_id)
}

fn serialize_tag_segment_v2_with_keys(
    index: &InvertedIndex,
    keys_by_doc_id: &[Bytes],
    buf: &mut Vec<u8>,
) -> Result<()> {
    use std::io::Write as IoWrite;

    let mut legacy = Vec::new();
    serialize_inverted_index(index, &mut legacy)?;

    let mut w = std::io::BufWriter::new(buf);
    w.write_all(TAG_SEGMENT_V2_MAGIC)
        .map_err(StorageError::Io)?;
    w.write_all(&TAG_SEGMENT_V2_VERSION.to_le_bytes())
        .map_err(StorageError::Io)?;
    w.write_all(&(legacy.len() as u32).to_le_bytes())
        .map_err(StorageError::Io)?;
    w.write_all(&legacy).map_err(StorageError::Io)?;
    w.write_all(&(keys_by_doc_id.len() as u32).to_le_bytes())
        .map_err(StorageError::Io)?;
    for key in keys_by_doc_id {
        w.write_all(&(key.len() as u32).to_le_bytes())
            .map_err(StorageError::Io)?;
        w.write_all(key).map_err(StorageError::Io)?;
    }
    w.flush().map_err(StorageError::Io)?;
    Ok(())
}

fn deserialize_tag_segment_v2(data: &[u8]) -> Result<(InvertedIndex, Vec<Bytes>)> {
    if !is_tag_segment_v2(data) {
        return Err(StorageError::Serialization(
            "Legacy tag segment payload found after index.tags v2 migration".into(),
        ));
    }

    let mut cursor = Cursor::new(data);
    let mut magic = [0u8; 8];
    cursor.read_exact(&mut magic)?;

    let mut buf4 = [0u8; 4];
    cursor.read_exact(&mut buf4)?;
    let version = u32::from_le_bytes(buf4);
    if version != TAG_SEGMENT_V2_VERSION {
        return Err(StorageError::Serialization(format!(
            "Unsupported tag segment payload version: {version}"
        )));
    }

    cursor.read_exact(&mut buf4)?;
    let legacy_len = u32::from_le_bytes(buf4) as usize;
    let mut legacy = vec![0u8; legacy_len];
    cursor.read_exact(&mut legacy)?;
    let index =
        deserialize_inverted_index(&mut Cursor::new(&legacy), &InvertedIndexConfig::default())?;

    cursor.read_exact(&mut buf4)?;
    let key_count = u32::from_le_bytes(buf4) as usize;
    let mut keys_by_doc_id = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        cursor.read_exact(&mut buf4)?;
        let key_len = u32::from_le_bytes(buf4) as usize;
        let mut key = vec![0u8; key_len];
        cursor.read_exact(&mut key)?;
        keys_by_doc_id.push(Bytes::from(key));
    }

    if cursor.position() != data.len() as u64 {
        return Err(StorageError::Serialization(
            "Trailing bytes in tag segment payload".into(),
        ));
    }
    if keys_by_doc_id.len() != index.len() {
        return Err(StorageError::Serialization(format!(
            "Tag segment key directory has {} keys; inverted index has {} docs",
            keys_by_doc_id.len(),
            index.len()
        )));
    }
    Ok((index, keys_by_doc_id))
}

fn key_directory(index: &InvertedIndex) -> Vec<Bytes> {
    let mut key_set: HashSet<Bytes> = HashSet::new();
    for token in index.all_tokens() {
        for key in index.search(&token) {
            key_set.insert(key);
        }
    }
    let mut keys: Vec<Bytes> = key_set.into_iter().collect();
    keys.sort();
    keys
}

fn build_doc_id_by_key(keys_by_doc_id: &[Bytes]) -> Result<HashMap<Bytes, usize>> {
    let mut doc_id_by_key = HashMap::with_capacity(keys_by_doc_id.len());
    for (doc_id, key) in keys_by_doc_id.iter().cloned().enumerate() {
        if doc_id_by_key.insert(key.clone(), doc_id).is_some() {
            return Err(StorageError::Serialization(format!(
                "Duplicate key in tag segment directory: {:?}",
                key
            )));
        }
    }
    Ok(doc_id_by_key)
}

fn write_v2_segment_file(
    path: &Path,
    seq_no: u32,
    first_id: u64,
    last_id: u64,
    index: &InvertedIndex,
    keys_by_doc_id: Option<&[Bytes]>,
) -> Result<u32> {
    let keys;
    let keys_by_doc_id = match keys_by_doc_id {
        Some(keys_by_doc_id) => keys_by_doc_id,
        None => {
            keys = key_directory(index);
            &keys
        }
    };

    let mut data_buf = Vec::new();
    serialize_tag_segment_v2_with_keys(index, keys_by_doc_id, &mut data_buf)?;
    let header = SegmentHeader::new(
        *b"TAGS_SEG",
        INDEX_TYPE_INVERTED,
        seq_no,
        keys_by_doc_id.len() as u32,
        first_id,
        last_id,
    );
    let mut writer = SegmentWriter::create(path, header)?;
    writer.write_bytes(&data_buf)?;
    writer.finish()
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

/// Serialize an `InvertedIndex` to a byte buffer using the legacy wire format.
fn serialize_inverted_index(index: &InvertedIndex, buf: &mut Vec<u8>) -> Result<()> {
    use std::io::Write as IoWrite;
    let mut w = std::io::BufWriter::new(buf);

    // Snapshot
    let (index_snap, ktt_snap) = {
        // We use add_tags / set_tags — snapshot via all_tokens + get_tokens
        let tokens = index.all_tokens();
        let mut idx: Vec<(String, Vec<(Bytes, f32)>)> = Vec::new();
        for token in &tokens {
            let scored = index.search_scored(token);
            idx.push((token.clone(), scored));
        }

        // For ktt_snap, we need keys. Collect from search results.
        let mut key_set: HashSet<Bytes> = HashSet::new();
        for (_, postings) in &idx {
            for (k, _) in postings {
                key_set.insert(k.clone());
            }
        }
        let ktt: Vec<(Bytes, Vec<String>)> = key_set
            .into_iter()
            .map(|k| {
                let ts = index.get_tokens(&k);
                (k, ts)
            })
            .collect();

        (idx, ktt)
    };

    // Write header
    w.write_all(b"INVI").map_err(StorageError::Io)?;
    w.write_all(&1u32.to_le_bytes()).map_err(StorageError::Io)?;

    // Write config (placeholder — use defaults when loading)
    w.write_all(&[1u8]).map_err(StorageError::Io)?; // lowercase = true
    w.write_all(&1u32.to_le_bytes()).map_err(StorageError::Io)?; // min_token_length
    w.write_all(&100u32.to_le_bytes())
        .map_err(StorageError::Io)?; // max_token_length
    let sep = " \t\n\r,.;:!?()[]{}\"'`~@#$%^&*-+=<>/\\|";
    let sep_bytes = sep.as_bytes();
    w.write_all(&(sep_bytes.len() as u32).to_le_bytes())
        .map_err(StorageError::Io)?;
    w.write_all(sep_bytes).map_err(StorageError::Io)?;

    // Write index
    w.write_all(&(index_snap.len() as u32).to_le_bytes())
        .map_err(StorageError::Io)?;
    for (token, postings) in &index_snap {
        let token_bytes = token.as_bytes();
        w.write_all(&(token_bytes.len() as u32).to_le_bytes())
            .map_err(StorageError::Io)?;
        w.write_all(token_bytes).map_err(StorageError::Io)?;
        w.write_all(&(postings.len() as u32).to_le_bytes())
            .map_err(StorageError::Io)?;
        for (key, score) in postings {
            w.write_all(&(key.len() as u32).to_le_bytes())
                .map_err(StorageError::Io)?;
            w.write_all(key).map_err(StorageError::Io)?;
            w.write_all(&score.to_le_bytes())
                .map_err(StorageError::Io)?;
        }
    }

    // Write ktt
    w.write_all(&(ktt_snap.len() as u32).to_le_bytes())
        .map_err(StorageError::Io)?;
    for (key, tokens) in &ktt_snap {
        w.write_all(&(key.len() as u32).to_le_bytes())
            .map_err(StorageError::Io)?;
        w.write_all(key).map_err(StorageError::Io)?;
        w.write_all(&(tokens.len() as u32).to_le_bytes())
            .map_err(StorageError::Io)?;
        for token in tokens {
            let token_bytes = token.as_bytes();
            w.write_all(&(token_bytes.len() as u32).to_le_bytes())
                .map_err(StorageError::Io)?;
            w.write_all(token_bytes).map_err(StorageError::Io)?;
        }
    }

    w.flush().map_err(StorageError::Io)?;
    Ok(())
}

/// Deserialize an `InvertedIndex` from a cursor using the legacy wire format.
fn deserialize_inverted_index(
    cursor: &mut impl Read,
    _config: &InvertedIndexConfig,
) -> Result<InvertedIndex> {
    deserialize_legacy_inverted_index_with_keys(cursor).map(|(index, _)| index)
}

fn deserialize_legacy_inverted_index_with_keys(
    cursor: &mut impl Read,
) -> Result<(InvertedIndex, Vec<Bytes>)> {
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic)?;
    if &magic != b"INVI" {
        return Err(StorageError::Serialization(
            "Invalid inverted index magic in segment".into(),
        ));
    }

    let mut buf4 = [0u8; 4];
    cursor.read_exact(&mut buf4)?;
    let version = u32::from_le_bytes(buf4);
    if version != 1 {
        return Err(StorageError::Serialization(format!(
            "Unsupported inverted index version: {version}"
        )));
    }

    // Read config
    let mut bool_byte = [0u8; 1];
    cursor.read_exact(&mut bool_byte)?;
    let lowercase = bool_byte[0] != 0;

    cursor.read_exact(&mut buf4)?;
    let min_token_length = u32::from_le_bytes(buf4) as usize;

    cursor.read_exact(&mut buf4)?;
    let max_token_length = u32::from_le_bytes(buf4) as usize;

    // `token_separators` was dropped from `InvertedIndexConfig` in REM-36 (dead
    // field once `tokenize()`/`index_text()` were deleted as dead code); the
    // length-prefixed separator string written by `serialize_inverted_index`
    // below is still consumed here to keep the cursor aligned, same treatment
    // as sub-task 6b's `BTIX` field-skip handling.
    cursor.read_exact(&mut buf4)?;
    let sep_len = u32::from_le_bytes(buf4) as usize;
    let mut sep_bytes = vec![0u8; sep_len];
    cursor.read_exact(&mut sep_bytes)?;
    drop(sep_bytes);

    let cfg = InvertedIndexConfig {
        lowercase,
        min_token_length,
        max_token_length,
    };

    // Read token count
    cursor.read_exact(&mut buf4)?;
    let token_count = u32::from_le_bytes(buf4) as usize;

    let index = InvertedIndex::new(cfg);

    for _ in 0..token_count {
        cursor.read_exact(&mut buf4)?;
        let token_len = u32::from_le_bytes(buf4) as usize;
        let mut token_bytes = vec![0u8; token_len];
        cursor.read_exact(&mut token_bytes)?;
        let _token = String::from_utf8(token_bytes)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        cursor.read_exact(&mut buf4)?;
        let posting_count = u32::from_le_bytes(buf4) as usize;

        for _ in 0..posting_count {
            cursor.read_exact(&mut buf4)?;
            let key_len = u32::from_le_bytes(buf4) as usize;
            let mut key_bytes = vec![0u8; key_len];
            cursor.read_exact(&mut key_bytes)?;
            let _key = Bytes::from(key_bytes);

            cursor.read_exact(&mut buf4)?;
            let _score = f32::from_le_bytes(buf4);
        }
    }

    // Read ktt
    cursor.read_exact(&mut buf4)?;
    let doc_count = u32::from_le_bytes(buf4) as usize;
    let mut keys_by_doc_id = Vec::with_capacity(doc_count);

    for _ in 0..doc_count {
        cursor.read_exact(&mut buf4)?;
        let key_len = u32::from_le_bytes(buf4) as usize;
        let mut key_bytes = vec![0u8; key_len];
        cursor.read_exact(&mut key_bytes)?;
        let key = Bytes::from(key_bytes);

        cursor.read_exact(&mut buf4)?;
        let token_count2 = u32::from_le_bytes(buf4) as usize;

        let mut tokens = Vec::with_capacity(token_count2);
        for _ in 0..token_count2 {
            cursor.read_exact(&mut buf4)?;
            let token_len = u32::from_le_bytes(buf4) as usize;
            let mut token_bytes = vec![0u8; token_len];
            cursor.read_exact(&mut token_bytes)?;
            let token = String::from_utf8(token_bytes)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            tokens.push(token);
        }

        // Re-add the key with its tokens
        if !tokens.is_empty() {
            let _ = index.add_tags(key.clone(), &tokens);
            keys_by_doc_id.push(key);
        }
    }

    Ok((index, keys_by_doc_id))
}

fn del_file_path(dir: &Path, index_name: &str, seq_no: u32) -> PathBuf {
    dir.join(format!("{index_name}_{seq_no:04}.del"))
}
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_basic_add_and_search() {
        let dir = tempdir().unwrap();
        let idx =
            SegmentedInvertedIndex::new(InvertedIndexConfig::default(), dir.path().to_path_buf());

        idx.add_tags(
            b"doc1".to_vec(),
            &["rust".to_string(), "programming".to_string()],
        )
        .unwrap();
        idx.add_tags(b"doc2".to_vec(), &["rust".to_string()])
            .unwrap();

        let and_results = idx.search_and(&["rust", "programming"]);
        assert_eq!(and_results.len(), 1);
    }

    #[test]
    fn test_seal_and_reload() {
        let dir = tempdir().unwrap();

        let mut idx =
            SegmentedInvertedIndex::new(InvertedIndexConfig::default(), dir.path().to_path_buf());

        for i in 0..5 {
            idx.add_tags(
                format!("doc{i}"),
                &[format!("tag{i}"), "common".to_string()],
            )
            .unwrap();
        }

        idx.seal_growing().unwrap();
        assert_eq!(idx.sealed.len(), 1);

        // Add more to growing
        idx.add_tags(b"doc100".to_vec(), &["new_tag".to_string()])
            .unwrap();

        // Reload from disk
        let reloaded = SegmentedInvertedIndex::load_from_dir(
            InvertedIndexConfig::default(),
            dir.path().to_path_buf(),
        )
        .unwrap();

        assert_eq!(reloaded.sealed.len(), 1);
        assert_eq!(reloaded.search_and(&["common"]).len(), 5);
    }

    #[test]
    fn test_remove() {
        let dir = tempdir().unwrap();
        let idx =
            SegmentedInvertedIndex::new(InvertedIndexConfig::default(), dir.path().to_path_buf());

        idx.add_tags(b"doc1".to_vec(), &["rust".to_string()])
            .unwrap();
        idx.add_tags(b"doc2".to_vec(), &["rust".to_string()])
            .unwrap();
        assert_eq!(idx.len(), 2);

        idx.remove(b"doc1").unwrap();
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn delete_after_seal_filters_exact_tag_search() {
        let dir = tempdir().unwrap();
        let mut idx =
            SegmentedInvertedIndex::new(InvertedIndexConfig::default(), dir.path().to_path_buf());

        idx.add_tags(b"doc1".to_vec(), &["rust".to_string()])
            .unwrap();
        idx.add_tags(b"doc2".to_vec(), &["rust".to_string()])
            .unwrap();
        idx.seal_growing().unwrap();

        assert!(idx.remove(b"doc1").unwrap());
        let results = idx.search_and(&["rust"]);

        assert_eq!(results, vec![Bytes::from_static(b"doc2")]);
    }

    #[test]
    fn retag_after_seal_removes_old_tags_and_adds_new_tags() {
        let dir = tempdir().unwrap();
        let mut idx =
            SegmentedInvertedIndex::new(InvertedIndexConfig::default(), dir.path().to_path_buf());

        idx.set_tags(b"doc1".to_vec(), &["old".to_string()])
            .unwrap();
        idx.seal_growing().unwrap();

        idx.set_tags(b"doc1".to_vec(), &["new".to_string()])
            .unwrap();

        assert!(idx.search_and(&["old"]).is_empty());
        assert_eq!(idx.search_and(&["new"]), vec![Bytes::from_static(b"doc1")]);
    }

    #[test]
    fn scored_search_skips_deleted_sealed_postings() {
        let dir = tempdir().unwrap();
        let mut idx =
            SegmentedInvertedIndex::new(InvertedIndexConfig::default(), dir.path().to_path_buf());

        idx.add_tags(
            b"doc1".to_vec(),
            &["rust".to_string(), "storage".to_string()],
        )
        .unwrap();
        idx.add_tags(b"doc2".to_vec(), &["rust".to_string()])
            .unwrap();
        idx.seal_growing().unwrap();

        idx.remove(b"doc1").unwrap();
        let results = idx.search_or_scored(&["rust", "storage"]);

        assert_eq!(results, vec![(Bytes::from_static(b"doc2"), 1.0)]);
    }

    #[test]
    fn len_and_compaction_reflect_sealed_deletions() {
        let dir = tempdir().unwrap();
        let mut idx =
            SegmentedInvertedIndex::new(InvertedIndexConfig::default(), dir.path().to_path_buf());

        for i in 0..5 {
            idx.add_tags(format!("doc{i}"), &["rust".to_string()])
                .unwrap();
        }
        idx.seal_growing().unwrap();

        idx.remove(b"doc0").unwrap();
        idx.remove(b"doc1").unwrap();

        assert_eq!(idx.len(), 3);
        assert!(idx.needs_compaction());
    }

    #[test]
    fn compact_and_reload_do_not_resurrect_deleted_or_retagged_postings() {
        let dir = tempdir().unwrap();
        let mut idx =
            SegmentedInvertedIndex::new(InvertedIndexConfig::default(), dir.path().to_path_buf());

        idx.set_tags(b"deleted".to_vec(), &["stale".to_string()])
            .unwrap();
        idx.set_tags(b"retagged".to_vec(), &["old".to_string()])
            .unwrap();
        idx.seal_growing().unwrap();

        idx.remove(b"deleted").unwrap();
        idx.set_tags(b"retagged".to_vec(), &["new".to_string()])
            .unwrap();
        idx.set_tags(b"live".to_vec(), &["fresh".to_string()])
            .unwrap();
        idx.seal_growing().unwrap();
        assert!(idx.compact().unwrap());

        let reloaded = SegmentedInvertedIndex::load_from_dir(
            InvertedIndexConfig::default(),
            dir.path().to_path_buf(),
        )
        .unwrap();

        assert!(reloaded.search_and(&["stale"]).is_empty());
        assert!(reloaded.search_and(&["old"]).is_empty());
        assert_eq!(
            reloaded.search_and(&["new"]),
            vec![Bytes::from_static(b"retagged")]
        );
        assert_eq!(
            reloaded.search_and(&["fresh"]),
            vec![Bytes::from_static(b"live")]
        );
    }

    #[test]
    fn legacy_v1_segments_migrate_to_v2_without_losing_live_tags() {
        let dir = tempdir().unwrap();
        let index = InvertedIndex::new(InvertedIndexConfig::default());
        index
            .set_tags(b"doc1".to_vec(), &["rust".to_string()])
            .unwrap();
        index
            .set_tags(b"doc2".to_vec(), &["storage".to_string()])
            .unwrap();

        let mut legacy_data = Vec::new();
        serialize_inverted_index(&index, &mut legacy_data).unwrap();
        let header = SegmentHeader::new(*b"TAGS_SEG", INDEX_TYPE_INVERTED, 0, 2, 0, 1);
        let seg_path = dir.path().join("tags_0000.seg");
        let mut writer = SegmentWriter::create(&seg_path, header).unwrap();
        writer.write_bytes(&legacy_data).unwrap();
        let crc32 = writer.finish().unwrap();

        let mut manifest = SegmentManifest::new(SegmentedInvertedIndex::INDEX_NAME);
        manifest.chunks.push(ChunkMeta {
            seq_no: 0,
            filename: "tags_0000.seg".into(),
            entry_count: 2,
            file_size: seg_path.metadata().unwrap().len(),
            first_id: 0,
            last_id: 1,
            crc32,
            sealed: true,
            has_deletions: false,
        });
        manifest.commit(dir.path()).unwrap();

        migrate_segments_to_v2(dir.path()).unwrap();

        let reloaded = SegmentedInvertedIndex::load_from_dir(
            InvertedIndexConfig::default(),
            dir.path().to_path_buf(),
        )
        .unwrap();
        assert_eq!(
            reloaded.search_and(&["rust"]),
            vec![Bytes::from_static(b"doc1")]
        );
        assert_eq!(
            reloaded.search_and(&["storage"]),
            vec![Bytes::from_static(b"doc2")]
        );
        assert!(is_tag_segment_v2(
            SegmentReader::open(&seg_path).unwrap().data()
        ));
    }
}
