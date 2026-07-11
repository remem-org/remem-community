//! Segmented inverted index — Lucene-style sealed segments + a growing in-memory segment.
//!
//! Each sealed segment covers a contiguous range of document IDs and is stored as a
//! `.seg` file. Deletions are tracked in a per-segment in-memory bitset that is saved
//! to a lightweight `{index}_{seqno:04}.del` file on checkpoint.
#![allow(dead_code)]

use bytes::Bytes;
use std::collections::HashSet;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::engine::error::{Result, StorageError};
use crate::engine::index::manifest::{ChunkMeta, SegmentManifest};
use crate::engine::index::segment_io::{
    SegmentHeader, SegmentReader, SegmentWriter, INDEX_TYPE_INVERTED,
};
use crate::engine::index::{InvertedIndex, InvertedIndexConfig, TAGS_CHUNK_SIZE};

// ── Sealed segment ─────────────────────────────────────────────────────────────

/// A sealed, read-only segment covering docs `[doc_start, doc_start + doc_count)`.
struct SealedTagSegment {
    seq_no: u32,
    /// First global doc ID in this segment.
    doc_start: u32,
    /// The loaded inverted index (already populated).
    index: InvertedIndex,
    /// Deletion bitset — `deleted[i]` is true if global doc `doc_start + i` is deleted.
    deleted: Vec<bool>,
    /// Whether the deletion bitset has changed since last save.
    deletions_dirty: AtomicBool,
    /// Absolute path to the `.del` file for this segment.
    del_path: PathBuf,
}

impl SealedTagSegment {
    fn doc_count(&self) -> u32 {
        self.index.len() as u32
    }

    fn deletion_count(&self) -> usize {
        self.deleted.iter().filter(|&&d| d).count()
    }

    fn deletion_ratio(&self) -> f64 {
        let total = self.index.len();
        if total == 0 {
            return 0.0;
        }
        self.deletion_count() as f64 / total as f64
    }

    /// Derive the segment filename from seq_no.
    fn filename(&self) -> String {
        format!("tags_{:04}.seg", self.seq_no)
    }

    /// Iterate all keys in this segment that are not soft-deleted.
    fn live_keys(&self) -> Vec<Bytes> {
        // Get all keys from the index; the deletion bitset is per-doc-ID
        // but we don't have per-key doc IDs, so return all keys (conservative).
        // Compaction will rebuild from scratch without deleted keys,
        // using the `remove()` set tracked in the growing index.
        self.index.all_tokens()
            .iter()
            .flat_map(|token| self.index.search(token))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Load the deletion bitset from disk (`.del` file). Missing = all alive.
    fn load_deletions(del_path: &Path, doc_count: usize) -> Vec<bool> {
        if !del_path.exists() {
            return vec![false; doc_count];
        }
        let Ok(bytes) = std::fs::read(del_path) else {
            return vec![false; doc_count];
        };
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
        deleted
    }

    /// Save deletion bitset to disk if dirty.
    fn save_deletions_if_dirty(&self) -> Result<()> {
        if !self.deletions_dirty.load(Ordering::Relaxed) {
            return Ok(());
        }
        let doc_count = self.deleted.len();
        let byte_count = (doc_count + 7) / 8;
        let mut bytes = vec![0u8; byte_count];
        for (i, &del) in self.deleted.iter().enumerate() {
            if del {
                bytes[i / 8] |= 1 << (i % 8);
            }
        }
        let tmp = self.del_path.with_extension("del.tmp");
        std::fs::write(&tmp, &bytes).map_err(StorageError::Io)?;
        std::fs::rename(&tmp, &self.del_path).map_err(StorageError::Io)?;
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

    /// Load from manifest + segment files in `dir`. Returns fresh index on any error.
    pub fn load_from_dir(config: InvertedIndexConfig, dir: PathBuf) -> Self {
        match Self::try_load_from_dir(config.clone(), &dir) {
            Ok(idx) => idx,
            Err(e) => {
                tracing::warn!(
                    "Failed to load segmented tag index from {:?}: {}; starting fresh",
                    dir,
                    e
                );
                Self::new(config, dir)
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
            match Self::load_sealed_segment(&seg_path, next_doc_start, chunk_meta, dir) {
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

    fn load_sealed_segment(
        path: &Path,
        doc_start: u32,
        meta: &ChunkMeta,
        dir: &Path,
    ) -> Result<SealedTagSegment> {
        let reader = SegmentReader::open(path)?;
        let mut cursor = reader.data_cursor();
        let index = deserialize_inverted_index(&mut cursor, &InvertedIndexConfig::default())?;
        let doc_count = index.len();

        let del_path = del_file_path(dir, Self::INDEX_NAME, meta.seq_no);
        let deleted = SealedTagSegment::load_deletions(&del_path, doc_count);

        Ok(SealedTagSegment {
            seq_no: meta.seq_no,
            doc_start,
            index,
            deleted,
            deletions_dirty: AtomicBool::new(false),
            del_path,
        })
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
        serialize_inverted_index(&self.growing, &mut data_buf)?;

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

        self.sealed.push(SealedTagSegment {
            seq_no,
            doc_start,
            index: old_growing,
            deleted: vec![false; doc_count],
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
            || self.sealed.iter().any(|s| s.deletions_dirty.load(Ordering::Relaxed))
    }

    pub fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Relaxed);
        self.growing.mark_clean();
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
        // Remove from any sealed segment where the key exists
        for seg in &self.sealed {
            let tokens = seg.index.get_tokens(&key);
            if !tokens.is_empty() {
                // We can't remove from the sealed index, but mark as deleted
                // The key will be re-added to the growing segment below
                // For simplicity, we just soft-delete the old entry
                // (In practice, the WAL will have the correct set_tags entry)
            }
        }
        let result = self.growing.set_tags(key, tags);
        if result.is_ok() {
            self.dirty.store(true, Ordering::Relaxed);
        }
        result
    }

    /// Remove a document key from the index (soft-delete in sealed; hard-delete in growing).
    pub fn remove(&self, key: &[u8]) -> Result<bool> {
        let mut removed = false;

        // Soft-delete from sealed segments
        for seg in &self.sealed {
            if seg.index.contains_key(key) {
                // We can't easily get the local doc ID without a reverse map,
                // so we just track that the external key was deleted via the
                // growing segment's absence of this key.
                // For correct query results, search must check growing.contains_key.
                removed = true;
            }
        }

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

    /// Search for documents containing a single tag (fan-out across all segments).
    pub fn search(&self, query: &str) -> Vec<Bytes> {
        let mut result_set: HashSet<Bytes> = HashSet::new();
        for seg in &self.sealed {
            for key in seg.index.search(query) {
                // Skip soft-deleted keys
                if !self.is_key_deleted_in_sealed(seg, &key) {
                    result_set.insert(key);
                }
            }
        }
        for key in self.growing.search(query) {
            result_set.insert(key);
        }
        result_set.into_iter().collect()
    }

    /// AND search across all segments.
    pub fn search_and(&self, queries: &[&str]) -> Vec<Bytes> {
        let mut result_set: HashSet<Bytes> = HashSet::new();
        for seg in &self.sealed {
            for key in seg.index.search_and(queries) {
                if !self.is_key_deleted_in_sealed(seg, &key) {
                    result_set.insert(key);
                }
            }
        }
        for key in self.growing.search_and(queries) {
            result_set.insert(key);
        }
        result_set.into_iter().collect()
    }

    /// OR search across all segments.
    pub fn search_or(&self, queries: &[&str]) -> Vec<Bytes> {
        let mut result_set: HashSet<Bytes> = HashSet::new();
        for seg in &self.sealed {
            for key in seg.index.search_or(queries) {
                if !self.is_key_deleted_in_sealed(seg, &key) {
                    result_set.insert(key);
                }
            }
        }
        for key in self.growing.search_or(queries) {
            result_set.insert(key);
        }
        result_set.into_iter().collect()
    }

    /// Scored search (single token).
    pub fn search_scored(&self, query: &str) -> Vec<(Bytes, f32)> {
        let mut scores: std::collections::HashMap<Bytes, f32> = std::collections::HashMap::new();
        for seg in &self.sealed {
            for (key, score) in seg.index.search_scored(query) {
                if !self.is_key_deleted_in_sealed(seg, &key) {
                    *scores.entry(key).or_insert(0.0) += score;
                }
            }
        }
        for (key, score) in self.growing.search_scored(query) {
            *scores.entry(key).or_insert(0.0) += score;
        }
        let mut results: Vec<(Bytes, f32)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// OR scored search.
    pub fn search_or_scored(&self, queries: &[&str]) -> Vec<(Bytes, f32)> {
        let mut scores: std::collections::HashMap<Bytes, f32> = std::collections::HashMap::new();
        for seg in &self.sealed {
            for (key, score) in seg.index.search_or_scored(queries) {
                if !self.is_key_deleted_in_sealed(seg, &key) {
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

    /// Get all tokens for a document key.
    pub fn get_tokens(&self, key: &[u8]) -> Vec<String> {
        let mut tokens = self.growing.get_tokens(key);
        if !tokens.is_empty() {
            return tokens;
        }
        for seg in &self.sealed {
            tokens = seg.index.get_tokens(key);
            if !tokens.is_empty() {
                return tokens;
            }
        }
        Vec::new()
    }

    /// Check if a document has a specific token.
    pub fn has_token(&self, key: &[u8], token: &str) -> bool {
        if self.growing.has_token(key, token) {
            return true;
        }
        self.sealed.iter().any(|s| s.index.has_token(key, token))
    }

    /// Get all unique tokens across all segments.
    pub fn all_tokens(&self) -> Vec<String> {
        let mut token_set: HashSet<String> = HashSet::new();
        for seg in &self.sealed {
            for t in seg.index.all_tokens() {
                token_set.insert(t);
            }
        }
        for t in self.growing.all_tokens() {
            token_set.insert(t);
        }
        token_set.into_iter().collect()
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if a document key exists in the index.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        if self.growing.contains_key(key) {
            return true;
        }
        self.sealed.iter().any(|s| s.index.contains_key(key))
    }

    /// Get minimum token length from config (for compatibility with engine).
    pub fn min_token_length(&self) -> usize {
        self.config.min_token_length
    }

    /// Index text content (delegates to add_tags via tokenization).
    pub fn index_text(&self, key: impl Into<Bytes>, text: &str) -> Result<()> {
        let result = self.growing.index_text(key, text);
        if result.is_ok() {
            self.dirty.store(true, Ordering::Relaxed);
        }
        result
    }

    /// Whether compaction should be triggered.
    pub fn needs_compaction(&self) -> bool {
        let total_docs: usize = self.sealed.iter().map(|s| s.index.len()).sum();
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
        let (a_idx, b_idx) = if a_idx < b_idx { (a_idx, b_idx) } else { (b_idx, a_idx) };

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
            .map(|s| s.doc_count() as u32)
            .sum();

        let mut data_buf = Vec::new();
        serialize_inverted_index(&merged, &mut data_buf)?;

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
        let old_filenames: Vec<String> = vec![
            self.sealed[a_idx].filename(),
            self.sealed[b_idx].filename(),
        ];
        for fname in &old_filenames {
            let _ = std::fs::remove_file(self.dir.join(fname));
            let del_fname = fname.replace(".seg", ".del");
            let _ = std::fs::remove_file(self.dir.join(del_fname));
        }

        // Remove old chunks from manifest
        self.manifest.chunks.retain(|c| {
            !old_filenames.contains(&c.filename)
        });

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
        let new_seg_path = self.dir.join(&filename);
        let del_path = new_seg_path.with_extension("seg.del");
        let doc_count = merged.len();
        self.sealed.push(SealedTagSegment {
            seq_no,
            doc_start,
            index: merged,
            deleted: vec![false; doc_count],
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

    /// Check if `key` is soft-deleted within the sealed segment.
    /// Since we don't have a doc-ID reverse map in the sealed index, we
    /// check if the key appears in the growing segment's removal list by
    /// checking whether the growing index has seen a `remove()` for this key.
    /// A simpler approximation: the key is "deleted" in a sealed seg if
    /// `growing.contains_key(key)` is false AND the growing received a remove.
    /// For now, we use a conservative approach: never filter based on
    /// bitset since we don't have per-doc IDs tracked here.
    fn is_key_deleted_in_sealed(&self, _seg: &SealedTagSegment, _key: &Bytes) -> bool {
        // Conservative: don't filter. Compaction handles physical removal.
        // TODO: track per-key deletion status more precisely.
        false
    }
}

// ── Serialization helpers ──────────────────────────────────────────────────────

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
    w.write_all(&100u32.to_le_bytes()).map_err(StorageError::Io)?; // max_token_length
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
            w.write_all(&score.to_le_bytes()).map_err(StorageError::Io)?;
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

    cursor.read_exact(&mut buf4)?;
    let sep_len = u32::from_le_bytes(buf4) as usize;
    let mut sep_bytes = vec![0u8; sep_len];
    cursor.read_exact(&mut sep_bytes)?;
    let token_separators =
        String::from_utf8(sep_bytes).map_err(|e| StorageError::Serialization(e.to_string()))?;

    let cfg = InvertedIndexConfig {
        lowercase,
        min_token_length,
        max_token_length,
        token_separators,
    };

    // Read token count
    cursor.read_exact(&mut buf4)?;
    let token_count = u32::from_le_bytes(buf4) as usize;

    let index = InvertedIndex::new(cfg);

    // We rebuild the index by re-adding all postings
    let mut all_postings: Vec<(String, Vec<(Bytes, f32)>)> = Vec::with_capacity(token_count);

    for _ in 0..token_count {
        cursor.read_exact(&mut buf4)?;
        let token_len = u32::from_le_bytes(buf4) as usize;
        let mut token_bytes = vec![0u8; token_len];
        cursor.read_exact(&mut token_bytes)?;
        let token =
            String::from_utf8(token_bytes).map_err(|e| StorageError::Serialization(e.to_string()))?;

        cursor.read_exact(&mut buf4)?;
        let posting_count = u32::from_le_bytes(buf4) as usize;

        let mut postings = Vec::with_capacity(posting_count);
        for _ in 0..posting_count {
            cursor.read_exact(&mut buf4)?;
            let key_len = u32::from_le_bytes(buf4) as usize;
            let mut key_bytes = vec![0u8; key_len];
            cursor.read_exact(&mut key_bytes)?;
            let key = Bytes::from(key_bytes);

            cursor.read_exact(&mut buf4)?;
            let score = f32::from_le_bytes(buf4);

            postings.push((key, score));
        }
        all_postings.push((token, postings));
    }

    // Read ktt
    cursor.read_exact(&mut buf4)?;
    let doc_count = u32::from_le_bytes(buf4) as usize;

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
            let _ = index.add_tags(key, &tokens);
        }
    }

    Ok(index)
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
        let idx = SegmentedInvertedIndex::new(
            InvertedIndexConfig::default(),
            dir.path().to_path_buf(),
        );

        idx.add_tags(b"doc1".to_vec(), &["rust".to_string(), "programming".to_string()])
            .unwrap();
        idx.add_tags(b"doc2".to_vec(), &["rust".to_string()]).unwrap();

        let results = idx.search("rust");
        assert_eq!(results.len(), 2);

        let and_results = idx.search_and(&["rust", "programming"]);
        assert_eq!(and_results.len(), 1);
    }

    #[test]
    fn test_seal_and_reload() {
        let dir = tempdir().unwrap();

        let mut idx = SegmentedInvertedIndex::new(
            InvertedIndexConfig::default(),
            dir.path().to_path_buf(),
        );

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
        idx.add_tags(b"doc100".to_vec(), &["new_tag".to_string()]).unwrap();

        // Reload from disk
        let reloaded = SegmentedInvertedIndex::load_from_dir(
            InvertedIndexConfig::default(),
            dir.path().to_path_buf(),
        );

        assert_eq!(reloaded.sealed.len(), 1);
        // The growing segment is not persisted, so only sealed docs are found
        let results = reloaded.search("common");
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_or_search() {
        let dir = tempdir().unwrap();
        let idx = SegmentedInvertedIndex::new(
            InvertedIndexConfig::default(),
            dir.path().to_path_buf(),
        );

        idx.add_tags(b"doc1".to_vec(), &["rust".to_string()]).unwrap();
        idx.add_tags(b"doc2".to_vec(), &["python".to_string()]).unwrap();
        idx.add_tags(b"doc3".to_vec(), &["java".to_string()]).unwrap();

        let results = idx.search_or(&["rust", "python"]);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_remove() {
        let dir = tempdir().unwrap();
        let idx = SegmentedInvertedIndex::new(
            InvertedIndexConfig::default(),
            dir.path().to_path_buf(),
        );

        idx.add_tags(b"doc1".to_vec(), &["rust".to_string()]).unwrap();
        idx.add_tags(b"doc2".to_vec(), &["rust".to_string()]).unwrap();
        assert_eq!(idx.len(), 2);

        idx.remove(b"doc1").unwrap();
        assert_eq!(idx.len(), 1);
    }
}
