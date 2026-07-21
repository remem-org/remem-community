//! Segment manifest — tracks all sealed chunk files for one index.
//!
//! The manifest is written atomically via tmp+rename. The `generation` field
//! inside the file is the authoritative version counter.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::engine::error::{Result, StorageError};
use crate::engine::storage::durable_rename::durable_rename;

/// Metadata for one sealed chunk file.
#[derive(Debug, Clone)]
pub struct ChunkMeta {
    pub seq_no: u32,
    pub filename: String,
    pub entry_count: u32,
    pub file_size: u64,
    /// Inclusive lower bound of the primary key range (node ID, timestamp, …).
    pub first_id: u64,
    /// Inclusive upper bound of the primary key range.
    pub last_id: u64,
    /// CRC32 of the DATA section.
    pub crc32: u32,
    pub sealed: bool,
    pub has_deletions: bool,
}

/// Point-in-time snapshot of which chunk files belong to an index.
#[derive(Debug, Clone)]
pub struct SegmentManifest {
    pub index_name: String,
    pub generation: u64,
    pub written_at_ms: u64,
    pub chunks: Vec<ChunkMeta>,
}

impl SegmentManifest {
    const MAGIC: &'static [u8; 8] = b"REMEM_MF";
    const VERSION: u16 = 1;

    /// Create an empty manifest for a new index.
    pub fn new(index_name: impl Into<String>) -> Self {
        Self {
            index_name: index_name.into(),
            generation: 0,
            written_at_ms: now_ms(),
            chunks: Vec::new(),
        }
    }

    /// Load manifest from `{dir}/{index_name}.manifest`.
    /// Returns `Ok(None)` if the file does not exist.
    pub fn load(dir: &Path, index_name: &str) -> Result<Option<Self>> {
        let path = manifest_path(dir, index_name);
        if !path.exists() {
            return Ok(None);
        }

        let file = std::fs::File::open(&path).map_err(StorageError::Io)?;
        let mut r = std::io::BufReader::new(file);

        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        if &magic != Self::MAGIC {
            return Err(StorageError::invalid_format(&path, "Invalid manifest magic"));
        }

        let mut buf2 = [0u8; 2];
        r.read_exact(&mut buf2)?;
        let version = u16::from_le_bytes(buf2);
        if version != Self::VERSION {
            return Err(StorageError::invalid_format(
                &path,
                format!("Unsupported manifest version {version}"),
            ));
        }

        let mut buf8 = [0u8; 8];
        let mut buf4 = [0u8; 4];

        r.read_exact(&mut buf8)?;
        let generation = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf8)?;
        let written_at_ms = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf2)?;
        let name_len = u16::from_le_bytes(buf2) as usize;
        let mut name_bytes = vec![0u8; name_len];
        r.read_exact(&mut name_bytes)?;
        let index_name = String::from_utf8(name_bytes)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        r.read_exact(&mut buf4)?;
        let chunk_count = u32::from_le_bytes(buf4) as usize;

        let mut chunks = Vec::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            r.read_exact(&mut buf4)?;
            let seq_no = u32::from_le_bytes(buf4);

            r.read_exact(&mut buf4)?;
            let entry_count = u32::from_le_bytes(buf4);

            r.read_exact(&mut buf8)?;
            let file_size = u64::from_le_bytes(buf8);

            r.read_exact(&mut buf8)?;
            let first_id = u64::from_le_bytes(buf8);

            r.read_exact(&mut buf8)?;
            let last_id = u64::from_le_bytes(buf8);

            r.read_exact(&mut buf4)?;
            let crc32 = u32::from_le_bytes(buf4);

            let mut flags = [0u8; 1];
            r.read_exact(&mut flags)?;
            let sealed = (flags[0] & 0x01) != 0;
            let has_deletions = (flags[0] & 0x02) != 0;

            r.read_exact(&mut buf2)?;
            let fname_len = u16::from_le_bytes(buf2) as usize;
            let mut fname_bytes = vec![0u8; fname_len];
            r.read_exact(&mut fname_bytes)?;
            let filename = String::from_utf8(fname_bytes)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            chunks.push(ChunkMeta {
                seq_no,
                filename,
                entry_count,
                file_size,
                first_id,
                last_id,
                crc32,
                sealed,
                has_deletions,
            });
        }

        Ok(Some(Self {
            index_name,
            generation,
            written_at_ms,
            chunks,
        }))
    }

    /// Save manifest atomically to `{dir}/{index_name}.manifest`.
    pub fn save(&self, dir: &Path) -> Result<()> {
        let path = manifest_path(dir, &self.index_name);
        let tmp = path.with_extension("manifest.tmp");

        let file = std::fs::File::create(&tmp).map_err(StorageError::Io)?;
        let mut w = std::io::BufWriter::new(file);

        w.write_all(Self::MAGIC)?;
        w.write_all(&Self::VERSION.to_le_bytes())?;
        w.write_all(&self.generation.to_le_bytes())?;
        w.write_all(&self.written_at_ms.to_le_bytes())?;

        let name_bytes = self.index_name.as_bytes();
        w.write_all(&(name_bytes.len() as u16).to_le_bytes())?;
        w.write_all(name_bytes)?;

        w.write_all(&(self.chunks.len() as u32).to_le_bytes())?;
        for c in &self.chunks {
            w.write_all(&c.seq_no.to_le_bytes())?;
            w.write_all(&c.entry_count.to_le_bytes())?;
            w.write_all(&c.file_size.to_le_bytes())?;
            w.write_all(&c.first_id.to_le_bytes())?;
            w.write_all(&c.last_id.to_le_bytes())?;
            w.write_all(&c.crc32.to_le_bytes())?;
            let flags: u8 = (c.sealed as u8) | ((c.has_deletions as u8) << 1);
            w.write_all(&[flags])?;
            let fname_bytes = c.filename.as_bytes();
            w.write_all(&(fname_bytes.len() as u16).to_le_bytes())?;
            w.write_all(fname_bytes)?;
        }

        w.flush()?;
        w.get_ref().sync_all()?;
        drop(w);
        durable_rename(&tmp, &path)?;
        Ok(())
    }

    /// Increment generation, update timestamp, and save.
    pub fn commit(&mut self, dir: &Path) -> Result<()> {
        self.generation += 1;
        self.written_at_ms = now_ms();
        self.save(dir)
    }

    /// Next available sequence number (max existing + 1).
    pub fn next_seq_no(&self) -> u32 {
        self.chunks.iter().map(|c| c.seq_no).max().map_or(0, |m| m + 1)
    }

    /// Chunk file path for a given `seq_no`.
    pub fn chunk_path(dir: &Path, index_name: &str, seq_no: u32) -> PathBuf {
        dir.join(format!("{index_name}_{seq_no:04}.seg"))
    }
}

fn manifest_path(dir: &Path, index_name: &str) -> PathBuf {
    dir.join(format!("{index_name}.manifest"))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_empty_manifest_round_trip() {
        let dir = tempdir().unwrap();
        let mut m = SegmentManifest::new("hnsw");
        m.commit(dir.path()).unwrap();

        let loaded = SegmentManifest::load(dir.path(), "hnsw").unwrap().unwrap();
        assert_eq!(loaded.index_name, "hnsw");
        assert_eq!(loaded.generation, 1);
        assert!(loaded.chunks.is_empty());
    }

    #[test]
    fn test_manifest_with_chunks() {
        let dir = tempdir().unwrap();
        let mut m = SegmentManifest::new("tags");
        m.chunks.push(ChunkMeta {
            seq_no: 0,
            filename: "tags_0000.seg".into(),
            entry_count: 1000,
            file_size: 65536,
            first_id: 0,
            last_id: 999,
            crc32: 0xDEADBEEF,
            sealed: true,
            has_deletions: false,
        });
        m.commit(dir.path()).unwrap();

        let loaded = SegmentManifest::load(dir.path(), "tags").unwrap().unwrap();
        assert_eq!(loaded.chunks.len(), 1);
        let c = &loaded.chunks[0];
        assert_eq!(c.seq_no, 0);
        assert_eq!(c.entry_count, 1000);
        assert_eq!(c.crc32, 0xDEADBEEF);
        assert!(c.sealed);
        assert!(!c.has_deletions);
    }

    #[test]
    fn test_missing_manifest_returns_none() {
        let dir = tempdir().unwrap();
        let result = SegmentManifest::load(dir.path(), "nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_next_seq_no() {
        let mut m = SegmentManifest::new("graph");
        assert_eq!(m.next_seq_no(), 0);
        m.chunks.push(ChunkMeta {
            seq_no: 5,
            filename: "x".into(),
            entry_count: 0,
            file_size: 0,
            first_id: 0,
            last_id: 0,
            crc32: 0,
            sealed: true,
            has_deletions: false,
        });
        assert_eq!(m.next_seq_no(), 6);
    }
}
