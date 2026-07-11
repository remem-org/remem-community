//! Low-level segment file I/O with header, data, and CRC32 footer.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::engine::error::{Result, StorageError};

// ──────────────────────────────────────────────────────────────────────────────
// Index type constants
// ──────────────────────────────────────────────────────────────────────────────

pub const INDEX_TYPE_HNSW: u8 = 0;
pub const INDEX_TYPE_GRAPH: u8 = 1;
pub const INDEX_TYPE_BTREE: u8 = 2;
pub const INDEX_TYPE_INVERTED: u8 = 3;

// ──────────────────────────────────────────────────────────────────────────────
// Header / Footer
// ──────────────────────────────────────────────────────────────────────────────

/// Fixed-size 36-byte segment file header.
#[derive(Debug, Clone)]
pub struct SegmentHeader {
    /// 8-byte magic e.g. b"HNSW_SEG"
    pub magic: [u8; 8],
    pub version: u16,
    pub index_type: u8,
    /// bit 0 = sealed
    pub flags: u8,
    pub seq_no: u32,
    pub entry_count: u32,
    pub first_id: u64,
    pub last_id: u64,
    // 8+2+1+1+4+4+8+8 = 36 bytes total
}

impl SegmentHeader {
    pub fn new(
        magic: [u8; 8],
        index_type: u8,
        seq_no: u32,
        entry_count: u32,
        first_id: u64,
        last_id: u64,
    ) -> Self {
        Self {
            magic,
            version: 1,
            index_type,
            flags: 0x01, // sealed by default (we only write sealed chunks)
            seq_no,
            entry_count,
            first_id,
            last_id,
        }
    }

    pub fn write_to(&self, w: &mut impl Write) -> Result<()> {
        w.write_all(&self.magic)?;
        w.write_all(&self.version.to_le_bytes())?;
        w.write_all(&[self.index_type])?;
        w.write_all(&[self.flags])?;
        w.write_all(&self.seq_no.to_le_bytes())?;
        w.write_all(&self.entry_count.to_le_bytes())?;
        w.write_all(&self.first_id.to_le_bytes())?;
        w.write_all(&self.last_id.to_le_bytes())?;
        Ok(())
    }

    pub fn read_from(r: &mut impl Read) -> Result<Self> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;

        let mut buf2 = [0u8; 2];
        r.read_exact(&mut buf2)?;
        let version = u16::from_le_bytes(buf2);

        let mut buf1 = [0u8; 1];
        r.read_exact(&mut buf1)?;
        let index_type = buf1[0];

        r.read_exact(&mut buf1)?;
        let flags = buf1[0];

        let mut buf4 = [0u8; 4];
        r.read_exact(&mut buf4)?;
        let seq_no = u32::from_le_bytes(buf4);

        r.read_exact(&mut buf4)?;
        let entry_count = u32::from_le_bytes(buf4);

        let mut buf8 = [0u8; 8];
        r.read_exact(&mut buf8)?;
        let first_id = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf8)?;
        let last_id = u64::from_le_bytes(buf8);

        Ok(Self {
            magic,
            version,
            index_type,
            flags,
            seq_no,
            entry_count,
            first_id,
            last_id,
        })
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Writer
// ──────────────────────────────────────────────────────────────────────────────

/// Writes a segment file: header → data → footer (CRC32).
///
/// Usage:
/// ```ignore
/// let mut w = SegmentWriter::create(&path, header)?;
/// w.write_all(&some_bytes)?;
/// let crc32 = w.finish()?;
/// ```
pub struct SegmentWriter {
    inner: std::io::BufWriter<std::fs::File>,
    hasher: crc32fast::Hasher,
    bytes_written: u64,
    tmp_path: PathBuf,
    final_path: PathBuf,
}

impl SegmentWriter {
    /// Create a new segment file at `path` (writes to `path.tmp` first).
    pub fn create(path: &Path, header: SegmentHeader) -> Result<Self> {
        let tmp_path = path.with_extension("seg.tmp");
        let file = std::fs::File::create(&tmp_path).map_err(StorageError::Io)?;
        let mut inner = std::io::BufWriter::new(file);
        header.write_to(&mut inner)?;
        Ok(Self {
            inner,
            hasher: crc32fast::Hasher::new(),
            bytes_written: 0,
            tmp_path,
            final_path: path.to_path_buf(),
        })
    }

    /// Write data bytes (included in CRC32 computation).
    pub fn write_bytes(&mut self, data: &[u8]) -> Result<()> {
        self.inner.write_all(data)?;
        self.hasher.update(data);
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    /// Flush, write footer (data_len + CRC32), rename tmp → final path.
    /// Returns the CRC32 checksum for storing in the manifest.
    pub fn finish(mut self) -> Result<u32> {
        let crc32 = self.hasher.finalize();
        // Footer: data_len (u64) + crc32 (u32)
        self.inner.write_all(&self.bytes_written.to_le_bytes())?;
        self.inner.write_all(&crc32.to_le_bytes())?;
        self.inner.flush()?;
        drop(self.inner);
        std::fs::rename(&self.tmp_path, &self.final_path)?;
        Ok(crc32)
    }
}

impl std::io::Write for SegmentWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.bytes_written += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Reader
// ──────────────────────────────────────────────────────────────────────────────

/// Reads a segment file and verifies its CRC32 footer.
#[derive(Debug)]
pub struct SegmentReader {
    pub header: SegmentHeader,
    data: Vec<u8>,
}

impl SegmentReader {
    /// Open and fully load a segment file, verifying the CRC32.
    pub fn open(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path).map_err(StorageError::Io)?;
        let mut r = std::io::BufReader::new(file);

        let header = SegmentHeader::read_from(&mut r)?;

        // Read remaining bytes (DATA + footer) all at once.
        let mut remaining = Vec::new();
        r.read_to_end(&mut remaining)?;

        if remaining.len() < 12 {
            return Err(StorageError::invalid_format(path, "Segment file too short"));
        }

        let footer_start = remaining.len() - 12;
        let data = remaining[..footer_start].to_vec();
        let footer = &remaining[footer_start..];

        let _data_len = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let stored_crc32 = u32::from_le_bytes(footer[8..12].try_into().unwrap());

        let computed = crc32fast::hash(&data);
        if computed != stored_crc32 {
            return Err(StorageError::invalid_format(
                path,
                format!(
                    "CRC32 mismatch: stored {stored_crc32:#010x}, computed {computed:#010x}"
                ),
            ));
        }

        Ok(Self { header, data })
    }

    /// Return a cursor over the data section for deserialization.
    pub fn data_cursor(&self) -> std::io::Cursor<&[u8]> {
        std::io::Cursor::new(&self.data)
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_header() -> SegmentHeader {
        SegmentHeader::new(*b"TEST_SEG", INDEX_TYPE_HNSW, 0, 42, 0, 41)
    }

    #[test]
    fn test_write_read_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_0000.seg");

        let mut w = SegmentWriter::create(&path, test_header()).unwrap();
        w.write_bytes(b"hello world").unwrap();
        let crc32 = w.finish().unwrap();
        assert_ne!(crc32, 0);

        let r = SegmentReader::open(&path).unwrap();
        assert_eq!(r.header.seq_no, 0);
        assert_eq!(r.header.entry_count, 42);
        assert_eq!(r.data(), b"hello world");
    }

    #[test]
    fn test_crc32_mismatch_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bad_0000.seg");

        let mut w = SegmentWriter::create(&path, test_header()).unwrap();
        w.write_bytes(b"original data").unwrap();
        w.finish().unwrap();

        // Corrupt the data section
        let mut bytes = std::fs::read(&path).unwrap();
        // flip a byte in the data section (after the header, before the footer)
        let header_size = 8 + 2 + 1 + 1 + 4 + 4 + 8 + 8; // = 36
        bytes[header_size] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let result = SegmentReader::open(&path);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("CRC32"), "Expected CRC32 error, got: {err}");
    }

    #[test]
    fn test_empty_data_section() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty_0000.seg");

        let w = SegmentWriter::create(&path, test_header()).unwrap();
        w.finish().unwrap();

        let r = SegmentReader::open(&path).unwrap();
        assert!(r.data().is_empty());
    }
}
