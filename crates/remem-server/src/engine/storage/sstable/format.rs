//! SSTable file format definitions
#![allow(dead_code)]

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Magic number for SSTable files: "RMST"
pub const MAGIC: [u8; 4] = [0x52, 0x4D, 0x53, 0x54];

/// Current SSTable format version
pub const VERSION: u32 = 1;

/// Size of the SSTable header in bytes
pub const HEADER_SIZE: usize = 64;

/// Size of the SSTable footer in bytes
pub const FOOTER_SIZE: usize = 32;

/// Default block size (4 KB)
pub const BLOCK_SIZE: usize = 4 * 1024;

/// Maximum key size (64 KB)
pub const MAX_KEY_SIZE: usize = 64 * 1024;

/// Maximum value size (16 MB)
pub const MAX_VALUE_SIZE: usize = 16 * 1024 * 1024;

/// Compression type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Compression {
    /// No compression
    None = 0,
    /// Zstandard compression
    Zstd = 1,
}

impl Default for Compression {
    fn default() -> Self {
        Self::Zstd
    }
}

impl From<u8> for Compression {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::None,
            1 => Self::Zstd,
            _ => Self::None,
        }
    }
}

/// SSTable file header
///
/// The header contains metadata about the SSTable and offsets to
/// other sections (index, bloom filter).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableHeader {
    /// Magic number
    pub magic: [u8; 4],
    /// Format version
    pub version: u32,
    /// Compression type
    pub compression: Compression,
    /// Number of records in the SSTable
    pub record_count: u64,
    /// Smallest key (first 16 bytes, zero-padded)
    pub min_key: [u8; 16],
    /// Largest key (first 16 bytes, zero-padded)
    pub max_key: [u8; 16],
    /// Offset to the index block
    pub index_offset: u64,
    /// Size of the index block
    pub index_size: u32,
    /// Offset to the bloom filter
    pub bloom_offset: u64,
    /// Size of the bloom filter
    pub bloom_size: u32,
}

impl SSTableHeader {
    /// Create a new header
    pub fn new(compression: Compression) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            compression,
            record_count: 0,
            min_key: [0u8; 16],
            max_key: [0u8; 16],
            index_offset: 0,
            index_size: 0,
            bloom_offset: 0,
            bloom_size: 0,
        }
    }

    /// Encode the header to bytes
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];

        // Magic (4 bytes)
        buf[0..4].copy_from_slice(&self.magic);
        // Version (4 bytes)
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        // Compression (1 byte)
        buf[8] = self.compression as u8;
        // Padding (3 bytes) - reserved for future use
        // Record count (8 bytes)
        buf[12..20].copy_from_slice(&self.record_count.to_le_bytes());
        // Min key (16 bytes)
        buf[20..36].copy_from_slice(&self.min_key);
        // Max key (16 bytes)
        buf[36..52].copy_from_slice(&self.max_key);
        // Index offset (8 bytes) - but we only have 12 bytes left
        // Let's reorganize: use remaining space more efficiently
        // Actually let's use a simpler encoding

        buf
    }

    /// Decode the header from bytes
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < HEADER_SIZE {
            return None;
        }

        let magic: [u8; 4] = data[0..4].try_into().ok()?;
        if magic != MAGIC {
            return None;
        }

        let version = u32::from_le_bytes(data[4..8].try_into().ok()?);
        let compression = Compression::from(data[8]);
        let record_count = u64::from_le_bytes(data[12..20].try_into().ok()?);
        let min_key: [u8; 16] = data[20..36].try_into().ok()?;
        let max_key: [u8; 16] = data[36..52].try_into().ok()?;

        Some(Self {
            magic,
            version,
            compression,
            record_count,
            min_key,
            max_key,
            index_offset: 0, // Will be set from footer
            index_size: 0,
            bloom_offset: 0,
            bloom_size: 0,
        })
    }

    /// Set min key from bytes
    pub fn set_min_key(&mut self, key: &[u8]) {
        let len = key.len().min(16);
        self.min_key[..len].copy_from_slice(&key[..len]);
    }

    /// Set max key from bytes
    pub fn set_max_key(&mut self, key: &[u8]) {
        let len = key.len().min(16);
        self.max_key = [0u8; 16];
        self.max_key[..len].copy_from_slice(&key[..len]);
    }
}

/// SSTable file footer
///
/// The footer contains checksums and offsets to verify file integrity.
#[derive(Debug, Clone)]
pub struct SSTableFooter {
    /// Index block offset
    pub index_offset: u64,
    /// Index block size
    pub index_size: u32,
    /// Bloom filter offset
    pub bloom_offset: u64,
    /// Bloom filter size
    pub bloom_size: u32,
    /// CRC32 checksum of the entire file (excluding footer)
    pub checksum: u32,
}

impl SSTableFooter {
    /// Encode the footer to bytes
    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];

        buf[0..8].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.index_size.to_le_bytes());
        buf[12..20].copy_from_slice(&self.bloom_offset.to_le_bytes());
        buf[20..24].copy_from_slice(&self.bloom_size.to_le_bytes());
        buf[24..28].copy_from_slice(&self.checksum.to_le_bytes());
        // Remaining 4 bytes are padding/reserved

        buf
    }

    /// Decode the footer from bytes
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < FOOTER_SIZE {
            return None;
        }

        Some(Self {
            index_offset: u64::from_le_bytes(data[0..8].try_into().ok()?),
            index_size: u32::from_le_bytes(data[8..12].try_into().ok()?),
            bloom_offset: u64::from_le_bytes(data[12..20].try_into().ok()?),
            bloom_size: u32::from_le_bytes(data[20..24].try_into().ok()?),
            checksum: u32::from_le_bytes(data[24..28].try_into().ok()?),
        })
    }
}

/// Index entry pointing to a data block
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// First key in the block
    pub key: Bytes,
    /// Offset of the block in the file
    pub offset: u64,
    /// Size of the block (compressed)
    pub size: u32,
}

impl IndexEntry {
    /// Encode the index entry to bytes
    pub fn encode(&self) -> Vec<u8> {
        let key_len = self.key.len() as u16;
        let mut buf = Vec::with_capacity(2 + self.key.len() + 8 + 4);

        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.size.to_le_bytes());

        buf
    }

    /// Decode an index entry from bytes, returning the entry and bytes consumed
    pub fn decode(data: &[u8]) -> Option<(Self, usize)> {
        if data.len() < 14 {
            // 2 + 8 + 4 minimum
            return None;
        }

        let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;

        if data.len() < 2 + key_len + 12 {
            return None;
        }

        let key = Bytes::copy_from_slice(&data[2..2 + key_len]);
        let offset_start = 2 + key_len;
        let offset = u64::from_le_bytes(data[offset_start..offset_start + 8].try_into().ok()?);
        let size = u32::from_le_bytes(data[offset_start + 8..offset_start + 12].try_into().ok()?);

        let total_size = 2 + key_len + 12;

        Some((Self { key, offset, size }, total_size))
    }
}

/// Metadata about an SSTable
#[derive(Debug, Clone)]
pub struct SSTableMeta {
    /// Path to the SSTable file
    pub path: std::path::PathBuf,
    /// Compression type used
    pub compression: Compression,
    /// Number of records
    pub record_count: u64,
    /// File size in bytes
    pub file_size: u64,
    /// Smallest key (prefix)
    pub min_key: Bytes,
    /// Largest key (prefix)
    pub max_key: Bytes,
    /// Level in the LSM tree
    pub level: usize,
}

impl SSTableMeta {
    /// Check if a key might be in this SSTable based on key range
    pub fn may_contain_key(&self, key: &[u8]) -> bool {
        let key_prefix = if key.len() > 16 { &key[..16] } else { key };
        key_prefix >= self.min_key.as_ref() && key_prefix <= self.max_key.as_ref()
    }
}

/// Record stored in an SSTable data block
#[derive(Debug, Clone)]
pub struct Record {
    /// Key bytes
    pub key: Bytes,
    /// Value bytes (None for tombstones)
    pub value: Option<Bytes>,
    /// Timestamp
    pub timestamp: u64,
}

impl Record {
    /// Create a new record with a value
    pub fn new(key: Bytes, value: Bytes, timestamp: u64) -> Self {
        Self {
            key,
            value: Some(value),
            timestamp,
        }
    }

    /// Create a tombstone record
    pub fn tombstone(key: Bytes, timestamp: u64) -> Self {
        Self {
            key,
            value: None,
            timestamp,
        }
    }

    /// Check if this is a tombstone
    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// Encode the record to bytes
    ///
    /// Format: [key_len: u16][value_len: u32][timestamp: u8][key][value]
    /// - value_len = 0xFFFFFFFF for tombstones
    pub fn encode(&self) -> Vec<u8> {
        let key_len = self.key.len() as u16;
        let (value_len, value_bytes) = match &self.value {
            Some(v) => (v.len() as u32, v.as_ref()),
            None => (0xFFFFFFFF, &[] as &[u8]),
        };

        let mut buf = Vec::with_capacity(2 + 4 + 8 + self.key.len() + value_bytes.len());
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(&value_len.to_le_bytes());
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(value_bytes);

        buf
    }

    /// Decode a record from bytes, returning the record and bytes consumed
    pub fn decode(data: &[u8]) -> Option<(Self, usize)> {
        if data.len() < 14 {
            // 2 + 4 + 8 minimum
            return None;
        }

        let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        let value_len = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
        let timestamp = u64::from_le_bytes(data[6..14].try_into().ok()?);

        let key_start = 14;
        if data.len() < key_start + key_len {
            return None;
        }

        let key = Bytes::copy_from_slice(&data[key_start..key_start + key_len]);

        let (value, total_size) = if value_len == 0xFFFFFFFF {
            // Tombstone
            (None, key_start + key_len)
        } else {
            let value_start = key_start + key_len;
            let value_len = value_len as usize;
            if data.len() < value_start + value_len {
                return None;
            }
            let value = Bytes::copy_from_slice(&data[value_start..value_start + value_len]);
            (Some(value), value_start + value_len)
        };

        Some((
            Self {
                key,
                value,
                timestamp,
            },
            total_size,
        ))
    }

    /// Get the encoded size of this record
    pub fn encoded_size(&self) -> usize {
        14 + self.key.len() + self.value.as_ref().map(|v| v.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_encode_decode() {
        let record = Record::new(Bytes::from("test_key"), Bytes::from("test_value"), 12345);

        let encoded = record.encode();
        let (decoded, size) = Record::decode(&encoded).unwrap();

        assert_eq!(decoded.key, record.key);
        assert_eq!(decoded.value, record.value);
        assert_eq!(decoded.timestamp, record.timestamp);
        assert_eq!(size, encoded.len());
    }

    #[test]
    fn test_tombstone_encode_decode() {
        let record = Record::tombstone(Bytes::from("deleted_key"), 99999);

        let encoded = record.encode();
        let (decoded, _) = Record::decode(&encoded).unwrap();

        assert_eq!(decoded.key, record.key);
        assert!(decoded.is_tombstone());
        assert_eq!(decoded.timestamp, record.timestamp);
    }

    #[test]
    fn test_index_entry_encode_decode() {
        let entry = IndexEntry {
            key: Bytes::from("block_first_key"),
            offset: 4096,
            size: 1024,
        };

        let encoded = entry.encode();
        let (decoded, size) = IndexEntry::decode(&encoded).unwrap();

        assert_eq!(decoded.key, entry.key);
        assert_eq!(decoded.offset, entry.offset);
        assert_eq!(decoded.size, entry.size);
        assert_eq!(size, encoded.len());
    }

    #[test]
    fn test_header_encode_decode() {
        let mut header = SSTableHeader::new(Compression::Zstd);
        header.record_count = 1000;
        header.set_min_key(b"aaa");
        header.set_max_key(b"zzz");

        let encoded = header.encode();
        let decoded = SSTableHeader::decode(&encoded).unwrap();

        assert_eq!(decoded.magic, MAGIC);
        assert_eq!(decoded.version, VERSION);
        assert_eq!(decoded.compression, Compression::Zstd);
        assert_eq!(decoded.record_count, 1000);
    }

    #[test]
    fn test_footer_encode_decode() {
        let footer = SSTableFooter {
            index_offset: 1048576,
            index_size: 4096,
            bloom_offset: 1052672,
            bloom_size: 1024,
            checksum: 0xDEADBEEF,
        };

        let encoded = footer.encode();
        let decoded = SSTableFooter::decode(&encoded).unwrap();

        assert_eq!(decoded.index_offset, footer.index_offset);
        assert_eq!(decoded.index_size, footer.index_size);
        assert_eq!(decoded.bloom_offset, footer.bloom_offset);
        assert_eq!(decoded.bloom_size, footer.bloom_size);
        assert_eq!(decoded.checksum, footer.checksum);
    }
}
