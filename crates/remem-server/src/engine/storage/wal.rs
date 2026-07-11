//! Write-Ahead Log (WAL) for durability
#![allow(dead_code)]
//!
//! The WAL ensures durability by logging all operations before they are
//! applied to the MemTable. In case of a crash, the WAL can be replayed
//! to recover the state of the MemTable.
//!
//! ## Record Format
//!
//! Each record in the WAL has the following format:
//! ```text
//! [length: u32][checksum: u32][type: u8][timestamp: u64][key_len: u16][key][value_len: u32][value]
//! ```
//!
//! - `length`: Total length of the record (excluding length and checksum fields)
//! - `checksum`: CRC32 checksum of the record data
//! - `type`: Operation type (1 = Insert, 2 = Delete)
//! - `timestamp`: Timestamp of the operation
//! - `key_len`: Length of the key
//! - `key`: Key bytes
//! - `value_len`: Length of the value (0 for deletes)
//! - `value`: Value bytes (empty for deletes)

use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use crate::engine::error::{Result, StorageError};

/// Operation type for WAL records
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordType {
    /// Insert or update a key-value pair
    Insert = 1,
    /// Delete a key
    Delete = 2,
    /// Insert with embedding vector (for HNSW index recovery)
    InsertWithEmbedding = 3,
    /// Set timestamp for a key (for time-series index recovery)
    SetTimestamp = 4,
    /// Add tags for a key (for tag index recovery)
    AddTags = 5,
    /// Add graph edge (for graph index recovery)
    AddEdge = 6,
    /// Replace all tags for a key (for tag index recovery — removes old tags first)
    SetTags = 7,
    /// Remove a graph edge (for graph index recovery)
    RemoveEdge = 8,
    /// Remove a key from the time-series index (for index recovery)
    RemoveTimestamp = 9,
    /// Remove all tags for a key from the tag index (for index recovery)
    RemoveTags = 10,
    /// Remove a key from the HNSW vector index (for index recovery)
    RemoveVector = 11,
}

impl TryFrom<u8> for WalRecordType {
    type Error = StorageError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Insert),
            2 => Ok(Self::Delete),
            3 => Ok(Self::InsertWithEmbedding),
            4 => Ok(Self::SetTimestamp),
            5 => Ok(Self::AddTags),
            6 => Ok(Self::AddEdge),
            7 => Ok(Self::SetTags),
            8 => Ok(Self::RemoveEdge),
            9 => Ok(Self::RemoveTimestamp),
            10 => Ok(Self::RemoveTags),
            11 => Ok(Self::RemoveVector),
            _ => Err(StorageError::InvalidArgument(format!(
                "Invalid WAL record type: {}",
                value
            ))),
        }
    }
}

/// A record in the WAL
#[derive(Debug, Clone)]
pub struct WalRecord {
    /// Type of operation
    pub record_type: WalRecordType,
    /// Timestamp of the operation (for MVCC, not time-series)
    pub timestamp: u64,
    /// Key bytes
    pub key: Bytes,
    /// Value bytes (empty for deletes)
    pub value: Bytes,
    /// Optional embedding vector (for InsertWithEmbedding records)
    pub embedding: Option<Vec<f32>>,
    /// Optional time-series timestamp (for SetTimestamp records)
    pub ts_timestamp: Option<u64>,
    /// Optional tags (for AddTags records)
    pub tags: Option<Vec<String>>,
    /// Optional edge source (for AddEdge records)
    pub edge_source: Option<Bytes>,
    /// Optional edge target (for AddEdge records)
    pub edge_target: Option<Bytes>,
    /// Optional edge type (for AddEdge records)
    pub edge_type: Option<String>,
    /// Optional edge weight (for AddEdge records)
    pub edge_weight: Option<f32>,
}

impl WalRecord {
    /// Create a new insert record
    pub fn insert(key: Bytes, value: Bytes, timestamp: u64) -> Self {
        Self {
            record_type: WalRecordType::Insert,
            timestamp,
            key,
            value,
            embedding: None,
            ts_timestamp: None,
            tags: None,
            edge_source: None,
            edge_target: None,
            edge_type: None,
            edge_weight: None,
        }
    }

    /// Create a new insert record with an embedding vector
    pub fn insert_with_embedding(
        key: Bytes,
        value: Bytes,
        timestamp: u64,
        embedding: Vec<f32>,
    ) -> Self {
        Self {
            record_type: WalRecordType::InsertWithEmbedding,
            timestamp,
            key,
            value,
            embedding: Some(embedding),
            ts_timestamp: None,
            tags: None,
            edge_source: None,
            edge_target: None,
            edge_type: None,
            edge_weight: None,
        }
    }

    /// Create a new delete record
    pub fn delete(key: Bytes, timestamp: u64) -> Self {
        Self {
            record_type: WalRecordType::Delete,
            timestamp,
            key,
            value: Bytes::new(),
            embedding: None,
            ts_timestamp: None,
            tags: None,
            edge_source: None,
            edge_target: None,
            edge_type: None,
            edge_weight: None,
        }
    }

    /// Create a new set timestamp record (for time-series index)
    pub fn set_timestamp(key: Bytes, ts_timestamp: u64, timestamp: u64) -> Self {
        Self {
            record_type: WalRecordType::SetTimestamp,
            timestamp,
            key,
            value: Bytes::new(),
            embedding: None,
            ts_timestamp: Some(ts_timestamp),
            tags: None,
            edge_source: None,
            edge_target: None,
            edge_type: None,
            edge_weight: None,
        }
    }

    /// Create a new add tags record (for tag index)
    pub fn add_tags(key: Bytes, tags: Vec<String>, timestamp: u64) -> Self {
        Self {
            record_type: WalRecordType::AddTags,
            timestamp,
            key,
            value: Bytes::new(),
            embedding: None,
            ts_timestamp: None,
            tags: Some(tags),
            edge_source: None,
            edge_target: None,
            edge_type: None,
            edge_weight: None,
        }
    }

    /// Create a new set tags record — replaces all existing tags for a key
    pub fn set_tags(key: Bytes, tags: Vec<String>, timestamp: u64) -> Self {
        Self {
            record_type: WalRecordType::SetTags,
            timestamp,
            key,
            value: Bytes::new(),
            embedding: None,
            ts_timestamp: None,
            tags: Some(tags),
            edge_source: None,
            edge_target: None,
            edge_type: None,
            edge_weight: None,
        }
    }

    /// Create a new add edge record (for graph index recovery)
    pub fn add_edge(
        source: Bytes,
        target: Bytes,
        edge_type: Option<String>,
        weight: Option<f32>,
        timestamp: u64,
    ) -> Self {
        Self {
            record_type: WalRecordType::AddEdge,
            timestamp,
            key: Bytes::new(),
            value: Bytes::new(),
            embedding: None,
            ts_timestamp: None,
            tags: None,
            edge_source: Some(source),
            edge_target: Some(target),
            edge_type,
            edge_weight: weight,
        }
    }

    /// Create a remove edge record — removes all edges from source to target
    pub fn remove_edge(source: Bytes, target: Bytes, timestamp: u64) -> Self {
        Self {
            record_type: WalRecordType::RemoveEdge,
            timestamp,
            key: Bytes::new(),
            value: Bytes::new(),
            embedding: None,
            ts_timestamp: None,
            tags: None,
            edge_source: Some(source),
            edge_target: Some(target),
            edge_type: None,
            edge_weight: None,
        }
    }

    /// Create a remove-timestamp record (signals time-series index cleanup on replay)
    pub fn remove_timestamp(key: Bytes, timestamp: u64) -> Self {
        Self {
            record_type: WalRecordType::RemoveTimestamp,
            timestamp,
            key,
            value: Bytes::new(),
            embedding: None,
            ts_timestamp: None,
            tags: None,
            edge_source: None,
            edge_target: None,
            edge_type: None,
            edge_weight: None,
        }
    }

    /// Create a remove-tags record (signals tag index cleanup on replay)
    pub fn remove_tags(key: Bytes, timestamp: u64) -> Self {
        Self {
            record_type: WalRecordType::RemoveTags,
            timestamp,
            key,
            value: Bytes::new(),
            embedding: None,
            ts_timestamp: None,
            tags: None,
            edge_source: None,
            edge_target: None,
            edge_type: None,
            edge_weight: None,
        }
    }

    /// Create a remove-vector record (signals HNSW index cleanup on replay)
    pub fn remove_vector(key: Bytes, timestamp: u64) -> Self {
        Self {
            record_type: WalRecordType::RemoveVector,
            timestamp,
            key,
            value: Bytes::new(),
            embedding: None,
            ts_timestamp: None,
            tags: None,
            edge_source: None,
            edge_target: None,
            edge_type: None,
            edge_weight: None,
        }
    }

    /// Encode the record to bytes (excluding length and checksum)
    fn encode_inner(&self) -> Vec<u8> {
        let key_len = self.key.len() as u16;
        let value_len = self.value.len() as u32;

        // Calculate embedding size if present
        let embedding_len = self.embedding.as_ref().map_or(0, |e| e.len()) as u32;
        let embedding_bytes = embedding_len as usize * 4; // 4 bytes per f32

        // Calculate tags size if present
        let tags_data: Vec<u8> = if let Some(tags) = &self.tags {
            let mut data = Vec::new();
            data.extend_from_slice(&(tags.len() as u32).to_le_bytes());
            for tag in tags {
                let tag_bytes = tag.as_bytes();
                data.extend_from_slice(&(tag_bytes.len() as u16).to_le_bytes());
                data.extend_from_slice(tag_bytes);
            }
            data
        } else {
            Vec::new()
        };

        let total_len = 1
            + 8
            + 2
            + self.key.len()
            + 4
            + self.value.len()
            + 4
            + embedding_bytes
            + 8
            + tags_data.len();
        let mut buf = Vec::with_capacity(total_len);

        buf.push(self.record_type as u8);
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&value_len.to_le_bytes());
        buf.extend_from_slice(&self.value);

        // Encode embedding for InsertWithEmbedding records
        if self.record_type == WalRecordType::InsertWithEmbedding {
            buf.extend_from_slice(&embedding_len.to_le_bytes());
            if let Some(embedding) = &self.embedding {
                for &val in embedding {
                    buf.extend_from_slice(&val.to_le_bytes());
                }
            }
        }

        // Encode timestamp for SetTimestamp records
        if self.record_type == WalRecordType::SetTimestamp {
            let ts = self.ts_timestamp.unwrap_or(0);
            buf.extend_from_slice(&ts.to_le_bytes());
        }

        // Encode tags for AddTags and SetTags records (same wire format, different semantics)
        if self.record_type == WalRecordType::AddTags
            || self.record_type == WalRecordType::SetTags
        {
            buf.extend_from_slice(&tags_data);
        }

        // Encode edge data for AddEdge records
        if self.record_type == WalRecordType::AddEdge {
            let source = self.edge_source.as_ref().map_or(&[][..], |b| &b[..]);
            let target = self.edge_target.as_ref().map_or(&[][..], |b| &b[..]);
            let edge_type_str = self.edge_type.as_deref().unwrap_or("");
            let weight = self.edge_weight.unwrap_or(1.0);

            buf.extend_from_slice(&(source.len() as u16).to_le_bytes());
            buf.extend_from_slice(source);
            buf.extend_from_slice(&(target.len() as u16).to_le_bytes());
            buf.extend_from_slice(target);
            buf.extend_from_slice(&(edge_type_str.len() as u16).to_le_bytes());
            buf.extend_from_slice(edge_type_str.as_bytes());
            buf.extend_from_slice(&weight.to_le_bytes());
        }

        // Encode source+target for RemoveEdge records (no edge_type or weight needed for deletion)
        if self.record_type == WalRecordType::RemoveEdge {
            let source = self.edge_source.as_ref().map_or(&[][..], |b| &b[..]);
            let target = self.edge_target.as_ref().map_or(&[][..], |b| &b[..]);

            buf.extend_from_slice(&(source.len() as u16).to_le_bytes());
            buf.extend_from_slice(source);
            buf.extend_from_slice(&(target.len() as u16).to_le_bytes());
            buf.extend_from_slice(target);
        }

        buf
    }

    /// Encode the full record with length and checksum
    pub fn encode(&self) -> Vec<u8> {
        let inner = self.encode_inner();
        let checksum = crc32fast::hash(&inner);
        let length = inner.len() as u32;

        let mut buf = Vec::with_capacity(8 + inner.len());
        buf.extend_from_slice(&length.to_le_bytes());
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf.extend_from_slice(&inner);

        buf
    }

    /// Decode a record from bytes (excluding length prefix, including checksum)
    fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 8 {
            return Err(StorageError::InvalidArgument(
                "WAL record too short".to_string(),
            ));
        }

        let checksum = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let inner = &data[4..];

        // Verify checksum
        let computed_checksum = crc32fast::hash(inner);
        if checksum != computed_checksum {
            return Err(StorageError::Corruption {
                file: PathBuf::from("WAL"),
                message: format!(
                    "Checksum mismatch: expected {:#x}, got {:#x}",
                    checksum, computed_checksum
                ),
            });
        }

        Self::decode_inner(inner)
    }

    /// Decode the inner record data (after checksum verification)
    fn decode_inner(data: &[u8]) -> Result<Self> {
        if data.len() < 15 {
            // 1 + 8 + 2 + 0 + 4 = 15 minimum
            return Err(StorageError::InvalidArgument(
                "WAL record too short".to_string(),
            ));
        }

        let record_type = WalRecordType::try_from(data[0])?;
        let timestamp = u64::from_le_bytes([
            data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
        ]);
        let key_len = u16::from_le_bytes([data[9], data[10]]) as usize;

        if data.len() < 15 + key_len {
            return Err(StorageError::InvalidArgument(
                "WAL record key truncated".to_string(),
            ));
        }

        let key = Bytes::copy_from_slice(&data[11..11 + key_len]);

        let value_offset = 11 + key_len;
        let value_len = u32::from_le_bytes([
            data[value_offset],
            data[value_offset + 1],
            data[value_offset + 2],
            data[value_offset + 3],
        ]) as usize;

        let value_start = value_offset + 4;
        if data.len() < value_start + value_len {
            return Err(StorageError::InvalidArgument(
                "WAL record value truncated".to_string(),
            ));
        }

        let value = Bytes::copy_from_slice(&data[value_start..value_start + value_len]);

        let mut current_offset = value_start + value_len;

        // Decode embedding for InsertWithEmbedding records
        let embedding = if record_type == WalRecordType::InsertWithEmbedding {
            if data.len() < current_offset + 4 {
                return Err(StorageError::InvalidArgument(
                    "WAL record embedding length truncated".to_string(),
                ));
            }

            let embedding_len = u32::from_le_bytes([
                data[current_offset],
                data[current_offset + 1],
                data[current_offset + 2],
                data[current_offset + 3],
            ]) as usize;

            let embedding_start = current_offset + 4;
            let embedding_bytes = embedding_len * 4;
            if data.len() < embedding_start + embedding_bytes {
                return Err(StorageError::InvalidArgument(
                    "WAL record embedding data truncated".to_string(),
                ));
            }

            let mut embedding = Vec::with_capacity(embedding_len);
            for i in 0..embedding_len {
                let offset = embedding_start + i * 4;
                let val = f32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                embedding.push(val);
            }
            current_offset = embedding_start + embedding_bytes;
            Some(embedding)
        } else {
            None
        };

        // Decode timestamp for SetTimestamp records
        let ts_timestamp = if record_type == WalRecordType::SetTimestamp {
            if data.len() < current_offset + 8 {
                return Err(StorageError::InvalidArgument(
                    "WAL record ts_timestamp truncated".to_string(),
                ));
            }

            let ts = u64::from_le_bytes([
                data[current_offset],
                data[current_offset + 1],
                data[current_offset + 2],
                data[current_offset + 3],
                data[current_offset + 4],
                data[current_offset + 5],
                data[current_offset + 6],
                data[current_offset + 7],
            ]);
            current_offset += 8;
            Some(ts)
        } else {
            None
        };

        // Decode tags for AddTags and SetTags records (same wire format)
        let tags = if record_type == WalRecordType::AddTags
            || record_type == WalRecordType::SetTags
        {
            if data.len() < current_offset + 4 {
                return Err(StorageError::InvalidArgument(
                    "WAL record tags count truncated".to_string(),
                ));
            }

            let tags_count = u32::from_le_bytes([
                data[current_offset],
                data[current_offset + 1],
                data[current_offset + 2],
                data[current_offset + 3],
            ]) as usize;
            current_offset += 4;

            let mut tags = Vec::with_capacity(tags_count);
            for _ in 0..tags_count {
                if data.len() < current_offset + 2 {
                    return Err(StorageError::InvalidArgument(
                        "WAL record tag length truncated".to_string(),
                    ));
                }

                let tag_len =
                    u16::from_le_bytes([data[current_offset], data[current_offset + 1]]) as usize;
                current_offset += 2;

                if data.len() < current_offset + tag_len {
                    return Err(StorageError::InvalidArgument(
                        "WAL record tag data truncated".to_string(),
                    ));
                }

                let tag = String::from_utf8_lossy(&data[current_offset..current_offset + tag_len])
                    .to_string();
                current_offset += tag_len;
                tags.push(tag);
            }
            Some(tags)
        } else {
            None
        };

        // Decode edge data for AddEdge and RemoveEdge records
        let (edge_source, edge_target, edge_type, edge_weight) = if record_type
            == WalRecordType::AddEdge
            || record_type == WalRecordType::RemoveEdge
        {
            // Decode source
            if data.len() < current_offset + 2 {
                return Err(StorageError::InvalidArgument(
                    "WAL edge source length truncated".into(),
                ));
            }
            let source_len =
                u16::from_le_bytes([data[current_offset], data[current_offset + 1]]) as usize;
            current_offset += 2;
            if data.len() < current_offset + source_len {
                return Err(StorageError::InvalidArgument(
                    "WAL edge source truncated".into(),
                ));
            }
            let source = Bytes::copy_from_slice(&data[current_offset..current_offset + source_len]);
            current_offset += source_len;

            // Decode target
            if data.len() < current_offset + 2 {
                return Err(StorageError::InvalidArgument(
                    "WAL edge target length truncated".into(),
                ));
            }
            let target_len =
                u16::from_le_bytes([data[current_offset], data[current_offset + 1]]) as usize;
            current_offset += 2;
            if data.len() < current_offset + target_len {
                return Err(StorageError::InvalidArgument(
                    "WAL edge target truncated".into(),
                ));
            }
            let target = Bytes::copy_from_slice(&data[current_offset..current_offset + target_len]);
            current_offset += target_len;

            // AddEdge carries edge_type and weight; RemoveEdge does not
            if record_type == WalRecordType::AddEdge {
                // Decode edge_type
                if data.len() < current_offset + 2 {
                    return Err(StorageError::InvalidArgument(
                        "WAL edge type length truncated".into(),
                    ));
                }
                let type_len =
                    u16::from_le_bytes([data[current_offset], data[current_offset + 1]]) as usize;
                current_offset += 2;
                let edge_type = if type_len > 0 {
                    if data.len() < current_offset + type_len {
                        return Err(StorageError::InvalidArgument(
                            "WAL edge type truncated".into(),
                        ));
                    }
                    Some(
                        String::from_utf8_lossy(&data[current_offset..current_offset + type_len])
                            .to_string(),
                    )
                } else {
                    None
                };
                current_offset += type_len;

                // Decode weight
                if data.len() < current_offset + 4 {
                    return Err(StorageError::InvalidArgument(
                        "WAL edge weight truncated".into(),
                    ));
                }
                let weight = f32::from_le_bytes([
                    data[current_offset],
                    data[current_offset + 1],
                    data[current_offset + 2],
                    data[current_offset + 3],
                ]);

                (Some(source), Some(target), edge_type, Some(weight))
            } else {
                // RemoveEdge: only source + target needed
                (Some(source), Some(target), None, None)
            }
        } else {
            (None, None, None, None)
        };

        Ok(Self {
            record_type,
            timestamp,
            key,
            value,
            embedding,
            ts_timestamp,
            tags,
            edge_source,
            edge_target,
            edge_type,
            edge_weight,
        })
    }
}

/// Write-Ahead Log for durability
///
/// The WAL provides:
/// - Durability: All operations are logged before being applied
/// - Recovery: Operations can be replayed after a crash
/// - Sync: Data can be synced to disk for crash consistency
pub struct WAL {
    /// Path to the WAL file
    path: PathBuf,
    /// Buffered writer for the WAL file
    writer: BufWriter<File>,
    /// Current file size
    size: u64,
}

impl WAL {
    /// Create a new WAL file
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        Ok(Self {
            path,
            writer: BufWriter::with_capacity(64 * 1024, file),
            size: 0,
        })
    }

    /// Open an existing WAL file for appending
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&path)?;

        let size = file.metadata()?.len();

        Ok(Self {
            path,
            writer: BufWriter::with_capacity(64 * 1024, file),
            size,
        })
    }

    /// Append a record to the WAL
    pub fn append(&mut self, record: &WalRecord) -> Result<()> {
        let encoded = record.encode();
        self.writer.write_all(&encoded)?;
        self.size += encoded.len() as u64;
        Ok(())
    }

    /// Write all records into the `BufWriter` in a single pass.
    ///
    /// The caller must call `sync()` afterwards to flush and fsync.
    /// This is more efficient than N separate `append` + `sync` pairs because
    /// a single subsequent `sync()` covers all records in the batch.
    pub fn append_batch(&mut self, records: &[WalRecord]) -> Result<()> {
        for record in records {
            let encoded = record.encode();
            self.writer.write_all(&encoded)?;
            self.size += encoded.len() as u64;
        }
        Ok(())
    }

    /// Sync the WAL to disk (fsync)
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Get the path to the WAL file
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the current size of the WAL
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Create an iterator to read records from the WAL
    pub fn iter(&self) -> Result<WalIterator> {
        WalIterator::new(&self.path)
    }

    /// Truncate the WAL (after flushing to SSTable)
    pub fn truncate(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().set_len(0)?;
        self.writer.get_ref().seek(SeekFrom::Start(0))?;
        self.size = 0;
        Ok(())
    }
}

/// Iterator over WAL records
pub struct WalIterator {
    reader: BufReader<File>,
    path: PathBuf,
    offset: u64,
}

impl WalIterator {
    /// Create a new iterator over a WAL file
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let reader = BufReader::with_capacity(64 * 1024, file);

        Ok(Self {
            reader,
            path,
            offset: 0,
        })
    }

    /// Read the next record from the WAL
    fn read_next(&mut self) -> Result<Option<WalRecord>> {
        // Read length prefix
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        let length = u32::from_le_bytes(len_buf) as usize;

        if length == 0 || length > 100 * 1024 * 1024 {
            // Sanity check: max 100MB per record
            return Err(StorageError::wal_replay(
                self.offset,
                format!("Invalid record length: {}", length),
            ));
        }

        // Read the rest of the record (checksum + data)
        let mut data = vec![0u8; 4 + length]; // 4 bytes for checksum
        match self.reader.read_exact(&mut data) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(StorageError::wal_replay(
                    self.offset,
                    "Truncated record at end of WAL",
                ));
            }
            Err(e) => return Err(e.into()),
        }

        let record = WalRecord::decode(&data).map_err(|e| {
            StorageError::wal_replay(self.offset, format!("Failed to decode record: {}", e))
        })?;

        self.offset += 4 + 4 + length as u64;

        Ok(Some(record))
    }
}

impl Iterator for WalIterator {
    type Item = Result<WalRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Async WAL operations
impl WAL {
    /// Async version of sync
    pub async fn sync_async(&mut self) -> Result<()> {
        self.writer.flush()?;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let file = File::open(&path)?;
            file.sync_all()?;
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))??;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_record_encode_decode() {
        let record = WalRecord::insert(Bytes::from("test_key"), Bytes::from("test_value"), 12345);

        let encoded = record.encode();

        // Skip the 4-byte length prefix
        let decoded = WalRecord::decode(&encoded[4..]).unwrap();

        assert_eq!(decoded.record_type, WalRecordType::Insert);
        assert_eq!(decoded.timestamp, 12345);
        assert_eq!(decoded.key, Bytes::from("test_key"));
        assert_eq!(decoded.value, Bytes::from("test_value"));
    }

    #[test]
    fn test_wal_record_delete() {
        let record = WalRecord::delete(Bytes::from("delete_key"), 99999);

        let encoded = record.encode();
        let decoded = WalRecord::decode(&encoded[4..]).unwrap();

        assert_eq!(decoded.record_type, WalRecordType::Delete);
        assert_eq!(decoded.timestamp, 99999);
        assert_eq!(decoded.key, Bytes::from("delete_key"));
        assert!(decoded.value.is_empty());
    }

    #[test]
    fn test_wal_write_and_read() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write records
        {
            let mut wal = WAL::create(&wal_path).unwrap();

            wal.append(&WalRecord::insert(
                Bytes::from("key1"),
                Bytes::from("value1"),
                1,
            ))
            .unwrap();

            wal.append(&WalRecord::insert(
                Bytes::from("key2"),
                Bytes::from("value2"),
                2,
            ))
            .unwrap();

            wal.append(&WalRecord::delete(Bytes::from("key1"), 3))
                .unwrap();

            wal.sync().unwrap();
        }

        // Read records back
        {
            let wal = WAL::open(&wal_path).unwrap();
            let records: Vec<_> = wal.iter().unwrap().collect();

            assert_eq!(records.len(), 3);

            let r1 = records[0].as_ref().unwrap();
            assert_eq!(r1.record_type, WalRecordType::Insert);
            assert_eq!(r1.key, Bytes::from("key1"));
            assert_eq!(r1.value, Bytes::from("value1"));
            assert_eq!(r1.timestamp, 1);

            let r2 = records[1].as_ref().unwrap();
            assert_eq!(r2.record_type, WalRecordType::Insert);
            assert_eq!(r2.key, Bytes::from("key2"));

            let r3 = records[2].as_ref().unwrap();
            assert_eq!(r3.record_type, WalRecordType::Delete);
            assert_eq!(r3.key, Bytes::from("key1"));
            assert_eq!(r3.timestamp, 3);
        }
    }

    #[test]
    fn test_wal_truncate() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        let mut wal = WAL::create(&wal_path).unwrap();

        wal.append(&WalRecord::insert(
            Bytes::from("key1"),
            Bytes::from("value1"),
            1,
        ))
        .unwrap();

        wal.sync().unwrap();
        assert!(wal.size() > 0);

        wal.truncate().unwrap();
        assert_eq!(wal.size(), 0);

        // Read should return no records
        let records: Vec<_> = wal.iter().unwrap().collect();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn test_wal_append_after_open() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Create and write initial records
        {
            let mut wal = WAL::create(&wal_path).unwrap();
            wal.append(&WalRecord::insert(
                Bytes::from("key1"),
                Bytes::from("value1"),
                1,
            ))
            .unwrap();
            wal.sync().unwrap();
        }

        // Open and append more records
        {
            let mut wal = WAL::open(&wal_path).unwrap();
            wal.append(&WalRecord::insert(
                Bytes::from("key2"),
                Bytes::from("value2"),
                2,
            ))
            .unwrap();
            wal.sync().unwrap();
        }

        // Read all records
        {
            let wal = WAL::open(&wal_path).unwrap();
            let records: Vec<_> = wal.iter().unwrap().collect();
            assert_eq!(records.len(), 2);
        }
    }

    #[test]
    fn test_checksum_validation() {
        let record = WalRecord::insert(Bytes::from("key"), Bytes::from("value"), 1);

        let mut encoded = record.encode();

        // Corrupt the data
        if encoded.len() > 10 {
            encoded[10] ^= 0xFF;
        }

        // Decode should fail due to checksum mismatch
        let result = WalRecord::decode(&encoded[4..]);
        assert!(result.is_err());
    }

    #[test]
    fn test_wal_record_with_embedding() {
        let embedding = vec![0.1, 0.2, 0.3, 0.4, 0.5];
        let record = WalRecord::insert_with_embedding(
            Bytes::from("vector_key"),
            Bytes::from("vector_value"),
            54321,
            embedding.clone(),
        );

        let encoded = record.encode();

        // Skip the 4-byte length prefix
        let decoded = WalRecord::decode(&encoded[4..]).unwrap();

        assert_eq!(decoded.record_type, WalRecordType::InsertWithEmbedding);
        assert_eq!(decoded.timestamp, 54321);
        assert_eq!(decoded.key, Bytes::from("vector_key"));
        assert_eq!(decoded.value, Bytes::from("vector_value"));
        assert!(decoded.embedding.is_some());

        let decoded_embedding = decoded.embedding.unwrap();
        assert_eq!(decoded_embedding.len(), embedding.len());
        for (i, &val) in embedding.iter().enumerate() {
            assert!((decoded_embedding[i] - val).abs() < 1e-6);
        }
    }

    #[test]
    fn test_wal_write_and_read_with_embeddings() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test_embed.wal");

        let embedding = vec![1.0, 2.0, 3.0, 4.0];

        // Write records with embeddings
        {
            let mut wal = WAL::create(&wal_path).unwrap();

            wal.append(&WalRecord::insert(
                Bytes::from("key1"),
                Bytes::from("value1"),
                1,
            ))
            .unwrap();

            wal.append(&WalRecord::insert_with_embedding(
                Bytes::from("key2"),
                Bytes::from("value2"),
                2,
                embedding.clone(),
            ))
            .unwrap();

            wal.sync().unwrap();
        }

        // Read records back
        {
            let wal = WAL::open(&wal_path).unwrap();
            let records: Vec<_> = wal.iter().unwrap().collect();

            assert_eq!(records.len(), 2);

            // First record - regular insert
            let r1 = records[0].as_ref().unwrap();
            assert_eq!(r1.record_type, WalRecordType::Insert);
            assert_eq!(r1.key, Bytes::from("key1"));
            assert!(r1.embedding.is_none());

            // Second record - insert with embedding
            let r2 = records[1].as_ref().unwrap();
            assert_eq!(r2.record_type, WalRecordType::InsertWithEmbedding);
            assert_eq!(r2.key, Bytes::from("key2"));
            assert!(r2.embedding.is_some());

            let decoded_embedding = r2.embedding.as_ref().unwrap();
            assert_eq!(decoded_embedding.len(), embedding.len());
            for (i, &val) in embedding.iter().enumerate() {
                assert!((decoded_embedding[i] - val).abs() < 1e-6);
            }
        }
    }

    #[test]
    fn test_wal_record_set_timestamp() {
        let record = WalRecord::set_timestamp(
            Bytes::from("event:1"),
            1705320000, // ts_timestamp (the actual time-series timestamp)
            12345,      // WAL timestamp (MVCC timestamp)
        );

        let encoded = record.encode();
        let decoded = WalRecord::decode(&encoded[4..]).unwrap();

        assert_eq!(decoded.record_type, WalRecordType::SetTimestamp);
        assert_eq!(decoded.timestamp, 12345);
        assert_eq!(decoded.key, Bytes::from("event:1"));
        assert!(decoded.value.is_empty());
        assert!(decoded.ts_timestamp.is_some());
        assert_eq!(decoded.ts_timestamp.unwrap(), 1705320000);
        assert!(decoded.embedding.is_none());
        assert!(decoded.tags.is_none());
    }

    #[test]
    fn test_wal_record_add_tags() {
        let tags = vec![
            "rust".to_string(),
            "programming".to_string(),
            "async".to_string(),
        ];
        let record = WalRecord::add_tags(Bytes::from("doc:1"), tags.clone(), 12345);

        let encoded = record.encode();
        let decoded = WalRecord::decode(&encoded[4..]).unwrap();

        assert_eq!(decoded.record_type, WalRecordType::AddTags);
        assert_eq!(decoded.timestamp, 12345);
        assert_eq!(decoded.key, Bytes::from("doc:1"));
        assert!(decoded.value.is_empty());
        assert!(decoded.ts_timestamp.is_none());
        assert!(decoded.embedding.is_none());
        assert!(decoded.tags.is_some());

        let decoded_tags = decoded.tags.unwrap();
        assert_eq!(decoded_tags.len(), tags.len());
        for (i, tag) in tags.iter().enumerate() {
            assert_eq!(&decoded_tags[i], tag);
        }
    }

    #[test]
    fn test_wal_write_and_read_with_timestamps_and_tags() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test_ts_tags.wal");

        let tags = vec!["tag1".to_string(), "tag2".to_string()];

        // Write records
        {
            let mut wal = WAL::create(&wal_path).unwrap();

            wal.append(&WalRecord::set_timestamp(Bytes::from("event:1"), 1000, 1))
                .unwrap();

            wal.append(&WalRecord::set_timestamp(Bytes::from("event:2"), 2000, 2))
                .unwrap();

            wal.append(&WalRecord::add_tags(Bytes::from("doc:1"), tags.clone(), 3))
                .unwrap();

            wal.sync().unwrap();
        }

        // Read records back
        {
            let wal = WAL::open(&wal_path).unwrap();
            let records: Vec<_> = wal.iter().unwrap().collect();

            assert_eq!(records.len(), 3);

            // First record - set timestamp
            let r1 = records[0].as_ref().unwrap();
            assert_eq!(r1.record_type, WalRecordType::SetTimestamp);
            assert_eq!(r1.key, Bytes::from("event:1"));
            assert_eq!(r1.ts_timestamp.unwrap(), 1000);

            // Second record - set timestamp
            let r2 = records[1].as_ref().unwrap();
            assert_eq!(r2.record_type, WalRecordType::SetTimestamp);
            assert_eq!(r2.key, Bytes::from("event:2"));
            assert_eq!(r2.ts_timestamp.unwrap(), 2000);

            // Third record - add tags
            let r3 = records[2].as_ref().unwrap();
            assert_eq!(r3.record_type, WalRecordType::AddTags);
            assert_eq!(r3.key, Bytes::from("doc:1"));
            let decoded_tags = r3.tags.as_ref().unwrap();
            assert_eq!(decoded_tags.len(), tags.len());
            assert_eq!(decoded_tags[0], "tag1");
            assert_eq!(decoded_tags[1], "tag2");
        }
    }

    #[test]
    fn test_wal_record_add_edge() {
        let record = WalRecord::add_edge(
            Bytes::from("node:1"),
            Bytes::from("node:2"),
            Some("follows".to_string()),
            Some(0.8),
            12345,
        );

        let encoded = record.encode();
        let decoded = WalRecord::decode(&encoded[4..]).unwrap();

        assert_eq!(decoded.record_type, WalRecordType::AddEdge);
        assert_eq!(decoded.timestamp, 12345);
        assert_eq!(decoded.edge_source.unwrap(), Bytes::from("node:1"));
        assert_eq!(decoded.edge_target.unwrap(), Bytes::from("node:2"));
        assert_eq!(decoded.edge_type.unwrap(), "follows");
        assert!((decoded.edge_weight.unwrap() - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_wal_record_add_edge_no_type() {
        // Test edge without edge_type (empty string)
        let record = WalRecord::add_edge(Bytes::from("a"), Bytes::from("b"), None, Some(1.0), 99);

        let encoded = record.encode();
        let decoded = WalRecord::decode(&encoded[4..]).unwrap();

        assert_eq!(decoded.record_type, WalRecordType::AddEdge);
        assert_eq!(decoded.edge_source.unwrap(), Bytes::from("a"));
        assert_eq!(decoded.edge_target.unwrap(), Bytes::from("b"));
        assert!(decoded.edge_type.is_none());
        assert!((decoded.edge_weight.unwrap() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_wal_write_and_read_with_edges() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test_edges.wal");

        // Write edge records
        {
            let mut wal = WAL::create(&wal_path).unwrap();

            wal.append(&WalRecord::add_edge(
                Bytes::from("user:1"),
                Bytes::from("user:2"),
                Some("follows".to_string()),
                Some(1.0),
                1,
            ))
            .unwrap();

            wal.append(&WalRecord::add_edge(
                Bytes::from("user:2"),
                Bytes::from("user:3"),
                Some("related_to".to_string()),
                Some(0.5),
                2,
            ))
            .unwrap();

            wal.sync().unwrap();
        }

        // Read records back
        {
            let wal = WAL::open(&wal_path).unwrap();
            let records: Vec<_> = wal.iter().unwrap().collect();

            assert_eq!(records.len(), 2);

            let r1 = records[0].as_ref().unwrap();
            assert_eq!(r1.record_type, WalRecordType::AddEdge);
            assert_eq!(r1.edge_source.as_ref().unwrap(), &Bytes::from("user:1"));
            assert_eq!(r1.edge_target.as_ref().unwrap(), &Bytes::from("user:2"));
            assert_eq!(r1.edge_type.as_ref().unwrap(), "follows");
            assert!((r1.edge_weight.unwrap() - 1.0).abs() < 1e-6);

            let r2 = records[1].as_ref().unwrap();
            assert_eq!(r2.record_type, WalRecordType::AddEdge);
            assert_eq!(r2.edge_source.as_ref().unwrap(), &Bytes::from("user:2"));
            assert_eq!(r2.edge_target.as_ref().unwrap(), &Bytes::from("user:3"));
            assert_eq!(r2.edge_type.as_ref().unwrap(), "related_to");
            assert!((r2.edge_weight.unwrap() - 0.5).abs() < 1e-6);
        }
    }

    #[test]
    fn remove_index_records_roundtrip() {
        let ts = 42u64;
        let key = Bytes::from("memory:test-uuid");

        for record in [
            WalRecord::remove_timestamp(key.clone(), ts),
            WalRecord::remove_tags(key.clone(), ts),
            WalRecord::remove_vector(key.clone(), ts),
        ] {
            let encoded = record.encode();
            // decode strips the 4-byte length prefix, then passes [checksum + inner]
            let decoded = WalRecord::decode(&encoded[4..]).expect("decode must succeed");
            assert_eq!(decoded.key, key);
            assert_eq!(decoded.timestamp, ts);
            assert!(decoded.value.is_empty());
            assert!(decoded.embedding.is_none());
            assert!(decoded.tags.is_none());
            assert!(decoded.edge_source.is_none());
        }
    }

    fn make_wal() -> (WAL, tempfile::NamedTempFile) {
        let f = tempfile::NamedTempFile::new().unwrap();
        let wal = WAL::open(f.path()).unwrap();
        (wal, f)
    }

    #[test]
    fn append_batch_writes_all_records_and_can_be_read_back() {
        let (mut wal, f) = make_wal();

        let records = vec![
            WalRecord::insert(
                Bytes::from("key1"),
                Bytes::from("value1"),
                100,
            ),
            WalRecord::set_timestamp(Bytes::from("key2"), 999, 101),
            WalRecord::add_tags(
                Bytes::from("key3"),
                vec!["tag_a".to_string(), "tag_b".to_string()],
                102,
            ),
        ];

        wal.append_batch(&records).unwrap();
        wal.sync().unwrap();

        // Read back via iterator
        let read_back: Vec<WalRecord> = WAL::open(f.path())
            .unwrap()
            .iter()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(read_back.len(), 3);
        assert_eq!(read_back[0].key, Bytes::from("key1"));
        assert_eq!(read_back[1].ts_timestamp, Some(999));
        assert_eq!(read_back[2].tags.as_ref().unwrap().len(), 2);
    }
}
