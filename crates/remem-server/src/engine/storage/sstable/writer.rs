//! SSTable writer implementation
#![allow(dead_code)]

use bytes::Bytes;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::format::{
    Compression, IndexEntry, Record, SSTableFooter, SSTableHeader, SSTableMeta, BLOCK_SIZE,
    HEADER_SIZE,
};
use crate::engine::error::{Result, StorageError};
use crate::engine::util::BloomFilter;

/// Builder for creating SSTable files
///
/// Records must be added in sorted key order. The writer will:
/// 1. Buffer records into blocks
/// 2. Compress blocks when they reach the target size
/// 3. Build a sparse index of block first-keys
/// 4. Build a bloom filter for all keys
/// 5. Write the final file with header, data, index, bloom, and footer
pub struct SSTableWriter {
    /// Path to the output file
    path: PathBuf,
    /// Buffered file writer
    writer: BufWriter<File>,
    /// Compression type
    compression: Compression,
    /// Current block being built
    current_block: Vec<u8>,
    /// Index entries for each block
    index_entries: Vec<IndexEntry>,
    /// Bloom filter for all keys
    bloom: BloomFilter,
    /// Current offset in the file (after header)
    current_offset: u64,
    /// Number of records written
    record_count: u64,
    /// First key (for header)
    min_key: Option<Bytes>,
    /// Last key (for header)
    max_key: Option<Bytes>,
    /// First key in the current block
    block_first_key: Option<Bytes>,
    /// CRC32 hasher for the entire file
    file_hasher: crc32fast::Hasher,
    /// Level in the LSM tree
    level: usize,
}

impl SSTableWriter {
    /// Create a new SSTable writer
    pub fn new(path: impl AsRef<Path>, compression: Compression) -> Result<Self> {
        Self::with_level(path, compression, 0)
    }

    /// Create a new SSTable writer for a specific level
    pub fn with_level(
        path: impl AsRef<Path>,
        compression: Compression,
        level: usize,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = File::create(&path)?;
        let mut writer = BufWriter::with_capacity(64 * 1024, file);

        // Write placeholder header (will be updated at the end)
        let header = SSTableHeader::new(compression);
        let header_bytes = header.encode();
        writer.write_all(&header_bytes)?;

        let mut file_hasher = crc32fast::Hasher::new();
        file_hasher.update(&header_bytes);

        // Estimate bloom filter size based on expected records
        // We'll resize if needed, but start with a reasonable default
        let bloom = BloomFilter::new(10000, 0.01);

        Ok(Self {
            path,
            writer,
            compression,
            current_block: Vec::with_capacity(BLOCK_SIZE),
            index_entries: Vec::new(),
            bloom,
            current_offset: HEADER_SIZE as u64,
            record_count: 0,
            min_key: None,
            max_key: None,
            block_first_key: None,
            file_hasher,
            level,
        })
    }

    /// Add a record to the SSTable
    ///
    /// Records must be added in sorted key order.
    pub fn add(&mut self, key: Bytes, value: Option<Bytes>, timestamp: u64) -> Result<()> {
        let record = match value {
            Some(v) => Record::new(key.clone(), v, timestamp),
            None => Record::tombstone(key.clone(), timestamp),
        };

        // Update min/max keys
        if self.min_key.is_none() {
            self.min_key = Some(key.clone());
        }
        self.max_key = Some(key.clone());

        // Track first key in block
        if self.block_first_key.is_none() {
            self.block_first_key = Some(key.clone());
        }

        // Add to bloom filter
        self.bloom.insert(&key);

        // Encode record and add to current block
        let encoded = record.encode();
        self.current_block.extend_from_slice(&encoded);
        self.record_count += 1;

        // Flush block if it's large enough
        if self.current_block.len() >= BLOCK_SIZE {
            self.flush_block()?;
        }

        Ok(())
    }

    /// Add a record from a MemTable entry
    pub fn add_entry(&mut self, key: Bytes, entry: &crate::engine::storage::memtable::Entry) -> Result<()> {
        self.add(key, entry.value.clone(), entry.timestamp)
    }

    /// Flush the current block to disk
    fn flush_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        let first_key = self.block_first_key.take().unwrap();

        // Compress the block
        let compressed = self.compress_block(&self.current_block)?;

        // Record index entry
        self.index_entries.push(IndexEntry {
            key: first_key,
            offset: self.current_offset,
            size: compressed.len() as u32,
        });

        // Write to file
        self.writer.write_all(&compressed)?;
        self.file_hasher.update(&compressed);
        self.current_offset += compressed.len() as u64;

        // Clear the block
        self.current_block.clear();

        Ok(())
    }

    /// Compress a block using the configured compression
    fn compress_block(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.compression {
            Compression::None => Ok(data.to_vec()),
            Compression::Zstd => {
                zstd::encode_all(data, 3).map_err(|e| StorageError::Compression(e.to_string()))
            }
        }
    }

    /// Finish writing the SSTable and return metadata
    pub fn finish(mut self) -> Result<SSTableMeta> {
        // Flush any remaining data in the current block
        self.flush_block()?;

        // Write index block
        let index_offset = self.current_offset;
        let index_data = self.encode_index();
        let compressed_index = self.compress_block(&index_data)?;
        self.writer.write_all(&compressed_index)?;
        self.current_offset += compressed_index.len() as u64;

        // Write bloom filter
        let bloom_offset = self.current_offset;
        let bloom_data = self.bloom.encode();
        self.writer.write_all(&bloom_data)?;
        self.current_offset += bloom_data.len() as u64;

        // Update header with final values
        let mut header = SSTableHeader::new(self.compression);
        header.record_count = self.record_count;
        if let Some(ref key) = self.min_key {
            header.set_min_key(key);
        }
        if let Some(ref key) = self.max_key {
            header.set_max_key(key);
        }

        // Seek back and write updated header
        self.writer.seek(SeekFrom::Start(0))?;
        self.writer.write_all(&header.encode())?;

        // Flush everything before calculating checksum
        self.writer.flush()?;

        // Calculate checksum by reading the entire file (excluding footer space)
        let checksum = {
            use std::io::Read;
            let mut file = std::fs::File::open(&self.path)?;
            let mut data = vec![0u8; self.current_offset as usize];
            file.read_exact(&mut data)?;
            crc32fast::hash(&data)
        };

        // Write footer at the end
        let footer = SSTableFooter {
            index_offset,
            index_size: compressed_index.len() as u32,
            bloom_offset,
            bloom_size: bloom_data.len() as u32,
            checksum,
        };
        let footer_bytes = footer.encode();

        // Seek to end and write footer
        self.writer.seek(SeekFrom::Start(self.current_offset))?;
        self.writer.write_all(&footer_bytes)?;
        self.writer.flush()?;

        let file_size = self.current_offset + footer_bytes.len() as u64;

        Ok(SSTableMeta {
            path: self.path,
            compression: self.compression,
            record_count: self.record_count,
            file_size,
            min_key: self.min_key.unwrap_or_default(),
            max_key: self.max_key.unwrap_or_default(),
            level: self.level,
        })
    }

    /// Encode the index block
    fn encode_index(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Number of entries
        let count = self.index_entries.len() as u32;
        buf.extend_from_slice(&count.to_le_bytes());

        // Encode each entry
        for entry in &self.index_entries {
            buf.extend_from_slice(&entry.encode());
        }

        buf
    }

    /// Get the current file size estimate
    pub fn estimated_size(&self) -> u64 {
        self.current_offset + self.current_block.len() as u64
    }

    /// Get the number of records written so far
    pub fn record_count(&self) -> u64 {
        self.record_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_write_empty_sstable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");

        let writer = SSTableWriter::new(&path, Compression::None).unwrap();
        let meta = writer.finish().unwrap();

        assert_eq!(meta.record_count, 0);
        assert!(meta.file_size > 0); // At least header + footer
    }

    #[test]
    fn test_write_single_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("single.sst");

        let mut writer = SSTableWriter::new(&path, Compression::None).unwrap();
        writer
            .add(Bytes::from("key1"), Some(Bytes::from("value1")), 1)
            .unwrap();
        let meta = writer.finish().unwrap();

        assert_eq!(meta.record_count, 1);
        assert_eq!(meta.min_key, Bytes::from("key1"));
        assert_eq!(meta.max_key, Bytes::from("key1"));
    }

    #[test]
    fn test_write_multiple_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi.sst");

        let mut writer = SSTableWriter::new(&path, Compression::None).unwrap();

        // Add records in sorted order
        for i in 0..100 {
            let key = format!("key{:05}", i);
            let value = format!("value{}", i);
            writer
                .add(Bytes::from(key), Some(Bytes::from(value)), i as u64)
                .unwrap();
        }

        let meta = writer.finish().unwrap();

        assert_eq!(meta.record_count, 100);
        assert_eq!(meta.min_key, Bytes::from("key00000"));
        assert_eq!(meta.max_key, Bytes::from("key00099"));
    }

    #[test]
    fn test_write_with_tombstones() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("tombstones.sst");

        let mut writer = SSTableWriter::new(&path, Compression::None).unwrap();

        writer
            .add(Bytes::from("key1"), Some(Bytes::from("value1")), 1)
            .unwrap();
        writer.add(Bytes::from("key2"), None, 2).unwrap(); // Tombstone
        writer
            .add(Bytes::from("key3"), Some(Bytes::from("value3")), 3)
            .unwrap();

        let meta = writer.finish().unwrap();

        assert_eq!(meta.record_count, 3);
    }

    #[test]
    fn test_write_with_compression() {
        let dir = tempdir().unwrap();
        let path_none = dir.path().join("none.sst");
        let path_zstd = dir.path().join("zstd.sst");

        let data: Vec<(Bytes, Bytes)> = (0..1000)
            .map(|i| {
                (
                    Bytes::from(format!("key{:05}", i)),
                    Bytes::from(format!("value{}", i).repeat(10)),
                )
            })
            .collect();

        // Write without compression
        let mut writer = SSTableWriter::new(&path_none, Compression::None).unwrap();
        for (key, value) in &data {
            writer.add(key.clone(), Some(value.clone()), 1).unwrap();
        }
        let meta_none = writer.finish().unwrap();

        // Write with compression
        let mut writer = SSTableWriter::new(&path_zstd, Compression::Zstd).unwrap();
        for (key, value) in &data {
            writer.add(key.clone(), Some(value.clone()), 1).unwrap();
        }
        let meta_zstd = writer.finish().unwrap();

        // Compressed should be smaller
        assert!(
            meta_zstd.file_size < meta_none.file_size,
            "Compressed size {} should be smaller than uncompressed {}",
            meta_zstd.file_size,
            meta_none.file_size
        );
    }
}
