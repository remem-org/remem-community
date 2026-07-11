//! SSTable reader implementation
#![allow(dead_code)]

use bytes::Bytes;
use memmap2::Mmap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::block::{Block, BlockCache, BlockCacheKey};
use super::format::{
    Compression, IndexEntry, Record, SSTableFooter, SSTableHeader, SSTableMeta, FOOTER_SIZE,
    HEADER_SIZE, MAGIC,
};
use crate::engine::error::{Result, StorageError};
use crate::engine::util::BloomFilter;

/// Reader for SSTable files
///
/// The reader provides:
/// - Point lookups with bloom filter optimization
/// - Range scans via iterator
/// - Block caching for frequently accessed data
pub struct SSTableReader {
    /// Path to the SSTable file
    path: PathBuf,
    /// Memory-mapped file
    mmap: Mmap,
    /// Parsed header
    header: SSTableHeader,
    /// Parsed footer
    footer: SSTableFooter,
    /// Index entries (sparse index)
    index: Vec<IndexEntry>,
    /// Bloom filter
    bloom: BloomFilter,
    /// Compression type
    compression: Compression,
    /// Block cache (shared across readers)
    cache: Option<Arc<BlockCache>>,
    /// Level in the LSM tree
    level: usize,
}

impl std::fmt::Debug for SSTableReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SSTableReader")
            .field("path", &self.path)
            .field("level", &self.level)
            .field("compression", &self.compression)
            .field("record_count", &self.header.record_count)
            .field("index_entries", &self.index.len())
            .finish()
    }
}

impl SSTableReader {
    /// Open an SSTable file for reading
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_cache(path, None, 0)
    }

    /// Open an SSTable file with a shared block cache
    pub fn open_with_cache(
        path: impl AsRef<Path>,
        cache: Option<Arc<BlockCache>>,
        level: usize,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = File::open(&path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        if mmap.len() < HEADER_SIZE + FOOTER_SIZE {
            return Err(StorageError::invalid_format(
                &path,
                "File too small to be a valid SSTable",
            ));
        }

        // Parse header
        let header = SSTableHeader::decode(&mmap[..HEADER_SIZE])
            .ok_or_else(|| StorageError::invalid_format(&path, "Invalid SSTable header"))?;

        // Verify magic number
        if header.magic != MAGIC {
            return Err(StorageError::invalid_format(
                &path,
                format!("Invalid magic number: {:?}", header.magic),
            ));
        }

        // Parse footer
        let footer_start = mmap.len() - FOOTER_SIZE;
        let footer = SSTableFooter::decode(&mmap[footer_start..])
            .ok_or_else(|| StorageError::invalid_format(&path, "Invalid SSTable footer"))?;

        // Verify checksum
        let expected_checksum = footer.checksum;
        let actual_checksum = crc32fast::hash(&mmap[..footer_start]);
        if expected_checksum != actual_checksum {
            return Err(StorageError::checksum_mismatch(
                &path,
                expected_checksum,
                actual_checksum,
            ));
        }

        // Parse index
        let index_data = &mmap[footer.index_offset as usize
            ..(footer.index_offset + footer.index_size as u64) as usize];
        let index_data = Self::decompress_data(index_data, header.compression)?;
        let index = Self::parse_index(&index_data)?;

        // Parse bloom filter
        let bloom_data = &mmap[footer.bloom_offset as usize
            ..(footer.bloom_offset + footer.bloom_size as u64) as usize];
        let bloom = BloomFilter::decode(bloom_data)
            .ok_or_else(|| StorageError::invalid_format(&path, "Invalid bloom filter"))?;

        let compression = header.compression;

        Ok(Self {
            path,
            mmap,
            header,
            footer,
            index,
            bloom,
            compression,
            cache,
            level,
        })
    }

    /// Decompress data if needed
    fn decompress_data(data: &[u8], compression: Compression) -> Result<Vec<u8>> {
        match compression {
            Compression::None => Ok(data.to_vec()),
            Compression::Zstd => {
                zstd::decode_all(data).map_err(|e| StorageError::Decompression(e.to_string()))
            }
        }
    }

    /// Parse the index block
    fn parse_index(data: &[u8]) -> Result<Vec<IndexEntry>> {
        if data.len() < 4 {
            return Ok(Vec::new());
        }

        let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut entries = Vec::with_capacity(count);
        let mut offset = 4;

        for _ in 0..count {
            let (entry, size) =
                IndexEntry::decode(&data[offset..]).ok_or_else(|| StorageError::Corruption {
                    file: PathBuf::from("index"),
                    message: "Failed to decode index entry".to_string(),
                })?;
            entries.push(entry);
            offset += size;
        }

        Ok(entries)
    }

    /// Get a record by key
    ///
    /// Returns `None` if the key is not found.
    pub fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        // Check bloom filter first
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }

        // Find the block that might contain this key
        let block_idx = self.find_block_for_key(key);
        if block_idx >= self.index.len() {
            return Ok(None);
        }

        // Load and search the block
        let block = self.load_block(block_idx)?;
        Ok(block.get(key).cloned())
    }

    /// Find the index of the block that might contain the key
    fn find_block_for_key(&self, key: &[u8]) -> usize {
        // Binary search for the right block
        let pos = self
            .index
            .binary_search_by(|entry| entry.key.as_ref().cmp(key));

        match pos {
            Ok(i) => i,      // Exact match on block first key
            Err(0) => 0,     // Key is smaller than all blocks (might be in first block)
            Err(i) => i - 1, // Key is between blocks, check previous block
        }
    }

    /// Load a block by index
    fn load_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let entry = &self.index[block_idx];
        let cache_key = BlockCacheKey {
            path: self.path.clone(),
            offset: entry.offset,
        };

        // Try cache first
        if let Some(ref cache) = self.cache {
            if let Some(block) = cache.get(&cache_key) {
                return Ok(block);
            }
        }

        // Load from disk
        let block_data =
            &self.mmap[entry.offset as usize..(entry.offset + entry.size as u64) as usize];
        let decompressed = Self::decompress_data(block_data, self.compression)?;
        let block =
            Block::parse(Bytes::from(decompressed)).ok_or_else(|| StorageError::Corruption {
                file: self.path.clone(),
                message: format!("Failed to parse block at offset {}", entry.offset),
            })?;

        let block = Arc::new(block);

        // Insert into cache
        if let Some(ref cache) = self.cache {
            cache.insert(cache_key, Arc::clone(&block));
        }

        Ok(block)
    }

    /// Check if a key might be in this SSTable (bloom filter check)
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.may_contain(key)
    }

    /// Get an iterator over all records in the SSTable
    pub fn iter(&self) -> SSTableIterator<'_> {
        SSTableIterator::new(self)
    }

    /// Get metadata about this SSTable
    pub fn meta(&self) -> SSTableMeta {
        SSTableMeta {
            path: self.path.clone(),
            compression: self.compression,
            record_count: self.header.record_count,
            file_size: self.mmap.len() as u64,
            min_key: Bytes::copy_from_slice(&self.header.min_key),
            max_key: Bytes::copy_from_slice(&self.header.max_key),
            level: self.level,
        }
    }

    /// Get the path to this SSTable
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the number of records
    pub fn record_count(&self) -> u64 {
        self.header.record_count
    }

    /// Get the level in the LSM tree
    pub fn level(&self) -> usize {
        self.level
    }

    /// Get the number of index entries (blocks)
    pub fn block_count(&self) -> usize {
        self.index.len()
    }
}

/// Iterator over all records in an SSTable
pub struct SSTableIterator<'a> {
    reader: &'a SSTableReader,
    block_idx: usize,
    current_block: Option<Arc<Block>>,
    record_idx: usize,
}

impl<'a> SSTableIterator<'a> {
    fn new(reader: &'a SSTableReader) -> Self {
        Self {
            reader,
            block_idx: 0,
            current_block: None,
            record_idx: 0,
        }
    }

    fn load_next_block(&mut self) -> Result<bool> {
        if self.block_idx >= self.reader.index.len() {
            return Ok(false);
        }

        let block = self.reader.load_block(self.block_idx)?;
        self.current_block = Some(block);
        self.record_idx = 0;
        self.block_idx += 1;

        Ok(true)
    }
}

impl<'a> Iterator for SSTableIterator<'a> {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get next record from current block
            if let Some(ref block) = self.current_block {
                let records: Vec<_> = block.iter().collect();
                if self.record_idx < records.len() {
                    let record = records[self.record_idx].clone();
                    self.record_idx += 1;
                    return Some(Ok(record));
                }
            }

            // Load next block
            match self.load_next_block() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::storage::sstable::SSTableWriter;
    use tempfile::tempdir;

    fn create_test_sstable(
        path: &Path,
        records: Vec<(Bytes, Option<Bytes>, u64)>,
    ) -> Result<SSTableMeta> {
        let mut writer = SSTableWriter::new(path, Compression::None)?;
        for (key, value, ts) in records {
            writer.add(key, value, ts)?;
        }
        writer.finish()
    }

    #[test]
    fn test_read_single_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        create_test_sstable(
            &path,
            vec![(Bytes::from("key1"), Some(Bytes::from("value1")), 1)],
        )
        .unwrap();

        let reader = SSTableReader::open(&path).unwrap();

        let record = reader.get(b"key1").unwrap().unwrap();
        assert_eq!(record.key, Bytes::from("key1"));
        assert_eq!(record.value.unwrap(), Bytes::from("value1"));
        assert_eq!(record.timestamp, 1);

        // Non-existent key
        assert!(reader.get(b"nonexistent").unwrap().is_none());
    }

    #[test]
    fn test_read_multiple_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let records: Vec<_> = (0..100)
            .map(|i| {
                (
                    Bytes::from(format!("key{:05}", i)),
                    Some(Bytes::from(format!("value{}", i))),
                    i as u64,
                )
            })
            .collect();

        create_test_sstable(&path, records).unwrap();

        let reader = SSTableReader::open(&path).unwrap();

        // Test random access
        let record = reader.get(b"key00050").unwrap().unwrap();
        assert_eq!(record.value.unwrap(), Bytes::from("value50"));

        let record = reader.get(b"key00000").unwrap().unwrap();
        assert_eq!(record.value.unwrap(), Bytes::from("value0"));

        let record = reader.get(b"key00099").unwrap().unwrap();
        assert_eq!(record.value.unwrap(), Bytes::from("value99"));
    }

    #[test]
    fn test_read_tombstones() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        create_test_sstable(
            &path,
            vec![
                (Bytes::from("key1"), Some(Bytes::from("value1")), 1),
                (Bytes::from("key2"), None, 2), // Tombstone
                (Bytes::from("key3"), Some(Bytes::from("value3")), 3),
            ],
        )
        .unwrap();

        let reader = SSTableReader::open(&path).unwrap();

        let record = reader.get(b"key2").unwrap().unwrap();
        assert!(record.is_tombstone());
        assert_eq!(record.timestamp, 2);
    }

    #[test]
    fn test_bloom_filter_negative() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        create_test_sstable(
            &path,
            vec![(Bytes::from("existing_key"), Some(Bytes::from("value")), 1)],
        )
        .unwrap();

        let reader = SSTableReader::open(&path).unwrap();

        // Bloom filter should say the key is not present
        assert!(!reader.may_contain(b"definitely_not_here_12345"));
    }

    #[test]
    fn test_iterator() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let input_records: Vec<_> = (0..50)
            .map(|i| {
                (
                    Bytes::from(format!("key{:05}", i)),
                    Some(Bytes::from(format!("value{}", i))),
                    i as u64,
                )
            })
            .collect();

        create_test_sstable(&path, input_records.clone()).unwrap();

        let reader = SSTableReader::open(&path).unwrap();
        let records: Vec<_> = reader.iter().collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(records.len(), 50);

        // Verify sorted order
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.key, Bytes::from(format!("key{:05}", i)));
        }
    }

    #[test]
    fn test_with_compression() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mut writer = SSTableWriter::new(&path, Compression::Zstd).unwrap();
        for i in 0..100 {
            writer
                .add(
                    Bytes::from(format!("key{:05}", i)),
                    Some(Bytes::from(format!("value{}", i).repeat(10))),
                    i as u64,
                )
                .unwrap();
        }
        writer.finish().unwrap();

        let reader = SSTableReader::open(&path).unwrap();

        let record = reader.get(b"key00050").unwrap().unwrap();
        assert_eq!(record.value.unwrap(), Bytes::from("value50".repeat(10)));
    }

    #[test]
    fn test_with_block_cache() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        create_test_sstable(
            &path,
            vec![
                (Bytes::from("key1"), Some(Bytes::from("value1")), 1),
                (Bytes::from("key2"), Some(Bytes::from("value2")), 2),
            ],
        )
        .unwrap();

        let cache = Arc::new(BlockCache::new(1024 * 1024));
        let reader = SSTableReader::open_with_cache(&path, Some(Arc::clone(&cache)), 0).unwrap();

        // First access loads into cache
        let _ = reader.get(b"key1").unwrap();
        assert!(cache.len() > 0);

        // Second access should hit cache
        let _ = reader.get(b"key1").unwrap();
    }
}
