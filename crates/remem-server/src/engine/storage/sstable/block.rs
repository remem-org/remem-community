//! Block cache for SSTable data blocks
#![allow(dead_code)]

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use super::format::Record;

/// A parsed data block from an SSTable
#[derive(Debug, Clone)]
pub struct Block {
    /// Raw data of the block (decompressed)
    pub data: Bytes,
    /// Parsed records from the block (cached for efficiency)
    records: Vec<Record>,
}

impl Block {
    /// Parse a block from raw bytes
    pub fn parse(data: Bytes) -> Option<Self> {
        let mut records = Vec::new();
        let mut offset = 0;

        while offset < data.len() {
            match Record::decode(&data[offset..]) {
                Some((record, size)) => {
                    records.push(record);
                    offset += size;
                }
                None => break,
            }
        }

        Some(Self { data, records })
    }

    /// Get an iterator over the records in this block
    pub fn iter(&self) -> impl Iterator<Item = &Record> {
        self.records.iter()
    }

    /// Find a record by key
    pub fn get(&self, key: &[u8]) -> Option<&Record> {
        // Since records are sorted, we could use binary search
        // For now, linear search is fine for small blocks
        self.records.iter().find(|r| r.key.as_ref() == key)
    }

    /// Get the number of records in this block
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if the block is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Get the raw data size
    pub fn data_size(&self) -> usize {
        self.data.len()
    }
}

/// Cache key for blocks
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BlockCacheKey {
    /// Path to the SSTable file
    pub path: PathBuf,
    /// Offset of the block within the file
    pub offset: u64,
}

/// LRU cache for SSTable data blocks
///
/// The block cache helps reduce disk I/O by caching recently accessed
/// blocks in memory. It uses an LRU eviction policy.
pub struct BlockCache {
    /// The actual cache
    cache: Mutex<LruCache<BlockCacheKey, Arc<Block>>>,
    /// Maximum cache size in bytes
    max_size: usize,
    /// Current cache size in bytes
    current_size: Mutex<usize>,
}

impl BlockCache {
    /// Create a new block cache with the specified capacity in bytes
    pub fn new(capacity_bytes: usize) -> Self {
        // Estimate number of entries based on average block size (4KB)
        let estimated_entries = capacity_bytes / 4096;
        let cache_size = NonZeroUsize::new(estimated_entries.max(16)).unwrap();

        Self {
            cache: Mutex::new(LruCache::new(cache_size)),
            max_size: capacity_bytes,
            current_size: Mutex::new(0),
        }
    }

    /// Get a block from the cache
    pub fn get(&self, key: &BlockCacheKey) -> Option<Arc<Block>> {
        let mut cache = self.cache.lock();
        cache.get(key).cloned()
    }

    /// Insert a block into the cache
    pub fn insert(&self, key: BlockCacheKey, block: Arc<Block>) {
        let block_size = block.data_size();

        let mut cache = self.cache.lock();
        let mut current_size = self.current_size.lock();

        // Evict entries if needed to make room
        while *current_size + block_size > self.max_size && !cache.is_empty() {
            if let Some((_, evicted)) = cache.pop_lru() {
                *current_size = current_size.saturating_sub(evicted.data_size());
            }
        }

        // Only insert if it fits
        if block_size <= self.max_size {
            if let Some(old) = cache.put(key, block) {
                *current_size = current_size.saturating_sub(old.data_size());
            }
            *current_size += block_size;
        }
    }

    /// Get or load a block
    ///
    /// If the block is in the cache, return it. Otherwise, call the loader
    /// function to load it, cache it, and return it.
    pub fn get_or_load<F>(&self, key: BlockCacheKey, loader: F) -> Option<Arc<Block>>
    where
        F: FnOnce() -> Option<Block>,
    {
        // Check cache first
        if let Some(block) = self.get(&key) {
            return Some(block);
        }

        // Load the block
        let block = loader()?;
        let block = Arc::new(block);

        // Insert into cache
        self.insert(key, Arc::clone(&block));

        Some(block)
    }

    /// Clear the cache
    pub fn clear(&self) {
        let mut cache = self.cache.lock();
        let mut current_size = self.current_size.lock();
        cache.clear();
        *current_size = 0;
    }

    /// Get the current cache size in bytes
    pub fn size(&self) -> usize {
        *self.current_size.lock()
    }

    /// Get the number of cached blocks
    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.lock().is_empty()
    }

    /// Get cache statistics
    pub fn stats(&self) -> BlockCacheStats {
        let cache = self.cache.lock();
        BlockCacheStats {
            entries: cache.len(),
            size_bytes: *self.current_size.lock(),
            max_size_bytes: self.max_size,
        }
    }
}

/// Statistics about the block cache
#[derive(Debug, Clone)]
pub struct BlockCacheStats {
    /// Number of entries in the cache
    pub entries: usize,
    /// Current size in bytes
    pub size_bytes: usize,
    /// Maximum size in bytes
    pub max_size_bytes: usize,
}

impl Default for BlockCache {
    fn default() -> Self {
        // Default to 64 MB cache
        Self::new(64 * 1024 * 1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_parse() {
        // Create some records
        let records = vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1"), 1),
            Record::new(Bytes::from("key2"), Bytes::from("value2"), 2),
            Record::tombstone(Bytes::from("key3"), 3),
        ];

        // Encode them
        let mut data = Vec::new();
        for record in &records {
            data.extend_from_slice(&record.encode());
        }

        // Parse the block
        let block = Block::parse(Bytes::from(data)).unwrap();

        assert_eq!(block.len(), 3);

        let r1 = block.get(b"key1").unwrap();
        assert_eq!(r1.value.as_ref().unwrap().as_ref(), b"value1");

        let r3 = block.get(b"key3").unwrap();
        assert!(r3.is_tombstone());
    }

    #[test]
    fn test_block_cache_basic() {
        let cache = BlockCache::new(1024 * 1024); // 1 MB

        let key = BlockCacheKey {
            path: PathBuf::from("/test/file.sst"),
            offset: 4096,
        };

        // Initially empty
        assert!(cache.get(&key).is_none());

        // Insert a block
        let records = vec![Record::new(Bytes::from("key"), Bytes::from("value"), 1)];
        let mut data = Vec::new();
        for record in &records {
            data.extend_from_slice(&record.encode());
        }
        let block = Block::parse(Bytes::from(data)).unwrap();

        cache.insert(key.clone(), Arc::new(block));

        // Now it should be found
        let cached = cache.get(&key).unwrap();
        assert_eq!(cached.len(), 1);
    }

    #[test]
    fn test_block_cache_eviction() {
        // Small cache that can only hold ~2 blocks
        let cache = BlockCache::new(200);

        let make_block = |size: usize| {
            let data = vec![0u8; size];
            Block {
                data: Bytes::from(data),
                records: Vec::new(),
            }
        };

        // Insert first block (100 bytes)
        let key1 = BlockCacheKey {
            path: PathBuf::from("/test/file.sst"),
            offset: 0,
        };
        cache.insert(key1.clone(), Arc::new(make_block(100)));

        // Insert second block (100 bytes)
        let key2 = BlockCacheKey {
            path: PathBuf::from("/test/file.sst"),
            offset: 100,
        };
        cache.insert(key2.clone(), Arc::new(make_block(100)));

        // Both should be cached
        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key2).is_some());

        // Insert third block, should evict one
        let key3 = BlockCacheKey {
            path: PathBuf::from("/test/file.sst"),
            offset: 200,
        };
        cache.insert(key3.clone(), Arc::new(make_block(100)));

        // Third should be cached
        assert!(cache.get(&key3).is_some());

        // At least one of the first two should be evicted
        let first_two_cached =
            cache.get(&key1).is_some() as i32 + cache.get(&key2).is_some() as i32;
        assert!(first_two_cached <= 1);
    }

    #[test]
    fn test_get_or_load() {
        let cache = BlockCache::new(1024 * 1024);

        let key = BlockCacheKey {
            path: PathBuf::from("/test/file.sst"),
            offset: 0,
        };

        let mut load_count = 0;

        // First call should load
        let block = cache.get_or_load(key.clone(), || {
            load_count += 1;
            let record = Record::new(Bytes::from("k"), Bytes::from("v"), 1);
            Block::parse(Bytes::from(record.encode()))
        });
        assert!(block.is_some());
        assert_eq!(load_count, 1);

        // Second call should use cache
        let block2 = cache.get_or_load(key.clone(), || {
            load_count += 1;
            None // Shouldn't be called
        });
        assert!(block2.is_some());
        assert_eq!(load_count, 1); // Still 1, loader not called
    }
}
