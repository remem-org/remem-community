//! B+Tree index for time-series and range queries
#![allow(dead_code)]
//!
//! This module implements a B+Tree index optimized for:
//! - Range queries: Find all records in a timestamp range
//! - Point lookups: Find records at a specific timestamp
//! - Ordered iteration: Iterate records in timestamp order
//!
//! # Design
//!
//! The B+Tree stores (timestamp, key) pairs where:
//! - timestamp: u64 representing time (e.g., Unix timestamp)
//! - key: Bytes reference to the actual record in storage
//!
//! All data is stored in leaf nodes, with internal nodes containing
//! only routing keys for efficient navigation.

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::engine::error::{Result, StorageError};

/// Configuration for the B+Tree index
#[derive(Debug, Clone)]
pub struct BTreeConfig {
    /// Maximum number of entries to pre-allocate
    pub max_entries: usize,
}

impl Default for BTreeConfig {
    fn default() -> Self {
        Self {
            max_entries: 10_000_000,
        }
    }
}

impl BTreeConfig {
    /// Create a new config with custom max entries
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self { max_entries }
    }
}

/// A value stored in the B+Tree (can have multiple keys per timestamp)
#[derive(Debug, Clone)]
struct TimestampEntry {
    /// The storage key(s) associated with this timestamp
    keys: Vec<Bytes>,
}

impl TimestampEntry {
    fn new(key: Bytes) -> Self {
        Self { keys: vec![key] }
    }

    fn add(&mut self, key: Bytes) {
        self.keys.push(key);
    }

    fn remove(&mut self, key: &[u8]) -> bool {
        if let Some(pos) = self.keys.iter().position(|k| k.as_ref() == key) {
            self.keys.swap_remove(pos);
            true
        } else {
            false
        }
    }
}

/// B+Tree index for timestamp-based queries
///
/// This uses Rust's standard BTreeMap internally, which provides
/// efficient O(log n) operations and cache-friendly iteration.
pub struct BTreeIndex {
    /// Configuration
    config: BTreeConfig,

    /// Main index: timestamp -> list of keys
    /// Using RwLock for concurrent access
    tree: RwLock<BTreeMap<u64, TimestampEntry>>,

    /// Reverse index: key -> timestamp (for efficient deletion)
    key_to_timestamp: RwLock<std::collections::HashMap<Bytes, u64>>,

    /// Number of entries in the index
    entry_count: AtomicUsize,

    /// Whether the index has been modified since last save
    dirty: AtomicBool,
}

impl BTreeIndex {
    /// Create a new empty B+Tree index
    pub fn new(config: BTreeConfig) -> Self {
        Self {
            config,
            tree: RwLock::new(BTreeMap::new()),
            key_to_timestamp: RwLock::new(std::collections::HashMap::new()),
            entry_count: AtomicUsize::new(0),
            dirty: AtomicBool::new(false),
        }
    }

    /// Insert a timestamp-key pair
    pub fn insert(&self, timestamp: u64, key: impl Into<Bytes>) -> Result<()> {
        let key = key.into();

        // Update reverse index first
        let is_new_key = {
            let mut key_to_ts = self.key_to_timestamp.write();
            if let Some(&old_ts) = key_to_ts.get(&key) {
                // Key already exists, need to update
                if old_ts != timestamp {
                    // Remove from old timestamp
                    let mut tree = self.tree.write();
                    if let Some(entry) = tree.get_mut(&old_ts) {
                        entry.remove(&key);
                        if entry.keys.is_empty() {
                            tree.remove(&old_ts);
                        }
                    }
                    key_to_ts.insert(key.clone(), timestamp);
                    false // Not a new key, just updating timestamp
                } else {
                    // Same timestamp, nothing to do
                    return Ok(());
                }
            } else {
                key_to_ts.insert(key.clone(), timestamp);
                true // New key
            }
        };

        // Insert into main tree
        {
            let mut tree = self.tree.write();
            tree.entry(timestamp)
                .and_modify(|e| e.add(key.clone()))
                .or_insert_with(|| TimestampEntry::new(key));
        }

        if is_new_key {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }
        self.dirty.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Remove a key from the index
    pub fn remove(&self, key: &[u8]) -> Result<bool> {
        let timestamp = {
            let mut key_to_ts = self.key_to_timestamp.write();
            match key_to_ts.remove(key) {
                Some(ts) => ts,
                None => return Ok(false),
            }
        };

        let mut tree = self.tree.write();
        if let Some(entry) = tree.get_mut(&timestamp) {
            if entry.remove(key) {
                if entry.keys.is_empty() {
                    tree.remove(&timestamp);
                }
                self.entry_count.fetch_sub(1, Ordering::Relaxed);
                self.dirty.store(true, Ordering::Relaxed);
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get all keys at a specific timestamp
    pub fn get(&self, timestamp: u64) -> Vec<Bytes> {
        let tree = self.tree.read();
        tree.get(&timestamp)
            .map(|e| e.keys.clone())
            .unwrap_or_default()
    }

    /// Get the timestamp for a key
    pub fn get_timestamp(&self, key: &[u8]) -> Option<u64> {
        let key_to_ts = self.key_to_timestamp.read();
        key_to_ts.get(key).copied()
    }

    /// Query a range of timestamps (inclusive)
    ///
    /// Returns (timestamp, key) pairs sorted by timestamp.
    pub fn range(&self, start: u64, end: u64) -> Vec<(u64, Bytes)> {
        let tree = self.tree.read();
        let mut results = Vec::new();

        for (&ts, entry) in tree.range(start..=end) {
            for key in &entry.keys {
                results.push((ts, key.clone()));
            }
        }

        results
    }

    /// Query a range of timestamps with a limit
    pub fn range_limit(&self, start: u64, end: u64, limit: usize) -> Vec<(u64, Bytes)> {
        let tree = self.tree.read();
        let mut results = Vec::with_capacity(limit);

        for (&ts, entry) in tree.range(start..=end) {
            for key in &entry.keys {
                results.push((ts, key.clone()));
                if results.len() >= limit {
                    return results;
                }
            }
        }

        results
    }

    /// Query timestamps before a given value
    pub fn before(&self, timestamp: u64, limit: usize) -> Vec<(u64, Bytes)> {
        let tree = self.tree.read();
        let mut results = Vec::with_capacity(limit);

        // Iterate in reverse order (most recent first)
        for (&ts, entry) in tree.range(..timestamp).rev() {
            for key in &entry.keys {
                results.push((ts, key.clone()));
                if results.len() >= limit {
                    return results;
                }
            }
        }

        results
    }

    /// Query timestamps after a given value
    pub fn after(&self, timestamp: u64, limit: usize) -> Vec<(u64, Bytes)> {
        let tree = self.tree.read();
        let mut results = Vec::with_capacity(limit);

        for (&ts, entry) in tree.range((Bound::Excluded(timestamp), Bound::Unbounded)) {
            for key in &entry.keys {
                results.push((ts, key.clone()));
                if results.len() >= limit {
                    return results;
                }
            }
        }

        results
    }

    /// Get the most recent entries
    pub fn latest(&self, limit: usize) -> Vec<(u64, Bytes)> {
        let tree = self.tree.read();
        let mut results = Vec::with_capacity(limit);

        for (&ts, entry) in tree.iter().rev() {
            for key in &entry.keys {
                results.push((ts, key.clone()));
                if results.len() >= limit {
                    return results;
                }
            }
        }

        results
    }

    /// Get the oldest entries
    pub fn oldest(&self, limit: usize) -> Vec<(u64, Bytes)> {
        let tree = self.tree.read();
        let mut results = Vec::with_capacity(limit);

        for (&ts, entry) in tree.iter() {
            for key in &entry.keys {
                results.push((ts, key.clone()));
                if results.len() >= limit {
                    return results;
                }
            }
        }

        results
    }

    /// Get the minimum timestamp in the index
    pub fn min_timestamp(&self) -> Option<u64> {
        let tree = self.tree.read();
        tree.keys().next().copied()
    }

    /// Get the maximum timestamp in the index
    pub fn max_timestamp(&self) -> Option<u64> {
        let tree = self.tree.read();
        tree.keys().next_back().copied()
    }

    /// Get the number of unique timestamps
    pub fn timestamp_count(&self) -> usize {
        self.tree.read().len()
    }

    /// Get the total number of entries
    pub fn len(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if a key exists in the index
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.key_to_timestamp.read().contains_key(key)
    }

    /// Check if a timestamp exists in the index
    pub fn contains_timestamp(&self, timestamp: u64) -> bool {
        self.tree.read().contains_key(&timestamp)
    }

    /// Check if the index has been modified
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }

    /// Mark the index as clean
    pub fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Relaxed);
    }

    /// Save the index to a file.
    ///
    /// The read lock on `tree` is held only for the in-memory snapshot, not
    /// during disk I/O.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();

        // --- Snapshot while holding the read lock (fast, in-memory only) ---
        let (entry_count, snapshot) = {
            let tree = self.tree.read();
            let snap: Vec<(u64, Vec<Bytes>)> = tree
                .iter()
                .map(|(&ts, e)| (ts, e.keys.clone()))
                .collect();
            (self.entry_count.load(Ordering::Relaxed) as u64, snap)
            // read lock released here
        };

        // --- Write snapshot to disk without holding any lock ---
        let tmp_path = path.with_extension("tmp");
        let file = std::fs::File::create(&tmp_path)?;
        let mut writer = std::io::BufWriter::new(file);

        // Write header
        writer.write_all(b"BTIX")?;
        writer.write_all(&1u32.to_le_bytes())?;

        // Write config
        writer.write_all(&(self.config.max_entries as u64).to_le_bytes())?;

        // Write entry count
        writer.write_all(&entry_count.to_le_bytes())?;

        // Write tree data
        writer.write_all(&(snapshot.len() as u64).to_le_bytes())?;
        for (timestamp, keys) in &snapshot {
            writer.write_all(&timestamp.to_le_bytes())?;
            writer.write_all(&(keys.len() as u32).to_le_bytes())?;
            for key in keys {
                writer.write_all(&(key.len() as u32).to_le_bytes())?;
                writer.write_all(key)?;
            }
        }

        writer.flush()?;
        drop(writer);
        std::fs::rename(&tmp_path, path)?;
        self.mark_clean();
        Ok(())
    }

    /// Load an index from a file
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = std::fs::File::open(path)?;
        let mut file = std::io::BufReader::new(file);

        // Read and verify magic
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != b"BTIX" {
            return Err(StorageError::invalid_format(path, "Invalid B+Tree magic"));
        }

        // Read version
        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];

        file.read_exact(&mut buf4)?;
        let version = u32::from_le_bytes(buf4);
        if version != 1 {
            return Err(StorageError::invalid_format(
                path,
                format!("Unsupported B+Tree version: {}", version),
            ));
        }

        // Read config
        file.read_exact(&mut buf8)?;
        let max_entries = u64::from_le_bytes(buf8) as usize;
        let config = BTreeConfig { max_entries };

        // Read entry count
        file.read_exact(&mut buf8)?;
        let entry_count = u64::from_le_bytes(buf8) as usize;

        // Read timestamp count
        file.read_exact(&mut buf8)?;
        let timestamp_count = u64::from_le_bytes(buf8) as usize;

        let mut tree = BTreeMap::new();
        let mut key_to_timestamp = std::collections::HashMap::with_capacity(entry_count);

        for _ in 0..timestamp_count {
            file.read_exact(&mut buf8)?;
            let timestamp = u64::from_le_bytes(buf8);

            file.read_exact(&mut buf4)?;
            let key_count = u32::from_le_bytes(buf4) as usize;

            let mut keys = Vec::with_capacity(key_count);
            for _ in 0..key_count {
                file.read_exact(&mut buf4)?;
                let key_len = u32::from_le_bytes(buf4) as usize;

                let mut key_bytes = vec![0u8; key_len];
                file.read_exact(&mut key_bytes)?;
                let key = Bytes::from(key_bytes);

                key_to_timestamp.insert(key.clone(), timestamp);
                keys.push(key);
            }

            tree.insert(timestamp, TimestampEntry { keys });
        }

        Ok(Self {
            config,
            tree: RwLock::new(tree),
            key_to_timestamp: RwLock::new(key_to_timestamp),
            entry_count: AtomicUsize::new(entry_count),
            dirty: AtomicBool::new(false),
        })
    }

    /// Clear all entries from the index
    pub fn clear(&self) {
        self.tree.write().clear();
        self.key_to_timestamp.write().clear();
        self.entry_count.store(0, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_index() {
        let index = BTreeIndex::new(BTreeConfig::default());
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert!(index.get(100).is_empty());
    }

    #[test]
    fn test_insert_and_get() {
        let index = BTreeIndex::new(BTreeConfig::default());

        index.insert(1000, b"key1".to_vec()).unwrap();
        index.insert(2000, b"key2".to_vec()).unwrap();
        index.insert(1000, b"key3".to_vec()).unwrap(); // Same timestamp

        assert_eq!(index.len(), 3);
        assert_eq!(index.timestamp_count(), 2);

        let keys_at_1000 = index.get(1000);
        assert_eq!(keys_at_1000.len(), 2);

        let keys_at_2000 = index.get(2000);
        assert_eq!(keys_at_2000.len(), 1);
        assert_eq!(keys_at_2000[0].as_ref(), b"key2");
    }

    #[test]
    fn test_range_query() {
        let index = BTreeIndex::new(BTreeConfig::default());

        for ts in 0..10 {
            index.insert(ts * 100, format!("key{}", ts)).unwrap();
        }

        // Range [200, 500]
        let results = index.range(200, 500);
        assert_eq!(results.len(), 4); // 200, 300, 400, 500
        assert_eq!(results[0].0, 200);
        assert_eq!(results[3].0, 500);
    }

    #[test]
    fn test_range_limit() {
        let index = BTreeIndex::new(BTreeConfig::default());

        for ts in 0..100 {
            index.insert(ts, format!("key{}", ts)).unwrap();
        }

        let results = index.range_limit(0, 50, 10);
        assert_eq!(results.len(), 10);
    }

    #[test]
    fn test_before_and_after() {
        let index = BTreeIndex::new(BTreeConfig::default());

        for ts in (0..10).map(|i| i * 100) {
            index.insert(ts, format!("key{}", ts)).unwrap();
        }

        // Before 500 (exclusive)
        let before = index.before(500, 3);
        assert_eq!(before.len(), 3);
        assert_eq!(before[0].0, 400); // Most recent first
        assert_eq!(before[1].0, 300);
        assert_eq!(before[2].0, 200);

        // After 500 (exclusive)
        let after = index.after(500, 3);
        assert_eq!(after.len(), 3);
        assert_eq!(after[0].0, 600); // Oldest first
        assert_eq!(after[1].0, 700);
        assert_eq!(after[2].0, 800);
    }

    #[test]
    fn test_latest_and_oldest() {
        let index = BTreeIndex::new(BTreeConfig::default());

        for ts in 0..100 {
            index.insert(ts, format!("key{}", ts)).unwrap();
        }

        let latest = index.latest(5);
        assert_eq!(latest.len(), 5);
        assert_eq!(latest[0].0, 99);
        assert_eq!(latest[4].0, 95);

        let oldest = index.oldest(5);
        assert_eq!(oldest.len(), 5);
        assert_eq!(oldest[0].0, 0);
        assert_eq!(oldest[4].0, 4);
    }

    #[test]
    fn test_remove() {
        let index = BTreeIndex::new(BTreeConfig::default());

        index.insert(1000, b"key1".to_vec()).unwrap();
        index.insert(1000, b"key2".to_vec()).unwrap();
        index.insert(2000, b"key3".to_vec()).unwrap();

        assert_eq!(index.len(), 3);

        // Remove one key from a multi-key timestamp
        assert!(index.remove(b"key1").unwrap());
        assert_eq!(index.len(), 2);
        assert_eq!(index.get(1000).len(), 1);

        // Remove the last key from timestamp 1000
        assert!(index.remove(b"key2").unwrap());
        assert_eq!(index.len(), 1);
        assert!(index.get(1000).is_empty());
        assert_eq!(index.timestamp_count(), 1);

        // Try to remove non-existent key
        assert!(!index.remove(b"key999").unwrap());
    }

    #[test]
    fn test_update_timestamp() {
        let index = BTreeIndex::new(BTreeConfig::default());

        index.insert(1000, b"key1".to_vec()).unwrap();
        assert_eq!(index.get_timestamp(b"key1"), Some(1000));

        // Update timestamp for same key
        index.insert(2000, b"key1".to_vec()).unwrap();
        assert_eq!(index.get_timestamp(b"key1"), Some(2000));
        assert_eq!(index.len(), 1); // Count should not increase
        assert!(index.get(1000).is_empty());
        assert_eq!(index.get(2000).len(), 1);
    }

    #[test]
    fn test_min_max_timestamp() {
        let index = BTreeIndex::new(BTreeConfig::default());

        assert!(index.min_timestamp().is_none());
        assert!(index.max_timestamp().is_none());

        index.insert(500, b"key1".to_vec()).unwrap();
        index.insert(100, b"key2".to_vec()).unwrap();
        index.insert(900, b"key3".to_vec()).unwrap();

        assert_eq!(index.min_timestamp(), Some(100));
        assert_eq!(index.max_timestamp(), Some(900));
    }

    #[test]
    fn test_save_and_load() {
        let index = BTreeIndex::new(BTreeConfig::default());

        for ts in 0..50 {
            index.insert(ts * 100, format!("key{}", ts)).unwrap();
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.btree");

        index.save(&path).unwrap();

        let loaded = BTreeIndex::load(&path).unwrap();
        assert_eq!(loaded.len(), 50);
        assert_eq!(loaded.timestamp_count(), 50);

        // Verify data
        let results = loaded.range(0, 4900);
        assert_eq!(results.len(), 50);
    }

    #[test]
    fn test_contains() {
        let index = BTreeIndex::new(BTreeConfig::default());

        index.insert(1000, b"key1".to_vec()).unwrap();

        assert!(index.contains_key(b"key1"));
        assert!(!index.contains_key(b"key2"));
        assert!(index.contains_timestamp(1000));
        assert!(!index.contains_timestamp(2000));
    }

    #[test]
    fn test_clear() {
        let index = BTreeIndex::new(BTreeConfig::default());

        for ts in 0..100 {
            index.insert(ts, format!("key{}", ts)).unwrap();
        }

        assert_eq!(index.len(), 100);

        index.clear();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert_eq!(index.timestamp_count(), 0);
    }
}
