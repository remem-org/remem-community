//! MemTable: In-memory sorted storage using a concurrent skip list
#![allow(dead_code)]
//!
//! The MemTable is the write buffer in the LSM-tree. All writes go to the
//! MemTable first (after being logged to the WAL), and once it reaches
//! capacity, it becomes immutable and is flushed to an SSTable on disk.

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::engine::error::{Result, StorageError};

/// Default maximum size for a MemTable (256 MB)
pub const DEFAULT_MEMTABLE_SIZE: usize = 256 * 1024 * 1024;

/// Entry in the MemTable
#[derive(Debug, Clone)]
pub struct Entry {
    /// Value bytes (None indicates a tombstone/deletion)
    pub value: Option<Bytes>,
    /// Timestamp for versioning (monotonically increasing)
    pub timestamp: u64,
}

impl Entry {
    /// Create a new entry with a value
    pub fn new(value: Bytes, timestamp: u64) -> Self {
        Self {
            value: Some(value),
            timestamp,
        }
    }

    /// Create a tombstone entry (for deletions)
    pub fn tombstone(timestamp: u64) -> Self {
        Self {
            value: None,
            timestamp,
        }
    }

    /// Check if this entry is a tombstone
    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// Get the approximate size of this entry in memory
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.value.as_ref().map(|v| v.len()).unwrap_or(0)
    }
}

/// In-memory sorted storage using a concurrent skip list
///
/// The MemTable provides:
/// - O(log n) insert and lookup operations
/// - Concurrent read/write access without global locks
/// - Ordered iteration for efficient SSTable flushing
#[derive(Debug)]
pub struct MemTable {
    /// Concurrent skip list storing key-value entries
    data: SkipMap<Bytes, Entry>,
    /// Approximate current size in bytes
    size: AtomicUsize,
    /// Maximum size before the MemTable should be flushed
    max_size: usize,
    /// Next timestamp for entries
    next_timestamp: AtomicU64,
    /// Number of entries in the MemTable
    entry_count: AtomicUsize,
}

impl MemTable {
    /// Create a new MemTable with default size limit
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MEMTABLE_SIZE)
    }

    /// Create a new MemTable with specified size limit
    pub fn with_capacity(max_size: usize) -> Self {
        Self {
            data: SkipMap::new(),
            size: AtomicUsize::new(0),
            max_size,
            next_timestamp: AtomicU64::new(1),
            entry_count: AtomicUsize::new(0),
        }
    }

    /// Insert a key-value pair into the MemTable
    ///
    /// Returns an error if the MemTable is full. The caller should check
    /// `is_full()` before inserting or handle the error by rotating to a
    /// new MemTable.
    pub fn insert(&self, key: Bytes, value: Bytes) -> Result<u64> {
        let entry_size = key.len() + value.len() + std::mem::size_of::<Entry>();
        let current_size = self.size.load(Ordering::Relaxed);

        if current_size + entry_size > self.max_size {
            return Err(StorageError::MemTableFull {
                current: current_size,
                max: self.max_size,
            });
        }

        let timestamp = self.next_timestamp.fetch_add(1, Ordering::SeqCst);
        let entry = Entry::new(value, timestamp);

        // Check if we're replacing an existing entry
        if let Some(old_entry) = self.data.get(&key) {
            let old_size = old_entry.value().size() + key.len();
            self.size.fetch_sub(old_size, Ordering::Relaxed);
        } else {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }

        self.data.insert(key, entry);
        self.size.fetch_add(entry_size, Ordering::Relaxed);

        Ok(timestamp)
    }

    /// Insert an entry with a specific timestamp (used during WAL replay)
    pub fn insert_with_timestamp(&self, key: Bytes, value: Bytes, timestamp: u64) -> Result<()> {
        let entry_size = key.len() + value.len() + std::mem::size_of::<Entry>();
        let current_size = self.size.load(Ordering::Relaxed);

        if current_size + entry_size > self.max_size {
            return Err(StorageError::MemTableFull {
                current: current_size,
                max: self.max_size,
            });
        }

        let entry = Entry::new(value, timestamp);

        if let Some(old_entry) = self.data.get(&key) {
            // Only replace if new timestamp is greater
            if old_entry.value().timestamp >= timestamp {
                return Ok(());
            }
            let old_size = old_entry.value().size() + key.len();
            self.size.fetch_sub(old_size, Ordering::Relaxed);
        } else {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }

        self.data.insert(key, entry);
        self.size.fetch_add(entry_size, Ordering::Relaxed);

        // Update next_timestamp if needed
        let mut current_next = self.next_timestamp.load(Ordering::Relaxed);
        while current_next <= timestamp {
            match self.next_timestamp.compare_exchange_weak(
                current_next,
                timestamp + 1,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_next = actual,
            }
        }

        Ok(())
    }

    /// Mark a key as deleted (insert a tombstone)
    pub fn delete(&self, key: Bytes) -> Result<u64> {
        let entry_size = key.len() + std::mem::size_of::<Entry>();
        let current_size = self.size.load(Ordering::Relaxed);

        if current_size + entry_size > self.max_size {
            return Err(StorageError::MemTableFull {
                current: current_size,
                max: self.max_size,
            });
        }

        let timestamp = self.next_timestamp.fetch_add(1, Ordering::SeqCst);
        let entry = Entry::tombstone(timestamp);

        if let Some(old_entry) = self.data.get(&key) {
            let old_size = old_entry.value().size() + key.len();
            self.size.fetch_sub(old_size, Ordering::Relaxed);
        } else {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }

        self.data.insert(key, entry);
        self.size.fetch_add(entry_size, Ordering::Relaxed);

        Ok(timestamp)
    }

    /// Mark a key as deleted with a specific timestamp (used during WAL replay)
    pub fn delete_with_timestamp(&self, key: Bytes, timestamp: u64) -> Result<()> {
        let entry_size = key.len() + std::mem::size_of::<Entry>();
        let current_size = self.size.load(Ordering::Relaxed);

        if current_size + entry_size > self.max_size {
            return Err(StorageError::MemTableFull {
                current: current_size,
                max: self.max_size,
            });
        }

        let entry = Entry::tombstone(timestamp);

        if let Some(old_entry) = self.data.get(&key) {
            if old_entry.value().timestamp >= timestamp {
                return Ok(());
            }
            let old_size = old_entry.value().size() + key.len();
            self.size.fetch_sub(old_size, Ordering::Relaxed);
        } else {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }

        self.data.insert(key, entry);
        self.size.fetch_add(entry_size, Ordering::Relaxed);

        // Update next_timestamp if needed
        let mut current_next = self.next_timestamp.load(Ordering::Relaxed);
        while current_next <= timestamp {
            match self.next_timestamp.compare_exchange_weak(
                current_next,
                timestamp + 1,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_next = actual,
            }
        }

        Ok(())
    }

    /// Get the value for a key
    ///
    /// Returns `None` if the key doesn't exist. Returns `Some(Entry)` if
    /// the key exists - check `entry.is_tombstone()` to see if it was deleted.
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        self.data.get(key).map(|e| e.value().clone())
    }

    /// Check if the MemTable is full
    pub fn is_full(&self) -> bool {
        self.size.load(Ordering::Relaxed) >= self.max_size
    }

    /// Get the approximate current size in bytes
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Get the maximum size
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Check if the MemTable is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get an iterator over all entries in sorted order
    pub fn iter(&self) -> impl Iterator<Item = (Bytes, Entry)> + '_ {
        self.data
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
    }

    /// Get the current timestamp
    pub fn current_timestamp(&self) -> u64 {
        self.next_timestamp.load(Ordering::Relaxed)
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

/// A read-only view of a MemTable that has been made immutable
#[derive(Debug)]
pub struct ImmutableMemTable {
    inner: Arc<MemTable>,
}

impl ImmutableMemTable {
    /// Create an immutable view from a MemTable
    pub fn from_memtable(memtable: MemTable) -> Self {
        Self {
            inner: Arc::new(memtable),
        }
    }

    /// Get the value for a key
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        self.inner.get(key)
    }

    /// Get an iterator over all entries in sorted order
    pub fn iter(&self) -> impl Iterator<Item = (Bytes, Entry)> + '_ {
        self.inner.iter()
    }

    /// Get the approximate size in bytes
    pub fn size(&self) -> usize {
        self.inner.size()
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl Clone for ImmutableMemTable {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let memtable = MemTable::new();

        memtable
            .insert(Bytes::from("key1"), Bytes::from("value1"))
            .unwrap();
        memtable
            .insert(Bytes::from("key2"), Bytes::from("value2"))
            .unwrap();

        let entry1 = memtable.get(b"key1").unwrap();
        assert_eq!(entry1.value.as_ref().unwrap().as_ref(), b"value1");
        assert!(!entry1.is_tombstone());

        let entry2 = memtable.get(b"key2").unwrap();
        assert_eq!(entry2.value.as_ref().unwrap().as_ref(), b"value2");

        assert!(memtable.get(b"key3").is_none());
    }

    #[test]
    fn test_update() {
        let memtable = MemTable::new();

        memtable
            .insert(Bytes::from("key1"), Bytes::from("value1"))
            .unwrap();
        let ts1 = memtable.get(b"key1").unwrap().timestamp;

        memtable
            .insert(Bytes::from("key1"), Bytes::from("value2"))
            .unwrap();
        let entry = memtable.get(b"key1").unwrap();

        assert_eq!(entry.value.as_ref().unwrap().as_ref(), b"value2");
        assert!(entry.timestamp > ts1);
    }

    #[test]
    fn test_delete() {
        let memtable = MemTable::new();

        memtable
            .insert(Bytes::from("key1"), Bytes::from("value1"))
            .unwrap();
        memtable.delete(Bytes::from("key1")).unwrap();

        let entry = memtable.get(b"key1").unwrap();
        assert!(entry.is_tombstone());
    }

    #[test]
    fn test_iterator_ordered() {
        let memtable = MemTable::new();

        // Insert in random order
        memtable.insert(Bytes::from("c"), Bytes::from("3")).unwrap();
        memtable.insert(Bytes::from("a"), Bytes::from("1")).unwrap();
        memtable.insert(Bytes::from("b"), Bytes::from("2")).unwrap();

        // Should iterate in sorted order
        let keys: Vec<_> = memtable.iter().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
        );
    }

    #[test]
    fn test_size_tracking() {
        let memtable = MemTable::with_capacity(1000);

        assert_eq!(memtable.size(), 0);

        memtable
            .insert(Bytes::from("key1"), Bytes::from("value1"))
            .unwrap();
        let size1 = memtable.size();
        assert!(size1 > 0);

        memtable
            .insert(Bytes::from("key2"), Bytes::from("value2"))
            .unwrap();
        let size2 = memtable.size();
        assert!(size2 > size1);
    }

    #[test]
    fn test_memtable_full() {
        let memtable = MemTable::with_capacity(100);

        // Keep inserting until full
        let mut i = 0;
        loop {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            match memtable.insert(Bytes::from(key), Bytes::from(value)) {
                Ok(_) => i += 1,
                Err(StorageError::MemTableFull { .. }) => break,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        assert!(memtable.is_full());
    }

    #[test]
    fn test_timestamp_ordering() {
        let memtable = MemTable::new();

        let ts1 = memtable
            .insert(Bytes::from("key1"), Bytes::from("v1"))
            .unwrap();
        let ts2 = memtable
            .insert(Bytes::from("key2"), Bytes::from("v2"))
            .unwrap();
        let ts3 = memtable.delete(Bytes::from("key3")).unwrap();

        assert!(ts1 < ts2);
        assert!(ts2 < ts3);
    }

    #[test]
    fn test_immutable_memtable() {
        let memtable = MemTable::new();
        memtable
            .insert(Bytes::from("key1"), Bytes::from("value1"))
            .unwrap();

        let immutable = ImmutableMemTable::from_memtable(memtable);

        let entry = immutable.get(b"key1").unwrap();
        assert_eq!(entry.value.as_ref().unwrap().as_ref(), b"value1");
    }
}
