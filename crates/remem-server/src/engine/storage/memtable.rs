//! MemTable: In-memory sorted storage using a concurrent skip list
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
    /// The store's record-version counter.
    ///
    /// Shared rather than owned: a rotation builds the replacement memtable
    /// from this same handle, so a version issued after the rotation still
    /// sorts after one issued before it. Every site that resolves two copies
    /// of a key -- compaction's merge, `insert_with_timestamp` below, WAL
    /// replay -- decides by this number, so a counter that restarted per
    /// memtable made a later write look older and let the stale copy win.
    next_timestamp: Arc<AtomicU64>,
    /// Number of entries in the MemTable
    entry_count: AtomicUsize,
}

impl MemTable {
    /// Create a new MemTable with default size limit
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MEMTABLE_SIZE)
    }

    /// Create a new MemTable with specified size limit, on a counter of its
    /// own. Suitable for a standalone table; the engine uses
    /// [`MemTable::with_sequence`] so its tables share one order.
    pub fn with_capacity(max_size: usize) -> Self {
        Self::with_sequence(max_size, Arc::new(AtomicU64::new(1)))
    }

    /// Create a MemTable that draws record versions from an existing counter.
    ///
    /// This is how a rotation preserves the order: the replacement inherits
    /// the counter of the table it replaces instead of starting over.
    pub fn with_sequence(max_size: usize, sequence: Arc<AtomicU64>) -> Self {
        Self {
            data: SkipMap::new(),
            size: AtomicUsize::new(0),
            max_size,
            next_timestamp: sequence,
            entry_count: AtomicUsize::new(0),
        }
    }

    /// A handle on the counter this table issues versions from, for building
    /// its replacement.
    pub fn sequence(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.next_timestamp)
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

    /// Atomically reserve the next timestamp for this MemTable's write
    /// sequence, without inserting anything. Callers use this to assign a
    /// timestamp *before* releasing the WAL lock, so the WAL's on-disk
    /// order for a key always matches the order `insert_with_timestamp`/
    /// `delete_with_timestamp` will apply it in — unlike `current_timestamp()`,
    /// which only peeks the counter and lets a later, unlocked
    /// `insert_with_timestamp()`/`delete_with_timestamp()` assign the real value.
    pub fn reserve_timestamp(&self) -> u64 {
        self.next_timestamp.fetch_add(1, Ordering::SeqCst)
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

    /// Test-only stand-in for the old `MemTable::insert` (deleted as dead code
    /// in REM-36 -- production code only ever goes through
    /// `insert_with_timestamp`, reserving the timestamp under the WAL lock
    /// first). Mirrors `insert`'s old behavior exactly: `reserve_timestamp`
    /// always hands back a strictly increasing counter value, so
    /// `insert_with_timestamp`'s "only replace if new timestamp is greater"
    /// guard never actually skips a write here.
    fn insert(mt: &MemTable, key: Bytes, value: Bytes) -> u64 {
        let ts = mt.reserve_timestamp();
        mt.insert_with_timestamp(key, value, ts).unwrap();
        ts
    }

    /// Test-only stand-in for the old `MemTable::delete` -- see `insert` above.
    fn delete(mt: &MemTable, key: Bytes) -> u64 {
        let ts = mt.reserve_timestamp();
        mt.delete_with_timestamp(key, ts).unwrap();
        ts
    }

    #[test]
    fn test_insert_and_get() {
        let memtable = MemTable::new();

        insert(&memtable, Bytes::from("key1"), Bytes::from("value1"));
        insert(&memtable, Bytes::from("key2"), Bytes::from("value2"));

        let entry1 = memtable.get(b"key1").unwrap();
        assert_eq!(entry1.value.as_ref().unwrap().as_ref(), b"value1");
        assert!(entry1.value.is_some());

        let entry2 = memtable.get(b"key2").unwrap();
        assert_eq!(entry2.value.as_ref().unwrap().as_ref(), b"value2");

        assert!(memtable.get(b"key3").is_none());
    }

    #[test]
    fn test_update() {
        let memtable = MemTable::new();

        insert(&memtable, Bytes::from("key1"), Bytes::from("value1"));
        let ts1 = memtable.get(b"key1").unwrap().timestamp;

        insert(&memtable, Bytes::from("key1"), Bytes::from("value2"));
        let entry = memtable.get(b"key1").unwrap();

        assert_eq!(entry.value.as_ref().unwrap().as_ref(), b"value2");
        assert!(entry.timestamp > ts1);
    }

    #[test]
    fn test_delete() {
        let memtable = MemTable::new();

        insert(&memtable, Bytes::from("key1"), Bytes::from("value1"));
        delete(&memtable, Bytes::from("key1"));

        let entry = memtable.get(b"key1").unwrap();
        assert!(entry.value.is_none());
    }

    #[test]
    fn test_iterator_ordered() {
        let memtable = MemTable::new();

        // Insert in random order
        insert(&memtable, Bytes::from("c"), Bytes::from("3"));
        insert(&memtable, Bytes::from("a"), Bytes::from("1"));
        insert(&memtable, Bytes::from("b"), Bytes::from("2"));

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

        insert(&memtable, Bytes::from("key1"), Bytes::from("value1"));
        let size1 = memtable.size();
        assert!(size1 > 0);

        insert(&memtable, Bytes::from("key2"), Bytes::from("value2"));
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
            let ts = memtable.reserve_timestamp();
            match memtable.insert_with_timestamp(Bytes::from(key), Bytes::from(value), ts) {
                Ok(()) => i += 1,
                Err(StorageError::MemTableFull { .. }) => break,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        assert!(memtable.is_full());
    }

    #[test]
    fn test_timestamp_ordering() {
        let memtable = MemTable::new();

        let ts1 = insert(&memtable, Bytes::from("key1"), Bytes::from("v1"));
        let ts2 = insert(&memtable, Bytes::from("key2"), Bytes::from("v2"));
        let ts3 = delete(&memtable, Bytes::from("key3"));

        assert!(ts1 < ts2);
        assert!(ts2 < ts3);
    }

    #[test]
    fn test_immutable_memtable() {
        let memtable = MemTable::new();
        insert(&memtable, Bytes::from("key1"), Bytes::from("value1"));

        let immutable = ImmutableMemTable::from_memtable(memtable);

        let entry = immutable.get(b"key1").unwrap();
        assert_eq!(entry.value.as_ref().unwrap().as_ref(), b"value1");
    }

    /// The admission rule is left exactly as it was — a version admits only if
    /// it exceeds the one already held — because the fix for reverted writes
    /// was to make the numbers order writes, not to special-case this site.
    ///
    /// Correct only while versions come from one counter, which is what
    /// `a_rotation_keeps_issuing_from_the_same_counter` below covers.
    #[test]
    fn a_lower_version_does_not_displace_a_higher_one() {
        let memtable = MemTable::new();
        let key = Bytes::from_static(b"k");

        memtable
            .insert_with_timestamp(key.clone(), Bytes::from_static(b"higher"), 5000)
            .unwrap();
        memtable
            .insert_with_timestamp(key.clone(), Bytes::from_static(b"lower"), 3)
            .unwrap();

        let held = memtable.get(&key).unwrap();
        assert_eq!(
            held.value.as_ref().unwrap().as_ref(),
            b"higher",
            "a smaller version must not displace a larger one"
        );
    }

    #[test]
    fn a_higher_version_displaces_a_lower_one() {
        let memtable = MemTable::new();
        let key = Bytes::from_static(b"k");

        memtable
            .insert_with_timestamp(key.clone(), Bytes::from_static(b"lower"), 3)
            .unwrap();
        memtable
            .insert_with_timestamp(key.clone(), Bytes::from_static(b"higher"), 5000)
            .unwrap();

        let held = memtable.get(&key).unwrap();
        assert_eq!(held.value.as_ref().unwrap().as_ref(), b"higher");
    }

    /// A rotation builds the replacement from the outgoing table's counter, so
    /// the first version the new table issues is greater than the last the old
    /// one issued. Without this, "higher wins" above silently means "older
    /// wins" for every write that follows a rotation.
    #[test]
    fn a_rotation_keeps_issuing_from_the_same_counter() {
        let first = MemTable::new();
        let last_before = (0..8).map(|_| first.reserve_timestamp()).last().unwrap();

        let second = MemTable::with_sequence(DEFAULT_MEMTABLE_SIZE, first.sequence());
        let first_after = second.reserve_timestamp();

        assert!(
            first_after > last_before,
            "a version issued after a rotation ({first_after}) must exceed one issued before it ({last_before})"
        );
    }
}
