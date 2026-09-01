//! B+Tree index for time-series and range queries
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
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[cfg(test)]
static RANGE_VISITS: AtomicUsize = AtomicUsize::new(0);

use crate::engine::error::{Result, StorageError};

/// Configuration for the B+Tree index
#[derive(Debug, Clone, Default)]
pub struct BTreeConfig {}

/// A position in an index's total order.
///
/// Entries are ordered by timestamp first and by key second. The key is what
/// makes the order total: timestamps collide (several memories written in one
/// millisecond), and a position that named only a timestamp could not say
/// which of the colliding entries a page had already reached.
pub type IndexPosition = (u64, Bytes);

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
    pub fn new(_config: BTreeConfig) -> Self {
        Self {
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

    /// Query a range of timestamps (inclusive)
    ///
    /// Returns (timestamp, key) pairs sorted by timestamp.
    pub fn range(&self, start: u64, end: u64) -> Vec<(u64, Bytes)> {
        let tree = self.tree.read();
        let mut results = Vec::new();

        for (&ts, entry) in tree.range(start..=end) {
            for key in &entry.keys {
                #[cfg(test)]
                RANGE_VISITS.fetch_add(1, Ordering::Relaxed);
                results.push((ts, key.clone()));
            }
        }

        results
    }

    /// Query a range, stopping after `limit` entries have been collected.
    ///
    /// Unlike callers that truncate [`Self::range`], this keeps the walk
    /// bounded by the requested result count. The BTreeMap range iterator
    /// remains ordered by timestamp, matching `range`'s contract.
    pub fn range_limit(&self, start: u64, end: u64, limit: usize) -> Vec<(u64, Bytes)> {
        if limit == 0 {
            return Vec::new();
        }

        let tree = self.tree.read();
        let mut results = Vec::with_capacity(limit);
        'timestamps: for (&ts, entry) in tree.range(start..=end) {
            for key in &entry.keys {
                #[cfg(test)]
                RANGE_VISITS.fetch_add(1, Ordering::Relaxed);
                results.push((ts, key.clone()));
                if results.len() >= limit {
                    break 'timestamps;
                }
            }
        }
        results
    }

    /// Visit a range in order, stopping when the visitor returns `false`.
    /// The callback receives borrowed keys so callers can stop before cloning
    /// or materializing the remainder of a large range.
    pub(crate) fn visit_range<F>(&self, start: u64, end: u64, mut visitor: F)
    where
        F: FnMut(u64, &Bytes) -> bool,
    {
        let tree = self.tree.read();
        'timestamps: for (&ts, entry) in tree.range(start..=end) {
            for key in &entry.keys {
                #[cfg(test)]
                RANGE_VISITS.fetch_add(1, Ordering::Relaxed);
                if !visitor(ts, key) {
                    break 'timestamps;
                }
            }
        }
    }

    /// Visit a range in the index's total order, optionally resuming after a
    /// position, in either direction. The visitor stops the walk by returning
    /// `false`.
    ///
    /// Keys sharing a timestamp are held in an unsorted `Vec` -- `remove` even
    /// uses `swap_remove` -- so a walk that took them in stored order would put
    /// a page boundary at a position that means nothing on the next call. They
    /// are sorted here, per timestamp, which is what makes `(timestamp, key)` a
    /// position a caller can resume from. The sort is over the keys of one
    /// timestamp, not the range.
    pub(crate) fn visit_range_ordered<F>(
        &self,
        start: u64,
        end: u64,
        after: Option<&IndexPosition>,
        descending: bool,
        mut visitor: F,
    ) where
        F: FnMut(u64, &Bytes) -> bool,
    {
        // Narrow the tree range by the resume position before walking: an
        // ascending resume can never yield an earlier timestamp, and a
        // descending one can never yield a later one.
        let (start, end) = match after {
            Some((ts, _)) if descending => (start, end.min(*ts)),
            Some((ts, _)) => (start.max(*ts), end),
            None => (start, end),
        };
        if start > end {
            return;
        }

        // One timestamp's keys at a time. Collecting the range first would
        // materialize everything the bound exists to avoid.
        let mut keys: Vec<Bytes> = Vec::new();
        let mut emit = |ts: u64, entry: &TimestampEntry| -> bool {
            keys.clear();
            keys.extend(entry.keys.iter().cloned());
            keys.sort();
            if descending {
                keys.reverse();
            }
            for key in &keys {
                if let Some((after_ts, after_key)) = after {
                    let position = (ts, key);
                    let boundary = (*after_ts, after_key);
                    let past = if descending {
                        position < boundary
                    } else {
                        position > boundary
                    };
                    if !past {
                        continue;
                    }
                }
                #[cfg(test)]
                RANGE_VISITS.fetch_add(1, Ordering::Relaxed);
                if !visitor(ts, key) {
                    return false;
                }
            }
            true
        };

        let tree = self.tree.read();
        if descending {
            for (&ts, entry) in tree.range(start..=end).rev() {
                if !emit(ts, entry) {
                    return;
                }
            }
        } else {
            for (&ts, entry) in tree.range(start..=end) {
                if !emit(ts, entry) {
                    return;
                }
            }
        }
    }

    /// One page of the total order: at most `limit` entries from `start..=end`,
    /// resuming after `after`, in the requested direction.
    pub(crate) fn range_page(
        &self,
        start: u64,
        end: u64,
        after: Option<&IndexPosition>,
        descending: bool,
        limit: usize,
    ) -> Vec<IndexPosition> {
        if limit == 0 {
            return Vec::new();
        }
        // Reserve for the page, not for the caller's number: `limit` is a
        // stopping condition and may legitimately be enormous.
        let mut results = Vec::with_capacity(limit.min(1024));
        self.visit_range_ordered(start, end, after, descending, |ts, key| {
            results.push((ts, key.clone()));
            results.len() < limit
        });
        results
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn reset_range_visit_count() {
        RANGE_VISITS.store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn range_visit_count() -> usize {
        RANGE_VISITS.load(Ordering::Relaxed)
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

    /// Get the total number of entries
    pub fn len(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if the index has been modified
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
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

        // Skip legacy config bytes (max_entries — no longer stored; kept for
        // on-disk wire-format compatibility with files written before this
        // field was removed).
        file.read_exact(&mut buf8)?;

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
            tree: RwLock::new(tree),
            key_to_timestamp: RwLock::new(key_to_timestamp),
            entry_count: AtomicUsize::new(entry_count),
            dirty: AtomicBool::new(false),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Walk a whole range one page at a time, resuming from each page's last
    /// position, and return everything seen.
    fn walk_by_pages(
        index: &BTreeIndex,
        start: u64,
        end: u64,
        descending: bool,
        page: usize,
    ) -> Vec<IndexPosition> {
        let mut seen = Vec::new();
        let mut after: Option<IndexPosition> = None;
        loop {
            let batch = index.range_page(start, end, after.as_ref(), descending, page);
            if batch.is_empty() {
                return seen;
            }
            after = batch.last().cloned();
            seen.extend(batch);
        }
    }

    #[test]
    fn an_ordered_walk_resumes_across_a_page_boundary_without_repeating_or_skipping() {
        let index = BTreeIndex::new(BTreeConfig::default());
        for i in 0..25u64 {
            index.insert(i * 10, format!("key{i:02}")).unwrap();
        }

        let whole = index.range_page(0, u64::MAX, None, false, usize::MAX);
        let paged = walk_by_pages(&index, 0, u64::MAX, false, 4);

        assert_eq!(
            paged, whole,
            "paging must reconstruct the full walk exactly"
        );
        assert_eq!(paged.len(), 25);
    }

    #[test]
    fn entries_sharing_a_timestamp_are_paged_without_loss() {
        // The case a timestamp-only cursor cannot express: a page boundary
        // landing among entries that share one timestamp.
        let index = BTreeIndex::new(BTreeConfig::default());
        for i in 0..9u64 {
            index.insert(500, format!("same{i}")).unwrap();
        }
        index.insert(400, "before").unwrap();
        index.insert(600, "after").unwrap();

        let paged = walk_by_pages(&index, 0, u64::MAX, false, 2);
        let mut keys: Vec<String> = paged
            .iter()
            .map(|(_, k)| String::from_utf8_lossy(k).into_owned())
            .collect();
        let unique: std::collections::HashSet<&String> = keys.iter().collect();

        assert_eq!(keys.len(), 11, "every entry is returned exactly once");
        assert_eq!(unique.len(), 11, "no entry is returned twice");
        keys.sort();
        assert_eq!(keys.first().unwrap(), "after");
        assert!(keys.contains(&"before".to_string()));
    }

    #[test]
    fn a_descending_walk_yields_the_ascending_walk_reversed() {
        let index = BTreeIndex::new(BTreeConfig::default());
        // Deliberately includes tied timestamps: ties are where a direction
        // reversal is easiest to get wrong.
        for (ts, key) in [
            (100, "a"),
            (100, "b"),
            (200, "c"),
            (300, "d"),
            (300, "e"),
            (300, "f"),
        ] {
            index.insert(ts, key).unwrap();
        }

        let ascending = walk_by_pages(&index, 0, u64::MAX, false, 2);
        let descending = walk_by_pages(&index, 0, u64::MAX, true, 2);
        let mut reversed = ascending.clone();
        reversed.reverse();

        assert_eq!(descending, reversed);
    }

    #[test]
    fn a_bounded_walk_visits_only_what_the_bound_asks_for() {
        let index = BTreeIndex::new(BTreeConfig::default());
        for i in 0..1000u64 {
            index.insert(i, format!("key{i:04}")).unwrap();
        }

        BTreeIndex::reset_range_visit_count();
        let page = index.range_page(0, u64::MAX, None, false, 10);
        let visits = BTreeIndex::range_visit_count();

        assert_eq!(page.len(), 10);
        assert_eq!(
            visits, 10,
            "a bounded walk must not visit the rest of the range"
        );
    }

    #[test]
    fn test_empty_index() {
        let index = BTreeIndex::new(BTreeConfig::default());
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
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
    fn test_remove() {
        let index = BTreeIndex::new(BTreeConfig::default());

        index.insert(1000, b"key1".to_vec()).unwrap();
        index.insert(1000, b"key2".to_vec()).unwrap();
        index.insert(2000, b"key3".to_vec()).unwrap();

        assert_eq!(index.len(), 3);

        // Remove one key from a multi-key timestamp
        assert!(index.remove(b"key1").unwrap());
        assert_eq!(index.len(), 2);

        // Remove the last key from timestamp 1000
        assert!(index.remove(b"key2").unwrap());
        assert_eq!(index.len(), 1);

        // Try to remove non-existent key
        assert!(!index.remove(b"key999").unwrap());
    }

    #[test]
    fn test_update_timestamp() {
        let index = BTreeIndex::new(BTreeConfig::default());

        index.insert(1000, b"key1".to_vec()).unwrap();
        assert_eq!(index.range(1000, 1000), vec![(1000, Bytes::from("key1"))]);

        // Update timestamp for same key: the old (1000) entry must be gone
        // and the new (2000) entry must be the only one -- not a duplicate.
        index.insert(2000, b"key1".to_vec()).unwrap();
        assert_eq!(index.range(1000, 1000), Vec::<(u64, Bytes)>::new());
        assert_eq!(index.range(2000, 2000), vec![(2000, Bytes::from("key1"))]);
        assert_eq!(index.len(), 1); // Count should not increase
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
}
