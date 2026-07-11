//! Per-chunk dirty tracking for incremental index checkpoints.

use std::sync::atomic::{AtomicBool, Ordering};

/// Tracks which fixed-size chunks of an index have been modified since the
/// last checkpoint.
///
/// Thread-safe: mark_dirty() and dirty_chunks() can be called concurrently.
pub struct DirtyChunkTracker {
    chunk_size: u32,
    dirty: Vec<AtomicBool>,
}

impl DirtyChunkTracker {
    pub fn new(chunk_size: u32) -> Self {
        Self {
            chunk_size,
            dirty: Vec::new(),
        }
    }

    /// Mark the chunk that contains `entry_id` as dirty.
    ///
    /// If `entry_id` is beyond the current capacity, grows the tracker first.
    pub fn mark_dirty(&self, entry_id: u32) {
        let idx = (entry_id / self.chunk_size) as usize;
        if idx < self.dirty.len() {
            self.dirty[idx].store(true, Ordering::Relaxed);
        }
        // If idx >= len the entry is in the growing (unsealed) range which is
        // covered by the WAL; nothing to track here.
    }

    /// Return the indices of all chunks currently marked dirty.
    pub fn dirty_chunks(&self) -> Vec<usize> {
        self.dirty
            .iter()
            .enumerate()
            .filter(|(_, d)| d.load(Ordering::Relaxed))
            .map(|(i, _)| i)
            .collect()
    }

    /// Mark a specific chunk as clean (called after it has been saved).
    pub fn mark_clean(&self, chunk_idx: usize) {
        if chunk_idx < self.dirty.len() {
            self.dirty[chunk_idx].store(false, Ordering::Relaxed);
        }
    }

    /// Ensure the tracker covers at least `total_entries` entries.
    /// Must be called from within a write lock on the owning data structure.
    pub fn grow_to(&mut self, total_entries: u32) {
        let needed = ((total_entries + self.chunk_size - 1) / self.chunk_size) as usize;
        while self.dirty.len() < needed {
            self.dirty.push(AtomicBool::new(false));
        }
    }

    /// Number of chunks being tracked.
    pub fn chunk_count(&self) -> usize {
        self.dirty.len()
    }

    /// The chunk size this tracker was configured with.
    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    /// First entry ID (inclusive) for chunk `idx`.
    pub fn chunk_start(&self, idx: usize) -> u32 {
        idx as u32 * self.chunk_size
    }

    /// Last entry ID (exclusive) for chunk `idx`.
    pub fn chunk_end(&self, idx: usize, total_entries: u32) -> u32 {
        ((idx as u32 + 1) * self.chunk_size).min(total_entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mark_and_query() {
        let mut tracker = DirtyChunkTracker::new(1000);
        tracker.grow_to(3000);

        assert!(tracker.dirty_chunks().is_empty());

        tracker.mark_dirty(500);   // chunk 0
        tracker.mark_dirty(1500);  // chunk 1

        let dirty = tracker.dirty_chunks();
        assert_eq!(dirty, vec![0, 1]);

        tracker.mark_clean(0);
        let dirty = tracker.dirty_chunks();
        assert_eq!(dirty, vec![1]);
    }

    #[test]
    fn test_grow_to() {
        let mut tracker = DirtyChunkTracker::new(100);
        assert_eq!(tracker.chunk_count(), 0);

        tracker.grow_to(250);
        assert_eq!(tracker.chunk_count(), 3); // chunks for 0-99, 100-199, 200-249

        tracker.grow_to(100); // should not shrink
        assert_eq!(tracker.chunk_count(), 3);
    }

    #[test]
    fn test_chunk_bounds() {
        let tracker = DirtyChunkTracker::new(1000);
        assert_eq!(tracker.chunk_start(0), 0);
        assert_eq!(tracker.chunk_start(2), 2000);
        assert_eq!(tracker.chunk_end(0, 3000), 1000);
        assert_eq!(tracker.chunk_end(2, 2500), 2500); // partial last chunk
    }

    #[test]
    fn test_out_of_range_mark_ignored() {
        let mut tracker = DirtyChunkTracker::new(100);
        tracker.grow_to(200);
        // Entry 500 is beyond current capacity — should not panic
        tracker.mark_dirty(500);
        assert!(tracker.dirty_chunks().is_empty());
    }
}
