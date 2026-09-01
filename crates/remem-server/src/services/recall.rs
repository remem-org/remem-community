//! Pending recall deltas, held in process memory rather than written per event.
//!
//! A recall is telemetry: `access_count`, `accessed_at`, `last_recalled_at`
//! and the health reinforcement that follows a retrieval. It feeds promotion
//! and active forgetting, both heuristics. Writing it durably on every
//! retrieval made a read append to the WAL and fsync inside the engine's
//! global write lock -- full record-rewrite cost, and read/write contention,
//! to protect data whose loss costs a promotion one recall later.
//!
//! So recalls accumulate here and are folded into the record the next time it
//! is written, whether by the flush task or by any ordinary update. See
//! `openspec/changes/redefine-recall-tracking/design.md`.

use std::collections::HashMap;
use std::sync::Mutex as StdMutex;

use tokio::sync::Notify;
use uuid::Uuid;

use crate::engine::storage::partition::PartitionBinding;
use crate::services::types::StoredMetadata;

/// Health granted for a recall, matching the pre-existing retrieval boost.
pub const RECALL_HEALTH_BOOST: f32 = 10.0;

/// Pending entries above which the flush task is woken early rather than
/// letting the map grow until the next tick. Not a hard limit: `record`
/// never blocks and never drops, so exceeding it only means the flush
/// happens sooner.
pub const DEFAULT_CAPACITY: usize = 50_000;

/// One memory's unflushed recall activity.
///
/// A flag, not a counter: a second recall inside the same flush window is a
/// no-op on the count, which is what makes `access_count` measure recall
/// sessions rather than round trips. A client that fetches a memory and then
/// updates it registers one recall, not two.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecallDelta {
    /// Whether this memory was recalled at all in the current window.
    recalled: bool,
    /// The most recent recall's wall-clock time.
    last_recall_ms: u64,
}

impl RecallDelta {
    /// Fold this delta into a record's metadata.
    ///
    /// Additive, never absolute: the count is incremented from whatever the
    /// record currently holds rather than computed from a value read earlier,
    /// so a concurrent update cannot make the count go backwards. Timestamps
    /// only ever move forward for the same reason.
    pub fn apply(&self, metadata: &mut StoredMetadata) {
        if !self.recalled {
            return;
        }
        metadata.access_count = metadata.access_count.saturating_add(1);
        if self.last_recall_ms > metadata.accessed_at {
            metadata.accessed_at = self.last_recall_ms;
        }
        if metadata
            .last_recalled_at
            .is_none_or(|at| self.last_recall_ms > at)
        {
            metadata.last_recalled_at = Some(self.last_recall_ms);
        }
        metadata.health = (metadata.health + RECALL_HEALTH_BOOST).clamp(0.0, 100.0);
    }
}

/// Recall deltas awaiting a write, keyed by the partition the record lives in
/// and its id.
///
/// The map is behind a `std::sync::Mutex` and every method releases it before
/// returning -- the guard is never held across an `.await`, so no caller can
/// park a runtime worker while holding it.
pub struct RecallLog {
    pending: StdMutex<HashMap<(PartitionBinding, Uuid), RecallDelta>>,
    capacity: usize,
    /// Signalled when `pending` grows past `capacity`, so the flush task can
    /// drain ahead of its next tick.
    over_capacity: Notify,
}

impl RecallLog {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            pending: StdMutex::new(HashMap::new()),
            capacity,
            over_capacity: Notify::new(),
        }
    }

    /// Record that a memory was recalled.
    ///
    /// Idempotent within a flush window: repeated calls advance the timestamp
    /// but leave the eventual count increment at one.
    pub fn record(&self, binding: &PartitionBinding, id: Uuid, now_ms: u64) {
        let over_capacity = {
            let mut pending = self.pending.lock().unwrap();
            let entry = pending.entry((binding.clone(), id)).or_insert(RecallDelta {
                recalled: false,
                last_recall_ms: 0,
            });
            entry.recalled = true;
            entry.last_recall_ms = entry.last_recall_ms.max(now_ms);
            pending.len() > self.capacity
        };

        // Notified outside the lock: waking the flush task while still
        // holding it would have it contend immediately on wake.
        if over_capacity {
            self.over_capacity.notify_one();
        }
    }

    /// Remove and return one memory's pending recall, if it has any.
    ///
    /// Called by the write path so a record about to be serialized carries
    /// its outstanding recall. Removing rather than peeking is what keeps a
    /// recall from being applied twice.
    pub fn take(&self, binding: &PartitionBinding, id: Uuid) -> Option<RecallDelta> {
        self.pending.lock().unwrap().remove(&(binding.clone(), id))
    }

    /// Read one memory's pending recall without consuming it.
    ///
    /// This is what lets a fetch show the caller its own recall while the
    /// write is still outstanding. `take` would answer the same question and
    /// then silently discard the recall, so a read must never use it.
    pub fn peek(&self, binding: &PartitionBinding, id: Uuid) -> Option<RecallDelta> {
        self.pending
            .lock()
            .unwrap()
            .get(&(binding.clone(), id))
            .copied()
    }

    /// Remove and return every pending recall.
    pub fn drain(&self) -> Vec<((PartitionBinding, Uuid), RecallDelta)> {
        self.pending.lock().unwrap().drain().collect()
    }

    /// Put entries back after a failed flush, without overwriting recalls
    /// recorded since they were drained.
    ///
    /// A retained entry that raced a fresh recall must not clobber it: the
    /// merge keeps the later timestamp and the `recalled` flag from either
    /// side, so nothing is lost in the direction that matters.
    pub fn restore(&self, entries: Vec<((PartitionBinding, Uuid), RecallDelta)>) {
        let mut pending = self.pending.lock().unwrap();
        for (key, delta) in entries {
            let entry = pending.entry(key).or_insert(RecallDelta {
                recalled: false,
                last_recall_ms: 0,
            });
            entry.recalled |= delta.recalled;
            entry.last_recall_ms = entry.last_recall_ms.max(delta.last_recall_ms);
        }
    }

    /// Number of memories with unflushed recall. Test-only: production code
    /// drains or folds the log, it never sizes it.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    /// Test-only; see [`Self::len`]. This is the assertion the recall-boundary
    /// tests are built on -- "that operation recorded nothing".
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Resolves once the pending map has outgrown its capacity.
    pub async fn over_capacity(&self) {
        self.over_capacity.notified().await;
    }
}

impl Default for RecallLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::storage::partition::{PartitionId, TenantId};
    use crate::services::types::{MemoryType, StoredMemory};

    fn binding() -> PartitionBinding {
        PartitionBinding::new(
            TenantId::new("acme").unwrap(),
            PartitionId::new("product").unwrap(),
        )
    }

    fn metadata() -> StoredMetadata {
        StoredMemory {
            id: Uuid::new_v4(),
            content: "c".into(),
            memory_type: MemoryType::ShortTerm,
            metadata: StoredMetadata {
                created_at: 1,
                updated_at: 1,
                accessed_at: 1,
                access_count: 0,
                source: None,
                tags: vec![],
                importance: 0.5,
                emotional_valence: 0.0,
                arousal: 0.0,
                health: 50.0,
                last_recalled_at: None,
                flashbulb_until: None,
                ttl: None,
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: false,
        }
        .metadata
    }

    /// The coalescing rule: `access_count` counts recall sessions, not
    /// operations, so a client that reads then writes inside one window
    /// registers once.
    #[test]
    fn repeated_recalls_in_one_window_yield_one_increment() {
        let log = RecallLog::new();
        let b = binding();
        let id = Uuid::new_v4();

        log.record(&b, id, 100);
        log.record(&b, id, 200);
        log.record(&b, id, 150);

        let delta = log.take(&b, id).expect("a recall must be pending");
        let mut m = metadata();
        delta.apply(&mut m);

        assert_eq!(m.access_count, 1);
        // The latest recall wins, not the last call.
        assert_eq!(m.accessed_at, 200);
        assert_eq!(m.last_recalled_at, Some(200));
    }

    #[test]
    fn distinct_memories_each_increment_once() {
        let log = RecallLog::new();
        let b = binding();
        let (a, c) = (Uuid::new_v4(), Uuid::new_v4());

        log.record(&b, a, 100);
        log.record(&b, c, 100);

        assert_eq!(log.len(), 2);
        for id in [a, c] {
            let mut m = metadata();
            log.take(&b, id).unwrap().apply(&mut m);
            assert_eq!(m.access_count, 1);
        }
    }

    /// Same id, different partitions: two distinct records, two counts.
    #[test]
    fn the_same_id_in_two_partitions_is_two_entries() {
        let log = RecallLog::new();
        let product = binding();
        let finance = PartitionBinding::new(
            TenantId::new("acme").unwrap(),
            PartitionId::new("finance").unwrap(),
        );
        let id = Uuid::new_v4();

        log.record(&product, id, 100);
        log.record(&finance, id, 100);

        assert_eq!(log.len(), 2);
        assert!(log.take(&product, id).is_some());
        assert!(log.take(&finance, id).is_some());
    }

    #[test]
    fn take_removes_the_entry() {
        let log = RecallLog::new();
        let b = binding();
        let id = Uuid::new_v4();
        log.record(&b, id, 100);

        assert!(log.take(&b, id).is_some());
        assert!(
            log.take(&b, id).is_none(),
            "a taken recall must not be applied a second time"
        );
        assert!(log.is_empty());
    }

    #[test]
    fn peek_leaves_the_entry_in_place() {
        let log = RecallLog::new();
        let b = binding();
        let id = Uuid::new_v4();
        log.record(&b, id, 100);

        assert!(log.peek(&b, id).is_some());
        assert_eq!(log.len(), 1, "peeking must not consume the recall");
        assert!(
            log.take(&b, id).is_some(),
            "the peeked recall must still be there for the write that persists it"
        );
    }

    #[test]
    fn take_of_an_unrecalled_memory_is_none() {
        let log = RecallLog::new();
        assert!(log.take(&binding(), Uuid::new_v4()).is_none());
    }

    #[test]
    fn drain_empties_the_log_and_returns_everything() {
        let log = RecallLog::new();
        let b = binding();
        for _ in 0..3 {
            log.record(&b, Uuid::new_v4(), 100);
        }

        let drained = log.drain();

        assert_eq!(drained.len(), 3);
        assert!(log.is_empty());
    }

    #[test]
    fn apply_clamps_health_at_the_ceiling() {
        let log = RecallLog::new();
        let b = binding();
        let id = Uuid::new_v4();
        log.record(&b, id, 100);
        let mut m = metadata();
        m.health = 95.0;

        log.take(&b, id).unwrap().apply(&mut m);

        assert_eq!(m.health, 100.0);
    }

    /// The monotonicity guarantee: a delta folded into a record that already
    /// holds a *later* timestamp must not drag it backwards. This is what a
    /// write racing a stale pending recall looks like.
    #[test]
    fn apply_never_moves_timestamps_backwards() {
        let log = RecallLog::new();
        let b = binding();
        let id = Uuid::new_v4();
        log.record(&b, id, 100);
        let mut m = metadata();
        m.accessed_at = 500;
        m.last_recalled_at = Some(500);

        log.take(&b, id).unwrap().apply(&mut m);

        assert_eq!(m.accessed_at, 500);
        assert_eq!(m.last_recalled_at, Some(500));
        // The count still moves: the recall happened, only its clock is older.
        assert_eq!(m.access_count, 1);
    }

    #[test]
    fn restore_merges_rather_than_clobbers() {
        let log = RecallLog::new();
        let b = binding();
        let id = Uuid::new_v4();
        log.record(&b, id, 100);
        let drained = log.drain();

        // A fresh recall lands while the drained batch is being written.
        log.record(&b, id, 300);
        log.restore(drained);

        let delta = log.take(&b, id).unwrap();
        let mut m = metadata();
        delta.apply(&mut m);
        assert_eq!(m.access_count, 1, "the merge must still coalesce");
        assert_eq!(m.accessed_at, 300, "the newer recall's clock must survive");
    }

    #[tokio::test]
    async fn exceeding_capacity_signals_an_early_flush() {
        let log = RecallLog::with_capacity(2);
        let b = binding();

        let mut signalled = tokio_test::task::spawn(log.over_capacity());

        log.record(&b, Uuid::new_v4(), 1);
        log.record(&b, Uuid::new_v4(), 1);
        // Still at capacity, not over it.
        assert!(
            signalled.poll().is_pending(),
            "capacity must not be signalled until it is exceeded"
        );

        log.record(&b, Uuid::new_v4(), 1);

        assert!(
            signalled.poll().is_ready(),
            "exceeding capacity must wake the flush task"
        );
        // Nothing was dropped to stay under the cap.
        assert_eq!(log.len(), 3);
    }
}
