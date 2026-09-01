use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use uuid::Uuid;

use crate::engine::storage::partition::{
    decode_record_key, encode_record_key, PartitionBinding, PartitionScope,
};
use crate::engine::StorageEngine;
use crate::engine::StorageError;
use crate::error::Result;
use crate::services::recall::RecallLog;
use crate::services::types::{memory_key, StoredMemory};
use bytes::Bytes;

/// Typed repository for `StoredMemory` records.
///
/// Centralises all JSON serialization / deserialization so that service code
/// never calls `serde_json` directly. The underlying `StorageEngine` is exposed
/// for operations that don't involve `StoredMemory` (tags, timestamps, graph,
/// vector search, etc.).
pub struct MemoryRepository {
    pub engine: Arc<StorageEngine>,
    read_scope: PartitionScope,
    write_target: PartitionBinding,
    /// Per-memory-id locks serializing load-mutate-store cycles (get/update/
    /// promote/lifecycle tasks) against concurrent ones on the same id.
    /// Grows to at most the number of distinct memory ids ever touched in
    /// this process's lifetime -- bounded by dataset size, not a leak;
    /// entries aren't pruned since the memory itself already dominates
    /// that same footprint.
    locks: StdMutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>,
    /// Recalls recorded but not yet written. Folded into every record this
    /// repository persists -- see `store_in`.
    recall: Arc<RecallLog>,
}

impl MemoryRepository {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        let read_scope = engine.default_partition_scope();
        let write_target = engine.default_partition_binding();
        Self::with_partition_scope(engine, read_scope, write_target)
    }

    pub fn with_partition_scope(
        engine: Arc<StorageEngine>,
        read_scope: PartitionScope,
        write_target: PartitionBinding,
    ) -> Self {
        Self {
            engine,
            read_scope,
            write_target,
            locks: StdMutex::new(HashMap::new()),
            recall: Arc::new(RecallLog::new()),
        }
    }

    /// The recall accumulator every write through this repository folds from.
    ///
    /// Shared rather than per-caller: `MemoryManager` records into it, the
    /// flush task and active forgetting drain it, and every `store_in` takes
    /// from it. One log per repository is what makes "the record that gets
    /// written carries its outstanding recall" true regardless of which
    /// caller does the writing.
    pub fn recall(&self) -> &Arc<RecallLog> {
        &self.recall
    }

    pub fn read_scope(&self) -> &PartitionScope {
        &self.read_scope
    }

    pub fn write_target(&self) -> &PartitionBinding {
        &self.write_target
    }

    /// Physical key for a memory in this repository's *write* partition.
    ///
    /// Only correct for a record this repository is about to write, or one it
    /// has already confirmed lives in the write target. To address a record
    /// that may live anywhere in the read scope, resolve its binding first —
    /// see [`Self::resolve_binding`].
    pub fn physical_memory_key(&self, id: Uuid) -> Result<Bytes> {
        self.physical_key_in(&self.write_target, memory_key(id).as_bytes())
    }

    pub fn physical_key_in(&self, binding: &PartitionBinding, logical_key: &[u8]) -> Result<Bytes> {
        Ok(encode_record_key(binding, logical_key)
            .map_err(|err| StorageError::InvalidArgument(err.to_string()))?)
    }

    /// Which partition in the read scope actually holds `id`.
    ///
    /// A multi-partition reader cannot assume the write target: the record it
    /// is about to traverse from, or write back, may live in any partition it
    /// is authorized for. Returns `None` when no partition in scope has it.
    pub async fn resolve_binding(&self, id: Uuid) -> Result<Option<PartitionBinding>> {
        let key = memory_key(id);
        Ok(self
            .engine
            .get_partitioned(&self.read_scope, &key)
            .await?
            .map(|(physical, _bytes)| physical.binding().clone()))
    }

    /// The partition a physical record key belongs to, if it is one this
    /// repository is allowed to read.
    pub fn binding_of_key(&self, key: &[u8]) -> Result<Option<PartitionBinding>> {
        if let Some(partitioned) =
            decode_record_key(key).map_err(|err| StorageError::InvalidArgument(err.to_string()))?
        {
            let binding = partitioned.binding();
            return Ok(self
                .read_scope
                .contains_binding(binding)
                .then(|| binding.clone()));
        }

        // An unpartitioned `memory:` key predates the layout migration, so it
        // belongs to whichever partition legacy data was assigned to — the
        // write target for a legacy-default repository.
        Ok(
            (key.starts_with(b"memory:") && self.read_scope.contains_binding(&self.write_target))
                .then(|| self.write_target.clone()),
        )
    }

    /// Acquire the per-memory lock for `id`. Hold the returned guard across
    /// the entire load...store span, including `.await` points -- it's an
    /// owned tokio guard, safe to hold across awaits (unlike a std::sync
    /// guard).
    pub async fn lock(&self, id: Uuid) -> tokio::sync::OwnedMutexGuard<()> {
        let mutex = {
            let mut locks = self.locks.lock().unwrap();
            Arc::clone(
                locks
                    .entry(id)
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
            )
        };
        mutex.lock_owned().await
    }

    /// Load a `StoredMemory` by UUID. Returns `Ok(None)` if the key doesn't exist.
    pub async fn load(&self, id: Uuid) -> Result<Option<StoredMemory>> {
        Ok(self.load_bound(id).await?.map(|(_binding, stored)| stored))
    }

    /// Load a `StoredMemory` together with the partition it actually lives in.
    ///
    /// Every read-modify-write must go through this rather than `load`: under
    /// a multi-partition read scope the record can be in any authorized
    /// partition, and writing it back to the repository's single write target
    /// would copy it across a partition boundary and leave the original
    /// behind.
    pub async fn load_bound(&self, id: Uuid) -> Result<Option<(PartitionBinding, StoredMemory)>> {
        let key = memory_key(id);
        let Some((physical, bytes)) = self.engine.get_partitioned(&self.read_scope, &key).await?
        else {
            return Ok(None);
        };
        Ok(Some((
            physical.binding().clone(),
            serde_json::from_slice(&bytes)?,
        )))
    }

    /// Load a `StoredMemory` by raw key bytes (e.g. from an index result).
    ///
    /// Returns `Ok(None)` if the key doesn't exist or the record is corrupt —
    /// callers in scan loops should treat `None` as "skip this entry".
    pub async fn load_by_key(&self, key: &[u8]) -> Result<Option<StoredMemory>> {
        Ok(self
            .load_bound_by_key(key)
            .await?
            .map(|(_binding, stored)| stored))
    }

    /// `load_by_key`, keeping the partition the key resolved to.
    ///
    /// A key outside the read scope reads as absent. This is the last line of
    /// defence rather than the first — index reads are already scoped — but
    /// it means a key that reaches here from anywhere else still cannot pull
    /// a record the caller is not authorized for.
    pub async fn load_bound_by_key(
        &self,
        key: &[u8],
    ) -> Result<Option<(PartitionBinding, StoredMemory)>> {
        let Some(binding) = self.binding_of_key(key)? else {
            return Ok(None);
        };
        // A legacy `memory:` key has to be re-encoded: the migration moved the
        // record under the partition prefix, so the bare key no longer
        // addresses anything.
        let physical = self.physical_key_in(&binding, logical_part(key))?;
        let bytes = self.engine.get(&physical).await?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        match serde_json::from_slice::<StoredMemory>(&bytes) {
            Ok(stored) => Ok(Some((binding, stored))),
            Err(e) => {
                tracing::warn!(
                    key = %String::from_utf8_lossy(key),
                    error = %e,
                    "corrupt record: failed to deserialize StoredMemory, skipping"
                );
                Ok(None)
            }
        }
    }

    /// Persist a `StoredMemory` (no embedding update).
    ///
    /// The attribute row is derived here rather than by callers: this is the
    /// one place a record becomes bytes, so it is the one place the row can
    /// be kept in step with the payload. This is the path `update`, `get`
    /// (access-count touch) and `archive` all take -- deriving the row here,
    /// rather than only in `MemoryManager::create`, is what keeps a filter
    /// over the sidecar row from drifting out of step with the payload on
    /// every one of those operations.
    /// Persist into the repository's own write target.
    ///
    /// Test-only. Production writes name their partition (`store_in`,
    /// `store_with_embedding_in`, `delete_in`) with the binding a load
    /// returned, so a read-modify-write cannot silently relocate a record
    /// from the partition it was read out of into this repository's write
    /// target. Keeping the defaulting form out of the production build is
    /// what makes that mistake unrepresentable rather than merely discouraged
    /// — the compiler enforces it, not review.
    #[cfg(test)]
    pub async fn store(&self, stored: &mut StoredMemory) -> Result<()> {
        self.store_in(&self.write_target.clone(), stored).await
    }

    /// Persist a `StoredMemory` into one named partition.
    ///
    /// Takes `&mut` because the record genuinely changes here: any recall
    /// recorded since it was loaded is folded in before it becomes bytes, so
    /// the caller's copy is left holding what was actually written rather
    /// than a version missing its own access count.
    pub async fn store_in(
        &self,
        target: &PartitionBinding,
        stored: &mut StoredMemory,
    ) -> Result<()> {
        self.check_write_target(target)?;
        let folded = self.fold_recall(target, stored);
        let key = memory_key(stored.id);
        let json = serde_json::to_vec(stored)?;
        let row = crate::services::attrs::project(stored);
        self.engine
            .put_with_attrs_partitioned(target, key.as_bytes(), json, &row)
            .await
            .inspect_err(|_| self.return_recall(target, stored.id, folded))?;
        Ok(())
    }

    /// Fold any pending recall for this record into the copy about to be
    /// written, returning what was folded so a failed write can put it back.
    ///
    /// This is the seam the whole change turns on: recall is applied where a
    /// record becomes bytes, so *every* write path -- update, promote,
    /// archive, expiry, the flush task -- carries outstanding recall without
    /// each caller having to remember to.
    fn fold_recall(
        &self,
        target: &PartitionBinding,
        stored: &mut StoredMemory,
    ) -> Option<crate::services::recall::RecallDelta> {
        let delta = self.recall.take(target, stored.id)?;
        delta.apply(&mut stored.metadata);
        Some(delta)
    }

    /// Put a folded recall back after the write that was carrying it failed.
    ///
    /// Without this a transient storage error would silently discard the
    /// recall, which is the one way the accumulator could lose data outside
    /// the crash window the spec allows for.
    fn return_recall(
        &self,
        target: &PartitionBinding,
        id: Uuid,
        delta: Option<crate::services::recall::RecallDelta>,
    ) {
        if let Some(delta) = delta {
            self.recall.restore(vec![((target.clone(), id), delta)]);
        }
    }

    /// A write may only land in a partition this repository can read.
    ///
    /// Not a redundant check: `store_in`'s binding comes from a prior load,
    /// and this is what keeps a stale or hand-built binding from writing
    /// outside the authorized scope.
    fn check_write_target(&self, target: &PartitionBinding) -> Result<()> {
        if !self.read_scope.contains_binding(target) {
            return Err(StorageError::InvalidArgument(format!(
                "write target partition {:?} (tenant {:?}) is outside the authorized read scope",
                target.partition().as_str(),
                target.tenant().as_str(),
            ))
            .into());
        }
        Ok(())
    }

    /// Persist a `StoredMemory` and update its embedding in the HNSW index.
    ///
    /// Uses `put_with_embedding_and_attrs` rather than routing through
    /// `store_memory_core`: the latter also writes a timestamp-index entry,
    /// and the time-series index's `insert` is not keyed by record key, so a
    /// second call for the same key (this method is the content-update path)
    /// would leave a genuine duplicate entry rather than replacing the first
    /// -- see that method's doc comment on `StorageEngine`.
    /// Test-only; see [`Self::store`].
    #[cfg(test)]
    pub async fn store_with_embedding(
        &self,
        stored: &mut StoredMemory,
        embedding: Vec<f32>,
    ) -> Result<()> {
        self.store_with_embedding_in(&self.write_target.clone(), stored, embedding)
            .await
    }

    pub async fn store_with_embedding_in(
        &self,
        target: &PartitionBinding,
        stored: &mut StoredMemory,
        embedding: Vec<f32>,
    ) -> Result<()> {
        self.check_write_target(target)?;
        let folded = self.fold_recall(target, stored);
        let key = memory_key(stored.id);
        let json = serde_json::to_vec(stored)?;
        let row = crate::services::attrs::project(stored);
        self.engine
            .put_with_embedding_and_attrs_partitioned(
                target,
                key.as_bytes(),
                json,
                Some(embedding),
                &row,
            )
            .await
            .inspect_err(|_| self.return_recall(target, stored.id, folded))?;
        Ok(())
    }

    /// Hard-delete a memory from the repository's own write target.
    ///
    /// Test-only; see [`Self::store`].
    #[cfg(test)]
    pub async fn delete(&self, id: Uuid) -> Result<()> {
        self.delete_in(&self.write_target.clone(), id).await
    }

    /// Retire an archived memory's vector from the similarity index, leaving
    /// its record and every other index entry in place.
    ///
    /// Archiving is a flag on the record, so without this the memory stays a
    /// search candidate for the whole retention window: it is retrieved,
    /// rejected by the `archived` predicate, and the shortfall it leaves
    /// makes the retrieval widen and run again. Retiring it stops that at
    /// the source.
    ///
    /// Deliberately *not* `remove_from_indexes`, which would also drop the
    /// timestamp entry `cleanup_archived` walks to find this record later.
    pub async fn retire_vector_in(&self, target: &PartitionBinding, id: Uuid) -> Result<()> {
        self.check_write_target(target)?;
        let key = self.physical_key_in(target, memory_key(id).as_bytes())?;
        self.engine.retire_vector(key.as_ref()).await?;
        Ok(())
    }

    /// Retire an archived memory from the browsing order, leaving its record
    /// and every other index entry in place.
    ///
    /// The listing walk verifies each candidate against its row, so an
    /// archived memory left in the ordering index still costs a row read on
    /// every page that passes over it — and that cost grows with how much a
    /// deployment has archived, which is backwards. Taking it out of the
    /// ordering index is what lets a page of ten cost ten candidates rather
    /// than ten plus everything archived between them.
    ///
    /// Paired with [`Self::retire_vector_in`], and issued after it for the
    /// same reason: both are terminal, and a memory that fails to archive
    /// must not have been taken out of anything first.
    pub async fn retire_from_ordering_in(&self, target: &PartitionBinding, id: Uuid) -> Result<()> {
        self.check_write_target(target)?;
        let key = self.physical_key_in(target, memory_key(id).as_bytes())?;
        self.engine
            .retire_ordering_entry(key.as_ref(), crate::services::attrs::SLOT_CREATED_AT)
            .await?;
        Ok(())
    }

    /// Retire an archived memory from every retrieval path that would
    /// otherwise keep paying for it: the similarity index and the browsing
    /// order.
    ///
    /// One call rather than two at each of the three sites that archive, so
    /// the two retirements cannot drift apart — a site that remembered one and
    /// forgot the other would leave a cost that only shows up as a slow
    /// listing on the deployments that archive most.
    ///
    /// Both are attempted even if the first fails: they are independent, and
    /// skipping the second because the first failed leaves more behind for no
    /// gain. The first error is returned; the caller decides how loudly to
    /// complain.
    pub async fn retire_from_retrieval_in(
        &self,
        target: &PartitionBinding,
        id: Uuid,
    ) -> Result<()> {
        let vector = self.retire_vector_in(target, id).await;
        let ordering = self.retire_from_ordering_in(target, id).await;
        vector.and(ordering)
    }

    /// One bounded page of the memories currently due for background
    /// attention, oldest due time first.
    ///
    /// The sweeps' access path. Walking the due-time index instead of every
    /// record is what makes a sweep cost the work that is due rather than the
    /// size of the store — and ordering by due time means the most overdue
    /// memory is always handled first, so a backlog drains in the order it
    /// accumulated.
    ///
    /// Resumption is inherent rather than bookkept: a memory a sweep acts on
    /// is written, and that write moves its due time into the future, so it
    /// leaves this range. A run cut short therefore finds exactly the memories
    /// it had not reached yet, with no cursor to persist and nothing to lose
    /// across a restart.
    ///
    /// The exception is a memory woken by the shared due time for a sweep that
    /// has nothing to do for it. That memory stays due until the sweep that
    /// does own it runs, so it is re-examined in the meantime — settled from
    /// its row, without reading its content. That is the cost of one shared
    /// attention time instead of one per sweep, and it is bounded by how much
    /// is due at once rather than by the corpus.
    pub async fn select_due(
        &self,
        now: u64,
        after: Option<crate::engine::index::IndexPosition>,
        limit: usize,
        effort: usize,
    ) -> Result<crate::engine::attr::select::OrderedPage> {
        use crate::engine::attr::select::{AttrPred, OrderedSelect};
        use crate::engine::attr::value::AttrValue;
        use crate::services::attrs::SLOT_NEXT_ATTENTION_AT;
        use std::ops::Bound;

        let preds = [AttrPred::Range(
            SLOT_NEXT_ATTENTION_AT,
            Bound::Unbounded,
            Bound::Included(AttrValue::U64(now)),
        )];
        Ok(self
            .engine
            .select_page_partitioned(
                &self.read_scope,
                OrderedSelect {
                    preds: &preds,
                    order_slot: SLOT_NEXT_ATTENTION_AT,
                    after,
                    descending: false,
                    limit,
                    effort,
                },
            )
            .await?)
    }

    /// One bounded page of live memories, in creation order.
    ///
    /// For background work that genuinely visits everything rather than
    /// selecting what is due. Archived memories are excluded by the walk
    /// itself, so a deployment's archived volume costs it nothing.
    pub async fn select_live_page(
        &self,
        after: Option<crate::engine::index::IndexPosition>,
        limit: usize,
    ) -> Result<crate::engine::attr::select::OrderedPage> {
        use crate::engine::attr::select::{AttrPred, OrderedSelect};
        use crate::engine::attr::value::AttrValue;
        use crate::services::attrs::{SLOT_ARCHIVED, SLOT_CREATED_AT};

        let preds = [AttrPred::Eq(SLOT_ARCHIVED, AttrValue::Bool(false))];
        Ok(self
            .engine
            .select_page_partitioned(
                &self.read_scope,
                OrderedSelect {
                    preds: &preds,
                    order_slot: SLOT_CREATED_AT,
                    after,
                    descending: false,
                    limit,
                    effort: limit.saturating_mul(4).max(limit),
                },
            )
            .await?)
    }

    /// Hard-delete a memory from one named partition.
    pub async fn delete_in(&self, target: &PartitionBinding, id: Uuid) -> Result<()> {
        self.check_write_target(target)?;
        let logical_key = memory_key(id);
        let key = self.physical_key_in(target, logical_key.as_bytes())?;
        self.engine.remove_from_indexes(key.as_ref()).await?;
        self.engine
            .delete_with_attrs_partitioned(target, logical_key.as_bytes())
            .await?;
        Ok(())
    }
}

/// The logical (`memory:<uuid>`) part of a record key, whether or not the key
/// already carries a partition prefix.
fn logical_part(key: &[u8]) -> &[u8] {
    match decode_record_key(key) {
        Ok(Some(_)) => {
            // `decode_record_key` borrows into an owned `Bytes`, so re-find the
            // boundary rather than returning a reference into a temporary.
            let after_prefix =
                &key[crate::engine::storage::partition::PARTITIONED_RECORD_PREFIX.len()..];
            let mut colons = 0;
            for (i, b) in after_prefix.iter().enumerate() {
                if *b == b':' {
                    colons += 1;
                    if colons == 2 {
                        return &after_prefix[i + 1..];
                    }
                }
            }
            key
        }
        _ => key,
    }
}

#[cfg(test)]
mod lock_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn lock_serializes_concurrent_access_to_the_same_id() {
        let engine = crate::engine::storage::engine::StorageEngine::new(
            crate::engine::storage::engine::EngineConfig {
                data_dir: tempfile::tempdir().unwrap().keep(),
                sync_writes: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let repo = Arc::new(MemoryRepository::new(Arc::new(engine)));
        let id = Uuid::new_v4();

        let counter = Arc::new(AtomicU32::new(0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let repo = Arc::clone(&repo);
            let counter = Arc::clone(&counter);
            handles.push(tokio::spawn(async move {
                let _guard = repo.lock(id).await;
                let before = counter.load(Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
                // If two tasks were ever inside the critical section
                // together, this increment would race and the final
                // count could be less than the number of tasks.
                counter.store(before + 1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn load_by_key_logs_and_returns_none_on_corrupt_json() {
        // No global subscriber is installed for `cargo test`, so `tracing::warn!`
        // is otherwise a silent no-op here -- install a test-scoped one (writes
        // through the test harness's captured stdout) so the log line is
        // actually visible under `--nocapture`, proving the warning fires and
        // not just that `None` is returned (which was already true before).
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();

        let engine = crate::engine::storage::engine::StorageEngine::new(
            crate::engine::storage::engine::EngineConfig {
                data_dir: tempfile::tempdir().unwrap().keep(),
                sync_writes: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let repo = MemoryRepository::new(Arc::new(engine));

        let key = b"memory:deadbeef-0000-0000-0000-000000000000";
        repo.engine
            .put(key.to_vec(), b"not valid json".to_vec())
            .await
            .unwrap();

        let result = repo.load_by_key(key.as_slice()).await.unwrap();
        assert!(
            result.is_none(),
            "corrupt record must be skipped, not surfaced as an error"
        );
    }
}

#[cfg(test)]
mod partition_tests {
    use super::*;
    use crate::engine::storage::partition::{PartitionId, PartitionScope, TenantId};
    use crate::services::types::{MemoryType, StoredMemory, StoredMetadata};

    async fn engine() -> Arc<crate::engine::StorageEngine> {
        Arc::new(
            crate::engine::storage::engine::StorageEngine::new(
                crate::engine::storage::engine::EngineConfig {
                    data_dir: tempfile::tempdir().unwrap().keep(),
                    sync_writes: false,
                    attr_schema: Some(crate::services::attrs::memory_schema()),
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        )
    }

    fn stored(id: Uuid, content: &str) -> StoredMemory {
        StoredMemory {
            id,
            content: content.into(),
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
                health: 100.0,
                last_recalled_at: None,
                flashbulb_until: None,
                ttl: None,
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived: false,
        }
    }

    fn bindings() -> (PartitionBinding, PartitionBinding, PartitionScope) {
        let tenant = TenantId::new("acme").unwrap();
        let product = PartitionBinding::new(tenant.clone(), PartitionId::new("product").unwrap());
        let finance = PartitionBinding::new(tenant.clone(), PartitionId::new("finance").unwrap());
        let scope = PartitionScope::new(
            tenant,
            [
                PartitionId::new("product").unwrap(),
                PartitionId::new("finance").unwrap(),
            ],
        )
        .unwrap();
        (product, finance, scope)
    }

    /// The read-modify-write hazard a multi-partition scope creates: `load`
    /// resolves across the whole scope, so a record can come out of a
    /// partition that is not the write target. Writing it back to the write
    /// target would leave the original in place and create a second copy in
    /// another partition — one record, two partitions, diverging.
    #[tokio::test]
    async fn a_record_read_from_one_partition_is_written_back_to_that_partition() {
        let engine = engine().await;
        let (product, finance, scope) = bindings();
        // Write target is `product`, but the record lives in `finance`.
        let repo =
            MemoryRepository::with_partition_scope(Arc::clone(&engine), scope, product.clone());
        let id = Uuid::new_v4();
        repo.store_in(&finance, &mut stored(id, "original"))
            .await
            .unwrap();

        let (binding, mut loaded) = repo.load_bound(id).await.unwrap().unwrap();
        assert_eq!(binding.partition().as_str(), "finance");
        loaded.content = "updated".into();
        repo.store_in(&binding, &mut loaded).await.unwrap();

        // The update landed where the record already was...
        let finance_key = repo
            .physical_key_in(&finance, memory_key(id).as_bytes())
            .unwrap();
        let raw = engine.get(&finance_key).await.unwrap().unwrap();
        assert!(String::from_utf8_lossy(&raw).contains("updated"));

        // ...and no copy appeared in the write target.
        let product_key = repo
            .physical_key_in(&product, memory_key(id).as_bytes())
            .unwrap();
        assert!(
            engine.get(&product_key).await.unwrap().is_none(),
            "the write must not duplicate the record into the write-target partition"
        );
    }

    /// Archiving resolves the partition that actually holds the record, which
    /// need not be the write target, so retirement has to honour the binding
    /// it is handed rather than defaulting to the repository's own.
    #[tokio::test]
    async fn retire_vector_in_targets_the_given_binding() {
        let engine = engine().await;
        let (product, finance, scope) = bindings();
        let repo = MemoryRepository::with_partition_scope(engine, scope, product.clone());

        let in_finance = Uuid::new_v4();
        let in_product = Uuid::new_v4();
        let embedding = vec![1.0f32; 384];
        repo.store_with_embedding_in(&finance, &mut stored(in_finance, "f"), embedding.clone())
            .await
            .unwrap();
        repo.store_with_embedding_in(&product, &mut stored(in_product, "p"), embedding.clone())
            .await
            .unwrap();

        repo.retire_vector_in(&finance, in_finance).await.unwrap();

        let finance_key = repo
            .physical_key_in(&finance, memory_key(in_finance).as_bytes())
            .unwrap();
        let product_key = repo
            .physical_key_in(&product, memory_key(in_product).as_bytes())
            .unwrap();
        assert!(
            repo.engine.get_vector(finance_key.as_ref()).is_none(),
            "the named partition's record must be retired"
        );
        assert!(
            repo.engine.get_vector(product_key.as_ref()).is_some(),
            "the write target's record must be untouched"
        );
    }

    #[tokio::test]
    async fn resolve_binding_finds_the_partition_holding_the_record() {
        let engine = engine().await;
        let (product, finance, scope) = bindings();
        let repo = MemoryRepository::with_partition_scope(engine, scope, product);
        let id = Uuid::new_v4();
        repo.store_in(&finance, &mut stored(id, "c")).await.unwrap();

        let resolved = repo.resolve_binding(id).await.unwrap().unwrap();

        assert_eq!(resolved.partition().as_str(), "finance");
        assert!(repo
            .resolve_binding(Uuid::new_v4())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn a_write_outside_the_read_scope_is_rejected() {
        let engine = engine().await;
        let (product, _finance, _scope) = bindings();
        let outsider = PartitionBinding::new(
            TenantId::new("acme").unwrap(),
            PartitionId::new("legal").unwrap(),
        );
        let repo = MemoryRepository::with_partition_scope(
            Arc::clone(&engine),
            PartitionScope::single(product.clone()),
            product,
        );
        let id = Uuid::new_v4();

        let err = repo
            .store_in(&outsider, &mut stored(id, "c"))
            .await
            .expect_err("a write outside the authorized scope must be refused");

        assert!(err.to_string().contains("legal"), "{err}");
        let key = repo
            .physical_key_in(&outsider, memory_key(id).as_bytes())
            .unwrap();
        assert!(engine.get(&key).await.unwrap().is_none());
    }

    /// Index reads are scoped already, so this is defence in depth — but a
    /// key that reaches the repository from anywhere else must not be able
    /// to pull a record the caller is not authorized for.
    #[tokio::test]
    async fn load_by_key_refuses_a_key_outside_the_read_scope() {
        let engine = engine().await;
        let (product, finance, _scope) = bindings();
        let finance_repo = MemoryRepository::with_partition_scope(
            Arc::clone(&engine),
            PartitionScope::single(finance.clone()),
            finance.clone(),
        );
        let id = Uuid::new_v4();
        finance_repo
            .store_in(&finance, &mut stored(id, "secret"))
            .await
            .unwrap();
        let finance_key = finance_repo
            .physical_key_in(&finance, memory_key(id).as_bytes())
            .unwrap();

        let product_repo = MemoryRepository::with_partition_scope(
            engine,
            PartitionScope::single(product.clone()),
            product,
        );

        assert!(product_repo
            .load_by_key(finance_key.as_ref())
            .await
            .unwrap()
            .is_none());
        assert!(finance_repo
            .load_by_key(finance_key.as_ref())
            .await
            .unwrap()
            .is_some());
    }
}

#[cfg(test)]
mod attr_tests {
    use super::*;
    use crate::engine::attr::value::AttrValue;
    use crate::services::attrs::{SLOT_ARCHIVED, SLOT_IMPORTANCE};
    use crate::services::types::{MemoryType, StoredMemory, StoredMetadata};

    async fn repo() -> (tempfile::TempDir, MemoryRepository) {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::engine::storage::engine::StorageEngine::new(
            crate::engine::storage::engine::EngineConfig {
                data_dir: dir.path().to_path_buf(),
                sync_writes: false,
                attr_schema: Some(crate::services::attrs::memory_schema()),
                vector: crate::engine::storage::engine::VectorConfig {
                    enabled: true,
                    dimension: 4,
                    hnsw_m: 4,
                    hnsw_ef_construction: 10,
                    hnsw_ef_search: 4,
                    metric: crate::engine::util::DistanceMetric::L2,
                    hnsw_resident_budget_bytes: None,
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        (dir, MemoryRepository::new(Arc::new(engine)))
    }

    fn stored(importance: f32, archived: bool) -> StoredMemory {
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
                importance,
                emotional_valence: 0.0,
                arousal: 0.0,
                health: 100.0,
                last_recalled_at: None,
                flashbulb_until: None,
                ttl: None,
                last_decay_at: None,
                last_health_check_at: None,
            },
            archived,
        }
    }

    #[tokio::test]
    async fn store_writes_the_row() {
        let (_d, repo) = repo().await;
        let mut s = stored(0.6, false);
        repo.store(&mut s).await.unwrap();

        let key = repo.physical_memory_key(s.id).unwrap();
        let row = repo.engine.get_attrs(key.as_ref()).await.unwrap().unwrap();
        assert_eq!(row.get(SLOT_IMPORTANCE), Some(AttrValue::F32(0.6)));
    }

    #[tokio::test]
    async fn an_update_moves_the_row_with_the_payload() {
        // This is the drift the design exists to prevent: `store` is the path
        // update/get/archive all take, and it bypassed store_memory_core.
        let (_d, repo) = repo().await;
        let mut s = stored(0.2, false);
        repo.store(&mut s).await.unwrap();

        s.metadata.importance = 0.95;
        s.archived = true;
        repo.store(&mut s).await.unwrap();

        let key = repo.physical_memory_key(s.id).unwrap();
        let row = repo.engine.get_attrs(key.as_ref()).await.unwrap().unwrap();
        assert_eq!(row.get(SLOT_IMPORTANCE), Some(AttrValue::F32(0.95)));
        assert_eq!(row.get(SLOT_ARCHIVED), Some(AttrValue::Bool(true)));
    }

    #[tokio::test]
    async fn delete_removes_the_row_too() {
        let (_d, repo) = repo().await;
        let mut s = stored(0.5, false);
        repo.store(&mut s).await.unwrap();
        repo.delete(s.id).await.unwrap();

        assert!(repo
            .engine
            .get_attrs(repo.physical_memory_key(s.id).unwrap().as_ref())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn store_with_embedding_writes_the_row_without_duplicating_the_time_index() {
        // Covers the hedge in the task-9 brief: store_with_embedding is the
        // content-update path (MemoryManager::update), so it must maintain
        // the sidecar row *without* adding a timestamp-index entry -- unlike
        // store_memory_core, which is only ever called once per key (memory
        // creation).
        let (_d, repo) = repo().await;
        let mut s = stored(0.3, false);
        repo.store_with_embedding(&mut s, vec![1.0, 0.0, 0.0, 0.0])
            .await
            .unwrap();

        let key = repo.physical_memory_key(s.id).unwrap();
        let row = repo.engine.get_attrs(key.as_ref()).await.unwrap().unwrap();
        assert_eq!(row.get(SLOT_IMPORTANCE), Some(AttrValue::F32(0.3)));

        let before = repo.engine.stats().time_series_count;

        let mut s2 = s.clone();
        s2.metadata.importance = 0.9;
        repo.store_with_embedding(&mut s2, vec![0.0, 1.0, 0.0, 0.0])
            .await
            .unwrap();

        let row2 = repo.engine.get_attrs(key.as_ref()).await.unwrap().unwrap();
        assert_eq!(row2.get(SLOT_IMPORTANCE), Some(AttrValue::F32(0.9)));

        let after = repo.engine.stats().time_series_count;
        assert_eq!(
            before, after,
            "content update must not add a duplicate time-series entry"
        );
    }
}

#[cfg(test)]
mod recall_fold_tests {
    use super::*;
    use crate::services::types::{MemoryType, StoredMemory, StoredMetadata};

    async fn repo() -> (tempfile::TempDir, MemoryRepository) {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::engine::storage::engine::StorageEngine::new(
            crate::engine::storage::engine::EngineConfig {
                data_dir: dir.path().to_path_buf(),
                sync_writes: false,
                attr_schema: Some(crate::services::attrs::memory_schema()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        (dir, MemoryRepository::new(Arc::new(engine)))
    }

    fn stored(id: Uuid) -> StoredMemory {
        StoredMemory {
            id,
            content: "c".into(),
            memory_type: MemoryType::ShortTerm,
            metadata: StoredMetadata {
                created_at: 10,
                updated_at: 10,
                accessed_at: 10,
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
    }

    /// The regression this fold exists to prevent: an update loads a record,
    /// a recall lands while it is being edited, and the update writes the
    /// copy it loaded. Without the fold the recall is silently discarded and
    /// `access_count` appears to stand still under active use.
    #[tokio::test]
    async fn a_recall_landing_during_an_update_survives_the_write() {
        let (_d, repo) = repo().await;
        let target = repo.write_target().clone();
        let id = Uuid::new_v4();
        repo.store_in(&target, &mut stored(id)).await.unwrap();

        // An update loads the record...
        let (binding, mut loaded) = repo.load_bound(id).await.unwrap().unwrap();
        assert_eq!(loaded.metadata.access_count, 0);

        // ...a recall is recorded while it is in flight...
        repo.recall().record(&binding, id, 900);

        // ...and the update writes the copy it loaded.
        loaded.content = "edited".into();
        repo.store_in(&binding, &mut loaded).await.unwrap();

        let (_b, reloaded) = repo.load_bound(id).await.unwrap().unwrap();
        assert_eq!(reloaded.content, "edited");
        assert_eq!(
            reloaded.metadata.access_count, 1,
            "the recall must not be lost to the concurrent update"
        );
        assert_eq!(reloaded.metadata.accessed_at, 900);
        assert_eq!(reloaded.metadata.last_recalled_at, Some(900));
        assert_eq!(reloaded.metadata.health, 60.0);

        // The caller's own copy holds what was actually written, so a
        // response built from it is not missing its own access count.
        assert_eq!(loaded.metadata.access_count, 1);
    }

    #[tokio::test]
    async fn the_fold_consumes_the_recall_exactly_once() {
        let (_d, repo) = repo().await;
        let target = repo.write_target().clone();
        let id = Uuid::new_v4();
        repo.store_in(&target, &mut stored(id)).await.unwrap();
        repo.recall().record(&target, id, 900);

        for _ in 0..3 {
            let (binding, mut loaded) = repo.load_bound(id).await.unwrap().unwrap();
            repo.store_in(&binding, &mut loaded).await.unwrap();
        }

        let (_b, reloaded) = repo.load_bound(id).await.unwrap().unwrap();
        assert_eq!(
            reloaded.metadata.access_count, 1,
            "one recorded recall must produce exactly one increment"
        );
    }

    #[tokio::test]
    async fn a_write_with_no_pending_recall_leaves_access_metadata_untouched() {
        let (_d, repo) = repo().await;
        let target = repo.write_target().clone();
        let id = Uuid::new_v4();
        let mut original = stored(id);
        original.metadata.access_count = 7;
        original.metadata.accessed_at = 500;
        original.metadata.last_recalled_at = Some(500);
        original.metadata.health = 42.0;
        repo.store_in(&target, &mut original).await.unwrap();

        let (binding, mut loaded) = repo.load_bound(id).await.unwrap().unwrap();
        loaded.metadata.importance = 0.9;
        repo.store_in(&binding, &mut loaded).await.unwrap();

        let (_b, reloaded) = repo.load_bound(id).await.unwrap().unwrap();
        assert_eq!(reloaded.metadata.importance, 0.9);
        assert_eq!(reloaded.metadata.access_count, 7);
        assert_eq!(reloaded.metadata.accessed_at, 500);
        assert_eq!(reloaded.metadata.last_recalled_at, Some(500));
        assert_eq!(reloaded.metadata.health, 42.0);
    }

    /// The embedding path is the content-update path, so it has to fold too —
    /// otherwise editing a memory's text would be the one write that drops a
    /// concurrent recall.
    #[tokio::test]
    async fn the_embedding_write_path_folds_recall_too() {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::engine::storage::engine::StorageEngine::new(
            crate::engine::storage::engine::EngineConfig {
                data_dir: dir.path().to_path_buf(),
                sync_writes: false,
                attr_schema: Some(crate::services::attrs::memory_schema()),
                vector: crate::engine::storage::engine::VectorConfig {
                    enabled: true,
                    dimension: 4,
                    hnsw_m: 4,
                    hnsw_ef_construction: 10,
                    hnsw_ef_search: 4,
                    metric: crate::engine::util::DistanceMetric::L2,
                    hnsw_resident_budget_bytes: None,
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let repo = MemoryRepository::new(Arc::new(engine));
        let target = repo.write_target().clone();
        let id = Uuid::new_v4();

        repo.recall().record(&target, id, 900);
        repo.store_with_embedding_in(&target, &mut stored(id), vec![1.0, 0.0, 0.0, 0.0])
            .await
            .unwrap();

        let (_b, reloaded) = repo.load_bound(id).await.unwrap().unwrap();
        assert_eq!(reloaded.metadata.access_count, 1);
    }

    /// A failed write must not swallow the recall it was carrying: the entry
    /// goes back so the next write (or the flush task) still applies it.
    #[tokio::test]
    async fn a_rejected_write_returns_the_recall_to_the_log() {
        use crate::engine::storage::partition::{PartitionId, TenantId};

        let (_d, repo) = repo().await;
        let outsider = PartitionBinding::new(
            TenantId::new("acme").unwrap(),
            PartitionId::new("legal").unwrap(),
        );
        let id = Uuid::new_v4();
        repo.recall().record(&outsider, id, 900);

        repo.store_in(&outsider, &mut stored(id))
            .await
            .expect_err("a write outside the authorized scope must be refused");

        assert!(
            repo.recall().take(&outsider, id).is_some(),
            "the recall must survive a write that never landed"
        );
    }
}
