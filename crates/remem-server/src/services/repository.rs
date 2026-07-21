use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use uuid::Uuid;

use crate::engine::StorageEngine;
use crate::error::Result;
use crate::services::types::{memory_key, StoredMemory};

/// Typed repository for `StoredMemory` records.
///
/// Centralises all JSON serialization / deserialization so that service code
/// never calls `serde_json` directly. The underlying `StorageEngine` is exposed
/// for operations that don't involve `StoredMemory` (tags, timestamps, graph,
/// vector search, etc.).
pub struct MemoryRepository {
    pub engine: Arc<StorageEngine>,
    /// Per-memory-id locks serializing load-mutate-store cycles (get/update/
    /// promote/lifecycle tasks) against concurrent ones on the same id.
    /// Grows to at most the number of distinct memory ids ever touched in
    /// this process's lifetime -- bounded by dataset size, not a leak;
    /// entries aren't pruned since the memory itself already dominates
    /// that same footprint.
    locks: StdMutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>,
}

impl MemoryRepository {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine, locks: StdMutex::new(HashMap::new()) }
    }

    /// Acquire the per-memory lock for `id`. Hold the returned guard across
    /// the entire load...store span, including `.await` points -- it's an
    /// owned tokio guard, safe to hold across awaits (unlike a std::sync
    /// guard).
    pub async fn lock(&self, id: Uuid) -> tokio::sync::OwnedMutexGuard<()> {
        let mutex = {
            let mut locks = self.locks.lock().unwrap();
            Arc::clone(locks.entry(id).or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))))
        };
        mutex.lock_owned().await
    }

    /// Load a `StoredMemory` by UUID. Returns `Ok(None)` if the key doesn't exist.
    pub async fn load(&self, id: Uuid) -> Result<Option<StoredMemory>> {
        let key = memory_key(id);
        let Some(bytes) = self.engine.get(&key).await? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    /// Load a `StoredMemory` by raw key bytes (e.g. from an index result).
    ///
    /// Returns `Ok(None)` if the key doesn't exist or the record is corrupt —
    /// callers in scan loops should treat `None` as "skip this entry".
    pub async fn load_by_key(&self, key: &[u8]) -> Result<Option<StoredMemory>> {
        let Some(bytes) = self.engine.get(key).await? else {
            return Ok(None);
        };
        match serde_json::from_slice::<StoredMemory>(&bytes) {
            Ok(stored) => Ok(Some(stored)),
            Err(_) => Ok(None),
        }
    }

    /// Persist a `StoredMemory` (no embedding update).
    pub async fn store(&self, stored: &StoredMemory) -> Result<()> {
        let key = memory_key(stored.id);
        let json = serde_json::to_vec(stored)?;
        self.engine.put(key, json).await?;
        Ok(())
    }

    /// Persist a `StoredMemory` and update its embedding in the HNSW index.
    pub async fn store_with_embedding(
        &self,
        stored: &StoredMemory,
        embedding: Vec<f32>,
    ) -> Result<()> {
        let key = memory_key(stored.id);
        let json = serde_json::to_vec(stored)?;
        self.engine.put_with_embedding(key, json, Some(embedding)).await?;
        Ok(())
    }

    /// Hard-delete a memory by UUID.
    pub async fn delete(&self, id: Uuid) -> Result<()> {
        let key = memory_key(id);
        self.engine.remove_from_indexes(key.as_bytes())?;
        self.engine.delete(key).await?;
        Ok(())
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
}
