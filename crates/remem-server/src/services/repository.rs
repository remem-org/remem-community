use std::sync::Arc;
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
}

impl MemoryRepository {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
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
