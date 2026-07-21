//! WAL recovery logic for the storage engine.
//!
//! Replays WAL records into the MemTable and all secondary indexes
//! so that a crash restart produces the same in-memory state.

use parking_lot::RwLock;
use std::path::Path;
use std::sync::Arc;

use crate::engine::error::Result;
use crate::engine::index::{
    EdgeMetadata, GraphIndex, HnswIndex, SegmentedBTreeIndex, SegmentedInvertedIndex,
};
use super::memtable::MemTable;
use super::wal::WAL;

/// Replay a WAL file into the given MemTable and indexes.
///
/// Called during engine startup to recover state since the last checkpoint.
/// Safe to call when `wal_path` does not exist — returns `Ok(())` immediately.
pub(super) fn replay_wal(
    wal_path: &Path,
    memtable: &Arc<RwLock<MemTable>>,
    hnsw_index: Option<&Arc<HnswIndex>>,
    time_series_index: Option<&Arc<RwLock<SegmentedBTreeIndex>>>,
    tag_index: Option<&Arc<RwLock<SegmentedInvertedIndex>>>,
    graph_index: Option<&Arc<RwLock<GraphIndex>>>,
) -> Result<()> {
    if !wal_path.exists() {
        return Ok(());
    }

    let wal = WAL::open(wal_path)?;
    let memtable = memtable.write();

    let mut record_count = 0u64;
    let mut embedding_count = 0u64;
    let mut timestamp_count = 0u64;
    let mut tag_count = 0u64;
    let mut edge_count = 0u64;

    for record_result in wal.iter()? {
        let record = match record_result {
            Ok(r) => r,
            Err(crate::engine::error::StorageError::WalReplayError { offset, message }) => {
                // Standard LSM recovery: a torn tail (crash mid-append) or a
                // corrupt trailing record is expected after an unclean
                // shutdown. Replay everything before it, then physically
                // truncate the WAL to the last valid offset so the next
                // append starts clean and this exact failure doesn't recur
                // on the next restart.
                tracing::warn!(
                    offset,
                    error = %message,
                    records_replayed = record_count,
                    "WAL replay hit a torn/corrupt record; truncating WAL to last \
                     valid offset and continuing startup"
                );
                let file = std::fs::OpenOptions::new().write(true).open(wal_path)?;
                file.set_len(offset)?;
                file.sync_all()?;
                break;
            }
            Err(e) => return Err(e),
        };
        record_count += 1;

        match record.record_type {
            super::wal::WalRecordType::Insert => {
                memtable.insert_with_timestamp(record.key, record.value, record.timestamp)?;
            }
            super::wal::WalRecordType::InsertWithEmbedding => {
                memtable.insert_with_timestamp(
                    record.key.clone(),
                    record.value,
                    record.timestamp,
                )?;
                if let (Some(embedding), Some(index)) = (record.embedding, hnsw_index) {
                    index.insert(record.key, embedding)?;
                    embedding_count += 1;
                }
            }
            super::wal::WalRecordType::Delete => {
                memtable.delete_with_timestamp(record.key, record.timestamp)?;
            }
            super::wal::WalRecordType::SetTimestamp => {
                if let (Some(ts), Some(index)) = (record.ts_timestamp, time_series_index) {
                    index.read().insert(ts, record.key)?;
                    timestamp_count += 1;
                }
            }
            super::wal::WalRecordType::AddTags => {
                if let (Some(tags), Some(index)) = (record.tags, tag_index) {
                    index.read().add_tags(record.key, &tags)?;
                    tag_count += 1;
                }
            }
            super::wal::WalRecordType::SetTags => {
                if let (Some(tags), Some(index)) = (record.tags, tag_index) {
                    index.read().set_tags(record.key, &tags)?;
                    tag_count += 1;
                }
            }
            super::wal::WalRecordType::RemoveEdge => {
                if let Some(index) = graph_index {
                    if let (Some(source), Some(target)) =
                        (record.edge_source, record.edge_target)
                    {
                        if let Err(e) = index.read().remove_edge(&source, &target) {
                            tracing::warn!("Failed to replay RemoveEdge from WAL: {}", e);
                        }
                    }
                }
            }
            super::wal::WalRecordType::AddEdge => {
                if let Some(index) = graph_index {
                    if let (Some(source), Some(target)) =
                        (record.edge_source, record.edge_target)
                    {
                        let mut metadata = EdgeMetadata::default();
                        if let Some(et) = record.edge_type {
                            metadata = EdgeMetadata::with_type(et);
                        }
                        if let Some(w) = record.edge_weight {
                            metadata = metadata.weight(w);
                        }
                        if let Err(e) = index.read().add_edge(source, target, metadata) {
                            tracing::warn!("Failed to recover edge from WAL: {}", e);
                        } else {
                            edge_count += 1;
                        }
                    }
                }
            }
            super::wal::WalRecordType::RemoveTimestamp => {
                if let Some(index) = time_series_index {
                    if let Err(e) = index.read().remove(&record.key) {
                        tracing::warn!("Failed to replay RemoveTimestamp from WAL: {}", e);
                    }
                }
            }
            super::wal::WalRecordType::RemoveTags => {
                if let Some(index) = tag_index {
                    if let Err(e) = index.read().remove(&record.key) {
                        tracing::warn!("Failed to replay RemoveTags from WAL: {}", e);
                    }
                }
            }
            super::wal::WalRecordType::RemoveVector => {
                if let Some(index) = hnsw_index {
                    index.remove(&record.key);
                }
            }
        }
    }

    if record_count > 0 {
        tracing::info!(
            "WAL recovery: {} records, {} embeddings, {} timestamps, {} tags, {} edges",
            record_count,
            embedding_count,
            timestamp_count,
            tag_count,
            edge_count
        );
    }

    Ok(())
}

#[cfg(test)]
mod torn_tail_tests {
    use crate::engine::storage::engine::{EngineConfig, StorageEngine, VectorConfig};
    use bytes::Bytes;
    use tempfile::TempDir;

    fn test_cfg(data_dir: std::path::PathBuf) -> EngineConfig {
        EngineConfig {
            data_dir,
            sync_writes: true,
            checkpoint_interval: std::time::Duration::from_secs(86_400), // no auto-checkpoint
            vector: VectorConfig {
                enabled: true,
                dimension: 4,
                hnsw_m: 4,
                hnsw_ef_construction: 10,
                hnsw_ef_search: 4,
                metric: crate::engine::util::DistanceMetric::L2,
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn torn_wal_tail_truncates_and_continues() {
        let dir = TempDir::new().unwrap();

        {
            let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf())).await.unwrap();
            engine.put(Bytes::from_static(b"key1"), Bytes::from_static(b"value1")).await.unwrap();
            engine.put(Bytes::from_static(b"key2"), Bytes::from_static(b"value2")).await.unwrap();
        } // engine dropped mid-session — no checkpoint, WAL holds both records

        // Simulate a crash mid-write: chop the last 3 bytes off the WAL file,
        // tearing the final record's tail.
        let wal_path = dir.path().join("wal").join("current.wal");
        let len = std::fs::metadata(&wal_path).unwrap().len();
        let file = std::fs::OpenOptions::new().write(true).open(&wal_path).unwrap();
        file.set_len(len - 3).unwrap();
        drop(file);

        // Must still boot (not error/panic) and recover the earlier, intact record.
        let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf())).await.unwrap();
        assert_eq!(
            engine.get(b"key1".as_slice()).await.unwrap(),
            Some(Bytes::from_static(b"value1")),
        );
    }

    #[tokio::test]
    async fn wal_replay_survives_a_tear_at_every_possible_offset() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("wal").join("current.wal");

        // Record the WAL length right after key1 lands -- the exact byte
        // offset where key2's record begins -- and again after key2 lands.
        // Deriving the boundary this way (rather than guessing a fixed byte
        // count for "one record's worth") keeps the sweep below targeting
        // only key2's record even if the wire format's per-field widths
        // ever change.
        let (record1_end, full_len) = {
            let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf())).await.unwrap();
            engine.put(Bytes::from_static(b"key1"), Bytes::from_static(b"value1")).await.unwrap();
            let record1_end = std::fs::metadata(&wal_path).unwrap().len();
            engine.put(Bytes::from_static(b"key2"), Bytes::from_static(b"value2")).await.unwrap();
            drop(engine);
            let full_len = std::fs::metadata(&wal_path).unwrap().len();
            (record1_end, full_len)
        };
        assert!(record1_end < full_len, "key2 must add at least one byte to the WAL");

        // Snapshot the intact WAL so each iteration starts from the same
        // known-good bytes.
        let intact = std::fs::read(&wal_path).unwrap();
        assert_eq!(intact.len() as u64, full_len);

        // Sweep every truncation point within key2's record -- from the
        // exact offset where it starts (key1 fully intact, key2 entirely
        // missing) up to one byte short of the complete file -- at each
        // offset, the engine must still boot without error and key1 (the
        // earlier, definitely-intact record) must always be recoverable.
        for cut_at in record1_end..full_len {
            std::fs::write(&wal_path, &intact[..cut_at as usize]).unwrap();

            let engine = StorageEngine::new(test_cfg(dir.path().to_path_buf()))
                .await
                .unwrap_or_else(|e| panic!("engine failed to boot with WAL truncated to {cut_at} bytes: {e}"));

            // key1's record is fully before this truncation window in all
            // cases tested here, so it must always survive.
            let got = engine.get(b"key1".as_slice()).await.unwrap();
            assert!(
                got.is_some(),
                "key1 should always be recoverable when only the tail is torn (cut_at={cut_at})"
            );

            drop(engine);
        }
    }
}
