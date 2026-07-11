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
        let record = record_result?;
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
