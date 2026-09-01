//! Background task management for the storage engine.
//!
//! Contains the flush loop, compaction loop, checkpoint loop, and the
//! `flush_memtable` helper used during both background and foreground flushes.

use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::compaction::CompactionManager;
use super::engine::EngineConfig;
use super::memtable::{ImmutableMemTable, MemTable};
use super::partitioned_hnsw::PartitionedHnswIndexes;
use super::partitioned_indexes::{PartitionedTagIndex, PartitionedTimeSeriesIndex};
use super::sstable::{SSTableReader, SSTableWriter};
use super::wal_commit::WalCommitCoordinator;
use crate::engine::attr::index::AttrIndexes;
use crate::engine::error::Result;
use crate::engine::index::SegmentedCsrGraph;

/// Global counter for unique SSTable file names (prevents collisions when
/// multiple flushes happen within the same millisecond).
pub(super) static SSTABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Flush an immutable MemTable to a new L0 SSTable file.
pub(super) fn flush_memtable(
    memtable: &ImmutableMemTable,
    compaction: &CompactionManager,
    config: &EngineConfig,
) -> Result<()> {
    if memtable.is_empty() {
        return Ok(());
    }

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let counter = SSTABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let path = config
        .data_dir
        .join("sstables")
        .join("level0")
        .join(format!("{}_{}.sst", timestamp, counter));

    std::fs::create_dir_all(path.parent().unwrap())?;

    let mut writer = SSTableWriter::with_level(&path, config.compaction.compression, 0)?;
    for (key, entry) in memtable.iter() {
        writer.add_entry(key, &entry)?;
    }
    let meta = writer.finish()?;

    let reader = SSTableReader::open_with_cache(&meta.path, None, 0)?;
    compaction.add_l0_sstable(Arc::new(reader));

    tracing::info!(
        "Flushed memtable to SSTable: {:?} ({} records, {} bytes)",
        path,
        meta.record_count,
        meta.file_size
    );

    Ok(())
}

/// Flush all current and immutable memtables during an exclusive checkpoint.
///
/// Callers must hold exclusive write admission and establish a coordinator
/// barrier before entering. They may ask the coordinator to truncate only
/// after this function and every required index save succeed. Shared by the
/// foreground and background checkpoint paths.
pub(super) fn flush_memtables_for_checkpoint(
    memtable: &RwLock<MemTable>,
    immutable_memtables: &RwLock<Vec<Arc<ImmutableMemTable>>>,
    compaction: &CompactionManager,
    config: &EngineConfig,
) -> Result<()> {
    let newly_immutable = {
        let mut mt = memtable.write();
        if mt.is_empty() {
            None
        } else {
            // Inherit the outgoing table's counter -- it is the engine's own
            // handle, so the replacement keeps issuing versions in order.
            let sequence = mt.sequence();
            let old = std::mem::replace(
                &mut *mt,
                MemTable::with_sequence(config.memtable_size, sequence),
            );
            Some(Arc::new(ImmutableMemTable::from_memtable(old)))
        }
    };
    if let Some(new_imm) = newly_immutable {
        immutable_memtables.write().push(Arc::clone(&new_imm));
    }

    let to_flush: Vec<_> = immutable_memtables.read().clone();
    for imm in to_flush {
        flush_memtable(&imm, compaction, config)?;
        immutable_memtables
            .write()
            .retain(|m| !Arc::ptr_eq(m, &imm));
    }

    Ok(())
}

/// Spawn the background flush, compaction, and checkpoint tasks.
///
/// These are storage-engine maintenance loops, not request paths. They process
/// every already-encoded physical key so partition prefixes and per-partition
/// HNSW roots are preserved without resolving an application read scope.
///
/// Returns every maintenance handle so the engine can await them during
/// graceful shutdown and abort them if the engine is dropped abruptly.
// Called exactly once, from `StorageEngine::new`. Every argument is a distinct
// piece of engine state the tasks need to own a handle to; bundling them into a
// struct would only move the same 12 fields one level down.
#[allow(clippy::too_many_arguments)]
pub(super) fn start_background_tasks(
    immutable_memtables: Arc<RwLock<Vec<Arc<ImmutableMemTable>>>>,
    memtable: Arc<RwLock<MemTable>>,
    compaction: Arc<CompactionManager>,
    wal: Arc<WalCommitCoordinator>,
    write_admission: Arc<tokio::sync::RwLock<()>>,
    checkpoint_attempts: Arc<std::sync::atomic::AtomicUsize>,
    hnsw_index: Option<Arc<PartitionedHnswIndexes>>,
    graph_index: Option<Arc<parking_lot::RwLock<SegmentedCsrGraph>>>,
    time_series_index: Option<Arc<parking_lot::RwLock<PartitionedTimeSeriesIndex>>>,
    tag_index: Option<Arc<parking_lot::RwLock<PartitionedTagIndex>>>,
    attr_indexes: Option<Arc<AttrIndexes>>,
    shutdown: Arc<AtomicBool>,
    config: EngineConfig,
    mut flush_rx: mpsc::Receiver<()>,
) -> (JoinHandle<()>, JoinHandle<()>, JoinHandle<()>) {
    // --- Flush task ---
    let immutable = Arc::clone(&immutable_memtables);
    let compaction_flush = Arc::clone(&compaction);
    let shutdown_flush = Arc::clone(&shutdown);
    let config_flush = config.clone();

    let flush_handle = tokio::spawn(async move {
        while !shutdown_flush.load(Ordering::Relaxed) {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), flush_rx.recv()).await;

            if shutdown_flush.load(Ordering::Relaxed) {
                break;
            }

            let to_flush: Vec<_> = {
                let imm = immutable.read();
                imm.clone()
            };

            for imm_memtable in to_flush {
                if let Err(e) = flush_memtable(&imm_memtable, &compaction_flush, &config_flush) {
                    tracing::error!("Failed to flush memtable: {}", e);
                    continue;
                }
                let mut imm = immutable.write();
                imm.retain(|m| !Arc::ptr_eq(m, &imm_memtable));
            }
        }
    });

    // --- Compaction task ---
    let compaction2 = Arc::clone(&compaction);
    let shutdown2 = Arc::clone(&shutdown);

    let compaction_handle = tokio::spawn(async move {
        while !shutdown2.load(Ordering::Relaxed) {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;

            if shutdown2.load(Ordering::Relaxed) {
                break;
            }

            while let Some(level) = compaction2.needs_compaction() {
                match compaction2.compact_level(level) {
                    Ok(result) => {
                        tracing::info!(
                            "Compacted L{} -> L{}: {} -> {} files",
                            result.input_level,
                            result.output_level,
                            result.input_count,
                            result.output_count
                        );
                    }
                    Err(e) => {
                        tracing::error!("Compaction failed: {}", e);
                        break;
                    }
                }
            }
        }
    });

    // --- Checkpoint task (fire-and-forget; stopped by shutdown flag) ---
    let memtable_cp = Arc::clone(&memtable);
    let immutable_cp = Arc::clone(&immutable_memtables);
    let compaction_cp = Arc::clone(&compaction);
    let checkpoint_handle = tokio::spawn(async move {
        // tokio::time::Instant, not std::time::Instant: this loop already
        // runs on tokio::time::sleep, and using tokio's clock here means
        // tokio::time::pause/advance (used by tests) actually affects this
        // elapsed-time check instead of silently ignoring it.
        let mut last_checkpoint = tokio::time::Instant::now();

        while !shutdown.load(Ordering::Relaxed) {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;

            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            let wal_size = match wal.size().await {
                Ok(size) => size,
                Err(error) => {
                    tracing::error!("Failed to read WAL size: {}", error);
                    continue;
                }
            };

            let should_checkpoint = wal_size > config.max_wal_size
                || last_checkpoint.elapsed() > config.checkpoint_interval;

            if should_checkpoint {
                let _admission = write_admission.write().await;
                if let Err(error) = wal.barrier().await {
                    tracing::error!("Failed to establish WAL checkpoint barrier: {}", error);
                    continue;
                }
                tracing::info!(
                    "Background checkpoint triggering (WAL size: {}, Last: {:?})",
                    wal_size,
                    last_checkpoint.elapsed()
                );

                let mut all_saves_ok = true;

                if let Some(index) = &hnsw_index {
                    if index.is_dirty() {
                        if let Err(e) = index.save_dirty() {
                            tracing::error!("Failed to save partitioned HNSW indexes: {}", e);
                            all_saves_ok = false;
                        }
                    }
                }

                if let Some(index) = &graph_index {
                    let is_dirty = index.read().is_dirty();
                    if is_dirty {
                        if let Err(e) = index.write().save_if_dirty() {
                            tracing::error!("Failed to save segmented graph index: {}", e);
                            all_saves_ok = false;
                        }
                    }
                }

                if let Some(index) = &time_series_index {
                    let is_dirty = index.read().is_dirty();
                    if is_dirty {
                        if let Err(e) = index.write().save_if_dirty() {
                            tracing::error!("Failed to save segmented time-series index: {}", e);
                            all_saves_ok = false;
                        }
                    }
                    // Compaction: merge small chunks if needed
                    if index.read().needs_compaction() {
                        if let Err(e) = index.write().compact() {
                            tracing::error!("BTree index compaction failed: {}", e);
                        }
                    }
                }

                if let Some(index) = &tag_index {
                    let is_dirty = index.read().is_dirty();
                    if is_dirty {
                        if let Err(e) = index.write().save_if_dirty() {
                            tracing::error!("Failed to save segmented tag index: {}", e);
                            all_saves_ok = false;
                        }
                    }
                    // Compaction: merge segments if deletion ratio is high or too many segments
                    if index.read().needs_compaction() {
                        if let Err(e) = index.write().compact() {
                            tracing::error!("Tag index compaction failed: {}", e);
                        }
                    }
                }

                if let Some(indexes) = &attr_indexes {
                    if let Err(e) = indexes.save_if_dirty() {
                        tracing::error!("Failed to save attribute indexes: {}", e);
                        all_saves_ok = false;
                    }
                    if let Err(e) = indexes.compact_if_needed() {
                        tracing::error!("Attribute index compaction failed: {}", e);
                    }
                }

                // Only reset the retry clock once the cycle actually
                // completes end to end (index saves, KV flush, WAL
                // truncate all succeed). Otherwise a persistent failure
                // (e.g. a permissions error) would still only get retried
                // once per `checkpoint_interval` -- the same cadence as a
                // healthy checkpoint -- rather than promptly on the very
                // next poll once the underlying problem clears up.
                let checkpoint_succeeded = if all_saves_ok {
                    match flush_memtables_for_checkpoint(
                        &memtable_cp,
                        &immutable_cp,
                        &compaction_cp,
                        &config,
                    ) {
                        Ok(()) => match wal.truncate().await {
                            Ok(()) => true,
                            Err(e) => {
                                tracing::error!("Checkpoint WAL truncate failed: {}", e);
                                false
                            }
                        },
                        Err(e) => {
                            tracing::error!("Checkpoint flush/truncate failed: {}", e);
                            false
                        }
                    }
                } else {
                    tracing::warn!(
                        "Skipping WAL truncation this cycle: one or more index saves failed"
                    );
                    false
                };

                if checkpoint_succeeded {
                    last_checkpoint = tokio::time::Instant::now();
                }
                tracing::info!(
                    "Background checkpoint complete (succeeded={})",
                    checkpoint_succeeded
                );
                checkpoint_attempts.fetch_add(1, Ordering::Release);
            }
        }
    });

    (flush_handle, compaction_handle, checkpoint_handle)
}
