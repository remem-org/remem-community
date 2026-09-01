//! Index implementations for the storage engine
//!
//! This module contains specialized indexes for efficient data retrieval:
//!
//! - **HNSW**: Hierarchical Navigable Small World graph for vector similarity search
//! - **B+Tree**: For time-series range queries
//! - **Inverted**: For tag/text search
//! - **Graph**: CSR format for relationship traversal

pub mod btree;
pub mod btree_segmented;
pub mod dirty;
pub mod graph;
#[cfg(feature = "kuzu")]
pub mod graph_kuzu;
pub mod graph_segmented;
pub mod graph_wrapper;
pub mod hnsw;
pub mod inverted;
pub mod inverted_segmented;
pub mod manifest;
pub(crate) mod rekey;
pub mod segment_io;

pub use btree::{BTreeConfig, BTreeIndex, IndexPosition};
pub use btree_segmented::SegmentedBTreeIndex;
pub use graph::{CsrGraph, EdgeMetadata, GraphConfig, TraversalResult};
#[cfg(feature = "kuzu")]
pub use graph_kuzu::KuzuGraphIndex;
pub use graph_segmented::SegmentedCsrGraph;
pub use graph_wrapper::{load_graph_index, new_graph_index, GraphIndex};
pub use hnsw::{HnswConfig, HnswIndex};
pub use inverted::{InvertedIndex, InvertedIndexConfig};
pub use inverted_segmented::SegmentedInvertedIndex;
pub use manifest::SegmentManifest;

// ── Chunk size constants ───────────────────────────────────────────────────────

/// Number of nodes per sealed HNSW chunk.
pub const HNSW_CHUNK_SIZE: u32 = 50_000;
/// Number of docs per sealed tag segment.
pub const TAGS_CHUNK_SIZE: u32 = 10_000;
/// Maximum tag segment count before compaction is triggered.
pub const MAX_TAG_SEGMENTS: usize = 20;
/// Deletion ratio threshold for compaction.
pub const COMPACTION_DELETION_RATIO: f64 = 0.2;

// ── Corruption policy ───────────────────────────────────────────────────────────

/// Decide what a failed index parse means.
///
/// Rebuilding from empty is lossy: index files are written at checkpoint and
/// the WAL is truncated at checkpoint, so WAL replay restores only what was
/// written since the last one. Everything older is gone, while the server
/// reports healthy and searches quietly under-return.
///
/// Returns `Ok(())` only when the operator has explicitly accepted that loss.
pub(crate) fn on_index_parse_failure(
    path: &std::path::Path,
    cause: &crate::engine::error::StorageError,
) -> crate::engine::error::Result<()> {
    if std::env::var("REMEM_ALLOW_INDEX_REBUILD").as_deref() == Ok("1") {
        tracing::error!(
            "Index {:?} is corrupt ({}); REMEM_ALLOW_INDEX_REBUILD=1 is set, so it is being \
             rebuilt from empty. Every entry older than the last checkpoint is lost.",
            path,
            cause
        );
        return Ok(());
    }

    Err(crate::engine::error::StorageError::Corruption {
        file: path.to_path_buf(),
        message: format!(
            "{cause}. Refusing to start: rebuilding this index from empty would lose every \
             entry older than the last checkpoint. Restore from a backup, or set \
             REMEM_ALLOW_INDEX_REBUILD=1 to accept that loss."
        ),
    })
}

/// Serializes tests that toggle `REMEM_ALLOW_INDEX_REBUILD`.
///
/// The variable is process-global and read by `on_index_parse_failure`, which
/// every index open goes through. `cargo test` runs test functions on parallel
/// threads (CI invokes a plain `cargo test -p remem-server`), so a test that
/// sets the variable can make an unrelated test's corrupt-index open succeed
/// when it expected a refusal, and vice versa. Any test that writes this
/// variable — or that depends on it being unset — must hold this lock.
#[cfg(test)]
pub(crate) fn index_rebuild_env_guard() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod parse_failure_tests {
    use super::*;
    use crate::engine::error::StorageError;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn cause() -> StorageError {
        StorageError::Corruption {
            file: PathBuf::from("hnsw.manifest"),
            message: "bad checksum".into(),
        }
    }

    #[test]
    fn parse_failure_is_an_error_by_default() {
        let _lock = index_rebuild_env_guard();
        let dir = tempdir().unwrap();
        let path = dir.path().join("hnsw.manifest");

        let err = on_index_parse_failure(&path, &cause())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("hnsw.manifest"),
            "error should name the file: {err}"
        );
        assert!(
            err.contains("REMEM_ALLOW_INDEX_REBUILD"),
            "error should tell the operator their options: {err}"
        );
    }

    #[test]
    fn parse_failure_is_tolerated_when_the_operator_opts_in() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("hnsw.manifest");

        let _lock = index_rebuild_env_guard();
        std::env::set_var("REMEM_ALLOW_INDEX_REBUILD", "1");
        let result = on_index_parse_failure(&path, &cause());
        std::env::remove_var("REMEM_ALLOW_INDEX_REBUILD");

        assert!(result.is_ok());
    }

    #[test]
    fn any_value_other_than_one_still_refuses() {
        let _lock = index_rebuild_env_guard();
        let dir = tempdir().unwrap();
        let path = dir.path().join("hnsw.manifest");

        std::env::set_var("REMEM_ALLOW_INDEX_REBUILD", "true");
        let result = on_index_parse_failure(&path, &cause());
        std::env::remove_var("REMEM_ALLOW_INDEX_REBUILD");

        assert!(result.is_err(), "only the exact value \"1\" opts in");
    }
}
