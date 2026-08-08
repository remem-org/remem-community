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
pub mod segment_io;

pub use btree::{BTreeConfig, BTreeIndex};
pub use btree_segmented::SegmentedBTreeIndex;
pub use graph::{CsrGraph, EdgeMetadata, GraphConfig, TraversalResult};
#[cfg(feature = "kuzu")]
pub use graph_kuzu::KuzuGraphIndex;
pub use graph_segmented::SegmentedCsrGraph;
pub use graph_wrapper::{load_graph_index, new_graph_index, GraphIndex};
pub use hnsw::{HnswConfig, HnswIndex};
pub use inverted::{InvertedIndex, InvertedIndexConfig};
pub use inverted_segmented::SegmentedInvertedIndex;

// ── Chunk size constants ───────────────────────────────────────────────────────

/// Number of nodes per sealed HNSW chunk.
pub const HNSW_CHUNK_SIZE: u32 = 50_000;
/// Number of docs per sealed tag segment.
pub const TAGS_CHUNK_SIZE: u32 = 10_000;
/// Maximum tag segment count before compaction is triggered.
pub const MAX_TAG_SEGMENTS: usize = 20;
/// Deletion ratio threshold for compaction.
pub const COMPACTION_DELETION_RATIO: f64 = 0.2;
