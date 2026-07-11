//! Index implementations for the storage engine
#![allow(unused_imports)]
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
pub use dirty::DirtyChunkTracker;
pub use graph::{CsrGraph, Edge, EdgeMetadata, GraphConfig, TraversalResult};
pub use graph_segmented::SegmentedCsrGraph;
#[cfg(feature = "kuzu")]
pub use graph_kuzu::KuzuGraphIndex;
pub use graph_wrapper::{new_graph_index, load_graph_index, GraphIndex};
pub use hnsw::{HnswConfig, HnswIndex};
pub use inverted::{InvertedIndex, InvertedIndexConfig};
pub use inverted_segmented::SegmentedInvertedIndex;
pub use manifest::{ChunkMeta, SegmentManifest};
pub use segment_io::{
    SegmentHeader, SegmentReader, SegmentWriter,
    INDEX_TYPE_BTREE, INDEX_TYPE_GRAPH, INDEX_TYPE_HNSW, INDEX_TYPE_INVERTED,
};

// ── Chunk size constants ───────────────────────────────────────────────────────

/// Number of nodes per sealed HNSW chunk.
pub const HNSW_CHUNK_SIZE: u32 = 50_000;
/// Number of nodes per sealed graph chunk.
pub const GRAPH_CHUNK_SIZE: u32 = 10_000;
/// Number of entries per sealed BTree chunk.
pub const BTREE_CHUNK_SIZE: u32 = 20_000;
/// Number of docs per sealed tag segment.
pub const TAGS_CHUNK_SIZE: u32 = 10_000;
/// Maximum tag segment count before compaction is triggered.
pub const MAX_TAG_SEGMENTS: usize = 20;
/// Deletion ratio threshold for compaction.
pub const COMPACTION_DELETION_RATIO: f64 = 0.2;
