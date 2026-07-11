//! Type wrapper for selecting between CSR graph and Kuzu graph implementations.

use super::{EdgeMetadata, GraphConfig, TraversalResult};
use crate::engine::error::Result;
use bytes::Bytes;
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "kuzu")]
use super::KuzuGraphIndex;
use super::SegmentedCsrGraph;

/// Graph index type selected at compile time based on the `kuzu` feature flag.
#[cfg(feature = "kuzu")]
pub type GraphIndex = KuzuGraphIndex;

#[cfg(not(feature = "kuzu"))]
pub type GraphIndex = SegmentedCsrGraph;

/// Create a new graph index with the configured backend.
pub fn new_graph_index(config: GraphConfig, dir: PathBuf) -> Result<GraphIndex> {
    #[cfg(feature = "kuzu")]
    return KuzuGraphIndex::new(config, dir);

    #[cfg(not(feature = "kuzu"))]
    return Ok(SegmentedCsrGraph::new(config, dir));
}

/// Load a graph index from disk with the configured backend.
pub fn load_graph_index(config: GraphConfig, dir: PathBuf) -> Result<GraphIndex> {
    #[cfg(feature = "kuzu")]
    return KuzuGraphIndex::load_from_dir(config, dir);

    #[cfg(not(feature = "kuzu"))]
    return Ok(SegmentedCsrGraph::load_from_dir(config, dir));
}
