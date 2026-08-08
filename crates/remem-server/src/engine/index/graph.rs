//! CSR (Compressed Sparse Row) Graph storage for relationship traversal
//!
//! This module implements a CSR graph format for efficient graph storage and
//! traversal. CSR is optimized for:
//! - Memory efficiency: O(|V| + |E|) space
//! - Fast neighbor iteration: O(degree) per node
//! - Cache-friendly traversal
//!
//! # Format
//!
//! The CSR format uses two main arrays:
//! - `offsets[i]` points to where node i's edges start in the edges array
//! - `edges[]` contains all destination node IDs sequentially
//!
//! For efficient external ID mapping, we maintain bidirectional mappings
//! between external IDs (Bytes) and internal numeric IDs (u32).

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::engine::error::Result;

/// Configuration for the CSR graph
#[derive(Debug, Clone)]
pub struct GraphConfig {
    /// Maximum number of nodes to pre-allocate
    pub max_nodes: usize,
    /// Average edges per node (for pre-allocation)
    pub avg_edges_per_node: usize,
    /// Whether the graph is directed
    pub directed: bool,
}

impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            max_nodes: 1_000_000,
            avg_edges_per_node: 10,
            directed: true,
        }
    }
}

impl GraphConfig {
    /// Set whether the graph is directed
    pub fn directed(mut self, directed: bool) -> Self {
        self.directed = directed;
        self
    }
}

/// Edge metadata
#[derive(Debug, Clone)]
pub struct EdgeMetadata {
    /// Edge type/label
    pub edge_type: String,
    /// Edge weight (default 1.0)
    pub weight: f32,
    /// Creation timestamp
    pub timestamp: u64,
}

impl Default for EdgeMetadata {
    fn default() -> Self {
        Self {
            edge_type: String::new(),
            weight: 1.0,
            timestamp: 0,
        }
    }
}

impl EdgeMetadata {
    /// Create a new edge with type
    pub fn with_type(edge_type: impl Into<String>) -> Self {
        Self {
            edge_type: edge_type.into(),
            ..Default::default()
        }
    }

    /// Set edge weight
    pub fn weight(mut self, weight: f32) -> Self {
        self.weight = weight;
        self
    }

    /// Set timestamp
    pub fn timestamp(mut self, ts: u64) -> Self {
        self.timestamp = ts;
        self
    }
}

/// Internal adjacency list node (used during construction)
#[derive(Debug)]
struct AdjacencyNode {
    /// Outgoing edges: (target_internal_id, metadata)
    edges: Vec<(u32, EdgeMetadata)>,
}

/// CSR Graph for efficient relationship storage and traversal
pub struct CsrGraph {
    /// Configuration
    config: GraphConfig,

    /// External ID -> Internal ID mapping
    id_to_internal: RwLock<HashMap<Bytes, u32>>,

    /// Internal ID -> External ID mapping
    internal_to_id: RwLock<Vec<Bytes>>,

    /// Adjacency lists (during construction phase)
    /// When finalized, this is converted to CSR format
    adjacency: RwLock<Vec<AdjacencyNode>>,

    /// CSR offset array (node i's edges start at offsets[i])
    /// Only populated after finalize()
    csr_offsets: RwLock<Vec<usize>>,

    /// CSR edges array (destination node IDs)
    csr_edges: RwLock<Vec<u32>>,

    /// CSR edge metadata
    csr_metadata: RwLock<Vec<EdgeMetadata>>,

    /// Whether the graph is in CSR format (finalized)
    is_finalized: AtomicBool,

    /// Number of nodes
    node_count: AtomicUsize,

    /// Number of edges
    edge_count: AtomicUsize,

    /// Whether the graph has been modified since last save
    dirty: AtomicBool,
}

impl CsrGraph {
    /// Create a new empty graph
    pub fn new(config: GraphConfig) -> Self {
        Self {
            config,
            id_to_internal: RwLock::new(HashMap::new()),
            internal_to_id: RwLock::new(Vec::new()),
            adjacency: RwLock::new(Vec::new()),
            csr_offsets: RwLock::new(Vec::new()),
            csr_edges: RwLock::new(Vec::new()),
            csr_metadata: RwLock::new(Vec::new()),
            is_finalized: AtomicBool::new(false),
            node_count: AtomicUsize::new(0),
            edge_count: AtomicUsize::new(0),
            dirty: AtomicBool::new(false),
        }
    }

    /// Get or create internal ID for an external ID
    fn get_or_create_internal_id(&self, external_id: Bytes) -> u32 {
        // Fast path: check if already exists
        {
            let id_map = self.id_to_internal.read();
            if let Some(&id) = id_map.get(&external_id) {
                return id;
            }
        }

        // Slow path: create new entry
        let mut id_map = self.id_to_internal.write();
        let mut internal_to_id = self.internal_to_id.write();
        let mut adjacency = self.adjacency.write();

        // Double-check after acquiring write lock
        if let Some(&id) = id_map.get(&external_id) {
            return id;
        }

        let internal_id = internal_to_id.len() as u32;
        id_map.insert(external_id.clone(), internal_id);
        internal_to_id.push(external_id);
        adjacency.push(AdjacencyNode { edges: Vec::new() });
        self.node_count.fetch_add(1, Ordering::Relaxed);

        internal_id
    }

    /// Add an edge to the graph
    ///
    /// If an edge with the same (source, target, edge_type) already exists,
    /// updates the weight and timestamp instead of creating a duplicate.
    /// If the graph is undirected, also adds/updates the reverse edge.
    ///
    /// If the graph is currently finalized (CSR cache active), it is
    /// automatically unfinalized so the adjacency list stays authoritative.
    /// Callers do not need to call `unfinalize()` before `add_edge()`.
    pub fn add_edge(
        &self,
        source: impl Into<Bytes>,
        target: impl Into<Bytes>,
        metadata: EdgeMetadata,
    ) -> Result<()> {
        // Invalidate the CSR cache on any mutation. The adjacency list is
        // always the source of truth; CSR is a read-only optimisation.
        self.is_finalized.store(false, Ordering::Release);

        let source = source.into();
        let target = target.into();

        let source_id = self.get_or_create_internal_id(source.clone());
        let target_id = self.get_or_create_internal_id(target.clone());

        // Add or update forward edge with deduplication
        let added_forward = {
            let mut adjacency = self.adjacency.write();
            let edges = &mut adjacency[source_id as usize].edges;

            // Check for existing edge with same target and edge_type
            if let Some(existing) = edges
                .iter_mut()
                .find(|(tid, meta)| *tid == target_id && meta.edge_type == metadata.edge_type)
            {
                // Update existing edge's weight and timestamp
                existing.1.weight = metadata.weight;
                existing.1.timestamp = metadata.timestamp;
                false // Not a new edge
            } else {
                // Add new edge
                edges.push((target_id, metadata.clone()));
                true // New edge added
            }
        };

        if added_forward {
            self.edge_count.fetch_add(1, Ordering::Relaxed);
        }

        // Add or update reverse edge if undirected
        if !self.config.directed {
            let added_reverse = {
                let mut adjacency = self.adjacency.write();
                let edges = &mut adjacency[target_id as usize].edges;

                if let Some(existing) = edges
                    .iter_mut()
                    .find(|(tid, meta)| *tid == source_id && meta.edge_type == metadata.edge_type)
                {
                    existing.1.weight = metadata.weight;
                    existing.1.timestamp = metadata.timestamp;
                    false
                } else {
                    edges.push((source_id, metadata));
                    true
                }
            };

            if added_reverse {
                self.edge_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.dirty.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Computes the (source, target) pairs for all edges incident to `node` —
    /// both outgoing (node → X) and incoming (X → node) — without mutating the
    /// adjacency lists. Used by callers that need to WAL-log a removal before
    /// applying it (log-before-mutate).
    pub fn peek_node_edges(&self, node: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let node_internal = {
            let id_map = self.id_to_internal.read();
            match id_map.get(node) {
                Some(&id) => id,
                None => return Ok(Vec::new()),
            }
        };

        let node_bytes = Bytes::copy_from_slice(node);
        // Snapshot external IDs before taking the adjacency read lock.
        let id_snapshot: Vec<Bytes> = self.internal_to_id.read().iter().cloned().collect();

        let mut found: Vec<(Bytes, Bytes)> = Vec::new();

        {
            let adjacency = self.adjacency.read();

            // 1. Outgoing edges: node -> X.
            for (target_internal, _) in adjacency[node_internal as usize].edges.iter() {
                found.push((
                    node_bytes.clone(),
                    id_snapshot[*target_internal as usize].clone(),
                ));
            }

            // 2. Incoming edges: scan all other nodes for edges pointing to node.
            for (src_internal, node_data) in adjacency.iter().enumerate() {
                if src_internal as u32 == node_internal {
                    continue;
                }
                let src_bytes = &id_snapshot[src_internal];
                for (tid, _) in node_data.edges.iter() {
                    if *tid == node_internal {
                        found.push((src_bytes.clone(), node_bytes.clone()));
                    }
                }
            }
        }

        Ok(found)
    }

    /// Remove all edges from `source` to `target`.
    ///
    /// Automatically unfinalizes (invalidates the CSR cache) before mutating
    /// the adjacency list. Returns `true` if at least one edge was removed.
    pub fn remove_edge(&self, source: &[u8], target: &[u8]) -> Result<bool> {
        // Invalidate the CSR cache on any mutation.
        self.is_finalized.store(false, Ordering::Release);

        let (source_id, target_id) = {
            let id_map = self.id_to_internal.read();
            match (id_map.get(source), id_map.get(target)) {
                (Some(&s), Some(&t)) => (s, t),
                _ => return Ok(false), // Either node doesn't exist — nothing to remove.
            }
        };

        let removed = {
            let mut adjacency = self.adjacency.write();
            let edges = &mut adjacency[source_id as usize].edges;
            let before = edges.len();
            edges.retain(|(tid, _)| *tid != target_id);
            before - edges.len()
        };

        if removed > 0 {
            self.edge_count.fetch_sub(removed, Ordering::Relaxed);
            self.dirty.store(true, Ordering::Relaxed);
        }

        // For undirected graphs, also remove the reverse edge.
        if !self.config.directed && removed > 0 {
            let mut adjacency = self.adjacency.write();
            let edges = &mut adjacency[target_id as usize].edges;
            let before = edges.len();
            edges.retain(|(tid, _)| *tid != source_id);
            let reverse_removed = before - edges.len();
            if reverse_removed > 0 {
                self.edge_count
                    .fetch_sub(reverse_removed, Ordering::Relaxed);
            }
        }

        Ok(removed > 0)
    }

    /// Add a node without any edges
    pub fn add_node(&self, external_id: impl Into<Bytes>) -> Result<u32> {
        // Invalidate the CSR cache on any mutation.
        self.is_finalized.store(false, Ordering::Release);

        let external_id = external_id.into();
        let internal_id = self.get_or_create_internal_id(external_id);
        self.dirty.store(true, Ordering::Relaxed);
        Ok(internal_id)
    }

    /// Get neighbors of a node (outgoing edges)
    ///
    /// Returns a vector of (target_external_id, metadata) pairs.
    pub fn get_neighbors(&self, external_id: &[u8]) -> Result<Vec<(Bytes, EdgeMetadata)>> {
        let internal_id = {
            let id_map = self.id_to_internal.read();
            match id_map.get(external_id) {
                Some(&id) => id,
                None => return Ok(Vec::new()),
            }
        };

        let internal_to_id = self.internal_to_id.read();

        if self.is_finalized.load(Ordering::Acquire) {
            // Use CSR format
            let offsets = self.csr_offsets.read();
            let edges = self.csr_edges.read();
            let metadata = self.csr_metadata.read();

            let start = offsets[internal_id as usize];
            let end = offsets[internal_id as usize + 1];

            let mut neighbors = Vec::with_capacity(end - start);
            for i in start..end {
                let target_id = edges[i];
                let target_external = internal_to_id[target_id as usize].clone();
                neighbors.push((target_external, metadata[i].clone()));
            }
            Ok(neighbors)
        } else {
            // Use adjacency list format
            let adjacency = self.adjacency.read();
            let node = &adjacency[internal_id as usize];

            let mut neighbors = Vec::with_capacity(node.edges.len());
            for (target_id, meta) in &node.edges {
                let target_external = internal_to_id[*target_id as usize].clone();
                neighbors.push((target_external, meta.clone()));
            }
            Ok(neighbors)
        }
    }

    /// Traverse the graph using BFS starting from a node
    ///
    /// Returns nodes within `max_depth` hops from the source.
    pub fn traverse_bfs(&self, start: &[u8], max_depth: usize) -> Result<Vec<TraversalResult>> {
        // Get internal ID for start node
        let start_internal = {
            let id_map = self.id_to_internal.read();
            match id_map.get(start) {
                Some(&id) => id,
                None => return Ok(Vec::new()),
            }
        };

        let internal_to_id = self.internal_to_id.read();

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut results = Vec::new();

        queue.push_back((start_internal, 0usize, None::<EdgeMetadata>));
        visited.insert(start_internal);

        while let Some((current, depth, edge_meta)) = queue.pop_front() {
            let external_id = internal_to_id[current as usize].clone();

            results.push(TraversalResult {
                node_id: external_id,
                depth,
                edge_metadata: edge_meta,
            });

            if depth >= max_depth {
                continue;
            }

            // Get neighbors
            let neighbors = if self.is_finalized.load(Ordering::Acquire) {
                let offsets = self.csr_offsets.read();
                let edges = self.csr_edges.read();
                let metadata = self.csr_metadata.read();

                let start_offset = offsets[current as usize];
                let end_offset = offsets[current as usize + 1];

                (start_offset..end_offset)
                    .map(|i| (edges[i], metadata[i].clone()))
                    .collect::<Vec<_>>()
            } else {
                let adjacency = self.adjacency.read();
                adjacency[current as usize].edges.clone()
            };

            for (neighbor_id, meta) in neighbors {
                if visited.insert(neighbor_id) {
                    queue.push_back((neighbor_id, depth + 1, Some(meta)));
                }
            }
        }

        Ok(results)
    }

    /// Traverse the graph using BFS with edge type filter
    pub fn traverse_bfs_with_type(
        &self,
        start: &[u8],
        max_depth: usize,
        edge_types: &[String],
    ) -> Result<Vec<TraversalResult>> {
        let start_internal = {
            let id_map = self.id_to_internal.read();
            match id_map.get(start) {
                Some(&id) => id,
                None => return Ok(Vec::new()),
            }
        };

        let internal_to_id = self.internal_to_id.read();
        let edge_type_set: HashSet<&String> = edge_types.iter().collect();

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut results = Vec::new();

        queue.push_back((start_internal, 0usize, None::<EdgeMetadata>));
        visited.insert(start_internal);

        while let Some((current, depth, edge_meta)) = queue.pop_front() {
            let external_id = internal_to_id[current as usize].clone();

            results.push(TraversalResult {
                node_id: external_id,
                depth,
                edge_metadata: edge_meta,
            });

            if depth >= max_depth {
                continue;
            }

            let neighbors = if self.is_finalized.load(Ordering::Acquire) {
                let offsets = self.csr_offsets.read();
                let edges = self.csr_edges.read();
                let metadata = self.csr_metadata.read();

                let start_offset = offsets[current as usize];
                let end_offset = offsets[current as usize + 1];

                (start_offset..end_offset)
                    .map(|i| (edges[i], metadata[i].clone()))
                    .collect::<Vec<_>>()
            } else {
                let adjacency = self.adjacency.read();
                adjacency[current as usize].edges.clone()
            };

            for (neighbor_id, meta) in neighbors {
                if (edge_type_set.is_empty() || edge_type_set.contains(&meta.edge_type))
                    && visited.insert(neighbor_id)
                {
                    queue.push_back((neighbor_id, depth + 1, Some(meta)));
                }
            }
        }

        Ok(results)
    }

    /// Get the number of nodes in the graph
    pub fn node_count(&self) -> usize {
        self.node_count.load(Ordering::Relaxed)
    }

    /// Get the number of edges in the graph
    pub fn edge_count(&self) -> usize {
        self.edge_count.load(Ordering::Relaxed)
    }

    /// Check if the graph has been modified
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }

    /// Mark the graph as clean
    pub fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Relaxed);
    }

    /// Save the graph to a file.
    ///
    /// The read locks on `internal_to_id` and `adjacency` are held only for the
    /// in-memory snapshot, not during disk I/O. This prevents checkpoints from
    /// blocking mutations for the full write duration.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();

        // --- Snapshot while holding read locks (fast, in-memory only) ---
        let (node_count, ids, adjacency_snapshot) = {
            let internal_to_id = self.internal_to_id.read();
            let adjacency = self.adjacency.read();
            let ids: Vec<Bytes> = internal_to_id.iter().cloned().collect();
            let adj: Vec<Vec<(u32, EdgeMetadata)>> =
                adjacency.iter().map(|n| n.edges.clone()).collect();
            let count = self.node_count.load(Ordering::Relaxed) as u32;
            (count, ids, adj)
            // both read locks released here
        };

        // --- Write snapshot to disk without holding any lock ---
        let tmp_path = path.with_extension("tmp");
        let file = std::fs::File::create(&tmp_path)?;
        let mut writer = std::io::BufWriter::new(file);

        // Write header
        writer.write_all(b"CSRG")?;
        writer.write_all(&1u32.to_le_bytes())?;

        // Write config
        writer.write_all(&(self.config.max_nodes as u64).to_le_bytes())?;
        writer.write_all(&(self.config.avg_edges_per_node as u32).to_le_bytes())?;
        writer.write_all(&[self.config.directed as u8])?;

        // Write node count
        writer.write_all(&node_count.to_le_bytes())?;

        // Write external IDs
        for external_id in &ids {
            writer.write_all(&(external_id.len() as u32).to_le_bytes())?;
            writer.write_all(external_id)?;
        }

        // Write adjacency list
        for edges in &adjacency_snapshot {
            writer.write_all(&(edges.len() as u32).to_le_bytes())?;
            for (target_id, meta) in edges {
                writer.write_all(&target_id.to_le_bytes())?;
                let type_bytes = meta.edge_type.as_bytes();
                writer.write_all(&(type_bytes.len() as u32).to_le_bytes())?;
                writer.write_all(type_bytes)?;
                writer.write_all(&meta.weight.to_le_bytes())?;
                writer.write_all(&meta.timestamp.to_le_bytes())?;
            }
        }

        writer.flush()?;
        drop(writer);
        std::fs::rename(&tmp_path, path)?;
        self.mark_clean();
        Ok(())
    }
}

/// Result from graph traversal
#[derive(Debug, Clone)]
pub struct TraversalResult {
    /// Node external ID
    pub node_id: Bytes,
    /// Depth from start node (0 = start node itself)
    pub depth: usize,
    /// Metadata of the edge used to reach this node (None for start node)
    pub edge_metadata: Option<EdgeMetadata>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_graph() {
        let graph = CsrGraph::new(GraphConfig::default());
        assert_eq!(graph.node_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_add_nodes_and_edges() {
        let graph = CsrGraph::new(GraphConfig::default());

        graph
            .add_edge(b"A".to_vec(), b"B".to_vec(), EdgeMetadata::default())
            .unwrap();
        graph
            .add_edge(b"B".to_vec(), b"C".to_vec(), EdgeMetadata::default())
            .unwrap();
        graph
            .add_edge(b"A".to_vec(), b"C".to_vec(), EdgeMetadata::default())
            .unwrap();

        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 3);
    }

    #[test]
    fn test_get_neighbors() {
        let graph = CsrGraph::new(GraphConfig::default());

        graph
            .add_edge(b"A".to_vec(), b"B".to_vec(), EdgeMetadata::default())
            .unwrap();
        graph
            .add_edge(b"A".to_vec(), b"C".to_vec(), EdgeMetadata::default())
            .unwrap();
        graph
            .add_edge(b"B".to_vec(), b"C".to_vec(), EdgeMetadata::default())
            .unwrap();

        let neighbors = graph.get_neighbors(b"A").unwrap();
        assert_eq!(neighbors.len(), 2);

        let neighbor_ids: Vec<&[u8]> = neighbors.iter().map(|(id, _)| id.as_ref()).collect();
        assert!(neighbor_ids.contains(&b"B".as_ref()));
        assert!(neighbor_ids.contains(&b"C".as_ref()));
    }

    #[test]
    fn test_traverse_bfs_depth_ordering() {
        // Was `test_finalize_and_traverse`: exercised the (now-removed) CSR
        // `finalize()` fast path around this same BFS. `finalize`/`unfinalize`
        // were deleted as dead code (REM-36 cascade from removing
        // `StorageEngine::finalize_graph`/`unfinalize_graph`, which had zero
        // real callers) -- the graph always serves reads correctly via the
        // adjacency-list representation regardless, so this keeps covering
        // multi-hop BFS depth assignment, just without the no-longer-existent
        // finalize step.
        let graph = CsrGraph::new(GraphConfig::default());

        graph
            .add_edge(b"A".to_vec(), b"B".to_vec(), EdgeMetadata::default())
            .unwrap();
        graph
            .add_edge(b"B".to_vec(), b"C".to_vec(), EdgeMetadata::default())
            .unwrap();
        graph
            .add_edge(b"C".to_vec(), b"D".to_vec(), EdgeMetadata::default())
            .unwrap();

        let results = graph.traverse_bfs(b"A", 3).unwrap();
        assert_eq!(results.len(), 4); // A, B, C, D

        assert_eq!(results[0].depth, 0);
        assert_eq!(results[1].depth, 1);
        assert_eq!(results[2].depth, 2);
        assert_eq!(results[3].depth, 3);
    }

    #[test]
    fn test_traverse_with_max_depth() {
        let graph = CsrGraph::new(GraphConfig::default());

        graph
            .add_edge(b"A".to_vec(), b"B".to_vec(), EdgeMetadata::default())
            .unwrap();
        graph
            .add_edge(b"B".to_vec(), b"C".to_vec(), EdgeMetadata::default())
            .unwrap();
        graph
            .add_edge(b"C".to_vec(), b"D".to_vec(), EdgeMetadata::default())
            .unwrap();

        let results = graph.traverse_bfs(b"A", 1).unwrap();
        assert_eq!(results.len(), 2); // A and B only
    }

    #[test]
    fn test_edge_metadata() {
        let graph = CsrGraph::new(GraphConfig::default());

        let meta = EdgeMetadata::with_type("follows")
            .weight(0.8)
            .timestamp(12345);

        graph.add_edge(b"A".to_vec(), b"B".to_vec(), meta).unwrap();

        let neighbors = graph.get_neighbors(b"A").unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].1.edge_type, "follows");
        assert!((neighbors[0].1.weight - 0.8).abs() < 0.001);
        assert_eq!(neighbors[0].1.timestamp, 12345);
    }

    #[test]
    fn test_remove_edge() {
        let graph = CsrGraph::new(GraphConfig::default());

        graph
            .add_edge(b"A".to_vec(), b"B".to_vec(), EdgeMetadata::default())
            .unwrap();
        graph
            .add_edge(b"A".to_vec(), b"C".to_vec(), EdgeMetadata::default())
            .unwrap();
        assert_eq!(graph.edge_count(), 2);

        let removed = graph.remove_edge(b"A", b"B").unwrap();
        assert!(removed);
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn test_remove_edge_nonexistent() {
        let graph = CsrGraph::new(GraphConfig::default());
        graph
            .add_edge(b"A".to_vec(), b"B".to_vec(), EdgeMetadata::default())
            .unwrap();

        // Removing an edge that doesn't exist returns false, not an error.
        let removed = graph.remove_edge(b"A", b"C").unwrap();
        assert!(!removed);
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn test_remove_edge_undirected() {
        let config = GraphConfig::default().directed(false);
        let graph = CsrGraph::new(config);

        graph
            .add_edge(b"A".to_vec(), b"B".to_vec(), EdgeMetadata::default())
            .unwrap();
        assert_eq!(graph.edge_count(), 2); // forward + reverse

        let removed = graph.remove_edge(b"A", b"B").unwrap();
        assert!(removed);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_traverse_bfs_with_type() {
        let graph = CsrGraph::new(GraphConfig::default());

        graph
            .add_edge(
                b"A".to_vec(),
                b"B".to_vec(),
                EdgeMetadata::with_type("related"),
            )
            .unwrap();
        graph
            .add_edge(
                b"A".to_vec(),
                b"C".to_vec(),
                EdgeMetadata::with_type("unrelated"),
            )
            .unwrap();
        graph
            .add_edge(
                b"B".to_vec(),
                b"D".to_vec(),
                EdgeMetadata::with_type("related"),
            )
            .unwrap();

        let results = graph
            .traverse_bfs_with_type(b"A", 3, &["related".to_string()])
            .unwrap();

        // Should only traverse "related" edges: A -> B -> D
        assert_eq!(results.len(), 3);
    }
}
