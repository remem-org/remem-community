//! CSR (Compressed Sparse Row) Graph storage for relationship traversal
#![allow(dead_code)]
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
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::engine::error::{Result, StorageError};

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
    /// Create a new config with custom max nodes
    pub fn with_max_nodes(max_nodes: usize) -> Self {
        Self {
            max_nodes,
            ..Default::default()
        }
    }

    /// Set whether the graph is directed
    pub fn directed(mut self, directed: bool) -> Self {
        self.directed = directed;
        self
    }

    /// Set average edges per node
    pub fn avg_edges(mut self, avg: usize) -> Self {
        self.avg_edges_per_node = avg;
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

/// Edge information
#[derive(Debug, Clone)]
pub struct Edge {
    /// Source node external ID
    pub source: Bytes,
    /// Target node external ID
    pub target: Bytes,
    /// Edge metadata
    pub metadata: EdgeMetadata,
}

/// Internal adjacency list node (used during construction)
#[derive(Debug)]
struct AdjacencyNode {
    /// External ID (stored for debugging/future use)
    #[allow(dead_code)]
    external_id: Bytes,
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
        internal_to_id.push(external_id.clone());
        adjacency.push(AdjacencyNode {
            external_id,
            edges: Vec::new(),
        });
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

    /// Add an edge with just source and target (default metadata)
    pub fn add_edge_simple(
        &self,
        source: impl Into<Bytes>,
        target: impl Into<Bytes>,
    ) -> Result<()> {
        self.add_edge(source, target, EdgeMetadata::default())
    }

    /// Remove all edges incident to `node` — both outgoing (node → X) and
    /// incoming (X → node). Returns the (source, target) pairs removed so the
    /// caller can write corresponding WAL records.
    pub fn remove_node_edges(&self, node: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        self.is_finalized.store(false, Ordering::Release);

        let node_internal = {
            let id_map = self.id_to_internal.read();
            match id_map.get(node) {
                Some(&id) => id,
                None => return Ok(Vec::new()),
            }
        };

        let node_bytes = Bytes::copy_from_slice(node);
        // Snapshot external IDs before taking adjacency write lock.
        let id_snapshot: Vec<Bytes> = self.internal_to_id.read().iter().cloned().collect();

        let mut removed: Vec<(Bytes, Bytes)> = Vec::new();
        let mut total_removed = 0usize;

        {
            let mut adjacency = self.adjacency.write();

            // 1. Outgoing edges: drain all edges from node.
            let outgoing = std::mem::take(&mut adjacency[node_internal as usize].edges);
            total_removed += outgoing.len();
            for (target_internal, _) in outgoing {
                removed.push((
                    node_bytes.clone(),
                    id_snapshot[target_internal as usize].clone(),
                ));
            }

            // 2. Incoming edges: scan all other nodes and remove edges pointing to node.
            for (src_internal, node_data) in adjacency.iter_mut().enumerate() {
                if src_internal as u32 == node_internal {
                    continue;
                }
                let before = node_data.edges.len();
                node_data.edges.retain(|(tid, _)| *tid != node_internal);
                let n = before - node_data.edges.len();
                if n > 0 {
                    let src_bytes = id_snapshot[src_internal].clone();
                    for _ in 0..n {
                        removed.push((src_bytes.clone(), node_bytes.clone()));
                    }
                    total_removed += n;
                }
            }
        }

        if total_removed > 0 {
            self.edge_count.fetch_sub(total_removed, Ordering::Relaxed);
            self.dirty.store(true, Ordering::Relaxed);
        }

        Ok(removed)
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
                self.edge_count.fetch_sub(reverse_removed, Ordering::Relaxed);
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

    /// Finalize the graph into CSR format for efficient traversal
    ///
    /// After finalization, no more edges can be added until unfinalize() is called.
    pub fn finalize(&self) -> Result<()> {
        if self.is_finalized.load(Ordering::Relaxed) {
            return Ok(()); // Already finalized
        }

        let adjacency = self.adjacency.read();
        let node_count = adjacency.len();

        let mut offsets = Vec::with_capacity(node_count + 1);
        let mut edges = Vec::new();
        let mut metadata = Vec::new();

        let mut offset = 0;
        for node in adjacency.iter() {
            offsets.push(offset);
            for (target_id, meta) in &node.edges {
                edges.push(*target_id);
                metadata.push(meta.clone());
                offset += 1;
            }
        }
        offsets.push(offset); // Final offset

        // Store CSR arrays
        *self.csr_offsets.write() = offsets;
        *self.csr_edges.write() = edges;
        *self.csr_metadata.write() = metadata;

        self.is_finalized.store(true, Ordering::Release);
        Ok(())
    }

    /// Unfinalize the graph to allow adding more edges
    pub fn unfinalize(&self) {
        self.is_finalized.store(false, Ordering::Release);
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

    /// Get neighbors with a specific edge type
    pub fn get_neighbors_by_type(
        &self,
        external_id: &[u8],
        edge_type: &str,
    ) -> Result<Vec<(Bytes, EdgeMetadata)>> {
        let neighbors = self.get_neighbors(external_id)?;
        Ok(neighbors
            .into_iter()
            .filter(|(_, meta)| meta.edge_type == edge_type)
            .collect())
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

    /// Check if a node exists in the graph
    pub fn contains_node(&self, external_id: &[u8]) -> bool {
        self.id_to_internal.read().contains_key(external_id)
    }

    /// Check if an edge exists between two nodes
    pub fn has_edge(&self, source: &[u8], target: &[u8]) -> bool {
        let (source_id, target_id) = {
            let id_map = self.id_to_internal.read();
            match (id_map.get(source), id_map.get(target)) {
                (Some(&s), Some(&t)) => (s, t),
                _ => return false,
            }
        };

        if self.is_finalized.load(Ordering::Acquire) {
            let offsets = self.csr_offsets.read();
            let edges = self.csr_edges.read();

            let start = offsets[source_id as usize];
            let end = offsets[source_id as usize + 1];

            edges[start..end].contains(&target_id)
        } else {
            let adjacency = self.adjacency.read();
            adjacency[source_id as usize]
                .edges
                .iter()
                .any(|(t, _)| *t == target_id)
        }
    }

    /// Get the number of nodes in the graph
    pub fn node_count(&self) -> usize {
        self.node_count.load(Ordering::Relaxed)
    }

    /// Get the number of edges in the graph
    pub fn edge_count(&self) -> usize {
        self.edge_count.load(Ordering::Relaxed)
    }

    /// Check if the graph is empty
    pub fn is_empty(&self) -> bool {
        self.node_count() == 0
    }

    /// Check if the graph is finalized
    pub fn is_finalized(&self) -> bool {
        self.is_finalized.load(Ordering::Relaxed)
    }

    /// Check if the graph has been modified
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }

    /// Mark the graph as clean
    pub fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Relaxed);
    }

    /// Get the degree (number of outgoing edges) for a node
    pub fn out_degree(&self, external_id: &[u8]) -> usize {
        let internal_id = {
            let id_map = self.id_to_internal.read();
            match id_map.get(external_id) {
                Some(&id) => id,
                None => return 0,
            }
        };

        if self.is_finalized.load(Ordering::Acquire) {
            let offsets = self.csr_offsets.read();
            offsets[internal_id as usize + 1] - offsets[internal_id as usize]
        } else {
            let adjacency = self.adjacency.read();
            adjacency[internal_id as usize].edges.len()
        }
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

    /// Load a graph from a file
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = std::fs::File::open(path)?;
        let mut file = std::io::BufReader::new(file);

        // Read and verify magic
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != b"CSRG" {
            return Err(StorageError::invalid_format(
                path,
                "Invalid CSR graph magic",
            ));
        }

        // Read version
        let mut buf4 = [0u8; 4];
        file.read_exact(&mut buf4)?;
        let version = u32::from_le_bytes(buf4);
        if version != 1 {
            return Err(StorageError::invalid_format(
                path,
                format!("Unsupported CSR graph version: {}", version),
            ));
        }

        // Read config
        let mut buf8 = [0u8; 8];
        file.read_exact(&mut buf8)?;
        let max_nodes = u64::from_le_bytes(buf8) as usize;

        file.read_exact(&mut buf4)?;
        let avg_edges_per_node = u32::from_le_bytes(buf4) as usize;

        let mut directed_byte = [0u8; 1];
        file.read_exact(&mut directed_byte)?;
        let directed = directed_byte[0] != 0;

        let config = GraphConfig {
            max_nodes,
            avg_edges_per_node,
            directed,
        };

        // Read node count
        file.read_exact(&mut buf4)?;
        let node_count = u32::from_le_bytes(buf4) as usize;

        // Read external IDs
        let mut id_to_internal = HashMap::with_capacity(node_count);
        let mut internal_to_id = Vec::with_capacity(node_count);

        for i in 0..node_count {
            file.read_exact(&mut buf4)?;
            let len = u32::from_le_bytes(buf4) as usize;
            let mut id_bytes = vec![0u8; len];
            file.read_exact(&mut id_bytes)?;
            let external_id = Bytes::from(id_bytes);

            id_to_internal.insert(external_id.clone(), i as u32);
            internal_to_id.push(external_id);
        }

        // Read adjacency list
        let mut adjacency = Vec::with_capacity(node_count);
        let mut total_edges = 0;

        for external_id in &internal_to_id {
            file.read_exact(&mut buf4)?;
            let edge_count = u32::from_le_bytes(buf4) as usize;
            total_edges += edge_count;

            let mut edges = Vec::with_capacity(edge_count);
            for _ in 0..edge_count {
                file.read_exact(&mut buf4)?;
                let target_id = u32::from_le_bytes(buf4);

                // Read edge metadata
                file.read_exact(&mut buf4)?;
                let type_len = u32::from_le_bytes(buf4) as usize;
                let mut type_bytes = vec![0u8; type_len];
                file.read_exact(&mut type_bytes)?;
                let edge_type = String::from_utf8(type_bytes)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;

                file.read_exact(&mut buf4)?;
                let weight = f32::from_le_bytes(buf4);

                file.read_exact(&mut buf8)?;
                let timestamp = u64::from_le_bytes(buf8);

                edges.push((
                    target_id,
                    EdgeMetadata {
                        edge_type,
                        weight,
                        timestamp,
                    },
                ));
            }

            adjacency.push(AdjacencyNode {
                external_id: external_id.clone(),
                edges,
            });
        }

        Ok(Self {
            config,
            id_to_internal: RwLock::new(id_to_internal),
            internal_to_id: RwLock::new(internal_to_id),
            adjacency: RwLock::new(adjacency),
            csr_offsets: RwLock::new(Vec::new()),
            csr_edges: RwLock::new(Vec::new()),
            csr_metadata: RwLock::new(Vec::new()),
            is_finalized: AtomicBool::new(false),
            node_count: AtomicUsize::new(node_count),
            edge_count: AtomicUsize::new(total_edges),
            dirty: AtomicBool::new(false),
        })
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
        assert!(graph.is_empty());
        assert_eq!(graph.node_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_add_nodes_and_edges() {
        let graph = CsrGraph::new(GraphConfig::default());

        graph.add_edge_simple(b"A".to_vec(), b"B".to_vec()).unwrap();
        graph.add_edge_simple(b"B".to_vec(), b"C".to_vec()).unwrap();
        graph.add_edge_simple(b"A".to_vec(), b"C".to_vec()).unwrap();

        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 3);

        assert!(graph.contains_node(b"A"));
        assert!(graph.contains_node(b"B"));
        assert!(graph.contains_node(b"C"));
        assert!(!graph.contains_node(b"D"));
    }

    #[test]
    fn test_get_neighbors() {
        let graph = CsrGraph::new(GraphConfig::default());

        graph.add_edge_simple(b"A".to_vec(), b"B".to_vec()).unwrap();
        graph.add_edge_simple(b"A".to_vec(), b"C".to_vec()).unwrap();
        graph.add_edge_simple(b"B".to_vec(), b"C".to_vec()).unwrap();

        let neighbors = graph.get_neighbors(b"A").unwrap();
        assert_eq!(neighbors.len(), 2);

        let neighbor_ids: Vec<&[u8]> = neighbors.iter().map(|(id, _)| id.as_ref()).collect();
        assert!(neighbor_ids.contains(&b"B".as_ref()));
        assert!(neighbor_ids.contains(&b"C".as_ref()));
    }

    #[test]
    fn test_finalize_and_traverse() {
        let graph = CsrGraph::new(GraphConfig::default());

        graph.add_edge_simple(b"A".to_vec(), b"B".to_vec()).unwrap();
        graph.add_edge_simple(b"B".to_vec(), b"C".to_vec()).unwrap();
        graph.add_edge_simple(b"C".to_vec(), b"D".to_vec()).unwrap();

        graph.finalize().unwrap();
        assert!(graph.is_finalized());

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

        graph.add_edge_simple(b"A".to_vec(), b"B".to_vec()).unwrap();
        graph.add_edge_simple(b"B".to_vec(), b"C".to_vec()).unwrap();
        graph.add_edge_simple(b"C".to_vec(), b"D".to_vec()).unwrap();

        let results = graph.traverse_bfs(b"A", 1).unwrap();
        assert_eq!(results.len(), 2); // A and B only
    }

    #[test]
    fn test_undirected_graph() {
        let config = GraphConfig::default().directed(false);
        let graph = CsrGraph::new(config);

        graph.add_edge_simple(b"A".to_vec(), b"B".to_vec()).unwrap();

        // Both directions should work
        assert!(graph.has_edge(b"A", b"B"));
        assert!(graph.has_edge(b"B", b"A"));
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
    fn test_filter_by_edge_type() {
        let graph = CsrGraph::new(GraphConfig::default());

        graph
            .add_edge(
                b"A".to_vec(),
                b"B".to_vec(),
                EdgeMetadata::with_type("follows"),
            )
            .unwrap();
        graph
            .add_edge(
                b"A".to_vec(),
                b"C".to_vec(),
                EdgeMetadata::with_type("likes"),
            )
            .unwrap();

        let follows = graph.get_neighbors_by_type(b"A", "follows").unwrap();
        assert_eq!(follows.len(), 1);
        assert_eq!(follows[0].0.as_ref(), b"B");

        let likes = graph.get_neighbors_by_type(b"A", "likes").unwrap();
        assert_eq!(likes.len(), 1);
        assert_eq!(likes[0].0.as_ref(), b"C");
    }

    #[test]
    fn test_save_and_load() {
        let graph = CsrGraph::new(GraphConfig::default());

        graph
            .add_edge(
                b"A".to_vec(),
                b"B".to_vec(),
                EdgeMetadata::with_type("follows").weight(0.5),
            )
            .unwrap();
        graph
            .add_edge(
                b"B".to_vec(),
                b"C".to_vec(),
                EdgeMetadata::with_type("likes"),
            )
            .unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.graph");

        graph.save(&path).unwrap();

        let loaded = CsrGraph::load(&path).unwrap();
        assert_eq!(loaded.node_count(), 3);
        assert_eq!(loaded.edge_count(), 2);

        let neighbors = loaded.get_neighbors(b"A").unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].1.edge_type, "follows");
    }

    #[test]
    fn test_out_degree() {
        let graph = CsrGraph::new(GraphConfig::default());

        graph.add_edge_simple(b"A".to_vec(), b"B".to_vec()).unwrap();
        graph.add_edge_simple(b"A".to_vec(), b"C".to_vec()).unwrap();
        graph.add_edge_simple(b"A".to_vec(), b"D".to_vec()).unwrap();

        assert_eq!(graph.out_degree(b"A"), 3);
        assert_eq!(graph.out_degree(b"B"), 0);
    }

    #[test]
    fn test_has_edge() {
        let graph = CsrGraph::new(GraphConfig::default());

        graph.add_edge_simple(b"A".to_vec(), b"B".to_vec()).unwrap();

        assert!(graph.has_edge(b"A", b"B"));
        assert!(!graph.has_edge(b"B", b"A")); // Directed graph
        assert!(!graph.has_edge(b"A", b"C"));
    }

    #[test]
    fn test_remove_edge() {
        let graph = CsrGraph::new(GraphConfig::default());

        graph.add_edge_simple(b"A".to_vec(), b"B".to_vec()).unwrap();
        graph.add_edge_simple(b"A".to_vec(), b"C".to_vec()).unwrap();
        assert_eq!(graph.edge_count(), 2);

        let removed = graph.remove_edge(b"A", b"B").unwrap();
        assert!(removed);
        assert_eq!(graph.edge_count(), 1);
        assert!(!graph.has_edge(b"A", b"B"));
        assert!(graph.has_edge(b"A", b"C"));
    }

    #[test]
    fn test_remove_edge_nonexistent() {
        let graph = CsrGraph::new(GraphConfig::default());
        graph.add_edge_simple(b"A".to_vec(), b"B".to_vec()).unwrap();

        // Removing an edge that doesn't exist returns false, not an error.
        let removed = graph.remove_edge(b"A", b"C").unwrap();
        assert!(!removed);
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn test_remove_edge_undirected() {
        let config = GraphConfig::default().directed(false);
        let graph = CsrGraph::new(config);

        graph.add_edge_simple(b"A".to_vec(), b"B".to_vec()).unwrap();
        assert_eq!(graph.edge_count(), 2); // forward + reverse

        let removed = graph.remove_edge(b"A", b"B").unwrap();
        assert!(removed);
        assert_eq!(graph.edge_count(), 0);
        assert!(!graph.has_edge(b"A", b"B"));
        assert!(!graph.has_edge(b"B", b"A"));
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
