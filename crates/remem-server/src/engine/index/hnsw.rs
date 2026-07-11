//! HNSW (Hierarchical Navigable Small World) index implementation
#![allow(dead_code)]
//!
//! This module implements an HNSW graph for approximate nearest neighbor search.
//! HNSW provides O(log n) search and insert complexity with high recall.
//!
//! # Algorithm Overview
//!
//! HNSW builds a multi-layer graph where:
//! - Higher layers contain fewer nodes with longer-range connections
//! - Lower layers contain more nodes with shorter-range connections
//! - Search starts from the top layer and descends through layers
//!
//! # References
//!
//! - Original paper: "Efficient and robust approximate nearest neighbor search using
//!   Hierarchical Navigable Small World graphs" by Malkov and Yashunin (2016)

use crate::engine::error::{Result, StorageError};
use crate::engine::index::dirty::DirtyChunkTracker;
use crate::engine::index::manifest::{ChunkMeta, SegmentManifest};
use crate::engine::index::segment_io::{SegmentHeader, SegmentReader, SegmentWriter, INDEX_TYPE_HNSW};
use crate::engine::index::HNSW_CHUNK_SIZE;
use crate::engine::util::simd::DistanceMetric;
use bytes::Bytes;
use ordered_float::OrderedFloat;
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

/// Configuration for the HNSW index
#[derive(Debug, Clone)]
pub struct HnswConfig {
    /// Vector dimension
    pub dim: usize,

    /// Maximum number of connections per node per layer (M parameter)
    /// Higher M = better recall, more memory, slower insert
    /// Typical values: 8-64, default: 16
    pub m: usize,

    /// Maximum number of connections for layer 0 (usually 2 * M)
    pub m0: usize,

    /// Size of dynamic candidate list during construction (ef_construction)
    /// Higher ef_construction = better recall, slower insert
    /// Typical values: 100-500, default: 200
    pub ef_construction: usize,

    /// Size of dynamic candidate list during search (ef_search)
    /// Higher ef_search = better recall, slower search
    /// Can be changed at query time
    /// Typical values: 50-500, default: 50
    pub ef_search: usize,

    /// Level generation multiplier (ml = 1/ln(M))
    /// Used to randomly assign nodes to layers
    pub ml: f64,

    /// Distance metric to use
    pub metric: DistanceMetric,

    /// Maximum number of vectors to store
    /// Used for pre-allocation
    pub max_elements: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        let m = 16;
        Self {
            dim: 384, // Common embedding dimension
            m,
            m0: m * 2,
            ef_construction: 200,
            ef_search: 50,
            ml: 1.0 / (m as f64).ln(),
            metric: DistanceMetric::default(),
            max_elements: 1_000_000,
        }
    }
}

impl HnswConfig {
    /// Create a new config with custom dimension
    pub fn with_dim(dim: usize) -> Self {
        Self {
            dim,
            ..Default::default()
        }
    }

    /// Set the M parameter
    pub fn m(mut self, m: usize) -> Self {
        self.m = m;
        self.m0 = m * 2;
        self.ml = 1.0 / (m as f64).ln();
        self
    }

    /// Set ef_construction
    pub fn ef_construction(mut self, ef_construction: usize) -> Self {
        self.ef_construction = ef_construction;
        self
    }

    /// Set ef_search
    pub fn ef_search(mut self, ef_search: usize) -> Self {
        self.ef_search = ef_search;
        self
    }

    /// Set distance metric
    pub fn metric(mut self, metric: DistanceMetric) -> Self {
        self.metric = metric;
        self
    }

    /// Set maximum elements
    pub fn max_elements(mut self, max_elements: usize) -> Self {
        self.max_elements = max_elements;
        self
    }
}

/// A node in the HNSW graph
#[derive(Debug)]
struct HnswNode {
    /// External ID (e.g., UUID bytes) for mapping back to storage
    external_id: Bytes,

    /// The vector data
    vector: Vec<f32>,

    /// Neighbors at each layer
    /// neighbors[layer] = list of internal node IDs
    neighbors: Vec<RwLock<Vec<u32>>>,

    /// Maximum layer this node exists in
    max_layer: usize,
}

impl HnswNode {
    fn new(external_id: Bytes, vector: Vec<f32>, max_layer: usize, m: usize, m0: usize) -> Self {
        let mut neighbors = Vec::with_capacity(max_layer + 1);
        for layer in 0..=max_layer {
            let capacity = if layer == 0 { m0 } else { m };
            neighbors.push(RwLock::new(Vec::with_capacity(capacity)));
        }
        Self {
            external_id,
            vector,
            neighbors,
            max_layer,
        }
    }

    fn get_neighbors(&self, layer: usize) -> Vec<u32> {
        if layer > self.max_layer {
            return Vec::new();
        }
        self.neighbors[layer].read().clone()
    }

    fn set_neighbors(&self, layer: usize, neighbors: Vec<u32>) {
        if layer <= self.max_layer {
            *self.neighbors[layer].write() = neighbors;
        }
    }
}

/// Candidate node for search operations
#[derive(Debug, Clone, Copy)]
struct Candidate {
    distance: OrderedFloat<f32>,
    id: u32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance && self.id == other.id
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .cmp(&other.distance)
            .then(self.id.cmp(&other.id))
    }
}

/// HNSW Index for approximate nearest neighbor search
pub struct HnswIndex {
    /// Configuration
    config: HnswConfig,

    /// All nodes in the index
    nodes: RwLock<Vec<HnswNode>>,

    /// Entry point node ID (highest layer node)
    entry_point: AtomicU32,

    /// Current maximum layer in the graph
    max_layer: AtomicUsize,

    /// Number of elements in the index
    count: AtomicUsize,

    /// Whether the index has been modified since last save
    dirty: AtomicUsize,

    /// Per-chunk dirty tracking for selective checkpoint writes.
    chunk_dirty: Mutex<DirtyChunkTracker>,

    /// Maps external_id → internal node index for O(1) lookup and in-place upsert.
    key_to_node: RwLock<HashMap<Bytes, u32>>,

    /// Internal node IDs that have been soft-deleted. Excluded from search results.
    deleted_nodes: RwLock<HashSet<u32>>,
}

impl HnswIndex {
    /// Create a new empty HNSW index
    pub fn new(config: HnswConfig) -> Self {
        Self {
            config,
            nodes: RwLock::new(Vec::new()),
            entry_point: AtomicU32::new(u32::MAX),
            max_layer: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            dirty: AtomicUsize::new(0),
            chunk_dirty: Mutex::new(DirtyChunkTracker::new(HNSW_CHUNK_SIZE)),
            key_to_node: RwLock::new(HashMap::new()),
            deleted_nodes: RwLock::new(HashSet::new()),
        }
    }

    /// Get the number of vectors in the index
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the configuration
    pub fn config(&self) -> &HnswConfig {
        &self.config
    }

    /// Insert a vector into the index
    ///
    /// # Arguments
    ///
    /// * `external_id` - External identifier for the vector
    /// * `vector` - The vector to insert
    ///
    /// # Returns
    ///
    /// The internal node ID assigned to this vector
    pub fn insert(&self, external_id: impl Into<Bytes>, vector: Vec<f32>) -> Result<u32> {
        let external_id = external_id.into();

        // Validate vector dimension
        if vector.len() != self.config.dim {
            return Err(StorageError::InvalidArgument(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.config.dim,
                vector.len()
            )));
        }

        // Check if this key already exists — update vector in place instead of appending.
        {
            let existing_id = self.key_to_node.read().get(&external_id).copied();
            if let Some(node_id) = existing_id {
                let mut nodes = self.nodes.write();
                if let Some(node) = nodes.get_mut(node_id as usize) {
                    node.vector = vector;
                }
                self.chunk_dirty.lock().mark_dirty(node_id);
                return Ok(node_id);
            }
        }

        // Assign a random layer for this node
        let node_layer = self.random_layer();

        // Create the new node
        let node = HnswNode::new(
            external_id.clone(),
            vector,
            node_layer,
            self.config.m,
            self.config.m0,
        );

        // Add node to the list and get its ID
        let node_id = {
            let mut nodes = self.nodes.write();
            let id = nodes.len() as u32;
            nodes.push(node);
            id
        };

        // Register the external_id → node_id mapping
        self.key_to_node.write().insert(external_id, node_id);

        self.count.fetch_add(1, Ordering::Relaxed);
        self.dirty.fetch_add(1, Ordering::Relaxed);

        // Mark the containing chunk as dirty
        {
            let mut tracker = self.chunk_dirty.lock();
            let total = self.count.load(Ordering::Relaxed) as u32;
            tracker.grow_to(total);
            tracker.mark_dirty(node_id);
        }

        // Handle first insertion
        let current_entry = self.entry_point.load(Ordering::Acquire);
        if current_entry == u32::MAX {
            self.entry_point.store(node_id, Ordering::Release);
            self.max_layer.store(node_layer, Ordering::Release);
            return Ok(node_id);
        }

        // Clone the vector for distance calculations (to avoid holding lock during search)
        let new_vector = {
            let nodes = self.nodes.read();
            nodes[node_id as usize].vector.clone()
        };

        // Find entry point for insertion
        let mut current_node = current_entry;
        let current_max_layer = self.max_layer.load(Ordering::Acquire);

        // Traverse from top layer down to node_layer + 1
        for layer in (node_layer + 1..=current_max_layer).rev() {
            let nodes = self.nodes.read();
            current_node = self.search_layer_single(&new_vector, current_node, layer, &nodes);
        }

        // Insert into layers from node_layer down to 0
        for layer in (0..=node_layer.min(current_max_layer)).rev() {
            // Find ef_construction nearest neighbors at this layer
            let (candidates, next_node) = {
                let nodes = self.nodes.read();
                let candidates = self.search_layer(
                    &new_vector,
                    current_node,
                    self.config.ef_construction,
                    layer,
                    &nodes,
                );
                let next = candidates.first().map(|c| c.id).unwrap_or(current_node);
                (candidates, next)
            };

            // Select M best neighbors (using heuristic selection)
            let max_connections = if layer == 0 {
                self.config.m0
            } else {
                self.config.m
            };

            let neighbors = {
                let nodes = self.nodes.read();
                self.select_neighbors(&candidates, max_connections, &nodes)
            };

            // Set neighbors for the new node and add bidirectional connections
            {
                let nodes = self.nodes.read();
                nodes[node_id as usize].set_neighbors(layer, neighbors.clone());

                // Add bidirectional connections
                for &neighbor_id in &neighbors {
                    self.add_connection(neighbor_id, node_id, layer, max_connections, &nodes);
                }
            }

            // Update current node for next layer
            current_node = next_node;
        }

        // Update entry point if new node has higher layer
        if node_layer > current_max_layer {
            self.max_layer.store(node_layer, Ordering::Release);
            self.entry_point.store(node_id, Ordering::Release);
        }

        Ok(node_id)
    }

    /// Search for k nearest neighbors
    ///
    /// # Arguments
    ///
    /// * `query` - Query vector
    /// * `k` - Number of nearest neighbors to return
    ///
    /// # Returns
    ///
    /// Vector of (external_id, distance) pairs sorted by distance
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(Bytes, f32)>> {
        self.search_with_ef(query, k, self.config.ef_search)
    }

    /// Search with custom ef parameter
    pub fn search_with_ef(&self, query: &[f32], k: usize, ef: usize) -> Result<Vec<(Bytes, f32)>> {
        // Validate query dimension
        if query.len() != self.config.dim {
            return Err(StorageError::InvalidArgument(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.config.dim,
                query.len()
            )));
        }

        // Handle empty index
        if self.is_empty() {
            return Ok(Vec::new());
        }

        let nodes = self.nodes.read();
        let entry_point = self.entry_point.load(Ordering::Acquire);
        let max_layer = self.max_layer.load(Ordering::Acquire);

        // Start from entry point
        let mut current_node = entry_point;

        // Traverse from top layer to layer 1
        for layer in (1..=max_layer).rev() {
            current_node = self.search_layer_single(query, current_node, layer, &nodes);
        }

        // Search layer 0 with ef candidates
        let candidates = self.search_layer(query, current_node, ef.max(k), 0, &nodes);

        // Return top k results, excluding soft-deleted nodes
        let deleted = self.deleted_nodes.read();
        let results: Vec<(Bytes, f32)> = candidates
            .into_iter()
            .filter(|c| !deleted.contains(&c.id))
            .take(k)
            .map(|c| {
                let node = &nodes[c.id as usize];
                (node.external_id.clone(), c.distance.into_inner())
            })
            .collect();

        Ok(results)
    }

    /// Get the vector for a given internal ID
    pub fn get_vector(&self, id: u32) -> Option<Vec<f32>> {
        let nodes = self.nodes.read();
        nodes.get(id as usize).map(|n| n.vector.clone())
    }

    /// Get the stored vector for an external key. Returns None if the key is unknown or deleted.
    pub fn get_vector_by_key(&self, key: &[u8]) -> Option<Vec<f32>> {
        let node_id = self.key_to_node.read().get(key).copied()?;
        if self.deleted_nodes.read().contains(&node_id) {
            return None;
        }
        let nodes = self.nodes.read();
        nodes.get(node_id as usize).map(|n| n.vector.clone())
    }

    /// Soft-delete a node by external key. Returns false if the key is not found.
    /// The node is excluded from search results and get_vector_by_key returns None.
    /// Deleted nodes are compacted out at the next checkpoint (not written to .seg files).
    pub fn remove(&self, key: &[u8]) -> bool {
        let node_id = match self.key_to_node.read().get(key).copied() {
            Some(id) => id,
            None => return false,
        };
        self.deleted_nodes.write().insert(node_id);
        // Saturating sub guards against underflow on double-remove.
        if self.count.load(Ordering::Relaxed) > 0 {
            self.count.fetch_sub(1, Ordering::Relaxed);
        }
        self.chunk_dirty.lock().mark_dirty(node_id);
        true
    }

    /// Get the external ID for a given internal ID
    pub fn get_external_id(&self, id: u32) -> Option<Bytes> {
        let nodes = self.nodes.read();
        nodes.get(id as usize).map(|n| n.external_id.clone())
    }

    /// Check if the index has been modified
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed) > 0
    }

    /// Mark the index as clean (after saving)
    pub fn mark_clean(&self) {
        self.dirty.store(0, Ordering::Relaxed);
    }

    /// Save only dirty chunks to `dir` using segment files + manifest.
    ///
    /// Writes one `.seg` file per dirty chunk and updates the manifest.
    /// Called from the checkpoint task instead of `save()`.
    pub fn save_dirty_chunks(&self, dir: &Path) -> Result<()> {
        if !self.is_dirty() {
            return Ok(());
        }

        std::fs::create_dir_all(dir)?;

        // Snapshot nodes under read lock
        let (entry_point, max_layer, node_records) = {
            let nodes = self.nodes.read();
            let records: Vec<(Bytes, Vec<f32>, usize, Vec<Vec<u32>>)> = nodes
                .iter()
                .map(|n| {
                    let neighbors: Vec<Vec<u32>> =
                        (0..=n.max_layer).map(|l| n.get_neighbors(l)).collect();
                    (n.external_id.clone(), n.vector.clone(), n.max_layer, neighbors)
                })
                .collect();
            (
                self.entry_point.load(Ordering::Relaxed),
                self.max_layer.load(Ordering::Relaxed),
                records,
            )
        };

        let total = node_records.len() as u32;

        // Load or create manifest
        let mut manifest = SegmentManifest::load(dir, "hnsw")?
            .unwrap_or_else(|| SegmentManifest::new("hnsw"));

        // Determine which chunk indices are dirty
        let dirty_chunk_idxs: Vec<usize> = {
            let tracker = self.chunk_dirty.lock();
            tracker.dirty_chunks()
        };

        let tracker_guard = self.chunk_dirty.lock();
        let chunk_size = tracker_guard.chunk_size();
        drop(tracker_guard);

        for chunk_idx in dirty_chunk_idxs {
            let tracker = self.chunk_dirty.lock();
            let start = tracker.chunk_start(chunk_idx) as usize;
            let end = tracker.chunk_end(chunk_idx, total) as usize;
            drop(tracker);

            if start >= node_records.len() {
                continue;
            }
            let end = end.min(node_records.len());

            // Build chunk data: config header + state + this node range
            let mut data_buf = Vec::new();
            // Write global state (needed to reconstruct entry_point, max_layer)
            data_buf.extend_from_slice(&entry_point.to_le_bytes());
            data_buf.extend_from_slice(&(max_layer as u32).to_le_bytes());
            // Write chunk node range
            data_buf.extend_from_slice(&(start as u32).to_le_bytes());
            data_buf.extend_from_slice(&(end as u32).to_le_bytes());
            for (external_id, vector, node_max_layer, neighbors) in &node_records[start..end] {
                data_buf.extend_from_slice(&(external_id.len() as u32).to_le_bytes());
                data_buf.extend_from_slice(external_id);
                for &v in vector {
                    data_buf.extend_from_slice(&v.to_le_bytes());
                }
                data_buf.extend_from_slice(&(*node_max_layer as u32).to_le_bytes());
                for layer_neighbors in neighbors {
                    data_buf.extend_from_slice(&(layer_neighbors.len() as u32).to_le_bytes());
                    for &n in layer_neighbors {
                        data_buf.extend_from_slice(&n.to_le_bytes());
                    }
                }
            }

            // Use/reuse seq_no for this chunk (stable: chunk_idx maps to seq_no directly)
            let seq_no = chunk_idx as u32;
            let filename = format!("hnsw_{:04}.seg", seq_no);
            let seg_path = dir.join(&filename);

            let entry_count = (end - start) as u32;
            let header = SegmentHeader::new(
                *b"HNSW_SEG",
                INDEX_TYPE_HNSW,
                seq_no,
                entry_count,
                start as u64,
                (end as u64).saturating_sub(1),
            );
            let mut writer = SegmentWriter::create(&seg_path, header)?;
            writer.write_bytes(&data_buf)?;
            let crc32 = writer.finish()?;

            let file_size = seg_path.metadata().map(|m| m.len()).unwrap_or(0);

            // Update or add chunk in manifest
            if let Some(existing) = manifest.chunks.iter_mut().find(|c| c.seq_no == seq_no) {
                existing.entry_count = entry_count;
                existing.file_size = file_size;
                existing.crc32 = crc32;
                existing.first_id = start as u64;
                existing.last_id = (end as u64).saturating_sub(1);
            } else {
                manifest.chunks.push(ChunkMeta {
                    seq_no,
                    filename,
                    entry_count,
                    file_size,
                    first_id: start as u64,
                    last_id: (end as u64).saturating_sub(1),
                    crc32,
                    sealed: start as u64 + chunk_size as u64 <= total as u64,
                    has_deletions: false,
                });
            }

            // Mark chunk as clean
            self.chunk_dirty.lock().mark_clean(chunk_idx);
        }

        manifest.commit(dir)?;
        self.mark_clean();
        tracing::info!(
            "HNSW: saved dirty chunks to {:?} ({} nodes total)",
            dir,
            total
        );
        Ok(())
    }

    /// Load from chunked segment files + manifest in `dir`.
    ///
    /// Falls back gracefully if chunks are corrupt (WAL will rebuild).
    /// Returns `None` if the manifest doesn't exist.
    pub fn load_chunked(dir: &Path, config: HnswConfig) -> Result<Option<Self>> {
        let manifest = match SegmentManifest::load(dir, "hnsw")? {
            Some(m) => m,
            None => return Ok(None),
        };

        if manifest.chunks.is_empty() {
            return Ok(None);
        }

        // We need to load all chunks and reconstruct the full node list.
        // Chunks are ordered by seq_no (= chunk_idx), covering node ranges.
        let mut all_chunks: Vec<_> = manifest.chunks.clone();
        all_chunks.sort_by_key(|c| c.seq_no);

        // Collect all nodes sorted by start index
        let mut node_records: Vec<Option<(Bytes, Vec<f32>, usize, Vec<Vec<u32>>)>> = Vec::new();
        let mut global_entry_point = u32::MAX;
        let mut global_max_layer = 0usize;

        for chunk_meta in &all_chunks {
            let seg_path = dir.join(&chunk_meta.filename);
            if !seg_path.exists() {
                tracing::warn!("HNSW chunk {:?} missing; skipping", seg_path);
                continue;
            }
            let reader = match SegmentReader::open(&seg_path) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("HNSW chunk {:?} corrupt ({}); skipping", seg_path, e);
                    continue;
                }
            };
            let data = reader.data().to_vec();
            if let Ok((ep, ml, start, nodes)) = parse_hnsw_chunk(&data, config.dim) {
                if ep != u32::MAX {
                    global_entry_point = ep;
                }
                if ml > global_max_layer {
                    global_max_layer = ml;
                }
                // Extend node_records to fit
                let end = start + nodes.len();
                if end > node_records.len() {
                    node_records.resize_with(end, || None);
                }
                for (i, node) in nodes.into_iter().enumerate() {
                    node_records[start + i] = Some(node);
                }
            } else {
                tracing::warn!("HNSW chunk {:?} failed to parse; skipping", seg_path);
            }
        }

        // Build the index from collected nodes
        let index = HnswIndex::new(config);
        {
            let mut nodes_guard = index.nodes.write();
            for (_i, node_opt) in node_records.iter().enumerate() {
                if let Some((external_id, vector, max_layer, _)) = node_opt {
                    let node = HnswNode::new(
                        external_id.clone(),
                        vector.clone(),
                        *max_layer,
                        index.config.m,
                        index.config.m0,
                    );
                    nodes_guard.push(node);
                    index.count.fetch_add(1, Ordering::Relaxed);
                }
            }
            // Replay neighbor connections
            for (i, node_opt) in node_records.iter().enumerate() {
                if let Some((_, _, _, neighbors)) = node_opt {
                    if i < nodes_guard.len() {
                        for (layer, layer_neighbors) in neighbors.iter().enumerate() {
                            nodes_guard[i].set_neighbors(layer, layer_neighbors.clone());
                        }
                    }
                }
            }
        }
        if global_entry_point != u32::MAX {
            index.entry_point.store(global_entry_point, Ordering::Release);
        }
        index.max_layer.store(global_max_layer, Ordering::Release);
        // Grow tracker to cover all loaded nodes (all clean)
        index.chunk_dirty.lock().grow_to(index.len() as u32);
        // Build key_to_node map from loaded nodes.
        {
            let nodes = index.nodes.read();
            let mut map = index.key_to_node.write();
            for (idx, node) in nodes.iter().enumerate() {
                map.insert(node.external_id.clone(), idx as u32);
            }
        }
        index.mark_clean();

        tracing::info!(
            "HNSW: loaded {} nodes from {} chunks in {:?}",
            index.len(),
            all_chunks.len(),
            dir
        );
        Ok(Some(index))
    }

    // ========== Private Methods ==========

    /// Generate a random layer for a new node
    fn random_layer(&self) -> usize {
        let mut rng = rand::thread_rng();
        let r: f64 = rng.gen();
        let layer = (-r.ln() * self.config.ml).floor() as usize;
        layer.min(self.config.m) // Cap at M to prevent unbounded growth
    }

    /// Calculate distance between query and a node
    #[inline]
    fn distance(&self, query: &[f32], node: &HnswNode) -> f32 {
        self.config.metric.distance(query, &node.vector)
    }

    /// Search a single layer to find the closest node (greedy traversal)
    fn search_layer_single(
        &self,
        query: &[f32],
        entry_id: u32,
        layer: usize,
        nodes: &[HnswNode],
    ) -> u32 {
        let mut current = entry_id;
        let mut current_dist = self.distance(query, &nodes[current as usize]);

        loop {
            let mut changed = false;
            let neighbors = nodes[current as usize].get_neighbors(layer);

            for &neighbor_id in &neighbors {
                let dist = self.distance(query, &nodes[neighbor_id as usize]);
                if dist < current_dist {
                    current = neighbor_id;
                    current_dist = dist;
                    changed = true;
                }
            }

            if !changed {
                break;
            }
        }

        current
    }

    /// Search a layer with ef candidates (beam search)
    fn search_layer(
        &self,
        query: &[f32],
        entry_id: u32,
        ef: usize,
        layer: usize,
        nodes: &[HnswNode],
    ) -> Vec<Candidate> {
        let mut visited = HashSet::new();
        visited.insert(entry_id);

        let entry_dist = self.distance(query, &nodes[entry_id as usize]);

        // Candidates heap (min-heap - closest first)
        let mut candidates: BinaryHeap<Reverse<Candidate>> = BinaryHeap::new();
        candidates.push(Reverse(Candidate {
            distance: OrderedFloat(entry_dist),
            id: entry_id,
        }));

        // Results heap (max-heap - furthest first for easy pruning)
        let mut results: BinaryHeap<Candidate> = BinaryHeap::new();
        results.push(Candidate {
            distance: OrderedFloat(entry_dist),
            id: entry_id,
        });

        while let Some(Reverse(current)) = candidates.pop() {
            // Get the furthest result distance for pruning
            let furthest_dist = results
                .peek()
                .map(|c| c.distance)
                .unwrap_or(OrderedFloat(f32::MAX));

            // If current candidate is further than furthest result and we have ef results, stop
            if current.distance > furthest_dist && results.len() >= ef {
                break;
            }

            // Explore neighbors
            let neighbors = nodes[current.id as usize].get_neighbors(layer);
            for &neighbor_id in &neighbors {
                if visited.insert(neighbor_id) {
                    let dist = self.distance(query, &nodes[neighbor_id as usize]);
                    let neighbor_candidate = Candidate {
                        distance: OrderedFloat(dist),
                        id: neighbor_id,
                    };

                    let furthest = results
                        .peek()
                        .map(|c| c.distance)
                        .unwrap_or(OrderedFloat(f32::MAX));

                    // Add to candidates if closer than furthest or if we don't have ef results
                    if dist < furthest.into_inner() || results.len() < ef {
                        candidates.push(Reverse(neighbor_candidate));
                        results.push(neighbor_candidate);

                        // Keep only ef best results
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }

        // Convert max-heap to sorted vector
        let mut result_vec: Vec<Candidate> = results.into_vec();
        result_vec.sort();
        result_vec
    }

    /// Select best neighbors using simple heuristic
    fn select_neighbors(
        &self,
        candidates: &[Candidate],
        max_connections: usize,
        _nodes: &[HnswNode],
    ) -> Vec<u32> {
        // Simple selection: take the closest candidates
        // A more sophisticated approach would use the "heuristic" from the paper
        candidates
            .iter()
            .take(max_connections)
            .map(|c| c.id)
            .collect()
    }

    /// Add a connection from source to target at given layer
    fn add_connection(
        &self,
        source_id: u32,
        target_id: u32,
        layer: usize,
        max_connections: usize,
        nodes: &[HnswNode],
    ) {
        let source = &nodes[source_id as usize];
        if layer > source.max_layer {
            return;
        }

        let mut neighbors = source.neighbors[layer].write();

        // Check if already connected
        if neighbors.contains(&target_id) {
            return;
        }

        neighbors.push(target_id);

        // Prune if over capacity
        if neighbors.len() > max_connections {
            // Calculate distances and keep closest
            let source_vector = &source.vector;
            let mut with_distances: Vec<(u32, f32)> = neighbors
                .iter()
                .map(|&n| {
                    (
                        n,
                        self.config
                            .metric
                            .distance(source_vector, &nodes[n as usize].vector),
                    )
                })
                .collect();

            with_distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            with_distances.truncate(max_connections);

            *neighbors = with_distances.into_iter().map(|(id, _)| id).collect();
        }
    }

    /// Save the index to a file.
    ///
    /// The read lock on `nodes` is held only for the in-memory snapshot, not
    /// during disk I/O. This prevents checkpoints from blocking inserts for
    /// the full write duration (which can be seconds for large indexes).
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();

        // --- Snapshot while holding the read lock (fast, in-memory only) ---
        // Each entry is (external_id, vector, max_layer, neighbors_per_layer).
        let (entry_point, max_layer, node_records) = {
            let nodes = self.nodes.read();
            let records: Vec<(Bytes, Vec<f32>, usize, Vec<Vec<u32>>)> = nodes
                .iter()
                .map(|n| {
                    let neighbors: Vec<Vec<u32>> =
                        (0..=n.max_layer).map(|l| n.get_neighbors(l)).collect();
                    (n.external_id.clone(), n.vector.clone(), n.max_layer, neighbors)
                })
                .collect();
            (
                self.entry_point.load(Ordering::Relaxed),
                self.max_layer.load(Ordering::Relaxed),
                records,
            )
            // read lock released here
        };

        // --- Write snapshot to disk without holding any lock ---
        let tmp_path = path.with_extension("tmp");
        let file = std::fs::File::create(&tmp_path).map_err(StorageError::Io)?;
        let mut writer = std::io::BufWriter::new(file);

        // Write header
        writer.write_all(b"HNSW")?;
        writer.write_all(&1u32.to_le_bytes())?;

        // Write config
        writer.write_all(&(self.config.dim as u32).to_le_bytes())?;
        writer.write_all(&(self.config.m as u32).to_le_bytes())?;
        writer.write_all(&(self.config.m0 as u32).to_le_bytes())?;
        writer.write_all(&(self.config.ef_construction as u32).to_le_bytes())?;
        writer.write_all(&(self.config.ef_search as u32).to_le_bytes())?;
        writer.write_all(&self.config.ml.to_le_bytes())?;
        writer.write_all(&(self.config.metric as u8).to_le_bytes())?;

        // Write state
        writer.write_all(&entry_point.to_le_bytes())?;
        writer.write_all(&(max_layer as u32).to_le_bytes())?;

        // Write nodes
        writer.write_all(&(node_records.len() as u32).to_le_bytes())?;
        for (external_id, vector, node_max_layer, neighbors) in &node_records {
            writer.write_all(&(external_id.len() as u32).to_le_bytes())?;
            writer.write_all(external_id)?;

            for &v in vector {
                writer.write_all(&v.to_le_bytes())?;
            }

            writer.write_all(&(*node_max_layer as u32).to_le_bytes())?;
            for layer_neighbors in neighbors {
                writer.write_all(&(layer_neighbors.len() as u32).to_le_bytes())?;
                for &n in layer_neighbors {
                    writer.write_all(&n.to_le_bytes())?;
                }
            }
        }

        writer.flush()?;
        drop(writer);
        std::fs::rename(&tmp_path, path)?;
        self.mark_clean();
        Ok(())
    }

    /// Load an index from a file
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = std::fs::File::open(path).map_err(StorageError::Io)?;
        let mut file = std::io::BufReader::new(file);

        // Read and verify magic
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != b"HNSW" {
            return Err(StorageError::invalid_format(
                path,
                "Invalid HNSW file magic",
            ));
        }

        // Read version
        let mut version_bytes = [0u8; 4];
        file.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);
        if version != 1 {
            return Err(StorageError::invalid_format(
                path,
                format!("Unsupported HNSW version: {}", version),
            ));
        }

        // Read config
        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];

        file.read_exact(&mut buf4)?;
        let dim = u32::from_le_bytes(buf4) as usize;

        file.read_exact(&mut buf4)?;
        let m = u32::from_le_bytes(buf4) as usize;

        file.read_exact(&mut buf4)?;
        let m0 = u32::from_le_bytes(buf4) as usize;

        file.read_exact(&mut buf4)?;
        let ef_construction = u32::from_le_bytes(buf4) as usize;

        file.read_exact(&mut buf4)?;
        let ef_search = u32::from_le_bytes(buf4) as usize;

        file.read_exact(&mut buf8)?;
        let ml = f64::from_le_bytes(buf8);

        let mut metric_byte = [0u8; 1];
        file.read_exact(&mut metric_byte)?;
        let metric = match metric_byte[0] {
            0 => DistanceMetric::L2,
            1 => DistanceMetric::Cosine,
            2 => DistanceMetric::DotProduct,
            _ => DistanceMetric::L2,
        };

        let config = HnswConfig {
            dim,
            m,
            m0,
            ef_construction,
            ef_search,
            ml,
            metric,
            max_elements: 1_000_000,
        };

        // Read state
        file.read_exact(&mut buf4)?;
        let entry_point = u32::from_le_bytes(buf4);

        file.read_exact(&mut buf4)?;
        let max_layer = u32::from_le_bytes(buf4) as usize;

        // Read nodes
        file.read_exact(&mut buf4)?;
        let node_count = u32::from_le_bytes(buf4) as usize;

        let mut nodes = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            // Read external ID
            file.read_exact(&mut buf4)?;
            let id_len = u32::from_le_bytes(buf4) as usize;
            let mut id_bytes = vec![0u8; id_len];
            file.read_exact(&mut id_bytes)?;
            let external_id = Bytes::from(id_bytes);

            // Read vector
            let mut vector = Vec::with_capacity(dim);
            for _ in 0..dim {
                file.read_exact(&mut buf4)?;
                vector.push(f32::from_le_bytes(buf4));
            }

            // Read max layer
            file.read_exact(&mut buf4)?;
            let node_max_layer = u32::from_le_bytes(buf4) as usize;

            // Create node
            let node = HnswNode::new(external_id, vector, node_max_layer, m, m0);

            // Read neighbors for each layer
            for layer in 0..=node_max_layer {
                file.read_exact(&mut buf4)?;
                let neighbor_count = u32::from_le_bytes(buf4) as usize;
                let mut neighbors = Vec::with_capacity(neighbor_count);
                for _ in 0..neighbor_count {
                    file.read_exact(&mut buf4)?;
                    neighbors.push(u32::from_le_bytes(buf4));
                }
                node.set_neighbors(layer, neighbors);
            }

            nodes.push(node);
        }

        let index = Self {
            config,
            nodes: RwLock::new(nodes),
            entry_point: AtomicU32::new(entry_point),
            max_layer: AtomicUsize::new(max_layer),
            count: AtomicUsize::new(node_count),
            dirty: AtomicUsize::new(0),
            chunk_dirty: Mutex::new(DirtyChunkTracker::new(HNSW_CHUNK_SIZE)),
            key_to_node: RwLock::new(HashMap::new()),
            deleted_nodes: RwLock::new(HashSet::new()),
        };
        // Initialize tracker to cover all loaded nodes (all clean)
        index.chunk_dirty.lock().grow_to(node_count as u32);
        // Build key_to_node map from loaded nodes.
        {
            let nodes = index.nodes.read();
            let mut map = index.key_to_node.write();
            for (idx, node) in nodes.iter().enumerate() {
                map.insert(node.external_id.clone(), idx as u32);
            }
        }
        Ok(index)
    }
}

// ── chunk format helpers ───────────────────────────────────────────────────────

/// Parse the data section of an HNSW `.seg` file.
///
/// `dim` is the vector dimension (from config, known at load time).
/// Returns `(entry_point, max_layer, start_idx, nodes)` where `nodes` is a list of
/// `(external_id, vector, max_layer, neighbors_per_layer)`.
fn parse_hnsw_chunk(
    data: &[u8],
    dim: usize,
) -> std::result::Result<
    (u32, usize, usize, Vec<(Bytes, Vec<f32>, usize, Vec<Vec<u32>>)>),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let mut cursor = std::io::Cursor::new(data);
    let mut buf4 = [0u8; 4];

    // Global state
    cursor.read_exact(&mut buf4)?;
    let entry_point = u32::from_le_bytes(buf4);
    cursor.read_exact(&mut buf4)?;
    let max_layer = u32::from_le_bytes(buf4) as usize;

    // Node range
    cursor.read_exact(&mut buf4)?;
    let start = u32::from_le_bytes(buf4) as usize;
    cursor.read_exact(&mut buf4)?;
    let end = u32::from_le_bytes(buf4) as usize;

    let count = end.saturating_sub(start);
    let mut nodes = Vec::with_capacity(count);

    for _ in 0..count {
        // external_id
        cursor.read_exact(&mut buf4)?;
        let id_len = u32::from_le_bytes(buf4) as usize;
        let mut id_bytes = vec![0u8; id_len];
        cursor.read_exact(&mut id_bytes)?;
        let external_id = Bytes::from(id_bytes);

        // vector (dim is known from config)
        let mut vector = Vec::with_capacity(dim);
        let mut buf_f32 = [0u8; 4];
        for _ in 0..dim {
            cursor.read_exact(&mut buf_f32)?;
            vector.push(f32::from_le_bytes(buf_f32));
        }

        // node_max_layer
        cursor.read_exact(&mut buf4)?;
        let node_max_layer = u32::from_le_bytes(buf4) as usize;

        // neighbors per layer
        let mut neighbors = Vec::with_capacity(node_max_layer + 1);
        for _ in 0..=node_max_layer {
            cursor.read_exact(&mut buf4)?;
            let n_count = u32::from_le_bytes(buf4) as usize;
            let mut layer_nbrs = Vec::with_capacity(n_count);
            for _ in 0..n_count {
                cursor.read_exact(&mut buf4)?;
                layer_nbrs.push(u32::from_le_bytes(buf4));
            }
            neighbors.push(layer_nbrs);
        }

        nodes.push((external_id, vector, node_max_layer, neighbors));
    }

    Ok((entry_point, max_layer, start, nodes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn random_vector(dim: usize) -> Vec<f32> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        (0..dim).map(|_| rng.gen::<f32>()).collect()
    }

    #[test]
    fn test_empty_index() {
        let config = HnswConfig::with_dim(4);
        let index = HnswIndex::new(config);

        assert!(index.is_empty());
        assert_eq!(index.len(), 0);

        let results = index.search(&[0.0, 0.0, 0.0, 0.0], 10).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_single_insert_search() {
        let config = HnswConfig::with_dim(4);
        let index = HnswIndex::new(config);

        let vec = vec![1.0, 0.0, 0.0, 0.0];
        let id = index.insert(b"test1".to_vec(), vec.clone()).unwrap();

        assert_eq!(index.len(), 1);
        assert_eq!(id, 0);

        // Search for exact match
        let results = index.search(&vec, 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.as_ref(), b"test1");
        assert!(results[0].1 < 1e-5); // Distance should be ~0
    }

    #[test]
    fn test_multiple_inserts() {
        let config = HnswConfig::with_dim(4);
        let index = HnswIndex::new(config);

        for i in 0..100 {
            let vec = vec![i as f32, 0.0, 0.0, 0.0];
            index.insert(format!("vec_{}", i), vec).unwrap();
        }

        assert_eq!(index.len(), 100);

        // Search for closest to [50, 0, 0, 0]
        let results = index.search(&[50.0, 0.0, 0.0, 0.0], 5).unwrap();
        assert_eq!(results.len(), 5);

        // First result should be vec_50
        assert_eq!(results[0].0.as_ref(), b"vec_50");
    }

    #[test]
    fn test_dimension_validation() {
        let config = HnswConfig::with_dim(4);
        let index = HnswIndex::new(config);

        // Wrong dimension on insert
        let result = index.insert(b"test".to_vec(), vec![1.0, 2.0, 3.0]); // 3 dims instead of 4
        assert!(result.is_err());

        // Insert valid vector
        index
            .insert(b"test".to_vec(), vec![1.0, 2.0, 3.0, 4.0])
            .unwrap();

        // Wrong dimension on search
        let result = index.search(&[1.0, 2.0, 3.0], 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_knn_recall() {
        let dim = 32;
        let n_vectors = 1000;
        let k = 10;

        let config = HnswConfig::with_dim(dim)
            .m(16)
            .ef_construction(200)
            .ef_search(100);

        let index = HnswIndex::new(config);

        // Insert random vectors
        let mut vectors = Vec::with_capacity(n_vectors);
        for i in 0..n_vectors {
            let vec = random_vector(dim);
            vectors.push(vec.clone());
            index.insert(format!("vec_{}", i), vec).unwrap();
        }

        // Test recall with multiple queries
        let n_queries = 10;
        let mut total_recall = 0.0;

        for _ in 0..n_queries {
            let query = random_vector(dim);

            // Get HNSW results
            let hnsw_results = index.search(&query, k).unwrap();
            let hnsw_ids: HashSet<_> = hnsw_results.iter().map(|(id, _)| id.clone()).collect();

            // Get brute force results
            let mut brute_force: Vec<(usize, f32)> = vectors
                .iter()
                .enumerate()
                .map(|(i, v)| (i, crate::engine::util::simd::l2_distance_squared(&query, v)))
                .collect();
            brute_force.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            let bf_ids: HashSet<_> = brute_force
                .iter()
                .take(k)
                .map(|(i, _)| Bytes::from(format!("vec_{}", i)))
                .collect();

            // Calculate recall
            let intersection = hnsw_ids.intersection(&bf_ids).count();
            total_recall += intersection as f64 / k as f64;
        }

        let avg_recall = total_recall / n_queries as f64;
        println!("Average recall@{}: {:.2}%", k, avg_recall * 100.0);

        // Expect at least 90% recall with these settings
        assert!(avg_recall > 0.9, "Recall too low: {}", avg_recall);
    }

    #[test]
    fn test_save_load() {
        let dim = 8;
        let config = HnswConfig::with_dim(dim);
        let index = HnswIndex::new(config);

        // Insert some vectors
        for i in 0..50 {
            let vec: Vec<f32> = (0..dim).map(|j| (i * dim + j) as f32).collect();
            index.insert(format!("vec_{}", i), vec).unwrap();
        }

        // Save to temp file
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.hnsw");
        index.save(&path).unwrap();

        // Load back
        let loaded = HnswIndex::load(&path).unwrap();

        assert_eq!(loaded.len(), index.len());
        assert_eq!(loaded.config().dim, index.config().dim);
        assert_eq!(loaded.config().m, index.config().m);

        // Verify search works on loaded index
        let query: Vec<f32> = (0..dim).map(|i| i as f32).collect();
        let results = loaded.search(&query, 5).unwrap();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0.as_ref(), b"vec_0");
    }

    #[test]
    fn test_cosine_metric() {
        let config = HnswConfig::with_dim(3).metric(DistanceMetric::Cosine);
        let index = HnswIndex::new(config);

        // Insert normalized vectors
        index.insert(b"a".to_vec(), vec![1.0, 0.0, 0.0]).unwrap();
        index.insert(b"b".to_vec(), vec![0.0, 1.0, 0.0]).unwrap();
        index
            .insert(b"c".to_vec(), vec![0.707, 0.707, 0.0])
            .unwrap(); // 45 degrees

        // Query with [1, 0, 0] - should find 'a' first
        let results = index.search(&[1.0, 0.0, 0.0], 3).unwrap();
        assert_eq!(results[0].0.as_ref(), b"a");

        // 'c' should be closer than 'b' to [1,0,0]
        assert_eq!(results[1].0.as_ref(), b"c");
        assert_eq!(results[2].0.as_ref(), b"b");
    }
}

#[cfg(test)]
mod hnsw_map_tests {
    use super::*;

    fn small_index() -> HnswIndex {
        HnswIndex::new(HnswConfig::with_dim(4).m(4).ef_construction(10).ef_search(4))
    }

    #[test]
    fn insert_same_key_twice_no_duplicate() {
        let idx = small_index();
        let key = Bytes::from("k1");
        idx.insert(key.clone(), vec![1.0, 0.0, 0.0, 0.0]).unwrap();
        idx.insert(key.clone(), vec![0.0, 1.0, 0.0, 0.0]).unwrap();
        assert_eq!(idx.len(), 1, "upsert must not grow the index");
    }

    #[test]
    fn get_vector_by_key_returns_latest() {
        let idx = small_index();
        let key = Bytes::from("k2");
        idx.insert(key.clone(), vec![1.0, 0.0, 0.0, 0.0]).unwrap();
        idx.insert(key.clone(), vec![0.0, 1.0, 0.0, 0.0]).unwrap();
        let v = idx.get_vector_by_key(b"k2").expect("must be present");
        assert_eq!(v, vec![0.0, 1.0, 0.0, 0.0], "must return updated vector");
    }

    #[test]
    fn get_vector_by_key_unknown_returns_none() {
        let idx = small_index();
        assert!(idx.get_vector_by_key(b"no-such-key").is_none());
    }

    #[test]
    fn removed_key_absent_from_search() {
        let idx = small_index();
        let key = Bytes::from("k3");
        idx.insert(key.clone(), vec![1.0, 0.0, 0.0, 0.0]).unwrap();
        assert_eq!(idx.len(), 1);

        let removed = idx.remove(b"k3");
        assert!(removed, "remove must return true for known key");
        assert_eq!(idx.len(), 0, "count must decrement");

        let results = idx.search(&[1.0, 0.0, 0.0, 0.0], 5).unwrap();
        assert!(results.is_empty(), "deleted node must not appear in search");
    }

    #[test]
    fn remove_unknown_key_returns_false() {
        let idx = small_index();
        assert!(!idx.remove(b"ghost"));
    }

    #[test]
    fn get_vector_by_key_returns_none_after_remove() {
        let idx = small_index();
        let key = Bytes::from("k4");
        idx.insert(key.clone(), vec![1.0, 0.0, 0.0, 0.0]).unwrap();
        idx.remove(b"k4");
        assert!(idx.get_vector_by_key(b"k4").is_none());
    }
}
