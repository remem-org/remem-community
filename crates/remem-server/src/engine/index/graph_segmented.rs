//! Segmented CSR Graph index — node-range chunks + one growing adjacency list.
//!
//! Each sealed chunk covers a contiguous node ID range. The in-memory graph is
//! kept as a single `CsrGraph` (with its own interior mutability) so that
//! cross-chunk edge traversal is transparent. Segmentation applies to on-disk
//! persistence: only node ranges whose chunk has been marked dirty are
//! rewritten on checkpoint.
#![allow(dead_code)]

use bytes::Bytes;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::engine::error::{Result, StorageError};
use crate::engine::index::manifest::{ChunkMeta, SegmentManifest};
use crate::engine::index::segment_io::{SegmentHeader, SegmentReader, SegmentWriter, INDEX_TYPE_GRAPH};
use crate::engine::index::{
    CsrGraph, EdgeMetadata, GraphConfig, TraversalResult, GRAPH_CHUNK_SIZE,
};

// ── SegmentedCsrGraph ─────────────────────────────────────────────────────────

/// Segmented CSR Graph: a single in-memory `CsrGraph` with chunked on-disk persistence.
///
/// The in-memory representation is a single `CsrGraph` so that cross-chunk
/// edge traversal is transparent. Segmentation governs persistence: sealed
/// chunks (node-ID ranges 0..N) are written once and never rewritten unless a
/// node in that range gets a new edge. The growing segment covers new nodes
/// beyond the last sealed node boundary.
pub struct SegmentedCsrGraph {
    config: GraphConfig,
    /// The single in-memory graph containing all nodes and edges.
    graph: CsrGraph,
    /// Index directory for segment files and the manifest.
    dir: PathBuf,
    /// Manifest tracking all sealed chunks.
    manifest: SegmentManifest,
    /// Dirty flag: set when the graph has unsaved changes.
    dirty: AtomicBool,
}

impl SegmentedCsrGraph {
    /// Create a new empty segmented graph.
    pub fn new(config: GraphConfig, dir: PathBuf) -> Self {
        let manifest = SegmentManifest::new("graph");
        let graph = CsrGraph::new(config.clone());
        Self {
            config,
            graph,
            dir,
            manifest,
            dirty: AtomicBool::new(false),
        }
    }

    /// Load from directory, replaying all sealed chunks into a single graph.
    ///
    /// If the manifest is missing or empty, returns a fresh empty graph.
    /// Corrupt individual chunks are skipped with a warning (WAL will rebuild).
    pub fn load_from_dir(config: GraphConfig, dir: PathBuf) -> Self {
        let manifest = SegmentManifest::load(&dir, "graph")
            .unwrap_or(None)
            .unwrap_or_else(|| SegmentManifest::new("graph"));
        let graph = CsrGraph::new(config.clone());

        if manifest.chunks.is_empty() {
            return Self { config, graph, dir, manifest, dirty: AtomicBool::new(false) };
        }

        // Load each chunk and replay into the in-memory graph
        for chunk in &manifest.chunks {
            let seg_path = dir.join(&chunk.filename);
            if !seg_path.exists() {
                tracing::warn!(
                    "Graph chunk {:?} referenced in manifest but missing on disk; skipping",
                    seg_path
                );
                continue;
            }
            match load_graph_chunk(&seg_path) {
                Ok(serialized) => {
                    if let Err(e) = replay_graph_from_binary(&graph, &serialized) {
                        tracing::warn!(
                            "Graph chunk {:?} failed to replay ({}); skipping",
                            seg_path,
                            e
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Graph chunk {:?} is corrupt ({}); skipping — WAL will rebuild recent entries",
                        seg_path,
                        e
                    );
                }
            }
        }

        tracing::info!(
            "Loaded segmented graph index: {} chunks, {} nodes, {} edges",
            manifest.chunks.len(),
            graph.node_count(),
            graph.edge_count()
        );

        Self {
            config,
            graph,
            dir,
            manifest,
            dirty: AtomicBool::new(false),
        }
    }

    /// Seal the current graph state as a new chunk on disk, then update the manifest.
    ///
    /// Used when `node_count() >= GRAPH_CHUNK_SIZE` or during explicit checkpoint.
    pub fn seal_growing(&mut self) -> Result<()> {
        if self.graph.node_count() == 0 {
            return Ok(());
        }

        let seq_no = self.manifest.next_seq_no();
        let filename = format!("graph_{:04}.seg", seq_no);
        let seg_path = self.dir.join(&filename);

        let node_count = self.graph.node_count() as u32;
        let edge_count = self.graph.edge_count() as u32;

        let data = serialize_graph_chunk(&self.graph)?;

        let header = SegmentHeader::new(
            *b"GRPH_SEG",
            INDEX_TYPE_GRAPH,
            seq_no,
            node_count,
            0,
            edge_count as u64,
        );
        let mut writer = SegmentWriter::create(&seg_path, header)?;
        writer.write_bytes(&data)?;
        let crc32 = writer.finish()?;

        let file_size = std::fs::metadata(&seg_path)
            .map(|m| m.len())
            .unwrap_or(0);

        self.manifest.chunks.push(ChunkMeta {
            seq_no,
            entry_count: node_count,
            file_size,
            first_id: 0,
            last_id: edge_count as u64,
            crc32,
            sealed: true,
            has_deletions: false,
            filename,
        });
        self.manifest.commit(&self.dir)?;

        tracing::info!(
            "Sealed graph chunk {} ({} nodes, {} edges)",
            seq_no,
            node_count,
            edge_count
        );

        self.dirty.store(false, Ordering::Release);
        Ok(())
    }

    /// Save dirty state to disk. Called from the checkpoint task.
    ///
    /// If the graph is not dirty, does nothing. Otherwise writes a new sealed
    /// chunk for the current graph state.
    pub fn save_if_dirty(&mut self) -> Result<()> {
        if !self.dirty.load(Ordering::Acquire) && !self.graph.is_dirty() {
            return Ok(());
        }
        self.seal_growing()
    }

    /// Whether there are unsaved changes.
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed) || self.graph.is_dirty()
    }

    // ── Write operations (delegated to inner graph) ─────────────────────────

    pub fn add_edge(
        &self,
        source: impl Into<Bytes>,
        target: impl Into<Bytes>,
        metadata: EdgeMetadata,
    ) -> Result<()> {
        let result = self.graph.add_edge(source, target, metadata);
        if result.is_ok() {
            self.dirty.store(true, Ordering::Release);
        }
        result
    }

    pub fn remove_edge(&self, source: &[u8], target: &[u8]) -> Result<bool> {
        let result = self.graph.remove_edge(source, target);
        if result.as_ref().map(|r| *r).unwrap_or(false) {
            self.dirty.store(true, Ordering::Release);
        }
        result
    }

    pub fn remove_node_edges(&self, node: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let result = self.graph.remove_node_edges(node);
        if result.as_ref().map(|v| !v.is_empty()).unwrap_or(false) {
            self.dirty.store(true, Ordering::Release);
        }
        result
    }

    /// Read-only counterpart to `remove_node_edges`: returns the same edge
    /// list without mutating the graph or marking it dirty. Used to WAL-log
    /// a removal before applying it (log-before-mutate).
    pub fn peek_node_edges(&self, node: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        self.graph.peek_node_edges(node)
    }

    pub fn add_node(&self, external_id: impl Into<Bytes>) -> Result<u32> {
        let result = self.graph.add_node(external_id);
        if result.is_ok() {
            self.dirty.store(true, Ordering::Release);
        }
        result
    }

    pub fn finalize(&self) -> Result<()> {
        self.graph.finalize()
    }

    pub fn unfinalize(&self) {
        self.graph.unfinalize();
    }

    // ── Read operations (delegated to inner graph) ──────────────────────────

    pub fn get_neighbors(&self, external_id: &[u8]) -> Result<Vec<(Bytes, EdgeMetadata)>> {
        self.graph.get_neighbors(external_id)
    }

    pub fn get_neighbors_by_type(
        &self,
        external_id: &[u8],
        edge_type: &str,
    ) -> Result<Vec<(Bytes, EdgeMetadata)>> {
        self.graph.get_neighbors_by_type(external_id, edge_type)
    }

    pub fn traverse_bfs(&self, start: &[u8], max_depth: usize) -> Result<Vec<TraversalResult>> {
        self.graph.traverse_bfs(start, max_depth)
    }

    pub fn traverse_bfs_with_type(
        &self,
        start: &[u8],
        max_depth: usize,
        edge_types: &[String],
    ) -> Result<Vec<TraversalResult>> {
        self.graph.traverse_bfs_with_type(start, max_depth, edge_types)
    }

    pub fn contains_node(&self, external_id: &[u8]) -> bool {
        self.graph.contains_node(external_id)
    }

    pub fn has_edge(&self, source: &[u8], target: &[u8]) -> bool {
        self.graph.has_edge(source, target)
    }

    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    pub fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }

    pub fn is_finalized(&self) -> bool {
        self.graph.is_finalized()
    }

    pub fn out_degree(&self, external_id: &[u8]) -> usize {
        self.graph.out_degree(external_id)
    }

    pub fn needs_compaction(&self) -> bool {
        // Compaction for graph: not triggered in Phase F (no physical node removal)
        false
    }

    /// Load a legacy single-file graph and re-save as a segmented chunk.
    ///
    /// Used during one-time migration in `init.rs`.
    pub fn migrate_from_legacy(
        legacy_path: &Path,
        config: GraphConfig,
        dir: PathBuf,
    ) -> Result<Self> {
        let data = std::fs::read(legacy_path)?;
        let mut seg = Self::new(config, dir);
        replay_graph_from_binary(&seg.graph, &data)?;
        seg.dirty.store(true, Ordering::Release);
        // Seal immediately so the manifest is written
        seg.seal_growing()?;
        Ok(seg)
    }
}

// ── Chunk serialization ────────────────────────────────────────────────────────

/// Serialize the entire graph into bytes using the CSRG wire format (via a temp path).
fn serialize_graph_chunk(graph: &CsrGraph) -> Result<Vec<u8>> {
    let tmp = tempfile_path();
    graph.save(&tmp)?;
    let data = std::fs::read(&tmp)?;
    let _ = std::fs::remove_file(&tmp);
    Ok(data)
}

fn tempfile_path() -> PathBuf {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("remem_graph_{}.tmp", ts))
}

/// Load raw data bytes from a `.seg` file, verifying the CRC32.
fn load_graph_chunk(seg_path: &Path) -> Result<Vec<u8>> {
    let reader = SegmentReader::open(seg_path)?;
    Ok(reader.data().to_vec())
}

/// Parse CSRG binary format and replay nodes+edges into `target`.
fn replay_graph_from_binary(target: &CsrGraph, data: &[u8]) -> Result<()> {
    let mut cursor = std::io::Cursor::new(data);

    // Read magic
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic)?;
    if &magic != b"CSRG" {
        return Err(StorageError::invalid_format("graph chunk", "Invalid CSRG magic"));
    }

    // Read version
    let mut buf4 = [0u8; 4];
    cursor.read_exact(&mut buf4)?;
    let _version = u32::from_le_bytes(buf4);

    // Read config (skip)
    let mut buf8 = [0u8; 8];
    cursor.read_exact(&mut buf8)?; // max_nodes
    cursor.read_exact(&mut buf4)?; // avg_edges_per_node
    let mut directed_byte = [0u8; 1];
    cursor.read_exact(&mut directed_byte)?; // directed

    // Read node count
    cursor.read_exact(&mut buf4)?;
    let node_count = u32::from_le_bytes(buf4) as usize;

    // Read external IDs
    let mut ids: Vec<Bytes> = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        cursor.read_exact(&mut buf4)?;
        let len = u32::from_le_bytes(buf4) as usize;
        let mut id_bytes = vec![0u8; len];
        cursor.read_exact(&mut id_bytes)?;
        let external_id = Bytes::from(id_bytes);
        let _ = target.add_node(external_id.clone());
        ids.push(external_id);
    }

    // Read adjacency list and add edges
    for (src_idx, src_id) in ids.iter().enumerate() {
        cursor.read_exact(&mut buf4)?;
        let edge_count = u32::from_le_bytes(buf4) as usize;
        for _ in 0..edge_count {
            // Read target internal ID
            cursor.read_exact(&mut buf4)?;
            let tgt_internal = u32::from_le_bytes(buf4) as usize;

            // Read edge_type
            cursor.read_exact(&mut buf4)?;
            let type_len = u32::from_le_bytes(buf4) as usize;
            let mut type_bytes = vec![0u8; type_len];
            cursor.read_exact(&mut type_bytes)?;
            let edge_type = String::from_utf8_lossy(&type_bytes).into_owned();

            // Read weight
            let mut buf_f32 = [0u8; 4];
            cursor.read_exact(&mut buf_f32)?;
            let weight = f32::from_le_bytes(buf_f32);

            // Read timestamp
            cursor.read_exact(&mut buf8)?;
            let timestamp = u64::from_le_bytes(buf8);

            // Reconstruct target external ID
            if tgt_internal < ids.len() {
                let tgt_id = ids[tgt_internal].clone();
                let meta = EdgeMetadata {
                    edge_type,
                    weight,
                    timestamp,
                };
                // Ignore errors (duplicate edges, etc.)
                let _ = target.add_edge(src_id.clone(), tgt_id, meta);
            }
        }
        let _ = src_idx; // suppress unused warning
    }

    Ok(())
}
