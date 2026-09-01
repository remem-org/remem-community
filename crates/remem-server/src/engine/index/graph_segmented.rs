//! Segmented CSR Graph index — node-range chunks + one growing adjacency list.
//!
//! Each sealed chunk covers a contiguous node ID range. The in-memory graph is
//! kept as a single `CsrGraph` (with its own interior mutability) so that
//! cross-chunk edge traversal is transparent. Segmentation applies to on-disk
//! persistence: only node ranges whose chunk has been marked dirty are
//! rewritten on checkpoint.

use bytes::Bytes;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::engine::error::{Result, StorageError};
use crate::engine::index::manifest::{ChunkMeta, SegmentManifest};
use crate::engine::index::segment_io::{
    SegmentHeader, SegmentReader, SegmentWriter, INDEX_TYPE_GRAPH,
};
use crate::engine::index::{CsrGraph, EdgeMetadata, GraphConfig, TraversalResult};

// ── SegmentedCsrGraph ─────────────────────────────────────────────────────────

/// Segmented CSR Graph: a single in-memory `CsrGraph` with chunked on-disk persistence.
///
/// The in-memory representation is a single `CsrGraph` so that cross-chunk
/// edge traversal is transparent. Segmentation governs persistence: sealed
/// chunks (node-ID ranges 0..N) are written once and never rewritten unless a
/// node in that range gets a new edge. The growing segment covers new nodes
/// beyond the last sealed node boundary.
pub struct SegmentedCsrGraph {
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
        let graph = CsrGraph::new(config);
        Self {
            graph,
            dir,
            manifest,
            dirty: AtomicBool::new(false),
        }
    }

    /// Load from directory, replaying all sealed chunks into a single graph.
    ///
    /// A parse failure of the manifest itself is refused by default rather
    /// than silently rebuilt from empty — see
    /// [`crate::engine::index::on_index_parse_failure`]. Corrupt individual
    /// chunks (the manifest parses fine, but a `.seg` file it references does
    /// not) are still skipped with a warning; that is tracked separately
    /// (REM-91), not covered by this refusal.
    pub fn load_from_dir(config: GraphConfig, dir: PathBuf) -> Result<Self> {
        // See `SegmentedBTreeIndex::load_from_dir` -- repair an interrupted
        // rekey swap before reading anything.
        crate::engine::index::rekey::recover_interrupted_publish(&dir, &Self::rekey_artifacts())?;
        match Self::try_load_from_dir(config.clone(), &dir) {
            Ok(graph) => Ok(graph),
            Err(e) => {
                crate::engine::index::on_index_parse_failure(&dir, &e)?;
                Ok(Self::new(config, dir))
            }
        }
    }

    fn try_load_from_dir(config: GraphConfig, dir: &Path) -> Result<Self> {
        let manifest = match SegmentManifest::load(dir, "graph")? {
            Some(m) => m,
            None => return Ok(Self::new(config, dir.to_path_buf())),
        };
        let graph = CsrGraph::new(config);

        if manifest.chunks.is_empty() {
            return Ok(Self {
                graph,
                dir: dir.to_path_buf(),
                manifest,
                dirty: AtomicBool::new(false),
            });
        }

        // Load each chunk and replay into the in-memory graph. A missing or
        // corrupt individual chunk is skipped with a warning rather than
        // refused (REM-91) -- only a corrupt manifest itself, handled by the
        // caller above, refuses startup.
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

        Ok(Self {
            graph,
            dir: dir.to_path_buf(),
            manifest,
            dirty: AtomicBool::new(false),
        })
    }

    /// Seal the current graph state as a new chunk on disk, then update the manifest.
    ///
    /// Called during explicit checkpoint.
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

        let file_size = std::fs::metadata(&seg_path).map(|m| m.len()).unwrap_or(0);

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

    /// Returns the same edge list without mutating the graph or marking it
    /// dirty. Used to WAL-log a removal before applying it (log-before-mutate).
    pub fn peek_node_edges(&self, node: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        self.graph.peek_node_edges(node)
    }

    // ── Read operations (delegated to inner graph) ──────────────────────────

    pub fn get_neighbors(&self, external_id: &[u8]) -> Result<Vec<(Bytes, EdgeMetadata)>> {
        self.graph.get_neighbors(external_id)
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
        self.graph
            .traverse_bfs_with_type(start, max_depth, edge_types)
    }

    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    pub(crate) fn rewrite_keys_in_dir<F>(
        config: GraphConfig,
        dir: &Path,
        mut rewrite_key: F,
    ) -> Result<bool>
    where
        F: FnMut(&Bytes) -> Result<Bytes>,
    {
        let existing = Self::load_from_dir(config.clone(), dir.to_path_buf())?;
        // Nothing to rekey, and worth checking before the serialize round
        // trip below: the migration calls this on every startup that has not
        // yet advanced the partition format, including brand-new directories.
        if existing.graph.node_count() == 0 {
            return Ok(false);
        }
        let serialized = serialize_graph_chunk(&existing.graph)?;
        let edges = graph_edges_from_binary(&serialized)?;

        let tmp_dir = dir.join(".graph-rekey.tmp");
        if tmp_dir.exists() {
            std::fs::remove_dir_all(&tmp_dir)?;
        }
        std::fs::create_dir_all(&tmp_dir)?;

        let mut rebuilt = Self::new(config, tmp_dir.clone());
        let mut changed = false;
        for (source, target, metadata) in edges {
            let rewritten_source = rewrite_key(&source)?;
            let rewritten_target = rewrite_key(&target)?;
            changed |= rewritten_source != source || rewritten_target != target;
            rebuilt.add_edge(rewritten_source, rewritten_target, metadata)?;
        }

        if !changed {
            std::fs::remove_dir_all(&tmp_dir)?;
            return Ok(false);
        }

        rebuilt.save_if_dirty()?;
        crate::engine::index::rekey::publish_rebuilt_index(
            dir,
            &tmp_dir,
            &Self::rekey_artifacts(),
        )?;
        Ok(true)
    }

    fn rekey_artifacts() -> crate::engine::index::rekey::IndexArtifacts {
        crate::engine::index::rekey::IndexArtifacts::new("graph")
    }

    /// Convert a pre-segmented `graph.idx` in `dir` into a segment chunk.
    ///
    /// See `SegmentedBTreeIndex::convert_legacy_file_in_dir` for why the
    /// partition migration needs this before it can rekey.
    pub(crate) fn convert_legacy_file_in_dir(config: GraphConfig, dir: &Path) -> Result<bool> {
        let legacy_path = dir.join("graph.idx");
        let manifest_path = dir.join("graph.manifest");
        if !legacy_path.exists() || manifest_path.exists() {
            return Ok(false);
        }

        // `migrate_from_legacy` seals immediately, so the manifest exists on
        // return and the rekey below can see the entries.
        if Self::migrate_from_legacy(&legacy_path, config, dir.to_path_buf()).is_err() {
            return Ok(false);
        }
        std::fs::remove_file(&legacy_path)?;
        tracing::info!(
            "Converted legacy {:?} to segmented format before partition rekey",
            legacy_path
        );
        Ok(true)
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
///
/// `CsrGraph::save` only writes to a path, so the bytes have to make a round
/// trip through the filesystem. The path must be unique per call: two graphs
/// serializing at once — two engines in one process, or the rekey migration
/// running while a checkpoint seals — would otherwise share a file, and the
/// first `remove_file` makes the other's `read` fail with `NotFound`.
fn serialize_graph_chunk(graph: &CsrGraph) -> Result<Vec<u8>> {
    let tmp = tempfile_path();
    graph.save(&tmp)?;
    let data = std::fs::read(&tmp);
    let _ = std::fs::remove_file(&tmp);
    Ok(data?)
}

/// A path no concurrent caller can also pick.
///
/// The timestamp alone is not enough: it is only as fine-grained as the
/// platform clock (coarser than a nanosecond on macOS), so parallel callers
/// land on the same name. Process id separates concurrent processes sharing
/// `/tmp`, and the counter separates threads within one process.
fn tempfile_path() -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "remem_graph_{}_{}_{}.tmp",
        std::process::id(),
        seq,
        ts
    ))
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
        return Err(StorageError::invalid_format(
            "graph chunk",
            "Invalid CSRG magic",
        ));
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

fn graph_edges_from_binary(data: &[u8]) -> Result<Vec<(Bytes, Bytes, EdgeMetadata)>> {
    let mut cursor = Cursor::new(data);

    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic)?;
    if &magic != b"CSRG" {
        return Err(StorageError::Serialization(
            "Invalid graph chunk magic".into(),
        ));
    }

    let mut buf4 = [0u8; 4];
    let mut buf8 = [0u8; 8];

    cursor.read_exact(&mut buf4)?; // version
    cursor.read_exact(&mut buf8)?; // max_nodes
    cursor.read_exact(&mut buf4)?; // avg_edges_per_node
    let mut directed_byte = [0u8; 1];
    cursor.read_exact(&mut directed_byte)?;

    cursor.read_exact(&mut buf4)?;
    let node_count = u32::from_le_bytes(buf4) as usize;

    let mut ids = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        cursor.read_exact(&mut buf4)?;
        let len = u32::from_le_bytes(buf4) as usize;
        let mut id_bytes = vec![0u8; len];
        cursor.read_exact(&mut id_bytes)?;
        ids.push(Bytes::from(id_bytes));
    }

    let mut edges = Vec::new();
    for source in &ids {
        cursor.read_exact(&mut buf4)?;
        let edge_count = u32::from_le_bytes(buf4) as usize;
        for _ in 0..edge_count {
            cursor.read_exact(&mut buf4)?;
            let target_internal = u32::from_le_bytes(buf4) as usize;

            cursor.read_exact(&mut buf4)?;
            let type_len = u32::from_le_bytes(buf4) as usize;
            let mut type_bytes = vec![0u8; type_len];
            cursor.read_exact(&mut type_bytes)?;
            let edge_type = String::from_utf8(type_bytes)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            let mut buf_f32 = [0u8; 4];
            cursor.read_exact(&mut buf_f32)?;
            let weight = f32::from_le_bytes(buf_f32);

            cursor.read_exact(&mut buf8)?;
            let timestamp = u64::from_le_bytes(buf8);

            if let Some(target) = ids.get(target_internal) {
                edges.push((
                    source.clone(),
                    target.clone(),
                    EdgeMetadata {
                        edge_type,
                        weight,
                        timestamp,
                    },
                ));
            }
        }
    }

    Ok(edges)
}
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// A corrupt `graph.manifest` must refuse startup by default, naming the
    /// file -- not be silently swallowed into "no manifest, start fresh"
    /// (REM-46 review finding 3: this was the one index of four not yet
    /// routed through `on_index_parse_failure`).
    #[test]
    fn a_corrupt_graph_manifest_refuses_by_default() {
        let _lock = crate::engine::index::index_rebuild_env_guard();
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("graph.manifest"), b"not a manifest").unwrap();

        let err = match SegmentedCsrGraph::load_from_dir(
            GraphConfig::default(),
            dir.path().to_path_buf(),
        ) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected a corrupt graph.manifest to refuse startup"),
        };
        assert!(err.contains("graph.manifest"), "{err}");
        assert!(err.contains("REMEM_ALLOW_INDEX_REBUILD"), "{err}");
    }

    /// With the operator's opt-in, the same corrupt manifest must still
    /// start -- rebuilt empty.
    #[test]
    fn a_corrupt_graph_manifest_rebuilds_empty_under_the_opt_in() {
        let _lock = crate::engine::index::index_rebuild_env_guard();
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("graph.manifest"), b"not a manifest").unwrap();

        std::env::set_var("REMEM_ALLOW_INDEX_REBUILD", "1");
        let result =
            SegmentedCsrGraph::load_from_dir(GraphConfig::default(), dir.path().to_path_buf());
        std::env::remove_var("REMEM_ALLOW_INDEX_REBUILD");

        let graph = result.expect("REMEM_ALLOW_INDEX_REBUILD=1 must allow startup");
        assert_eq!(graph.node_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }

    /// A directory with no manifest at all (fresh install) must still open,
    /// unaffected by the refusal added for corrupt manifests.
    #[test]
    fn no_manifest_at_all_opens_a_fresh_empty_graph() {
        let dir = tempdir().unwrap();
        let graph =
            SegmentedCsrGraph::load_from_dir(GraphConfig::default(), dir.path().to_path_buf())
                .expect("a missing manifest is not a parse failure");
        assert_eq!(graph.node_count(), 0);
    }
}
