//! Kuzu-backed graph index — drop-in alternative to the CSR graph.
//!
//! Uses Kuzu embedded graph database with Cypher queries instead of
//! hand-rolled BFS. Schema:
//!   CREATE NODE TABLE Memory(key STRING PRIMARY KEY)
//!   CREATE REL TABLE Connection(FROM Memory TO Memory,
//!       edge_type STRING, weight FLOAT, timestamp INT64)

use bytes::Bytes;
use kuzu::{Connection, Database, SystemConfig};
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::engine::error::{Result, StorageError};
use crate::engine::index::{EdgeMetadata, GraphConfig, TraversalResult};

pub struct KuzuGraphIndex {
    #[allow(dead_code)]
    config: GraphConfig,
    db: Database,
    conn: Mutex<Connection>,
    dir: PathBuf,
    node_count: AtomicUsize,
    edge_count: AtomicUsize,
    dirty: AtomicBool,
}

impl KuzuGraphIndex {
    pub fn new(config: GraphConfig, dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&dir).map_err(|e| StorageError::Io(e))?;
        let kuzu_dir = dir.join("kuzu_data");
        std::fs::create_dir_all(&kuzu_dir).map_err(|e| StorageError::Io(e))?;

        let db = Database::new(&kuzu_dir, SystemConfig::default())
            .map_err(|e| StorageError::InvalidArgument(format!("Kuzu init: {e}")))?;
        let conn = Connection::new(&db)
            .map_err(|e| StorageError::InvalidArgument(format!("Kuzu connection: {e}")))?;

        Self::init_schema(&conn)?;

        let (node_count, edge_count) = Self::count_stats(&conn)?;

        Ok(Self {
            config,
            db,
            conn: Mutex::new(conn),
            dir,
            node_count: AtomicUsize::new(node_count),
            edge_count: AtomicUsize::new(edge_count),
            dirty: AtomicBool::new(false),
        })
    }

    pub fn load_from_dir(config: GraphConfig, dir: PathBuf) -> Result<Self> {
        Self::new(config, dir)
    }

    fn init_schema(conn: &Connection) -> Result<()> {
        // Create tables if they don't exist — Kuzu supports IF NOT EXISTS
        conn.query(
            "CREATE NODE TABLE IF NOT EXISTS Memory(key STRING PRIMARY KEY)"
        ).map_err(|e| StorageError::InvalidArgument(format!("Schema Memory: {e}")))?;

        conn.query(
            "CREATE REL TABLE IF NOT EXISTS Connection(\
                FROM Memory TO Memory, \
                edge_type STRING, \
                weight FLOAT, \
                ts INT64)"
        ).map_err(|e| StorageError::InvalidArgument(format!("Schema Connection: {e}")))?;

        Ok(())
    }

    fn count_stats(conn: &Connection) -> Result<(usize, usize)> {
        let nodes = conn.query("MATCH (n:Memory) RETURN count(n)")
            .map_err(|e| StorageError::InvalidArgument(format!("Count nodes: {e}")))?;
        let node_count = Self::extract_count(nodes);

        let edges = conn.query("MATCH ()-[r:Connection]->() RETURN count(r)")
            .map_err(|e| StorageError::InvalidArgument(format!("Count edges: {e}")))?;
        let edge_count = Self::extract_count(edges);

        Ok((node_count, edge_count))
    }

    fn extract_count(mut result: kuzu::QueryResult) -> usize {
        if let Some(row) = result.next() {
            match &row[0] {
                kuzu::Value::Int64(n) => *n as usize,
                _ => 0,
            }
        } else {
            0
        }
    }

    fn ensure_node(conn: &Connection, key: &str) -> Result<()> {
        let query = format!(
            "MERGE (n:Memory {{key: '{}'}})",
            key.replace('\'', "''")
        );
        conn.query(&query)
            .map_err(|e| StorageError::InvalidArgument(format!("Ensure node: {e}")))?;
        Ok(())
    }

    // ── Write operations ─────────────────────────────────────────────────

    pub fn add_edge(
        &self,
        source: impl Into<Bytes>,
        target: impl Into<Bytes>,
        metadata: EdgeMetadata,
    ) -> Result<()> {
        let source = source.into();
        let target = target.into();
        let src_str = String::from_utf8_lossy(&source);
        let dst_str = String::from_utf8_lossy(&target);

        let conn = self.conn.lock();

        Self::ensure_node(&conn, &src_str)?;
        Self::ensure_node(&conn, &dst_str)?;

        let escaped_type = metadata.edge_type.replace('\'', "''");

        // Upsert: delete existing edge with same type, then create new one
        let delete_query = format!(
            "MATCH (s:Memory {{key: '{}'}})-[r:Connection {{edge_type: '{}'}}]->(t:Memory {{key: '{}'}}) DELETE r",
            src_str.replace('\'', "''"),
            escaped_type,
            dst_str.replace('\'', "''"),
        );
        let _ = conn.query(&delete_query);

        let create_query = format!(
            "MATCH (s:Memory {{key: '{}'}}), (t:Memory {{key: '{}'}}) \
             CREATE (s)-[:Connection {{edge_type: '{}', weight: {}, ts: {}}}]->(t)",
            src_str.replace('\'', "''"),
            dst_str.replace('\'', "''"),
            escaped_type,
            metadata.weight,
            metadata.timestamp,
        );
        conn.query(&create_query)
            .map_err(|e| StorageError::InvalidArgument(format!("Add edge: {e}")))?;

        self.edge_count.fetch_add(1, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub fn remove_edge(&self, source: &[u8], target: &[u8]) -> Result<bool> {
        let src_str = String::from_utf8_lossy(source);
        let dst_str = String::from_utf8_lossy(target);

        let conn = self.conn.lock();
        let query = format!(
            "MATCH (s:Memory {{key: '{}'}})-[r:Connection]->(t:Memory {{key: '{}'}}) DELETE r",
            src_str.replace('\'', "''"),
            dst_str.replace('\'', "''"),
        );
        conn.query(&query)
            .map_err(|e| StorageError::InvalidArgument(format!("Remove edge: {e}")))?;

        self.dirty.store(true, Ordering::Relaxed);
        Ok(true)
    }

    pub fn remove_node_edges(&self, node: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let node_str = String::from_utf8_lossy(node);
        let escaped = node_str.replace('\'', "''");
        let conn = self.conn.lock();

        // Find all edges involving this node first
        let query = format!(
            "MATCH (s:Memory)-[r:Connection]->(t:Memory) \
             WHERE s.key = '{}' OR t.key = '{}' \
             RETURN s.key, t.key",
            escaped, escaped,
        );
        let mut result = conn.query(&query)
            .map_err(|e| StorageError::InvalidArgument(format!("Find node edges: {e}")))?;

        let mut removed = Vec::new();
        while let Some(row) = result.next() {
            if let (kuzu::Value::String(src), kuzu::Value::String(dst)) = (&row[0], &row[1]) {
                removed.push((Bytes::from(src.clone()), Bytes::from(dst.clone())));
            }
        }

        // Delete all edges
        let delete_query = format!(
            "MATCH (s:Memory)-[r:Connection]->(t:Memory) \
             WHERE s.key = '{}' OR t.key = '{}' \
             DELETE r",
            escaped, escaped,
        );
        conn.query(&delete_query)
            .map_err(|e| StorageError::InvalidArgument(format!("Remove node edges: {e}")))?;

        if !removed.is_empty() {
            self.dirty.store(true, Ordering::Relaxed);
        }
        Ok(removed)
    }

    pub fn add_node(&self, external_id: impl Into<Bytes>) -> Result<u32> {
        let external_id = external_id.into();
        let key_str = String::from_utf8_lossy(&external_id);

        let conn = self.conn.lock();
        Self::ensure_node(&conn, &key_str)?;

        self.node_count.fetch_add(1, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
        Ok(self.node_count.load(Ordering::Relaxed) as u32 - 1)
    }

    // ── Read operations ──────────────────────────────────────────────────

    pub fn get_neighbors(&self, external_id: &[u8]) -> Result<Vec<(Bytes, EdgeMetadata)>> {
        let key_str = String::from_utf8_lossy(external_id);
        let conn = self.conn.lock();

        let query = format!(
            "MATCH (s:Memory {{key: '{}'}})-[r:Connection]->(t:Memory) \
             RETURN t.key, r.edge_type, r.weight, r.ts",
            key_str.replace('\'', "''"),
        );
        let mut result = conn.query(&query)
            .map_err(|e| StorageError::InvalidArgument(format!("Get neighbors: {e}")))?;

        let mut neighbors = Vec::new();
        while let Some(row) = result.next() {
            let target_key = match &row[0] {
                kuzu::Value::String(s) => Bytes::from(s.clone()),
                _ => continue,
            };
            let edge_type = match &row[1] {
                kuzu::Value::String(s) => s.clone(),
                _ => String::new(),
            };
            let weight = match &row[2] {
                kuzu::Value::Float(f) => *f,
                kuzu::Value::Double(d) => *d as f32,
                _ => 1.0,
            };
            let timestamp = match &row[3] {
                kuzu::Value::Int64(t) => *t as u64,
                _ => 0,
            };

            neighbors.push((
                target_key,
                EdgeMetadata {
                    edge_type,
                    weight,
                    timestamp,
                },
            ));
        }
        Ok(neighbors)
    }

    pub fn get_neighbors_by_type(
        &self,
        external_id: &[u8],
        edge_type: &str,
    ) -> Result<Vec<(Bytes, EdgeMetadata)>> {
        let key_str = String::from_utf8_lossy(external_id);
        let conn = self.conn.lock();

        let query = format!(
            "MATCH (s:Memory {{key: '{}'}})-[r:Connection {{edge_type: '{}'}}]->(t:Memory) \
             RETURN t.key, r.edge_type, r.weight, r.ts",
            key_str.replace('\'', "''"),
            edge_type.replace('\'', "''"),
        );
        let mut result = conn.query(&query)
            .map_err(|e| StorageError::InvalidArgument(format!("Get neighbors by type: {e}")))?;

        let mut neighbors = Vec::new();
        while let Some(row) = result.next() {
            let target_key = match &row[0] {
                kuzu::Value::String(s) => Bytes::from(s.clone()),
                _ => continue,
            };
            let edge_type = match &row[1] {
                kuzu::Value::String(s) => s.clone(),
                _ => String::new(),
            };
            let weight = match &row[2] {
                kuzu::Value::Float(f) => *f,
                kuzu::Value::Double(d) => *d as f32,
                _ => 1.0,
            };
            let timestamp = match &row[3] {
                kuzu::Value::Int64(t) => *t as u64,
                _ => 0,
            };

            neighbors.push((
                target_key,
                EdgeMetadata {
                    edge_type,
                    weight,
                    timestamp,
                },
            ));
        }
        Ok(neighbors)
    }

    pub fn traverse_bfs(&self, start: &[u8], max_depth: usize) -> Result<Vec<TraversalResult>> {
        let key_str = String::from_utf8_lossy(start);
        let conn = self.conn.lock();

        // Kuzu supports variable-length path queries via Cypher
        // For BFS we query paths up to max_depth hops
        let query = format!(
            "MATCH p = (s:Memory {{key: '{}'}})-[r:Connection*1..{}]->(t:Memory) \
             RETURN t.key, length(p) AS depth, \
                    properties(r, 'edge_type')[-1] AS et, \
                    properties(r, 'weight')[-1] AS w, \
                    properties(r, 'ts')[-1] AS ts",
            key_str.replace('\'', "''"),
            max_depth,
        );

        let mut result = conn.query(&query)
            .map_err(|e| StorageError::InvalidArgument(format!("Traverse BFS: {e}")))?;

        // Start node is always included
        let mut results = vec![TraversalResult {
            node_id: Bytes::copy_from_slice(start),
            depth: 0,
            edge_metadata: None,
        }];

        let mut seen = std::collections::HashSet::new();
        seen.insert(Bytes::copy_from_slice(start));

        while let Some(row) = result.next() {
            let target_key = match &row[0] {
                kuzu::Value::String(s) => Bytes::from(s.clone()),
                _ => continue,
            };

            if !seen.insert(target_key.clone()) {
                continue;
            }

            let depth = match &row[1] {
                kuzu::Value::Int64(d) => *d as usize,
                _ => 1,
            };

            let edge_type = match &row[2] {
                kuzu::Value::String(s) => s.clone(),
                _ => String::new(),
            };
            let weight = match &row[3] {
                kuzu::Value::Float(f) => *f,
                kuzu::Value::Double(d) => *d as f32,
                _ => 1.0,
            };
            let timestamp = match &row[4] {
                kuzu::Value::Int64(t) => *t as u64,
                _ => 0,
            };

            results.push(TraversalResult {
                node_id: target_key,
                depth,
                edge_metadata: Some(EdgeMetadata {
                    edge_type,
                    weight,
                    timestamp,
                }),
            });
        }

        Ok(results)
    }

    pub fn traverse_bfs_with_type(
        &self,
        start: &[u8],
        max_depth: usize,
        edge_types: &[String],
    ) -> Result<Vec<TraversalResult>> {
        let key_str = String::from_utf8_lossy(start);
        let conn = self.conn.lock();

        let type_filter = edge_types
            .iter()
            .map(|t| format!("'{}'", t.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            "MATCH p = (s:Memory {{key: '{}'}})-[r:Connection*1..{}]->(t:Memory) \
             WHERE list_any_value(properties(r, 'edge_type'), x -> x IN [{type_filter}]) \
             RETURN t.key, length(p) AS depth, \
                    properties(r, 'edge_type')[-1] AS et, \
                    properties(r, 'weight')[-1] AS w, \
                    properties(r, 'ts')[-1] AS ts",
            key_str.replace('\'', "''"),
            max_depth,
        );

        let mut result = conn.query(&query)
            .map_err(|e| StorageError::InvalidArgument(format!("Traverse BFS typed: {e}")))?;

        let mut results = vec![TraversalResult {
            node_id: Bytes::copy_from_slice(start),
            depth: 0,
            edge_metadata: None,
        }];

        let mut seen = std::collections::HashSet::new();
        seen.insert(Bytes::copy_from_slice(start));

        while let Some(row) = result.next() {
            let target_key = match &row[0] {
                kuzu::Value::String(s) => Bytes::from(s.clone()),
                _ => continue,
            };

            if !seen.insert(target_key.clone()) {
                continue;
            }

            let depth = match &row[1] {
                kuzu::Value::Int64(d) => *d as usize,
                _ => 1,
            };

            let edge_type = match &row[2] {
                kuzu::Value::String(s) => s.clone(),
                _ => String::new(),
            };
            let weight = match &row[3] {
                kuzu::Value::Float(f) => *f,
                kuzu::Value::Double(d) => *d as f32,
                _ => 1.0,
            };
            let timestamp = match &row[4] {
                kuzu::Value::Int64(t) => *t as u64,
                _ => 0,
            };

            results.push(TraversalResult {
                node_id: target_key,
                depth,
                edge_metadata: Some(EdgeMetadata {
                    edge_type,
                    weight,
                    timestamp,
                }),
            });
        }

        Ok(results)
    }

    // ── Stat / lifecycle operations ──────────────────────────────────────

    pub fn contains_node(&self, external_id: &[u8]) -> bool {
        let key_str = String::from_utf8_lossy(external_id);
        let conn = self.conn.lock();
        let query = format!(
            "MATCH (n:Memory {{key: '{}'}}) RETURN count(n)",
            key_str.replace('\'', "''"),
        );
        conn.query(&query)
            .ok()
            .and_then(|r| Self::extract_count_opt(r))
            .unwrap_or(0)
            > 0
    }

    pub fn has_edge(&self, source: &[u8], target: &[u8]) -> bool {
        let src_str = String::from_utf8_lossy(source);
        let dst_str = String::from_utf8_lossy(target);
        let conn = self.conn.lock();
        let query = format!(
            "MATCH (s:Memory {{key: '{}'}})-[r:Connection]->(t:Memory {{key: '{}'}}) RETURN count(r)",
            src_str.replace('\'', "''"),
            dst_str.replace('\'', "''"),
        );
        conn.query(&query)
            .ok()
            .and_then(|r| Self::extract_count_opt(r))
            .unwrap_or(0)
            > 0
    }

    fn extract_count_opt(mut result: kuzu::QueryResult) -> Option<usize> {
        result.next().and_then(|row| match &row[0] {
            kuzu::Value::Int64(n) => Some(*n as usize),
            _ => None,
        })
    }

    pub fn node_count(&self) -> usize {
        let conn = self.conn.lock();
        Self::count_stats_inner(&conn).map(|(n, _)| n).unwrap_or(0)
    }

    pub fn edge_count(&self) -> usize {
        let conn = self.conn.lock();
        Self::count_stats_inner(&conn).map(|(_, e)| e).unwrap_or(0)
    }

    fn count_stats_inner(conn: &Connection) -> Result<(usize, usize)> {
        Self::count_stats(conn)
    }

    pub fn is_empty(&self) -> bool {
        self.node_count() == 0
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }

    // Kuzu handles persistence internally — these are no-ops
    pub fn finalize(&self) -> Result<()> {
        Ok(())
    }

    pub fn unfinalize(&self) {}

    pub fn save_if_dirty(&mut self) -> Result<()> {
        self.dirty.store(false, Ordering::Release);
        Ok(())
    }

    pub fn is_finalized(&self) -> bool {
        true
    }

    pub fn out_degree(&self, external_id: &[u8]) -> usize {
        self.get_neighbors(external_id)
            .map(|n| n.len())
            .unwrap_or(0)
    }

    pub fn needs_compaction(&self) -> bool {
        false
    }

    pub fn seal_growing(&mut self) -> Result<()> {
        self.dirty.store(false, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_kuzu() -> KuzuGraphIndex {
        let dir = tempfile::tempdir().unwrap();
        KuzuGraphIndex::new(GraphConfig::default(), dir.into_path()).unwrap()
    }

    #[test]
    fn test_empty_graph() {
        let g = temp_kuzu();
        assert!(g.is_empty());
        assert_eq!(g.node_count(), 0);
        assert_eq!(g.edge_count(), 0);
    }

    #[test]
    fn test_add_edge_and_get_neighbors() {
        let g = temp_kuzu();
        let meta = EdgeMetadata::with_type("SimilarTo").weight(0.85);
        g.add_edge(
            Bytes::from("memory:aaa"),
            Bytes::from("memory:bbb"),
            meta,
        )
        .unwrap();

        let neighbors = g.get_neighbors(b"memory:aaa").unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, Bytes::from("memory:bbb"));
        assert_eq!(neighbors[0].1.edge_type, "SimilarTo");
        assert!((neighbors[0].1.weight - 0.85).abs() < 0.01);
    }

    #[test]
    fn test_remove_edge() {
        let g = temp_kuzu();
        g.add_edge(
            Bytes::from("memory:a"),
            Bytes::from("memory:b"),
            EdgeMetadata::with_type("related_to"),
        )
        .unwrap();

        assert!(g.has_edge(b"memory:a", b"memory:b"));
        g.remove_edge(b"memory:a", b"memory:b").unwrap();
        assert!(!g.has_edge(b"memory:a", b"memory:b"));
    }

    #[test]
    fn test_traverse_bfs() {
        let g = temp_kuzu();

        g.add_edge(
            Bytes::from("memory:a"),
            Bytes::from("memory:b"),
            EdgeMetadata::with_type("related_to").weight(0.9),
        )
        .unwrap();
        g.add_edge(
            Bytes::from("memory:b"),
            Bytes::from("memory:c"),
            EdgeMetadata::with_type("caused_by").weight(0.7),
        )
        .unwrap();
        g.add_edge(
            Bytes::from("memory:a"),
            Bytes::from("memory:d"),
            EdgeMetadata::with_type("part_of").weight(0.5),
        )
        .unwrap();

        let results = g.traverse_bfs(b"memory:a", 2).unwrap();
        assert!(results.len() >= 3); // a, b, c, d — at least a + 2 neighbors

        // Start node at depth 0
        assert_eq!(results[0].depth, 0);
        assert_eq!(results[0].node_id, Bytes::from("memory:a"));

        // All reachable nodes should be present
        let keys: std::collections::HashSet<_> = results.iter().map(|r| r.node_id.clone()).collect();
        assert!(keys.contains(&Bytes::from("memory:b")));
        assert!(keys.contains(&Bytes::from("memory:c")));
        assert!(keys.contains(&Bytes::from("memory:d")));
    }

    #[test]
    fn test_traverse_bfs_with_type_filter() {
        let g = temp_kuzu();

        g.add_edge(
            Bytes::from("memory:a"),
            Bytes::from("memory:b"),
            EdgeMetadata::with_type("related_to"),
        )
        .unwrap();
        g.add_edge(
            Bytes::from("memory:a"),
            Bytes::from("memory:c"),
            EdgeMetadata::with_type("caused_by"),
        )
        .unwrap();

        let results = g
            .traverse_bfs_with_type(b"memory:a", 1, &["related_to".to_string()])
            .unwrap();
        let keys: std::collections::HashSet<_> = results.iter().map(|r| r.node_id.clone()).collect();
        assert!(keys.contains(&Bytes::from("memory:b")));
        // memory:c should be filtered out since it's a "caused_by" edge
    }

    #[test]
    fn test_get_neighbors_by_type() {
        let g = temp_kuzu();

        g.add_edge(
            Bytes::from("memory:a"),
            Bytes::from("memory:b"),
            EdgeMetadata::with_type("related_to"),
        )
        .unwrap();
        g.add_edge(
            Bytes::from("memory:a"),
            Bytes::from("memory:c"),
            EdgeMetadata::with_type("caused_by"),
        )
        .unwrap();

        let by_type = g.get_neighbors_by_type(b"memory:a", "related_to").unwrap();
        assert_eq!(by_type.len(), 1);
        assert_eq!(by_type[0].0, Bytes::from("memory:b"));
    }

    #[test]
    fn test_contains_node() {
        let g = temp_kuzu();
        assert!(!g.contains_node(b"memory:x"));

        g.add_edge(
            Bytes::from("memory:x"),
            Bytes::from("memory:y"),
            EdgeMetadata::default(),
        )
        .unwrap();

        assert!(g.contains_node(b"memory:x"));
        assert!(g.contains_node(b"memory:y"));
    }

    #[test]
    fn test_remove_node_edges() {
        let g = temp_kuzu();

        g.add_edge(
            Bytes::from("memory:a"),
            Bytes::from("memory:b"),
            EdgeMetadata::with_type("r1"),
        )
        .unwrap();
        g.add_edge(
            Bytes::from("memory:c"),
            Bytes::from("memory:a"),
            EdgeMetadata::with_type("r2"),
        )
        .unwrap();

        let removed = g.remove_node_edges(b"memory:a").unwrap();
        assert_eq!(removed.len(), 2);

        // No edges remaining for node a
        let neighbors = g.get_neighbors(b"memory:a").unwrap();
        assert!(neighbors.is_empty());
    }

    #[test]
    fn test_edge_upsert() {
        let g = temp_kuzu();

        g.add_edge(
            Bytes::from("memory:a"),
            Bytes::from("memory:b"),
            EdgeMetadata::with_type("SimilarTo").weight(0.5),
        )
        .unwrap();
        g.add_edge(
            Bytes::from("memory:a"),
            Bytes::from("memory:b"),
            EdgeMetadata::with_type("SimilarTo").weight(0.9),
        )
        .unwrap();

        let neighbors = g.get_neighbors(b"memory:a").unwrap();
        assert_eq!(neighbors.len(), 1);
        assert!((neighbors[0].1.weight - 0.9).abs() < 0.01);
    }
}
