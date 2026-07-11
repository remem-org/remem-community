# remem-server

REST API + embedded storage engine. Axum on port 8001.

## Source Layout

```
src/
├── main.rs
├── config.rs           # TaskConfig (lifecycle intervals, discovery_workers, queue_size)
├── error.rs
├── metrics.rs
├── api/                # Axum route handlers
├── services/           # memory_manager, search, connection_manager, lifecycle
├── embedding/          # fastembed-rs (MiniLM-L6-v2, 384 dims)
├── engine/
│   ├── index/
│   │   ├── hnsw.rs             # ANN search; chunked .seg files + DirtyChunkTracker
│   │   ├── graph_segmented.rs  # SegmentedCsrGraph — connection traversal
│   │   ├── btree_segmented.rs  # SegmentedBTreeIndex — time-range queries
│   │   ├── inverted_segmented.rs # SegmentedInvertedIndex — tag search + soft-delete
│   │   ├── manifest.rs         # Generation-based SegmentManifest (atomic tmp→rename)
│   │   ├── segment_io.rs       # SegmentWriter/SegmentReader + CRC32 footer
│   │   └── dirty.rs            # DirtyChunkTracker (per-chunk AtomicBool)
│   └── storage/
│       ├── engine.rs           # StorageEngine
│       ├── init.rs             # Index loading + legacy .idx migration
│       ├── tasks.rs            # Flush/compaction/checkpoint Tokio loops
│       └── recovery.rs         # WAL replay
└── tasks/              # Background task registry + lifecycle task runners
```

## REST API

```
POST   /api/v1/memories              Create memory (fires discovery async)
GET    /api/v1/memories/{id}         Get memory
PUT    /api/v1/memories/{id}         Update memory
DELETE /api/v1/memories/{id}         Delete/archive memory
GET    /api/v1/memories              List memories
POST   /api/v1/memories/search       Search (semantic/keyword/hybrid)
GET    /api/v1/memories/{id}/related Find related memories
POST   /api/v1/memories/{id}/promote Promote to long-term
GET    /api/v1/health                Health check (deep: storage + embedding + tasks)
GET    /api/v1/stats                 System statistics
GET    /api/v1/tasks                 Background task registry
POST   /api/v1/tasks/{name}/run      Manually trigger a task
```

## Write-Path Architecture

`POST /api/v1/memories` critical path (~291 req/s at 50 concurrency):

1. `memory_manager.create()` embeds content → `(Memory, Vec<f32>)` — embedding computed once
2. `engine.store_memory_core()` — 1 WAL lock, 1 fsync (KV + timestamp + tags); HNSW insert on `spawn_blocking`
3. Handler fires `discovery_tx.try_send(DiscoveryTask { ... })` — **fire-and-forget**, returns immediately
4. Background worker calls `connection_manager.auto_discover()` → `add_edges_batch` (1 WAL lock for all edges)

`AppServices` has `discovery_tx: mpsc::Sender<DiscoveryTask>`. Two workers drain via `Arc<Mutex<mpsc::Receiver>>`. Channel capacity 10,000 — if full, discovery is skipped (warn only).

## Storage Engine API Patterns

- `impl Into<Bytes>` params (put, add_edge, etc.): pass owned `String` or `.clone()` — never `&str`
- `impl AsRef<[u8]>` params (get, get_neighbors, etc.): pass `&key` or `key.as_bytes()`
- `BooleanMode`, `MergeStrategyType`: in `remem_storage::query`, not re-exported from root
- **Preferred write**: `engine.store_memory_core(key, value, embedding, timestamp, tags)` — single WAL lock + fsync
- **Batch edges**: `engine.add_edges_batch(edges)` — single WAL lock for all edges

## WAL Record Types

All eight types (`Insert`, `InsertWithEmbedding`, `Delete`, `SetTimestamp`, `AddTags`, `SetTags`, `AddEdge`, `RemoveEdge`) are correctly encoded/decoded:
- `SetTags` uses same wire format as `AddTags` (tag count + length-prefixed strings)
- `RemoveEdge` encodes only `source` + `target` (no edge_type/weight needed for deletion)
- Bug fixed 2026-03-26: `SetTags` and `RemoveEdge` payloads were silently dropped from WAL

## Segmented Index Storage

All four indexes use Lucene-style sealed segments:
- **Manifest**: `SegmentManifest` written atomically via `.manifest.tmp` + rename; contains `ChunkMeta` (CRC32, entry range, sealed flag)
- **Segment I/O**: 8-byte magic header, version, index type byte, CRC32 footer; corrupt chunk → warn + skip
- **HNSW**: single in-memory graph (recall preserved), node data in `nodes_{start}_{end}.seg`; `DirtyChunkTracker` ensures only dirty chunks flush at checkpoint
- **Graph/BTree/Tags**: growing in-memory segment + sealed immutable segments on disk; `seal_growing()` writes `.seg` + updates manifest
- **Compaction**: `SegmentedBTreeIndex` and `SegmentedInvertedIndex` use size-tiered compaction (merges two smallest, drops deleted entries)
- **Legacy migration**: `init.rs` auto-detects `.idx` files on startup; migrates once

## Configuration

`config/remem-server.toml`:
```toml
[storage]
data_dir = "/var/lib/remem"
sync_writes = true
checkpoint_interval_secs = 300
max_wal_size_mb = 256

[vector]
dimension = 384
hnsw_m = 16
hnsw_ef_construction = 200
hnsw_ef_search = 50
```

`TaskConfig` (in `config.rs`) controls lifecycle intervals:
- `expiration_interval_secs` (default 300)
- `decay_interval_secs` (default 86400)
- `consolidation_interval_secs` (default 604800)
- `cleanup_interval_secs` (default 2592000)
- `discovery_workers` (default 2)
- `discovery_queue_size` (default 10000)

## Known Quirks

### fastembed Cache Path
fastembed 3.14.1 ignores `FASTEMBED_CACHE_PATH`. `InitOptions` must explicitly read the env var and set `cache_dir`. Fix in `src/embedding/mod.rs`.

### ONNX Runtime (Docker)
ort-sys rc.4 downloads ONNX Runtime 1.18.1 at build time. Dockerfile pre-downloads it and sets `ORT_LIB_LOCATION=/ort-libs`. Model pre-baked via Python `huggingface_hub.snapshot_download` stage.

### ort-sys Version Pin
`fastembed 3.14.1` → `ort 2.0.0-rc.4` → `ort-sys`. Without pin, Cargo resolves ort-sys to rc.12 which requires TLS and breaks. Pin: `ort-sys = { version = "=2.0.0-rc.4" }` in `Cargo.toml`.

### Stats Endpoint — Memory Count Bug (fixed)
`compute_memory_counts` in `health.rs` previously counted promoted memories in both short_term AND long_term (tag-based counting). Fixed by scanning the actual `memory_type` field in stored JSON.

## Scripts

```bash
# Generate 100,000 test memories
pip install httpx
python ../../scripts/generate_memories.py
python ../../scripts/generate_memories.py --url http://localhost:8001/api/v1/memories --count 100000 --concurrency 100
```
