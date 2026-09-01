# remem-server

REST API + embedded storage engine. Axum on port 4545.

## Source Layout

```
src/
├── main.rs
├── config.rs           # TaskConfig (lifecycle intervals, discovery_workers, queue_size)
├── error.rs
├── metrics.rs
├── api/                # Axum route handlers
│   └── mcp/            # In-process MCP server — Streamable HTTP at /mcp
│       ├── mod.rs      # Module wiring
│       ├── protocol.rs # JSON-RPC 2.0 types
│       ├── tools.rs    # 8 MCP tools, calls AppServices directly (no HTTP hop)
│       ├── resources.rs # memory://stats, collections/*, graph/{id}
│       ├── handler.rs  # JSON-RPC method dispatch (initialize/ping/tools/resources)
│       └── transport.rs # Streamable HTTP session management + POST/GET/DELETE /mcp
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
POST   /mcp                          MCP JSON-RPC (Streamable HTTP, 2025-03-26 spec)
GET    /mcp                          405 — no server-initiated push streams
DELETE /mcp                          End an MCP session
```

`/mcp` is merged into the `api` router *before* the auth middleware layer
(`api/mod.rs`), so it inherits the same API-key auth as `/api/v1/*` — see
`api/mcp/transport.rs`. Session IDs are UUIDs issued on `initialize`, tracked
in an in-memory map with a 30-minute idle sweep and a 1000-session cap
(`MAX_SESSIONS` in `transport.rs`; returns 503 past the cap). This is the
network transport; `crates/remem-mcp/` still exists separately for stdio only
— see `crates/remem-mcp/AGENTS.md`.

## Write-Path Architecture

`POST /api/v1/memories` critical path (~260 req/s at 50 concurrency, embedding-bound — durability is not the limit here, see `docs/WRITE_PATH_OPTIMISATIONS.md`):

1. `memory_manager.create()` embeds content → `(Memory, Vec<f32>)` — embedding computed once
2. `engine.store_memory_core()` — submits one contiguous WAL batch (KV + timestamp + tags) through the commit coordinator; naturally overlapping batches share one fsync; HNSW insert runs on `spawn_blocking`
3. Handler fires `discovery_tx.try_send(DiscoveryTask { ... })` — **fire-and-forget**, returns immediately
4. Background worker calls `connection_manager.auto_discover()` → async `add_edges_batch` (one contiguous coordinator batch for all edges)

`AppServices` has `discovery_tx: mpsc::Sender<DiscoveryTask>`. Two workers drain via `Arc<Mutex<mpsc::Receiver>>`. Channel capacity 10,000 — if full, discovery is skipped (warn only).

### WAL commit coordinator invariants

- `WalCommitCoordinator` is the sole runtime owner of `WAL`; mutation code must never append, sync, size, or truncate it directly. Pre-startup format migrations are the intentional exception.
- Normal mutations hold shared write admission through WAL completion and in-memory application. Timestamp reservation, commit-position assignment, and complete-batch submission stay together under the async write sequencer.
- There is no batching timer. The worker drains only writes already queued, up to its bounded group limit; an isolated durable write proceeds immediately.
- With `sync_writes = true`, every acknowledged group has one successful physical sync. With `false`, writes complete after buffered append, while barriers still flush before persistence.
- Checkpoint and shutdown hold exclusive admission, establish a coordinator barrier, persist indexes and MemTables, and truncate only after every prerequisite succeeds. A failure retains the WAL. Shutdown rejects new writes, drains accepted writes, stops the coordinator, and joins its OS thread even when persistence fails.
- An append or sync failure is returned to every dependent waiter and never advances the durable position. A later successful sync may cover bytes from an earlier ambiguous sync error, but the earlier callers retain their errors.

## Storage Engine API Patterns

- `impl Into<Bytes>` params (put, add_edge, etc.): pass owned `String` or `.clone()` — never `&str`
- `impl AsRef<[u8]>` params (get, get_neighbors, etc.): pass `&key` or `key.as_bytes()`
- `BooleanMode`: in `engine::query`, not re-exported from the crate root (`MergeStrategyType` was deleted with the unreachable planner — RRF is the only fusion strategy, see REM-72)
- **Preferred write**: await `engine.store_memory_core(key, value, embedding, timestamp, tags)` — one coordinator submission; durability completes before in-memory application
- **Batch edges**: await `engine.add_edges_batch(edges, created_at_ms)` — one contiguous WAL batch for all edges, shared timestamp across the batch
- **Attribute sidecar rows** (`engine/attr/`, REM-75): a record's filterable
  attributes (importance, health, valence, timestamps, …) travel with the
  payload as a compact `attr:<record key>` row, written in the same WAL
  batch and under the same write-sequencer acquisition as the payload — via
  `put_with_attrs`, `put_with_embedding_and_attrs`, or `store_memory_core`'s
  `attrs` parameter. `select()` (`engine/attr/select.rs`) returns record
  keys, never payloads — narrowing candidates by row before any
  deserialization. `MemoryRepository` (`services/repository.rs`) is the only
  place that derives a row (`services::attrs::project`); service code above
  it never builds one directly. `select_ordered()` walks one slot's index
  ascending and backs `MemoryManager::list`: `services/filters.rs` maps
  `MemoryFilters` onto predicates, the walked slot supplies the sort order,
  and payloads are read only for the returned page. Tags are the exception —
  they carry no slot, so a tag filter narrows through the tag index only
  when `StorageEngine::tag_index_can_answer` confirms the index can answer
  for every requested tag; it returns false when the tag index is off, or
  when a tag falls outside the index's token-length bounds
  (`max_token_length` is hardwired to 100), because `tag_search_and` reports
  both cases as "no matches" and a caller cannot tell them apart from a real
  miss. When it returns false, `list()` skips the narrowing and the payload
  check alone decides. Either way the payload is the authority: tags carry
  no slot, so the predicate only decides whether the index gets to narrow
  first. `matches_filters`
  (`services/filters.rs`) remains the search path's filter evaluator until
  REM-78.
- **Search paths**: all three `search_type` values execute through `QueryEngine`
  (REM-88). `services/search_engine.rs` must not call the engine's retrieval
  methods directly — a guard test in `services/mod.rs` enforces this.
  `vector_metric()` is allowed; it reads configuration.
- **Keyword search** plans `TagSearch` + `ContentScan` and fuses by RRF. The
  scan reads the whole corpus on every keyword query and is deleted by REM-29,
  which indexes content into the inverted index.
- **Format versions**: `<data_dir>/FORMAT` maps subsystem to version; `StorageEngine::new`
  refuses a directory from a newer build and runs pending migrations before opening
  anything. Adding an on-disk change means adding a `Migration` — see
  [docs/STORAGE_FORMAT.md](../../docs/STORAGE_FORMAT.md).

## WAL Record Types

Twelve types, all correctly encoded/decoded (`wal.rs::WalRecordType`):
`Insert`, `Delete`, `InsertWithEmbedding`, `SetTimestamp`, `AddTags`,
`AddEdge`, `SetTags`, `RemoveEdge`, `RemoveTimestamp`, `RemoveTags`,
`RemoveVector`, `PutAttrs`.
- `SetTags` uses same wire format as `AddTags` (tag count + length-prefixed strings)
- `RemoveEdge` encodes only `source` + `target` (no edge_type/weight needed for deletion)
- `RemoveTimestamp` / `RemoveTags` / `RemoveVector` signal index cleanup on replay — no payload beyond the key
- `PutAttrs` carries a record's encoded attribute sidecar row in `value`, keyed by the *record* key (not the sidecar key — the reader derives `attr:<key>` itself)
- Bug fixed 2026-03-26: `SetTags` and `RemoveEdge` payloads were silently dropped from WAL

## Segmented Index Storage

All four indexes use Lucene-style sealed segments:
- **Manifest**: `SegmentManifest` written atomically via `.manifest.tmp` + rename; contains `ChunkMeta` (CRC32, entry range, sealed flag)
- **Segment I/O**: 8-byte magic header, version, index type byte, CRC32 footer; corrupt chunk → warn + skip
- **HNSW**: single in-memory graph (recall preserved), node data in `nodes_{start}_{end}.seg`; `DirtyChunkTracker` ensures only dirty chunks flush at checkpoint
- **Graph/BTree/Tags**: growing in-memory segment + sealed immutable segments on disk; `seal_growing()` writes `.seg` + updates manifest. Tag segments are `index.tags` v2 payloads with an ordered key directory so sealed `.del` bitsets can be updated by external key.
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
python ../../scripts/generate_memories.py --url http://localhost:4545/api/v1/memories --count 100000 --concurrency 100
```
