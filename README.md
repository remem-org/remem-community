# Remem

> A memory system for LLMs and AI agents with persistent long-term and short-term memory capabilities

[![Rust](https://img.shields.io/badge/rust-1.82+-orange.svg)](https://www.rust-lang.org/)
[![License: FSL-1.1-ALv2](https://img.shields.io/badge/License-FSL--1.1--ALv2-blue.svg)](https://fsl.software)

## Overview

Remem is a high-performance memory system designed for Large Language Models (LLMs) and AI agents. It provides:

- **Persistent Memory**: Short-term (with TTL) and long-term memory storage
- **Semantic Search**: Fast vector similarity search via HNSW index
- **Automatic Connections**: Discovers and maintains relationships between related memories
- **Lifecycle Management**: Background tasks for memory expiration, importance decay, consolidation, and cleanup
- **MCP Integration**: Exposes all functionality through the Model Context Protocol for seamless LLM use
- **Unified Rust Backend**: Single binary embedding storage engine, embedding service, REST API, and background tasks

## Architecture

```
┌──────────────────────────────────────────────┐
│        remem-server  (port 4545)             │
│  REST API (Axum)                             │
│  + in-process MCP (Streamable HTTP) at /mcp  │
│  Memory Manager | Search Engine              │
│  Connection Manager | Lifecycle Manager      │
│  Embedding Service (fastembed, MiniLM-L6-v2) │
│  Storage Engine (HNSW | CSR | LSM-tree)      │
│  Background Tasks (Tokio)                    │
└──────────────────────────────────────────────┘
```

`remem-mcp` is a separate stdio-only binary for clients that only speak
stdio MCP; clients that speak Streamable HTTP can talk to `remem-server`'s
`/mcp` route directly.

### Storage Engine (embedded in remem-server)

The storage engine is implemented in Rust and embedded directly in-process:

- **HNSW Vector Index** — Approximate nearest neighbour search for semantic queries; node data stored in per-chunk `.seg` files with CRC32 integrity checking and a generation-based manifest
- **CSR Graph Index** — Efficient traversal of memory connections (`SegmentedCsrGraph`)
- **BTree Time-series Index** — Time-windowed sealed chunks for range queries (`SegmentedBTreeIndex`)
- **Inverted Tag Index** — Lucene-style sealed segments with soft-delete bitsets and size-tiered compaction (`SegmentedInvertedIndex`)
- **LSM-tree KV Store** — Durable metadata storage with write-ahead log, memtable, SSTables, and compaction

All four indexes use a shared segmented architecture: each index consists of a set of sealed immutable segment files plus a growing in-memory segment. Checkpoints write only dirty chunks. Corrupt segments are skipped with a warning rather than causing startup failure. Legacy single-file `.idx` indexes are auto-migrated on first run.

### Background Tasks

Lifecycle management runs as Tokio tasks inside remem-server — no separate worker process needed:

- Memory expiration check (every 5 minutes)
- Importance decay for long-term memories (daily)
- Consolidation of similar memories (weekly)
- Cleanup of archived memories (monthly)

## Quick Start

### Prerequisites

- Docker and Docker Compose
- 2GB+ RAM

### Up in 30 seconds

Prebuilt image:

```bash
docker pull rememorg/remem-community:server-latest
docker compose up -d

# Prebuilt binaries: coming soon
```

See [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) for a ready-to-use compose file.

### Run from source

```bash
git clone https://github.com/yourusername/remem.git
cd remem
cp .env.example .env
docker compose up -d
```

Services started:

| Service | Port | Description |
|---------|------|--------------|
| remem-server | 4545 | REST API + storage engine; MCP Streamable HTTP at `/mcp` |

### Verify

```bash
docker compose ps

# Core API
curl http://localhost:4545/api/v1/health

# MCP server (Streamable HTTP, in-process)
curl -sf -X POST http://localhost:4545/mcp \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}'
```

## MCP Tools

`remem-mcp` is a stdio-only binary, run directly by the client. Two options:

**Option 1: run the `remem-mcp` binary directly (stdio)**

```json
{
  "mcpServers": {
    "remem": {
      "command": "cargo",
      "args": ["run", "--release", "-p", "remem-mcp", "--", "--server-url", "http://localhost:4545"]
    }
  }
}
```

**Option 2: point a Streamable-HTTP-capable client straight at `remem-server`**

For clients that speak MCP Streamable HTTP directly, skip `remem-mcp`
entirely and configure the client with the URL `http://localhost:4545/mcp`,
passing `REMEM_API_KEY` as a bearer token if auth is enabled.

Available tools:

| Tool | Description |
|------|-------------|
| `store_memory` | Store a new memory with auto-connection discovery |
| `search_memories` | Semantic, keyword, or hybrid search |
| `get_memory` | Retrieve a memory by ID |
| `update_memory` | Update content, tags, or importance |
| `delete_memory` | Soft archive or hard delete |
| `find_related` | Graph traversal to find related memories |
| `promote_to_longterm` | Promote short-term → long-term |
| `list_recent_memories` | List recently created/accessed memories |

MCP resources: `memory://stats`, `memory://collections/recent`, `memory://collections/important`

## REST API

Base URL: `http://localhost:4545`
Interactive docs: `http://localhost:4545/docs`

```bash
# Store a memory
curl -X POST http://localhost:4545/api/v1/memories \
  -H "Content-Type: application/json" \
  -d '{"content": "User prefers Rust for backend", "tags": ["preferences"], "importance": 0.8}'

# Semantic search
curl -X POST http://localhost:4545/api/v1/memories/search \
  -H "Content-Type: application/json" \
  -d '{"query": "programming language preferences", "search_type": "hybrid", "limit": 5}'

# Find related memories
curl "http://localhost:4545/api/v1/memories/{id}/related?depth=2"
```

Endpoints:

```
POST   /api/v1/memories              Create memory
GET    /api/v1/memories/{id}         Get memory
PUT    /api/v1/memories/{id}         Update memory
DELETE /api/v1/memories/{id}         Delete/archive memory
GET    /api/v1/memories              List memories
POST   /api/v1/memories/search       Search (semantic/keyword/hybrid)
GET    /api/v1/memories/{id}/related Find related memories
POST   /api/v1/memories/{id}/promote Promote to long-term
GET    /api/v1/health                Health check
GET    /api/v1/stats                 System statistics
```

## Configuration

Key settings in `config/remem-server.toml` and `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `REMEM_API_KEY` | _(empty)_ | API key for `remem-server`. Required in production. Empty key returns HTTP 500 unless `REMEM_ALLOW_AUTH_DISABLED=true` is also set. |
| `REMEM_ALLOW_AUTH_DISABLED` | `false` | Set to `true` to explicitly allow running without an API key (development only). |
| `RUST_LOG` | `info` | Log filter (trace/debug/info/warn/error) |

Storage engine parameters (in `config/remem-server.toml`):

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

## Development

### Build

```bash
# Check all crates
cargo check

# Build release binaries
cargo build --release -p remem-server
cargo build --release -p remem-mcp
```

### Code Quality

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

### Pre-commit Hooks

```bash
pre-commit install
pre-commit run --all-files
```

## Performance

- Vector search (100K memories): **~4ms**
- Write throughput: **~291 req/s** (`POST /memories`, 50 concurrency, `sync_writes = true`)
- Hybrid search: **>186 ops/sec**
- Targets: search <500ms, 1000 req/s, millions of memories

Write path optimisations (merged 2026-03-26):
- Single WAL lock + fsync per memory creation (`store_memory_core`)
- Batch edge WAL writes for auto-discovery (`add_edges_batch`)
- HNSW insert dispatched to blocking thread pool
- Auto-discovery runs asynchronously (background workers, fire-and-forget)

## Project Status

| Component | Status |
|-----------|--------|
| Storage engine (LSM-tree, HNSW, CSR) | ✅ Complete |
| Embedding service (fastembed, MiniLM) | ✅ Complete |
| Memory services (CRUD, search, connections, lifecycle) | ✅ Complete |
| REST API (Axum) | ✅ Complete |
| MCP server (Rust, stdio via remem-mcp + in-process Streamable HTTP at remem-server:/mcp) | ✅ Complete |
| Background lifecycle tasks (Tokio) | ✅ Complete |

## Development Utilities

```bash
# Generate 100,000 test memories for stress testing / benchmarking
pip install httpx
python scripts/generate_memories.py

# Options: --url, --count, --concurrency
python scripts/generate_memories.py --count 10000 --concurrency 50
```

## Documentation

### Development Guides (CLAUDE.md hierarchy)

| File | Contents |
|------|----------|
| [CLAUDE.md](CLAUDE.md) | Architecture overview, build commands, design principles, roadmap |
| [crates/remem-server/CLAUDE.md](crates/remem-server/CLAUDE.md) | Storage engine internals, write-path, WAL, HNSW segments, API patterns, quirks |
| [crates/remem-mcp/CLAUDE.md](crates/remem-mcp/CLAUDE.md) | MCP tools, resources, transport, client configuration |

## License

Functional Source License, Version 1.1, ALv2 Future License (FSL-1.1-ALv2) — see [LICENSE](LICENSE) for details.

Source is available and free to use for any purpose except competing commercial use. Converts to Apache License 2.0 two years after each release. See [fsl.software](https://fsl.software) for details.

---

**Status**: Alpha — under active development
