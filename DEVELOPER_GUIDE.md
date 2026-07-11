# Remem Developer Guide (Community Edition)

Remem is a persistent memory system for LLMs and AI agents. It stores memories with
vector embeddings, automatically discovers connections between them, and exposes
everything through the Model Context Protocol (MCP) as well as a plain REST API.

This guide covers the **Community Edition**: the open-source core made up of two
Rust services, `remem-server` and `remem-mcp`. It does not cover the web-based
Backoffice admin UI, Prometheus/Grafana observability, or RBAC/multi-database
features — those ship in the Business Edition.

## Architecture

```
┌──────────────────────────────────────────────┐
│        remem-mcp   (port 8000)               │
│  MCP Server — stdio / SSE transport          │
│  8 tools | 3 resources | JSON-RPC 2.0        │
└──────────────────────────────────────────────┘
                      │  HTTP
┌──────────────────────────────────────────────┐
│        remem-server   (port 8001)            │
│  REST API (Axum)                             │
│  Memory Manager | Search Engine              │
│  Connection Manager | Lifecycle Manager      │
│  Embedding Service (fastembed, MiniLM-L6-v2) │
│  Storage Engine (HNSW | CSR | BTree | LSM)   │
│  Background Tasks (Tokio)                    │
└──────────────────────────────────────────────┘
```

`remem-mcp` is a thin protocol adapter — it holds no data itself. All state lives in
`remem-server`, which embeds its own storage engine directly in-process (no external
database, no Qdrant/Postgres/Redis dependency for the core).

### Storage engine

Four purpose-built indexes, all embedded in `remem-server`:

| Index | Purpose |
|-------|---------|
| HNSW | Approximate nearest-neighbour vector search for semantic queries |
| CSR Graph | Traversal of memory-to-memory connections |
| BTree | Time-windowed range queries |
| Inverted Tag Index | Tag-based lookup with soft-delete support |
| LSM-tree KV store | Durable metadata storage with a write-ahead log |

All writes go through a WAL before being acknowledged, so a crash mid-write never
loses committed data. Indexes checkpoint to disk periodically (`checkpoint_interval_secs`)
rather than on every write.

### Background tasks

Lifecycle tasks run as Tokio tasks inside `remem-server` — no separate worker process
is needed:

- **Short-term expiration** — archives short-term memories past their `ttl` (default: every 5 minutes)
- **Importance decay** — long-term memories lose ~0.5%/day importance (default: daily); flashbulb memories (see below) are exempt
- **Active forgetting** — every memory's `health` (0–100) decays daily (short-term −8/day, long-term −2/day from its last recall); a memory is hard-deleted once `health` reaches 0. Fetching a memory (`GET /api/v1/memories/{id}` or MCP `get_memory`) boosts its `health` by +10, capped at 100 — frequently-recalled memories effectively never decay away. Flashbulb memories are exempt. (default: daily)
- **Consolidation** of similar memories (default: weekly)
- **Cleanup** of archived memories (default: monthly)

### Emotional memory & flashbulb protection

`store_memory` accepts `emotional_valence` (-1.0…1.0) and `arousal` (0.0…1.0). A
memory **created** with `arousal >= 0.8` becomes a **flashbulb memory**: it's
force-promoted to `long_term`, its `importance` is floored at `0.9`, and it's exempt
from importance decay and active forgetting for 30 days (`flashbulb_until`). Note
this promotion only happens at creation time — raising `arousal` past `0.8` via
`update_memory` on an existing memory does not retroactively apply it.

## Quick Start

### Prerequisites

- Docker and Docker Compose
- 2GB+ RAM free

### 1. Pull the images

```bash
docker pull remem/remem-server:latest
docker pull remem/remem-mcp:latest
```

### 2. Create a compose file

```yaml
# docker-compose.yml
services:
  remem-server:
    image: remem/remem-server:latest
    container_name: remem-server
    ports:
      - "8001:8001"
    volumes:
      - ./data/remem:/var/lib/remem
      - ./config/remem-server.toml:/etc/remem/config.toml:ro
    environment:
      - RUST_LOG=info
      - REMEM_API_KEY=${REMEM_API_KEY:-}
      - REMEM_ALLOW_AUTH_DISABLED=true   # dev only — remove once REMEM_API_KEY is set
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8001/api/v1/ready"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
    restart: unless-stopped

  remem-mcp:
    image: remem/remem-mcp:latest
    container_name: remem-mcp-server
    ports:
      - "8000:8000"
    environment:
      - REMEM_SERVER_URL=http://remem-server:8001
      - MCP_TRANSPORT=sse
      - REMEM_API_KEY=${REMEM_API_KEY:-}
    depends_on:
      remem-server:
        condition: service_healthy
    restart: unless-stopped
```

Download the default config into `./config/remem-server.toml` — see
[Configuration](#configuration) below for what each field does — or start without
a mounted config file to run on built-in defaults.

### 3. Start the services

```bash
docker compose up -d
docker compose ps
```

The `start_period: 60s` on `remem-server`'s healthcheck exists because the embedding
model loads on first boot; give it a minute before expecting `healthy`.

### 4. Verify

```bash
curl http://localhost:8001/api/v1/health
curl http://localhost:8000/health
```

### 5. Store and search a memory

```bash
curl -X POST http://localhost:8001/api/v1/memories \
  -H "Content-Type: application/json" \
  -d '{"content": "User prefers Rust for backend services", "tags": ["preferences"], "importance": 0.8}'

curl -X POST http://localhost:8001/api/v1/memories/search \
  -H "Content-Type: application/json" \
  -d '{"query": "programming language preferences", "search_type": "hybrid", "limit": 5}'
```

If you set `REMEM_API_KEY`, add `-H "Authorization: Bearer $REMEM_API_KEY"` to every
call except `/api/v1/health`.

## Connecting an LLM client via MCP

`remem-mcp` speaks JSON-RPC 2.0 over stdio or SSE. To use it from Claude Desktop or
Claude Code, exec into the running container over stdio:

```json
{
  "mcpServers": {
    "remem": {
      "command": "docker",
      "args": ["exec", "-i", "remem-mcp-server", "remem-mcp", "--transport", "stdio"]
    }
  }
}
```

For a client that speaks SSE directly, point it at `http://localhost:8000` instead —
no exec needed.

### MCP tools

Each tool's `inputSchema` is standard JSON Schema, returned from `tools/list`. Full
parameter reference:

| Tool | Required | Optional | Notes |
|------|----------|----------|-------|
| `store_memory` | `content` | `memory_type` (`short_term`\|`long_term`), `tags[]`, `importance` (0-1), `emotional_valence` (-1 to 1), `arousal` (0-1), `health` (0-100), `ttl` (seconds), `source`, `graph_extraction` | `graph_extraction: {entities[], relationships[]}` lets the calling LLM supply entities/relationships extracted from the conversation; each becomes a linked long-term memory. `arousal >= 0.8` creates a protected "flashbulb" memory (see [Emotional memory & flashbulb protection](#emotional-memory--flashbulb-protection)). |
| `search_memories` | `query` | `search_type` (`semantic`\|`keyword`\|`hybrid`, default `hybrid`), `limit`, `related_to` (memory UUID), `filters: {memory_type, tags[], importance_min, importance_max}` | `related_to` boosts memories connected to that memory in the graph. |
| `get_memory` | `memory_id` | `include_connections` | |
| `update_memory` | `memory_id` | `content`, `tags[]`, `importance`, `source` | At least one optional field must be present. |
| `delete_memory` | `memory_id` | `hard_delete` (default `false` = soft archive) | |
| `find_related` | `memory_id` | `depth` (default 1), `limit` (default 10) | Graph traversal via connection edges. |
| `promote_to_longterm` | `memory_id` | — | |
| `list_recent_memories` | — | `limit`, `memory_type`, `sort_by` (`created_at`\|`accessed_at`) | |

Example `tools/call` request and response for `search_memories`:

```json
// request
{"name": "search_memories", "arguments": {"query": "rust backend preferences", "search_type": "hybrid", "limit": 5}}

// response (unwrapped from the MCP text-content envelope)
{
  "success": true,
  "query": "rust backend preferences",
  "results_count": 1,
  "results": [ { "memory": { "id": "...", "content": "..." }, "score": 0.91 } ],
  "summary": "Found 1 memories for 'rust backend preferences'"
}
```

Every tool returns `{"success": true, ...}` on success or
`{"success": false, "error": "TOOL_ERROR", "message": "..."}` on failure — errors are
never thrown as JSON-RPC protocol errors, so check `success` in the result body.

### MCP resources

| Resource URI | Description |
|---|---|
| `memory://stats` | System statistics snapshot |
| `memory://collections/recent` | Recently created/accessed memories |
| `memory://collections/important` | High-importance memories |

## REST API reference

Base URL: `http://localhost:8001`. The full OpenAPI 3 spec is served at
`GET /api/v1/openapi.json` (no UI is bundled — point any Swagger/Redoc/Postman
client at that URL, or import it directly).

All endpoints require `Authorization: Bearer <api_key>` (or `X-API-Key: <api_key>`)
once `REMEM_API_KEY` is set, **except** `/api/v1/health`, `/api/v1/ready`, and
`/api/v1/openapi.json`. Errors share one shape:

```json
{ "error": "validation_error", "message": "content must not be empty" }
```

### Memories

**`POST /api/v1/memories`** — create a memory. Fires connection discovery
asynchronously; the response returns before discovery finishes.

| Field | Type | Default | Notes |
|---|---|---|---|
| `content` | string | _required_ | 1–100,000 bytes |
| `memory_type` | `"short_term"` \| `"long_term"` | `short_term` | |
| `tags` | string[] | `[]` | max 50 |
| `importance` | float 0–1 | `0.5` | |
| `emotional_valence` | float -1–1 | `0.0` | |
| `arousal` | float 0–1 | `0.0` | `>= 0.8` triggers flashbulb protection |
| `health` | float 0–100 | `100.0` | active-forgetting health score |
| `ttl` | integer (seconds) | _none_ | only meaningful for `short_term` |
| `source` | string | _none_ | free-text provenance tag |
| `graph_extraction` | object | _none_ | `{entities: [{name, entity_type?, description?}], relationships: [{source, target, relationship_type?, strength?}]}` — max 100 entities / 200 relationships |

→ `201 Created` with the full `Memory` object (see [Data model](#data-model)).
`422` on empty/oversized content or too many tags/entities. `500` on embedding or
storage failure.

```bash
curl -X POST http://localhost:8001/api/v1/memories \
  -H "Content-Type: application/json" \
  -d '{
    "content": "User prefers Rust for backend services",
    "tags": ["preferences"],
    "importance": 0.8,
    "emotional_valence": 0.3
  }'
```

**`GET /api/v1/memories/{id}?include_connections=false`** — fetch one memory.
`200` with `Memory`, `404` if not found.

**`PUT /api/v1/memories/{id}`** — partial update. Body accepts any subset of
`content`, `tags`, `importance`, `emotional_valence`, `arousal`, `health`, `source`.
`200` with the updated `Memory`, `404` if not found, `422` on oversized content/tags.

**`DELETE /api/v1/memories/{id}?hard=false`** — soft-archives by default
(`archived: true`, excluded from search/list/stats); `hard=true` permanently removes
it. `200` with `{"success": true, "message": "memory deleted"}`, `404` if not found.

**`GET /api/v1/memories`** — paginated list.

| Query param | Notes |
|---|---|
| `limit` | default 10, max 100 |
| `offset` | default 0 |
| `memory_type` | `short_term` \| `long_term` |
| `tags` | comma-separated, e.g. `tags=rust,async` |
| `min_importance`, `max_importance` | 0.0–1.0 |
| `created_after`, `created_before` | RFC-3339, e.g. `2024-01-01T00:00:00Z` |
| `include_connections` | default `false` |

→ `200` with `{"total": n, "limit": n, "offset": n, "memories": [Memory, ...]}`.

### Search

**`POST /api/v1/memories/search`**

| Field | Type | Default | Notes |
|---|---|---|---|
| `query` | string | _required_ | |
| `search_type` | `"semantic"` \| `"keyword"` \| `"hybrid"` | `semantic` | |
| `limit` | integer | 10 | max 500 |
| `memory_type` | `"short_term"` \| `"long_term"` | _none_ | |
| `tags` | string[] | `[]` | |
| `min_importance`, `max_importance` | float 0–1 | _none_ | |
| `related_to` | UUID | _none_ | boosts memories graph-connected to this memory |

→ `200` with `{"results": [{"memory": Memory, "score": float}, ...], "total": n}`.
`422` on empty query or unknown `search_type`.

```bash
curl -X POST http://localhost:8001/api/v1/memories/search \
  -H "Content-Type: application/json" \
  -d '{"query": "programming language preferences", "search_type": "hybrid", "limit": 5}'
```

### Connections & graph traversal

Connections are usually created automatically by auto-discovery, but can also be
managed directly:

| Method & Path | Description |
|---|---|
| `GET /api/v1/connections?limit=50&offset=0` | List all connections (max limit 500) → `{"connections": [{"source_id", "connection": Connection}], "total", "limit", "offset"}` |
| `POST /api/v1/connections` | Create one: body `{"source_id", "target_id", "relationship_type"?, "strength"?}` (`relationship_type` defaults to `related_to`; one of `related_to`, `caused_by`, `part_of`, `references`, `contradicts`, `supports`, `similar_to`, `derived_from`) → `200` with `{"source_id", "connection"}` |
| `DELETE /api/v1/connections/{source_id}/{target_id}` | Remove a connection → `200` or `404` |
| `GET /api/v1/memories/{id}/related?depth=1&relationship_types=&limit=20` | Graph traversal from a memory. `depth` max 5, `limit` max 100, `relationship_types` comma-separated filter → `200` with `{"memory_id", "related": [{"memory": Memory, "connection": Connection}]}` |

```bash
curl "http://localhost:8001/api/v1/memories/{id}/related?depth=2"
```

### Lifecycle

**`POST /api/v1/memories/{id}/promote`** — promote a short-term memory to
long-term. `200` with the updated `Memory`, `404` if not found.

### Background tasks

| Method & Path | Description |
|---|---|
| `GET /api/v1/tasks` | Status of all registered tasks plus discovery-queue metrics: `{"tasks": [...], "uptime_ms", "discovery_queue_depth", "discovery_dropped", "discovery_workers_alive", "discovery_workers_total", "discovery_worker_restarts", "discovery_worker_last_panic"}` |
| `POST /api/v1/tasks/{name}/run` | Manually trigger a task now (`202 Accepted`, runs in background); `409` if already running, `422` if `name` is unknown |
| `GET /api/v1/tasks/{name}/history` | Recent run log for a task → `{"task", "history": [RunLog, ...]}` |
| `POST /api/v1/tasks/{name}/pause` / `.../resume` | Pause/resume a task's scheduled runs → `{"task", "paused": bool}` |

Known task names: `expire_short_term`, `apply_importance_decay`, `active_forgetting`,
`consolidate_similar`, `cleanup_archived`, `discover_connections` (see
[`[tasks]`](#configremem-servertoml) for their default intervals).

### System & health

| Method & Path | Auth | Description |
|---|---|---|
| `GET /api/v1/health` | none | Liveness: `{"status": "healthy", "message": "..."}` |
| `GET /api/v1/ready` | none | Readiness (used by container healthchecks): `{"status", "storage": {"ok", "vector_count", "vector_enabled", "graph_node_count"}, "embedding": bool}` |
| `GET /api/v1/health/deep` | required | Exercises the KV layer and vector index; `200` if both pass, `503` otherwise |
| `GET /api/v1/stats` | required | `{"success", "stats": {"total_memories", "short_term_memories", "long_term_memories", "total_connections", "avg_importance"}}` |
| `GET /api/v1/openapi.json` | none | Full OpenAPI 3 spec |

## Data model

This is the `Memory` object as returned by every REST endpoint and MCP tool
(embeddings themselves are never returned over the API — they're an internal
storage-engine detail):

```json
{
  "id": "uuid",
  "content": "string",
  "memory_type": "short_term | long_term",
  "metadata": {
    "created_at": "timestamp",
    "updated_at": "timestamp",
    "accessed_at": "timestamp",
    "access_count": "integer",
    "source": "string | null",
    "tags": ["string"],
    "importance": "float (0-1)",
    "emotional_valence": "float (-1 to 1)",
    "arousal": "float (0-1)",
    "health": "float (0-100)",
    "last_recalled_at": "timestamp | null",
    "flashbulb_until": "timestamp | null",
    "ttl": "integer (seconds) | null"
  },
  "connections": [
    {
      "target_id": "uuid",
      "relationship_type": "related_to | caused_by | part_of | references | contradicts | supports | similar_to | derived_from",
      "strength": "float (0-1)",
      "created_at": "timestamp"
    }
  ]
}
```

`connections` is only populated when a request opts in via `include_connections=true`
(REST) or `include_connections: true` (MCP `get_memory`) — list/get calls omit it by
default to keep responses small.

Connections are created automatically whenever a memory is stored: `remem-server`
compares the new embedding against existing memories and links anything above the
similarity threshold (`auto_discovery_threshold`, default `0.7`), capped at
`auto_discovery_top_k` (default `5`) new edges per memory. Discovery runs on a
background worker, so `store_memory` returns before discovery completes.

`emotional_valence`, `arousal`, and `health` drive real lifecycle behavior today —
see [Emotional memory & flashbulb protection](#emotional-memory--flashbulb-protection)
and the active-forgetting task under [Background tasks](#background-tasks).

## Configuration

### Environment variables

| Variable | Default | Description |
|----------|---------|--------------|
| `REMEM_API_KEY` | _(empty)_ | API key for `remem-server`. Required in production. |
| `REMEM_ALLOW_AUTH_DISABLED` | `false` | Set `true` to explicitly allow running with no API key (development only). |
| `REMEM_CORS_ORIGINS` | _(empty)_ | Comma-separated allowed CORS origins. |
| `RUST_LOG` | `info` | Log filter: `trace`/`debug`/`info`/`warn`/`error`. |
| `REMEM_SERVER_URL` | `http://remem-server:8001` | `remem-mcp` → `remem-server` endpoint. |
| `MCP_TRANSPORT` | `sse` | `stdio` or `sse`. |

### `config/remem-server.toml`

```toml
[server]
port = 8001
host = "0.0.0.0"
api_key = ""                        # empty = auth disabled (dev only)

[storage]
data_dir = "/var/lib/remem"
sync_writes = true
checkpoint_interval_secs = 300      # flush indexes to disk every 5 minutes
max_wal_size_mb = 256               # trigger checkpoint when WAL exceeds this

[vector]
dimension = 384                     # must match the embedding model
hnsw_m = 16                         # connections per node (16-64 recommended)
hnsw_ef_construction = 200          # build quality (100-400 recommended)
hnsw_ef_search = 50                 # search quality (40-200 recommended)

[embedding]
model = "all-MiniLM-L6-v2"
cache_size = 10000                  # embeddings kept in LRU cache

[connections]
auto_discovery_threshold = 0.7      # minimum similarity to auto-link two memories
auto_discovery_top_k = 5            # max auto-created connections per new memory

[tasks]
expire_short_term_secs = 300
apply_importance_decay_secs = 86400
active_forgetting_secs = 86400
consolidate_similar_secs = 604800
cleanup_archived_secs = 2592000
discover_connections_secs = 3600
discovery_workers = 2
discovery_queue_size = 10000
```

`dimension` in `[vector]` must match the embedding model's output size — changing
`[embedding].model` to something other than `all-MiniLM-L6-v2` requires updating
`dimension` to match, and requires reindexing existing data (dimension changes are
not migrated automatically).

## Building from source

```bash
git clone https://github.com/<org>/remem-community.git
cd remem-community

cargo check
cargo build --release -p remem-server
cargo build --release -p remem-mcp

cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test --workspace
```

Building `remem-server` from source downloads ONNX Runtime and the embedding model
on first build/run; expect the first `cargo build` and first container start to be
noticeably slower than subsequent ones.

## Performance

Measured on the reference benchmark setup (see repository benchmarks for methodology):

- Vector search (100K memories): **~4ms**
- Write throughput: **~291 req/s** (`POST /memories`, 50 concurrency, `sync_writes = true`)
- Hybrid search: **>186 ops/sec**

## Troubleshooting

**`remem-server` returns HTTP 500 on every request.**
`REMEM_API_KEY` is empty and `REMEM_ALLOW_AUTH_DISABLED` isn't set. Either set an API
key or explicitly set `REMEM_ALLOW_AUTH_DISABLED=true` for local development.

**Healthcheck stays `unhealthy` for the first minute.**
Expected — the embedding model loads into memory on startup. Wait for `start_period`
to elapse before treating it as a real failure.

**CORS errors from a browser-based client.**
Set `REMEM_CORS_ORIGINS` to a comma-separated list including your client's origin.

**Search results look stale after an update.**
Indexes checkpoint to disk on `checkpoint_interval_secs` (default 5 minutes), not on
every write — this only affects on-disk durability timing, not read-after-write
consistency, since reads go through the in-memory segment first.

## License

Functional Source License, Version 1.1, ALv2 Future License (FSL-1.1-ALv2). Source is
available and free to use for any purpose except competing commercial use. Converts to
Apache License 2.0 two years after each release. See [fsl.software](https://fsl.software).
