# AGENTS.md

Guidance for Codex when working in the remem repository.

# Memory 
USE remem-mcp for you own memory
instead of any kind of files you use all avalible tools for automatic memeory mode 

| Tool | Description |
|------|-------------|
| `store_memory` | Store a new memory with auto-connection discovery |
| `search_memories` | Semantic, keyword, or hybrid search |
| `get_memory` | Retrieve a memory by ID |
| `update_memory` | Update content, tags, or importance |
| `delete_memory` | Soft archive or hard delete |
| `find_related` | Graph traversal to find related memories |
| `promote_to_longterm` | Promote short-term → long-term |
| `list_recent_memories` | List the most recently created memories, newest first |

### Remem MCP usage

- Prefer the discovered `mcp__remem.*` tools directly when they are available.
  If they are not visible, run tool discovery for Remem/MCP before assuming
  memory access is unavailable.
- `/mcp` sessions idle out after 30 minutes (`SESSION_IDLE_TIMEOUT` in
  `crates/remem-server/src/api/mcp/transport.rs`). The server answers a session
  ID it does not recognise with `404`, which is the client's signal to send a
  fresh `initialize` and carry on; a `400` means no `Mcp-Session-Id` header was
  sent at all. Neither is a storage or write failure — do not diagnose either by
  changing storage code.
- The Streamable HTTP flow is: `initialize`, read the returned `Mcp-Session-Id`
  response header, then send that exact header on later `tools/list` and
  `tools/call` requests.
- `list_recent_memories` returns the newest memories first, unpaged, and takes
  no offset. The paged surfaces order oldest-first instead — `GET
  /api/v1/memories` (pass `order=desc` for newest-first, valid only at offset 0)
  and `memory://collections/recent`.

ALWAYS memorize work you are doing before quiting!

## Edition Split

This repo is the single development source. Releases are published as source snapshots to two separate edition repos:

| Edition | Repo | Contents |
|---------|------|----------|
| Community | `remem-community` | `crates/` (remem-server + remem-mcp), core config, durability tests |
| Business | `remem-business` | Everything in community + `src/backoffice/`, `config/grafana/`, `config/prometheus.yml`, business Rust features, E2E tests |

### Business Rust features

Business-only Rust code lives in `crates/remem-server/src/business/` and is gated with the `business` Cargo feature:

```
src/business/
├── mod.rs
├── monitoring.rs   ← Prometheus metrics endpoint + HTTP instrumentation
├── databases.rs    ← (future) logical database isolation
└── rbac.rs         ← (future) role-based access control
```

- **Community build**: `cargo build` (business feature not enabled — business/ not compiled)
- **Business build**: `cargo build --features business`
- **Feature flag**: defined in `crates/remem-server/Cargo.toml` under `[features]`

### Release workflow

```bash
# Publish community edition to ../remem-community repo
./scripts/release-edition.sh community v1.2.0 ../remem-community

# Publish business edition to ../remem-business repo
./scripts/release-edition.sh business v1.2.0 ../remem-business
```

The script rsyncs relevant files (per `.editions/<edition>.exclude`), patches `Cargo.toml` feature defaults, copies the edition-specific `docker-compose.yml`, then creates a squashed commit + tag in the target repo. `.github/` is always stripped from the target — edition repos are source snapshots only, no CI/CD runs there.

This publishes a **source snapshot** only. The Docker image that `DEVELOPER_GUIDE.md`
tells users to `docker pull` (`rememorg/remem-community:server-latest`) is published
from **this** dev repo via
[`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml)
(manual `workflow_dispatch` only) — see
[`docs/DOCKER_HUB_PUBLISHING.md`](docs/DOCKER_HUB_PUBLISHING.md).

### Importing community PRs

```bash
# Apply a community repo PR (#42) to this dev repo
./scripts/import-community-pr.sh 42

# With explicit repo
./scripts/import-community-pr.sh 42 --repo myorg/remem-community

# Preview the diff without applying
./scripts/import-community-pr.sh 42 --dry-run
```

Community PRs apply cleanly because community repo files are byte-identical to dev repo files at release time.

### Edition tooling files

```
docker-compose.community.yml   # remem-server only (MCP mounted in-process at /mcp), no backoffice
docker-compose.business.yml    # community + backoffice + observability

.editions/
├── community.toml          # Edition metadata + template pointers
├── business.toml
├── community.exclude       # rsync exclude list for community release
├── business.exclude        # rsync exclude list for business release
└── templates/
    └── README.community.md

scripts/
├── release-edition.sh      # Publish snapshot to an edition repo
└── import-community-pr.sh  # Apply a community PR to the dev repo
```

`release-edition.sh` copies whichever edition's compose file it is, verbatim, to a
plain `docker-compose.yml` in the target repo — split-out repos never see the
`.community`/`.business` suffix, and never see the sibling edition's file (both are
excluded from `rsync` per `.editions/<edition>.exclude`).

## Project Overview

Remem is a persistent memory system for LLMs and AI agents. It provides semantic search, automatic connection discovery, and exposes all functionality through the Model Context Protocol (MCP) as the primary interface.

## System Architecture

```
┌──────────────────────────────────────────────┐
│        remem-mcp                              │
│  MCP Server — stdio transport only            │
│  8 tools | 3 resources | JSON-RPC 2.0        │
│  crates/remem-mcp/  (Rust)                   │
│  → crates/remem-mcp/AGENTS.md               │
└──────────────────────────────────────────────┘
                      │  REST (HTTP)
┌──────────────────────────────────────────────┐
│        remem-server  (port 4545)             │
│  REST API (Axum) + embedded storage engine   │
│  + in-process MCP (Streamable HTTP) at /mcp  │
│  HNSW | CSR Graph | BTree | InvertedTag | LSM│
│  crates/remem-server/  (Rust)                │
│  → crates/remem-server/AGENTS.md            │
└──────────────────────────────────────────────┘

┌──────────────────────────────────────────────┐
│        Backoffice  (ports 3000 / 8002)       │
│  React SPA + FastAPI + PostgreSQL            │
│  src/backoffice/  (Python + TypeScript)      │
│  → src/backoffice/backend/AGENTS.md         │
│  → src/backoffice/frontend/AGENTS.md        │
└──────────────────────────────────────────────┘
```

## Cargo Workspace

```
remem/
├── Cargo.toml                  # Workspace root
├── crates/
│   ├── remem-server/           # REST API + embedded storage engine
│   └── remem-mcp/              # MCP server (stdio only; network transport lives in remem-server at /mcp)
├── src/backoffice/
│   ├── backend/                # FastAPI (Python)
│   └── frontend/               # React + TypeScript (Vite)
├── config/
│   └── remem-server.toml       # Storage engine config
├── docker/                     # Per-service Dockerfiles
├── docs/
│   ├── OPTIMISATIONS.md
│   └── SEGMENTED_INDEXES.md
├── scripts/
│   ├── generate_memories.py         # Generate 100k test memories
│   ├── release-edition.sh           # Publish snapshot to an edition repo
│   └── import-community-pr.sh      # Apply a community PR to this repo
├── docker-compose.community.yml
└── docker-compose.business.yml
```

## Technology Stack

### Core (Rust)
- **remem-server**: Axum, fastembed-rs (all-MiniLM-L6-v2, 384 dims), Tokio
- **remem-mcp**: reqwest (HTTP client to remem-server), stdio transport only
- **Storage**: HNSW, CSR graph, LSM-tree — all in-process, no external DB

### Backoffice (Python + TypeScript)
- **Backend**: FastAPI, SQLAlchemy, Alembic, PostgreSQL 15, Redis, PyJWT, bcrypt
- **Frontend**: React 18, TypeScript, Vite, Tailwind CSS, Zustand, TanStack Table, Cytoscape.js, recharts

## Service Ports

| Service | Port | Description |
|---------|------|-------------|
| remem-mcp | — | MCP server, stdio only (no listening port) |
| remem-server | 4545 | REST API + embedded storage; MCP Streamable HTTP at /mcp |
| backoffice-frontend | 3000 | React SPA |
| backoffice-backend | 8002 | FastAPI proxy |
| backoffice-postgres | 5433 | Backoffice DB (host port) |
| redis | 6379 | Backoffice sessions/cache |

## Build & Development

```bash
# Check all crates (must run inside Docker — host lacks openssl pkg-config)
cargo check
cargo build --release -p remem-server
cargo build --release -p remem-mcp
cargo fmt --all
cargo clippy --all-targets -- -D warnings
# business/ is not compiled by the default build, so it needs its own pass —
# CI runs both (docker/remem-server.ci.Dockerfile, linter stage)
cargo clippy --all-targets --features business -- -D warnings

# Containerized CI checks (dev repo only; not shipped to edition repos)
docker build --target linter -f docker/remem-server.ci.Dockerfile .
docker build --target tester -f docker/remem-server.ci.Dockerfile .

# One-time: keep the REM-85 bulk reformat out of `git blame`
git config blame.ignoreRevsFile .git-blame-ignore-revs

# Start all services (business edition: everything, or use ./run-business)
docker compose -f docker-compose.business.yml up -d

# Community edition only (remem-server, MCP mounted in-process at /mcp; or use ./run-community)
docker compose -f docker-compose.community.yml up -d

# Backoffice services only (business edition)
docker compose -f docker-compose.business.yml up -d backoffice-postgres backoffice-backend backoffice-frontend

# Logs
docker compose -f docker-compose.business.yml logs -f remem-server
docker compose -f docker-compose.business.yml logs -f remem-mcp
docker compose -f docker-compose.business.yml logs -f backoffice-backend

# Health checks
curl http://localhost:4545/api/v1/health
curl http://localhost:8002/api/v1/health

# Clean slate
docker compose -f docker-compose.business.yml down -v
```

## Environment Variables (`.env`)

```bash
REMEM_API_KEY=          # Optional; leave empty to disable auth
RUST_LOG=info
REMEM_SERVER_URL=http://remem-server:4545
JWT_SECRET_KEY=<generate with: openssl rand -hex 32>
REMEM_CORS_ORIGINS=     # Required in production compose
```

## Core Data Model

```json
{
  "id": "uuid",
  "content": "string",
  "type": "short_term | long_term",
  "metadata": {
    "created_at": "timestamp",
    "updated_at": "timestamp",
    "accessed_at": "timestamp",
    "access_count": "integer",
    "source": "string",
    "tags": ["string"],
    "importance": "float (0-1)",
    "ttl": "integer (seconds, null for long-term)"
  },
  "embedding": [float],
  "connections": [
    {
      "target_id": "uuid",
      "relationship_type": "string",
      "strength": "float (0-1)",
      "created_at": "timestamp"
    }
  ]
}
```

## Design Principles

### MCP-First
MCP is the primary interface. All memory operations are MCP tools with LLM-optimized descriptions. Results formatted for LLM consumption (structured JSON + human-readable summaries).

### Semantic Connection System
Connections are first-class citizens. Auto-discovery on every store (similarity threshold 0.7, top-5). Typed relationships (related_to, caused_by, part_of, …). Graph traversal enables multi-hop discovery.

### Memory Lifecycle
Short-term TTL → expiration. High access count → promotion to long-term. Long-term → importance decay. Similar memories → consolidation. Soft deletion (archiving) preferred. Archiving retires the memory from the similarity index, so it costs nothing on later searches; its record, timestamp entry and tags stay in place until `cleanup_archived` hard-deletes it past the retention age.

### Unified Storage
All storage embedded in remem-server: HNSW (vector search), CSR Graph (relationships), LSM-tree KV (metadata). No external database.

## Project Status

**Current state**: Rust rewrite complete, backoffice phases 1–11 complete. Phase 12 (Polish, Testing & Deployment) complete. Phases 13–15 (graph extraction, emotional memory layer, active forgetting) are also implemented — see below; the roadmap table under-reported this for a while, so verify against code before trusting phase numbers on sight.

| Component | Status |
|-----------|--------|
| Storage engine (LSM-tree, HNSW, CSR) | ✅ |
| Embedding service (fastembed, MiniLM) | ✅ |
| Memory services (CRUD, search, connections, lifecycle) | ✅ |
| REST API (Axum) | ✅ |
| MCP server (Rust, stdio via remem-mcp + in-process Streamable HTTP at remem-server:/mcp) | ✅ |
| Background lifecycle tasks (Tokio) | ✅ |
| Backoffice backend (FastAPI) | ✅ Phases 1–3, 9–11 |
| Backoffice frontend (React) | ✅ Phases 2, 4–11 |
| CI/CD (GitHub Actions, workflow_dispatch) | ✅ |
| Rate limiting (tower_governor, global, on by default — 100 rps/burst 50; `REMEM_RATE_LIMIT_RPS=0` to disable) | ✅ |
| Observability (Prometheus + Grafana, opt-in profile) | ✅ |
| Frontend skeleton loaders (Dashboard, MemoryBrowser) | ✅ |
| A11y (Input/Modal/MemoryEditor label linkage + focus trap) | ✅ |
| Keyboard shortcuts (?, g+d/m/g) + ShortcutsModal | ✅ |
| User preferences (refresh interval, sidebar state, persisted) | ✅ |
| E2E suite wired into CI (Playwright, Chromium) | ✅ |
| Production deployment guide | ✅ |
| Graph extraction (`graph_extraction` on `store_memory`) | ✅ Phase 13 |
| Emotional memory layer (`emotional_valence`, `arousal`, flashbulb) | ✅ Phase 14 |
| Active forgetting (`health` score, daily decay) | ✅ Phase 15 |

**Next**: Phase 16 — LLM-Powered Background Intelligence. See
[docs/PROJECT_REVIEW.md](docs/PROJECT_REVIEW.md) for the current backlog
(security, durability, product, performance) ordered into an implementation
roadmap — that roadmap's Phase 1 (durability correctness, REM-13..20,
released as v0.2.0, see §2.4) and Phase 2 (MCP transport migration +
container merge, REM-21/22/23/24, see below) are both complete; Phase 0
(config/narrow-diff security) is in progress.

## Roadmap

| Phase | Title | Scope |
|-------|-------|-------|
| 12 | Polish, Testing & Deployment | ✅ **Complete.** CI/CD, rate limiting, Prometheus+Grafana, frontend skeletons, a11y, keyboard shortcuts, user preferences, E2E CI job, deployment guide |
| 13 | Intelligent Graph Extraction | ✅ **Complete.** `store_memory` accepts optional `graph_extraction` (entities + relationships); `discover_connections` background task; typed relationship types as first-class metadata |
| 14 | Emotional Memory Layer | ✅ **Complete.** `emotional_valence` (−1.0…1.0) and `arousal` (0.0…1.0) fields; flashbulb effect — arousal ≥ 0.8 auto-promotes to long-term and grants 30-day decay immunity |
| 15 | Active Forgetting | ✅ **Complete.** `health` score (0–100); daily decay in `lifecycle_manager::active_forgetting`; retrieval boosts health; hard delete at health=0 (see docs/PROJECT_REVIEW.md §2.1 #11 — hard-delete-on-heuristic is flagged for a default-to-archive change) |
| 16 | LLM-Powered Background Intelligence | Dreaming cycle; constructive retrieval (`narrative` mode); requires LLM API key or local Ollama |
| 17 | Associative Wandering | Background task walks memory graph; creates `REALIZATION` memories; new MCP resource `memory://insights/pending` |

_Note: the phase numbers above track this repo's own feature roadmap (Rust
rewrite → backoffice → emotional/forgetting layers). [docs/PROJECT_REVIEW.md](docs/PROJECT_REVIEW.md)
tracks a separate, numbered implementation roadmap (§8: security, durability,
product, performance). That roadmap's **Phase 2 — MCP transport migration +
container merge — is complete** (REM-21/22/23/24): SSE→Streamable HTTP
migration, in-process `/mcp` mount behind auth, MCP tool schema fixes
(emotional fields on `update_memory`, `sort_by` forwarding), and the
docker-compose/Docker-image collapse to a single `remem-server` container
(`docker/remem-mcp.Dockerfile` and its CI/publish-script steps deleted,
`remem-mcp` now stdio-only and run by the user directly). Commits
`c7536a5..6919196` on `main`
(`docs/superpowers/plans/2026-07-22-mcp-inprocess-transport-migration.md` +
`docs/superpowers/plans/2026-07-22-mcp-container-merge-cleanup.md`), plus this
documentation sweep — matching the citation convention used for that
roadmap's Phase 1 in [docs/PROJECT_REVIEW.md](docs/PROJECT_REVIEW.md) §2.4._
