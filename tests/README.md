# Remem Tests

The project has been rewritten in Rust. All tests now live inside the Rust crates as
idiomatic `#[cfg(test)]` modules. This directory is kept for future end-to-end tests.

## Test Locations

| Old Python file | Rust equivalent |
|---|---|
| `tests/unit/test_memory.py` | `crates/remem-server/src/services/types.rs` (unit tests) |
| `tests/unit/test_connection.py` | `crates/remem-server/src/services/types.rs` (RelationshipType tests) |
| `tests/unit/test_search.py` | `crates/remem-server/src/services/types.rs` (SearchType + helper tests) |
| `tests/unit/test_config.py` | `crates/remem-server/src/config.rs` (unit tests) |
| `tests/integration/test_api_integration.py` | `crates/remem-server/src/api/tests.rs` (integration, `#[ignore]`) |
| `tests/integration/test_mcp_integration.py` | `crates/remem-mcp/src/protocol.rs` (unit tests) |

## Running Tests

```bash
# Unit tests (fast, no dependencies)
cargo test

# Integration tests (require fastembed ONNX model on disk)
FASTEMBED_CACHE_PATH=/var/lib/fastembed cargo test -- --ignored

# Single crate
cargo test -p remem-server
cargo test -p remem-mcp

# Specific test
cargo test memory_type_display
```

## Test Categories

### Unit tests (always run)

These test pure logic with no I/O or external dependencies:

- **`crates/remem-server/src/services/types.rs`** — `MemoryType`, `RelationshipType`,
  `SearchType` serialization; `distance_to_score`, `memory_key`, `ms_to_dt`, `now_ms`
  helpers; `StoredMemory::is_expired` and `::into_api` conversions.

- **`crates/remem-server/src/services/memory_manager.rs`** — `matches_filters` logic
  covering all filter combinations (type, importance range, tags, time range, combined).

- **`crates/remem-server/src/config.rs`** — Default values, TOML file loading, CLI
  argument overrides, partial TOML with default fallback, error cases.

- **`crates/remem-mcp/src/protocol.rs`** — JSON-RPC 2.0 request/response serialization,
  notification detection, error code constants, MCP tools list structure.

### Integration tests (`#[ignore]` — require embedding model)

These test the full HTTP API with a real storage engine and embedding service.
They are skipped by default because they require the fastembed ONNX model.

- **`crates/remem-server/src/api/tests.rs`** — Full Axum router tests: health, stats,
  memory CRUD (create/get/update/delete), semantic/keyword/hybrid search, authentication,
  lifecycle (promote), and connection graph traversal.

## End-to-End / MCP Scenario Tests

For LLM-in-the-loop E2E testing (equivalent to the old `tests/mcp/` scenarios), use the
Docker Compose stack with a real MCP client:

```bash
docker compose up -d
# Then connect Claude Desktop or any MCP client to port 8000
```

The old Python MCP scenario tests (`store_memory`, `search_memories`) verified LLM
tool-call behaviour. These are best covered by connecting an actual LLM client to the
running MCP server.
