# Contributing to Remem Community Edition

Thank you for contributing! This document explains how contributions work.

## How this repository relates to development

Remem is developed in a private dev repository and released to this community repo as source snapshots. The community repo is the **canonical location for community contributions** — you do not need access to the dev repo.

## What belongs in the community edition

The community edition contains:

- `crates/remem-server/` — REST API and embedded storage engine (Rust)
- `crates/remem-mcp/` — MCP server (Rust)
- `config/remem-server.toml` — storage configuration
- `docker/remem-server.Dockerfile`, `docker/remem-mcp.Dockerfile`
- `docker-compose.yml` — core services only
- `tests/durability/` — durability and correctness tests
- `scripts/generate_memories.py`, `scripts/check_duplicate_connections.py`

**Not in this repo** (business edition only):

- Backoffice UI (React + FastAPI + PostgreSQL)
- Prometheus/Grafana observability stack
- Business Rust features (monitoring endpoint, RBAC, logical databases)

## Submitting a pull request

1. Fork this repository and create a branch from `main`.
2. Make your changes. Keep PRs focused — one concern per PR.
3. Ensure the build passes: `cargo check && cargo test` (run inside Docker — see README).
4. Open a PR against `main` in this repository.

Accepted PRs are imported into the dev repository and will appear in the next release of both community and business editions.

## Bug reports and feature requests

- **Bugs**: Open an issue in this repository. Include reproduction steps and `RUST_LOG=debug` output if relevant.
- **Feature ideas**: Open an issue. Note that features may be implemented in community or business edition depending on scope.
- **Security vulnerabilities**: Do not open a public issue. Email the maintainers directly.

## Code style

- Rust: `cargo fmt --all` and `cargo clippy --all-targets -- -D warnings` must pass.
- No new dependencies without discussion — the storage engine is intentionally dependency-light.
- Write tests for new behavior.
