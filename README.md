# Remem Community

Remem is a persistent memory engine for LLM applications and agents. It stores
memories, retrieves them with semantic, keyword, or hybrid search, discovers
relationships, and applies lifecycle policies over time. HTTP and MCP are the
primary interfaces, with a stdio MCP bridge and administration CLI also
included.

Community provides the complete single-node memory engine: vector search,
graph relationships, hybrid search, memory lifecycle, MCP and HTTP API access,
local persistence, backup and restore, and the shared core data formats and
protocols. Client SDKs are part of the Community interface roadmap. The current
server has tenant-aware storage and APIs; the supported Community deployment
model is intended to be single-tenant. The proposed edition boundary is
documented in `docs/EDITION_BOUNDARY.md`.

## Run it

```bash
mkdir -p ./remem-data && sudo chown 10001:10001 ./remem-data

REMEM_SERVER_API_KEY='replace-with-a-long-random-secret' \
  docker compose up --build -d
```

The first line matters on Linux: the corpus is a host directory, and Compose
creates a missing one as root, which the container's unprivileged user cannot
write to. Docker Desktop on macOS and Windows does not need it.

The API is at `http://localhost:4545/api/v1` and MCP at `http://localhost:4545/mcp`,
both behind the API key as a bearer token.

The corpus lives on the host, in the directory `REMEM_DATA_DIR` names --
`./remem-data` beside the compose file by default. Set it to an absolute path
in `.env` for a managed server, and create the directory owned by uid 10001
first, which is the unprivileged user the container runs as:

```bash
mkdir -p /srv/remem/data && sudo chown 10001:10001 /srv/remem/data
```

Because it is a bind mount rather than a Docker-managed volume, the corpus is
backed up with ordinary host tools and `docker compose down -v` does not reach
it. [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) has the path-resolution rules,
the backup procedure, and how to migrate a deployment that still keeps its data
in the old `remem-data` named volume.

```bash
KEY='replace-with-a-long-random-secret'
curl -s -X POST -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"content":"The staging database moved to eu-west-2 in March"}' \
  http://localhost:4545/api/v1/memories

curl -s -X POST -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"query":"where is the staging database","search_type":"semantic","limit":5}' \
  http://localhost:4545/api/v1/memories/search
```

`search_type` is required: `semantic` ranks by meaning, `keyword` matches terms,
and `hybrid` combines both by rank. `GET /api/v1/health` reports the version
and edition.

## Build from source

The server links ONNX Runtime and a tokenizer through cgo. Docker is the
supported build. For a native build, first fetch the model and runtime assets:

```bash
make model
CGO_ENABLED=1 CGO_LDFLAGS="-L$(pwd)/.tools/lib" \
  go build -tags onnx -o remem ./cmd/remem
```

`make build test` builds and tests this edition.

## Included tools

| Binary | Purpose |
|---|---|
| `remem` | HTTP server and MCP endpoint |
| `remem-mcp` | MCP over stdio |
| `remem-admin` | Snapshot export, import, verification, and index maintenance |

## Future editions

Remem Enterprise / Cloud is planned as a commercial offering on the same
engine. Planned capabilities include multi-tenancy, horizontal scaling,
distributed consensus, online rebalancing, high availability and failover,
RBAC / SSO / audit, an admin control plane, enterprise backup and disaster
recovery, observability, usage and billing, and SLA / support. These planned
capabilities are not part of Community today.

## License

Remem Community is licensed under the Apache License 2.0. See
[`LICENSE`](LICENSE).

Third-party notices — the copyright notices and licence texts of everything
compiled into these binaries — are in
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md). The container image carries
them at `/usr/local/share/remem/licenses/`.
