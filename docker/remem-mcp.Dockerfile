FROM rust:1.85-slim AS builder

WORKDIR /build

# Cache dependencies by copying manifests first
COPY Cargo.toml Cargo.lock ./
COPY crates/remem-mcp/Cargo.toml crates/remem-mcp/
COPY crates/remem-server/Cargo.toml crates/remem-server/

# Create dummy sources so Cargo can resolve the workspace
RUN mkdir -p crates/remem-mcp/src crates/remem-server/src && \
    echo 'fn main(){}' > crates/remem-mcp/src/main.rs && \
    echo 'fn main(){}' > crates/remem-server/src/main.rs

RUN cargo fetch

# Build only remem-mcp with real sources
COPY crates/remem-mcp/src crates/remem-mcp/src/
RUN touch crates/remem-mcp/src/main.rs && \
    cargo build --release -p remem-mcp

# Runtime stage — minimal image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/remem-mcp /usr/local/bin/remem-mcp

ENV MCP_TRANSPORT=sse
ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8000
ENV REMEM_SERVER_URL=http://remem-server:8001

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["remem-mcp"]
