# Build remem-server from the local workspace
FROM rust:1.85-slim AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    curl \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Download ONNX Runtime 1.18.1 (required by ort-sys 2.0.0-rc.4) so the build
# script uses this local copy instead of trying to download it itself.
RUN mkdir -p /ort-libs && \
    curl -fsSL https://github.com/microsoft/onnxruntime/releases/download/v1.18.1/onnxruntime-linux-x64-1.18.1.tgz \
        -o /tmp/onnxruntime.tgz && \
    tar -xzf /tmp/onnxruntime.tgz -C /tmp && \
    cp /tmp/onnxruntime-linux-x64-1.18.1/lib/libonnxruntime.so.1.18.1 /ort-libs/ && \
    ln -sf libonnxruntime.so.1.18.1 /ort-libs/libonnxruntime.so && \
    rm -rf /tmp/onnxruntime*

# Copy workspace manifests first for layer caching
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY crates/remem-mcp/Cargo.toml crates/remem-mcp/
COPY crates/remem-server/Cargo.toml crates/remem-server/

# Create stub source files so `cargo fetch` can resolve all dependencies.
RUN mkdir -p crates/remem-mcp/src crates/remem-server/src crates/remem-server/src/bin && \
    echo "fn main() {}" > crates/remem-mcp/src/main.rs && \
    echo "fn main() {}" > crates/remem-server/src/main.rs && \
    echo "fn main() {}" > crates/remem-server/src/bin/download_model.rs && \
    cargo fetch

# Copy actual source code and proto files
COPY crates/ crates/

ENV ORT_LIB_LOCATION=/ort-libs

# ── Lint stage (used by CI / docker build --target linter) ───────────────────
FROM builder AS linter
RUN cargo fmt --all -- --check
RUN cargo clippy --all-targets -- -D warnings

# ── Test stage (used by CI / docker build --target tester) ───────────────────
FROM builder AS tester
ENV LD_LIBRARY_PATH=/ort-libs
RUN cargo test -p remem-server

# ── Binary build stage ────────────────────────────────────────────────────────
FROM builder AS release-builder
# EDITION_FEATURES controls which Cargo features are compiled in.
# Empty string (default) → community edition (no business features).
# "business" → business edition (Prometheus metrics, RBAC, logical DBs, ...).
# Passed via docker-compose build args or: docker build --build-arg EDITION_FEATURES=business
ARG EDITION_FEATURES=""
# ORT_LIB_LOCATION tells ort-sys to use our pre-downloaded ONNX Runtime instead
# of downloading it again during the build script.
RUN cargo build --release -p remem-server ${EDITION_FEATURES:+--features $EDITION_FEATURES}

# ── Model download stage ──────────────────────────────────────────────────────
# Use Python huggingface_hub to download the embedding model in the standard
# hf-hub cache format, which is compatible with the Rust hf-hub crate used by
# fastembed. This avoids running the Rust binary during build (which fails in
# some network environments).
FROM python:3.11-slim AS model-downloader

RUN pip install --no-cache-dir "huggingface_hub>=0.20"

# fastembed 3.14.1: EmbeddingModel::AllMiniLML6V2 → Qdrant/all-MiniLM-L6-v2-onnx
# NOTE: Python multiline strings can't be used here because Docker parses
# "from ..." at line start as a Dockerfile FROM instruction (case-insensitive).
RUN python3 -c "from huggingface_hub import snapshot_download; print('Downloading Qdrant/all-MiniLM-L6-v2-onnx ...'); snapshot_download('Qdrant/all-MiniLM-L6-v2-onnx', cache_dir='/fastembed-cache'); print('Done.')"

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 remem && \
    mkdir -p /var/lib/remem /etc/remem /var/lib/fastembed && \
    chown -R remem:remem /var/lib/remem /var/lib/fastembed

COPY docker/remem-server-entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Copy the ONNX Runtime shared library (needed at runtime for embedding inference)
COPY --from=release-builder /ort-libs/libonnxruntime.so.1.18.1 /usr/local/lib/
RUN ln -sf /usr/local/lib/libonnxruntime.so.1.18.1 /usr/local/lib/libonnxruntime.so && \
    ldconfig

# Copy pre-downloaded embedding model (Python hf-hub cache format is compatible
# with Rust hf-hub, so fastembed finds the model without any network access)
COPY --from=model-downloader /fastembed-cache /var/lib/fastembed
RUN chown -R remem:remem /var/lib/fastembed

COPY --from=release-builder /workspace/target/release/remem-server /usr/local/bin/remem-server

# Point fastembed at the pre-baked model cache
ENV FASTEMBED_CACHE_PATH=/var/lib/fastembed

EXPOSE 8001
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8001/api/v1/health || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["remem-server", "--config", "/etc/remem/config.toml"]
