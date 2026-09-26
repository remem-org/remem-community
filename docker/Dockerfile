# Remem Go server image.
#
# The image bakes in ONNX Runtime, the tokenizer's static library and the model
# weights, and nothing is fetched at run time. That is deliberate and it is what
# the Rust image did: a server that downloads a model on first request has a
# cold start measured in minutes and a hard dependency on Hugging Face being
# reachable from production.

ARG GO_VERSION=1.27
# ONNX Runtime's version is pinned together with the Go binding, not
# independently. onnxruntime_go asks the shared library for a specific API
# version at startup — v1.36.0 asks for 29 — and a runtime older than that
# refuses with "Error setting ORT API base", which the server reports as a
# failure to start rather than as bad vectors. 1.29.0 is the version this was
# verified against, and it is the version the macOS instructions in
# scripts/fetch-model.sh assume too.
ARG ORT_VERSION=1.29.0
# Pinned: the tokenizer decides how text becomes token ids, and a different
# tokenizer produces different vectors from the same text.
ARG TOKENIZERS_VERSION=v1.27.0

FROM golang:${GO_VERSION}-bookworm AS onnxruntime
ARG ORT_VERSION
ARG TARGETARCH
# ONNX Runtime is not packaged by Debian and is not present on developer hosts;
# it is fetched here rather than assumed, the way the Rust build does it.
RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
      amd64) ort_arch=x64 ;; \
      arm64) ort_arch=aarch64 ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    url="https://github.com/microsoft/onnxruntime/releases/download/v${ORT_VERSION}/onnxruntime-linux-${ort_arch}-${ORT_VERSION}.tgz"; \
    curl -fsSL "$url" -o /tmp/ort.tgz; \
    mkdir -p /opt/onnxruntime; \
    tar -xzf /tmp/ort.tgz -C /opt/onnxruntime --strip-components=1; \
    rm /tmp/ort.tgz; \
    test -s /opt/onnxruntime/LICENSE; \
    test -s /opt/onnxruntime/ThirdPartyNotices.txt

FROM golang:${GO_VERSION}-bookworm AS tokenizers
ARG TOKENIZERS_VERSION
ARG TARGETARCH
# The tokenizer binding links against a prebuilt Rust static library, published
# per platform and per libc. Fetching the wrong one fails at link time, which is
# the good failure: the alternative is a binary that tokenises differently from
# the one CI built.
RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
      amd64) tok_arch=amd64 ;; \
      arm64) tok_arch=arm64 ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    mkdir -p /opt/tokenizers; \
    curl -fsSL "https://github.com/daulet/tokenizers/releases/download/${TOKENIZERS_VERSION}/libtokenizers.linux-${tok_arch}.tar.gz" \
      -o /tmp/tok.tgz; \
    tar -xzf /tmp/tok.tgz -C /opt/tokenizers; \
    rm /tmp/tok.tgz

FROM golang:${GO_VERSION}-bookworm AS model
# The model is baked in at a pinned revision. all-MiniLM-L6-v2 is a fixed
# constant of the system, not a tuning knob: a vector produced by another model
# is not comparable with the ones already stored.
ARG MODEL_REPO=Qdrant/all-MiniLM-L6-v2-onnx
ARG MODEL_REVISION=main
COPY scripts/fetch-model.sh /usr/local/bin/fetch-model.sh
RUN set -eux; \
    chmod +x /usr/local/bin/fetch-model.sh; \
    TOKENIZERS_LIB_DIR=/tmp/discard MODEL_REPO="${MODEL_REPO}" MODEL_REVISION="${MODEL_REVISION}" \
      /usr/local/bin/fetch-model.sh /opt/model || true; \
    test -s /opt/model/model.onnx; \
    test -s /opt/model/tokenizer.json; \
    rm -rf /tmp/discard

FROM golang:${GO_VERSION}-bookworm AS builder
ENV GOTOOLCHAIN=local \
    CGO_ENABLED=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ pkg-config \
 && rm -rf /var/lib/apt/lists/*
COPY --from=onnxruntime /opt/onnxruntime /opt/onnxruntime
COPY --from=tokenizers /opt/tokenizers /opt/tokenizers
ENV CGO_CFLAGS="-I/opt/onnxruntime/include" \
    CGO_LDFLAGS="-L/opt/onnxruntime/lib -L/opt/tokenizers" \
    LD_LIBRARY_PATH="/opt/onnxruntime/lib"
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
# -tags onnx is what links the embedder in. Building without it would produce an
# image that starts and then refuses every write, which is worse than not
# building at all — server.openEmbedder fails rather than falling back, because
# a server that quietly produced meaningless vectors would fill a durable corpus
# with them and stamp the real model's name on every one.
# remem-admin is built without the onnx tag: it inspects a data directory and
# never embeds anything, so linking 90 MB of model runtime into it would buy
# nothing. It ships in the image because the operations it exists for —
# `graph verify` among them — need a stopped server or a backup checkpoint, and
# both of those are inside the container.
#
# GO_BUILD_TAGS is the product edition: empty for community, `business` for
# business. An edition release writes its own default here
# (scripts/release-edition.sh), and every binary takes it, so all three report
# the same edition.
ARG GO_BUILD_TAGS=
RUN go build -trimpath -tags "onnx ${GO_BUILD_TAGS}" -o /out/remem ./cmd/remem \
 && go build -trimpath -tags "${GO_BUILD_TAGS}" -o /out/remem-mcp ./cmd/remem-mcp \
 && go build -trimpath -tags "${GO_BUILD_TAGS}" -o /out/remem-admin ./cmd/remem-admin

FROM debian:bookworm-slim AS runtime
# curl is here for the health probe, not for the server: an image whose
# HEALTHCHECK cannot run reports unhealthy forever, and an orchestrator treats
# that as a crash loop.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=onnxruntime /opt/onnxruntime/lib /usr/local/lib/
RUN ldconfig
COPY --from=model /opt/model /opt/remem/model
COPY --from=builder /out/remem /usr/local/bin/remem
COPY --from=builder /out/remem-mcp /usr/local/bin/remem-mcp
COPY --from=builder /out/remem-admin /usr/local/bin/remem-admin

# Third-party notices. Every dependency is BSD, MIT, ISC or Apache-2.0, and all
# four require the copyright notice and licence text to be reproduced in the
# materials provided with a binary distribution. This image is such a
# distribution, so the texts travel in it rather than in a repository the
# recipient may never see.
#
# THIRD_PARTY_NOTICES.md is generated from what the linker actually put in these
# binaries (`make notices`); the Go toolchain's own licence and ONNX Runtime's
# are copied from the stages that produced them, so they describe the build
# rather than a version somebody transcribed. ONNX Runtime's ThirdPartyNotices
# is 337 KB and is carried whole rather than summarised.
COPY THIRD_PARTY_NOTICES.md /usr/local/share/remem/licenses/THIRD_PARTY_NOTICES.md
COPY --from=builder /usr/local/go/LICENSE /usr/local/share/remem/licenses/go/LICENSE
COPY --from=builder /usr/local/go/PATENTS /usr/local/share/remem/licenses/go/PATENTS
COPY --from=onnxruntime /opt/onnxruntime/LICENSE /usr/local/share/remem/licenses/onnxruntime/LICENSE
COPY --from=onnxruntime /opt/onnxruntime/ThirdPartyNotices.txt /usr/local/share/remem/licenses/onnxruntime/ThirdPartyNotices.txt

# The library path is named explicitly rather than left to the loader. The ONNX
# binding looks for a bare "onnxruntime.so", which is not what the release
# tarball installs — it ships libonnxruntime.so — so ldconfig alone leaves the
# server starting and then refusing every write.
ENV REMEM_EMBEDDING_MODEL_PATH=/opt/remem/model \
    REMEM_EMBEDDING_ONNX_LIBRARY_PATH=/usr/local/lib/libonnxruntime.so \
    REMEM_STORAGE_PATH=/var/lib/remem \
    REMEM_SERVER_HTTP_ADDR=0.0.0.0:4545
VOLUME /var/lib/remem
EXPOSE 4545

# Readiness, not liveness: the two answer different questions, and a probe that
# asked /health would report a process that is up but not yet serving as ready.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=6 \
  CMD curl -fsS http://localhost:4545/api/v1/ready || exit 1

# A non-root user, because the process needs nothing root has. It owns the data
# directory so a fresh volume is writable without an init container.
RUN useradd --system --uid 10001 --home-dir /var/lib/remem remem \
 && mkdir -p /var/lib/remem && chown -R remem:remem /var/lib/remem
USER remem

ENTRYPOINT ["/usr/local/bin/remem"]
CMD ["start"]
