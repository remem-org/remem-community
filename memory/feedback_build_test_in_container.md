---
name: feedback-build-test-in-container
description: Always build and run Rust tests inside Docker containers, not on the host — host lacks openssl pkg-config
metadata:
  type: feedback
---

Build and run Rust (`cargo build`, `cargo test`, `cargo check`) inside Docker containers, not on the host. The host machine lacks the necessary `pkg-config`/openssl setup.

**Why:** Host has no `openssl.pc` in pkg-config path, causing `openssl-sys` build failures. The Docker build environment has all dependencies pre-configured.

**How to apply:** Use `docker compose run --rm` with a builder service, or build a test image from the Dockerfile's builder stage. Never run `cargo` directly on the host.
