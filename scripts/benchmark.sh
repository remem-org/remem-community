#!/usr/bin/env bash
# Run Remem's benchmarks (bench/) and optionally compare them with a saved
# baseline, the way Rust Remem's scripts/benchmark-in-container.sh does with
# Criterion baselines.
#
#   scripts/benchmark.sh                          run and print
#   scripts/benchmark.sh --save-baseline main     run and keep as "main"
#   scripts/benchmark.sh --baseline main          run and compare with "main"
#
# A number from a benchmark is a statement about this host, this filesystem and
# this revision. Compare a change against its merge base on the same host, not
# against a figure recorded somewhere else. docs/BENCHMARKS.md has the method.
#
# Environment:
#   REMEM_BENCHMARK_DATA_DIR      an empty persistent directory (never /tmp);
#                                 default: fresh directories under .benchmark-data/
#   REMEM_BENCHMARK_TOTAL_WRITES  creates per write-path operation, a positive
#                                 multiple of 50 (default 1000)
#   REMEM_BENCHMARK_COUNT         runs of each benchmark (default 10)
#   REMEM_ONNX_LIBRARY_PATH       ONNX Runtime; default .tools/onnxruntime/lib
#   REMEM_EMBEDDING_MODEL_PATH    the model; default .models/all-MiniLM-L6-v2
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS="$REPO_ROOT/.benchmark-data/results"
# golang.org/x/perf publishes no tags, so benchstat is pinned by pseudo-version.
BENCHSTAT="golang.org/x/perf/cmd/benchstat@v0.0.0-20260908200009-22c9c6c9d4da"
COUNT="${REMEM_BENCHMARK_COUNT:-10}"

save=""
baseline=""
while [ $# -gt 0 ]; do
    case "$1" in
        --save-baseline) save="${2:?--save-baseline needs a name}"; shift 2 ;;
        --baseline) baseline="${2:?--baseline needs a name}"; shift 2 ;;
        -h|--help) sed -n '2,24p' "$0"; exit 0 ;;
        *) echo "error: unknown argument $1" >&2; exit 2 ;;
    esac
done

case "${REMEM_BENCHMARK_DATA_DIR:-}" in
    /tmp|/tmp/*)
        echo "error: REMEM_BENCHMARK_DATA_DIR must be persistent storage, not /tmp." >&2
        exit 2
        ;;
esac

if [ -n "$baseline" ] && [ ! -f "$RESULTS/$baseline.txt" ]; then
    echo "error: no baseline named $baseline in $RESULTS; save one first with --save-baseline $baseline." >&2
    exit 2
fi

library="${REMEM_ONNX_LIBRARY_PATH:-$REPO_ROOT/.tools/onnxruntime/lib/libonnxruntime.so}"
model="${REMEM_EMBEDDING_MODEL_PATH:-$REPO_ROOT/.models/all-MiniLM-L6-v2}"
if [ ! -f "$library" ] || [ ! -d "$model" ]; then
    echo "error: the write-path benchmark needs the real model and ONNX Runtime." >&2
    echo "Run make model, and see CLAUDE.md for fetching ONNX Runtime on Linux." >&2
    exit 1
fi

mkdir -p "$RESULTS"
out="$(mktemp "$RESULTS/run.XXXXXX.txt")"
revision="$(git -C "$REPO_ROOT" rev-parse --short HEAD)"
dirty=""
git -C "$REPO_ROOT" diff --quiet || dirty=" (with uncommitted changes)"
echo "==> remem benchmarks at $revision$dirty, count $COUNT" >&2
echo "==> $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //'), kernel $(uname -r)" >&2

export CGO_ENABLED=1
export CGO_LDFLAGS="-L$REPO_ROOT/.tools/lib"
export REMEM_ONNX_LIBRARY_PATH="$library"
export REMEM_EMBEDDING_MODEL_PATH="$model"

cd "$REPO_ROOT"
# The write path is one operation of 1,000 synced creates; the default one-second
# benchtime would run it once per count. Ten operations per count averages over
# ten, where Rust's Criterion takes a hundred samples.
env -u REMEM_API_KEY go test -tags onnx -run '^$' -bench 'WritePath' -benchtime=10x -count "$COUNT" ./bench/ | tee "$out"
env -u REMEM_API_KEY go test -tags onnx -run '^$' -bench 'VectorRetrieval|Listing|KeywordSearch' -count "$COUNT" ./bench/ | tee -a "$out"

if [ -n "$save" ]; then
    cp "$out" "$RESULTS/$save.txt"
    echo "==> saved as baseline $save ($RESULTS/$save.txt)" >&2
fi
if [ -n "$baseline" ]; then
    echo "==> compared with baseline $baseline" >&2
    go run "$BENCHSTAT" "$RESULTS/$baseline.txt" "$out"
fi
