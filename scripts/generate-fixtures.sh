#!/usr/bin/env bash
#
# Regenerates the Rust reference corpora that test/migration compares against.
#
# The corpora are not committed. They are 22 MB in total and they are *derived*:
# the same seed against the frozen `remem-development` tree produces them byte
# for byte, which is what makes the digests below a check rather than a
# decoration. What is committed instead is the digest list here, and a 1.4 KB
# wire test vector in internal/snapshot/rustgolden_test.go — see that file for
# why one artifact is worth committing and a corpus is not.
#
# It needs the frozen Rust tree, a Rust toolchain and protoc. It does not need
# Docker: remem-development's own CLAUDE.md says cargo cannot run on a host
# without protoc, and that note predates protoc being installed.
#
#   ./scripts/generate-fixtures.sh [--rust-dir ../remem-development] [--out ./fixtures/rust]
#
# The digests come from remem-development/docs/FIXTURES.md, which pins them at
# seed 42, and this script fails if a corpus does not reproduce one. A fixture
# that has quietly drifted is worse than no fixture: every comparison against it
# would still pass, against the wrong corpus.
set -euo pipefail

RUST_DIR="${RUST_DIR:-$(cd "$(dirname "$0")/.." && pwd)/../remem-development}"
OUT_DIR="${OUT_DIR:-$(cd "$(dirname "$0")/.." && pwd)/fixtures/rust}"
SEED="${SEED:-42}"
ORT_LIB_LOCATION="${ORT_LIB_LOCATION:-/opt/homebrew/opt/onnxruntime/lib}"

while [ $# -gt 0 ]; do
  case "$1" in
    --rust-dir) RUST_DIR="$2"; shift 2 ;;
    --out) OUT_DIR="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

# profile:digest, at seed 42, from remem-development/docs/FIXTURES.md.
PROFILES=(
  "tiny:f01bb1e7913bd0f19b81772bd8b51cbbf04fe0c5ec654ae71a07983b61fd673b"
  "pathological:f871e5e37caf051d484bbf10d35df497cd3302c8b68b80bb65b47e71f43f4027"
  "typical:04f0f612b3ffca37d5149e716ca9364ab71907c503b5bb7ed84c8736ea7a0f58"
)

if [ ! -d "$RUST_DIR" ]; then
  echo "the frozen Rust tree is not at $RUST_DIR." >&2
  echo "Clone remem-org/remem-development at the pre-go-freeze tag, or pass --rust-dir." >&2
  exit 1
fi
for tool in cargo protoc; do
  command -v "$tool" >/dev/null || { echo "$tool is not installed" >&2; exit 1; }
done

echo "building remem-fixture from $RUST_DIR"
( cd "$RUST_DIR" && ORT_LIB_LOCATION="$ORT_LIB_LOCATION" \
    cargo build --release -p remem-server --bin remem-fixture >/dev/null )
FIXTURE_BIN="$RUST_DIR/target/release/remem-fixture"

mkdir -p "$OUT_DIR"
for entry in "${PROFILES[@]}"; do
  profile="${entry%%:*}"
  want="${entry##*:}"
  echo "generating $profile (seed $SEED)"
  rm -rf "$OUT_DIR/$profile" "$OUT_DIR/$profile.rsnap"
  got=$(ORT_LIB_LOCATION="$ORT_LIB_LOCATION" "$FIXTURE_BIN" \
      --profile "$profile" --seed "$SEED" \
      --data-dir "$OUT_DIR/$profile" --out "$OUT_DIR/$profile.rsnap" \
      --print-digest 2>/dev/null | sed -n 's/^digest: //p')
  if [ "$got" != "$want" ]; then
    echo "  the $profile corpus does not reproduce its pinned digest:" >&2
    echo "    got  $got" >&2
    echo "    want $want" >&2
    echo "  The reference has drifted, or the seed is not 42. Nothing that compares" >&2
    echo "  against this corpus would fail; it would pass against the wrong one." >&2
    exit 1
  fi
  # The data directory is a Pebble-shaped artifact of the generator and is not
  # what anything here reads; only the snapshot is.
  rm -rf "$OUT_DIR/$profile"
  echo "  digest $got, $(wc -c < "$OUT_DIR/$profile.rsnap" | tr -d ' ') bytes"
done

echo
echo "the reference corpora are in $OUT_DIR; run: go test ./test/migration/"
