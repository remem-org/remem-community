#!/usr/bin/env bash
# Fetches everything the ONNX embedder needs, at pinned versions, into paths
# that are gitignored.
#
# Nothing is downloaded at runtime. The Rust implementation let fastembed pull
# the model on first use and then baked it into its Docker image to stop that
# happening in production; Go does only the second half. A server that
# downloads a model on first request has a cold start measured in minutes and a
# hard dependency on Hugging Face being reachable from production.
#
#   scripts/fetch-model.sh [destination]
#
# Destination defaults to .models/all-MiniLM-L6-v2. The tokenizer's static
# library goes to .tools/lib, which CGO_LDFLAGS points at.
set -euo pipefail

# The model. all-MiniLM-L6-v2 is a fixed constant of the system (plan §Global
# Constraints), not a tuning knob: a vector produced by another model is not
# comparable with the ones already stored.
MODEL_REPO="${MODEL_REPO:-Qdrant/all-MiniLM-L6-v2-onnx}"
MODEL_REVISION="${MODEL_REVISION:-main}"

# The tokenizer's prebuilt Rust static library. Pinned, because the tokenizer
# decides how text becomes token ids, and a different tokenizer produces
# different vectors from the same text.
TOKENIZERS_VERSION="${TOKENIZERS_VERSION:-v1.27.0}"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dest="${1:-$root/.models/all-MiniLM-L6-v2}"
libdir="${TOKENIZERS_LIB_DIR:-$root/.tools/lib}"

mkdir -p "$dest" "$libdir"

fetch() {
  local url="$1" out="$2"
  if [ -s "$out" ]; then
    echo "have $(basename "$out")"
    return
  fi
  echo "fetching $(basename "$out")"
  curl -fsSL --retry 3 "$url" -o "$out.part"
  mv "$out.part" "$out"
}

base="https://huggingface.co/${MODEL_REPO}/resolve/${MODEL_REVISION}"
for f in model.onnx tokenizer.json config.json special_tokens_map.json tokenizer_config.json; do
  fetch "$base/$f" "$dest/$f"
done

# The static library is per platform and per libc. Getting this wrong fails at
# link time, which is the good failure: the alternative would be a binary that
# tokenises differently from the one CI built.
case "$(uname -s)" in
  Darwin) os="darwin" ;;
  Linux)  os="linux" ;;
  *) echo "unsupported operating system $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  arm64|aarch64) arch="arm64" ;;
  x86_64|amd64)  arch="amd64" ;;
  *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac
if [ "$os" = "linux" ] && [ "${TOKENIZERS_MUSL:-0}" = "1" ]; then
  os="linux-musl"
fi

if [ ! -s "$libdir/libtokenizers.a" ]; then
  echo "fetching libtokenizers.a ($os-$arch, $TOKENIZERS_VERSION)"
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  curl -fsSL --retry 3 \
    "https://github.com/daulet/tokenizers/releases/download/${TOKENIZERS_VERSION}/libtokenizers.${os}-${arch}.tar.gz" \
    -o "$tmp/lib.tar.gz"
  tar -xzf "$tmp/lib.tar.gz" -C "$tmp"
  mv "$tmp/libtokenizers.a" "$libdir/libtokenizers.a"
else
  echo "have libtokenizers.a"
fi

cat <<EOF

Model at   $dest
Tokenizer  $libdir/libtokenizers.a

Set these to build and run the ONNX embedder:

  export REMEM_EMBEDDING_MODEL_PATH="$dest"
  export CGO_LDFLAGS="-L$libdir"
EOF
