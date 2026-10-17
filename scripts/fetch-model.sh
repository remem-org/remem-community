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
# Destination defaults to .models/pplx-embed-context-v1-0.6b. The tokenizer's
# static library goes to .tools/lib, which CGO_LDFLAGS points at.
set -euo pipefail

# The model. pplx-embed-context-v1-0.6b is a fixed constant of the system (plan
# §Global Constraints), not a tuning knob: a vector produced by another model is
# not comparable with the ones already stored. The revision is a commit, never a
# branch, and every file is checked against the hash recorded for that commit,
# because `main` moving under a build would change every vector it produces.
MODEL_REPO="perplexity-ai/pplx-embed-context-v1-0.6b"
MODEL_REVISION="b42df969d4d78d1840769e45c68c4e4ce763768b"

# The fp32 export. Its weights live in two external data files that ONNX Runtime
# resolves relative to the graph, so all three must stay side by side under
# onnx/. The int8-weight export (model_quantized.onnx, 706 MB) is a third the
# size and was slower on every input measured -- 6.6x on a short query -- on a
# CPU without VNNI; docs/architecture/retrieval.md has the table. The int8 that
# matters is the model's output rule, which applies to either export.
MODEL_FILES=(
  "tokenizer.json c6fb5c5bbba5fa5f8332edfb6d8aa67bd7fb3d75365b1765f108201698eaebf5"
  "config.json f73a7efb2b3b914bd6c2bf5816408dd8de62fae0b01325e852c63e89057db6a9"
  "tokenizer_config.json 29282daeff3615d49f86c9e6c6fefb3a798a516da39f9297f3fcb2a8b39e0180"
  "special_tokens_map.json d2e4dbe57bf9d97e642352ac50f12007fc28f07e43ab2a3ce42db7ca7575c351"
  "1_Pooling/config.json aa629215c1d83e73d9c51184e566f2c53456bc742f936984355a2990c8c8d046"
  "onnx/model.onnx c70e1c441888f16e6fb6c5f7f2a3ea9c6c1f626d21f7a997e8fbbdcffb1bb293"
  "onnx/model.onnx_data 8d19e288f72fc29d9fd39f431de840380b6d839ff23e70d43e4abd21cfeccb77"
  "onnx/model.onnx_data_1 3a4b762ae4e0084f81c7419e5c2d132f1592c243bbbfa18ccb20c6b2a97f5dc3"
)

# The tokenizer's prebuilt Rust static library. Pinned, because the tokenizer
# decides how text becomes token ids, and a different tokenizer produces
# different vectors from the same text.
TOKENIZERS_VERSION="${TOKENIZERS_VERSION:-v1.27.0}"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dest="${1:-$root/.models/pplx-embed-context-v1-0.6b}"
libdir="${TOKENIZERS_LIB_DIR:-$root/.tools/lib}"

mkdir -p "$dest" "$libdir"

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

# fetch downloads one file unless a copy with the pinned hash is already there.
# A present file with the wrong hash is fetched again rather than trusted: a
# half-written download from an interrupted run is exactly that file.
fetch() {
  local url="$1" out="$2" want="$3"
  mkdir -p "$(dirname "$out")"
  if [ -s "$out" ] && [ "$(sha256 "$out")" = "$want" ]; then
    echo "have ${out#"$dest"/}"
    return
  fi
  echo "fetching ${out#"$dest"/}"
  curl -fsSL --retry 3 "$url" -o "$out.part"
  local got
  got="$(sha256 "$out.part")"
  if [ "$got" != "$want" ]; then
    rm -f "$out.part"
    echo "${out#"$dest"/}: sha256 $got, want $want" >&2
    exit 1
  fi
  mv "$out.part" "$out"
}

base="https://huggingface.co/${MODEL_REPO}/resolve/${MODEL_REVISION}"
for entry in "${MODEL_FILES[@]}"; do
  f="${entry%% *}"
  fetch "$base/$f" "$dest/$f" "${entry##* }"
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
