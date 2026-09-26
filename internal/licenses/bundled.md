The container image carries five things that are not Go modules and therefore
cannot be read out of the module graph. They are listed here by hand, and the
image ships the licence files themselves under `/usr/local/share/remem/licenses/`.

### 2.1 The Go standard library

Compiled into `remem`, `remem-mcp` and `remem-admin`. BSD-3-Clause, The Go
Authors. The image carries this text at
`/usr/local/share/remem/licenses/go/LICENSE`, copied from the toolchain that
built the binaries, together with the Go project's additional patent grant at
`PATENTS` beside it — the same grant reproduced in section 3 for the
`golang.org/x` modules, byte for byte.

```text
Copyright 2009 The Go Authors.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of Google LLC nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

### 2.2 ONNX Runtime

Fetched at image build time (v1.29.0, pinned with the `onnxruntime_go` binding)
and installed as a shared library the server links against. MIT, Microsoft
Corporation.

ONNX Runtime has its own third-party notices — 337 KB of them, for the libraries
it in turn builds on. They are not reproduced here; the image carries them
verbatim at `/usr/local/share/remem/licenses/onnxruntime/ThirdPartyNotices.txt`,
beside `LICENSE`.

```text
MIT License

Copyright (c) Microsoft Corporation

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### 2.3 The tokenizer's static archive

`libtokenizers.a`, fetched at image build time from `daulet/tokenizers`
v1.27.0 and linked into `remem`. The Go binding is MIT and appears in section 1
as `github.com/daulet/tokenizers`; the archive itself is compiled Rust, built
from HuggingFace `tokenizers` (Apache-2.0, HuggingFace Inc.) and its own
dependency tree.

**This is a known gap.** The published archive contains no licence file and the
Rust dependencies it was built from cannot be enumerated from the artefact, so
the transitive notices for that tree are not reproduced. Every crate involved is
MIT or Apache-2.0 — there is no copyleft exposure — but the attribution is
incomplete. Closing it means building the archive from source rather than
fetching it, or asking upstream to publish notices with the release.

### 2.4 The embedding model

`all-MiniLM-L6-v2`, baked into the image at `/opt/remem/model` from the
`Qdrant/all-MiniLM-L6-v2-onnx` repository. Apache-2.0, declared in the model
card; the repository publishes no `LICENSE` file, so there is no file to carry.
The weights derive from `sentence-transformers/all-MiniLM-L6-v2`, also
Apache-2.0. The Apache License 2.0 text is reproduced in section 3.

### 2.5 The base image

`debian:bookworm-slim`, with `ca-certificates` and `curl` installed. Debian's
packages carry their own copyright files inside the image at
`/usr/share/doc/*/copyright`, which is where a Debian recipient expects to find
them; they are not duplicated here.
