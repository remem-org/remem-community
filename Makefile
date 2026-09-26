# Remem Go — build entry points.
#
# GOTOOLCHAIN=local: build with the toolchain that is installed, never download
# one. go.mod's `go 1.25` is a minimum, not a pin.
GOFLAGS ?= -trimpath
export GOTOOLCHAIN = local

# The product edition: empty for community, `business` for business. An edition
# release sets its own default here (scripts/release-edition.sh), and CI builds
# and tests both. Nothing is business-only yet; see internal/version/edition_*.go.
GO_BUILD_TAGS ?=
TAGFLAGS = -tags "$(GO_BUILD_TAGS)"

# staticcheck tracks Go releases: 2025.1 rejects every stdlib import on Go 1.27
# with "export data version 4 is greater than maximum supported version 2".
# v0.8.1 is the version audited against this host's toolchain.
STATICCHECK ?= honnef.co/go/tools/cmd/staticcheck@v0.8.1

# Protobuf generation without protoc, which this host does not have and which
# would otherwise be a versioned binary every contributor installs by hand. buf
# compiles the schemas and protoc-gen-go writes the Go; both are Go programs,
# both are pinned here, and neither is a checked-in dependency.
BUF ?= github.com/bufbuild/buf/cmd/buf@v1.47.2
PROTOC_GEN_GO ?= google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
TOOLS := $(CURDIR)/.tools

build:
	go build $(GOFLAGS) $(TAGFLAGS) ./...

test:
	go test $(TAGFLAGS) -race -count=1 ./...

lint:
	go vet $(TAGFLAGS) ./...
	go run $(STATICCHECK) $(TAGFLAGS) ./...

# -coverpkg matters here: without it a package is credited only for the lines
# its own tests execute, so internal/storage/storagetest — whose entire purpose
# is to be run from other packages — reads as 0% and drags the total down by
# twenty points. The gate is on internal/, which is where the code is.
COVER_MIN ?= 80

cover:
	go test $(TAGFLAGS) -race -count=1 -coverprofile=cover.out -coverpkg=./internal/... ./...
	go tool cover -func=cover.out | tail -1
	@total=$$(go tool cover -func=cover.out | awk '/^total:/ {print $$3}' | tr -d '%'); \
	if [ "$$(printf '%s\n' "$$total" "$(COVER_MIN)" | sort -g | head -1)" != "$(COVER_MIN)" ]; then \
		echo "coverage $$total% is below the $(COVER_MIN)% floor"; \
		exit 1; \
	fi; \
	echo "coverage $$total% meets the $(COVER_MIN)% floor"

# The import-graph guard (Invariants 6 and 7). Runs as its own CI job so a
# boundary violation is never buried in the general test output.
arch:
	go test -count=1 ./internal/arch/...

# The vector-index recall measurements the plan's completion criteria name, at
# the sizes it names them at: 10,000 and 250,000 vectors. They take minutes, so
# they are behind a build tag and run as their own job rather than inside
# `make test` — the same reasoning as `arch` above. -race is deliberately absent:
# these measure numbers, not concurrency, and the detector triples the runtime.
recall:
	go test -tags recall -count=1 -timeout 60m -v ./internal/vector/

# The fuzz targets over every parser that reads bytes Remem did not necessarily
# write: keys from a damaged store, snapshots from a foreign implementation or a
# truncated transfer, record envelopes. `go test` alone replays only the seeds
# and the checked-in failures under testdata/fuzz; this is what searches. A
# failure writes its input to testdata/fuzz/<Target>/, and that file is
# committed with the fix as a regression case.
FUZZTIME ?= 30s

fuzz:
	go test ./internal/keys -run '^$$' -fuzz FuzzKeyDecoders -fuzztime $(FUZZTIME)
	go test ./internal/snapshot -run '^$$' -fuzz FuzzSnapshotReader -fuzztime $(FUZZTIME)
	go test ./internal/codec -run '^$$' -fuzz FuzzUnmarshalRecord -fuzztime $(FUZZTIME)

# The crash tests (spec §46.5) start a real server or runner as a child process,
# SIGKILL it at a chosen moment, and check what it left on disk. Each takes tens
# of seconds, so they are behind a build tag and run as their own CI job. -race
# is absent for the reason it is absent from `recall`: these measure what
# survives a kill, and the detector's overhead only moves where the kills land.
crash:
	go test -tags crash -count=1 -timeout 30m -v ./test/crash/

# The benchmarks in bench/: write path (real model, synced commits), vector
# retrieval, listing and keyword search. Minutes, and meaningful only against a
# baseline taken on the same host: scripts/benchmark.sh --save-baseline NAME,
# then --baseline NAME. docs/BENCHMARKS.md has the method.
bench:
	scripts/benchmark.sh

# Regenerates the Go types from proto/. The output is checked in, so that
# building Remem never requires a protobuf toolchain.
proto:
	GOBIN=$(TOOLS) go install $(PROTOC_GEN_GO)
	PATH="$(TOOLS):$$PATH" go run $(BUF) generate --path proto/record --path proto/tenant --path proto/session --path proto/schema --path proto/job --path proto/event --path proto/lifecycle
	PATH="$(TOOLS):$$PATH" go run $(BUF) generate --template buf.gen.snapshot.yaml --path proto/snapshot
	PATH="$(TOOLS):$$PATH" go run $(BUF) generate --path proto/snapshotext

# Checked-in generated code that no longer matches its schema is code nobody is
# reviewing. CI runs this so the two cannot drift.
proto-check: proto
	@if [ -n "$$(git status --porcelain -- internal/codec/pb internal/tenant/tenantkv/pb internal/session/pb internal/schema/pb internal/jobs/pb internal/events/pb internal/lifecycle/policykv/pb internal/snapshot/pb internal/snapshot/pbext)" ]; then \
		echo "generated protobuf code is out of date — run 'make proto' and commit the result:"; \
		git status --short -- internal/codec/pb internal/tenant/tenantkv/pb internal/session/pb internal/schema/pb internal/jobs/pb internal/events/pb internal/lifecycle/policykv/pb internal/snapshot/pb internal/snapshot/pbext; \
		exit 1; \
	fi
	@echo "generated protobuf code is up to date"

# Regenerates THIRD_PARTY_NOTICES.md from the modules the shipped binaries
# actually link. Every dependency is BSD, MIT, ISC or Apache-2.0, and all four
# require the copyright notice and licence text to be reproduced in the
# materials distributed with a binary — which a published image is.
#
# There is no notices-check target: TestThirdPartyNoticesAreCurrent runs inside
# `make test`, so drift fails the ordinary suite rather than a job somebody has
# to remember to add.
notices:
	go test ./internal/licenses -run TestThirdPartyNoticesAreCurrent -update-notices
	@echo "wrote THIRD_PARTY_NOTICES.md"

# Regenerates fixtures/decay_reference.json from Rust's lifecycle arithmetic.
#
# Needs rustc and nothing else — no cargo, no crate, and no change to
# remem-development, which is frozen at pre-go-freeze. The generator transcribes
# four expressions with the line numbers they came from, so a reviewer checks it
# by reading four lines rather than by trusting a build. See the file's own
# comment for what that does and does not buy.
decay-reference:
	rustc -O test/differential/decayref/main.rs -o $(TOOLS)/decayref
	$(TOOLS)/decayref > fixtures/decay_reference.json
	@echo "wrote fixtures/decay_reference.json"

# Regenerates the Rust reference corpora that test/migration imports.
#
# It replaces a target that pointed at scripts/fetch-fixtures.sh, a script that
# was never written: Phase 0 left `fixtures-v1.tar.zst` unpublished, so there
# was nothing to fetch. Generating is better than fetching anyway — the corpora
# are derived, the same seed reproduces them byte for byte against the frozen
# tree, and the pinned digests are what is worth keeping rather than 22 MB of
# bytes. The tests that read them skip, naming this target, when they are absent.
fixtures:
	./scripts/generate-fixtures.sh

# The model, the tokenizer archive and ONNX Runtime are not checked in and are
# not fetched at run time. This puts them where `make test-onnx` expects them.
model:
	./scripts/fetch-model.sh

# The embedder is behind a build tag because it links against a shared library
# and a static archive that a plain `go build` cannot find. Run `make model`
# first; ONNX Runtime itself comes from the platform (brew install onnxruntime
# on macOS, the base image in docker/Dockerfile).
ONNX_LDFLAGS ?= -L$(CURDIR)/.tools/lib
ONNX_MODEL ?= $(CURDIR)/.models/all-MiniLM-L6-v2

test-onnx:
	CGO_ENABLED=1 CGO_LDFLAGS="$(ONNX_LDFLAGS)" 	REMEM_EMBEDDING_MODEL_PATH="$(ONNX_MODEL)" 	go test -tags onnx -count=1 ./internal/embedding/... ./internal/memory/...

.PHONY: build test lint cover arch recall bench proto proto-check notices fixtures decay-reference model test-onnx
