# Foundations

The cross-cutting packages every subsystem depends on: `version`, `errs`, `id`,
`clock`, `obs`, `config`, and the import-graph guard in `arch`. Spec §60 asks
these documents to explain invariants rather than APIs — what is authoritative,
what is derived, where atomicity ends, and how things fail — so that is what
this one does. Signatures are in the code.

Landed in Phase 1, before any subsystem existed to violate them.

## What is authoritative

| Concern | Authority | Everything else |
|---|---|---|
| Durable format versions | the constants in `internal/version` | the manifest on disk is data, compared against them |
| Error classification | `errs.Kind` | HTTP status codes, MCP error bodies, retry decisions — all derived from the kind |
| Time | the `clock.Clock` a caller was given | no durable code path reads `time.Now()` |
| Identity of a record | its `id.ID` | keys, edges and index entries all reference it |
| Log and metric shape | `internal/obs` | no other package constructs a logger or a registry |
| Configuration | `config.Config` after `Validate` | no subsystem reads the environment |

## Versions: four formats, one binary version

Invariant 11 separates the binary version from the active storage and cluster
compatibility versions, because a rolling upgrade runs mixed binaries against
one format. `version.Binary` is therefore a string that nothing compares: no
decision in Remem may be taken on it.

Invariant 4 versions each durable format independently — key encoding, record
envelope, snapshot format, attribute slot schema — so a change to what
`internal/attr` indexes does not force a key-encoding migration. A fifth
number, the cluster compatibility version, is zero until Phase 14.

The open policy (spec §59) is asymmetric on purpose:

- **Older or absent on disk** is accepted. That is what migrations are for, and
  absent is how a database written before a format existed presents itself.
  `Versions.NeedsMigration` answers the separate question of whether the
  migration runner has work.
- **Newer on disk is refused**, per format, at open. A newer writer may have
  written bytes this binary would misread, and silently mutating them is
  exactly what spec §59 forbids. The refusal names every offending format, both
  version numbers, and says a downgrade is never performed automatically.

Failure mode: `errs.IncompatibleVersion`, at open, before any write.

## Errors: a kind, not a status code

Spec §58 forbids exposing Pebble or etcd/raft errors through a public API.
Adapters translate; everything above them classifies with `errs.Kind`. Only
`internal/api/http` maps a kind to a status code — nothing below the API layer
knows HTTP exists.

Two rules the taxonomy encodes:

- **`Corruption` is never retryable.** Retrying cannot repair a byte. Canonical
  data is restored from a snapshot; derived indexes are rebuilt (Invariant 3).
- **An unclassified error is `Storage`.** An error that reaches us unwrapped
  came from an adapter or a dependency, which is an infrastructure failure
  until someone proves otherwise. Classification never panics.

`Conflict` is *not* retryable: recovering from a lost write means re-reading
and re-deciding, which only the caller can do.

Spec §58's "retryable distributed error" is named `Transient`, because the
package already exports `Retryable(error) bool`.

## Identifiers: both formats, forever

Spec §49 wants uncoordinated, concurrency-safe, sortable ids and asks what
happens to the ones that already exist.

- **New ids are UUIDv7.** The leading 48 bits are the creation time in
  milliseconds, so a key built from an id sorts by creation time and a
  `created_at` listing is a forward scan rather than a sort.
- **Existing ids are UUIDv4 and keep their identity.** `id.Parse` accepts any
  version. An import that renumbered a memory would orphan every edge pointing
  at it, so it does not.

`ID` is a `[16]byte` value: comparable, usable as a map key, no pointer inside.
`id.FromBytes` copies rather than aliases, because its input is normally a
slice into an iterator buffer the storage layer reuses after the next step.
`ID.Time()` returns the zero time for a v4 id rather than inventing a
plausible-looking one; code that needs a creation time for every record reads
it from the record.

## Time: injected, never ambient

Spec §48 forbids arbitrary `time.Now()` in durable business logic. A timestamp
that reaches disk — or, later, a replicated command — must come from the
`clock.Clock` its caller was given, so lifecycle decay, TTL expiry and job
leases are testable without sleeping and reproducible without a real elapsed
hour.

`clock.NewFake` moves only when a test moves it. Two properties make it usable
as a test oracle rather than merely a stub:

- `Advance` fires every deadline it passes, in chronological order, **before it
  returns**, so a test may advance and immediately assert.
- The clock reads each deadline *while that tick is delivered*, so code
  computing from `Now()` inside a tick sees the instant it was scheduled for,
  not where the `Advance` was heading.

Ticks coalesce exactly as `time.Ticker`'s do: a loop that falls behind under
the fake behaves the way it would in production. `Advance` panics on a negative
duration; `Set` is how a test moves time backwards.

This is the Go answer to a trap recorded in the project's history:
`std::time::Instant` cannot be paused while `tokio::time::Instant` can, so the
same Rust lifecycle code was deterministic under one and not the other.

## Observability: identity travels in the context

Spec §50 makes observability infrastructure, not decoration. The identifiers it
requires — tenant, node, shard, request id, job id, migration id — live in the
`context.Context` and are attached by a `slog` handler, so a line logged three
layers below an HTTP handler carries them without any signature mentioning
requests. Absent fields are omitted rather than logged empty: a field that is
always present says nothing about whether it was known.

Two rules, both enforced by source scans in `internal/obs`, both demonstrated
to fail before being trusted:

- **No package outside `internal/obs` constructs a logger.** Format, level and
  redaction are decided once.
- **Memory content is never logged.** A memory is the user's data; a log line
  is not where it lives.

Metrics cover storage, search, jobs and migrations (cluster arrives with Phase
14). The labelling rule: work done on a tenant's behalf carries a `tenant`
label — Invariant 1 reaches the metrics, not only the keyspace — while facts
about the storage engine as a physical thing (compaction, block cache, file
size) carry a `component` label, because they cannot be attributed to a tenant
without inventing the attribution.

There is no package-level registry. `Metrics` is constructed by the composition
root and passed down, so two servers, or two tests, in one process do not fight
over the same collectors.

## Configuration: explicit, inspectable, validated once

Spec §51 asks for configuration with no behaviour hidden behind
environment-specific defaults. Three consequences:

- Every default is a named constant in `internal/config`, so what an
  unconfigured Remem does is one page of reading. `config/remem.toml` writes
  them all out with their reasoning, and a test loads it, so the documentation
  cannot drift from the code.
- **Nothing reads the environment except `config.Load`.** A subsystem
  consulting `os.Getenv` would be behaviour no configuration file describes.
- Precedence is flags > environment > file > defaults, and all three sources
  converge on one string-parsing path: `"45s"` means the same thing in each,
  and every error names the key in the same `section.key` spelling all three
  use.

Unknown keys are refused from every source, and so are retired ones — by name,
with the reason they went. A typo that silently became a default, and a dial an
operator believes is in force after it stopped being read, are the same failure.

**Two names for one setting are refused too**, and that rule was bought by the
Phase 9 verification run. The legacy Rust variable names are still honoured —
`REMEM_API_KEY` and `REMEM_DATA_DIR` are in every existing deployment manifest —
which means two variables can address one setting. `readEnv` wrote both into one
map, and `os.Environ()` has no defined order, so the winner was whichever the
operating system happened to return second: a server refused its own operator
credential while both variables sat plainly visible in its environment.
Different values under two names now refuse start-up naming both; identical
values are accepted, because a deployment that set the belt and the braces is
exactly the upgrade the legacy names exist to protect.

`Validate` reports every problem at once rather than one per restart, and
refuses contradictions rather than only malformed values: a production server
with no API key, a vector dimension that disagrees with the embedding model, a
search that cannot widen, a tenant id that is not a legal key prefix. `Load`
validates, so a `Config` that comes back from it is usable.

`Redacted()` is the inspectable half: the whole configuration as sorted
`key = value` lines with secrets replaced, so "what was this process running
with" is answerable from the logs without the API key being one of the answers.

## The boundary guard

`internal/arch` has no production code. It reads the real import graph with
`go/build` — exact prefixes, not the substring search Rust Remem's
`include_str!` guards do — and fails the build when a dependency crosses a
boundary the architecture depends on:

| Dependency | Only importer | Why |
|---|---|---|
| `github.com/cockroachdb/pebble` | `internal/storage/pebble` | Invariant 6 |
| `go.etcd.io/raft` | `internal/cluster/consensus/etcdraft` | Invariant 7 |
| `onnxruntime_go`, `tokenizers` | `internal/embedding/onnx` | the embedding runtime is replaceable |
| `internal/vector/hnsw` | `internal/vector` | callers use `vector.Index` |

Plus two rules that are not about a third-party dependency:

- The domain (`record`, `memory`, `graph`, `vector`, `query`, `lifecycle`,
  `attr`, `text`, `jobs`) never imports `internal/api`. Dependencies point
  inward (spec §6).
- **The job framework runs work and must not know what the work is**:
  `internal/jobs` never imports the packages whose work it runs. Handlers are
  registered into it by the composition root. Without the rule, the framework
  would depend on every index in the system, and Phase 11's
  `internal/discovery` — which imports `jobs` in order to enqueue — would close
  a cycle the compiler would then refuse.

Test imports count. A boundary a test helper crosses is still crossed, and a
test helper is the easiest place to smuggle an adapter into the domain.

A rule naming a package that does not exist yet passes vacuously — most of them
name packages later phases create, and a guard that failed until then is a
guard someone deletes. A third test asserts the walk found packages and read
their imports, so a rename of `internal/` cannot quietly turn every rule into a
no-op.

**A guard that has never failed is a guard nobody has verified.** Every rule was
demonstrated failing before it was trusted — a temporary Pebble import in
`internal/version`, a throwaway `internal/record` importing `internal/api/http`,
an `internal/vector` import added to `internal/jobs` — and the output is recorded
in the commit that introduced each one.

## Where this is not the answer

- Key layout, record framing and the snapshot format are named here as versions
  only. Their bytes are Phase 2 and Phase 12, documented separately.
- Tenancy is a validated `tenant.default` here and nothing more. The directory,
  the resolver and `tenant.ForEach` arrive in Phase 3.
- Cluster identity (`node`, `shard`) exists in the log schema and nowhere else,
  deliberately: the shape of a log line should not change when the cluster
  arrives.
