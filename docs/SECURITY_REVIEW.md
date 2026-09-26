# Security review — Phase 13

A review of the Go implementation at the end of Phase 13, on the branch
`phase-13-parity-and-hardening`. It follows the plan's headings (Task 6.1).
Each finding has a severity, the evidence at a file and line or as a request,
and a disposition: the commit that fixed it, or why it is left open.

Severity is about this product's deployment. A memory server holds what its
agents were told, for many tenants at once, so **the worst outcome is one
tenant reading another's memories**. Next come a server made unavailable by one
request, and then a setting that does not do what it says.

## Summary

| # | Finding | Severity | Disposition |
|---|---|---|---|
| 1 | GO-2026-5970: an infinite loop in `golang.org/x/text` v0.25.0, reachable from the tokeniser | Medium (not reachable in practice) | Fixed, `cd2db41` |
| 2 | Tags, batch size and query text were bounded only by the 8 MiB request body | Medium | Fixed, `16ab26a` |
| 3 | A production server accepted a one-character API key | Medium | Fixed, `2200162` |
| 4 | `metrics.enabled` and `metrics.path` were validated and never read | Medium | Fixed, `854cecc` |
| 5 | `metrics.addr` was neither validated nor read | Medium | Fixed, `db5b2e9` |
| 6 | `storage.engine = "memory"` was accepted in production | Medium | Fixed, `5ae1eaa` |
| 7 | An MCP session is bound to its tenant, not to the credential that opened it | Low | Accepted |
| 8 | The HTTP server sets no `ReadHeaderTimeout` or `IdleTimeout` | Low | Accepted |

Nothing found lets one tenant read another's data.

## Authentication

**Keys are compared in constant time, over every key.** `auth.Keyring.Verify`
(`internal/auth/auth.go:107`) hashes the presented credential with SHA-256 and
compares it against every configured key's hash with
`subtle.ConstantTimeCompare` (`:118`). It never stops at the first match, so
response time reveals neither how many keys exist nor where one sits in the
list. The keyring holds hashes rather than secrets, so a heap dump does not
hand over the keys. Verified by reading; there is no timing test.

**A production server refuses to start without a key**
(`internal/config/validate.go:37`).

**Finding 3: a production server accepted any key length.** The plan lists a
key length floor among the checks, and there was none: a one-character key
started a production server. Rust refused a key under 16 characters or on a
placeholder list (`config.rs`, `validate_production_config`), so this was a
regression. **Fixed in `2200162`**:
- In production, every key must be at least 16 characters and not a
  placeholder: Rust's three, plus the two this repository's own README and
  compose files print (`weakSecret`, `validate.go:296`).
- The refusal names the setting and never the key.
- Development still accepts short keys, as Rust does.

**The bare-token header form is deliberate.** `bearer` (`internal/api/http/middleware.go:206`)
accepts `Authorization: Bearer <key>` and a bare `Authorization: <key>`. The
comment gives the reason: the MCP stdio client and hand-written curl both send
the bare form. It weakens nothing, because the header is the same one and the
token is checked the same way.

**`/metrics` needs a credential not bound to one tenant.** Every per-tenant
series carries a tenant label, so a scrape lists every tenant the server has
served. A tenant-bound key gets 403, in words naming the metrics.

**Findings 4 and 5: three metrics settings were not read.** Each was offered
in the configuration and ignored, so an operator who used one to close or
move the endpoint still had it on the main listener:
- `metrics.enabled` and `metrics.path` were validated and never read. Fixed
  in `854cecc`: `false` mounts no endpoint, and a path moves it.
- `metrics.addr` was neither validated nor read. Fixed in `db5b2e9`: it
  serves the endpoint on a listener of its own, removed from the main one.
  That listener keeps the credential rule and answers nothing else.

All three were decided with the user and are tested against a real
`server.New`.

**CORS.** The server sends no `Access-Control-*` headers at all, so a browser
refuses a cross-origin read by default. Rust refused permissive CORS in
production; Go has no CORS to be permissive with.

## Tenant isolation

**Nothing found lets one tenant reach another's data.** The evidence:

| What | Where |
|---|---|
| Ten tenants, 1,000 concurrent operations under `-race`, including reads of each other's ids | `TestTenantIsolationUnderConcurrency`, `internal/api/http/isolation_test.go:68` (Task 1.6) |
| Cross-tenant work goes through `tenant.ForEach` only, and the guard is proven to find a call | `TestForEachIsTheOnlyCrossTenantPath`, `TestTheForEachGuardActuallyFindsCalls`, `internal/arch/boundaries_test.go:256,326` |
| A cursor minted in one tenant is refused in another | `TestACursorDoesNotCrossTheTenantBoundary`, `internal/api/http/list_test.go:116` |
| A memory in another tenant is 404, never 403, on connections and related | `internal/api/http/connections_test.go:77,203` |
| Job listings and policy overrides are scoped | `TestAJobListingIsScopedToItsTenant`, `TestAPolicyOverrideIsScopedToItsTenant` |
| Admin and policy routes need a credential not bound to one tenant | `TestPolicyRoutesNeedACrossTenantCredential` |

**Finding 7: an MCP session is bound to its tenant, not its credential.** A
session is stored under a tenant-scoped key (`internal/session/session.go:151`),
so a request resolved to another tenant cannot find it. The credential that
opened it is recorded, for audit, and not enforced. So another key bound to the
**same** tenant, holding the session id, can use the session. **Accepted**: it
crosses no tenant boundary, the session holds no data the second key could not
read directly, and the session id is handed out only to whoever opened it. The
id is a UUIDv7 (`id.New`): a millisecond timestamp and 74 random bits, which is
not guessable in practice, though it is less than a 128-bit token would be.

## Rate limiting

**Keyed by tenant, not by IP**, and a deliberate divergence from Rust, which
keys by client IP. Behind a proxy or NAT, per-IP limiting groups unrelated
tenants and splits one tenant across addresses; a tenant is what the limit is
for. It is recorded in the plan as §II.10 row 19 (Task 6.3).

**Bounded.** The limiter holds at most 10,000 tenants and forgets the least
recently used (`internal/api/http/ratelimit.go:42`, `Allow` at `:92`), so a
caller cannot grow its map. `TestTheLimiterStaysBoundedAndForgetsIdleTenants`.

**An unauthenticated request costs a tenant nothing.** Authentication runs
before the limiter, so a refused credential is not charged to the tenant it
claimed. `TestARefusedCredentialIsNotChargedToATenant`.

**`/metrics` is not rate limited**, on purpose: a scrape refused while the
default tenant is busy is a monitoring gap at exactly the moment it matters.

## Resource bounds

Every input a caller controls, and the bound on it. **Finding 2** is the three
rows marked new: they were bounded by the request body alone. They were fixed
in `16ab26a` with limits decided with the user, and each is tested at the limit
and one past it (`TestCallerInputsAreBounded`).

| Input | Bound | Where |
|---|---|---|
| Request body | 8 MiB | `internal/api/http/router.go:122`, `middleware.go:155` |
| A memory's content | 1 MiB (Rust: 100 KB) | `memory.MaxContentBytes`, `service.go:33` |
| A memory's tags, and a filter's | **50** (new, Rust's number) | `memory.MaxTags`, `service.go:42` |
| Memories in a batch store | **1,000** (new) | `memory.MaxBatch`, `service.go:47` |
| A search query, a recall context | **64 KiB** (new) | `memory.MaxQueryBytes`, `service.go:54` |
| A search's page size | 200 (`search.max_limit`) | `config.go` `DefaultSearchMaxLimit` |
| A search's ranking depth | 2,000 (`search.max_page_depth`) | `config.go` `DefaultMaxPageDepth` |
| A recall's result count | capped at `search.max_limit`, whatever the token budget | `recall.go:123` |
| Paging sessions | 128, least recently used idle one reclaimed | `paging.go:17` |
| Graph traversal | 5 hops, 10,000 nodes | `internal/graph/traverse.go:21,25` |
| Discovery candidates for one subject | 64 | `discovery.MaxCandidates` |
| A job checkpoint | 64 KiB | `internal/jobs/checkpoint.go:39` |
| A snapshot block | 512 MiB stored and uncompressed. The body is read through a limit rather than allocated from the claimed size, and decompression stops one byte past the declared length | `internal/snapshot/reader.go:233,307` |
| Rate limiter tenants | 10,000 | `ratelimit.go:42` |
| HTTP read and write | 30 s each | `server.go:200` |

**Finding 8: no `ReadHeaderTimeout` or `IdleTimeout`.** `http.Server` sets
`ReadTimeout` and `WriteTimeout` only (`internal/server/server.go:200`). When the
other two are zero, Go uses `ReadTimeout` for both, so a slow-header client is
held for at most 30 seconds. Headers are capped at Go's default 1 MB.
**Accepted.** Setting the two explicitly would change nothing today.

**Finding 6: the in-memory storage engine was accepted in production.**
`storage.engine` takes `pebble` or `memory` in any environment
(`internal/config/validate.go:30`). The memory engine keeps nothing across a
restart. Measured in Stage 2, it also slows down super-linearly past about
5,000 memories, over 30 seconds for a batch Pebble writes in a few. A
production server on it loses every memory at its next restart, and since all
tenants share the one store, all of them share the slowdown. **Fixed in `5ae1eaa`**, decided with the user: a
production server refuses `memory` and names the setting, as it refuses a
missing key. Development and tests keep using it.

## Dependencies

`govulncheck` v1.8.0 (`golang.org/x/vuln`, BSD), over `./...`:

- **Before:** one vulnerability our code calls, **finding 1**, GO-2026-5970:
  an infinite loop on invalid UTF-8 in `golang.org/x/text` v0.25.0. It is
  reached from `text.TagTerm` and `tokenize` through `norm.Form.String`
  (`internal/text/tokenize.go:132`). It could not be reached in practice:
  JSON decoding turns invalid UTF-8 into U+FFFD, and every protobuf schema is
  proto3, which refuses it. The regression case from `x/text`'s own test did
  not hang `norm.NFC.String` here either.
- **Fixed in `cd2db41`**, decided with the user: `x/text` v0.39.0. That
  version needs Go 1.25, so the module's minimum rose from 1.23. On Go 1.27
  it also normalises with Unicode 17 tables, which changes the terms derived
  from characters Unicode 16 and 17 assigned. So the text index became a
  versioned format, and a migration queues one keyword-index rebuild per
  tenant (`docs/architecture/text.md`).
- **After:** "Your code is affected by 0 vulnerabilities." The scan still lists
  one vulnerability in an imported package and one in a required module that
  our code does not call.

## Fuzzing

Three targets, each over bytes Remem did not necessarily write. Each ran for
ten minutes on this host (Intel Xeon W-2135, 6 cores), with Go's fuzzer on
every core:

| Target | Parses | Executions | Corpus entries | Findings |
|---|---|---|---|---|
| `FuzzKeyDecoders` | keys from a damaged store | 51,680,715 | 41 | none |
| `FuzzSnapshotReader` | snapshots from a foreign or truncated file | 6,588,395 | 253 | none |
| `FuzzUnmarshalRecord` | record envelopes | 35,746,501 | 312 | none |

`make fuzz` runs each target for 30 seconds in CI, as a smoke test that
replays the corpus and searches briefly. A failure writes its input under
`testdata/fuzz/<Target>/`, which is committed with the fix. The one checked-in
input is for `FuzzKeyDecoders`, from Stage 1.

## What this review did not cover

- **Timing tests for the key comparison.** Constant time is by construction,
  and verified by reading, not measurement.
- **TLS.** The server speaks plain HTTP and expects a proxy to terminate TLS.
  Nothing in the compose files provides one.
- **The container image.** It runs as a non-root user on `debian:bookworm-slim`,
  and no image scanner was run.
