# Hardening

What Phase 13 did to make the claims in the other documents checked rather
than asserted: fuzzing, killing real processes, isolation under load, and the
bounds on what a caller can ask for. `docs/SECURITY_REVIEW.md` is the review
this work fed.

## What is authoritative

This document holds no state. Everything here is a test, a bound or a limiter.
The one piece of durable state it touches is the fuzz corpus: a failing input
is committed under `testdata/fuzz/<Target>/` with its fix, and replayed by every
ordinary `go test` from then on.

## Fuzzing

Every parser that reads bytes Remem did not necessarily write has a target:

| Target | Parses | Why |
|---|---|---|
| `FuzzKeyDecoders` (`internal/keys`) | keys from a damaged store | A key is read back from disk and decoded, and corruption must be refused, not misread |
| `FuzzSnapshotReader` (`internal/snapshot`) | snapshots from a foreign or truncated file | A snapshot comes from another implementation or over a network |
| `FuzzUnmarshalRecord` (`internal/codec`) | record envelopes | Every read of a record goes through it |

`make fuzz` runs each for 30 seconds as a CI smoke test. The security review ran
each for ten minutes: 51.7 million, 6.6 million and 35.7 million executions, and
no findings. The snapshot reader's earlier findings are why it reads a block
body through a limit rather than allocating the claimed size: a 57-byte file
once made it allocate half a gigabyte.

## Killing a real process

**A durability claim reasoned from how the storage engine ought to behave is
not a durability claim.** Phase 10 wrote down that an unsynced event survived a
crash because it reached the write-ahead log, and a `kill -9` lost it.

The crash harness (`test/crash`, `make crash`) is the test binary acting as
both parent and child. The parent re-executes itself as a child that runs one
scenario against a real Pebble directory and reports progress on stdout. The
parent sends SIGKILL at a moment it chooses, then reopens the directory and
checks the invariants against the keyspace. No production code knows a crash
test exists.

| Test | What it proves |
|---|---|
| `TestCrashDuringWriteLeavesNoPartialRecord` | Twenty kills while sixteen clients create, rewrite and archive. After each kill the directory is consistent, and **every create acknowledged with 201 is on disk**. With syncing turned off it fails on its first kill, so the guard still bites on the group-committed path Stage 4 introduced |
| `TestCrashDuringMigrationResumes` | Ten kills of the migration runner. After each, the records carrying the migration's mark equal the count the durable cursor claims, so work and cursor land together, and it finishes with every record migrated exactly once |
| `TestCrashDuringJobResumes` | A worker killed mid-job. A second process reclaims the job once the lease lapses, resumes from the checkpoint rather than the start, and does no item before the checkpoint twice |
| `TestCrashDuringRebuildResumesAcrossAProcess` | A vector rebuild killed a fifth of the way through, finished by a process that knows only what is on disk and in the job row |
| `TestRestartPreservesEverything` | Ten thousand memories, a SIGTERM, a new process. Every memory is fetchable, findable by its keyword and found first by its own content |

## Isolation under load

`TestTenantIsolationUnderConcurrency` (`internal/api/http/isolation_test.go`)
runs ten tenants and a thousand operations, all in flight together under
`-race`. The mix covers creates, reads, all three search modes, paged listings,
updates, history, connections, and reads of *another* tenant's ids. Each memory
carries its tenant in its content and tags, so a leak is visible in any
response that contains one.

## Rate limiting

**One token bucket per tenant**, applied after authentication, so a refused
credential costs no tenant anything. REST and MCP draw on the same allowance.

- **Bounded.** The limiter holds at most 10,000 tenants and forgets the least
  recently used, so a caller cannot grow its map.
- **Keyed by tenant, not IP**, where Rust keys by client IP. Behind a proxy
  or NAT, per-IP limiting groups unrelated tenants and splits one tenant across
  addresses (plan §II.10 row 19).
- **`/metrics` is exempt**, because a scrape refused while the default tenant
  is busy is a gap in monitoring at the moment it matters.

## Bounds

Every input a caller controls has a bound smaller than the 8 MiB request body,
and each is checked at the limit and one past it. The table is in
`docs/SECURITY_REVIEW.md`. Phase 13 added three: 50 tags, 1,000 memories to a
batch, and 64 KiB of query text.

## Failure semantics

A bound that bites is `errs.Invalid`, naming the limit, on both surfaces. A rate
limit that bites is 429 with `Retry-After`. A fuzz or crash failure is a failing
test, never a log line.
