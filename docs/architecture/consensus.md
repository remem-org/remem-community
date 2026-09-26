# Consensus

**Scheduled for Phases 15 and 16, and deliberately not written yet.** Consensus
arrives as an etcd/raft adapter (plan §Phase 15) and then as replicated shards
(§Phase 16), after clustering (`clustering.md`). What exists today are the rules
the single-node code already keeps, so that replication is additive:

- **Only `internal/cluster/consensus/etcdraft` may import `go.etcd.io/raft`.**
  The Phase 1 boundary guard already carries that rule, though no such package
  exists yet (Invariant 7).
- **Nothing in a deterministic apply path reads a clock, draws a random id,
  embeds, or calls the network** (Invariants 8 and 9). That is why record ids
  and timestamps are decided outside `txn.Do` bodies, and why an HNSW node's
  layer is a function of its record id rather than a random draw
  (`docs/architecture/vector.md`).
- **Raft's own snapshots will be distinct from the portable export**
  (`docs/architecture/snapshots.md`), which remains the escape route from any
  one storage engine.

This document will say what a replicated apply guarantees, how leadership and
reads work, and where the Raft log lives, when Phase 15 builds it.
