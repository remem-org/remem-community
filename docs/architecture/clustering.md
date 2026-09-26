# Clustering

**Scheduled for Phase 14, and deliberately not written yet.** The plan (§Phase
14) holds cluster primitives until Phase 13 declares parity, because the
specification says the milestones depend on what single-node operation teaches
(spec §54). What exists today is the three seams the single-node phases were
required to leave, so that clustering is additive rather than a rewrite:

- **A `Query` serialises** (Phase 5, `docs/architecture/query.md`), so a shard
  can be sent a query rather than a sequence of index calls.
- **Fusion is rank-based** (Phase 5), because ranks mean something locally on
  every node and raw scores do not.
- **Every job lease carries an owner and a fencing token** (Phase 9,
  `docs/architecture/jobs.md`), so claiming a job can become cross-node without
  touching a handler.

`version.ClusterCompat` is zero, and stays zero until a cluster format is
active. This document will say what is authoritative across nodes, what shards
own, and how the `tenant → []shard` mapping is kept, when Phase 14 builds it.
