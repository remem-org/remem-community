# Remem Go Rewrite and Distributed Storage Architecture

## Status

Proposed architecture.

This document defines the target architecture and implementation constraints for migrating Remem from Rust to Go and preparing it for multi-tenant, multi-node operation.

It is intentionally not an implementation plan.

The implementation plan should be created separately after reviewing the existing repository, mapping current Rust functionality to this architecture, identifying dependencies between subsystems, and defining incremental migration stages.

---

# 1. Context

Remem is an agent-native memory database designed for AI agents and language models.

The current implementation is written in Rust and provides an embedded single-node database with functionality including:

- persistent memory storage
- metadata
- vector embeddings
- semantic search
- keyword search
- hybrid search
- relationships between memories
- short-term memory TTL
- importance decay
- promotion and archival lifecycle
- background maintenance
- MCP integration

The existing storage engine includes an LSM-tree, WAL, and related persistence functionality.

The project is now moving towards substantially more complex requirements:

- multi-tenancy
- multi-session operation
- multi-node clusters
- replication
- horizontal scaling
- increasing use of background processing
- continuous relationship discovery
- richer graph semantics
- vector search
- structured and relational access patterns
- temporal/time-series access patterns
- long-term schema evolution
- rolling upgrades
- production-grade durability

At the same time, the current Rust codebase has become increasingly difficult to understand and maintain. Some source files have grown very large, subsystem boundaries are insufficiently explicit, and adding distributed behaviour to the existing architecture would significantly increase complexity.

The migration to Go should therefore be treated as an architectural rewrite rather than a line-by-line port.

---

# 2. Primary Objective

Build a clean Go implementation of Remem that provides a stable foundation for the next stage of the product:

> An agent-native, multi-tenant, multi-model database that can operate efficiently as a single node and evolve into a distributed multi-node system.

The design must prioritise:

1. correctness
2. understandability
3. maintainability
4. architectural boundaries
5. development velocity
6. operational simplicity
7. extensibility
8. performance

Raw latency is not the primary optimisation target.

Remem's primary consumers are AI agents and LLM-based applications, where database latency is usually small compared with model execution latency.

---

# 3. Product Positioning Assumption

Remem should not become merely an orchestration layer over several external databases.

The target is not:

```text
Remem
  |
  +-- vector DB
  +-- graph DB
  +-- relational DB
  +-- time-series DB
```

This architecture would make Remem structurally similar to existing memory frameworks and introduce operational complexity and external dependencies.

Instead, Remem should own its multi-model semantics and indexes.

The intended architecture is:

```text
                 Remem

        Agent memory semantics
                 |
             Query layer
                 |
         Unified data model
                 |
   +-------------+--------------+
   |             |              |
 Vector        Graph         Structured
 Index         Index           Index
   |             |              |
   +-------------+--------------+
                 |
       Transactional KV layer
                 |
              Pebble
```

In distributed mode:

```text
                 Remem

        Agent memory semantics
                 |
             Query layer
                 |
         Unified data model
                 |
         Multi-model indexes
                 |
          State machine
                 |
             Shards
                 |
            etcd/raft
                 |
              Pebble
```

Pebble provides physical local persistence.

etcd/raft provides the consensus algorithm.

Remem owns everything between these two infrastructure primitives and the user-facing database semantics.

---

# 4. Language

The new implementation should be written in Go.

## 4.1 Rationale

Go is preferred because Remem's future complexity will increasingly come from:

- concurrency
- networking
- distributed state
- long-running workers
- background jobs
- request routing
- cluster membership
- replication
- timers and leases
- operational tooling

rather than from extremely latency-sensitive low-level compute.

Go provides an appropriate trade-off between performance and implementation complexity.

## 4.2 Go design principles

The implementation should deliberately prefer simple, explicit code.

Avoid unnecessary abstraction.

Avoid:

- deep interface hierarchies
- generic abstractions without proven reuse
- framework-like internal APIs
- excessive dependency injection
- implicit global state
- clever concurrency patterns
- large multipurpose packages
- very large source files

Prefer:

- small packages with clear ownership
- explicit dependencies
- narrow interfaces
- composition
- clear context propagation
- deterministic lifecycle management
- straightforward error handling

A subsystem should generally be understandable without reading unrelated packages.

---

# 5. Licensing Constraints

Remem Community is licensed under Apache 2.0. Remem Enterprise / Cloud is
intended to be offered under commercial terms prepared by counsel. The engine
and core data formats remain shared across editions.

The architecture must preserve the ability to continue licensing Remem independently.

Dependencies should therefore strongly prefer permissive licences:

- Apache 2.0
- BSD
- MIT

Avoid foundational dependencies that impose:

- service restrictions
- source redistribution constraints affecting Remem
- database-as-a-service restrictions
- SSPL-style obligations
- AGPL obligations unless deliberately chosen
- edition-specific proprietary dependencies unless explicitly approved and kept
  out of Community distributions

## 5.1 Selected dependencies

### Pebble

Use CockroachDB Pebble as the local ordered key-value storage engine.

Pebble is permissively licensed and does not constrain Remem's licensing.

### etcd/raft

Use `go.etcd.io/raft` as the initial consensus implementation.

etcd/raft is preferred because it is:

- mature
- widely deployed
- actively maintained
- Apache 2.0 licensed
- sufficiently low-level to avoid owning Remem's cluster architecture

Do not use a higher-level distributed framework as the core architecture unless future evidence strongly justifies it.

In particular, the architecture must not depend on Dragonboat concepts outside an adapter if Dragonboat is ever evaluated.

---

# 6. Core Architectural Principle: Remem Owns Its Semantics

Pebble must not leak into the domain model.

etcd/raft must not leak into the database model.

The dependency direction should always be:

```text
Domain
  |
Services
  |
Storage / Consensus abstractions
  |
Adapters
  |
Pebble / etcd-raft
```

Never:

```text
MemoryService -> pebble.DB
GraphEngine   -> raft.Node
VectorSearch  -> pebble.Batch
```

Direct imports of infrastructure libraries outside their adapter packages should be prohibited.

---

# 7. Proposed Package Structure

The final package structure may evolve after inspection of the existing repository, but the implementation should aim for boundaries similar to:

```text
cmd/
    remem/

internal/
    api/
        mcp/
        http/

    auth/

    tenant/

    session/

    memory/
        model.go
        service.go
        repository.go

    schema/
        schema.go
        registry.go
        migration.go

    query/
        query.go
        planner.go
        executor.go
        filter.go

    graph/
        model.go
        index.go
        traversal.go

    vector/
        index.go
        flat/
        hnsw/

    text/
        index.go

    temporal/
        index.go

    relations/
        service.go
        discovery.go

    lifecycle/
        ttl.go
        decay.go
        promotion.go
        archive.go

    jobs/
        queue.go
        worker.go
        lease.go
        scheduler.go

    transaction/
        tx.go

    storage/
        kv.go
        batch.go
        snapshot.go
        iterator.go

        pebble/
            store.go
            batch.go
            snapshot.go
            iterator.go

    cluster/
        node.go

        consensus/
            consensus.go

            etcdraft/
                node.go
                group.go
                storage.go
                transport.go

        shard/
            shard.go
            manager.go

        routing/
            router.go

        membership/
            membership.go

        placement/
            placement.go

        rebalance/
            rebalance.go

    snapshot/
        format.go
        export.go
        import.go

    observability/
        metrics.go
        logging.go
```

Packages should be reorganised if repository inspection reveals better natural boundaries.

The implementation plan must not mechanically create packages merely because they appear in this document.

---

# 8. Unified Data Model

Remem should not implement vector, graph, relational, and temporal models as unrelated storage systems.

They should operate over a common canonical entity model.

A conceptual record may contain:

```go
type Record struct {
    ID       RecordID
    TenantID TenantID

    Type string

    Fields map[string]Value

    Vectors map[string]Vector

    CreatedAt time.Time
    UpdatedAt time.Time

    ValidFrom *time.Time
    ValidTo   *time.Time
}
```

This definition is illustrative rather than prescriptive.

Graph relationships should be independent first-class entities or indexes referring to record IDs.

A memory is a specialised record with memory semantics rather than a completely separate physical storage architecture.

---

# 9. Identity Hierarchy

Multi-tenancy must be introduced before multi-node support.

Every operation must have explicit ownership context.

The minimum logical hierarchy should support:

```text
Tenant
  |
  +-- Namespace / Project
        |
        +-- Agent
        |
        +-- Session
        |
        +-- Memories / Records
```

Exact naming should reflect existing Remem concepts.

At minimum, every persisted user-owned object must include or be addressable through `TenantID`.

Do not rely only on globally unique record IDs for isolation.

Incorrect:

```text
memory/{memoryID}
```

Preferred:

```text
memory/{tenantID}/{memoryID}
```

or an equivalent encoded key.

Tenant identity must propagate through:

- API
- services
- queries
- storage
- indexes
- background jobs
- replication
- snapshots
- metrics where relevant

Background processes must not accidentally become global operations unless explicitly designed as such.

---

# 10. Storage Abstraction

Pebble should live behind a Remem-owned storage abstraction.

The exact interface should be designed based on real requirements rather than copied literally from this example.

Conceptually:

```go
type KV interface {
    Get(ctx context.Context, key []byte) ([]byte, error)

    Set(ctx context.Context, key, value []byte) error

    Delete(ctx context.Context, key []byte) error

    NewBatch() Batch

    NewSnapshot() Snapshot

    Scan(
        ctx context.Context,
        start []byte,
        end []byte,
        fn func(key, value []byte) error,
    ) error

    Close() error
}
```

Required capabilities will probably include:

- point reads
- point writes
- delete
- ordered scans
- atomic batches
- snapshots
- iterators
- prefix scans
- durable sync
- compare/version semantics where needed

Do not make the abstraction overly generic.

It exists to:

1. isolate Remem from Pebble
2. provide test implementations
3. make future engine migration possible
4. provide a logical boundary for transactions

It does not need to support every possible storage engine.

---

# 11. Keyspace Design

Remem should define its own logical key encoding.

Pebble key layout is part of Remem's internal storage format and must therefore be versioned.

A conceptual layout:

```text
/system/...

/tenant/{tenant}/record/{type}/{id}

/tenant/{tenant}/graph/out/{from}/{relation}/{to}
/tenant/{tenant}/graph/in/{to}/{relation}/{from}

/tenant/{tenant}/index/{type}/{index}/...

/tenant/{tenant}/job/{state}/{job}

/tenant/{tenant}/session/{session}/...
```

The concrete binary encoding should:

- preserve useful sort order
- minimise allocations
- support efficient prefix scans
- make tenant ranges contiguous where useful
- have explicit version semantics
- avoid textual encoding overhead if unnecessary

Human-readable examples in this document are not a requirement for physical key representation.

---

# 12. Transaction Model

All logical writes that modify canonical state and its required indexes must be atomic where possible.

For example, creating a record may require:

```text
record
+ scalar indexes
+ graph indexes
+ session membership
+ durable indexing jobs
```

These should not be independently written without consistency guarantees.

A transaction abstraction should exist above Pebble batches.

Initially this may provide only single-node atomicity.

Later the state-machine/Raft boundary should preserve equivalent semantics within a shard.

Cross-shard distributed transactions should not be implemented unless a concrete use case requires them.

Avoid introducing distributed transactions prematurely.

---

# 13. Multi-Model Index Architecture

Indexes are Remem-owned structures.

The canonical source of truth is the record/relationship data.

Indexes should be considered either:

- transactional authoritative indexes, or
- reconstructible derived indexes

The distinction must be explicit for every index type.

---

# 14. Graph Model

Graph support is a first-class requirement.

Relationships should support at minimum:

- source
- destination
- relationship type
- optional weight
- metadata
- timestamps
- future temporal validity if needed

Efficient traversal requires both outgoing and incoming indexes.

Conceptually:

```text
graph/out/{tenant}/{from}/{type}/{to}
graph/in/{tenant}/{to}/{type}/{from}
```

Required operations should eventually include:

- neighbours
- incoming neighbours
- outgoing neighbours
- filtering by relationship type
- bounded traversal
- path discovery
- relationship updates
- relationship removal

Graph design must support relationship discovery running continuously in background workers.

---

# 15. Vector Model

Vector search is a core Remem capability and must not depend on an external vector database.

The vector subsystem should expose a Remem-owned interface.

Conceptually:

```go
type VectorIndex interface {
    Insert(...)
    Delete(...)
    Search(...)
    Rebuild(...)
}
```

Vector index implementation should be replaceable independently from the domain layer.

## 15.1 Initial strategies

Support an architecture where index selection can vary based on workload.

Possible strategies:

### Flat

Exact brute-force vector search.

Useful for:

- small tenants
- tests
- correctness reference
- small namespaces

### HNSW

Approximate nearest-neighbour search for larger datasets.

The current Remem implementation already uses HNSW concepts and should be reviewed during migration.

Future algorithms may include:

- IVF
- IVF-PQ
- disk-oriented ANN indexes

Do not design the public API around HNSW.

HNSW is one possible implementation.

---

# 16. Vector Index Durability

The canonical vector values should be persisted independently of the derived ANN structure.

Recommended model:

```text
canonical record
    |
    +-- vector
          |
          +--> derived ANN index
```

A damaged or incompatible ANN index should be rebuildable from canonical vector data.

This is especially important for:

- schema changes
- version upgrades
- snapshot restore
- replica recovery
- algorithm changes

Do not require HNSW internal node state to be consensus-replicated unless there is a proven requirement.

---

# 17. Text and Hybrid Search

Remem currently supports keyword and hybrid search.

These capabilities must survive the rewrite.

The implementation should separate:

- full-text/keyword index
- vector index
- ranking/fusion

Hybrid ranking should exist above the underlying indexes.

For example:

```text
keyword results
       \
        -> fusion/ranking -> final results
       /
vector results
```

Existing reciprocal-rank-fusion behaviour should be preserved unless intentionally redesigned.

---

# 18. Temporal / Time-Series Access

Temporal functionality should initially be implemented through ordered keys and indexes rather than by introducing an independent time-series engine.

Example logical ordering:

```text
tenant/{tenant}/time/{series}/{timestamp}/{record}
```

Expected functionality may include:

- creation time
- update time
- access time
- event history
- importance history
- relationship changes
- lifecycle changes

Future optimisations may include:

- partitioning
- compression
- retention
- downsampling

These should be introduced only if workloads justify them.

---

# 19. Schema System

Schema evolution is a first-class architectural requirement.

Three separate version concepts must exist.

---

## 19.1 Physical Storage Engine Version

Owned by Pebble.

Examples:

- SST format
- WAL format
- manifest format

Remem must not duplicate this responsibility.

---

## 19.2 Remem Internal Storage Format Version

Owned by Remem.

This represents things such as:

- key layout
- record serialisation
- graph representation
- index representation
- metadata encoding

Persist explicit metadata such as:

```text
system/storage_format/current
system/storage_format/min_reader
system/storage_format/min_writer
```

Exact key representation may differ.

---

## 19.3 User Schema Version

Future user-visible schema definitions must be independent from internal storage format.

For example:

```text
tenant A schema v15
tenant B schema v8
```

Changes to tenant schemas must not imply changing Remem's physical storage format version.

---

# 20. Schema Migrations

Migration infrastructure must be implemented before complex storage evolution begins.

Migrations should be:

- explicit
- ordered
- resumable
- idempotent where practical
- observable
- testable
- crash-safe

At least three migration strategies should be supported conceptually.

## 20.1 Instant migrations

No large data rewrite required.

Examples:

- new optional metadata
- new key namespace
- feature flag introduction

## 20.2 Lazy migrations

Old records are upgraded when accessed.

Example:

```text
read old format
    |
decode
    |
convert
    |
write current format
```

Use only when mixed-format operation is safe.

## 20.3 Background migrations

Large datasets are migrated incrementally by workers.

Migration state should include information such as:

```text
migration ID
source format
target format
state
cursor
processed count
started timestamp
last progress timestamp
error
```

Migrations must resume after:

- process restart
- node restart
- cluster failover

---

# 21. Cluster Compatibility Version

Binary version and active storage/cluster format version must be separate.

Required for rolling upgrades.

Example:

```text
Node A binary 2.1
Node B binary 2.1
Node C binary 2.0

Cluster compatibility version: 2.0
```

The cluster must not activate features or storage changes that the oldest active node cannot understand.

After every node supports the new version:

```text
Cluster compatibility version -> 2.1
```

then migrations/features may activate.

This concept must be incorporated into the multi-node design from the beginning.

---

# 22. Background Job System

Background processing is expected to become a major part of Remem.

Potential workloads include:

- relationship discovery
- embedding generation
- vector indexing
- index rebuilding
- TTL expiration
- importance decay
- promotion
- archival
- schema migrations
- snapshot creation
- cleanup
- future summarisation or consolidation

These should not each invent independent worker infrastructure.

Build a common job framework.

---

# 23. Job Requirements

Jobs should have:

- ID
- tenant
- type
- payload/reference
- creation time
- state
- retry count
- lease
- owner
- checkpoint/progress where relevant
- last error
- priority if eventually required

Possible states:

```text
pending
running
retry
completed
failed
cancelled
```

The exact model should remain minimal until requirements emerge.

---

# 24. Job Execution

Workers should be Go goroutines controlled through contexts.

Expected lifecycle:

```text
scheduler
    |
pending jobs
    |
worker claims job
    |
lease acquired
    |
processing
    |
checkpoint
    |
complete
```

Job claiming must eventually work safely across nodes.

Initially single-node implementation should nevertheless use abstractions compatible with future distributed execution.

Do not implement distributed leases until multi-node work actually begins.

---

# 25. Continuous Relationship Discovery

Continuous relationship discovery is a specific target workload.

A conceptual flow:

```text
new/changed memory
        |
relationship job
        |
candidate retrieval
        |
vector similarity
        |
existing graph context
        |
relationship evaluation
        |
edge updates
```

The architecture must permit future strategies combining:

- vector similarity
- text similarity
- metadata
- graph neighbourhood
- temporal context
- LLM evaluation
- heuristic scoring

Relationship discovery should not be hard-coded into storage-layer operations.

It is domain/background-processing logic.

---

# 26. Single-Node First

The Go implementation must reach a stable single-node architecture before multi-node clustering is introduced.

The first target should be:

```text
Go
+ Pebble
+ multi-tenancy
+ unified model
+ graph
+ vector
+ text/hybrid
+ lifecycle
+ jobs
+ schema/migrations
```

with no Raft dependency required in the request path.

This provides a correctness baseline.

---

# 27. Distributed Architecture

Once single-node semantics are stable, introduce a distributed state-machine layer.

The conceptual architecture:

```text
                       Client
                          |
                       Router
                          |
               +----------+----------+
               |                     |
             Shard A               Shard B
               |                     |
          Raft Group A          Raft Group B
               |                     |
          +----+----+            +---+----+
          |    |    |            |   |    |
        Node1 Node2 Node3      Node1 Node3 Node4
```

The shard is the primary unit of:

- consistency
- replication
- ownership
- recovery
- movement

---

# 28. Shard Key

The initial sharding strategy should favour simplicity.

Likely starting point:

```text
hash(tenantID) -> shard
```

Benefits:

- strong tenant isolation
- operations for a tenant stay within a shard
- avoids cross-shard transactions for most operations
- easy routing

However, large tenants may eventually require multiple shards.

The architecture should therefore avoid assuming forever that:

```text
tenant == shard
```

Prefer:

```text
tenant -> one or more shards
```

even if initially there is only one.

---

# 29. Raft Responsibility

Raft replicates logical state-machine commands.

It does not replicate Pebble files.

Correct conceptual model:

```text
PutRecord(...)
AddEdge(...)
DeleteRecord(...)
UpdateMetadata(...)
```

becomes:

```text
proposal
   |
Raft log
   |
committed
   |
apply state machine
   |
Pebble transaction
```

All replicas independently apply the same committed logical command.

---

# 30. Consensus Abstraction

etcd/raft must exist behind a Remem-owned interface.

Conceptually:

```go
type ConsensusGroup interface {
    Propose(
        ctx context.Context,
        command []byte,
    ) error

    ReadIndex(
        ctx context.Context,
    ) (uint64, error)

    AddReplica(
        ctx context.Context,
        replica Replica,
    ) error

    RemoveReplica(
        ctx context.Context,
        id ReplicaID,
    ) error

    Status() GroupStatus
}
```

The actual interface should be based on implementation requirements.

No code outside the etcd/raft adapter package should import `go.etcd.io/raft`.

---

# 31. etcd/raft Responsibilities vs Remem Responsibilities

etcd/raft should provide:

- Raft state machine
- election algorithm
- log progression
- quorum logic
- membership-change protocol
- consensus correctness

Remem owns:

- network transport
- node identity
- shard identity
- Raft group lifecycle
- persistent Raft log integration
- snapshots
- replica placement
- leader routing
- client routing
- rebalancing
- health
- cluster metadata
- operational APIs

This is deliberate.

Remem should avoid architectural dependency on a higher-level distributed framework.

---

# 32. Raft Log Persistence

Raft log persistence may use Pebble.

It should use a logically separate keyspace from user data.

Example:

```text
/system/raft/{group}/log/{index}
/system/raft/{group}/hard_state
/system/raft/{group}/snapshot
```

The implementation plan must determine whether separate Pebble databases or separate keyspaces are preferable.

Correctness and lifecycle isolation matter more than minimising database handles.

---

# 33. Read Consistency

Initial distributed implementation should prefer simple strong consistency.

Leader-based reads or Raft ReadIndex semantics are preferred initially.

Do not introduce stale follower reads until needed.

Future API may distinguish:

```text
strong
bounded-stale
eventual
```

but this should not be implemented prematurely.

---

# 34. Replica Placement

Initial replication factor should probably default to 3.

Placement must eventually consider:

- node availability
- shard balance
- capacity
- failure domains
- future regions/zones

Initial implementation can be substantially simpler.

Avoid designing a full Kubernetes-style scheduler upfront.

---

# 35. Rebalancing

Rebalancing is a later distributed milestone.

It should move logical shard replicas, not physical storage-engine implementation details.

Conceptually:

```text
Shard A:
Node1 Node2 Node3

->

Node1 Node3 Node4
```

using:

1. add Node4 replica
2. catch up
3. verify
4. remove Node2

Do not implement complex automatic balancing in the first multi-node release.

Manual/admin-driven movement is acceptable initially.

---

# 36. Cluster Membership

Cluster membership and shard replication membership are different concepts.

Cluster membership answers:

```text
Which Remem nodes exist?
```

Raft group membership answers:

```text
Which nodes host this shard?
```

Keep these separate.

Do not attempt to represent the entire Remem cluster as one giant Raft group.

---

# 37. Multi-Raft

The architecture should support many Raft groups within one Remem process.

A node may host replicas for many shards:

```text
Node A
 |
 +-- shard 1 replica
 +-- shard 4 replica
 +-- shard 7 replica
 +-- shard 22 replica
```

The etcd/raft adapter and scheduler should therefore avoid assumptions of one Raft group per process.

---

# 38. Snapshot Semantics

There are two distinct snapshot concepts.

## 38.1 Raft shard snapshot

Used for:

- follower catch-up
- log compaction
- replica recovery

## 38.2 Remem logical export snapshot

Used for:

- backup
- migration
- portability
- engine replacement
- disaster recovery

Do not conflate them.

---

# 39. Stable Remem Export Format

Pebble SST files should not become the long-term public backup format.

Remem must own a storage-engine-independent logical export format.

Conceptually:

```text
Remem Snapshot vN

manifest
schema
records
vectors
relationships
metadata
tenant metadata
optional derived indexes
```

Derived indexes should generally be optional because they can be rebuilt.

This provides an escape route from Pebble.

A future migration should be possible:

```text
Pebble
  |
export Remem snapshot
  |
new storage engine
  |
import
```

without requiring users to understand underlying engine files.

---

# 40. Dependency Lock-In Rules

The project should explicitly enforce dependency isolation.

## 40.1 Pebble

Only `internal/storage/pebble` may import Pebble directly.

## 40.2 etcd/raft

Only `internal/cluster/consensus/etcdraft` may import etcd/raft directly.

## 40.3 Vector implementation

HNSW implementation must live behind `VectorIndex`.

## 40.4 Serialisation

Avoid allowing one third-party serialisation library to define Remem's entire long-term storage architecture without explicit version wrapping.

Every durable format must be versioned by Remem.

---

# 41. Serialisation

The implementation plan should evaluate suitable binary formats.

Requirements:

- explicit versioning
- forwards/backwards compatibility where possible
- low overhead
- deterministic decoding
- safe corruption handling
- cross-language readability if practical

Potential approaches include:

- protobuf
- custom binary structures
- MessagePack-like formats

Do not choose based solely on speed.

Durable format evolution is more important.

All persisted records should have sufficient format information to support migration.

---

# 42. Corruption Handling

Database corruption must fail explicitly.

Do not silently discard malformed canonical records.

Distinguish between:

- canonical record corruption
- derived index corruption

For reconstructible indexes:

```text
corrupt index
    ->
mark unhealthy
    ->
rebuild
```

For canonical data:

```text
corrupt record
    ->
surface error
    ->
recovery tooling
```

This boundary should be part of the storage design.

---

# 43. Index Rebuild

Every derived index must expose a rebuild path.

Expected examples:

```text
vector index
graph-derived indexes
text index
temporal indexes
```

Rebuild should support:

- entire database
- tenant
- shard
- specific index

where practical.

This is important for migrations and recovery.

---

# 44. API Compatibility

The Go rewrite should preserve current user-facing behaviour wherever feasible.

Particularly:

- MCP operations
- memory storage
- semantic search
- keyword search
- hybrid search
- relationship behaviour
- lifecycle semantics

Breaking API changes must be intentional and documented.

The implementation plan should inventory the existing Rust APIs and classify each as:

- preserve exactly
- preserve semantically
- deprecate
- redesign

---

# 45. Data Migration from Existing Rust Remem

Existing databases should have a migration path.

Do not require users to discard their data.

Preferred approach:

```text
old Rust Remem DB
      |
migration/export utility
      |
stable Remem logical snapshot
      |
Go Remem import
```

Directly teaching the Go engine to read every historical Rust on-disk representation should be avoided unless trivial.

A one-time migration utility is preferable to permanent legacy complexity.

---

# 46. Testing Strategy

The rewrite must have much stronger test boundaries than a line-by-line port.

Required test categories:

## 46.1 Unit tests

For:

- key encoding
- schema conversions
- graph traversal
- ranking
- vector calculations
- lifecycle logic
- job transitions

## 46.2 Storage contract tests

Every storage implementation must pass the same behaviour suite.

## 46.3 Migration tests

For every schema migration:

```text
old fixture
   ->
upgrade
   ->
new expected representation
```

Migration tests must include restart/resume scenarios.

## 46.4 Property tests

Useful for:

- key ordering
- serialisation round-trips
- graph consistency
- index invariants

## 46.5 Crash tests

Eventually:

- interrupted writes
- interrupted migrations
- interrupted jobs
- process restart
- partial index rebuild

## 46.6 Distributed tests

Once Raft is introduced:

- leader loss
- follower loss
- network partitions
- delayed messages
- duplicate messages
- node restart
- shard movement
- snapshot recovery

---

# 47. Determinism

All commands applied through Raft must be deterministic.

Do not execute nondeterministic operations inside the replicated state-machine apply step.

Examples of dangerous operations:

- reading current wall clock independently
- random ID generation
- network calls
- embedding generation
- LLM calls

Instead:

```text
leader determines value
      |
puts value into command
      |
Raft replicates command
      |
all replicas apply identical value
```

Background AI processing should occur outside deterministic state-machine application.

Its resulting mutations are then proposed through Raft.

---

# 48. Time

Clock semantics must be explicit.

Do not use arbitrary `time.Now()` calls throughout durable business logic.

Introduce an internal clock abstraction where deterministic testing or distributed semantics require it.

Persist timestamps generated before consensus proposal when they affect replicated state.

---

# 49. IDs

ID generation must not require central coordination.

Preferred characteristics:

- globally unique
- sortable if useful
- generation possible independently on nodes
- safe under concurrency

Possible approaches include UUIDv7 or similar.

The implementation plan should evaluate compatibility with existing IDs before selecting a new format.

---

# 50. Observability

Observability must be designed as a core infrastructure capability.

At minimum expose metrics for:

### Storage

- writes
- reads
- scan latency
- storage size
- compaction
- cache behaviour

### Search

- vector search latency
- keyword search latency
- hybrid latency
- candidates considered

### Background jobs

- pending
- running
- failures
- retries
- processing time

### Cluster

Later:

- leaders
- elections
- replication lag
- shard count
- replica count
- proposal latency
- snapshot activity

### Migrations

- current migration
- progress
- failures
- estimated remaining records where feasible

Structured logging should include:

- tenant where safe
- node
- shard
- request ID
- job ID
- migration ID

Avoid logging memory content by default.

---

# 51. Configuration

Configuration should be explicit and inspectable.

Avoid configuration behaviour hidden behind environment-specific defaults.

Potential domains:

```text
node
storage
network
cluster
replication
jobs
vector
lifecycle
logging
metrics
```

Configuration validation should happen at startup.

Invalid configuration should fail fast.

---

# 52. Embedded and Server Modes

The architecture should preserve the possibility of single-node local/embedded use.

Conceptually:

```text
remem start --data-dir ./data
```

should remain simple.

Cluster functionality should be additive.

Potential future experience:

```text
remem start
```

single node

versus:

```text
remem start --join node1:...
```

clustered node

The multi-node design must not make single-node deployments operationally heavy.

---

# 53. Non-Goals for the Initial Rewrite

The implementation plan should explicitly avoid doing everything at once.

Not required in the initial Go rewrite:

- Raft
- automatic cluster rebalancing
- cross-shard transactions
- geo-distribution
- follower reads
- sophisticated distributed query planning
- multiple ANN algorithms
- distributed graph traversal
- distributed SQL compatibility
- a full SQL parser
- advanced time-series compression
- automatic storage tiering

The rewrite should establish boundaries that allow these later.

---

# 54. Recommended Delivery Stages

The implementation plan should determine exact milestones after repository inspection, but the intended sequence is approximately:

## Stage 0: Inventory and behavioural specification

Analyse the existing Rust repository.

Document:

- modules
- APIs
- persistence model
- storage layout
- search behaviour
- lifecycle behaviour
- jobs
- tests
- data formats

Create compatibility tests or fixtures where possible.

---

## Stage 1: Go project skeleton

Establish:

- package layout
- config
- logging
- error conventions
- IDs
- core models
- test infrastructure

No major feature rewrite yet.

---

## Stage 2: Pebble storage foundation

Implement:

- KV abstraction
- Pebble adapter
- key encoding
- batches
- snapshots
- scans
- basic storage version metadata

---

## Stage 3: Canonical record model

Implement:

- tenant-aware records
- metadata
- sessions/namespaces
- serialisation
- basic CRUD
- single-node transactions

---

## Stage 4: Schema and migrations

Before complex indexes.

Implement:

- storage format version
- migration registry
- migration execution
- resumability
- tests
- upgrade fixtures

---

## Stage 5: Graph model

Implement:

- edge persistence
- incoming/outgoing indexes
- neighbour queries
- relationship management

---

## Stage 6: Vector model

Implement:

- canonical vector persistence
- flat reference index
- HNSW implementation/adapter
- rebuild capability
- vector search compatibility tests

---

## Stage 7: Text and hybrid search

Port:

- keyword indexing
- keyword search
- hybrid search
- RRF or current equivalent

---

## Stage 8: Lifecycle functionality

Port:

- TTL
- importance decay
- promotion
- archive semantics

---

## Stage 9: Shared background job framework

Implement common:

- queue
- worker
- retry
- lease-ready model
- progress/checkpoint support

Then migrate lifecycle jobs onto it.

---

## Stage 10: Relationship discovery

Move relationship work onto the job framework.

Prepare for continuous processing.

---

## Stage 11: Rust migration tooling

Implement old-to-new export/import path.

Validate representative real databases.

---

## Stage 12: Go implementation becomes canonical single-node Remem

Feature parity.

Performance verification.

Migration documentation.

---

## Stage 13: Cluster primitives

Only after single-node architecture is stable.

Implement:

- node identity
- cluster membership
- shard abstraction
- routing
- consensus interface

---

## Stage 14: etcd/raft adapter

Implement:

- multi-group Raft hosting
- transport
- log persistence
- snapshots
- proposal/apply
- leader routing

---

## Stage 15: Replicated shards

Move canonical state-machine writes through Raft.

Start with a small fixed shard count and replication factor.

---

## Stage 16: Operational cluster functionality

Add:

- replica placement
- manual shard movement
- recovery
- cluster compatibility version
- rolling upgrade support

---

## Stage 17: Automated rebalancing

Only after production behaviour is understood.

---

# 55. Performance Philosophy

Do not prematurely optimise the new implementation against the Rust version.

Initial performance target:

> Fast enough that database latency remains insignificant relative to normal AI-agent workflows.

Optimise based on profiling.

Particularly avoid premature complexity in:

- memory pooling
- zero-copy structures
- custom allocators
- unsafe Go
- manual binary tricks
- specialised vector structures

unless measurements demonstrate need.

Correctness and clarity come first.

---

# 56. Memory Management

Go GC is acceptable for Remem's current performance requirements.

Nevertheless:

- avoid unnecessary allocations in hot paths
- reuse buffers where profiling justifies it
- watch long-lived object graphs
- measure heap impact of vectors
- avoid putting huge transient objects on the heap unnecessarily

Do not introduce unsafe memory handling merely to imitate Rust performance.

---

# 57. Concurrency Model

Prefer clear ownership.

Subsystems with independent lifecycles should typically expose:

```go
Run(ctx context.Context) error
```

or equivalent.

Shutdown should propagate through contexts.

Avoid untracked goroutines.

Every long-running goroutine should have:

- an owner
- cancellation
- error handling
- tests where practical

Use channels when they simplify ownership or scheduling.

Do not use channels simply because the implementation is in Go.

---

# 58. Error Handling

Errors should preserve context.

Distinguish important classes such as:

- not found
- conflict
- invalid input
- storage error
- corruption
- unavailable
- migration required
- incompatible version
- not leader
- retryable distributed error

Do not expose Pebble or etcd/raft errors directly through public APIs.

Translate them into Remem-owned errors.

---

# 59. Compatibility Rules

The following should become explicit project policies.

## Storage

A newer binary must never silently open and mutate an unsupported older/newer database format.

## Downgrade

Downgrade behaviour must be explicit.

If downgrade is unsupported after a format migration, fail clearly.

## Cluster

A node must not join a cluster with an incompatible active cluster version.

## Snapshot

Snapshot/import tooling must report unsupported format versions clearly.

---

# 60. Documentation Requirements

Each major subsystem should have a concise architecture document.

At minimum:

```text
docs/
    architecture/
        storage.md
        schema-and-migrations.md
        graph.md
        vector.md
        jobs.md
        clustering.md
        consensus.md
```

These documents should explain invariants, not just APIs.

Examples:

- what is authoritative
- what is derived
- what can be rebuilt
- atomicity boundaries
- ownership
- version semantics
- failure semantics

---

# 61. Architectural Invariants

The implementation plan should preserve these invariants.

### Invariant 1

All user-owned data belongs to an explicit tenant.

### Invariant 2

Canonical state must never depend on a derived index for recovery.

### Invariant 3

Derived indexes must be rebuildable.

### Invariant 4

Durable formats are explicitly versioned.

### Invariant 5

Migrations are resumable or provably atomic.

### Invariant 6

Pebble is inaccessible outside the storage adapter.

### Invariant 7

etcd/raft is inaccessible outside the consensus adapter.

### Invariant 8

Raft applies deterministic state-machine commands only.

### Invariant 9

AI calls and external network operations never happen during deterministic Raft apply.

### Invariant 10

Single-node Remem remains usable without cluster infrastructure.

### Invariant 11

Binary version is distinct from active storage/cluster compatibility version.

### Invariant 12

Physical Pebble files are not Remem's portable backup format.

---

# 62. Questions the Implementation Plan Must Answer

After inspecting the current repository, the implementation-plan phase must explicitly answer the following.

## Existing architecture

1. Which current Rust modules correspond to the proposed Go subsystems?
2. Which current modules should not be ported because their responsibility moves to Pebble?
3. Which current abstractions should be preserved?
4. Which current abstractions should be discarded?

## Persistence

5. What is the exact existing on-disk format?
6. What data must be migrated?
7. Which indexes can be rebuilt instead of migrated?
8. Is there any user data currently stored only inside an index?

## Vector

9. Which HNSW implementation currently exists?
10. Should it be ported, replaced, or wrapped?
11. What search semantics must remain byte-for-byte or behaviourally compatible?
12. What metadata filtering exists today?

## Graph

13. What relationship semantics already exist?
14. Are relationships canonical or derived today?
15. What compatibility behaviour must be preserved?

## Lifecycle

16. How are TTL and decay represented?
17. What background state must survive restarts?
18. Which processes can move onto the common job framework immediately?

## MCP/API

19. What current MCP commands exist?
20. Which request and response formats are compatibility-critical?
21. Are there hidden behaviours currently relied upon by clients?

## Migration

22. Can the Rust binary expose a stable export path?
23. Should migration tooling live in Rust, Go, or both?
24. What representative data fixtures are required?

## Build transition

25. Can Rust and Go implementations coexist temporarily in the repository?
26. Should feature parity happen subsystem-by-subsystem or through a parallel implementation?
27. How will regressions be detected between the implementations?

---

# 63. Requirements for the Future Implementation Plan

When creating the implementation plan from this document and the actual repository:

Do not produce a high-level roadmap only.

The plan must include:

- exact files/packages to create
- existing files/modules affected
- migration order
- dependencies between tasks
- tests for every stage
- expected invariants
- temporary compatibility layers
- data migration strategy
- explicit completion criteria

The plan should favour incremental, testable changes.

Every major stage should leave the repository in a working state where practical.

Avoid a "rewrite everything and switch at the end" approach unless repository analysis demonstrates that incremental coexistence is impossible.

---

# 64. Definition of Success for the Go Rewrite

The Go rewrite is complete when:

- existing core Remem behaviour is preserved
- existing data has a supported migration path
- Remem runs reliably as a single-node database
- all user data is tenant-aware
- vector search works without an external vector DB
- graph relationships are first-class
- hybrid search works
- lifecycle processes work
- background jobs are unified
- storage schema is versioned
- migrations are resumable
- Pebble is isolated behind a Remem abstraction
- indexes are clearly classified as canonical or rebuildable
- the codebase is materially easier to understand than the Rust implementation
- subsystem boundaries are documented
- multi-node development can begin without another architectural rewrite

---

# 65. Definition of Success for the Initial Distributed Architecture

The first multi-node version is complete when:

- multiple Remem nodes can form a cluster
- data is split into shards
- shards use independent Raft groups
- each shard can have multiple replicas
- writes survive loss of a minority of replicas
- leader failure results in automatic election
- requests route to the correct shard
- new replicas can recover from existing replicas
- Raft logs can compact through snapshots
- a node can restart and recover
- rolling binary upgrades are possible
- active cluster compatibility version prevents premature format activation
- a shard can be moved manually between nodes
- single-node deployments remain supported

Automatic global rebalancing is not required for this milestone.

---

# 66. Strategic Boundary

The most important architecture decision is this:

Remem should own:

```text
memory semantics
record model
graph model
vector model
temporal model
text search
query semantics
schema
migrations
background jobs
sharding
routing
replica placement
cluster behaviour
portable snapshots
```

Remem should delegate:

```text
LSM tree
SST management
WAL mechanics
compaction
block cache
low-level crash recovery

            -> Pebble

Raft consensus algorithm
leader election
quorum calculation
log commitment rules

            -> etcd/raft
```

This is the intended dependency boundary.

It provides mature implementations for low-level functionality where mistakes are expensive while keeping Remem's defining database architecture under Remem's control.

---

# 67. Final Architecture

```text
                        AI Agents
                            |
                    MCP / HTTP / SDK
                            |
                  +---------+---------+
                  |                   |
             Authentication       Tenant Context
                  |                   |
                  +---------+---------+
                            |
                     Query Engine
                            |
                    Memory Semantics
                            |
                    Unified Records
                            |
       +------------+-------+-------+------------+
       |            |               |            |
     Graph        Vector           Text       Temporal
     Index        Index            Index        Index
       |            |               |            |
       +------------+-------+-------+------------+
                            |
                     Transaction Layer
                            |
                      State Machine
                            |
              +-------------+-------------+
              |                           |
        Single Node                    Cluster
              |                           |
              |                         Shards
              |                           |
              |                      Raft Groups
              |                           |
              |                       etcd/raft
              |                           |
              +-------------+-------------+
                            |
                     Storage Interface
                            |
                         Pebble
                            |
                  Files / SST / WAL
```

The single-node implementation should use the same logical database model without requiring Raft.

The distributed system should add replication beneath the transaction/state-machine boundary rather than introducing a second database architecture.

---

# 68. Instruction for the Next Planning Session

When this document is provided inside the Remem repository, the next task should be:

> Inspect the current Remem repository in detail and create a concrete implementation plan for the Go rewrite described in this architecture specification. Do not start implementation. Map every existing Rust subsystem and user-facing behaviour to the target architecture, identify data-format and migration requirements, determine which functionality can be reused conceptually and which should be redesigned, then produce an incremental file-level implementation plan with tests and completion criteria for every phase. Prioritise achieving a clean, feature-complete single-node Go implementation before introducing etcd/raft and multi-node functionality.
