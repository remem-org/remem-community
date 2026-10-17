# Maintained memory: the native model

What is authoritative, what is derived, how each durable object is versioned and
where it lives, where the atomicity boundaries are, and which phase owns each of
the twenty-two maintained-memory invariants (Revision 7 added 21 and 22). Maintained-memory P0
(`openspec/changes/archive/2026-10-10-design-maintained-memory-model`). **No code implements this
document yet.** It is the contract that the phases of
`docs/superpowers/plans/2026-10-07-maintained-memory-plan.md` implement, and
the place each of them amends first when it needs something not listed here.

Semantics come from the architecture proposal,
`docs/superpowers/proposals/2026-10-05-maintained-memory-architecture.md`. This
document cites its sections (§NN) rather than restating them, and holds only
decisions, maps and invariants. Where the two disagree, the proposal governs
semantics and this document governs representation. A disagreement is a defect
in this document.

## The model

### Five objects, and what is canonical

The five first-class objects are observation, focused memory, justification,
decision record and usage event (§02). Their properties are in the proposal's
table. Three distinctions decide everything below.

- **Observed is not believed.** An observation is what was reported. A focused
  memory is what is held, and why. Formation never deletes an observation's
  episodic access (Invariant 12). An observation never resolves to "its" memory:
  it may have zero, one or many formed outputs (Invariant 17).
- **Content is not support.** A memory version is an immutable declaration of
  content, typed claim, scope, perspective and applicable interval, identified
  by `(memory_id, version)`. Support, status and the current projection change
  without a new version. Only a change of text, value or scope makes one (§02,
  "Identity, structure and versions").
- **History is events, not flags.** Contribution activation and deactivation,
  and vocabulary or policy changes, are append-only canonical events. An
  `active` flag may exist as a derived acceleration and is never the only
  record (§02, "Canonical and derived state"; Invariant 16).

**Canonical** (snapshot-carried, never rebuilt):

- observations and their capture metadata;
- retained evidence excerpts;
- memory versions;
- contributions;
- decision records;
- change events;
- evidence-origin and lineage records; vocabulary versions (entities, aliases,
  predicates, scope dimensions and values) and pending vocabulary claims;
- suppression markers;
- processing epochs;
- import id maps and attributed links;
- worklist and drain-control rows;
- usage events;
- the tenant control counters.

**Derived** (rebuilt from canonical rows by a recorded deterministic rule
version, with **no model call**):

- current projections and publications;
- the reverse-dependency index;
- episode-segment metadata;
- keyword postings;
- attribute rows;
- HNSW nodes;
- usage summaries;
- reliability weights (P6b).

**Expendable** (canonical in class, because nothing derives it, but losing it
loses no knowledge; like MCP sessions):

- the model response cache.

### Claims, identity, perspective and scope

A structured claim has entity ids, a registered canonical predicate id, typed
arguments or value, polarity, scope, perspective (`held_by`) and applicability
(§02). It does not depend on the language of the prose (§16). The predicate
definition owns cardinality, value equality, permitted scopes, freshness and
incompatibility rules. A model can propose a predicate; its destructive
properties act only when corroborated.

- **Identity is evidence.** Entities and aliases are vocabulary that grows from
  content. A merge takes effect only when corroborated by independent evidence
  groups: equal external identifiers or explicit identity statements. Name or
  vector similarity never merges entities (§02).
- **An unresolved claim is narrative.** It can be a hypothesis or an event, and
  it cannot supersede a structured belief.
- **Perspective is not subject is not source.** `held_by` is an entity reference —
  a person, a character or an agent — and the three fields are never inferred
  from each other.
- **Scope is typed and never widened silently.** Dimensions are categorical,
  numeric range with unit, or set, each with a deterministic overlap rule; an
  unspecified dimension is unresolved (§02).
- **Narrative kinds** (procedure, skill, goal, plan) have versions and evidence.
  Execution and validation are deferred (§09).

### Two axes, never one

The **epistemic** status is `supported`, `hypothesis`, `contested`,
`unsupported` or `retracted`; `supported` is graded `reported` or `corroborated`.
**Applicability** is `current`, `historical`,
`future`, `expired` or `unknown`, and `superseded` is a derived applicability
label. Domain state (a decision in force or revoked, a goal active or
abandoned) is a third, kind-specific field. `supported` means justified under
the recorded policy, not true (§04).

### Evidence groups, grades and reliability

- **Independence is structural** (Invariant 8). Evidence identity is the
  originating event, document or version; an evidence group is the authenticated
  user or a declared external origin. Two groups are independent only when
  they differ in authenticated user or in declared external persistent origin
  (DOI, arXiv id, dataset or instrument id, distinct-authorship URL) and no
  lineage link — forwarding, re-export, translation, a known echo — joins them.
  A user token proves the user (P0a, `add-tenant-users`): the group is the
  user's stable id, which survives token rotation and renames. A credential
  carrying no user (`server.api_keys`, the operator key, an unauthenticated
  development server) joins one shared unattributed group per tenant, so such
  credentials never corroborate one another. Speaker, role and source labels are
  claims.
- **Grades** are `hypothesis`, `reported`, `corroborated` (independent group
  weights reach *k*, default 2, per predicate family and evidence kind) and
  `contested` (incompatible at an equal grade). `corroborated` beats `reported`.
  A source supersedes only its own earlier report. Nothing ranks claims by
  arrival, and no person decides between them (§§04, 11).
- **Reliability is derived** (Invariant 22): each group's weight lies in
  [0.5, 1.0], starts at 1.0, and is recomputed from settled claims by a versioned
  clamped rule that excludes the claim under evaluation. It is rebuildable and
  calls no model.
- **The operator** sets *k*, the reliability rule and clamp, and rate bounds.
  Nothing per fact.

### Contributions

There are eight contribution types:

- assertion or confirmation;
- refinement;
- asserted-current report;
- state transition or interval closure;
- historical correction or withdrawal;
- registered derivation or defeater;
- identity operation;
- retraction.

Their reduction meaning is the proposal's table (§02, "Contribution reduction
and historical state"). Each contribution has an immutable id, a decision id,
exact target refs, semantic dependencies fixed at acceptance, evidence refs, an
effective interval and the rule versions that interpret it.

A proposer cannot declare a dependent interpretation independent to escape
rollback. The validator fixes dependencies (P7a).

## Ordering and generations

All counters are **tenant-local, durable, monotonically increasing `uint64`s
that never reset and never wrap**. Each lives in its own row in the tenant
control space. A counter is read and advanced **inside** the `txn.Do` body (the
read-modify-write rule), so a lost race re-reads it rather than re-issuing a
stale expectation.

| Counter | Advanced by | Read by |
|---|---|---|
| **Command sequence** | every accepted managed command | reduction, `known_at`, change-event keys |
| **Requested revision** | every invalidating command, in its own transaction | default reads, revision workers |
| **Published revision** + its **input cut** | verified revision publication only | default reads |
| **Privacy generation** (requested / published) | erasure, in its own transaction, before any closure walk | every response path, cursors, exports, caches |
| **Vector generation** | P23 cutover only | every vector producer and query |

- **Events are ordered by `(command sequence, ordinal)`**, never by timestamp.
  Several commands can share a clock tick, and the order breaks storage ties
  without asserting which fact happened first (§02).
- **Separate rows, not one struct.** Every default read checks the revision
  and privacy rows, and every managed commit advances the sequence row. One
  combined row would make every read conflict with every write. Writers to a
  tenant are serialised on the sequence row by `txn.Gate`, which is Phase 8's
  answer to the BM25 statistics row, for the same reason: the gate removes
  contention, and the conditional commit is what makes it correct.
- **Computed revision travels with each projection row**, together with its
  input cut and rule and policy versions. A row with a lower computed revision
  may still belong to a newer publication if dependency-closure verification
  carried it forward (§02, "Minimum projection/publication contract").
- **Default current reads require requested == published** for both revision
  and privacy. Otherwise they return `revision_pending` /
  `privacy_revision_pending` and no content from either tier (§05).
- **The atomic revision domain for the first slice is the tenant.** Smaller
  domains are deferred until a sound dependent-read protocol exists (§09). This
  is the constraint Phase 14 inherits (`clustering.md`).

## Durable formats

Invariant 4 governs every row below: newer-on-disk is refused at open, and
older migrates forward. Each family is **one manifest entry**, registered in
`version.Formats` by its owning phase **in the commit that first writes the
family**, not before. Registering a version before anything is written would
make every directory "need migration" for a format it does not hold.

| Manifest name | Family | Class | Owner | Rebuild / verify | Reversal cost once shipped |
|---|---|---|---|---|---|
| `managed_model` | Directory marker for a managed store | canonical | P4a | n/a | High: decides what startup refuses |
| `observation_format` | Observations, capture metadata (referencing `0x13` origins), segment metadata | canonical (segments derived) | P2 | Segments re-derived from bytes + segmenter version; verify compares | Migration |
| `evidence_format` | Retained excerpts with byte mappings | canonical | P5a | Verify against the source mapping while the source is retained | Migration |
| `vocabulary_format` | Entities, aliases, predicates, scope dimensions and values, pending vocabulary claims; evidence origins and lineage links | canonical | P0b | Verify rule-graph acyclicity, forwarding and origin references | Migration |
| `belief_format` | Versions, contributions, decision records, change events, control counters | canonical | P4a (versions, counters), P5a (contributions), P7a (decisions), P6e (events) | P5b verifies references | Migration |
| `belief_rules` | The deterministic reduction and projection rules | rule version | P6 | A bump forces a projection rebuild | Rebuild |
| `projection_format` | Projections, publications, reverse-dependency index, reliability weights | derived | P4b, P5a, P6b, P6d | Rebuild from canonical rows + `belief_rules` | Rebuild |
| `work_format` | Worklist items, drain control | canonical | P1c, P7b | Verify one live owner per tenant and type | Migration |
| `privacy_format` | Suppression markers, quarantine | canonical | P17a | Verify closure coverage before publication | Migration |
| `epoch_format` | Processing epochs, revocations, fencing generation | canonical | P10c (first record written by P7a) | none | Migration |
| `import_format` | Previous-release id map, attributed links, import report | canonical | P20a | Verify against the report | Migration |
| `usage_format` | Usage events, usage summaries | events canonical, summaries derived | P13 | Summaries rebuilt from events | Migration (events), rebuild (summaries) |

**Existing derived engines are reused**, not duplicated: text postings,
attribute rows and index, HNSW nodes, and graph edges. Their document identity
becomes a typed reference (segment or memory version) rather than a record id.
That is a `text_index` bump in P2 and an `attr_schema` bump in P4b, and each
owning phase updates the `derivations` map in `internal/keys/space.go` so the
rebuild guard follows.

**Typed references are not a manifest format.** They are wire schema, versioned
with the `/api/v2` contract and the managed MCP schema version (P3c), because
no bytes on disk are typed-reference envelopes.

### Key-space reservations

The managed model owns the block `0x10`–`0x2F`. A byte is *reserved* here and
*allocated* (added to `space.go`, `AllSpaces` and, if derived, `derivations`) by
the owning phase. Every space keeps the tenant-first layout of
`internal/keys` (Invariant 1). The managed model needs no system-space row
beyond its manifest entries.

| Byte | Space | Class | Family |
|---|---|---|---|
| `0x0F` | passage | derived | **taken by `passage-vectors`** (`SpacePassage`, shipped in PR #33) on the current runtime, outside the managed block |
| `0x10` | observation | canonical | observation |
| `0x11` | episode segment | derived from `0x10` | observation |
| `0x12` | evidence excerpt | canonical | evidence |
| `0x13` | evidence origin, lineage link | canonical | vocabulary |
| `0x14` | vocabulary: entity, alias, predicate, scope dimension and value, pending vocabulary claim | canonical | vocabulary |
| `0x15` | memory version | canonical | belief |
| `0x16` | contribution | canonical | belief |
| `0x17` | decision record | canonical | belief |
| `0x18` | change event, keyed by (command sequence, ordinal) | canonical | belief |
| `0x19` | tenant control counters | canonical | belief |
| `0x1A` | projection and publication | derived | projection |
| `0x1B` | reverse dependency | derived | projection |
| `0x1C` | worklist item | canonical | work |
| `0x1D` | drain control | canonical | work |
| `0x1E` | suppression, quarantine | canonical | privacy |
| `0x1F` | processing epoch | canonical | epoch |
| `0x20` | import id map, attributed link | canonical | import |
| `0x21` | unassigned (reserved in revision 6 for a work-item kind Revision 7 removed; never allocated) | — | amend this table first |
| `0x22` | model response cache | canonical, expendable (as `0x0C`) | P10a decides whether it is versioned at all |
| `0x23` | usage event | canonical | usage |
| `0x24` | usage summary | derived from `0x23` | usage |
| `0x25` | managed canonical vector, keyed by typed ref, stamped with vector generation | canonical | belief |
| `0x26`–`0x2F` | unassigned, managed only | — | amend this table first |
| `0x30` | user: users, tokens, name index | canonical (name index derived from users) | **taken by P0a** (`add-tenant-users`), platform identity outside the managed block; manifest `user_format`, token index at the system path `/tokens/<id>` |

Two choices that look like duplication and are not:

- **Change events are not `SpaceEvent` (`0x0B`).** That stream is keyed by
  subject and is the lifecycle audit trail. Reduction and `known_at` scan in
  tenant command order, and one key cannot sort both ways.
- **Managed vectors are not `SpaceVector` (`0x03`).** They are keyed by typed
  ref and carry a vector generation (§16). Reusing `0x03` would need a
  key-encoding bump and would put two embedding spaces in one range during a
  P23 cutover, which is exactly what Invariant 18 forbids serving.

### Snapshot sections

Sections **11–40** are reserved for managed companions, in a new
`proto/managed/v1` package rather than `proto/snapshotext/v1`. `snapshotext`
describes the previous-release record model, which a managed store does not
hold. Keeping the two apart lets the previous-format decoder (P20a) be retired
without touching managed restore. `snapshot.ParseSection` already refuses an
unknown section, so an older binary refuses a managed snapshot rather than
importing part of it. P20 assigns numbers within the range and decides what
`--shared-only` means for a managed store (§06).

Section **41** is taken by P0a's users companion (`go_users`, in
`proto/snapshotext/v1`). It sits outside the managed range because users are
platform identity, carried by every Go snapshot, and never carry a token.

## Staging and atomicity

**One public service method, one `txn.Do`.** Inside its body the method calls
**stagers**. A stager:

- takes the caller's `txn.Tx` and the values it needs, and stages rows into it;
- never opens a transaction and never calls a public service method;
- never reads the clock, generates an id, embeds or calls a provider. Ids,
  timestamps and vectors are resolved before the first attempt and travel in
  the accepted command (governing-spec Invariants 8 and 9; proposal §12 and
  maintained Invariant 4).

This generalises the precedent `record.Indexer.Stage` set in Phase 8: derived
rows staged into the caller's transaction by the package that owns their key
space, registered so that no write path can forget one.

**Staged synchronously, in the same transaction as the canonical write:**

- keyword postings for segments and versions (which is why `captured` means
  keyword-ready);
- attribute rows;
- reverse dependencies;
- change events;
- the command-sequence advance;
- the requested-revision or privacy-generation advance, for an invalidating
  write;
- worklist and drain-control rows that wake the asynchronous work below.

**Asynchronous, woken by those rows:**

- HNSW maintenance (unchanged since Phase 7);
- segment and version embedding (P2, P23);
- revision closure and publication (P6d);
- privacy closure (P17a);
- formation (P7b, P10a).

**Job finalisation is a stager too.** P1c's domain completion (item
acknowledgement, drain-control handoff, continuation, queue transition and run
history) is staged in one conditional transaction by a stager the composition
root registers. `internal/jobs` calls it through an interface and imports no
domain package. Today the handler runs (`internal/jobs/execute.go:163`) and then
`CompleteWithRun` commits separately (`execute.go:113`). That gap is what P1c
closes.

**Guard (owned by P4a).** A boundary test fails if a stager package imports
`internal/clock`, calls `id.New`, imports `internal/embedding` or a provider
adapter, or calls `txn.Do`. Like every guard here, it must be shown to fail on
a deliberate violation.

## The previous-release boundary

Data from the previous release moves by export and then import into a fresh
store (§01). There is no in-place migration and no legacy serving mode. A new
managed directory has a `managed_model` manifest entry from its first write. At
open:

| `managed_model` in manifest | Rows in spaces `0x02`–`0x07` | Result |
|---|---|---|
| present | — | managed store; ordinary migration rules for managed formats |
| absent | present | **refused** with `migration_required`, naming `docs/MIGRATION.md`'s export/import |
| absent | absent | new directory: created as a managed store |

- **The managed runtime never writes `0x02` (record) or `0x03` (vector).**
  They stay allocated, since a byte is never reused, because their presence is
  how a previous-release directory is recognised.
- **`0x04`/`0x05` (graph edges) are reused** for memory-to-memory edges (P12),
  keyed by memory id. Imported links are attributed provenance in `0x20` until
  endpoint adoption, and never become justifications (§04).
- **The previous-format decoder is reachable only from `import`** (Invariant
  9). An old `/api/v1` memory request is refused with contract information
  (§17), not served.

## Package boundaries

Each package is created by the phase that first needs it, and each row lands as
an `internal/arch` rule in that phase.

| Package | Holds | May import | Must not import |
|---|---|---|---|
| `internal/vocabulary` | vocabulary objects, seed catalogue, evidence-group identity, scope overlap | `txn`, `keys`, `codec`, `errs`, `tenant` | `belief`, `formation`, `api` |
| `internal/observation` | capture, segments, evidence excerpts, origin references | `vocabulary`, `text`, `txn` | `belief`, `formation`, `api` |
| `internal/belief` | versions, contributions, reducer, projections, publication, change events, counters | `vocabulary`, `observation`, `text`, `attr`, `graph` | `formation`, any provider, `api` |
| `internal/privacy` | suppression, privacy barrier, closure walk | `belief`, `observation` | `formation`, `api` |
| `internal/formation` | proposer interface, validator, accepted commands, drains | `belief`, `observation`, `vocabulary`, `jobs` | `api`; providers only through its adapter subpackages |
| `internal/formation/provider/<name>` | one language-model adapter each | `formation` | any storage package (provider isolation, as ONNX) |
| `internal/usage` | usage events, summaries, recall activation | `belief` | `formation`, `api` |

The direction is `formation → belief → observation → vocabulary`. The reducer
calling no provider is therefore an import fact rather than a convention.
`internal/memory` and `internal/record` are retired by P3c and P4a, and P4a's
staging extraction moves into `belief`. `internal/jobs` keeps its existing rule:
it imports none of these.

## Invariant ownership

Proposal §14's twenty-two invariants, numbered as there. Each guard is one named
test that the owning phase writes first and shows failing when the guard is
deliberately broken. A phase may rename its test, and the row is updated in the
same commit. P9 runs each scenario on the real binary, and P21a/P21b supply the
semantic evidence.

| # | Invariant | Owner | Guard test | P9 scenario |
|---|---|---|---|---|
| 1 | Deterministic policy owns acceptance | P6c, P10b | `TestAcceptanceIgnoresModelVerdict` | A structurally valid wrong claim counted as a semantic error, never accepted by verdict |
| 2 | Mutation permission is structural | P7a | `TestAcceptanceRefusesNewlyInsertedConflict` | A forbidden target, and a conflict inserted during inference, both refused at commit |
| 3 | Support is grounded | P6, P6b | `TestRootlessCycleHasNoSupport` | A+B→C with D→C; the last external root of a cycle removed |
| 4 | Replicated apply never calls a model | P7a | `TestReplayUsesRecordedCommandOnly` | Replay with the provider unreachable reproduces the store |
| 5 | Invalidation needs no model | P6d, P17a, P10c | `TestInvalidationDuringProviderOutage` | Revision, expiry, erasure and rollback during a provider outage |
| 6 | Use is not evidence | P13, P15 | `TestRepeatedUseCannotPromoteHypothesis` | A hypothesis delivered a hundred times stays a hypothesis |
| 7 | No agent maintenance budget | P3c, P14 | `TestManagedToolSchemaBudget` | Schema and default result sizes measured; no polling or processing tool |
| 8 | Independence is structural | P0a, P0b, P6 | `TestClaimedIdentityCreatesNoIndependentGroup` | A declared speaker label, a forwarded copy and a rotated key add no evidence group; a late receipt confers no onset |
| 9 | One managed mutation policy | P3c, P20a, P22 | `TestOldContractRefusedAndImportCannotBypassPolicy` | `/api/v1` refused; an import's origins collapse to the importing caller and cannot manufacture independence |
| 10 | Both tiers respect invalidation | P6d, P3b | `TestCorrectedSpanCannotEscapeViaEpisodeOrCursor` | The corrected Berlin span through episodes, pages and cache |
| 11 | Generations and logical identity | P1c, P7a, P7b | `TestProducerRaceDuringDrainFinalisation` | A producer racing completion, cancellation and recovery; two responses yield one decision |
| 12 | Formation keeps episodic access | P2, P3b | `TestZeroOutputObservationStaysSearchable` | Zero/partial output, and an unextracted sentence at the end of a transcript |
| 13 | Retention follows copy lineage | P17, P17a | `TestThreeHopErasureSuppressesExcerptsAndCaches` | Three-hop erased lineage; rollback cannot resurrect it |
| 14 | Non-provider resource isolation | P19, P6d | `TestHotSubjectObeysReadWindow` | A continuously dirty subject; protected shares under saturation |
| 15 | Epoch-first rollback | P10c | `TestLateRevokedEpochResponseRefused` | A late and a cached response from a revoked epoch |
| 16 | Reconstructible contribution history | P6e, P20 | `TestKnownAtUsesHistoricalPolicy` | `known_at` across a policy change; removed history reported missing |
| 17 | Typed, versioned references | P3c | `TestWrongRefTypeIsObjectTypeMismatch` | A capture with zero and with several outputs; no fabricated memory id |
| 18 | Every vector producer is gated | P23 | `TestNoWriterEscapesVectorGenerationGate` | An import and an admin writer racing cutover |
| 19 | Capture initiates learning | P3, P22 | `TestCaptureSchedulesLearningWithoutPermissionFields` | Capture and import schedule learning with no per-input flag |
| 20 | Raw expiry is not forgetting | P17, P17a | `TestRetainedExcerptOutlivesRawExpiry` | Raw expiry keeps support; source erasure suppresses raw and retained evidence |
| 21 | Vocabulary is versioned evidence | P0b, P6 | `TestUncorroboratedExclusivityDoesNotSupersede` | A model-proposed exclusivity leaves competing values coexisting until independent groups corroborate it; a merge forwards without rewriting old references |
| 22 | Reliability is derived | P6b | `TestReliabilityIsRebuildableAndExcludesSubject` | Weights rebuilt from canonical rows match; a claim cannot raise its own support |

## Seed catalogue and fixtures

The binary ships a seed vocabulary; it is a starting point, not a limit, and the
vocabulary grows from content (§02, P0b). Seed predicates: `instance_of`,
`part_of`, `located_in`, `measured_value` (value, unit, uncertainty), `states`,
`holds`, `proves`, `refutes`, `primary_residence`, `visited`, `prefers_tool`,
`committed_to`, aligned to Wikidata where an equivalent exists. Seed dimensions:
`project`, `task`, `environment`, `within_work`, `theory`, `axiom_system`
(categorical); `conditions` (numeric range with unit); `population` (set). One
seed defeater: a corroborated `refutes` defeats `holds` within its scope. Nothing is approved
per source or per fact.

| Fixture | What it proves |
|---|---|
| Personal | Berlin→Lisbon by a source superseding its own report; another source's contradiction is contested |
| Physics | Three independent measurements: two with overlapping uncertainty corroborate, one disjoint is contested |
| Mathematics | A conjecture, a proof report scoped to an axiom system, a refutation in another scope that does not conflict |
| Fiction | A novel's opening paragraph forms no real-world claim; a character's opinion is `held_by` that character and scoped `within_work` |
| Identity | Two "Alice"s stay separate until external-identifier evidence from independent groups is corroborated, then merge with forwarding |

## Mandatory limits

Each limit is a configuration key whose absence **refuses the operation it
governs**, including deterministic capture and revision on a deployment with no
language model (§09). Values are measured under P19.3 and declared with their
workload and hardware. None is chosen here.

| Limit | Governs | Owner |
|---|---|---|
| observation bytes; batch items and bytes | capture | P2, P19 |
| segment bytes, segment count, overlap; postings per transaction | capture | P2, P19 |
| worklist batch items, bytes, time | every drain | P1c, P19 |
| backlog count, bytes, age | capture admission | P19 |
| provider expenditure per window | formation, re-forming, consolidation | P19 |
| embedding concurrency, interactive and background | the shared embedder | P1b, P19 |
| closure fan-out | revision, privacy, rollback walks | P19, P6d |
| invalidation rate and burst | nonurgent invalidating activation | P19, P6d |
| `max_revision_pending`, `max_privacy_pending` | barrier overload outcomes | P6d, P17a |
| revision retry interval, `min_current_read_window` | read-window scheduling | P6d |
| vocabulary growth rate (new terms per tenant per window) | formation | P0b, P19 |

## Recorded decisions

| Decision | Recorded answer | Reversal cost |
|---|---|---|
| Phase 14 placement (P0.3) | Maintained memory first. Phase 14 is replanned against the managed model, accepted commands and the tenant revision domain | Replanning only; nothing is shipped |
| Editions (P0.4) | Community and Business both ship managed formation. Stored users and tokens (P0a) are core in every edition, and editions differ only in how many tenants they serve; administration runs through the operator credential, `remem-admin` and HTTP admin routes | An edition-boundary change under `docs/EDITION_BOUNDARY.md` |
| Embedder (P0.14) | `pplx-embed-context-v1-0.6b`, per `passage-vectors` decision 1, shipped in PR #33: fp32 ONNX inference, the model's int8 output rule `round(tanh(x)·127)`, 1,024 wide, values stored L2-normalised. Its spike, pairing and discovery measurements are reused. Its blocking in-place `0003-reembed-passages` migration serves the current runtime; **for managed stores** it is superseded by P20a's fresh-store conversion and P23's vector-generation gate, and it introduces no old runtime here | A P23 migration |
| Release languages (P0.15) | English, French, Spanish, German, Russian; results reported per language; tolerances are P21.1's measured decision | Re-evaluation |
| Representation (P0.16) | Families, bytes and sections reserved above; byte layouts, protobuf messages and field numbers in each owning phase's design | Low until the owner ships, then migration |
| Vocabulary (P0.17, Revision 7) | Seed catalogue above; grows from content | Low |
| Trust (Revision 7) | Evidence-rooted: graded support and derived reliability; nothing approved per fact, no review queue | High once P6 ships |

`passage-vectors` landed on the current runtime before maintained memory and
took `0x0F` and `passage_index`, as anticipated. P23 reuses its verified model
pipeline and adds the managed safeguards rather than switching models twice (plan
§"Dependency overview").

## What is not decided here

| Not decided | Decided by |
|---|---|
| Byte layouts, protobuf messages, field numbers for every family | The owning phase's design (P0.16) |
| Section numbers within 11–40; managed `--shared-only` semantics | P20 |
| Whether the response cache (`0x22`) carries a manifest version | P10a |
| Numeric limits, shares, windows, retry intervals | P19.3 |
| Semantic and retrieval tolerances per language | P21.1 |
| Exact seed catalogue membership | P0b |
| Whether a managed store reuses `0x0F` passage rows for segment or version vectors, retires the space, or treats it only as a previous-release marker beside `0x02`–`0x07` | P2, P23 |
| Default k per predicate family and evidence kind; the reliability clamp | P6b, P21a |
