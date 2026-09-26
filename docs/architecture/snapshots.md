# Snapshots — the portable format, export, import and verification

What is authoritative, what is derived, what is rebuilt, where the atomicity
boundaries are, and what happens when something is wrong. Not an API reference.

## What a snapshot is

A logical dump of canonical data, framed so that a reader who knows nothing
about Pebble can consume it (spec §39). It is the only backup format Remem has,
and Invariant 12 is the reason: a copy of Pebble's own files is not an escape
route from Pebble.

It is also the one surface the two implementations share. Only Rust can read
Rust's on-disk layout and only Go can read Go's (spec §45), so migration goes
through this format and nothing else: Rust exports, Go imports, and neither ever
learns the other's storage.

**A snapshot is neither canonical nor derived.** It is written from canonical
rows and read back into them, and nothing ever queries it.

## What is in it, and what is not

| In the file | Why |
|---|---|
| Record bodies | Canonical. Nothing rebuilds them |
| Canonical vectors | Canonical (plan §II.4). Rebuildable from content only by re-embedding, which is what an import does when it must |
| Out-edges | Canonical. Nothing else in the system reconstructs a relationship |
| Tenants | Canonical. Losing the directory loses the answer to "which tenants exist" |
| The lifecycle audit stream | Canonical, and the one row that outlives its own subject: a `hard_deleted` event is what answers "why did my memory disappear" |

| Not in the file | Why |
|---|---|
| In-edges | Derived from out-edges, rebuilt in the same transaction as the edge they come from |
| Attribute rows and slot index entries | Derived from record bodies |
| Text postings and corpus statistics | Derived from record bodies |
| HNSW node records | Derived from canonical vectors, and the only asynchronously maintained index |
| Job rows | Canonical, and deliberately absent. A queue is a statement about work this deployment still owes, and restoring another deployment's outstanding work into a fresh store would re-run it |
| `next_attention_at` | Derived from the record and the retention policy **in force at the destination**. See below |

## Two schemas, one framing

`proto/snapshot/v1/snapshot.proto` is the migration contract: byte-shared with
the frozen Rust repository, checksummed in both (`test/proto_checksum_test.go`
and `crates/remem-server/tests/proto_checksum.rs`), and therefore fixed.

It predates Go's graph (Phase 6) and its lifecycle (Phase 10), so there are five
things Go stores that it has no field for:

- an edge's caller-supplied metadata, which the connections API accepts and
  returns;
- an edge's update time;
- a record's archive time;
- the per-tenant user-schema version;
- an audit event in the shape Go keeps one — v1's `Event` flattens an event into
  a kind and free text, where Go's records what moved, who moved it and why.

Dropping them would make Remem's only backup lossy. **Decided with the user: two
schemas sharing one framing.** Sections 1 to 6 carry the shared messages and are
written exactly as Rust would write them; sections 7 upward are Go-native
companions defined by `proto/snapshotext/v1/snapshot_ext.proto`, which Go alone
writes and reads.

Strip the companion sections from a Go snapshot and what remains is precisely a
`snapshot.v1` file. `remem-admin export --shared-only` does exactly that, and
prints what it cost rather than dropping it quietly.

**A companion block always immediately follows the block it annotates**, and the
reader enforces it. That is what bounds an importer's memory to one page rather
than a map of the whole corpus.

## The framing

```
magic     "REMEMSNAP" (9 bytes)
u16       snapshot format version = 1
u8        compression: 0 none, 1 zstd
u32       header length, then the header protobuf
then a sequence of framed, optionally-compressed blocks:
  u8 section, u32 uncompressed_len, u32 compressed_len, u32 crc32, bytes
trailer   "REMEMEND" + u32 block count + u64 total bytes + u32 crc32 of all block crcs
```

Every multi-byte integer is little-endian. A block body is the concatenation of
length-delimited protobuf messages, which is what lets a reader hold one block at
a time however large the corpus is.

**The checksum is CRC-32/IEEE, and implementation plan §II.7 says CRC-32C.** The
exporter that wrote every snapshot in existence uses `crc32fast`, which is IEEE,
and records the choice in its own module documentation. The wire wins over the
document: a verifier written from §II.7 would reject every real file. This is a
plan correction, recorded rather than silently matched.

### What the framing protects, and what it does not

The per-block checksum covers a block's stored bytes. The trailer's
checksum-over-checksums covers the set and order of the blocks. A single flipped
bit anywhere in either is caught, and a property test says so.

**Two regions carry no checksum, and both are properties of the shared format
rather than omissions here.** The header has none — Rust's does not either, and
the file is byte-shared, so Go cannot add one. Neither does a block's *frame
header*: the section byte and the two lengths sit outside the CRC that follows
them, so a flipped section byte relabels a block without any framing check
noticing.

What catches both is that **nothing trusts the header**. Import and verify count
what they actually read and refuse when the header disagrees. A relabelled block
is usually caught one step earlier, because one section's messages rarely decode
as another's — but the count cross-check is the backstop, and it is where the
guarantee lives.

## Export

`snapshot.Export` walks a pinned `storage.Snapshot`, so an export taken from a
running server is one point in time rather than a smear.

**Two passes, not a scratch file.** The header opens the file and carries final
counts, which are only known once the walk is done. The Rust exporter stages
every block into a temporary file and assembles the snapshot around them
afterwards. Go counts first — a keys-only scan of the record, vector, edge and
event spaces, reading no values — then walks, and **aborts if the two disagree**.
Both readings come from one pinned snapshot, so a disagreement is not
concurrency: it is a canonical row that could be counted and not read, which is
exactly the silent short-export the count exists to catch.

Peak memory is one page (1,000 records by default), not one corpus.

An archived memory is exported with no vector rather than a placeholder, and it
is still a record: a backup does not prune the corpus.

## Import

### The vector rule, which decides everything else

`§II.10 row 16`: Rust CLS-pools its embeddings and Go mean-pools. **A Rust
vector is the right width, the right norm, and a point in a different space.**
Importing one verbatim produces a corpus that looks healthy and ranks wrongly,
forever, with every vector carrying a model id that says `all-MiniLM-L6-v2` —
which is true, and is exactly why the model id cannot be the discriminator.

So the import decides in two steps:

1. **The source implementation, from the header.** Anything not written by Go is
   recomputed wholesale. There is nothing to decide per row.
2. **The model id and width, per vector**, but only for a Go snapshot: did *this*
   vector come from the model that is running now.

The plan's `ImportOpts.ReEmbedMissing` reads "present" as "usable" and predates
row 16. It is replaced by a three-way policy — `auto`, `re-embed`, `verbatim` —
and `verbatim` on a foreign snapshot is **refused by name** rather than obeyed.

**A record the file carries no vector for follows the same decision.** Absence
means two different things: in a Rust snapshot it is bookkeeping about an
archived memory, and Go keeps vectors for archived memories, so recomputing is
faithful; in a Go snapshot it is a fact about that corpus, and inventing a vector
would make the round trip lossy in the one direction nothing else detects.

### What an import builds

Records, canonical vectors, out-edges, tenants and audit events come from the
file. **Everything derived from them is built inside the same transaction as the
row it derives from**, by the indexers the record repository already carries —
so an imported memory gets the attribute rows, slot entries and text postings a
stored one would, written by the code that owns them rather than by a second
copy inside the importer.

The HNSW graph is the exception and is deliberately not built here. It is
materialised lazily and rebuilt by a job, and until it is, `vector.Index.Health`
reports the tenant as truncated rather than answering short (Phase 7). An unbuilt
index that says so is not the half-built access path Phase 5 refused.

`remem-admin import` enqueues one `vector.rebuild` job per tenant afterwards, so
the server builds it in the background at next start. The *library* does not, and
that boundary is deliberate: `internal/snapshot` never imports `internal/jobs`,
and the composition site is where a durable job type name belongs.

### `next_attention_at` is recomputed, never carried

It is derived from the record and the retention policy in force *at the
destination*. Trusting a number written by a differently configured deployment
would schedule an imported corpus by somebody else's rules.

This has a consequence that shapes two other things. A stored record is
*expected* to differ from its own file in that one field, so both the import's
idempotence check and the verifier compare a **field list** rather than the
encoded bodies. A byte comparison would report every correct import as a
conflict.

### Idempotence, and why resume is only an optimisation

Every row a snapshot carries is addressed by its own id, so importing a block
twice writes the same bytes. That is what makes a crashed import safe to restart
**from the beginning**, and it is why a second run reports `Unchanged` rather
than `Conflict`.

The resume cursor saves the work, not the correctness. It is issued only at page
boundaries — a records block and the vectors block after it describe one page,
and resuming between them would skip the records and then meet their vectors
with nothing to attach them to. It is returned to the caller and written beside
the snapshot, **not into the key space**: Invariant 1 gives every key a tenant
and an import cursor spans them.

A cursor carries a fingerprint of the header, so one offered against a different
file is refused rather than followed to a byte offset that means something else
there.

### Rejections are named, never silent

| Rejected | When | Why not kept |
|---|---|---|
| Self-edge | On sight | Nothing Go's write path can write is one; traversal would spend budget returning the anchor to itself |
| Orphan edge | In a sweep at the end | Its target record does not exist, so it is not user data a restore can recover — and traversal spends its node budget reaching records that are not there |

The orphan sweep runs at the end rather than per edge because the Rust exporter
interleaves records and edges a page at a time: an edge to a record in a later
page looks like an orphan when it is read and is not one.

Counts are always exact. The named examples are capped at 1,000, the bound
`internal/graph`'s `Verify` already settled on for the same reason.

### Conflicts are decided outside the transaction

`txn.Do` retries a `Conflict`, on the reading that a conflict is a lost race. An
import's conflict is a disagreement about data and will never resolve itself, so
deciding it inside the transaction body made an import refuse eight times, with
backoff between, before reporting it. The decision now happens first, which also
makes the read that informs it honest — it was reading the store rather than the
transaction anyway.

## Verification

`snapshot.Verify` reads a file and a store and reports where they disagree. It
writes nothing, and it exists because **an import that reports success has proved
the file was readable, not that what is now in the store is what was in the
file.** It is the step between an import and decommissioning the system the file
came from, and `remem-admin verify` exits 2 on disagreement — the disposition
`graph verify` already has.

Three things it deliberately does not compare:

- **`next_attention_at`**, for the reason above.
- **Vector values**, when the snapshot is not one this model wrote. Comparing
  them would measure the pooling difference and nothing else. The *norm* is
  compared, and that is not nothing: a vector that is not unit-length ranks
  against a corpus it is not on the same scale as.
- **Derived rows.** They are not in the file, and Invariant 3 says they are
  rebuilt rather than restored.

It also checks the file's own header against the file's own blocks, which is the
one check here that is about the snapshot rather than the store. A header nobody
checks is a header nobody can trust, and the header is the one part of the format
with no checksum in either implementation.

**Verify before the lifecycle runs.** A sweep decays health, archives expired
memories and changes exactly the fields the verifier compares, so a store that
has been serving for an hour legitimately disagrees with the snapshot it was
built from. Measured on a real 6,686-record corpus: clean immediately after
import, 4,864 disagreements after two short server sessions, every one of them
the sweep's own work.

## Failure semantics

| What is wrong | What happens |
|---|---|
| Not a snapshot | `Corruption`, naming what the file opens with instead |
| A newer format version | `IncompatibleVersion`, naming the version and saying to upgrade |
| An unknown section | `IncompatibleVersion`, listing what this binary understands. It **fails rather than skipping** (spec §59): the one thing worse than a failed import is a successful one that left something behind |
| A section declared in the shared schema and written by no released binary (`schema`, `events`) | Refused, for the same reason |
| A block that fails its checksum | `Corruption`, naming the block, the byte offset, and how many blocks before it read cleanly |
| A truncated file | `Corruption`, naming the last complete block. A snapshot cut at block 900 of 1,000 is 900 blocks of recoverable data, and a message that only says "corrupt" throws them away |
| No trailer at all | `Corruption`, distinguished from a truncated block — a complete set of blocks with no trailer is the cheapest kind of truncation |
| Two snapshots concatenated | `Corruption`, because the common cause is a shell redirect and only the first would otherwise be imported |
| A header whose counts the blocks do not support | `Corruption` at the end of the import, naming the disagreement |
| An unknown relationship type | `IncompatibleVersion`. A type number is two bytes of every edge key and is never renumbered, so an unknown one means a newer Remem wrote the file |
| An import that needs a model on a binary without one | `Unavailable`, with the build command |

## What is open

**A snapshot does not carry retention policy overrides.** A tenant's per-tenant
policy table is canonical and is not exported, so a restored corpus is scheduled
by the destination's policies. That is the right default for a migration and the
wrong one for a disaster restore, and nothing currently distinguishes them.

**Nor does it carry job rows**, deliberately, for the reason in the table above.
An imported corpus has no discovered relationships until something asks for them,
and `remem-admin discovery backfill` is that something.

**Namespaces are fixed at `default`.** Export and import both walk one namespace
per tenant. Nothing enumerates namespaces because nothing creates a second one;
when something does, this is where it threads through.

**The rebuild commands are not resumable.** `remem-admin rebuild` takes the whole
index as its unit of work, the disposition Phase 7 recorded for `vector rebuild`
and Phase 9 did not change. An interrupted rebuild leaves the canonical rows
untouched and is repaired by running it again.
