# Storage

The layer between Remem and the bytes on disk: the `storage.KV` abstraction and
its two implementations, the key encoding, the record envelope, and the format
manifest that gates every open. Spec §60 asks these documents to explain
invariants rather than APIs — what is authoritative, what is derived, where
atomicity ends, and how things fail — so that is what this one does. Signatures
are in the code.

Landed in Phase 2. Nothing above this layer exists yet: no record model, no
repository, no query. What exists is the set of decisions that become expensive
the moment a customer stores something.

## What is authoritative

| Concern | Authority | Everything else |
|---|---|---|
| Where a row lives | the key built by `internal/keys` | every scan is a range over those keys; nothing filters after the fact |
| What a record body means | the envelope's codec byte | the body encoding may be replaced; the envelope says which one wrote it |
| Whether a directory may be opened | the manifest under `/format/*` | the binary's constants are compared against it, never the reverse |
| Isolation | the tenant component of the key | no higher layer is trusted to filter by tenant |
| Ordering of an indexed value | `internal/keys`' order-preserving encoders | the store sorts bytes; these make byte order mean value order |

## The keyspace

```
key := <tlen:uvarint><tenant> <nslen:uvarint><namespace> <space:1> <remainder>
```

Three properties, each load-bearing.

**The tenant is outermost.** Everything one tenant owns is therefore one
contiguous range. Erasing a departing customer, exporting one, and splitting one
onto its own node are each a single range operation rather than one per space
that somebody must remember to keep in step — and the last of those is what
makes the tenant a real shard boundary rather than an aspiration.

This departs from plan §II.2's one-line grammar, which puts the space byte
first. Three things in the plan outweigh that line: §II.2's own opening sentence
promises tenant ranges are contiguous, Task 2.3 mandates a `TenantRange(t)`
returning a single range, and §II.5 calls the tenant the future shard boundary.
Under space-first none of the three is achievable and `TenantRange` cannot be
written correctly at all.

**Both identifiers are length-prefixed.** Without it `acme` is a byte prefix of
`acmecorp`, and a prefix scan over one silently returns the other's rows. That
is a tenant-isolation defect, not a formatting preference, and no care at a
higher layer can undo it. A consequence worth knowing: keys sort by identifier
*length* before identifier *bytes*, so tenants are not in lexicographic order in
the keyspace. Nothing depends on them being so — the tenant directory lives in
the system space and is ordered there.

**The namespace exists before the feature does.** It is hardwired to `default`
and no user sees it. It is in the encoding because retrofitting it means
rewriting every key every tenant owns, and plan §II.5 records the failure that
actually follows from deferring: under delivery pressure the namespace model
gets built as a post-retrieval filter, which is precisely the mistake tenant
partitioning exists to prevent.

The system space uses an empty tenant and an empty namespace rather than a
special layout. The parser therefore has no branch, and system rows sort ahead of
all user data.

### Invariant 1 is structural here

There is no builder for a user-space key that does not take a tenant.
`RecordChecked` refuses an empty or malformed one outright. Invariant 1 — no
unscoped read path — is not a rule the storage layer asks callers to follow; it
is a shape that has no way to express the violation.

## Canonical versus derived

Plan §II.4 assigns every space a recovery class, and the key encoding keeps them
distinguishable at a glance:

| Space | Class | Rebuildable from |
|---|---|---|
| record, vector, edge-out, job, event, session, system | **canonical** | nothing — corruption is an error, never a silent discard |
| edge-in | derived | out-edges |
| attr-row | derived | record bodies |
| attr-index | derived | attribute rows |
| text | derived | record bodies |
| vector-index | derived, **asynchronous** | canonical vectors |

The consequence that matters at this layer: a canonical decode failure returns
`errs.Corruption` naming the key and never falls back to a zero value. A record
that reads as empty because its bytes were damaged is worse than a record that
fails to read, because nothing downstream can tell the difference.

## Atomicity ends at the batch

`storage.Batch` is the atomicity boundary. Writes staged in one become visible
together or not at all, and two batches committing concurrently never interleave
— a reader sees all of one or none of it, never half of each.

A batch is also readable: staged writes shadow the store, and a staged delete
reads as `NotFound` even while the row is still present for everyone else. That
is what lets a transaction consult what it has already decided before deciding
the rest, and it is why the Pebble adapter uses an *indexed* batch — a plain
Pebble batch cannot be read from at all.

A batch may also carry expectations about the value or absence of keys it read
before staging a mutation. Every adapter checks those expectations under the
same serialization as all other writes. A mismatch returns `errs.Conflict` and
publishes none of the batch. Record read-modify-write paths use this to prevent
a stale archive or lazy upgrade from overwriting a concurrent update or
resurrecting a hard-deleted record.

`storage.Snapshot` is the read boundary. It pins a state. The memory service
keeps that object in a bounded paging-session registry, so every page reads the
same state and mutable ordering values cannot move across the cursor. Snapshot
sequence numbers remain process-local diagnostics; they are not persistence or
resume handles and cannot detect a restored database.

## Borrowed memory is part of the contract

`Iterator.Key` and `Iterator.Value` return slices valid only until the next
positioning call. Pebble hands back a window into a block buffer it reuses; a
caller that retains one without copying reads a plausible wrong value later,
which is the worst failure mode available because it neither crashes nor looks
wrong.

The contract states this as a *permission* implementations may take, not an
obligation, because the two implementations take it differently: Pebble reuses
its key buffer but leaves old value bytes in a block it has not yet overwritten.
So the shared contract suite asserts the direction that is universal — a caller
that copies gets the right answer, forwards and backwards — and `memkv`
deliberately goes further, poisoning both buffers on every positioning call.
That asymmetry is the point: every layer above storage is unit tested against
`memkv`, so `memkv` is where a missing copy has to fail loudly.

`KV.Get` and `Snapshot.Get` are the exception. They return a fresh copy the
caller owns, because Pebble's `Get` returns a slice plus a closer and the slice
dies with the closer.

## Containing Pebble

Invariant 6 says Pebble must not leak into the domain model, and the
import-graph guard in `internal/arch` fails the build if any package but
`internal/storage/pebble` imports it. That guard has been verified against a
deliberate violation rather than assumed.

But an import guard is only half of containment, because two things cross an
interface with no import at the far end:

- **Error values.** Every Pebble error is translated and its text discarded. A
  Pebble message names sstables, sequence numbers and manifest offsets; a Remem
  operator can act on none of it, and it is exactly the detail the interface
  exists to hide. `TestPebbleErrorsAreNeverExposed` holds this, because the
  import guard structurally cannot.
- **Borrowed memory**, as above.

Pebble's own log messages are routed into `slog` rather than its default
unstructured writes to stderr. The adapter takes a logger rather than making
one — only `internal/obs` constructs loggers, and a nil logger drops the output.

### The failure translation

| Pebble condition | `errs.Kind` | Why |
|---|---|---|
| `ErrNotFound` | `NotFound` | the row is absent |
| `ErrCorruption` | `Corruption` | never retryable; retrying cannot repair a byte |
| `ErrClosed` | `Unavailable` | shutting down |
| anything else | `Storage` | the store failed and the data is intact as far as anyone knows, so it is retryable |

## Order-preserving encoding

The attribute index sorts by value and the store sorts by bytes. The encoders in
`keys/order.go` are the bridge, and they are what makes "the ten most important
memories" a bounded scan instead of a sort over everything the tenant owns.

The float transform is the piece plan §II.2 singles out as easy to get subtly
wrong, and it is: a wrong implementation still produces plausible bytes and still
round-trips. Negatives are inverted whole — which both moves them below the
positives and undoes their backwards ordering — and positives get the sign bit
set. `-inf < -1.0 < -0.0 < +0.0 < 1.0 < +inf` holds, including the signed-zero
pair that `==` calls equal and an index must not. NaN is encoded rather than
rejected and lands above `+inf`: it has no correct position, and the least
surprising wrong one beats a value that cannot be stored.

Strings are terminated, not length-prefixed. A length prefix sorts by length
before content, which would put `"b"` below `"aa"`. `0x00` is escaped to
`0x00 0xFF` so it cannot be mistaken for the `0x00 0x00` terminator; since
`0xFF` sorts above `0x00`, escaping preserves order as well as disambiguating.
Without the escape two different values encode identically and one silently
overwrites the other's index entry.

These carry property tests because examples cannot cover a float. The tests have
been verified to catch a break — replacing the transform with the plausible
wrong version fails within five draws and shrinks to `-1 vs -1.5` — and they
found a real tenant-isolation defect that every hand-written case had missed.

## The record envelope

```
byte 0..3  magic "RMM1"   byte 4  envelope version   byte 5  codec   byte 6..  body
```

Six bytes buys the ability to replace the body encoding later without ambiguity,
which is spec §40.4's requirement that no third-party serialisation library
defines Remem's long-term storage format without an explicit wrapper.

Protobuf is the body encoding for one reason (plan §II.3): field-number
evolution is the only option that lets a binary decode a record written by a
version that did not exist when it was compiled, **and preserve the fields it
does not understand when it writes that record back**. That second half is what
a rolling upgrade depends on, and it is tested the way it actually happens — an
older binary decodes a newer record, edits a field it understands, and writes it
back; both the edit and the unknown field survive.

### Two failure classes, deliberately different

- A value that will not parse is **`Corruption`**. These bytes came from the
  store. Returning a zero-valued record would silently replace a memory with an
  empty one.
- A value whose envelope version or codec is *higher* than this binary knows is
  **`IncompatibleVersion`**. The bytes are probably fine and this binary is too
  old. Spec §59 says refuse rather than guess.

`EnvelopeVersion` reads the header without decoding the body, because a
migration walking every record to decide what to do with each one should not
parse bodies it is about to rewrite.

## The format manifest

One row per durable format under the untenanted system space, each carrying
`current`, `min_reader` and `min_writer` as spec §19.2 requires. It is
untenanted because it describes the whole directory and must be readable before
any tenant is known — which at open time is always.

The open policy is asymmetric on purpose:

- **No manifest** is stamped at current. Brand-new and written-before-the-
  manifest-existed are one case, and treating them as one removes a branch that
  would otherwise be tested by nobody.
- **A format this binary has never heard of** refuses the open. It means files
  exist this build cannot interpret, and continuing would mean operating on a
  directory while ignoring part of it.
- **A directory needing a newer reader or writer** refuses, naming the format,
  both numbers, and which direction to move.
- **An older directory opens.** Moving it forward is the migration
  runner's job; `Format.NeedsMigration` is how a caller learns there is work.

`min_reader` and `min_writer` default to `current`, so a version bump made
without thinking about compatibility locks older binaries out rather than
letting them in. Lowering them deliberately is what allows a format bump that
did not change how existing bytes are interpreted to leave older binaries
running.

The manifest is deliberately **not** wrapped in a versioned envelope the way a
record is. It is the root of the versioning scheme, so an envelope would only
move the question — something has to be readable without first consulting a
version, and this is it. A row that is not exactly twelve bytes is `Corruption`,
not an absent version: reading damage as zero would silently declare the
directory ancient and migrate it.

Opening does not rewrite a manifest it can already use, so a restart is not a
write to the one row that says whether the data is readable.

## Where the contract suite lives, and why it is shared

`internal/storage/storagetest` is exported so that `memkv` and the Pebble
adapter run the *identical* suite. A behaviour that differs between the store
used in tests and the store used in production is a behaviour that will be
discovered in production.

A case belongs there, not in one implementation's own file, whenever the
requirement is about what a KV *is* rather than about how one is built. The two
exceptions in this phase are documented above: `memkv`'s deliberate buffer
poisoning, and the assertion that no Pebble error escapes.

## What this phase deliberately does not decide

- **The record body's field set** beyond identity, content, timestamps, tags and
  source. Lifecycle and retention fields are Phase 4's to design; the proto
  reserves numbered ranges for them, and unknown-field retention is what makes
  adding them safe.
- **Migrations.** The registry and the runner are Phase 4, and are documented in
  `schema-and-migrations.md`. This phase built the manifest and the gate, which
  is what has to exist before anything writes a byte of user data.
- **Transactions.** `txn.Tx` over a batch is Phase 3 Task 3.2.
- **Tenant resolution.** `internal/tenant` here is reduced to the identity types
  the encoding needs. The directory, resolver and provisioning are Phase 3.
