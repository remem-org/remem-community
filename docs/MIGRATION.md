# Migrating a Rust Remem deployment to Go

The procedure, end to end, including how to satisfy yourself that it worked
before you decommission anything.

Read `docs/architecture/snapshots.md` for why the format is shaped the way it
is. This document is what to run.

## What you are doing, in one paragraph

Only Rust can read Rust's on-disk layout and only Go can read Go's, so the two
never meet on disk. They meet in one file format: a portable logical dump of
record bodies, embeddings, relationships and tenants. Rust writes it; Go reads
it and rebuilds every index from it. **The embeddings are the one thing that does
not carry across** — Rust pools the model's output differently, so its vectors
are the right width in the wrong space — and the import recomputes them from your
content with the same model. That is the slow part: about 2.5 milliseconds per
memory on an M-series laptop, so roughly seven minutes per hundred thousand
memories.

Nothing here modifies your Rust deployment except the one step that says it does.

## Before you start

- **A Rust binary that can export.** `remem-export` from the
  `remem-development` tree. If your deployment is older than the current
  on-disk format, opening it runs the migration chain — see "Old data
  directories" below.
- **A Go binary built with the embedder.** The plain build cannot recompute
  embeddings and will refuse the import by name:

  ```bash
  ./scripts/fetch-model.sh
  CGO_ENABLED=1 CGO_LDFLAGS="-L$(pwd)/.tools/lib" \
    go build -tags onnx -o remem-admin ./cmd/remem-admin
  CGO_ENABLED=1 CGO_LDFLAGS="-L$(pwd)/.tools/lib" \
    go build -tags onnx -o remem ./cmd/remem
  ```

- **Disk.** The snapshot runs about 1.5 KB per memory — the embeddings
  dominate it — so a 10,000-memory corpus exports to roughly 14 MB. The Go data
  directory is comparable in size to the Rust one.

## 1. Stop the Rust server

`remem-export` opens the data directory read-write, because opening it replays
the write-ahead log. Two processes writing one WAL corrupts the directory
without reporting an error. Stop the server.

## 2. Export

```bash
REMEM_DATA_DIR=/var/lib/remem remem-export --out corpus.rsnap
```

It prints what it wrote:

```
exported 6686 record(s), 6686 vector(s) (0 missing), 32911 edge(s) -> corpus.rsnap
```

`missing_vectors` counts archived memories, whose embeddings Rust retires from
its index. They are still exported as records, and the import recomputes their
embeddings — Go keeps vectors for archived memories.

**Keep this file.** It is the only artifact that lets you redo the rest without
restarting the Rust server.

## 3. Import

Into a directory that does not exist yet:

```bash
./remem-admin import \
  --data-dir /var/lib/remem-go \
  --in corpus.rsnap \
  --model-path ./.models/all-MiniLM-L6-v2 \
  --onnx-library-path /opt/homebrew/opt/onnxruntime/lib/libonnxruntime.dylib
```

It says what it read before it writes anything:

```
importing a snapshot written by rust 0.1.0 at 2026-09-08T22:16:08Z: 1 tenant(s),
  6686 record(s), 6686 vector(s), 32911 edge(s), 0 event(s)
recomputing embeddings with all-MiniLM-L6-v2
imported 6686 record(s), 32911 edge(s), 0 event(s) in 15.97s
embeddings: 6686 recomputed, 0 taken from the snapshot
```

`0 taken from the snapshot` is correct and is not a warning. Every Rust vector is
recomputed; see the paragraph at the top.

**Rejections are printed, not swallowed.** A self-edge — a memory related to
itself, which the Rust REST API rejects but its storage engine can hold — and an
orphan edge whose target was hard-deleted are both refused and named. If either
list is non-empty, read it: those relationships are gone from the Go corpus, on
purpose.

### If it is interrupted

Re-run the same command with `--resume`. It picks up the cursor it left beside
the snapshot. Running it from the beginning is also correct and only costs time:
every row is addressed by its own id, so importing a block twice writes the same
bytes.

### If it refuses

| Message | What to do |
|---|---|
| `built without the model` | Rebuild `remem-admin` with `-tags onnx`, as above |
| `not comparable with the ones this binary produces` | You passed `--vectors=verbatim` on a Rust snapshot. Don't; it would give every search a plausible wrong answer |
| `the path exists but does not hold a Remem data directory` | Import into a new path |
| `already exists ... and its <field> differs` | You are importing into a store that already holds different data. Import into an empty directory, or choose `--on-conflict=skip` or `--on-conflict=overwrite` deliberately |
| `unknown snapshot section` / `unsupported snapshot format version` | The file was written by a newer Remem than this binary. Upgrade rather than partially importing |
| `ends inside ... the last complete block is block N` | The file was truncated in transfer. Re-transfer it; the message tells you how much was readable |

## 4. Verify, before anything else touches the store

```bash
./remem-admin verify --data-dir /var/lib/remem-go --in corpus.rsnap
```

```
snapshot: 6686 record(s), 6686 vector(s), 32911 edge(s), 0 event(s)
store:    6686 record(s), 6686 vector(s), 32911 edge(s)
the store holds what the snapshot describes
```

It exits 0 when they agree and **2** when they do not, listing what differs. Two
is neither success nor failure: zero on a store that does not match would make
this useless as a check.

**Run it before you start the server.** A lifecycle sweep decays health, archives
expired memories and moves exactly the fields the verifier compares, so a store
that has been serving legitimately disagrees with the snapshot it was built from.
On a real 6,686-memory corpus this was clean immediately after import and showed
4,864 disagreements after two short server sessions — every one of them the
sweep's own work, and none of them an import defect.

The expected divergence, and the only one: the edges the import refused by name
in step 3 appear as `missing edge`. Their count should equal the self-edges plus
orphan edges the import reported.

## 5. Build the approximate index (optional)

An import does not build the HNSW graph. It queues one rebuild job per tenant,
which the server runs when it next starts; until then the index materialises
lazily and reports `truncated: true`, so a search answers correctly and says it
is degraded rather than answering short. To pay the cost up front instead:

```bash
./remem-admin rebuild --data-dir /var/lib/remem-go --index vector
```

About three milliseconds per memory. Skipping it costs nothing but a slower
first few searches.

## 6. Start the Go server and check it for yourself

```bash
REMEM_SERVER_HTTP_ADDR=127.0.0.1:4545 \
REMEM_SERVER_API_KEY=<your key> \
REMEM_VECTOR_INDEX=hnsw \
REMEM_EMBEDDING_MODEL_PATH=$(pwd)/.models/all-MiniLM-L6-v2 \
REMEM_EMBEDDING_ONNX_LIBRARY_PATH=/opt/homebrew/opt/onnxruntime/lib/libonnxruntime.dylib \
./remem start --data-dir /var/lib/remem-go
```

Four checks worth doing by hand, because they exercise four different things
that could have gone wrong:

```bash
KEY=<your key>; BASE=http://127.0.0.1:4545/api/v1

# The corpus is there, and its lifecycle fields survived.
curl -s -H "Authorization: Bearer $KEY" "$BASE/memories?limit=3"

# All three search modes. search_type is required; there is no default.
for m in semantic keyword hybrid; do
  curl -s -X POST -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
    -d "{\"query\":\"something you know is in there\",\"search_type\":\"$m\",\"limit\":3}" \
    "$BASE/memories/search"
done

# The relationships came across, with their strengths.
curl -s -H "Authorization: Bearer $KEY" "$BASE/memories/<an-id>/connections?direction=out"
curl -s -H "Authorization: Bearer $KEY" "$BASE/memories/<an-id>/related?limit=5"
```

Check the server's log for `WARN`. A clean import produces none.

## 7. Ask for relationships the Rust corpus never had

Relationship discovery runs when a memory is written, so an imported corpus holds
memories nothing ever considered — and nothing notices on its own, because there
is deliberately no "was this discovered" flag to sweep on. With the server
stopped:

```bash
./remem-admin discovery backfill --data-dir /var/lib/remem-go
```

It queues work rather than doing it; the server drains the jobs when it next
starts. Running it twice is safe and is still work.

## 8. Decommission

Only after step 4 was clean and step 6 satisfied you. Keep `corpus.rsnap`: it is
a complete logical backup of the Rust deployment, readable without either
implementation's storage engine, and it is the last thing that will be.

---

## Old data directories

If your Rust deployment predates the current on-disk format, `remem-export`
migrates it forward on open, backing it up first:

```
Backed up "/var/lib/remem" to "/var/lib/remem/.backups/pre-1788905768" before migration index-timeseries-v2
Running on-disk format migration index-timeseries-v2
Running on-disk format migration index-tags-v3
Running on-disk format migration index-attr-v2
Running on-disk format migration attrs-v2
```

**This modifies the directory.** It is the one step in this document that does.
Copy the directory first if you want the option of going back to a Rust binary
that predates those migrations.

This path was exercised on a real deployment four migrations behind
(`index.tags` 2, `index.attr` 1, `attr` 1, `index.timeseries` 1): the chain ran,
6,686 records and 32,911 edges exported, imported and verified clean.

## Going the other way

There is no path from Go back to Rust, and there will not be one. Rust is frozen
for security and correctness fixes and has no importer. `corpus.rsnap` remains
readable — the schema is checksummed in both repositories precisely so it stays
that way — but nothing consumes it on the Rust side.

## Backing up a Go deployment

The same export, without stopping anything: it reads through a pinned snapshot,
so a backup taken from a live server is one point in time.

```bash
./remem-admin export --data-dir /var/lib/remem-go --out backup-$(date +%F).rsnap
```

A Go snapshot carries five things the shared schema cannot: connection metadata,
an edge's update time, a record's archive time, per-tenant schema versions, and
the lifecycle audit stream. `--shared-only` writes the shared sections alone —
useful only if something other than Go must read the file — and prints what that
costs:

```
dropped, because proto/snapshot/v1 has no field for them: metadata on 0
connection(s), 1997 lifecycle audit event(s). Export without --shared-only to keep them.
```

Restoring a Go snapshot needs no model: the vectors were written by this
implementation and are taken verbatim.

```bash
./remem-admin import --data-dir /var/lib/remem-restored --in backup-2026-09-09.rsnap
```

## Upgrading a multi-tenant deployment to a build that serves one tenant

A Remem build whose tenancy capability is `single-tenant` resolves every
operation to one implicit tenant — `tenant.default`, `default` unless you
changed it — and refuses a caller-supplied tenant selector. `remem version`
reports the capability, and `GET /api/v1/health` carries it too.

Such a build **refuses to start** over a data directory holding any other
tenant. That refusal is deliberate: starting and quietly serving only the
implicit tenant would leave the others' memories on disk, unreachable and
unmentioned, and a successful start would look like a successful upgrade.

Find out where you stand first, with the binary you already have running or with
any `remem-admin`:

```bash
./remem-admin tenants inventory --data-dir /var/lib/remem-go
```

It opens the directory read-only, lists what it holds, marks the implicit
tenant with `*`, and exits `2` when there is anything else — so it works as a
pre-upgrade check in a script. Exit `0` means the upgrade needs nothing from
you.

If it exits `2`, each of the other tenants moves into a deployment of its own.
One export and one import per tenant, into **separate directories**:

```bash
for T in acme globex; do
  ./remem-admin export --data-dir /var/lib/remem-go --tenant "$T" --out "$T.rsnap"

  # The destination serves this tenant, so it is the tenant its implicit one is
  # set to. Tenant identities are never renamed by a migration.
  REMEM_TENANT_DEFAULT="$T" ./remem-admin import --data-dir "/var/lib/remem-$T" \
      --in "$T.rsnap" --vectors verbatim
  REMEM_TENANT_DEFAULT="$T" ./remem-admin verify --data-dir "/var/lib/remem-$T" \
      --in "$T.rsnap"                                  # exits 2 on disagreement
done
```

Each destination above is itself a single-tenant deployment — three tenants
becoming three installs, which is what a small deployment usually wants. Set
`tenant.default` (or `REMEM_TENANT_DEFAULT`) on each new server to the tenant it
serves, and every command that touches its directory needs the same value. A
destination that resolves tenants from an authenticated identity takes the same
files and needs no such setting.

Two things about a single-tenant build's `remem-admin` are worth knowing before
you start, because both are deliberate:

- **`export` will read a tenant this build does not serve. Nothing else will.**
  Export is the way out of a directory the server refuses to start over, so
  refusing it there would strand you mid-upgrade with the fix not in the box. It
  is read-only, it changes nothing, and it prints a note saying what it is for.
  `import`, `verify`, `rebuild`, `vector rebuild`, `text rebuild`,
  `discovery backfill` and `inspect` all refuse such a directory, exactly as the
  server does.
- **`import` refuses a snapshot carrying a tenant this build does not serve**,
  whole rather than in part, and before it creates anything — a refused import
  leaves no database behind. Importing a subset would leave you believing you had
  restored your backup, and folding another tenant into the implicit one would
  join two corpora that were never one.

A per-tenant export carries that tenant's records, canonical vectors, edges with
their metadata, lifecycle audit events and directory row, under the tenant
identity it already had. Nothing is renamed and nothing is merged. `verify`
before anything starts serving from the new directory, for the reason
[§4](#4-verify-before-anything-else-touches-the-store) gives: a lifecycle sweep
moves exactly the fields the verifier compares.

Two things this procedure deliberately does not do:

- **It does not rewrite the original directory.** Rollback is the prior binary
  over that directory, unchanged. Keep it until the migrated deployments have
  been serving long enough to trust.
- **It does not merge namespaces, in either direction.** If you roll back, the
  data written into the migrated deployments since cutover stays there; there is
  no supported way to fold it back into a combined directory, because the two
  tenants' key spaces were never one.

What a snapshot does not carry is in
[`docs/architecture/snapshots.md`](architecture/snapshots.md): retention policy
overrides and job rows. Re-apply per-tenant retention overrides on the
destination with `PATCH /api/v1/tenants/{id}/policies` after the import.
