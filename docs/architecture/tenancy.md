# Tenancy and identity

Who a request belongs to, how that is decided, and what stops the answer being
wrong. Spec §60 asks these documents to explain invariants rather than APIs, so
that is what this one does; signatures are in the code.

Landed in Phase 3, on top of the identity types Phase 2 needed for the key
encoding. The edition tenancy boundary is later work and has a section of its
own below; read it before reasoning about the resolution order, because which
of the two policies is in force decides what that order even is.

## What is authoritative

| Concern | Authority | Everything else |
|---|---|---|
| Which tenant a request belongs to | the credential it presented | a header is a request, honoured only for a credential authorised to make it |
| Which tenants exist | the directory rows under `/tenants/*` | nothing infers a tenant from the presence of its data |
| Whether a caller is who they say | `auth.Keyring`, over the configured credentials | no subsystem re-checks; they read the resolved scope from the context |
| The scope of any read or write | the tenant in the `context.Context` | no function below the API layer takes a tenant as a parameter |

## Two tenant policies, chosen at composition

`tenant.Capability` is the policy a build serves under, and there are two. It
has no default value: the two differ precisely in what they refuse, so a
resolver built without one refuses everything rather than picking the
permissive one. That is the whole of the "fail closed" claim — it is a property
of the zero value, not a check somewhere.

| | `single-tenant` | `identity` |
|---|---|---|
| Which tenant a request is for | the configured implicit tenant, always | the authenticated identity's authorised scope |
| A caller-supplied selector | refused, including the implicit tenant's own name | input to the authorisation decision, never authority |
| Tenant enumeration and provisioning | refused by name | an explicitly authorised scope |
| Background work, metrics, snapshots | the implicit tenant only | the resolved or authorised scope |

The build declares its policy in `version.TenantCapability`, behind an
`enterprise` build tag. That tag is deliberately **not** `business`: the
business edition packages observability around the same single-node engine and
contains no identity or authorisation layer, and mapping a release target to a
capability the binary cannot enforce would advertise isolation that does not
exist. `TestTheBusinessTagDoesNotImplyIdentityTenancy` holds the two apart.
`server.WithTenantCapability` is how a composition root that *does* carry an
identity layer states its policy; the tag is only the default for one that does
not.

Both policies use the same engine, the same durable key model, the same
protocols and the same portable snapshot. A tenant identity is present in every
key and every snapshot section whichever policy is in force, so a corpus written
under one is readable under the other and a migration between them moves rows
rather than rewriting them. There is no single-tenant storage format.

### The resolution order under `identity`

1. **A bound credential decides.** A header cannot override it.
2. **A header is honoured only for a credential authorised across tenants**, and
   only for a tenant within its explicit authorised set when it has one.
3. **Otherwise the configured default** — unless the credential has an explicit
   set of more than one tenant, in which case it is told to name one. Falling
   back to the deployment default would serve a tenant the credential may not be
   authorised for at all, and picking the first of the set would make the answer
   depend on configuration order.

There is no fourth step. A resolver with no default refuses rather than
inventing a tenant, because Invariant 1 has no unscoped fallback.

### The resolution order under `single-tenant`

There is one step: the configured implicit tenant. A selector is **refused**
rather than ignored, and that is the important word. Ignoring a header naming
another tenant would answer the request with *this* tenant's memories, which the
caller cannot tell from a correct answer — and a client that gets away with
sending the header against one deployment sends it against the next, where it
names something else. A credential bound to a tenant this build does not serve
is refused too, because it is evidence the directory was serving more than one.

### Authority is stated, not inferred

An unbound credential is not, on its own, authorisation for every tenant that
happens to exist — including one created tomorrow. A credential may carry an
explicit authorised set, written in configuration as `secret@acme,globex`. It is
refused on a third tenant, sees only those two in a tenant listing, cannot
provision outside them, and its metrics scrape carries only their series. One
tenant after the `@` stays a *binding* rather than becoming a set, because a
binding is already a set of one. A credential with no set is the operator
credential, and that it sees everything is a decision a configuration file made.

Step 1 is what makes an issued API key an isolation boundary rather than a
suggestion: a key leaked from an agent exposes exactly the one tenant it was
cut for, whatever headers the holder sends. Step 2 is what lets one process
serve many tenants without a key per request path. Step 3 is what keeps
`remem start --data-dir ./data` a complete experience with no configuration at
all (Invariant 10) — a server that demanded a tenant before it would store
anything would fail the first five minutes.

## Invariant 1 is structural, in four places

"There is no unscoped read path" is not a rule anybody remembers to follow. It
is held by construction at four layers, and each catches what the one above it
would miss:

1. **The key encoding.** There is no builder for a user-space key without a
   tenant, and `keys.RecordChecked` refuses an empty one rather than defaulting.
2. **`tenant.Require`.** Every repository and service method calls it. A context
   with no tenant produces an error, never a zero id — because a zero id builds
   a perfectly valid key that belongs to nobody.
3. **The context, not a parameter.** A tenant that travelled as an argument is
   one a caller can pass wrongly, and adding a method would add a place to
   forget it. The request boundary sets it once.
4. **The metrics.** A metric counting work done on a tenant's behalf carries
   that tenant's label. This is the one that was actually broken during Phase
   3 and caught by running the server: the authentication middleware resolves
   the tenant into a *derived* context, so an observability middleware wrapped
   around it sees the original and labels everything empty. Invariant 1 reaching
   the keyspace and not the metrics is a half-kept guarantee.

### Confinement, for everything the resolver does not reach

The resolver settles which tenant a *request* is for, and that covers HTTP and
MCP. It covers nothing else. Background jobs are scheduled by walking the
directory; metrics label every tenant the directory lists; a snapshot exports
every tenant the directory names; the session sweep visits each of them; a
retention-policy route exists for any tenant the directory can produce. Each of
those is a cross-tenant path with no request behind it, and each asks the
directory.

So the confinement goes into the directory. `tenant.Confine` narrows one to a
single tenant, and the composition root wraps the real directory once and hands
the wrapper to all of them — while keeping the unconfined one for exactly two
callers, the start-up inventory and the migration runner, because a migration
that skipped a tenant's rows would leave them at an old format with nothing
reporting it.

The alternative was an `if capability == SingleTenant` beside every one of those
call sites. It was rejected for the reason one missed site is worse than a
hundred correct ones: a forgotten check is a cross-tenant path, and nothing
fails when it is forgotten.

Through the wrapper, a tenant other than the confined one reads as **absent**
rather than forbidden — the disposition a memory in another tenant already has,
because whether some other tenant exists is not information this deployment has
any business confirming. Creating one is `errs.Forbidden` instead: a caller
asking for it has asked for something this build will not do, and "not found"
would read as a bug in the request.

## The start-up gate

A build serving one tenant refuses to start over a data directory holding any
other. It refuses in `build`, before the migrations, before an index is opened
and before anything listens or any worker starts — so nothing has been written,
the directory is exactly as the operator left it, and rollback is the prior
binary over it rather than a repair.

The alternative was to start and serve only the implicit tenant. That was
rejected because it leaves the other tenants' memories on disk, unreachable and
unmentioned, and makes a successful start look like a successful upgrade.
`tenant.TakeInventory` is the read-only check behind it — it never writes, so
taking it cannot be the thing that creates the implicit tenant — and its refusal
names every tenant it found rather than counting them, because the operator's
next step is one export per tenant and a count does not tell them what to type.
`remem-admin tenants inventory` is the same check from outside, exiting 2 so it
works in a pre-upgrade script. The migration path is `docs/MIGRATION.md`.

### One command is deliberately exempt

`remem-admin export` reads a tenant a single-tenant build does not serve.
Everything that writes — `import`, `rebuild`, the two index rebuilds,
`discovery backfill` — and `verify` and `inspect` refuse such a directory
exactly as the server does.

Export is exempt because it is the way out. A binary that refuses to start and
also cannot export leaves an operator stranded mid-upgrade with their service
down and the fix not in the box, and no data is safer for it: the previous
release's binary reads the same bytes, and the capability is offline, read-only
and available only to somebody who already has the disk. An export naming no
tenant still writes only the implicit one, and one reaching further says out
loud what it is for.

`import` is where the boundary meets a file rather than a request, since a
restore is the one way a tenant can arrive on disk without anything asking the
resolver. A snapshot carrying a foreign tenant is refused **whole**, and before
any database is created: importing a subset would leave an operator believing
they had restored their backup, and folding another tenant into the implicit one
would join two corpora that were never one.

## The edition boundary is a supported-build contract

It is not a cryptographic access control and must never be described as one.
Community is Apache-2.0, which permits a recipient to modify and rebuild it with
whatever capabilities they like; what the boundary states is which capabilities
the officially supported builds ship. Runtime authentication and tenant
authorisation are what actually isolate an identity-scoped deployment, and they
are required there regardless of how the binary was packaged.

### The one cross-tenant path

`tenant.Directory.ForEach`, and nothing else. A guard in `internal/arch` fails
the build when any package outside `jobs`, `snapshot`, `schema`, `tenant`,
`server` and `cmd/remem-admin` calls a method by that name.

The mistake it exists to prevent is a specific one: Rust Remem shipped both
`vector_search` and `vector_search_partitioned`, and once an unscoped variant
exists something eventually calls it.

`internal/jobs` is on that list and Phase 9 is what made it earn its place: the
queue has no key range that spans tenants — the tenant leads every key — so the
job dispatcher and the scheduler must be *told* which tenants exist, and this is
how. It is also why `jobs.Claim` takes a tenant as a parameter rather than
scanning across them, which is the plan correction that phase recorded. The
allowlist is a licence to walk tenants, not a licence to read without a scope.

The guard is syntactic — a call to a method named `ForEach` in a package that
imports `internal/tenant` — because a full type-check of the module would need
`golang.org/x/tools` and this has to be cheap enough to run on every commit.
That imprecision produced a naming rule, and the rule is worth more than the
precision would have been: **`ForEach` is reserved for the cross-tenant path,
and a tenant-scoped walk is called `Scan`.** When `vector.Store` first grew a
scoped iteration the guard failed, and the choice was between an allowlist entry
excusing `internal/vector` from the real rule and a better name. The name won.

## Namespaces exist before the feature does

A namespace is a subdivision inside a tenant — what Rust called a partition. It
is in every key, and it is fixed at `"default"`; nothing in the product exposes
it.

That ordering is deliberate and one-directional. Retrofitting a namespace means
rewriting every key every customer owns, and what actually follows from that
cost, under delivery pressure, is that the namespace model gets built as a
post-retrieval filter — which is precisely the failure tenant partitioning
exists to prevent. The layout is settled first; only the resolver is pinned.

## What a credential is, and what it is not

`auth.Principal` is the thing holding a credential: an agent, a service, a
person's CLI. Plan §II.5 is explicit that this is **not** a level of the
identity hierarchy. Making an agent a containment level would mean a memory
belongs to one agent, and shared memory across an agent fleet is the normal case
rather than the exception. If per-agent ownership is ever wanted it is a field
on the record, not a level of the keyspace.

`internal/tenant` never learns that credentials exist. `auth` imports `tenant`,
so the resolver could not take an `auth.Principal` without a cycle; the
middleware converts a verified principal into a `tenant.Claim` and the resolver
reads only that.

Verification is constant-time over every configured credential and does not
short-circuit on the first match, so response time reveals neither how many
credentials exist nor how far down the list one sits. Secrets are held as their
SHA-256, so a heap dump of a running server does not hand over every API key.

## Provisioning

`tenant.Ensure` is a function over the interface rather than a fifth method: it
is a policy composed from `Get` and `Create`, and an implementation free to
provide it would be free to make it mean something subtly different. A `Create`
race is treated as success and re-read — two requests provisioning the same
tenant at once is the normal case for a fresh deployment, not an error either
should see.

The configured default tenant is provisioned at startup rather than on the first
request, so a fresh directory serves immediately rather than provisioning at a
moment nobody chose.

## Failure semantics

| Situation | Answer | Why |
|---|---|---|
| A record in another tenant | `NotFound`, never `Forbidden` | "you may not see this" confirms it exists, which is itself a cross-tenant disclosure |
| An unknown credential | `Unauthorized`, with a challenge | without `WWW-Authenticate` a client cannot tell "you sent none" from "yours was rejected" |
| A header from an unauthorised principal | `Forbidden` | the credential is valid; the request is not |
| A tenant directory row that will not decode | `Corruption` | reporting it absent would let a request proceed against a tenant whose metadata could not be read — its schema version included |
| Administering tenants without a cross-tenant credential | `Forbidden`, not `NotFound` | unlike a memory, the existence of the endpoint is not a secret, and hiding it makes a misconfigured key look like a routing bug |

## The seam for per-tenant schema versions

`tenant.Meta.SchemaVersion` is persisted from the first version, defaulting to
1, and nothing reads it before Phase 13. It is there so that spec §19.3's
per-tenant user schema is additive later rather than a migration of every tenant
row — and the rule that must hold from the beginning is that moving a tenant's
schema version never moves Remem's storage format version, or the reverse.

The row is protobuf rather than a fixed-width record for one reason: a rolling
upgrade runs mixed binaries against one directory, and an older binary that
reads a newer tenant row and writes it back must not destroy the fields it never
heard of.

## Sessions

An MCP session is canonical but expendable, and it is the one place in Remem
where a decode failure is reported as *absent* rather than as corruption. That
is what expendable means here: nothing is lost by telling a client to
re-initialise, where refusing its request would strand it.

Sessions are stored rather than held in memory. Losing one costs a
re-initialise, not data — but an in-memory registry strands every connected
agent on a rolling restart, and a row per session is a rounding error against
the memories beside it.

## Canonical, derived, and atomicity

**Tenant rows are canonical, and nothing is derived from them.** A tenant is one
row in `internal/tenant/tenantkv`. It holds the tenant's metadata, including the
per-tenant schema version, which is 1 for every tenant today. No index is built
over tenant rows, so there is nothing to rebuild. A snapshot carries them, and
that is how they are restored.

**Creating a tenant is one conditional commit.** `tenant.Ensure` reads, then
creates, and treats a lost race as success and re-reads the winner's row. Two
requests provisioning a fresh tenant at once is the ordinary case for a new
deployment, not an error either should see.

**Sessions are canonical but disposable.** A session row lives under its
tenant's key prefix, expires, and is swept by the server through
`tenant.ForEach`. Losing one costs a client a new `initialize`, and nothing else.

## What this phase deliberately does not decide

- **Roles and permissions.** A principal is either bound to a tenant or
  authorised across tenants. There is no read-only credential, no per-tool
  permission and no delegation, because nothing yet needs one and a permission
  model built before its first requirement is a permission model that does not
  fit it.
- **Namespace resolution.** The layout is settled; the product decision is not
  taken.
- **Tenant deletion.** `TenantRange` makes it a single range operation, and the
  operation is not written: erasure has retention and audit consequences that
  belong with the snapshot and event subsystems.
