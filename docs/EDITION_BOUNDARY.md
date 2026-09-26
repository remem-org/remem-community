# Edition boundary: tenancy and the shared engine

## Status

**Implemented.** What this document recommended is what the code now does; the
"Rollout" section below records what each step turned into. The architecture is
`docs/architecture/tenancy.md`, and the upgrade procedure for a deployment
serving several tenants is `docs/MIGRATION.md`.

A build reports its policy in three places: `remem version`, the start-up log
line, and `GET /api/v1/health` as `tenancy`. Every officially supported build
today reports `single-tenant`.

## Recommendation

Keep tenant identity in the common engine, storage model, snapshot format, and
protocols. Define Community's supported contract as one tenant per installation
and make Enterprise / Cloud responsible for selecting and authorizing multiple
tenants. Do not create a second single-tenant storage engine or remove tenant
fields from shared formats.

This keeps persistence, backup and restore, search, graph traversal, lifecycle,
and memory semantics common across editions. Enterprise can add identity and
control-plane capabilities around the same tenant-aware engine, while its
distributed layer can build on the same durable data model.

## Edition behavior

Community should use one implicit tenant, such as `default`, for all supported
operations. Its public HTTP and MCP surfaces should not accept a caller-chosen
tenant or expose cross-tenant administration. Tenant IDs should remain in
internal keys and portable snapshots, with Community snapshots containing only
the implicit tenant. Shared APIs and snapshot schemas should remain capable of
representing tenants so Enterprise does not need a format fork.

Enterprise / Cloud should map an authenticated principal to a tenant through
its identity and authorization layer. Every operation must carry that resolved
tenant into the shared engine. Multi-tenancy is complete only when isolation
covers reads, writes, search, graph traversal, jobs, metrics, backups, restore,
and administrative operations—not merely when requests accept a tenant ID.

## Rollout: what each step became

The four steps below were the plan. This is what shipped for each.

1. **An edition capability in the composition layer.** `tenant.Capability`, with
   two values and no default — a resolver built without one refuses everything,
   because the two policies differ in what they refuse and picking the
   permissive one would be an isolation decision nobody made. The build declares
   its policy in `version.TenantCapability`, behind an `enterprise` build tag
   that is deliberately not `business`: see "Editions and capabilities" below.
2. **An audit of every surface.** HTTP and MCP share one authentication and
   resolution step, so the header is refused on both at once; the
   retention-policy routes name a tenant in the path and are answered by the
   confined directory; background jobs, the scheduler's fan-out, the session
   sweep, the metrics labels and the snapshot's tenant list all read the
   directory, which is where the confinement lives (`tenant.Confine`). No tool
   or route takes a tenant argument, and a test asserts that of the tool schemas
   rather than of the current behaviour.
3. **Contract tests on both sides.** A single-tenant surface refuses a selector
   on eight surfaces including `/mcp`, refuses tenant administration by name,
   reaches no other tenant through any of the ten MCP tools, and exposes one
   tenant's series to a scrape. An identity-scoped surface carries an authorised
   scope through every operation, refuses a tenant outside it before reading
   anything, keeps a bound credential bound, narrows a tenant listing and a
   scrape to the authorised set, and will not provision outside it.
4. **An explicit migration path.** `remem-admin tenants inventory` reports what
   a directory holds and exits 2 when a single-tenant build could not serve all
   of it; the server refuses to start over such a directory before writing
   anything; and the per-tenant export/import path is exercised end to end
   against real Pebble directories. `docs/MIGRATION.md` is the procedure.

### Export is the boundary's escape route

A single-tenant build's `remem-admin export` will read a tenant that build does
not serve. Every command that writes refuses such a directory, as the server
does.

This is a deliberate exception, and the reason is operational rather than
technical. A binary that refuses to start and also cannot export strands an
operator mid-upgrade with their service down and the fix not in the box. No data
is safer for the refusal — the previous release's binary reads the same bytes, and
the capability is offline, read-only, and available only to somebody who already
has the disk. An export naming no tenant still writes only the implicit tenant,
and one reaching further prints a note saying what it is for.

`import` is the other side and refuses: a snapshot carrying a tenant this build
does not serve is rejected whole, before any database is created. A subset would
leave an operator believing they had restored their backup.

### Editions and capabilities are separate axes

| Build | Edition | Tenancy | Why |
|---|---|---|---|
| default | `community` | `single-tenant` | the open-source core |
| `-tags business` | `business` | `single-tenant` | packages Prometheus and Grafana observability, and no identity layer |
| `-tags enterprise` | — | `identity` | the seam for a build that carries an identity and authorisation layer |

The `business` tag does **not** imply identity tenancy, and
`TestTheBusinessTagDoesNotImplyIdentityTenancy` fails if it ever does. A release
target is mapped to a capability only when the build actually contains what that
capability requires; mapping it by product tier would advertise tenant isolation
the binary has no way to enforce. The Enterprise / Cloud identity provider,
account lifecycle and role model are not in this repository, so
`server.WithTenantCapability` is how a composition root that has them states its
policy, and the default remains the one a build with none can honour.

The edition switch is a supported-build boundary, not a cryptographic access
control. Community is Apache 2.0, which permits recipients to modify and
redistribute it under the license terms. The product distinction should be the
officially supported capabilities and the commercial Enterprise / Cloud
services, not a claim that the Community license prevents adding features.

## Alternatives considered

- **Remove tenancy from Community's storage and snapshot formats:** rejected.
  It forks core data handling and makes future upgrades, backups, and common
  protocols harder to share.
- **Keep current unrestricted multi-tenancy in Community and differentiate only
  by hosted services:** simplest technically, but it does not implement the
  requested single-tenant Community contract.
- **Keep a tenant-aware core and enforce a one-tenant Community capability:**
  recommended. It preserves one engine and makes the edition boundary visible
  at the public surfaces where tenant selection matters.
