# Observability

What the server says about itself: its metrics, its logs, and the rules that
keep either one from becoming a leak or a lie.

## What is authoritative

Nothing here is. Metrics and logs are observations *of* the canonical state,
never inputs to it, and nothing reads them back to decide anything.

- **Metrics are rebuilt from zero at every start.** Counters reset and gauges
  are re-sampled. A process restart is a counter reset, which Prometheus's
  `rate()` already expects.
- **They belong to `internal/obs`.** Only that package constructs a registry or
  a logger. Subsystems are handed their slice of the metrics and record into it.

## The metric set

Twenty-eight families, each with the prefix `remem_`. `Metrics.DeclaredNames()`
lists them from the registered collectors themselves, rather than from a second
list that could drift.

| Subsystem | Families | Labels |
|---|---|---|
| API | `api_requests_total`, `api_request_duration_seconds`, `api_requests_in_flight` | tenant, route, method, status on the counter; route and method on the histogram; surface on the gauge |
| Storage | `storage_writes_total`, `storage_reads_total`, `storage_scan_duration_seconds` | tenant, keyspace |
| Storage engine | `storage_size_bytes`, `storage_compactions_total`, `storage_cache_accesses_total` | component (and result, for the cache) |
| Search | `search_duration_seconds`, `search_candidates_considered`, `search_truncated_total` | tenant, mode |
| Jobs | `jobs_pending`, `jobs_running`, `jobs_failures_total`, `jobs_retries_total`, `jobs_processing_duration_seconds` | tenant, kind |
| Lifecycle | `lifecycle_transitions_total`, `lifecycle_sweep_duration_seconds`, `lifecycle_due_backlog` | tenant (and kind, for transitions) |
| Discovery | `discovery_subjects_total`, `discovery_edges_created_total`, `discovery_candidates_considered`, `discovery_run_duration_seconds` | tenant (and outcome, for subjects) |
| Migration | `migration_running`, `migration_progress_ratio`, `migration_records_remaining`, `migration_failures_total` | migration |

## Labelling rules

**A series carries a tenant when it describes one tenant's work**, and not
otherwise. Requests, storage operations, searches, jobs, lifecycle and discovery
are each done on a tenant's behalf, so each carries its tenant. That is also why
a scrape lists every tenant served, and why `/metrics` needs a credential not
bound to one (`delivery.md`).

**Why engine facts carry no tenant.** Disk size, compactions and block-cache
hits describe the one Pebble store every tenant shares. There is no honest way
to split a compaction between tenants, and a per-tenant share would be a number
that looks measured and was invented. They carry a component instead.

**Routes are patterns, never paths.** A route label is the matched pattern
(`GET /api/v1/memories/{id}`), not the requested path. One series per memory
would take a metrics store down within a day.

**The request-duration histogram has no tenant.** Buckets multiplied by tenants
multiplied by routes is the cardinality a histogram cannot afford. The counter
beside it keeps the tenant.

**Storage operations are counted by a decorator** over `storage.KV`
(`internal/storage/metered`). It reads the tenant and keyspace from the key
bytes, so no caller can forget to count. Measured in Stage 2: about 70 ns and no
allocation per operation.

## The coverage guard

**A declared family that nothing records is invisible**, because a family with
no samples does not appear in a scrape. The storage and search collectors were
registered in Phase 3 and scraped empty for ten phases, and nothing noticed.

`TestEveryDeclaredMetricIsRecorded` (`internal/server/metrics_coverage_test.go`)
drives a real server through an ordinary workload and requires every declared
family to have samples. Its allowlist of families an ordinary workload cannot
produce fails the test if one of them turns up after all, so the list cannot
grow into a list of excuses. The Grafana dashboard is held to the declared
names by `internal/obs/dashboard_test.go`, compiled with the business edition
that ships the dashboard.

## Versions

Metric names and labels are an interface: the dashboard, alert rules and
operators' queries name them. Renaming a family or a label breaks them silently,
so a rename is a change to announce, and the dashboard test catches the ones
this repository owns.

## Failure semantics

- **A failed scrape never affects serving.** The scrape handler reads the
  registry, and nothing on the request path waits for it.
- **The job gauges are refreshed every 30 seconds**, by the worker pool. The
  first refresh comes 30 seconds after start, so a dashboard shows no queue
  depth for the first half-minute after every restart. Known and left.
- **The in-memory storage engine reports no engine facts at all**, rather than
  a row of zeros that would read as a measured idle engine.

## Serving the metrics

`GET /metrics`, behind a credential not bound to one tenant, and not rate
limited. `metrics.enabled = false` mounts nothing, `metrics.path` moves it, and
`metrics.addr` serves it on a listener of its own, off the main one. Until Phase
13's security review the server read none of the three (`docs/SECURITY_REVIEW.md`).

## Logs

**Only `internal/obs` constructs a logger.** `TestOnlyObsConstructsLoggers`
enforces it by scanning for the constructor calls.

**Memory content is never logged**, and a test enforces that too. Identity
travels in the context, so a log line names the tenant, the request id and the
route, and never what a memory says. The whole configuration is logged once at
start, with secrets redacted.
