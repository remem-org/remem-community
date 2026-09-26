# Delivery: HTTP, MCP, and the composition root

The surfaces a caller reaches and the process that owns them. Spec §60 asks
these documents to explain invariants rather than APIs, so that is what this one
does; the endpoint list is in `internal/api/http/router.go` and the tool list in
`internal/api/mcp/tools.go`.

Landed in Phase 3.

## What is authoritative

| Concern | Authority | Everything else |
|---|---|---|
| What an error means | `errs.Kind` | `internal/api/http` maps a kind to a status, in one table; nothing below knows HTTP exists |
| What a request is allowed to do | the credential, resolved once in middleware | no handler re-checks; they read the scope from the context |
| What a client receives | the response types in the api packages | they are separate from the domain types on purpose |
| Whether the process may serve | readiness, which is not liveness | an orchestrator routes on the first and restarts on the second |

## Both surfaces share one door

The MCP handler is mounted *inside* the REST router, not beside it. Both then
pass through one authentication step and one tenant resolution.

This is the load-bearing decision of the delivery layer. An MCP client that
could reach a tenant the REST API refuses would be an isolation hole with two
doors, one of them unreviewed — and two authentication paths drift, because only
one of them is the one people look at.

## Rate limiting is per tenant, after authentication

Every authenticated route and `/mcp` draw on one token bucket per tenant:
`server.rate_limit_rps` sustained (default 100) and `server.rate_limit_burst` at
once (default 50). Zero rps turns it off. A refusal is a 429
`urn:remem:problem:rate_limited` with a `Retry-After` computed from the actual
time to the next token, rounded up to a whole second so that the retry it invites
is not refused again.

**Rust used the same numbers and a different key: the client address.** That fit
a server whose isolation unit was the process. Go's is the tenant, and keying by
address gets both directions wrong — two tenants behind one proxy share an
address and starve each other, and one tenant spread over many agents escapes the
limit. So the limiter sits *inside* `authenticate` and counts against the tenant
the request resolved to.

Two consequences follow from that placement, and both are tested:

- **A refused credential costs a tenant nothing.** It never reaches the limiter,
  so fifty bad keys naming a tenant cannot lock out its real clients. A flood of
  bad credentials is bounded by what refusing one costs — a SHA-256 and a map
  lookup — rather than by this limiter.
- **REST and MCP share one allowance.** One limiter serves the whole router, so a
  model in a loop spends the same budget its operator's scripts do.

The bucket map is bounded (10,000 tenants, least recently used out). Forgetting a
tenant hands it a full bucket, which is at most one burst of extra allowance —
the price of a map whose keys a caller influences not growing without limit. The
limiter reads the injected clock, which is why its tests assert refills to the
millisecond.

## `/metrics` needs an operator credential

Every per-tenant series carries a `tenant` label, so a scrape is a list of every
tenant the server has served — cross-tenant information, and therefore behind the
same rule as the administration routes: a credential that is not bound to one
tenant. Anonymous is 401, a tenant-bound key is 403. Until Phase 13 the route was
mounted without authentication at all, although Appendix B had always said
"authenticated".

A Prometheus scrape configuration therefore needs `authorization: credentials:`
(or `credentials_file`) set to an operator key; `config/prometheus.yml` does, and
the business compose file supplies the key as a Docker secret. `metrics.enabled = false`
mounts no endpoint at all, and `metrics.path` moves it; a scrape configuration
has to follow a moved path with `metrics_path`. `metrics.addr` serves the
endpoint on a listener of its own and removes it from the main one, so it can be
kept off a public interface; that listener answers nothing else, and still asks
for the operator credential. Until Phase 13's security review
the server read none of the three, so `/metrics` was served on the main listener
whatever an operator had configured. The scrape is deliberately not rate limited — a scrape refused while the
default tenant is busy is a monitoring gap at exactly the moment it matters.

## Index and migration administration

Three routes beside the job routes, under the same rule — a credential not bound
to one tenant, acting on the tenant `X-Remem-Tenant` names:

- `GET /api/v1/admin/indexes` reports every **derived** key space for the tenant,
  walked from `keys.AllSpaces`, so a derived space added later appears without a
  handler change. Each carries the name `rebuild` takes, the job that rebuilds it,
  and a state. The vector and keyword indexes keep a health signal of their own
  and report `ok`, `degraded` or `rebuilding`, with a count. The attribute rows,
  the attribute index and the in-edges keep none, and report **`unmonitored`
  rather than `ok`**: a health nobody checked is not a health, and
  `remem-admin inspect check` is how they are examined.
- `POST /api/v1/admin/rebuild` with `{"index": "vector" | "text" | "attr" |
  "graph-in" | "all"}` queues the rebuild jobs, as `run` queues one. It queues
  what it can: a job type already waiting or running for the tenant is reported
  under `already_outstanding` rather than queued twice, and is not a reason to
  refuse the others. Only a request that could queue nothing is a 409. An unknown
  name is a 422 naming the ones that exist.
- `GET /api/v1/admin/migrations` lists the directory's migration state rows —
  state, records processed, the versions moved between, and the error that
  stopped a failed attempt. The rows belong to no tenant, so the tenant header is
  ignored.

The composition root supplies all three through `http.AdminDeps`, because only it
holds the indexes, the store and the mapping from a space to its rebuild job; the
delivery layer renders and decides nothing.

## Errors

Every error response is an RFC 9457 problem document, including the ones a
framework would normally produce itself: a client that has to handle two error
shapes handles one of them badly. A mistyped URL gets a problem document, not
`404 page not found`.

The kind-to-status mapping is a table rather than a switch, so that adding an
`errs.Kind` without deciding its status is a visible omission rather than a
silent fall-through to 500.

### 400 and 422 are different answers

A body that will not parse is a malformed request — 400. A body that parses into
something the server refuses to process is 422. A malformed id in the path is
400 for the same reason: the request line itself is wrong.

Rust returned 422 for both. The distinction matters to a client deciding whether
to retry with different data or fix its serialiser.

### An internal failure does not explain itself

A kind marked internal has its message replaced with a generic sentence and the
real one logged against the request id. Below the API layer an error can name a
storage path, a key, or a tenant, and spec §58 forbids letting one through. A
test closes the store and asserts the response mentions none of `memkv`,
`pebble`, "closed" or "store".

Every problem document carries the request id, and every response echoes it in a
header. That is the one string that turns a user's report into a log line.

## Two disclosure rules

**A memory in another tenant is 404, never 403.** "You may not see this"
confirms it exists, which is a cross-tenant disclosure by itself.

**An administration endpoint is 403, not 404.** Unlike a memory, the existence
of the endpoint is not a secret, and hiding it would make a misconfigured
operator key look like a routing bug. This covers `/api/v1/tenants` and, since
Phase 9, `/api/v1/admin/jobs`: both require a credential that is not bound to
one tenant.

**A refusal names what the caller asked for.** The Phase 9 verification run met
the version that did not: a credential asking about background jobs was told
"administering *tenants* requires a credential that is not bound to one tenant",
because the helper was written for the tenant routes and reused. A shared
refusal must take its subject from the request, or the second surface to use it
sends operators to look at the first.

## Some smaller decisions, each with a reason

- **Search is POST.** Not REST aesthetics: a query is user content, and a GET
  would put it in access logs, proxy logs and browser history.
- **An update is PATCH, where Rust has PUT.** The body is partial and every
  field is a pointer, so absent means "leave this alone" — which is what PATCH
  means and what a PUT cannot say. Under PUT, "correct this one typo" drops
  every field the client did not restate. `"tags": []` still clears the tags:
  nil and empty have to mean different things, which is why the field is a
  pointer to a slice rather than a slice.
- **Updating an archived memory is 409, not 404.** The memory is there and the
  caller can still fetch it with `include_archived`; archiving is a retirement,
  and an edit that silently resurrected one into search would be the "why did my
  memory come back" counterpart of the question archiving exists to answer.
  Un-archiving is a separate, explicit verb and nothing has asked for one.
- **Unknown JSON fields are refused.** A client that sends `{"contnet": "..."}`
  and receives 201 has stored an empty memory and will find out much later.
- **Response types are separate from domain types.** The wire shape is a
  compatibility surface with clients; the domain type is not. Adding a field to
  one must not silently add it to the other.
- **A list response is an object.** Adding a count, a warning or a
  partial-failure report later is then not a breaking change; a bare JSON array
  has nowhere to put one.
- **`truncated` is always present, even when false.** A flag that appears only
  when set is a flag clients forget to check.
- **The route metric label is the registered pattern, never the path.** A path
  holds record ids, and one time series per memory takes a metrics store down
  inside a day.
- **A panic becomes a 500, not a dropped connection.** A dropped connection is
  reported by a client as a network error, sending an operator to the load
  balancer for a bug that is in this process.

## Paging: two kinds of cursor behind one token

Five surfaces return a `next_cursor` — listing, search, `related`, history and
connections — on REST and on MCP alike. A caller sees one opaque, versioned,
tenant-bound token everywhere and never learns which kind it holds. The kinds
differ in the one way a caller can observe:

| Cursor | Surfaces | After a restart | Left idle |
|---|---|---|---|
| **Keyset** — a durable position in a key range | history, connections | Resumes | Resumes, indefinitely |
| **Session** — a position in a ranking or snapshot held in memory | search, `related`, listing | Refused | Refused after 60s (search, `related`) or 5 min (listing) |

A refused cursor is `errs.Invalid`, and the message ends by telling the caller to
start again, because the remedy is a fresh first page and not a retry of the same
token. It is never a 404: nothing is missing, the token is simply not valid *for
this request*, and "not found" would suggest presenting it somewhere else might
work. A cursor minted in one tenant is refused in every other, whichever kind it
is.

The mechanisms are in `query.md`'s "Ranked paging" and `graph.md`'s paging
sections. What belongs here is the contract every paged response keeps:
`has_more` is always present, `truncated` is always present, and they are
different facts. `SearchResponse` grew `has_more` in Phase 13, which is a contract
change worth naming — a client that inferred "that was everything" from a short
page must now read the flag.

**`recall` is deliberately not paged.** Its continuation is `already_have` plus
`token_budget`, and paging a budget is a category error: the second page of a
budgeted recall is a recall with the first page's ids excluded, which is exactly
what `already_have` already says. A cursor would be a second way to ask one
question, with a session pinned to answer it.

## Liveness and readiness answer different questions

Liveness checks that the HTTP loop is answering, and *nothing else*. A liveness
probe that consulted the store would restart a healthy process whenever the
store was briefly slow, turning a degradation into an outage.

Readiness is false before `Run` and false again after it returns. A process that
is up but not yet serving must fail readiness and pass liveness, or an
orchestrator restarts it in a loop while it is trying to start.

## MCP

### Two session errors, meaning opposite things

No session id means the client never initialised — a bug in the client, 400. An
id the server does not know means the session expired, or was served by a node
that has since forgotten it — normal operation meaning "initialise again", 404.

Serving 400 for an expired session strands every client that idled past the
sweep: it retries with the same dead id forever, because nothing ever told it to
start over. A malformed id gets the 404 too — it also cannot name a live session,
and the client's remedy is the same.

### A tool that failed is not a protocol error

A tool that ran and failed returns a *successful* JSON-RPC response with
`isError` set; an unknown tool is a JSON-RPC error. A memory that does not exist
is not a protocol fault, and reporting it as one makes a client say "Remem is
broken" instead of showing the model something it can act on.

**But `isError` is only half of it: the text has to be actionable too.** Below
the tool layer an error can name a storage path or a key, so `toolErrorText`
shows the model the real message for `NotFound`, `Invalid`, `Conflict`,
`Unauthorized` and `Forbidden`, and replaces everything else with one generic
sentence. That means an error reaching it *unclassified* is reported as a server
failure — and an unclassified argument-decode failure did exactly that on every
tool, telling a model Remem had broken when what had happened was that the model
misspelled an argument. That is the one reading that makes a model stop trying
rather than fix its call. A decode failure is `errs.Invalid`, which is both the
truthful kind and the one that reaches the model.

### The tools, and what each is for

Ten, and the count is asserted in the schema test rather than merely iterated,
so a tool added without a schema review fails there. `TestEveryMCPToolHasATest`
then requires every tool's name to appear in some test: a tool nobody calls is a
schema nobody has checked against its handler, and the two drift silently into a
model that "cannot use the tool".

| Tool | For |
|---|---|
| `store_memory` | One fact worth recalling in a later conversation |
| `store_memories` | Several at once: one write, one embedding pass |
| `get_memory` | Fetch by id, optionally with the relationships it starts — up to 200, with `connections_has_more` saying when there were more. This is the only read that records a recall |
| `list_memories` | What is stored, in one of five orders — newest first by default |
| `update_memory` | Change a stored memory, partially |
| `delete_memory` | Archive, or remove with `hard` |
| `search_memories` | Find memories. `search_type` is required; filters conjoin |
| `find_related` | The memories connected to one memory, strongest path first |
| `recall` | Fill a context budget before a turn |
| `memory_history` | What happened to one memory, and why |

**There is no `promote_to_longterm`.** Promotion is `update_memory` with a
`policy`, and a second tool onto one field is a second tool description in every
context window. The field also generalises — to `pinned`, and to any policy a
tenant defines — in a way a verb named after one destination never could.

**`update_memory` takes no `health`.** Health is the lifecycle's own number,
computed by decay from the record's state; a caller who writes 100 into it has
cancelled the forgetting curve for that memory with nothing recording that they
did. Rust accepts it on store and update, and that is a gap in Rust rather than
a feature to port. It is refused *by name* rather than ignored, which costs
nothing because `decodeArgs` already disallows unknown fields — a model that
guessed at it learns that it guessed.

### Five things Rust's MCP surface has, deliberately absent

Recorded with reasons rather than left as silent absences, because an absent
feature with no stated reason reads as an oversight — the disposition
consolidation (REM-113) already has.

- **`promote_to_longterm`** — a `policy` field on `update_memory`, above.
- **`health` on store and update** — the lifecycle's own number, above.
- **`graph_extraction` on `store_memory`** — REM-115. Inline extraction is a
  latency cliff on the write path and Rust's failure mode is silent and partial;
  a durable job introduces a race the feature's own workflow can see.
  `discovery.md` records both shapes and why neither is obviously right. The
  field does not exist, so nothing is silently ignored.
- **`memory://stats`** — Rust's walks the whole corpus and loads every record to
  count it (`health.rs:99-127`), and a resource is content a model loads
  unprompted, so it would be an O(corpus) read charged to every conversation.
  `/metrics` carries the counts, per tenant.
- **`memory://collections/recent` and `memory://collections/important`** —
  `list_memories` with `order_by` is the same query, asked for deliberately
  rather than loaded unprompted. Memories are not resources (below).

### Context is a cost the model pays

Search results carry the score the model can act on and a compact list of the
indexes that matched. The per-source breakdown appears only with
`explain: true`. This is Rust's contract kept deliberately (behaviour baseline
§7): every field is context paid for on every call.

Resources describe the server, never the user's memories. A resource is content
a model loads unprompted, and exposing memories that way would put a tenant's
data into every conversation whether or not it was relevant — which is what
search is for.

### The stdio bridge is a pipe

`cmd/remem-mcp` forwards stdin to a server and writes the responses back.
Embedding the engine in it would mean two processes writing one data directory
whenever a user also ran the server, which is the single thing a storage engine
cannot survive.

It holds exactly one piece of state — the session id, captured from initialize
and replayed, because the stdio protocol has nowhere to put a header. Responses
are forwarded verbatim rather than re-encoded, so a field this build does not
know about is not dropped from a protocol that is still moving. Diagnostics go
to stderr, never stdout: one stray line on stdout corrupts the client's parse
for the rest of the session.

## The composition root

`internal/server` is the only package that constructs anything concrete.
Everything below it takes interfaces and has no opinion about which
implementation it is given.

**Everything that can refuse, refuses in `New`** — before `Run` listens. A
server that has bound its port is one an orchestrator may believe. A failed
`New` releases the data directory, or an operator who fixes the configuration
and restarts meets a second, unrelated failure.

**Every goroutine has an owner** (spec §57). Four long-lived ones exist — the
HTTP listener, the session sweeper, and since Phase 9 the job pool and the job
scheduler — and every one is started by `Run`, watches the same context, and is
waited for before `Run` returns.

The pool's shutdown is the one with a budget of its own: it cancels running
handlers, gives them `jobs.drain_timeout` to checkpoint and return, and releases
whatever comes back to the queue without charging an attempt. A process stopping
is not a job failing.

Shutdown is graceful first, then forced at the configured timeout. A shutdown
that can be delayed indefinitely by one slow client is a shutdown an
orchestrator turns into a kill. The same reasoning bounds
`embedding.Service.Close`: a wedged model run must not consume the entire
shutdown budget, so it waits five seconds and then reports failure. A leaked
goroutine in an exiting process is a smaller problem than a process that will
not exit.

## Configuration compatibility

`REMEM_API_KEY`, `REMEM_DATA_DIR` and `REMEM_ENV` are in every existing Rust
deployment, compose file and MCP client configuration. Go's strict
`REMEM_SECTION_KEY` scheme refused all of them, which would have made the first
thing a Go server did be to break a working deployment. They are aliases.
`REMEM_RATE_LIMIT_RPS` and `REMEM_RATE_LIMIT_BURST` joined them in Phase 13, when
Go gained a limiter; until then they were refused as having no equivalent.

Variables with no Go equivalent still refuse startup and say why — every one of
them is security-relevant, and an operator whose CORS restriction silently
stopped being enforced would find out from an incident rather than a message. A
one-time edit is the cheaper outcome. The typo rule survives for everything
else: a misspelled setting silently ignored is a setting an operator believes is
in force and is not.

`remem-mcp` uses `REMEM_MCP_*`, which `internal/config` reserves and ignores.
The bridge and the server are routinely launched from one shell.

## State, atomicity and versions

**The delivery layer holds no durable state of its own.** Everything it serves
belongs to a service. MCP session rows belong to `internal/session`. Search
paging sessions live in process memory in `internal/memory`, and a restart
drops them, so their cursors are refused by name afterwards.

**Its atomicity is the service's.** A request is one service call, and a batch
store is one transaction. No request spans two transactions that a failure could
leave half-applied.

**Three things are versioned, and each is refused rather than guessed at when
unknown:**
- **The REST surface**, by its `/api/v1` prefix.
- **MCP**, by the protocol version negotiated in `initialize`.
- **Page tokens**, by a version byte (`codec.TokenVersion`, currently 3). A token
  minted by an older binary is refused, not misread.

## What this phase deliberately does not decide

- **CORS and proxy-header trust.** Rust had both; Go has neither, and says so
  rather than pretending. They belong with a deployment story that has not been
  written. Rate limiting was on this list until Phase 13 — see "Rate limiting is
  per tenant, after authentication" above. Proxy-header trust matters less to Go
  than it did to Rust for exactly that reason: Rust needed the forwarded address
  to key its limiter, and Go keys by tenant.
- **Pagination and listing endpoints.** They arrived with the attribute store
  that makes ordering by a mutable field indexable (Phase 5), and on the ranked
  and graph surfaces in Phase 13 — see "Paging" above.
- **SSE on the MCP transport.** Nothing streams. An SSE frame around a single
  response is ceremony a client still has to parse.
- **Roles.** See `tenancy.md`.
