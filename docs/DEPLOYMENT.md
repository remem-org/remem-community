# Deploying Remem with Compose

Where the corpus lives on the host, how to point it somewhere else, and how to
move an existing deployment off the Docker-managed volume it used to sit in.

`docs/MIGRATION.md` is a different document: that one is moving from Rust Remem
to Go. This one is about a Go deployment's own storage.

## The data directory

Remem's data directory is `/var/lib/remem` inside the container, and that does
not change. What is configurable is the host directory mounted there:

```bash
# .env, beside the compose file
REMEM_DATA_DIR=/srv/remem/data
```

Every production compose file mounts `${REMEM_DATA_DIR:-./remem-data}` at
`/var/lib/remem` — the `docker-compose.yml` an edition ships, and the
community and business files a development checkout keeps beside it. It is a
bind mount, not a named volume, so:

- the corpus is an ordinary host directory, backed up and inspected with
  ordinary host tools;
- `docker compose down -v` does not reach it. It still removes the named volumes
  that remain in the business stack — Prometheus, Grafana, the console's
  PostgreSQL and Redis — and none of those hold a memory;
- `remem-admin` can be pointed straight at it when the stack is stopped, which
  is what makes `graph verify`, `inspect check` and `export` usable on a
  deployment rather than only on a checkout.

### How the path is resolved

**An absolute path is used exactly as written.** That is what a managed server
usually wants, and it is the form to prefer when anything else on the host also
refers to the directory — a backup job, a monitoring check.

**A relative path resolves from the Compose project directory**, which is the
directory holding the compose file unless `docker compose` was given
`--project-directory`. `./remem-data` in a checkout is therefore the checkout's
own `remem-data/`, which `.gitignore` excludes. Running `docker compose` from
elsewhere with `-f /path/to/docker-compose.yml` moves the project directory and
therefore moves the default. If that is a deployment anybody else will operate,
set an absolute path and remove the ambiguity.

**The setting may be absent entirely.** The compose fallback `:-./remem-data`
means a deployment with no `.env` at all still has one deterministic answer
rather than an error. `.env.example` sets the same value explicitly, so copying
it changes nothing.

### Ownership and permissions

The container runs as the unprivileged user `remem`, **uid 10001**
(`docker/Dockerfile`). The host directory must be writable by that uid or the
server cannot open its database.

Compose creates a missing bind-mount source itself, as root, which the server
then cannot write to. Create it first:

```bash
mkdir -p /srv/remem/data
sudo chown 10001:10001 /srv/remem/data
sudo chmod 700 /srv/remem/data
```

`700` is deliberate: the directory holds memory content in the clear, and the
uid that needs it is the only one that should have it.

On an SELinux host the mount carries `:z` in every compose file, which relabels
the directory so the container may use it. Hosts without SELinux ignore it.

Point `REMEM_DATA_DIR` only at a directory that is empty or that holds *this*
deployment's corpus. It is opened as a Pebble database; another database, or a
directory something else also writes to, is not a supported configuration.

## Migrating off the `remem-data` named volume

Deployments started before this change keep their corpus in a Docker-managed
named volume called `remem-data`. Nothing moves it for you, and nothing at
startup will: an automatic import cannot tell a fresh empty target from a
half-finished earlier copy, and guessing wrong mixes two sets of live Pebble
files. Do it once, by hand, with the server stopped.

**The old volume is your rollback, so nothing below deletes it.**

### 1. Record what you have

With the stack still running, note something you can check for afterwards — the
id of a memory you can fetch, and a count:

```bash
KEY='your-api-key'
curl -s -H "Authorization: Bearer $KEY" \
  'http://localhost:4545/api/v1/memories?limit=1' | head -c 400
```

A count over a corpus larger than one page means following `next_cursor` to the
end; if you have `jq`, this totals it:

```bash
cursor=''; total=0
while :; do
  page=$(curl -s -H "Authorization: Bearer $KEY" \
    "http://localhost:4545/api/v1/memories?limit=100${cursor:+&cursor=$cursor}")
  total=$((total + $(printf '%s' "$page" | jq '.memories | length')))
  [ "$(printf '%s' "$page" | jq -r '.has_more')" = true ] || break
  cursor=$(printf '%s' "$page" | jq -r '.next_cursor')
done
echo "$total memories"
```

### 2. Stop Remem

The database must not be open while it is copied. A live Pebble directory copied
underneath a running process is not a database.

```bash
docker compose stop remem
```

Stopping the one service is enough, and leaves the rest of a business stack up.

### 3. Copy the volume into the host directory

The copy runs in a throwaway container that mounts both sides, so the files are
read and written by the same uids they already have. `cp -a` preserves
ownership, permissions and timestamps; the source is mounted read-only so a
mistake in this command cannot damage the corpus you still depend on.

```bash
DEST=/srv/remem/data                      # what REMEM_DATA_DIR will say
mkdir -p "$DEST"

docker run --rm \
  -v remem-data:/from:ro \
  -v "$DEST":/to \
  alpine:3 sh -c 'cp -a /from/. /to/ && ls -la /to | head'
```

The volume's real name is the project name and the volume name joined by an
underscore — `remem-go_remem-data` for a checkout in a directory called
`remem-go`. `docker volume ls` shows what yours is called; use that name above.

Check that the copy has both the files and the ownership:

```bash
sudo ls -ln "$DEST" | head
```

Every entry should be owned by `10001 10001`. If the volume predates the
unprivileged image and its files are root-owned, `chown -R 10001:10001 "$DEST"`
now, before starting anything.

### 4. Configure the path

```bash
# .env
REMEM_DATA_DIR=/srv/remem/data
```

Confirm that Compose agrees with you before starting anything, which costs a
second and catches a relative path resolving somewhere you did not expect:

```bash
docker compose config | grep -B2 -A1 'target: /var/lib/remem'
```

It prints the `source:` Compose resolved, absolute. If that is not the directory
you copied into, stop here — starting now creates an empty corpus somewhere
else and the deployment will look like it lost everything.

### 5. Start and verify

```bash
docker compose up -d remem
docker compose logs --tail=50 remem
```

Then repeat step 1's checks against the running server: the memory you noted is
fetchable, the count matches, and a search answers. `GET /api/v1/health` reports
the version and edition; a corpus that failed to open does not get that far.

With the stack stopped you can also check the store directly, which the named
volume made awkward:

```bash
docker compose stop remem
remem-admin inspect check --data-dir /srv/remem/data
remem-admin graph verify --data-dir /srv/remem/data
docker compose up -d remem
```

`remem-admin` is not in the image's entrypoint; build it from the source tree
with `go build -o remem-admin ./cmd/remem-admin`. It needs no model and no cgo
for either of those two commands.

Both open the directory read-only first, so a mistyped path is refused rather
than becoming a new empty database that reports a clean corpus. `graph verify`
exits 2 when the graph is inconsistent — neither success nor failure.

### 6. Remove the old volume, later

Only once the host-path deployment has been verified and has run long enough
that you would have noticed a problem:

```bash
docker volume rm remem-go_remem-data
```

There is no hurry. A stopped named volume costs disk and nothing else.

## Rolling back

Rollback is going back to the volume you did not delete. Neither copy is
removed by any step here.

1. Stop Remem: `docker compose stop remem`.
2. Restore the old mount. Either check out the previous compose file, or edit
   the `remem` service's mount back to the named volume and re-declare it:

   ```yaml
   services:
     remem:
       volumes:
         - remem-data:/var/lib/remem

   volumes:
     remem-data:
   ```

3. Start it: `docker compose up -d remem`.

The host directory stays where it is, holding the copy. If the deployment ran
on the host path for a while before you rolled back, that directory — not the
volume — holds the newer writes, and going back to the volume discards them.
Copy it back the way step 3 copied it out, with the source and destination
exchanged, before deciding which is authoritative.

## Backing up

The corpus is a live Pebble database, so a file copy of a running deployment is
not a backup. Two things that are:

- **A portable snapshot**, which is the supported format and the only hard
  compatibility surface Remem has:

  ```bash
  docker compose stop remem
  remem-admin export --data-dir /srv/remem/data --out corpus-$(date +%F).rsnap
  docker compose up -d remem
  ```

  `remem-admin verify --data-dir ... --in ...` checks one against a store, and
  is a check on the moment after an import rather than a health check: a store
  that has been serving legitimately disagrees with the snapshot it came from,
  because the lifecycle sweep moves exactly the fields the verifier compares.

- **A copy of the stopped directory**, with `cp -a` or `rsync -a`, which is now
  an ordinary host operation. Stopped, not running.

`docs/architecture/snapshots.md` has what the format does and does not carry —
retention policy overrides and job rows are deliberately not in it.
