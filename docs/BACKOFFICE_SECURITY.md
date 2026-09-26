# Backoffice security: inherited findings

Five findings from an automated review of the ported console, all present in the
Rust original at `src/backoffice/backend`. None was introduced by the port, and
none is fixed by it: the port is a replication pass, and a security change made
inside it would make the copy unverifiable against its source. This file is the
record, and closing them is its own piece of work.

| # | Finding | Where | Severity |
|---|---|---|---|
| 1 | **Registration is open.** `POST /api/v1/auth/register` takes no credential and no invite, so anyone who can reach the API creates an account. The account is `is_superuser=False`, so this is not privilege escalation — but an ordinary account reads every memory in the tenant through the console. | `app/api/v1/auth.py:27` | **High** |
| 2 | **Login is not rate limited.** Nothing bounds attempts, so credential stuffing is unimpeded. Failures do increment `backoffice_auth_failures_total`, so the attempt is visible to monitoring — but visible is not blocked. | `app/api/v1/auth.py:71` | High |
| 3 | **A token cannot be revoked.** `jwt_access_token_expire_minutes` is 240, so a leaked token is good for four hours, and logout is client-side only. Redis is already a dependency and `config.py` names a "JWT blacklist" as one of its uses; nothing implements one. | `app/core/security.py:42` | Medium |
| 4 | **Eight characters is the whole password policy.** No complexity rule and no breach check, on an account that reads the corpus. | `app/schemas/auth.py:33` | Medium |
| 5 | **`GET /api/v1/system/health` returns combined server health and corpus statistics** to any authenticated user. Mild by itself; it is finding 1 that makes "authenticated" cheap. | `app/api/v1/system.py:139` | Low |

## What the port does about them

Nothing in the Python. Two things outside it:

- **The compose stack binds the console to loopback by default**
  (`BACKOFFICE_BIND`, default `127.0.0.1`). Publishing it is an explicit act,
  because with finding 1 open a published port is open account creation.
- **This file exists**, and `CLAUDE.md` points at it.

## Before this is exposed to a network

Close finding 1 at minimum. The smallest honest change is to remove the
`register` route and create accounts through an administrative path, which is
what a console with a bootstrapped admin already implies —
`ensure_default_admin` creates the first superuser, and nothing about the
product needs self-service signup.
