# Tenant isolation: bug fixes and the settings that remain

## Scope

A cross-tenant audit of the platform, worker, web-runner, ext-runner and
control plane (September 2026) found no route that lets one tenant's
credentials reach another tenant's resources, but several ways for one
tenant's deployed code to read or affect another tenant at runtime. This
change fixes those that need no change to a default. The rest depend on
settings that ship permissive so existing installs can migrate; they are
listed in [what remains](#what-remains-settings-an-operator-must-set) and
documented for operators, and a later release changes the defaults.

## Fixes

1. **A foreign principal under `tenant_binding: warn` is anonymous.** Today a
   warn mismatch keeps the principal with its flags stripped, so a
   `auth_required` table without a permission area admits another tenant's
   token and a guest receives foreign claims. Under warn the violation is
   still recorded and logged, and the request proceeds as unauthenticated.
   Enforce is unchanged.
2. **Backup jobs trust only their row.** The worker takes `service_id` and
   `tenant_id` from the backup job row, refuses a message whose ids differ,
   and claims the job atomically (`queued → running` with a row count check),
   so a completed or running job cannot be re-run from a message.
3. **The worker replies only to inboxes.** A job whose reply subject does not
   start with `_INBOX.` is not answered (counted as
   `cufflink_worker_reply_refused_total`).
4. **Redis warn fallback never deletes a foreign key.** Under
   `redis_scope: warn`, `del` deletes only the scoped key; a fallback read is
   still logged and returned as today.
5. **`/api/services/{id}/query` runs as the service's role** whenever the
   tenant runs `db_roles: enforce` (its read-only role when one is
   configured), never as the platform role.
6. **A foreign service id is 404**, not 403, on every `/api/services/{id}`
   route.
7. **Tenants do not share a realm by accident.** Creating or updating a
   non-preview tenant is refused when another non-preview tenant uses the same
   realm with the same deploy role. Existing tenants are untouched until
   updated.
8. **LogQL input is escaped.** `search` and `level` in the logs API cannot
   carry a backtick.
9. **Web-runner hardening.**
   - The runner makes itself undumpable at start (`prctl(PR_SET_DUMPABLE, 0)`
     on Linux), so no app reads its environment or memory.
   - Opt-in tenant-scoped mode: with `WEB_TENANT`, `PLATFORM_URL` and
     `CUFFLINK_RUNNER_KEY` (or `CUFFLINK_RUNNER_KEY_FILE`) set, the web-runner
     serves one tenant, reads its apps, bundles and environments from the
     runner API (new endpoints mirroring the native ones, reusing
     `RunnerAuth`, the runner key and the ext-runner's API client — move the
     client into a shared crate rather than duplicating it:
     `GET /api/runner/web`, the bundle, the environment), serves only hosts of
     that tenant, subscribes only that tenant's deploy events, and needs no
     `DATABASE_URL`, secrets key or storage credentials. Without them it runs
     as today. The docs recommend one web-runner per tenant for any install
     that hosts more than one tenant's web apps.

10. **A runner whose key is refused stops.** Today a runner API call that
    fails keeps the running set (right for transient failures). A 401 is
    definitive — the runner key was revoked or its tenant deleted, and a new
    tenant may later take the same slug and so the same NATS subjects — so on
    401 the ext-runner unsubscribes and stops every child, stays unready, and
    keeps polling; a later accepted key converges as usual. The same applies to
    the web-runner's tenant-scoped mode. Rotation (new key first, revoke the
    old one after the runner uses the new one) is unaffected; the docs spell
    out that order.

## What remains: settings an operator must set

`docs/tenant-isolation.md` (new, linked from the setup and deployment docs)
lists, for an install hosting more than one tenant, every setting required
for isolation, what each prevents, and how to migrate to it:

| Setting | Prevents |
| --- | --- |
| `CUFFLINK_POLICY_DB_ROLES=enforce` (and a non-owner tenant database role) | guest SQL acting on other tenants' databases |
| `CUFFLINK_POLICY_STORAGE_SCOPE` `strict` with per-tenant prefixes or buckets | guest access to other tenants' objects |
| `CUFFLINK_POLICY_EGRESS=enforce` | guest requests to private and internal addresses |
| `CUFFLINK_POLICY_WASM_LIMITS=enforce` | one guest exhausting shared memory |
| `CUFFLINK_POLICY_TENANT_BINDING=enforce` | foreign tokens on data routes |
| `CUFFLINK_POLICY_REDIS_SCOPE=enforce` | fallback reads of unscoped keys |
| NATS users with per-component permissions (no anonymous access) | forged jobs and results on the platform's subjects |
| One web-runner and one ext-runner per tenant, each with its own credentials | apps and native services reaching another tenant |
| Authentication or a private listener for `/metrics` and the monitoring stack | tenant names and activity leaking |

Also listed as not yet available and planned: per-tenant concurrency and job
quotas, per-tenant JetStream limits, signed worker jobs, tick durable names
that include the tenant.

## Tests

One per fix, at the lowest level that proves it (unit where pure,
integration through testkit where the behaviour crosses components), plus the
web-runner tenant-scoped mode end to end against the runner API.
