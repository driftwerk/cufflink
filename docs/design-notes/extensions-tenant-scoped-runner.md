# Extensions: tenant-scoped runners and ordinary deploy authority

Amends [extensions.md](extensions.md); where they differ, this note wins.

## Outcome

Native services deploy, roll back, delete and change status with the same
authority as WASM and web services: the tenant's deploy key or deploy role.
Isolation between tenants no longer depends on who deploys, but on the
runner: every ext-runner serves exactly one tenant and holds only credentials
that reach that tenant. A fully malicious native service can affect its own
tenant's other native services (in-tenant, accepted) and nothing else.

## 1. Ordinary deploy authority

- `native::authorize` loses the platform-admin requirement for every change.
  Deploy and rollback of a native service need `DeployAuth` (the tenant's
  deploy authority) and `CUFFLINK_EXTENSIONS=true`; delete and status need
  `DeployAuth` only. The `Change` distinction stays only if it still carries
  a difference after this; otherwise it goes.
- The CLI deploys, rolls back, deletes, enables and disables native services
  with the environment's normal credentials. `native::acting_as_admin` and
  every path that switches to `CUFFLINK_PLATFORM_API_KEY` for native services
  are removed; `cufflink services delete --force` stays.
- Tests that assert a tenant deployer is refused become tests that it
  succeeds; tests of `CUFFLINK_EXTENSIONS=false` refusals stay.
- `cli/assets/cufflink-dev-realm.json`: `alice` loses `cufflink-platform-admin`
  if nothing in the dev flow still needs it (registering NATS providers is
  still operator-only; if the dev docs rely on it for that, keep it and say
  why in the docs).

## 2. Runner keys

A runner authenticates to the platform with a **runner key**: a credential
bound to one tenant that can do nothing but read that tenant's runner data.

- Stored like API keys (hashed, prefix, created_by, created_at, revoked_at,
  last_used_at), in the existing `api_keys` table with a new `scope` column
  (`'tenant' | 'runner'`, default `'tenant'`, migration `V6__runner_keys.sql`)
  or a new table if the existing key paths cannot cleanly refuse a runner key
  everywhere — pick the design that guarantees, by construction, that a runner
  key is refused by every route except the runner API (an extractor type
  `RunnerAuth` that only runner routes use, and `AuthUser` extraction refusing
  `scope = 'runner'`). A test enumerates every other route family and proves a
  runner key is refused.
- Minted, listed and revoked by a platform admin:
  `POST/GET /api/tenants/{slug}/runner-keys`,
  `DELETE /api/tenants/{slug}/runner-keys/{id}`, audited; the secret is shown
  once. CLI: `cufflink tenants runner-keys create|list|revoke <slug>`.
- Rotation: create a new key, point the runner at it, revoke the old one.

## 3. Runner API on the platform

All `RunnerAuth`, tenant from the key, nothing else accepted:

- `GET /api/runner/native` → the tenant's desired set: every active native
  service at its current version with an artifact —
  `{service_id, service_name, version, sha256, size, extensions}`. Same
  selection as the runner's SQL today (`s.status = 'active'`,
  `d.status = 'active'`, `d.manifest->>'mode' = 'native'`).
- `GET /api/runner/native/{service_id}/{sha256}` → the binary, streamed from
  storage (`application/octet-stream`, `Content-Length`), only when the service
  belongs to the key's tenant and some deployment of it records that sha.
- `GET /api/runner/native/{service_id}/environment` → the service's configs
  with secrets decrypted, exactly what `cufflink_secrets::configs::environment`
  returns today, only for the key's tenant.
- Each response is audited in metrics
  (`cufflink_runner_api_requests_total{endpoint, result}`), never logged with
  secret values. 404 (not 403) for anything outside the tenant.

## 4. The ext-runner

- `EXT_TENANT` (a tenant slug) is required; `EXT_TENANTS` is removed.
- New required settings: `PLATFORM_URL` (the platform's internal URL) and
  `CUFFLINK_RUNNER_KEY`. Removed: `DATABASE_URL`, the secrets KEK/key ring,
  and all storage settings — the runner holds no database, secrets or storage
  credentials. Its crate no longer depends on `cufflink-db`,
  `cufflink-secrets` or `cufflink-storage` unless something else genuinely
  needs them (the supervisor's artifact staging takes bytes, not storage).
- Reconcile reads the desired set from `GET /api/runner/native`; installing a
  binary downloads it from the runner API and verifies its SHA-256 as today;
  the child's environment comes from the environment endpoint.
- NATS: subscribes only `deploy.ext.<tenant>.>`, `undeploy.ext.<tenant>.>` and
  `cufflink.ext.<tenant>.*…` for its own services. The docs give the exact
  NATS permissions a tenant runner's user needs (subscribe those three
  patterns; publish only `allow_responses`) and state that the runner's NATS
  user, cloud identity and platform key must be that tenant's alone.
- A failing runner API call keeps the running set and retries next tick, as a
  failing database did.

## 5. Storage

`native/` joins `PLATFORM_KEY_PREFIXES`, so no guest can read, overwrite or
delete a native binary in any storage mode. A test asserts every prefix the
platform itself writes is reserved.

## 6. Documentation

`docs/extensions.md` Security and `docs/deployment.md`: one ext-runner per
tenant; what it holds (its runner key, its NATS user, its cloud identity);
what a native service can reach (its own tenant's other native services,
by design); how to mint and rotate runner keys; the NATS permissions.
Deploying native services needs the tenant's normal deploy authority.

## Tests

- platform: runner key mint/list/revoke (admin only, audited); a runner key is
  refused by every non-runner route family; a tenant key is refused by the
  runner API; each runner endpoint only answers for the key's tenant (404
  otherwise, including another tenant's service id and a sha no deployment
  records); secrets appear only in the environment endpoint; revoked key → 401.
- native deploy/rollback/delete/status with a tenant deploy key succeed;
  `CUFFLINK_EXTENSIONS=false` still refuses deploy and rollback.
- ext-runner end to end (existing suite) against the runner API with a runner
  key and no database or storage settings; a runner for tenant A never starts
  tenant B's native service and cannot fetch its binary or environment.
- storage: guest access to `native/` refused in every mode.
