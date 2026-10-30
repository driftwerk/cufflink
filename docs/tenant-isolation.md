# Tenant isolation

One Cufflink install can host many tenants. No route lets one tenant's
credentials reach another tenant's resources: a service, job or key of
another tenant is not found, a token of another tenant carries no
authority on its routes unless the tenant sets `tenant_binding: off`, and
deploy authority stays within its tenant. What one tenant's **deployed code** (WASM guests, web apps, native
services) can reach at run time depends on settings that ship permissive,
so that existing installs keep working as they upgrade. An install that
hosts more than one tenant sets all of them. R4, the planned hardening
release that changes defaults, changes them.

## What is isolated whatever the settings

- **Routes.** A principal of another tenant on a data route is recorded
  and served as unauthenticated under `tenant_binding: warn` (the default),
  and refused under `enforce` ([policies](policies.md)); `off` keeps every
  principal as it authenticated, and is for an install of one tenant. Another tenant's
  service id is 404 on every `/api/services/{id}` route, as one that does
  not exist.
- **Realms.** Two tenants that are not previews cannot share a Keycloak
  realm with the same deploy role, which would make each one's deployers
  the other's: creating or updating such a tenant answers 409. Tenants that
  shared one before the upgrade are left as they are until they are
  updated, when the update must give them deploy roles of their own
  (created in the realm and granted to their deployers first; the
  [v0.29.0 release notes](versions.md#v0290-release-notes) give the query that finds
  them). A preview whose source tenant is deleted is no preview any more:
  previews are tenants of their own, not deleted with their source, so
  delete them with it, or give each a deploy role of its own.
- **Worker jobs.** A worker answers a request/reply job only on a NATS
  inbox (`_INBOX.…`), never on a subject a job names
  (`cufflink_worker_reply_refused_total`), and runs a backup job from its
  row: the service and tenant a message names must be the row's, and a job
  runs once; a running job is claimed again only by its message's
  redelivery, once the worker that ran it stopped.
- **Redis.** Under `redis_scope: warn` a guest's delete never removes the
  unprefixed key it reads as a fallback, which may be another tenant's;
  it leaves a tombstone in the tenant's key instead, so the deleted value
  is not read back from the fallback.
- **SQL console.** Under `db_roles: enforce`, `cufflink sql`
  (`POST /api/services/{id}/query`) runs as the service's read-only role
  when one is bound, else as the service's own role, never as the
  platform's.
- **Logs.** The search and level of `cufflink logs` are matched as quoted
  strings, never as more query.
- The controls [policies](policies.md#unconditional-controls) lists as
  unconditional.

## Settings an install hosting more than one tenant must set

| Setting | Prevents |
|---------|----------|
| `CUFFLINK_POLICY_DB_ROLES=enforce` | Guest SQL running as the platform's database role, which reaches every schema of the tenant's database and can change other tenants' databases and roles |
| `storage_scope` with `"scope": "strict"` and each tenant's `allowed_buckets` | Guest access to other tenants' objects |
| `CUFFLINK_POLICY_EGRESS=enforce` | Guest requests to private and internal addresses (the platform, the database, NATS, other tenants' services) |
| `CUFFLINK_POLICY_WASM_LIMITS=enforce` | One guest exhausting memory every tenant's guests share |
| `WASM_MEMORY_BUDGET_MODE=enforce` | Many guests together, of one service or several, growing the process into its container's limit, where the kernel kills every tenant's work at once; and one service taking every execution slot |
| `CUFFLINK_POLICY_TENANT_BINDING=enforce` | Callers' `authorization`, `cookie` and `proxy-authorization` headers reaching guests whose routes do not declare `forward_auth`; a token or key of another tenant is then refused (403) where `warn` serves it as unauthenticated |
| `CUFFLINK_POLICY_REDIS_SCOPE=enforce` | Fallback reads of unprefixed Redis keys, which any tenant may have written |
| A NATS user per component, and no anonymous access | Forged jobs, events and results on the platform's subjects |
| One web runner and one extension runner per tenant, each with that tenant's credentials alone | Web apps and native services reaching another tenant's configs, bundles, binaries and calls |
| Each runner on a network that reaches `PLATFORM_URL` and NATS alone (no route to Postgres, Redis or object storage; NATS only with the runner's own user) | Native code any of a tenant's deployers can deploy, and web apps, connecting to the platform's database, Redis or storage, which hold every tenant's data |
| Authentication, or a private listener, in front of `/metrics` and the monitoring stack | Tenant names, service names and activity leaking |

Each policy is set as the platform default with `CUFFLINK_POLICY_<KEY>`
(and `CUFFLINK_POLICY_<KEY>_CONFIG`), and per tenant with
`cufflink tenants policy set <tenant> <key> <mode> [--config <json>]`,
which reaches every replica within seconds. Soak each in `warn` first, per
tenant, and read `cufflink_policy_violation_total{key, tenant, reason}`
until it stays at zero ([rollout](policies.md#rollout)).

### Database roles

`CUFFLINK_POLICY_DB_ROLES=enforce` runs each service's guest SQL, data
routes and `cufflink sql` on a login as a role of its own that reaches its
own schema alone. The platform's database role needs `CREATEROLE` and must
not be a superuser, `CUFFLINK_SECRETS_KEK` must be set, and tenant pools
reached directly need `DB_POOL_RECYCLE=clean` (the default); the policy is
refused otherwise. Migrate per tenant: `warn` makes each service's role at
its next deploy and records what the role is refused
(`cufflink_db_role_fallback_total`); mend those, check every service with
`cufflink services db-role verify <tenant> <service>`, then `enforce`
([database roles](policies.md#database-roles)).

### Storage

`storage_scope` already defaults to `enforce`, with the `legacy` scope: a
guest may use any bucket that no other tenant group lists. `strict` confines
a guest's keys to `tenants/{tenant}/{service}/` and requires an allowlist,
so set it per tenant with the buckets that tenant uses:

```sh
cufflink tenants policy set acme storage_scope enforce \
  --config '{"scope":"strict","allowed_buckets":["acme-data"]}'
```

Buckets of their own per tenant, or one shared bucket under the per-tenant
prefixes, both keep tenants apart. Move a tenant's existing objects under
`tenants/{tenant}/{service}/` before `strict`, and give its previews the
same row.

### Egress, memory, binding and Redis

- `CUFFLINK_POLICY_EGRESS=enforce`: first run `warn` and add the internal
  hosts each tenant legitimately calls to its `allow_hosts` (from
  `cufflink_policy_violation_total{key="egress"}`).
- `CUFFLINK_POLICY_WASM_LIMITS=enforce`: guests are held to
  `WASM_MAX_MEMORY_MB`, or a tenant's lower `memory_bytes`; a guest past it
  fails its handler instead of growing.
- `CUFFLINK_POLICY_TENANT_BINDING=enforce`: annotate the routes that must
  forward the caller's credentials with `forward_auth` first; under `warn`
  those still reaching a guest are recorded as `auth_header_forwarded`, and
  under `enforce` the credentials stripped are counted in
  `cufflink_route_credentials_stripped_total`.
- `CUFFLINK_POLICY_REDIS_SCOPE=enforce`: after one full rewrite cycle of
  every key (at least 24 hours of `warn`).

### NATS

Give the platform, the workers and every runner a NATS user of its own
(`NATS_CREDS_FILE`, `NATS_NKEY_SEED` or `NATS_USER` and `NATS_PASSWORD`),
and allow no anonymous connection. Only the platform's and the workers'
users may publish on `cufflink.>`, `deploy.>` and `undeploy.>`: whoever may
publish there can submit jobs the workers run and events the runners act
on. A tenant's extension runner and web runner subscribe to their tenant's
subjects alone and publish nothing but replies
([extension runner](deployment.md#extension-runner),
[web runner](deployment.md#tenant-scoped-web-runner)); a NATS provider of a
tenant's extensions subscribes to `cufflink.ext.<tenant>.>` alone.

### Runners

A web app or native service runs as its runner's user, beside every other
app or service of that runner, with the runner's network and cloud
identity. Run **one web runner and one extension runner per tenant**, each
holding that tenant's runner key, NATS user and cloud identity alone:

- the extension runner always serves one tenant (`EXT_TENANT`);
- a web runner serves one tenant with `WEB_TENANT`, `PLATFORM_URL` and
  `CUFFLINK_RUNNER_KEY` (or `CUFFLINK_RUNNER_KEY_FILE`), and then holds no
  database, secrets key or storage credentials
  ([tenant-scoped web runner](deployment.md#tenant-scoped-web-runner));
- each on a network that reaches `PLATFORM_URL` and NATS alone
  ([a network policy](deployment.md#extension-runner)).

A platform admin mints each runner key for one kind of runner:
`cufflink tenants runner-keys create <tenant> --for native` for the
extension runner, `--for web` for the web runner. The runner API answers a
key on its own kind's routes alone, so a web runner's key, which its apps
can read in a file, never reaches the tenant's native binaries or their
configs. Rotate a key in this order: create the new key, point the runner
at it (a new `CUFFLINK_RUNNER_KEY`, or the new key written to a file
beside the one `CUFFLINK_RUNNER_KEY_FILE` names and renamed over it, as a
Kubernetes secret mount does, so the runner never reads half a key), and
revoke the old key only once the runner uses the new one. A web runner
that asked with the old key just before it was revoked stops its apps and
serves again at its next poll, within one `WEB_POLL_SECS`. A runner whose
key the platform refuses, or answers for another tenant, stops everything
it runs and stays unready until a key the platform accepts is in place,
since a refused key means the key was revoked or its tenant deleted, and a
new tenant may later take the same slug.

A runner key in a file is readable by what the runner runs, which gives an
app or native service its own tenant's runner data; prefer the variable.

### Metrics and monitoring

The platform serves `/metrics` on its API port without authentication,
and the worker on `WORKER_HTTP_PORT`; their labels name tenants and
services. Keep both, and Prometheus, Grafana, Loki and the NATS monitoring
port, off the public network, or behind authentication at the ingress. The
runners serve their metrics only with `WEB_STATUS_TOKEN` and
`EXT_STATUS_TOKEN`. A runner's network reaches the platform's API port, and
so its `/metrics`: where native services or web apps of more than one
tenant run, put the platform's `PLATFORM_URL` for runners behind a proxy
that answers `/metrics` 404.

The collector believes whoever writes to it, and so do Loki, Tempo and
Prometheus, which take writes without asking who is calling; `cufflink
logs` and the tenant dashboards show what they were sent. A runner's
children reach what the runner's network reaches, and a guest's `http::*`
calls reach internal addresses until its tenant's `egress` is enforced,
and public addresses under either mode: give each tenant's runners a
collector that pins the tenant, keep the collector, Loki, Tempo and
Prometheus out of every `allow_hosts`, and keep them on private addresses
or behind authentication
([who can write a tenant's lines](observability.md#who-can-write-a-tenants-lines)).

## Not yet available

Planned, with no setting today:

- per-tenant shares of the guest memory budget (per-service concurrency
  and memory shares are `WASM_MAX_CONCURRENT_PER_SERVICE` and
  `WASM_MEMORY_SERVICE_SHARE_PERCENT`);
- job quotas on the workers;
- per-tenant JetStream limits (storage and message counts of each
  tenant's stream);
- signed worker jobs, so a job carries proof that the platform made it,
  beyond what a NATS user's permissions give;
- tick consumers named for their tenant by default. Until then a service
  that subscribes to `cufflink.tick.*` declares `consumers: scoped`
  ([durable names](subscriptions.md#durable-names)); otherwise two
  tenants' services of the same name and handler share one tick consumer,
  and each tick reaches only one of them.

Until then, one tenant's load can slow every tenant's jobs, and the NATS
permissions above are what keeps jobs the platform's own.

## Known limits

- A shared web runner (without `WEB_TENANT`) that was down when a tenant
  was deleted keeps that tenant's unpacked bundles on disk until an app of
  the same tenant slug and service name is next started there. They are
  never served: each is named by its service's id, which no other service
  has. Tenant-scoped web runners remove everything of their tenant once its
  key is refused.
- A backup message whose job cannot be read or claimed in three
  deliveries, 30 s apart (the database failing for a minute or more), is
  given up, and its job stays `queued`: start a new export or restore.
