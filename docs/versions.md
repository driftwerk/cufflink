# Versions and Platform Features

A platform advertises what it can do in `GET /api/version` →
`supports`. A service manifest lists what it needs in
`required_platform_features`, and a deploy that needs a feature the platform
lacks is refused (422) instead of running without it. The CLI checks the same
list before uploading, and prints what it negotiated:

```
platform sha-4f2a… (features bundled_wasm, …) ← sdk v0.27.0 (needs db_v2, route_posture)
```

`cufflink --version` prints the CLI's version; with `--env <name>` it also
prints that environment's platform version, deploy API and features.

The tables below are every feature a platform of this release advertises; a
platform test reads them and fails when they and `supports` differ, and an
SDK test fails when the map of cargo features differs from what the SDK
declares.

## Where a requirement comes from

- **The manifest** — the `service!` macro derives the features the declared
  shape needs (for example `route_posture` for any route posture,
  `scoped_permissions` for a table with `scoped_by`). A manifest without new
  declarations derives exactly what it did before, so its deploy hash is
  unchanged.
- **The module** — `cufflink-fn` cargo features that change what a guest
  imports or emits write their name into the module's `cufflink.features`
  custom section ([the map](#sdk-cargo-features)). Both the CLI and the
  platform read it at deploy and upload.

## SDK cargo features

| `cufflink-fn` feature | Platform feature it requires |
|-----------------------|------------------------------|
| `db-v2` | `db_v2` |
| `cache` | `cache` |
| `service-call` | `service_call` |
| `response-v2` | `response_v2` |
| `delivery-control` | `delivery_control` (and `response_v2`, which it enables) |
| `nats-publish-v2` | `nats_publish_v2` |
| `http-v2` | `http_v2` |
| `storage-v2` | `storage_v2` |
| `util-v2` | `util_v2` |
| `ext-call` | `ext_call` |
| `test-host` | none: it runs handlers natively in `cargo test` ([testing](testing.md)) |
| `stripe` | none |

## Features before R2

| Feature | Provides |
|---------|----------|
| `bundled_wasm` | a deploy carries its module |
| `on_migrate` | the `on_migrate` hook, run on every deploy that is not skipped |
| `wasm_hash_echo` | the deploy answer carries the hash of the module it stored |
| `deploy_fanout` | every replica is told of a deploy and loads its module |
| `tenant_policies` | [per-tenant policies](policies.md) |
| `route_posture` | [route postures](routing.md), derived from any declared posture |
| `nats_publish_authz` | a guest publishes only within its tenant's subjects |
| `forward_auth` | a route that declares it hands the caller's credentials to its guest |
| `graphql_opt_in` | GraphQL only for services that declare it |
| `host_core` | logging, responses, time, UUIDs, tenant and config |
| `db` | `db::query` and `db::execute` |
| `db_execute_all` | `db::execute_all`: statements in one transaction |
| `db_query_read_only` | `db::query_read_only` |
| `http_fetch` | `http::*` |
| `http_fetch_timeout` | `http::*` with a timeout |
| `http_fetch_many` | concurrent fetches |
| `nats` | `nats::publish` and `nats::request` |
| `s3` | object download and presigned URLs |
| `s3_delete` | object delete |
| `s3_download_many` | concurrent downloads |
| `image_transform` | JPEG, WebP and AVIF transforms |
| `image_transform_multi` | several variants in one call |
| `redis` | the legacy Redis calls |
| `jobs` | `jobs::enqueue` |

## Features added in R2

| Feature | Needed when | Provides |
|---------|-------------|----------|
| `db_v2` | the guest enables `cufflink-fn/db-v2` | [SQL with bound parameters](db-v2.md) |
| `cache` | the guest enables `cufflink-fn/cache` | the [namespaced cache](service-calls.md#the-namespaced-cache) |
| `service_call` | the guest enables `cufflink-fn/service-call` | [service calls](service-calls.md) |
| `precompiled_modules` | never demanded | compiled modules cached per engine version |
| `scoped_permissions` | any table sets `scoped_by` | [scoped permissions](scoped-permissions.md) |
| `response_v2` | the guest enables `cufflink-fn/response-v2` | [statuses, headers, files, streamed objects and events](routing.md#responses-cufflink-fn-feature-response-v2) |
| `route_matching` | `routing: strict` | [routes at their own paths](routing.md#strict-routing) |
| `route_limits` | any route sets `body_limit`, `timeout` or `multipart` | [route limits](routing.md#route-limits) |
| `error_profile` | `errors: { key: … }` | [the error profile](routing.md#error-profile) |
| `sse_bridge` | any route declares `sse(...)` | [server-sent events](routing.md#server-sent-events) |
| `internal_posture` | any route is `internal(...)` | [internal routes](service-calls.md#internal-routes) |
| `api_key_posture` | any route is `api_key(...)` | [partner-key routes](partner-keys.md) |
| `scoped_posture` | any route is `scoped(...)` | [scoped custom routes](scoped-permissions.md#custom-routes) |
| `typed_routes` | never demanded | [typed routes and OpenAPI](openapi.md) |
| `partner_keys` | never demanded | [partner keys](partner-keys.md) and their management API |

## Features added in R3

| Feature | Needed when | Provides |
|---------|-------------|----------|
| `namespace_claims` | never demanded | NATS subjects a tenant is granted beyond its own ([policies](policies.md)) |
| `nats_consumer_config` | a subscription declares a consumer, `ack` other than `always` or a timeout; `consumers: scoped` | durables configured and renamed by the deploy |
| `consumer_reconciler` | never demanded | every replica runs the loops of every durable |
| `delivery_control` | the guest enables `cufflink-fn/delivery-control` | a handler's answer retries, delays or dead-letters its message |
| `nats_message_meta` | never demanded | a delivery carries its subject, stream, consumer, sequences, attempt and headers |
| `streams` | the manifest declares `streams` | streams created and extended by the deploy, never shrunk |
| `nats_publish_v2` | the guest enables `cufflink-fn/nats-publish-v2` | acknowledged, deduplicated publishes with headers |
| `schedules` | the manifest declares `schedules` | schedules that fire once across replicas and catch up |
| `jobs_v2` | a job declares `retry_on` or `backoff` | leased, fenced, reclaimed jobs whose handler decides its retry |
| `http_v2` | the guest enables `cufflink-fn/http-v2` | bytes, multipart and repeated query keys over HTTP |
| `storage_v2` | the guest enables `cufflink-fn/storage-v2` | uploads with metadata and `head` |
| `util_v2` | the guest enables `cufflink-fn/util-v2` | `sleep_ms` within the budget and a millisecond clock |
| `schema_adopted` | the manifest names a schema it does not own (`schema: adopted`, `schema_name`) or a table the platform does not manage | [schemas and tables a service adopts](schema-ownership.md) |
| `sql_migrations` | the manifest declares `migrations` | [SQL migrations](schema-ownership.md) under a declared history |
| `schema_guard` | a deploy passes `--accept-drift` or `--purge` | [the schema guard](schema-ownership.md) and its tenant policy |
| `column_types_v2` | a table declares checks, search, a v2 index or a v2 column type | the column types and indexes of R3 |
| `keyset_lists` | a table's lists are not offset-paginated | keyset pagination of lists |
| `async_deploy` | a deploy passes `--plan` | deploys that run to their end, `?async=true` answered 202 and followed, and plans |
| `outbox` | never demanded | [the transactional outbox](outbox.md) and its tenant policy |
| `audit_redaction` | a table or route declares `audit_redact`, or a table is `sensitive` | audit records without the redacted fields |
| `db_roles` | never demanded | [a database role per service](db-roles-and-secrets.md) and its tenant policy |
| `secret_encryption` | never demanded | [stored secrets sealed](db-roles-and-secrets.md#stored-secrets) under a key per tenant |

## Features added in v0.29.0

| Feature | Needed when | Provides |
|---------|-------------|----------|
| `ext_call` | the manifest declares `extensions`, or the guest enables `cufflink-fn/ext-call` | calls to the extensions the tenant registers ([host functions](building-on-cufflink.md#ext--extensions)) |
| `native_services` | the manifest is `mode: native` | deploying and rolling back native services, which provide extensions to the tenant ([deployment](deployment.md#native-services)) |

## Features added in v0.30.0

| Feature | Needed when | Provides |
|---------|-------------|----------|
| `private_tables` | the manifest declares `private_tables` | tables the platform checks against the database and does not serve ([private tables](schema-ownership.md#private-tables)) |
| `inactive_deploy` | a deploy passes `--inactive` | a deploy that starts nothing: a new service is created inactive, an active one is refused ([deploying a service inactive](deployment.md#deploying-a-service-inactive)) |
| `config_apply` | `cufflink config apply`; never demanded by a deploy, whose config step falls back to a request for each key | a service's config applied as one set in one transaction, with `prune` and `dry_run` ([service config as code](deployment.md#service-config-as-code)) |

## Features added in v0.31.0

| Feature | Needed when | Provides |
|---------|-------------|----------|
| `column_permissions` | any column declares `#[restricted(...)]` | columns read and written through the data routes only with a grant ([column permissions](column-permissions.md)) |

`cufflink services list --features` prints what each deployed service
requires.

## Changes in v0.33.0

No new module-declared feature. Spec and client changes:

- Generated clients take `ClientOptions` (`fetch`, `headers`) after the base
  URL and throw `CufflinkError` with the status and parsed body for a non-2xx
  response; a plain headers record is still accepted. A typed client answers
  `undefined` for an empty JSON body ([generating a client](openapi.md#generating-a-client)).
- A table's row schema requires every column it is answered, a nullable one
  as `null`; the create body no longer requires the owner column; a PUT body
  has its own `…Update` schema with nothing required; a JSONB column is any
  JSON value rather than an object. Regenerated clients change type shape
  accordingly.
- A table scoped by grants documents `scope=own|all` on its list and get
  routes, and a column named like a list parameter is no longer offered as a
  filter twice.
- The telemetry crate is on the opentelemetry 0.32 family and token signing
  uses jsonwebtoken 10 on the aws-lc-rs backend.

## Changes in v0.32.0

No new module-declared feature. Platform behaviour:

- `GET /api/tenants/{tenant}/users/{user}/permissions` answers only the user
  themself, a caller with deploy authority, or a service account; any other
  tenant-realm token is refused.
- `RATE_LIMIT_EXEMPT_IPS` lists addresses or CIDR ranges the per-IP rate
  limiter never counts, and `RATE_LIMIT_EXEMPT_PRIVATE_PEERS=false` stops
  exempting private-address callers ([rate limiting](guides.md#rate-limiting)).

## CLI, SDK and platform together

| CLI | Platform | Guest uses a module-declared feature | Outcome |
|-----|----------|--------------------------------------|---------|
| new | new | yes | deploys |
| new | older | yes | the CLI refuses before uploading, naming the feature |
| older | new | yes | the platform reads the module itself and accepts it |
| older | older | `db-v2`, `cache`, `service-call` | the older platform refuses the module's unknown imports |
| older | older | `response-v2` only | not detectable: `response-v2` needs a CLI and platform of this release or newer |
| any | any | none | as before |

A CLI of this release is the minimum for `cufflink deploy --plan`, for
following an asynchronous deploy, and for the check that the project's
`migrations/` match what the manifest declares; an older CLI deploys
synchronously and without those checks.

A v0.30.0 CLI is the minimum for `cufflink deploy --inactive`, `cufflink
config apply`, `cufflink build --bundle`, `cufflink deploy --bundle` and
`cufflink rollback --commit` and `--service`. Against a platform older than
v0.30.0 it refuses `--inactive` and `config apply` (the platform lacks the
feature), runs a deploy's config step as a request for each key, deploys a
bundle as it deploys sources, and rolls back by commit, though the version
that platform activates reports no commit. A deploy from a bundle
negotiates as `← bundle from cufflink v<cli> (needs …)`: a bundle says
which CLI built it, not which SDK.

## Rolling a platform back

Roll back in this order: services first, then the platform, then the CLI.
A platform image may only be rolled back below the release that introduced
a feature after every service using that feature has been rolled back to a
version that does not: an older platform cannot read such a manifest, skips
it (`cufflink_registry_load_errors_total`) and the service answers 404. Check
with `cufflink services list --features` first.

A feature carried in a manifest key the older platform does not know is
different: the older platform drops the key and runs the service without
it. For `private_tables` that means the service keeps running, its private
tables are neither served nor checked, and nothing in the database is
changed. On the older platform such a service cannot be deployed (422,
naming the feature). **Do not `cufflink rollback` it there, and do not
re-upload its module there**: a platform before v0.30.0 does not check a
rollback's features, so the rollback runs and stores a version without
the list; and its `POST /api/services/{id}/wasm`, which a deploy whose
manifest is unchanged sends, re-reads the running manifest without the
key it does not know and stores the new version without the list too.
If either happened, redeploy the service once the platform is back on
v0.30.0. From v0.30.0 a rollback checks the target version's features as
a deploy does.

Rolling a platform back from v0.30.0 to v0.29, beside the paragraph above:

| What v0.30.0 added | Before rolling the platform back |
|--------------------|----------------------------------|
| Services that declare `private_tables` (`cufflink services list --features` names them) | Nothing must be done first. The v0.29 platform drops the key and runs the service without those tables: nothing is exposed and nothing is altered. They cannot be deployed there, and must not be rolled back with `cufflink rollback`, or have their module re-uploaded by a deploy whose manifest is unchanged, there (above) |
| A deploy while the platform's replicas are of two versions | **Deploy no service while replicas of v0.29 and v0.30.0 run side by side, in either direction, upgrading or rolling back.** A v0.29 replica ignores `inactive` and deploys the service active; it also answers a config apply with 405, which the CLI meets by sending a request for each key |
| Secrets given as `<name>_FILE` | Give each as a plain variable again first. v0.29 does not read the files, and would start without the secret or refuse to start ([secrets as files](deployment.md#secrets-as-files)) |
| `LOKI_LABEL_MODEL`, `CUFFLINK_CHILD_TELEMETRY`, `CUFFLINK_CHILD_OUTPUT_LEVEL` | Nothing: v0.29 does not read them. Its logs API asks Loki the `legacy` way, so behind a collector that no longer writes that shape `cufflink logs` returns nothing until the platform is back on v0.30.0 |
| Services that are inactive | Nothing to undo. But v0.29 starts an inactive service's loops on its next deploy, rollback or module upload, on the replica that took it, until that replica reloads its registry: do not deploy an inactive service to a rolled-back platform unless something else keeps its durables from being consumed twice |
| Config applied as a set (`cufflink config apply`, a deploy's config step) | Nothing: the rows are ordinary `service_configs` rows |
| The database | Nothing: v0.30.0 has no migration, so v0.29 logs no "missing migration" line for it |

Rolling a platform back from v0.31.0 to v0.30:

| What v0.31.0 added | Before rolling the platform back |
|--------------------|----------------------------------|
| Services that declare column permissions (`cufflink services list --features` names them, as `column_permissions`) | **Roll each back first, to a version without them.** The v0.30 platform drops the key it does not know and serves the service without it: every restricted column is answered to, and written by, every caller that reaches the row. Such a service cannot be deployed there (422, naming the feature) |
| The database | Nothing: v0.31.0 has no migration |

A v0.30.0 CLI keeps working against the rolled-back platform: `--inactive`
is refused by the feature check, `cufflink config apply` likewise, a
deploy's config step falls back to a request for each key, `deploy
--bundle` works (the request is the one a deploy from sources sends) and
`rollback --commit` works (the CLI finds the version itself), though the
version that platform activates reports no commit.

A service goes back to an earlier version with `cufflink rollback --commit
<sha>` or `--version <n>` on either release. From v0.30.0 a rollback of an
inactive service leaves it inactive.

Admin API-key expiries written by this release persist across a rollback
([deployment](deployment.md#api-keys)).

A platform whose secrets are stored `encrypted` rolls back below v0.28.0
only after it has returned to `dual` and resealed: an earlier release reads
only the plaintext column, which `encrypted` storage empties
([leaving `encrypted`](db-roles-and-secrets.md#stored-secrets)).

Rolling the platform back after a later release has migrated the database
logs an ERROR at every start that the later release's migration is missing
from the filesystem (`V3__r2` on v0.26.0, `V4__r3` on v0.27.0,
`V5__extensions` and `V6__runner_keys` on v0.28.0): expected and harmless, as migrations are
additive and the older binary does not abort on a newer schema. It serves `_fn`, CRUD and API keys as before.

## Release checklist

1. Bump the crate set: each crate's version, and the version of its
   `[workspace.dependencies]` entry where it has one, which every crate that
   depends on it inherits. The set is every crate step 2 names:
   `cufflink-nats` and `cufflink-native` are published too and are bumped
   with the rest on every release. Then `cufflink init`'s scaffold (a unit
   test pins it to the workspace versions).
2. `cargo publish --dry-run -p cufflink-types -p cufflink-migrate -p cufflink-pgcodec
   -p cufflink-fn -p cufflink-sdk-macros -p cufflink -p cufflink-cli -p cufflink-nats
   -p cufflink-native` on a clean tree.
3. A crate crates.io has never had is published only by a token with the
   `publish-new` scope whose crate pattern covers its name: check
   `CARGO_REGISTRY_TOKEN` before tagging a release that adds one, and add
   the co-owners once it is published. `publish.yml` names each new crate
   before it publishes anything.
4. Start the previous release's platform image on a database this release
   migrated; `/ready` must turn 200 and `_fn`, CRUD and API-key calls answer.
5. The tagged commit's `sha-<commit>` platform, worker, web-runner and
   ext-runner images are on Docker Hub (CI pushes them from `main`): the released CLI's
   `cufflink dev up` runs them.
6. Tag `v<cufflink-fn version>`. `publish.yml` refuses a tag that does not
   match, publishes nothing while any crate's version is on crates.io from
   another commit or packaged with uncommitted changes (`--allow-dirty`),
   and fails unless crates.io ends up serving each crate built from the
   tagged commit.

## v0.31.0 release notes

**Version set (tag `v0.31.0`):** `cufflink-types`, `cufflink-sdk-macros`,
`cufflink`, `cufflink-migrate` and `cufflink-pgcodec` 0.15.0;
`cufflink-fn` 0.31.0; `cufflink-cli` 0.22.0; `cufflink-nats` and
`cufflink-native` 0.3.0; the platform, worker, web-runner and ext-runner
images of the same commit. `cufflink init` scaffolds `cufflink = "0.15"`
and `cufflink-fn = "0.31"`. The minor moves because `ColumnDefinition`
gains a public field (`access`), which breaks a struct literal, and the
0.14 sdk-macros emits an exhaustive literal, so a 0.15 SDK cannot be mixed
with a 0.14 macro crate; `cufflink-native` re-exports a `cufflink-types`
type and moves with it, `cufflink-nats` with it. A service built with the
v0.30.0 SDK and CLI deploys to and runs on this platform unchanged, with
the same deploy hash.

This release adds one thing, opt-in: column permissions. With no new
setting and no new flag, a v0.30.0 install upgraded to this release serves
as it did.

**Before the first start of this release (operators):** nothing. There is
no migration, of the control plane or of a tenant database, and no new
environment variable.

**What is new:**

- **Column permissions** (feature `column_permissions`):
  `#[restricted(read = "internal", write = "internal")]` on a field asks a
  grant of the table's permission area to read the column through the data
  routes, to write it, or both. A caller without it is answered rows
  without the column from list, get, create, update, batch, GraphQL and the
  table's event stream, names it in `select`, `sort`, `filter`, search or a
  GraphQL query as a column the table never had, and is refused with 403 a
  write that sets it, before any hook runs and before anything is written.
  Owning the row grants neither; a service account is asked nothing. Hooks,
  events, audit, webhooks, backups and the service's own SQL keep whole
  rows. The OpenAPI spec carries what each such column asks
  (`x-cufflink-access`) and does not require it of a response; the GraphQL
  schema marks the field. A deploy refuses a declaration the data routes
  depend on (the key, the owner column, a timestamp, `created_at`), one a
  caller without the grant could not create a row under, one a declared
  search or a unique constraint would answer for every caller, one naming an
  operation its area does not list, and a reducer-less event-stream route
  over the table's events; the macro refuses most of these at compile time
  ([column permissions](column-permissions.md)).
- `cufflink_column_decisions_total{table, columns}` counts the decisions.

**Rolling back** to v0.30.0 is an image change, services that declare
column permissions first: the v0.30.0 platform drops the key it does not
know and serves every restricted column to every caller of the row
([rolling a platform back](#rolling-a-platform-back)).

## v0.30.0 release notes

**Version set (tag `v0.30.0`):** `cufflink-types`, `cufflink-sdk-macros`,
`cufflink`, `cufflink-migrate` and `cufflink-pgcodec` 0.14.0;
`cufflink-fn` 0.30.0; `cufflink-cli` 0.21.0; `cufflink-nats` and
`cufflink-native` 0.2.0; the platform, worker, web-runner and ext-runner
images of the same commit. `cufflink init` scaffolds `cufflink = "0.14"`
and `cufflink-fn = "0.30"`. The minor moves because `ServiceManifest` gains
a public field (`private_tables`); a service built with the v0.29 SDK and
CLI deploys to and runs on this platform unchanged, with the same deploy
hash. `cufflink-cli` is packaged without its `tests/`, whose integration
tests need the workspace.

This release adds tables the platform checks and does not serve, deploys
that create a service inactive, telemetry scoped to the tenant and service
it belongs to (with the collector, Loki, Tempo, Prometheus and Grafana
assets that show it), and an operations set: secrets from files, a
service's config applied as one set, bundles, rollback to a commit, and a
check of what a key ring can open. Every addition is opt-in. With no new
setting and no new flag, a v0.29.2 install upgraded to this release serves
as it did, apart from the list under **What changes at the defaults**.

**Before the first start of this release (operators):**

- There is no migration, of the control plane or of a tenant database: a
  v0.29.2 binary runs against a database this release has used.
- A `<secret>_FILE` variable that already happens to be in a binary's
  environment is now read ([secrets as files](deployment.md#secrets-as-files)).
  Every binary reads all of them at start and refuses to start when the
  variable itself is also set, or the file is unreadable, blank or not
  UTF-8, even for a secret that binary never uses. Look for stray `*_FILE`
  variables first, an entrypoint wrapper that does `export X=$(cat
  $X_FILE)` among them.
- Do not deploy a service while the platform's replicas are of two
  versions (the runbook rules below).

**What is new:**

- **Private tables** (feature `private_tables`): `service! { private_tables:
  [...] }` declares adopted tables only the service's own SQL reaches. The
  platform checks their columns at every deploy and rollback and serves
  none of them; `GET /api/services/{id}/schema` gains `private_tables`, and
  `cufflink status` and `cufflink deploy --plan` list them
  ([private tables](schema-ownership.md#private-tables)).
- **Inactive deploys** (feature `inactive_deploy`): `cufflink deploy
  --inactive` (`POST /api/services/deploy?inactive=true`) creates a service
  that routes nothing, consumes nothing and fires no schedule until it is
  enabled; the answer, the plan and the job record gain `status`. `cufflink
  services enable|disable` gain `--tenant`, which is refused for a tenant
  other than the environment's own unless `CUFFLINK_PLATFORM_API_KEY` is
  set: without it the tenant's own key would have acted on the tenant of
  `Cufflink.toml`, and nothing is sent. The registry now holds what the
  database says is active, on every replica
  ([deploying a service inactive](deployment.md#deploying-a-service-inactive)).
- **Config as one set** (feature `config_apply`): `cufflink config apply
  [--prune] [--check] [--service <name>]` and `POST
  /api/services/{id}/config/apply` write a service's config and secrets in
  one transaction, audited as `config.apply` (`config.check` for a dry
  run). `--check` exits 2 when the stored set differs. `--service` naming a
  service other than the one the project's `Cufflink.toml` names is
  refused, whatever the flags, before the vault is read or anything is
  sent, as `deploy --bundle` refuses a bundle of another service
  ([service config as code](deployment.md#service-config-as-code)).
- **Bundles and rollback to a commit:** `cufflink build --bundle <file>
  [--commit-hash <sha>]` writes what a deploy would send, `cufflink deploy
  --bundle <file>` deploys it without a toolchain, and `cufflink rollback
  --commit <sha> [--service <name>]` rolls back to the version built from a
  commit ([bundles](deployment.md#bundles),
  [rolling back to a commit](deployment.md#rolling-back-to-a-commit)).
- **Secrets from files:** every secret setting of the platform, the worker
  and the runners can be given as `<name>_FILE`
  ([secrets as files](deployment.md#secrets-as-files)).
- **Verifying a key ring:** `cufflink admin secrets reseal --operation
  verify` (`POST /api/admin/secrets/reseal`, operation `verify`) reports
  what the configured key-encryption keys can and cannot open, reading one
  snapshot and taking no lock; the report gains `verified`
  ([verifying a key ring](db-roles-and-secrets.md#verifying-a-key-ring)).
- **Tenant-scoped telemetry:** spans and log records carry the tenant,
  service, version, handler and kind they ran for, log records carry their
  trace, and the canonical collector files them under the service
  ([observability](observability.md)). Three settings, each at its
  v0.29 behaviour by default: `LOKI_LABEL_MODEL` (`legacy` | `scoped`) for
  the logs API, under which `cufflink logs` also returns the lines of
  handlers a worker ran, entries gain `level` and `fields`, the answer of
  `GET /api/services/{id}/logs` carries `label_model: "scoped"` beside
  `logs` (nothing is added under `legacy`), and `cufflink logs --output`
  writes a file of one form per answer: every entry as JSON when the
  answer says `scoped`, else each line as it is, which is what v0.29.2
  wrote; `CUFFLINK_CHILD_TELEMETRY` (`legacy` | `scoped`) and
  `CUFFLINK_CHILD_OUTPUT_LEVEL` (`debug` | `info`) for runner children
  ([children's telemetry](deployment.md#childrens-telemetry)). A new
  metric, `cufflink_service_info{tenant, service, version, commit, mode}`.
- **Canonical monitoring assets** in `infra/monitoring`, proven end to end
  by CI's `observability` job: the collector, Loki, Tempo and Prometheus
  configs, the Grafana datasources and three dashboards (Tenants: tenant,
  service; Platform), and a collector for a runner that pins its tenant
  ([the canonical monitoring assets](observability.md#the-canonical-monitoring-assets)).
  They replace the two dashboards and the collector config of earlier
  releases, which wrote the legacy Loki shape: the repository's compose
  stack therefore sets `LOKI_LABEL_MODEL=scoped`,
  `CUFFLINK_CHILD_TELEMETRY=scoped` and `CUFFLINK_CHILD_OUTPUT_LEVEL=info`,
  puts the collector on the `runner` network, gives the ext-runner the
  endpoint and no longer scrapes NATS. An install that vendored the old
  files and keeps `legacy` keeps its own collector config.
- **Backup and disaster recovery** for a dedicated platform and for an
  adopted database are documented
  ([backup](backup.md#backup-and-disaster-recovery)).

**Before turning on `LOKI_LABEL_MODEL=scoped`:** under `scoped` the logs
API returns whatever Loki holds under the tenant's `service_namespace`, so
whoever can post to the collector can write lines that appear in another
tenant's `cufflink logs` and dashboards. Native code a runner starts is
told the collector's address. Where runners of more than one tenant share
a collector, each runner must export through a collector of its own that
pins its tenant, `infra/monitoring/otel-collector-tenant.yaml`, before
`scoped` is enabled ([a runner's collector](deployment.md#a-runners-collector),
[who can write a tenant's lines](observability.md#who-can-write-a-tenants-lines)).
The repository's compose stack shares one collector with its ext-runner:
it is one developer's stack, not a pattern for a shared install. The same
boundary held under `legacy` in earlier releases for a collector that
writes JSON bodies, and guests reach internal addresses, a collector and
its stores among them, until their tenant's `egress` policy is `enforce`
([tenant isolation](tenant-isolation.md)). At the defaults this release
opens no way to read or write another tenant's lines that v0.29.2 did not
have.

**What changes at the defaults** (the complete list; nothing else differs
for an install that changes no setting and passes no new flag):

Platform, worker and runners:

1. A disabled service stays disabled when it is deployed, rolled back or
   has a module uploaded. The registry holds what the database says, so no
   replica routes or consumes for it afterwards; in v0.29 the replica that
   took the request did until its next reload.
2. A version made by a rollback carries the commit of the version it
   restores: `_meta/health.commit_hash` and the deployments listing show
   it, where they showed `null`.
3. Under `/svc`, a request's span name and the `route` label of
   `cufflink_http_request_duration_seconds` come from what a handler
   resolved, never from the path. Anything answered before that is `METHOD
   unmatched`, on a service that exists too: an unknown tenant or service;
   a rate limit (429), load shedding (503), a body limit (413) and
   preflights; a body or query the framework rejects (400, 415, 422); a 401
   from a route that demands a token; a 405; `_fn/{handler}` for a handler
   the service does not declare; a path of a `routing: strict` service that
   matches nothing. A table the service does not serve is
   `{tenant}/{service}/:id`, as is a declared route whose first segment is
   a parameter. `/svc/{tenant}/_jobs/{id}` and `…/retry` are named
   `{tenant}/_jobs` (were `{tenant}/_jobs/:id`).
4. `cufflink_crud_requests_total` and the `crud request` log line are
   written once the service and the table are resolved: not for an unknown
   service or table, a request refused at tenant or principal binding, or
   a batch over the size limit.
5. Spans and the `span`/`spans` of stdout JSON lines gain `cufflink.*`
   fields. Logs exported over OTLP carry a trace and span id. Every
   binary's OTLP resource gains `cufflink.component`, and a tenant-scoped
   runner's `cufflink.tenant`. A request span is a root joined to its
   caller's trace by headers alone. A subscription delivery, a schedule
   fire and a worker job without a propagated trace start a trace of their
   own, and their stdout lines' `spans` no longer begin with the pump's
   `task`.
6. New spans: `backup_job`; `child_output`, one for each ten-second window
   in which a runner logged a child's output (none at the default
   `CUFFLINK_CHILD_OUTPUT_LEVEL=debug` unless that target is enabled);
   `outbox_relay`, for a pass that has rows to relay; and a `request` span
   for each service call dispatched in process, under which the called
   service's lines are logged (they were under the caller's). A worker's
   service call over HTTP sends `traceparent`. A guest's own `traceparent`
   and `tracestate` headers on a service call are dropped.
7. Logs exported over OTLP no longer carry the diagnostics of
   `opentelemetry*`, `h2`, `hyper`, `hyper_util`, `reqwest`, `rustls` and
   `tonic` (stdout still has them). A binary that exports over OTLP now
   stops cleanly: it could abort at shutdown, and under `RUST_LOG=debug`
   exported once a second for ever.
8. One new metric on the platform's `/metrics`, `cufflink_service_info`.
   `cufflink_registry_reload_total{result}` also counts each single-service
   refresh (a deploy, rollback, upload or enable, and a skipped deploy that
   moves its commit, which now also tells the other replicas); deploys in
   parallel can count a harmless `result="error"`. `PATCH
   /api/services/{id}/status` to `active` refreshes that service rather
   than reloading the registry, so `cufflink_registry_version` does not
   move, and the other replicas are told even when this one's read fails.
9. A rollback is refused (422) when the target version needs a feature the
   platform lacks, as a deploy is.
10. Runner children are never given a secret setting or its `_FILE` form,
    whatever the passthrough lists: `TENANT_DATABASE_URL`,
    `CUFFLINK_DEFAULT_KC_OPS_CLIENT_SECRET` and the other runner's status
    token are newly denied. With no passthrough set nothing changes.
11. A deploy's exports are inspected from module storage when the module
    has left the cache, so a deploy v0.29 failed with `is not loaded` now
    succeeds.
12. A table that goes from platform-made to `managed = false` under another
    spelling of its name (`Notes`, then `notes`) is no longer planned as a
    drop.
13. A stored service status that is neither `active` nor `inactive` is
    listed as `inactive`. The log line `Not routing a service whose route
    table does not build` is replaced by `Skipping service whose stored
    manifest cannot be decoded or routed`
    (`cufflink_registry_load_errors_total` counts it as before).
14. The logs API answers a service's lines oldest first whatever stream
    Loki returned them in; `/logs/tail` sends each line once, reads on from
    one nanosecond past the latest line it sent (a poll that found nothing
    no longer moves the start on by a nanosecond) and stops polling Loki
    when the client disconnects; an entry with more than two elements is
    returned (it was dropped) while one whose timestamp or line is not a
    string is skipped (it was returned empty).
15. Response fields are added and none removed or retyped: `status` on the
    deploy answer, plan and job record; `private_tables` on the schema
    route (`[]` when none); `verified` on a reseal report when asked for;
    `label_model` on the logs answer under `LOKI_LABEL_MODEL=scoped` only.
    `GET` and `PUT` on `/api/services/{id}/config/apply` answer 405 with
    an `Allow` that also lists `POST`; `DELETE` there still deletes the key
    named `apply`.
16. `CUFFLINK_SERVICE_IDENTITY_KEYS` errors name an entry by its position
    and never print it. The plaintext-storage refusal of a reseal ends
    `before resealing or verifying`. `cufflink-nats`: `NatsAuth::from_vars`
    treats an empty variable as unset.

CLI (a v0.30 CLI, against a platform of either release unless said):

17. With a v0.30 platform, a deploy's config step is one transaction: an
    unchanged key keeps its `updated_at`, and the audit log gets one
    `config.apply` row instead of a `config.set` for each key. Its output
    changes from `  Synced N config(s) and M secret(s)` to `  Applied
    config: …` and a line for each key that differs.
18. Against an older platform the step is still a request for each key,
    but the vault is read (and its warning printed) before the first one,
    keys go in key order, and a key in both `[config]` and `[secrets]` is
    one request, as the secret. `cufflink config sync` and a web app's
    deploy with config or secrets ask `/api/version` first; `config sync`
    of an environment that declares nothing sends nothing.
19. A source deploy that is skipped uploads the module it already built:
    `Building WASM module...` and `  WASM artifact: …` are not printed a
    second time.
20. Wording: `Service 'x' not found on platform: deploy it first` (rollback
    and `config list|set|delete|sync`, which now look the service up before
    building); a failed build in `rollback` and `config apply` ends ` Or
    name the service: --service <name>`; a manifest without a name is an
    error; `Failed to list services: <status>`; `No environment given: pass
    --env <name> or set [service].default_env in Cufflink.toml` when the
    file exists and no environment resolves; a blocked rollback's hint is
    `cufflink rollback --version <n> --service <name> [--env <env>]
    --allow-destructive`; an `/api/version` that answers 200 without JSON
    is `did not answer as a platform does`.
21. `cufflink status` prints no `Tables:` or `Endpoints:` heading for a
    service that serves no table. `cufflink rollback` in a web project
    reads the service name from `Cufflink.toml`. `cufflink logs` colours
    `CRITICAL` and `FATAL` red. The `--help` of `deploy --tenant` and
    `workspace deploy --tenant` says it takes effect only with
    `CUFFLINK_PLATFORM_API_KEY` set, which is what it always did.
    `cufflink-cli` depends on `flate2`.

**Runbook rules:**

- **No service is deployed while the platform's replicas are of two
  versions**, in either direction: a v0.29 replica ignores `inactive`, and
  registers a module upload whatever the service's status. The CLI checks
  the answer of an `--inactive` deploy and disables a service a replica
  started, and after a skipped deploy's module upload looks the service up
  and sets it `inactive` again where it still is (never overruling whoever
  enabled it meanwhile), but only the rule closes the window.
- **Do not `cufflink rollback`, or re-upload the module of, a service that
  declares `private_tables` on a platform rolled back to v0.29:** it stores
  a version without the list. If it happened, redeploy the service once the
  platform is back on v0.30.0.
- **A private table that goes back to a managed, served table is taken
  over:** the platform runs its `CREATE … IF NOT EXISTS` statements and
  rebuilds the search vector it keeps, which rewrites the table. Treat that
  deploy or rollback as a migration window
  ([private tables](schema-ownership.md#private-tables)).
- **`scoped` and shared collectors:** the paragraph above.
- **Scripts that read the CLI's output** meet items 17 to 21.
- The release run now also starts the collector, Loki, Tempo and
  Prometheus images; a registry rate limit fails it before anything is
  published, and a re-run mends it.

**Upgrading:** an image change. Upgrade the platform, the worker and the
runners together; nothing is set and nothing migrates. A CLI of this
release works against a v0.29 platform (item 18): it refuses `--inactive`
and `config apply` there, deploys a bundle as it deploys sources, and
rolls back by commit, though the version that platform activates reports
no commit.

**Rolling back** to v0.29.2 is an image change too, services first
([rolling a platform back](#rolling-a-platform-back)): give secrets set as
`*_FILE` as plain variables again; the three telemetry settings need
nothing; a service that is inactive stays so, but v0.29 starts its
subscriptions and schedules on its next deploy; config applied as a set is
ordinary rows; the database needs nothing. A v0.29 binary that exports
over OTLP has the shutdown abort again (item 7).

## v0.29.2 release notes

**Version set (tag `v0.29.2`):** `cufflink-types`, `cufflink-sdk-macros`,
`cufflink`, `cufflink-migrate` and `cufflink-pgcodec` 0.13.2;
`cufflink-fn` 0.29.2; `cufflink-cli` 0.20.2; `cufflink-nats` and
`cufflink-native` 0.1.2; the platform, worker, web-runner and ext-runner
images of the same commit. `cufflink init` scaffolds `cufflink = "0.13"`
and `cufflink-fn = "0.29"`.

A tenant that enforces `tenant_binding` no longer records a policy
violation each time caller credentials are stripped from a custom route
without `forward_auth`. Browsers send them on every call, so v0.29.1 logged
`Policy violation` (`reason=auth_header_forwarded`, outcome `denied`) and
counted `cufflink_policy_violation_total` on most `_fn` requests, burying
the violations that matter. The credentials are still stripped; each strip
is now counted in `cufflink_route_credentials_stripped_total{tenant,
service}`, so dashboards or alerts on `auth_header_forwarded` under
`enforce` go quiet and should watch that metric instead. Under `warn` the
violation is recorded and the credentials forwarded as before, and every
other `tenant_binding` reason is unchanged. Only the platform changes:
guests need no rebuild.

## v0.29.1 release notes

**Version set (tag `v0.29.1`):** `cufflink-types`, `cufflink-sdk-macros`,
`cufflink`, `cufflink-migrate` and `cufflink-pgcodec` 0.13.1;
`cufflink-fn` 0.29.1; `cufflink-cli` 0.20.1; `cufflink-nats` and
`cufflink-native` 0.1.1; the platform, worker, web-runner and ext-runner
images of the same commit. `cufflink init` scaffolds `cufflink = "0.13"`
and `cufflink-fn = "0.29"`.

Guests that enable uuid's `js` feature no longer import wasm-bindgen
functions. In v0.29.0 `cufflink-types` depended on `uuid` for the ids of
the extension-call and runner-API messages, so every guest compiled it
through `cufflink-fn`; a guest whose own `uuid` dependency enables `js`
then imported `__wbindgen_placeholder__` and `__wbindgen_externref_xform__`
functions no platform provides, and its deploy was refused. Those messages
are now behind the `cufflink-types` feature `protocol`, which the platform,
the runners and `cufflink-native` enable and `cufflink-fn` and `cufflink`
never do, so a guest's dependencies are v0.28.0's again. Nothing else
changes: rebuild a guest against `cufflink-fn` 0.29.1 and redeploy it.

## v0.29.0 release notes

**Version set (tag `v0.29.0`):** `cufflink-types`, `cufflink-sdk-macros`,
`cufflink`, `cufflink-migrate` and `cufflink-pgcodec` 0.13.0;
`cufflink-fn` 0.29.0; `cufflink-cli` 0.20.0; `cufflink-nats` and
`cufflink-native` 0.1.0, published for the first time (their first publish
needs a `CARGO_REGISTRY_TOKEN` with the `publish-new` scope whose crate
pattern covers both names: step 3 of the
[release checklist](#release-checklist)); the platform, worker, web-runner
and ext-runner images of the same commit. `cufflink init` scaffolds
`cufflink = "0.13"` and `cufflink-fn = "0.29"`.

This release adds extensions ([the guide](extensions.md)): `ext_call` from
WASM services, native services run by the new extension runner, and NATS
providers. It also closes the ways found for one tenant to reach another
(**tenant isolation**, below). With `CUFFLINK_EXTENSIONS` at its default
(`false`) and no extension runner, a platform of this release serves every
R3 service as R3 did, apart from the tenant-isolation changes below, some
of which apply at the defaults (`tenant_binding: warn` and `redis_scope:
warn`); a manifest that declares no `extensions` and is not `mode: native`
keeps its deploy hash.

**Before the first start of this release (operators):**

- Migrations `V5__extensions` and `V6__runner_keys` create the tables
  `tenant_extensions` and `runner_keys`. They are additive, so an R3
  binary runs against the migrated database.
- Find the tenants that share a realm and deploy role, which this release
  refuses to update (below), before the upgrade:

  ```sql
  SELECT keycloak_realm, deploy_role, array_agg(slug)
  FROM tenants
  WHERE source_tenant_id IS NULL AND keycloak_realm IS NOT NULL
  GROUP BY 1, 2 HAVING count(*) > 1;
  ```

  Each row's tenants keep working, but any update of one of them answers
  409 until it has a deploy role of its own. Create that role in the
  tenant's Keycloak realm and grant it to the tenant's deployers first,
  then set it (`cufflink tenants update <tenant> --deploy-role <role>`):
  a tenant whose deploy role nobody holds has no deployers.
- Find the callers that reach a tenant's data routes with another
  tenant's credentials: under `tenant_binding: warn`, the default, they are
  now served as unauthenticated (below). R3 already counts them in
  `cufflink_policy_violation_total{key="tenant_binding"}`, by `tenant`
  and `reason`; give each credentials of the route's tenant before the
  upgrade.
- Nothing else is required: registering providers and deploying native
  services stay refused (403) until the platform is started with
  `CUFFLINK_EXTENSIONS=true`. Upgrade the web-runners with the platform
  before turning it on: an R3 web-runner does not tell a native
  deployment's binary from a web artifact.

**What is new:**

- The platform and the worker serve `ext_call` (feature `ext_call`), and
  the platform takes `mode: native` deploys (feature `native_services`)
  with the tenant's ordinary deploy authority.
- A fourth image, `kingsleyh/cufflink-ext-runner`, runs one tenant's
  native services (`EXT_TENANT`) with that tenant's runner key, reading the
  platform's new runner API (`/api/runner/native`), and holds no database,
  secrets or storage credentials; its settings are in
  [deployment](deployment.md#extension-runner) and its metrics in
  [observability](observability.md). A platform admin mints runner keys,
  each for one kind of runner (`/api/tenants/{slug}/runner-keys`,
  `cufflink tenants runner-keys create <tenant> --for native|web`). The
  repository's compose runs one under the profile `ext-runner`, on a
  network with the platform and NATS alone.
- `native/` joins the storage prefixes no guest may touch.
- `cufflink-fn` gains the `ext-call` feature and the test host `host.ext`;
  `service!` gains `extensions`; `cufflink-native` (the SDK of native
  services and NATS providers) and `cufflink-nats` are published for the
  first time, which needs a token that may publish new crates (step 3 of
  the [release checklist](#release-checklist)).
- The CLI deploys, rolls back, deletes and switches native services with
  the environment's own credentials, administers NATS providers (`cufflink
  ext`) and runner keys (`cufflink tenants runner-keys`), and runs the
  extension runner in the development stack (`cufflink dev up
  --with-ext-runner`) as its tenant's, whose user `alice` is now a platform
  admin of it (for NATS providers and runner keys).
- The source Dockerfiles build again: their builders install `libssl-dev`,
  which `openssl-sys` needs.

**Turning extensions on** is an operator's decision: run an extension
runner per tenant that deploys native services, each with that tenant's
runner key, NATS user and cloud identity alone, on a network that reaches
the platform and NATS alone (no route to Postgres, Redis or object
storage, since any of the tenant's deployers can deploy the native code it
runs), and set `CUFFLINK_EXTENSIONS=true`. Turning it off again stops new registrations
and native deploys; native services can still be stopped, started and
deleted
([extensions](extensions.md#turning-extensions-on),
[security](extensions.md#security)). Roll a platform back below v0.29.0 only
after every service declaring `extensions` is rolled back to a version
that declares none and every native service is deleted: an R3 platform
cannot read either manifest.

**Tenant isolation.** This release also closes the ways found for one
tenant's deployed code or credentials to reach another tenant that needed
no change to a default; [tenant isolation](tenant-isolation.md) lists the
settings an install hosting more than one tenant still sets. What changes:

- Under `tenant_binding: warn`, the default, a principal of another tenant
  is served as unauthenticated (an `auth_required` table answers 401, and a
  guest receives no claims), where it kept its identity without its
  service-account, deploy and platform-admin flags. `enforce` still
  refuses it with 403.
- Another tenant's service id answers 404 on every `/api/services/{id}`
  route, as one that does not exist, where it answered 403.
- Creating or updating a tenant that is not a preview answers 409 while
  another tenant that is not a preview uses the same realm with the same
  deploy role (find those before the upgrade with the query above). A
  preview whose source tenant is deleted is no preview any more
  (`source_tenant_id` is cleared, and previews are tenants of their own,
  not deleted with it): while it shares its realm and deploy role with
  another tenant, updating it answers 409 too, so delete such previews
  with their source, or give each a deploy role of its own.
- The worker answers request/reply jobs only on `_INBOX.` inboxes
  (`cufflink_worker_reply_refused_total`), and runs a backup job from its
  row, once; a job whose worker stopped mid-run is run again by its
  message's redelivery, and one whose row could not be read is tried
  again, 30 s apart, rather than acknowledged.
- Under `redis_scope: warn`, the default, a guest's delete removes its
  prefixed key only (it removed the unprefixed key too, which may be
  another tenant's), leaving a tombstone there while the unprefixed key
  exists, so the deleted value is not read back from it.
- Under `db_roles: enforce`, `cufflink sql` runs as the service's role.
- The logs API quotes its `search` and `level`.
- An extension runner whose key the platform refuses (401), or answers
  for another tenant, stops every child and stays unready until a key it
  accepts is in place.
- A guest's `ext_call` that no subscriber received is sent again for up to
  a second within its timeout, so a runner that has just subscribed is not
  missed and an extension nothing serves is still unavailable within a
  second.
- The web runner makes itself undumpable, and serves one tenant from the
  runner API (`/api/runner/web`) with `WEB_TENANT`, `PLATFORM_URL` and a
  runner key minted `--for web`
  ([tenant-scoped web runner](deployment.md#tenant-scoped-web-runner)). It
  unpacks each bundle once per service and content
  (`APPS_DIR/{tenant}/{service}/{service id}-{sha256}`; the directories of
  earlier releases are removed as each app is next started) and takes from
  a deploy event only which app changed.

## R3 release notes

**Version set (tag `v0.28.0`):** `cufflink-types`, `cufflink-sdk-macros`,
`cufflink`, `cufflink-migrate` and `cufflink-pgcodec` 0.12.0 (migrate and
pgcodec are published for the first time, in step with types);
`cufflink-fn` 0.28.0; `cufflink-cli` 0.19.0; the platform, worker and
web-runner images of the same commit. `cufflink init` scaffolds
`cufflink = "0.12"` and `cufflink-fn = "0.28"`.

With every policy and manifest option at its default, a service of
v0.27.0 behaves as it did: its durables are named `{service}_{handler}`,
every message is acknowledged, `on_migrate` runs on every deploy that is
not skipped (one whose manifest and module are both unchanged runs
nothing, as in v0.27.0), the migrator drops nothing it was not told to,
and a manifest's deploy hash is unchanged. What follows is what an
operator upgrading from v0.27.0 meets anyway.

**Before the first start of this release (operators):**

- **`WEB_ENV_PASSTHROUGH=*` stops the web runner at startup**, naming the
  built-in allowlist; list the variables an app needs (`NAME,PREFIX_*`).
- **A long job's consumer is held to `LONG_JOBS_ACK_WAIT_SECS`, now 60 s**
  (it was an hour), with a heartbeat; a value you set is kept. During a
  rolling upgrade a v0.27.0 worker sends no heartbeat, so a job of its that
  outlasts the minute is delivered again, found held and acknowledged; the
  reclaimer publishes it again if that worker dies.
- **At the first start, the worker updates two consumers in place:**
  `CUFFLINK_LONG_JOBS/cufflink-long-jobs` to the ack wait above, and the
  after-hook consumer `CUFFLINK_WASM_JOBS/wasm-worker` from 35 s to 60 s.
  Neither is recreated or replays. The platform widens
  `CUFFLINK_LONG_JOBS`'s duplicate window from 120 s to 600 s and creates
  `CUFFLINK_DLQ` and `CUFFLINK_SCHEDULES` (with the outbox on,
  `CUFFLINK_AUDIT` and `CUFFLINK_WEBHOOKS` too): count them against the
  account's stream limits.
- Migration `V4__r3` waits up to 10 s at a time for its lock on
  `wasm_jobs` behind long readers of it, and startup applies it again
  until it has it; `/health` answers meanwhile and `/ready` once it has
  applied. A job a v0.27.0 replica enqueues during the rolling upgrade may
  be delivered a second time, which the worker acknowledges without
  running it.
- Secret encryption, per-service database roles and the transactional
  outbox are policies, off by default: see [policies](policies.md) and
  [database roles and secrets](db-roles-and-secrets.md) before turning
  them on. Each needs the same key on every replica:
  `CUFFLINK_SECRETS_KEK` (and `_KEK_ID`) on every platform, worker and
  web-runner replica for secrets and roles; `CUFFLINK_SECRETS_KEK` or
  `CUFFLINK_CACHE_SIGNING_KEY` on every platform and worker replica before
  `outbox: enforce`: without one the platform neither sets the policy nor
  starts with it as the default. Turning the outbox on creates the
  `_cufflink` schema in each tenant database it applies to.
- **Stay on `CUFFLINK_SECRET_STORAGE=dual` until rolling back below this
  release is ruled out**: a release before it reads no secret stored
  `encrypted` ([leaving `encrypted`](db-roles-and-secrets.md#stored-secrets)).
  Before rolling back from `db_roles`, turn the policy `off` and hand each
  service schema back with `REASSIGN OWNED BY <service role> TO <platform
  role>` in each tenant database: an earlier release runs as the
  platform's role and does not own them otherwise.

**What changes at default:**

- **Every replica runs the consumer loops of every service** (the deploying
  replica alone did), and reconciles them every
  `CONSUMER_RECONCILE_INTERVAL_SECS`. A durable the platform creates waits
  `CONSUMER_DEFAULT_ACK_WAIT_SECS` (30 s, as before). Leave it unset to keep
  v0.27.0's behaviour: any other value updates every durable the platform
  manages at its service's next deploy, and restarts its loops on every
  replica.
- **Deleting a service or a tenant deletes the durables the platform
  manages for it** (v0.27.0 left them); a durable a subscription binds
  (`durable = "…"`) is never deleted, and one whose service is being
  deployed at that moment is left, with a warning.
- **A deploy that changes a subscription's filter updates its durable in
  place** (it was deleted and recreated, which replayed a stream that keeps
  its messages); a deploy that removes every subscription deletes the
  service's durables. A subscription that cannot be placed is a 422 before
  anything changes, and a deploy that needs JetStream while it is down is a
  503, where both were a 500 after the version was activated.
- **After-hooks** (`wasm-worker`) are held to a 60 s ack wait (it was
  35 s), with a heartbeat while they run; one interrupted as the worker
  stops is handed back (it was acknowledged); one that fails its third
  delivery goes to the dead-letter queue (`cufflink dlq`, kept
  `CUFFLINK_DLQ_MAX_AGE_DAYS`, 14) where it was left undelivered.
- **Jobs:** a job route whose backlog is full answers 429 (it was 500), and
  a job whose publish failed is accepted with 202 and published by the
  reclaimer. A job's stored request keeps no credentials. A crashed job is
  attempted again after its backoff (30 s, doubling to an hour), not after
  the ack wait; a job that exhausts its attempts fails with
  `worker lost after N attempts`. A job that waits for a worker is published
  again every `JOB_REPUBLISH_AFTER_SECS` (300 s), and never less often than
  `CUFFLINK_LONG_JOBS`'s duplicate window, so each publish is stored. A
  message on a job subject that names no job is terminated (v0.27.0
  acknowledged it); what a handler asks a job to wait is held to the job's
  longest backoff.
- **`db-v2`: a guest whose connection checkout outlasts the pool's wait**
  gets `PoolTimeout`, as [db-v2](db-v2.md) documents; v0.27.0 could report
  it as `QueryCanceled` when the call's deadline fired first.
- **A NATS subject is 1024 bytes at most** (the server closes a connection
  whose control line passes 4096). A guest publish on a longer subject is
  refused, and so is a manifest whose subscription subject, filter subject
  or stream subject, as declared, is longer; a subscription subject that is
  not a subject pattern, which NATS refuses to filter on, is refused too,
  at build and at deploy. A guest `nats::publish`/`request` on what is not
  a subject (a wildcard, an empty token, a subject too long) is counted in
  `cufflink_nats_publish_denied_total{reason="invalid"}`, where v0.27.0
  counted it `reserved`.
- **A batch write publishes**, for each row it wrote, the event and the audit
  record a single write publishes; a batch `delete` of a soft-deleted row
  leaves it and publishes nothing.
- **The durable audit record of a write made with a user's API key names
  that user** (it named the tenant's owner). `dw.{tenant}.logs`, which
  audit services consume, is unchanged: it still names the owner.
- **Deploys run to their end** when the CLI gives up on them; a deploy of
  the same service meanwhile is a 409 until it ends. The deploy answer
  gains a `plan` key.
- **An archived column drops `NOT NULL`**; a restore sets it again, and
  fails if rows were written without the column in between. An index
  redeclared in another shape under its name is replaced only under
  `schema_guard: enforce`, with a warning in the plan elsewhere.
- **`cufflink sql --limit`** wraps the query (`SELECT * FROM (…) q LIMIT n`,
  without the comments and `;` that trail it) where it appended `LIMIT` to a
  query without one. It no longer applies to a statement that is not a
  query, to several statements, or to a query that writes (a `WITH` that
  changes data, `SELECT … INTO`): those are sent as typed, and the CLI says
  so.
- **`cufflink deploy`** prints the platform's version and features beside
  what the service needs, and a CLI of this release against a platform that
  advertises `async_deploy` deploys asynchronously and follows the deploy;
  `--plan` exits 3 only when the platform refuses the deploy.

**Per-service database roles** are turned on a tenant at a time, `warn`
before `enforce`, once the platform's role may create roles and the
key-encryption key is on every replica:
[database roles and secrets](db-roles-and-secrets.md#rollout-for-a-tenant).

**New, opt-in:** consumer configuration and scoped durables, delivery
control and the dead-letter queue (`cufflink consumers`, `cufflink dlq`),
streams, publish v2, schedules, jobs v2 and `cufflink jobs retry`, HTTP,
storage and util v2, the outbox and durable audit, webhook deliveries,
adopted schemas and SQL migrations, the schema guard, column types v2,
declared search and keyset lists, per-service database roles, secret
encryption, the native test host (`cufflink-fn/test-host`), the in-process
testkit and [`cufflink dev`](dev.md). The variables and metrics they add
are in [setup](setup.md#platform-configuration) and
[observability](observability.md).

**Upgrading the SDK (service authors):**

- Optional, and to be done for `cufflink` and `cufflink-fn` together: the
  macros of `cufflink` 0.12 build the manifest types of `cufflink-types`
  0.12, which `cufflink-fn` 0.28 uses too. A service that only declares
  its manifest builds unchanged, emits the same manifest and keeps its
  deploy hash; its first deploy after the bump is not skipped (the module
  changed) and changes no schema. Modules grow by 5–62 % (a small service
  by the most).
- Breaking changes, for code that constructs or matches the types:
  - `ColumnType::to_sql` returns `Cow<'static, str>`: `&*` or `.as_ref()`
    where a `&str` was used, `.into_owned()` where a `String` was.
  - `ColumnType` has new variants (`Numeric`, `Varchar`, `TimestampNaive`,
    `Time`, `Bytea` and five arrays) and `PolicyKey` three (`Outbox`,
    `DbRoles`, `SchemaGuard`; `PolicyKey::ALL` has 12): an exhaustive
    `match` needs arms for them.
  - `ServiceManifest`, `TableDefinition`, `CustomRouteDefinition`,
    `JobConfig`, `SubscriptionDefinition` and `IndexDefinition` have new
    public fields, and `JobContext` has `last_error` and `retry_on`: a
    struct literal ends in `..Default::default()`.
- `db-v2` still deprecates `db::sql_escape` in every workspace member
  built with it, through Cargo's feature unification: under
  `-D warnings`, enable it for every member at once or add
  `#![allow(deprecated)]` to each while it migrates ([db-v2](db-v2.md)).

## R2 release notes

**Version set (tag `v0.27.0`):** `cufflink-types`, `cufflink-sdk-macros` and
`cufflink` 0.11.0; `cufflink-fn` 0.27.0; `cufflink-cli` 0.18.0; the platform,
worker and web-runner images of the same commit. The crate changes are
breaking in the 0.x sense (new public fields and enum variants), which is
why every crate moves a minor version; a service that constructs none of
them builds unchanged. `cufflink init` scaffolds `cufflink = "0.11"` and
`cufflink-fn = "0.27"`.

**Before the first start of this release (operators):**

- Rotate every long-lived platform-admin key with the **0.18 CLI**
  (`cufflink auth keys rotate <id> --admin`; older CLIs have no `--admin`)
  and update the secret that holds it (a pipeline's
  `CUFFLINK_PLATFORM_API_KEY`, for one), or set `CUFFLINK_ADMIN_API_KEY_TTL_DAYS=0`.
  Otherwise such keys stop working 90 days after this release first starts,
  and rolling back does not stop the clock ([undo](deployment.md#api-keys)).
- Pin the CLI to 0.18 wherever the platform is upgraded.

**Upgrading the SDK:**

- Optional and drop-in: bump `cufflink` and `cufflink-fn` together.
- `serde_json`'s `float_roundtrip` is on for every guest and the platform
  (through `cufflink-types`): a float parses to the nearest `f64` exactly,
  where it could differ in the last bit before.
- `db-v2` deprecates `db::sql_escape`, and Cargo's feature unification
  applies that to every workspace member built with it: under
  `-D warnings`, migrate all members together or add `#![allow(deprecated)]`
  to each crate while it migrates ([db-v2](db-v2.md)).
- `Response::redirect` takes a `Redirect`; a response-v2 status is 200–599.

- **Web runner:** apps start with an allowlisted environment instead of the
  runner's whole environment, and listen on `127.0.0.1`. Verify web apps on
  staging before promoting; `WEB_ENV_PASSTHROUGH` widens the list
  (`*` restored the old behaviour for that release only and is refused
  since).
- **Platform-admin API keys expire** after `CUFFLINK_ADMIN_API_KEY_TTL_DAYS`
  (default 90), including existing keys, whose 90 days start at the first
  start of this release (rolling the image back does not stop the clock).
  Before rolling out, rotate long-lived admin keys, such as CI preview keys,
  with `cufflink auth keys rotate --admin`, or set the variable to `0` to
  keep them unbounded.
- **API key errors are specific:** an unknown key answers
  `401 {"error": "Invalid API key"}` and an expired one
  `401 {"error": "API key expired"}` (both were
  `Invalid or expired API key`). Telling them apart needs the secret, so
  neither reveals which keys exist.
- **Web runner operational routes** moved under `/_cufflink/` so an app's own
  paths reach it: readiness is `/_cufflink/ready`, and `/_cufflink/status`
  and `/_cufflink/metrics` need `Authorization: Bearer $WEB_STATUS_TOKEN`
  (not served without it). `/_cufflink/metrics` is new: earlier runners
  served no metrics ([catalogue](observability.md#web-runner)). `/health`
  is unchanged.
- **Invalid `?count=` or `?filter_mode=`** answers 400 (they were ignored):
  `?count=` and `?filter_mode=` on a list, unless the table has a column of
  that name, and `?filter_mode=` on a create, update or batch.
- `cufflink-fn`: `Auth` and `JobContext` derive `Default`; build them in
  tests with `..Default::default()` so later fields are not breaking.
- **CRUD constraint errors** answer 409 (a unique or exclusion violation, a
  referenced row, a serialisation conflict), 422 (an invalid reference on
  create or update, a check or not-null violation, a raised exception), 400
  (an invalid value) or 503 (a cancelled statement) with the same
  `{"error"}` body instead of an opaque 500, counted in
  `cufflink_crud_db_errors_total`. Delete still answers a JSON body.
- **A startup that can never succeed exits with an error** instead of
  running unready: rejected database credentials, a missing database or
  privilege, or applied migrations this binary contradicts. Orchestrators
  now restart (and surface) such a pod; `STARTUP_DEADLINE_SECS` optionally
  bounds a startup still waiting on a dependency.
- **Redeploying the same build heals a service whose module storage lost**:
  the upload stores it again (`"restored": true`).
- **Service calls** carry only their identity: a guest's `authorization`,
  `cookie`, `x-api-key` and forwarded client-address headers are dropped,
  and depth and budget apply to every `/svc` route, CRUD hooks included.
- A data route's `413` carries `connection: close`; a client still writing
  a larger body may see a reset instead ([HTTP layers](deployment.md#http-request-layers)).
- `CUFFLINK_WASM_LIMITS` is no longer read; set `CUFFLINK_POLICY_WASM_LIMITS`
  (the platform warns when the old name is set).
- **Custom routes** read their body only after the caller is authorized, with
  the same 4 MiB default and the same 413; an unauthenticated caller of a
  non-public route is refused before its body is read. The request and
  response JSON of a route using no R2 keyword is unchanged.
- Everything else is opt-in: strict routing, route limits, response v2,
  the error profile, event streams, typed routes, the `internal`, `api_key`
  and `scoped` postures, CRUD v2 (`?filter_mode=v2`, `?count=`, the
  `strict_filters` policy), `CUFFLINK_HTTP_PROFILE=hardened`, scoped tables,
  partner keys, `db-v2`, the cache, service calls, `TENANT_DB_POOLER`.
