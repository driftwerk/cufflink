# Building on Cufflink — Architecture Overview for Agents

Audience: an AI agent writing or modifying a product built **on top of** Cufflink (not the
Cufflink platform itself). Everything below is drawn from this repository; file references
point at the source of truth.

---

## What Cufflink is

Cufflink is a **multi-tenant backend platform**. You write a *service* as a Rust crate that
declares its data tables (and, optionally, custom logic). The CLI compiles it and deploys a
**manifest** (+ an optional **WASM module**) to the platform. The platform then:

- provisions the service's database schema and runs migrations,
- auto-generates a **REST + GraphQL API** over the tables,
- runs your custom logic inside a sandboxed **WASM runtime**.

A service runs in one of five modes (`types/src/manifest/service.rs`, `enum ServiceMode`):

| Mode | What it is |
|------|------------|
| `Crud` | Tables only — platform serves auto-generated CRUD. No WASM needed. |
| `Wasm` | Tables + custom Rust logic compiled to WASM (custom routes, hooks, jobs, subscriptions). |
| `Web` | A Node/Next.js web app served by the web-runner. |
| `Container` | A containerised workload. |
| `Native` | A Rust binary built with `cufflink-native`, supervised by the tenant's own extension runner, that serves the extensions it `provides` to the tenant's WASM services. Deployed, rolled back, stopped, started and deleted with the tenant's deploy authority, as any service is; deploying and rolling back need a platform with `CUFFLINK_EXTENSIONS=true`. It has no tables, routes, subscriptions or schedules. |

`cufflink init` scaffolds only `--mode crud` (default) or `--mode wasm`. `Web` and `Container`
services are declared with the `mode:` key in `Cufflink.toml` / the manifest rather than
scaffolded; a `Native` service's manifest is what its binary prints under `--emit-manifest`.
A service keeps its kind: a native service cannot be redeployed in another mode, nor another
service as a native one, without deleting it first.

---

## Core principles

- **A service is the unit of deployment and isolation.** One Rust crate → one manifest →
  one independently versioned deployment.
- **Microservices, not a monolith.** Each service owns its own tables, its own database
  schema, its own versioned WASM binary, and its own authorization config. Services are
  deployed, rolled back, enabled, and disabled independently.
- **No cross-database access.** A service can only touch its own tables. There is **no**
  shared-DB path to another service's data or another tenant's data (see *Data model &
  isolation*). Services that must collaborate do so via **NATS events** or **HTTP**, never a
  shared table. The platform publishes a change event for every CRUD write on
  `dw.{tenant}.{service}.{table}.{created|updated|deleted}` and an audit record on
  `dw.{tenant}.logs` (`platform/src/router/crud.rs`); both land in the tenant's JetStream
  stream `DW_{TENANT}`, which is what `subscriptions:` consume.
- **Declarative data model.** You describe tables as Rust structs; the platform derives the
  SQL schema, migrations, REST routes, GraphQL schema, and OpenAPI spec.
- **Sandboxed custom logic.** WASM handlers reach the outside world (DB, HTTP, storage,
  cache, etc.) **only** through a fixed set of **host functions**. They cannot touch the
  filesystem, spawn processes, or read system clocks/randomness directly
  (`docs/wasm-guide.md`).

---

## The runtime components

Cufflink runs as four separate binaries (`docker-compose.yml`). When building a product
you interact with the **platform**; the worker and the runners are how certain workloads get
executed.

### 1. Platform (`platform/`)
The front door. A stateless HTTP gateway that:
- serves every inbound request — deploys, CRUD, custom WASM routes, GraphQL, SSE;
- holds the service registry and runs the WASM runtime in-process for normal requests;
- manages per-tenant Postgres pools and applies schema migrations on deploy.

### 2. Worker (`worker/`)
Executes work that should not block the request path:
- **long-running / background jobs** (the `jobs` feature),
- backup/restore and data export jobs,
- optionally, specific handlers explicitly routed to workers so heavy compute scales
  independently of the platform (`docs/wasm-workers.md`).

You don't call the worker directly — you **enqueue a job** (from a handler or over HTTP) and
the worker picks it up. See *Jobs*.

### 3. Web-runner (`web-runner/`)
Runs `Web`-mode services (Node/Next.js apps). It lazily starts the app process on first
request, health-checks it, and reverse-proxies traffic to it. Relevant only if your product
includes a web front-end deployed as a `Web` service.

It routes by `Host` header, so it needs (`web-runner/src/config.rs`):

- `CUFFLINK_BASE_DOMAIN` — **required**; the runner exits without it (e.g. `example.com`).
- `CUFFLINK_PUBLIC_SCHEME` — `https` (default) or `http` for a local stack without TLS.
- `CUFFLINK_HOSTNAME_FORMAT` — `flat` (default): apps live at
  `{service}--{tenant}.{base}`; `hierarchical`: `{service}.{tenant}.{base}`. The default
  tenant is served bare (`{service}.{base}`).
- `RUNNER_PORT` (default `3100`), `APPS_DIR` (default `./apps`), plus the same
  `DATABASE_URL`, `NATS_URL`, and object-storage variables as the platform.

Because every app gets its own hostname, web-mode deployments need a **wildcard DNS record
and wildcard TLS certificate** for `*.{CUFFLINK_BASE_DOMAIN}` pointing at the runner.

### 4. Ext-runner (`ext-runner/`)
Runs one tenant's `Native`-mode services: one child per active native service of
`EXT_TENANT`, started from the binary its deploy carried, and relays the tenant's `ext_call`s
to it over the child's Unix socket. Relevant only if your product adds capabilities as
[extensions](extensions.md), and only on a platform with `CUFFLINK_EXTENSIONS=true`. Run one
per tenant that deploys native services. It takes no inbound traffic but its probes
(`RUNNER_PORT`, default `3200`), and holds only the tenant's runner key (reading the
platform's runner API at `PLATFORM_URL`) and a NATS user of that tenant's: no database,
secrets or storage credentials ([settings](deployment.md#extension-runner)).

---

## Data model & isolation

This is the load-bearing principle, so it's stated by mechanism.

**Two tiers of isolation** (`platform/src/tenant_db.rs`, `platform/src/schema_manager/`,
`platform/src/wasm_runtime/host/`):

1. **Per-tenant — separate database.** Each tenant gets its own Postgres database named
   `cufflink_{tenant_slug}` with `-` replaced by `_` (tenant `my-co` → `cufflink_my_co`).
   Cross-tenant access is impossible because there is no shared database.
2. **Per-service — separate schema.** Within a tenant's database, each service owns a
   Postgres **schema** named after the service, again with `-` → `_` (`stock-service` →
   `stock_service`). Before running any query for a service the
   platform sets `search_path` to that service's schema, so the service's SQL only ever
   resolves its own tables. A service therefore cannot read or write a sibling service's
   tables, even within the same tenant.

**Tables** are declared as Rust structs and become SQL tables in the service schema.
**Migrations** are computed by diffing the newly deployed manifest's tables against the
tables of the version deployed before it (the snapshot stored with that version), not
against the live database, so a change made to the database by hand is neither seen nor
undone: additive changes (new tables/columns) apply first, then an optional `on_migrate` WASM hook
runs for data backfill, then destructive changes (dropping tables/columns) apply — and the
destructive phase only runs when you deploy with `--allow-destructive`.

---

## Defining a service

A minimal `Crud` service (`examples/todo-service/src/main.rs`):

```rust
use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "todos", auth_required, owner_field = "user_id")]
pub struct Todo {
    #[key]
    pub id: Uuid,
    pub user_id: String,
    pub title: String,
    pub completed: bool,
    #[timestamp] pub created_at: DateTime<Utc>,
    #[timestamp] pub updated_at: DateTime<Utc>,
}

cufflink::service! {
    name: "todo-service",
    tables: [Todo],
}
```

`cargo run -- --emit-manifest` prints the service manifest; the CLI uses this during deploy.

### Table attributes (`#[derive(Table)]`)

| Attribute | Effect |
|-----------|--------|
| `#[table(name = "...")]` | SQL table name (required). |
| `#[key]` | Primary key (UUID, auto-generated). |
| `#[timestamp]` | Auto-managed timestamp column. |
| `#[references("table.column")]` | Foreign key. |
| `#[on_delete("cascade")]` | FK delete behaviour. |
| `#[table(auth_required)]` | Require auth for this table's CRUD. |
| `#[table(owner_field = "...")]` | Row-level ownership (row scoped to a user field). |
| `#[table(permission_area = "...")]` | RBAC permission area for the table. |
| `#[hooks(...)]` | Attach lifecycle hooks (below). |

(See `examples/blog-service/src/main.rs` for FK/cascade usage, and `docs/wasm-guide.md` for
the full attribute set.)

### Custom logic: WASM handlers

Custom Rust logic is written as **handlers** using `cufflink_fn` (the host-function ABI),
and referenced **by name** from the manifest (`docs/wasm-guide.md`):

```rust
use cufflink_fn::prelude::*;
cufflink_fn::init!();

handler!(hello, |req: Request| {
    let name = req.body()["name"].as_str().unwrap_or("world");
    Response::json(&json!({ "message": format!("Hello, {}!", name), "tenant": req.tenant() }))
});
```

A handler can be wired into the service in four ways. All keys below are recognised by the
`service!` macro (`sdk-macros/src/service_macro.rs`):

- **Custom HTTP routes** — `custom_routes: [("METHOD", "/path", "handler_name", <modifiers>)]`.
  Exposed at `/svc/{tenant}/{service}/_fn/{handler_name}`. Only handlers declared here
  are HTTP-invocable.
  Each route takes an optional **posture** modifier declaring who may call it:

  | Modifier | Meaning |
  |----------|---------|
  | `public("reason")` | Anyone; the non-empty reason is recorded in the manifest. |
  | `auth` | Any authenticated principal. |
  | `perm("area", "op")` | Caller needs that RBAC permission (see `authorization`). |
  | `owner("field")` | Caller must own the row named by `field`. |

  A route with no posture runs in legacy (unenforced) mode and the platform logs
  `Custom route has no declared posture` on each call. Declaring any posture makes the
  macro add `route_posture` to the manifest's `required_platform_features`, so the deploy
  is rejected by a platform that cannot enforce it.
  Add a trailing `job(...)` to make the route run as a background job instead of inline:
  `("POST", "/reports", "generate_report", auth, job(timeout = 300, max_attempts = 5))`
  (defaults: `timeout = 600`, `max_attempts = 3`).
- **Table hooks** — `#[hooks(before_create = "validate", after_create = "audit", ...)]` on a
  table struct. Available hooks and what each receives:

  | Hook | Timing | Receives | Can |
  |------|--------|----------|-----|
  | `before_create` | before INSERT | `{ data, auth }` | modify data, reject |
  | `after_create` | after INSERT | `{ data, result, auth }` | side effects |
  | `before_update` | before UPDATE | `{ id, data, existing, auth }` | modify data, reject |
  | `after_update` | after UPDATE | `{ id, data, result, auth }` | side effects |
  | `before_delete` | before DELETE | `{ id, existing, auth }` | reject |
  | `after_delete` | after DELETE | `{ id, auth }` | side effects |

- **NATS subscriptions** — `subscriptions: [("subject", "handler_name", wasm)]`. The handler
  runs when a message lands on the subject. Handler types include `wasm` and `webhook`.
  Delivery keywords (`ack = on_success`, `max_deliver`, `backoff`, …), declared `streams`
  and timed `schedules` are in `docs/subscriptions.md`.
- **Migration hook** — `on_migrate: "handler_name"`. Runs between the additive and
  destructive migration phases; receives the schema diff; should be idempotent.

Two more manifest keys govern what the service may do at runtime (`types/src/manifest/service.rs`):

- **`publishes: ["subject.pattern", ...]`** — allowlist of NATS subjects a handler may
  `nats::publish`/`request` **outside** its own tenant namespace (`*` matches one token,
  `>` a trailing run). A guest may always publish to `dw.{tenant}.*`. With `publishes`
  present, anything else is denied (`cufflink_nats_publish_denied_total`); with it absent,
  the manifest is in legacy mode and everything is allowed with a warning. A platform
  advertising `nats_publish_authz` enforces this. A subject the tenant claims
  (`cufflink tenants nats-claims set`) is its own, as `dw.{tenant}.*` is; one another tenant
  claims is denied whatever `publishes` says.
- **`required_platform_features`** — filled in by the macro (currently `route_posture`);
  the platform refuses a deploy naming a feature it lacks, so a posture never degrades to
  silently-public.

> Two preludes: `cufflink::prelude` (the SDK — `#[derive(Table)]`, `service!`) defines the
> service and its data model; `cufflink_fn::prelude` (the host ABI — `handler!`, `db::`,
> `http::`, …) is what runs **inside** WASM at request time.

---

## Build & deploy (the developer loop)

Cufflink is driven entirely by the **`cufflink` CLI** (`cli/`). Core loop:

```bash
cufflink init my-service --mode wasm     # scaffold a project (--mode crud is the default)
# ...edit src: define tables + handlers...
cufflink test                            # run tests (hash-gated; --all forces full run)
cufflink deploy                          # build + upload manifest (+ WASM) + migrate
cufflink logs --follow                   # stream service logs
cufflink rollback                        # revert to the previous version
```

- **Build:** `Wasm`/hook services compile to `wasm32-unknown-unknown` as a `cdylib`. The CLI
  runs `cargo build --lib --target wasm32-unknown-unknown` and bundles the `.wasm`
  (`cli/src/commands/deploy.rs`). One-time setup: `rustup target add wasm32-unknown-unknown`.
  Pure `Crud` services need no WASM.
- **Deploy:** `cufflink deploy` emits the manifest, uploads it (plus the WASM module if the
  service has handlers), and applies migrations. Add `--allow-destructive` to permit dropping
  tables/columns. Target an environment from `Cufflink.toml` with `--env <name>`.
  `cufflink build --bundle <file>` builds the same thing into one file and sends nothing;
  `cufflink deploy --bundle <file>` deploys that file with no sources
  ([bundles](deployment.md#bundles)).
- **Rollback / versions:** `cufflink rollback [--version N | --commit <sha>]`, `cufflink deployments list`,
  `cufflink deployments prune --keep N`.

Config lives in `Cufflink.toml` (per project) and `cufflink-workspace.toml` (multi-service
workspace). Auth is via `cufflink login`; credentials are stored under
`~/.config/cufflink/`. The full platform/worker/web-runner environment variable reference
is in `docs/setup.md`.

---

## HTTP surface (auto-generated per service)

Every deployed service exposes these routes (`platform/src/main.rs`). `{tenant}` is the
tenant slug, `{service}` the service name.

| Route | Methods | Purpose |
|-------|---------|---------|
| `/svc/{tenant}/{service}/{table}` | GET, POST | List / create rows. |
| `/svc/{tenant}/{service}/{table}/{id}` | GET, PUT, DELETE | Read / update / delete a row. |
| `/svc/{tenant}/{service}/{table}/batch` | POST | Batch operations. |
| `/svc/{tenant}/{service}/{table}/stream` | GET | Server-Sent Events real-time stream. |
| `/svc/{tenant}/{service}/_fn/{handler}` | GET, POST, PUT, DELETE, PATCH | Invoke a custom WASM handler. |
| `/svc/{tenant}/{service}/graphql` | POST | GraphQL query endpoint, for services declaring `graphql: true` in `service!`; the same table permissions and owner scoping as CRUD. |
| `/svc/{tenant}/{service}/graphql/schema` | GET | GraphQL schema (authenticated). |
| `/svc/{tenant}/_jobs/{job_id}` | GET | Poll a background job's status/result. |
| `/svc/{tenant}/{service}/_meta/health` | GET | Service health. |
| `/svc/{tenant}/{service}/_meta/openapi.json` | GET | OpenAPI spec. |
| `/svc/{tenant}/{service}/_meta/docs` | GET | API docs. |

Platform/admin operations (deploy, tenants, auth, audit, backup, etc.) live under `/api/...`
and are normally driven through the CLI rather than called directly.

---

## Host function reference

Inside a WASM handler, these modules are the **only** way to reach the outside world
(`cufflink_fn`, modules verified in `cufflink-fn/src/`; signatures per
`docs/wasm-guide.md`). Table names need no schema prefix — the platform scopes queries to the
service's schema automatically.

### `db` — the service's database
| Function | Returns | Purpose |
|----------|---------|---------|
| `db::query(sql)` | `Vec<Value>` | SELECT → all rows as JSON. |
| `db::query_one(sql)` | `Option<Value>` | SELECT → first row or none. |
| `db::execute(sql)` | `i32` | INSERT/UPDATE/DELETE → affected rows. |
| `db::execute_all(&[sql])` | `Result<Vec<i32>, String>` | N statements, one transaction, one connection — all commit or none. |

> **Each `db` call is its own transaction on its own pooled connection.** Two
> `db::execute` calls cannot be made atomic, and a lock taken by one is gone
> before the next runs. When a sequence of writes must be all-or-nothing — or an
> advisory lock must span them — put them in one `db::execute_all`. It returns
> the affected-row count per statement, or an `Err` naming the statement that
> failed, with nothing committed. `nats::publish` is a separate host call and
> cannot join the transaction, so this is not a transactional outbox.

> **⚠️ Generate UUIDs and timestamps in the handler — never in SQL.** This is a common
> mistake. Do **not** put `gen_random_uuid()` or `now()` inside handler SQL, and do not reach
> for `Uuid::new_v4()` / `Utc::now()` in Rust (WASM has no system RNG or clock). Call the host
> functions `util::generate_uuid()` and `util::current_time()`, then use the values:
>
> ```rust
> // ✅ the value is in hand — you can return it, log it, publish it, reuse it as an FK
> let id = util::generate_uuid();
> let now = util::current_time();
> db::execute(&format!(
>     "INSERT INTO orders (id, status, created_at) VALUES ('{}', 'pending', '{}')",
>     id, now
> ));
>
> // ❌ the row gets an id/timestamp the handler never sees, and can't return or reference it
> db::execute("INSERT INTO orders (status) VALUES ('pending')"); // relies on DB gen_random_uuid()/now()
> ```
>
> Generating in the handler means you **hold the value** — return it in the response, log it,
> publish it on an event, reuse one timestamp across several inserts. Pushing generation into
> the SQL hides the value from your code. (Tables' auto-generated `#[key]`/`#[timestamp]`
> columns still default at the DB level — this rule is about values a handler creates
> explicitly.)

### `http` — outbound HTTP
`http::get`, `http::post`, `http::put`, `http::delete`, `http::patch`, `http::fetch`
(generic), and `http::fetch_many` (bounded parallel fan-out). Responses expose `.json()` and
`.is_success()`.

Every request has a budget: the install default (28s total, `HOST_OUTBOUND_HTTP_TIMEOUT_SECS`;
20s of that for connecting, `HOST_OUTBOUND_CONNECT_TIMEOUT_SECS`) unless the guest passes its
own via `http::fetch_with_timeout` / `http::post_with_timeout` (milliseconds). Either way the host clamps the budget to the handler's remaining execution
time, so a route handler (30s wall) can never hold a request past its own death, while a
`job(timeout = …)` handler can legitimately wait minutes on a slow upstream by asking for it. Requires a v0.25+ platform; a service
calling these on an older platform fails to instantiate on the missing import, so roll the
platform first.

### `storage` — object storage (S3 / GCS compatible)
| Function | Purpose |
|----------|---------|
| `storage::download(bucket, key)` | Download an object (base64). |
| `storage::presign_upload(bucket, key, content_type, expires_secs)` | Presigned PUT URL for direct client upload. |
| `storage::presign_download(bucket, key, expires_secs)` | Presigned GET URL for direct client download. |
| `storage::download_many(items)` | Bounded parallel downloads. |
| `storage::delete(bucket, key)` | Delete an object. Idempotent: already-absent keys succeed. |

### `redis` — key-value cache
`redis::get`, `redis::set(key, value, ttl_secs)`, `redis::del`, `redis::mget`, and
`redis::get_with_status` (distinguishes "missing" from "error"). Requires `REDIS_URL` on
the platform. Redis is **disabled on the worker**, so these calls error inside handlers
that run there (worker-routed handlers and `job(...)` / `jobs::enqueue` work).

### `nats` — messaging
`nats::publish(subject, payload)` and `nats::request(subject, payload, timeout_ms)`. A
handler may always publish within its own tenant namespace `dw.{tenant}.*`; any other
subject must match the manifest's `publishes` allowlist (see *Custom logic*), enforced by
platforms that advertise `nats_publish_authz`. With the `nats-publish-v2` feature,
`nats::publish_with`/`publish_json` wait for the stream's acknowledgement and deduplicate
by `msg_id`. Platform
change events use `dw.{tenant}.{service}.{table}.{created|updated|deleted}`.

### `ext` — extensions
`ext::call(name, &payload)` and `ext::call_with_timeout(name, &payload, timeout_ms)`
(cargo feature `ext-call`, platform feature `ext_call`) call a capability the tenant
registers without a Cufflink release: served by a native service or by a NATS provider the
project runs, which the guest never learns. The service lists every extension it calls in
`service! { extensions: ["google.token"] }`; a deploy (or a rollback) declaring one that is
not registered for the tenant, or whose registration does not list the service (or `*`), is
refused with 422. A platform admin registers a NATS provider with
`PUT /api/tenants/{slug}/extensions/{name}` `{"callers": ["reports-service"]}`, lists them
with `GET /api/tenants/{slug}/extensions`, and removes one with `DELETE` (or
`cufflink ext list|register|unregister --tenant <slug>`, with `--caller <svc>` per caller);
changing or removing a registration a deployed service still calls is refused with 409
unless `?force=true` (`--force`). A native service's extensions are registered by its
deploys instead ([native services](deployment.md#native-services)). Registering needs
`CUFFLINK_EXTENSIONS=true` on the platform; calls and removals work regardless. Writing, deploying and
testing extensions end to end: [extensions](extensions.md).

A call is checked in this order, and fails with the `ExtError` that says why:
`Invalid` (malformed request), `NotDeclared` (not in the manifest), `NotFound` (not
registered), `Denied` (the registration does not list the service), `TooLarge` (the request
is over `EXT_CALL_MAX_BYTES`, 1 MiB, or what NATS carries), then `Timeout` (the default
`EXT_CALL_TIMEOUT_SECS`, 30, or the guest's own, never past the handler's budget),
`Unavailable` (nothing serves the extension, or it answered what is not a reply),
`TooLarge` (the reply is over the limit) or `Failed { code, message }` (the extension's own
error; a provider that answers the reserved code `unavailable`, `timeout` or `too_large`
is seen as that kind instead). The extension receives the payload with the tenant, the calling service and its
version, the caller's verified claims (never a token) and the deadline. It works the same
from the platform and the worker.

### `jobs` — enqueue background work
`jobs::enqueue(handler, payload)` and `jobs::enqueue_with(handler, payload, opts)`. See *Jobs*.

### `image` — image transforms
`image::transform_jpeg`, `image::transform_webp`, `image::transform_avif`, and
`image::transform_multi` (decode once, encode several outputs). Run on the host, not in WASM.

### `config` — service config & secrets
`config::get(key)` reads values set via `cufflink config set KEY VALUE [--secret]`.

### `log` — structured logging
`log::error`, `log::warn`, `log::info`, `log::debug`. Lines appear in platform logs prefixed
with `[wasm]`.

### `util` / `context` / `migrate` / `stripe`
- `util::generate_uuid()`, `util::current_time()` (RFC3339) — WASM has no direct clock/RNG.
  `current_time()` is the platform's clock when called; SQL `now()` is the database's clock
  at the start of the transaction (one value for a whole `db::execute_all`), so the two
  differ and should not be mixed in one column.
  **Always use these for ids/timestamps in handler code — never `gen_random_uuid()`/`now()` in
  SQL or `Uuid::new_v4()`/`Utc::now()` in Rust** (see the warning under `db`).
- `context::tenant()` — current tenant slug.
- `migrate::*` — helpers for the `on_migrate` hook.
- `stripe::*` — first-class Stripe support (webhook signature verification + REST client);
  behind the `stripe` feature.

---

## Jobs (long-running / background work)

For work too slow for the request path (reports, bulk processing, third-party calls):

1. **Enqueue** — from a handler: `jobs::enqueue("handler_name", &payload)` returns a job id.
   Or declare a custom route with `job(...)` so an HTTP call enqueues automatically and
   returns the id immediately. Defaults: `timeout = 600s`, `max_attempts = 3`.
2. **Execute** — the **worker** claims the job under a lease, runs the named WASM handler
   with the configured timeout, and attempts a crash again up to `max_attempts`; errors,
   timeouts and 5xx answers are retried only with `retry_on` (`docs/wasm-guide.md`, *Jobs
   v2*). A failed job is dead-lettered and can be run again with `cufflink jobs retry <id>`.
3. **Poll** — `GET /svc/{tenant}/_jobs/{job_id}` returns the job's status and result
   (`platform/src/api/jobs.rs`). The job runs as the same authenticated principal that
   enqueued it; the handler can see its `JobContext` (id, attempt, `is_retry()`).

To scale heavy handlers off the platform entirely, route specific handlers to the worker
fleet — see `docs/wasm-workers.md`.

---

## Other capabilities (where to look)

| Feature | How / where |
|---------|-------------|
| GraphQL + SSE | SSE streams are auto-exposed per table (owner tables stream only the caller's rows); GraphQL is opt-in with `graphql: true` (see route table). |
| Webhooks on CRUD | Register HTTP endpoints to receive table change events (`docs/guides.md`). |
| Backup / export / restore | `cufflink backup {schema,export,restore,list,jobs}` (worker-executed). |
| Seed data | `cufflink seed [file] [--clear]`. |
| Secrets & config | `cufflink secrets ...`, `cufflink config ...`; read in WASM via `config::get`. |
| AuthZ / RBAC | `authorization` block in `service!`; `cufflink roles {list,assign,unassign}`. |
| Preview environments | `cufflink workspace preview {create,destroy}` — isolated tenant clones. |
| OpenAPI / TS client | `cufflink openapi`, `cufflink generate-client`. |
| Read-only SQL | `cufflink sql "<query>"` against a tenant DB. |
| Tests | Handlers tested natively against a real Postgres and fakes (`cufflink-fn` feature `test-host`), or against an in-process platform (`cufflink-testkit`): `docs/testing.md`. |
| Local platform | `cufflink dev up` — a whole stack in Docker (`docs/dev.md`). |
| Dead letters, consumers | `cufflink dlq {ls,replay,drop}`, `cufflink consumers ls` (`docs/subscriptions.md`). |

---

## CLI command map (`cufflink <command>`)

Verified from `cli/src/main.rs`. Most take `--env <name>`; run `cufflink <command> --help`
for flags.

`init` · `templates` · `install` · `deploy` · `deploy-package` · `rollback` · `status` ·
`logs` · `routes` · `services` · `deployments` · `consumers` · `dlq` · `jobs` · `dev` ·
`openapi` · `generate-client` · `tenants` · `login` · `test` · `seed` · `config` ·
`secrets` · `backup` · `sql` · `workspace` · `ci` · `roles`

---

## Source map

| Concern | Path |
|---------|------|
| Service manifest & types | `types/src/manifest/` |
| SDK (`service!`, `#[derive(Table)]`) | `sdk/src/lib.rs`, `sdk-macros/src/` |
| Host-function ABI (`handler!`, `db::`, …) | `cufflink-fn/src/` |
| Platform routes | `platform/src/main.rs`, `platform/src/router/`, `platform/src/api/` |
| Per-tenant DB / schema isolation | `platform/src/tenant_db.rs`, `platform/src/schema_manager/` |
| WASM runtime & host fns | `platform/src/wasm_runtime/` |
| Worker (jobs, backups) | `worker/src/` |
| Web-runner | `web-runner/src/` |
| CLI | `cli/src/` |
| Service author guide | `docs/wasm-guide.md`, `docs/guides.md` |
| Platform setup & env reference | `docs/setup.md` |
| WASM worker scaling | `docs/wasm-workers.md`, `docs/scaling.md` |
| Examples | `examples/todo-service/`, `examples/blog-service/` |
