# Cufflink

Deploy CRUD microservices in seconds. Define your data model in Rust, run `cufflink deploy`, and get a full REST API with PostgreSQL persistence, automatic schema migrations, NATS events, and multi-tenant isolation.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌───────────┐
│  Cufflink   │────>│   Platform   │────>│ PostgreSQL│
│  CLI        │     │   (Axum)     │     │           │
└─────────────┘     └──────┬───────┘     └───────────┘
                           │
┌─────────────┐     ┌──────┴───────┐     ┌───────────┐
│  Keycloak   │<────│  Dynamic     │────>│   NATS    │
│  (JWT)      │     │  CRUD Router │     │ JetStream │
└─────────────┘     └──────────────┘     └───────────┘
```

- **SDK** — `#[derive(Table)]` and `service!` proc macros generate a JSON manifest from Rust structs
- **CLI** — `cufflink deploy` builds the project, captures the manifest and POSTs it to the platform
- **Platform** — applies schema migrations, registers CRUD routes, runs WASM handlers, publishes events
- **WASM worker** — runs offloaded WASM handlers and long-running jobs over NATS, scales independently
- **web-runner** — serves deployed Next.js apps (`mode = "web"` services) at `{service}--{tenant}.{base domain}`
- **ext-runner** — runs native services (`mode = "native"`), which provide [extensions](docs/extensions.md) to a tenant's WASM services

## Quick Start

### Prerequisites

- **Rust** — `rust-toolchain.toml` pins 1.93.1; rustup installs it on the first `cargo` command
- **Docker + Docker Compose**
- The `wasm32-unknown-unknown` target, only if you build WASM services: `rustup target add wasm32-unknown-unknown`

### 1. Start the platform

```bash
docker compose up -d
curl localhost:8080/health
# cufflink-platform ok
```

This starts PostgreSQL, NATS (JetStream), Keycloak (with two realms imported from `infra/keycloak/`), Redis, RustFS (S3-compatible object storage, bucket `cufflink`), the platform, the WASM worker, web-runner, ext-runner, and a monitoring stack (OTel collector, Tempo, Loki, Prometheus, Grafana). Two one-shot containers run once: `bucket-init` creates the bucket and `tenant-init` registers the dev tenant.

Everything you need to log in is pre-provisioned:

| What | Value |
|------|-------|
| Platform API | http://localhost:8080 |
| Keycloak admin console | http://localhost:8180 — `admin` / `admin` |
| Platform realm `cufflink` (platform admins) | user `admin` / `admin` |
| Tenant realm `acme` (developers) | user `dev` / `dev` |
| Dev tenant slug | `acme` |
| Grafana | http://localhost:3001 — `admin` / `admin` |

The `kingsleyh/cufflink-*` images are published for `linux/amd64` only; compose pins `platform: linux/amd64`, so Docker Desktop on Apple Silicon runs them under emulation (slower startup, but it works).

### 2. Install the CLI

```bash
cargo install cufflink-cli          # from crates.io
# or, from this checkout:
cargo build -p cufflink-cli         # binary at target/debug/cufflink
```

The binary is `cufflink`. (`cargo run -p cufflink` is the SDK library crate, not the CLI.)

### 3. Log in

Every CLI command (except `cufflink tenants ...`) reads a `Cufflink.toml` from the project directory. Both examples ship one pointing at the compose stack:

```toml
[service]
default_env = "local"

[environments.local]
api_url = "http://localhost:8080"
tenant = "acme"
keycloak_url = "http://localhost:8180"
keycloak_realm = "acme"
```

```bash
cd examples/todo-service
cufflink login
```

```
Environment: local
Authenticating with Cufflink...
  Open this URL in your browser:
  http://localhost:8180/realms/acme/device?user_code=XXXX-XXXX

  Enter code: XXXX-XXXX

Waiting for authentication...
```

This is the OAuth device-code flow: the CLI does not open a browser. Open the URL, sign in as `dev` / `dev`, and the CLI exchanges the Keycloak token for a platform API key (`ck_...`) which it stores in `~/.config/cufflink/credentials.json`.

Non-interactive alternative: set `CUFFLINK_API_KEY` in the environment. It takes precedence over stored credentials.

### 4. Deploy

```bash
cufflink deploy
```

```
Environment: local
Building service...
Deploying todo-service (mode: crud)...
Deployed todo-service v2
  Schema changes: 1
  Deployment ID:  "98e338c3-..."
  Tenant:         "acme"
  Commit:         f1af9c6...
```

### 5. Use the API

The `todos` table in this example is `auth_required` with `owner_field = "user_id"`, so every request needs an API key and every row is scoped to its owner. Take the key from `~/.config/cufflink/credentials.json` (or use the one in `CUFFLINK_API_KEY`):

```bash
export KEY=ck_...
BASE=http://localhost:8080/svc/acme/todo-service/todos

# Create — user_id is set from the caller, not the body
curl -X POST $BASE -H "Authorization: ApiKey $KEY" -H "Content-Type: application/json" \
  -d '{"title": "Buy groceries", "completed": false}'

# List (sorting, pagination, search, filter)
curl -H "Authorization: ApiKey $KEY" "$BASE?sort=-created_at&per_page=10"
curl -H "Authorization: ApiKey $KEY" "$BASE?search=groceries"
curl -H "Authorization: ApiKey $KEY" --get $BASE \
  --data-urlencode 'filter=[{"n":"completed","f":"=","v":"false"}]'

# Get / update / delete by ID
curl -H "Authorization: ApiKey $KEY" $BASE/{id}
curl -X PUT $BASE/{id} -H "Authorization: ApiKey $KEY" -H "Content-Type: application/json" \
  -d '{"completed": true}'
curl -X DELETE -H "Authorization: ApiKey $KEY" $BASE/{id}
```

List responses look like this:

```json
{
  "results": [
    {"id": "…", "user_id": "platform-acme", "title": "Buy groceries", "completed": false,
     "created_at": "…", "updated_at": "…"}
  ],
  "pagination": {"current_page": 1, "per_page": 10, "total_pages": 1, "total_records": 1}
}
```

Without the header you get `401 {"error": "..."}`; a GET after DELETE returns 404. For a service with no auth at all, deploy `examples/blog-service` and drop the header.

### The service you just deployed

`examples/todo-service/src/main.rs`:

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
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

cufflink::service! {
    name: "todo-service",
    tables: [Todo],
}
```

- `auth_required` — anonymous requests to this table get 401.
- `owner_field = "user_id"` — on create the column is filled with the caller's subject, and reads, updates and deletes only see that caller's rows.

## SDK Reference

### `#[table(...)]` attributes

| Attribute | Description |
|-----------|-------------|
| `name = "..."` | SQL table name (required) |
| `auth_required` | Reject unauthenticated CRUD requests with 401 |
| `permission_area = "..."` | Enforce RBAC: `"staff"` checks `staff:view`, `staff:create`, `staff:edit`, `staff:delete` |
| `owner_field = "..."` | Row-level scoping: the column is set from the caller's subject on create, and all operations are filtered to the caller's rows |
| `owner_self_service` | With `owner_field`, row ownership is the authorization — no `permission_area` grant needed. Implies authentication |
| `soft_delete` | DELETE sets `deleted_at` instead of removing the row; lists hide those rows unless `?include_deleted=true` |

### Field attributes

| Attribute | Description |
|-----------|-------------|
| `#[key]` | Primary key (auto-generated UUID) |
| `#[timestamp]` | Auto-generated timestamp (`DEFAULT now()`) |
| `#[unique]` | UNIQUE constraint |
| `#[index]` | Create an index on the column |
| `#[default("value")]` | Column default |
| `#[references("table.column")]` | Foreign key |
| `#[on_delete("cascade")]` | FK delete action: `cascade`, `set_null`, `restrict`, `no_action` |
| `#[validate("min_length=3", "max_length=200")]` | Validated on create/update. Rules: `regex=`, `min=`, `max=`, `min_length=`, `max_length=` |
| `#[restricted(read = "op", write = "op")]` | The column is read, and written, through the data routes only with a grant of that operation of the table's permission area ([column permissions](docs/column-permissions.md)) |

Table-level hooks call WASM handlers around CRUD operations:

```rust
#[hooks(before_create = "validate_item", after_create = "audit_item_created", before_delete = "check_owner_can_delete")]
```

All six are available: `before_create`, `after_create`, `before_update`, `after_update`, `before_delete`, `after_delete`.

### Supported types

| Rust | SQL |
|------|-----|
| `Uuid` | UUID |
| `String` | TEXT |
| `bool` | BOOLEAN |
| `i32` / `i64` | INTEGER / BIGINT |
| `f32` / `f64` | REAL / DOUBLE PRECISION |
| `DateTime<Utc>` | TIMESTAMPTZ |
| `NaiveDate` | DATE |
| `Value` | JSONB |
| `Option<T>` | nullable T |

### `service!` keys

```rust
cufflink::service! {
    name: "orders",
    mode: wasm,                       // crud (default) | wasm | container
    tables: [Order, LineItem],
    authorization: [
        areas: [("orders", ["create", "view", "edit", "delete"])],
        default_roles: [
            ("admin", "Full access", ["orders:*"]),
            ("viewer", "Read-only", ["orders:view"]),
        ],
    ],
    custom_routes: [
        // (method, path, handler, posture[, job(...)])
        ("GET",  "/health",   "health",   public("unauthenticated probe")),
        ("POST", "/checkout", "checkout", auth),
        ("POST", "/refund",   "refund",   perm("orders", "edit")),
        ("GET",  "/mine",     "mine",     owner("user_id")),
        ("POST", "/export",   "export",   auth, job(timeout = 900, max_attempts = 3)),
    ],
    subscriptions: [("orders.>", "on_order_event", wasm)],   // wasm | webhook | delete_cascade | update_field
    on_migrate: "backfill",
    publishes: ["orders.created", "orders.refunded"],
}
```

Every custom route should declare exactly one posture — `public("reason")`, `auth`, `perm("area", "op")` or `owner("field")`. A route without one runs unenforced and the platform logs a warning; set `CUFFLINK_ENFORCE_ROUTE_POSTURE=true` on the platform to reject such deploys. `job(...)` turns the route into a long-running job (defaults: 600 s timeout, 3 attempts). Web apps (`mode = "web"`) are declared in `Cufflink.toml`, not in `service!` — see [docs/setup.md](docs/setup.md).

See [docs/wasm-guide.md](docs/wasm-guide.md) for handlers, [docs/authorization.md](docs/authorization.md) for RBAC, and [docs/guides.md](docs/guides.md) for the rest.

## CLI Reference

All commands accept `--env/-e <name>` to pick an `[environments.<name>]` block from `Cufflink.toml` (default: `[service].default_env`).

| Command | Description |
|---------|-------------|
| `cufflink init <name> [--template todo\|blog\|ecommerce\|cms] [--mode crud\|wasm] [--from github:owner/repo]` | Scaffold a new service project |
| `cufflink templates` | List available project templates |
| `cufflink install <source>` | Install a package from GitHub or a local path |
| `cufflink deploy-package <name> [--skip a,b]` | Deploy every component of an installed package |
| `cufflink deploy [--plan] [--allow-destructive] [--accept-drift] [--purge] [--inactive] [--commit-hash <sha>] [--tenant <slug>] [--bundle <file>]` | Build and deploy the service in the current directory (`--bundle`: deploy [a bundle](docs/deployment.md#bundles) and build nothing; `--plan`: say what it would do and deploy nothing; `--accept-drift`, `--purge`: under an enforced [schema guard](docs/schema-ownership.md#the-schema-guard); `--inactive`: [without starting it](docs/deployment.md#deploying-a-service-inactive)) |
| `cufflink build --bundle <file> [--commit-hash <sha>]` | Build the service into [a bundle](docs/deployment.md#bundles) and send nothing |
| `cufflink rollback [--version N \| --commit <sha>] [--service <name>] [--allow-destructive]` | Roll back to a previous deployment, by its version or [its commit](docs/deployment.md#rolling-back-to-a-commit) |
| `cufflink status` | Show service status |
| `cufflink logs [--follow] [--since 1h] [--search text] [--level warn] [-n 100]` | Query or tail service logs |
| `cufflink services [delete\|enable\|disable <name>]` | List, delete, enable or disable deployed services (`enable\|disable --tenant <slug>`: in the tenant `deploy --tenant` deployed to) |
| `cufflink routes [--all-services] [--audit]` | Audit custom-route authorization postures |
| `cufflink deployments list\|prune\|delete` | Inspect and clean up deployment history |
| `cufflink openapi [--output f.json] [--local]` | Print or save the OpenAPI spec (`--local` needs no deployment) |
| `cufflink generate-client [--output ./generated] [--local]` | Generate a TypeScript client |
| `cufflink tenants create\|update\|list\|delete\|create-api-key\|list-api-keys\|revoke-api-key` | Tenant management (platform admin) |
| `cufflink login` | Device-code login; stores an API key in `~/.config/cufflink/credentials.json` |
| `cufflink test [--all]` | Run tests (hash-based, only changed unless `--all`) |
| `cufflink seed [file] [--clear] [--tables a,b]` | Seed data from a JSON file (default `seed.json`) |
| `cufflink config list\|set\|delete\|sync\|apply` | Manage service config values (`apply [--prune] [--check] [--service name]` applies `Cufflink.toml` as one set) |
| `cufflink backup schema\|export\|restore\|list\|jobs\|cancel` | Backup and restore service data (export defaults to `backup.jsonl`) |
| `cufflink workspace deploy\|test\|status\|seed\|preview` | Multi-service workspace operations |
| `cufflink ci init` | Generate a GitHub Actions deploy workflow |
| `cufflink roles list\|assign\|unassign` | Manage roles and role assignments |
| `cufflink secrets init\|init-from\|rekey\|set\|list\|delete` | Manage encrypted secrets |
| `cufflink sql <query> [--service s] [--format table\|jsonl\|csv\|tsv]` | Read-only SQL against a service's tenant schema |

## API Endpoints

### Dynamic CRUD routes

| Method | Path | Description |
|--------|------|-------------|
| GET | `/svc/{tenant}/{service}/{table}` | List records |
| POST | `/svc/{tenant}/{service}/{table}` | Create record |
| GET | `/svc/{tenant}/{service}/{table}/{id}` | Get record |
| PUT | `/svc/{tenant}/{service}/{table}/{id}` | Update record |
| DELETE | `/svc/{tenant}/{service}/{table}/{id}` | Delete record |
| POST | `/svc/{tenant}/{service}/{table}/batch` | Batch operations |
| GET | `/svc/{tenant}/{service}/{table}/stream` | Server-sent events |
| ANY | `/svc/{tenant}/{service}/_fn/{handler}` | WASM custom route (4 MB body limit) |
| POST | `/svc/{tenant}/{service}/graphql` | GraphQL |

### Query parameters (list endpoints)

| Parameter | Example | Description |
|-----------|---------|-------------|
| `page` | `?page=2` | Page number (default 1) |
| `per_page` | `?per_page=25` | Items per page (default 50, max 200) |
| `sort` | `?sort=-created_at,title` | Sort columns, `-` prefix for DESC (default `-created_at`) |
| `search` | `?search=rust` | Case-insensitive substring match (`ILIKE`) across text columns |
| `fts` | `?fts=rust` | PostgreSQL full-text search (`plainto_tsquery`) |
| `select` | `?select=id,title` | Return only these columns |
| `filter` | `?filter=[{"n":"status","f":"=","v":"active"}]` | JSON array of `{n, f, v}` objects |
| `<column>` | `?completed=true` | Shorthand equality filter on any column |
| `cursor` + `limit` | `?cursor=&limit=50` | Cursor pagination (see below) |
| `include_deleted` | `?include_deleted=true` | Include soft-deleted rows |

Filter operators: `=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `ILIKE`, `IN`, `IS NULL`, `IS NOT NULL` (case-insensitive; `eq`, `neq`, `gt`, `gte`, `lt`, `lte` also work). Conditions on the **same** column are OR'd together, conditions on different columns are AND'd — so a range on one column (`>= a AND <= b`) is not expressible with `filter`. Only the object form is supported; array-of-arrays is not.

Cursor pagination: an empty `?cursor=` starts from the first row (ordered by primary key ascending) and the response is `{"results": [...], "next_cursor": "<base64>", "has_more": true}`; pass `next_cursor` back to fetch the next page. Offset mode (`page`/`per_page`) never returns `next_cursor`.

### Management API

All routes are relative to the platform (`http://localhost:8080`). `/health` returns plain text, `/ready` returns JSON.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health`, `/ready`, `/metrics`, `/api/version` | Liveness, readiness, Prometheus metrics, version |
| GET | `/api/auth/config` | Keycloak URL, realm and client IDs for the CLI |
| POST | `/api/auth/api-keys` | Create an API key (authenticated) |
| POST | `/api/auth/device/token` | Exchange a Keycloak token for an API key |
| GET | `/api/auth/me` | Authenticated user info |
| POST | `/api/auth/logout` | End the caller's Keycloak sessions (needs tenant ops-client credentials) |
| POST / GET | `/api/tenants` | Create / list tenants (platform admin) |
| PUT / DELETE | `/api/tenants/{slug}` | Update / delete a tenant |
| POST / GET | `/api/platform/api-keys` | Create / list platform admin API keys |
| DELETE | `/api/platform/api-keys/{id}` | Revoke a platform admin API key |
| GET | `/api/services` | List services |
| GET / DELETE | `/api/services/{id}` | Get / delete a service |
| POST | `/api/services/deploy` | Deploy a service |
| GET | `/api/services/{id}/deployments` | Deployment history |
| POST | `/api/services/{id}/rollback` | Roll back |
| GET | `/api/services/{id}/openapi.json` | Generated OpenAPI spec |
| GET | `/api/services/{id}/schema` | Tables, columns, FK dependencies |
| POST | `/api/services/{id}/wasm` | Upload a WASM module |
| GET | `/api/services/{id}/wasm/status` | WASM runtime status |
| POST | `/api/services/{id}/backup/export` | Start an export job |
| POST | `/api/services/{id}/backup/restore` | Start a restore job |
| GET | `/api/services/{id}/backup/jobs[/{job_id}[/download]]` | List jobs, job status, download export |
| POST | `/api/services/{id}/backup/jobs/{job_id}/cancel` | Cancel a job |
| GET | `/api/audit` | Audit log |

The full list (webhooks, files, config, logs, SQL, RBAC, web apps) is in [docs/setup.md](docs/setup.md#api-reference).

## Project Structure

```
cufflink/
├── platform/          # Axum platform server (deployer, CRUD router, WASM runtime)
├── worker/            # WASM worker process (NATS-driven, scales independently)
├── web-runner/        # Serves deployed Next.js "web" mode apps
├── ext-runner/        # Runs native services and relays their extension calls
├── supervisor/        # Child-process supervision the runners share
├── cli/               # The `cufflink` binary (crate: cufflink-cli)
├── sdk/               # User-facing SDK crate (crate: cufflink)
├── sdk-macros/        # #[derive(Table)] and service! proc macros
├── cufflink-fn/       # WASM handler SDK (handler!, Request, Response, host calls)
├── native/            # Native extension SDK (crate: cufflink-native)
├── types/             # Shared types (ServiceManifest, ColumnType, ...)
├── db/                # Shared PostgreSQL pool helpers
├── storage/           # Object storage abstraction (S3-compatible or GCS)
├── examples/
│   ├── todo-service/  # Single table with auth_required + owner_field
│   ├── blog-service/  # Multi-table example with foreign keys, no auth
│   └── ext-echo/      # Native service whose extensions echo, fail and sleep
├── infra/             # Keycloak realm imports, compose bootstrap, monitoring config
├── docs/
├── docker-compose.yml
├── Dockerfile.platform
├── Dockerfile.worker
├── Dockerfile.web-runner
└── Dockerfile.ext-runner
```

## Development

Run the infrastructure from compose and the platform natively:

```bash
docker compose up -d
docker compose stop platform

DATABASE_URL=postgres://cufflink:cufflink@localhost:5432/cufflink \
NATS_URL=nats://localhost:4222 \
KEYCLOAK_URL=http://localhost:8180 KEYCLOAK_REALM=cufflink \
REDIS_URL=redis://localhost:6379 \
STORAGE_BACKEND=s3 S3_ENDPOINT=http://localhost:9000 S3_REGION=us-east-1 \
S3_ACCESS_KEY=rustfsadmin S3_SECRET_KEY=rustfsadmin S3_BUCKET=cufflink \
cargo run -p cufflink-platform
```

`KEYCLOAK_ISSUER` is not needed natively because `KEYCLOAK_URL` is already the browser-facing address.

```bash
cargo build --workspace
cargo test --workspace
```

## Documentation

- [Setup & operations](docs/setup.md) — services, environment variables, tenants, production checklist, full API reference
- [`cufflink dev`](docs/dev.md) — a platform on your machine, with a tenant, a key and users
- [Guides](docs/guides.md) — feature walkthroughs
- [WASM guide](docs/wasm-guide.md) and [WASM workers](docs/wasm-workers.md)
- [Testing](docs/testing.md) — handlers in `cargo test`, and a platform inside the test
- [Subscriptions, streams and schedules](docs/subscriptions.md)
- [Schema ownership](docs/schema-ownership.md) — adopted schemas, SQL migrations, the schema guard; [The outbox](docs/outbox.md) — outbox, durable audit, webhook deliveries
- [Database roles and secrets](docs/db-roles-and-secrets.md) — for operators
- [Tenant isolation](docs/tenant-isolation.md) — what an install hosting more than one tenant sets
- [Authentication](docs/authentication.md) and [Authorization](docs/authorization.md)
- [Building on Cufflink](docs/building-on-cufflink.md)
- [Extensions](docs/extensions.md) — native services and NATS providers that WASM services call with `ext_call`
- [Observability](docs/observability.md), [Scaling](docs/scaling.md), [Audit](docs/audit.md)
- [Backup](docs/backup.md), [Workspace](docs/workspace.md), [Packages](docs/packages.md)
- [Versions & platform features](docs/versions.md) (with the v0.30.0, v0.29.0, R3 and R2 release notes) and [Deployment notes](docs/deployment.md)
- [Scoped permissions](docs/scoped-permissions.md), [Column permissions](docs/column-permissions.md), [Partner keys](docs/partner-keys.md), [External identity providers](docs/tenants.md)
- [SQL with bound parameters](docs/db-v2.md), [Service calls & the cache](docs/service-calls.md)

## Contributing

Bug reports, fixes, and proposals are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).
Security issues should be reported privately per [SECURITY.md](SECURITY.md).

## License

The platform and its server components are licensed under the
[Functional Source License, Version 1.1, ALv2 Future License](LICENSE-FSL.md)
(FSL-1.1-ALv2). You may use, modify and self-host them for any purpose other
than offering a competing product or service. Each version becomes available
under the Apache License, Version 2.0 two years after its release.

The crates you build services with — `cufflink`, `cufflink-fn`,
`cufflink-types`, `cufflink-sdk-macros`, `cufflink-migrate`,
`cufflink-pgcodec`, `cufflink-cli`, `cufflink-nats` and `cufflink-native` —
and the code under `examples/` are licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option. Each crate's `Cargo.toml` states its license.

Contributions are accepted on the terms in [CONTRIBUTING.md](CONTRIBUTING.md#license).
