# Cufflink Platform Setup & Operations Guide

Cufflink is a platform for deploying CRUD microservices in seconds. You define your data schema in Rust, run `cufflink deploy`, and get REST APIs, real-time streaming, GraphQL, webhooks, file storage, WASM custom routes and more.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Quick Start (Docker Compose)](#quick-start-docker-compose)
- [Tenants and Keycloak Realms](#tenants-and-keycloak-realms)
- [Local Development Setup](#local-development-setup)
- [Platform Configuration](#platform-configuration)
- [Worker Configuration](#worker-configuration)
- [Web Runner Configuration](#web-runner-configuration)
- [CLI Configuration](#cli-configuration)
- [Authentication](#authentication)
- [Tenant Policies and Isolation](policies.md)
- [Tenant isolation: what an install hosting many tenants sets](tenant-isolation.md)
- [Monitoring & Observability](#monitoring--observability)
- [Production Deployment](#production-deployment)
- [API Reference](#api-reference)
- [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
┌──────────┐    HTTP/REST    ┌──────────────┐    SQL     ┌────────────┐
│ cufflink │ ──────────────> │   Platform   │ ────────> │ PostgreSQL │
│   CLI    │                 │  (Axum/Rust) │            │   :5432    │
└──────────┘                 │   :8080      │            └────────────┘
                             │              │    NATS    ┌────────────┐
                             │              │ ────────> │    NATS    │ <──┐
                             │              │            │   :4222    │    │
                             └──┬───────┬───┘            └────────────┘    │
                       JWT/JWKS │       │ S3 API                           │
                                ▼       ▼                                  │
                       ┌──────────┐  ┌─────────────┐   ┌─────────────┐    │
                       │ Keycloak │  │ Object store│   │ WASM worker │ ───┘
                       │  :8180   │  │ (RustFS)    │   │             │
                       └──────────┘  │  :9000      │   └─────────────┘
                                     └─────────────┘   ┌─────────────┐
                                                       │ web-runner  │
                                                       │  :3100      │
                                                       └─────────────┘
```

**Components:**

| Component | Purpose | Technology |
|-----------|---------|------------|
| **Platform** | Core API server — deployments, CRUD routing, schema management, WASM runtime | Rust (Axum 0.8, tokio-postgres, wasmtime) |
| **WASM Worker** | Runs offloaded WASM handlers and long-running jobs from NATS (see [WASM Workers](wasm-workers.md)) | Rust (wasmtime, async-nats) |
| **web-runner** | Serves deployed Next.js apps (`mode = "web"` services) | Rust (reverse proxy + process supervisor) |
| **ext-runner** | Runs native services and relays the tenant's extension calls to them ([extensions](extensions.md)) | Rust (process supervisor over Unix sockets) |
| **CLI** | Developer tool — deploy, manage, seed services | Rust (clap) |
| **SDK** | Rust macros for defining services | Rust proc macros |
| **PostgreSQL** | Primary data store with per-tenant schemas | PostgreSQL 16 |
| **NATS** | Event streaming, worker dispatch, webhooks | NATS 2.10 with JetStream |
| **Keycloak** | Identity provider; one realm per tenant plus a platform realm | Keycloak 26 |
| **Object storage** | WASM modules, web artifacts, backups, file uploads | Any S3-compatible store (RustFS locally) or GCS |
| **Redis** | Shared rate-limit counters and permission cache | Redis 6 |

---

## Prerequisites

- **Docker & Docker Compose**
- **Rust** — the workspace's `rust-toolchain.toml` pins 1.93.1 and rustup installs it automatically on the first `cargo` command:

  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```

- **`wasm32-unknown-unknown` target** — only for WASM services: `rustup target add wasm32-unknown-unknown`

---

## Quick Start (Docker Compose)

To develop a service against a platform on your machine without a checkout, use [`cufflink dev`](dev.md). The repository's own stack, with monitoring, is below.

```bash
cd cufflink
docker compose up -d
docker compose ps            # wait until platform is running and tenant-init has exited 0

curl http://localhost:8080/health
# cufflink-platform ok
curl http://localhost:8080/ready
# {"db":"ok","nats":"connected","redis":"ok"}
```

The `kingsleyh/cufflink-platform`, `-worker`, `-web-runner` and `-ext-runner` images are built for `linux/amd64` only. Compose pins `platform: linux/amd64` on those four services, so Docker Desktop on Apple Silicon runs them under emulation. First start takes a minute or two.

This starts:

| Service | Host port / URL | Credentials | Notes |
|---------|-----------------|-------------|-------|
| Platform API | http://localhost:8080 | — | `/health`, `/ready`, `/metrics` |
| WASM worker | *(none)* | — | subscribes to `cufflink.wasm.>` and `cufflink.jobs.>` |
| web-runner | http://localhost:3100 | — | serves web apps at `{service}--{tenant}.localhost` |
| ext-runner | http://localhost:3200 | — | `/health` and `/_cufflink/ready`; runs native services once the platform has `CUFFLINK_EXTENSIONS=true` ([extensions](extensions.md#turning-extensions-on)) |
| PostgreSQL | localhost:5432 | `cufflink` / `cufflink` | database `cufflink` |
| NATS | localhost:4222 (monitor: http://localhost:8222) | — | JetStream enabled |
| Keycloak | http://localhost:8180 | admin console `admin` / `admin` | realms `cufflink` and `acme` imported from `infra/keycloak/` |
| Redis | localhost:6379 | — | |
| RustFS (S3) | http://localhost:9000 (console: 9001) | `rustfsadmin` / `rustfsadmin` | bucket `cufflink`, created by `bucket-init` |
| Grafana | http://localhost:3001 | `admin` / `admin` | dashboards from `infra/monitoring/dashboards` |
| Prometheus | http://localhost:9090 | — | |
| OTel collector | localhost:4317 (gRPC), 4318 (HTTP) | — | forwards to Tempo and Loki |
| Tempo, Loki | *(not published)* | — | reached through Grafana |
| `bucket-init`, `tenant-init` | one-shot | — | exit 0 after bootstrapping; safe to re-run |

### First deployment

1. **Install the CLI.**

   ```bash
   cargo install cufflink-cli            # or: cargo build -p cufflink-cli  → target/debug/cufflink
   ```

2. **Log in from a project directory.** The CLI reads `Cufflink.toml` from the current directory (or a parent). `examples/todo-service/Cufflink.toml` already points at the compose stack and tenant `acme`:

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

   Open the URL and sign in as `dev` / `dev`. The CLI polls Keycloak, then POSTs the token to `/api/auth/device/token` and stores the returned `ck_...` API key in `~/.config/cufflink/credentials.json`. It finishes with `Logged in as dev (tenant: acme)`.

   Non-interactive alternative: `export CUFFLINK_API_KEY=ck_...` (create keys with `POST /api/auth/api-keys` once you have one, or via `cufflink login` on a workstation).

3. **Deploy.**

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

4. **Call the API.** The example's `todos` table is `auth_required` with `owner_field = "user_id"`, so send the API key (from `credentials.json` or `CUFFLINK_API_KEY`):

   ```bash
   export KEY=ck_...
   curl -X POST http://localhost:8080/svc/acme/todo-service/todos \
     -H "Authorization: ApiKey $KEY" -H 'Content-Type: application/json' \
     -d '{"title": "Hello Cufflink!", "completed": false}'

   curl -H "Authorization: ApiKey $KEY" http://localhost:8080/svc/acme/todo-service/todos
   # {"results":[{"id":"…","user_id":"platform-acme","title":"Hello Cufflink!",…}],
   #  "pagination":{"current_page":1,"per_page":50,"total_pages":1,"total_records":1}}
   ```

   `examples/blog-service` has no auth on its tables; deploy it the same way and call it without the header.

5. **Inspect.**

   ```bash
   cufflink services      # NAME  VERSION  MODE  STATUS  TENANT  COMMIT
   cufflink status
   cufflink openapi --local --output openapi.json
   ```

---

## Tenants and Keycloak Realms

A Cufflink tenant is a slug, a database schema, and a Keycloak realm the platform trusts. Compose imports two realms on first start (`infra/keycloak/*.json`):

| Realm | Purpose | Users | Client |
|-------|---------|-------|--------|
| `cufflink` | Platform realm. `KEYCLOAK_REALM` on the platform. Its users are platform admins (`cufflink tenants ...`). | `admin` / `admin` with realm role `cufflink-deploy` | `cufflink-cli` (public; device grant and password grant enabled) |
| `acme` | Dev tenant realm. Its users deploy services to tenant `acme`. | `dev` / `dev` with realm role `cufflink-deploy` | `cufflink-cli` (same settings) |

`KC_HOSTNAME=http://localhost:8180` pins the token issuer to the browser-facing address, and the platform's `KEYCLOAK_ISSUER=http://localhost:8180` matches it even though the platform talks to Keycloak at `http://keycloak:8080` inside the compose network.

**What `tenant-init` does.** `infra/compose/bootstrap-tenant.sh` waits for the platform and the `cufflink` realm, obtains an `admin` token via the password grant, and `POST /api/tenants` with:

```json
{"name": "Acme", "slug": "acme", "keycloak_url": "http://keycloak:8080",
 "keycloak_realm": "acme", "deploy_role": "cufflink-deploy"}
```

A 409 (`Tenant 'acme' already exists`) is treated as success, so the container is idempotent across restarts.

**Adding another tenant.** Create a realm in Keycloak (copy `infra/keycloak/acme-realm.json`, change `realm`, the user and `default-roles-<realm>`), then register it:

```bash
cufflink tenants create --name "Beta Corp" --slug beta \
  --tenant-keycloak-url http://keycloak:8080 \
  --tenant-keycloak-realm beta
```

Two things to get right:

- `cufflink tenants ...` needs a **platform-realm** login: it discovers the realm from `GET /api/auth/config`, runs the device-code flow against it (sign in as `admin` / `admin`), and caches the tokens in `~/.config/cufflink/platform-tokens.json`. Run it inside a project with a `Cufflink.toml`, or set `CUFFLINK_API_URL` / `--api-url` when running elsewhere.
- `--tenant-keycloak-url` is the address **the platform** reaches Keycloak at — it fetches the realm's JWKS from there to validate tokens and to exchange logins for API keys. Inside compose that is `http://keycloak:8080`, not `http://localhost:8180`. The browser-facing address goes in the project's `Cufflink.toml` (`keycloak_url`).

Then point a project at it:

```toml
[environments.local]
api_url = "http://localhost:8080"
tenant = "beta"
keycloak_url = "http://localhost:8180"
keycloak_realm = "beta"
```

`cufflink tenants list|update|delete` manage existing tenants. `cufflink tenants create-api-key --name ci` mints a platform admin API key (`CUFFLINK_PLATFORM_API_KEY`) for non-interactive admin use.

---

## Local Development Setup

To work on the platform itself, keep the infrastructure in Docker and run the platform natively:

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

`KEYCLOAK_ISSUER` is unnecessary here: `KEYCLOAK_URL` is already the browser-facing address, so the issuer in tokens matches. The dockerised worker, web-runner and `tenant-init` keep working against the native platform because they reach it through the published port.

The same pattern works for the worker (`cargo run -p cufflink-worker`) and web-runner (`cargo run -p cufflink-web-runner`, which additionally needs `CUFFLINK_BASE_DOMAIN`).

Build and test:

```bash
cargo build -p cufflink-cli          # target/debug/cufflink
cargo build --workspace
cargo test --workspace
```

The integration tier under `platform/tests` needs a Postgres; its harness is the `cufflink-testkit` crate (`testkit/`). It uses `CUFFLINK_TEST_DATABASE_URL` when set (what CI provides), and otherwise starts one embedded server per test binary through `postgresql_embedded`, which downloads a PostgreSQL archive from GitHub on first use and installs it under `target/tmp/postgresql` (so `cargo clean` removes it, and the leaked servers of this checkout are `pkill -f "$PWD/target/tmp/postgresql"` from its root: every checkout's servers run from a `target/tmp/postgresql` of its own, so the relative path stops those of every other checkout too, mid-run); offline with no cache the tier prints `skipped: no Postgres` and passes. A child that reports its database down while the harness cannot reach it either fails at once, naming the test Postgres, rather than timing out; `CUFFLINK_TEST_HEALTH_TIMEOUT_SECS` (default 90) bounds how long a child has to answer `/health` and then to become ready. Concurrent local runs from several checkouts should each set `CUFFLINK_TEST_DATABASE_URL` and `CUFFLINK_TEST_NATS_URL` to servers of their own. Tests that need NATS take `CUFFLINK_TEST_NATS_URL`, else a `nats-server` binary on `PATH` (or `NATS_SERVER_BIN`), else skip the same way. A test that leaves its server in a state the others could not run in takes a second one: `CUFFLINK_TEST_SPARE_NATS_URL`, else another `nats-server` process. Skips are written to stderr, so a plain `cargo test` shows them; with `CUFFLINK_CI=1` every skip is a failure. The database-role tier `db_roles_unprivileged` runs the platform as a role that may create roles and databases and is no superuser (what a hosted Postgres gives it) on the test server and on embedded PostgreSQL 17.6 and 18.6 of their own (`cufflink_testkit::postgres_at`), taken from `postgresql_embedded`'s release cache (`~/.theseus/postgresql`) or downloaded into it. The worker tests share the worker's durable JetStream consumers, so against a long-lived `CUFFLINK_TEST_NATS_URL` server they run one at a time and purge the worker streams first — point them at a NATS dedicated to tests. Test processes lease the ports they hand out in `target/tmp/ports`; runs from several checkouts on one machine share a directory by setting `CUFFLINK_TEST_PORT_LEASES` to it. Service authors' tests use the same harness and the native test host ([testing](testing.md)); the test host's embedded Postgres is installed under `CUFFLINK_TEST_POSTGRES_DIR` when set.

CI runs the suite against servers of its own, not embedded ones: Postgres 16, NATS 2.10 (the version Cufflink supports and the strictest in how it reads headers), a spare NATS and Redis. To run it as CI does:

```bash
docker run -d --name cufflink-test-pg -p 25432:5432 -e POSTGRES_USER=cufflink -e POSTGRES_PASSWORD=cufflink postgres:16
docker run -d --name cufflink-test-nats -p 24222:4222 nats:2.10 -js
docker run -d --name cufflink-test-nats-spare -p 24223:4222 nats:2.10 -js
docker run -d --name cufflink-test-redis -p 26379:6379 redis:7-alpine

CUFFLINK_TEST_DATABASE_URL=postgres://cufflink:cufflink@127.0.0.1:25432/postgres \
CUFFLINK_TEST_NATS_URL=nats://127.0.0.1:24222 \
CUFFLINK_TEST_SPARE_NATS_URL=nats://127.0.0.1:24223 \
CUFFLINK_TEST_REDIS_URL=redis://127.0.0.1:26379 \
CUFFLINK_CI=1 cargo test --workspace --no-fail-fast
```

Two test seams are read by the release platform binary, because the tier runs the real binary; neither may ever be set in production:

| Variable | Effect |
|----------|--------|
| `CUFFLINK_TEST_FAIL_AFTER_SCHEMA` | `1` fails every deploy between its schema phases and the metadata write (proves activation is atomic) |
| `WASM_TEST_ROUTE_BUDGET_SECS` | Shortens the 30 s custom-route budget (lets the tier observe a deadline quickly) |

---

## Platform Configuration

All configuration is via environment variables. Read in `platform/src/config.rs` unless noted.

A setting that carries a secret can be given as a file instead: `<name>_FILE` names the file and `<name>` stays unset. It applies to `DATABASE_URL`, `TENANT_DATABASE_URL`, `REDIS_URL`, `NATS_PASSWORD`, `NATS_NKEY_SEED`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `CUFFLINK_SECRETS_KEK`, `CUFFLINK_SECRETS_KEK_PREVIOUS`, `CUFFLINK_CACHE_SIGNING_KEY`, `CUFFLINK_SERVICE_IDENTITY_KEYS`, `CUFFLINK_DEFAULT_KC_OPS_CLIENT_SECRET`, `WEB_STATUS_TOKEN` and `EXT_STATUS_TOKEN`, in the platform, the worker and both runners ([secrets as files](deployment.md#secrets-as-files)).

### Core

| Variable | Default | Description |
|----------|---------|-------------|
| `PLATFORM_HOST` | `0.0.0.0` | Bind address |
| `PLATFORM_PORT` | `8080` | Port |
| `<name>_FILE` | *(unset)* | For each secret setting named above: the path of a file holding it, read once at start with one trailing newline removed. Setting both `<name>` and `<name>_FILE`, or a file that is missing, empty or not UTF-8, stops the process at start, naming the variable |
| `DATABASE_URL` | `postgres://cufflink:cufflink@localhost:5432/cufflink` | Platform PostgreSQL |
| `NATS_URL` | `nats://localhost:4222` | NATS server |
| `KEYCLOAK_URL` | `http://localhost:8180` | Keycloak as the platform reaches it (JWKS, admin API) |
| `KEYCLOAK_JWKS_MAX_STALE_SECS` | `86400` | How long a realm's cached JWKS keeps validating tokens after refreshes start failing (a Keycloak outage does not log everyone out); served stale sets are counted in `cufflink_jwks_stale_served_total{realm}`. An unknown `kid` still forces a fresh fetch |
| `KEYCLOAK_REALM` | `cufflink` | Platform realm (platform admins) |
| `KEYCLOAK_ISSUER` | *(unset)* | Browser-facing Keycloak base URL when it differs from `KEYCLOAK_URL` (e.g. `http://localhost:8180` in compose). Tokens must carry `{issuer}/realms/{realm}` (a tenant's own `keycloak_issuer` wins; unset, the issuer is discovered from Keycloak). Returned by `/api/auth/config` |
| `REDIS_URL` | *(unset)* | Redis for shared rate-limit counters and the permission cache; in-memory fallback when unset |
| `LOKI_URL` | *(unset)* | Loki base URL backing `GET /api/services/{id}/logs` and `cufflink logs` |
| `LOKI_LABEL_MODEL` | `legacy` | How that Loki labels the lines: `legacy` (JSON bodies from the collector's deprecated `loki` exporter) or `scoped` (OTLP ingestion labelled by tenant and service) ([observability](observability.md#loki_label_model)) |
| `DEFAULT_TENANT_SLUG` | `default` | Tenant auto-assigned to realm users that have no roles yet |
| `MAX_TENANT_POOLS` | `64` | Maximum tenant connection pools kept open |
| `MAX_CONNS_PER_TENANT` | `4` | Connections per tenant pool |
| `WORKER_ROUTES` | *(unset)* | JSON map `{"service": ["handler", ...]}` of handlers to dispatch to NATS workers instead of running in-process (see [WASM Workers](wasm-workers.md)) |
| `PLATFORM_SQL_MAX_ROWS` | `100000` | Row cap for `POST /api/services/{id}/query` (`cufflink sql`) |
| `PLATFORM_SQL_TIMEOUT_SECS` | `30` | Statement timeout for the same endpoint |
| `CUFFLINK_DEFAULT_KC_OPS_CLIENT_ID` | *(unset)* | Keycloak confidential client used for admin-API calls (role assignment, user search, logout) when a tenant has no ops client of its own |
| `CUFFLINK_DEFAULT_KC_OPS_CLIENT_SECRET` | *(unset)* | Its secret |
| `CUFFLINK_SERVICE_ACCOUNT_CLIENTS` | *(unset)* | Comma-separated Keycloak client IDs (matched on `azp`) treated as service accounts on every tenant, bypassing per-user RBAC, while a tenant's `service_accounts` policy is `off` or `warn`. The default ops client is always included. Prefer each tenant's `service_account_clients`. Never list human-login clients |
| `CUFFLINK_POLICY_<KEY>` / `CUFFLINK_POLICY_<KEY>_CONFIG` | *(see [policies](policies.md))* | Platform default mode and JSON config of a tenant policy (`TENANT_BINDING`, `GRAPHQL`, `STORAGE_SCOPE`, `EGRESS`, `WASM_LIMITS`, `SERVICE_ACCOUNTS`, `AUDIENCE`, `REDIS_SCOPE`, `STRICT_FILTERS`, `OUTBOX`, `DB_ROLES`, `SCHEMA_GUARD`); read by the platform and the worker |
| `CUFFLINK_PLATFORM_ADMIN_ROLE` | `cufflink-platform-admin` | Platform-realm role a JWT needs for platform administration |
| `CUFFLINK_API_KEY_TTL_DAYS` | `90` | Lifetime of a newly minted tenant API key; a tenant key without an expiry gets one this far from the platform's next start |
| `CUFFLINK_ADMIN_API_KEY_TTL_DAYS` | `90` | The same for platform-admin keys; `0` leaves them unbounded (set it before first starting a release that has it, if admin keys must not expire; see [deployment](deployment.md#api-keys)) |
| `CUFFLINK_SERVICE_IDENTITY_KEYS` | *(unset)* | `kid:<64 hex>` list signing and verifying service-call identities (first signs, all verify); the same value on every platform and worker replica, else each process uses its own key and worker-made calls fail ([deployment](deployment.md#service-calls-and-identities)) |
| `CUFFLINK_CACHE_SIGNING_KEY` | *(unset)* | Key (hex or raw, at least 16 bytes) sealing permission-cache entries; set the same value on every platform and worker replica, or each process uses its own. Without `CUFFLINK_SECRETS_KEK` it is also what the outbox's audit and webhook rows are authenticated under ([policies](policies.md#rows-the-platform-wrote)) |
| `CUFFLINK_SECRETS_KEK` / `CUFFLINK_SECRETS_KEK_ID` | *(unset)* | A 32-byte key in base64 and its name: the key each tenant's data key is wrapped under. Stored secrets (service configs, webhook secrets, tenants' Keycloak ops secrets, database-role passwords) are sealed under the tenant's data key with AES-256-GCM, bound to their row. Set the same values on every platform, worker and shared-mode web-runner replica (a tenant-scoped web runner and the ext-runner do not read them) |
| `CUFFLINK_SECRETS_KEK_PREVIOUS` | *(unset)* | `id:base64,…`: keys that still open data keys wrapped before a rotation, never seal. After `cufflink admin secrets reseal --operation rotate_kek`, and once `--operation verify` counts no data key under a previous key ([verifying a key ring](db-roles-and-secrets.md#verifying-a-key-ring)), they can be dropped. They still authenticate outbox rows written under them, so drop a key only once every outbox is drained ([policies](policies.md#rows-the-platform-wrote)) |
| `CUFFLINK_SECRET_STORAGE` | `plaintext` without a key, `dual` with one | `plaintext`: the plaintext column alone, as before. `dual`: both columns, so a replica of an earlier release reads what this one writes; a sealed value that does not open is an error, never a fallback. `encrypted`: the sealed column alone; startup is refused while any secret is stored in plaintext alone (run `cufflink admin secrets reseal --operation encrypt` under `dual` first). A release before v0.28.0 reads no secret stored `encrypted`: return to `dual` and reseal before rolling back ([database roles and secrets](db-roles-and-secrets.md#stored-secrets)) |
| `SSE_MAX_STREAMS_PER_PRINCIPAL` | `20` | Open SSE streams per principal per replica |
| `NATS_CREDS_FILE` / `NATS_NKEY_SEED` / `NATS_USER` + `NATS_PASSWORD` | *(unset)* | This component's NATS credentials (at most one method); anonymous when unset. Also read by the worker, web-runner and ext-runner |
| `CUFFLINK_ENFORCE_ROUTE_POSTURE` | `false` | `true`/`1` rejects deploys whose custom routes lack a posture; otherwise they run unenforced with a warning (`platform/src/deployer/validator.rs`) |
| `CUFFLINK_STORAGE_DIR` | `./data/files` | Local directory for file uploads, used only when no object storage is configured (`platform/src/api/files.rs`) |
| `RUN_ENV` | `development` | `development`, `staging` or `production`. In production the default `DATABASE_URL` credentials and `STORAGE_BACKEND=memory`/`local` are refused at startup |
| `CUFFLINK_DEV_BOOTSTRAP` | *(unset)* | `tenant=<slug>,realm=<realm>,key_file=<absolute path>`: the tenant and the API key of a development stack, made before the platform is ready ([cufflink dev](dev.md#the-bootstrap)). Refused at startup unless `RUN_ENV=development` is set explicitly (unset, `staging` and `production` are refused) |
| `SHUTDOWN_GRACE_SECS` | `45` | Time from SIGTERM/SIGINT to exit: in-flight requests, tracked background tasks, NATS and telemetry flushes share it |
| `SHUTDOWN_UNREADY_DELAY_SECS` | `0` | How long `/ready` answers 503 before the listener closes, for installs without a preStop hook |
| `STARTUP_DEADLINE_SECS` | `0` (none) | How long startup may take to connect, migrate and load the registry before the process exits with an error. A failure no retry can fix (rejected credentials, a missing database or privilege, applied migrations this binary contradicts) exits at once whatever the deadline; a transient one (the database or NATS not up yet) is retried until the deadline |
| `RUST_LOG` | `info,cufflink_platform=debug` | Log filter (`telemetry/src/lib.rs`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(unset)* | OTLP/HTTP endpoint; set to export traces and logs (e.g. `http://localhost:4318`) |

Any variable set to a value that does not parse (including `WORKER_ROUTES` that is not valid JSON) is a startup error rather than a silent fallback to the default.

| Variable | Default | Description |
|----------|---------|-------------|
| `REGISTRY_RELOAD_SECS` | `60` | How often each replica reloads the service registry from the database, on top of the reload every deploy triggers over NATS |
| `SQL_MIGRATION_LOCK_WAIT_SECS` | `600` | How long a deploy waits for the lock of its SQL migrations' history before it fails with 503 |
| `SQL_MIGRATION_STATEMENT_TIMEOUT_SECS` | `600` | `statement_timeout` of a deploy's SQL migrations |
| `SQL_MIGRATION_LOCK_TIMEOUT_SECS` | `10` | `lock_timeout` of a deploy's SQL migrations |
| `DEPLOY_JOB_RETENTION_DAYS` | `30` | How long the record of a deploy that ran beyond its request (`GET /api/deploys/{id}`) is kept once it ended; each replica's deploy sweep deletes older ones. `0` is refused |
| `CUFFLINK_DLQ_MAX_AGE_DAYS` | `14` | How long the dead-letter queue (`CUFFLINK_DLQ`) keeps a message a consumer gave up on (platform and worker). `0` is refused |
| `OUTBOX_RELAY_BATCH` | `256` | Rows the outbox relay publishes in one transaction of the tenant's database |
| `OUTBOX_RELAY_POLL_MS` | `1000` | How often a relay with nothing to publish looks for rows. Between looks it is woken by the write itself (`LISTEN`); behind a transaction pooler (`TENANT_DB_POOLER=pgbouncer_transaction`), which carries no `LISTEN`, it looks every 250 ms at the most |
| `OUTBOX_PUBLISH_CONCURRENCY` | `64` | Acknowledgements of JetStream the relay of one tenant awaits at once. Rows are sent in the order they were written and nothing is sent after the first that is known to have failed, but the rows already in flight are settled by their own acknowledgements, so up to that many rows written after a failed row may be published before it is tried again; `1` publishes strictly one row after another |
| `OUTBOX_MAX_ATTEMPTS` | `20` | Failures after which an outbox row is moved to the dead-letter queue (`cufflink.dlq.{tenant}.{service}.outbox.{kind}`). Until then the row is tried again, after 1 s doubling to 5 min, before any row written after it |
| `CUFFLINK_AUDIT_MAX_AGE_DAYS` | `90` | How long the stream of durable audit records (`CUFFLINK_AUDIT`) keeps a record. `0` is refused |
| `CUFFLINK_AUDIT_MAX_BYTES` | `1073741824` (1 GiB) | The most the stream of durable audit records holds, the oldest going first. The server reserves it of its JetStream storage (`max_file_store`, and the account's when the account is limited) when the stream is made; a stream the account cannot hold is not made, which is logged and counted (`cufflink_platform_stream_unavailable_total{stream="CUFFLINK_AUDIT"}`): the outboxes are held until it is, and a record published directly (`CUFFLINK_AUDIT_DURABLE_ALL`) is dropped and counted. `0` bounds it by age alone. The stream is made with it and not changed after |
| `CUFFLINK_AUDIT_READ_SAMPLE_PERCENT` | `100` | Of every hundred reads of `sensitive` tables, how many a replica records; a record of a sampled read says `"sample_percent"`. 1 to 100 |
| `CUFFLINK_AUDIT_READ_COALESCE_SECS` | `0` (off) | For how long a read like one a replica recorded (the same caller, table, rows and query) is counted into the next record of its kind (`"coalesced": n`) in place of one of its own. A replica coalesces at most 10 000 kinds at once |
| `CUFFLINK_AUDIT_DURABLE_ALL` | `false` | `true` keeps the audit records of every tenant on `CUFFLINK_AUDIT`, published once a write is committed. Otherwise the records kept are those of the tenants whose `outbox` is `enforce`, written with the row |
| `WEBHOOK_DELIVERY_CONCURRENCY` | `16` | Webhook deliveries a replica makes at once, for the tenants whose `outbox` is `enforce` |
| `WEBHOOK_DELIVERY_PER_TENANT` | `8`, at most `WEBHOOK_DELIVERY_CONCURRENCY` | Of those, the most one tenant's deliveries take; set, at most `WEBHOOK_DELIVERY_CONCURRENCY`. A delivery that finds its tenant's full waits about `WEBHOOK_DELIVERY_TIMEOUT_SECS`, which is no attempt |
| `WEBHOOK_DELIVERY_PER_WEBHOOK` | `4`, at most `WEBHOOK_DELIVERY_PER_TENANT` | Of those, the most one webhook's deliveries take; set, at most `WEBHOOK_DELIVERY_PER_TENANT`. Both are per replica |
| `WEBHOOK_DELIVERY_RETENTION_DAYS` | `30` | How long what became of a delivery is kept (`GET …/webhooks/{id}/deliveries`): a delivery not written to for as long is deleted, every hour. `0` is refused |
| `WEBHOOK_DELIVERY_TIMEOUT_SECS` | `10` | The longest one attempt at a delivery takes, the receiver's answer read included; a receiver that has not answered by then is tried again, one whose answer's status came is judged by it |
| `CONSUMER_DEFAULT_ACK_WAIT_SECS` | `30` | `ack_wait` of a durable the platform creates for a subscription that declares none (5 to 3600; anything else is refused at startup). Unset, durables get the 30 s v0.27.0's had ([subscriptions](subscriptions.md)) |
| `CONSUMER_RECONCILE_INTERVAL_SECS` | `60` | How often every replica reconciles its consumer loops with the registry, besides whenever the registry changes. `0` is refused |
| `SCHEDULE_ON_TIME_GRACE_SECS` | `60` | How late a schedule's fire may start and still count as on time; a fire later than this after an outage is a catch-up ([schedules](subscriptions.md#schedules)) |
| `SCHEDULE_CATCH_UP_CAP` | `10` | The most fires an outage missed that one schedule with `catch_up: all` runs when it resumes; `0` is refused (declare `catch_up: none` instead) |
| `CUFFLINK_VERSION` | *(build time)* | Stamped into the binary by CI (`sha-<commit>`) and reported by `GET /api/version`; a local build reports `<crate version>-dev` |

Request bodies are capped at 4 MB on `/svc/{tenant}/{service}/_fn/{handler}` and 250 MB elsewhere (sized for web app artifact uploads), unless the HTTP layers below tighten the data routes.

### HTTP request layers (`platform/src/app/layers.rs`)

`CUFFLINK_HTTP_PROFILE` (`legacy`, the default, or `hardened`) sets the defaults of `HTTP_DATA_BODY_LIMIT_BYTES`, `HTTP_DATA_TIMEOUT_SECS`, `HTTP_MAX_CONCURRENT_REQUESTS`, `CORS_ALLOWED_ORIGINS`, `RATE_LIMIT_ENABLED` and `CRUD_MAX_IN_ITEMS`; each is overridable. The table of values is in [deployment](deployment.md#http-request-layers). `legacy` leaves every limit as it was.

### Database connections (`db/src/lib.rs`)

Every pool and single connection the platform, worker, web-runner and ext-runner open shares these settings. Unset variables keep the defaults; a value that does not parse is a startup error.

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_POOL_TIMEOUT_MS` | `5000` | Bound on waiting for a free pooled connection; `0` is rejected (a pool wait must be bounded) |
| `DB_STATEMENT_TIMEOUT_MS` | `30000` | Session default `statement_timeout` for every connection (`0` disables). Set through the startup `options` parameter so a clean recycle restores rather than clears it |
| `DB_POOL_RECYCLE` | `clean` | How a connection is checked before reuse: `fast` (none), `verified` (round trip), `clean` (round trip plus `RESET ALL`, `UNLISTEN *`, `pg_advisory_unlock_all()`, `DISCARD TEMP`) |
| `DB_CONN_MAX_LIFETIME_SECS` | `1800` | A connection older than this is discarded at its next recycle (`0` never), so pools cycle after a failover |
| `DB_TCP_KEEPALIVE_IDLE_SECS` / `DB_TCP_KEEPALIVE_INTERVAL_SECS` / `DB_TCP_KEEPALIVE_RETRIES` | `30` / `10` / `3` | TCP keepalives |
| `DB_TCP_USER_TIMEOUT_MS` | `30000` | Linux `TCP_USER_TIMEOUT`; ignored elsewhere |
| `DATABASE_TLS` | *(unset)* | `true`/`1`/`yes`/`on` encrypts the connection |
| `DATABASE_TLS_VERIFY` | `off` | With TLS on: `off` (encrypt only, the Cloud SQL private-IP posture), `ca` (chain verified against `DATABASE_TLS_CA_CERT`, hostname not), `full` (system store plus the optional CA, hostname verified) |
| `DATABASE_TLS_CA_CERT` | *(unset)* | PEM path of the CA certificate |
| `DB_DEPLOY_STATEMENT_TIMEOUT_MS` | `600000` | `SET LOCAL statement_timeout` for each schema-executor phase of a deploy (platform only) |
| `TENANT_DATABASE_URL` | *(unset: `DATABASE_URL`)* | Where tenant databases are reached, its database name replaced per tenant; typically a PgBouncer in front of the server. Refused with the default development password when `RUN_ENV=production`. The platform database always uses `DATABASE_URL` |
| `TENANT_DB_POOLER` | `direct` | `pgbouncer_transaction` when `TENANT_DATABASE_URL` is a PgBouncer in `pool_mode=transaction` (below) |

Longer work sets its own transaction-local override: backup export/restore and `CREATE DATABASE` for a new tenant run without a statement timeout.

#### Tenant databases behind PgBouncer

With `TENANT_DB_POOLER=pgbouncer_transaction`, tenant pools run behind PgBouncer in `pool_mode=transaction`, where a server connection is held only for the length of a transaction:

- **Prepared statements**: PgBouncer needs `max_prepared_statements` above `0` (the default in 1.21 and later is `200`). CRUD reads use protocol-level prepared statements outside an explicit transaction.
- **Statement timeout**: tenant pools send no startup `options` (PgBouncer refuses them), so `DB_STATEMENT_TIMEOUT_MS` does not reach tenant sessions. Set it on each tenant role instead, for example `ALTER ROLE <tenant role> SET statement_timeout = '30s'`; Postgres applies it to every server session PgBouncer opens. Without it CRUD statements have no timeout. Guest statements set their own `SET LOCAL statement_timeout` either way. No `track_extra_parameters` or `ignore_startup_parameters` entry is needed.
- **Recycling**: tenant connections are recycled with `verified` whatever `DB_POOL_RECYCLE` says, because a clean recycle's session commands would run on whichever server connection PgBouncer picks.
- **Guest statements that need a session are refused** (legacy calls return `-1`, `db-v2` returns `not_supported`, `db::execute_all` fails at the statement's index):
  - statements Postgres refuses inside a transaction (`CREATE INDEX CONCURRENTLY`, `VACUUM`, …): run them in deploy-time migrations;
  - `SET` other than `SET LOCAL`, `SET TRANSACTION` and `SET CONSTRAINTS`, and `RESET`: their effect would outlive the transaction on a server connection other invocations share.

  `set_config(name, value, false)` and `DO` blocks can still change session state and cannot be recognised; use `set_config(name, value, true)` or `SET LOCAL` inside a transaction.
- **The platform database stays direct**: deploy locks are session-level advisory locks, so `DATABASE_URL` must not point at a transaction pooler.

### Object storage (`storage/src/lib.rs`, `s3.rs`, `gcs.rs`, `common.rs`)

The platform degrades gracefully without object storage (WASM upload, web apps and backups need it; file uploads fall back to `CUFFLINK_STORAGE_DIR`). The worker treats it as optional too; web-runner requires it.

| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_BACKEND` | `s3` | `s3` (any S3-compatible store), `gcs`, `memory` (process-local, for tests) or `local` (a shared directory, for tests and development) |
| `STORAGE_LOCAL_DIR` | *(required for local)* | Directory the `local` backend stores objects under; every process pointing at it sees the same objects |
| `S3_ENDPOINT` | *(required for s3)* | e.g. `http://rustfs:9000`; `http://` endpoints are allowed |
| `S3_ACCESS_KEY` | *(required for s3)* | |
| `S3_SECRET_KEY` | *(required for s3)* | |
| `S3_REGION` | `us-east-1` | |
| `S3_BUCKET` | `cufflink` | Primary bucket |
| `GCS_BUCKET` | *(required for gcs)* | Primary bucket |
| `GOOGLE_APPLICATION_CREDENTIALS` | *(unset)* | Service-account JSON key path for GCS; without it, Application Default Credentials / Workload Identity are used and presigning goes through IAM `signBlob` |
| `OBJECT_STORE_RETRY_TIMEOUT_SECS` | `20` | Overall retry budget per object-store request |

With `STORAGE_BACKEND=s3` (or unset), the S3 backend is only activated when all five `S3_*` variables are set; otherwise the platform starts without object storage.

### Rate limiting (`platform/src/middleware/rate_limit.rs`)

Per-client-IP limiter, **disabled by default**. Counters are shared across replicas through Redis when `REDIS_URL` is set, with an in-memory fallback. `/health` and `/ready` are mounted outside the limiter. Rejections return 429 and increment `cufflink_rate_limited_total`.

| Variable | Default | Description |
|----------|---------|-------------|
| `RATE_LIMIT_ENABLED` | `false` | `1`/`true`/`yes`/`on` to enable |
| `RATE_LIMIT_PER_SECOND` | `20` | Sustained requests per second per client IP |
| `RATE_LIMIT_BURST` | `60` | Burst allowance (never below `RATE_LIMIT_PER_SECOND`) |
| `RATE_LIMIT_CLIENT_IP_SOURCE` | `peer` | `peer` (TCP peer), `gclb` (Google external ALB, Cloudflare-aware), `xff-first` (first `X-Forwarded-For` entry — only behind a proxy that overwrites the header). Also keys anonymous SSE streams and the address recorded for platform-admin calls |
| `RATE_LIMIT_EXEMPT_PRIVATE_PEERS` | `true` | Never limit callers whose socket peer is a private address (in-cluster traffic) |
| `RATE_LIMIT_EXEMPT_IPS` | unset | Comma-separated IPv4/IPv6 addresses and CIDR ranges (e.g. `203.0.113.7,2001:db8::/32`) whose client IP is never limited. For a trusted upstream that calls on behalf of many users from one egress address, which would otherwise share a single bucket. An entry that is not an address or range refuses startup |

### WASM runtime limits (`platform/src/wasm_runtime/mod.rs`)

Every guest execution runs under one deadline: the caller's timeout, the epoch interrupt that stops the guest at that instant, and the bound every host call clamps its own I/O to. The platform and worker read the same variables.

| Variable | Default | Description |
|----------|---------|-------------|
| `CUFFLINK_POLICY_WASM_LIMITS` | `warn` | Platform default of the `wasm_limits` tenant policy, which decides what happens when a guest grows past its memory cap (`WASM_MAX_MEMORY_MB`, or the tenant's `memory_bytes` when lower): `off` allows silently, `warn` allows, counts `cufflink_wasm_limit_events_total{limit="memory",mode="warn"}` and records a policy violation once per execution, `enforce` denies the growth, which ends the handler with a 500 naming the limit. Set per tenant with `cufflink tenants policy set`. The retired `CUFFLINK_WASM_LIMITS` is not read; a platform that has it set warns at startup. Everything else in this table is always enforced |
| `WASM_MAX_MEMORY_MB` | `512` | Linear-memory cap per execution |
| `WASM_MAX_TABLES` / `WASM_MAX_TABLE_ELEMENTS` / `WASM_MAX_INSTANCES` | `8` / `1000000` / `1` | Table and instance caps; a Rust `cdylib` guest needs one table and instantiates nothing |
| `WASM_MAX_GUEST_LEN_MB` | `64` | Largest pointer/length pair a guest may hand a host function; a negative or larger length returns `-1` and counts `limit="length"` |
| `WASM_MAX_RESPONSE_MB` | `64` | Largest handler response; beyond it the call fails with a 500 rather than allocating |
| `WASM_MAX_CONCURRENT` | `256` | Guest executions in flight at once; queueing for a slot counts against the handler's budget |
| `WASM_MAX_OPEN_TX` | `2` | Interactive `db_v2` transactions one invocation may hold open ([db-v2](db-v2.md)) |
| `WASM_ADMISSION_WAIT_MS` | `1000` | Longest a route waits for a slot of its `max_concurrent` before it is answered `503` ([route limits](routing.md#route-limits)) |
| `WASM_ADMISSION_RETRY_AFTER_SECS` | `2` | `Retry-After` of a `503` answered for capacity |
| `WASM_EPOCH_TICK_MS` | `100` | How often the deadline is checked while a guest runs; a runaway guest survives its deadline by at most one tick |
| `WASM_MODULE_CACHE_MAX` | `128` | Compiled modules kept in memory (least recently used evicted) |

### WASM host (`platform/src/wasm_runtime/host.rs`)

| Variable | Default | Description |
|----------|---------|-------------|
| `HOST_OUTBOUND_HTTP_TIMEOUT_SECS` | `28` | Total budget for a guest `http::*` call: every redirect hop's lookup, connect and exchange, and reading the body |
| `HOST_OUTBOUND_CONNECT_TIMEOUT_SECS` | `20` | Connect budget for the same |
| `HOST_HTTP_MAX_RESPONSE_BYTES` | `10485760` | Largest response body read by a guest `http::*` call; a tenant's `egress` policy may lower it |
| `HOST_FANOUT_CONCURRENCY` | `32` | Concurrency for fan-out host calls |
| `DB_READ_ONLY_ROLE` | `reports_ro` | PostgreSQL role the read-only query host function runs as |
| `DB_READ_ONLY_TIMEOUT_SECS` | `30` | Statement timeout for read-only guest queries (further capped by the handler's remaining budget) |
| `DB_READ_ONLY_MAX_ROWS` | `10000` | Row cap for read-only guest queries |
| `CACHE_MGET_MAX_KEYS` | `256` | Keys one guest `cache::mget` may read (at most `4096`; `0` is refused); more is `too_large` |
| `CACHE_MGET_MAX_BYTES` | `8388608` | Bytes of values one `cache::mget` may return (`0` is refused); Redis checks the sizes before reading, and a larger reply is `too_large` |
| `HOST_HTTP_MAX_REQUEST_BYTES` | `33554432` | Largest request body one `http::fetch_bytes`/`http::multipart` call (feature `http-v2`) may send; a larger one is `too_large` before anything is sent |
| `HOST_HTTP_V2_MAX_RESPONSE_BYTES` | `52428800` | Most an `http-v2` call may ask to read of a response; one that does not ask reads up to `HOST_HTTP_MAX_RESPONSE_BYTES` |
| `HOST_STORAGE_MAX_UPLOAD_BYTES` | `52428800` | Largest object one `storage::upload` (feature `storage-v2`) may store. It is held to what fits the envelope a guest may hand over (the body travels as base64 inside `WASM_MAX_GUEST_LEN_MB`): `50319360` bytes (≈48 MiB) at the defaults. A larger body is `too_large` |
| `UTIL_SLEEP_MAX_MS` | `30000` | Longest one `util::sleep_ms` (feature `util-v2`) lasts; it is clamped to the handler's remaining budget as well |

The platform and the worker read this table. Zero or unparseable values fall back to the defaults, except for the `CACHE_MGET_*`, `HOST_HTTP_MAX_REQUEST_BYTES`, `HOST_HTTP_V2_MAX_RESPONSE_BYTES`, `HOST_STORAGE_MAX_UPLOAD_BYTES` and `UTIL_SLEEP_MAX_MS` limits, which refuse them at startup, naming the variable.

Every host call is bounded by the handler's remaining execution budget. Guest `db::query`/`db::execute` statements run in their own transaction with the service schema on the search path and `statement_timeout` set to the remaining budget (minus a 500 ms margin, floor 1 s), so both settings revert whether the statement commits or fails; `db::execute_all` and `db::query_read_only` carry the same timeout. Statements Postgres refuses inside a transaction (`CREATE INDEX CONCURRENTLY`, `VACUUM`, …) run on the bare connection between a `SET` and `RESET` pair. NATS requests, HTTP fetches and everything that waits on object storage, image transforms, job enqueue or config reads are clamped to the budget as well, so no host call can outlive the execution it belongs to.

---

## Worker Configuration

`worker/src/main.rs` and `worker/src/long_jobs.rs`. The worker loads the same core config as the platform (it needs `DATABASE_URL` and `NATS_URL`) and the same optional object-storage variables.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | *(platform default)* | Platform PostgreSQL |
| `NATS_URL` | *(platform default)* | NATS server |
| `MAX_TENANT_POOLS` | `32` | Tenant pools (smaller than the platform's 64); `0` is refused |
| `MAX_CONNS_PER_TENANT` | `2` | Connections per tenant pool (the platform's default is 4); `0` is refused |
| `SUBSCRIBE_PATTERN` | `cufflink.wasm.>` | NATS subject pattern for handler dispatch |
| `QUEUE_GROUP` | `wasm-workers` | NATS queue group (replicas share the load) |
| `LONG_JOBS_SUBSCRIBE_PATTERN` | `cufflink.jobs.>` | JetStream subject pattern for long-running jobs |
| `MAX_CONCURRENT_JOBS` | `4` | Long-running jobs executed concurrently per worker |
| `LONG_JOBS_ACK_WAIT_SECS` | `60` | JetStream ack wait of the jobs consumer. A running job's message is kept alive every third of it, so it no longer bounds how long a job runs |
| `JOB_LEASE_SECS` | `60` | How long a job is its worker's without a heartbeat (at least `3`); the worker extends it every third of that. A worker that cannot gives the job up when the lease runs out, and the reclaimer publishes it again |
| `JOB_NAK_MAX_DELAY_SECS` | `60` | Longest a job's message is handed back to the stream for before a retry; a job due later has its message acknowledged and is published by the reclaimer when it is due. `0` has every retry's message acknowledged and the job published when it is due |
| `JOB_RECLAIM_INTERVAL_SECS` | `30` | How often each worker's reclaimer runs: it publishes due jobs no worker holds, fails those out of attempts, files dead letters, and reports `cufflink_job_backlog` |
| `JOB_REPUBLISH_AFTER_SECS` | `300` | How long a published job may wait for a worker before the reclaimer publishes it again; never longer than `CUFFLINK_LONG_JOBS`'s duplicate window, which the reclaimer reads off the stream on every pass, so a job published again is stored again rather than dropped as a duplicate |
| `PLATFORM_INTERNAL_URL` | *(unset)* | The platform address guests on this worker send [service calls](service-calls.md) to; unset, they cannot call other services (warned at startup) |
| `WORKER_HTTP_PORT` | `8081` | Probe listener: `/health` (liveness), `/ready` (every consumer up, the platform database answering, not shutting down) and `/metrics`; `0` disables it |
| `WORKER_MAX_CONCURRENT_SYNC` | `32` | Request/reply handlers executed concurrently; further requests wait in NATS. `0` is refused |
| `WORKER_AFTER_HOOK_CONCURRENCY` | `1` | After-hooks executed at once per worker. `1` runs them one at a time, in the order they were published; a higher number gives the order up for throughput. One that fails is delivered again, up to three times, then kept in the dead-letter queue. `0` is refused |
| `REDIS_URL` | *(unset)* | Lets `redis::*` host functions work in worker-routed handlers and jobs; without it they report Redis as disabled |
| `SHUTDOWN_GRACE_SECS` | `45` | Grace from SIGTERM to exit: `/ready` turns 503 (`shutting_down`) while `/health` keeps answering, consumers stop fetching, the message each consumer is handling finishes and is acked, the rest of its batch is handed back at once, and jobs still running at the hard deadline (grace minus 2 s) are stopped and handed back for immediate redelivery. The probe listener closes last |
| `STORAGE_BACKEND`, `S3_*`, `GCS_BUCKET` | *(see above)* | Optional; needed to load WASM modules from object storage |
| `RUST_LOG` | `info,cufflink_worker=debug` | Log filter |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(unset)* | OTLP trace and log export, as for the platform; worker spans are parented on the dispatching request's trace |

A `JOB_*` value the worker cannot use (not a number, a `JOB_LEASE_SECS` below `3`, or `0` for `JOB_RECLAIM_INTERVAL_SECS` or `JOB_REPUBLISH_AFTER_SECS`) stops it at startup with an error naming the variable. An unusable `MAX_CONCURRENT_JOBS` or `LONG_JOBS_ACK_WAIT_SECS`, which earlier releases read, is ignored and the default used, as before. The worker also reads the WASM runtime and host tables above.

---

## Web Runner Configuration

`web-runner/src/config.rs`. Serves `mode = "web"` services (Next.js apps uploaded through `POST /api/services/{id}/web`) and exits at startup if a required variable is missing.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | *(required without `WEB_TENANT`)* | Platform PostgreSQL |
| `WEB_TENANT` | *(unset)* | Serve this one tenant's apps from the platform's runner API, holding no database, secrets or storage credentials ([tenant-scoped web runner](deployment.md#tenant-scoped-web-runner)); needs `PLATFORM_URL` and a runner key |
| `PLATFORM_URL` | *(unset)* | With `WEB_TENANT`, the platform's URL the runner API is reached at; otherwise a value apps are passed |
| `CUFFLINK_RUNNER_KEY` / `CUFFLINK_RUNNER_KEY_FILE` | *(unset)* | With `WEB_TENANT`, the tenant's runner key or a file holding it; refused without `WEB_TENANT` |
| `WEB_POLL_SECS` | `30` | With `WEB_TENANT`, how often the runner asks whether its key is still accepted |
| `NATS_URL` | `nats://localhost:4222` | NATS server |
| `NATS_CREDS_FILE` / `NATS_NKEY_SEED` / `NATS_USER` + `NATS_PASSWORD` | *(unset)* | NATS credentials, as for the platform |
| `RUNNER_PORT` | `3100` | Listen port |
| `APPS_DIR` | `./apps` | Where app artifacts are unpacked and run; must be writable by the runner (uid `10001` in the image), checked at startup |
| `CUFFLINK_BASE_DOMAIN` | *(required)* | Public DNS suffix, e.g. `example.com` (compose uses `localhost`) |
| `CUFFLINK_PUBLIC_SCHEME` | `https` | `http` only for a local stack without TLS |
| `CUFFLINK_HOSTNAME_FORMAT` | `flat` | `flat` → `{service}--{tenant}.{base}`; `hierarchical` → nested labels under the base domain |
| `SHUTDOWN_GRACE_SECS` | `45` | Grace from SIGTERM to exit: proxied requests finish, then every Next.js child is stopped, then NATS and telemetry flush |
| `WEB_CHILD_STOP_TIMEOUT_SECS` | `10` | How long a child gets between SIGTERM and SIGKILL at shutdown |
| `STORAGE_BACKEND`, `S3_*`, `GCS_BUCKET` | *(required without `WEB_TENANT`)* | Object storage holding the web artifacts |
| `RUST_LOG` | `info,cufflink_web_runner=debug` | Log filter |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(unset)* | OTLP/HTTP trace and log export |
| `WEB_ENV_PASSTHROUGH` | *(unset)* | Runner variables an app sees beyond the built-in allowlist (`NAME,PREFIX_*`, never a credential); `*` is refused at startup ([deployment](deployment.md#web-runner)) |
| `CUFFLINK_CHILD_TELEMETRY` | `legacy` | `scoped` makes each app report to OpenTelemetry as its service ([deployment](deployment.md#childrens-telemetry)) |
| `CUFFLINK_CHILD_OUTPUT_LEVEL` | `debug` | `info` logs what apps print at `info` ([deployment](deployment.md#childrens-telemetry)) |
| `WEB_PORT_RANGE` | `3200-3999` | Ports apps listen on (on `127.0.0.1`); exhausting it fails the deploy |
| `WEB_ARTIFACT_MAX_ENTRIES` / `WEB_ARTIFACT_MAX_BYTES` | `200000` / `2147483648` | Bounds on what one artifact extracts to |
| `WEB_STATUS_TOKEN` | *(unset)* | Bearer token `/_cufflink/status` and `/_cufflink/metrics` require; unset, neither is served. `/health` and `/_cufflink/ready` are always open. Never passed to apps |

A web-mode project declares `mode = "web"` and `name` under `[service]` in `Cufflink.toml`; `cufflink deploy` builds and uploads the artifact instead of a Rust manifest.

---

## CLI Configuration

### Cufflink.toml

Every command except `cufflink tenants ...` needs a `Cufflink.toml` in the current directory or a parent. `--env/-e <name>` (global flag) picks an environment; otherwise `[service].default_env` is used.

```toml
[service]
default_env = "local"
# name = "my-app"      # required for mode = "web"
# mode = "web"         # crud (default, from the manifest), wasm, or web

[environments.local]
api_url = "http://localhost:8080"
tenant = "acme"
keycloak_url = "http://localhost:8180"      # browser-facing, for `cufflink login`
keycloak_realm = "acme"
# keycloak_client_id = "cufflink-cli"
# api_key = "ck_..."                         # avoid committing this
# api_key_env = "CUFFLINK_STAGING_KEY"       # read the key from this variable instead

[environments.local.config]                  # pushed by `cufflink config apply`, `config sync` and `deploy`
# LOG_LEVEL = "debug"
[environments.local.secrets]
# STRIPE_KEY = "..."
```

### Credential precedence

For the selected environment, the API key is the first of (`cli/src/config.rs`):

1. `api_key` in `Cufflink.toml`
2. the variable named by `api_key_env`
3. `CUFFLINK_API_KEY`
4. stored credentials from `cufflink login` in `~/.config/cufflink/credentials.json` (keyed by `api_url` + tenant)

Stored credentials come **last**; an environment variable overrides them.

### Environment variables

| Variable | Used by | Description |
|----------|---------|-------------|
| `CUFFLINK_API_KEY` | all commands | Tenant API key (see precedence above) |
| `CUFFLINK_API_URL` | `cufflink tenants ...` only, when no `Cufflink.toml` is found | Platform URL (default `http://localhost:8080`); `--api-url` overrides it |
| `CUFFLINK_KEYCLOAK_URL` | `login`, `tenants` | Fallback when `Cufflink.toml` has no `keycloak_url` (then default `http://localhost:8180`) |
| `CUFFLINK_KEYCLOAK_REALM` | `login`, `tenants` | Fallback realm (default `cufflink`; `tenants` prefers what `/api/auth/config` reports) |
| `CUFFLINK_CLIENT_ID` | `login`, `tenants` | Keycloak client (default `cufflink-cli`) |
| `CUFFLINK_PLATFORM_API_KEY` | `tenants create-api-key\|list-api-keys\|revoke-api-key`, `deploy --tenant` | Platform admin key; when set, `--tenant` overrides send it with an `X-Tenant-Slug` header instead of the tenant key |

There is no `CUFFLINK_TENANT` variable; the tenant comes from `Cufflink.toml` (or `--tenant`).

### `cufflink login`

1. Loads the environment's `keycloak_url`, `keycloak_realm` and client ID (defaults above).
2. Requests a device code from `{keycloak_url}/realms/{realm}/protocol/openid-connect/auth/device` and prints the verification URL and user code. It does **not** open a browser.
3. Polls for the token until you complete the login (or the code expires).
4. POSTs `{access_token, tenant_slug}` to `{api_url}/api/auth/device/token`, which returns `{api_key, tenant_slug, username}`.
5. Saves the key to `~/.config/cufflink/credentials.json`.

If the device endpoint is unreachable, it prints `Keycloak device auth not available.` and prompts for an API key to paste instead (it verifies the key against `/api/auth/me` before saving).

---

## Authentication

### API keys

```bash
curl -H 'Authorization: ApiKey ck_...' http://localhost:8080/api/services
curl -H 'Authorization: ApiKey ck_...' http://localhost:8080/api/auth/me
```

Keys are scoped to a tenant. Create additional keys with an authenticated `POST /api/auth/api-keys`. Platform admin keys (`POST /api/platform/api-keys`, `cufflink tenants create-api-key`) manage tenants and can act on any tenant with an `X-Tenant-Slug` header.

Every key expires (`CUFFLINK_API_KEY_TTL_DAYS`, `CUFFLINK_ADMIN_API_KEY_TTL_DAYS`); an expired one answers `401 API key expired`. Replace a key before then with `cufflink auth keys rotate <id>` (`--admin` for a platform key): the old key keeps working for `--grace-hours` (default 24). `cufflink auth keys list` shows expiry and last use. Keys for external partners calling `api_key` routes are [partner keys](partner-keys.md).

### Keycloak JWTs

Bearer tokens from any registered tenant realm (or the platform realm) are accepted; the platform picks the realm from the token's `iss`, fetches that realm's JWKS from the tenant's `keycloak_url`, and requires the tenant's `deploy_role` (`cufflink-deploy`) for deploy/admin operations.

```bash
# Password grant is enabled on cufflink-cli in the dev realms only
TOKEN=$(curl -s -X POST http://localhost:8180/realms/acme/protocol/openid-connect/token \
  -d grant_type=password -d client_id=cufflink-cli -d username=dev -d password=dev | jq -r .access_token)

curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/services
```

Row-level behaviour (`auth_required`, `owner_field`, `permission_area`) is described in [authorization.md](authorization.md); the login flows in [authentication.md](authentication.md).

---

## Monitoring & Observability

### Prometheus metrics

```bash
curl http://localhost:8080/metrics
```

Includes `cufflink_services_total`, `cufflink_deployments_total`, `cufflink_crud_requests_total{tenant,service,operation,table}`, `cufflink_rate_limited_total`, `cufflink_webhooks_delivered_total`, `cufflink_webhooks_failed_total`, and `cufflink_route_posture_undeclared_total`. Compose scrapes them into Prometheus (http://localhost:9090) and Grafana (http://localhost:3001) has provisioned dashboards. See [observability.md](observability.md).

### Tracing

Set `OTEL_EXPORTER_OTLP_ENDPOINT` on the platform, worker, web-runner or ext-runner to export traces and logs over OTLP/HTTP. Compose points them at the OTel collector, which forwards traces to Tempo and logs to Loki, each under the tenant's service it belongs to (browse in Grafana; [observability.md](observability.md#the-contract)).

### Logs

Structured logs go to stdout; control verbosity with `RUST_LOG` (`info,cufflink_platform=trace`, etc.). With `LOKI_URL` set, `cufflink logs --follow` and `GET /api/services/{id}/logs` query Loki for a service's logs.

### NATS

http://localhost:8222 shows connections, JetStream streams and consumers.

---

## Production Deployment

### Docker images

```bash
docker build -f Dockerfile.platform   -t cufflink-platform .
docker build -f Dockerfile.worker     -t cufflink-worker .
docker build -f Dockerfile.web-runner -t cufflink-web-runner .
docker build -f Dockerfile.ext-runner -t cufflink-ext-runner .
```

(The `*.ci` variants are what `.github/workflows/ci.yml` builds.)

Every image runs its binary under `tini` as PID 1 (signals are forwarded and orphaned children reaped) as user `10001:10001`; the web-runner's `/apps`, and the ext-runner's `/apps` and `/tmp/cufflink-ext`, are owned by that user. Base images are tag-pinned in the Dockerfiles and Renovate pins them to digests on its next run. Set `stop_grace_period` (compose) or `terminationGracePeriodSeconds` (Kubernetes) above `SHUTDOWN_GRACE_SECS`, or the orchestrator kills the process mid-drain; the repository's compose sets `50s`.

**Upgrading an install whose web-runner ran as root.** A named or bind volume mounted at `/apps` keeps the ownership it was created with, and a volume first used by a root-run image is `root:0755`, which the non-root runner cannot write. The runner checks `APPS_DIR` at startup and exits with an error naming the fix rather than failing every deploy. Before the first start of this version, chown the volume once, for example:

```bash
docker run --rm -v <project>_webrunnerdata:/apps busybox chown -R 10001:10001 /apps
```

A fresh named volume, and Kubernetes `emptyDir`, need nothing.

### Checklist

1. **Database** — managed PostgreSQL with SSL, backups and enough connections for `MAX_TENANT_POOLS × MAX_CONNS_PER_TENANT` per replica.
2. **NATS** — a JetStream-enabled cluster with persistent storage. Each connection attempt (TCP, TLS and the NATS handshake) is bounded at 2 s per server and reconnects retry forever with backoff, so a slow TLS handshake costs a slower reconnect, never a dead client.
3. **Keycloak** — a production realm per tenant; disable the password grant on `cufflink-cli` (compose enables it for the bootstrap script only); set `KEYCLOAK_ISSUER` to the public URL if the platform reaches Keycloak internally.
4. **Object storage** — `STORAGE_BACKEND=s3` with any S3-compatible store, or `STORAGE_BACKEND=gcs` with Workload Identity. Required for WASM, web apps and backups.
5. **TLS** — terminate at a reverse proxy or load balancer; set `RATE_LIMIT_CLIENT_IP_SOURCE` to match it.
6. **Rate limiting** — off by default; enable with `RATE_LIMIT_ENABLED=true` and size `RATE_LIMIT_PER_SECOND` / `RATE_LIMIT_BURST`. Point `REDIS_URL` at a shared Redis so every replica sees the same counters.
7. **Route postures** — once every service declares postures, set `CUFFLINK_ENFORCE_ROUTE_POSTURE=true`.
8. **Secrets** — keep `DATABASE_URL`, `S3_SECRET_KEY`, ops-client secrets and API keys in a secrets manager; the platform and the worker can read each from a mounted file ([secrets as files](deployment.md#secrets-as-files)).
9. **Monitoring and probes** — point the platform's readiness probe at `/ready` (liveness stays on `/health`); expose the worker's `WORKER_HTTP_PORT` (8081) and point its readiness probe at `:8081/ready`; scrape `/metrics` on the platform and `:8081/metrics` on each worker ([observability.md](observability.md)); ship traces via OTLP; point `LOKI_URL` at your Loki.
10. **Workers** — run dedicated worker replicas for heavy WASM handlers and long-running jobs ([wasm-workers.md](wasm-workers.md), [scaling.md](scaling.md)).
11. **web-runner** — set `CUFFLINK_BASE_DOMAIN` to your public suffix with a wildcard DNS record and `CUFFLINK_PUBLIC_SCHEME=https`.
12. **Extensions** — off until `CUFFLINK_EXTENSIONS=true`; turning them on means running an ext-runner per tenant that deploys native services, each with that tenant's runner key, NATS user and cloud identity alone ([extensions](extensions.md#security)).
13. **More than one tenant** — set every isolation setting [tenant isolation](tenant-isolation.md) lists, and run a web runner and an ext-runner per tenant.

---

## API Reference

All paths are relative to the platform URL. `/health` and `/ready` are unauthenticated; everything under `/api` needs an `Authorization: ApiKey ...` or `Bearer ...` header; `/svc` routes follow each table's or route's declared posture.

### Health & metrics

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness; plain text `cufflink-platform ok` |
| GET | `/ready` | Readiness; JSON `{"state","db","nats","redis","modules","registry_version"}`. 200 only once migrations and the registry have loaded (`state: ready`) and the database answers; 503 while `starting`/`migrated`, when the database is unreachable, and for the rest of the process's life once a shutdown signal arrives (`shutting_down`) |
| GET | `/metrics` | Prometheus metrics |
| GET | `/api/version` | Platform version (unauthenticated; the CLI probes it) |

### Authentication

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/auth/config` | `{keycloak_url, realm, cli_client_id, web_client_id}` for CLI discovery |
| POST | `/api/auth/api-keys` | Create an API key for the caller's tenant (`expires_at` in the response) |
| GET | `/api/auth/api-keys` | List the caller's tenant's API keys (deploy authority) |
| DELETE | `/api/auth/api-keys/{id}` | Revoke one of them |
| POST | `/api/auth/api-keys/{id}/rotate?grace_hours=24` | Replace one of them; the old key works for the grace (0–168 h). Answers `{id, key, expires_at, replaces, replaced_key_expires_at}` |
| POST | `/api/auth/device/token` | Exchange `{access_token, tenant_slug}` for `{api_key, tenant_slug, username, expires_at}`; the key is revoked when the user logs out |
| GET | `/api/auth/me` | Authenticated principal |
| POST | `/api/auth/logout` | End the caller's Keycloak sessions (needs ops-client credentials) |

### Tenants & platform admin

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/tenants` | Create a tenant `{name, slug, keycloak_url, keycloak_realm, deploy_role, service_account_clients, keycloak_issuer, clone_rbac_from}`; slugs match `^[a-z0-9-]{1,40}$`; 409 when another tenant that is not a preview uses the same `keycloak_realm` with the same `deploy_role` |
| GET | `/api/tenants` | List tenants |
| PUT | `/api/tenants/{slug}` | Update a tenant (`service_account_clients: []` and `keycloak_issuer: ""` clear them); 409 on a realm and deploy role shared as on create |
| GET / PUT | `/api/tenants/{slug}/policies` | Read or set tenant policies (see [policies](policies.md)) |
| DELETE | `/api/tenants/{slug}/policies/{key}` | Return a policy to the platform default |
| GET | `/api/tenants/{slug}/extensions` | List the tenant's extensions: `{tenant, extensions: [{name, backend, service_id, callers, created_by, created_at}]}` |
| PUT | `/api/tenants/{slug}/extensions/{name}` | Register a NATS provider of `name`, or give it other callers: `{"callers": ["reports-service"]}` (service names or `*`). 403 while `CUFFLINK_EXTENSIONS=false`; 409 over a native service's extension, or while a deployed service calls it and would lose it, unless `?force=true` |
| DELETE | `/api/tenants/{slug}/extensions/{name}` | Remove a NATS provider, whatever `CUFFLINK_EXTENSIONS` is; 409 and `?force=true` as for `PUT` |
| DELETE | `/api/tenants/{slug}` | Delete a tenant and its data |
| POST | `/api/tenants/{slug}/rbac/sync` | Re-sync RBAC areas/roles for a tenant |
| POST | `/api/platform/api-keys` | Create a platform admin API key |
| GET | `/api/platform/api-keys` | List platform admin API keys |
| DELETE | `/api/platform/api-keys/{id}` | Revoke one |
| POST | `/api/platform/api-keys/{id}/rotate?grace_hours=24` | Replace one |
| POST / GET | `/api/tenants/{slug}/partner-keys` | Create (`201`, the key shown once) or list the tenant's [partner keys](partner-keys.md) |
| DELETE | `/api/tenants/{slug}/partner-keys/{id}` | Revoke one |
| POST / GET | `/api/tenants/{slug}/runner-keys` | Mint `{"name": "ext-runner", "runs": "native"}` (`runs`: `native` for the extension runner, `web` for a tenant-scoped web runner; `201`, the key shown once) or list the tenant's [runner keys](deployment.md#extension-runner), never their secrets; audited |
| DELETE | `/api/tenants/{slug}/runner-keys/{id}` | Revoke one; the runner API refuses it from its next request |
| GET | `/api/audit` | Query the audit log |
| GET | `/api/users/lookup` | Look up users across tenants |

### Runner API (runner keys only)

Every route answers only for the tenant the runner key (`Authorization:
RunnerKey <key>`) is bound to and for the kind of runner it was minted for
(`native` routes a `native` key, `web` routes a `web` key), 404 for
anything outside them (a route of the other kind, another tenant's
service, a native service on a web route or a web app on a native one, a
hash no deployment records), 401 for any other credential; each answer is
counted in `cufflink_runner_api_requests_total`.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/runner/native` | The key's tenant and its active native services at their current versions: `{tenant, services: [{service_id, service_name, version, sha256, size, extensions}]}` |
| GET | `/api/runner/native/{service_id}/{sha256}` | The binary of that hash (`application/octet-stream`), when a deployment of the tenant's native service records it |
| GET | `/api/runner/native/{service_id}/environment` | The native service's configs and secrets, opened: `{environment: {KEY: value}}` (`Cache-Control: no-store`) |
| GET | `/api/runner/web` | The key's tenant and its web apps with a bundle, at their current versions: `{tenant, apps: [{service_id, service_name, version, sha256, size}]}`, for a [tenant-scoped web runner](deployment.md#tenant-scoped-web-runner) |
| GET | `/api/runner/web/{service_id}/{sha256}` | The bundle of that hash (`application/gzip`), when a deployment of the tenant's web app records it |
| GET | `/api/runner/web/{service_id}/environment` | The web app's configs and secrets, opened: `{environment: {KEY: value}}` (`Cache-Control: no-store`) |

### Services & deployments

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/services` | List services |
| GET | `/api/services/{id}` | Service details |
| DELETE | `/api/services/{id}` | Delete a service (a native one whatever `CUFFLINK_EXTENSIONS` is); 409 while a deployed service calls one of its extensions unless `?force=true` |
| PATCH | `/api/services/{id}/status` | Enable/disable (`active` / `inactive`); a native one whatever `CUFFLINK_EXTENSIONS` is |
| GET | `/api/services/{id}/permission-areas` | Permission areas declared by the service |
| GET | `/api/services/{id}/openapi.json` | Generated OpenAPI spec |
| POST | `/api/services/deploy` | Deploy a manifest (WASM bundled as `wasm_base64`; a `mode: native` service's binary as `native_base64`, 403 while `CUFFLINK_EXTENSIONS=false`). Query: `plan=true` (say what it would do), `async=true` (answer 202 and run on; `GET /api/deploys/{id}` follows it), `inactive=true` ([start nothing](deployment.md#deploying-a-service-inactive); 409 for an active service, 422 for `mode: web`). The answer carries the service's `status` |
| GET | `/api/services/{id}/deployments` | Deployment history |
| POST | `/api/services/{id}/deployments/prune` | Delete old deployments |
| DELETE | `/api/services/{id}/deployments/{version}` | Delete one deployment |
| POST | `/api/services/{id}/rollback` | Roll back |
| POST | `/api/services/{id}/wasm` | Upload a WASM module |
| GET | `/api/services/{id}/wasm/status` | WASM runtime status |
| POST | `/api/services/{id}/web` | Upload a web app artifact |
| GET | `/api/services/{id}/web/status` | Web app status |

### Data, backup, logs

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/services/{id}/seed` | Seed data |
| GET | `/api/services/{id}/schema` | Tables, columns, FK dependencies |
| POST | `/api/services/{id}/backup/export` | Start an export job |
| POST | `/api/services/{id}/backup/restore` | Start a restore job |
| POST | `/api/services/{id}/backup/upload` | Get a presigned upload URL for a restore file |
| GET | `/api/services/{id}/backup/jobs` | List backup/restore jobs |
| GET | `/api/services/{id}/backup/jobs/{job_id}` | Job status |
| POST | `/api/services/{id}/backup/jobs/{job_id}/cancel` | Cancel a job |
| GET | `/api/services/{id}/backup/jobs/{job_id}/download` | Download an export |
| POST | `/api/services/{id}/query` | Read-only SQL (`cufflink sql`), bounded by `PLATFORM_SQL_*`; as the service's role under `db_roles: enforce` |
| GET | `/api/services/{id}/logs` | Query logs (Loki) |
| GET | `/api/services/{id}/logs/tail` | Tail logs |

### Config, webhooks, files

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/services/{id}/config` | List config keys |
| PUT | `/api/services/{id}/config` | Set a config value |
| DELETE | `/api/services/{id}/config/{key}` | Delete a config key |
| POST | `/api/services/{id}/config/apply` | Apply a set of config values in one transaction: `entries`, `prune` (remove what the set does not name), `dry_run` (answer the difference, write nothing). Answers the keys `added`, `changed`, `removed` and `unchanged` ([service config as code](deployment.md#service-config-as-code)) |
| POST | `/api/services/{id}/webhooks` | Register a webhook |
| GET | `/api/services/{id}/webhooks` | List webhooks |
| GET | `/api/services/{id}/webhooks/{webhook_id}/deliveries?limit=` | What became of a webhook's deliveries, the latest first (50, at most 500): `pending`, `delivered`, `failed` or `dropped`, the attempts made, what the receiver last answered. Recorded for tenants whose `outbox` is `enforce` |
| PUT | `/api/services/{id}/webhooks/{webhook_id}` | Update / enable / disable |
| DELETE | `/api/services/{id}/webhooks/{webhook_id}` | Delete |
| POST | `/api/services/{id}/files` | Upload a file (multipart) |
| GET | `/api/services/{id}/files` | List files |
| GET | `/api/services/{id}/files/{file_id}` | Download |
| DELETE | `/api/services/{id}/files/{file_id}` | Delete |

### RBAC

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/tenants/{tenant_id}/permission-areas` | All permission areas in the tenant |
| POST | `/api/tenants/{tenant_id}/roles` | Create a role |
| GET | `/api/tenants/{tenant_id}/roles` | List roles |
| PUT | `/api/tenants/{tenant_id}/roles/{role_id}` | Update a role |
| DELETE | `/api/tenants/{tenant_id}/roles/{role_id}` | Delete a role |
| POST | `/api/tenants/{tenant_id}/roles/assign` | Assign a role to a user |
| DELETE | `/api/tenants/{tenant_id}/roles/unassign` | Unassign |
| GET | `/api/tenants/{tenant_id}/users/search` | Search tenant users |
| GET | `/api/tenants/{tenant_id}/users/{user_id}/permissions` | Effective permissions |

### Service routes

| Method | Path | Description |
|--------|------|-------------|
| GET | `/svc/{tenant}/{service}/{table}` | List records |
| POST | `/svc/{tenant}/{service}/{table}` | Create |
| GET | `/svc/{tenant}/{service}/{table}/{id}` | Get |
| PUT | `/svc/{tenant}/{service}/{table}/{id}` | Update |
| DELETE | `/svc/{tenant}/{service}/{table}/{id}` | Delete (soft when `soft_delete`) |
| POST | `/svc/{tenant}/{service}/{table}/batch` | Batch create/update/delete |
| GET | `/svc/{tenant}/{service}/{table}/stream` | Server-sent events |
| GET/POST/PUT/DELETE/PATCH | `/svc/{tenant}/{service}/_fn/{handler}` | WASM custom route (4 MB body limit) |
| GET | `/svc/{tenant}/{service}/_meta/health` | Per-service health (unauthenticated) |
| GET | `/svc/{tenant}/{service}/_meta/openapi.json` | Per-service OpenAPI |
| GET | `/svc/{tenant}/{service}/_meta/docs` | Rendered API docs |
| POST | `/svc/{tenant}/{service}/graphql` | GraphQL (services declaring `graphql: true`) |
| GET | `/svc/{tenant}/{service}/graphql/schema` | GraphQL schema (authenticated) |
| GET | `/svc/{tenant}/_jobs/{job_id}` | Long-running job status |

### List query parameters

| Parameter | Example | Description |
|-----------|---------|-------------|
| `page` | `?page=2` | Page number (default 1) |
| `per_page` | `?per_page=25` | Page size (default 50, max 200) |
| `sort` | `?sort=-created_at,title` | Sort columns, `-` for DESC (default `-created_at`) |
| `search` | `?search=hello` | Case-insensitive substring (`ILIKE`) across text columns |
| `fts` | `?fts=search+terms` | PostgreSQL full-text search (`plainto_tsquery`) |
| `select` | `?select=id,name` | Return only these columns |
| `filter` | `?filter=[{"n":"status","f":"=","v":"active"}]` | JSON array of `{n, f, v}` objects (URL-encode it) |
| `<column>` | `?status=active` | Shorthand equality on any column |
| `cursor` | `?cursor=` | Cursor pagination; empty starts from the first row (primary key ascending) |
| `limit` | `?limit=50` | Page size in cursor mode (default 50, max 200) |
| `include_deleted` | `?include_deleted=true` | Include soft-deleted rows |
| `filter_mode` | `?filter_mode=v2` | Read `filter` as a typed tree (below); a tenant with `strict_filters=enforce` always does |
| `count` | `?count=none` | `exact` (default), `estimate` (`total_records_estimated: true`) or `none` (no totals; `has_more` instead) |
| `scope` | `?scope=own` | On a [scoped table](scoped-permissions.md): `own` narrows to the caller's rows, `all` insists on every row (403 without an `@all` grant) |

`filter_mode`, `count` and `scope` stay `?column=value` filters on a table with a column of that name (`scope` is read only on scoped tables).

Offset mode responds with `{"results": [...], "pagination": {current_page, per_page, total_pages, total_records}}`; cursor mode with `{"results": [...], "next_cursor": "<base64>", "has_more": bool}`.

### Filter operators

`f` is matched case-insensitively:

| `f` | SQL | Example |
|-----|-----|---------|
| `=` or `eq` | `=` | `{"n":"status","f":"=","v":"active"}` |
| `!=` or `neq` | `!=` | `{"n":"status","f":"!=","v":"deleted"}` |
| `<`, `<=`, `>`, `>=` (or `lt`, `lte`, `gt`, `gte`) | same | `{"n":"age","f":">=","v":"18"}` |
| `LIKE` | `LIKE` (case-sensitive) | `{"n":"name","f":"LIKE","v":"%Smith%"}` |
| `ILIKE` | `ILIKE` | `{"n":"name","f":"ILIKE","v":"%smith%"}` |
| `IN` | `IN (...)` | `{"n":"status","f":"IN","v":"active,pending"}` |
| `IS NULL` / `IS NOT NULL` | same | `{"n":"deleted_at","f":"IS NULL"}` |

Conditions on the same column are combined with OR; conditions on different columns with AND. A range on a single column (`>=` and `<=` together) therefore cannot be expressed with `filter`. Unknown operators fall back to `=`. The array-of-arrays form (`[["status","eq","active"]]`) is not supported.

### Filter v2

With `?filter_mode=v2`, `filter` is a JSON boolean tree over typed conditions, and every mistake is a `400 {"error": "filter: …"}` instead of being ignored:

```json
{"and": [{"f": "created_at", "op": "gte", "v": "2026-01-01T00:00:00Z"},
         {"f": "created_at", "op": "lt",  "v": "2026-02-01T00:00:00Z"},
         {"or": [{"f": "status", "op": "in", "v": ["new", "open"]}, {"f": "owner_id", "op": "is_null"}]},
         {"not": {"f": "archived", "op": "eq", "v": true}}]}
```

- Operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `like`, `ilike` (text columns), `in`, `not_in` (arrays of at most 100), `is_null`, `is_not_null`, `contains` (`@>`, JSON columns).
- Values must be the column's type (a UUID, an integer in range, an RFC 3339 timestamp, a `YYYY-MM-DD` date…); nothing is coerced to a default.
- A top-level array is the `and` of its elements; v1 `{n, f, v, c}` objects inside it are accepted, a `c: "OR"` joining the element before it.
- At most 8 levels and 64 conditions; unknown fields, operators and keys are refused.
- `?column=value` shorthands are typed too. `per_page` must be 1–500 (not clamped) and a page past the last is `404`. Without `created_at` the default sort is the primary key.

---

## Troubleshooting

**`No Cufflink.toml found. Run from a cufflink project directory or use `cufflink init` to create one.`**
Every command except `cufflink tenants ...` needs a project. `cd` into one (the examples ship a `Cufflink.toml`) or run `cufflink init <name>`.

**`Environment 'staging' not found in Cufflink.toml. Available: ["local"]`**
`--env` (or `[service].default_env`) names an environment that has no `[environments.<name>]` block.

**`Error: No credentials found for tenant 'acme'.`**
No key resolved for this environment. Run `cufflink login` in the project, or set `CUFFLINK_API_KEY`. Stored credentials are keyed by `api_url` and tenant, so changing either in `Cufflink.toml` requires a fresh login.

**`cufflink login` prints `Keycloak device auth not available.`**
The CLI could not reach `{keycloak_url}/realms/{realm}/protocol/openid-connect/auth/device`. Check `keycloak_url` / `keycloak_realm` in `Cufflink.toml` (browser-facing address, e.g. `http://localhost:8180`, realm `acme`), and that the realm's `cufflink-cli` client has the device grant enabled. You can paste an API key at the prompt instead.

**`cufflink tenants create` fails with `Failed to start device auth (is the platform realm 'cufflink' configured?)`**
Tenant commands authenticate against the platform realm reported by `/api/auth/config`. Make sure the platform is reachable (`CUFFLINK_API_URL` / `--api-url` outside a project) and that the `cufflink` realm exists in Keycloak.

**`401 {"error":"Authentication required for this resource"}` on a `/svc` route**
The table is `auth_required` (or has `owner_field` / `owner_self_service`). Send `Authorization: ApiKey ck_...`.

**`422 {"error":"Missing required field: author_name"}`**
A non-`Option` column was omitted from the body.

**`500 {"error":"Internal database error"}` on create**
Most often a foreign key pointing at a row that does not exist; the platform currently surfaces the constraint violation as a 500.

**Platform logs `Custom route has no declared posture — running in legacy (unenforced) mode.`**
Add `public("reason")`, `auth`, `perm("area","op")` or `owner("field")` to the route in `service!`. With `CUFFLINK_ENFORCE_ROUTE_POSTURE=true` the deploy is rejected instead.

**`401` with `Cannot decode JWT issuer` or `JWT issuer is not a Keycloak realm`**
The token's `iss` does not match a URL the platform knows. When Keycloak is reached internally (`KEYCLOAK_URL=http://keycloak:8080`) but tokens are minted against the public address, set `KEYCLOAK_ISSUER` to the public base URL and give tenants a `keycloak_url` the platform can reach.

**`tenant-init` exits with `could not obtain platform admin token` or `platform did not become healthy`**
Keycloak or the platform took longer than the script's 4-minute wait (common on first pull under emulation). Re-run it: `docker compose up -d tenant-init`. `Tenant 'acme' already exists` on a later run is normal.

**web-runner exits immediately with `Missing required environment variable: CUFFLINK_BASE_DOMAIN`**
Set `CUFFLINK_BASE_DOMAIN` (compose uses `localhost`) and, for a local stack, `CUFFLINK_PUBLIC_SCHEME=http`.

**`Missing required environment variable: S3_ENDPOINT` or `Unknown STORAGE_BACKEND 'x' (expected 's3' or 'gcs')`**
Set `STORAGE_BACKEND` to `s3` or `gcs` and the matching variables. web-runner requires storage and fails on any missing `S3_*` variable. The platform and worker treat it as optional: with `STORAGE_BACKEND` unset or `s3`, storage is only enabled when all five `S3_*` variables are present, and a partial set silently disables it (WASM upload, web apps and backups then fail at use time) — check the startup log for the storage backend line.

**`Service 'todo-service' not found on platform`**
The service has not been deployed to this environment's tenant yet, or `--env` points elsewhere. Run `cufflink deploy` first.

**Everything is slow on Apple Silicon**
The published images are amd64 and run under emulation. Build native images with the Dockerfiles (drop the `platform: linux/amd64` lines) or run the platform natively as described in [Local Development Setup](#local-development-setup).
