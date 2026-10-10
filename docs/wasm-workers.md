# WASM Worker Pool

The worker is a separate process that executes WASM handlers outside the platform: every long-running job, every service backup, and — when you opt in with `WORKER_ROUTES` — selected synchronous handlers and after-hooks. Workers scale independently of the platform, so heavy work never competes with lightweight CRUD requests for the platform's CPU.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [How It Works](#how-it-works)
- [Configuration](#configuration)
- [Running Workers](#running-workers)
- [Docker Compose](#docker-compose)
- [Docker Swarm Scaling](#docker-swarm-scaling)
- [NATS Subject Design](#nats-subject-design)
- [JetStream Streams and Retry](#jetstream-streams-and-retry)
- [Worker Environment Variables](#worker-environment-variables)
- [Examples](#examples)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

---

## Overview

The platform executes custom routes, lifecycle hooks and subscription handlers **in-process** by default, via `spawn_blocking` on the same wasmtime engine. That is the fastest path — zero serialization, zero network hops — and it is bounded by the 30-second handler budget.

The worker exists for work that must not run there:

| Work | Where it runs | Opt-in? |
|------|---------------|---------|
| `job(...)` routes and `jobs::enqueue` (long-running jobs) | **Always on a worker** | No — that is what `job(...)` means |
| Service backups (export jobs) | **Always on a worker** | No |
| Custom routes and before-hooks listed in `WORKER_ROUTES` | Worker, via NATS request/reply | Yes, per handler |
| After-hooks listed in `WORKER_ROUTES` | Worker, via JetStream with retry | Yes, per handler |
| Everything else | Platform, in-process | — |

So a deployment that uses `job(...)` routes or `cufflink backup` needs at least one worker running even if `WORKER_ROUTES` is never set.

**Key design principles:**

- **Nothing moves without a reason** — synchronous handlers stay in-process unless you list them in `WORKER_ROUTES`
- **No SDK changes for routing** — `WORKER_ROUTES` is platform-side configuration; a handler's code is identical wherever it runs
- **NATS queue groups** — automatic load balancing across worker replicas for request/reply work
- **JetStream for asynchronous work** — after-hooks, long-running jobs and backups are persisted and redelivered on failure

---

## Architecture

```
                          Platform (Axum)
                         /       |        \
        job(...) route   |   WORKER_ROUTES?  |   backup requested
              ↓          |    NO ↓    YES ↓  |          ↓
   CUFFLINK_LONG_JOBS    | in-process  NATS  |   CUFFLINK_BACKUP
   (JetStream)           |  (default)  req/  |   (JetStream)
                         |            reply  |
                         |         or JetStream (after-hooks,
                         |            CUFFLINK_WASM_JOBS)
                          \       |        /
                           NATS queue group / durable consumers
                              /      |      \
                         Worker1  Worker2  Worker3
```

Each worker process runs:

- **WasmEngine** — the same wasmtime runtime as the platform, with module caching; modules are loaded from object storage on a cache miss
- **TenantPoolManager** — PostgreSQL connection pools per tenant (smaller than the platform's)
- **Platform DB pool** — for `config::get`, job rows and the service registry
- **All host functions** — `db_*`, `nats_publish`/`nats_request`, `http_fetch*`, `get_config`, `s3_*`, `image_transform_*`, `jobs_enqueue`, etc.
- **Four consumers**: a core NATS queue subscription on `SUBSCRIBE_PATTERN` (request/reply), and durable JetStream consumers on `CUFFLINK_WASM_JOBS`, `CUFFLINK_LONG_JOBS` and `CUFFLINK_BACKUP`

The worker is *not* an identical copy of the platform. Handler authors should know:

- **Redis only with `REDIS_URL`.** With `REDIS_URL` set the worker connects in the background and `redis::*` host functions work as on the platform. Without it the handle is disabled: `redis::get` returns `None`, `redis::set`/`del` return `false`, `redis::get_with_status` and `redis::mget` return `Err`. Treat Redis as a cache, never as the source of truth for a job.
- **No Keycloak.** The worker never validates tokens. The platform authenticates and authorizes the request *before* dispatch and carries the resolved principal inside the job, so `req.auth()` works as usual.
- **Probes instead of an API.** The worker's only listener is `WORKER_HTTP_PORT` (8081): `/health` (liveness, `{"status":"ok","version"}`), `/ready` (200 once the tenant catalog has loaded, all three JetStream consumers are up and the platform database answers `SELECT 1` within 2 s; 503 with `state` `starting` or `shutting_down` otherwise) and `/metrics` (Prometheus). With `OTEL_EXPORTER_OTLP_ENDPOINT` set it exports traces and logs over OTLP; request/reply jobs and after-hooks are parented on the dispatching request's trace, long jobs start their own.

---

## How It Works

### Custom routes (`/_fn/handler`) and before-hooks

1. The request arrives at the platform, which authenticates it and enforces the route's posture
2. The platform checks `is_worker_routed(service_name, handler_name)` against `WORKER_ROUTES`
3. **Not worker-routed**: the handler executes in-process via `spawn_blocking` (default)
4. **Worker-routed**: the platform serializes a `WasmJob` (service id + version, handler name, the full request JSON, tenant, service schema and the service's `publishes` allowlist) and publishes it to `cufflink.wasm.{service}.{handler}` as a NATS request
5. A worker in the queue group picks it up, executes the handler with a 30 s budget, and replies with `{ok, response, error}`
6. The platform waits up to 30 s for the reply and returns the handler's response to the client

Before-hooks (`before_create`, `before_update`, `before_delete`) use the same request/reply path: the platform waits for the worker before touching the database.

### After-hooks (asynchronous)

After-hooks (`after_create`, `after_update`, `after_delete`) that are worker-routed use **JetStream**:

1. The platform publishes the job to `cufflink.wasm.async.{service}.{handler}`
2. The message is persisted in the `CUFFLINK_WASM_JOBS` stream
3. A worker consumes it from the durable pull consumer `wasm-worker`
4. If the handler fails, the message is handed back and NATS redelivers it after a growing delay (5 s, 10 s), up to 3 deliveries in total; a hook out of deliveries, or one that names no deployment, is kept in the dead-letter queue (`CUFFLINK_DLQ`)
5. The platform never waits — the HTTP response was already sent

After-hooks run one at a time, in the order they were published, unless `WORKER_AFTER_HOOK_CONCURRENCY` is raised.

On shutdown the hook in hand finishes and is acked, so it never runs twice; a hook not yet fetched stays in its stream for another worker, and one the shutdown's hard deadline stops is handed back for immediate redelivery.

### Long-running jobs (`job(...)` routes, `jobs::enqueue`)

The job's row, not its message, is the job.

1. The platform stores a job row (request snapshot without credential headers, auth snapshot, `timeout_secs`, `max_attempts`, `retry_on`, `backoff`, the trace it was enqueued under) and publishes `{"job_id": ...}` to `cufflink.jobs.{tenant}.{service}.{handler}` on the `CUFFLINK_LONG_JOBS` stream, then answers `202 Accepted {"job_id", "status": "pending"}`. A job recorded but not published (NATS unavailable) is still accepted; the reclaimer publishes it. A tenant with 1000 jobs pending or running is refused with `429`
2. A worker claims the row under a lease of `JOB_LEASE_SECS` (60 s), extends it every third of that while the handler runs, and keeps the message alive with heartbeats. It runs the handler with the **job's own timeout** (default 600 s) and `req.job()` set; every write of an attempt is fenced, so an attempt that lost its lease cannot overwrite its successor's
3. A crash (runtime error, lost worker) is attempted again after the job's backoff (30 s doubling to an hour) while attempts remain. A handler error or a timeout fails the job, and a 5xx response completes it, unless the job declares `retry_on`. A handler with `delivery-control` may answer `Response::retry_after` or `Response::terminal` ([jobs v2](wasm-guide.md#jobs-v2-retries-and-dead-letters)). A job out of attempts is failed and dead-lettered to `CUFFLINK_DLQ`
4. A worker that dies or loses its lease gives the job up; every worker's reclaimer (every `JOB_RECLAIM_INTERVAL_SECS`) publishes due jobs nobody holds, including those whose lease ran out and those published more than `JOB_REPUBLISH_AFTER_SECS` ago that no worker took, and fails those out of attempts
5. A worker that is shut down stops fetching and lets running jobs continue until the hard deadline (`SHUTDOWN_GRACE_SECS` minus 2 s); a job still running then is stopped and handed back for immediate redelivery

A failed job runs again with `POST /svc/{tenant}/_jobs/{id}/retry` or `cufflink jobs retry <id>` (a job that has not failed is a 409). Ended jobs are kept for a day, then deleted by the reclaimer.

### Backups

Backup exports requested through the platform API are published to `cufflink.backup.>` on the `CUFFLINK_BACKUP` stream and processed by the worker's `backup-worker` consumer.

---

## Configuration

### WORKER_ROUTES environment variable

Set on the **platform** to specify which synchronous handlers and after-hooks should be dispatched to workers:

```bash
WORKER_ROUTES='{"service-name": ["handler1", "handler2"]}'
```

The value is a JSON object mapping service names to arrays of handler names. `job(...)` routes are not listed here — they always run on a worker.

### Example

Given an analytics service with handlers `get_stats` (fast) and `generate_report` (slow but under 30 s):

```bash
# Only generate_report goes to workers; get_stats stays in-process
WORKER_ROUTES='{"analytics-service": ["generate_report"]}'
```

### Multiple services

```bash
WORKER_ROUTES='{
  "analytics-service": ["generate_report", "export_csv"],
  "invoice-service": ["process_refund", "reconcile"],
  "notification-service": ["send_bulk_email"]
}'
```

### No configuration (default)

When `WORKER_ROUTES` is unset or empty, every custom route, hook and subscription executes in-process. A value that is not valid JSON stops the platform (and the worker) at startup with an error naming `WORKER_ROUTES`.

---

## Running Workers

### Build the worker

```bash
# From the cufflink root directory
cargo build --release -p cufflink-worker
```

### Run locally

The worker loads the same `PlatformConfig` as the platform, so it reads the same variables with the same defaults, except for its smaller tenant pools. Against the bundled `docker compose` stack (RustFS on port 9000):

```bash
export DATABASE_URL=postgres://cufflink:cufflink@localhost:5432/cufflink
export NATS_URL=nats://localhost:4222
export STORAGE_BACKEND=s3
export S3_ENDPOINT=http://localhost:9000
export S3_REGION=us-east-1
export S3_ACCESS_KEY=rustfsadmin
export S3_SECRET_KEY=rustfsadmin
export S3_BUCKET=cufflink

./target/release/cufflink-worker
```

Object storage is where WASM modules live; a worker without it starts, logs `Object storage not configured`, and then fails every job with `No WASM module found`.

### Build the Docker image

```bash
docker build -f Dockerfile.worker -t cufflink-worker .
```

Published image: `kingsleyh/cufflink-worker:latest` (linux/amd64).

---

## Docker Compose

The repository's `docker-compose.yml` includes a `wasm-worker` service:

```yaml
wasm-worker:
  image: kingsleyh/cufflink-worker:latest
  stop_grace_period: 50s
  # Images are published for linux/amd64 only; Docker Desktop on Apple
  # Silicon runs them under emulation. Remove if you build your own arm64 image.
  platform: linux/amd64
  environment:
    DATABASE_URL: postgres://cufflink:cufflink@postgres:5432/cufflink
    NATS_URL: nats://nats:4222
    S3_ENDPOINT: http://rustfs:9000
    S3_REGION: us-east-1
    S3_ACCESS_KEY: rustfsadmin
    S3_SECRET_KEY: rustfsadmin
    S3_BUCKET: cufflink
    RUST_LOG: info,cufflink_worker=debug
    OTEL_EXPORTER_OTLP_ENDPOINT: http://otel-collector:4318
    # Smaller pools for worker processes
    MAX_TENANT_POOLS: "32"
    MAX_CONNS_PER_TENANT: "2"
    # NATS subscription config (defaults shown)
    # SUBSCRIBE_PATTERN: "cufflink.wasm.>"
    # QUEUE_GROUP: "wasm-workers"
  depends_on:
    postgres:
      condition: service_healthy
    nats:
      condition: service_healthy
    rustfs:
      condition: service_started
    platform:
      condition: service_started
```

To route synchronous handlers or after-hooks to it, add `WORKER_ROUTES` to the **platform** service:

```yaml
platform:
  environment:
    # ... existing config ...
    WORKER_ROUTES: '{"analytics-service": ["generate_report"]}'
```

---

## Docker Swarm Scaling

Scale workers independently of the platform:

```bash
# Scale platform replicas
docker service scale cufflink_platform=3

# Scale workers independently
docker service scale cufflink_wasm-worker=5
```

NATS queue groups balance request/reply work automatically — each message is delivered to exactly one worker in the group — and the JetStream consumers are durable and shared, so every replica pulls from the same queues.

### Service-specific workers

For services with very different resource needs, deploy dedicated worker pools. Narrow both the request/reply pattern and the long-jobs pattern:

```yaml
# General-purpose workers (handle everything)
wasm-worker:
  image: kingsleyh/cufflink-worker:latest
  deploy:
    replicas: 2
  environment:
    SUBSCRIBE_PATTERN: "cufflink.wasm.>"
    QUEUE_GROUP: "wasm-workers"
    LONG_JOBS_SUBSCRIBE_PATTERN: "cufflink.jobs.>"
    # ... other config ...

# Dedicated analytics workers (3 replicas, more resources)
analytics-worker:
  image: kingsleyh/cufflink-worker:latest
  deploy:
    replicas: 3
    resources:
      limits:
        cpus: "2"
        memory: 2G
  environment:
    SUBSCRIBE_PATTERN: "cufflink.wasm.analytics-service.>"
    QUEUE_GROUP: "wasm-analytics"
    LONG_JOBS_SUBSCRIBE_PATTERN: "cufflink.jobs.acme.analytics-service.>"
    # ... other config ...
```

Each distinct `LONG_JOBS_SUBSCRIBE_PATTERN` gets its own durable consumer (`cufflink-long-jobs-<pattern>`), because JetStream rejects one consumer with conflicting filter subjects. Note that every worker also runs the after-hook and backup consumers regardless of pattern.

---

## NATS Subject Design

### Synchronous handlers (custom routes, before-hooks)

```
cufflink.wasm.{service_name}.{handler_name}
```

Examples:
- `cufflink.wasm.analytics-service.generate_report`
- `cufflink.wasm.invoice-service.process_refund`

### Asynchronous after-hooks

```
cufflink.wasm.async.{service_name}.{handler_name}
```

Examples:
- `cufflink.wasm.async.order-service.after_create_order`
- `cufflink.wasm.async.notification-service.send_email`

### Long-running jobs

```
cufflink.jobs.{tenant}.{service_name}.{handler_name}
```

Example: `cufflink.jobs.acme.stock-service.handle_run_report`

### Backups

```
cufflink.backup.>
```

### Worker subscription patterns

| Variable | Pattern | Matches |
|----------|---------|---------|
| `SUBSCRIBE_PATTERN` | `cufflink.wasm.>` (default) | All request/reply subjects. `>` also matches the `cufflink.wasm.async.*` subjects, which the worker skips on this path — they belong to the JetStream consumer |
| `SUBSCRIBE_PATTERN` | `cufflink.wasm.*.*` | Exactly the request/reply subjects (`service.handler`) |
| `SUBSCRIBE_PATTERN` | `cufflink.wasm.analytics-service.>` | One service's request/reply subjects |
| `LONG_JOBS_SUBSCRIBE_PATTERN` | `cufflink.jobs.>` (default) | Every tenant's jobs |
| `LONG_JOBS_SUBSCRIBE_PATTERN` | `cufflink.jobs.acme.stock-service.>` | One service's jobs |

Because a JetStream publish is an ordinary NATS message on its subject, the default `cufflink.wasm.>` core subscription also receives every `cufflink.wasm.async.*` after-hook job. The worker recognises those subjects and leaves them to the durable `CUFFLINK_WASM_JOBS` consumer, so each async job executes exactly once.

---

## JetStream Streams and Retry

| Stream | Subjects | Consumer | Retention | Max age | Ack wait | Max deliveries |
|--------|----------|----------|-----------|---------|----------|----------------|
| `CUFFLINK_WASM_JOBS` (after-hooks) | `cufflink.wasm.async.>` | `wasm-worker` (durable pull, `WORKER_AFTER_HOOK_CONCURRENCY` in flight) | WorkQueue | 1 hour | 60 s, restarted every 20 s while the hook runs | 3 |
| `CUFFLINK_LONG_JOBS` (jobs) | `cufflink.jobs.>` | `cufflink-long-jobs` (durable pull, `MAX_CONCURRENT_JOBS` in flight) | created by the platform | — | `LONG_JOBS_ACK_WAIT_SECS` (60 s, kept alive by heartbeats) | attempts tracked per job in the database (`max_attempts`); a failed job is dead-lettered |
| `CUFFLINK_BACKUP` (backups) | `cufflink.backup.>` | `backup-worker` (durable pull) | WorkQueue | 24 hours | 300 s | 3 |

The worker creates `CUFFLINK_WASM_JOBS` and `CUFFLINK_BACKUP` itself if they are missing. `CUFFLINK_LONG_JOBS` is created by the platform; a worker that starts before the platform has created it logs `Long-jobs stream 'CUFFLINK_LONG_JOBS' not found` and its supervisor retries with backoff.

**An after-hook out of deliveries is dead-lettered.** It is published to `cufflink.dlq.{tenant}.{service}.hook.{handler}` on `CUFFLINK_DLQ` with why (`Cufflink-Dlq-Reason`) and where it came from, and kept for `CUFFLINK_DLQ_MAX_AGE_DAYS`. The tenant and the service are those of the deployment the platform recorded; a job that claims another identity is filed under the tenant `_`. The worker creates `CUFFLINK_DLQ` in the background and runs after-hooks whether or not it exists: while it cannot store a dead letter, the hook is handed back instead of ended (`cufflink_dlq_publish_failed_total`). A backup is acknowledged however it ends, and its job row records a failure. Long-running jobs record every attempt and the final failure message in the job row.

Every consumer is supervised: if it exits or errors (NATS unreachable at startup, stream or consumer deleted after NATS lost state) the worker rebuilds it after a backoff, logging `<name> exited; restarting` or `<name> error: ...; restarting`.

---

## Worker Environment Variables

The worker loads the same `PlatformConfig` as the platform, so every platform variable is read with the platform's defaults (tenant pools excepted); the ones that matter on a worker are listed first.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgres://cufflink:cufflink@localhost:5432/cufflink` | PostgreSQL connection string (same database as the platform). Until it answers and the platform has migrated it, the worker retries with backoff, stays unready and takes no jobs |
| `NATS_URL` | `nats://localhost:4222` | NATS server URL. Connects (and reconnects) in the background, so a NATS outage never stops the worker from starting |
| `STORAGE_BACKEND` | `s3` | `s3` (AWS S3 / RustFS / Hetzner / any S3-compatible API) or `gcs`; `memory` and `local` are for tests and refused when `RUN_ENV=production`. Anything else is a startup error |
| `S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET` | *(unset)* | S3 configuration. All five must be set or the worker runs with **no object storage** and cannot load modules |
| `GCS_BUCKET` | *(unset)* | Bucket for the `gcs` backend (credentials via Application Default Credentials / Workload Identity) |
| `SUBSCRIBE_PATTERN` | `cufflink.wasm.>` | Core NATS pattern for request/reply jobs |
| `QUEUE_GROUP` | `wasm-workers` | NATS queue group for request/reply jobs |
| `LONG_JOBS_SUBSCRIBE_PATTERN` | `cufflink.jobs.>` | Filter subject for the long-jobs consumer |
| `MAX_CONCURRENT_JOBS` | `4` | Long-running jobs executing at once per worker |
| `LONG_JOBS_ACK_WAIT_SECS` | `60` | Ack wait of the long-jobs consumer; a running job's message is kept alive every third of it, so it need not exceed any job's timeout |
| `JOB_LEASE_SECS` | `60` | How long a job is its worker's without a heartbeat (at least 3) |
| `JOB_NAK_MAX_DELAY_SECS` | `60` | Longest a retried job's message is handed back for; a later retry is published by the reclaimer when due |
| `JOB_RECLAIM_INTERVAL_SECS` | `30` | How often the reclaimer runs |
| `JOB_REPUBLISH_AFTER_SECS` | `300` | How long a published job may wait for a worker before it is published again |
| `MAX_TENANT_POOLS` | `32` | Maximum number of tenant database pools; `0` is refused |
| `MAX_CONNS_PER_TENANT` | `2` | Max PostgreSQL connections per tenant pool; `0` is refused |
| `WORKER_HTTP_PORT` | `8081` | Probe listener (`/health`, `/ready`, `/metrics`); `0` disables it |
| `WORKER_MAX_CONCURRENT_SYNC` | `32` | Request/reply handlers executed at once; further requests wait in NATS. `0` is refused |
| `WORKER_AFTER_HOOK_CONCURRENCY` | `1` | After-hooks executed at once. `1` runs them one at a time, in the order they were published; a higher number gives the order up for throughput. A failed one is retried up to three deliveries, then dead-lettered. `0` is refused |
| `REDIS_URL` | *(unset)* | Redis for the `redis::*` host functions; disabled when unset |
| `SHUTDOWN_GRACE_SECS` | `45` | Grace from SIGTERM to exit (see [Shutdown](#shutdown)) |
| `RUST_LOG` | `info,cufflink_worker=debug` | Log level filter |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(unset)* | OTLP/HTTP trace and log export; JSON logs on stdout without it |
| `HOST_OUTBOUND_HTTP_TIMEOUT_SECS` / `HOST_OUTBOUND_CONNECT_TIMEOUT_SECS` | `28` / `20` | Default budgets for guest `http::*` calls (always clamped to the handler's remaining time) |
| `HOST_FANOUT_CONCURRENCY` | `32` | Concurrency bound for `http::fetch_many` and `storage::download_many` |
| `DB_READ_ONLY_ROLE` / `DB_READ_ONLY_TIMEOUT_SECS` / `DB_READ_ONLY_MAX_ROWS` | `reports_ro` / `30` / `10000` | Guards for `db::query_read_only` |
| `KEYCLOAK_URL`, `KEYCLOAK_REALM`, `KEYCLOAK_ISSUER` | platform defaults | Read by the shared loader but **unused** — the worker never talks to Keycloak |
| `WORKER_ROUTES` | *(unset)* | Read by the shared loader but only meaningful on the platform |
| `PLATFORM_PORT` | `8080` | Parsed by the shared loader; must be numeric if set, although the worker opens no port |

Workers use smaller database pools than the platform (32 pools of 2 connections vs 64 of 4 by default) since they only need connections for host function calls.

### Shutdown

On SIGTERM or SIGINT the worker:

1. reports `/ready` 503 `shutting_down` (`/health` stays 200) and unsubscribes from the request/reply queue, so queue-group siblings take new requests;
2. stops fetching on every JetStream consumer; the message each after-hook or backup consumer is handling finishes and is acked, and the rest of its batch is naked for immediate redelivery;
3. lets request/reply replies, after-hooks and long jobs run on, bounded by their own budgets and by the hard deadline at `SHUTDOWN_GRACE_SECS` minus 2 s, where anything still running is stopped and handed back;
4. closes the probe listener, drains NATS and flushes telemetry (each capped at 5 s), and exits 0 — or 1 when tasks overran the grace.

Give the orchestrator more than `SHUTDOWN_GRACE_SECS` (`stop_grace_period: 50s` in compose, `terminationGracePeriodSeconds` in Kubernetes).

---

## Examples

### Example 1: A report that needs minutes

Your analytics service has a `generate_report` handler that scans millions of rows and calls an external renderer. It cannot fit in 30 s, so make it a job:

```rust
custom_routes: [
    ("GET",  "/stats",  "get_stats",       perm("analytics", "view")),
    ("POST", "/report", "generate_report", perm("analytics", "view"), job(timeout = 900, max_attempts = 2)),
],
```

**Result:**
- `GET /svc/acme/analytics-service/_fn/get_stats` — executes in-process on the platform
- `POST /svc/acme/analytics-service/_fn/generate_report` — returns `202 {"job_id": "...", "status": "pending"}` immediately; a worker's long-jobs consumer runs the handler with a 900 s budget, retrying once on failure. No `WORKER_ROUTES` entry is needed.

### Example 2: Offload a slow-but-synchronous handler

`export_csv` takes 2–10 seconds and the client wants the bytes back in the same request. Keep it synchronous but move the CPU off the platform:

```bash
WORKER_ROUTES='{"analytics-service": ["export_csv"]}'
```

The platform dispatches the request over NATS and waits for the reply; the 30 s budget still applies end to end.

### Example 3: Worker-routed CRUD hooks

Your order service has hooks:
- `before_create` calls `validate_order` — fast validation, keep in-process
- `after_create` calls `send_confirmation` — sends emails, offload to a worker with retry

```bash
WORKER_ROUTES='{"order-service": ["send_confirmation"]}'
```

`validate_order` runs in-process (not listed). `send_confirmation` is published to `CUFFLINK_WASM_JOBS` and executed by a worker, redelivered up to 3 times if it fails. Remember that a handler on the worker sees Redis as disabled unless the worker has `REDIS_URL`.

### Example 4: Multiple platform replicas with shared workers

```
                    Load Balancer
                   /      |      \
            Platform  Platform  Platform
            (replica) (replica) (replica)
                   \      |      /
                    NATS Cluster
                   /      |      \
              Worker  Worker  Worker
```

All three platform replicas share the same `WORKER_ROUTES` config. Any replica can dispatch to any worker, because:

- The platform is stateless — the job carries everything the worker needs (request, principal, service version, publish policy)
- NATS queue groups deliver each request/reply message to exactly one worker; JetStream consumers are durable and shared
- Workers load WASM modules from the same object storage the platform deployed them to

---

## Monitoring

### Worker logs

A healthy worker starts up like this (`RUST_LOG=info,cufflink_worker=debug`):

```
INFO  Starting Cufflink WASM Worker
INFO  NATS client started for nats://nats:4222 (connects in the background)
INFO  Object storage configured (backend: s3, bucket: cufflink)
INFO  WASM runtime initialized
INFO  Tenant catalog loaded
INFO  Subscribing to 'cufflink.wasm.>' (queue group: 'wasm-workers')
INFO  Cufflink WASM Worker ready, waiting for jobs...
INFO  Backup consumer ready
INFO  JetStream consumer 'wasm-worker' ready for async jobs
INFO  Long-jobs consumer 'cufflink-long-jobs' subscribed to 'cufflink.jobs.>' (concurrency=4, ack_wait=60s)
```

While processing:

```
DEBUG Processing WASM job: handler='generate_report' service_id=1c2d... v3
INFO  Executing long-running job tenant=acme service=analytics-service handler=generate_report job_id=7e4f... attempt=1
WARN  Async WASM job failed: WASM execution error: ...
WARN  JetStream consumer exited; restarting
```

Guest `log::*` output appears with the `[wasm]` prefix, exactly as on the platform.

### Platform dispatch logs

At debug level the platform logs each hand-off:

```
DEBUG Dispatching WASM job to worker: cufflink.wasm.analytics-service.export_csv (service=analytics-service, handler=export_csv)
DEBUG Publishing async WASM job: cufflink.wasm.async.order-service.send_confirmation (service=order-service, handler=send_confirmation)
INFO  Job enqueued tenant=acme service=analytics-service handler=generate_report job_id=7e4f...
```

### Metrics

The worker serves its metrics on `:8081/metrics` (`WORKER_HTTP_PORT`), beside `/health` and `/ready`: the host functions' counters (for example `cufflink_nats_publish_denied_total`) and the worker's own ([observability](observability.md#worker)). It pushes no metrics over OTLP; its traces and logs are exported when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, each job under its tenant's service. Observe workers through:

- Worker logs (above)
- The worker's `/metrics`, and the platform's for what the platform sees: dispatch failures, `cufflink_router_wasm_binary_missing_total`, job enqueue errors
- NATS monitoring at `:8222` — subscription counts (`/subsz`) and JetStream consumer lag (`/jsz`)
- Long-running job rows in the platform database (status, attempts, last error)

```bash
# View all subscriptions
curl http://localhost:8222/subsz

# View JetStream stream info
curl http://localhost:8222/jsz
```

---

## Troubleshooting

| Symptom | Cause | Solution |
|---------|-------|---------|
| Platform returns 500 `WASM worker execution timed out` after 30 s | No worker is subscribed to the subject, or workers can't reach NATS | Start workers; check `NATS_URL` and that `SUBSCRIBE_PATTERN` covers `cufflink.wasm.{service}.{handler}` |
| `job(...)` route returns 202 but the job never runs | No worker, or the worker's `LONG_JOBS_SUBSCRIBE_PATTERN` excludes the subject | Check for `Long-jobs consumer ... subscribed to` in the worker log and widen the pattern |
| `Long-jobs stream 'CUFFLINK_LONG_JOBS' not found` in worker logs | The platform has not created the stream yet (worker started first, or NATS lost state) | Start the platform; the worker's supervisor retries automatically |
| `No WASM module found for service ... v...` in worker replies | Object storage is not configured on the worker, or has different credentials/bucket than the platform | Set the `S3_*` (or `STORAGE_BACKEND=gcs` + `GCS_BUCKET`) variables to the same storage the platform deploys to; look for `Object storage not configured` at startup |
| A handler works in-process but not on the worker | The handler depends on Redis (`REDIS_URL` unset on the worker) or on a platform-only environment variable | Set `REDIS_URL` on the worker, or make Redis a cache with a fallback; give the worker the same `config`/env the platform has |
| Worker `/ready` stays 503 `starting` | The tenant catalog cannot load (database unreachable, or the platform has not applied its migrations yet: `tenant catalog not ready; retrying` in the log), a JetStream consumer cannot be built (NATS unreachable, or the long-jobs stream not yet created by the platform) or the platform database does not answer | The body's `catalog`, `consumers.up` and `db` fields say which; the worker keeps retrying |
| After-hook fails repeatedly and then stops | 3 failed deliveries — the message is in the dead-letter queue | Fix the handler; the letter's `Cufflink-Dlq-Reason` header and the `Async WASM job failed` warning in the worker log carry the error |
| Long job runs again while an earlier attempt is still running | The worker could not extend its lease (platform database unreachable) and gave the job up; the fence refuses what the older attempt writes | Check `cufflink_job_lease_lost_total` and the worker's database connectivity |
| Jobs not distributed evenly | Workers in different queue groups | Ensure all general workers use `QUEUE_GROUP=wasm-workers` |
| Worker exits at startup with a database error | `DATABASE_URL` not set or PostgreSQL not reachable | Check network connectivity and credentials |
