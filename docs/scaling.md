# Scaling Cufflink

Cufflink is designed for horizontal scaling. The platform, WASM workers, and web runner can all run as multiple replicas behind a load balancer. The examples below use Docker Swarm's `docker service scale`; Kubernetes, Nomad, or any orchestrator that can run several copies of a stateless container works the same way.

## Architecture Overview

All shared state lives in external systems (PostgreSQL, S3-compatible object storage, NATS JetStream, Redis), so each component instance is stateless and independently replaceable.

```
                    Load Balancer
                         |
            +------------+------------+
            |            |            |
        Platform     Platform     Platform
        Replica 1    Replica 2    Replica 3
            |            |            |
            +-----+------+------+----+
                  |             |
               +--+--+     +---+---+
               | NATS |     | Postgres |
               +--+--+     +---+---+
                  |
       +----------+----------+
       |          |          |
    Worker     Worker     Worker
    1          2          3
```

## Platform

The platform (Axum API server) is fully horizontally scalable. On Docker Swarm:

```bash
docker service scale cufflink_platform=3
```

Each replica maintains its own in-memory caches that stay in sync automatically:

### Service Registry

- Cached in memory per replica
- Synced via NATS topic `cufflink.registry.changed`
- When any replica deploys a service, it publishes a change notification and all other replicas reload from the database
- Brief eventual consistency window (sub-second in practice)

### WASM Module Cache

- Compiled modules cached in memory per replica
- On cache miss, the module is fetched from object storage and compiled
- No inter-replica coordination needed since modules are immutable per version

### Tenant Database Pools

- Each replica maintains its own pool cache (`MAX_TENANT_POOLS`, default 64 tenants; `MAX_CONNS_PER_TENANT`, default 4 connections each)
- LRU eviction when the cache is full
- Independent per replica, no coordination needed
- Connection budget per platform replica at the defaults: 64 pools x 4 connections = 256 connections. Size PostgreSQL's `max_connections` for `replicas x 256` plus the workers (below) and the platform DB pool.

### Rate Limiting

The rate limiter is **off by default**. Enable it with `RATE_LIMIT_ENABLED=true` (`platform/src/middleware/rate_limit.rs`).

When enabled it limits **per client IP** — never per credential, since credentials are unvalidated at that point in the request. Defaults are 20 requests/second sustained with a burst of 60.

| Variable | Default | Meaning |
|----------|---------|---------|
| `RATE_LIMIT_ENABLED` | `false` | Turn the limiter on. |
| `RATE_LIMIT_PER_SECOND` | `20` | Sustained requests per second per client IP. |
| `RATE_LIMIT_BURST` | `60` | Burst allowance per client IP (never below `RATE_LIMIT_PER_SECOND`). |
| `RATE_LIMIT_CLIENT_IP_SOURCE` | `peer` | Where the client IP comes from: `peer` (TCP peer; only correct with nothing proxying in front), `gclb` (second-to-last `X-Forwarded-For` entry as appended by a cloud L7 load balancer, upgraded to `CF-Connecting-IP` when the request came through Cloudflare), or `xff-first` (first `X-Forwarded-For` entry; only for a proxy that overwrites the header). The same address keys anonymous SSE streams and is recorded for platform-admin calls. |
| `RATE_LIMIT_EXEMPT_PRIVATE_PEERS` | `true` | Never limit callers whose socket peer is a private address (in-cluster traffic such as web-runner SSR or the platform calling itself). |
| `RATE_LIMIT_EXEMPT_IPS` | unset | Comma-separated IPv4/IPv6 addresses and CIDR ranges (e.g. `203.0.113.7,2001:db8::/32`) whose client IP (as resolved by `RATE_LIMIT_CLIENT_IP_SOURCE`) is never limited. Use it for a trusted upstream that calls on behalf of many users from one egress address which would otherwise share a single per-IP bucket. An entry that is not an address or range refuses startup. |

`/health`, `/ready`, and `/metrics` are always exempt so probes get an answer.

| Mode | Behavior |
|------|----------|
| With Redis (`REDIS_URL` set) | Shared fixed-window counter in Redis, keyed `cufflink:rate:{ip}:{window}`, so the budget is exact across all replicas. |
| Without Redis, or Redis unavailable | Each replica falls back to its own in-memory keyed limiter (effective limit ≈ N replicas x the configured rate). The Redis check is bounded and fails open to the in-memory limiter; it never turns into a rejection on its own. |

Rejected requests get `429` with a `Retry-After` header and increment `cufflink_rate_limited_total`.

### Body Limits

Request bodies are capped per route family (`platform/src/main.rs`):

- `/svc/{tenant}/{service}/_fn/*` (custom WASM routes): **4 MB**
- everything else on the router (deploys, web app artifact uploads, CRUD): **250 MB**

Put a reverse proxy with its own limit in front if you need something tighter for public traffic.

### Authentication

Each replica caches Keycloak JWKS keys independently. Keys are refreshed on cache miss or periodically. No inter-replica state needed.

## WASM Workers

Workers are interchangeable. Each worker runs **four consumers** (`worker/src/main.rs`, `worker/src/long_jobs.rs`, `worker/src/backup.rs`):

| Consumer | Transport | Subjects | Config |
|----------|-----------|----------|--------|
| Synchronous handlers | Core NATS request/reply, queue group | `SUBSCRIBE_PATTERN` (default `cufflink.wasm.>`) | `QUEUE_GROUP` (default `wasm-workers`) |
| Async WASM jobs (after-hooks) | JetStream stream `CUFFLINK_WASM_JOBS`, durable pull consumer `wasm-worker` | `cufflink.wasm.async.{service}.{handler}` | ack wait 60s, `max_deliver` 3 then dead-lettered, stream `max_age` 1 hour, work-queue retention |
| Backup / restore | JetStream stream `CUFFLINK_BACKUP` | `cufflink.backup.>` | ack wait 300s, `max_deliver` 3, stream `max_age` 24 hours |
| Long-running jobs (`job(...)` routes, `jobs::enqueue`) | JetStream stream `CUFFLINK_LONG_JOBS` | `LONG_JOBS_SUBSCRIBE_PATTERN` (default `cufflink.jobs.>`) | `MAX_CONCURRENT_JOBS` (default 4), `LONG_JOBS_ACK_WAIT_SECS` (default 60), `JOB_LEASE_SECS` (default 60), stream `max_age` 24 hours |

A JetStream `max_deliver` of 3 means the first delivery plus **two redeliveries** if the worker does not ack in time.

On Docker Swarm:

```bash
docker service scale cufflink_wasm-worker=5
```

### Which work reaches a worker

- **Table hooks and custom routes run in-process on the platform by default.** They are dispatched to a worker over NATS request/reply **only** when the handler is listed in the platform's `WORKER_ROUTES` (`platform/src/config.rs`, `is_worker_routed`). An unlisted after-hook is still executed by the platform.
- **`job(...)` routes and `jobs::enqueue`** always go to the `CUFFLINK_LONG_JOBS` stream and are executed by whichever worker pulls them.
- **Backup and restore** jobs always go to `CUFFLINK_BACKUP`.

To dedicate a worker pool to one tenant or service, narrow `LONG_JOBS_SUBSCRIBE_PATTERN` (for example `cufflink.jobs.acme.stock-service.>`) on that pool.

### Worker Configuration

```env
SUBSCRIBE_PATTERN=cufflink.wasm.>
QUEUE_GROUP=wasm-workers
LONG_JOBS_SUBSCRIBE_PATTERN=cufflink.jobs.>
MAX_CONCURRENT_JOBS=4
LONG_JOBS_ACK_WAIT_SECS=60
# Tenant pool cache — the worker defaults are smaller than the platform's (64 / 4)
MAX_TENANT_POOLS=32
MAX_CONNS_PER_TENANT=2
```

Workers also need the same `DATABASE_URL`, `NATS_URL`, and object storage (`STORAGE_BACKEND` + `S3_*` / `GCS_BUCKET`) settings as the platform. Redis is disabled on the worker, so `redis::*` host calls fail inside handlers that run there.

On the platform side, configure which handlers route to workers:

```env
WORKER_ROUTES='{"my-service": ["heavy_handler", "slow_report"]}'
```

## Web Runner

The web runner manages Node.js processes for deployed web-mode (Next.js) applications. It **warms apps lazily**: the HTTP listener opens immediately and each `(tenant, service)` is downloaded from object storage, started, and health-checked on its **first request** (`web-runner/src/warmer.rs`). Concurrent cold-start requests for the same app are coalesced into one warmup. Until warmed, the first request to a cold app pays the startup cost.

It routes HTTP requests by `Host` header to the correct process, so it needs `CUFFLINK_BASE_DOMAIN` (required) plus `CUFFLINK_PUBLIC_SCHEME` (default `https`) and `CUFFLINK_HOSTNAME_FORMAT` (`flat` = `{service}--{tenant}.{base}`, default; `hierarchical` = `{service}.{tenant}.{base}`). Point a wildcard DNS record (and wildcard TLS certificate) at the runner replicas.

On Docker Swarm:

```bash
docker service scale cufflink_web-runner=2
```

### Multi-Instance Behavior

With multiple replicas, **each replica runs its own copy of every app that has been requested through it**. This provides redundancy but means each replica needs enough memory and CPU for all apps it may end up hosting.

Deploy and undeploy events arrive via NATS (`deploy.web.>`, `undeploy.web.>`), so all replicas pick up new versions. Each replica performs its own zero-downtime swap (atomic directory swap, health check, rollback on failure) for apps it is currently running.

### Resource Planning

Per web-runner replica, budget for:
- Disk: artifact storage for all apps in `APPS_DIR`
- Memory: one Node.js process per warmed app
- CPU: proportional to app traffic hitting that replica

## NATS Persistence

JetStream is where in-flight work lives. Run NATS with `--jetstream` and a persistent store directory (the compose stack mounts the `natsdata` volume at `/data`). Streams that must survive a NATS restart:

| Stream | Subjects | Holds |
|--------|----------|-------|
| `DW_{TENANT}` (one per tenant, `-` → `_`) | `dw.{tenant}.>` | CRUD change events and audit log, 7 days, interest retention |
| `CUFFLINK_BACKUP` | `cufflink.backup.>` | Backup / restore jobs, 24 hours |
| `CUFFLINK_WASM_JOBS` | `cufflink.wasm.async.>` | Async after-hook jobs, 1 hour |
| `CUFFLINK_LONG_JOBS` | `cufflink.jobs.>` | Long-running jobs, 24 hours |
| `CUFFLINK_TICKS` | `cufflink.tick.>` | Scheduler ticks (`1min`, `5min`, `1hour`) for subscriptions |

The platform creates or updates these on startup and on tenant creation; the worker also creates `CUFFLINK_WASM_JOBS` and `CUFFLINK_BACKUP` if they are missing. Losing the store loses queued (not yet acked) jobs, so back it up alongside PostgreSQL (see `docs/backup.md`).

## State Sync Summary

| Concern | Mechanism |
|---------|-----------|
| Request distribution | Load balancer round-robin |
| Service registry sync | NATS pub/sub (`cufflink.registry.changed`) |
| Sync handler distribution | NATS queue groups (one worker per request) |
| Async / long jobs / backups | NATS JetStream with durable consumers |
| Artifact and WASM storage | S3-compatible object storage or GCS (shared, durable) |
| Data persistence | PostgreSQL |
| Rate limiting | Redis fixed-window counter (shared) with per-replica in-memory fallback; off unless `RATE_LIMIT_ENABLED` |
| Auth token validation | Independent JWKS cache per replica |

## Consistency Model

Cufflink uses **eventual consistency** for its in-memory caches:

- After a deployment, there is a brief window where some replicas may serve the old version while others have already updated
- This window is typically sub-second (NATS notification + DB reload)
- WASM modules are versioned, so there is no risk of serving a partially-updated module
- Database operations (CRUD) are always consistent since they go directly to PostgreSQL

## Example: Scaling on Docker Swarm

Docker Swarm is one deployment option; the same replica counts apply to a Kubernetes `Deployment` or any equivalent.

```bash
# Scale API servers
docker service scale cufflink_platform=3

# Scale WASM workers independently based on job volume
docker service scale cufflink_wasm-worker=5

# Scale web runners for app redundancy
docker service scale cufflink_web-runner=2
```

All components can be scaled independently based on load characteristics:
- **Platform**: scale based on API request volume
- **Workers**: scale based on job queue depth (`CUFFLINK_LONG_JOBS`, `CUFFLINK_WASM_JOBS`, `CUFFLINK_BACKUP`)
- **Web Runner**: scale based on web app traffic and memory requirements
