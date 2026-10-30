# Guest memory budget and admission control

Design only. Nothing here is implemented yet; section 11 lists the
decisions the maintainer still has to make.

## Outcome

No request, burst of requests or slow dependency can push a platform or
worker process past its container's memory limit through guest execution.
Guests that run within their memory today run as before. When guests in
flight together reach the process's **guest memory budget**:

- a new HTTP invocation waits briefly and is then refused with
  `503 Service Unavailable` and `Retry-After`;
- a consumer or job is not fetched while there is no room, and one
  already fetched waits instead of being refused, so no message is lost
  and no delivery attempt is spent;
- a guest that tries to grow past the budget fails with its own error
  (503 for a route), not a kill of the process.

Per-service and per-route concurrency limits keep one service from taking
the whole budget or every execution slot.

## 1. What the platform does today

Read from `platform/src/wasm_runtime`, `platform/src/router`,
`platform/src/nats_manager` and `worker/src` on `main`. Since v0.33.1, `main`
has changed only crate versions, the licence and dependency patches,
wasmtime among them (36.0.16 to 36.0.19). v0.34.0 changes nothing described
here.

**Engine and stores.**

- One `WasmEngine` per process: the platform builds one in
  `app/mod.rs` and the worker one in `worker/src/main.rs`, from the same
  `WasmLimits::from_env()`.
- Its `Config` sets only `epoch_interruption(true)`. There is no pooling
  allocator, so linear memories come from wasmtime's on-demand allocator.
- A compiled module and its `InstancePre` are cached per `(service_id, version)`
  (`WASM_MODULE_CACHE_MAX`, 128).
- Every invocation builds a fresh `Store<HostState>` and instantiates from
  the `InstancePre` (`WasmEngine::execute`, `call_handler`). Nothing is
  pooled or reused across invocations. Linear memory is released when the
  store drops at the end of `execute`.

**Per-guest limits.**

- `GuestLimits` (`host/limits.rs`) is the store's `ResourceLimiter`.
- `memory_growing` records the peak and compares `desired` with the cap,
  `min(WASM_MAX_MEMORY_MB, tenant wasm_limits.memory_bytes)`, 512 MiB by
  default.
- Past the cap, the tenant's `wasm_limits` policy decides. `warn` (the
  default) allows the growth and counts
  `cufflink_wasm_limit_events_total{limit="memory",mode="warn"}`.
  `enforce` returns `Ok(false)`, so `memory.grow` yields `-1`, and records a
  `Denial`, which `classify` turns into `ExecutionError::MemoryLimit`.
- Tables, instances and memories are capped unconditionally.
- wasmtime consults the limiter when a memory is created as well as when
  it grows: `limit_new` calls `memory_growing(0, minimum, maximum)` during
  instantiation. When an approved growth then fails, wasmtime calls
  `memory_grow_failed`.

**Deadlines.**

- An epoch ticker (`WASM_EPOCH_TICK_MS`, 100 ms) and
  `epoch_deadline_callback` interrupt a guest at its deadline, on
  cancellation, or at the shutdown hard deadline.
- The deadline is the invocation's budget: 30 s for a route unless
  `timeout_secs` sets it, a subscription's `timeout_secs`, a job's own
  timeout.

**Concurrency.**

- One process-wide `Semaphore` of `WASM_MAX_CONCURRENT` (256) permits,
  acquired in `WasmEngine::run`. Waiting for it counts against the
  invocation's budget. `cufflink_wasm_queue_wait_seconds` measures the wait,
  and `cufflink_wasm_inflight` counts invocations from the start of the
  wait.
- Other bounds that sit around guests:
  - each subscription's `consumer.concurrency` (1 when unset) is the
    pump's `slots` semaphore, acquired **before** a fetch;
  - `MAX_CONCURRENT_JOBS` (4) and `WORKER_MAX_CONCURRENT_SYNC` (32) on the
    worker;
  - `HTTP_MAX_CONCURRENT_REQUESTS`, unset by default, sheds every request
    with `503 {"error":"server busy"}` and no `Retry-After`;
  - the per-IP rate limiter, off by default.
- No bound exists per service or per route.

**How guests are invoked.** Every path calls `WasmEngine::run` with an
`Invocation` and an `InvocationKind`:

| Path | Kind | Where | What follows a failure |
|------|------|-------|------------------------|
| Custom route, `_fn` | `Route` | `router/custom/dispatch.rs::execute` | `map_execution_error` turns every `ExecutionError` into a 500 |
| CRUD before-hook | `Hook` | `router/crud.rs::execute_hook`, inline in the request | the hook's error is the request's answer |
| CRUD after-hook | `Hook` | `spawn_after_hook`, after the commit, detached | logged at `warn` and dropped |
| `on_migrate` | `OnMigrate` | the deploy | the deploy fails |
| Subscription | `Subscription` | `nats_manager/subscriptions.rs`, from the pump | `decide()`: `ack: always` acks on every outcome; `on_success` naks with backoff, spending an attempt, or gives up at `max_deliver` |
| Long job | `Job` | worker `long_jobs.rs`, `router/jobs` | `classify`: `MemoryLimit` is `HandlerError`, `ShuttingDown` is `Interrupted` |
| Worker-routed route or hook | `WorkerRoute` | `worker_dispatch::execute_on_worker`, a NATS request to the worker | `WasmJobResult { ok, response, error }`, which the platform answers with a 500 |
| SSE reducer | `SseReducer` | the SSE bridge | the stream's event is dropped |

A service call reaches its callee over HTTP through the platform's own
router. The callee's invocation is an ordinary `Route` whose request carries
`caller.depth`. Its caller is still running: it holds its memory and its
permit on a blocking thread.

**Memory outside linear memory.** These allocations sit in the host
process. No `ResourceLimiter` sees them:

- `request_json`, held for the whole invocation;
- `HostState.host_response`. `db_query` renders every row it reads with no
  row cap: `Vec<Row>`, then a `serde_json::Value` tree, then a `String`, kept
  until the guest copies it in. A test pins that a host response is copied
  "however large". `http::*` is capped by `HOST_HTTP_MAX_RESPONSE_BYTES`;
  `db_query_read_only` is capped by `DB_READ_ONLY_MAX_ROWS`;
- the response copy in `read_response` (`to_vec`, at most
  `WASM_MAX_RESPONSE_MB`);
- a custom route's body, read after authorization and before the guest
  runs (at most its `body_limit`, 64 MiB).

So one unbounded read costs the host roughly two to three times what it
costs the guest, for as long as the read lasts.

**The gap.** Every limit above applies to one guest or counts executions.
Nothing bounds the memory of all guests together:

- at the defaults, `WASM_MAX_CONCURRENT × WASM_MAX_MEMORY_MB` is
  256 × 512 MiB = 128 GiB of linear memory alone;
- under the default `wasm_limits: warn` even the 512 MiB per guest is not
  enforced.

A handful of concurrent invocations of one handler that reads without a
bound can therefore pass a container limit of a few GiB. Each of them is
within its own cap. The kernel then kills the process, and with it every
service and in-flight request of every tenant on it, not only the
invocations that caused the growth.

## 2. The accounting unit

**Decision: a process-wide ledger of bytes, charged for linear memory as
the limiter approves it and for the host buffers an invocation holds,
credited when the store drops.**

Each invocation holds a `Charge` in its `HostState` (inside `GuestLimits`).
The charge records what the invocation has been given:

- **Reservation**, taken at admission (section 3) before anything is
  allocated: the handler's estimate (section 2.2) plus the length of
  `request_json`.
- **Linear growth.** `memory_growing(current, desired)` asks for
  `desired - current`. The first `reservation` bytes come from the
  reservation; past it, the charge draws from the ledger's free bytes.
  This includes the instantiation call (`current = 0`).
- **Host buffers.** Setting `host_response` debits its length; `take()`
  credits it. The same applies to the db_v2 result envelope.
- `memory_grow_failed` credits the growth that `memory_growing` had just
  approved.
- `Drop` for `Charge` credits everything: reservation, growth and buffers.
  Credit therefore happens on every path out of an invocation: success,
  trap, deadline, cancellation, panic unwinding through `spawn_blocking`.

The ledger is one `Mutex<LedgerState>`, plus a `Notify` for admission
waiters. It is touched only at admission, at each `memory.grow` and host
response, and at drop, never per instruction. Rust guests grow in 64 KiB
pages, usually in batches, so contention is negligible next to the work
each grow implies.

### 2.1 Alternatives considered

| Option | Why not |
|--------|---------|
| Reserve the per-guest cap (512 MiB) at admission | Exact and simple. But a 1 GiB budget admits one guest at a time, while typical handlers peak at a few MiB. It would turn the protection into an outage of its own |
| Count only actual growth, with no reservation | Simultaneous arrivals all see the same free bytes and are all admitted. A burst of heavy invocations then fails mid-run instead of waiting at admission |
| Measure resident memory (RSS, cgroup `memory.current`) instead of a ledger | Sampled and lagging. It cannot attribute usage to a service, and cannot be checked inside `memory_growing` cheaply. Kept as a backstop only (section 3.4) |
| Count linear memory only | Misses the host buffers in section 1. A ledger reading healthy while the host holds two copies of a large result is the failure this design exists to prevent |
| Charge an estimated multiple of every host call | A guess. Charging the buffers actually held is exact for what is held; section 3.4 covers the transient decode memory |

Growth is an upper bound on resident linear memory. Pages the guest grew
but never touched are not resident. The ledger therefore over-counts
slightly, which is the safe direction.

### 2.2 The estimate

The reservation at admission is the handler's estimate:

- the largest charge seen in the last 16 completed invocations of that
  `(service_id, handler)`, kept per replica in a bounded map (the module
  cache's size, least recently used evicted);
- for a handler not seen yet, the module's declared initial memory
  (`MemoryType::minimum`) plus 1 MiB;
- capped at the per-guest cap.

The key leaves out the version, so a new deploy inherits the last
version's estimate. A handler whose memory jumps fails its first heavy
invocation mid-run (section 2.3) if the budget is full. From then on its
estimate makes later invocations wait at admission instead.

### 2.3 When a grow would pass the budget

**Decision: deny at once, never block inside `memory_growing`.**

- `memory_growing` returns `Ok(false)`, the same way the per-guest cap does
  under `enforce`. It records `Denial { reason: Budget }` or
  `{ reason: ServiceShare }`.
- `classify` maps it to the new
  `ExecutionError::Overloaded { reason, retry_after }`. A host buffer that
  would pass the budget fails its host call with the call's existing
  failure: `-1` for the legacy imports, or the v2 envelope's error with a new
  `capacity` kind.
- Each path then treats `Overloaded` as section 4 describes.

Rejected: blocking the grow until other guests release memory.

- `memory_growing` runs synchronously on a `spawn_blocking` thread. That
  thread holds an execution permit, possibly open `db_v2` transactions
  (rows locked) and the caller's connection. Two guests each waiting on the
  other's memory deadlock until their deadlines, holding all of that.
- `ResourceLimiterAsync` would avoid blocking the thread. But it needs
  wasmtime's `async` feature and async calls into every guest, a far larger
  change, and it removes neither the wait-for-each-other cycle nor the held
  locks.
- Waiting belongs where nothing is held yet: admission.

**Interaction with the per-guest cap.** The per-guest check runs first,
exactly as today: same cap, same `wasm_limits` policy, same
`MemoryLimit` (500). Only growth that passes it is charged to the ledger.
At startup the per-guest cap is clamped to the service share of the budget
(section 3.2), with a warning naming both values: one guest can never be
allowed more than its service may hold.

## 3. Admission

Admission runs in `WasmEngine::run`, in place of today's single
`permits.acquire_owned()`, and inside the same `timeout(budget)`. A route
calls it earlier (section 3.5). Admission takes, in order:

1. a **route slot**, when the route declares `max_concurrent`;
2. a **service slot** (`WASM_MAX_CONCURRENT_PER_SERVICE`);
3. a **global slot** (`WASM_MAX_CONCURRENT`, as today);
4. a **memory reservation** of the estimate, within the class's share
   and the service's share.

Admission holds nothing while it waits for a later step: a waiter that
times out releases what it took. The result is an `Admission` value, the
permits plus the `Charge`, moved into the blocking task and dropped with
the store.

### 3.1 Classes

| Class | Who | Waits for admission | When the wait ends without room |
|-------|-----|---------------------|---------------------------------|
| `interactive` | `Route`, CRUD before-hook, `WorkerRoute`, `SseReducer` | up to `WASM_ADMISSION_WAIT_MS` (1000), never past the budget | `Overloaded`: 503 with `Retry-After` |
| `nested` | an invocation whose verified `ServiceCaller` has `depth > 0` | not at all: no route, service or global slot (its caller already holds one), memory only | `Overloaded` at once. The caller's service call receives a 503 it can handle |
| `committed` | CRUD after-hook in process, `on_migrate` | up to its own budget, as today | a deadline, as today. Never refused for capacity: the write has committed and the deploy is running |
| `background` | `Subscription`, `Job` | a headroom check before the fetch, then a reservation that may wait without bound (section 3.3) | the message is not fetched, or waits in progress; never refused |

A nested invocation does not queue for two reasons. Its caller holds
memory and a blocking thread while it waits. And a per-service limit would
otherwise deadlock a service that calls itself.

The class is taken from the `ServiceCaller` that `postures::internal`
builds from the verified service-identity header (`service_identity::verify`),
never from the request body's `caller` field or anything else a client
sends. A request without a verified identity is never `nested`, so no
caller can claim nesting to skip the concurrency limits.

### 3.2 Shares (fairness)

- **Service share.** One `(tenant, service)` may hold at most
  `WASM_MEMORY_SERVICE_SHARE_PERCENT` (50) of the budget, reservations and
  growth together. A grow past it is denied with `ServiceShare` even while
  the budget has room. The share leaves room for the other services; it is
  not a guarantee.
- **Background share.** Subscriptions and jobs together may hold at most
  `WASM_MEMORY_BACKGROUND_SHARE_PERCENT` (75) at admission. So a consumer
  backlog draining at full concurrency can never keep routes out.
  Interactive, nested and committed invocations may use the whole budget.
- **Growth headroom.** New reservations are granted only while the ledger
  stays under `WASM_MEMORY_ADMIT_PERCENT` (90) of the budget. The rest is
  left to invocations already running, which are further along and cheaper
  to finish than to fail.
- Waiters are woken on every credit and re-check in arrival order within
  their class. A waiter that cannot fit does not block one behind it that
  can: there is no head-of-line blocking. Starvation of large requests is
  bounded by the wait limit.

`tokio::sync::Semaphore` with one permit per MiB was considered for the
ledger and rejected. Its fairness assigns released permits to the head
waiter, so a running guest's `try_acquire` on `memory.grow` would fail in
favour of a request not yet admitted: the opposite of the priority wanted.

### 3.3 Consumers and jobs

Refusing a message after it is fetched loses or spends it:

- `ack: always` acks every outcome;
- `Interrupted` naks with no delay, a hot loop;
- `Retry` spends a JetStream delivery and can dead-letter a valid message.

So a fetched message is never refused for capacity: it waits. A pump does
not reserve before it fetches either. The pump's fetch is a long poll that
is outstanding almost all the time, so a reservation taken before it would
hold an idle durable's `concurrency × estimate` of the budget away from
routes for as long as the durable is idle.

- **Before the fetch: a headroom check, not a reservation.** The pump
  already acquires a slot before each fetch. With the budget on, it then
  waits until the background share has room for at least one estimate of
  its handler, which the `SubscriptionSpec` names. Nothing is reserved. It
  sizes the fetch as the smaller of today's `fetch_size` and the number of
  estimates that fit at that moment. A durable whose handler cannot fit
  does not fetch, so its messages stay in the stream undelivered. The
  pump waits without bound but keeps watching `halted`, so it stops for a
  stop or shutdown as today.
- **After the fetch: a reservation that may wait.** Each fetched message is
  admitted as `background` before its invocation starts. If the room
  checked before the fetch has been taken meanwhile, the message waits.
  - The wait sits outside the handler's budget timeout, so waiting does
    not eat into the handler's time.
  - The pump's heartbeat (`AckProgress`) keeps the message in progress
    while it waits.
  - The message is delayed, never nak'd, so it spends no delivery and
    cannot be dead-lettered by the wait.
- **Long jobs (worker).** One durable carries every job, so the handler
  is unknown before the fetch. The consumer checks for at least
  `WASM_MEMORY_JOB_FLOOR_MB` (32) free in the background share before each
  fetch. It then admits the fetched job as `background` with the job's
  estimate, keeping the job leased while it waits.
- **Mid-run denial.**
  - A job denied mid-run is classified `Interrupted`, not `HandlerError`,
    so it is re-run without counting a failed attempt.
  - A subscription denied mid-run is `Outcome::Retry { after:
    Some(retry_after) }`. That spends one delivery, as a timeout does today
    (owner question 9).
- Worker after-hooks (`CUFFLINK_WASM_JOBS`) and worker request/reply
  handlers are admitted the same way on the worker's own ledger.

### 3.4 What the ledger cannot see

The ledger cannot see host memory that is never handed to a guest: the
`Vec<Row>` and `Value` tree while `db_query` renders its result, CRUD
lists, module compilation, pools. Three measures cover it:

- the **host reserve** (section 5) is subtracted from the container limit
  before the budget is set;
- an optional **pressure gate**. Where cgroup v2 is mounted, a task reads
  `anon` from the cgroup's `memory.stat` every 250 ms. While it is above
  `limit - reserve / 2`, interactive and background admissions wait as if
  the budget were full (`reason="pressure"`);
- a cap on host responses (`db_query`'s result in particular) would
  bound the decode itself. It changes what a guest observes today, so it
  is owner question 10 rather than part of this change.

### 3.5 Routes take admission before reading the body

A custom route's body is host memory, up to its `body_limit`. Under a
burst, queued requests would hold their bodies while they wait.

- `router/custom/dispatch.rs` therefore admits right after authorization
  and before reading the body. The reservation adds the `Content-Length`
  (or the route's `body_limit` when the length is not given).
- It passes the `Admission` into `execute`, and `WasmEngine::run` takes an
  admission already made instead of making one.
- A worker-routed route admits on the worker, since that is where the
  guest runs. The platform reads its body as today.

## 4. Errors and answers

New `ExecutionError::Overloaded { reason, retry_after }`, with `label()`
`overloaded`. `reason` is one of `memory`, `service_share`,
`service_concurrency`, `route_concurrency` or `pressure`.

| Path | Answer |
|------|--------|
| Route, before-hook, `_fn` | `PlatformError::RetryLater`: 503, `Retry-After: WASM_ADMISSION_RETRY_AFTER_SECS` (2), body `{"error": "guest capacity exhausted, retry later"}`. Under the error profile, the profile's 503 shape |
| Service call (nested) | the callee's 503 reaches the caller's guest as the call's response |
| Worker-routed | `WasmJobResult` gains `retry_after_ms: Option<u64>` (`serde(default)`, skipped when `None`). The platform answers `RetryLater` when it is set. An older platform ignores the field and answers 500 as today; an older worker never sets it |
| After-hook, `on_migrate` | not refused at admission. Mid-run denial is logged like any hook failure |
| Subscription, job | section 3.3 |

The per-guest `MemoryLimit` stays a 500, since it is the guest's own
fault and retrying will not help. `Overloaded` is the platform's capacity
and is retryable.

A mid-run denial of a route has already run part of the handler, exactly
like a timeout today. Its 503 says to retry; whether a route is safe to
retry is the client's concern, as it is for any 503 (owner question 8).

## 5. Configuration and defaults

All process-wide, read by the platform and the worker alike. Each
process uses its own container's limit.

| Variable | Default | Meaning |
|----------|---------|---------|
| `WASM_MEMORY_BUDGET_MODE` | `warn` | `off`, `warn` or `enforce`, in the vocabulary of [policies](../policies.md). `off`: no ledger and no admission beyond `WASM_MAX_CONCURRENT`, exactly today. `warn`: the ledger, shares and slots are all evaluated. What would wait, be refused or be denied is counted and logged, and allowed. `enforce`: as this note describes |
| `WASM_MEMORY_BUDGET_MB` | derived | The budget. When unset: `limit - reserve`, where `limit` is the cgroup v2 `memory.max`, else cgroup v1 `memory.limit_in_bytes`. With no limit (`max`, an absurd v1 value, no cgroup, macOS) the budget is off whatever the mode, and the platform logs that once at startup |
| `WASM_MEMORY_HOST_RESERVE_MB` | `max(256, 30 % of limit)` | What the derived budget leaves for the host: compiled modules, pools, the runtime, decode buffers |
| `WASM_MEMORY_SERVICE_SHARE_PERCENT` | `50` | Section 3.2 |
| `WASM_MEMORY_BACKGROUND_SHARE_PERCENT` | `75` | Section 3.2 |
| `WASM_MEMORY_ADMIT_PERCENT` | `90` | Section 3.2 |
| `WASM_MEMORY_JOB_FLOOR_MB` | `32` | Section 3.3 |
| `WASM_MEMORY_PRESSURE_GATE` | `true` | Section 3.4; ignored without cgroup v2 |
| `WASM_MAX_CONCURRENT_PER_SERVICE` | `32` | Invocations of one `(tenant, service)` in flight, nested excluded. At most `WASM_MAX_CONCURRENT` |
| `WASM_ADMISSION_WAIT_MS` | `1000` | Longest interactive wait |
| `WASM_ADMISSION_RETRY_AFTER_SECS` | `2` | `Retry-After` on an `Overloaded` 503 |

Startup checks:

- A malformed value or a zero refuses startup, as `WasmLimits::validate`
  does today.
- Startup logs the limit, reserve, budget, mode and shares at `info`,
  once.
- When `WASM_MAX_MEMORY_MB` exceeds the service share of the budget, the
  per-guest cap is clamped to it and the platform warns.

**Why a process-wide mode, not a tenant policy.** The tenant policy
mechanism is how this repository soaks a behaviour change. But the budget
protects a resource every tenant shares: one tenant left in `warn` could
still grow the process into the kernel's limit and take every other
tenant down with it. The mode is therefore one environment variable, using
the policy vocabulary and recorded the same way. Per-tenant shares are
left for later (section 10, owner question 2).

**Manifest: per-route concurrency.**

- `CustomRouteDefinition.max_concurrent: Option<u32>`,
  `skip_serializing_if = "Option::is_none"`, so every existing manifest
  keeps its canonical form and deploy hash.
- Validated 1 to 1024.
- Refused together with `job` (a job route's guest runs as a job) and
  with `sse`.
- In the `service!` macro: `max_concurrent = 4` in the route tuple, beside
  `body_limit` and `timeout`.
- It derives a new feature, `route_concurrency`, rather than reusing
  `route_limits`. A platform that advertises only `route_limits` would
  accept the manifest and drop the key.
- A service author can only lower their own route's share. The
  install-wide numbers stay with the operator.

Subscriptions already declare `consumer.concurrency`. It is unchanged,
and the service slot applies on top of it.

## 6. Metrics, logs and alerts

New metrics, platform and worker alike (`docs/observability.md`, "WASM
execution"):

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cufflink_wasm_memory_budget_bytes` | gauge | — | The budget in force (0 when off) |
| `cufflink_wasm_memory_in_use_bytes` | gauge | `class` | Ledger charges: reservations, growth and host buffers |
| `cufflink_wasm_memory_service_bytes` | gauge | `tenant`, `service` | The same, per service. Set at admission and release, and at scrape |
| `cufflink_wasm_admission_wait_seconds` | histogram | `class`, `result` (`admitted`, `refused`) | Time from arrival to admission. Replaces `cufflink_wasm_queue_wait_seconds`, which stays one release as an alias |
| `cufflink_wasm_admission_refused_total` | counter | `tenant`, `service`, `class`, `reason`, `mode` (`warn`, `enforce`) | Invocations that waited out their bound. Under `warn`, those that would have been |
| `cufflink_wasm_memory_denied_total` | counter | `tenant`, `service`, `kind`, `reason`, `mode` | Grows and host buffers past the budget or the service share |
| `cufflink_wasm_estimate_bytes` | histogram | `kind` | Reservations taken at admission |
| `cufflink_process_memory_limit_bytes` | gauge | — | The cgroup limit read at startup |
| `cufflink_process_memory_anon_bytes` | gauge | — | The cgroup's `anon`, as the pressure gate last read it |
| `cufflink_pump_admission_wait_seconds` | histogram | `kind` | A pump's wait for headroom before a fetch |

Changes to existing metrics:

- `cufflink_wasm_handler_duration_seconds{result}` gains `overloaded`.
- `cufflink_wasm_limit_events_total{limit}` gains `budget`.
- `cufflink_wasm_inflight` counts from admission, no longer from the
  start of the wait.

**Logs.** A `warn` under target `cufflink::wasm::budget`, naming tenant,
service, handler, class, reason and the ledger's numbers. It is logged at
most once a minute per `(tenant, service, reason)`, as policy violations
are.

**Alerts** (`docs/observability.md`, alerting examples):

- `cufflink_wasm_memory_in_use_bytes / cufflink_wasm_memory_budget_bytes > 0.8`
  for 5 m: sustained pressure, so add memory or replicas, or bound the
  heavy handler.
- `increase(cufflink_wasm_admission_refused_total{mode="enforce"}[5m]) > 0`:
  callers are being told to retry. Under `warn`, the same expression with
  `mode="warn"` is the soak signal for turning on `enforce`.
- `increase(cufflink_wasm_memory_denied_total[5m]) > 0`: a handler grew past
  what admission expected.
- `cufflink_process_memory_anon_bytes / cufflink_process_memory_limit_bytes > 0.9`:
  host memory the ledger does not see is close to the limit.

## 7. Compatibility

- **Below the budget nothing changes.** Admission that fits is
  immediate: no wait, the same errors, the same answers. `off` is exactly
  today, and `warn` changes only metrics and logs.
- `cufflink_wasm_queue_wait_seconds` keeps being exported for one release.
- **Features.**
  - `guest_memory_budget` is advertised and never demanded.
  - `route_concurrency` is derived from any route with `max_concurrent`. A
    platform without it refuses such a deploy (422, naming it).
- **Platform/worker skew.** `retry_after_ms` is optional in both
  directions (section 4).
- **Rollback.**
  - A platform rolled back below the release ignores the new variables.
    It runs with today's single semaphore and drops `max_concurrent` from
    any service already deployed with it. Such a service keeps running
    without its route limit and cannot be deployed there (422).
  - Neither the database nor any tenant policy row is touched, so nothing
    needs undoing.
- **wasmtime.** The design depends only on `ResourceLimiter::memory_growing`
  and `memory_grow_failed`, as wasmtime 36 defines them. Check both against
  any wasmtime upgrade that lands first; the open one does not build yet.

## 8. Tests

**Unit** (`wasm_runtime`):

- Ledger: reservation, growth, host buffers and their credits.
  `memory_grow_failed` credits the approved growth. `Drop` credits
  everything, including after a trap and a deadline. The ledger is back to
  zero after a thousand mixed invocations.
- Shares: a service at its share is denied while the budget has room;
  background admission stops at its share while interactive admission
  continues; the admit percentage leaves headroom to growth.
- Order: a waiter that fits is admitted past one that does not; waiters
  past their bound are refused and release what they held; nested
  invocations never wait.
- Estimates: the first invocation reserves the module minimum; then the
  largest of the last sixteen; capped at the per-guest cap; inherited
  across versions.
- `memory_growing` never blocks: a WAT guest growing past the budget gets
  `-1` at once.
- Configuration: cgroup v2 `memory.max` (a number, `max`), v1
  `memory.limit_in_bytes` (a number, the unlimited sentinel), no cgroup
  at all, from fixture files. The reserve and budget arithmetic. The
  per-guest cap clamp and its warning. Zero and malformed values refused.
- `classify` and `map_execution_error`: `Overloaded` is a 503 with
  `Retry-After`; `MemoryLimit` is still the same 500.
- Jobs: `Overloaded` is `Interrupted`. Subscriptions: `Retry { after }`.

**Engine** (`#[tokio::test(flavor = "multi_thread")]`, WAT guests, like the
existing `wasm_runtime` tests):

- A budget of 64 MiB, `enforce`. Eight concurrent invocations of a guest
  that grows 24 MiB and spins for 300 ms. At most two hold memory at any
  instant (sampled from the ledger). The rest wait and run, or are refused
  past the wait bound. The ledger never exceeds 64 MiB, and is back to zero.
- The same under `warn`: all eight run, and the refusals are counted with
  `mode="warn"`.

**Integration** (`platform/tests/wasm_budget.rs`, through `testkit`,
fixtures `acme` and `globex`, services `catalog-service` and
`loan-service`):

- **The burst.**
  - `WASM_MEMORY_BUDGET_MB=128`, `enforce`, and a test guest (extending
    `test-guests`) that grows to 48 MiB, holds it with `util::sleep_ms(500)`,
    and answers.
  - Twelve concurrent calls. Some succeed. Every other one is a 503 whose
    `Retry-After` is `2`. None is a 500.
  - `/health` answers 200 throughout, and the process is the same process
    (pid).
  - On Linux, `VmHWM` from `/proc/<pid>/status` stays below the baseline
    plus 128 MiB plus a fixed slack.
- **Fairness.** `acme/catalog-service` saturates its share. A light
  `globex/loan-service` route keeps answering 200 within its normal latency.
- **Route concurrency.** A route with `max_concurrent = 2` and three
  concurrent calls: the third is a 503 after the wait bound. Also: the
  deploy derives `route_concurrency`, and an older manifest's hash is
  unchanged.
- **Consumers.**
  - Twenty messages to an `on_success` subscription whose handler grows
    48 MiB, with a budget that fits two.
  - Every message is handled once, with `num_delivered == 1`, and none is
    dead-lettered.
  - Messages a pump fetched while the budget was full wait in progress
    and are run, with no nak. A durable whose estimate does not fit fetches
    nothing until room frees.
  - An idle durable holds nothing in the ledger.
  - The pump's headroom wait is recorded.
- **Committed work.** An after-hook and an `on_migrate` started while the
  budget is full both run once room frees.
- **Worker.** A worker-routed route refused on the worker answers 503 with
  `Retry-After` through the platform.
- **Under a real limit.** When the test can create a cgroup (Linux with a
  delegated cgroup v2, `systemd-run --user --scope -p MemoryMax=768M`), the
  platform runs inside it with the derived budget. Twenty concurrent 96 MiB
  guests leave it alive: no exit 137 and no `oom_kill` in the cgroup's
  `memory.events`. Elsewhere the test is skipped and says why.

## 9. Documentation, when implemented

- `docs/setup.md`, "WASM runtime limits": the variables of section 5.
  `WASM_MAX_CONCURRENT` notes the per-service limit.
- `docs/deployment.md`: sizing a container: what the budget, the reserve
  and the replicas mean together.
- `docs/routing.md`, "Route limits": `max_concurrent`.
- `docs/observability.md`: the metrics and alerts of section 6.
- `docs/tenant-isolation.md`: `WASM_MEMORY_BUDGET_MODE=enforce` joins the
  table of settings an operator must set. "Per-tenant concurrency" in the
  planned list becomes per-service, done, and per-tenant shares, planned.
- `docs/scaling.md`: one replica's budget is its own. Budgets do not add
  across replicas for one burst routed to one replica.
- `docs/versions.md`, in the release that ships it:

  > ## Features added in v0.35.0
  >
  > | Feature | Needed when | Provides |
  > |---------|-------------|----------|
  > | `guest_memory_budget` | never demanded | a process-wide budget for guest memory, with admission that waits or answers 503 and `Retry-After` rather than exceeding it ([setup](setup.md#wasm-runtime-limits-platformsrcwasm_runtimemodrs)) |
  > | `route_concurrency` | any route sets `max_concurrent` | invocations of a route in flight at once, per replica ([route limits](routing.md#route-limits)) |
  >
  > `WASM_MEMORY_BUDGET_MODE` is `warn` by default: nothing is refused yet,
  > and `cufflink_wasm_admission_refused_total{mode="warn"}` shows what
  > would have been. A later release makes `enforce` the default.

  The rollback table gains a row: services that declare `max_concurrent`
  run without the limit on an older platform and cannot be deployed there.

## 10. Not in this change

- **Per-tenant shares or budgets** as a tenant policy key (for example
  `wasm_budget` with `{"memory_share_percent": …}`). A new key, never a field
  on `WasmLimitsConfig`: the existing configs `deny_unknown_fields`, and an
  older binary ignores an unknown key but not an unknown field.
- **A cap on host responses**, and rendering `db_query` rows straight to
  text without the `Value` tree (owner question 10).
- **Memory hints in the manifest** (a route declaring the memory it
  needs), instead of learned estimates.
- **The pooling allocator.** Orthogonal: it bounds slots, not bytes
  in use, and would reserve address space up front.
- **A budget shared across replicas.** Each replica protects its own
  process.

## 11. Owner questions

None is answered yet. Each states the proposed default.

1. **Default mode in the first release.** `warn`, as every behaviour change
   here has shipped, with `enforce` the default one release later?
   Or `enforce` from the start whenever the budget is derived from a
   container limit, since the alternative at the limit is the kernel killing
   the process? *Proposed: `warn` first, `enforce` in the next release;
   operators who need it now set `enforce`.*
2. **Scope of the mode.** One process-wide `WASM_MEMORY_BUDGET_MODE`
   (proposed, for the reason in section 5)? Or a tenant policy key,
   accepting that a tenant in `warn` can still exhaust the process?
   *Proposed: process-wide; per-tenant shares later as a new policy key.*
3. **Host reserve.** `max(256 MiB, 30 % of the limit)`, so a 2 GiB limit
   gives a budget of about 1.4 GiB and a 4 GiB limit about 2.8 GiB? *Proposed:
   as stated; revisit with `cufflink_process_memory_anon_bytes` data.*
4. **Service share.** At most 50 % of the budget per `(tenant, service)`?
   *Proposed: 50 %.*
5. **Background share.** Subscriptions and jobs at most 75 % of the budget at
   admission, leaving 25 % that only routes and hooks can take? *Proposed:
   75 %.*
6. **Interactive wait and `Retry-After`.** Wait at most 1 s, then 503 with
   `Retry-After: 2`? *Proposed: 1 s and 2 s.*
7. **Concurrency limits.** `WASM_MAX_CONCURRENT_PER_SERVICE` 32 by default,
   plus the manifest's per-route `max_concurrent`? *Proposed: yes, 32.*
8. **A route denied mid-run.** 503 with `Retry-After` (proposed), even though
   part of the handler has run? Or 500, as for the per-guest cap, so that
   clients do not retry a partly applied request? *Proposed: 503.*
9. **A subscription denied mid-run.** Nak after `Retry-After`, spending one
   JetStream delivery as a timeout does today? *Proposed: yes; the headroom
   check before the fetch makes it rare.*
10. **Host response cap.** Add `WASM_MAX_HOST_RESPONSE_MB` (64, matching
    `WASM_MAX_GUEST_LEN_MB`) for `db_query` and the other host responses
    under the same mode, in this change or the next? It changes what a guest
    that reads more than 64 MiB observes. *Proposed: the next change, with
    the streaming render.*
11. **Pressure gate.** Include the cgroup v2 `anon` gate (section 3.4)
    now? *Proposed: yes, on by default where cgroup v2 is mounted.*
12. **Per-guest cap clamp.** Clamp `WASM_MAX_MEMORY_MB` to the service share
    of the budget at startup with a warning (proposed)? Or refuse to start
    when it is larger? *Proposed: clamp and warn.*
