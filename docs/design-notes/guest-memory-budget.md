# Guest memory budget and admission control

Design only. Nothing here is implemented yet.

- The maintainer answered section 11's questions on 2026-10-30, accepting
  every proposed default. On the same day the maintainer re-decided
  question 10, bringing the host-side caps into this change.
- The note was then revised after an independent review. Section 12
  lists the questions the revision raised.

## Outcome

Under `WASM_MEMORY_BUDGET_MODE=enforce`, guest execution cannot push a
platform or worker process past its container's memory limit. That holds
for any single request, any burst of requests and any slow dependency.
(`warn`, the first release's default, only measures what `enforce` would
do; section 5.1.) Precisely:

- Every byte a guest holds is charged to a process-wide **guest memory
  budget** before it exists. That covers its linear memory, its tables and
  the host buffers made on its behalf: requests, host-call inputs and
  results, and responses.
- What is not charged byte for byte is bounded by caps and declared
  multiples (section 2.4). That bound is what the host reserve (section 5)
  is sized from.
- Guests that run within their memory today run as before.

When guests in flight together reach the budget:

- a new HTTP invocation waits briefly and is then refused with
  `503 Service Unavailable` and `Retry-After`, having run nothing;
- a consumer or job is not fetched while there is no room. One already
  fetched waits, or is deferred with a delay, instead of being refused, so
  no message is lost;
- a guest that tries to grow past the budget fails with its own error,
  not a kill of the process. For a route that is a 503 saying the handler
  may have partly run.

Per-service and per-route concurrency limits keep one service from taking
the whole budget or every execution slot.

## 1. What the platform does today

Read from `platform/src`, `worker/src`, `storage/src` and `testkit/src` on
`main`. Since v0.33.1, `main` has changed only crate versions, the licence
and dependency patches, wasmtime among them (36.0.16 to 36.0.19). v0.34.0
changes nothing described here.

### 1.1 Engine, stores and limits

- **Engine.** One `WasmEngine` per process: the platform builds one in
  `app/mod.rs`, and the worker one in `worker/src/main.rs`, from the same
  `WasmLimits::from_env()`.
  - Its `Config` sets only `epoch_interruption(true)`.
  - There is no pooling allocator, so linear memories come from wasmtime's
    on-demand allocator: `mmap`ed, and `munmap`ed when the store drops.
  - wasmtime's `threads` feature is not enabled, so no memory is shared.
- **Module cache.** A compiled module and its `InstancePre` are cached per
  `(service_id, version)` (`WASM_MODULE_CACHE_MAX`, 128).
- **Stores.** Every invocation builds a fresh `Store<HostState>` and
  instantiates from the `InstancePre` (`WasmEngine::execute`,
  `call_handler`). Nothing is pooled or reused across invocations.
- **Per-guest limits.** `GuestLimits` (`host/limits.rs`) is the store's
  `ResourceLimiter`.
  - `memory_growing` records the peak (`desired`) and compares it with the
    per-guest cap: `min(WASM_MAX_MEMORY_MB, tenant wasm_limits.memory_bytes)`,
    512 MiB by default.
  - Past the cap, the tenant's `wasm_limits` policy decides. `warn` (the
    default) allows the growth and counts it. `enforce` returns `Ok(false)`,
    so `memory.grow` yields `-1`, and records a `Denial`, which `classify`
    turns into `ExecutionError::MemoryLimit` (500).
  - The policy verdict is cached per store (`memory_verdict`).
  - Tables are capped at `WASM_MAX_TABLES` (8) of `WASM_MAX_TABLE_ELEMENTS`
    (1 000 000) elements. At 8 bytes an element that is up to 64 MiB a
    guest, charged nowhere.
- **When wasmtime calls the limiter.** It calls it when a memory is
  created as well as when it grows: `limit_new` calls
  `memory_growing(0, minimum, maximum)` at instantiation. When an approved
  growth then fails, it calls `memory_grow_failed(error)`, which is not
  told the size.
- **`classify`** attributes any non-interrupt error to a recorded `Denial`.
  That includes a failure that comes later and for another reason, after
  the guest handled the `-1`.
- **Deadlines.** An epoch ticker (`WASM_EPOCH_TICK_MS`, 100 ms) and
  `epoch_deadline_callback` interrupt a guest at its deadline, on
  cancellation, or at the shutdown hard deadline.
  - `WasmEngine::run` wraps the slot wait and the blocking task in
    `timeout(budget)`.
  - On timeout the caller gets its error at once. The blocking task, and
    the store with everything it holds, ends up to a tick later.

### 1.2 Concurrency

- One process-wide `Semaphore` of `WASM_MAX_CONCURRENT` (256) permits is
  acquired in `WasmEngine::run`, inside the budget.
- Execution runs on `spawn_blocking`. Both binaries use `#[tokio::main]`,
  so the blocking pool is tokio's default of 512 threads.
- A subscription's `consumer.concurrency` (1 when unset) is its pump's
  `slots` semaphore, acquired **before** each fetch. The pump keeps a
  fetched message in progress with heartbeats while it runs.
- On the worker: `MAX_CONCURRENT_JOBS` (4), `WORKER_MAX_CONCURRENT_SYNC`
  (32) and `WORKER_AFTER_HOOK_CONCURRENCY` (1).
- `HTTP_MAX_CONCURRENT_REQUESTS`, unset by default, sheds every request
  with `503 {"error":"server busy"}` and no `Retry-After`.
- No bound exists per service or per route.

### 1.3 How guests are invoked

Every path calls `WasmEngine::run` with an `Invocation`. The `kind` does
not tell the paths apart. `Hook` is used for both before-hooks and
after-hooks (`router/crud.rs:53`, `spawn_after_hook`). The worker runs its
request/reply routes and its after-hooks through one function with
`WorkerRoute` (`worker/src/main.rs`).

| Path | Kind | Where | What follows a failure |
|------|------|-------|------------------------|
| Custom route, `_fn` | `Route` | `router/custom/dispatch.rs::execute` | `map_execution_error`: every `ExecutionError` is a 500, a timeout included |
| CRUD before-hook | `Hook` | `router/crud.rs::execute_hook`, inline | the hook's error is the request's answer |
| CRUD after-hook (in process) | `Hook` | `spawn_after_hook`, after the commit, detached | logged at `warn` and dropped |
| `on_migrate` | `OnMigrate` | `deployer/schema.rs` | the deploy fails |
| Subscription | `Subscription` | `nats_manager/subscriptions.rs`, from the pump | `decide()` (`delivery.rs`): `ack: always` acks **every** outcome. `on_success` naks with backoff (spending a JetStream delivery), naks `Interrupted` with no delay, and gives up at `max_deliver`, by dead-letter or term |
| Long job | `Job` | worker `long_jobs.rs`, `router/jobs/state.rs::classify` | `MemoryLimit` is `HandlerError`. `Interrupted` calls `wasm_jobs::release`, which sets `attempts - 1` and `next_attempt_at = now()`, and naks at once |
| Worker request/reply | `WorkerRoute` | `worker_dispatch::execute_on_worker`, NATS request | `WasmJobResult { ok, response, error }`. The platform answers 500 |
| Worker after-hook | `WorkerRoute` | worker JetStream consumer `CUFFLINK_WASM_JOBS` | retried up to three deliveries, then dead-lettered |
| SSE reducer | `SseReducer` | `router/sse_bridge.rs` | the event is dropped (`reducer_error`) |

A **service call** from a guest on the platform is dispatched in process,
through `svc_service` (`LocalDispatch`). From the worker it goes over HTTP.

- Either way the callee is an ordinary `Route` invocation. The
  service-identity header is verified (`postures::internal`,
  `service_identity::verify`) into a `ServiceCaller` with a `depth`, at
  most `MAX_CALL_DEPTH` (4).
- `execute` re-parses the whole request into a `Value` only to read
  `auth` and `caller.depth`.
- The caller keeps its memory, its permit and its blocking thread while the
  callee runs.
- An after-hook triggered by a service call carries the call's depth. A
  job submitted from a nested route stores it too.

### 1.4 Memory the host allocates for guests

None of this passes through a `ResourceLimiter`.

**The request** (`router/custom/dispatch.rs`, `request.rs`):

- the raw body;
- the body parsed into a `Value` (`parse_body`);
- a clone of that tree for the audit record (`audit_body`);
- `body_raw_b64`, always encoded (`into_json`): the body again, at 4/3;
- the request `Value` itself;
- `request_str` (`execute`);
- the re-parse in `WasmEngine::execute`.

Most of these are held until the route returns, so peak host memory
during a large request is several times the body's size.

**Host calls:**

- **Inputs.** `read_guest_bytes` copies up to `WASM_MAX_GUEST_LEN_MB`
  (64) out of the guest per call, and most calls then parse the copy.
- **`host_response`** is a public field, assigned directly at about 40
  sites across `db`, `db_v2`, `http`, `storage`, `service_call`, `cache`,
  `nats`, `image`, `redis`, `jobs` and `util`.
  - `get_host_response` `take()`s the response, copies it into the guest,
    and puts it back.
  - So a response lives until the next host call replaces it, or until the
    store drops.
  - Every buffer is fully built before it is assigned.
- **`db_query`** has no row cap. It reads every row (`Vec<Row>`), renders
  them as a `Value` tree (`legacy_rows`), then serialises a `String`. A
  test pins that a host response is copied "however large".
  `db_query_read_only` is capped at `DB_READ_ONLY_MAX_ROWS`.
- **`s3_download`** reads the whole object (`get_bytes`: `bytes().to_vec()`,
  a second copy) and base64-encodes it.
  - `s3_download_many` does that for any number of items, 32 at a time
    (`HOST_FANOUT_CONCURRENCY`).
  - It holds every result until `join_all`, then builds a `Value` per item
    and a `String` of them all.
- **`http_fetch_many`** takes any number of items. Each response is capped
  at `HOST_HTTP_MAX_RESPONSE_BYTES` (10 MiB), and all of them are held until
  `join_all`.
- **`image_transform` and `_multi`** download the source whole, with no
  size cap.
  - They decode it with no `image::Limits` (a `DynamicImage`, then
    `to_rgb8`), resize, and hold every encoded output.
  - All of this runs on a further `spawn_blocking` thread.

**The response:**

- `read_response` copies it out of the guest (`to_vec`, at most
  `WASM_MAX_RESPONSE_MB`, 64).
- The `String` outlives the store.
- `guest_response::decode` parses it into a `Value` and clones `__body`.

**The allocator.** The binaries use glibc's allocator (Debian base image,
no global allocator). Its dynamic mmap threshold rises to 32 MiB once a
large block is freed, so freed host buffers below it may stay resident
in the process's arenas.

### 1.5 The gap

Every limit above applies to one guest, or counts executions. Nothing
bounds the memory of all guests together:

- at the defaults `WASM_MAX_CONCURRENT × WASM_MAX_MEMORY_MB` is
  256 × 512 MiB = 128 GiB of linear memory alone;
- under the default `wasm_limits: warn` even the 512 MiB per guest is not
  enforced;
- the host buffers of section 1.4 add a multiple of what the guests read.

A handful of concurrent invocations of one handler that reads without a
bound can therefore pass a container limit of a few GiB, each within its
own cap. The kernel then kills the process: every service and in-flight
request of every tenant on it, not only the invocations that grew.

## 2. Accounting

**Decision: a process-wide ledger of bytes.** Each invocation's `Charge`
is debited before memory is allocated on its behalf, and credited when that
memory is released. The `Charge` lives as long as the memory does, which
can be after the store.

### 2.1 The ledger

- One `Mutex<LedgerState>`, poison-tolerant like the module cache
  (`unwrap_or_else(PoisonError::into_inner)`). Every ledger operation is a
  handful of integer updates, with no I/O and no allocation under the lock.
- It holds:
  - the budget;
  - bytes in use, in total, per class and per `(tenant, service)`;
  - the grant queue of admission waiters (section 3.6).
- It is touched at admission, at each grow and host allocation, and at
  release, never per instruction.

### 2.2 What a `Charge` holds

A `Charge` sits in `GuestLimits`, inside `HostState`, and records:

| Part | Debited | Credited |
|------|---------|----------|
| **Reservation** | at admission, before anything is allocated: the handler's estimate (section 2.5) plus the request's host cost (section 2.4) | when the `Charge` drops |
| **Linear growth** | `memory_growing(current, desired)`: `desired - current`, first from the reservation, then from the ledger's free bytes. The pending amount is remembered | `memory_grow_failed` credits the pending amount. The rest is credited at drop |
| **Tables** | `table_growing`: `(desired - current) × 8` bytes, the same way | as above, with `table_grow_failed` |
| **Host buffers** | through `HostState::set_host_response`, which debits the new length and credits the replaced one in one ledger operation. Every other host allocation is reserved before it is made (section 2.4) | on replacement, or when the `Charge` drops; never on read |
| **Host-call input** | `read_guest_bytes`: the copied length times the call's declared parse multiple, before copying | when the host call returns |

**`host_response` becomes private.** Every site goes through
`set_host_response`. `get_host_response` copies from the buffer in place,
borrowing guest memory and host state together through
`Memory::data_and_store_mut` instead of `take()` and put back. So a
response is debited once, while it exists, and never credited by a read.

**Drop credits everything.** It runs on every path out of an
invocation: success, trap, deadline, cancellation, a host function's panic
unwinding through `spawn_blocking`.

- The store drops when the blocking task ends, not when `run`'s timeout
  returns. So the ledger stays charged until the memory is actually freed.
- The guest's response is the exception. `call_handler` moves the
  `Charge`'s response part into a `ResponseCharge` of
  `len × RESPONSE_HOST_MULTIPLE`, returned beside the response `String`.
  The router drops it once the HTTP response is rendered, or the
  subscription or job has settled.

Growth is an upper bound on resident linear memory: pages grown but never
touched are not resident, so the ledger over-counts slightly, in the safe
direction.

### 2.3 Alternatives considered

| Option | Why not |
|--------|---------|
| Reserve the per-guest cap (512 MiB) at admission | A 1 GiB budget would admit one guest at a time while typical handlers peak at a few MiB: the protection would become an outage of its own |
| Count only actual growth, with no reservation | Simultaneous arrivals all see the same free bytes and are all admitted. A burst of heavy invocations then fails mid-run instead of waiting at admission |
| Measure resident memory (RSS, cgroup) instead of a ledger | Sampled and lagging, unattributable to a service, and too slow to read inside `memory_growing`. Kept as a backstop only (section 3.8) |
| Count linear memory only | Misses section 1.4, where the host holds several copies of what a guest reads |
| Debit host buffers after they are built | The memory exists before the ledger can refuse it. Section 2.4 reserves before allocating |
| `tokio::sync::Semaphore` with a permit per MiB as the ledger | Its fairness assigns released permits to the head waiter, so a running guest's growth would fail in favour of a request not yet admitted |

### 2.4 Host allocations: caps, and reserving before allocating

Each path of section 1.4 either reserves exactly before allocating, or is
capped and reserved at a declared multiple of its size.

- A multiple is a named constant per path, such as
  `REQUEST_HOST_MULTIPLE`.
- Each multiple is pinned by a test that measures the path's peak host
  allocation with a counting global allocator, and fails when the measure
  exceeds the constant (section 8).

**The mode governs every cap**, as it governs the budget (section 5.1):

- `enforce` refuses past a cap;
- `warn` counts and logs what it would refuse, and lets the call proceed
  as today;
- `off` evaluates nothing.

So the test that pins a host response copied "however large" holds under
`off` and `warn`. The rewrites that change only how a result is built (the
streaming render, the request and response changes below) apply in every
mode. They must therefore produce byte-identical output: golden tests in
section 8 compare old and new.

Under `enforce`, a cap or a refused reservation answers the guest with the
call's existing failure, and no import gains an error kind an older SDK
cannot read:

- `-1` for the legacy imports;
- `TooLarge` in the v2 envelopes of `http`, `storage`, `ext` and `nats`;
- for `db_v2`, a new `DbErrorKind::TooLarge`, which older SDKs read as
  `Other` (the enum has `#[serde(other)]`).

| Path | Change |
|------|--------|
| Every host response | `WASM_MAX_HOST_RESPONSE_MB` (64; question 10), enforced inside each path as the buffer grows, not after it is built |
| `db_query`, `db_v2` results | Rows are streamed (`query_raw`) and rendered straight to text, row by row, with no `Value` tree. A running byte count is reserved in steps of 1 MiB. At the cap, or when a step cannot be reserved, the statement is cancelled and the call fails. `DB_READ_ONLY_MAX_ROWS` still applies to read-only queries |
| `s3_download` | `head` first. An object whose base64 would pass the response cap (above 48 MiB) is refused before downloading. Otherwise its size × `S3_HOST_MULTIPLE` is reserved, and the body is read into one buffer and encoded with no `to_vec` copy |
| `s3_download_many`, `http_fetch_many` | At most `HOST_FANOUT_MAX_ITEMS` (128) items. Each item reserves its size (`head`, or `HOST_HTTP_MAX_RESPONSE_BYTES` for HTTP) before it starts. Results are appended to the response text as they finish, not held until `join_all`. The total stays under the response cap |
| `image_transform`, `_multi` | Source objects of at most `IMAGE_MAX_INPUT_MB` (32). `image::Limits` with `max_image_width` and `max_image_height` (16 384) and `max_alloc` from the reservation. The header's dimensions are read before decoding. Then `width × height × IMAGE_HOST_MULTIPLE` is reserved (decoded, RGB and resized copies), and each output as it is encoded. Images over `IMAGE_MAX_PIXELS` (40 megapixels) are refused |
| Host-call inputs | Reserved at copy time (section 2.2). The parse multiple is 1 for byte payloads and `JSON_PARSE_MULTIPLE` for parsed JSON |
| The custom-route request | `execute` no longer re-parses the request: the caller passes `auth` and `caller.depth` in the `Invocation`. The audit record keeps the raw body, not a cloned tree, and parses it when the record is written, after the guest has run. The request `Value` is dropped as soon as `request_str` is built. `body_raw_b64` stays, because guests read it. What remains is reserved before the body is read, at `body length × REQUEST_HOST_MULTIPLE` (section 3.7) |
| The guest's response | `guest_response::decode` takes `__body` out of the parsed map instead of cloning it. The `ResponseCharge` (section 2.2) covers the `String` and the parsed tree until the response is rendered |

### 2.5 The estimate

The reservation at admission is the handler's estimate.

- **Samples.** One per finished invocation of a `(service_id, handler)`:
  the `Charge`'s peak. A denied invocation's sample is the size it asked
  for: the `desired` of its denied grow, or the length of its refused
  host allocation, plus what it held. Exits, denials, traps and deadlines
  all count.
- **Estimate.** The largest sample of the last 16 that is younger than
  `WASM_ESTIMATE_MAX_AGE_SECS` (600). Older samples decay away, so a
  handler that has stopped being heavy stops being treated as heavy.
- **Cold handler.** One with no sample in this process (first call, after a
  restart, after decay) reserves
  `max(module initial memory + 1 MiB, WASM_MEMORY_COLD_ESTIMATE_MB)`. The
  default is 32 MiB, a conservative seed until the handler is measured.
- **Cap.** The per-guest cap, as enforced (section 2.6).
- **Map.** `WASM_ESTIMATE_MAP_MAX` (4096) entries, sized apart from the
  module cache, least recently used evicted. The key leaves out the
  version, so a new deploy inherits the last version's estimate.

A handler whose need jumps fails its first heavy invocation mid-run, if
the budget is full. Its denial sample then makes later invocations wait
at admission. A manifest hint declaring a handler's need is left for
later (section 10).

### 2.6 When a grow or a host allocation would pass the budget

**Decision: deny at once, never block inside the limiter or a host call.**

- **The grow.** `memory_growing` returns `Ok(false)`, as the per-guest cap
  does under `enforce`. It records a `BudgetDenial { reason, requested }`,
  where `reason` is `budget` or `service_share`.
  - The budget is checked on every grow. The per-guest policy verdict
    cache (`memory_verdict`) is not used for it, since the ledger changes
    between grows.
- **A host allocation.** The host call fails with its existing failure
  (section 2.4). The guest sees `-1` or the envelope's error, not a 503,
  and decides what to do with it. Its denial is recorded the same way.
- **Attribution.** `classify` attributes an outcome to a recorded denial
  only when the invocation ends in a wasm trap: a Rust guest's failed
  allocation aborts with `unreachable`. Such an outcome becomes
  `ExecutionError::Overloaded`. A guest that handled the failure and
  answered keeps its answer, and the denial is still counted and sampled.
  A later failure of another kind (a deadline, a host error) is that
  failure.
- **Too large to ever fit.** A denial whose request exceeds what the
  service may ever hold (its share) cannot be cured by waiting. It
  becomes `ExecutionError::TooLarge`, answered like `MemoryLimit` with a
  500 naming the service share. A job classifies it `HandlerError`, and a
  subscription a handler failure.

**Rejected: blocking until other guests release memory.**

- The limiter runs synchronously on a blocking thread that holds an
  execution slot, maybe open `db_v2` transactions (rows locked), and the
  caller's connection.
- Two guests each waiting on the other's memory would deadlock until
  their deadlines, holding all of that.
- `ResourceLimiterAsync` needs wasmtime's `async` feature and async calls
  into every guest, a far larger change. It removes neither the cycle nor
  the held locks.

Waiting belongs at admission.

**The per-guest cap.** Its check runs first, exactly as today: same cap,
same `wasm_limits` policy, same `MemoryLimit` (500). Only growth that
passes it is charged.

- Under `WASM_MEMORY_BUDGET_MODE=enforce`, a per-guest cap above the
  service share of the budget is clamped to it at startup, with a warning
  naming both values.
- Under `warn` and `off` the cap is not clamped. Startup only logs what
  `enforce` would clamp it to.

## 3. Admission

### 3.1 The class of an invocation

**`Invocation` gains an explicit `class`, set by each call site.** It is
not derived from `kind`, which does not distinguish the paths (section
1.3). When more than one applies, precedence is
`committed > background > nested > interactive`.

| Class | Call sites |
|-------|------------|
| `committed` | CRUD after-hooks run in process (also when a service call triggered them), `on_migrate` |
| `background` | subscriptions, long jobs (whatever depth they were submitted at), worker after-hooks |
| `nested` | an invocation whose `ServiceCaller`, built from the **verified** service-identity header, has `depth > 0`. Never from the request body's `caller` field or anything else a client sends; a request without a verified identity is never nested |
| `interactive` | custom routes, `_fn`, CRUD before-hooks, worker request/reply, SSE reducers |

### 3.2 What admission takes

All waits are bounded by one **admission deadline** per invocation:

- `interactive`: `WASM_ADMISSION_WAIT_MS` (1000), inside the invocation's
  budget;
- `nested`: none, since a nested invocation never waits;
- `committed`: its budget;
- `background`: section 3.5, outside the handler's budget.

Steps are taken in order, each held while the next is awaited, and all
are released if the deadline passes:

1. **Route slot**, when the route declares `max_concurrent`. Enforced in
   every mode: the author opted in, and it is not a soak target.
2. **Service slot** (`WASM_MAX_CONCURRENT_PER_SERVICE`, 32).
3. **Global slot** (`WASM_MAX_CONCURRENT`, 256).
4. **Memory reservation**: the estimate, plus the request's host cost
   where section 3.7 has not reserved it already.

**Holding slots while waiting for memory.**

- An interactive waiter holds a slot for at most the 1 s deadline. That
  is the inversion accepted for it: the slot it holds is one it would use
  a moment later.
- Background work takes its memory reservation **before** any slot
  (section 3.5). Background work may also hold at most half of a
  service's slots and 75 % of the global slots, so a backlog never fills
  the slots that routes need.

The result is an `Admission`: the slots plus the `Charge`. It moves into
the blocking task and drops with the store.

### 3.3 Nested invocations

- **Slots.** Route and service slots are tried with `try_acquire`, never
  awaited. If either is full, the call fails at once with `Overloaded`,
  and the caller's guest receives a 503 it can handle.
  - A service that calls itself with every slot taken is refused rather
    than deadlocked.
  - The global slot is replaced by a separate nested pool of
    `WASM_MAX_CONCURRENT × MAX_CALL_DEPTH`, also only tried. The caller
    already holds a global slot, and a chain must not wait on its own
    callers.
- **Memory.** A nested invocation's reservation counts toward its
  service's share, since it is that service's memory. It may use the
  growth headroom above `WASM_MEMORY_ADMIT_PERCENT`: its caller is already
  running, and finishing the chain frees more than refusing it. It never
  waits.
- **Blocking threads.** Each binary builds its runtime with
  `max_blocking_threads = WASM_MAX_CONCURRENT × (1 + MAX_CALL_DEPTH) + 64`,
  instead of tokio's default of 512. That covers every slot's chain, plus
  compilation and image work. A chain can then never wait for a thread
  that its own callers hold.

### 3.4 Shares and headroom

- **Service share.** One `(tenant, service)` may hold at most
  `WASM_MEMORY_SERVICE_SHARE_PERCENT` (50) of the budget, all classes
  together. A grow or reservation past it is denied with `service_share`,
  even while the budget has room.
- **Background share.** Background work may hold at most
  `WASM_MEMORY_BACKGROUND_SHARE_PERCENT` (75) of the budget at admission.
- **Admit threshold.** New reservations (nested excepted) are granted only
  while the ledger stays under `WASM_MEMORY_ADMIT_PERCENT` (90). The rest
  is headroom for invocations already running, which are cheaper to
  finish than to fail.

### 3.5 Consumers and jobs

A refusal after the fetch would lose or spend the message: `ack: always`
acks every outcome, `Interrupted` naks with no delay, and `Retry` spends a
delivery. Background admission therefore works around the fetch.

**Before the fetch: a headroom check, not a reservation.** A pump's
fetch is a long poll that is outstanding almost all the time. A
reservation held across it would keep an idle durable's
`concurrency × estimate` away from routes.

- A subscription pump, after taking its pump slot, waits until the
  background share has room for one estimate of its handler (its
  `SubscriptionSpec` names the handler).
- It fetches at most as many messages as estimates fit at that moment.
- The long-jobs consumer, whose one durable carries every handler, waits
  for `WASM_MEMORY_JOB_FLOOR_MB` (32).
- The worker's after-hook consumer does the same as a subscription pump.
- The wait has no bound, but watches `halted`, so it stops for a stop or a
  shutdown as today.

**After the fetch: a reservation that may wait.**

- Each message or job reserves before its invocation starts, and then
  takes its slots.
- The wait is outside the handler's budget. Heartbeats (`AckProgress`, or
  the job's lease) keep it in progress, so it spends no delivery and no
  attempt.
- **Aging.** A background waiter older than
  `WASM_BACKGROUND_AGING_SECS` (30) is granted before interactive waiters
  (section 3.6). Large reservations therefore cannot be starved by a
  steady flow of small interactive ones.

**Denied mid-run.**

- **Subscriptions.** A capacity denial (`Overloaded`) does not settle the
  message.
  - The pump keeps it in progress, drops the invocation's `Charge`, waits
    for a reservation of the denied size, and runs the handler again.
  - It does this up to `WASM_CAPACITY_RETRIES` (3) times in a row. That
    spends no delivery and works under both ack modes.
  - After the last retry the outcome is the new `Outcome::Deferred
    { after }`. `decide()` answers it `Nak(after)` under **both**
    `ack: always` and `on_success`, with `after` backing off from
    `Retry-After` up to 60 s.
  - That one nak spends a JetStream delivery. Under a declared
    `max_deliver`, a message deferred at its last delivery is dead-lettered
    with reason `capacity` (or termed, without `dead_letter`) rather than
    silently abandoned by the server.
  - Without `max_deliver`, the JetStream default, deferrals are unbounded,
    and so is the delay between them.
  - Re-running a handler that has partly run is the redelivery contract of
    `on_success`. **Under `ack: always` it is new.** Today such a
    subscription runs a message at most once, since every outcome is acked.
    With this change, a message denied for capacity mid-run may run again,
    so a capacity denial turns at-most-once into possibly-twice for that
    message. The alternative, acking it and losing it, is what this change
    exists to avoid. Section 7 and the release notes state it.
- **Jobs.** A capacity denial is the new `Attempt::Deferred(retry_after)`,
  not `Interrupted`.
  - The job is released with `attempts` unchanged and
    `next_attempt_at = now() + retry_after`. The worker naks with the same
    delay, so it neither spends an attempt nor loops hot.
  - After `WASM_CAPACITY_RETRIES_JOB` (10) consecutive deferrals the next
    one counts as a failed attempt of class `Capacity`, retried under the
    job's own policy. A job that can never fit is thereby bounded by its
    attempts.
  - `cufflink_wasm_job_deferrals_total` counts every deferral.
- **Worker after-hooks** are deferred like subscriptions on the worker's
  ledger.

### 3.6 Granting

Waiters are kept in a grant queue under the ledger lock, not woken
together.

- On every credit, the ledger walks the queue in order:
  1. committed;
  2. background waiters past their aging;
  3. interactive;
  4. other background.
- It grants every waiter whose reservation fits, within its class's
  limits, and signals each granted waiter through its oneshot.
- A waiter that does not fit does not block one behind it that does: there
  is no head-of-line blocking.
- Growth of running invocations never queues: it takes free bytes
  directly, above the admit threshold included.

### 3.7 Routes: reserve the body, then read it, then take slots

A custom route's body is host memory. The order in
`router/custom/dispatch.rs`, after authorization:

1. **Reserve the request's host cost** before reading the body:
   `body length × REQUEST_HOST_MULTIPLE`, where the length is
   `Content-Length`, or the route's `body_limit` for a chunked body.
   - Once the body has been read, the reservation shrinks to the actual
     length.
   - A slow upload therefore holds only the memory its body needs, and no
     slot.
2. **Read the body.**
3. **Take** the route, service and global slots and the handler's
   estimate (section 3.2), within what is left of the one admission
   deadline.

The `Admission` is passed into `execute`, and `WasmEngine::run` uses it
instead of admitting again.

A worker-routed route reserves its request on the platform, which reads the
body, and admits the guest on the worker, where it runs.

### 3.8 The pressure gate

Host memory the ledger cannot see stays: tokio, connection pools,
compiled modules, CRUD lists, and the transient work section 2.4 bounds
by multiples. It is covered by the host reserve (section 5) and by a gate.

- Where cgroup v2 is mounted, a task reads the process cgroup's
  `memory.stat` every 250 ms.
- It takes the non-reclaimable part: `anon + shmem + kernel` where
  `kernel` is reported, else `memory.current − active_file −
  inactive_file`.
- While that is above `limit − reserve / 2`, new interactive and background
  reservations wait as if the budget were full (`reason="pressure"`).
- Running invocations, committed and nested ones are not gated.

## 4. Errors and answers

New: `ExecutionError::Overloaded { reason, retry_after, ran: bool }`,
labelled `overloaded`, and `ExecutionError::TooLarge`, labelled
`too_large`. `reason` is one of `memory`, `service_share`,
`service_concurrency`, `route_concurrency` or `pressure`.

| Path | Refused at admission (nothing ran) | Denied mid-run (`ran: true`) |
|------|-----------------------------------|------------------------------|
| Route, `_fn`, before-hook | `PlatformError::RetryLater`: 503, `Retry-After: WASM_ADMISSION_RETRY_AFTER_SECS` (2), `{"error": "guest capacity exhausted; the request was not run"}` | 503 with `Retry-After`, `{"error": "guest capacity exhausted while the request ran; it may have been partly applied"}` |
| Service call (nested) | the callee's 503 reaches the caller's guest as the call's response | the same |
| Worker request/reply | `WasmJobResult` gains `retry_after_ms: Option<u64>` and `ran: Option<bool>` (`serde(default)`, skipped when `None`). The platform answers as above when they are set. An older platform ignores them and answers 500; an older worker never sets them | the same |
| After-hook in process, `on_migrate` | never refused at admission | logged like any hook failure; `on_migrate` fails the deploy as any error does |
| SSE reducer | waits at most the reducer's budget, then the event is dropped and counted `reducer_error`, as any reducer failure is today | the same |
| Subscription, job, worker after-hook | never refused (section 3.5) | section 3.5 |

Every `ExecutionError` is a 500 today, a timeout included. The maintainer
chose 503 for a mid-run denial (question 8). Its message says the handler
may have partly run, and the refusal before running says it did not, so a
client can tell the two apart. `MemoryLimit` and `TooLarge` stay 500: the
guest's own size, which no retry cures.

## 5. Configuration and defaults

All process-wide, read by the platform and the worker alike. Each process
uses its own cgroup.

### 5.1 Mode

`WASM_MEMORY_BUDGET_MODE`: `off`, `warn` or `enforce`, default `warn`
(question 1), in the vocabulary of [policies](../policies.md).

- **`off`.** No ledger and no admission beyond `WASM_MAX_CONCURRENT`,
  exactly today.
- **`warn`.** Changes metrics and logs only. The ledger, shares, slots and
  pressure gate are all evaluated. What would wait, be refused, be denied
  or be clamped is counted (`mode="warn"`) and logged, and allowed.
  Nothing waits. The per-guest cap is not clamped.
- **`enforce`.** As this note describes.

The mode governs everything new here, the host caps of section 2.4 and
`WASM_MAX_CONCURRENT_PER_SERVICE` included: under `warn` they are counted
and logged, and enforced only under `enforce`. There are two exceptions:

- **A route's declared `max_concurrent`** is enforced in every mode. The
  service's author opted into it, no existing manifest has the key, and it
  is not a soak target.
- **The byte-identical rewrites** of section 2.4 apply in every mode,
  because they change no output.

The mode is process-wide rather than a tenant policy (question 2). One
tenant left in `warn` could otherwise still grow the shared process into
the kernel's limit.

### 5.2 The budget

`WASM_MEMORY_BUDGET_MB`, when set, is the budget. A malformed value or
`0` refuses startup.

When unset, the budget is `limit − reserve`:

- **The limit.**
  - **cgroup v2.** Resolve the process's cgroup from `/proc/self/cgroup`
    (the `0::<path>` line), and take the smallest `memory.max` and
    `memory.high` that is not `max` over that cgroup and each ancestor up
    to the mount root. `memory.high` counts because above it the kernel
    throttles and reclaims hard.
  - **cgroup v1.** `hierarchical_memory_limit` from the memory
    controller's `memory.stat`, which already folds in the ancestors. A
    value at or above `MemTotal` means unlimited.
  - **No limit** (no cgroup, `max` all the way up, macOS). The budget is
    off whatever the mode, logged once.
- **The reserve.** `WASM_MEMORY_HOST_RESERVE_MB`, default
  `max(256, 30 % of limit)` (question 3).
- **A derived budget that is zero or negative** (a very small limit, a
  large reserve) turns the budget off with a warning. Only an explicit
  `WASM_MEMORY_BUDGET_MB=0` refuses startup.

Startup logs the limit, its source file, the reserve, the budget, the mode
and the shares once, at `info`.

**The allocator.** On glibc, startup calls
`mallopt(M_MMAP_THRESHOLD, 1 MiB)`, which also turns off the dynamic
threshold. Every host buffer of a mebibyte or more is then `mmap`ed and
returned to the kernel when freed, so the ledger's credits for large host
buffers are real reductions of resident memory. Smaller blocks fragment as
they do today, inside the reserve. `MALLOC_MMAP_THRESHOLD_` in the
environment still overrides it.

### 5.3 Variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `WASM_MEMORY_BUDGET_MODE` | `warn` | Section 5.1 |
| `WASM_MEMORY_BUDGET_MB` | derived | Section 5.2 |
| `WASM_MEMORY_HOST_RESERVE_MB` | `max(256, 30 % of limit)` | Section 5.2 |
| `WASM_MEMORY_SERVICE_SHARE_PERCENT` | `50` | Section 3.4 |
| `WASM_MEMORY_BACKGROUND_SHARE_PERCENT` | `75` | Section 3.4 |
| `WASM_MEMORY_ADMIT_PERCENT` | `90` | Section 3.4 |
| `WASM_MEMORY_COLD_ESTIMATE_MB` | `32` | Section 2.5 |
| `WASM_ESTIMATE_MAX_AGE_SECS` / `WASM_ESTIMATE_MAP_MAX` | `600` / `4096` | Section 2.5 |
| `WASM_MEMORY_JOB_FLOOR_MB` | `32` | Section 3.5 |
| `WASM_BACKGROUND_AGING_SECS` | `30` | Section 3.5 |
| `WASM_CAPACITY_RETRIES` / `WASM_CAPACITY_RETRIES_JOB` | `3` / `10` | Section 3.5 |
| `WASM_MEMORY_PRESSURE_GATE` | `true` | Section 3.8; ignored without cgroup v2 |
| `WASM_MAX_CONCURRENT_PER_SERVICE` | `32` | At most `WASM_MAX_CONCURRENT` (question 7) |
| `WASM_ADMISSION_WAIT_MS` | `1000` | Section 3.2 (question 6) |
| `WASM_ADMISSION_RETRY_AFTER_SECS` | `2` | Section 4 (question 6) |
| `WASM_MAX_HOST_RESPONSE_MB` | `64` | Section 2.4 (question 10) |
| `HOST_FANOUT_MAX_ITEMS` | `128` | Section 2.4 |
| `IMAGE_MAX_INPUT_MB` / `IMAGE_MAX_PIXELS` | `32` / `40000000` | Section 2.4 |

A malformed value or a zero refuses startup, as `WasmLimits::validate`
does today. Warnings and the per-denial logs go through
`policy::record::LogThrottle`: at most once a minute per
`(tenant, service, reason)`.

### 5.4 Manifest: per-route concurrency

- **Field.** `CustomRouteDefinition.max_concurrent: Option<u32>`, with
  `skip_serializing_if = "Option::is_none"`, so existing manifests keep
  their canonical form and deploy hash.
- **Validation.** 1 to 1024. Refused together with `job` and with `sse`.
- **SDK.** `max_concurrent = 4` in the `service!` route tuple, beside
  `body_limit` and `timeout`.
- **Feature.** Derived as `route_concurrency`, not `route_limits`. A
  platform advertising only `route_limits` would accept the manifest and
  drop the key.
- **Enforcement.** In every mode, so the feature is always advertised.

Subscriptions keep `consumer.concurrency`; service slots apply on top.

## 6. Metrics, logs and alerts

New metrics, platform and worker alike:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cufflink_wasm_memory_budget_bytes` | gauge | — | The budget in force (0 when off) |
| `cufflink_wasm_memory_in_use_bytes` | gauge | `class` | Ledger charges: reservations, growth, tables, host buffers, response charges |
| `cufflink_wasm_memory_service_bytes` | gauge | `tenant`, `service` | The same per service; set at admission, at release and at scrape |
| `cufflink_wasm_admission_waiters` | gauge | `class` | Waiters in the grant queue |
| `cufflink_wasm_admission_oldest_wait_seconds` | gauge | `class` | Age of the oldest waiter; for `background`, starvation shows here |
| `cufflink_wasm_admission_wait_seconds` | histogram | `class`, `result` (`admitted`, `refused`) | Arrival to admission. Replaces `cufflink_wasm_queue_wait_seconds`, kept one release as an alias |
| `cufflink_wasm_admission_refused_total` | counter | `tenant`, `service`, `class`, `reason`, `mode` | Refusals, or would-be refusals under `warn` |
| `cufflink_wasm_memory_denied_total` | counter | `tenant`, `service`, `kind`, `reason`, `what` (`grow`, `table`, `host`), `mode` | Denials past the budget or a share |
| `cufflink_wasm_host_cap_total` | counter | `path`, `cap` | Host calls refused by a cap of section 2.4 |
| `cufflink_wasm_capacity_retries_total` | counter | `kind` | In-place re-runs of subscriptions and worker after-hooks |
| `cufflink_wasm_job_deferrals_total` | counter | `result` (`deferred`, `counted`) | Job capacity deferrals |
| `cufflink_wasm_estimate_bytes` | histogram | `kind` | Reservations taken |
| `cufflink_process_memory_limit_bytes` | gauge | `source` (`max`, `high`, `v1`) | The limit in force |
| `cufflink_process_memory_current_bytes` | gauge | — | cgroup `memory.current` |
| `cufflink_process_memory_unreclaimable_bytes` | gauge | — | What the pressure gate reads |
| `cufflink_process_memory_events_total` | counter | `event` (`high`, `max`, `oom`, `oom_kill`) | cgroup `memory.events`, as last read |
| `cufflink_pump_admission_wait_seconds` | histogram | `kind` | A pump's wait for headroom before a fetch |

Existing metrics:

- `cufflink_wasm_handler_duration_seconds{result}` gains `overloaded` and
  `too_large`.
- `cufflink_wasm_limit_events_total{limit}` gains `budget`.
- `cufflink_wasm_inflight` counts from admission.

**Logs.** A `warn` under target `cufflink::wasm::budget`, naming the
tenant, service, handler, class, reason and the ledger's numbers, throttled
as section 5.3 says.

**Alerts.** This change adds `infra/monitoring/alerts.yml`, loaded by the
vendored `prometheus.yml` and checked with `promtool check rules` in the
observability CI job. `docs/observability.md` gains an "Alerts" section
describing each rule:

| Alert | Expression | For |
|-------|------------|-----|
| `CufflinkGuestMemoryHigh` | `sum without (class) (cufflink_wasm_memory_in_use_bytes) / cufflink_wasm_memory_budget_bytes > 0.8` | 5 m |
| `CufflinkGuestAdmissionRefused` | `increase(cufflink_wasm_admission_refused_total{mode="enforce"}[5m]) > 0`; under `warn` the same with `mode="warn"` is the soak signal | 0 m |
| `CufflinkGuestMemoryDenied` | `increase(cufflink_wasm_memory_denied_total[5m]) > 0` | 0 m |
| `CufflinkBackgroundStarved` | `cufflink_wasm_admission_oldest_wait_seconds{class="background"} > 300` | 5 m |
| `CufflinkGuestLedgerLeak` | `sum by (job, instance) (cufflink_wasm_memory_in_use_bytes) > 0 and on (job, instance) sum by (job, instance) (cufflink_wasm_inflight) == 0` | 10 m |
| `CufflinkProcessMemoryPressure` | `cufflink_process_memory_unreclaimable_bytes / cufflink_process_memory_limit_bytes > 0.9` | 5 m |
| `CufflinkProcessOomKill` | `increase(cufflink_process_memory_events_total{event="oom_kill"}[10m]) > 0` | 0 m |

## 7. Compatibility

- **Below the budget, nothing changes in what a guest observes.**
  - `off` is exactly today, and `warn` changes metrics and logs only. The
    host caps and the per-service slot limit follow the mode.
  - A route's declared `max_concurrent` is enforced in every mode, but only
    a manifest that adds the key has one.
  - The streaming render and the request and response rewrites run in
    every mode, pinned byte-identical by golden tests.
  - Under `enforce`, the host caps can refuse a call that succeeds today,
    and the release notes say which.
  - **`ack: always` subscriptions.** Under `enforce`, a message denied for
    capacity mid-run is re-run or deferred rather than acked. It may
    therefore run twice where today it runs at most once (section 3.5).
- **Error kinds.** No new error kind reaches an SDK that cannot read it.
  The legacy imports keep `-1`, the v2 envelopes reuse `TooLarge`, and
  `db_v2`'s new `TooLarge` reads as `Other` in older SDKs.
- **Features.** `guest_memory_budget` is advertised and never demanded.
  `route_concurrency` is derived from `max_concurrent`, and a platform
  without it refuses such a deploy with a 422.
- **Platform and worker skew.** `retry_after_ms` and `ran` are optional in
  both directions.
- **Rollback.**
  - A platform rolled back below the release ignores the new variables,
    runs today's single semaphore, and lifts the host caps.
  - It drops `max_concurrent` from a service already deployed with it: the
    service runs without the limit there and cannot be deployed there.
  - No database migration and no tenant policy row is involved.
- **wasmtime.** The design depends only on `ResourceLimiter`'s
  `memory_growing`, `memory_grow_failed`, `table_growing` and
  `table_grow_failed`, as wasmtime 36 defines them, and on
  `Memory::data_and_store_mut`. The open update to wasmtime 50 does not
  build yet. Whichever lands first, check these against wasmtime 50.
  `threads` stays off: a test asserts that a module with a shared memory is
  refused (section 8).

## 8. Tests

**Unit** (`wasm_runtime`, `host`):

- **Ledger.**
  - Reservation, growth and tables are credited, and `memory_grow_failed`
    credits the pending amount.
  - `set_host_response` debits the new buffer and credits the old.
    `get_host_response` called twice credits nothing.
  - `Drop` credits everything after a trap, a deadline, and a host
    function that panics (the ledger returns to zero).
  - The ledger stays charged after `run`'s timeout returns, until the
    blocking task ends.
  - A thousand mixed invocations leave it at zero.
  - A poisoned lock is recovered.
- **Shares and granting.**
  - A service at its share is denied while the budget has room.
  - Background stops at its share and its slot fractions.
  - The admit threshold leaves headroom, which nested invocations may use.
  - The grant queue order, with aging.
  - No head-of-line blocking.
  - Waiters past their deadline release everything they hold.
- **Nested.** Nested invocations never wait. A service calling itself
  with every slot taken is refused at once.
- **Estimates.**
  - The cold seed, and the maximum of the last sixteen.
  - Decay past the maximum age.
  - Denial samples use the requested size.
  - The map's own bound, and inheritance across versions.
- **Classes.** Every call site's class, and the precedence (an after-hook
  triggered by a service call is `committed`).
- **Attribution.**
  - A trap after a denial is `Overloaded`.
  - A guest that handles `-1` and answers keeps its answer.
  - A denial larger than the service share is `TooLarge`.
- **`memory_growing` never blocks.** A WAT guest growing past the budget
  gets `-1` at once, under `enforce`. Under `warn` the grow succeeds and is
  counted.
- **Configuration**, from fixture trees:
  - cgroup v2 path resolution from `/proc/self/cgroup`, and the minimum
    over ancestors;
  - `memory.high` below `memory.max`;
  - v1 `hierarchical_memory_limit`, and a value at `MemTotal`;
  - no cgroup at all;
  - a zero or negative derived budget turning the budget off with a
    warning, while an explicit `0` refuses startup;
  - the clamp under `enforce` only;
  - malformed values.
- **Delivery and jobs.**
  - `decide()` naks `Deferred` under both ack modes, and dead-letters it at
    `max_deliver`.
  - `classify` makes `Overloaded` `Deferred`, and makes the eleventh
    consecutive deferral a counted `Capacity` failure.
- **Errors.** Admission refusal and mid-run denial give their two 503
  messages, both with `Retry-After`. `MemoryLimit` gives the same 500 as
  today.
- **Host multiples.** A test binary with a counting `#[global_allocator]`
  measures the peak host allocation of each path in section 2.4 and fails
  when it exceeds its declared multiple:
  - a 16 MiB request through a custom route;
  - `db_query` over a large result;
  - `s3_download` and `_many`;
  - `http_fetch_many`;
  - an image transform;
  - a 64 MiB response.
- **`threads` stays off.** A module with a shared memory fails to compile.
- **Byte-identical rewrites (golden tests).** The streaming `db_query`
  render is compared byte for byte with
  `serde_json::to_string(&legacy_rows(&rows))` over a table of every column
  type the codec renders: integers, floats, NUMERIC, text, booleans,
  UUIDs, dates, timestamps with and without zone, JSON and JSONB, arrays
  and NULLs. Key order and number and time text must match. The same holds
  for the `db_v2` result envelope, for the request a custom route hands its
  guest (old and new `request_str`), and for `guest_response::decode`.

**Engine** (multi-thread tokio tests with WAT guests, like the existing
`wasm_runtime` tests):

- A 64 MiB budget under `enforce`, and eight concurrent guests that grow
  24 MiB and spin for 300 ms. At most two hold memory at any instant
  (sampled from the ledger). The rest wait and run, or are refused past
  the deadline. The ledger never exceeds the budget and ends at zero.
- The same under `warn`: all eight run, nothing waits, and would-be
  refusals are counted.

**Integration** (`platform/tests/wasm_budget.rs`, through `testkit`, with
tenants `acme` and `globex` and services `catalog-service` and
`loan-service`):

- **Environment.**
  - `PlatformProcess` launches its child with a cleared environment, so the
    child would otherwise derive a budget from the CI runner's cgroup.
  - `testkit` therefore passes `WASM_MEMORY_BUDGET_MODE=off` to every
    platform and worker it starts, unless the test sets the variable.
  - Existing integration tests keep today's behaviour on any runner. The
    budget tests opt in with an explicit `WASM_MEMORY_BUDGET_MB`.
- **Linear burst.**
  - `WASM_MEMORY_BUDGET_MB=128` under `enforce`, with a test guest
    (extending `test-guests`) that grows to 48 MiB, holds it with
    `util::sleep_ms(500)`, and answers. Twelve concurrent calls.
  - Some succeed. Every other call is a 503 with `Retry-After: 2` and the
    not-run message, and none is a 500.
  - `/health` answers 200 throughout, from the same pid.
  - On Linux, `VmHWM` stays below the baseline plus 128 MiB plus the
    declared slack.
- **Host-heavy burst.** The same with guests that:
  - run an unbounded `db_query` over a large table;
  - download a 40 MiB object;
  - run `http_fetch_many` with 128 items against a local server;
  - take a 64 MiB body and return a 64 MiB response.

  Expect the same answers and bounds. A 65 MiB result fails the call with
  `-1` or `TooLarge`, and the guest answers.
- **Fairness.** `acme/catalog-service` saturates its share. A light
  `globex/loan-service` route keeps answering 200.
- **Route concurrency.** With `max_concurrent = 2`, the third concurrent
  call is a 503 after the wait. The deploy derives `route_concurrency`,
  and an older manifest's hash is unchanged.
- **Chunked body.** A chunked upload reserves `body_limit` until it has
  been read, then shrinks to its length.
- **Self-calls.** A service whose handler calls itself, with its slots
  full, answers 503 and does not hang.
- **Consumers.**
  - Twenty messages to an `on_success` subscription whose handler grows
    48 MiB, with a budget that fits two. Every message is handled once, with
    `num_delivered == 1`, and none is dead-lettered.
  - An idle durable holds nothing in the ledger.
  - An `ack: always` subscription denied mid-run is re-run in place, then
    deferred, and not lost.
- **Jobs.** A job denied mid-run is deferred with a delay, its attempts
  unchanged, and runs once room frees. No hot loop: at most one delivery per
  delay window.
- **Committed and worker work.**
  - An after-hook and an `on_migrate` started with the budget full run
    once room frees.
  - A worker after-hook under load is deferred, not dead-lettered.
  - A worker-routed route refused on the worker answers 503 with
    `Retry-After` through the platform.

**Under a real limit: a required CI job.**

- `.github/workflows/ci.yml` gains a `memory-limit` job, required like
  `test`.
- **It builds outside the limit.** rustc and the linker need more than the
  scope allows, so the job first builds with no limit:
  `cargo build -p cufflink-platform --bins` and
  `cargo test -p cufflink-platform --test wasm_budget_cgroup --no-run`. It
  reads the test binary's path from `--message-format=json`.
- **It runs inside the limit.** Then it runs only that binary in the scope:
  `sudo systemd-run --scope -p MemoryMax=768M --uid=$(id -u) --gid=$(id -g)
  -E … -- <test binary> --ignored`. The platform child it starts inherits
  the limit.
- The tests are `#[ignore = "needs a memory-limited cgroup; run by the
  memory-limit CI job"]`. The `test` job therefore never runs them, and
  never reaches `testkit::skip`, which panics under `CUFFLINK_CI`.
- Run with `--ignored`, they **fail** unless `/proc/self/cgroup` resolves to
  a `memory.max` of at most 1 GiB, so the job cannot pass vacuously.
- They start the platform with the derived budget under `enforce`, then
  send twenty concurrent 96 MiB guests and the host-heavy guests above.
- They assert that the platform pid survives and that the scope's
  `memory.events` reports `oom_kill 0`.
- Outside CI they run the same way under a local `systemd-run --user
  --scope`, or are ignored.

## 9. Documentation, when implemented

- `docs/setup.md`, "WASM runtime limits": the variables of section 5, and
  the host caps.
- `docs/deployment.md`: sizing a container, covering the limit sources,
  the reserve, replicas and the allocator setting.
- `docs/routing.md`, "Route limits": `max_concurrent`.
- `docs/observability.md`: the metrics of section 6 and the new "Alerts"
  section.
- `docs/building-on-cufflink.md`: the host-response cap, the fan-out item
  cap and the image limits, and what a guest receives at each.
- `docs/tenant-isolation.md`:
  - `WASM_MEMORY_BUDGET_MODE=enforce` joins the settings an operator must
    set;
  - in the planned list, per-service concurrency moves to done, and
    per-tenant shares stay planned.
- `docs/scaling.md`: a replica's budget is its own.
- `docs/versions.md`, in the release that ships it:

  > ## Features added in v0.35.0
  >
  > | Feature | Needed when | Provides |
  > |---------|-------------|----------|
  > | `guest_memory_budget` | never demanded | a process-wide budget for guest memory and the host memory made for guests, with admission that waits or answers 503 and `Retry-After` rather than exceeding it ([setup](setup.md#wasm-runtime-limits-platformsrcwasm_runtimemodrs)) |
  > | `route_concurrency` | any route sets `max_concurrent` | invocations of a route in flight at once, per replica ([route limits](routing.md#route-limits)) |
  >
  > `WASM_MEMORY_BUDGET_MODE` is `warn` by default: nothing waits or is
  > refused for the budget yet, and `cufflink_wasm_admission_refused_total{mode="warn"}`
  > shows what would have been. A later release makes `enforce` the default.
  > Under `enforce`, a host response is capped at 64 MiB
  > (`WASM_MAX_HOST_RESPONSE_MB`), a fan-out call at 128 items, and an image
  > transform at 32 MiB and 40 megapixels; a call past a cap fails as the
  > call fails today (`-1`, or `TooLarge`). A service is limited to 32
  > invocations in flight per replica (`WASM_MAX_CONCURRENT_PER_SERVICE`).
  > Under `warn` each of these is counted and logged instead. A message of
  > an `ack: always` subscription denied for capacity mid-run is run again
  > rather than acked.

  The rollback table gains a row: services that declare `max_concurrent`
  run without the limit on an older platform and cannot be deployed there.
  An older platform also lifts the host caps.

## 10. Not in this change

- **Per-tenant shares or budgets** as a tenant policy key. It would be a new
  key, never a field on `WasmLimitsConfig`: its config has
  `deny_unknown_fields`, and an older binary ignores an unknown key but not
  an unknown field.
- **Memory hints in the manifest** (a handler declaring its need) in place
  of learned estimates.
- **The pooling allocator.** Orthogonal: it bounds slots, not bytes in use,
  and reserves address space up front.
- **A budget shared across replicas.** Each replica protects its own
  process.
- **A different global allocator** (jemalloc or mimalloc), if the mmap
  threshold proves insufficient in practice.

## 11. Owner questions

Answered by the owner on 2026-10-30: every proposed default below is
accepted as stated, except question 10, which the owner re-decided the same
day (see its entry).

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
   (The revision renames that gauge `cufflink_process_memory_unreclaimable_bytes`.)
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
   check before the fetch makes it rare.* (The revision first re-runs the
   handler in place, spending nothing, and defers with a nak only after
   that: section 3.5.)
10. **Host response cap.** Add `WASM_MAX_HOST_RESPONSE_MB` (64, matching
    `WASM_MAX_GUEST_LEN_MB`) for `db_query` and the other host responses
    under the same mode, in this change or the next? It changes what a guest
    that reads more than 64 MiB observes. *Proposed: the next change, with
    the streaming render.*
    **Changed by the owner on 2026-10-30: in this change.** The review
    found that the outcome cannot hold while one host call can allocate
    without bound before the ledger sees it. `db_query`, downloads,
    fan-outs and image transforms build unbounded buffers in the host, so a
    budget guarding linear memory alone leaves the process exposed to the
    failure it exists to prevent. The caps and the streaming render are
    therefore section 2.4. As the question proposed, the caps follow the
    same mode as the budget (section 5.1).
11. **Pressure gate.** Include the cgroup v2 `anon` gate (section 3.4)
    now? *Proposed: yes, on by default where cgroup v2 is mounted.*
    (The revision moves the gate to section 3.8 and has it read the
    non-reclaimable total rather than `anon` alone.)
12. **Per-guest cap clamp.** Clamp `WASM_MAX_MEMORY_MB` to the service share
    of the budget at startup with a warning (proposed)? Or refuse to start
    when it is larger? *Proposed: clamp and warn.* (The revision clamps only
    under `enforce`, so that `warn` changes metrics and logs only.)

## 12. Questions raised by the revision

None is answered yet. Each states the proposed default.

13. **A declared `max_concurrent` outside the mode.** Enforce a route's
    `max_concurrent` in every mode, since its author opted in, while
    everything else follows `WASM_MEMORY_BUDGET_MODE`? Or have it follow the
    mode too? *Proposed: every mode.*
14. **Fan-out item cap.** `HOST_FANOUT_MAX_ITEMS` 128 for
    `s3_download_many` and `http_fetch_many`? *Proposed: 128.*
15. **Image limits.** `IMAGE_MAX_INPUT_MB` 32, `IMAGE_MAX_PIXELS` 40 megapixels,
    and `image::Limits` of 16 384 pixels a side? *Proposed: as stated.*
16. **Cold estimate.** Reserve 32 MiB for a handler with no sample in this
    process (`WASM_MEMORY_COLD_ESTIMATE_MB`), which throttles a cold burst
    of very many invocations of a light handler until it is measured?
    *Proposed: 32 MiB.*
17. **Capacity retries.** Re-run a subscription in place up to 3 times, and
    count a job's eleventh consecutive deferral as a failed attempt?
    *Proposed: 3 and 10.*
18. **Allocator.** Set `M_MMAP_THRESHOLD` to 1 MiB at startup on glibc, or
    switch the binaries to jemalloc now? *Proposed: the threshold; revisit
    with the `VmHWM` data of the real-limit job.*
19. **Required CI job.** Make `memory-limit` (sudo `systemd-run` with a
    768 MiB scope) a required check? *Proposed: yes.*
