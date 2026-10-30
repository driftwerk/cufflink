# Guest memory budget and admission control

Design only. Nothing here is implemented yet.

- The maintainer answered section 11's questions on 2026-10-30, accepting
  every proposed default. On the same day the maintainer re-decided
  question 10, bringing the host-side caps into this change.
- The note was then revised after an independent review. Section 12
  lists the questions that revision raised. The owner answered them on
  2026-10-30, all as proposed, and accepted the change `ack: always`
  subscriptions see for capacity denials (section 3.5).
- A second review followed. Section 13 lists the questions it raised.

## Outcome

Under `WASM_MEMORY_BUDGET_MODE=enforce`, guest execution cannot push a
platform or worker process past its container's memory limit. That holds
for any single request, any burst of requests and any slow dependency.
(`warn`, the first release's default, only measures what `enforce` would
do; section 5.1.) Precisely:

- Every byte held for a guest is charged to a process-wide **guest memory
  budget**. That covers its linear memory, its tables, its stack, and the
  host memory made on its behalf: the request, host-call inputs and
  results, work handed to other tasks, and the response.
- Each byte is charged before it is allocated, or as it is built in
  steps of at most 1 MiB, and stays charged until it is freed.
  - That includes work that outlives the invocation: a detached decode, or
    a response body a slow client is still reading.
  - Where a library allocates for us, the charge is a declared multiple,
    and a test pins that it is not exceeded (section 2.4).
- What stays outside the ledger is the process's own baseline, idle thread
  stacks and allocator fragmentation. The host reserve is sized for these
  (section 5.2), and the pressure gate (section 3.8) watches them.
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

- **`redis_get` and `redis_mget`** read whole values, which Redis
  allows up to 512 MB, with no cap (`host/redis.rs`). `cache::mget` is
  capped (`CACHE_MGET_MAX_BYTES`).
- **`service_call`** collects the callee's response up to
  `HOST_HTTP_MAX_RESPONSE_BYTES` (`to_bytes`), copies it (`to_vec`), and
  sets an envelope carrying it again in base64.
- **Detached host work.** `image::transform_multi` decodes on its own
  `spawn_blocking` task. `block_on_within` (`host/mod.rs`) drops only the
  future on a deadline or cancellation. The decode therefore keeps running,
  and keeps its memory, after the host call, and even the store, has
  ended.
- **The JSON trees** the host builds have a size that depends on the
  shape, not just the length.
  - An array starts empty and grows to a capacity of 4 (4 × 32 bytes), on
    top of the 32 bytes of each element. So `[[0],[0],…]` costs 40 to 50
    host bytes per input byte.
  - A small object costs a whole B-tree node.
  - This affects `parse_body` on every custom route (public ones
    included), `guest_json`'s `to_value`, `guest_response::decode` (whose
    shape the guest controls), and host inputs parsed into a `Value`, such
    as `image_transform_multi`'s targets.

**The response:**

- `read_response` copies it out of the guest (`to_vec`, at most
  `WASM_MAX_RESPONSE_MB`, 64).
- The `String` outlives the store.
- `guest_response::decode` parses it into a `Value` and clones `__body`.

**Thread stacks.** wasm runs on the blocking thread's own stack:
wasmtime's default `max_wasm_stack` is 512 KiB, which `Config` does not
change. Pages a thread has touched stay resident while the thread lives,
which tokio keeps for 10 s after its last task.

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
| **Host-call input** | `read_guest_bytes`: the copied length, before copying. A parse of it is charged as it builds (section 2.4) | when the host call returns |
| **Stack** | at admission, `WASM_STACK_CHARGE` (1 MiB): the 512 KiB wasm stack and the host frames under it | when the `Charge` drops |
| **Sub-charges** | work a host call hands to another task (section 2.4: the image decode, a fan-out item) takes an owned sub-charge of what it will allocate, moved into that task | when **that task** ends, not when the host call, the store or the `Charge` does |

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
- **Sub-charges outlive the store when the work does.** A sub-charge is
  owned by the closure or future it was moved into. When a deadline makes
  `block_on_within` drop the waiting future, a detached `spawn_blocking`
  decode keeps running with its sub-charge, and credits it only when it
  returns.
  - Repeated timeouts therefore cannot stack uncharged decodes: each one
    stays on the ledger until it actually ends.
  - The rule holds for every `spawn_blocking` and every spawned task a host
    call starts on a guest's behalf.
- **The guest's response is the other exception.** `call_handler` moves
  the response part of the `Charge` into a `ResponseCharge`, returned
  beside the response `String`. It covers the `String` and what the router
  builds from it, both charged as they are built (section 2.4).
  - On a route the router wraps the HTTP response body in a `ChargedBody`
    that owns the `ResponseCharge`. The charge is dropped when the body has
    yielded its last frame or is itself dropped (the client went away), not
    when the response is handed to hyper. A slow client therefore stays on
    the ledger for as long as its body is held.
  - A subscription or job drops it once settled.
  - A worker-routed route's reply is charged on the platform as it arrives,
    by its payload length, into the same kind of `ResponseCharge`.

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

Each path of section 1.4 is charged in one of three ways.

**Exactly, before allocating.** When the size is known in advance:

- a body's length;
- base64 at 4/3;
- an object's `head` size;
- a buffer grown in 1 MiB steps, charged by its capacity rather than its
  length. Growth uses `reserve_exact`, so capacity stays within 1 MiB of
  the length. With the mmap threshold at 1 MiB (section 5.2), glibc's
  `realloc` of such a block is an `mremap`, with no second copy.

**As built, through a charged builder.** For a JSON tree, whose cost
depends on its shape:

- **`ChargedJson::parse`** deserialises into a `Value` through a visitor
  that charges each node as it allocates it: a `Vec`'s capacity × 32, a
  map node, a `String`'s capacity. It charges in 1 MiB steps.
- **`ChargedJson::write`** serialises into a buffer grown as above.
- When a step cannot be charged, the parse or write stops. Under
  `enforce` the call fails with `Overloaded`, or with `TooLarge` when the
  structure alone exceeds the service share. Under `warn` the step is
  counted and allowed.
- No fixed multiple is reserved for JSON, so an adversarial shape is
  charged what it costs rather than refused for its length.
- Every `Value` the host builds for a guest goes through these:
  - `parse_body`;
  - `guest_json`;
  - `guest_response::decode`;
  - host inputs such as `image_transform_multi`'s targets, and the
    `s3_download_many` and `http_fetch_many` item lists.

**At a declared multiple.** Where a library allocates for us:

- `S3_HOST_MULTIPLE` = 7/3 (the bytes and their base64);
- `IMAGE_BYTES_PER_PIXEL` = 20 (a 16-bit RGBA decode at 8, the RGB copy at
  3, each resize at most 3, the encoder's buffers);
- `WASM_STACK_CHARGE` = 1 MiB.

**Tests.** A counting `#[global_allocator]` test measures each path's
peak host allocation, with ordinary and worst-case shapes, and fails when:

- a charged builder's charge is more than 10 % below what was really
  allocated; or
- a multiple is below what was really allocated.

The worst-case shapes include `[[0],[0],…]`, `[{},{},…]`, deep nesting,
many one-key objects, and strings of escapes (section 8).

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
| `db_query`, `db_v2` results | Detailed below the table |
| `s3_download` | `head` first. An object whose base64 would pass the response cap (above 48 MiB) is refused before downloading. Otherwise `size × S3_HOST_MULTIPLE` is reserved. The object is read through `open_read`'s stream (`ObjectReader`), never `get_bytes`' unbounded `bytes()`. The read stops at `head`'s size plus one byte, so an object replaced between `head` and `get` can never be read past what was charged; a longer one fails the call. It is encoded with no `to_vec` copy |
| `s3_download_many`, `http_fetch_many` | At most `HOST_FANOUT_MAX_ITEMS` (128) items. Each item takes a sub-charge of its size (`head`, or `HOST_HTTP_MAX_RESPONSE_BYTES` for HTTP) before it starts. **Results stay in input order**, since guests index them by position. Item *i* is written to the response text once items 0 to *i − 1* are written; a result that finishes early waits under its own sub-charge. The total stays under the response cap. Golden tests compare both envelopes byte for byte with today's |
| `image_transform`, `_multi` | Source objects of at most `IMAGE_MAX_INPUT_MB` (32), read as `s3_download` reads. The header's dimensions are read before decoding, and `width × height × IMAGE_BYTES_PER_PIXEL` is taken as a sub-charge moved into the `spawn_blocking` decode (section 2.2). `image::Limits` caps each side at 16 384 pixels and sets `max_alloc` to the sub-charge. Images over the pixel cap are refused. The cap is the smaller of `IMAGE_MAX_PIXELS` (40 megapixels) and the service share ÷ 20 bytes: 40 MP is about 800 MB at 20 bytes a pixel, more than a 2 GiB container's service share, so there the share decides (about 37 MP) |
| `redis_get`, `redis_mget` | `GETRANGE key 0 <cap>` in place of `GET` (pipelined per key for `mget`): one byte past the cap fails the call, and nothing longer is ever read. For values within the cap the answer is the same. A type error is the same error `GET` returns |
| `service_call` | The callee's body is reserved at `HOST_HTTP_MAX_RESPONSE_BYTES` before it is collected, then shrunk to its length. It is encoded into the envelope without the `to_vec` copy. The envelope is charged through `set_host_response` |
| Host-call inputs | The copy is charged before copying (section 2.2); a parse of it through `ChargedJson` |
| The custom-route request | Detailed below the table |
| The guest's response | `guest_response::decode` parses through `ChargedJson`, into the `ResponseCharge`, and takes `__body` out of the parsed map instead of cloning it. The rendered body is written through `ChargedJson::write`. The charge lives until the body is sent or dropped (section 2.2) |

**`db_query` and `db_v2` results.** Rows are read through `bind` and
`query_portal` with a row limit per fetch (1000), inside the per-call
transaction the call already runs in (`run_transactional`). They are
rendered straight to text, row by row, with no `Value` tree, into a buffer
grown and charged as above.

- **Byte-identical to today.** `legacy_rows` collects each row into a
  `serde_json::Map`, a `BTreeMap` because `preserve_order` is off: keys
  come out sorted, and of two columns with the same name the last wins.
  The streamed render computes that order once per statement, as a
  permutation of the columns with each duplicate name resolved to its last
  index, and writes each row in that order through the same
  `column_to_json` encoding. `NaN` and `±Infinity` render as they do today.
- **Stopping.** At the cap, or when a step cannot be charged, the portal
  is not fetched again and the call fails. Its transaction ends as a
  failed call's does, which closes the portal, so the server stops
  producing rows. Dropping a `query_raw` stream would not stop it.
- `DB_READ_ONLY_MAX_ROWS` still applies to read-only queries.

**The custom-route request.**

- `execute` no longer re-parses the request: the caller passes `auth` and
  `caller.depth` in the `Invocation`.
- The audit record keeps the raw body, not a cloned tree, and parses it
  through `ChargedJson` when the record is written.
- The request's reservation is held until `record()` has built and handed
  off its payloads (`outbox/emit.rs`).
- The body is parsed through `ChargedJson`. The request `Value` is dropped
  as soon as `request_str` is written through `ChargedJson::write`.
- `body_raw_b64` stays, because guests read it, and is reserved at 4/3.
- The raw body is reserved before it is read (section 3.7).

### 2.5 The estimate

The reservation at admission is the handler's estimate.

- **Samples.** One per finished invocation of a `(service_id, handler)`:
  its peak **actual use**, meaning linear growth plus tables plus host
  buffers and sub-charges at the same instant.
  - A sample never includes unused reservation, so it can fall below
    the last estimate, or the cold seed.
  - It never includes the request's host cost or the stack, which are
    charged separately at every admission, so they are not counted twice.
  - A denied invocation's sample is its actual use plus what it asked for:
    the `desired − current` of its denied grow, or the size of its refused
    host allocation.
  - Exits, denials, traps and deadlines all count.
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
  - With the default 32 service slots, this caps a subscription's
    effective concurrency at 16 per process under `enforce`, whatever its
    `consumer.concurrency`.
  - A worker-only process can raise `WASM_MAX_CONCURRENT_PER_SERVICE` for
    itself; the release notes say so.
- Committed work (in-process after-hooks) is capped the same way: at most
  half of a service's slots. A bulk write's after-hooks therefore cannot
  hold every slot of their service.
  - Under `enforce` they wait for a slot outside their handler budget,
    for up to `WASM_COMMITTED_WAIT_SECS` (300).
  - Past that they are dropped and logged, as a failed after-hook is today.
  - The slow path for bulk after-hooks remains worker routing, which is
    durable.

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
    exists to avoid. The owner accepted this on 2026-10-30. Section 7 and
    the release notes state it.
- **Jobs.** A capacity denial is the new `Attempt::Deferred(retry_after)`,
  not `Interrupted`.
  - **Persistence.** A claim increments `attempts`. A deferral writes:

    ```sql
    status = 'pending',
    attempts = attempts - 1,
    capacity_deferrals = capacity_deferrals + 1,
    next_attempt_at = now() + retry_after
    ```

    Undoing the claim's increment leaves `attempts` as it was before the
    claim, as `release` does today. Unlike `release`, the next attempt is
    not now. The worker naks with the same delay, so a deferral neither
    spends an attempt nor loops hot.
  - **The counter.** `capacity_deferrals` is a new column (migration
    `V7__capacity_deferrals.sql`: `ADD COLUMN IF NOT EXISTS
    capacity_deferrals INTEGER NOT NULL DEFAULT 0`). It survives
    redelivery and replicas, and any conclusion other than a deferral
    resets it to 0.
  - **The bound.** When a deferral would make it exceed
    `WASM_CAPACITY_RETRIES_JOB` (10), the attempt concludes instead as
    `Failed { class: Capacity }`: the attempt counts and the counter
    resets.
  - **Retry.** `JobErrorClass::Capacity` is always retried, as `Crash` is
    (`is_retried`), so it is bounded by the job's `max_attempts` and then
    fails as any exhausted job does.
  - `cufflink_wasm_job_deferrals_total` counts every deferral.
- **Worker after-hooks** are re-run in place and deferred like
  subscriptions, on the worker's ledger.
  - Their consumer's `AFTER_HOOK_MAX_DELIVER` is 3. A capacity deferral's
    nak spends one of those deliveries; JetStream cannot nak without
    counting.
  - The bound is therefore 3 deliveries, each with up to
    `WASM_CAPACITY_RETRIES` in-place re-runs. After that, the hook is
    dead-lettered with reason `capacity`, from where it can be replayed.

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

1. **Reserve the raw body** before reading it: its `Content-Length`, or
   the route's `body_limit` for a chunked body.
   - Once the body has been read, the reservation shrinks to the actual
     length.
   - A slow upload therefore holds only the memory its body needs, and no
     slot.
   - What is built from the body afterwards is charged as it is built
     (section 2.4): the base64 at 4/3, then the parsed tree and
     `request_str` through `ChargedJson`.
2. **Read the body.**
3. **Take** the route, service and global slots and the handler's
   estimate (section 3.2), within what is left of the one admission
   deadline.

The `Admission` is passed into `execute`, and `WasmEngine::run` uses it
instead of admitting again.

A worker-routed route reserves its request on the platform, which reads the
body, and admits the guest on the worker, where it runs.

### 3.8 The pressure gate

Some host memory stays outside the ledger:

- tokio, connection pools, compiled modules and CRUD lists;
- the stacks of idle blocking threads, which keep the pages they touched
  for tokio's 10 s keep-alive. The pool may hold up to
  `max_blocking_threads` (section 3.3) threads, each with up to the 512 KiB
  of wasm stack it last touched.
  - The running invocations' stacks are charged (`WASM_STACK_CHARGE`).
  - Idle ones count against the reserve.

This is covered by the host reserve (section 5.2) and by a gate.

- Where the process's cgroup is a v2 cgroup with the memory controller
  (section 5.2), a task reads its `memory.stat` every 250 ms.
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
  - **v2 or v1.** v2 is chosen only when the unified hierarchy's
    `cgroup.controllers` (at the mount root, and in the process's cgroup)
    lists `memory`. On a hybrid host the memory controller is on v1, so v1
    is used.
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
  `max(256, 30 % of limit)` (question 3). It must hold:
  - the process's baseline;
  - the compiled modules;
  - idle threads' stacks (section 3.8);
  - fragmentation below the mmap threshold.

  The startup log prints the worst case of the idle stacks,
  `max_blocking_threads × 512 KiB`, beside the reserve. It warns when that
  is above half the reserve: lower `WASM_MAX_CONCURRENT` or raise the
  reserve.
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
| `WASM_COMMITTED_WAIT_SECS` | `300` | Section 3.2 |
| `WASM_STACK_CHARGE_KB` | `1024` | Section 2.2 |
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
  - **One additive migration**, `V7__capacity_deferrals.sql`. An older
    platform or worker ignores the column. Like every additive migration
    before it, it makes an older binary log the "missing migration" line
    at start, which is expected and harmless ("Rolling a platform back").
    A job deferred when the rollback happens keeps its delayed
    `next_attempt_at`, and the older binary runs it then.
  - No tenant policy row is involved.
- **wasmtime.** The design depends only on `ResourceLimiter`'s
  `memory_growing`, `memory_grow_failed`, `table_growing` and
  `table_grow_failed`, as wasmtime 36 defines them, and on
  `Memory::data_and_store_mut`. If the update to wasmtime 50 lands
  before this change, these four methods and that accessor are checked
  against wasmtime 50, and the design is adjusted where they differ.
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
- **Host charges.** A test binary with a counting `#[global_allocator]`
  measures the peak host allocation of each path in section 2.4. It fails
  when the path's charge falls more than 10 % below that measure, or below
  its declared multiple. The paths:
  - a 16 MiB request through a custom route;
  - `db_query` over a large result;
  - `s3_download` and `_many`;
  - `http_fetch_many`;
  - an image transform;
  - a 64 MiB response.
- **`threads` stays off.** A module with a shared memory fails to compile.
- **Sub-charges.** An image decode, its host call timed out, stays charged
  until the decode returns. Ten repeated timeouts never hold more than ten
  decodes' sub-charges, and the ledger returns to zero.
- **`ChargedBody`.** A response read slowly by a client stays charged until
  the last frame. A client that disconnects mid-body releases the charge.
- **Estimates.** A handler that peaks at 2 MiB, after its cold seed, is
  estimated at 2 MiB: no ratchet to the seed, and no request cost counted.
- **Jobs.** A deferral leaves `attempts` unchanged and `next_attempt_at`
  in the future. The eleventh consecutive deferral is a counted `Capacity`
  failure, and the counter resets. `Capacity` is retried up to
  `max_attempts`.
- **Byte-identical rewrites (golden tests).** The streaming `db_query`
  render is compared byte for byte with
  `serde_json::to_string(&legacy_rows(&rows))` over a table of every column
  type the codec renders: integers, floats including `NaN` and `±Infinity`,
  NUMERIC, text, booleans, UUIDs, dates, timestamps with and without zone,
  JSON and JSONB, arrays and NULLs. Two columns share a name, so the last
  must win. Key order, and the text of numbers and times, must match.

  The same holds for:
  - the `db_v2` result envelope;
  - the request a custom route hands its guest (old and new
    `request_str`);
  - `guest_response::decode`;
  - the `s3_download_many` and `http_fetch_many` envelopes, with results
    finishing out of order;
  - `GETRANGE` against `GET` for values within the cap.
- **Worst-case shapes.** The counting-allocator tests of section 2.4 run
  `ChargedJson` over ordinary JSON and over `[[0],[0],…]`, `[{},{},…]`,
  64-deep nesting, a million one-key objects and strings of `\u` escapes,
  as request bodies, guest responses and host inputs.

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
  - On Linux, `VmHWM` stays below the baseline, plus 128 MiB, plus the
    idle-stack worst case of section 3.8.
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
  - A worker after-hook under load is re-run in place and deferred. It
    reaches the dead-letter subject, with reason `capacity`, only after its
    three deliveries.
  - A bulk write's after-hooks hold at most half of their service's
    slots.
  - A worker-routed route refused on the worker answers 503 with
    `Retry-After` through the platform.

**Under a real limit: a required CI job.**

- **The job.** `.github/workflows/ci.yml` gains a `memory-limit` job,
  required like `test`.
  - It uses the `test` job's service containers for Postgres and Redis, and
    its NATS step, and sets the same `CUFFLINK_TEST_*` variables.
  - So no embedded Postgres, NATS or Redis shares the limit.
- **Only the platform is limited.** The build, the test process and its
  client run outside any scope, so rustc and the test's own buffers never
  count.
  - `testkit` gains `CUFFLINK_TEST_PLATFORM_LAUNCHER`, a command prefix
    that `PlatformProcess` puts before the platform binary.
  - The job sets it to `sudo -E systemd-run --scope --quiet
    --unit=cufflink-budget-<n> -p MemoryMax=768M -p MemorySwapMax=0
    --uid=<runner uid> --gid=<runner gid> --`. `-E` keeps the variables
    `testkit` sets after it clears the environment.
- **Running the tests.** The job runs `cargo test -p cufflink-platform
  --test wasm_budget_cgroup -- --ignored`.
  - The tests are `#[ignore = "needs CUFFLINK_TEST_PLATFORM_LAUNCHER with a
    memory limit; run by the memory-limit CI job"]`. The `test` job never
    runs them, and never reaches `testkit::skip`, which panics under
    `CUFFLINK_CI`.
  - Run with `--ignored`, they **fail** unless the platform child's cgroup
    has a `memory.max` of at most 1 GiB and a `memory.swap.max` of 0, so
    the job cannot pass vacuously.
  - The tests find the child's pid in the scope's `cgroup.procs` (`sudo` is
    its parent), and read the scope's memory files from there.
- **The numbers.** 768 MiB, with the derived reserve `max(256, 230)` =
  256 MiB, gives a 512 MiB budget and a 256 MiB service share. Every
  expectation fits inside that share:
  - twenty concurrent 96 MiB guests: at most two of one service at once;
  - 16 MiB request bodies: raw 16, base64 21.3, a string-valued `Value`
    16, `request_str` 37.3, the guest's copy 37.3, about 128 MiB at the
    peak;
  - a 24 MiB object: about 88 MiB with the guest's copy;
  - a `db_query` stopped at the 64 MiB response cap: about 140 MiB with the
    guest's copy;
  - 128 HTTP items of 1 MiB, which are refused at the response cap.
- **Assertions.**
  - The platform pid survives.
  - The scope's `memory.events` reports `oom_kill 0`.
  - `memory.swap.peak` (or `memory.swap.current` on kernels without it)
    stays 0.
  - The count of `max` events stays under a bound recorded by the test.
- **Outside CI** the same tests run with a local launcher, `systemd-run
  --user --scope -p MemoryMax=768M -p MemorySwapMax=0 --`, or stay
  ignored.

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
  > rather than acked, so it may run twice. Under `enforce`, background work
  > may hold half of a service's slots, so with the default of 32 a
  > subscription runs at most 16 messages at once per process, whatever its
  > `consumer.concurrency`. Raise `WASM_MAX_CONCURRENT_PER_SERVICE` on a
  > worker-only process to suit it. Migration `V7__capacity_deferrals.sql`
  > adds a column to `wasm_jobs`.

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

Answered by the owner on 2026-10-30: every proposed default below is
accepted as stated. On the same day the owner also accepted that an
`ack: always` subscription's message denied for capacity mid-run may run
twice (section 3.5), in place of being acked and lost.

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

## 13. Questions raised by the second review

None is answered yet. Each states the proposed default.

20. **Committed waits.** Under `enforce`, in-process after-hooks are capped
    at half of their service's slots. They wait for one outside their
    budget, for up to `WASM_COMMITTED_WAIT_SECS` (300), and are dropped and
    logged past it. *Proposed: as stated.*
21. **The job migration.** Add `wasm_jobs.capacity_deferrals`
    (`V7__capacity_deferrals.sql`) so the bound on consecutive deferrals
    survives redelivery? *Proposed: yes.*
22. **The image pixel cap.** Take the smaller of `IMAGE_MAX_PIXELS`
    (40 megapixels) and the service share ÷ 20 bytes, so the cap shrinks on
    small containers (about 37 MP in a 2 GiB container)? *Proposed: yes.*
