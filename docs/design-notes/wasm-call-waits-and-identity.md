# Outbound waits that release admission, and service calls as the caller

Proposed. This note covers two issues:

- **#58**: an outbound call that does not hold the guest's admission while it
  waits.
- **#59**: a service-to-service call that carries the end user's verified
  identity.

They are designed together because both change the same host functions
(`http_fetch_v2` and `service_call`), the same SDK call types and the same
release. They do not depend on each other. Either can ship alone, and the
PR plan (section 9) keeps them in separate pull requests.

Section 12 lists the questions for the owner, each with a recommended
default.

## Outcome

**#58.** A guest that opts in, per call, can give back its service slot and
its global slot while it waits on an outbound HTTP request or a service
call, and take them again when the answer arrives.

- A proxy-shaped service then holds execution slots only while it computes.
  It no longer starves compute-shaped services of slots.
- What the guest keeps while it waits:
  - its memory charge;
  - its blocking thread;
  - its route slot;
  - any database connection it holds.
- The wait pool is bounded globally and per service. When the pool is full,
  the call waits holding its slots, exactly as it does today.
- A guest that cannot get its slots back before its deadline ends with
  `DeadlineExceeded`, as a guest whose upstream is slow does today.

**#59.** A guest that opts in, per call, can call another service of its
tenant *as the end user it is serving*. The target then applies that user's
own permissions.

- The identity is the principal the platform verified on the inbound
  request. It travels inside the platform-minted service identity, as a
  signed delegation that never outlives the user's token.
- The guest never handles a credential. It sets a flag, and the platform
  decides whether a verified principal exists and whether the route may
  delegate it.
- The call is bounded by the same depth limit (4) and time limit (the
  caller's remaining budget, at most 60 s for the identity) as every service
  call.
- The target sees the user in `req.auth()`, exactly as on a direct call. It
  sees the calling service in `req.caller()`, with a marker that the
  principal was delegated. The audit trail names the user *via* the calling
  service.

Guests built with older SDKs, and manifests that declare neither feature,
run exactly as before. Their request JSON, identity tokens and admission
are byte-for-byte and slot-for-slot unchanged.

## 1. What the platform does today

Read from `main` at `4a2d341`. wasmtime is on its 36 series, built with
`default-features = false` and the features `cranelift`, `runtime`, `std`,
`parallel-compilation` and `wat` (`Cargo.toml:83`), so **without** wasmtime's
`async` feature.

### 1.1 How a guest runs, and how it waits on a host call

- **Synchronous execution on a blocking thread.**
  - `WasmEngine::run_admitted` admits the invocation (`wasm_runtime/mod.rs:1001`).
  - It then calls `tokio::task::spawn_blocking` (`mod.rs:1008`), which runs
    `execute` on a tokio blocking-pool thread.
  - The store is a plain `Store<HostState>`, and the handler is called with
    `TypedFunc::call` (`mod.rs:1172`).
- **Deadlines** use epoch interruption (`config.epoch_interruption(true)`,
  `mod.rs:365`), and nothing uses fuel.
  - An epoch ticker increments the engine every `WASM_EPOCH_TICK_MS`
    (100 ms, `mod.rs:1292`).
  - On each tick, `epoch_deadline_callback` (`mod.rs:1083`) traps the guest
    with `Interrupt::Deadline`, `Interrupt::Cancelled` or
    `Interrupt::Shutdown`.
- **Host functions are synchronous.** Each one is registered with
  `Linker::func_wrap` (`host/mod.rs:451`, `wrap`). A host function that does
  I/O drives a future to completion on the guest's own thread, through
  `tokio::runtime::Handle::current().block_on` (`host/mod.rs:216`), usually
  through `block_on_within` (`host/mod.rs:253`), which adds a timeout and the
  invocation's cancel token. There are about fifty such call sites across
  fourteen host modules.
- **Outbound HTTP:**
  - `http_fetch_host` blocks in `block_on_within` (`host/http.rs:294`).
  - `http_fetch_many` blocks in `block_on_within` (`host/http.rs:136`).
  - `fetch_v2` blocks in `block_on_within` (`host/http.rs:563`).
- **Service calls.** `service_call` blocks in `block_on_within`
  (`host/service_call.rs:643`).
  - On the platform, the callee is dispatched in process through
    `LocalDispatch`. The caller's thread drives the in-process router
    future, and the callee's own guest runs on another blocking thread.
  - On a worker, the call goes over HTTP to `PLATFORM_INTERNAL_URL`.

### 1.2 What a waiting guest holds

An invocation's `Admission` holds:

- an unlabelled `Vec<OwnedSemaphorePermit>`;
- an optional memory `Charge` (`budget/mod.rs:239`).

`Budget::take` (`budget/mod.rs:579`) fills the permits in this order:

1. the route slot, when the route declares `max_concurrent`;
2. the class cap: half of a service's slots, for committed and background
   work;
3. the service slot (`WASM_MAX_CONCURRENT_PER_SERVICE`, 32);
4. the background global cap, for background work under `enforce`;
5. the global slot (`WASM_MAX_CONCURRENT`, 256). A nested invocation takes a
   permit from the nested pool instead, and only by `try_acquire`
   (`budget/mod.rs:556`).

`execute` destructures the admission with `let (_slots, charge) =
admission.into_parts()` (`mod.rs:1029`). The permits therefore live in a
local of the blocking closure until the store is dropped.

So a guest blocked in `http_fetch` or `service_call` keeps all of the
following for the whole upstream wait:

- every slot it took;
- its memory charge: linear memory, tables, its stack residue, and the
  response reservation that `service_call` makes before sending
  (`host/service_call.rs:628`);
- its blocking thread.

The memory budget design states this for service calls: "The caller keeps
its memory, its permit and its blocking thread while the callee runs"
(`guest-memory-budget.md` §1.3).

Two sizes rest on one thread and one global slot per running top-level
invocation:

- The blocking pool is sized `WASM_MAX_CONCURRENT × (1 + MAX_CALL_DEPTH) + 64`
  (`budget/mod.rs:703`).
- The nested pool is sized `WASM_MAX_CONCURRENT × MAX_CALL_DEPTH`
  (`budget/mod.rs:310`).

**The consequence.** A service whose handlers mostly wait on upstreams holds
its per-service slots, and a share of the global slots, while it does no
work. Under `enforce`, the slots of every other service then fill up:

- interactive arrivals wait their 1 s and are refused with 503;
- background work waits.

The waiting service is not using CPU or new memory. Only the slots and the
thread stand between it and everyone else.

### 1.3 Service calls and identity

- **The service identity.** `service_call` replaces the guest's credentials
  with a platform-minted identity.
  - Reserved headers are dropped: `authorization`, `cookie`, `x-api-key`,
    the forwarded-address headers, `host`, the identity header itself and
    hop-by-hop headers (`host/service_call.rs:34`; `docs/service-calls.md:21-30`).
  - The identity is an HMAC-SHA256 `cfsi1` token (`auth/service_identity.rs`).
    It is signed with `CUFFLINK_SERVICE_IDENTITY_KEYS`, which every replica
    and worker shares.
  - It binds tenant, target service, method and path. It lives at most
    `MAX_LIFETIME_SECS` (60), and less when the caller's remaining budget is
    shorter (`host/service_call.rs:241`).
  - It carries a depth of at most `MAX_CALL_DEPTH` (4, `service_identity.rs:10`)
    and a `jti`.
- **`on_behalf_of`.** The token carries `on_behalf_of` (`obo`,
  `service_identity.rs:125`).
  - It is the subject of the caller's `auth` snapshot
    (`host/service_call.rs:479`).
  - It is attribution only. The SDK documents it as "attribution only"
    (`cufflink-fn/src/request.rs:142`).
  - Nothing on the target authenticates a user from it.
- **What the target does with the identity.** `authorize_route`
  (`router/postures/mod.rs:169`) verifies the identity into a
  `ServiceCaller`.
  - On an `internal` route, the principal is `Principal::Service`.
  - On any other posture, the principal still comes from
    `authenticate(headers)` (`router/postures/mod.rs:237`). The call has no
    `authorization` header, so `auth`, `perm`, `owner` and `scoped` routes
    answer 401. CRUD does the same through `resolve_data_route`
    (`router/authz.rs:81`).
  - The audit actor of any service call is `Actor::service`
    (`router/custom/dispatch.rs:541`).
- **The workaround today.** A route declares `forward_auth`. The raw
  `authorization` header then reaches the guest's request headers
  (`docs/wasm-guide.md`, "forward_auth"). The guest copies it into an
  `http_fetch` back through the public gateway.
  - The guest holds a bearer token in its linear memory, where a log line, a
    panic message or a bug can leak it.
  - The call leaves the in-process path, so it pays TLS and the gateway, and
    it loses the depth and budget bounds of a service call.
- **Replay.** `docs/service-calls.md` says a "replayed" identity answers 401,
  but no verifier reads `jti`.
  - `grep jti` finds only the field (`service_identity.rs:121`) and its mint
    (`host/service_call.rs:265`).
  - Today an identity can be replayed for the same tenant, service, method
    and path until it expires.
  - For a service-only identity, the authority replayed is a tenant
    service's, for at most 60 s. A delegated identity would carry a user's
    authority, so section 3.7 closes this before any delegation is minted.
    It also corrects the document.

### 1.4 How the platform verifies an end user

- **A bearer JWT.**
  - It is validated against the realm's JWKS: RS256, the issuer, and `exp`
    (`validate_exp = true`, `validate_aud = false`; `auth/keycloak.rs:378`).
  - The tenant's audience allowlist is then applied (`auth/audience.rs::check`).
  - Then tenant binding (`auth/binding.rs`).
  - Every claim the token carries outside the named fields, `exp` included,
    is kept in `TokenInfo.claims` through `#[serde(flatten)]`
    (`auth/keycloak.rs:96`).
- **A user API key** is `AuthMethod::ApiKey { id, user_sub }`. It has no
  token.
- **Permissions** are not read from the token. `guest_auth_json` and the
  `perm` posture resolve them by subject from the tenant's role assignments
  (`resolve_permissions_cached`, `router/authz.rs:376`), cached for 300 s.
- **Partner keys** (`api_key` posture) are a `PartnerPrincipal`, not an
  `AuthUser`.

What a target needs in order to treat a caller as a user:

- an `AuthUser`: subject, tenant, realm, method, service-account flag, and
  token info for the guest's `auth.claims`;
- an assurance that the platform verified it.

It does not need the token itself.

### 1.5 The gaps

- **#58.** No host call can give back slots while it waits. The only lever
  is to raise slot counts for everyone.
- **#59.** No in-process call can act as the user. The only way is a raw
  token through the guest and out through the gateway.

## 2. #58: releasing admission while a call waits

### 2.1 Options and the feasibility verdict

**A. Release the slots and keep the thread (recommended).** Within the
existing synchronous model:

1. The host function hands the guest's releasable permits to the wait.
2. If the call is still outstanding after a short threshold, the wait drops
   them.
3. When the answer arrives, the wait takes them again before the host
   function returns to the guest.

The guest's thread stays blocked in `Handle::block_on`, as it does today.

- **What changes:**
  - the `Admission` slots become labelled and live in `HostState`;
  - two pools bound how many guests may wait this way;
  - two host functions accept a per-call flag.
- **What it frees:** the service slot, the class cap, the background cap and
  the global slot, which are the slots that starve other services.
- **What it keeps:** the thread, the memory charge and the route slot.
- **Cost:** moderate, confined to the budget module, two host functions and
  the SDK. No wasmtime change.

**B. A bounded wait pool on its own (part of A, not an alternative).** A
released slot admits a new guest, and that guest needs its own blocking
thread. Releasing slots without bounding the waiters therefore converts
slot exhaustion into thread exhaustion. A therefore includes:

- a global wait pool, `WASM_MAX_PARKED`;
- a per-service wait pool, `WASM_MAX_PARKED_PER_SERVICE`;
- a blocking pool and a nested pool resized to cover them (section 2.6).

**C. Park the guest itself with wasmtime async (not recommended now).** This
would free the thread too. What it entails:

- **Engine and calls:**
  - enable wasmtime's `async` feature and `Config::async_support(true)`;
  - instantiate with `instantiate_async` and call with `call_async`;
  - make deadlines yield, with `epoch_deadline_async_yield_and_update`.
- **Host functions.** Convert every host function that blocks to
  `func_wrap_async`: the fifty-odd `block_on` sites in fourteen modules,
  including the database session (`host/db_v2/session.rs`). A
  `Handle::block_on` left inside a fiber panics ("cannot start a runtime from
  within a runtime"), so the conversion is all at once.
- **Threads.**
  - Execution leaves `spawn_blocking`. Guest code between yields runs on the
    thread that polls it, so a CPU-bound guest would stall tokio's core
    workers for up to a whole epoch tick.
  - That needs either a dedicated runtime for guest execution or a much
    shorter tick, and a shorter tick costs every guest.
  - Thread pinning goes: a suspended store may resume on another thread.
- **Stacks.**
  - `budget/stack.rs` charges a guest's stack through a thread-local
    (`THREAD`) and moves it to the thread's residue. Both assume one guest
    per thread at a time and no migration.
  - Fiber stacks (`Config::async_stack_size`, 2 MiB by default) would need
    their own charge.
- **What it saves.** Each suspended guest still needs its fiber stack and
  its linear memory, so C saves **no memory** per waiting guest compared
  with A: a fiber stack costs what a thread stack costs. What it saves is OS
  threads, which matters only where `pids.max` or the kernel's thread limits
  bind.

**Verdict.**

- A (including B) is feasible now, inside the current execution model. It
  delivers what #58 asks: proxy-shaped services stop starving compute-shaped
  ones, and the memory charge is still held.
- C is a rewrite of the runtime's execution model, with a large blast
  radius and little memory benefit.
- C should be its own design note, opened only if measured thread counts of
  parked guests approach a deployment's `pids.max`. The metrics in section
  2.8 measure exactly that.

The rest of this section designs A. The note calls a guest whose slots are
released while it waits a **parked** guest.

### 2.2 What a released wait does

A guest opts in per call. Old guests never send the flag (section 4).

```rust
// cufflink-fn, features "http-v2" + "wait-release"
let response = http::HttpRequest::get("https://rates.example.com/v1/eur")
    .timeout_ms(10_000)
    .release_while_waiting()
    .send()?;

// cufflink-fn, features "service-call" + "wait-release"
let assessment = http::ServiceCall::new("loan-service", "POST", "/_fn/assess")
    .json(&request)?
    .release_while_waiting()
    .send()?;
```

For such a call, the host function does the following:

1. **Prepares the request** exactly as today: it reads the guest's input,
   reserves the response's charge and builds the request. Nothing about the
   call itself changes: egress policy, limits, headers, timeouts and the
   answer.
2. **Starts the outbound future** inside its existing `block_on_within`.
3. **Parks after a threshold.** If the future has not finished after
   `WASM_WAIT_RELEASE_AFTER_MS` (50 ms), the wait tries to park:
   - It tries to take one permit from the global wait pool and one from the
     service's wait pool, by `try_acquire` only, never waiting.
   - If both are taken, it drops the guest's releasable permits back to
     their semaphores, and the guest is parked.
   - If either pool is full, nothing is released. The call goes on holding
     its slots, as today.

   A call that answers within the threshold never touches a semaphore, so
   fast calls cost nothing extra.
4. **Resumes.** When the outbound future finishes, with an answer or an
   error, a parked guest takes its releasable permits again (section 2.5),
   then releases its wait-pool permits.
5. **Returns to the guest.** Only then does the host function write the
   answer and return.

The guest never runs without its slots. From the guest's point of view, the
call took longer by the time it waited to resume, and nothing else
differs.

### 2.3 What never releases

A call with the flag still holds its slots in each of these cases. The
reason is counted (section 2.8), and the call runs exactly as without the
flag.

| Case | Why |
|------|-----|
| A `nested` invocation (depth > 0) | It took its nested and service permits by `try_acquire` (§3.3 of the memory design) because a chain must never wait on its own callers. Its resume would have to wait, and its ancestors hold the slots it waits on, because an ancestor blocked in a held call keeps its permits. A try-only resume would instead fail spuriously, after the call's effects. |
| A `committed` invocation (after-hooks, `on_migrate`) | After-hooks already wait outside their budget for up to `WASM_COMMITTED_WAIT_SECS`. An in-process after-hook is at-most-once (§14 of the memory design), and parking adds a second wait after its effects. `on_migrate` runs inside a deploy. |
| An SSE reducer | Interactive and tiny, with no outbound calls of note. |
| A guest holding an open guest-owned transaction (`DbSession` has one) | Releasing its slots would admit more guests while it pins a database connection, which turns slot pressure into pool exhaustion. |
| `WASM_WAIT_RELEASE=off` | The operator's switch (section 5). |
| A wait pool is full | The bound (section 2.6). |

So the guests that park are top-level interactive and background
invocations:

- custom routes and `_fn`;
- CRUD before-hooks;
- worker request/reply;
- subscriptions;
- long jobs.

**The route slot is kept.** `max_concurrent` is the author's declared
"invocations of the route in flight at once" (`types/src/manifest/route.rs:40`),
and an author may use it to protect the upstream the route calls. Releasing
it would quietly change what the declaration means. Section 12, question 1,
asks whether a route should be able to release it as well.

### 2.4 The seat

`Admission` keeps its constructor and its public shape. Its permits become
labelled, and `execute` moves them into `HostState` instead of a local.

```rust
// platform/src/wasm_runtime/budget/seat.rs
pub(crate) struct Seat {
    route: Option<OwnedSemaphorePermit>,
    releasable: Vec<Taken>,
    parking: Result<Parking, Held>,
}

struct Taken {
    gate: Arc<Gate>,
    permit: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub(crate) struct Parking {
    global: Arc<Semaphore>,
    service: Arc<Semaphore>,
}

pub(crate) struct Parked {
    gates: Vec<Arc<Gate>>,
    _global: OwnedSemaphorePermit,
    _service: OwnedSemaphorePermit,
    since: Instant,
}

impl Lent {
    pub(crate) fn park(&mut self) -> Result<Parked, Held> {
        let parking = self.parking.as_ref().map_err(|held| *held)?;
        let global = Arc::clone(&parking.global)
            .try_acquire_owned()
            .map_err(|_| Held::PoolFull)?;
        let service = Arc::clone(&parking.service)
            .try_acquire_owned()
            .map_err(|_| Held::ServicePoolFull)?;
        let gates = std::mem::take(&mut self.releasable)
            .into_iter()
            .map(|taken| taken.gate)
            .collect();
        Ok(Parked { gates, _global: global, _service: service, since: Instant::now() })
    }
}
```

- **The pieces.**
  - `Gate` wraps each releasable semaphore (section 2.5).
  - `Held` (`Copy`) is the reason a call did not park: `NotRequested`,
    `NotParkable`, `PoolFull`, `ServicePoolFull`, `Transaction` or
    `Disabled`. `Held::count` counts every reason except `NotRequested`.
  - The seat's `parking` is `Err(NotParkable)` or `Err(Disabled)` for every
    case in section 2.3 that is known at admission: class, kind, and the
    operator's switch.
- **The admission order.** `Budget::take` builds the seat in its current
  order. `Parked::gates` keeps that order, so a resume takes them in the
  same order as admission, and two resumes cannot deadlock against each
  other.
- **The wait helper.** It is the one place that parks. It replaces
  `block_on_within` in the two host functions.
  - Both build their outbound future from a shared borrow of the state:
    - `fetch_v2(state: &HostState, …)` runs inside `exchange`'s
      `FnOnce(&HostState, Request)` (`host/util.rs:16`);
    - `service_call` calls `call(caller.data(), …)`.
  - A `&mut` seat borrowed across that future would conflict with them. So
    the host function first takes the releasable permits out of the seat by
    value (`Seat::lend`), and only then borrows the state.
  - The helper owns what it may release, and the host function puts back
    what was retaken after `block_on` returns.
  - No interior mutability is needed.

```rust
// platform/src/wasm_runtime/budget/seat.rs
pub(crate) struct Lent {
    releasable: Vec<Taken>,
    parking: Result<Parking, Held>,
}

impl Seat {
    pub(crate) fn lend(&mut self, wait: Wait, transaction_open: bool) -> Lent {
        let parking = match (wait, transaction_open) {
            (Wait::Hold, _) => Err(Held::NotRequested),
            (Wait::Release, true) => Err(Held::Transaction),
            (Wait::Release, false) => self.parking.clone(),
        };
        Lent { releasable: std::mem::take(&mut self.releasable), parking }
    }

    pub(crate) fn restore(&mut self, retaken: Vec<Taken>) {
        self.releasable = retaken;
    }
}

// platform/src/wasm_runtime/host/mod.rs
pub(crate) struct Waiting {
    pub timeout: Duration,
    pub threshold: Duration,
    pub until: Instant,
    pub cancel: CancellationToken,
}

pub(crate) fn block_on_released<T, E>(
    mut lent: Lent,
    waiting: Waiting,
    fut: impl Future<Output = Result<T, E>>,
) -> Result<(Result<T, HostIoError<E>>, Vec<Taken>), Interrupt> {
    block_on(async {
        let call = bounded(waiting.timeout, &waiting.cancel, fut);
        tokio::pin!(call);
        let parked = match lent.parking {
            Err(Held::NotRequested) => None,
            _ => tokio::select! {
                biased;
                ended = &mut call => return Ok((ended, lent.releasable)),
                () = tokio::time::sleep(waiting.threshold) => lent.park().map_err(Held::count).ok(),
            },
        };
        let ended = call.await;
        match parked {
            None => Ok((ended, lent.releasable)),
            Some(parked) => parked
                .resume(waiting.until, &waiting.cancel)
                .await
                .map(|retaken| (ended, retaken)),
        }
    })
}
```

  At a call site, for example `service_call`:

```rust
let lent = caller.data_mut().lend(request.wait);
let state = caller.data();
let released = block_on_released(lent, state.waiting(timeout), call(state, request, timeout, &resolved));
let (outcome, retaken) = released?;
caller.data_mut().seat.restore(retaken);
```

  - `http_fetch_v2` uses a new `exchange_released`, next to `exchange`
    (`host/util.rs:11`):
    1. it reads and parses the request;
    2. it takes `lend(request.wait)` through `data_mut`;
    3. it runs the handler with `&HostState`;
    4. it restores the seat and answers.

    `exchange` itself is unchanged for every other host function.
  - `HostState::lend` passes `self.db.has_open_transaction()` to
    `Seat::lend`.
  - `Lent::park` (above) works on the lent permits only. A call that
    answers within the threshold counts nothing, whatever its `Held`
    reason.

- **The helpers it uses.**
  - `bounded` is today's `block_on_within` body, without its `block_on`.
  - `DbSession::has_open_transaction` is new: whether the session holds any
    guest-owned transaction slot.
  - `Parked::resume` returns `Interrupt::Deadline`, `Interrupt::Cancelled`
    or `Interrupt::Shutdown` when it cannot finish.
- **How it ends.**
  - A resume that cannot finish makes the host function return
    `Err(Interrupt)`. wasmtime turns that into a trap at once, and
    `classify` already maps each to its `ExecutionError`
    (`mod.rs:1206`). The guest never runs another instruction without its
    slots.
  - The `?` on `released` returns the `Interrupt` from the host function.
    The two host functions therefore change from returning `i32` to
    returning `wasmtime::Result<i32>`. `func_wrap` accepts both, and the guest-facing
    signature is unchanged.
- **Memory.** The memory charge is untouched throughout. The guest's
  `GuestLimits`, linear memory and response reservation stay where they
  are, so the ledger sees no change from parking.

### 2.5 Resuming

A resumed guest has partly run, so it cannot be refused cleanly. It is
therefore granted ahead of new work, and it waits only within its own
deadline.

- **Gates.** Each releasable semaphore is wrapped in a `Gate`:
  - the service `all` slot;
  - the committed and background class caps;
  - the background global cap;
  - the global pool.

  A `Gate` is the semaphore plus a count of resumes waiting on it.

```rust
pub(crate) struct Gate {
    slots: Arc<Semaphore>,
    resuming: AtomicUsize,
    resumed: Notify,
}

impl Gate {
    async fn resume(self: &Arc<Self>) -> OwnedSemaphorePermit;
    async fn admit(self: &Arc<Self>) -> Result<OwnedSemaphorePermit, AcquireError>;
}
```

- **The two ways in.**
  - `resume` increments `resuming`, awaits a permit, then decrements it and
    signals `resumed`.
  - `admit` is what `Budget::slot` and `Budget::global_slot` call for new
    work. While `resuming > 0`, it first waits on `resumed`, then awaits a
    permit.
  - Admission's existing deadlines wrap the whole of `admit`: 1 s for
    interactive work, and the background and committed bounds. An arrival
    held behind resumes is refused exactly as one held behind running
    guests is today.
  - Try-only takers (nested, and warn-mode service slots) use
    `try_acquire` on the inner semaphore and are not held back.
- **Why resumes cannot starve new work.** Resumes are bounded by the wait
  pools.
- **The bound on a resume.** It waits at most until the invocation's
  deadline, its cancel token or the shutdown hard deadline. On the first of
  these, it drops the permits it has retaken so far, which are a prefix of
  the order, and returns the matching `Interrupt`.
- **No deadlock.** A resume waits only for top-level guests' slots. Its own
  descendants have finished, because the call it waited on has returned.
  The nested pool is not one of its gates, because nested guests never park.
  Every holder of a slot it waits on is either running, which ends, or
  parked, which holds no slot. So no cycle exists.
- **Under `off` and `warn`.** Admission takes the global slot by an
  unbounded `acquire_owned` (`budget/mod.rs:571`) and takes no or try-only
  service slots. A resume retakes exactly what the seat held, so it waits
  for the global slot, bounded by the deadline.

### 2.6 Bounds

Parking lets more top-level guests be in flight at once than there are
global slots: at most `WASM_MAX_CONCURRENT + WASM_MAX_PARKED`. Every size
that assumed one top-level guest per global slot grows by the same amount,
and only when releasing is enabled.

| Size | Today | With releasing on |
|------|-------|-------------------|
| Blocking pool (`budget::runtime`) | `C × (1 + D) + 64` | `(C + P) × (1 + D) + 64` |
| Nested pool | `C × D` | `(C + P) × D` |
| Top-level guests in flight | `C` | `C + P` (at most `P` parked) |
| One service's guests in flight | `S` | `S + Ps` (at most `Ps` parked) |

Here:

- `C` is `WASM_MAX_CONCURRENT` (256);
- `D` is `MAX_CALL_DEPTH` (4);
- `P` is `WASM_MAX_PARKED`, which defaults to `C`;
- `S` is `WASM_MAX_CONCURRENT_PER_SERVICE` (32);
- `Ps` is `WASM_MAX_PARKED_PER_SERVICE`, which defaults to `2 × S`.

**Why the factor `(1 + D)`.** A parked guest that waits on a service call
has a chain of up to `D` callees running, each on its own thread with a
nested permit. A new top-level guest admitted into its released slot can
build its own chain. So each top-level guest in flight, parked or not, can
cover `1 + D` threads and `D` nested permits, which is the rule the
memory design applied to slots (§3.3).

**What it costs.**

- With the defaults, the blocking pool's ceiling rises from 1 344 to 2 624
  threads. tokio starts blocking threads lazily and retires idle ones after
  two seconds (`thread_keep_alive`), so the ceiling is only reached under
  that much parked load.
- Every thread that has run a guest carries a stack charge in the ledger
  (§2.2 of the memory design, `budget/stack.rs`). Under `enforce`, the
  memory budget therefore bounds parked guests by what they really hold:
  their memory charge plus their thread stack. Parking never lets memory
  pass the budget.
- Under `warn`, the extra threads are measured and counted, as every other
  memory use is.

**The pids limit.** Where the process's cgroup is v2, startup reads
`pids.max` next to `memory.max` (`budget/cgroup.rs`). It warns when the
blocking ceiling plus the runtime's core threads plus 64 reaches it. A
deployment that hits the warning lowers `WASM_MAX_PARKED` or raises the
limit. If the pids limit is the binding constraint, option C becomes the
answer (section 2.1).

**Fairness between services.** A parked guest does not count against its
service's slots, so a proxy-shaped service can have `S` guests running and
`Ps` parked at once. `Ps` keeps one service from taking the whole global
wait pool, as `S` keeps it from taking the whole global slot pool. Its
memory still counts toward its `WASM_MEMORY_SERVICE_SHARE_PERCENT`.

### 2.7 Failure handling

| Event | What happens |
|-------|--------------|
| Pool full, open transaction, nested, committed, reducer or switch off | The call holds its slots and runs as today. `cufflink_wasm_wait_release_total{result="held_*"}` counts it. |
| The call answers within the threshold | Nothing is released. The call returns as today. |
| The call fails or times out while parked | The guest resumes, then receives the call's error as today: `HttpErrorKind::Timeout`, or `CallError::Timeout`. |
| Resume passes the invocation's deadline | The host function traps with `Interrupt::Deadline`, and the invocation ends `DeadlineExceeded` (a route answers as for any deadline). The upstream call has finished, and its effects stand, as when a guest times out after a held call today. |
| The invocation is cancelled while parked or resuming | `Interrupt::Cancelled`, and `ExecutionError::Cancelled`. |
| Shutdown's hard deadline while parked or resuming | `Interrupt::Shutdown`, and `ExecutionError::ShuttingDown`. A subscription message or a job is then settled as an interrupted one is today: a nak, or `wasm_jobs::release`. |
| A guest panics or traps after resuming | Unchanged. The seat drops with the store, and every permit with it. |

A parked guest's wait-pool permits drop with its `Parked`. A resumed
guest's retaken permits drop with the store, as admission's permits do
today. No path holds a permit past the store.

### 2.8 Metrics and logs

- `cufflink_wasm_wait_release_total{kind, result}`. `result` is one of:
  - `parked`;
  - `answered_first`;
  - `held_pool_full`;
  - `held_service_pool_full`;
  - `held_transaction`;
  - `held_not_parkable`;
  - `held_disabled`.
- `cufflink_wasm_parked{kind}`: a gauge of guests parked now.
- `cufflink_wasm_parked_seconds{kind}`: a histogram of time parked.
- `cufflink_wasm_resume_wait_seconds{kind, result}`: a histogram of time
  from the answer to slots retaken. `result` is `resumed`, `deadline`,
  `cancelled` or `shutdown`.
- `cufflink_wasm_blocking_threads`: a gauge of live blocking threads, read
  from tokio's runtime metrics. It is the number that decides whether
  option C is ever needed.
- **Logs.**
  - A resume that ends at its deadline is logged at `warn` once per
    invocation, with tenant, service, handler and the time parked.
  - Parking itself is logged at `debug`.

## 3. #59: a service call as the caller

### 3.1 Options

**A. Forward the raw bearer token through the in-process path.**

- The platform would keep the inbound `authorization` value host-side and
  attach it to the call.
- The target would authenticate it through `authenticate`, as on a direct
  call.
- **Against:**
  - The raw token would be re-validated on every hop.
  - It would cross the worker's HTTP hop to `PLATFORM_INTERNAL_URL` as a
    bearer, which is replayable for its whole lifetime: minutes, not 60 s.
  - It would have to be stripped from the target guest's headers.
  - It would not exist for API-key principals.
  - It would make every hop's validity depend on JWKS availability.

**B. A platform-minted delegation inside the service identity
(recommended).**

- The caller's platform turns the `AuthUser` it verified into a
  `Delegation` and signs it into the `cfsi1` token it already mints.
- The target verifies the token, as it does today, and rebuilds the
  `AuthUser` from the delegation.
- Permissions are resolved by subject on the target, as on a direct call
  (section 1.4).
- **For:**
  - No credential crosses any hop.
  - The delegation lives at most as long as the identity (60 s, and the
    caller's budget).
  - It never outlives the user's token.
  - The same path serves the in-process and worker cases.
  - It is fail-closed on platforms that do not understand it (section 6).

**C. A typed request extension on the in-process path only.**

- Unforgeable and cheap in process, but it has no worker path.
- It would be a second trust mechanism next to the identity token, so it
  is rejected.

### 3.2 The security model

1. **Opt-in twice, by declaration and by call.**
   - A custom route declares the services it may act as the caller on:
     `as_caller("loan-service")` or `as_caller("*")`.
   - The call sets `.as_caller()`.
   - The declaration is in the manifest, so every deploy shows which routes
     can carry a user's authority into which services. A call without the
     declaration is refused.
2. **Only a principal the platform verified on the inbound request.**
   - **Built from `AuthUser`.** The delegation is built from the `AuthUser`
     that `authorize_route` (or `resolve_data_route` for CRUD before-hooks)
     produced. That happens after JWKS validation (signature, issuer and
     `exp`), the tenant's audience allowlist and tenant binding.
   - **Carried as a typed field.** It is carried host-side as a typed
     `Invocation` field.
   - **Never from guest data.** It is never built from `HostState.auth`,
     which is parsed from the request JSON, and never from anything the
     guest sends. The guest's `authorization`, `cookie` and `x-api-key`
     headers stay reserved and dropped (`host/service_call.rs:34`).
3. **Never a token from guest memory as-is.** The guest sends one flag.
   Nothing the guest writes reaches the delegation.
4. **Who can be delegated.**
   - **Users and service accounts.** JWT users, user API keys
     (`AuthMethod::ApiKey { user_sub }`) and service accounts (their own
     authority, section 12, question 6) can be delegated.
   - **Partner keys** cannot. Neither can anonymous callers, subscriptions,
     schedules, jobs, or after-hooks.
   - **Elevated flags.** `is_platform_admin` and `has_deploy_authority` are
     never delegated: the target's `AuthUser` has both `false`.
5. **Expiry.** The delegation carries the user token's `exp` (`uexp`), read
   from `TokenInfo.claims["exp"]`.
   - **At mint.** The caller refuses to mint when `uexp ≤ now + 1 s`. The
     identity's lifetime becomes `min(60 s, remaining budget, uexp − now)`.
   - **At verify.** The target refuses when `uexp < now − 5 s` of clock
     skew, which is the skew the identity already allows.
   - **Without `exp`.** API-key and service-account principals that have no
     `exp` are bounded by the identity's lifetime alone.
   - **Down a chain.** Each hop copies `uexp` and never extends it.
6. **Depth and time.** A delegated call is a service call:
   - it nests at most `MAX_CALL_DEPTH` deep;
   - it runs within the caller's remaining budget;
   - it is refused with 504 when the budget is spent.
7. **Single use.** A delegated identity is accepted once per `jti`
   (section 3.7).
8. **Same tenant only**, as every service call is. The target's tenant
   binding is therefore satisfied by construction. The audience allowlist is
   the tenant's, and the inbound token already passed it.
9. **Audit.** The target's audit record names the user as actor, and the
   calling service as `via` (section 3.6).
10. **Revocation** behaves exactly as on a direct call:
    - a role removed is seen when the 300 s permission cache refreshes;
    - a disabled user's token is honoured until its `exp`.

    Delegation adds no lifetime beyond the token's own.

### 3.3 Caller side

**Manifest.** `CustomRouteDefinition` gains one field:

```rust
/// Services this route's guest may call as its verified caller
/// (`"*"` for any service of the tenant). Empty: none.
#[serde(default, skip_serializing_if = "Vec::is_empty")]
pub as_caller: Vec<String>,
```

- **The macro** accepts the `as_caller("loan-service", …)` route keyword,
  like `internal(…)`:

  ```rust
  ("GET", "/loans/{id}/summary", "summary", perm("loans", "view"), as_caller("loan-service")),
  ```

- **Derived feature.** `derived_features` adds `service_call_as_caller`
  when the field is non-empty. A manifest without it derives what it did,
  so its deploy hash is unchanged.
- **The validator refuses `as_caller` on:**
  - a `job` route (jobs carry no delegation);
  - an `api_key` route (partner keys are not delegable);
  - an SSE route.
- **It accepts it** on `public`, `auth`, `perm`, `owner`, `scoped` and
  `internal` routes. On `public`, the principal is optional and the call is
  refused when there is none. On `internal`, the route may pass on a
  delegated principal it received.

**Dispatch.** After `authorize_route`, `dispatch` builds the grant when the
route declares `as_caller` and the principal is an `AuthUser`:

```rust
// platform/src/auth/delegation.rs
pub(crate) struct DelegationGrant {
    pub principal: Delegation,
    pub targets: Targets,
}

pub(crate) enum Targets {
    Any,
    Listed(Vec<String>),
}

impl DelegationGrant {
    pub(crate) fn of(
        route: &CustomRouteDefinition,
        user: Option<&AuthUser>,
        call: Option<&ServiceCaller>,
    ) -> Option<Self> {
        let targets = Targets::of(&route.as_caller)?;
        let principal = call
            .and_then(|call| call.delegated.clone())
            .or_else(|| user.and_then(Delegation::of))?;
        Some(Self { principal, targets })
    }
}
```

- **Where the grant comes from.**
  - On a route reached by a delegated call, the grant takes the delegation
    it arrived with, `uexp` included, so a chain never extends it.
  - Otherwise, `Delegation::of(&AuthUser)` builds one for a JWT user or a
    user API key. It returns `None` only for a JWT whose claims lack `exp`,
    which validation never admits. Partner keys are not an `AuthUser`, so
    they never reach it.
- **Where the grant travels.**
  - The grant goes into `Invocation.delegation: Option<Arc<DelegationGrant>>`
    and from there into `HostState`.
  - A worker-routed route sends it in
    `WasmJob.delegation: Option<DelegationGrant>`, which is
    `#[serde(default, skip_serializing_if = "Option::is_none")]`, so other
    jobs are byte-identical.
  - The worker trusts the platform's envelope, as it trusts the whole
    request JSON today. Guests cannot publish on the worker subjects
    (`nats_publish_authz`).
- **CRUD before-hooks** get the grant only when their table declares it.
  That is not in this change (section 11), so they get `None`.

**Host function.** `CallRequest` gains one field:

```rust
#[serde(default, skip_serializing_if = "Identity::is_service")]
identity: Identity,

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Identity {
    #[default]
    Service,
    Caller,
}
```

`call` checks the following, in order, before minting:

1. `HostState.delegation` is `Some`. Otherwise: `Denied("this invocation has
   no verified caller to act as")`.
2. The target is in `targets`. Otherwise: `Denied("route does not declare
   as_caller for loan-service")`.
3. `uexp` is more than a second away. Otherwise: `Denied("the caller's token
   has expired")`.
4. After minting, the whole token fits `MAX_IDENTITY_BYTES` (7 680 bytes).
   Otherwise: `Denied("the caller's identity is too large to forward")`.

   The cap applies to the minted token, not to the delegation. The header
   carries every claim, the `cfsi1.kid.` prefix and the 43-character
   signature, and `path`, whose length the guest chooses, is one of those
   claims. So only a check on the minted token bounds the header.

It then mints the identity with the delegation and the shortened lifetime.
`on_behalf_of` keeps its meaning and value.

### 3.4 The delegation claim

```rust
// platform/src/auth/service_identity.rs
pub struct Claims {
    // every existing field, unchanged
    #[serde(rename = "act", default, skip_serializing_if = "Option::is_none")]
    pub acting_for: Option<Delegation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delegation {
    #[serde(rename = "sub")]
    pub subject: String,
    #[serde(rename = "am")]
    pub method: DelegatedMethod,
    #[serde(rename = "sa", default, skip_serializing_if = "std::ops::Not::not")]
    pub service_account: bool,
    #[serde(rename = "rlm", default, skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
    #[serde(rename = "uexp", default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    #[serde(rename = "tok", default, skip_serializing_if = "Option::is_none")]
    pub token: Option<DelegatedToken>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "k", rename_all = "snake_case")]
pub enum DelegatedMethod {
    Jwt,
    ApiKey { id: Uuid },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegatedToken {
    pub preferred_username: Option<String>,
    pub name: Option<String>,
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub realm_roles: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub claims: BTreeMap<String, Value>,
}
```

- **Existing tokens stay byte-identical.** An identity without a delegation
  serialises exactly as today, which
  `a_token_is_signed_as_every_release_signs_it` keeps pinning. The token
  format stays `cfsi1`.
- **`DelegatedToken.claims` is the whole `TokenInfo.claims`.** The target
  guest's `auth.claims` is therefore identical to a direct call's, so no
  handler has to learn a subset.
  - A delegated identity whose minted token would pass
    `MAX_IDENTITY_BYTES` (7 680 bytes, below the 8 KiB a header value can
    safely carry) is refused rather than cut.
  - In practice, the delegation's JSON gets roughly 5 KiB once base64 (4/3)
    and the other claims are counted.
  - Section 12, question 4, asks whether to carry a subset instead.
- **The verify checks.** `verify` stays a pure function of the token, the
  expected call and the time. It keeps its checks and adds one, in the same
  `find_map` list: a delegation present with `depth == 0` gives
  `IdentityError::Format`. A depth of 0 cannot come from a call.
- **The delegation checks.** The user's expiry and single use are checked
  after `verify`, by `internal::delegated(claims, now, seen)`. It is called
  where `ServiceCaller` is built, in `internal::continued` and
  `internal::admit`:
  - `uexp` past gives `Refusal::Delegation(DelegationError::Expired)`
    (`delegation_expired`);
  - a second use gives `Replayed`, `ReplayCapacity` or `ReplayUnavailable`
    (section 3.7).

  Unlike an invalid identity, which `continued` ignores on lenient postures
  today, a refused delegation answers 401 on **every** posture (section
  3.8).

### 3.5 Target side

- **`ServiceCaller`.** It gains `delegated: Option<Delegation>`, set from
  `claims.acting_for` once the single-use check (section 3.7) passes.
- **`authorize_route`.** When `call.delegated` is `Some`, it presents the
  rebuilt `AuthUser` to the posture gate in place of
  `authenticate(headers)`.
  - It runs the same `gate.admit`, `gate.admission` and
    `resolve_permissions_cached` path that a direct call runs.
  - `bind_principal` is skipped: the tenant is the service's by
    construction.
  - Any `authorization` header beside a verified delegation is ignored.
    This cannot happen through `service_call`, which drops it.
- **The rebuilt user:**

  ```rust
  impl Delegation {
      pub(crate) fn into_user(self, tenant: &TenantEntry) -> AuthUser {
          AuthUser {
              subject: self.subject.clone(),
              name: self.token.as_ref().and_then(|t| t.name.clone()),
              email: self.token.as_ref().and_then(|t| t.email.clone()),
              tenant_id: tenant.id,
              tenant_slug: tenant.slug.clone(),
              auth_method: self.method.into(),
              token_info: self.token.map(|t| t.into_info(self.subject)),
              is_service_account: self.service_account,
              is_platform_admin: false,
              has_deploy_authority: false,
              realm: self.realm,
          }
      }
  }
  ```

- **By posture:**
  - **`auth`, `owner` and `perm`** admit the delegated user exactly as a
    direct one. `perm` resolves the user's grants by subject.
  - **`scoped`.** `scoped::admit` takes a presented principal instead of
    reading headers itself, so it computes the same effective scope.
  - **`internal`** admits by its caller list as today. With a delegation,
    the principal becomes `Principal::User(Some(user))` instead of
    `Principal::Service`, so the guest sees `auth` and can check
    permissions itself.
  - **`public`** treats the delegated user as its optional principal.
  - **`api_key`** ignores the delegation. Only partner keys pass it.
- **CRUD.** `resolve_data_route` takes the delegated user as `principal`
  when one verified, so `auth_required`, `owner_field`, `permission_area`
  and scoped tables apply the user's rights.
- **What the target guest sees.**
  - `req.auth()` is the user, including `permissions`, `role_names` and
    `claims`.
  - `req.caller()` is the calling service. The request JSON's `caller`
    object gains `"delegated": true`, only when delegated, so every other
    request is byte-identical.
  - The SDK exposes it as `Request::delegated_by() -> Option<&Caller>`,
    which is `Some` only for a delegated principal. The `Caller` struct
    keeps its fields.

### 3.6 Audit

`Actor` gains `via: Option<String>`, serialised only when set. In
`dispatch`, `actor` (`router/custom/dispatch.rs:529`):

```rust
match (call, principal) {
    (Some(call), _) if call.delegated.is_some() => Actor::of(user).via(&call.service),
    (Some(call), _) => Actor::service(&call.service),
    (None, Some(NamedPrincipal::ApiKey(key))) => partner(key).unwrap_or_else(|| Actor::of(user)),
    (None, _) => Actor::of(user),
}
```

- **The record.** `user_id`, `user_name` and `subject` are the user's. The
  actor kind is the user's (`user`, `service_account` or `api_key`).
  `actor.via` names the calling service.
- **CRUD** builds the same actor from `DataRoute.call`.
- **What it answers.** A reader of the audit trail can tell a user's direct
  write from the same user's write made through another service, and which
  service it was.

### 3.7 Replay

Every delegated identity is single-use. Each replica keeps a seen-set:

```rust
pub(crate) struct SeenIdentities {
    seen: Mutex<HashMap<Uuid, i64>>,
    capacity: usize,
}

impl SeenIdentities {
    pub(crate) fn first_use(&self, jti: Uuid, expires_at: i64, now: i64) -> Result<(), DelegationError>;
}
```

- **Where it runs.** `first_use` runs exactly once per request, inside
  `internal::delegated`, and never inside `verify`, which unit tests and
  the token's own checks call freely.
  - A request reaches it from exactly one site: `authorize_route`
    (`router/postures/mod.rs:178`) for custom routes, or
    `resolve_data_route` (`router/authz.rs:81`) for CRUD, GraphQL and data
    streams. Each handler calls exactly one of them once.
  - `send_http` sends through `EgressGuard::send`, which does not retry, so
    a worker's delegated call is sent once. A retry added there later must
    mint a fresh identity per attempt. The test
    `a_delegated_call_is_verified_once_and_sent_once` pins both.
- **How it decides.** `first_use` prunes entries past
  `expires_at + CLOCK_SKEW_SECS`. It then inserts the `jti`, or refuses it:
  - `DelegationError::Replayed` (`replayed`) when the `jti` is present;
  - `DelegationError::ReplayCapacity` (`replay_capacity`) when the set is at
    `SERVICE_IDENTITY_REPLAY_MAX` (262 144) after pruning.
- **Memory.** Entries live at most 65 s, so the default bounds the set near
  13 MiB and admits about 4 000 delegated calls a second per replica.
- **In-process calls** are minted and verified by the same replica, so the
  replica-local set catches every replay.
- **Calls from a worker** reach any replica. A replay to a *different*
  replica within the identity's lifetime is not caught locally.
  - When the platform has Redis, a delegated identity that arrived over
    HTTP is also claimed with `SET cufflink:sid:{jti} 1 NX EX {ttl}`. A
    failed claim is refused as `replayed`, and Redis being unreachable is
    refused as `replay_unavailable`, fail-closed.
  - The in-process path skips Redis: an extension set by `LocalDispatch`
    marks the request as minted here.
  - Section 12, question 7, asks to confirm the Redis claim.
- **Service-only identities** are left as they are. `docs/service-calls.md`
  is corrected to say what is true: an identity is bound to one tenant,
  service, method and path, lives at most 60 s, and is single-use only when
  it carries a delegation. Section 12, question 8, asks whether to extend
  the seen-set to every identity.

Binding a delegated identity to a digest of its body was considered and
rejected. Routes authorise before they read the body (memory design §3.7),
and CRUD bodies are parsed by the extractor before any hook. A digest
check would either move authorisation after body reads or add a second,
late refusal path. Single use gives the same protection for a captured
header.

### 3.8 Failure handling

On the caller, every refusal is a `CallError`, never a trap:

| Event | `CallError` | Envelope `kind` |
|-------|-------------|-----------------|
| No verified principal (anonymous, partner key, subscription, schedule, job, after-hook) | `Denied` | `denied` |
| The route does not declare `as_caller` for the target | `Denied` | `denied` |
| The user's token expires within a second | `Denied` | `denied` |
| The minted token passes `MAX_IDENTITY_BYTES` | `Denied` | `denied` |
| Depth exceeded | `DepthExceeded` | `depth_exceeded` |
| Budget spent | `Timeout` | `timeout` |

No new `CallError` variant is added, so no guest's exhaustive `match`
breaks. An older SDK maps any unknown `kind` to `Transport`
(`cufflink-fn/src/call.rs:141`).

On the target, a refusal is an HTTP answer the caller reads as a
`ServiceResponse`:

| Event | Status | `cufflink_service_identity_rejected_total{reason}` |
|-------|--------|---------------------------------------------------|
| Delegation expired, on any posture | 401 | `delegation_expired` |
| Replayed, or the seen-set is full, or Redis is unreachable, on any posture | 401 | `replayed`, `replay_capacity`, `replay_unavailable` |
| An identity that fails `verify` (signature, binding, lifetime) | Unchanged: 401 on `internal`; ignored on other postures, which then run as for a request without one | unchanged reasons |
| The user lacks the permission | 403 | not counted (an authorisation answer) |
| A target replica too old to read `act` | 401 on `auth`, `perm`, `owner` and `scoped` routes; `internal` and `public` routes run with no user | not counted |

The last row is the fail-closed behaviour of a mixed-version fleet
(section 6).

**A refused delegation answers 401 on every posture.** This is a deliberate
departure from today's rule that `continued` ignores an invalid identity
outside `internal` routes.

- The identity verified, so the platform knows the call was made to act as
  a user.
- Running it anonymously on a `public` or legacy route would answer a
  question the caller did not ask: data shaped for no user, under the
  user's name in the caller's logic.

An identity whose signature or binding fails is still treated as today,
because nothing in it can be trusted, not even the claim that it was
delegated.

### 3.9 Metrics and logs

- `cufflink_service_calls_total{caller, target, result}` gains
  `identity="service"|"caller"`.
- `cufflink_service_call_delegations_total{result}`. `result` is one of:
  - `minted`;
  - `no_principal`;
  - `not_declared`;
  - `expired`;
  - `too_large`.
- `cufflink_service_identity_rejected_total{reason}` gains these reasons:
  - `delegation_expired`;
  - `replayed`;
  - `replay_capacity`;
  - `replay_unavailable`.
- **Logs.**
  - A delegated call is logged at `debug` on the caller, with target, depth
    and the user's subject.
  - A refused delegation is logged at `warn` on the target, with reason,
    service and path. The token, its claims and the user's email are never
    logged.

## 4. SDK, host functions, manifest and features

### 4.1 Host functions

No new host function is added. Two existing JSON requests gain an optional
field. Both are `deny_unknown_fields`, and both fields are omitted when
they hold their default, so every existing request is byte-identical.

| Host function | Type | New field | Wire |
|---------------|------|-----------|------|
| `http_fetch_v2` | `FetchRequestV2` (`types/src/host/http.rs:7`) | `wait: Wait` | `"wait": "release"` |
| `service_call` | `CallRequest` (`host/service_call.rs:71`) | `wait: Wait`, `identity: Identity` | `"wait": "release"`, `"identity": "caller"` |

```rust
// types/src/host/wait.rs
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Wait {
    #[default]
    Hold,
    Release,
}

impl Wait {
    pub fn is_hold(&self) -> bool {
        matches!(self, Wait::Hold)
    }
}
```

`http_fetch`, `http_fetch_timeout` and `http_fetch_many` keep their
signatures and always hold. A guest moves to `HttpRequest` (feature
`http-v2`) to release.

### 4.2 Platform features

New `HOST_FEATURES` entries are keyed on the existing functions, as
`guest_statement_timeout` → `db_call` and `storage_acl` → `s3_upload`
already are:

```rust
("wait_release", &["http_fetch_v2", "service_call"]),
("service_call_as_caller", &["service_call"]),
```

`GET /api/version` advertises both, and the tables in `docs/versions.md`
list them. The platform test that compares those tables with `supports`
fails until they do.

### 4.3 SDK

| `cufflink-fn` feature | Platform feature it requires | Enables |
|-----------------------|------------------------------|---------|
| `wait-release` | `wait_release` | none. `release_while_waiting` exists on `HttpRequest` with `http-v2` and on `ServiceCall` with `service-call` |
| `service-call-as-caller` | `service_call_as_caller` (and `service_call`) | `service-call` |

The markers go into `declare_features!` (`cufflink-fn/src/features.rs`),
and the SDK test that compares the map with `[features]` covers them.

```rust
// cufflink-fn/src/call.rs
pub struct ServiceCall<'a> { /* service, method, path, headers, body, wait, identity */ }

impl<'a> ServiceCall<'a> {
    pub fn new(service: &'a str, method: &'a str, path: &'a str) -> Self;
    pub fn header(self, name: &'a str, value: &'a str) -> Self;
    pub fn body(self, body: &'a [u8]) -> Self;
    pub fn json<T: Serialize>(self, value: &T) -> Result<ServiceCall<'a>, CallError>;
    #[cfg(feature = "wait-release")]
    pub fn release_while_waiting(self) -> Self;
    #[cfg(feature = "service-call-as-caller")]
    pub fn as_caller(self) -> Self;
    pub fn send(self) -> Result<ServiceResponse, CallError>;
}
```

- **`call_service` is unchanged.** It becomes
  `ServiceCall::new(..).headers(..).body(..).send()`, with the same request
  bytes.
- **`HttpRequest`** gains `release_while_waiting()` under `wait-release`.
- **`Request`** gains `delegated_by() -> Option<&Caller>` under
  `service-call-as-caller`.
- **The test host** (`test-host`) handles both:
  - `release_while_waiting` is a no-op there: the test host runs natively
    and has no slots.
  - `as_caller` hands the target the `auth` the test request was built
    with, and `RequestBuilder::delegated_by(service)` sets the marker on a
    request under test.

### 4.4 Manifest

- `CustomRouteDefinition.as_caller` (section 3.3) derives
  `service_call_as_caller`.
- The `service!` macro gains the `as_caller(…)` keyword, and its validator
  refuses `as_caller` on `job`, `api_key` and SSE routes.
- No manifest field exists for releasing: it is a per-call choice.

## 5. Configuration

| Variable | Default | Meaning |
|----------|---------|---------|
| `WASM_WAIT_RELEASE` | `on` | `off` makes every released wait hold, and sizes the pools as today |
| `WASM_WAIT_RELEASE_AFTER_MS` | `50` | How long a call runs before its guest parks |
| `WASM_MAX_PARKED` | `WASM_MAX_CONCURRENT` | Guests parked at once per process |
| `WASM_MAX_PARKED_PER_SERVICE` | `2 × WASM_MAX_CONCURRENT_PER_SERVICE` | Guests of one `(tenant, service)` parked at once |
| `SERVICE_IDENTITY_REPLAY_MAX` | `262144` | Delegated identities a replica remembers |

- The blocking pool and the nested pool are derived (section 2.6) and have
  no variable of their own.
- `MAX_IDENTITY_BYTES` (7 680) is a constant, because the header limit it
  protects is not configurable.
- A worker-only process reads the same variables.

## 6. Compatibility

| Combination | Behaviour |
|-------------|-----------|
| A guest built with an SDK before this release | Unchanged. It sends neither field, its calls hold, and its identities carry no delegation. |
| A module declaring `wait_release` or `service_call_as_caller`, deployed to a platform that does not advertise it | The CLI refuses before upload, and the platform refuses the deploy with 422, as for every feature (`docs/versions.md`). |
| A manifest with `as_caller`, deployed to a platform without `service_call_as_caller` | Refused, through the derived feature. |
| A new platform calling an old platform replica (mixed rollout behind one internal URL) | The old replica ignores `act` (`Claims` is not `deny_unknown_fields`). The call arrives as a service-only identity: `auth`-family routes answer 401, and `internal` and `public` routes run without a user. Fail-closed: no user authority is ever granted by a replica that cannot check it. |
| A new platform sending a delegation to an old worker | The old worker ignores `WasmJob.delegation`, and `as_caller` answers `Denied("no verified caller")`. Fail-closed. |
| An old platform sending to a new worker | No `delegation` field, so the same `Denied`. |
| `WASM_WAIT_RELEASE=off` on a platform that advertises `wait_release` | Released waits hold. The feature stays advertised, because the request is accepted and answered correctly, and holding is always a valid way to run it. |
| Identity tokens without a delegation | Byte-identical to today's, and the signature test keeps pinning them. |
| Request JSON of non-delegated calls | Byte-identical: `caller.delegated` appears only when true. |

**Rollout order.** Upgrade platforms before workers, which is already the
documented order. A fleet mid-upgrade only ever refuses a delegated call; it
never wrongly grants one.

## 7. Tests

Every fixture uses the repository's invented names: tenants `acme` and
`globex`, services `loan-service` and `catalog-service`, hosts under
`example.com`.

### 7.1 Unit tests

`platform/src/wasm_runtime/budget/seat.rs`:

- `parking_releases_the_service_and_global_slots_and_keeps_the_route_slot`
- `a_seat_parks_only_with_a_permit_from_both_wait_pools`
- `a_full_service_wait_pool_holds_one_service_and_parks_another`
- `nested_committed_and_reducer_seats_never_park`
- `resume_retakes_slots_in_admission_order`
- `a_resume_is_granted_before_new_interactive_work`
- `new_work_held_behind_resumes_is_refused_at_its_admission_deadline`
- `a_resume_that_reaches_its_deadline_holds_nothing`
- `a_cancelled_resume_releases_the_slots_it_retook`

`platform/src/wasm_runtime/budget/mod.rs`:

- `the_blocking_and_nested_pools_cover_parked_chains`, checking
  `(C + P) × (1 + D) + 64` and `(C + P) × D`, and today's sizes when
  releasing is off.

`platform/src/wasm_runtime/budget/cgroup.rs`:

- `startup_warns_when_the_blocking_ceiling_reaches_pids_max`

`platform/src/wasm_runtime/host/mod.rs`:

- `a_call_answered_within_the_threshold_touches_no_semaphore`
- `a_guest_with_an_open_transaction_holds_its_slots`

`platform/src/auth/service_identity.rs`:

- `a_token_is_signed_as_every_release_signs_it` (unchanged, and it must
  still pass)
- `a_delegated_token_is_signed_and_verifies_for_its_exact_call`
- `a_delegation_at_depth_zero_is_malformed`
- `a_verifier_without_the_act_field_reads_a_delegated_token_as_service_only`,
  which deserialises into a copy of today's `Claims`

`platform/src/auth/delegation.rs`:

- `platform_admin_and_deploy_authority_are_never_delegated`
- `a_delegation_past_its_users_expiry_is_refused_after_verify`
- `partner_keys_and_anonymous_callers_have_no_delegation`
- `a_grant_on_a_delegated_route_keeps_the_user_expiry_it_arrived_with`
- `an_identity_never_outlives_the_users_token`
- `an_identity_past_max_identity_bytes_is_not_minted`, including one made
  too long only by its path
- `a_delegated_call_is_verified_once_and_sent_once`
- `an_expired_delegation_is_refused_on_a_public_route_too`
- `a_replayed_jti_is_refused_and_an_expired_one_is_forgotten`
- `a_full_seen_set_refuses_rather_than_forgets`

`platform/src/wasm_runtime/host/service_call.rs`:

- `as_caller_without_a_grant_is_denied`
- `as_caller_to_an_unlisted_target_is_denied`
- `the_delegation_comes_from_the_invocation_and_never_from_guest_headers`,
  where a guest-set `authorization` header is still dropped
- `a_request_without_wait_or_identity_is_byte_identical`

`types/src/manifest/route.rs`:

- `as_caller_derives_its_feature_and_is_omitted_when_empty`

`sdk-macros`:

- `as_caller_is_refused_on_job_api_key_and_sse_routes`

`cufflink-fn`:

- `call_service_sends_the_bytes_it_always_sent`
- `release_while_waiting_and_as_caller_set_their_fields`
- `delegated_by_is_some_only_for_a_delegated_principal`
- the feature-map test, with the two markers

### 7.2 Integration tests

`platform/tests/wasm_budget.rs`, under `enforce`, with
`WASM_MAX_CONCURRENT_PER_SERVICE=2`:

- `a_parked_proxy_lets_its_service_keep_computing`:
  - Two `catalog-service` routes call an upstream held at a gate, with
    `release_while_waiting`.
  - A third, compute-only route of `catalog-service` answers 200. Today it
    is refused 503.
  - Both proxies answer 200 once the gate opens.
- `a_held_proxy_still_holds_its_slots`: the same scenario without the flag,
  asserting today's 503, so the change is proven to be the flag.
- `a_parked_proxy_does_not_starve_another_service`: the same scenario with
  `WASM_MAX_CONCURRENT=2`, where a `loan-service` route answers 200.
- `a_full_wait_pool_runs_the_call_held`: `WASM_MAX_PARKED=1`. The second
  proxy holds, and `cufflink_wasm_wait_release_total{result="held_pool_full"}`
  counts it.
- `a_guest_that_cannot_resume_before_its_deadline_ends_deadline_exceeded`:
  - The upstream answers while compute routes hold every slot past the
    proxy's deadline.
  - The proxy ends at its deadline, and its upstream request was made once.
- `wait_release_off_holds_every_call`

`platform/tests/service_call_in_process.rs`:

- `a_parked_self_call_is_admitted_when_its_service_has_one_slot`: with
  `WASM_MAX_CONCURRENT_PER_SERVICE=1`, a route calling its own service with
  `release_while_waiting`. The callee is a nested invocation, and its
  `try_acquire` now finds the slot the caller released. Today that call
  answers 503.
- `a_nested_callee_never_parks`: the callee's own released wait is counted
  `held_not_parkable`.

New file `platform/tests/service_call_as_caller.rs`. The fixture is a
caller route with `perm("loans", "view")` and
`as_caller("loan-service")`. The targets are `loan-service` routes with
`auth`, `perm("loans", "approve")`, `owner` and `scoped`.

- `a_perm_route_applies_the_callers_own_permissions`: a user with
  `loans:approve` gets 200, and one without gets 403.
- `a_service_identity_alone_still_gets_401_on_an_auth_route`: the same call
  without `as_caller`.
- `the_target_sees_the_user_as_auth_and_the_service_as_caller`: it asserts
  `auth.sub`, `auth.claims` identical to a direct call, `caller.service`
  and `caller.delegated`.
- `an_undeclared_target_is_denied_before_any_call`
- `an_anonymous_caller_on_a_public_route_is_denied`
- `a_user_api_key_is_delegated_and_a_partner_key_is_not`
- `an_expired_user_token_cannot_be_delegated`: the realm is configured with
  a short token lifetime, and the call is made after it expires.
- `a_delegation_continues_through_a_chain_and_stops_at_depth_four`
- `a_chain_never_extends_the_users_expiry`
- `a_replayed_delegated_identity_is_refused`: the test captures the header
  of a worker-path call and replays it, and expects 401 `replayed`.
- `crud_tables_apply_the_delegated_users_rights`: covers
  `auth_required`, `owner_field` and `permission_area`.
- `a_scoped_route_reaches_the_rows_the_users_grants_reach`
- `audit_records_the_user_via_the_calling_service`
- `an_internal_route_sees_the_delegated_user`

`platform/tests/internal_posture.rs`:

- `an_internal_route_without_a_delegation_still_has_a_service_principal`

`worker/tests/worker.rs`:

- `a_worker_routed_route_acts_as_its_caller_over_http`
- `a_worker_parked_fetch_releases_its_slots`
- `an_envelope_without_a_delegation_denies_as_caller`

Feature negotiation, in the existing deploy and feature tests:

- `a_module_needing_wait_release_is_refused_by_a_platform_without_it`
- `a_manifest_declaring_as_caller_derives_its_feature`

Test guests go in `test-guests/guests`: a relay guest that sets either
flag from its request, and a target that echoes `auth` and `caller`.

## 8. Documentation, when implemented

- **`docs/service-calls.md`:**
  - "Calling as the caller": the declaration, the flag, expiry, depth, what
    the target sees, and audit.
  - "Waiting without holding a slot".
  - The corrected replay sentence (section 3.7).
- **`docs/wasm-guide.md`.** The `forward_auth` section points to
  `as_caller` as the way to call another service of the tenant as the user.
  `forward_auth` stays for calls to services outside the platform.
- **`docs/routing.md`.** The `as_caller` keyword next to the postures.
- **`docs/versions.md`.** Both features, both cargo features.
- **`docs/scaling.md`:**
  - the wait pools;
  - the derived pool sizes;
  - the `pids.max` warning;
  - when to lower `WASM_MAX_PARKED`.
- **`docs/observability.md`.** The metrics of sections 2.8 and 3.9.
- **`docs/audit.md`.** `actor.via`.
- **`docs/design-notes/guest-memory-budget.md`.** A line in §1.3 and §3.3
  saying that a parked caller gives back its slots, with a pointer here.

## 9. PR plan

Each PR passes `cargo fmt`, `cargo clippy --all-targets -- -D warnings`
and `cargo test --workspace` on its own, and ships nothing half-wired:
until the PR that accepts a field, no request can carry it.

**#58:**

1. **The seat.**
   - Labelled admission slots, `Seat`, `Gate`, `Parked` and the wait pools.
   - The derived pool sizes and the `pids.max` warning.
   - The configuration variables and the metrics.
   - The 7.1 seat, pool and cgroup tests.
   - No host function parks yet, and behaviour is unchanged.
2. **Released waits in the host.**
   - `Wait` in `cufflink-types`.
   - The `wait` fields on `FetchRequestV2` and `CallRequest`.
   - `block_on_released`.
   - The `wait_release` platform feature and the `docs/versions.md` rows.
   - The 7.2 budget and in-process tests, and the worker parking test.
3. **SDK for released waits.**
   - `wait-release`, `HttpRequest::release_while_waiting`, and `ServiceCall`
     with `release_while_waiting`.
   - The test host.
   - The guide, service-call and scaling documentation.

**#59** (independent of 1–3, except that `ServiceCall` lands with whichever
SDK PR comes first):

4. **Delegation in the identity.**
   - `Delegation`, `Claims.acting_for`, the verify checks, `SeenIdentities`
     and the Redis claim.
   - Target-side reconstruction in `authorize_route`, `scoped::admit` and
     `resolve_data_route`.
   - `Actor.via`.
   - The replay sentence fixed in `docs/service-calls.md`.
   - Nothing mints a delegation yet. A delegated token can only come from a
     test, and 7.1's verifier tests cover it.
5. **Minting.**
   - `CustomRouteDefinition.as_caller`, the macro keyword, the validator
     and the derived feature.
   - `DelegationGrant` on `Invocation` and `WasmJob`.
   - `identity` on `CallRequest`, and the `service_call_as_caller` feature.
   - `platform/tests/service_call_as_caller.rs` and the worker tests.
6. **SDK for calls as the caller.**
   - `service-call-as-caller`, `ServiceCall::as_caller` and
     `Request::delegated_by`.
   - The test host.
   - The routing, service-call, audit and observability documentation.

## 10. Release notes outline

- **Added: waits that release slots.** `HttpRequest::release_while_waiting`
  and `ServiceCall::release_while_waiting` (feature `wait-release`, platform
  feature `wait_release`).
  - A guest waiting on an outbound call gives back its service and global
    slots and takes them again before it continues.
  - Its memory, its thread, its route slot and any database connection
    stay held.
  - Bounded by `WASM_MAX_PARKED` and `WASM_MAX_PARKED_PER_SERVICE`.
  - Off with `WASM_WAIT_RELEASE=off`.
- **Added: service calls as the caller.** `ServiceCall::as_caller` (feature
  `service-call-as-caller`, platform feature `service_call_as_caller`) and
  the route keyword `as_caller(…)`.
  - The target applies the verified end user's own permissions.
  - The identity lives no longer than the user's token, is single-use, and
    is never handled by the guest.
  - `Request::delegated_by` tells a target the principal came through
    another service.
  - Audit records gain `actor.via`.
- **Changed: pool sizes.**
  - With releasing on, the blocking pool's ceiling is
    `(WASM_MAX_CONCURRENT + WASM_MAX_PARKED) × 5 + 64`, and the nested pool
    is `(WASM_MAX_CONCURRENT + WASM_MAX_PARKED) × 4`.
  - Startup warns when the ceiling reaches the cgroup's `pids.max`.
- **Changed: docs.** `docs/service-calls.md` no longer says every replayed
  identity is refused. Delegated identities are single-use, and other
  identities are bound to one call and live at most 60 s.
- **Upgrade notes.**
  - Upgrade platforms before workers. A mixed fleet refuses delegated calls
    (401 or `Denied`) and never grants them.
  - Deployments with a `pids.max` limit check the new warning.
- **Compatibility.** Guests and manifests that use neither feature run
  unchanged. Their requests, identity tokens and admission are identical.

## 11. Not in this change

- Parking the guest itself with wasmtime async (section 2.1, option C).
  That is its own design note, gated on the blocking-thread metric.
- Releasing on `http_fetch`, `http_fetch_timeout`, `http_fetch_many`,
  `nats_request`, `ext_call` and `util_sleep_ms`. The same
  `block_on_released` serves them once each has a request type that can
  carry the flag.
- Releasing the route slot (section 12, question 1).
- `as_caller` on table hooks. Their principal is the CRUD request's, and
  a declaration would belong on the table.
- Delegation into jobs, subscriptions or schedules, which run after the
  user's token has usually expired.
- A target-side opt-out for delegated principals (section 12, question 5).
- Per-tenant fairness for the wait pools. They are per service, as the
  memory budget's shares are.

## 12. Owner questions

Each question has a recommended default, which the design above assumes.

1. **Should a route be able to release its own `max_concurrent` slot too?**
   - *Default: no.* The route slot is the author's declared in-flight limit
     and may protect the upstream.
   - A later manifest flag can add it if authors ask.
2. **Parking threshold and pool sizes.**
   - *Default:* `WASM_WAIT_RELEASE_AFTER_MS=50`,
     `WASM_MAX_PARKED = WASM_MAX_CONCURRENT`, and
     `WASM_MAX_PARKED_PER_SERVICE = 2 × WASM_MAX_CONCURRENT_PER_SERVICE`.
3. **Should resumes go ahead of new interactive work?**
   - *Default: yes,* through `Gate` (section 2.5).
   - The alternative, plain FIFO, is simpler. But it lets a burst of new
     arrivals hold a partly-run guest, with its memory and thread, for up
     to their 1 s each.
4. **A user whose token claims make the identity pass `MAX_IDENTITY_BYTES`.**
   - *Default: refuse the delegation* (`Denied`), so `auth.claims` is
     always identical to a direct call's.
   - The alternative carries only the named fields and drops `claims`.
     That would change what the target guest sees.
5. **Should a target route be able to refuse delegated principals?**
   - *Default: not in this change.* A handler can check
     `req.delegated_by()` and answer 403.
   - A posture modifier can follow if needed.
6. **Delegate service-account and user-API-key principals?**
   - *Default: yes for both.* That is the caller's own authority. A
     service-account principal is admitted by `perm` routes without a
     grant, on the target exactly as on a direct call.
   - Partner keys: never.
7. **Shared replay protection.**
   - *Default: when Redis is configured, claim each delegated identity
     that arrived over HTTP with `SET NX EX`, and refuse when Redis is
     unreachable.* Without Redis, the replica-local seen-set only.
8. **Extend single-use to service-only identities?**
   - *Default: no.* Correct the document instead.
   - A service-only identity carries no user authority. Its replay window
     is one call's tenant, service, method and path for at most 60 s.
9. **Option C (wasmtime async).**
   - *Default: do not start it.* Revisit if
     `cufflink_wasm_blocking_threads` under parked load approaches a
     deployment's `pids.max`.
