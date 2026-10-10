# Extensions: native services and NATS providers

As amended by [tenant-scoped runners](extensions-tenant-scoped-runner.md)
(one ext-runner per tenant on a runner key; native services deployed with
the tenant's deploy authority), which wins where the two differ.

## Outcome

A project adds a capability to Cufflink without a Cufflink release, platform
image build or platform roll. A WASM service calls one stable host function,
`ext_call`. Behind it sits an extension registered for the tenant, served
either by a **native service** (Rust binary deployed with `cufflink deploy`
and supervised by the new `cufflink-ext-runner`) or a **NATS provider**
(a process the project runs itself). The guest never learns which.

## Architecture

```
WASM guest ──ext_call──▶ platform/worker host fn
                          1. manifest declares the name        → else not_declared
                          2. tenant registry has the name      → else not_found
                          3. registration lists the caller     → else denied
                          4. NATS request cufflink.ext.<tenant>.<name>
                                    │
             ┌──────────────────────┴─────────────────────┐
     cufflink-ext-runner (queue group)              NATS provider (queue group)
     relays over the child's Unix socket            cufflink-native run_nats()
             │                                            │
     native service child (cufflink-native run())   project's own process
```

One transport for both backends. The platform and the worker run identical
code (both already hold NATS and the tenant catalog), load balancing across
runner replicas comes from the queue group, children never hold NATS
credentials, and `cufflink.` is already reserved from guest publishing
(`nats_manager/subjects.rs`). A subject with no subscriber answers
"no responders" at once, which is how `unavailable` is detected.

## Names and subjects

- Extension name: 1–128 bytes, dot-separated segments of `[a-z0-9-]+`
  (`google.token`, `pdf.render-v2`). Validation lives once in
  `cufflink_types::ext::is_valid_extension_name` and is used by the manifest
  validator, the admin API, the SDK builder and the host function.
- Call subject: `cufflink.ext.<tenant_slug>.<name>` (`ext::call_subject`).
- Queue group: `cufflink-ext` for every subscriber (runner or provider).
- Deploy events: `deploy.ext.<tenant>.<service>`,
  `undeploy.ext.<tenant>.<service>` (reserved prefixes already).

## Shared wire types (`cufflink-types`, module `ext`)

`types/src/ext.rs` (published crate, used by platform, worker, runner,
`cufflink-fn`, `cufflink-native`):

- `is_valid_extension_name`, `call_subject(tenant, name)`, `QUEUE_GROUP`,
  `PROTOCOL_VERSION: u32 = 1`.
- `ExtRequest { v: u32, call_id: Uuid, extension: String, context: CallContext, payload: Value }`
- `CallContext { tenant: String, service: CallerService { name, version: i32 }, auth: Option<Value>, deadline_ms: u64 }`
  (`auth` is `AuthSnapshot.claims`, the verified claims the guest itself sees
  under `auth`; never a raw token or header, and `forward_auth` credentials,
  which reach the guest as headers, are never included. Trace context travels
  in NATS headers via `traced_nats_headers`, not in the body.)
- `ExtReply` = `{"ok": <value>}` | `{"error": {"code", "message"}}`, modelled as
  `enum ExtReply { Ok(Value), Error(ExtFailure) }` with the untagged-by-key
  serde shape.
- Guest ⇄ host envelope in `types/src/host/ext.rs`, following
  `types/src/host/nats.rs`: `CallRequest { extension, payload, timeout_ms: Option<u64> }`
  and `CallResponse { ok, value?, error?: { kind, code?, message } }` with
  `CallErrorKind { NotDeclared, NotFound, Denied, Unavailable, Timeout, TooLarge, Failed, Invalid }`
  (`as_str` gives the snake_case label). Round-trip tests use the existing
  `round_trip` helper.
- Readiness document served by a native child: `Ready { protocol: u32, extensions: Vec<String> }`.

## Manifest

`ServiceManifest` gains (both `#[serde(default, skip_serializing_if = "Vec::is_empty")]`):

- `extensions: Vec<String>` — extensions a WASM service may call.
- `provides: Vec<ProvidedExtension>` with
  `ProvidedExtension { name: String, callers: Vec<String> }` — only on
  `mode: native`.

`ServiceMode::Native` (`"native"`) is added. Features (`types/src/features.rs`):

- `EXT_CALL = "ext_call"` — derived when `extensions` is non-empty; also the
  host feature for the `ext_call` import and the `ext-call` cargo feature.
- `NATIVE_SERVICES = "native_services"` — derived for `mode: native`.

Every `ServiceMode` decision point handles `Native` explicitly: the two
exhaustive label matches (`api/deployments.rs service_mode_label`,
`cli deploy.rs`), `requires_wasm` (false), validator, `pipeline::run_locked` /
`after_release` / rollback, `app/mod.rs`, scheduler, reconciler,
`wasm_required_reason`. A native service has no tables, routes, subscriptions,
schedules, streams, events, cells, authorization, migrations or `extensions`;
the validator refuses any of them, refuses an empty `provides`, invalid or
duplicate names, invalid caller names (a service name or `*`), and `provides`
on any other mode.

`service!` gets an `extensions: ["a.b", ...]` keyword (parsed with
`keywords::once` + `strings`, added to the unknown-field message and its
`.stderr`). `mode: native` in `service!` is a compile error naming
`cufflink_native::service`; the existing catch-all for other modes is left as
is.

## The `ext_call` host function (platform and worker)

`platform/src/wasm_runtime/host/ext.rs`, registered with `wrap` through
`util::exchange`, `HOST_FEATURES` entry `("ext_call", &["ext_call"])`.

Plumbing: `extensions: Vec<String>` follows the `publishes` route exactly:
`RegisteredService.manifest` → `Invocation.extensions` (every constructor:
`router/custom/dispatch.rs`, `router/crud.rs`, `nats_manager/subscriptions.rs`,
hooks, schedules, and any other `Invocation` site the compiler finds) →
`HostStateInit`/`HostState.extensions: Arc<[String]>`; worker:
`ExecutionContext.extensions` read from `d.manifest->'extensions'` (never
from the job message).

Order of a call (pure `decide` function over state + request, then I/O):

1. Malformed request → `invalid`.
2. Name invalid or not in `HostState.extensions` → `not_declared`.
3. `state.app_state.tenants` entry for the caller's tenant has no registration
   → `not_found`.
4. Registration's callers contain neither the caller service nor `*` → `denied`.
5. The serialized `ExtRequest` (payload plus envelope) larger than
   `min(EXT_CALL_MAX_BYTES, NATS server max_payload)` → `too_large`.
6. Timeout = `clamp_to_budget(request.timeout_ms, EXT_CALL_TIMEOUT, remaining)`.
   Less than the floor left → `timeout` without sending.
7. `nats.request_with_headers(call_subject, traced headers, ExtRequest)` inside
   `block_on_within` (cancel-aware). No responders → `unavailable`; elapsed or
   cancelled → `timeout`; reply larger than the cap → `too_large`; reply not an
   `ExtReply` → `unavailable` with the decode error; `ExtReply::Error` with a
   reserved code (`unavailable`, `timeout`, `too_large`, which the runner and
   `cufflink-native` answer for the transport; `ExtFailure::kind`) → that
   kind; any other `ExtReply::Error` → `failed` carrying `code` and
   `message`; `ExtReply::Ok(v)` → ok.

Observability: span `ext.call` (`otel.kind = "client"`, `extension`);
`cufflink_ext_calls_total{caller, extension, result}` where `result` is `ok`
or the kind label and `extension` is `-` when the name was invalid;
`cufflink_ext_call_duration_seconds{extension}`. Payloads are never logged.

Platform config (`config.rs`, shared parser): `EXT_CALL_MAX_BYTES` (default
1 MiB, positive), `EXT_CALL_TIMEOUT_SECS` (default 30).

## Extension registry

Migration `platform/migrations/V5__extensions.sql`:

```sql
SET LOCAL lock_timeout = '10s';
CREATE TABLE IF NOT EXISTS tenant_extensions (
    tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name       TEXT NOT NULL,
    backend    TEXT NOT NULL CHECK (backend IN ('nats', 'native')),
    service_id UUID REFERENCES services(id) ON DELETE CASCADE,
    callers    TEXT[] NOT NULL CHECK (cardinality(callers) > 0),
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, name),
    CHECK ((backend = 'native') = (service_id IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS tenant_extensions_service_idx ON tenant_extensions (service_id);
```

`repo/extensions.rs`: list for tenant(s), replace a service's native rows,
register/unregister a NATS row. `TenantEntry` gains
`extensions: BTreeMap<String, Registration>` with
`Registration { backend: Backend, callers: Vec<String>, service_id: Option<Uuid> }`,
loaded with the tenant and refreshed through the existing
`tenants.refresh()` / `cufflink.tenants.changed` path, so the platform and the
worker authorise from memory.

Writers:

- **Native deploy** (activation of a `mode: native` manifest and its rollback):
  inside the activation transaction, lock the service's rows `FOR UPDATE`,
  refuse a `provides` that strands a deployed caller, then replace the rows
  with its `provides` (callers normalised by `ext::callers`); `refresh()`
  after release.
- **Operator** (`PlatformAdmin`, audited, same flow as `put_nats_claims`):
  - `GET /api/tenants/{slug}/extensions` → every registration.
  - `PUT /api/tenants/{slug}/extensions/{name}` body `{"callers": [..]}` →
    registers or updates a NATS provider.
  - `DELETE /api/tenants/{slug}/extensions/{name}` → removes a NATS provider.

Admission rules (deploy and admin), each a pure function with unit tests:

- A name is unique per tenant: a native deploy whose `provides` names an
  extension registered by another service or as NATS is refused (409); a
  NATS registration over a native one is refused (409).
- A WASM deploy, a WASM rollback and a module upload are refused (422) when
  any declared extension is not registered for the tenant or its registration
  does not list the service (or `*`), judged from the database at admission
  and again inside the activation transaction over the registrations read
  `FOR SHARE`, so an operator's concurrent change either waits for the
  activation or fails it.
- Removing a registration, narrowing its callers, deploying a native service
  whose new `provides` drops or narrows a name, or deleting a native service
  is refused (409) while a deployed service still declares it: the
  `stranded_by` rule over the current deployments of the tenant's active
  services (`repo::extensions::declarations`), read after the registration is
  locked `FOR UPDATE` in the same transaction as the write. `?force=true` on
  the admin DELETE/PUT and on the service DELETE overrides and is audited.
  Deleting a service cascades its rows.
- `CUFFLINK_EXTENSIONS` (platform bool, default `false`): when false,
  registering NATS providers and deploying or rolling back `mode: native`
  are refused (403 with the setting named). Calls, the read-only list,
  removing a NATS provider and deleting or switching a native service work
  regardless, so turning the setting off never strands what was deployed
  (`native::authorize` tells a `Change::Deploy` from a `Change::Manage`).
- Deploying, rolling back, deleting or switching the status of `mode: native`
  requires `user.is_platform_admin` (a platform-admin API key
  with `X-Tenant-Slug`, or a platform-realm token); tenant deployers are
  refused (403). Native code runs with the runner's identity, so it is an
  operator action.

## Native services

### `cufflink-native` crate (published, `native/`)

```rust
use cufflink_native::prelude::*;

struct GoogleToken;

impl Extension for GoogleToken {
    fn call(&self, ctx: &CallContext, payload: Value) -> Result<Value> {
        let scope = payload["scope"].as_str().ok_or("scope is required")?;
        Ok(json!({ "token": mint(scope)? }))
    }
}

fn main() -> std::process::ExitCode {
    cufflink_native::service("ext-google")
        .extension("google.token", GoogleToken, ["reports-service"])
        .run()
}
```

- `Extension: Send + Sync + 'static` with a sync `call`; the server runs each
  call on the blocking pool and gives up at the context deadline.
- `ExtError { code, message }` (anyhow-style: no `std::error::Error` impl,
  `From<E: Into<Box<dyn Error + Send + Sync>>>` with code `failed`,
  `ExtError::new(code, message)`), `Result<T> = std::result::Result<T, ExtError>`.
  A panicking extension answers `failed` with code `panic`.
- `CallContext` from `cufflink-types`, plus `deadline()` as an `Instant`.
- `service(name).extension(name, ext, callers)` validates names eagerly
  (`run` exits non-zero with the reason).
- `run()`:
  - `--emit-manifest`: prints the `mode: native` manifest
    (`name`, `tables: []`, `provides`) and exits 0. This is how the CLI reads it.
  - otherwise makes the process undumpable on Linux
    (`PR_SET_DUMPABLE` 0: execve resets it, so each child sets it itself;
    until it does, its environment is readable by its siblings, which is
    why tenants that must not see each other's secrets get runners of their
    own) and serves HTTP/1.1 on the Unix socket at `CUFFLINK_EXT_SOCKET`, to
    requests bearing `Authorization: Bearer $CUFFLINK_EXT_TOKEN` (401
    otherwise, compared in constant time; `run` refuses to start without
    the variable): `GET /ready` → `Ready`, `POST /call` with `ExtRequest` →
    `ExtReply` (unknown extension → error `not_found`). SIGTERM stops
    accepting and drains in-flight calls.
- `run_nats(NatsProvider::from_env()?)` serves the same extensions directly
  from NATS for a tenant (`CUFFLINK_TENANT`, `NATS_URL`, auth via
  `cufflink_nats::NatsAuth::from_env`), subscribing each
  `call_subject` with `QUEUE_GROUP`, undumpable on Linux as `run` is. This
  is the NATS-provider SDK.
- Both modes share one dispatch function (request → reply), tested once.

`cufflink-nats` becomes published (drop `publish = false`, add it to
`CRATES` before `cufflink-native` in `publish.yml`); `cufflink-native` is
added after it. No version bumps.

### `cufflink-supervisor` crate (`supervisor/`, internal)

Extracted from `web-runner/src/process_manager.rs` and `artifact.rs` without
behaviour change for the web-runner: the process table (`admit` / `close` /
`exited` keyed by an opaque endpoint), `supervise`, `Stop`, `ChildStop`,
`stop_child`/SIGTERM, quick-exit and crash-loop accounting (for a runner
that asks for it with `Runner::TRACKS_CRASH_LOOPS`: the web-runner; the
ext-runner's own holds are its only crash-loop count), rate-limited
output forwarding (log target and metric names supplied by the caller), the
env allowlist/credential filter (`EnvMode` parameterised by its passthrough
variable name, builtin allowlist split into a generic core plus a per-runner
list), `ensure_writable_dir`, hash-verified download to staging and atomic
`persist`. Web-specific parts (Next.js command, `.next` checks, ports,
`wait_for_ready` over TCP, hostnames, tarball validation) stay in the
web-runner. Existing web-runner tests keep passing unchanged in substance
(moved with the code they test).

### `cufflink-ext-runner` crate (`ext-runner/`, binary)

Startup mirrors the web-runner (telemetry, config, shutdown, DB, secrets,
NATS, storage, admin endpoints `/health`, `/_cufflink/ready`,
`/_cufflink/status`, `/_cufflink/metrics` gated by `EXT_STATUS_TOKEN`).

- **Converge, don't just react.** One reconcile function compares the desired
  set (every active native deployment with its artifact:
  `deployments d JOIN services s ON s.id = d.service_id AND d.version = s.current_version JOIN tenants t ...
  WHERE s.status = 'active' AND d.status = 'active' AND d.manifest->>'mode' = 'native'`,
  of the tenants `EXT_TENANTS` lists when it is set) with the
  running children and converges: start what is missing, replace what runs a
  different artifact hash, stop what is no longer wanted. It runs at start,
  every `EXT_RECONCILE_SECS` (30), and at once on any `deploy.ext.>` /
  `undeploy.ext.>` event (`deploy.ext.<tenant>.>` / `undeploy.ext.<tenant>.>`
  of its tenants with `EXT_TENANTS`; events are core NATS, at most once, so
  they only bring the next reconcile forward). There is no separate
  event-handling path. What follows a replacement or a removal (leaving
  queue groups, the old child's calls settling, its stop) runs in tasks the
  shutdown tracks, never in the reconcile, and a draining child's binary is
  kept from the prune until it stops.
- **Artifact:** the immutable, content-addressed object
  `native/{service_id}/{sha256}`; downloaded, SHA-256 verified, size-capped,
  written `0755` to `{APPS_DIR}/{tenant}/{service}/{sha256}/service` via the
  shared staging and atomic persist, and reused when already present.
- **Child:** `Command::new(binary)` with `env_clear`, the allowlisted env,
  the service's configs and secrets (`cufflink_secrets::configs::all`), then
  identity `CUFFLINK_EXT_SOCKET`, `CUFFLINK_EXT_TOKEN` (32 random bytes, hex,
  per start, sent as a bearer with every `/ready` and `/call`, so a sibling
  that can connect to the socket cannot call as the runner),
  `CUFFLINK_TENANT`, `CUFFLINK_SERVICE`. The runner makes itself undumpable
  on Linux at start, so no child reads its environment or memory.
  Socket path `{EXT_SOCKET_DIR}/{12 hex of service_id}-{version}-{start}.sock`
  where `start` is a per-runner counter, so every child start has its own
  socket and supervisor endpoint (a restart or replacement never unlinks or
  shadows a live child's socket). The runner checks at start that the directory
  leaves room under the 104-byte socket path limit.
- **Ready:** poll `GET /ready` over the socket until `EXT_READY_TIMEOUT_SECS`;
  its `protocol` must equal `PROTOCOL_VERSION` and its `extensions` must equal
  the manifest's `provides` names, else the version is refused (killed, old
  version keeps serving).
- **Routing:** only once ready, subscribe `call_subject(tenant, name)` with
  `QUEUE_GROUP` per provided name; each message is relayed as `POST /call`
  over a fresh socket connection with the request's deadline, and the child's
  reply (or `unavailable`/`too_large` `ExtReply::Error` on failure) is
  published to the message's reply subject. A relay takes its next call,
  then a permit of the service's `EXT_MAX_CONCURRENT_CALLS`, each raced
  against its stop, so an idle subscription holds no permit; stopped, it
  unsubscribes, relays what it took as permits free for 10 s and answers the
  rest `unavailable`. Replace: the new child becomes ready, the
  subscriptions switch to it, then the old child is terminated with
  `EXT_CHILD_STOP_TIMEOUT_SECS` so in-flight calls drain. Unsubscribe before
  stopping on undeploy and shutdown, so no replica without a child is in the
  queue group.
- **Crash and hang:** a child that exits is restarted with exponential backoff
  (1 s doubling, capped at 30 s); three quick exits (< 10 s) in a row mark it
  crash-looping until the next deploy. Every `EXT_HEALTH_INTERVAL_SECS` the
  runner probes `/ready`; three consecutive failures kill the child (which then
  follows the restart path).
- **Metrics:** `cufflink_ext_runner_children{state}` (gauge: `ready`,
  `starting`, `crash_looping`, `refused`),
  `cufflink_ext_runner_restarts_total{reason}`,
  `cufflink_ext_runner_calls_total{result}` (a child's reply carrying a
  reserved code under that code),
  `cufflink_ext_runner_call_duration_seconds`. Child output forwarded under log
  target `ext_child`.
- **Config:** `DATABASE_URL`, `NATS_URL` + NATS auth, storage env,
  `RUNNER_PORT` (3200), `APPS_DIR` (`./apps`), `EXT_SOCKET_DIR`
  (`/tmp/cufflink-ext`), `EXT_ENV_PASSTHROUGH`, `EXT_ARTIFACT_MAX_BYTES`
  (128 MiB), `EXT_RECONCILE_SECS` (30), `EXT_READY_TIMEOUT_SECS` (30),
  `EXT_HEALTH_INTERVAL_SECS` (10),
  `EXT_CHILD_STOP_TIMEOUT_SECS` (10), `EXT_MAX_CONCURRENT_CALLS` (64),
  `SHUTDOWN_GRACE_SECS` (45), `EXT_STATUS_TOKEN`, `EXT_TENANTS` (one runner
  per tenant or trust group, each with its own cloud identity), secrets env
  as the web-runner.

### Platform native deploy path

A native deploy is one atomic request, following `bundled_wasm` ("a deploy
carries its module"), not web's two-step manifest-then-upload flow:

- `DeployRequest` gains `native_base64: Option<String>`, required for and only
  accepted with `mode: native` (400 otherwise), decoded size capped by
  `NATIVE_ARTIFACT_MAX_BYTES` (default 128 MiB; base64 stays inside the
  router's 250 MiB body limit).
- The artifact is identified by its SHA-256. A deploy is skipped only when the
  manifest hash **and** the artifact hash equal the running deployment's (the
  existing `should_skip_deploy` generalised from the wasm hash to "the
  deployment's code hash"), so a code-only change always makes a new version.
- Before activation the binary is stored at the immutable key
  `native/{service_id}/{sha256}` (skipped when that key already exists, and
  cleaned up by the existing orphan cleanup on a failed deploy). Activation
  records `artifact_s3_key`, `artifact_hash`, `artifact_size` in the same
  transaction that activates the version and replaces the extension rows, so a
  version is never active without its binary.
- The pipeline otherwise activates like `Web` (no schema, module, db roles or
  eventing). `after_release` refreshes the tenant catalog and publishes
  `deploy.ext.<tenant>.<service>`; the deploy answer echoes `native_hash`
  (as `wasm_hash_echo` does) and the CLI verifies it.
- Rollback copies the target deployment's artifact columns to the new
  deployment row (the object is immutable, so it still exists), restores its
  `provides` rows and publishes `deploy.ext`.
- Service delete and tenant delete publish `undeploy.ext.<tenant>.<service>`
  for native services and delete every `native/{service_id}/` object of
  every service deleted (a failed first deploy can leave one behind a
  service with no version).
- Every web path that reads the artifact columns filters on
  `d.manifest->>'mode' = 'web'`: the web-runner warmer query,
  `delete_service`'s `web_artifact_keys`,
  `repo::services::list_web_artifacts_for_tenant`, the web status endpoint and
  any other query the implementer finds on `artifact_s3_key`. A regression test
  shows a native service is invisible to each of them.

## Guest SDK (`cufflink-fn`, feature `ext-call`)

```rust
let token = ext::call("google.token", &json!({ "scope": "analytics.readonly" }))?;
let token = ext::call_with_timeout("pdf.render", &doc, 5_000)?;
```

- `ext::call(name, &Value) -> Result<Value, ExtError>`;
  `ExtError { NotDeclared, NotFound, Denied, Unavailable(String), Timeout, TooLarge, Failed { code, message }, Invalid(String) }`
  implementing `Display` + `Error`.
- wasm32: local `extern "C" { fn ext_call(ptr: i32, len: i32) -> i32; }`
  through `envelope::call` (add `ext-call` to the `envelope` cfg list).
  Native: `crate::host::ext()` under `test-host`, else `Unavailable`.
- `declare_features!` marker `"ext-call" => EXT_CALL`.
- Test host: `HostSet.ext`, trait `ExtHost`, fake `MockExtensions` on
  `TestHost` as `host.ext`: `expect(name).respond(value)` /
  `.fail(code, message)` / `.refuse(kind)`, `.times(n)`, `calls()`; an
  unscripted call answers `not_found` and is recorded; `host.verify()` lists
  unmet expectations. `host.request()` builders unchanged; the handler's
  declared extensions are enforced when the test host is built with
  `.extensions(["google.token"])`, answering `not_declared` otherwise.

## CLI

- `cufflink deploy` in a project whose emitted manifest is `mode: native`:
  1. negotiate `native_services` with the platform;
  2. build a static `x86_64-unknown-linux-musl` release binary of the package's
     binary: `cargo zigbuild` when present, else native `cargo build` when the
     host is Linux x86_64 (which needs `musl-tools` for crates with C code),
     both with the target installed, else fail naming what is missing
     (`rustup target add x86_64-unknown-linux-musl`, `cargo install cargo-zigbuild`,
     `musl-tools`);
  3. POST the manifest with `native_base64` in one deploy request,
     authenticated with `CUFFLINK_PLATFORM_API_KEY` and `X-Tenant-Slug`
     (the environment's tenant), refusing early with the reason when that key
     is not set;
  4. verify the echoed `native_hash`.
- `cufflink ext list|register|unregister --tenant <slug>` using `admin_request`
  (`register <name> --caller <svc>...`, `--force` on narrowing/unregister).
- `cufflink services delete|enable|disable` act on a native service (its
  mode read from the tenant's listing) with `CUFFLINK_PLATFORM_API_KEY` and
  `X-Tenant-Slug`; `services delete --force` sends `force=true`.
- `cufflink dev`: an `ext-runner` profile, `--with-ext-runner`,
  `--ext-runner-image`, compose entries in `dev-compose.yml` and
  `dev-compose.source.yml`.

## Packaging

`Dockerfile.ext-runner` (source build) and `Dockerfile.ext-runner.ci`
(`ubuntu:24.04`, `ca-certificates`, `tini`, `/apps` and `/tmp/cufflink-ext`
owned by 10001, `USER 10001:10001`). New workspace dirs (`native`,
`supervisor`, `ext-runner`, `examples/ext-echo`) are added to the COPY lists of
every source Dockerfile. CI `build` builds and uploads `cufflink-ext-runner`;
`push_ext_runner` pushes `kingsleyh/cufflink-ext-runner`. `docker-compose.yml`
gains an `ext-runner` service.

## Tests

- **types:** name validation table, subject, envelope and reply round trips,
  manifest fields absent until set (hash unchanged), present → hash changes,
  round-trips and demands its feature.
- **sdk-macros / sdk:** `extensions` keyword expansion, duplicate keyword
  error, `mode: native` error (trybuild `.stderr`), golden updated only where a
  new golden fixture uses the keyword.
- **platform unit:** `decide` table (every outcome), admission rules, validator
  rules for native manifests, reply mapping, config parsing, WAT-harness
  envelope test (malformed request → `invalid`, never a stale response).
- **platform integration** (`platform/tests/ext_call.rs`, testkit, SDK guest
  `ext-call`): operator registers a NATS provider; an in-test provider built
  with `cufflink-native`'s `run_nats` answers; every outcome end to end
  (ok, not_declared, not_found, denied, unavailable, timeout, too_large,
  failed); deploy refused for an unregistered extension; unregister refused
  while declared, allowed with force; `CUFFLINK_EXTENSIONS=false` refusals;
  tenant deployer refused a native deploy; metrics counted.
- **platform NATS safety:** no platform or tenant stream covers
  `cufflink.ext.>`, and no subscription or claim can bind it (unit tests over
  the stream configs and `validate_claims` / `validate_tenant_scope`).
- **web isolation:** a deployed native service is invisible to every web path
  listed above.
- **native deploy:** code-only change makes a new version; identical redeploy
  is skipped; the object key is content-addressed and survives rollback;
  `native_base64` refused on other modes and required on native; size cap.
- **worker:** an `ext_call` from a job reaches the provider.
- **cufflink-native:** dispatch (ok, unknown, error, panic, deadline),
  `--emit-manifest` output, Unix-socket server ready/call/drain.
- **supervisor:** the moved web-runner tests.
- **ext-runner:** end to end with the `examples/ext-echo` binary built for the
  host: platform (testkit) with local-dir storage shared with the runner,
  native deploy + artifact upload, the runner starts it, a guest's `ext_call`
  is answered through it; redeploy switches versions with no failed call;
  crash restart; crash loop; bad readiness (wrong extensions) refused;
  undeploy stops and unsubscribes; a missed event is converged by the timer
  reconcile. Tests set `EXT_SOCKET_DIR` to a short directory (macOS's
  `$TMPDIR` alone nearly exhausts the socket path limit).
- **cli:** musl build command selection and missing-toolchain error, manifest
  mode detection, `ext` command request shapes.

## Documentation

`docs/extensions.md` (user guide), rows in `docs/versions.md` (feature table
under the next release heading and the SDK cargo feature table),
`docs/observability.md` (metrics), `docs/deployment.md` (ext-runner and
platform settings), `docs/building-on-cufflink.md` (host function reference and
mode table), `docs/testing.md` (`host.ext`).

## Not in this change

- Signed native binaries.
- `aarch64` native builds.
- Tenant database or other platform services exposed to native code.
- Streamed extension replies.
- Server-side compilation of native services.
- Per-service cloud identity for native children (they share the runner pod's).
