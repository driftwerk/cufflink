# Extensions

An extension is a capability a project adds to Cufflink without a Cufflink
release: a WASM service calls it by name with `ext_call`, and the tenant's
registration decides what answers. Two kinds of provider serve one:

- a **native service**: a Rust binary built with `cufflink-native`, deployed
  with `cufflink deploy` and run by the extension runner
  (`cufflink-ext-runner`);
- a **NATS provider**: a process the project runs itself, built with the
  same crate, that answers the tenant's calls straight from NATS.

The guest never learns which. Both answer on the NATS subject
`cufflink.ext.<tenant>.<name>` in the queue group `cufflink-ext`, so the
calls spread over every replica that serves the extension.

```
WASM guest ── ext_call ──▶ platform or worker
                            1. the manifest declares the name     → else NotDeclared
                            2. the tenant registers it            → else NotFound
                            3. the registration lists the caller  → else Denied
                            4. NATS request cufflink.ext.<tenant>.<name>
                                      │
               ┌──────────────────────┴───────────────────────┐
       cufflink-ext-runner                              NATS provider
       relays over the child's Unix socket              (Service::run_nats)
               │
       native service child (Service::run)
```

## When to use one

| Need | Use |
|------|-----|
| Tables, routes, events, jobs, and logic that fits the WASM sandbox and the host functions it has | a WASM service ([WASM guide](wasm-guide.md)) |
| A capability every tenant should have, maintained with the platform | a host function in Cufflink itself ([building on Cufflink](building-on-cufflink.md#host-function-reference)) |
| Native code: a library with no WASM build, a cloud SDK, a credential that must stay out of guests, CPU-heavy work, a long-lived connection | an extension |
| The same, where the code must run on infrastructure the project owns, or in another language speaking the protocol | an extension served by a NATS provider |

An extension has no tables, routes, subscriptions or schedules of its own:
it answers calls. Its code runs outside the WASM sandbox, on its tenant's
own extension runner (see [security](#security)), so extensions are an
operator's decision, off until the platform is started with
`CUFFLINK_EXTENSIONS=true`.

## Turning extensions on

1. Set `CUFFLINK_EXTENSIONS=true` on every platform replica. Registering
   NATS providers, and deploying and rolling back native services, answer
   403 naming the setting until then; guests' calls, listings, removing a
   NATS provider and stopping, starting or removing a native service work
   either way, so turning the setting off never strands what was deployed
   ([settings](deployment.md#extensions)).
2. For native services, run an extension runner for each tenant that
   deploys them, beside the platform: `EXT_TENANT` naming the tenant,
   `PLATFORM_URL`, the tenant's runner key (`cufflink tenants runner-keys
   create <tenant> --for native`, as a platform admin) and a NATS user of
   that tenant's alone; it holds no database, secrets or storage
   credentials. Any of the tenant's deployers can deploy the native code it
   runs, so give it a network that reaches the platform and NATS alone: no
   route to Postgres, Redis or object storage, and NATS only with the
   runner's own user
   ([extension runner](deployment.md#extension-runner)). The repository's
   `docker-compose.yml` runs one (`kingsleyh/cufflink-ext-runner`) under
   the profile `ext-runner`, on a network with the platform and NATS alone:
   add `CUFFLINK_EXTENSIONS: "true"` to the `platform` service's
   environment, `docker compose up -d platform`, mint the tenant's runner
   key, then `EXT_TENANT=<tenant> CUFFLINK_RUNNER_KEY=<key> docker compose
   --profile ext-runner up -d ext-runner`. `cufflink dev up
   --with-ext-runner` does all of it for the development stack's tenant
   ([`cufflink dev`](dev.md#native-services-and-extensions)).
3. Grant NATS permissions: only the platform and the workers publish on
   `cufflink.ext.>`; each runner and each provider subscribes to its own
   tenant's subjects alone ([extension subjects](deployment.md#extensions),
   [a runner's permissions](deployment.md#extension-runner)).

## Calling an extension from a WASM service

Enable the `ext-call` feature of `cufflink-fn` (from v0.29.0 on; the module then
requires the platform feature `ext_call`), and list every extension the
service calls in its manifest:

```toml
[dependencies]
cufflink-fn = { version = "<version>", features = ["ext-call"] }
```

```rust
cufflink::service! {
    name: "reports-service",
    mode: wasm,
    tables: [],
    custom_routes: [("POST", "/greet", "greet_handler", auth)],
    extensions: ["greeting.say"],
}
```

```rust
use cufflink_fn::ext::{self, ExtError};
use cufflink_fn::prelude::*;

cufflink_fn::init!();

pub fn greet(req: Request) -> Response {
    let payload = json!({ "name": req.body()["name"] });
    match ext::call_with_timeout("greeting.say", &payload, 2_000) {
        Ok(reply) => Response::json(&reply),
        Err(ExtError::Failed { code, message }) => {
            Response::error(&format!("{code}: {message}")).with_status(502)
        }
        Err(error @ (ExtError::Timeout | ExtError::Unavailable(_))) => {
            Response::error(&error.to_string()).with_status(503)
        }
        Err(error) => Response::error(&error.to_string()).with_status(500),
    }
}

cufflink_fn::handler!(greet_handler, greet);
```

`ext::call(name, &payload)` waits as long as the platform's default,
`EXT_CALL_TIMEOUT_SECS` (30 s); `ext::call_with_timeout(name, &payload, ms)`
waits at most `ms`. Neither waits past what is left of the handler's own
budget. Calls work the same from the platform and from the worker (jobs,
subscriptions, handlers routed to workers).

An extension name is 1 to 128 bytes of dot-separated segments of
`[a-z0-9-]` (`google.token`, `pdf.render-v2`). A deploy, or a rollback,
that declares an extension the tenant does not register, or whose
registration does not list the service (or `*`), is refused with 422, so
a service never runs calling what it may not call.

### Errors

| `ExtError` | Why |
|------------|-----|
| `NotDeclared` | the manifest does not list the extension |
| `NotFound` | the tenant registers no extension of the name |
| `Denied` | the registration does not list the service |
| `TooLarge` | the call or the reply is over `EXT_CALL_MAX_BYTES` (1 MiB), or over what NATS carries |
| `Timeout` | no reply in time, or the provider gave up at the call's deadline |
| `Unavailable(message)` | nothing serves the extension now (no provider subscribed within about a second, the call being sent again meanwhile; the runner cannot reach the child; or the child is stopping), or it answered something that is not a reply |
| `Failed { code, message }` | the provider answered with an error: the extension's own `ExtError`, or one the SDK answers for it (below) |
| `Invalid(message)` | the call itself is malformed |

The codes `unavailable`, `timeout` and `too_large` are reserved: the
runner and `cufflink-native` answer them for the transport, and the guest
sees them as `Unavailable`, `Timeout` and `TooLarge`, never as `Failed`,
whoever answers them. An extension names a code of its own with any other.
Codes a `Failed` carries besides an extension's own: `failed` (any error
converted with `?`), `panic` (the extension panicked), `not_found` (the
binary does not serve the name), `unsupported_protocol` and `invalid`. The
order in which a call is checked is in
[building on Cufflink](building-on-cufflink.md#ext--extensions).

### Testing

With `test-host` ([testing](testing.md)), the test host scripts the
tenant's extensions as `host.ext`, and `.extensions([...])` makes it refuse
what the manifest does not declare, as the platform does:

```toml
[dev-dependencies]
cufflink-fn = { version = "<version>", features = ["ext-call", "test-host"] }
```

```rust
use cufflink_fn::ext::CallErrorKind;
use cufflink_fn::testing::TestHost;
use serde_json::json;

#[test]
fn the_greeting_is_the_extensions() -> Result<(), Box<dyn std::error::Error>> {
    let host = TestHost::builder()
        .tenant("harbor")
        .service("reports-service")
        .extensions(["greeting.say"])
        .build()?;
    host.ext
        .expect("greeting.say")
        .respond(json!({ "greeting": "Hello, Ada" }))
        .times(1);

    let response = host.call(greet, host.request().json(&json!({ "name": "Ada" })).build()?);

    assert_eq!(response.status(), 200);
    assert_eq!(host.ext.calls()[0].payload, json!({ "name": "Ada" }));
    assert_eq!(host.verify(), Ok(()));
    Ok(())
}

#[test]
fn nothing_serving_the_extension_is_a_503() -> Result<(), Box<dyn std::error::Error>> {
    let host = TestHost::builder().service("reports-service").build()?;
    host.ext.expect("greeting.say").refuse(CallErrorKind::Unavailable);
    let response = host.call(greet, host.request().json(&json!({ "name": "Ada" })).build()?);
    assert_eq!(response.status(), 503);
    Ok(())
}
```

`.fail(code, message)` answers an extension's error as the platform
does: `Failed` with its code, or the kind a reserved code names. An unscripted
call answers `not_found` and is recorded; `host.verify()` lists what was
expected and never called, and what was called and never scripted.

## Writing a provider with `cufflink-native`

Both kinds of provider are built with `cufflink-native` (from v0.29.0 on). A
service names itself and each extension it provides with the services
allowed to call it, service names or `*` for every service of the tenant:

```toml
[package]
name = "ext-greeting"
version = "0.1.0"
edition = "2021"

[dependencies]
cufflink-native = "0.4"
```

```rust
use cufflink_native::prelude::*;

struct Greeting {
    salutation: String,
}

impl Extension for Greeting {
    fn call(&self, ctx: &CallContext, payload: Value) -> Result<Value> {
        let name = payload["name"].as_str().ok_or("name is required")?;
        if name.len() > 64 {
            return Err(ExtError::new("name_too_long", "at most 64 bytes"));
        }
        Ok(json!({
            "greeting": format!("{}, {name}", self.salutation),
            "for": ctx.service.name,
        }))
    }
}

fn main() -> std::process::ExitCode {
    let salutation = std::env::var("SALUTATION").unwrap_or_else(|_| "Hello".into());
    cufflink_native::service("ext-greeting")
        .extension("greeting.say", Greeting { salutation }, ["reports-service"])
        .run()
}
```

- `Extension::call` is synchronous and runs on the blocking pool, so it may
  block; a function or closure `Fn(&CallContext, Value) -> Result<Value>`
  is an extension too (`examples/ext-echo` provides functions).
- `CallContext` holds the calling `tenant`, the `service` (`name` and
  `version`), `auth` (the verified claims of the caller's principal as the
  handler saw them, never a token) and `deadline_ms`; `ctx.deadline()`
  (`CallDeadline`, in the prelude) gives the deadline as an `Instant`. A
  call still running at its deadline is answered `timeout`, and its caller
  has stopped waiting: stop work that no longer has a use.
- `?` turns any error into `ExtError` with the code `failed`;
  `ExtError::new(code, message)` names a code the caller can tell apart
  (not one of the [reserved codes](#errors)).
- `.concurrency(n)` bounds the calls running at once (64 unless set); the
  others wait for a slot until their deadline.
- `service(...)` checks the service name, the extension names and the
  callers when it runs, and exits non-zero saying what is wrong.

### As a NATS provider

`run_nats` serves the extensions from NATS for one tenant, subscribing each
extension's subject in the queue group `cufflink-ext`, so replicas of a
provider share the calls. It stops on SIGTERM or SIGINT: it unsubscribes,
answers the calls already delivered to it and writes its last replies (a
call routed to it while it unsubscribes is lost; see [limits](#limits)).
On Linux it first makes its process undumpable, so other processes of its
user cannot read its environment or memory.

```rust
use cufflink_native::prelude::*;
use cufflink_native::NatsProvider;

fn say(ctx: &CallContext, payload: Value) -> Result<Value> {
    let name = payload["name"].as_str().ok_or("name is required")?;
    Ok(json!({ "greeting": format!("Hello, {name}"), "for": ctx.service.name }))
}

fn main() -> eyre::Result<()> {
    cufflink_native::service("greeting-provider")
        .extension("greeting.say", say, ["reports-service"])
        .run_nats(NatsProvider::from_env()?)
}
```

(`eyre = "0.6"` in its dependencies.) `NatsProvider::from_env` reads:

| Variable | Meaning |
|----------|---------|
| `CUFFLINK_TENANT` | required: the tenant whose calls it answers |
| `NATS_URL` | the platform's NATS, a comma-separated list (`nats://localhost:4222` when unset) |
| `NATS_CREDS_FILE` / `NATS_NKEY_SEED` / `NATS_USER` + `NATS_PASSWORD` | its NATS credentials, at most one method; anonymous when unset |

It logs to stderr, filtered by `RUST_LOG` (`info` when unset).

`NatsProvider::new(tenant, url, auth)` builds one in code. Give the
provider's NATS user permission to subscribe to `cufflink.ext.<tenant>.>`
alone (in the queue group `cufflink-ext`) and to publish replies
(`_INBOX.>`, or `allow_responses`), never to publish on `cufflink.ext.>`:
whoever subscribes there answers the tenant's calls.

A NATS provider's extensions are served only once a platform admin
registers them for the tenant, with the services that may call each (the
callers given to `.extension` are what a native deploy registers, and are
not read here):

```bash
export CUFFLINK_PLATFORM_API_KEY=<platform admin key>
cufflink ext register greeting.say --tenant harbor --caller reports-service
cufflink ext list --tenant harbor
cufflink ext register greeting.say --tenant harbor --caller '*'          # other callers
cufflink ext unregister greeting.say --tenant harbor
```

Narrowing the callers so that a deployed service would lose an extension
it declares, or removing a registration a deployed service declares, is
refused with 409; `--force` does it anyway, and that service's calls then
answer `Denied` or `NotFound`. A deploy that calls an extension an
operator removes or narrows while it runs is refused (422) rather than
activated. Registrations are removed on a platform with
`CUFFLINK_EXTENSIONS=false` too; only registering needs it. A name the
tenant's native service provides cannot be registered as a NATS provider
(409). The HTTP API is
`GET`/`PUT`/`DELETE /api/tenants/{slug}/extensions[/{name}]`
([API reference](setup.md#api-reference)).

### As a native service

`run` makes the binary a native service: `--emit-manifest` prints its
`mode: native` manifest (its name and what it `provides`, with each
extension's callers), which is how `cufflink deploy` reads it, and
otherwise it makes its process undumpable (on Linux) and serves the Unix
socket the runner names in `CUFFLINK_EXT_SOCKET` until SIGTERM, then drains
the calls in flight. It answers only requests bearing the token the runner
gives it in `CUFFLINK_EXT_TOKEN` (401 otherwise), and refuses to start
without one.

A native project is a Cargo package with one binary target (or a
`default-run` among several) and a `Cufflink.toml` naming its environments,
as any service's. It is deployed with the environment's own credentials
(the tenant's deploy key or deploy role), as any service is:

```bash
rustup target add x86_64-unknown-linux-musl
cargo install cargo-zigbuild            # needs zig; or, on Linux x86_64, apt install musl-tools
cufflink --env production deploy
```

`cufflink deploy` sees `mode: native`, checks that the platform supports
`native_services`, builds a static `x86_64-unknown-linux-musl` release
binary (`cargo zigbuild` wherever it is installed; otherwise `cargo build`
on Linux x86_64, which needs `musl-tools` (`musl-gcc`) for crates with C
code such as `ring`; it says what is missing), and sends the
manifest and the binary in one request. The platform stores the binary by its SHA-256
and the CLI checks the hash the platform echoes. A deploy whose manifest
and binary are both unchanged is skipped; a new binary alone makes a new
version. Activating a version registers its `provides` for the tenant in
the same transaction, so a name another native service or a NATS provider
holds is refused, and publishes the change to the tenant's runners.

`cufflink rollback` returns to an earlier version's
binary and extensions. Deleting the service (`cufflink services delete`, or
`DELETE /api/services/{id}`) removes its registrations,
stops its children and deletes its binaries; it is refused with 409 while a
deployed service calls one of its extensions, unless `--force`
(`?force=true`), and audited. Setting it `inactive` (`cufflink services
disable`, or `PATCH /api/services/{id}/status`) stops its children and
keeps its registrations, so its callers see `Unavailable` until it is
`active` again (`cufflink services enable`). Deleting and switching work
with `CUFFLINK_EXTENSIONS=false` too.

**What the runner does** ([every detail](deployment.md#extension-runner)):

- Every replica of the tenant's runner runs one child of each of the
  tenant's active native services' current versions, and converges on what should run at start,
  every `EXT_RECONCILE_SECS` (30 s) and at once on each deploy, rollback or
  removal.
- A child starts with an allowlisted environment (`PATH`, `HOME`, locale,
  `TZ`, TLS and OTEL settings, `EXT_ENV_PASSTHROUGH`), then its service's
  configs and secrets as variables (`cufflink config set KEY VALUE
  [--secret]` in the project; read at each start of the child, so a change
  applies at its next deploy or restart), then `CUFFLINK_EXT_SOCKET`,
  `CUFFLINK_EXT_TOKEN`, `CUFFLINK_TENANT` and `CUFFLINK_SERVICE`. Neither
  the runner's key nor its NATS credentials is passed to it, but it shares
  the runner's cloud identity ([security](#security)).
- **Ready:** a new child must answer `GET /ready` within
  `EXT_READY_TIMEOUT_SECS` (30 s), serving exactly the extensions its
  manifest provides. Only then does the runner join the queue group for
  them; on a redeploy the old child keeps serving until the new one is
  ready, then drains, so a deploy fails no call. A binary that serves
  other extensions than its manifest provides is refused and the old
  version keeps serving.
- **Crashes and hangs:** a child that exits is started again after 1 s,
  doubling to 30 s; three failures within 10 s of starting leave the
  service stopped (`crash_looping`) until its next deploy. A child that
  fails three health probes in a row (every `EXT_HEALTH_INTERVAL_SECS`) is
  killed and started again.

## Security

- **One runner per tenant.** Every extension runner serves exactly one
  tenant (`EXT_TENANT`) and holds only what reaches that tenant: its
  runner key, minted for the extension runner (`--for native`), which the
  runner API accepts for that tenant's native services, binaries and
  environments alone, while its web routes and every other route refuse
  it; its NATS user, allowed that tenant's subjects alone; and its cloud
  identity, granting only what that tenant's native services need. It
  holds no database, secrets or storage credentials. Give each its own
  ([extension runner](deployment.md#extension-runner)). A runner whose key
  the platform refuses (revoked, or its tenant deleted), or answers for
  another tenant, stops every child and leaves their call subjects until a
  key it accepts is in place, so a new tenant that takes the slug is never
  answered by the old one's services; rotate a key by creating the new one
  first and revoking the old one once the runner uses the new.
- **The runner's network.** A native service connects wherever its
  runner's network lets it, and any of the tenant's deployers can deploy
  one, so the network is what keeps it from the platform's own stores: the
  runner reaches `PLATFORM_URL` and NATS alone, with no route to Postgres,
  Redis or object storage, and NATS only with its own user
  ([a network policy](deployment.md#extension-runner)).
- **What a native service can reach.** Its code is not sandboxed: it runs
  as the runner's user, in the runner's container, with the runner's
  network and cloud identity, shared by the tenant's other native services
  on that runner. So it can reach its own tenant's other native services
  (by design: they are the tenant's own), and nothing of another tenant's.
  On Linux the runner and every `cufflink-native` process make themselves
  undumpable, so a child cannot read the runner's environment (its runner
  key and NATS credentials) through `/proc`, and each child requires its
  own token, so a sibling cannot call it as the runner; a child becomes
  undumpable only once its `main` makes the call, so a sibling can read
  its environment in the moment after it starts. A child that is not built
  with `cufflink-native` must make itself undumpable too.
- **Who deploys.** A native service is deployed, rolled back, stopped,
  started and removed with the tenant's ordinary deploy authority, as its
  WASM and web services are; deploying and rolling one back also needs a
  platform with `CUFFLINK_EXTENSIONS=true`. Registering a NATS provider and
  minting runner keys are a platform admin's.
- **Runner keys.** A platform admin mints, lists and revokes them
  (`cufflink tenants runner-keys`), each change audited; a key is minted
  for one kind of runner (`--for native` or `--for web`), shown once and
  stored as its hash. Rotate one by minting its successor, pointing the
  runner at it (a key file is written beside the old one and renamed over
  it) and revoking the old key.
- **Callers.** The platform refuses a call from a service the registration
  does not list before anything reaches the provider, and tells the
  provider who calls (`ctx.tenant`, `ctx.service`, `ctx.auth`). Decide on
  those, never on what the payload claims.
- **NATS.** Whoever can subscribe to `cufflink.ext.<tenant>.>` answers that
  tenant's calls, and whoever can publish there speaks for the platform:
  grant the permissions above and nothing wider.

## Observability

- The platform and the worker count calls in
  `cufflink_ext_calls_total{caller, extension, result}` and time them in
  `cufflink_ext_call_duration_seconds`; the runner exports
  `cufflink_ext_runner_children{state}`,
  `cufflink_ext_runner_restarts_total{reason}`,
  `cufflink_ext_runner_calls_total{result}` and
  `cufflink_ext_runner_call_duration_seconds`
  ([metrics](observability.md)).
- `GET /_cufflink/status` on the runner (with `EXT_STATUS_TOKEN`) lists
  each child's tenant, service, version, binary hash and state;
  `/health` and `/_cufflink/ready` are its probes.
- A child's stdout and stderr are logged at `debug` under the target
  `ext_child` (`RUST_LOG=ext_child=debug`); `cufflink-native` logs to
  stderr with `RUST_LOG`.

## Limits

- **At most once.** A call is one NATS request: when the reply is lost or
  late the caller sees `Timeout` or `Unavailable` although the extension
  may have done its work, and nothing retries it. An extension that changes
  state must be idempotent: have the caller send a key that identifies the
  change (the row it concerns, or an id it stored before calling) and
  apply each key once.
- **Unsubscribing can drop calls.** Unsubscribing is not lossless in
  the NATS client: it forgets a subscription as soon as it has sent the
  unsubscribe, before the server takes it in. So a runner or a NATS
  provider that stops, a native service undeployed or switched off, a
  version that provides fewer extensions, or a runner or provider
  restarting after a crash can drop calls NATS had already routed to it,
  and their callers time out. Replacing a native service's child loses
  none: the runner keeps its subscriptions and only sends the calls that
  follow to the new child.
- **Size.** A call and a reply carry at most `EXT_CALL_MAX_BYTES` (1 MiB),
  never more than the NATS server's `max_payload`; replies are not
  streamed.
- **Time.** `EXT_CALL_TIMEOUT_SECS` (30 s) unless the guest sets its own
  `timeout_ms`, which may be longer or shorter, never past the handler's
  budget.
- **Binaries.** Native services are built for `x86_64` Linux only, at most
  `NATIVE_ARTIFACT_MAX_BYTES` (128 MiB) and signed by nothing but their
  hash.
- **Concurrency.** Each child runs at most its `.concurrency(n)` calls at
  once, and the runner relays at most `EXT_MAX_CONCURRENT_CALLS` (64) to
  one child. The calls beyond that which NATS delivered to a runner wait in
  that runner's client buffer for a slot, and are not handed to another
  replica; one whose deadline passes before it gets a slot is not
  answered, and past the buffer (65536 calls per extension) the client
  drops calls as a slow consumer's and their callers time out.
