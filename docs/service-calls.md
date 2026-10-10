# Service Calls and the Namespaced Cache

## Calling another service

A guest can call a route of another service of the **same tenant**:

```toml
cufflink-fn = { version = "…", features = ["service-call"] }
```

```rust
let response = http::call_service("pricing", "POST", "/_fn/quote", &[], Some(br#"{"sku":"a"}"#))?;
if response.is_success() {
    let quote: serde_json::Value = response.json()?;
}
```

- `path` is the target's route path with its parameters filled in (and an
  optional `?query`), or `/_fn/{handler}`. A body without a `content-type`
  header is sent as JSON.
- The call carries a platform-minted **service identity** naming the calling
  service and pinning the exact call; the target verifies it and never sees
  the token itself. Routes can then trust the caller without a shared secret.
- Calls nest at most 4 deep (`CallError::DepthExceeded`), are bounded by the
  caller's remaining budget (at most 30 s, counted in whole seconds, so a
  caller with less than a second left gets `Timeout` without calling), and
  read at most the egress response limit. Other failures: `Timeout`,
  `UnknownService`, `Transport`, `Denied`.
- The call's own credentials are the identity alone: `authorization`,
  `cookie`, `x-api-key`, the forwarded client address headers
  (`x-forwarded-for`, `x-real-ip`, `forwarded`, `cf-connecting-ip`),
  `host` and connection headers a guest sets are dropped.
- On the platform the call runs in-process through the same routing, postures
  and error handling as an external request. On a worker it goes to
  `PLATFORM_INTERNAL_URL`.

Configuration ([deployment](deployment.md#service-calls-and-identities)):
`CUFFLINK_SERVICE_IDENTITY_KEYS`, identical on every platform and worker
replica, and `PLATFORM_INTERNAL_URL` on workers.

## Internal routes

A route only the tenant's own services may call:

```rust
("POST", "/reprice", "reprice", internal("pricing-service", "orders-service")),
("POST", "/sweep", "sweep", internal("*")),
```

It admits only a call carrying a service identity verified for exactly this
tenant, service, method and path, from a listed service. A missing, forged,
expired, replayed or too-deep identity answers 401, an unlisted caller 403,
each counted in `cufflink_service_identity_rejected_total{reason}`. Bearer
tokens and API keys are never consulted. The guest sees who called:

```rust
if let Some(caller) = req.caller() {
    log::info(&format!("called by {} for {:?}", caller.service, caller.on_behalf_of));
}
```

`caller.depth` is how deep the chain already is; the guest's own service
calls continue from it. A service call to a route of any other posture, and
to a table's CRUD, batch, GraphQL and stream routes, carries its verified
`caller` too, so depth and budget follow a chain through public and
permission routes alike: the callee, and every lifecycle hook the call runs
(inline, on a worker, or after the response), runs within its caller's
remaining time, and a hook's own service calls continue the depth. A call
whose budget is already spent is refused with 504 before anything runs; an
invalid identity there is ignored (and counted). No guest ever
sees the identity header, and a client cannot set `caller`. Internal routes
are left out of the OpenAPI document.

## The namespaced cache

```toml
cufflink-fn = { version = "…", features = ["cache"] }
```

```rust
use std::time::Duration;

if cache::set_nx("lock:invoice:42", "1", Duration::from_secs(30))? {
    // this invocation holds the lock
}
let hits = cache::incr("hits:today", 1, Duration::from_secs(86_400))?;
let profile: Option<Profile> = cache::get_json("profile:7")?;
```

Keys are private to the tenant and service (the same key in two services or
two tenants never collides, and raw `redis_*` host calls cannot read them).
Keys are 1–512 printable ASCII bytes without spaces, values at most 1 MiB,
and every write needs a TTL between 1 second and 30 days. `incr` sets the
TTL only when it creates the key. Without Redis every call is
`Err(CacheError::Unavailable)`, never a silent success.
