# Web runner hosting: hostname aliases, CORS, restarts and forwarded headers

Proposed. Covers issues #60, #65, #68 and #69. Section 14 holds the
questions for the maintainer, each with a proposed default.

## Outcome

- A web app can be reached at exact hostnames of any domain
  (`shop.example.com`), as well as its generated name. One of those
  hostnames can be its public URL (`CUFFLINK_PUBLIC_URL`). A hostname is
  claimed by one service on the whole platform. A tenant can claim only
  names under domains a platform admin has granted it. No other tenant can
  route, capture or shadow that name, including after the service is gone.
- A web app owns its CORS. The runner answers preflights and sets CORS
  headers only for an app that asks it to, either permissively or for listed
  origins. The default changes over two releases, with a warning in between
  and a setting that keeps today's behaviour, so nothing changes silently.
- A config change can restart a web app without redeploying it:
  `cufflink web restart`, and the CLI's config commands request it by
  default. A deploy picks up the app's current configs even when its bundle
  has not changed. A failed restart leaves the running process as it was.
- An app behind the runner learns the original client address, host and
  scheme from `X-Forwarded-For`, `X-Forwarded-Host` and
  `X-Forwarded-Proto`. It can trust them because the runner sets them
  itself and accepts incoming values only from proxies the operator lists.
- The Node version the image runs is the one the documentation states.
  Both Dockerfiles name the same version, and CI fails when they drift.
- An app that declares nothing new, on an install that sets nothing new,
  behaves as it does today until the second release flips the CORS and flat
  hostname defaults, as section 5.3 lays out.

## 1. What the runner does today

Read from `web-runner/src`, `platform/src`, `cli/src`, `types/src`, the
Dockerfiles and `docs/` on `main` at `4a2d341` (after v0.37.1).

### 1.1 Routing and the public URL

- Every request that is not an ops route goes to
  `proxy::host_proxy_handler` (`web-runner/src/main.rs:133-139`). The
  handler reads the `Host` header only, and drops its port by splitting on
  the first `:` (`web-runner/src/proxy.rs:46-51`). An HTTP/2 request with
  no `Host` header, which carries only `:authority`, therefore resolves to
  the empty host and gets a 404. An IPv6 literal is cut at its first colon.
- `parse_host` (`web-runner/src/host.rs:37-46`) maps a host to
  `(tenant, service)` in one of two formats:
  - **Flat**, the default (`host.rs:68-78`), reads only the first label and
    **never checks the base domain**. `{service}--{tenant}.anything` routes
    to that tenant, and `{service}.anything` routes to tenant `default`.
    The test `flat_bare_service_host_resolves_to_default_tenant`
    (`host.rs:114-128`) pins this: it resolves `portal.prod.example.com`
    and `backoffice.localhost` against the base `staging.example.com`. Flat
    parsing also does not lowercase the host.
  - **Hierarchical** (`host.rs:80-96`) requires the base-domain suffix,
    compares case-insensitively and accepts one or two labels under it.
- So only generated names route. A name of another domain either 404s or,
  in flat mode, routes by its first label to a `default`-tenant service
  that happens to share it.
- The public URL is composed from the same rules
  (`compose_public_url`, `host.rs:48-66`) when the process starts
  (`web-runner/src/process_manager.rs:190-196`). It is passed to the app as
  `CUFFLINK_PUBLIC_URL` (`web-runner/src/child_env.rs:36-47`).
- After resolving, the handler refuses tenants the runner does not serve
  (`proxy.rs:65-71`), warms the app on first use (`proxy.rs:73-102`) and
  forwards the request to `127.0.0.1:{port}`.
- An operator who wants an app on its own domain today puts a reverse
  proxy in front. The proxy rewrites `Host` to the generated name and
  rewrites the app's redirects back (`proxy_redirect`). The app then sees
  the generated name, not the one the browser used.

### 1.2 TLS

The runner terminates no TLS. It binds a plain `TcpListener` on
`0.0.0.0:{RUNNER_PORT}` and serves it with `axum::serve`
(`main.rs:143-149`). The documentation puts a wildcard DNS record and a
wildcard certificate in front of the runner replicas (`docs/scaling.md:155`).
`CUFFLINK_PUBLIC_SCHEME` (default `https`) only states what the edge serves.

### 1.3 CORS

- The whole router, ops routes included, is wrapped in
  `CorsLayer::permissive()` (`main.rs:140`). That allows any origin, any
  method and any request header, and exposes every response header
  (tower-http 0.6.11, `cors/mod.rs:134-140`). It sends no credentials
  header.
- The layer answers **every** `OPTIONS` request itself, with an empty body,
  whether or not it carries `Origin` or is a preflight
  (`cors/mod.rs:677-690`). An app never sees an `OPTIONS` request.
- On every other response the layer **overwrites** the app's headers of
  the same names. It calls `response_headers.extend(headers.drain())`,
  which replaces by name; only `Vary` is appended (`cors/mod.rs:755-764`).
  An app that sets `Access-Control-Allow-Origin: https://shop.example.com`
  and `Access-Control-Allow-Credentials: true` has the origin replaced with
  `*`. Browsers refuse `*` on a credentialed request, so a credentialed
  cross-origin call to a web app cannot work today, whatever the app does.
- The fallback routes only `GET` (and therefore `HEAD`), `POST`, `PUT`,
  `DELETE` and `PATCH` (`main.rs:133-139`). Removing the layer alone would
  give every `OPTIONS` request a 405 from the method router, so an app
  still could not answer its own preflights.
- The platform already has its own CORS precedent. `CORS_ALLOWED_ORIGINS`
  is validated by `allowed_origin` (`platform/src/app/layers.rs:111-136`),
  and `CUFFLINK_HTTP_PROFILE=hardened` turns the permissive default off
  (`layers.rs:96-109`, `docs/setup.md:378`).

### 1.4 Configs and restarts

- A web app's configs become its environment once, when its process
  starts (`docs/deployment.md:439-442`). `Apps::run` reads them through
  `Source::environment` (`web-runner/src/apps.rs:144-148`,
  `web-runner/src/source.rs:208-216`).
- `Apps::run` returns `AlreadyRunning` **before reading configs** when the
  deployment's recorded bundle hash matches the directory the app runs from
  (`apps.rs:139-143`). It returns `AlreadyRunning` again after installing
  (`apps.rs:153-155`). A deploy event for an unchanged bundle therefore
  never applies changed configs. The documentation states the bundle half
  of this: "a deploy of a bundle the app already runs changes nothing"
  (`docs/deployment.md:844`).
- `apply_config`, `set_config` and `delete_config`
  (`platform/src/api/config.rs:193-307`) write rows and an audit entry, and
  publish nothing. The only publisher of `deploy.web.*` is the artifact
  upload (`platform/src/api/web.rs:76`). Rollback publishes no web event;
  `rollback` in `platform/src/api/deployments.rs:871` and after has no
  publish call.
- In shared mode, a config read that fails is swallowed. `Shared::configs`
  logs it and returns an empty map (`source.rs:258-282`), so the app starts
  with no configs at all. Tenant-scoped mode returns the error
  (`source.rs:214`).
- Deploy events only name the app. The runner reads what to run from the
  database or the runner API, never from the event
  (`docs/deployment.md:852-856`, `source.rs:167-183`). The `deploy` and
  `undeploy` subject namespaces cannot be claimed by guests
  (`platform/src/nats_manager/subjects.rs:93`). A tenant-scoped runner's
  NATS user subscribes to `deploy.web.<tenant>.>` and
  `undeploy.web.<tenant>.>` only (`docs/deployment.md:915-921`).
- Events are handled one at a time per runner, in order
  (`main.rs:256-283`). A restart replaces a process with zero downtime: it
  starts the new process on a new port, waits until it is ready, then kills
  the old one (`process_manager.rs:239-261`). If the new process fails its
  health check, the old one keeps serving (`process_manager.rs:268-285`).

### 1.5 Forwarded headers

- The handler copies every incoming header except `x-tenant-slug` to the
  app, then sets `x-tenant-slug` (`proxy.rs:115-121`). It adds no
  `X-Forwarded-*` header. Whatever the client sent, including a forged
  `X-Forwarded-For`, `X-Forwarded-Host` or `Forwarded`, reaches the app
  unchanged.
- `Host` survives: the copied header is sent as is, and the hyper client
  sets `Host` only when it is missing.
- The server is started without connect info (`main.rs:147`), so the
  handler does not know the TCP peer's address. It has nothing to append to
  `X-Forwarded-For` and nothing to check trust against.
- The platform has a client-IP precedent: `RATE_LIMIT_CLIENT_IP_SOURCE`
  and `RATE_LIMIT_EXEMPT_IPS`, the latter parsed as `IpNet`s
  (`platform/src/middleware/rate_limit.rs:135-150`), with the peer read
  from `ConnectInfo<SocketAddr>` (`rate_limit.rs:349`).
- Frameworks rely on these headers. Next.js documents, under
  `serverActions.allowedOrigins`, that a Server Action is refused unless
  its `Origin` matches the request's host, taken from `X-Forwarded-Host`
  when present and otherwise from `Host`. It also builds absolute URLs from
  `X-Forwarded-Host` and `X-Forwarded-Proto`. An app reached through a
  proxy that rewrites `Host`, and given no `X-Forwarded-Host`, has its
  Server Actions refused.

### 1.6 The Node version

The version differs in three places, not two:

| Where | Node |
|---|---|
| `Dockerfile.web-runner:37` (local builds) | `node:26.13.0-slim`, bumped by the Renovate update to Node 26 (#35) |
| `Dockerfile.web-runner.ci:5-6`, the image CI publishes (`.github/workflows/ci.yml:609,625`) | 24.22.1, a tarball fetched by `curl` and checked by SHA-256 |
| `docs/versions.md:1448`, the v0.33.1 notes | "Node 22". That release's published image actually ran 20.18.1 (`git show v0.33.1:Dockerfile.web-runner.ci`) |

Renovate manages `FROM` lines (`renovate.json`, the "docker images" group).
It does not manage the tarball URL in the `.ci` file, and that is how the
two files drifted apart. Nothing in CI compares them. The running runner
does not report its Node version anywhere.

## 2. Design

### 2.1 Hostname aliases (#60)

#### 2.1.1 What a service declares

A web app's manifest gains a `web` section (section 3.1):

```json
"web": {
  "hosts": ["shop.example.com", "www.shop.example.com"],
  "public_host": "shop.example.com"
}
```

- `hosts`: exact hostnames, at most 16 per service. No wildcards, no
  ports, no IP literals.
- `public_host`: optional, and must be one of `hosts`. When it is set, the
  app's `CUFFLINK_PUBLIC_URL` is `{CUFFLINK_PUBLIC_SCHEME}://{public_host}`.
  When it is not set, the generated URL stays the public one and the
  aliases are extra names.
- The generated name keeps routing. Health checks, previews and operator
  access keep working (question 3).

#### 2.1.2 Canonical names

`cufflink_types::web::hostname(raw) -> Result<String, HostnameError>` is
the one validator. The CLI, the platform and the runner all use it:

- Parse with `url::Host::parse`. An `Ipv4` or `Ipv6` result is refused
  (`ip_literal`).
- The name must already be canonical: lowercase, IDNA A-labels (punycode),
  no trailing dot. A name that parses but is not canonical is refused with
  what to write instead (`not_canonical: write 'xn--bcher-kva.example.com'`),
  as `allowed_origin` does for origins. Nothing is silently rewritten, so
  the name in `Cufflink.toml`, the name stored and the name routed are the
  same string.
- At least two labels. Each label is 1-63 letters, digits or hyphens, and
  does not start or end with a hyphen. At most 253 characters in total. No
  `*`.

#### 2.1.3 Who may claim a hostname

The model follows tenant buckets (`docs/design-notes/storage-tenant-buckets.md`
sections 2.1 and 7). A platform admin decides who owns a name. The
platform makes the claim unique. A deleted owner's claim stays retired
until an admin releases it.

- **Domain grants.** A platform admin grants a domain to one tenant:
  `cufflink tenants domains set harbor example.com`. A grant of
  `example.com` covers `example.com` and every name ending in
  `.example.com`.
  - Two tenants' grants may not overlap: neither may be equal to the other
    or end with `.` plus the other. The check runs under a transaction-scoped
    advisory lock, since a unique index cannot express suffixes.
  - Grants of one tenant may overlap.
- **Hostname claims.** A deploy whose manifest names `hosts` claims each
  one for its service, in the deploy's transaction. A name is refused when:
  - it is under no grant of the deploying tenant (`403 host_not_granted`);
  - another service holds it (`409 host_claimed`). The answer does not name
    the other service or its tenant.

  `web_hosts.hostname` is the primary key, so a name has one claimant on
  the whole platform.
- **Which grant a claim records.** A claim records the most specific grant
  of its tenant that covers it: `shop.example.com` over `example.com`.
- **Retired grants.** Removing a grant is refused when it covers a claimed
  host that no other grant of the same tenant covers (`409 domain_in_use`,
  listing those hosts). A claim recorded under the removed grant moves to
  the most specific remaining grant in the same transaction.
  Otherwise the grant is retired. When a tenant is deleted, its grants are
  retired and its claims are deleted with its services. A retired grant
  covers nothing, and no tenant can be granted an overlapping domain until
  an admin releases it (`cufflink tenants domains release example.com`).
  So when a service or tenant goes away, nobody can immediately take over
  its traffic or its cookies.
- **Preview tenants** inherit no grants and no claims. A preview deploys
  with the hosts its own environment declares, which are normally none
  (section 3.2).
- **The base domain** is reserved for generated names. The runner ignores,
  and logs as an error, any alias that is equal to or under its
  `CUFFLINK_BASE_DOMAIN` (section 2.1.5). The platform does not know that
  setting today. It gains an optional `CUFFLINK_WEB_BASE_DOMAIN`, and when
  that is set, a grant under it is refused (question 4).
- **Verification.** There is no DNS challenge in this change. The admin's
  grant is the proof of ownership. The operator controls DNS and the edge,
  and a self-hosted platform should not need outbound DNS. Section 8.1
  explains why, and question 2 asks.

#### 2.1.4 What changes claims

| Event | Claims |
|---|---|
| Web deploy (`POST /api/services/deploy`, mode `web`) | Replaced by the new version's `hosts`, in the deploy's transaction. A refusal fails the deploy, and no deployment row is written |
| Rollback of a web app | Replaced by the target version's `hosts`. A conflict refuses the rollback (`409 host_claimed`). The rollback then publishes `deploy.web.<tenant>.<service>`, as the artifact upload does, so runners serve the restored version and hosts |
| Undeploy, service delete | Deleted in the same transaction. Runners already get `undeploy.web.*` (`platform/src/api/services.rs:290`) |
| Tenant delete | Deleted with its services. Its grants are retired |
| Grant removal | Refused while it covers a claim no other grant of the tenant covers; other claims move to the most specific remaining grant |

Claims follow the service's current version. The runner reads hosts from
that version's manifest (section 2.1.5). The table exists for uniqueness,
not for routing.

#### 2.1.5 The runner's index

The runner needs every alias before any app is warm, because the lazy
warmer resolves a host before anything has started (`proxy.rs:73-102`). A
per-request lookup in the database would let any unknown `Host` cost a
query. So the runner keeps one in-memory index, built from the current
deployments:

Each app's `WebHosting` (section 3.1) becomes one entry:

```rust
pub struct AppIndex {
    by_host: HashMap<String, AppKey>,
    apps: HashMap<AppKey, AppHosting>,
}

pub struct AppHosting {
    pub public_host: Option<String>,
    pub cors: Cors,
}
```

- **Shared mode** reads every current web deployment's `manifest->'web'`,
  using the same predicate `Shared::current` uses (`source.rs:226-236`),
  in one query. It reads no new table, so a runner of this release works
  against an older platform's schema, where it finds no `web` key.
- **Only a platform of this release can record `web`.** An older platform
  deserializes the deploy's manifest as the typed `ServiceManifest`
  (`platform/src/api/deployments.rs:35-36`) and stores it re-serialized
  (`serde_json::to_value(req.manifest)`, `platform/src/repo/deployments.rs:167`).
  An older `ServiceManifest` has no `web` field, so the section is dropped
  before it is stored. A tenant cannot get unvalidated hosts into a manifest
  through an older platform. As defence in depth, the runner honours
  `web.hosts` and `web.public_host` only in a manifest whose
  `required_platform_features` contains `web_hosts`. A platform of this
  release always stores the derived features: it adds any that are missing
  before storing, as it already refuses deploys whose features it lacks.
- **Tenant-scoped mode** reads `WebApp.hosting` from `GET /api/runner/web`
  (section 3.4). `watch_key` already fetches that set every `WEB_POLL_SECS`
  and throws it away (`apps.rs:325`). The index is rebuilt from that
  response.
- **When the index is built:**
  - at start, before `/_cufflink/ready` answers 200;
  - after every deploy or undeploy event;
  - every `WEB_INDEX_REFRESH_SECS` (default 60) in shared mode, and at each
    poll in tenant-scoped mode, so a lost core-NATS event heals.
- **Building is a pure function** from the rows to an `AppIndex`:
  - An alias that is not canonical is dropped and logged.
  - An alias under the base domain is dropped and logged.
  - An alias that two apps name is dropped for both and logged.
    `web_hosts` makes that impossible through the API; the runner still
    fails closed when the rows say otherwise.
  - Each drop is counted in
    `cufflink_web_runner_index_dropped_total{reason}`.
- **The swap is atomic.** The index sits in an `ArcSwap` (`arc-swap` is
  already in `Cargo.lock`), so a request sees either the old index or the
  new one, never a partial one.
- **A failed refresh keeps the last good index** and logs at `warn`. A
  stale alias is better than none. If the first build fails, the runner
  stays unready.

#### 2.1.6 Resolving a request

`host::resolve(request_host, &index, &routing) -> Option<AppKey>`:

1. Take the host from `Host`, or from the URI authority when there is no
   `Host` (HTTP/2). Drop a port and handle a bracketed IPv6 literal
   properly. Lowercase it and drop one trailing dot.
2. If the name is in `index.by_host`, that is the app.
3. Otherwise apply generated-name parsing, with the flat suffix rule below.
4. Otherwise, 404 as today.

The alias lookup comes first in both formats. In flat mode, a name like
`shop.example.com` would otherwise parse as tenant `default`, service
`shop`.

**The flat suffix rule.** A released or never-claimed alias must not fall
through to a `default`-tenant service that shares its first label. So flat
parsing has to require the base domain. That changes the behaviour
`flat_bare_service_host_resolves_to_default_tenant` pins, so it gets a
setting and the same two-release migration as CORS:

| `WEB_FLAT_HOSTS` | Flat parsing accepts |
|---|---|
| `any` | any host, as today. The runner warns once per first label it routes from outside the base domain, and counts it in `cufflink_web_runner_flat_outside_base_total` |
| `base` | only `{label}.{CUFFLINK_BASE_DOMAIN}`, case-insensitively |

The default is `any` in the release that ships aliases, and `base` in the
next minor (section 5.3). `localhost` setups set
`CUFFLINK_BASE_DOMAIN=localhost`, which `docs/setup.md:1008` already does,
and keep working under `base`.

#### 2.1.7 The public URL and restarts

`ProcessManager::deploy` composes the public URL from the app's
`AppHosting`: `public_host` when it is set, otherwise
`compose_public_url` as today. The public URL is part of the process
environment. A change to `public_host` therefore changes the environment
digest (section 2.3), and the next deploy event restarts the app with zero
downtime. A change to `hosts` alone only changes the index and restarts
nothing.

#### 2.1.8 TLS

TLS stays at the operator's edge. An alias needs a certificate there that
covers it, and DNS pointing at the edge. The runner sees plain HTTP as
today. The documentation says so where aliases are described. An "ask"
endpoint for an edge's on-demand certificates, answering whether a name is
claimed, is question 5 and not part of this change.

### 2.2 CORS (#65)

#### 2.2.1 Per app, declared

```json
"web": { "cors": { "mode": "app" } }
"web": { "cors": { "mode": "permissive" } }
"web": { "cors": { "mode": "origins",
                   "origins": ["https://shop.example.com"],
                   "credentials": true } }
```

| Mode | The runner |
|---|---|
| `app` | adds no CORS header and answers no preflight. Every request, `OPTIONS` included, reaches the app, and the app's headers are left as they are |
| `permissive` | does exactly what it does today: `CorsLayer::permissive()` |
| `origins` | answers preflights and sets CORS headers for the listed origins only. Methods and request headers are mirrored from the preflight (`AllowMethods::mirror_request()`, `AllowHeaders::mirror_request()`). `credentials` adds `Access-Control-Allow-Credentials: true`. No headers are exposed, so an app can set its own `Access-Control-Expose-Headers` and the runner does not overwrite it |

- Origins are validated by the platform's `allowed_origin` rules
  (`layers.rs:111-136`). That function moves to
  `cufflink_types::web::origin`, and the platform, the CLI and the runner
  call the one copy. The rules: an exact browser origin, `http` or `https`,
  no path, no wildcard, no `*`, at most 32 origins.
- tower-http panics on credentials combined with any wildcard
  (`cors/mod.rs:783-810`). The `origins` mode never builds a wildcard, and
  validation runs at deploy, so that panic cannot be reached. A test pins
  this.
- A manifest with no `cors` gets the runner's default (section 2.2.3).

#### 2.2.2 Where the runner applies it

- **Per app, in the handler.** The router-wide layer goes. Each app's
  `Cors` is built once, when the index is built:
  `enum Cors { App, Layer(CorsLayer) }`. The handler resolves the app, then
  either forwards the request or calls
  `layer.layer(service_fn(forward)).oneshot(req)`.
- **The fallback also routes `OPTIONS`.** It becomes
  `get(..).post(..).put(..).delete(..).patch(..).options(..)`, so an `app`
  app receives its preflights. `TRACE` and `CONNECT` stay unrouted (405).
- **Ops routes** (`/_cufflink/*`, `/health`) get no CORS headers. Browsers
  have no reason to call them, and the status and metrics routes need a
  bearer token.

#### 2.2.3 The default and its migration

`WEB_CORS_DEFAULT` applies to apps that declare no `cors`:

| Value | Meaning |
|---|---|
| `permissive` | today's behaviour |
| `app` | the app owns its CORS |

- **Release A** (the release that ships this; v0.38.0 if nothing comes
  first):
  - The default is `permissive`.
  - Each app that relies on it is logged once per process start at `warn`
    (`web app acme/portal relies on WEB_CORS_DEFAULT=permissive; declare
    web.cors or expect 'app' from the next minor`).
  - The gauge `cufflink_web_runner_cors_default_apps` shows how many apps
    rely on it.
  - Ops routes lose CORS (release note).
- **Release B** (the next minor): the default becomes `app`.
  `WEB_CORS_DEFAULT=permissive` stays accepted, so an operator can hold the
  old behaviour for every undeclared app while their owners add
  `web.cors`. Question 6 asks whether it is ever removed.
- An unknown value stops the runner at start, naming the setting, as
  `CUFFLINK_HTTP_PROFILE` does.

### 2.3 Restarting on config change (#68)

#### 2.3.1 The environment digest

- `Release` (`process_manager.rs:23-27`) gains
  `env_digest: [u8; 32]`. That is the SHA-256 of the process's final
  environment: the `BTreeMap` that `child_env` returns
  (`process_manager.rs:197-212`), **without `PORT`**, hashed as
  length-prefixed key and value bytes in key order.
  - `PORT` is left out because it is assigned per launch:
    `PortAllocator::allocate` always picks a port the running process is
    not using (`process_manager.rs:51-58`). With `PORT` included, the
    digest would never match and every event would restart the app.
  - The digest covers the runner's allowlisted environment, the configs and
    the rest of the identity, including `CUFFLINK_PUBLIC_URL`.
  - When children's telemetry is scoped, the identity also includes
    `service.version` in `OTEL_RESOURCE_ATTRIBUTES` (`supervisor/src/env.rs:242`).
    So a new version with an identical bundle and configs restarts, and its
    process reports the version it now is. That is intended.
- `Apps::run` is reordered:
  1. Read the configs. A failure ends the run with an error, and the
     running process is unchanged.
  2. Compose the launch environment. This is the pure part of
     `ProcessManager::deploy`, moved into `ProcessManager::launch_env`.
  3. Digest it.
  4. Return `AlreadyRunning` only when the app runs from the recorded
     directory **and** with the same digest, and the event is not a forced
     restart (section 2.3.2).
  5. Otherwise start the new process with zero downtime, as today. A failed
     health check keeps the old process.

  When the directory is unchanged, the `drain` and `prune` of
  `apps.rs:166-169` leave it alone, because the new process runs from it
  (`apps.rs:233-234`).
- `Shared::configs` returns `eyre::Result<HashMap<..>>` instead of an empty
  map on failure (`source.rs:258-282`). Before this change, a failed read
  only affected a cold start. With restarts, a database blip would restart
  a healthy app with no configs.
- What this alone gives: any deploy event, including the artifact upload of
  a bundle whose hash did not change, applies the current configs.
  `docs/deployment.md:844` is corrected to say so.

#### 2.3.2 Asking for a restart

- `POST /api/services/{id}/web/restart`, with a deploy credential:
  - `202 {"restart_id": "<uuid>", "version": 7}`;
  - `404` for a service of another tenant or none;
  - `409 not_a_web_app`;
  - `409 no_artifact` when the current version has no bundle.
- It publishes `deploy.web.<tenant>.<service>` with the usual payload plus
  `"restart": "<uuid>"`. A failed publish answers
  `503 restart_not_published`. Unlike the upload, the event is the whole
  point of this request, so a lost publish is not just logged. It writes
  one audit row, `web.restart`.
- It is advertised as `web_restart` in `/api/version`.
- A runner that gets an event with `restart`:
  - restarts the app with zero downtime from what the database (or runner
    API) records, if the app is running or crash-looping on that replica;
  - calls `forget_exits` first, as a deploy does (`apps.rs:183`);
  - remembers the last 1024 restart ids and ignores repeats;
  - leaves an app that is not running alone. Its next request cold-starts
    it with the current configs anyway.
- **The event invariant holds.** The event still only names the app. The
  runner reads the bundle, hash, service and configs itself. `restart`
  cannot change what runs, only when it is started again. Whoever can
  publish on `deploy.web.>` can already publish `undeploy.web.>`, which
  stops apps, so the flag gives nobody new power. Guests cannot claim
  either namespace (`subjects.rs:93`). The subject does not change, so
  tenant-scoped runners' NATS permissions do not change. Section 8.4
  explains why the restart is not a generation stored in the database.

#### 2.3.3 The CLI

- `cufflink web restart [--service <name>]` calls the endpoint. It prints
  `Restart requested for portal v7; each web runner replica running it
  starts a new process with the current configs`.
- `config apply`, `config set`, `config delete` and `config sync`, for a
  web app, on a platform that advertises `web_restart`, call the endpoint
  after a change that wrote something: any key added, changed or removed.
  `--no-restart` skips the call.
  - On an older platform they keep today's hint (`deployment.md:439-442`).
  - A dry run or `--check` never restarts.
- `cufflink deploy` never requests a restart. The artifact upload that
  follows its config step already starts the new bundle with the new
  configs, and a restart in between would briefly run the old bundle with
  the new configs.
- The platform never restarts on its own when configs change (question 7).
  A caller using the API directly calls the restart endpoint.

#### 2.3.4 Lost events

Core NATS can drop an event (`publish_bounded`, `web.rs:77-86`). The
consequences:

- A lost restart means that replica keeps the old environment until its
  next deploy event or cold start.
- The index refresh (section 2.1.5) re-reads hosts, not configs. It does
  not restart apps: re-reading every app's configs on every refresh would
  cost a secrets open per app per minute.
- `/_cufflink/status` gains each app's `started_at` and the first 12 hex
  digits of its `env_digest`, so an operator can check that every replica
  restarted.
- Question 8 asks whether a periodic config reconcile is wanted.

### 2.4 Forwarded headers (#69)

#### 2.4.1 Trust

- `WEB_TRUSTED_PROXIES`: a comma-separated list of IP addresses or CIDR
  ranges, default empty. It is parsed by the function that parses
  `RATE_LIMIT_EXEMPT_IPS`, which moves from
  `platform/src/middleware/rate_limit.rs:135-150` into `cufflink-settings`
  as `ip_nets(name, raw)`.
- The server is started with
  `into_make_service_with_connect_info::<SocketAddr>()`, and the handler
  reads `ConnectInfo<SocketAddr>`, as the platform does.

#### 2.4.2 What the app receives

| Header | From a trusted peer | From any other peer |
|---|---|---|
| `X-Forwarded-For` | the incoming value with `, {peer}` appended, or `{peer}` when absent | replaced by `{peer}` |
| `X-Forwarded-Host` | the incoming value when present, otherwise the request host | replaced by the request host (section 2.1.6, step 1) |
| `X-Forwarded-Proto` | the incoming value when it is `http` or `https`, otherwise `CUFFLINK_PUBLIC_SCHEME` | replaced by `CUFFLINK_PUBLIC_SCHEME` |
| `Forwarded` (RFC 7239) | passed unchanged | removed |
| `X-Real-IP` | passed unchanged | removed |

- Several values of the same header are joined in order before appending.
  A value that is not visible ASCII is dropped rather than passed through.
- The runner never sees TLS, so it cannot learn the scheme. The configured
  public scheme is the honest default: it is what `CUFFLINK_PUBLIC_URL`
  already tells the app.
- With aliases, `Host` is already the name the browser used, so
  `X-Forwarded-Host` normally equals it. It is still set, because
  frameworks prefer it, and because an install that keeps rewriting `Host`
  at its edge can pass the original through a trusted peer.
- `x-tenant-slug` handling is unchanged.

### 2.5 The Node version (#69)

- **One version, both Dockerfiles.** `Dockerfile.web-runner.ci` drops the
  tarball and starts `FROM node:<version>-slim@sha256:…`, exactly as
  `Dockerfile.web-runner` does. Renovate then manages both `FROM` lines in
  one group, and majors are never automerged (`renovate.json`). Question 9
  asks which version, and whether the published image may move from the
  Ubuntu base to Debian slim.
- **Renovate.** A package rule restricts the `node` image to even-numbered
  (LTS-line) majors.
- **A CI check,** `.github/scripts/node-version.sh`, run in the `fmt` job,
  fails when:
  - the two Dockerfiles name different Node versions; or
  - the "Node" line of `docs/deployment.md`'s web runner section names a
    different major.
- **The runner reports it.** It runs `node --version` once at start, logs
  it, and answers it in `/_cufflink/status` as `node`. A runner with no
  `node` on its path logs that at `error` and stays ready, so an operator
  sees it before an app's cold start fails.
- **History.** The v0.33.1 notes are not rewritten. The next release's
  notes state the image's Node version and correct the v0.33.1 line.

## 3. Manifest, `Cufflink.toml`, API and CLI

### 3.1 Manifest types

In `types/src/manifest/web.rs`:

```rust
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebHosting {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cors: Option<WebCors>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum WebCors {
    App,
    Permissive,
    Origins {
        origins: Vec<String>,
        #[serde(default)]
        credentials: bool,
    },
}
```

- `ServiceManifest` gains
  `#[serde(default, skip_serializing_if = "Option::is_none")] pub web: Option<WebHosting>`.
- `derived_features` adds:
  - `web_hosts` when `hosts` is non-empty or `public_host` is set;
  - `web_cors` when `cors` is set.
- `validate_manifest` refuses a `web` section on any mode but `web`. It
  also refuses each case of sections 2.1.2 and 2.2.1, and a `public_host`
  that is not in `hosts`.
- `ServiceManifest` has no `deny_unknown_fields`, so an older platform
  would drop `web` without a word. The CLI builds the web manifest by hand
  (`cli/src/commands/deploy.rs:1451-1456`). `deploy_web` therefore sets
  `required_platform_features` from the same `derived_features`, and an
  older platform refuses the deploy instead of ignoring the section.

### 3.2 `Cufflink.toml`

Hosts differ per environment, so the section lives under the environment:

```toml
[service]
name = "portal"
mode = "web"

[environments.production.web]
hosts = ["shop.example.com", "www.shop.example.com"]
public_host = "shop.example.com"
cors = { mode = "origins", origins = ["https://admin.example.com"], credentials = true }

[environments.staging.web]
cors = { mode = "app" }
```

- `EnvironmentConfig` (`cli/src/project_config.rs:23-35`) gains
  `web: Option<WebHosting>`, deserialized with the same type.
- The CLI validates the section with the shared validators before anything
  is built or sent.

### 3.3 Platform API

| Method | Path | Who | Answers |
|---|---|---|---|
| PUT | `/api/tenants/{slug}/domains/{domain}` | platform admin | `200 {domain, tenant, updated_at, updated_by}`; `409 domain_overlaps` (another tenant's grant, or a retired claim); `422 invalid_domain` |
| GET | `/api/tenants/{slug}/domains` | platform admin, or the tenant's own admin | the grants, each with the tenant's hosts under it |
| DELETE | `/api/tenants/{slug}/domains/{domain}` | platform admin | `200`, retired; `409 domain_in_use` with the hosts |
| POST | `/api/domains/{domain}/release` | platform admin | `200`, frees a retired claim; `409` while it is active |
| POST | `/api/services/{id}/web/restart` | deploy credential | section 2.3.2 |
| GET | `/api/services/{id}/web` | deploy credential | as today (`web.rs:121-159`), plus `hosts`, `public_url` and `cors` of the current version |

- The deploy and rollback refusals are `403 host_not_granted` and
  `409 host_claimed`, naming the host.
- Every grant change and release is audited (`web.domain.set`,
  `web.domain.retire`, `web.domain.release`).
- Features advertised in `/api/version`: `web_hosts`, `web_cors`,
  `web_restart`.

### 3.4 Runner API

`WebApp` (`types/src/runner.rs:59-69`) gains
`#[serde(default)] pub hosting: Option<WebHosting>`, filled from the
current deployment's manifest. An older platform omits the field, and a
runner of this release reads `None`. An older runner ignores the field.
`WebApp` has no `deny_unknown_fields`.

### 3.5 Runner settings

| Variable | Default | Meaning |
|---|---|---|
| `WEB_CORS_DEFAULT` | `permissive` (Release A), `app` (Release B) | CORS for apps that declare none |
| `WEB_FLAT_HOSTS` | `any` (Release A), `base` (Release B) | whether flat parsing requires `CUFFLINK_BASE_DOMAIN` |
| `WEB_TRUSTED_PROXIES` | empty | peers whose `X-Forwarded-*`, `Forwarded` and `X-Real-IP` are kept |
| `WEB_INDEX_REFRESH_SECS` | `60` | how often a shared runner re-reads hosts and CORS; greater than 0 |

Each is parsed in `WebRunnerConfig::load` (`web-runner/src/config.rs:76-107`)
and stops the runner at start when it is invalid, naming the variable.

### 3.6 CLI

- `cufflink tenants domains set|list|remove <tenant> [<domain>]` and
  `cufflink tenants domains release <domain>`, alongside `tenants buckets`.
- `cufflink web restart [--service <name>]`.
- `--no-restart` on `config apply|set|delete|sync`.
- `cufflink deploy` for a web app sends `web` from the selected
  environment, and refuses before building when the platform does not
  advertise a feature the section needs.

## 4. Data and migration

`platform/migrations/V9__web_hosting.sql`. It is additive only, so an
earlier binary runs against this schema and never reads it:

```sql
SET LOCAL lock_timeout = '10s';
CREATE TABLE IF NOT EXISTS tenant_domains (
    id          UUID PRIMARY KEY,
    tenant_id   UUID REFERENCES tenants(id) ON DELETE SET NULL,
    domain      TEXT NOT NULL UNIQUE,
    retired_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by  TEXT,
    CHECK (domain = lower(domain)),
    CHECK (tenant_id IS NOT NULL OR retired_at IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS tenant_domains_tenant_idx ON tenant_domains (tenant_id);

CREATE TABLE IF NOT EXISTS web_hosts (
    hostname    TEXT PRIMARY KEY,
    tenant_id   UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    service_id  UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    domain_id   UUID NOT NULL REFERENCES tenant_domains(id),
    claimed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (hostname = lower(hostname))
);
CREATE INDEX IF NOT EXISTS web_hosts_service_idx ON web_hosts (service_id);
```

- A tenant delete with `ON DELETE SET NULL` leaves a grant with no tenant.
  The delete path retires it in the same transaction, so the `CHECK` holds.
  This is the same pattern as `tenant_buckets` (`V8__tenant_buckets.sql`).
- `web_hosts.domain_id` has no cascade, so a grant cannot be removed under
  a claim even by a direct SQL delete.
- **Existing data:** there is none to backfill. No deployment has a `web`
  section before this release.

## 5. Features and compatibility

### 5.1 Features

| Feature | Needed when | Provides |
|---|---|---|
| `web_hosts` | the manifest's `web` names `hosts` or `public_host` | hostname claims within domain grants; aliases routed by runners of this release |
| `web_cors` | the manifest's `web` names `cors` | the app's CORS mode, applied by runners of this release |
| `web_restart` | never demanded; the CLI checks it before calling | `POST /api/services/{id}/web/restart` |

### 5.2 Version skew

| Platform | Runner | What happens |
|---|---|---|
| new | new | as designed |
| old | new | No manifest has `web`: an older platform drops it as it stores the typed manifest (section 2.1.5). So there are no aliases, and every app gets `WEB_CORS_DEFAULT`. Forwarded headers, the environment digest and the Node report work. The shared runner's index query reads `manifest->'web'`, which is absent, not missing. Restarts are never requested |
| new | old | **Aliases fail closed:** an old runner does not know them and answers 404, or in flat mode routes by first label as today. Declared CORS is ignored and stays permissive. A restart event is taken as a plain deploy event and changes nothing (`AlreadyRunning`) |
| any | any, with an old CLI | The CLI sends no `web` and never calls restart, which is today's behaviour |
| old | any, with a new CLI | A manifest with `web` is refused by the CLI, which checks `/api/version`. Restart is not called and today's hint is printed |

**Upgrade order: web runners first, then the platform, then the CLI.** A
new runner on an old platform behaves as today. A new platform with an old
runner accepts aliases that runner cannot serve, and in flat mode such a
name can reach a `default`-tenant service that shares its first label.
Until every runner is upgraded, no domain grant should be made; the
release note says so. Tenant-scoped runners follow the same order.

### 5.3 The two releases

| | Release A | Release B (next minor) |
|---|---|---|
| CORS | `app`, `permissive`, `origins` declarable; default `permissive` with warning and gauge; ops routes lose CORS; `OPTIONS` routed | default `app`; `WEB_CORS_DEFAULT=permissive` still accepted |
| Flat hosts | `WEB_FLAT_HOSTS=any` default, warning and counter for names outside the base | default `base`; `any` still accepted |
| Aliases, restarts, forwarded headers, Node | shipped | unchanged |

### 5.4 Rolling back

- **Runner below Release A:** aliases stop routing (404, or in flat mode
  the first label). Declared CORS reverts to permissive. Apps lose
  forwarded headers. Nothing on disk changes.
- **Platform below Release A:**
  - `tenant_domains` and `web_hosts` stay, unread.
  - Deploys with `web` are refused by the CLI.
  - Already-recorded manifests keep their `web`, and a Release A runner
    keeps routing them.
  - A later return to Release A finds the claims as they were.

  Any web deploy made on the older platform replaces the manifest with one
  that has no `web`, while its claims stay in `web_hosts` until the next
  deploy on Release A replaces them. Those stale claims block other
  services, and only other services, from taking those names: the fail-safe
  direction.
- **Release B to Release A:** the defaults return to `permissive` and
  `any`. Set the variables explicitly to keep Release B's behaviour.

## 6. Failure handling

| Failure | Behaviour |
|---|---|
| Index build fails at start | runner stays unready (`/_cufflink/ready` 503) and retries every `WEB_INDEX_REFRESH_SECS` (or `WEB_POLL_SECS`) |
| Index refresh fails later | last good index kept; `warn`; `cufflink_web_runner_index_refresh_failures_total` |
| Alias not canonical, under the base domain, or claimed twice in the rows | dropped from the index, logged at `error`, counted by reason; other aliases unaffected |
| Request for an unclaimed name | 404, as for any unknown host; no database query |
| Config read fails during a deploy or restart | run ends with an error; the running process unchanged (shared mode no longer starts with empty configs) |
| New process fails its health check on restart | old process keeps serving, as for a deploy (`process_manager.rs:268-285`) |
| Restart event lost | that replica keeps its environment until the next event or cold start; visible in `/_cufflink/status` |
| Restart publish fails | `503 restart_not_published`; nothing changed; safe to repeat |
| Deploy names a host not granted, or claimed by another service | deploy refused (`403`/`409`), no deployment written; the previous version keeps serving |
| Rollback target's hosts now claimed elsewhere | rollback refused (`409 host_claimed`) |
| Grant removal under a claim no other grant covers | `409 domain_in_use` |
| Invalid `WEB_*` setting | runner refuses to start, naming the variable |
| `node` missing from the image | `error` at start; runner stays ready; `node` absent from status |

## 7. Invariants

1. A hostname routes to at most one app on the whole platform. That holds
   in the database (`web_hosts` primary key) and in each runner (a
   duplicate is dropped for both apps).
2. A tenant's app can be reached at an alias only when the name is under a
   domain an admin granted that tenant, and the grant is not retired.
3. A tenant-scoped runner routes only its own tenant's aliases, because
   they come from its own runner API response, and its existing tenant
   check (`proxy.rs:65-71`) still applies after resolution.
4. No alias shadows a generated name: aliases under the base domain are
   refused by the runner and, when configured, by the platform.
5. Events only say which app changed. What runs, and with which configs,
   is always read by the runner, and a restart flag only re-runs it.
6. A restart or deploy that fails at any step leaves the running process
   as it was.
7. An app receives `X-Forwarded-*`, `Forwarded` or `X-Real-IP` values from
   the client only when the TCP peer is in `WEB_TRUSTED_PROXIES`.
8. An app in `app` mode receives every request unchanged by CORS, its
   `OPTIONS` requests included, and its CORS headers are never
   overwritten.

## 8. Alternatives considered

### 8.1 DNS verification instead of an admin grant

A TXT-record challenge would let tenants claim domains without an admin.
But the platform does no outbound DNS today, a self-hosted platform often
cannot, and the operator already has to configure DNS and certificates at
the edge for every alias. The grant puts the decision where the edge work
is. A challenge can be added later as another way to create a grant,
without changing claims or routing.

### 8.2 Hosts as their own API instead of in the manifest

`PUT /api/services/{id}/web/hosts` would let hosts change without a
deploy. But hosts would then not be versioned with the deployment, a
rollback could not restore them, and `Cufflink.toml` would no longer
describe what runs. Keeping them in the manifest gives deploy, rollback and
review one source.

### 8.3 Routing from `web_hosts` in the runner

That would be simpler for the runner, but a shared runner of this release
would then need the new table, and so the platform's migration, before it
could start. Reading the manifest keeps the runner-first upgrade order
safe.

### 8.4 A restart generation stored in the database

A `services.web_generation` counter, bumped by the restart endpoint and
read by the runner, would keep even the "when" out of the event. But the
runner reads it from a column an older schema lacks, which breaks the
runner-first order. It also adds nothing over the flag: a lost event loses
either, and a new replica starts fresh in both. The flag cannot change
what runs (section 2.3.2).

### 8.5 The platform restarting on every config change

That would be the simplest for API callers. But `cufflink deploy` applies
configs before uploading the bundle, and a restart in between briefly runs
the old bundle with the new configs. Existing scripts that change several
keys one at a time would restart once per key. The opt-in endpoint, which
the CLI calls by default, avoids both.

### 8.6 A global `WEB_CORS_ALLOWED_ORIGINS` on the runner

A runner-wide list, like the platform's, cannot express that different
tenants' apps trust different origins, and it keeps the runner answering
for apps that want to answer for themselves.

## 9. Tests

**`cufflink-types`:**

- `a_hostname_is_canonical_or_refused_with_what_to_write`: uppercase, a
  trailing dot, a U-label, an IP literal, a single label, a 64-character
  label, `*.example.com` and a leading hyphen are each refused with their
  reason.
- `an_origin_is_validated_as_the_platform_validated_it`: the existing
  `layers.rs` cases, moved.
- `web_hosting_round_trips_and_refuses_unknown_fields`.
- `a_manifest_with_web_hosts_derives_web_hosts_and_with_cors_web_cors`.

**`web-runner`, in-crate:**

- `host.rs`:
  - `an_alias_wins_over_a_flat_name_that_would_parse`;
  - `an_alias_wins_over_a_hierarchical_name`;
  - `flat_base_refuses_a_host_outside_the_base_domain`;
  - `flat_any_routes_as_before_and_reports_outside_base`;
  - `the_request_host_is_lowercased_unported_and_undotted`;
  - `an_http2_request_resolves_from_its_authority`;
  - `an_ipv6_literal_host_is_not_cut_at_its_first_colon`.
- `index.rs`:
  - `the_index_drops_an_alias_two_apps_name_for_both`;
  - `the_index_drops_an_alias_under_the_base_domain`;
  - `a_failed_refresh_keeps_the_last_index`;
  - `the_public_url_is_the_public_host_when_set`.
- `cors.rs`:
  - `an_app_mode_app_receives_its_options_requests`;
  - `an_app_mode_apps_cors_headers_are_not_overwritten`;
  - `permissive_mode_answers_as_corslayer_permissive_did` (same headers as
    today for a preflight and a simple request);
  - `origins_mode_admits_listed_origins_only_and_mirrors_methods`;
  - `origins_mode_with_credentials_never_builds_a_wildcard`;
  - `ops_routes_carry_no_cors_headers`.
- `forwarded.rs`:
  - `an_untrusted_peers_forwarded_headers_are_replaced`;
  - `a_trusted_peers_x_forwarded_for_is_appended`;
  - `a_trusted_peers_proto_is_kept_only_when_http_or_https`;
  - `forwarded_and_x_real_ip_are_removed_from_an_untrusted_peer`;
  - `x_forwarded_host_is_the_request_host`.
- `apps.rs`:
  - `a_deploy_of_the_same_bundle_with_other_configs_restarts`;
  - `a_deploy_of_the_same_bundle_and_configs_is_already_running`;
  - `a_failed_config_read_leaves_the_running_process`;
  - `a_restart_event_restarts_a_running_app_once_per_id`;
  - `a_restart_event_leaves_an_app_that_is_not_running`;
  - `the_env_digest_is_independent_of_insertion_order`;
  - `the_env_digest_ignores_the_assigned_port`.
- `index.rs` also:
  - `hosts_are_honoured_only_with_web_hosts_in_the_required_features`.
- `config.rs`:
  - `the_new_settings_default_as_documented`;
  - `an_invalid_cors_default_flat_hosts_or_proxy_list_stops_the_runner`.

**`web-runner/tests/hosting.rs`** (shared mode, with a Node stub bundle
as `tenant_scoped.rs` builds one; skipped without `node`, as there):

- `acme`'s `portal` declares `shop.example.com`. A request with that `Host`
  reaches it, and its `CUFFLINK_PUBLIC_URL` is `https://shop.example.com`.
- Tenant `default` has a service named `shop`. In flat `base` mode it is
  not reached through `shop.example.com` once `acme`'s alias is removed;
  under `any` it is, which is the capture this setting closes.
- An app with `cors = app` answers its own preflight. An app with no
  `cors` under `WEB_CORS_DEFAULT=permissive` gets today's headers.
- A restart request changes the process (its port changes) and its
  environment, and a forged `X-Forwarded-For` from an untrusted peer never
  reaches the app.

**`web-runner/tests/tenant_scoped.rs`:**

- `a_tenant_scoped_runner_routes_its_tenants_aliases_from_the_runner_api`.
- `an_alias_of_another_tenant_is_not_in_a_tenant_scoped_index`.
- `an_older_platforms_web_set_without_hosting_routes_as_before`.

**Platform, integration (`platform/tests/web_hosting.rs`):**

- Grants:
  - `a_grant_overlapping_another_tenants_is_refused`, both ways round;
  - `a_grant_under_the_web_base_domain_is_refused`;
  - `removing_a_grant_under_a_claim_is_refused`;
  - `a_deleted_tenants_grant_stays_retired_until_released`.
- Claims:
  - `a_deploy_claims_its_hosts_within_its_grants`;
  - `a_host_outside_every_grant_refuses_the_deploy_and_writes_nothing`;
  - `a_host_another_service_holds_refuses_the_deploy_without_naming_it`;
  - `a_redeploy_without_a_host_releases_it`.
- Rollback:
  - `a_rollback_restores_its_versions_hosts_and_publishes_deploy_web`;
  - `a_rollback_to_hosts_now_claimed_elsewhere_is_refused`.
- Lifecycle:
  - `undeploy_and_service_delete_release_hosts`;
  - `a_preview_tenant_inherits_no_grant`.
- Runner API:
  - `the_runner_api_answers_hosting_of_the_current_version`.
- Restart:
  - `restart_publishes_deploy_web_with_a_restart_id_and_audits`;
  - `restart_of_a_non_web_service_is_409`;
  - `restart_without_an_artifact_is_409`.
- Features:
  - `version_advertises_web_hosts_web_cors_web_restart`.
- `a_manifest_with_web_on_a_non_web_mode_is_refused`.

**Platform, in-crate:** `validate_manifest` cases for section 3.1. The
`platform/tests/config_apply.rs` suite gains
`apply_publishes_nothing_restart_is_its_own_request`, which pins
section 8.5.

**CLI:**

- `deploy_web_sends_web_and_required_features_from_the_environment`.
- `deploy_web_refuses_web_on_a_platform_without_the_features`.
- `config_apply_requests_a_restart_after_a_change_unless_no_restart`.
- `config_check_and_dry_run_never_restart`.
- `deploy_never_requests_a_restart`.
- `cufflink_toml_web_is_validated_before_building`.

**CI:**

- `.github/scripts/node-version.sh` fails on a fixture where the two
  Dockerfiles differ, and passes on the repository.

## 10. PR plan

One PR per issue, except that #69 holds two independent points and gets
two small PRs. Each PR updates its documentation and the unreleased
section of `docs/versions.md`.

| PR | Issue | Contents | Depends on |
|---|---|---|---|
| 1 | #69 | Node: one version in both Dockerfiles, Renovate rule, `node-version.sh` in CI, `node` in `/_cufflink/status` and the start log, `deployment.md` names the version | none |
| 2 | #69 | Forwarded headers: connect info, `WEB_TRUSTED_PROXIES`, `ip_nets` moved to `cufflink-settings`, header rules, request-host normalisation (HTTP/2 authority, IPv6) | none |
| 3 | #65 | CORS: `WebHosting`/`WebCors` (cors only), `web_cors` feature, `allowed_origin` moved to `cufflink-types`, `WebApp.hosting`, the runner's `AppIndex` with refresh (cors only), per-app CORS in the handler, `OPTIONS` routed, ops routes without CORS, `WEB_CORS_DEFAULT=permissive` with warning and gauge, CLI `[environments.*.web] cors` | none |
| 4 | #68 | Restarts: env digest, `Apps::run` reordered, `Shared::configs` returns `Result`, `POST /web/restart` and `web_restart`, restart ids in the runner, `cufflink web restart`, `--no-restart` on config commands, status fields | none; lands before 5 so a `public_host` change restarts |
| 5 | #60 | Aliases: `hosts`/`public_host`, `web_hosts` feature, `V9__web_hosting.sql`, grants API and CLI, claims in deploy, rollback (with its `deploy.web` publish) and undeploy, `CUFFLINK_WEB_BASE_DOMAIN`, alias index and resolution, public URL, `WEB_FLAT_HOSTS=any` with warning | 3 (index), 4 (restart on public URL change) |
| 6 | #65, #60 | Release B: `WEB_CORS_DEFAULT` → `app`, `WEB_FLAT_HOSTS` → `base` | ships in the next minor after 1-5 |

## 11. Release notes outline

**Release A**

- New: hostname aliases for web apps (`web.hosts`, `web.public_host`).
  Domain grants by platform admins (`cufflink tenants domains`). Feature
  `web_hosts`. **Upgrade web runners before the platform**, and make no
  grant until every runner is upgraded.
- New: per-app CORS (`web.cors`: `app`, `permissive`, `origins`). Feature
  `web_cors`. `WEB_CORS_DEFAULT` defaults to `permissive`, and apps relying
  on it are logged. **The default becomes `app` in the next minor.**
- Changed: the runner's ops routes (`/_cufflink/*`, `/health`) no longer
  carry CORS headers.
- New: `cufflink web restart` and `POST /api/services/{id}/web/restart`
  (feature `web_restart`). The CLI's config commands restart a web app
  after a change (`--no-restart` to skip).
- Changed: a deploy of an unchanged bundle now applies the app's current
  configs. A shared runner that cannot read configs no longer starts an app
  without them.
- New: `X-Forwarded-For`, `X-Forwarded-Host` and `X-Forwarded-Proto` on
  every proxied request. `WEB_TRUSTED_PROXIES` lists peers whose values are
  kept. Client-sent forwarding headers are otherwise replaced or removed.
- Changed: `WEB_FLAT_HOSTS` (default `any`). Flat names outside the base
  domain are logged. **The default becomes `base` in the next minor.**
- Fixed: the published image runs Node `<version>`; the v0.33.1 notes said
  22 while that image ran 20.18.1. Both Dockerfiles now agree, and CI
  checks it.
- Rollback: section 5.4's table.

**Release B**

- Changed: `WEB_CORS_DEFAULT=app` and `WEB_FLAT_HOSTS=base` by default.
  Set either to its old value to keep the old behaviour.

## 12. Documentation, when implemented

- `docs/deployment.md`, web runner section:
  - aliases, grants and claims;
  - TLS at the edge for aliases;
  - CORS modes and the default;
  - restarts, and the corrected line 844;
  - forwarded headers and `WEB_TRUSTED_PROXIES`;
  - the Node version;
  - the new settings table.
- `docs/setup.md`: the four runner variables,
  `CUFFLINK_WEB_BASE_DOMAIN`, and the domain and restart API rows.
- `docs/scaling.md:155`: aliases need DNS and certificates at the edge
  beyond the wildcard.
- `docs/building-on-cufflink.md`: `[environments.*.web]` in
  `Cufflink.toml`, `CUFFLINK_PUBLIC_URL` from `public_host`, and the
  forwarded headers an app can rely on.
- `docs/tenant-isolation.md`: invariants 1-4 and 7.
- `docs/observability.md`: the new metrics.
- `docs/versions.md`: the three features, both releases' changes, the
  upgrade order and the rollback table.

## 13. Not in this change

- Wildcard aliases (`*.shop.example.com`).
- Certificates issued or stored by the platform, and the edge "ask"
  endpoint (question 5).
- DNS verification of grants (section 8.1).
- Redirects from the generated name or other aliases to `public_host`.
- Restart on config change for native services in the extension runner.
- Stripping hop-by-hop headers (`Connection`, `Keep-Alive`, `TE`,
  `Upgrade`) on the proxy path; today they are copied as is
  (`proxy.rs:115-120`).
- A periodic reconcile of configs (question 8).

## 14. Questions for the maintainer

| # | Question | Proposed default |
|---|----------|------------------|
| 1 | Should hostnames be claimed within admin-granted domains, unique on the platform, with retired grants held until released (section 2.1.3)? | Yes |
| 2 | Should tenants also be able to prove a domain by a DNS TXT challenge (section 8.1)? | Not now; a later way to create a grant |
| 3 | Should an app with aliases keep answering on its generated name? | Yes. A `generated_host = false` option can come later |
| 4 | Where is the base domain reserved: the runner alone, or also the platform through a new optional `CUFFLINK_WEB_BASE_DOMAIN`? | Both. The runner always; the platform when the variable is set |
| 5 | Should the platform offer an endpoint an edge proxy asks before issuing an on-demand certificate for a name? | Not in this change |
| 6 | Should `WEB_CORS_DEFAULT=permissive` (and `WEB_FLAT_HOSTS=any`) ever be removed after Release B? | No removal planned; revisit at 1.0 |
| 7 | Should the platform restart a web app by itself on every config change, rather than on request (section 8.5)? | No. The CLI requests it by default |
| 8 | Should runners periodically re-read configs and restart apps whose environment changed, to heal lost restart events? | No. `/_cufflink/status` shows each replica's digest |
| 9 | Which Node version should both images run, and may the published image move from the Ubuntu base to `node:*-slim` (Debian)? | Node 24, which the published image already runs, so apps see no change in Release A. Debian slim for both. Node 26 as its own release-noted change once it has soaked |
| 10 | Should `WEB_TRUSTED_PROXIES` default to empty rather than private ranges? | Empty. A container network's other clients would otherwise be trusted |
| 11 | Are 16 hosts per service and 32 CORS origins per app the right caps? | Yes |
| 12 | Should the `origins` CORS mode ship with `app` and `permissive`, or later? | With them. It reuses the platform's validator and is the only way to make credentialed calls work through the runner |
