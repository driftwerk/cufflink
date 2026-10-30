# Deployment Notes

Operational settings that decide how an install behaves beyond its defaults.
Every variable is also listed in [setup](setup.md#platform-configuration).
An install that sets none of them behaves as it did before each setting
existed, except where a section says otherwise.
An install that hosts more than one tenant sets everything
[tenant isolation](tenant-isolation.md) lists.

## Guest memory budget

`WASM_MEMORY_BUDGET_MODE` ([setup](setup.md#wasm-runtime-limits-platformsrcwasm_runtimemodrs))
bounds what guests in flight together hold, so a burst cannot grow a
platform or worker past its container's limit.

- **The budget** is the container's limit less the host reserve,
  `max(256 MiB, 30 %)`. The limit is read from the process's cgroup: the
  smallest `memory.max` or `memory.high` of its cgroup v2 and every
  ancestor, or cgroup v1's `hierarchical_memory_limit`. Startup logs the
  limit, the file it came from, the reserve and the budget. Without a limit
  the budget is off.
- **What it counts:** each guest's linear memory and tables, and the stack
  of every blocking thread that has run a guest, until that thread retires
  2 s after its last task.
- **Stacks.** The 2 MiB stack charge (`WASM_THREAD_STACK_KB`) is an upper
  bound, often about ten times what a thread touches. In a 768 MiB
  container the 512 MiB budget holds the stacks of 256 invocations, light
  ones included. For many light concurrent guests, raise the container's
  memory, lower `WASM_THREAD_STACK_KB` (at least 1024; it sets every runtime
  thread's stack), or lower `WASM_MAX_CONCURRENT`.
- **What the reserve holds:** the process's baseline, compiled modules, the
  core threads' stacks, fragmentation, and for now the host buffers of
  host calls, requests and responses. On glibc startup sets
  `M_MMAP_THRESHOLD` to 1 MiB, so freed buffers of a mebibyte or more go
  back to the kernel; `MALLOC_MMAP_THRESHOLD_` still overrides it.
- **Soak** in `warn`, the default, and read
  `cufflink_wasm_admission_refused_total{mode="warn"}` and
  `cufflink_wasm_memory_denied_total{mode="warn"}` before setting `enforce`.

## HTTP request layers

`CUFFLINK_HTTP_PROFILE` picks the defaults; each value can be overridden on
its own. Invalid values stop the platform at startup.

| Variable | `legacy` (default) | `hardened` | Applies to |
|----------|--------------------|------------|------------|
| `HTTP_DATA_BODY_LIMIT_BYTES` | 250 MiB | 4 MiB | CRUD, batch and GraphQL requests |
| `HTTP_DATA_TIMEOUT_SECS` | none (`0`) | 60 | the same, until the response head (`503 {"error":"request timed out"}`). Never custom routes (their WASM budget is the timeout), SSE streams or control-plane routes |
| `HTTP_MAX_CONCURRENT_REQUESTS` | none (`0`) | 1024 | every request past the limit is shed with `503 {"error":"server busy"}` (`cufflink_http_load_shed_total`); `/health` and `/ready` are never limited |
| `CORS_ALLOWED_ORIGINS` | unset: any origin | required by the platform (not the worker), comma-separated origins exactly as a browser sends them (`https://app.example.com`: no path, no trailing `/`, lower case, no default port, no wildcard); anything else stops the platform at startup | browser origins |
| `RATE_LIMIT_ENABLED` | off | on (explicitly `false` turns it off) | the per-client-IP limiter |
| `CRUD_MAX_IN_ITEMS` | unlimited (`0`) | 100 | v1 `IN` filter lists (v2 lists are always bounded at 100) |

New installs should run `hardened`. Existing installs keep `legacy` until
their clients are known to fit the limits.

A body over `HTTP_DATA_BODY_LIMIT_BYTES` is refused with `413` and
`connection: close` once the platform has read past the limit, and the rest
is never read: a client still writing a larger upload may see the connection
reset before it reads the `413`, and should treat a reset during an upload
as too large when its body exceeds the limit.

The timeout answers once the handler has taken too long, not when its work
stopped: a write, or a batch, can still commit after the client got the
`503`. Clients retrying a timed-out write should make it idempotent (a
client-chosen `id`, or re-reading before writing again).

## CRUD filters, counts and errors

- `?filter_mode=v2` reads `filter` as a typed JSON tree (see
  [setup](setup.md#list-query-parameters)), refuses `per_page` outside 1–500
  and a page past the last with 404. `?count=estimate|none` skips the exact
  count.
- The tenant policy `strict_filters` ([policies](policies.md)) makes every
  listing v2 (`enforce`), or records each v1 listing v2 would refuse
  (`warn`, the soak signal: `cufflink_policy_violation_total{key="strict_filters"}`).
  Under `enforce`, or with `?filter_mode=v2`, write bodies are type-checked
  too (400 naming the column) instead of coerced.
- Database constraint errors on data routes answer by their SQLSTATE: unique
  and exclusion violations 409 `conflict: <constraint>`, a foreign key 422 on
  create/update and 409 on delete, check/not-null 422, trigger `RAISE` 422,
  invalid values 400, a cancelled statement 503; the body keeps its
  `{"error": …}` shape (`cufflink_crud_db_errors_total{kind,status}`).

## API keys

- `CUFFLINK_ADMIN_API_KEY_TTL_DAYS` (default 90) dates new platform-admin
  keys and, at every platform start, every admin key that has no expiry.
  `0` disables both. **Set it to `0` before first starting this release if
  admin keys must stay unbounded**: expiries written at startup persist, and
  every release enforces them, so rolling the image back does not undo them.
  To clear them on the platform database:

  ```sql
  UPDATE api_keys SET expires_at = NULL WHERE is_platform_admin;
  ```
- Rotate before a key expires: `cufflink auth keys rotate <id>` (tenant keys)
  or `cufflink auth keys rotate <id> --admin` (platform-admin keys, with
  `CUFFLINK_PLATFORM_API_KEY`). The old key keeps working for
  `--grace-hours` (default 24, at most 168). Rotations of the same key racing
  each other mint one new key; the others answer `409`.
- An expired key answers `401 {"error": "API key expired"}` and counts
  `cufflink_api_key_rejected_total{reason="expired"}`; an unknown one
  `Invalid API key` (`reason="unknown"`). `cufflink auth keys list` shows each
  key's expiry and last use.
- Once a tenant enforces its own `service_account_clients`, the platform warns
  at startup that `CUFFLINK_SERVICE_ACCOUNT_CLIENTS` and
  `CUFFLINK_DEFAULT_KC_OPS_CLIENT_ID` are deprecated.

## Service calls and identities

- `CUFFLINK_SERVICE_IDENTITY_KEYS="kid1:<64 hex>,kid2:<64 hex>"` signs and
  verifies the identity a service call carries: the first key signs, every
  key verifies, so a rotation adds the new key second, then moves it first,
  then drops the old one. Set the **same value on every platform and worker
  replica**. Unset, each process uses a random key of its own: in-process
  calls work, calls a worker makes do not verify.
- `PLATFORM_INTERNAL_URL` (worker only): the platform's base address
  (scheme, host and port) a worker sends service calls to. Unset, a guest
  running on a worker cannot call another service (the worker warns at
  startup). It is the worker's own setting: `CUFFLINK_INTERNAL_URL` and
  `PLATFORM_URL` are values the web runner passes through to web apps.

See [service calls](service-calls.md).

## Extensions

| Variable | Default | Meaning |
|----------|---------|---------|
| `CUFFLINK_EXTENSIONS` | `false` | Whether a platform admin may register NATS providers (`PUT /api/tenants/{slug}/extensions/{name}`) and tenants may deploy or roll back native services; while `false` each answers 403 naming the setting. Listing, removing a NATS provider, stopping, starting or deleting a native service and guests' `ext_call` work either way, so what was deployed can always be removed |
| `EXT_CALL_MAX_BYTES` | `1048576` | The largest request one `ext_call` sends and the largest reply it accepts, in bytes; never more than the NATS server's `max_payload`. Read by the platform and the worker |
| `EXT_CALL_TIMEOUT_SECS` | `30` | How long an `ext_call` waits when the guest sets no timeout; a call never waits past the handler's own budget. Read by the platform and the worker |
| `NATIVE_ARTIFACT_MAX_BYTES` | `134217728` | The largest native service binary a deploy may carry, decoded, in bytes (128 MiB; its base64 stays inside the platform's 250 MiB request limit) |

**Extension subjects.** A call to extension `name` of tenant `slug` is a NATS
request on `cufflink.ext.<slug>.<name>`, answered by one member of the queue
group `cufflink-ext`. Whoever subscribes there answers the tenant's calls, and
whoever publishes there speaks for the platform: in the NATS permissions, the
platform and the workers must be the only publishers on `cufflink.ext.>`, and
each provider may subscribe only to its own tenant's subjects
(`cufflink.ext.<slug>.>`). No stream may capture `cufflink.ext.>`, or it would
answer calls with its acknowledgement; the platform creates none that does.
A worker whose `SUBSCRIBE_PATTERN` overlaps it is refused at start.

A call no subscriber received (NATS answers that it has no responders) ran
nowhere, so the platform sends it again, 25 ms after the first and then
doubling up to 250 ms apart, each time within what is left of the call's
timeout, for at most a second: a runner that has just subscribed is not
known yet to every server. It fails `unavailable` after that second, or
sooner when too little of the timeout is left for another attempt, so a
guest that calls an extension nothing serves learns it within a second.

### Native services

A native service (`mode: native`) is deployed in one request that carries its
binary (`native_base64`), with the tenant's ordinary deploy authority (its
deploy key or deploy role), as a WASM or web service is: `cufflink deploy`,
`cufflink rollback` and `cufflink services delete|enable|disable` use the
environment's own credentials. Its code runs on the tenant's own extension
runner, which holds nothing of another tenant's ([extension
runner](#extension-runner)), so who deploys it no longer decides what it can
reach.

The binary is stored once per content at `native/{service_id}/{sha256}` in the
platform's object storage before the version is activated, so object storage
is required. A deploy whose manifest and binary are both unchanged is skipped;
a new binary alone makes a new version, and the objects of earlier versions stay
for rollback until those versions are deleted. Activating a version registers
the extensions it `provides` for the tenant in the same transaction and
publishes `deploy.ext.<tenant>.<service>`; deleting the service or its tenant
publishes `undeploy.ext.<tenant>.<service>`, and deleting any service removes
every object under `native/{service_id}/` (a first deploy that failed can
leave one behind a service with no version). Setting the service `inactive` publishes
`undeploy.ext` too and keeps its registrations; `active` publishes `deploy.ext`
again. A name the tenant registers as a NATS provider or for another native
service is refused (409), as is a deploy that drops or narrows a name a
deployed service calls, and a delete while one calls it (unless
`?force=true`, `cufflink services delete --force`).

## Active and inactive services

A service is `active` or `inactive` (`cufflink services enable|disable`,
`PATCH /api/services/{id}/status`; a new service is active unless its first
deploy [asks otherwise](#deploying-a-service-inactive)). Each replica runs the services its
registry holds, and its registry holds the active ones alone: after a
deploy, a rollback or a module upload a replica reads the service back from
the database and registers it only if it is active. So a service that was
disabled stays disabled through all three, on every replica, until it is
enabled; enabling it registers the version it was last given.

| While a service is inactive | |
|---|---|
| Its routes, `_meta/*`, GraphQL and SSE | 404 on every replica |
| Its consumer loops and schedules | Not started: nothing pulls from its durables and no schedule fires |
| A native service | Not started by its extension runner; its registrations are kept |
| A deploy's schema changes, SQL migrations and `on_migrate` | Run, as on any deploy that is not skipped |
| Its durables | Created, updated and handed over by the deploy, as for an active service |
| Its module | Stored and compiled by the deploy; loaded again when the service is enabled |
| Its database role and default roles | Provisioned by the deploy |
| Its jobs | Run: see below |
| A web app | Keeps running: the web runner starts an app from its deployment, whatever its service's status |

**Durables.** A deploy owns a service's durables whether or not the service
runs. A durable the platform manages for an inactive service therefore
exists, and on an `interest` stream it holds every message published for it
until the service is enabled and its loop takes them; on a `limits` stream
it reads from where it stopped. Disable a service for as long as its
streams can hold what arrives meanwhile, and delete it if it is not coming
back.

**Jobs.** The worker runs a job from its own row and does not ask whether
the job's service is active. Jobs already queued when a service is disabled,
and jobs an inactive service's `on_migrate` enqueues, run to their end.

A registry that could not read a service back after a deploy (the database
was away, or the registry kept changing under the read) leaves the deploy
committed and answered as done: the replica goes on with what it ran until
its next reload, which the deploy's own notification brings, and counts the
miss in `cufflink_registry_reload_total{result="error"}`.

### Deploying a service inactive

```sh
cufflink deploy --inactive
```

deploys without starting the service (`POST /api/services/deploy?inactive=true`).
A deploy never changes the status of a service that exists:

| The service | `--inactive` | no flag |
|---|---|---|
| does not exist | created inactive and deployed; nothing starts | created active and deployed |
| is inactive | deployed, stays inactive | deployed, stays inactive |
| is active | refused (409): disable it first, or deploy without the flag | deployed |

What a deploy does for an inactive service, it does here: the schema
changes, the SQL migrations and `on_migrate` run, and the deploy plans and
carries out the service's durables. A durable the manifest binds that already exists is kept
as it is, so a service can be deployed first while the consumer it replaces
still pulls from that durable: the deploy puts no second subscriber on it.
Stop that consumer, then `cufflink services enable <name>`, and the
service's loop starts from the durable.

The deploy's answer, its plan (`--plan`, which prints `status: inactive`)
and the record of a deploy that runs beyond its request
(`GET /api/deploys/{id}`, under `request` and `result`) each carry
`status`: `active` or `inactive`, what the service's status is once the
deploy is done. The CLI summarises such a deploy as
`Deployed <name> v<n> (inactive; start it with: cufflink services enable <name>)`,
and one that was skipped as `No schema changes for <name> v<n> — manifest
unchanged (inactive; start it with: cufflink services enable <name>)`. A
command the CLI names carries the `--env` the deploy was given, and its
`--tenant` where the deploy acted for that tenant, so it acts on the same
service. `--tenant` takes effect only with a platform admin's key in
`CUFFLINK_PLATFORM_API_KEY`, which is sent with the tenant's slug. Without
that key `cufflink deploy --tenant` deploys to the tenant of `Cufflink.toml`,
as it always did, and `cufflink services enable|disable --tenant <other>`
are refused before anything is sent (`--tenant <other> needs
CUFFLINK_PLATFORM_API_KEY: …`), since they would have acted on the service
of that name in the tenant of `Cufflink.toml`.

- A first deploy with `--inactive` that fails leaves the service's row,
  inactive and with no deployment. The next deploy of it is inactive with
  or without the flag.
- `--inactive --plan` of a service that does not exist reports
  `status: inactive` and creates nothing.
- A web app cannot be deployed inactive (422, and the CLI refuses it before
  it builds): its runner starts it whatever its service's status.
- `cufflink workspace deploy` and `cufflink deploy-package` have no such
  flag: deploy the service that is to start inactive by itself, once.

**What the CLI checks.** The flag needs a platform that advertises
`inactive_deploy`; the CLI refuses it for one that does not (`platform
<version> cannot deploy a service inactive (needs inactive_deploy). Upgrade
the platform or deploy without the flag.`), since that platform would ignore
the parameter and start the service. Before it builds the service's code,
the CLI also looks the service up and refuses one that is active, in the
words of the platform's 409: `Service '<name>' is active; a deploy never
stops a running service. Disable it first, or deploy without --inactive`.
With `--plan` it does the same.

**Deploy no service while a platform is being upgraded or rolled back**: a
replica of the older version can take the deploy after a newer one answered
`/api/version`. It reads no `inactive`, starts the service, and its answer
names no `status`. The CLI therefore checks the answer, and exits 1 for
each of these (3 stays the exit code of a plan the platform refused with a
422):

| The answer to `--inactive` | The CLI |
|---|---|
| says `inactive` | goes on |
| names no status | disables the service, which it saw absent or inactive before the deploy, and fails: `the platform deployed <name> active; it was disabled. Is a platform upgrade in progress?` Until then, the service ran |
| names no status, and the disable fails | fails: `the platform deployed <name> active, and it could not be disabled (<why>). Disable it now: cufflink services disable <name>` |
| says `active` | stops nothing and fails: `the platform deployed <name> active: it was enabled while the deploy ran, and was not stopped. If it should not run: cufflink services disable <name>`. Someone enabled the service between the CLI's look and the deploy's end |
| is a plan that does not say `inactive` | fails, and nothing was deployed: `the platform planned <name> as a deploy that starts it; nothing was deployed. Is a platform upgrade in progress?` |
| never came: the deploy could not be followed to its end | fails with what `GET /api/deploys/{id}` is for, and `it was asked not to start <name>: when it ends, check the service's status and, if it is active, disable it: cufflink services disable <name>` |

A deploy whose manifest is unchanged sends the module in a second request
(`POST /api/services/{id}/wasm`), whose answer names no status, and an older
replica that takes it registers the service whatever its status. After that
upload a deploy with `--inactive` therefore looks the service up again
(`GET /api/services`) and, exiting 1 for all but the first:

| The service is listed | The CLI |
|---|---|
| `inactive` | writes `inactive` again (`PATCH /api/services/{id}/status`), which removes the service from the replica that takes the write and has every other replica reload, and goes on. Until then, a service an older replica registered ran |
| `active` | stops nothing and fails: `<name> was enabled while its module was uploaded, and was not stopped. If it should not run: cufflink services disable <name>` |
| `inactive`, and the write fails, or the listing does | fails: `the module of <name> was uploaded, and the service could not be kept inactive (<why>). Disable it now: cufflink services disable <name>` |

A deploy without `--inactive` sends neither request.

The CLI never disables a service that was active before the command: it
refused that one before it sent anything.

A **first** deploy with `--inactive` that an older replica takes and that
then **fails** is the one case the CLI cannot mend: that replica created
the service's row active, the deploy has no answer to check, and nothing
was disabled. The service has no deployment, so nothing runs, but the next
`cufflink deploy --inactive` is refused, because the service is active.
Run `cufflink services disable <name>`, then deploy again.

## Service config as code

A service's config and secrets live in `Cufflink.toml`
(`[environments.<env>.config]`, and `[environments.<env>.secrets]` mapping
each key to an entry of the environment's vault). One command brings the
platform to what the file declares:

```sh
cufflink config apply --env staging                  # add and change, as one set
cufflink config apply --env staging --prune          # and remove what the file does not declare
cufflink config apply --env staging --prune --check  # change nothing; exit 2 if anything would
cufflink config apply --env staging --service notes  # by name: nothing is built
```

The CLI sends the whole set to `POST /api/services/{id}/config/apply`:

```json
{ "entries": [{ "key": "API_URL", "value": "https://…", "is_secret": false }],
  "prune": false, "dry_run": false }
```

and the platform applies it in **one transaction**: every key is written or
none is. The answer names keys and never a value:

```json
{ "added": ["NEW"], "changed": ["API_URL"], "removed": [], "unchanged": ["TOKEN"], "applied": true }
```

- A key is `changed` when its value or its `is_secret` differs. A key that
  is `unchanged` is not rewritten and keeps its `updated_at`.
- A key the set does not name is left alone, and not mentioned, unless
  `prune` is set: then it is `removed`. `"entries": []` with `prune`
  removes every key.
- `dry_run` computes the same answer and writes nothing (`"applied": false`).
- A key given twice is a 400; a field the request does not have is a 422;
  a service that does not exist is a 404, so a service's first deploy comes
  before its first apply.
- Two applies of one service wait for each other: the second reads what the
  first wrote. One that waits past the database's statement timeout
  (`DB_STATEMENT_TIMEOUT_MS`) is answered 503 and has done nothing. An
  apply neither waits for a deploy of the service nor is refused for one.
- A stored value that does not open (its sealed form was altered, or its
  data key is gone) is never "unchanged": an apply that names its key
  writes it again, `prune` removes it, and an apply that does not name it
  leaves it. The platform logs `config_value_unreadable` with the key.
- An apply holds the rows it has written until it commits, as
  `cufflink admin secrets reseal` holds those of its batch. Run together,
  the database may end one of them as a deadlock; run it again.
- One audit row is written for an apply (`config.apply`) and one for a dry
  run (`config.check`), each with the four lists of keys. A dry run tells
  its caller whether a value it sends is the stored one, which a deploy
  credential can already overwrite, and read through a module it deploys.

A WASM service reads its config at each `config::get`. A web app and a
native service are given theirs when their process starts, so a running one
keeps what it started with; the CLI says so when it applies a change to
one.

**What the CLI does.** It reads everything it is to send, vault included,
before it asks the platform anything, so a missing vault entry fails with
nothing written.

| Flag | Effect |
|---|---|
| none | adds and changes; prints `Applied config to <name>: 1 added, 0 changed, 0 removed, 4 unchanged`, then a line for each key that differs (`  + KEY`, `  ~ KEY`, `  - KEY`) |
| `--prune` | also removes. It first asks for a dry run and prints `Removing 2 key(s) of <name> that [environments.<env>] does not declare: A, B`; the lines under `Applied config` are what was removed, and include a key set in between. An environment that declares nothing is said to remove every key. Two audit rows are written, the dry run's `config.check` and the `config.apply`. A vault entry no `[secrets]` key maps is an error instead of a warning (also with `--check`: exit 1), since the forgotten mapping would be a removed key |
| `--check` | sends a dry run and changes nothing. With a difference it prints `Config of <name> differs from Cufflink.toml:`, the keys, and `  1 to add, 0 to change, 1 to remove, 3 unchanged` on stdout, one line on stderr, and exits **2**; with none it exits 0. Without `--prune` a key the platform holds and the file does not declare is not a difference, and the line printed says so; with `--prune` it is. A check sends the declared values, so it needs the vault key and a deploy credential |
| `--service <name>` | names the service. Without it the name is a web app's `[service].name`, or else is read from the project's manifest, which builds it. It does not send one project's config to another service: where `Cufflink.toml` has a `[service].name`, a `--service` that names another is refused, with or without `--prune` or `--check`, before the vault is read or anything is sent (`--service <name> is not this project's service: the Cufflink.toml here is of <own>, …`). A `Cufflink.toml` that names no service applies to whichever is named |

Exit 1 stays "it failed" and 3 a plan the platform refused. 2 is also what
the CLI exits with for a command line it cannot parse, which includes a CLI
older than v0.30.0 given `config apply`: pin the CLI's version in CI, or
take 2 as a difference only when stdout holds the `differs from
Cufflink.toml:` line.

A key in both `[config]` and `[secrets]` is sent once, as the secret.
`config apply` needs a platform that advertises `config_apply` and says so
for one that does not.

`cufflink deploy` runs its config step after the deploy, and `cufflink
config sync` does the same step alone. On a platform that advertises
`config_apply` the step is one apply without `prune`, so a key set by hand
(`cufflink config set`) survives every deploy; it prints `  Applied config:
0 added, 1 changed, 0 removed, 2 unchanged` and a line for each key that
differs. On an older platform it is a request for each key, in key order,
and prints `  Synced 2 config(s) and 1 secret(s)` as before; so it is when
a replica that predates the apply answers it with a 405 while a platform is
being upgraded. An environment that declares nothing sends nothing. The
routes for one key (`GET`/`PUT /api/services/{id}/config`, `DELETE
…/config/{key}`) are unchanged.

## Bundles

A bundle is a service built once, as one file a deploy uploads later and
elsewhere: build it in CI at a commit, deploy that file to staging, and
deploy the same file to production.

```sh
cufflink build --bundle out/notes.bundle                  # builds; sends nothing
cufflink deploy --bundle out/notes.bundle --env staging   # uploads; builds nothing
```

`cufflink build --bundle <file> [--commit-hash <sha>]` does what a deploy
does before it sends anything: it emits the manifest, checks its private
tables and that the project's `migrations/` are the ones the manifest
declares, builds the module and folds the features the module declares
into the manifest. The file's directory is made if it is not there, before
anything is built. After the build's own lines it prints, on stdout:

```
Wrote out/notes.bundle (48213 bytes)
  Service: notes (mode: wasm)
  Commit:  4f2a9c1d8e…
  Module:  30602f71c8…
  SHA-256: 9e8d7c6b5a…
```

`SHA-256` is the file's, as `sha256sum out/notes.bundle` gives it; `Module`
is the module's hash, the one `cufflink deployments list` shows. The commit
is `git rev-parse HEAD` unless it is given. A bundle built with neither (no
git checkout, no `--commit-hash`) is written with a warning on stderr: a
deploy of it reports no commit, and `rollback --commit` cannot find it. A
build that succeeds replaces a file that was there; one that fails leaves
no file, and a file that was there as it was.

The file is gzip-compressed JSON:

```json
{ "bundle": 1, "service": "note-service", "mode": "wasm", "manifest": { … },
  "wasm_base64": "…", "wasm_hash": "…", "features": ["…"],
  "commit_hash": "…", "cli_version": "…" }
```

`manifest` is exactly what a deploy from source uploads, so the platform
derives the same `deploy_hash` from either, and a bundle deployed over the
source deploy of the same tree is `skipped`. A service with no user code
has no module: `wasm_base64` and `wasm_hash` are `null`. The gzip header
carries no time and the fields are written in one order: the same manifest,
module and commit give the same file byte for byte from the same CLI
binary, so the file's SHA-256 can name the build. Whether two builds of one
tree give one module is the compiler's business, not the bundle's.

`cufflink deploy --bundle <file>` needs a `Cufflink.toml` (for `--env`, and
for the config step) and no sources. It reads and checks the bundle before
it asks the platform anything, prints which file and commit it deploys:

```
Deploying notes (mode: wasm) from the bundle out/notes.bundle (built by cufflink v0.21.0)...
  Commit:  4f2a9c1d8e…
  SHA-256: 9e8d7c6b5a…
  platform v0.30.0 (features …) ← bundle from cufflink v0.21.0 (needs …)
```

and then follows the ordinary deploy from the feature check on: `--plan`
(exit 3 for a plan the platform refuses), `--inactive`, `--tenant`,
`--allow-destructive`, the check that the platform stored the module the
bundle carries, and the config step all work as they do from source.
Nothing is built, on the skipped path either. The deploy is of the commit
the bundle was built at unless `--commit-hash` gives another, which the
`Commit:` line then says. The config step applies the `[config]` and
`[secrets]` of the `Cufflink.toml` in the working directory to the bundle's
service.

Every refusal of a bundle is `Cannot deploy the bundle <file>: …`, exit 1,
before anything is sent:

| A bundle | Is refused as |
|---|---|
| that is not there | the operating system's error |
| that is truncated, or not gzip | `it is not a whole gzip file (…)` |
| with bytes after its end | `it has bytes after its end` |
| that is some other gzip file | `it is not a bundle (…)` |
| of another format (`"bundle": 2`) | `it is bundle format 2, and this CLI (v…) reads format 1: deploy it with the CLI that built it, or build it again with this one` |
| whose module is not the one `wasm_hash` names | `its module was altered: …`; the platform's own check of the stored module stays behind it |
| whose `service`, `mode` or `features` are not its manifest's | `it says it is …, and its manifest is of …`; `it says it needs the features …, and its manifest requires …` |
| with no module where its manifest needs one, or one where it needs none | `it carries no module, and its manifest needs one (…)`; `it carries a module, and its manifest declares nothing that runs one` |
| whose manifest a deploy from source would refuse | `its manifest is refused: …` |
| deployed where `Cufflink.toml` names another service (`[service].name`, as every web project does) | `it is of notes, and the Cufflink.toml here is of site, whose config the deploy would write to notes. …` |

A bundle whose manifest needs a feature the platform lacks is refused as a
deploy from source is, before upload. `cufflink build --bundle` refuses a
native service, a web app and a container service: `bundles carry WASM
services; deploy <mode> services from source`. `cufflink build` without
`--bundle` is a usage error (exit 2). `cufflink workspace deploy` and
`cufflink deploy-package` take no bundle. A v0.30 CLI deploys a bundle to
an older platform too: the request is the one a deploy from source sends.

## Rolling back to a commit

A rollback activates a new version that runs an earlier version's manifest
and module. That new version carries the commit of the version it was
rolled back to, so `_meta/health.commit_hash` names the code that runs
after a rollback as it does after a deploy. (Before v0.30.0 it was `null`
after every rollback.)

```sh
cufflink rollback --commit 4f2a9c1 --service note-service --env production
```

`--commit <sha>` takes a commit's hash or at least 7 characters of it, in
either case; fewer is a usage error (exit 2), as is `--commit` with
`--version`. The CLI lists the service's retained versions and rolls back
to the newest one, other than the running version, that was deployed at
that commit. It is then the rollback `--version <n>` does. Before it acts
it prints `Rolling back notes from v7 to v3 (commit 4f2a9c1d8e…)...`, and
afterwards `  Source commit:  <hash>`. `--service <name>` names the
service, so the command needs no sources; without it the name is a web
app's `[service].name`, or else is read from the project's manifest, which
builds it.

The label a version carries is the latest commit that produced it: a
deploy that changes nothing is skipped and relabels the running version
with its commit. The commit is compared with that label as the deploy gave
it (`--commit-hash`, or `git rev-parse HEAD`): a version labelled with
exactly what is given is preferred to one whose label only begins with it,
and a version labelled with an abbreviated hash is not found by the full
one.

| The commit | The CLI (exit 1 unless it rolls back) |
|---|---|
| deployed a version that is retained | rolls back to it |
| is the one the running version carries | `<sha> is the running version`. The service already runs that commit; a job that retries a rollback should take this sentence as done |
| deployed nothing new, or its version was relabelled by a later skipped deploy, or pruned | `no retained version was deployed at <sha>`, and that `cufflink deployments list` shows what is retained. Deploy the commit's bundle instead: it produces the same code as a new version |
| is a prefix of two deployed commits, and is neither | `<sha> names more than one deployed commit (…): give more of its hash` |
| names a service that was never deployed | `Service '<name>' not found on platform: deploy it first` |

A rollback the platform blocks for destructive schema changes prints the
command that forces the same one: `cufflink rollback --version <n>
--service <name> [--env <env>] --allow-destructive`.

Against a platform older than v0.30.0 `rollback --commit` works, since the
lookup is the CLI's, but the version it activates reports no commit.

## Tenant databases and PgBouncer

| Variable | Default | Meaning |
|----------|---------|---------|
| `TENANT_DATABASE_URL` | `DATABASE_URL` | Where tenant databases are reached (the database name is replaced per tenant), e.g. a PgBouncer in front of the tenants' Postgres. The platform database always stays direct |
| `TENANT_DB_POOLER` | `direct` | `direct`, or `pgbouncer_transaction` for a PgBouncer in `pool_mode=transaction` |
| `WASM_MAX_OPEN_TX` | `2` | Interactive transactions one guest invocation may hold open |

With `TENANT_DB_POOLER=pgbouncer_transaction` the tenant pools send no
startup options and verify connections with a round trip. PgBouncer and the
database must then provide:

- `max_prepared_statements > 0` (protocol-level prepared statements; 200 is
  the default in current releases);
- `track_extra_parameters = statement_timeout` (or `ignore_startup_parameters`
  covering it);
- a role-level statement timeout for CRUD and registry queries, e.g.
  `ALTER ROLE <tenant role> SET statement_timeout = '30s'`.

Guest statements that need a session of their own (`SET` + DDL through the
legacy host functions) are refused under the pooler; such DDL belongs in
deploy-time migrations.

Under `db_roles: enforce` a service's statements run on logins as its own
role, and those never go through the pooler: the pooler would have to
authenticate every service role, and refuses the `statement_timeout` a
login sends. They reach the server at `DATABASE_URL`'s host, as the
verification's login does, so a binding that verifies is one its sessions
can use. Count them against `max_connections` beside the pooler's: per
platform replica and per worker, `login_pool_max` for each service used.

## Stored secrets

- **A new key-encryption key** reaches every platform, worker and
  web-runner replica as `CUFFLINK_SECRETS_KEK_PREVIOUS` first: a replica
  that does not know a key cannot open a data key wrapped under it. Then
  make it `CUFFLINK_SECRETS_KEK` everywhere, run
  `cufflink admin secrets reseal --operation rotate_kek`, and drop the old
  key from `_PREVIOUS` only once `reseal --operation verify` counts no data
  key under another key and every outbox is drained: the steps are in
  [database roles and secrets](db-roles-and-secrets.md#stored-secrets).
- **`dual` to `encrypted`**: rows written under `dual` hold the plaintext
  beside the sealed value, and keep it after the switch until
  `reseal --operation encrypt` runs under `encrypted`. The platform counts
  them at startup (`cufflink_secret_plaintext_rows{table}`) and logs
  `secret_plaintext_kept`. `dual` is for the rollout alone: while the
  plaintext column exists a sealed value proves nothing about it.
- **`encrypted` back to `dual`**, which a rollback below v0.28.0 needs:
  once every replica is on `dual`, `reseal --operation encrypt` writes each
  plaintext back ([leaving `encrypted`](db-roles-and-secrets.md#stored-secrets)).
- **`rotate_dek`**: a replica follows a new data key within 60 seconds and
  seals under the retired one until then. Retired keys still open what they
  sealed; `reseal --operation encrypt` a minute later seals those values
  under the new key.
- **A row a reseal cannot open** stops the reseal, whose batch is rolled
  back: the answer (409) names the table and the row's id, never its value.
  Earlier batches stay done; run the operation again once the row is mended
  or deleted.

## Children's telemetry

A web runner's apps and an extension runner's native services are its
children. Two settings, read by both runners, decide how they appear in
telemetry. Each defaults to what every earlier release did, and a value
that is neither of its two stops the runner at start, naming the variable.

| Setting | Values | Default |
|---|---|---|
| `CUFFLINK_CHILD_TELEMETRY` | `legacy`, `scoped` | `legacy` |
| `CUFFLINK_CHILD_OUTPUT_LEVEL` | `debug`, `info` | `debug` |

- **`CUFFLINK_CHILD_TELEMETRY=scoped` makes a child report as its
  service.** A child is passed those of `OTEL_SERVICE_NAME`,
  `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL` and
  `OTEL_RESOURCE_ATTRIBUTES` that are set in the runner's environment.
  Under `legacy` it receives them as they are: a child with an
  OpenTelemetry SDK reports under whatever `OTEL_SERVICE_NAME` the
  runner's environment holds, which the runner itself does not read (its
  own name is fixed), and under its SDK's default name when there is
  none. Under
  `scoped` the runner sets `OTEL_SERVICE_NAME` to the service's name and
  puts `service.namespace=<tenant>`, `service.version=v<n>` and
  `cufflink.component=<web-runner|ext-runner>` into
  `OTEL_RESOURCE_ATTRIBUTES`, beside what the runner's own value holds.
  - The service's own config wins, key by key: an `OTEL_SERVICE_NAME` or an
    attribute it sets is kept.
  - What the runner's environment says of the runner (its own
    `OTEL_SERVICE_NAME`, `service.namespace`, `service.version`) never
    reaches a child under `scoped`; its other attributes
    (`deployment.environment.name`, for one) do.
  - An entry of `OTEL_RESOURCE_ATTRIBUTES` that is not `key=value` is
    passed on as it is, and the runner logs a warning.
- **`CUFFLINK_CHILD_OUTPUT_LEVEL=info` logs what children print at `info`**
  instead of `debug`, under the same targets (`web_child`, `ext_child`),
  fields and line budget. It may carry the data of the app's users or the
  service's callers: set it where that may be in the logs.
- **What a child prints is exported under its service.** Each line is
  logged inside a `child_output` span that carries the child's
  [scope](observability.md#the-scope) (tenant, service, version, kind
  `child`), whatever the two settings say. A native service therefore
  needs no OpenTelemetry SDK for its logs to land under its service: with
  `CUFFLINK_CHILD_OUTPUT_LEVEL=info`, the runner exports its stderr once.
  Traces from inside a native child are not produced. A span covers one
  window of the line budget (ten seconds, from the window's first line)
  of one output stream, and is exported when the window ends: a line's
  trace link books to the lines the child printed around it, within
  seconds. A window in which the child printed nothing has no span.
- **Both settings are read once, at start**, without regard to case or
  surrounding spaces: `CUFFLINK_CHILD_TELEMETRY has an invalid value "on":
  'on' is not one of legacy, scoped` is what a runner stops with.
- **A child can reach what its runner's network reaches**, and under
  either value of `CUFFLINK_CHILD_TELEMETRY` it is given the runner's
  `OTEL_EXPORTER_OTLP_ENDPOINT`. Before the platform reads logs by tenant
  (`LOKI_LABEL_MODEL=scoped`) where more than one tenant's children run,
  each tenant's runners need [a collector of their
  own](#a-runners-collector): it is a requirement, set out in
  [observability](observability.md#who-can-write-a-tenants-lines).

## Secrets as files

Every setting that carries a secret can be given as a file instead of a
variable: set `<name>_FILE` to the file's path and leave `<name>` unset.
The platform, the worker, the web runner and the extension runner each
read the file once, as they start.

| Setting | Read by |
|---|---|
| `DATABASE_URL` | platform, worker, shared web runner |
| `TENANT_DATABASE_URL`, `REDIS_URL`, `CUFFLINK_CACHE_SIGNING_KEY`, `CUFFLINK_SERVICE_IDENTITY_KEYS`, `CUFFLINK_DEFAULT_KC_OPS_CLIENT_SECRET` | platform, worker |
| `CUFFLINK_SECRETS_KEK`, `CUFFLINK_SECRETS_KEK_PREVIOUS` | platform, worker, shared web runner |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | platform, worker, shared web runner |
| `NATS_PASSWORD`, `NATS_NKEY_SEED` | every binary |
| `WEB_STATUS_TOKEN` | web runner |
| `EXT_STATUS_TOKEN` | extension runner |

- **The content is the secret**, as UTF-8, with one trailing newline (`\n`
  or `\r\n`) removed; it is then read exactly as the variable's value
  would be.
- **Refused at start**, with a message that names the variable and the
  path and never the content: `<name>` and `<name>_FILE` both set (a blank
  value counts as unset, for either); a file that is missing, unreadable,
  blank (empty, or only whitespace and newlines) or not UTF-8; a
  `<name>_FILE` whose own value is not UTF-8. A missing or blank file is
  never treated as an unset secret. The path is printed as it was given,
  so a secret put in `<name>_FILE` by mistake is shown: put only paths
  there.
- **Each binary checks every name of the table at start**, including
  those it does not read: a bad `<name>_FILE` stops it. So a
  `<name>_FILE` variable that is already in a binary's environment,
  which v0.29 ignored, is read from v0.30.0 on by every binary that
  sees it, and stops the binary when `<name>` is set beside it, or the
  file is unreadable, blank or not UTF-8, even for a secret that binary
  does not use. Before upgrading, look for stray `*_FILE` variables: an
  entrypoint wrapper that does `export X=$(cat $X_FILE)` and leaves
  `X_FILE` set is one.
- **Rotation is picked up at the next start.** A file changed on disk
  changes nothing in a running process: restart the binary after writing
  the new value. For a key-encryption key, follow the order under
  [stored secrets](#stored-secrets).
- **Mount secret files into the platform and the worker only.** A runner's
  children run as the runner's user, so a file the runner can read, they
  can read, whatever the child environment's deny list does. Never give a
  shared-mode web runner `DATABASE_URL_FILE`: use the variable there, or
  run [tenant-scoped web runners](#tenant-scoped-web-runner), which hold
  no database, secrets key or storage credentials at all.
- **A runner's children receive neither form.** Each name above and its
  `_FILE` form is denied to web apps and native services whatever
  `WEB_ENV_PASSTHROUGH` or `EXT_ENV_PASSTHROUGH` lists. `SSL_CERT_FILE` and
  `NATS_CREDS_FILE` name files that are not secrets' file forms and are
  unaffected.
- **The runner key keeps its own semantics.** `CUFFLINK_RUNNER_KEY_FILE` is
  read at each use, so a key rotated in place is taken without a restart,
  and the file need not exist when the runner starts
  ([extension runner](#extension-runner)).
- **`OTEL_EXPORTER_OTLP_HEADERS` has no file form**: the OpenTelemetry SDK
  reads it, not Cufflink.
- **Rolling back below v0.30.0**: give each secret as a plain variable
  again first. An earlier release does not read the files, and would start
  without the secret or refuse to start.

## Web runner

- A web app's process starts with only an allowlist of the runner's
  environment (`PATH`, `HOME`, locale, `TZ`, Node and TLS settings,
  `CUFFLINK_INTERNAL_URL`, `CUFFLINK_BASE_DOMAIN`, `CUFFLINK_PUBLIC_SCHEME`,
  `PLATFORM_URL`, the OTEL service and endpoint settings,
  `NEXT_TELEMETRY_DISABLED`), then its service's config, then the identity
  the runner gives it (`PORT`, `NODE_ENV`, `HOSTNAME`, `CUFFLINK_PUBLIC_URL`,
  `CUFFLINK_TENANT`, `CUFFLINK_SERVICE`). Credentials (every setting of
  [secrets as files](#secrets-as-files) and its `_FILE` form, `DATABASE_*`,
  `NATS_*`, `AWS_*`, `S3_*`, `GCS_*`, `STORAGE_*`,
  `GOOGLE_APPLICATION_CREDENTIALS`, `CUFFLINK_SECRETS_KEY`,
  `CUFFLINK_*_KEY(S)`, `CUFFLINK_RUNNER_KEY_FILE`,
  `OTEL_EXPORTER_OTLP_HEADERS`) never pass, nor does a variable whose name
  or value is not UTF-8.
- The runner makes itself undumpable on Linux as it starts, so an app,
  which runs as the runner's user, cannot read the runner's credentials
  from its environment (`/proc/<pid>/environ`) or memory.
- `WEB_ENV_PASSTHROUGH="NAME,PREFIX_*"` lets more variables through
  (never a credential). `*`, which passed everything for one release, is
  refused at startup with the built-in list.
- **Rollout gate:** on staging, with the default allowlist, confirm each web
  app's server routes (authentication, uploads, platform URL resolution)
  before promoting.
- Apps listen on `127.0.0.1`, on ports from `WEB_PORT_RANGE` (default
  `3200-3999`, which must not contain `RUNNER_PORT`). Apps used to take the
  ports from `RUNNER_PORT + 100` up, so an install with a non-default
  `RUNNER_PORT` sees its apps on different ports.
- `WEB_ARTIFACT_MAX_ENTRIES` (200 000) and `WEB_ARTIFACT_MAX_BYTES` (2 GiB)
  bound what an artifact extracts to, counting the bytes each entry writes;
  an artifact larger than `WEB_ARTIFACT_MAX_BYTES` is not downloaded.
  Entries and links leaving the app directory are refused, including a link
  that leaves through another link.
- An app that exits on its own is started again by the next request
  (`cufflink_web_runner_child_exits_total{reason}`); one that exits within 10 s
  of starting three times in a row is left stopped until its next deploy. A
  failed cold start waits 30 s before the next attempt, even when every
  request that asked for it has gone; a host with no active deployment
  answers 404 for 10 s without asking the database again.
- **Bundles.** Each bundle is unpacked once, under
  `APPS_DIR/{tenant}/{service}/{service id}-{sha256}`, renamed into place
  only once it was checked against that hash, and never replaced there, so
  a directory of that name holds that service's bundle of that content and
  is used again without checking it. A bundle uploaded before bundles were
  hashed is downloaded at each start and named by the hash of what
  arrived. A deploy of a bundle the app already runs changes nothing; a
  bundle of other content uploaded again under the same version runs in
  its place.
  The app's configs are read before anything is downloaded, and the process
  it replaces keeps its own directory, so a deploy that fails at any step
  leaves the running app as it was. A directory no process runs from is
  removed once nothing is installing or starting from it, 5 s after its
  process was replaced, with the directories of earlier releases.
- **Deploy events** only say which app changed: the runner reads what runs
  of it, its bundle, hash and service, from the database (or the runner API,
  [tenant-scoped](#tenant-scoped-web-runner)), never from the event, so no
  publisher on `deploy.web.>` can pair a bundle with another service's
  configs.
- An app's stdout and stderr are logged at `debug` under the target
  `web_child` (they may carry the app's users' data): lines cut at 8 KiB, at
  most 100 lines per stream per 10 s, the rest counted in a summary line.
  Enable them with `RUST_LOG=web_child=debug` when debugging an app, or
  set `CUFFLINK_CHILD_OUTPUT_LEVEL=info` to log them at `info` always
  ([children's telemetry](#childrens-telemetry)).
- The runner's own routes live under `/_cufflink/`, so they never hide an
  app's paths: `/_cufflink/ready` answers 503 until the deploy listener is
  subscribed, while a tenant-scoped runner's key is not accepted, and once
  the runner is shutting down; `/_cufflink/status` and
  `/_cufflink/metrics` need `Authorization: Bearer $WEB_STATUS_TOKEN`
  and are not served without the token. `/health` stays where it was.
  Every other path, `/ready`, `/status` and `/metrics` included, reaches the
  app.

### Tenant-scoped web runner

By default one web runner serves every tenant's apps, reading them with
the platform's database, secrets key and storage credentials, and every app
runs as its user beside the others. An install that hosts more than one
tenant's web apps runs **one web runner per tenant** instead, each holding
only what reaches its tenant:

| Variable | Default | Meaning |
|----------|---------|---------|
| `WEB_TENANT` | — | The slug of the one tenant whose apps this runner serves. Set, the runner needs `PLATFORM_URL` and a runner key, and reads no `DATABASE_URL`, `CUFFLINK_SECRETS_*` or storage settings |
| `PLATFORM_URL` | — | The platform's URL the runner API is reached at (its internal address), `http` or `https`; without `WEB_TENANT` only a value apps are passed |
| `CUFFLINK_RUNNER_KEY` / `CUFFLINK_RUNNER_KEY_FILE` | — | The tenant's web runner key (`cufflink tenants runner-keys create <tenant> --for web`), or a file holding it, read at each use; a key without `WEB_TENANT` is refused at start. An app can read a file the runner can (it runs as the runner's user), which gives it its own tenant's other apps' configs, never its native services': prefer the variable |
| `WEB_POLL_SECS` | `30` | How often the runner asks the runner API whether its key is still accepted |

Such a runner reads its tenant's apps, each bundle (by its SHA-256, checked
again as it arrives) and each app's configs from the runner API
(`/api/runner/web`); a bundle uploaded before bundles were hashed cannot be
served that way and must be uploaded again. It answers 404 for a host of
another tenant, subscribes only `deploy.web.<tenant>.>` and
`undeploy.web.<tenant>.>`, refuses an event that names another tenant, and
takes a deploy event only as a sign to read that app's deployment from the
runner API, never its bundle, hash or service from the event.
`/_cufflink/ready` answers 503 until the runner API has accepted the key.
When it refuses the key (401: revoked, or the tenant deleted), or answers
for another tenant than `WEB_TENANT`, the runner stops every app of the
tenant, removes what they ran from, answers their hosts 503 and stays
unready, asking again every `WEB_POLL_SECS`; any other failure keeps the
apps running. A key minted `--for native` finds nothing here (404). Rotate
a key as for the extension runner (below): the new key first, the old one
revoked once the runner uses the new one. A request the runner made with
the old key just before it was revoked can still be refused: the runner
then stops its apps and serves again at its next poll with the new key,
within one `WEB_POLL_SECS`.

**Network.** Its apps run with the runner's network, so give the runner a
network that reaches `PLATFORM_URL` and NATS alone: no route to Postgres,
Redis, object storage or the monitoring stack, and NATS only with the
runner's own user (below). A runner that exports telemetry also reaches
a collector's OTLP port ([a runner's collector](#a-runners-collector)). The extension runner's
[network policy](#extension-runner) fits it too, with the web runner's
labels.

Its NATS user subscribes to `deploy.web.<tenant>.>` and
`undeploy.web.<tenant>.>` and publishes nothing:

```
{ user: web-runner-acme, permissions: {
    subscribe: ["deploy.web.acme.>", "undeploy.web.acme.>"],
    publish: { deny: [">"] } } }
```

## Extension runner

`cufflink-ext-runner` runs one tenant's native services. Every replica runs
one child of each active native service's current deployment of its tenant
(`EXT_TENANT`) and joins the queue group of that service's calls, so the
calls spread over the replicas and a replica that stops leaves the group
before its children do. It reads what should run, each binary and each
child's configs and secrets from the platform's runner API with the
tenant's runner key, and needs NATS; it holds no database, secrets or
storage credentials, and writes nothing to the platform.

**One runner per tenant.** A child is not sandboxed: it runs as the
runner's user, with the runner's network and cloud identity (a GKE workload
identity, an instance role), shared by every native service on that runner.
So each tenant has runners of its own, and everything a runner holds must
be that tenant's alone:

- its **runner key**, bound to the tenant and minted for the extension
  runner (`--for native`), accepted by that half of the runner API alone,
  which answers only that tenant's native services, binaries and
  environments (404 for anything else, a web app's included) and is
  refused by every other route;
- its **NATS user**, allowed only that tenant's subjects (below);
- its **cloud identity**, granting only what that tenant's native services
  need;
- its own `APPS_DIR` and `EXT_SOCKET_DIR`;
- a **network** that reaches `PLATFORM_URL` and NATS alone: no route to
  Postgres, Redis, object storage or the monitoring stack, and NATS only
  with the runner's own user. A runner that exports telemetry also
  reaches a collector's OTLP port, and nothing else of the monitoring
  stack ([below](#a-runners-collector)).

A native service can then reach its own tenant's other native services (a
child can read a sibling's environment in the moment before the sibling's
`main` makes it undumpable, and shares the runner's identity), which is
accepted: they are the tenant's own. It reaches nothing of another tenant's.
A runner whose key the runner API answers for another tenant than
`EXT_TENANT` treats it as a refused key: it stops every child, leaves their
call subjects and stays unready.

**Network.** A native service can connect wherever its runner's network
lets it, so the runner's network, not the platform, is what keeps it from
the platform's database, Redis and storage. In Kubernetes, a
NetworkPolicy per runner allows only the platform's API port, NATS's client
port and DNS (add the external hosts the tenant's services call, as
`ipBlock`s that exclude the cluster's private ranges):

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: cufflink-ext-runner-acme
spec:
  podSelector:
    matchLabels: {app: cufflink-ext-runner, cufflink.dev/tenant: acme}
  policyTypes: [Egress]
  egress:
    - to: [{podSelector: {matchLabels: {app: cufflink-platform}}}]
      ports: [{protocol: TCP, port: 8080}]
    - to: [{podSelector: {matchLabels: {app: nats}}}]
      ports: [{protocol: TCP, port: 4222}]
    - to:
        - namespaceSelector: {}
          podSelector: {matchLabels: {k8s-app: kube-dns}}
      ports: [{protocol: UDP, port: 53}, {protocol: TCP, port: 53}]
```

The repository's `docker-compose.yml` and `cufflink dev up` put the
runner on a network of its own with the platform and NATS alone.

**Runner keys.** A platform admin mints a tenant's runner key, lists its
keys (never their secrets) and revokes one, each change audited:

```bash
export CUFFLINK_PLATFORM_API_KEY=<platform admin key>
cufflink tenants runner-keys create acme --for native   # the key is shown once
cufflink tenants runner-keys list acme
cufflink tenants runner-keys revoke acme <id>
```

(`POST`/`GET /api/tenants/{slug}/runner-keys`, `DELETE
/api/tenants/{slug}/runner-keys/{id}`.) A key is minted for one kind of
runner, `--for native` (this runner) or `--for web` (a
[tenant-scoped web runner](#tenant-scoped-web-runner)), and the runner API
answers a key only on its own kind's routes, 404 on the other's. To rotate
one, create its successor, point the runner at it (restart it with the new
`CUFFLINK_RUNNER_KEY`, or write the new key to a file beside the one
`CUFFLINK_RUNNER_KEY_FILE` names and rename it over that one, so the
runner, which reads the file at each use, never reads half a key; a
Kubernetes secret mounted as a file is replaced that way), then revoke the
old key, in that order: the runner API refuses a revoked key from its next
request, and a runner whose key is refused (401) stops every child, leaves
their call subjects and stays unready until a key the platform accepts is
in place, since a revoked key, or a deleted tenant, is final and a new
tenant may later take the same slug.

| Variable | Default | Meaning |
|----------|---------|---------|
| `EXT_TENANT` | required | The slug of the one tenant whose native services this runner runs |
| `PLATFORM_URL` | required | The platform's URL the runner API is reached at (its internal address), `http` or `https` |
| `CUFFLINK_RUNNER_KEY` / `CUFFLINK_RUNNER_KEY_FILE` | required, one of them | The tenant's runner key minted `--for native`, or a file holding it (read at each use, so a key written later or rotated in place is taken). Prefer the variable: a file the runner can read, its children can read too (they run as its user), which gives a native service its own tenant's runner data |
| `NATS_URL`, `NATS_CREDS_FILE` / `NATS_NKEY_SEED` / `NATS_USER` + `NATS_PASSWORD` | `nats://localhost:4222` | The NATS the platform uses, and the runner's credentials: the tenant's runner user |
| `RUNNER_PORT` | `3200` | Port of `/health` and the `/_cufflink/` routes |
| `APPS_DIR` | `./apps` | Where each deployment's binary is kept, as `{tenant}/{service}/{sha256}/service` |
| `EXT_SOCKET_DIR` | `/tmp/cufflink-ext` | Where the children's sockets are; the runner refuses to start when the longest socket path in it would pass the operating system's limit (103 bytes on macOS, 107 on Linux: keep it under about 50) |
| `EXT_ENV_PASSTHROUGH` | — | More of the runner's variables to pass to the children, as `NAME,PREFIX_*` (never a credential) |
| `CUFFLINK_CHILD_TELEMETRY`, `CUFFLINK_CHILD_OUTPUT_LEVEL` | `legacy`, `debug` | How children are named to OpenTelemetry and their output logged ([children's telemetry](#childrens-telemetry)) |
| `EXT_ARTIFACT_MAX_BYTES` | `134217728` | The largest binary downloaded, in bytes (128 MiB) |
| `EXT_RECONCILE_SECS` | `30` | How often the children are converged without an event |
| `EXT_READY_TIMEOUT_SECS` | `30` | How long a new child has to answer ready |
| `EXT_HEALTH_INTERVAL_SECS` | `10` | How often a running child is probed |
| `EXT_CHILD_STOP_TIMEOUT_SECS` | `10` | Between SIGTERM and SIGKILL of a child that is stopped |
| `EXT_MAX_CONCURRENT_CALLS` | `64` | Calls relayed to one child at once; the others NATS delivered to this runner wait in its client's buffer for a slot (**Concurrency** below) |
| `SHUTDOWN_GRACE_SECS` | `45` | From the signal to exit |
| `EXT_STATUS_TOKEN` | — | Bearer token of `/_cufflink/status` and `/_cufflink/metrics`; without it neither is served |

`APPS_DIR` and `EXT_SOCKET_DIR` must be writable by the runner's user (it
refuses to start otherwise); neither needs to survive a restart, since a
missing binary is downloaded again. `APPS_DIR` holds each running binary
and, during a replacement, the one it replaces.

**NATS permissions.** A tenant's runner user subscribes to exactly three
patterns: `cufflink.ext.<tenant>.>` (in the queue group `cufflink-ext`; the
runner subscribes each of its services' extension subjects there),
`deploy.ext.<tenant>.>` and `undeploy.ext.<tenant>.>`. It publishes nothing
but the reply to each call it takes: grant it `allow_responses` and no
publish permission of its own, so the server lets it answer exactly the
requests it received (where response permissions are not available, allow
publishing on the reply inboxes of the platform and the workers,
`_INBOX.>`, and nothing else). It must never be allowed to publish on
`cufflink.ext.>`, nor to subscribe to another tenant's subjects. For
example:

```
{ user: ext-runner-acme, permissions: {
    subscribe: ["cufflink.ext.acme.>", "deploy.ext.acme.>", "undeploy.ext.acme.>"],
    allow_responses: true } }
```

The runner's NATS user, cloud identity and runner key are its tenant's
alone. When a tenant is deleted, stop its runners and remove their NATS
user and cloud identity before its slug is given to a new tenant: its
runners stop their children once the platform refuses their keys, but
their NATS user would still match the new tenant's
`cufflink.ext.<tenant>.>`.

The children hold no NATS credentials.

- **Converging.** The runner compares what should run (every active service
  of its tenant whose current deployment is `mode: native`, as `GET
  /api/runner/native` answers) with its children, and starts
  what is missing, replaces a child whose deployment changed (a new binary,
  a new version or other extensions) and stops what should no longer run. It
  does so at start, every `EXT_RECONCILE_SECS`, at once on a `deploy.ext` or
  `undeploy.ext` event (a burst of them counts once), and when a restart's
  backoff ends; events only bring the next pass forward, so a missed one is
  caught by the timer. While the runner API cannot be read (the platform
  unreachable or failing, the key file unreadable), the children keep
  running as they are and the next pass tries again; while it refuses the
  runner key (401) or answers for another tenant than `EXT_TENANT`, the
  pass runs nothing: every child is stopped and leaves its call subjects
  (the runner keeps its `deploy.ext` and `undeploy.ext` subscriptions), the
  refusal is logged once, and the runner answers `/_cufflink/ready` 503
  until a pass reads what should run with a key the platform accepts. A
  deployment that records no binary or
  unreadable `provides` is logged and its running child left alone.
- **Binaries.** A binary is downloaded once from the runner API, checked
  against its SHA-256 and the size cap (by its recorded size, its answered
  length and what arrives), and renamed into place only once complete, so a directory
  named by a hash always holds that content and is reused without hashing it
  again. Directories no child runs from (a replaced child still answering
  its calls included) and no deployment needs are removed after each pass.
- **A child's environment** is the allowlist every runner shares (`PATH`,
  `HOME`, locale, `TZ`, TLS settings, `CUFFLINK_INTERNAL_URL`,
  `PLATFORM_URL`, the OTEL service and endpoint settings) and
  `EXT_ENV_PASSTHROUGH`, then its service's configs and secrets (read from
  the runner API at each start), then `CUFFLINK_EXT_SOCKET`,
  `CUFFLINK_EXT_TOKEN`, `CUFFLINK_TENANT` and `CUFFLINK_SERVICE`. The
  runner's own credentials (its runner key, NATS, `EXT_STATUS_TOKEN`)
  never pass, and the runner
  makes itself undumpable on Linux, so a child cannot read them from its
  environment or memory either; a child holds only its own service's config
  and whatever the runner's cloud identity grants. A child whose config
  cannot be read is not started until it can.
- **Children apart.** Each start of a child gets 32 random bytes as its
  token (`CUFFLINK_EXT_TOKEN`), which the runner sends as a bearer with
  every `/ready` and `/call`; `cufflink-native` answers anything else 401,
  and makes the child undumpable on Linux as its `main` starts, so from
  then on another child cannot read its token or its tenant's secrets, nor
  call it as the runner. Between the child's exec and that call its
  environment is still readable by its siblings through `/proc`, which
  are its own tenant's.
- **Ready.** A new child is polled with `GET /ready` on its socket until it
  answers or `EXT_READY_TIMEOUT_SECS` passes. It must speak the runner's
  protocol and serve exactly the extensions its manifest provides; a binary
  that serves others is refused (killed, logged with the difference) and not
  started again until the service's next deploy, while the child it was to
  replace keeps serving.
- **Calls.** Only a ready child is subscribed to its calls. Each call is
  relayed over a new connection to its socket, and the child's reply sent
  back; when the child cannot be reached, answers no reply or answers more
  than NATS carries, the call fails with the code `unavailable` or
  `too_large`, and a malformed call, or one whose body names another tenant
  or extension than its subject, with `invalid`. Nothing is answered once
  the caller's deadline has passed: its own timeout decides the call. A
  replacement keeps the service's subscriptions and only sends the calls
  that follow to the new child, then stops the old child once the calls it
  took are answered, so no call fails during a deploy; only the extensions
  the new version no longer provides leave the queue group. The old child
  drains and stops in the background: a slow call to one service holds up
  nothing the runner does for another.
- **Concurrency.** A child is relayed at most `EXT_MAX_CONCURRENT_CALLS`
  calls at once, shared by its extensions; a call beyond them waits in the
  runner's NATS client buffer (65536 messages per subscription) until a
  slot frees, and is never handed to another replica; one whose deadline
  has passed by then is not answered. Past that buffer the client drops
  calls as a slow consumer's, and their callers time out.
- **Crashes and hangs.** A child that exits is started again after 1 s,
  doubling with each failure in a row up to 30 s (each ±20%); a child that
  ran for a minute starts the count afresh. Three failures in a row within
  10 s of starting (never answering ready counts) leave the service stopped
  (`crash_looping`) until its next deploy. A child that fails three health
  probes in a row is killed and follows the same path.
- **Logs.** A child's stdout and stderr are logged at `debug` under the
  target `ext_child` (they may carry its callers' data), within the same
  budget as a web app's: `RUST_LOG=ext_child=debug`, or
  `CUFFLINK_CHILD_OUTPUT_LEVEL=info` ([children's telemetry](#childrens-telemetry)).
- **Routes.** `/health` answers while the listener is open;
  `/_cufflink/ready` answers 200 once a pass has read what should run and
  NATS is connected, and 503 once shutdown starts. `/_cufflink/status`
  (each child's tenant, service, version, binary hash prefix, state,
  extensions and pid) and `/_cufflink/metrics` need
  `Authorization: Bearer $EXT_STATUS_TOKEN`.
- **Shutdown.** On SIGTERM the runner stops converging, unsubscribes from
  every call subject while answering the calls it took (those still waiting
  for a slot after 10 s are answered `unavailable`), then stops its children
  with `EXT_CHILD_STOP_TIMEOUT_SECS` each, and the children a replacement
  retired once their calls are answered. Unsubscribing is not lossless in
  the NATS client (async-nats forgets the subscription as soon as it has
  sent the unsubscribe, before the server takes it in): a call the server
  routed to the runner just then is dropped, and its caller waits out its
  timeout. The same goes for a service removed or switched off, a version
  that provides fewer extensions, and a runner that crashed. Replacing a
  child loses nothing: the subscriptions stay and only their target
  changes.

### A runner's collector

A collector only receives, so nothing can be read through it. But it
believes what it receives, and what a runner's network reaches, the code
the runner runs reaches too, whatever its environment says: a tenant's
native service or web app can post spans and log lines to the collector
under any tenant's name. Those lines are then returned by `cufflink logs`
to the tenant they name, under either `LOKI_LABEL_MODEL`, and shown on its
dashboards.

Where more than one tenant's code runs, do not put the shared collector on
a runner's network. Before `LOKI_LABEL_MODEL=scoped` is set there, each
tenant's runners must have a collector of their own that sets the tenant
on everything it forwards, and only that one may reach the shared
collector. `infra/monitoring/otel-collector-tenant.yaml` is that
collector's configuration, used as it is:

- Start it with `CUFFLINK_TENANT=<slug>` in its environment, on the
  tenant's runner network, and point the tenant's runners at it
  (`OTEL_EXPORTER_OTLP_ENDPOINT=http://<its host>:4318`). It forwards to
  `http://otel-collector:4318`, the shared collector as the compose stack
  names it. Started without `CUFFLINK_TENANT`, it forwards nothing.
- Whatever it receives leaves with `cufflink.tenant` and
  `service.namespace` set to its tenant on the resource, in place of
  every value the sender gave under those names, and with both names
  removed from scopes, log records, spans and span events. A name given
  twice in one attribute list, or given on a span whose resource has
  none, pins nothing on another tenant.
- Nothing leaves it as `cufflink-platform` or `cufflink-worker`: a
  resource's `service.name` of that value, or a `cufflink.service` of
  that value (which the shared collector would make the `service.name`),
  is forwarded as `runner-cufflink-platform` or `runner-cufflink-worker`.
  The lines are kept, under the tenant, where their sender can be found;
  what a runner sends for a tenant's service of either name is stored
  under the prefixed one.
- So the runner's own lines and spans are the tenant's too: they are
  stored under `service_namespace="<slug>"`,
  `service_name="cufflink-ext-runner"` (or `cufflink-web-runner`), beside
  the tenant's services, and no longer among the lines outside any
  service on the Platform dashboard.
- This holds under `LOKI_LABEL_MODEL=legacy` as well: the `legacy` query
  reads the stream `service_name="cufflink-platform"`, which nothing
  forwarded by this collector is in.

The CI job `observability` posts forged spans and log lines through it and
finds each under its tenant and under no other
(`platform/tests/observability.rs`). `docker-compose.yml` declares it as
`otel-collector-tenant`, under the profile `tenant-collector`. The compose
stack itself is one developer's: its ext-runner shares the one collector
with the platform, and the native code it runs can post telemetry under
any tenant's name.

What this collector does not do:

- It stands between a runner's children and the shared collector, not
  between guests and it. A guest's `http::*` call leaves from the platform
  or the worker, and reaches an internal address until the tenant's
  `egress` policy is `enforce` ([policies](policies.md)); enforce it, name
  no monitoring host in `allow_hosts`, and keep the collector and the
  stores on private addresses or behind authentication, since `enforce`
  refuses only internal addresses
  ([who can write a tenant's lines](observability.md#who-can-write-a-tenants-lines)).
- It stands before the shared collector alone. A child that can reach
  Loki, Tempo or Prometheus writes to them directly, past it.
