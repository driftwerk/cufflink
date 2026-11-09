# Web app lifecycle: releases by content, rollbacks that switch, drained swaps

Proposed. Covers issues #62 (a web rollback that switches), #63
(transactional web deploys, versions by content) and #64 (drain the
replaced process). Section 13 holds the questions for the maintainer, each
with a proposed default.

The examples use the tenant `acme` and its web app `site`, served at
`site.acme.example.com`.

## Outcome

- A web app's version exists only once its bundle is stored. A build that
  fails, an upload that fails or a deploy the platform refuses leaves the
  running version, its row and its bundle exactly as they were.
- A bundle is stored under its SHA-256 and never rewritten. Deploying new
  content always creates a new version, whatever `package.json`'s
  `version` says. Deploying the same content and manifest again is skipped.
  No upload can replace the bundle a running or retained version records.
- `cufflink rollback` of a web app activates a version that records the
  target's bundle and tells the runners. The earlier code serves within
  seconds, and within one `WEB_RECONCILE_SECS` even when a runner missed
  the event.
- On a swap, the replaced process gets no new request. Its requests in
  flight may finish within a configurable grace. Only then is it sent
  SIGTERM, and SIGKILL after `WEB_CHILD_STOP_TIMEOUT_SECS`.
- WASM, CRUD, container and native services see no change.

## 1. What the platform does today

Read from `platform/src`, `cli/src`, `web-runner/src` and `supervisor/src`
on `main` at v0.37.1 (`4a2d341`).

### 1.1 How a web app is deployed

`cufflink deploy` in a web project (`cli/src/commands/deploy.rs:1426-1570`)
works in three steps, and only the first one is a deploy:

1. **Register.** It builds a manifest of `{name, mode: "web", version,
   tables: []}`, taking `version` from `package.json` (`"0.1.0"` when that
   has none; `deploy.rs:1450-1456`). It sends that manifest to `POST
   /api/services/deploy` with no code (`deploy.rs:1458-1479`). The
   platform activates a new version with no bundle: `activate_in_runner`
   writes the row with `wasm_hash: None` and no artifact, and moves
   `services.current_version` to it (`platform/src/deployer/pipeline.rs:215`,
   `232-261`).
2. **Build.** It syncs the project's config to the service, which needs
   the `service_id` that step 1 returned (`deploy.rs:1497`). It fetches the
   service's configs, runs `pnpm|yarn|npm run build` with them
   (`deploy.rs:1499-1532`) and tars `.next`, `node_modules`, `package.json`
   and `public` (`deploy.rs:1534-1541`, `1644-1682`).
3. **Upload.** It posts the tarball to `POST /api/services/{id}/web`
   (`deploy.rs:1543-1566`). The platform:
   - stores it at `web/{service_id}/v{current_version}.tar.gz`
     (`platform/src/api/web.rs:15-17`, `46-51`);
   - writes its key, hash and size onto the row of **whatever version is
     current at that moment** (`web.rs:53-66`);
   - publishes `deploy.web.{tenant}.{service}` (`web.rs:68-86`).

### 1.2 The same `package.json` version overwrites the running bundle

The web code hash is `wasm_hash` (`pipeline.rs:121-132`), which is `None`
for every web deploy. So `should_skip_deploy` (`pipeline.rs:134-141`)
compares only the manifest hash. A second deploy with an unchanged
`package.json` version is therefore `Skipped` (`pipeline.rs:161-205`):
the current version stays current. Step 3 then uploads to that
version's key, `v{current_version}`. That replaces, in place, the object
and the row of the version that is running. Two consequences:

- the running version's recorded bundle is no longer the one it was
  released with, so there is no version to roll back to;
- a runner that restarts between the upload and the event downloads the
  new bytes for the old version number.

### 1.3 A failed build leaves a version with no bundle

When `package.json`'s version changed, step 1 activated a new version.
If step 2 or 3 then fails, that version is current and records no
bundle. Every reader of "the current web deployment" requires a bundle:

- the shared runner, `web-runner/src/source.rs:220-248`
  (`artifact_s3_key IS NOT NULL ... AND d.version = s.current_version`);
- the runner API for tenant-scoped runners,
  `platform/src/repo/runner_services.rs:27-32`.

A running process keeps serving until its runner restarts or the warmer
next starts it. After that, the app answers 404 (`No active deployment`)
until a deploy succeeds.

### 1.4 A rollback of a web app switches nothing

`rollback_locked` treats `Web` like CRUD, WASM and container services
(`pipeline.rs:457-509`):

- It runs `eventing::prepare`, `snapshot` and `schema::apply_rollback`
  over an empty schema.
- It restores a module only when the target has a `wasm_hash`
  (`pipeline.rs:476-480`), which a web target never has.
- It activates the new version with no artifact. `ActivateVersion` can
  carry an artifact only through `native: Option<NativeVersion>`
  (`platform/src/repo/deployments.rs:45-60`, `146`, `173-175`), and that
  field also runs `replace_native` (`deployments.rs:179-188`).

`after_rollback` then runs `eventing::after_rollback` for web too
(`pipeline.rs:541-549`) and publishes no `deploy.web`. The result is the
same hole as section 1.3: a current version with no bundle, and no runner
told.

### 1.5 How a runner learns of a change

- **Events are triggers.** `handle_deploy_events`
  (`web-runner/src/main.rs:256-282`) takes `deploy.web.{tenant}.>` on core
  NATS and re-reads the app's current deployment from the database or the
  runner API (`source.rs:167-183`). It never acts on the event's fields.
  The platform's publish is fire-and-forget (`web.rs:77-86`): a runner
  that is disconnected at that moment never hears it.
- **There is no reconcile.** `watch_key` (`web-runner/src/apps.rs:314-341`)
  polls the runner API every `WEB_POLL_SECS`, but only to learn whether
  the key is accepted. It discards the answer. A shared runner polls
  nothing. A missed event therefore leaves the old process serving until
  the runner restarts.
- **Events are handled one at a time.** The loop awaits `apps.deploy` for
  each event before reading the next.
- **The warmer caches outcomes.** A cold start that found no deployment
  answers 404 for 10 s, and a failed cold start waits 30 s
  (`web-runner/src/warmer.rs:13-14`). A deploy event does not clear
  either.
- **The runner reuses content.** It unpacks a bundle into
  `APPS_DIR/{tenant}/{service}/{service_id}-{sha256}` and treats a
  deployment whose hash it already runs as `AlreadyRunning`
  (`apps.rs:85-88`, `137-155`).

### 1.6 The swap

- `Supervisor::start` admits the new child into the table under its lock
  and returns the child it replaced (`supervisor/src/supervisor.rs:132-172`).
  From that moment `endpoint()` answers the new port
  (`supervisor.rs:176-181`).
- The proxy looks the port up once per request (`proxy.rs:73-102`) and
  forwards to it (`proxy.rs:134-143`). New requests therefore already go to
  the new process once the swap is admitted.
- A request that read the old port just before the swap still connects to
  the old one, and requests already forwarded are still being answered
  there.
- `ProcessManager::deploy` then stops the replaced child with `Stop::Kill`
  at once (`web-runner/src/process_manager.rs:240-252`). Every request in
  flight on it is cut, mid-body included.
- `Apps::drain` keeps the replaced directory for `OLD_VERSION_KEPT` (5 s)
  and then prunes it (`apps.rs:18`, `198-206`). This hold was written for
  installs in progress, not for a process still serving.
- `stop` (undeploy) also kills at once (`process_manager.rs:324-340`).
  `shutdown_all` terminates with `WEB_CHILD_STOP_TIMEOUT_SECS`
  (`process_manager.rs:342-357`), but only for children still in the
  table.

### 1.7 What happens to stored bundles

- `delete_by_version` returns a key only for native rows
  (`deployments.rs:339-354`), so `delete_version`
  (`platform/src/api/deployments.rs:600-624`) never removes a web bundle.
  Pruning a web app's versions leaves its objects.
- Deleting a web app removes the keys its rows record
  (`platform/src/api/services.rs:250-254`, `296-303`). Deleting a tenant
  does the same (`platform/src/api/tenants.rs:369-378`).
- The native path already does what web lacks:
  - `object_key(service_id, hash)`;
  - store with head-before-put (`platform/src/deployer/native.rs:166-189`);
  - activate in the same transaction that records the artifact;
  - `restore` on rollback after a head check (`native.rs:191-206`);
  - `remove_orphan` guarded by `records_artifact` (`native.rs:208-234`).

  This design gives web the same shape.

## 2. Design

### 2.1 Bundles by content

A web bundle is stored at:

```
web/{service_id}/{sha256}.tar.gz
```

`api::web::artifact_key(service_id, version)` is replaced by
`deployer::web::object_key(service_id, sha256)`.

- A key names one content forever. Rewriting it writes the same bytes, so
  no upload can change what any version records.
- Keys recorded before this release (`v{n}.tar.gz`) stay valid. Every
  reader takes the key from the row and never builds it.
- The `web/` prefix is already reserved from guests in every policy mode
  (`platform/src/wasm_runtime/storage_scope.rs:10`). Only the platform
  writes there, so an object at a content key holds that content.

### 2.2 A release is one deploy that names its bundle

The CLI's steps become **reserve, build, upload, release**. Only the
last one changes what runs.

1. **Reserve** (`POST /api/services/web`, section 4.1). This runs
   `services::find_or_create` with the same name, schema and status rules
   a deploy's `claim` uses (`platform/src/api/deployments.rs:123-140`), and
   creates no version.
   - A first deploy gets its `service_id` here, so config sync and the
     build's configs keep working as in section 1.1.
   - A service row with `current_version = 0` already exists today
     whenever a first deploy fails after its claim. Every reader already
     treats it as "nothing to run".
2. **Build.** Unchanged.
3. **Upload** (`PUT /api/services/{id}/web/artifacts/{sha256}`, section
   4.2). It stores the bundle by content and records it as an upload of
   that service (section 3). It changes no deployment and publishes
   nothing.
4. **Release** (`POST /api/services/deploy` with `web_artifact: {sha256,
   size}`, section 4.3). Under the service's deploy lock, the platform:
   - finds the upload row and heads its object;
   - compares the recorded size and the stored object's size with
     `size`;
   - skips or activates (below);
   - records the bundle in the same transaction that moves
     `current_version`.

**The skip rule.** For `Web`, the code hash is the bundle's SHA-256:

```rust
fn code_hash<'a>(mode: &ServiceMode, code: Code<'a>) -> Option<&'a str> {
    match mode {
        ServiceMode::Native | ServiceMode::Web => code.artifact,
        ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container => code.wasm,
    }
}
```

Here `Code { wasm: Option<&str>, artifact: Option<&str> }` replaces the
two positional hashes. `DeployContext::code_hash` passes the native
binary's hash or the web bundle's sha256 as `artifact`. The running side
already reads `latest.artifact.hash` for native (`pipeline.rs:162-169`),
and from now on for web too.

- Same manifest and same bundle: `Skipped`. As today, the commit label
  moves.
- Anything else: a new version.
- A changed `package.json` version with the same bundle is still a new
  version. The manifest changed, and `version` stays in the manifest as a
  label.
- Next.js writes a new build id into each build, so in practice a skip
  happens only when the same bytes are sent again, for example a retried
  release step.

**Activation.** `activate_in_runner` (`pipeline.rs:232-261`) takes the
version's artifact (section 2.3). For web it is the claimed upload. No
schema, eventing or role work runs for web, as today.

### 2.3 `ActivateVersion` carries an artifact of either kind

```rust
pub enum VersionArtifact<'a> {
    Native(NativeVersion<'a>),
    Web(&'a Artifact),
}

impl VersionArtifact<'_> {
    fn artifact(&self) -> &Artifact {
        match self {
            VersionArtifact::Native(native) => native.artifact,
            VersionArtifact::Web(artifact) => artifact,
        }
    }
}

pub struct ActivateVersion<'a> {
    // every field as today, except `native`
    pub artifact: Option<VersionArtifact<'a>>,
}
```

- `activate_version` writes `artifact_s3_key`, `artifact_hash` and
  `artifact_size` from `artifact.map(VersionArtifact::artifact)`.
- It calls `replace_native` only for `VersionArtifact::Native`.
- Every current caller passes `artifact: native.map(VersionArtifact::Native)`
  or `None`, so no other mode's behaviour changes.

### 2.4 Rollback

`rollback_locked` gets a `ServiceMode::Web` arm of its own, beside
`Native` (`pipeline.rs:430-456`):

```rust
ServiceMode::Web => {
    let artifact = super::web::restore(state, target_deploy).await?;
    let deployment = activate_version(&state.platform_db, ActivateVersion {
        service_id,
        version: new_version,
        manifest_hash: &deploy_hash(&manifest)?,
        manifest: &manifest,
        wasm_hash: None,
        commit_hash: target_commit_hash.as_deref(),
        schema_json: None,
        schema_hash: &manifest.schema_hash()?,
        artifact: Some(VersionArtifact::Web(&artifact)),
    }).await?;
    (schema::SchemaReport::default(), EventingPlan::default(), deployment)
}
```

`web::restore(state, target)`:

- answers `Conflict` when the target records no bundle. That is a version
  section 1.3 left behind, or one registered by an older CLI whose upload
  never came. The message names the newest earlier version that has a
  bundle: `v7 of site has no bundle (its build or upload never completed);
  the newest earlier version with one is v5: cufflink rollback --version 5`;
- heads the recorded key and answers `Conflict` when the object is missing
  or its size differs from the recorded size: `the bundle of v5
  (web/…/….tar.gz) is missing from object storage; deploy the app again`;
- otherwise returns the target's `Artifact` unchanged. A legacy
  `v{n}.tar.gz` key is copied as it is.

Both refusals happen before anything is written. `rollback_locked` takes
the deploy lock first, as today, so a prune cannot remove the object
between the head check and the activation.

The CLI's `--version`, `--commit` and default "previous" targets are
unchanged. "Previous" is `current_version - 1` in both the CLI
(`cli/src/commands/rollback.rs:166-169`) and the platform
(`pipeline.rs:404`). When that version has no bundle, the refusal above
names the one to use (question 3).

`after_rollback` gets a `Web` arm that calls `web::announce` (section
2.5) instead of `eventing::after_rollback`. The rollback answer gains
`"artifact_hash"`, which is the restored bundle's hash for web and the
binary's hash for native. `native_hash` stays as it is. The CLI prints
`  Bundle:         <sha256>` for a web app.

### 2.5 Telling the runners

`deployer::web::announce(state, identity, deployment)` publishes
`deploy.web.{tenant}.{service}` with the payload `api/web.rs:68-75`
sends today:

```json
{"service_id": "…", "service_name": "site", "version": 8,
 "s3_key": "web/…/….tar.gz", "artifact_hash": "…", "tenant_slug": "acme"}
```

It is called:

- after a release that activated a version (`after_release`'s `Web` arm,
  which returns `Ok(())` today at `pipeline.rs:352`);
- after a skipped release of a web app. A retried release then also
  repairs a runner that missed the first event, and a runner that already
  runs that bundle does nothing (question 7);
- after a web rollback (section 2.4);
- by the legacy upload route when it changes what runs (section 2.8).

The payload is unchanged, so every runner version reads it. A failed
publish is logged and counted (`cufflink_web_announce_failures_total`).
The answer says whether the runners were told (`"runners_told": false`),
and the CLI prints that the runners pick the version up within
`WEB_RECONCILE_SECS`.

### 2.6 Runners converge without an event

A web runner converges the apps it runs, as the extension runner does
(`EXT_RECONCILE_SECS`):

- **When.** Every `WEB_RECONCILE_SECS` (default 30) and once at start.
  The tick is a task on the runner's `Shutdown`.
- **What it reads.**
  - A shared runner reads the current deployment of every app it runs in
    one query. That is `source.rs:226-236`'s query over
    `unnest($1::text[], $2::text[])` pairs of tenant and service.
  - A tenant-scoped runner reads one `GET /api/runner/web`. `watch_key`'s
    poll and the reconcile become one loop: the answer that tells the key
    is accepted is also the set to converge on. A refused key retires the
    tenant's apps as it does today.
- **What it does,** for each app it runs whose current deployment has a
  bundle of a different SHA-256 than the one it runs from: `apps.deploy`,
  which is the path an event takes.
- **What it never does.**
  - It never stops an app. An app whose current version has no bundle,
    or that no longer appears, keeps running. Undeploy stays event-driven
    (question 8). A version without a bundle must never take down a
    running app.
  - It never retries a bundle that failed its health check. The runner
    remembers, per app, the SHA-256 whose start failed, and skips it until
    a deploy event names the app again (an event clears it, as
    `forget_exits` does today) or the current hash changes.
  - It starts nothing that is not running. Cold apps are still started by
    their first request.
- **The warmer.** A deploy event and a reconcile switch both call a new
  `LazyWarmer::forget(&key)`, which clears the 10 s not-found and the 30 s
  failed-start entries for that app. A rollback that heals a hole then
  serves on the next request.

### 2.7 Draining the replaced process

**Requests are counted by the runner, not left to the app.** `next start`
is not guaranteed to finish its requests on SIGTERM, and a Node process
with no handler exits at once. So the runner waits for the requests it
forwarded before it signals.

```rust
#[derive(Debug, Default)]
pub struct Gate {
    open: bool,
    requests: usize,
}

#[derive(Debug)]
pub struct InFlight(tokio::sync::watch::Sender<Gate>);

pub struct Entered(Arc<InFlight>);

impl InFlight {
    pub fn new() -> Arc<Self>;                          // open, no requests
    pub fn enter(self: &Arc<Self>) -> Option<Entered>;  // None once closed
    pub fn close(&self);                                // admits no more
    pub async fn idle(&self);                           // closed and no requests
}

impl Drop for Entered {
    fn drop(&mut self) {
        self.0 .0.send_modify(|gate| gate.requests -= 1);
    }
}
```

- `Release` gains `in_flight: Arc<InFlight>`. Its `PartialEq` compares
  `version` and `dir` only, since the gate is identity, not value.
- `ProcessManager::lease(tenant, service) -> Option<Lease>` (`Lease {
  port, entered }`) reads the release and enters its gate.
- When the gate is closed, the table already holds the release that
  replaced it, or nothing. The swap is admitted before the drain closes
  anything. So `lease` reads again, and is never refused by a gate that
  has a successor.
- A request either entered before `close`, and is waited for, or is
  refused and goes to the new process. There is no window in between.
- The proxy (`proxy.rs:73-102`) takes a `Lease` instead of a port. It
  wraps the response body in a body that owns the `Entered`, so a request
  counts until its body ends or its client goes away. A streamed response
  (server-sent events) therefore counts until the grace ends.

**The drain.** On a successful swap, `ProcessManager::deploy` no longer
stops the replaced child. It returns it, and `Apps::run` hands it to
`Apps::drain`. That spawns the task `web_drain` on the runner's `Shutdown`,
so the event loop moves on at once (section 1.5). The task:

1. closes the old release's gate;
2. waits for the first of three things: `in_flight.idle()`,
   `WEB_DRAIN_GRACE_SECS`, or the runner's shutdown;
3. stops the child with `Stop::Terminate(WEB_CHILD_STOP_TIMEOUT_SECS)`,
   which sends SIGTERM and then SIGKILL (`supervisor/src/child.rs:52-63`);
4. drops its hold on the old directory and prunes. The directory hold of
   section 1.6 becomes "until the old process has exited", and
   `OLD_VERSION_KEPT` is removed.

**Settings and edge cases.**

- `WEB_DRAIN_GRACE_SECS=0` keeps today's behaviour: the replaced child is
  killed at once, with no wait and no SIGTERM.
- Shutdown cuts a drain short to step 3. `shutdown_all` only reaches
  children still in the table, so the drain task is what stops a child
  being drained. It runs on `Shutdown`, so the runner's exit waits for
  it within `SHUTDOWN_GRACE_SECS`, like the supervisor tasks. A step 3
  that starts at shutdown takes at most `WEB_CHILD_STOP_TIMEOUT_SECS`,
  which already fits the shutdown budget. So no cross-check between the
  grace and `SHUTDOWN_GRACE_SECS` is needed.
- A second swap while the first drain runs drains its own replaced child.
  Each drain owns one child.
- A new process that fails its health check replaces nothing, so nothing
  is drained (unchanged).
- Undeploy (`retire`) and a refused key (`retire_tenant`) still kill at
  once (question 6). A refused key must stop the tenant's apps without
  delay.
- Metrics:
  - `cufflink_web_runner_drains_total{outcome="drained|grace_elapsed|shutdown"}`
  - `cufflink_web_runner_drain_seconds` (histogram)
  - `cufflink_web_runner_draining` (gauge)
  
  Each drain logs tenant, service, old and new version, the requests it
  waited for and the outcome.

### 2.8 The legacy upload route

`POST /api/services/{id}/web` stays for CLIs older than this release and
for the mend in section 7.2. It stores by content (section 2.1) and
records an upload row. Then, under the deploy lock:

| The current version | The route |
|---|---|
| records no bundle | records this bundle on that row (`UPDATE … WHERE artifact_s3_key IS NULL`), then announces. This fills the hole that an older CLI's register, or an older replica, left |
| records this bundle | changes nothing, and announces |
| records another bundle | activates a **new** version with the current row's manifest and commit and this bundle, then announces. The running version's row and object are never touched |

The answer keeps today's fields and adds `"version"` (the version that
now records the bundle) and `"released": true|false`.

**Deploys without a bundle.** A web deploy that names no `web_artifact` is
what every older CLI sends. During this release it is accepted as today,
with `"warnings": ["this CLI registers a web app before building it; …
upgrade the CLI"]` and `cufflink_web_legacy_deploys_total` (question 1).
A failed build from an older CLI can still leave a version with no bundle.
Rollback (section 2.4) and the runners (section 2.6) handle such a
version safely.

### 2.9 Unreleased uploads and removing bundles

- **Retention of unreleased uploads.** An upload row that no deployment
  records and that is older than `WEB_UPLOAD_RETENTION_SECS` (default
  86 400) is removed, with its object, by:
  - each web release and rollback of that service, after activation and
    still under its lock;
  - each prune or version delete of that service.
- **Shared objects.** Rollbacks and content keys let several rows record
  one object. `delete_by_version` returns the key of web rows too (its
  `CASE` covers `'web'`). `native::remove_orphan` becomes
  `deployer::artifacts::remove_orphan`, and it removes a key only when
  `records_artifact` finds no row, and no upload row younger than the
  retention, that holds it. Pruning a web app now frees what no retained
  version needs.
- **Service and tenant delete** call `delete_prefix("web/{service_id}/")`
  as native does (`native.rs:299-306`). That covers recorded keys, legacy
  keys and unreleased uploads. The upload rows go with the service
  (`ON DELETE CASCADE`).
- **Races.**
  - The removal runs under the deploy lock, as releases do, so it cannot
    remove an upload while a release claims it.
  - An upload is not under the lock. It writes the object, then upserts
    its row (refreshing `uploaded_at`). The removal deletes the row, then
    reads again that no upload row holds the key, then deletes the object.
  - The one interleaving left is an upload of the same content between
    those last two reads. The release then finds the object missing and
    answers `409` (`upload it again`). The CLI uploads once more and
    releases again (section 4.5).

## 3. Data model and migration

`platform/migrations/V9__web_uploads.sql`:

```sql
-- Web bundles uploaded for a service, stored by content, before or after
-- a release records them. A row no deployment records is removed, with
-- its object, once older than WEB_UPLOAD_RETENTION_SECS.
-- Additive only, so an earlier binary runs against this schema and never
-- reads it.

SET LOCAL lock_timeout = '10s';
CREATE TABLE IF NOT EXISTS web_uploads (
    service_id  UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    sha256      TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    s3_key      TEXT NOT NULL,
    size        BIGINT NOT NULL CHECK (size > 0),
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    uploaded_by TEXT NOT NULL,
    PRIMARY KEY (service_id, sha256)
);
CREATE INDEX IF NOT EXISTS web_uploads_uploaded_at_idx ON web_uploads (uploaded_at);
```

- `deployments` is unchanged. A web version records its bundle in the
  existing `artifact_s3_key`, `artifact_hash` and `artifact_size`.
- `platform/src/repo/web_uploads.rs` holds `record` (upsert), `claim`
  (by `service_id` and `sha256`) and `expired` (`DELETE … WHERE
  uploaded_at < now() - $retention AND NOT EXISTS (SELECT 1 FROM
  deployments WHERE artifact_s3_key = web_uploads.s3_key) RETURNING
  s3_key`).
- No data is migrated. Rows written before this release keep their
  `v{n}` keys, and those with no bundle stay as they are.

## 4. API, CLI and events

### 4.1 `POST /api/services/web`: reserve

```json
{ "name": "site" }
```

- Deploy auth, the caller's tenant.
- Runs `find_or_create` with the web manifest's schema and status `active`
  (a web app cannot be inactive). It is idempotent.
- Answers `200` `{"service_id": "…", "name": "site", "current_version": 7,
  "created": false}`.
- `409` when the service exists and its current version is of another
  mode, in `require_same_kind`'s words. `422` for an invalid name.
- No version, no event and no audit row. A reservation changes nothing
  that runs.

### 4.2 `PUT /api/services/{id}/web/artifacts/{sha256}`: upload

- Body: the gzip tarball, within `ARTIFACT_BODY_LIMIT` (250 MiB,
  `platform/src/app/layers.rs:16`) as today.
- `400` when `{sha256}` is not 64 lowercase hex digits.
- `409` when the service's current version is of a mode other than web.
  A reserved service with no version is accepted.
- `422` `the bundle's SHA-256 is <actual>, not <sha256>` when the body
  does not hash to the path. Nothing is stored.
- Stores at `object_key(service_id, sha256)`, skipping the put when an
  object of that size is already there, then upserts the upload row.
- Answers `200` `{"sha256": "…", "size": 41234567, "s3_key": "…",
  "stored": "uploaded" | "present"}`.
- Audited as `web.upload` with `sha256` and `size`. Counted as
  `cufflink_web_uploads_total{result}`.

### 4.3 `POST /api/services/deploy`: release

`DeployRequest` (`platform/src/api/deployments.rs:35-53`) gains:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebArtifactRef {
    pub sha256: String,
    pub size: u64,
}

#[serde(default)]
pub web_artifact: Option<WebArtifactRef>,
```

- `admit` refuses `web_artifact` on any mode but `web` (`400`). It also
  refuses a malformed `sha256` (`400`).
- In `run_locked`, a claim that finds no upload row, no object or another
  size answers `409` `bundle <sha256> of site was not uploaded, or is no
  longer stored; upload it again`. The running version is unchanged.
- The answer of a web release, deployed or skipped, adds `"web_artifact":
  {"sha256", "size"}`, which is the bundle the current version records,
  and `"runners_told"`. The audit row's details add `artifact_hash`.
- An asynchronous release (`?async=true`) records `web_artifact_hash` in
  its job's request (`Admitted::request`).
- `DEPLOY_API_VERSION` stays 1. The field is optional, and the CLI
  negotiates through the feature below.

### 4.4 Feature

`/api/version` advertises `web_releases` (`cufflink_types::features::WEB_RELEASES`).
It means that the reserve, upload and release routes exist, that a
rollback carries a web bundle, and that releases are skipped by content.
It gets a row in `docs/versions.md` under the release's features, which
the platform test of `supports` against the doc tables requires.

### 4.5 CLI

`deploy_web` becomes:

1. Ask `/api/version`. Without `web_releases`, fail before building:
   `platform <version> cannot release a web app with its bundle (needs
   web_releases). Upgrade the platform, or deploy with a CLI older than
   v0.38.0` (question 2).
2. Run `typecheck`, `lint` and `test` as today.
3. Reserve, then `service_id`.
4. Sync configs, fetch configs, build, tar. These are unchanged and use
   the reserved `service_id`.
5. Hash the tarball and `PUT` it.
6. Release with the manifest, `web_artifact`, `commit_hash`,
   `cli_version` and `api_version`.
7. Check the answer:

| The answer | The CLI (exit 1 unless it says otherwise) |
|---|---|
| `web_artifact.sha256` is the bundle sent, and the release deployed | prints `Released site v8 (bundle 3f9a…)`, exit 0 |
| the same, and it was skipped | prints `site v7 already runs bundle 3f9a…; nothing to release`, exit 0 |
| `409`, bundle not uploaded or no longer stored | uploads once more and releases once more. A second `409` fails with the platform's words |
| names no `web_artifact`: an older replica took the release (section 7.2) | posts the same bundle to `POST /api/services/{id}/web`, which records it on the version that replica created, and warns `an older platform replica took the release; the bundle was attached to v8. Is a platform upgrade in progress?`. Exit 0 when that answer names the bundle, otherwise fails with `site v8 has no bundle: deploy again once the platform is upgraded` |
| `runners_told: false` | warns that the runners pick the version up within `WEB_RECONCILE_SECS`. Exit 0 |

A failure in steps 2 to 6 releases nothing. The CLI's message says so:
`the build failed; site still runs v7`.

`cufflink rollback` is unchanged, except for the `Bundle:` line (section
2.4).

### 4.6 Events

- `deploy.web.{tenant}.{service}`: same subject, same payload, now
  published after releases, skipped releases, rollbacks and legacy
  uploads that change or confirm what runs (section 2.5).
- `undeploy.web.{tenant}.{service}`: unchanged.

## 5. Failure handling

| Failure | What runs afterwards | What is left behind |
|---|---|---|
| typecheck, lint, test or build fails | the running version, untouched | a reserved service row with no version on a first deploy (as today after a failed first deploy). On later deploys, nothing. Configs synced in step 4 are written, as today |
| the upload fails or is cut | the running version | at most a stored object without its row. A `put` is atomic in object storage, so there is no partial object. It is removed with the service's prefix, or replaced by the next identical upload |
| the upload's bytes do not match its hash | the running version | nothing (`422`) |
| the release is refused (`409`, `422`, lock held) | the running version | the upload row, removed after `WEB_UPLOAD_RETENTION_SECS` |
| the platform dies during the release | the running version, or the new one if the transaction committed. Never a version with no bundle | the CLI fails without an answer. Running the deploy again releases or skips by content |
| the publish to NATS fails | the database's current version. Runners switch within `WEB_RECONCILE_SECS` | `runners_told: false`, a warning and a metric |
| a runner is offline during a release or rollback | when it starts, it serves the current version on the first request (cold start reads the current version) | nothing |
| a runner is up but missed the event | it switches at its next reconcile | nothing |
| the rollback target has no bundle | the running version (`409`, naming the newest version with one) | nothing |
| the rollback target's object is missing | the running version (`409`) | nothing |
| the new process fails its health check | the old process, undrained and serving | the failed SHA-256 is remembered, and reconcile does not retry it until an event or a new hash |
| the old process still has requests at the end of the grace | the new process | those requests are cut by SIGTERM, then SIGKILL after `WEB_CHILD_STOP_TIMEOUT_SECS`. Counted as `grace_elapsed` |
| the runner shuts down during a drain | nothing | the drain goes straight to SIGTERM. Counted as `shutdown` |
| an older replica takes the release | the version it created, with the bundle attached by the CLI's mend (section 4.5) | when the mend also fails, a version with no bundle. Section 2.4 and section 2.6 keep that safe, and the CLI says how to repair it |

## 6. Configuration

| Variable | Component | Default | Meaning |
|---|---|---|---|
| `WEB_DRAIN_GRACE_SECS` | web runner | `30` | How long a replaced process may finish the requests it was given before SIGTERM. `0`: killed at once, as before this release. Above `3600` is refused at start |
| `WEB_CHILD_STOP_TIMEOUT_SECS` | web runner | `10` (unchanged) | Between SIGTERM and SIGKILL, now for drained processes as well as at shutdown |
| `WEB_RECONCILE_SECS` | web runner | `30` | How often the apps it runs are converged without an event. Must be positive |
| `WEB_POLL_SECS` | tenant-scoped web runner | `30` (unchanged) | Kept for the key check. The reconcile reads the same answer on its own tick. When both are set, each keeps its own period |
| `WEB_UPLOAD_RETENTION_SECS` | platform | `86400` | How long an upload no version records is kept. Must be at least `3600`, so a release is never refused for a bundle it just uploaded |

No setting changes behaviour for WASM, CRUD, container or native
services.

## 7. Compatibility

### 7.1 CLI, platform and runner versions

| CLI | Platform | Outcome |
|---|---|---|
| new | new | reserve, upload, release |
| new | older | refused before building, naming `web_releases` (section 4.5) |
| older | new | works as today: an artifact-less register (accepted with a warning) and the legacy upload (section 2.8), which never replaces a recorded bundle and releases new content as a new version. A failed build can still leave a version with no bundle. Upgrade the CLI with the platform |
| older | older | as today |

The runner is independent of both:

- **A new runner on an older platform** reconciles and drains. It reads
  the same rows and the same runner API.
- **An older runner on a new platform** reads content keys as it reads
  legacy keys, since they are opaque. It switches on events, rollbacks
  included. It has no reconcile and kills replaced processes at once.
- Upgrade runners in any order. A tenant-scoped runner sees no API
  change.

### 7.2 A platform with replicas of two versions

This follows the existing rule: deploy no web app while replicas of
two versions run side by side, upgrading or rolling back
(`docs/deployment.md`, "Deploy no service while a platform is being
upgraded or rolled back").

If it happens anyway:

| The release is taken by | Effect | The CLI |
|---|---|---|
| a new replica | released | as section 4.5 |
| an older replica | it ignores `web_artifact` (unknown fields are dropped) and activates a version with no bundle, as an older CLI's register does | sees no `web_artifact` in the answer and mends through the legacy upload, which either replica accepts. On an older replica it fills the current version, as today. On a new one it fills the version with no bundle (section 2.8) |
| an older replica, for the upload `PUT` | `404` or `405` (unknown route) | fails before releasing: `the platform does not take web bundles by content: is a platform upgrade in progress?` |
| an older replica, for a rollback | the hole of section 1.4 | nothing to check. The runners keep the old process, and section 2.6 keeps it running. Roll back again once the platform is upgraded |

### 7.3 Rolling a platform back below this release

For the `docs/versions.md` table:

| What this release added | Before rolling the platform back |
|---|---|
| Web bundles by content | Nothing to undo. The older platform reads the recorded keys as they are, and its runners serve them |
| Web rollbacks that carry the bundle | **Do not `cufflink rollback` a web app on the older platform.** Its rollback creates a version with no bundle (section 1.4). Deploy the earlier commit instead |
| Web releases (`web_releases`) | Pin the CLI to the older release for web deploys. A CLI of this release refuses to deploy a web app there (section 4.5) |
| Unreleased uploads | Nothing. They stay in storage, unread, until the platform is back (retention) or the service is deleted (prefix delete) |
| Drain and reconcile | Runner settings. The runners need no change for a platform rollback |
| The database | Nothing: `V9__web_uploads` only adds a table the older binary does not read. The older binary logs its missing migration at each start, as for earlier migrations |

## 8. Alternatives considered

### 8.1 The bundle inside the deploy request, as `native_base64`

This is one request and one transaction, like native. But:

- a web bundle is commonly tens of megabytes, and base64 in a JSON body
  makes it a third larger and parses it in memory;
- it moves the build ahead of the service's first existence, so a first
  deploy cannot sync or read configs before it builds (section 1.1).

The separate upload is idempotent by content, so a retry costs nothing
when the bundle is already there.

### 8.2 Keeping the version number in the key and refusing a second upload

Refusing an upload to a version that already records a bundle would stop
the overwrite of section 1.2. But a failed build would still leave a
version with no bundle. A rollback would also still need a second copy
of a bundle under a new version's name.

### 8.3 Relying on the app's SIGTERM handling

SIGTERM and wait is what the issue first proposes. Whether a request in
flight completes would then depend on each app's framework and version.
Counting forwarded requests in the runner holds for any app. SIGTERM is
still sent, after the requests end.

### 8.4 A durable event stream instead of reconcile

Publishing `deploy.web` on JetStream would let a runner catch up after a
disconnect. But it adds a stream per tenant and consumer state per runner.
It also still needs a read of the current version, which is all the
reconcile is. The extension runner already converges this way.

### 8.5 Refusing artifact-less web deploys at once

This would close section 1.3 for every client from this release. But
every older CLI would break the moment the platform is upgraded, before
the CLI can be. Question 1 proposes doing it in the following minor
release.

## 9. Tests

**Platform, in-crate:**

- `pipeline.rs`:
  - `a_web_apps_code_hash_is_its_bundle_hash`;
  - `the_same_bundle_and_manifest_is_skipped`;
  - `a_new_bundle_under_the_same_package_version_is_a_new_version`;
  - `the_same_bundle_under_a_new_package_version_is_a_new_version`.
- `api/deployments.rs`:
  - `a_web_artifact_on_another_mode_is_refused`;
  - `a_malformed_bundle_hash_is_refused`;
  - `a_web_deploy_without_its_bundle_is_accepted_with_a_warning`.
- `deployer/web.rs`:
  - `a_bundle_is_keyed_by_its_service_and_content`;
  - `restore_refuses_a_target_without_a_bundle_and_names_the_newest_with_one`;
  - `restore_refuses_a_bundle_missing_from_storage_or_of_another_size`;
  - `restore_keeps_a_legacy_key_as_recorded`.
- `repo/deployments.rs`:
  - `a_web_version_records_its_bundle_in_the_activating_transaction`;
  - `only_a_native_version_replaces_its_extensions`;
  - `deleting_a_web_version_returns_its_key`.
- `repo/web_uploads.rs`:
  - `an_upload_is_recorded_once_and_refreshed_by_the_next`;
  - `expired_returns_only_unrecorded_uploads_past_the_retention`.
- `api/version.rs`: the existing doc-table test covers `web_releases`
  once its row is added.

**Platform, integration (`platform/tests/web_releases.rs`):**

These run against the object storage the platform tests already use
(as `native_deploy.rs` does), with a NATS subscription on
`deploy.web.acme.>`, for the tenant `acme` and the app `site`.

- **Reserve and upload:**
  - `reserving_a_web_app_creates_no_version`;
  - `reserving_a_service_of_another_mode_is_refused`;
  - `an_upload_is_stored_by_content_and_releases_nothing` (no row change,
    no event);
  - `an_upload_whose_bytes_are_not_its_hash_is_refused`.
- **Release:**
  - `a_release_records_its_bundle_and_announces_it`;
  - `a_release_naming_a_bundle_never_uploaded_changes_nothing`;
  - `a_second_bundle_under_the_same_package_version_never_replaces_the_first`.
    v1's object bytes, key and row are unchanged, and v2 is new;
  - `the_same_bundle_again_is_skipped_and_announced`;
  - `a_web_release_records_its_bundle_in_the_deploy_audit`.
- **Rollback:**
  - `rolling_back_a_web_app_carries_the_targets_bundle_and_announces_it`
    (#62). The runner API's `GET /api/runner/web` then answers the
    target's hash;
  - `rolling_back_to_a_version_without_a_bundle_is_refused_before_anything_changes`;
  - `rolling_back_to_a_bundle_missing_from_storage_is_refused`.
- **Legacy route:**
  - `the_legacy_upload_fills_a_version_without_a_bundle`;
  - `the_legacy_upload_of_the_running_bundle_changes_nothing`;
  - `the_legacy_upload_of_new_content_releases_a_new_version`.
- **Removal:**
  - `pruning_removes_a_bundle_no_version_records_and_keeps_a_shared_one`;
  - `an_unreleased_upload_is_removed_after_its_retention_and_a_fresh_one_kept`;
  - `deleting_a_web_app_removes_every_bundle_and_upload`.

**Web runner, in-crate** (`apps.rs`'s `World` with Node bundles, as
today):

- **Reconcile:**
  - `a_missed_event_is_caught_by_the_next_reconcile`;
  - `reconcile_never_stops_an_app_whose_current_version_has_no_bundle`;
  - `reconcile_does_not_retry_a_bundle_that_failed_its_health_check_until_an_event`;
  - `reconcile_starts_no_app_that_is_not_running`.
- **Warmer:** `a_deploy_event_clears_the_warmers_not_found_entry`.
- **Gate (`process_manager.rs`):**
  - `a_closed_gate_admits_no_request_and_is_idle_when_the_last_ends`;
  - `a_lease_taken_across_a_swap_reaches_the_new_process`.
- **Drain:**
  - `the_replaced_process_finishes_its_request_in_flight_before_it_is_stopped`.
    The bundle's server answers after 2 s, the request starts before the
    swap, and it ends 200 with the old marker;
  - `a_request_past_the_grace_is_cut_and_the_process_stopped`;
  - `a_zero_grace_kills_the_replaced_process_at_once`;
  - `shutdown_cuts_a_drain_short_and_stops_the_old_process`;
  - `the_replaced_directory_stays_until_its_process_has_exited`.
- `config.rs`:
  - `the_settings_default_as_documented` (extended);
  - `a_drain_grace_above_an_hour_is_refused`;
  - `a_zero_reconcile_period_is_refused`.

**Web runner, integration (`web-runner/tests/drain.rs`):** through the
runner's proxy port, a slow request to `site.acme.example.com` that
spans a deploy event completes, and the request after it reaches the new
version.

**CLI** (`deploy.rs` unit tests against `commands/stub.rs`, as the
inactive-deploy tests do):

- **Order:** `a_web_deploy_reserves_builds_uploads_then_releases`. The
  stub sees the requests in that order, and the release names the
  uploaded hash.
- **Failures release nothing:**
  - `a_failed_build_releases_nothing`;
  - `a_failed_upload_releases_nothing`.
- **Retries and mends:**
  - `a_bundle_no_longer_stored_is_uploaded_again_once`;
  - `an_answer_without_the_bundle_mends_through_the_legacy_upload`;
  - `a_mend_that_fails_says_the_version_has_no_bundle`.
- **Negotiation:**
  `a_platform_without_web_releases_is_refused_before_building`.
- **Rollback:** `a_web_rollback_prints_the_bundle_it_runs`
  (`rollback.rs`).

## 10. PR plan

Each PR passes `cargo fmt`, `cargo clippy --all-targets -- -D warnings`
and `cargo test --workspace`, and updates the docs and the
`docs/versions.md` rows for what it changes.

1. **Web rollbacks carry their bundle** (#62, platform).
   - `VersionArtifact` (section 2.3), `deployer/web.rs` with `restore` and
     `announce`, and the `Web` arms of `rollback_locked` and
     `after_rollback`.
   - `artifact_hash` in the rollback answer, and the CLI's `Bundle:` line.
   - Tests: rollback in-crate and the three rollback integration tests.
   - Works with legacy keys alone, so it ships before anything else.
2. **Web runners converge** (#62, runner).
   - `WEB_RECONCILE_SECS`, the shared runner's batch read, and
     `watch_key` merged with the reconcile.
   - The failed-hash memory and `LazyWarmer::forget`.
   - Tests: reconcile and warmer.
3. **Web bundles by content** (#63, platform).
   - `V9__web_uploads`, `repo/web_uploads.rs`, `object_key`, and the
     reserve and upload routes.
   - `web_artifact` on deploy, the skip rule, and `after_release`'s
     announce.
   - The legacy route of section 2.8 and the legacy-deploy warning.
   - `delete_by_version` for web, `artifacts::remove_orphan`, retention,
     prefix delete, and the `web_releases` feature.
   - Tests: the remaining platform tests.
4. **The CLI releases a web app in one step** (#63, CLI).
   - Section 4.5, and the CLI tests.
   - Needs PR 3's feature, which it checks.
5. **Drain the replaced process** (#64, runner).
   - `InFlight`, `Lease`, the proxy's body guard, the `web_drain` task,
     `WEB_DRAIN_GRACE_SECS`, metrics, and the directory hold.
   - Tests: drain, plus `web-runner/tests/drain.rs`.
   - Independent of 1 to 4.

PRs 1, 2 and 5 can be reviewed in parallel. PR 4 follows PR 3.

## 11. Documentation and release notes

**Documentation, with the PRs:**

- `docs/deployment.md`, "Web runner":
  - replace the sentence "a bundle of other content uploaded again under
    the same version runs in its place" with the content rule of section
    2.1;
  - add "Releases" (section 2.2), "Converging" (section 2.6) and
    "Draining" (section 2.7);
  - list the new variables;
  - extend "Rolling back to a commit" with web apps.
- `docs/setup.md`: `WEB_DRAIN_GRACE_SECS`, `WEB_RECONCILE_SECS` and
  `WEB_UPLOAD_RETENTION_SECS`.
- `docs/observability.md`: the metrics of sections 2.5, 2.7, 2.8 and
  4.2.
- `docs/versions.md`:
  - `web_releases` under the release's features;
  - "Changes in v0.38.0";
  - the CLI and platform table rows of section 7.1;
  - the rollback table of section 7.3;
  - `V9__web_uploads` in the list of migrations an older binary logs as
    missing.

**Release notes outline (v0.38.0):**

- **Web apps release with their bundle.** A version exists only once its
  bundle is stored. A failed build or upload leaves the running version
  as it was. New routes: reserve, upload by content, and `web_artifact`
  on deploy. The new feature is `web_releases`. A CLI of this release
  needs it.
- **Versions by content.** A new bundle is a new version whatever
  `package.json` says, and the same bundle is skipped. Bundles are stored
  as `web/{service_id}/{sha256}.tar.gz`, and recorded keys of earlier
  releases keep working.
- **`cufflink rollback` of a web app switches.** It carries the target's
  bundle and tells the runners. It refuses a target with no bundle and
  names the newest one that has one.
- **Runners converge.** Every `WEB_RECONCILE_SECS`, a runner switches an
  app whose current bundle differs from the one it runs. It never stops
  one.
- **Drained swaps.** A replaced process finishes its requests within
  `WEB_DRAIN_GRACE_SECS` (30 s), then gets SIGTERM, then SIGKILL.
  `WEB_DRAIN_GRACE_SECS=0` restores the previous behaviour.
- **Pruning frees web bundles,** and unreleased uploads are removed after
  `WEB_UPLOAD_RETENTION_SECS`.
- **Compatibility.**
  - Older CLIs keep working with a warning, and their uploads never
    replace a recorded bundle.
  - Upgrade the CLI with the platform.
  - Do not roll back a web app on a platform older than this release.
  - Migration `V9__web_uploads` is additive.

## 12. Not in this change

- Refusing artifact-less web deploys (question 1, a later release).
- Draining on undeploy (question 6).
- WebSocket upgrades through the runner's proxy, which it does not relay
  today.
- Reproducible web bundles (stable tar entry order and times in the CLI).
  With them, a rebuild of the same sources would be skipped instead of
  released.
- Draining the extension runner's native services on replacement.

## 13. Questions for the maintainer

| # | Question | Proposed default |
|---|----------|------------------|
| 1 | Should a web deploy with no `web_artifact` (every older CLI) still be accepted in this release? | Yes, with a warning and `cufflink_web_legacy_deploys_total`. Refuse it (`422`, naming the CLI version) from the next minor release |
| 2 | Should a CLI of this release fall back to the two-step flow against a platform without `web_releases`? | No. It refuses before building and names the fix. A fallback would keep the flow that leaves versions without a bundle |
| 3 | When the version before the running one has no bundle, should `cufflink rollback` (no `--version`) refuse or skip to the newest version with one? | Refuse, naming that version and the command. A rollback never lands somewhere other than the version asked for |
| 4 | How long should an unreleased upload be kept? | 24 h (`WEB_UPLOAD_RETENTION_SECS=86400`), at least 1 h |
| 5 | Default drain grace? | 30 s, with `0` meaning an immediate kill, as before |
| 6 | Should undeploy (service delete) drain too? | No. It kills at once as today. A refused key always kills at once |
| 7 | Should a skipped web release publish `deploy.web` again? | Yes. It is free for a runner that already runs the bundle, and it repairs one that missed the first event |
| 8 | Should the runner's reconcile ever stop an app? | No. It only switches to a different bundle. Stopping stays with `undeploy.web` and a refused key |
| 9 | Should a missing rollback target object be `409` (proposed) rather than the `500` native rollbacks answer today? | `409` for web. Native is left as it is in this change |
| 10 | Should reserving be its own route (`POST /api/services/web`), rather than a flag on deploy? | Its own route, for web apps only: it creates no version, so it should not look like a deploy |
