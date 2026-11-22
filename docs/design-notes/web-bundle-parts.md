# Web bundles uploaded in parts

Proposed. Covers #84 item 1 and the CLI half of item 2: the CLI uploads a
large web bundle to the platform in parts, the way #88 made the platform
upload a large object to its bucket in parts. It builds on the web deploy
lifecycle design (`docs/design-notes/web-deploy-lifecycle.md`), whose
reserve, build, upload, release steps and content-addressed uploads it keeps.

## Outcome

- A bundle above 32 MiB goes from the CLI to the platform as parts of 8 MiB,
  four in flight. Each part is its own request under the CLI's 120 s request
  timeout, so the upload no longer has to cross the link in 120 s as a
  whole. A 250 MiB bundle, the platform's cap, needs about 0.3 MB/s per part
  instead of 2.2 MB/s for the whole.
- No timeout grows and nothing is retried. A part that fails stops the
  upload, and nothing is released, as a failed upload does today.
- Every storage backend works: parts are ordinary objects, written and read
  with the primitives `cufflink-storage` already has.
- Every platform replica serves every request: no upload state lives in a
  process.
- No bundle is ever stored at its content key unless its bytes are that
  hash.
- A smaller bundle, a platform without the feature, and a CLI without it,
  all upload in one request as today.

## 1. Why the upload fails today

`web_release.rs` `upload` sends the whole bundle as one
`PUT /api/services/{id}/web/artifacts/{sha256}` through
`CliConfig::http_client`, whose `timeout(120 s)` bounds the whole request:
sending the body, the platform hashing it, storing it and answering. The
platform takes bundles up to 250 MiB (`ARTIFACT_BODY_LIMIT`), so any link
slower than about 2.2 MB/s, or one that slows down for part of the upload,
fails with `operation timed out`, and nothing is released. #88 fixed the
same limit one hop further on, between the platform and its bucket, by
uploading in parts. This design applies the same pattern between the CLI
and the platform.

## 2. The protocol

A new platform feature, `web_bundle_parts`, advertises two routes. Both take
the `DeployAuth` credential and the checks the single upload makes: the
service belongs to the caller's tenant, it is a web app or not yet anything
(`web::require_web`), and every hash is 64 lowercase hex digits
(`web::require_sha256`).

### 2.1 Upload a part

`PUT /api/services/{id}/web/parts/{sha256}`, body the part's bytes.

- The route's body limit is `WEB_PART_MAX_BYTES`, 16 MiB, set with its own
  `DefaultBodyLimit` so no part request buffers up to the artifact limit.
  The CLI sends 8 MiB; the headroom lets a later CLI send larger parts
  without a platform change.
- An empty part is refused (`400`), bytes that are not `sha256` are refused
  (`422`, in `web::require_content`'s words).
- The part is stored at `web/{service_id}/parts/{sha256}` and recorded in
  `web_upload_parts` (section 3). A part already recorded and stored at that
  size is not stored again.
- The answer is `{"sha256", "size", "stored": "uploaded" | "present"}`.

Parts are addressed by content, so uploading one twice, from any replica, in
any order, is the same as uploading it once.

### 2.2 Assemble the bundle

`POST /api/services/{id}/web/artifacts/{sha256}/parts`, body
`{"size": <bytes>, "parts": [<part sha256>, ...]}` in order.

- `parts` is not empty and `size` is at most `ARTIFACT_BODY_LIMIT`;
  otherwise `400` / `413`.
- When the bundle is already stored at its key at `size`, the platform does
  not read a part: it records the upload and answers `"stored": "present"`,
  as the single upload does.
- Otherwise every part must be recorded for this service and stored at its
  recorded size. A part that is not is named in a `409`
  (`part {sha256} of bundle {sha256} was not uploaded, or is no longer
  stored; upload it again`), and nothing is stored.
- The recorded sizes must add up to `size`; otherwise `422`.
- The platform reads the parts in order into one buffer of `size` bytes,
  hashes it, and refuses it with `web::require_content`'s `422` when it is
  not `sha256`. Only then does it call `web::store`, the function the single
  upload calls, which stores the bundle at its content key (through #88's
  multipart upload when it is large) and records it in `web_uploads`. A
  bundle whose bytes are not its hash therefore never reaches its key.
- After the bundle is stored, the platform deletes the parts' objects and
  rows. A failure to delete is logged at `warn` and left to the sweep
  (section 3).
- The answer, its audit record (`web.upload`), its log line and the
  `cufflink_web_uploads_total{result}` count are the single upload's, built
  by the same function.

The release step is unchanged: it claims the bundle from `web_uploads` by
hash, whichever route stored it.

### 2.3 Why assembly is synchronous

Assembly reads the parts from the bucket and writes the bundle back. Both
happen next to the bucket, without the CLI's link. The single upload already
holds the same write inside its request today. Assembly adds one read of the
same size, at bucket speed, which is also the speed the platform's own
store timeouts (`OBJECT_STORE_REQUEST_TIMEOUT_SECS`, per part since #88)
already assume. An asynchronous assembly with a status to poll would need a
lease shared between replicas and a recovery path for a replica that dies
mid-assembly. That buys nothing while the bucket is no slower than the
store itself already requires. If the assembly request does time out, its
future is dropped, #88's guard aborts the bundle's multipart upload, and
nothing is stored at the key.

### 2.4 Memory

The platform holds one buffer of the bundle while it assembles, where the
single upload holds the request body and a copy of it. The CLI keeps the
bundle as `Bytes`, as today, and sends zero-copy slices of it. Bounding the
platform's memory by a buffer rather than the bundle, #84's remaining item,
stays open: it needs a streaming write in `cufflink-storage`, which this
change neither needs nor blocks.

## 3. Storage and retention

A new migration, `V10__web_upload_parts.sql`, additive only:

```sql
CREATE TABLE IF NOT EXISTS web_upload_parts (
    service_id  UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    sha256      TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    s3_key      TEXT NOT NULL,
    size        BIGINT NOT NULL CHECK (size > 0),
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (service_id, sha256)
);
CREATE INDEX IF NOT EXISTS web_upload_parts_uploaded_at_idx
    ON web_upload_parts (uploaded_at);
```

Parts get their own table rather than rows in `web_uploads`, so a release
can never claim a part as a bundle.

- `web::sweep`, which runs at each release, rollback, prune and version
  delete of a service, also removes that service's parts older than
  `WEB_UPLOAD_RETENTION_SECS`, with their objects. An upload abandoned
  between its parts and its assembly is therefore cleaned up like an
  unreleased bundle.
- Deleting a service already deletes everything under `web/{service_id}/`,
  parts included, and the foreign key removes their rows.

## 4. The CLI

- `deploy_web` already asks the platform for its version and features. It
  passes whether the platform offers `web_bundle_parts` into `WebApp`, as
  `parts: Option<Parts>`.
- `Parts { threshold, size, in_flight }` defaults to #88's constants: 32 MiB,
  8 MiB, 4. Tests construct smaller values. Nothing reads them from the
  environment.
- `WebApp::upload` is the one place that sends a bundle, both for the first
  upload and for the upload again after a release refused as not claimed.
  It sends one request when `parts` is `None` or the bundle is at most
  `threshold`. Otherwise it splits the `Bytes` into `size` slices, hashes
  each, sends them `in_flight` at a time and stops at the first that fails,
  then asks the platform to assemble them.
- A part's error names its place and the bundle, and ends the way every
  upload error does today:
  `Uploading part 3 of 24 of bundle {sha256} got no answer; nothing was
  released; {name} still runs v{n} (bundle {short})`, and
  `... failed ({status}): {body}...` for a refusal. Assembly errors read
  `Assembling bundle {sha256} ...` with the same endings. The `404`/`405`
  hint for a replica mid-upgrade applies to both routes.
- The single request stays exactly as it is, for older platforms and small
  bundles.

### Compatibility

| CLI | Platform | Outcome |
|-----|----------|---------|
| new | new | a bundle above 32 MiB in parts, a smaller one in one request |
| new | older | one request, as today |
| older | new | one request, as today; the single upload route is unchanged |

While replicas of both releases run, a part or assembly request that reaches
an older replica gets a `404` or `405`, and the CLI says a platform upgrade
may be in progress. Nothing is released, and the next deploy finds every
replica upgraded.

## 5. Tests

Platform (`platform/tests/web_releases.rs`, against the local backend):

- A bundle uploaded in three parts, assembled and released, runs, and the
  bundle stored at its key is the parts' bytes in order.
- Uploading a part twice answers `present` the second time.
- A part whose bytes are not its hash is refused with `422`.
- Assembly naming a part never uploaded is refused with `409` naming it, and
  stores nothing.
- Assembly whose parts are not the bundle's hash is refused with `422`, and
  no object exists at the bundle's key.
- Assembly whose part sizes do not add up to `size` is refused with `422`.
- Assembling a bundle already stored answers `present` and reads no part.
- After a successful assembly the parts' objects and rows are gone. Parts
  past retention are removed by the sweep, and parts within it are kept.
- A part above `WEB_PART_MAX_BYTES` is refused with `413`.
- A service of another mode is refused on both routes.
- The features test covers `web_bundle_parts` through `docs/versions.md`.

CLI (`web_release.rs`, against `commands::stub`):

- A bundle above the threshold, on a platform offering the feature, is sent
  as its parts and then assembled. The parts' hashes and order in the
  assembly request are the slices of the bundle, and the release names the
  bundle.
- A bundle at or below the threshold, or on a platform without the feature,
  is sent in one request, as today's tests already check.
- A part that gets no answer, and a refused part, release nothing, and the
  error names the part, the bundle and what still runs.
- A refused assembly and a lost assembly release nothing.
- A release refused as not claimed uploads again in parts, then releases.
- Splitting: slices cover the bundle in order and only the last is shorter.

## 6. Documentation

- `docs/versions.md`: `web_bundle_parts` in the next release's feature
  table, the compatibility row, and the release's changes.
- `docs/deployment.md`'s web releases section: large bundles go in parts.
- `docs/observability.md`: the existing `cufflink_web_uploads_total` counts
  an assembly like an upload.

## 7. Rules for the change

- No TODOs and nothing partly implemented.
- No comment that the code does not need.
- No duplicated logic: assembly reuses `web::store`, `require_sha256`,
  `require_content` and the single upload's answer and audit; the CLI keeps
  one upload entry point and one error ending.
- Idiomatic, functional Rust in the style of the surrounding code, with no
  `unwrap` outside tests.
