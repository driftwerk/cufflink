# Web bundles uploaded in parts

Proposed. Covers #84: the CLI uploads a large web bundle to the platform in
parts that it can send again one by one, and the platform streams those
parts into the bundle's object without holding the bundle in memory. It
builds on the web deploy lifecycle design
(`docs/design-notes/web-deploy-lifecycle.md`), whose reserve, build,
upload, release steps and content-addressed uploads it keeps.

## Outcome

- A bundle above 32 MiB goes from the CLI to the platform as parts of 8 MiB,
  four in flight. Each part is its own request under the CLI's 120 s
  request timeout, so no request has to carry the whole bundle across the
  link. A 250 MiB bundle, the platform's cap, needs about 0.3 MB/s across
  the four parts in flight instead of 2.2 MB/s for one request.
- A part or an assembly that gets no answer, or a `5xx`, `408` or `429`, is
  sent again, up to three times in all. Only that request is sent again,
  never the bundle.
- The platform holds a few parts in memory while it assembles, not the
  bundle.
- No bundle is ever stored at its content key unless its bytes are that
  hash.
- Every storage backend works, and every platform replica serves every
  request: no upload state lives in a process.
- A smaller bundle, a platform without the feature, and a CLI without it,
  all upload in one request as today.

## 1. Why the upload fails today

`web_release.rs` `upload` sends the whole bundle as one
`PUT /api/services/{id}/web/artifacts/{sha256}` through
`CliConfig::http_client`, whose `timeout(120 s)` bounds the whole request:
sending the body, the platform hashing it, storing it and answering. The
platform takes bundles up to 250 MiB (`ARTIFACT_BODY_LIMIT`), so any link
slower than about 2.2 MB/s, or one that slows down for part of the upload,
fails with `operation timed out`, and nothing is released. The platform
also holds the request body and a copy of it in memory while it stores the
bundle. #88 fixed the same limit one hop further on, between the platform
and its bucket, by uploading in parts.

## 2. The approach

Three ways to carry a large bundle were weighed against four criteria: how
reliable it is over a slow or unsteady link, what a failure costs to
retry, how much memory the platform holds, and how well it fits the
patterns already here (#88's multipart writer, and the single upload route
at `control.rs`).

| | Reliable over a slow link | Retrying a failure | Platform memory | Fit |
|---|---|---|---|---|
| Stream the body in one request | no: the whole transfer is one request, and the 120 s total timeout must become an idle timeout | the whole bundle again | bounded, with a streamed write | the single route, rewritten to stream |
| Parts, assembled in memory | yes: each part is bounded | that part alone | the bundle | #88's pattern on the CLI hop |
| Parts, streamed into the bundle | yes: each part is bounded | that part alone | a few parts | #88's pattern on both hops, through #88's writer |

The third is chosen: it alone bounds every request over the link, resends
only what failed, and keeps the platform's memory at a few parts, all
through #88's own multipart writer and abort guard.

Resuming across runs, by asking the platform which parts it already has,
is left out. A fresh build of a web app rarely gives the same bytes, so
its parts would not match, and a run that fails three times in a row has
a cause that a later run must face anyway.

## 3. The protocol

A new platform feature, `web_bundle_parts`, advertises two routes. Both take
the `DeployAuth` credential and the checks the single upload makes: the
service belongs to the caller's tenant, it is a web app or not yet anything
(`web::require_web`), and every hash is 64 lowercase hex digits.

### 3.1 Upload a part

`PUT /api/services/{id}/web/parts/{sha256}`, body the part's bytes.

- The route's body limit is `PART_MAX_BYTES`, 16 MiB, set with its own
  `DefaultBodyLimit` so no part request buffers up to the artifact limit.
  The CLI sends 8 MiB; the headroom lets a later CLI send larger parts
  without a platform change.
- An empty part is refused (`400`), and bytes that are not `sha256` are
  refused (`422`, `the part's SHA-256 is …`).
- The part is stored at `web/{service_id}/parts/{sha256}` and recorded in
  `web_upload_parts` (section 4). A part already stored at that size is not
  stored again.
- The answer is `{"sha256", "size", "stored": "uploaded" | "present"}`.

Parts are addressed by content, so uploading one twice, from any replica, in
any order, is the same as uploading it once.

### 3.2 Assemble the bundle

`POST /api/services/{id}/web/artifacts/{sha256}/parts`, body
`{"size": <bytes>, "parts": [<part sha256>, ...]}` in order.

- The route's body limit is `ASSEMBLY_MAX_BYTES`, 128 KiB. `parts` names 1 to
  1024 parts (`400`), each a hash (`400`), and `size` is at most
  `ARTIFACT_BODY_LIMIT` (`413`).
- When the bundle is already stored at its key at `size`, the platform
  reads no part: it records the upload and answers `"stored": "present"`,
  as the single upload does.
- Otherwise every part must be recorded for this service (`409`, naming it:
  `part {sha256} of bundle {sha256} was not uploaded, or is no longer
  stored; upload it again`), and the recorded sizes must add up to `size`
  (`422`).
- The platform streams the parts in order, opening up to four ahead, into
  `ObjectStorage::upload_stream` at the bundle's key, and hashes them as
  they pass. A part whose object is missing, or of another size than
  recorded, ends the stream with the same `409`. When the parts end, a hash
  that is not `sha256` ends the stream with `web::require_content`'s `422`.
  An error from the stream means the object is never completed, so a
  bundle whose bytes are not its hash never reaches its key.
- If the stream fails but the bundle is by then stored at its key at
  `size`, another assembly of the same bundle stored it, and the answer is
  `"stored": "present"`.
- After the bundle is recorded, the platform removes the parts the
  assembly named: each object first, then the rows of the objects it
  removed. A part whose object it could not remove keeps its row, and the
  sweep removes it later.
- The answer, its audit record (`web.upload`), its log line and the
  `cufflink_web_uploads_total{result}` count are the single upload's, built
  by the same function.

The release step is unchanged: it claims the bundle from `web_uploads` by
hash, whichever route stored it.

### 3.3 Why assembly is synchronous, and runs on

Assembly reads the parts from the bucket and writes the bundle back. Both
happen next to the bucket, without the CLI's link, at the speed the
platform's own store timeouts (`OBJECT_STORE_REQUEST_TIMEOUT_SECS`, per
part since #88) already assume. An asynchronous assembly with a status to
poll would need a lease shared between replicas and recovery for a replica
that dies mid-assembly.

The assembly runs in a task of its own, which the request awaits. When the
request is dropped, the CLI's timeout or a lost connection, the assembly
still finishes, and the CLI's next attempt finds the bundle stored and is
answered `present`. Two assemblies of the same bundle at once write the
same bytes to the same key, and both answer.

### 3.4 Memory

The platform holds the multipart threshold's worth of the stream before it
decides between one write and a multipart upload, then at most four parts
in flight to the bucket and the part being cut: about 64 MiB at the
defaults, for a bundle of any size. The single upload is unchanged. The
CLI keeps the bundle as `Bytes`, as today, and sends zero-copy slices of
it.

## 4. Storage

### 4.1 Streamed writes

`ObjectStorage::upload_stream(key, data, content_type)` stores what a
stream of `Result<Bytes>` yields, on every backend:

- It gathers the stream up to the multipart threshold. A stream that ends
  there, or a write that is not an overwrite, is stored in one request, as
  `upload` stores it.
- A longer stream goes up through #88's multipart upload: the stream is cut
  into parts of the minimum part size, whatever sizes it arrives in, so
  every part but the last meets S3's and GCS's minimum, and the parts go
  up with #88's concurrency.
- An error from the stream stops the write and is returned as it was
  yielded, so a caller can tell its own refusal from a storage failure. A
  multipart upload that does not complete is aborted by #88's guard, and
  nothing is stored.

### 4.2 Parts and retention

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
  `WEB_UPLOAD_RETENTION_SECS`, objects first and then rows, as an assembly
  removes its parts.
- Deleting a service already deletes everything under `web/{service_id}/`,
  parts included, and the foreign key removes their rows.

## 5. The CLI

- `deploy_web` already asks the platform for its version and features. It
  passes whether the platform offers `web_bundle_parts` into `WebApp`, as
  `parts: Option<Parts>`.
- `Parts { threshold, size, in_flight, attempts, backoff }` defaults to
  32 MiB, 8 MiB, 4, 3 and 2 s. Tests construct smaller values. Nothing
  reads them from the environment.
- `WebApp::upload` is the one place that sends a bundle, both for the first
  upload and for the upload again after a release refused as not claimed.
  It sends one request when `parts` is `None` or the bundle is at most
  `threshold`. Otherwise it slices the `Bytes` into `size` parts, hashes
  each, sends them `in_flight` at a time and stops at the first that fails
  for good, then asks the platform to assemble them.
- A part or an assembly that gets no answer, or a `5xx`, `408` or `429`, is
  sent again after `backoff`, then twice `backoff`, up to `attempts` in
  all, and each attempt again is printed with its reason. A `4xx` other
  than `408` and `429`, the `404`/`405` of a replica mid-upgrade included,
  is final. The single request is not sent again: it is only used for
  bundles of at most 32 MiB and for older platforms.
- One function, `answered`, turns every upload request's outcome into its
  error, ending as every upload error does today:
  `Uploading part 3 of 24 of bundle {sha256} got no answer; nothing was
  released; {name} still runs v{n} (bundle {short})`, and
  `... failed ({status}): {body}...` for a refusal. Assembly errors read
  `Assembling bundle {sha256} ...` with the same endings. The `404`/`405`
  hint names a replica without the route.

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

## 6. Tests

`cufflink-storage` (`common.rs`, against an in-memory store that counts
whole writes, multipart uploads and aborts):

- A stream is cut into parts of the part size in order, whatever the sizes
  it arrives in, and an empty stream into none.
- A stream above the threshold goes up in parts, one at or below it whole,
  an empty one whole, each byte-identical with its content type.
- A stream ending in an error returns that error unchanged and stores
  nothing: below the threshold nothing is written, above it the multipart
  upload is aborted.

Platform (`platform/tests/web_releases.rs`, against the local backend):

- A bundle uploaded in three parts, assembled and released, runs. The bundle
  stored at its key is the parts' bytes in order, its parts are removed,
  and the same assembly asked again answers `present`.
- Uploading a part twice answers `present` the second time.
- A part whose bytes are not its hash is refused with `422`, an empty one
  with `400`, one above `PART_MAX_BYTES` with `413`, and one of exactly
  that size is taken.
- An assembly naming a part never uploaded is refused with `409` naming it;
  one whose part was recorded but is no longer stored likewise.
- An assembly whose parts are not the bundle's hash is refused with `422`;
  one whose part sizes do not add up with `422`; one naming no parts, more
  than 1024 parts, a malformed hash or an oversized bundle with `400` or
  `413`. No object exists at the bundle's key, and the parts are kept.
- A bundle above the multipart threshold is streamed from its parts: under
  a wrong hash nothing is completed at its key, and under its own hash it
  is stored byte-identical.
- Two assemblies of the same bundle at once both answer, and it is stored
  and recorded once.
- Assembling a bundle already stored answers `present`, reads no part and
  removes the parts it names.
- Parts past retention are removed by the sweep, and parts within it are
  kept.
- A service of another mode is refused on both routes.
- The repository's record, find, forget and expiry queries, and the
  tenant-isolation route list.
- The features test covers `web_bundle_parts` through `docs/versions.md`.

CLI (`web_release.rs`, against `commands::stub`):

- A bundle above the threshold, on a platform offering the feature, is sent
  as its parts and then assembled. The assembly names the parts' hashes in
  order, and the release names the bundle. Parts in flight together all
  reach the platform before the assembly.
- A bundle at or below the threshold, or on a platform without the feature,
  is sent in one request.
- A part that gets no answer three times releases nothing, and the error
  names the part, the bundle and what still runs. A part refused with
  `422`, `409` or `404` is not sent again.
- A part lost once and then unavailable once is sent again and stored; an
  assembly lost once is sent again and answered `present`; the release
  follows.
- A refused assembly and an assembly lost three times release nothing.
- A release refused as not claimed uploads again in parts, then releases.
- Only no answer, `5xx`, `408` and `429` are worth sending again.
- Splitting: slices cover the bundle in order and only the last is shorter.

## 7. Documentation

- `docs/versions.md`: `web_bundle_parts` in the next release's feature
  table, the compatibility table, and the release's changes, the storage
  crate's `upload_stream` included.
- `docs/deployment.md`'s web releases section: large bundles go in parts,
  and which failures are sent again.
- `docs/observability.md`: the existing `cufflink_web_uploads_total` counts
  an assembly like an upload.

## 8. Rules for the change

- No TODOs and nothing partly implemented.
- No comment that the code does not need.
- No duplicated logic: assembly reuses #88's multipart writer and abort
  guard, `require_sha256`, `require_content` and the single upload's
  answer and audit; the CLI keeps one upload entry point and one function
  for every upload request's errors.
- Idiomatic, functional Rust in the style of the surrounding code, with no
  `unwrap` outside tests.
