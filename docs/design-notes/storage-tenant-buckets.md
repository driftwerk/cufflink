# Tenant buckets: a tenant's own buckets, credentials, keys and ACLs

Proposed. Section 12 holds the questions for the maintainer, each with a
proposed default.

## Outcome

A tenant can bring buckets it already owns to its guests, with the objects
it already has in them:

- A platform admin registers a bucket to one tenant, with the key prefixes
  its guests may use, the canned ACLs they may set on upload, and, when the
  platform's own credentials do not reach it, the credential that does.
- That tenant's guests read, write, head, delete, presign, transform and
  stream objects there with the same calls they use today, under keys of the
  tenant's choosing, spaces and non-ASCII characters included, stored
  exactly as given.
- An upload may ask for a canned ACL (`private` or `public-read`), and the
  host refuses one the bucket's registration does not list.
- No other tenant can reach a registered bucket, its credential or its
  objects, whatever that tenant's `storage_scope` says and in every policy
  mode.
- Guests that use no registered bucket see no change.

## 1. What the platform does today

Read from `platform/src`, `storage/src`, `types/src` and `cufflink-fn/src`
on `main` at v0.35.0.

### 1.1 Who may use a bucket

Every guest storage call (`s3_download`, `s3_download_many`, `s3_presign_*`,
`s3_delete`, `s3_upload`, `s3_head`, the image transforms and the
`Response::s3_object` stream) goes through
`storage_scope::authorize_guest`:

- **Reserved, in every mode:** an empty bucket name or one with `/`, a key
  under `wasm/`, `backups/`, `files/`, `web/` or `native/` in the platform's
  bucket, and a `..` segment are refused.
- **The `storage_scope` policy** (default `enforce`,
  `{"scope": "legacy", "allowed_buckets": []}`):
  - with an allowlist, only its buckets;
  - without one, any bucket no other tenant group lists;
  - `strict` also confines every key to `tenants/{tenant}/{service}/` and
    requires an allowlist.

The rules are per tenant, not per bucket. A tenant whose new services should
be `strict` cannot also give a guest its existing bucket with its existing
keys: `legacy` opens every key in every listed bucket.

### 1.2 Whose credentials

There is one object store per process, built from `S3_*` (or the GCS
variables). Every guest call, on any bucket, signs with the platform's
credentials. A tenant's bucket held under another account is out of reach,
and the only way to reach it would be to give the platform's credentials
access to it. Every tenant's guests would then reach it too, held back only
by policy.

### 1.3 Keys

`cufflink-storage` turns every key into an `object_store` path with
`Path::from(key)`:

- It drops empty segments, so `a//b`, `/a` and `a/` become `a/b` and `a`.
- It percent-encodes, into the stored name itself, every non-ASCII byte and
  `% # ? * [ ] ~ " < > | { } ^` backtick, backslash and control characters.
  The request path is then percent-encoded again.

So `Café.pdf` is stored as the key `Caf%C3%A9.pdf`, and `#7 Smith.pdf` as
`%237 Smith.pdf`. A guest that writes and reads through the platform gets
its bytes back, because both directions agree. An object written by anything
else under its real name cannot be read, and one the guest writes cannot be
found by anything else. Spaces pass unchanged, so a test with spaces alone
does not show this.

### 1.4 ACLs

`storage-v2`'s `UploadOptions` carries `content_disposition` and
`cache_control`. The host builds headers from them only after checking that
neither value can add a header of its own (`no_value_can_add_a_header_of_its_own`),
so nothing can set `x-amz-acl`. `object_store` has no ACL attribute: its
`Attributes` cover content headers, storage class and user metadata.

### 1.5 Sizes

Nothing here is short of what a tenant's documents need:

- an upload is held to `HOST_STORAGE_MAX_UPLOAD_BYTES` and to what fits the
  guest's envelope (50 319 360 bytes, about 48 MiB, at the defaults);
- under `WASM_MEMORY_BUDGET_MODE=enforce` a download is refused only when
  its base64 passes `WASM_MAX_HOST_RESPONSE_MB` (64 MiB), which is about
  48 MiB of object, and `s3_download_many` holds all of its objects together
  to the same cap;
- `Response::s3_object` streams, and is not capped.

This change raises no cap. Section 9 pins them for registered buckets.

## 2. Design

### 2.1 Tenant buckets

A **tenant bucket** is a row the platform admin writes for one tenant:

| Field | Meaning |
|-------|---------|
| `bucket` | The bucket's name. One tenant per bucket across the platform (a unique index) |
| `key_prefixes` | Keys its guests may use: `null` for any key, or a list of 1 to 32 prefixes |
| `acls` | Canned ACLs an upload may set: a subset of `private` and `public-read`, empty by default |
| `credential` | `null` for the platform's credentials, or an access key id and a secret access key that reach the bucket at the platform's `S3_ENDPOINT` and `S3_REGION` |

Rows live in a new platform table, `tenant_buckets`, with the secret sealed
as every stored secret is (section 2.2). They are loaded into the tenant
catalog's snapshot with the policies and reach every replica, and the
worker, the same way: on the change notification, with the one-minute
reload as the backstop.

Refused at write (422, naming the field), so a bad row never reaches a
guest:

- a bucket name that is not 3 to 63 lowercase letters, digits, `.`, `-` or
  `_`, beginning and ending with a letter or digit, without `..`;
- the platform's own bucket (`S3_BUCKET`);
- a prefix that is empty, longer than 1024 bytes, or not an exact key
  (section 2.4), apart from a trailing `/`;
- an empty `key_prefixes` list (`null` already means any key);
- an ACL that is not `private` or `public-read`;
- a credential, or a canned ACL, on a platform whose backend is not `s3`.

Refused with `409`, in both directions:

- registering a bucket another tenant holds (naming that tenant), retired
  claims included, or one another tenant group lists in its
  `storage_scope` `allowed_buckets`;
- a `storage_scope` `allowed_buckets` that names a bucket registered to
  another tenant (`PUT /api/tenants/{slug}/policies`).

The database holds the same rules (a `CHECK` on `acls`). A row that cannot
be read as rules all the same, written by hand, is loaded as its tenant's
bucket in an unreadable state: every other tenant is refused it
(`foreign_tenant_bucket`) and its own tenant's calls are `not_configured`
until it is set again. It never fails the catalog's load and is never
dropped from it.

**Retiring and releasing.** A registration is never simply deleted, because
a bucket nobody holds is an ordinary bucket, which a `legacy` tenant with no
allowlist may use with the platform's credentials.

- `DELETE` retires it: the credential is dropped and the bucket stays
  claimed by its tenant, refused to every guest, its own tenant's included
  (`bucket_retired`). Setting it again by the same tenant revives it.
- Deleting the tenant retires its buckets in the same transaction. The
  claim is kept, with no tenant, and refused to every guest.
- An admin frees a retired bucket with `POST /api/buckets/{bucket}/release`.
  It is refused (`409`) until the bucket has been retired for twice the
  catalog's reload interval (two minutes), so every replica has loaded the
  retirement first. Only then may another tenant register it.

**Staleness.** A change reaches a replica on the change notification, or at
the latest at its next one-minute reload. Until then the replica serves the
rules it holds, which is why a retired bucket cannot change hands sooner.
A replica whose reload keeps failing keeps its last policies, and logs
every failure, but refuses every guest storage call (`not_configured`)
once its tenant buckets were loaded more than two minutes ago: a bucket
released since may already belong to another tenant.

A tenant bucket is outside `storage_scope`. Its rules are its own, and
they hold in every policy mode: no tenant relies on them yet, which is how
the platform makes every control unconditional ([policies](../policies.md)).

### 2.2 Credentials

- **Where they live.** In `tenant_buckets`, as a new `SealedColumn`
  (`TENANT_BUCKET_SECRET`: the table `tenant_buckets`, the plaintext
  `secret_access_key` beside `secret_access_key_enc`, the row id as its
  additional data, the tenant as its key's owner). They are therefore sealed
  under the tenant's data key whenever `CUFFLINK_SECRETS_KEK` is set,
  rewritten by `reseal` and checked by `verify` with the other four columns,
  and follow `CUFFLINK_SECRETS_STORAGE` (`dual` or `encrypted`).
- **Never in a policy.** `GET /api/tenants/{slug}/policies` answers a
  policy's config, and policy changes are audited with their config, so a
  credential there would be readable by anyone who can read policies.
- **Never answered.** The API answers a bucket's access key id and never its
  secret. The audit record of a change names the bucket, the prefixes, the
  ACLs and the access key id.
- **Bound to the row's tenant.** A guest call resolves a credential only
  from a row whose tenant is the guest's own (section 2.3), so no
  configuration of another tenant can lend it one.
- **One endpoint.** A credential is used at the platform's `S3_ENDPOINT`
  and `S3_REGION`. A row cannot name another host, so a row cannot make the
  platform sign requests to an address of its choosing.
- **Opened at load.** The catalog opens each secret when it loads. A secret
  that cannot be opened (a missing key, a failed authentication) is logged
  at ERROR with the bucket. That bucket's calls are refused as
  `not_configured`, and the rest of the catalog loads.

### 2.3 How a guest call on a bucket is decided

In order, for the bucket and every key of the call:

1. **Reserved checks** (section 1.1), with every bucket name validated by
   the rule of section 2.1 on every call, in every mode, before any
   lookup: any other name is refused `invalid_bucket`. Each store also
   refuses an invalid name, and a store built for a tenant bucket serves
   that bucket alone.
2. **Another tenant's bucket.** The bucket is a tenant bucket whose tenant
   is not the guest's: refused, `foreign_tenant_bucket`, in every mode,
   whatever the guest's `storage_scope` says. A preview tenant is a tenant
   of its own here (section 12, question 2).
3. **The tenant's own bucket.**
   - a retired registration is refused `bucket_retired`, and an unreadable
     one `not_configured`;
   - every key must be exact (section 2.4): refused `key_not_exact`;
   - and lie under one of `key_prefixes` when that is set, on a segment
     boundary: the prefix `docs` holds `docs` and `docs/a`, never
     `docs-old/a`, and `docs/` holds what is under it. Refused
     `key_outside_prefix`;
   - and `storage_scope` is not consulted. That includes `strict`'s
     `tenants/{tenant}/{service}/` prefix: a registered bucket keeps its
     own layout, and the registration's prefixes are the limit (section
     12, question 8).

   The call then uses the bucket's credential, or the platform's, with keys
   stored exactly.
4. **Any other bucket:** `storage_scope` as today, with the platform's
   credentials and keys as today.

Refusals are recorded as every `storage_scope` refusal is
(`cufflink_policy_violation_total{key="storage_scope", outcome="denied"}`,
the reason as its code).

**One function decides and hands over the store.** `authorize_guest` is
replaced by a resolver that answers the object store the call must use, or
its refusal. No call site reads `AppState::storage` for a guest any more, so
none can authorize against one bucket's rules and then use another store.
Section 2.6 lists the call sites.

### 2.4 Exact keys

In a tenant bucket a key is stored and sent exactly as the guest gives it.
A key is exact when `object_store` can address it without changing it:

- it is 1 to 1024 bytes;
- it does not begin or end with `/`, and has no empty segment (`//`);
- no segment is `.` or `..`;
- it has no ASCII control character.

Anything else is allowed: spaces, non-ASCII letters, `%`, `#`, `?`, `&`,
`+`, `~`, brackets and quotes.

`cufflink-storage` gains an exact-keys mode. It builds the path with
`Path::parse`, which does not encode, and checks that the path's text is the
key. `object_store` then percent-encodes the request path once, so the
store receives the key itself. The exact rule lives in `cufflink-storage`
and the resolver calls it, so the check and the address cannot differ.

The platform's buckets and `storage_scope` buckets keep today's encoding
(section 8.3).

### 2.5 ACLs on upload

- **Wire.** `UploadRequest` gains `acl`, one of `"private"` and
  `"public-read"`, left out when unset. The request already has
  `deny_unknown_fields`, so an older platform refuses an upload that names
  an ACL (`denied`) rather than storing it without one.
- **Admission.** An upload that names an ACL is admitted only to a tenant
  bucket of the guest's own tenant whose `acls` list it. Anything else is
  refused `acl_not_admitted`, in every mode: a `storage_scope` bucket, the
  platform's bucket, or a tenant bucket that does not list the ACL. An
  upload with no ACL is admitted as today.
- **Per bucket, not per tenant.** A tenant may want public images in one
  bucket and private documents in another. With a tenant-wide switch, any
  of its guests could publish any document.
- **Applied, or refused.**
  - On `s3`, the upload is a single `PUT` with `x-amz-acl`, sent from a
    client built for that bucket and ACL. `object_store` signs its default
    headers, so the header is part of the signature.
  - The `gcs`, `memory` and `local` backends cannot apply a canned ACL.
    There an upload that names one is refused (`backend`, "this backend
    stores no canned ACL"), never stored without it.
- **No free-form value.** The closed enum keeps every header the guest can
  influence a header value the host builds itself.
- **Presigned uploads.** A presigned `PUT` names no ACL. AWS S3 refuses a
  request whose `x-amz-*` headers are not all signed, so the holder of the
  URL cannot add `x-amz-acl`. An S3-compatible store that accepts unsigned
  `x-amz-*` headers on a presigned request would let it: on such a store,
  block public ACLs in the bucket's policy for every principal but the
  platform's uploads.

### 2.6 Every path that reaches storage for a guest

| Call | Today | With this change |
|------|-------|------------------|
| `s3_download`, `s3_download_many` (each item) | `authorize_storage`, then `app_state.storage` | the resolver's store; `download_many` resolves each item on its own |
| `s3_presign_upload`, `s3_presign_download` | the same | the resolver's store, so a presigned URL is signed with the bucket's credential |
| `s3_delete` | the same | the resolver's store |
| `s3_upload`, `s3_head` (`storage-v2`) | `guest_storage` | the resolver's store, and ACL admission for uploads |
| `image_transform_*`, `image_transform_multi` | `authorize_storage`, then `app_state.storage` | the resolver's store for the source and every target |
| `Response::s3_object` (`router/custom/stream.rs`) | `authorize_guest`, then `state.storage` | the resolver's store |

The worker runs guests too. It loads the same catalog and so resolves the
same way.

### 2.7 Sizes

Registered buckets take the caps of section 1.5 unchanged:

- a download of 25 MiB is admitted under `enforce`, and so is a
  `download_many` whose objects together reach that;
- an upload up to the envelope's cap of about 48 MiB is stored;
- `s3_object` streams an object of any size.

## 3. API and CLI

Platform admin only, like every tenant policy route.

| Method | Path | Body / result |
|--------|------|---------------|
| GET | `/api/tenants/{slug}/buckets` | `{"tenant", "buckets": [{"bucket", "key_prefixes", "acls", "credential": null \| {"access_key_id"}, "retired_at", "updated_at", "updated_by"}]}`, retired buckets included |
| PUT | `/api/tenants/{slug}/buckets/{bucket}` | `{"key_prefixes"?, "acls"?, "credential"?: {"access_key_id", "secret_access_key"}}`. It replaces the row: what is left out takes its default, and a left-out `credential` means the platform's. `200` with the row as GET answers it; `409` and `422` as section 2.1 |
| DELETE | `/api/tenants/{slug}/buckets/{bucket}` | Retires it (section 2.1): `204`, again for a retired one; `404` when the tenant has no such bucket |
| POST | `/api/buckets/{bucket}/release` | Frees a retired bucket: `204`; `409` while it is registered, or retired for less than two minutes; `404` when nobody holds it |

Each change is audited (`tenant.bucket.set`, `tenant.bucket.delete`,
`tenant.bucket.release`) and announces a tenant change, so every replica
reloads.

```sh
cufflink tenants buckets list harbor
cufflink tenants buckets set harbor example-documents --key-prefix leads/
printf '%s' "$SECRET" | cufflink tenants buckets set harbor example-assets \
  --acl public-read --access-key-id AKEXAMPLE --secret-access-key-stdin
cufflink tenants buckets delete harbor example-documents
cufflink tenants buckets release example-documents
```

The CLI reads a secret from stdin only, never from an argument, which would
show in the shell's history and the process list.

## 4. SDK

- **A cargo feature, `storage-acl`.** It enables `storage-v2` and adds
  `UploadOptions::acl(CannedAcl)`, with `CannedAcl::{Private, PublicRead}`
  re-exported from `cufflink_types::host::storage`. It writes `storage_acl`
  into the module's `cufflink.features`, so a deploy to a platform without
  ACLs is refused with a 422, as for every other cargo feature. Nothing else
  in the SDK changes: tenant buckets are configuration, and a guest calls
  `storage::*` on them as on any bucket.
- **The test host.** `InMemoryStorage`:
  - stores keys exactly, as it does today;
  - records the ACL on `StoredObject`;
  - refuses an upload naming an ACL that `admit_acl(bucket, acl)` has not
    admitted, as the platform does, so a guest's tests fail where its
    tenant's registration would be missing;
  - records every call in `calls()`, as `MockHttp::requests` and
    `MockExtensions::calls` do, so a test can assert what was read, written and
    deleted.

```rust,ignore
let stored = storage::upload_with(
    "example-assets",
    &format!("logos/{branch_id}/Café façade.png"),
    "image/png",
    &png,
    &UploadOptions::new()
        .cache_control("public, max-age=86400")
        .acl(CannedAcl::PublicRead),
)?;
```

## 5. Features and compatibility

| Feature | Needed when | Provides |
|---------|-------------|----------|
| `tenant_buckets` | never demanded | buckets registered to a tenant, with their credentials, key prefixes, canned ACLs and exact keys |
| `storage_acl` | the guest enables `cufflink-fn/storage-acl` | a canned ACL on `storage::upload_with`, admitted per tenant bucket |

- **Existing guests and tenants.** Nothing changes for a guest that uses no
  tenant bucket. The existing host functions keep their signatures, and an
  upload's request is unchanged when it names no ACL.
- **A new SDK on an old platform.** A module with `storage-acl` is refused
  at deploy. A module built with an ACL on an older SDK path cannot exist,
  because `acl` is only reachable through the feature.
- **Rolling the platform back** below this release:
  - `tenant_buckets` stays in the database, unread. Calls on those buckets
    fall back to `storage_scope` with the platform's credentials. Under a
    `strict` allowlist that does not list them, which is what section 6
    recommends, they are refused. Under `legacy` with an empty allowlist the
    older platform admits them, as it did before this release.
  - Keys are encoded again (section 1.3), so objects written under exact
    keys that the encoding changes cannot be read until the platform is
    back.
  - ACL uploads are refused, since `acl` is an unknown field there.
  - The older platform logs the missing migration at each start, as for
    earlier migrations.
  - `reseal` and `verify` on the older binary do not know
    `tenant_buckets`, so they skip its secrets: run them from this release
    before rolling back, and again after returning to it.

  So before rolling back: roll back the services that enable `storage-acl`,
  and set the tenant's `storage_scope` to `strict` with an allowlist if its
  guests must not reach other buckets on the older platform. The
  `versions.md` table records this.
- **Why no `storage_scope` key.** `StorageScopeConfig` has
  `deny_unknown_fields`, and a config the platform cannot read is replaced
  by the builtin default (`legacy`, no allowlist), with a warning. A
  `storage_scope` row carrying a new key would therefore open storage
  wide on any older replica during a rolling upgrade or after a rollback.
  A table the older platform does not read fails closed instead.

## 6. Recommended configuration

For a tenant that keeps its own buckets and runs new services beside them:

```sh
cufflink tenants policy set harbor storage_scope enforce \
  --config '{"scope":"strict","allowed_buckets":["harbor-data"]}'
cufflink tenants buckets set harbor example-documents --key-prefix leads/ \
  --access-key-id AKEXAMPLE --secret-access-key-stdin < documents.secret
cufflink tenants buckets set harbor example-assets --acl public-read \
  --access-key-id AKEXAMPLE --secret-access-key-stdin < assets.secret
```

New services stay under `tenants/harbor/{service}/` in `harbor-data`. The
registered buckets keep their own layout. An older platform refuses the
registered buckets outright, because the allowlist does not name them.

## 7. Multi-tenant invariants

These are what the tests of section 9 prove:

1. A guest of tenant B never reaches a bucket registered to tenant A, in any
   policy mode and whatever B's `storage_scope` lists. That covers every
   call, presigned URLs and `s3_object` included.
2. A guest of tenant B is never handed a store built from A's credential.
   The resolver opens a credential only from a row whose tenant is the
   guest's, and only after step 2 of section 2.3.
3. A credential's secret is never answered by an API, logged, audited or
   put in a metric label, and is never read from a command-line argument.
4. An upload never sets an ACL its bucket's row does not list, and never
   stores an object without the ACL it asked for.
5. A key is never stored under a name other than the one the guest gave in
   a tenant bucket. One that cannot be stored exactly is refused.
6. The reserved checks (platform prefixes, traversal, bucket names) run
   before any tenant-bucket rule, so no registration can open a platform
   prefix. The platform's own bucket cannot be registered.
7. Registration is exclusive. A bucket belongs to one tenant, and a bucket
   another tenant group lists in its `allowed_buckets` cannot be registered
   until that listing is removed, nor listed while it is registered.
8. A registration never lapses into an ordinary bucket by itself: deleting
   it, or its tenant, leaves a retired claim refused to every guest until an
   admin releases it, and only after every replica has loaded the
   retirement.

## 8. Alternatives considered

### 8.1 New keys in `storage_scope` (per-bucket entries, `allow_acls`)

This was the first proposal. It keeps one place for storage rules, but:

- it fails open on an older replica (section 5);
- it has no safe home for a credential (section 2.2);
- it ties the rules to the policy's mode, so `warn` would let a guest
  publish what `enforce` refuses.

### 8.2 Credentials from the platform's environment

`STORAGE_PROFILE_<NAME>_*` variables would bind a credential to buckets
and tenants. Credentials would stay out of the database, but:

- every change is a restart of every replica and the worker;
- the binding to tenants would sit apart from the tenant it serves;
- it would add a second way to configure a tenant that `cufflink tenants`
  does not show.

The sealed column reuses the key rotation, `reseal` and `verify` every
other stored secret has.

### 8.3 Exact keys for every bucket

Changing `Path::from` for every bucket would correct section 1.3
everywhere. But objects already stored under encoded names in the
platform's and `storage_scope` buckets would stop being found. Exact keys
are therefore opt-in per bucket, by registering it. A later release can
offer a migration for the others.

### 8.4 A guest-side S3 client

A guest could sign its own requests with `http-v2`. It would then hold the
tenant's credential in its configuration, implement SigV4 itself, and be
held only by the tenant's egress policy. The platform could neither confine
its keys nor keep another of the tenant's services from using the
credential.

## 9. Tests

**`cufflink-storage`, against `wiremock` as the S3 endpoint:**

- an exact key with spaces, non-ASCII, `%`, `#`, `?` and `+` is sent as
  that key, percent-encoded once on the wire, for `PUT`, `GET`, `HEAD`,
  `DELETE` and a presigned URL;
- a key that is not exact is refused before any request;
- the encoded mode still sends today's paths (a pinned example);
- `x-amz-acl: public-read` is sent, listed in `SignedHeaders`, only on the
  `PUT` that asked for it;
- a credential signs as its own access key id;
- `gcs`, `memory` and `local` refuse an ACL.

**Platform, in-crate:**

- the resolver, case by case for section 2.3: each step, each code, each
  mode (`off`, `warn` and `enforce` must agree for tenant buckets);
- invariant 1 for every call of section 2.6 through the one resolver;
- `storage_scope` buckets unchanged (the existing tests stay as they are);
- ACL admission for each case of section 2.5;
- validation of a row (section 2.1), and the `409`s;
- a snapshot with an unopenable secret, or an unreadable row, loads, and
  that bucket is `not_configured` to its tenant and refused to every other;
- every name outside the bucket naming rules is `invalid_bucket` on every call, in every
  mode;
- a retired or orphaned claim is refused to every tenant;
- `TENANT_BUCKET_SECRET` is in `columns::ALL`, so `reseal` and `verify`
  cover it;
- under `enforce` caps, a 25 MiB object passes `fetch_object` and
  `download_many`, and an upload at `upload_cap` is admitted.

**Platform, integration (`platform/tests/tenant_buckets.rs`):**

- tenants `harbor` and `globex` on a platform whose `S3_ENDPOINT` is an S3
  stand-in served by the test;
- `harbor` registers `example-documents`, with a credential and a `leads/`
  prefix, and `example-assets`, with `public-read` on the platform's
  credentials;
- through a deployed guest:
  - upload, head, download, `download_many`, delete and `s3_object` under a
    key with spaces and non-ASCII, read back from the stand-in under that
    exact key and signed with the bucket's access key id;
  - an ACL upload carries the header;
  - a key outside `leads/` is refused, and so is an unlisted ACL;
- `globex`, with `storage_scope` `legacy` and its allowlist naming
  `example-documents`, is refused on every call;
- `globex`, with its allowlist naming `example-assets` (written before
  the registration, as the API refuses it after), is refused on every call
  there too, presigned uploads, image transforms and `s3_object` included;
- the API never answers the secret, `409` and `422` as specified, and a
  change reaches the guest without a restart;
- a delete retires the bucket; another tenant cannot register it until it
  is released, nor release it before the grace; a deleted tenant's bucket
  stays claimed;
- the guest's SDK tests (`test-guests`) exercise `storage-acl` against the
  test host.

## 10. Documentation, when implemented

- `docs/tenants.md`: tenant buckets, the API and the CLI.
- `docs/policies.md`: how tenant buckets sit beside `storage_scope`, and
  the reasons the resolver refuses with.
- `docs/tenant-isolation.md`: the storage section and section 6's
  recommendation.
- `docs/wasm-guide.md`: `storage-acl`, `UploadOptions::acl`, exact keys in
  tenant buckets, and the sizes of section 2.7.
- `docs/testing.md`: `admit_acl` and `calls()`.
- `docs/observability.md`: `cufflink_host_storage_v2_total` gains
  `acl_not_admitted` and `key_not_exact` refusals under `denied`, and
  `cufflink_host_storage_acl_total{acl, result}` counts uploads that name an
  ACL.
- `docs/versions.md`: the two features, the changes and the rollback table,
  as unreleased.

## 11. Not in this change

- Exact keys for the platform's and `storage_scope` buckets (section 8.3).
- A credential at another endpoint or region than the platform's.
- Canned ACLs other than `private` and `public-read`, and ACLs on GCS.
- A v2 `download`, `delete` or `download_many` that answers a
  `StorageError` with a kind. Today `download` answers `None`, `delete`
  `false` and `download_many` a message. A guest that must tell "missing"
  from "refused" calls `head`, which already does.
- Caching the S3 clients built for buckets other than the platform's.
  Building one per call is what the platform does today for every other
  bucket.

## 12. Questions for the maintainer

| # | Question | Proposed default |
|---|----------|------------------|
| 1 | Should tenant buckets be a resource of their own, outside `storage_scope` (section 8.1)? | Yes |
| 2 | Should a preview tenant reach the tenant buckets of the tenant it was cloned from? Previews share their source's `storage_scope` buckets today | No. A preview runs code that has not been reviewed. Register a bucket for the preview if it needs one |
| 3 | Should credentials be stored sealed in the platform's database (section 2.2) or come from the environment (section 8.2)? | Sealed in the database |
| 4 | Should a credential be bound to the platform's `S3_ENDPOINT` and `S3_REGION` (section 2.2)? | Yes |
| 5 | Should tenant-bucket rules hold in every policy mode, rather than follow `storage_scope`'s mode? | Yes |
| 6 | Should an ACL upload on a backend that cannot apply it be refused rather than stored without it? | Refused |
| 7 | Should the API answer a credential's access key id? | Yes, never its secret |
| 8 | Should a registered bucket also hold `strict`'s `tenants/{tenant}/{service}/` prefix? | No, on purpose: a registered bucket keeps the layout it already has, and its `key_prefixes` are its limit. A tenant that wants a prefix per service registers one per service's prefix |
