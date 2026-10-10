# Tenant Policies and Isolation

Cufflink hosts several tenants on one platform. Every isolation control that
could change what an existing tenant observes is a **policy**: a
`(key, mode, config)` triple per tenant, layered over a platform default, so
it can be soaked in `warn` and flipped to `enforce` per tenant without a
restart or a deploy. Controls that no existing tenant relies on are
unconditional and listed at the end.

## Modes

| Mode | Behaviour |
|------|-----------|
| `off` | As before the policy existed; nothing is evaluated or recorded |
| `warn` | Evaluated; a violation is recorded (metric + log) and allowed |
| `enforce` | Evaluated; a violation is recorded and refused |

Every violation increments
`cufflink_policy_violation_total{key, tenant, outcome, reason}` (`outcome` is
`warned` or `denied`; `reason` is a short fixed code such as `realm_mismatch`)
and logs a `warn` under target `cufflink::policy`, at most once a minute per
`(key, tenant, reason)`.

## Keys

| Key | Default | Config (default) | What it governs |
|-----|---------|------------------|-----------------|
| `tenant_binding` | `warn` | `{}` | Principals on data routes (CRUD, batch, GraphQL, SSE, `_fn`, job submission) must belong to the route's tenant. A tenant-realm JWT without `X-Tenant-Slug` is bound to the path tenant when that tenant shares its realm (previews). Mismatches: `header_mismatch`, `admin_header_mismatch`, `api_key_foreign_tenant`, `realm_mismatch`, `path_tenant_has_no_realm`; under `warn` the mismatch is recorded and the request continues unauthenticated (an `auth_required` table answers 401 and a guest receives no claims), under `enforce` it is refused with 403, and a public or legacy `_fn` route then serves the caller anonymously. `off` binds nothing: every principal stays exactly as authenticated. Caller credentials (`authorization`, `cookie`, `proxy-authorization`) reach a guest only on routes declaring `forward_auth` once enforced: under `warn` a route without it still receives them and each such request is recorded as `auth_header_forwarded`; under `enforce` they are stripped without recording a violation, since that is the policy at work on every request a browser makes, and counted in `cufflink_route_credentials_stripped_total` |
| `graphql` | `off` | `{}` | GraphQL is served only for services whose manifest sets `graphql: true`; `warn` counts use, `enforce` turns it off for the tenant (404) |
| `storage_scope` | `enforce` | `{"scope": "legacy", "allowed_buckets": []}` | Guest storage host functions. With `allowed_buckets` the guest is confined to those buckets; without, it may not use a bucket another tenant group lists. A tenant group is a source tenant and the previews cloned from it: a bucket any of them lists counts as listed by all. `strict` additionally confines keys to `tenants/{tenant}/{service}/` and requires an allowlist |
| `egress` | `warn` | `{"allow_hosts": [], "max_response_bytes": null}` | Outbound HTTP on the tenant's behalf (guest `http::*`, platform webhooks, manifest webhook handlers): private, loopback, link-local and other internal addresses (`private_address`) unless the host matches an `allow_hosts` entry (`host` or `*.suffix`). `max_response_bytes` can lower the platform's response cap |
| `wasm_limits` | `warn` | `{"memory_bytes": null}` | Guest memory growth past the cap: `WASM_MAX_MEMORY_MB`, or the tenant's `memory_bytes` (at least 64 MiB) when lower (`WASM_MAX_MEMORY_MB` is the install-wide ceiling). `warn` lets it grow, `enforce` fails the handler with a 500 naming the limit. The pre-policy `CUFFLINK_WASM_LIMITS` is no longer read; a platform that has it set warns at startup |
| `service_accounts` | `warn` | `{}` | Which tokens bypass per-user permission checks: clients (`azp`) on the tenant's `service_account_clients`; under `off`/`warn` also `CUFFLINK_SERVICE_ACCOUNT_CLIENTS` (recorded as `env_fallback` under `warn`) |
| `audience` | `warn` | `{"allowed": []}` | Tenant JWTs must name an allowed client in `aud` or `azp` (401 under `enforce`; an empty list checks nothing, and `enforce` with an empty list is refused) |
| `redis_scope` | `warn` | `{}` | Guest Redis keys: `warn` writes `t:{tenant_id}:{key}` and reads the unprefixed key when the prefixed one is missing (`unprefixed_fallback`), a delete never removes the unprefixed key, which may be another tenant's, but leaves a tombstone in the prefixed one for as long as the unprefixed key lives, so no later read falls back to it; `enforce` uses the tenant prefix only; `off` uses raw keys |
| `outbox` | `off` | `{}` | How a tenant's data events, audit records and platform webhook deliveries leave its database. `enforce`: written to the outbox of the tenant's database in the transaction that writes the row, and published from there in commit order; the audit records are kept as well ([below](#durable-audit-and-webhook-deliveries)), and a webhook is delivered until its receiver takes it. There is no `warn` mode (a shadow write would publish twice) |
| `db_roles` | `off` | `{"readers": [], "login_pool_max": null}` | The Postgres role a service's SQL runs as ([below](#database-roles)). `warn`: the service's role inside the platform's connection, falling back to the platform's role when the service's is refused (recorded as `role_refused`; a service without a verified role runs as the platform, recorded as `no_verified_binding`). `enforce`: a connection that logs in as the service's role, `login_pool_max` of them per service (4 when unset), and `cufflink sql` (`POST /api/services/{id}/query`) runs as the service's read-only role, or its role where its binding names none, never as the platform; refused for a tenant while any of its services that has a schema lacks a verified role. `readers` are roles that may read every service schema of the tenant |
| `schema_guard` | `off` | `{"accept_extra_columns": true}` | What a deploy does where the database holds more than the manifest says. `warn` reports drift and the objects that depend on what a change would drop; `enforce` refuses them. `accept_extra_columns: false` reports a column the manifest does not declare |
| `strict_filters` | `off` | `{}` | CRUD filter v2 ([setup](setup.md#filter-v2)). `off`: v2 only on `?filter_mode=v2`. `warn`: every v1 listing that v2 would refuse is recorded (reason `would_reject`), the soak signal before enforcing. `enforce`: every listing is v2 (the parameter cannot opt out) and write bodies are type-checked (400 naming the column) instead of coerced |

A new tenant created with `clone_rbac_from` (previews) inherits the source's
policy rows, `service_account_clients` and `keycloak_issuer`, and joins the
source's tenant group (`source_tenant_id`, always the group's root).

## Setting policies

Platform defaults come from `CUFFLINK_POLICY_<KEY>` (mode) and
`CUFFLINK_POLICY_<KEY>_CONFIG` (JSON), e.g. `CUFFLINK_POLICY_GRAPHQL=enforce`.
A malformed value stops the platform at startup. Per-tenant rows override
them and reach every replica within seconds (a change notification, with a
one-minute reload as the backstop).

```sh
cufflink tenants policy get harbor
cufflink tenants policy set harbor storage_scope enforce \
  --config '{"scope":"strict","allowed_buckets":["harbor-data"]}'
cufflink tenants policy set default egress warn --config '{"allow_hosts":["keycloak","*.example.com"]}'
cufflink tenants policy reset default egress
cufflink tenants update default --service-account-clients orchard-ops
```

`set` without `--config` keeps the policy's current config. The same
operations over HTTP (platform admin only):

| Method | Path | Body / result |
|--------|------|---------------|
| GET | `/api/tenants/{slug}/policies` | `{"tenant", "policies": {key: {mode, config, source}}}` with `source` `tenant` or `platform_default` |
| PUT | `/api/tenants/{slug}/policies` | `{"policies": {key: {"mode", "config"?}}}`; all entries validated (422 naming the key; an unknown field such as a misspelt `config` is refused too) and written in one transaction |
| DELETE | `/api/tenants/{slug}/policies/{key}` | Back to the platform default; 404 when the tenant has no row for it |

## Database roles

With `db_roles` off every service of a tenant runs its SQL as the role the
platform connects as, which can reach every schema of the tenant's database.
Once the policy leaves `off`, each service has a role of its own:

- A service is given the role `svc_{tenant}_{schema}_{digest}` at its next
  deploy (before the deploy changes anything, so its `on_migrate` hook runs
  as the role too), where the digest is of the tenant's and the service's
  ids, so no two services of any tenants share a name: a login role whose
  password the platform mints and keeps sealed, owner of the service's
  schema and of everything in it, granted `_cufflink` and the outbox
  function, read by the `readers` now and by default privilege later, and
  given the tenant pools' `statement_timeout` as its own default. It needs
  `CUFFLINK_SECRETS_KEK` (under `warn` too), the platform's role needs
  `CREATEROLE` and never superuser, and tenant pools reached directly need
  `DB_POOL_RECYCLE=clean` (the default): the policy is refused otherwise,
  since a guest statement cut short could leave its role set on a pooled
  connection. On PostgreSQL 16 and later the platform may grant a
  `read_only_role` only once its role holds it `WITH ADMIN OPTION`; the
  refusal names the statement.
  The platform marks a role it makes with a comment naming the tenant and
  the service, in the transaction that makes it, and changes no role
  without that mark: a role of the name that exists already is refused,
  never given a password.
- A service whose schema belongs to a role that exists already is bound to
  it by an admin. The platform changes nothing of such a role but its
  grants on `_cufflink`, never its password; the verification names what
  is missing and the statement that mends it. A role bound to another
  service, the platform's own, a superuser and `postgres` are refused.
- Verification checks, besides what the role reaches, that it is none of
  `SUPERUSER CREATEDB CREATEROLE REPLICATION BYPASSRLS`, a member of no
  role that is or of the platform's, and cannot create in `public` (which
  every PostgreSQL 14 database lets `PUBLIC` do; the platform takes that
  back in the databases it made). A login that could not be tried (the
  server was not reached, or had no connection to give) leaves what was
  known of the binding and counts `cufflink_db_role_verify_unanswered_total`;
  only a login Postgres refuses unverifies it.
- A service's role puts rows in the outbox through
  `_cufflink.outbox_enqueue_own`, which takes the service from the role
  that calls it, never from an argument; it may not call
  `outbox_enqueue_many` nor touch the outbox's tables.
- Deleting a service makes the role the platform made for it `NOLOGIN`;
  the role keeps what it owns, since the schema stays.

```sh
cufflink services db-role set harbor catalog-service --role catalog_service_user --password-stdin
cufflink services db-role set default reports-service --read-only-role reports_ro
cufflink services db-role verify default reports-service   # non-zero until verified
cufflink services db-role rotate default reports-service   # a new password for a managed role
cufflink services db-role show default reports-service
```

| | `warn` | `enforce` |
|---|---|---|
| Guest SQL and data routes | The platform's connection with `SET LOCAL ROLE` to the service's role. A statement Postgres refuses the role (SQLSTATE `42501`) runs again as the platform and is recorded with Postgres's message, never the statement | A connection from the service's own pool, logged in as the role: `RESET ROLE`, `SET ROLE` and `SET SESSION AUTHORIZATION` give a statement nothing the role does not have |
| A deploy's schema changes | In transactions on the platform's connection with `SET LOCAL ROLE`, with the same fallback | In transactions on the platform's connection with `SET LOCAL ROLE`, no fallback; a refusal fails the deploy. The `on_migrate` hook logs in as the role |
| `db::query_read_only` | As `DB_READ_ONLY_ROLE`, as before; what that role is refused is refused | As the binding's read-only role, or the service's own role where it has none, read-only either way |
| A service without a verified role | Runs as the platform (recorded once a minute) | 503 for its guest and data routes; its deploy runs, and makes the role |

Under `warn` every statement of a transaction runs behind a savepoint, so
that a refusal can be undone: a round trip or two more for each statement,
and a subtransaction for each that writes. A statement that sets the
isolation of its transaction is run in the transaction itself; one that
sets it read-only (`SET TRANSACTION READ ONLY` as a statement of its own)
sets that for itself alone, where the SDK's `transaction_with` sets it for
the transaction. `warn` is for the weeks in which the violations are read
and mended, not for good.

Under `enforce` a replica holds up to `login_pool_max` connections for each
service that is used, beside the tenant's own pool, in the platform and in
the worker. These logins reach the server directly, never through a
transaction pooler (`TENANT_DB_POOLER`), with the platform's statement
timeout; the verification logs in the same way.

**Trust boundary.** Under `enforce` a service's guest SQL, data routes and
`on_migrate` hook run on a login as its role, which no statement can leave.
A deploy's schema changes are generated by the platform from the manifest
and run on the platform's own login with the role set: a statement there
that said `RESET ROLE` would be the platform, whose role may change every
service role's password. Whoever may deploy (`DeployAuth`) is therefore
trusted with the platform's database role; guest code is what `enforce`
confines. SQL an author writes into a deploy is to log in as the role
before it runs under `enforce`.

A role's catalog is readable to it as to every role: other roles' and
schemas' names, and the bodies of other schemas' functions.

A service's role is a member of its binding's `read_only_role` with `SET`
(and without `INHERIT`): `db::query_read_only` runs on the service's own
login, which can take on another role only through such a membership. Any
statement of the service can therefore `SET ROLE` to it, not only
`query_read_only`, and reach what the read-only role reaches, read-only or
not. Bind a read-only role that reaches no more than the service may read
in its tenant's database: whatever other services' schemas the role
reaches, the service's every statement reaches too. Running
read-only queries on the platform's connection instead would not confine
them better: there a guest's `set_config('role', …)` reaches the platform's
role.

Taking a reader out of `readers` does not take back what it was granted:
`REVOKE ALL ON ALL TABLES IN SCHEMA <schema> FROM <reader>`, `REVOKE USAGE
ON SCHEMA <schema> FROM <reader>` and `ALTER DEFAULT PRIVILEGES FOR ROLE
<service role> IN SCHEMA <schema> REVOKE SELECT ON TABLES FROM <reader>`
do. To hand a schema back to the platform after `db_roles` is turned off,
run `REASSIGN OWNED BY <service role> TO <platform role>` in the tenant
database.

Ad-hoc SQL (data fixes) belongs to a login of its own that is a member of
the service roles it needs, not to the platform's role.

## Durable audit and webhook deliveries

With `outbox: enforce` a write is a transaction that holds its row and,
in the tenant's outbox, what the row publishes: its event, its audit
record as `dw.{tenant}.logs` has always carried it, the same record for
`cufflink.audit.{tenant}.{service}`, and a delivery for every webhook that
is registered for the event.

**The durable record** is kept on the stream `CUFFLINK_AUDIT` for
`CUFFLINK_AUDIT_MAX_AGE_DAYS`. It holds what the legacy record holds, and:

| Field | What it says |
|---|---|
| `v` | `2` |
| `user_id` | the user an API key was minted for (`POST /api/auth/device/token`), where the legacy record names the tenant's owner as it always did; otherwise the same |
| `actor` | `{"sub", "kind", "key_id", "azp"}`: who asked, as a `user`, a `service_account`, an `api_key`, a `partner_key`, a `service` (the service whose call the request continues) or `anonymous` |
| `request_id` | the request's `x-request-id` |
| `scope` | the widest scope the caller's rows were confined to, `null` on a table without scopes |
| `redacted` | the columns whose values the record does not hold |
| `previous` | of an update of a `sensitive` table, the row as it was: its declared columns, as `data` holds them |

A table's `audit_redact` names what of its rows no audit record holds: a
column by its name or, with a final `*`, by what its name begins with, and
inside a JSON column the keys that lead to a value joined by `.`
(`profile.dob`), a key whose value is an array followed by `[]`
(`contacts[].email`, `tags[]`). Both records hold `"[redacted]"` in the
place of what a rule names, whatever it held, and the durable one lists
the paths that were redacted. Names are matched as they are written (`Dob`
is not `dob`), a rule that books through something other than an object,
or with `[]` through something other than an array, names nothing there,
and `record_id` is never redacted (a key that is an email is recorded as
it is). A custom route's `audit_redact` names what of its request body
the record of a call holds not, in the same way; the body of a call
without a caller is not recorded at all.

Redaction is of audit records alone. The event on
`dw.{tenant}.{service}.{table}.*`, the body of a webhook delivery, what
`CUFFLINK_WEBHOOKS` stores, and the dead letters of both carry the row
whole.

A `sensitive` table has its reads recorded too, on the durable subject
alone: a listing, a read of one row, a GraphQL query and an SSE stream
that is opened each make a record whose `action` is `read` and whose
`data` is `{"record_ids": [...], "returned": n, "truncated": bool,
"query": {"filter_columns": [...], "search": bool, "sort": "..."}}`. It
names at most 100 rows, and the columns a listing was filtered by, never
the values; `sort` is as the caller sent it. A read that cannot be
recorded is refused, and a read that is refused (403, 404) is not
recorded.

A record of every read is what it costs: under `enforce` each listing,
read of a row, GraphQL query and SSE stream of a `sensitive` table is a
row of the outbox (with its `NOTIFY`), a publish, and a message kept for
`CUFFLINK_AUDIT_MAX_AGE_DAYS` within `CUFFLINK_AUDIT_MAX_BYTES` (1 GiB by
default, the oldest records going first). A table read often by the same
callers is better recorded with `CUFFLINK_AUDIT_READ_COALESCE_SECS`, which
counts the reads like one recorded into the next record, or sampled
(`CUFFLINK_AUDIT_READ_SAMPLE_PERCENT`); both are off by default, and each
read they leave unrecorded is counted
(`cufflink_audit_reads_unrecorded_total`).

`CUFFLINK_AUDIT_DURABLE_ALL=true` keeps the records of every tenant,
those of a tenant without an outbox being published once its write is
committed.

**A webhook delivery** is sent as the dispatcher sends it (the same body,
`Content-Type`, `X-Cufflink-Event`, `X-Cufflink-Signature` and
`X-Cufflink-Signature-256`), with the id of its outbox row as
`X-Cufflink-Delivery`, which is the same at every attempt. A receiver that
answers 2xx has taken it. One that answers 408, 429 or 5xx, or cannot be
reached, is tried again, after 10 s doubling to an hour or after the
`Retry-After` it answers when that is an hour or less, eight times in all;
any other answer, an address the tenant's `egress` policy refuses, and a
webhook with no secret to sign with end the delivery at once. A delivery that is given up is kept in the
dead-letter queue (`cufflink.dlq.{tenant}.{service}.webhook.{webhook_id}`).
`GET /api/services/{id}/webhooks/{webhook_id}/deliveries` lists what became
of each, for `WEBHOOK_DELIVERY_RETENTION_DAYS` (30) after it was last
attempted.

An attempt takes at most `WEBHOOK_DELIVERY_TIMEOUT_SECS` (10 s), the
receiver's answer read included; an answer whose body does not come in
that time is judged by its status. A replica makes at most four deliveries
to one webhook and eight of one tenant at once
(`WEBHOOK_DELIVERY_PER_WEBHOOK`, `WEBHOOK_DELIVERY_PER_TENANT`), so a
receiver that stalls holds up its own deliveries and no other tenant's; a
delivery that finds no room waits about as long as an attempt, which
counts as none of its eight. Deliveries are not ordered: several of a
webhook are made at once, and one that is tried again is overtaken by
those after it. A receiver orders by what the event holds and takes a
delivery once by its `X-Cufflink-Delivery`; one it took is not sent again
by the platform, which records it as delivered.

Under `enforce` the webhooks of a tenant fire for the rows the data routes
write. An event a guest publishes itself on
`dw.{tenant}.{service}.{table}.created` fires none, where under `off` the
dispatcher cannot tell it from a row's.

### Rows the platform wrote

A guest's SQL can call the function the platform writes the outbox with,
and with `db_roles` off it runs as the platform itself, so a row of the
outbox proves nothing by being there. The audit and webhook rows the
platform writes carry a MAC (the row header `Cufflink-Outbox-Mac`, which is
never published) over the row's tenant, kind, service, subject, id and the
SHA-256 of its payload. The relay publishes an audit or webhook row only
when its MAC holds: any other is moved to the dead-letter queue as
`cufflink.dlq.{tenant}.{service}.outbox.unauthenticated`, counted by
`cufflink_outbox_rows_unauthenticated_total{tenant,kind}`, logged at
`error` (`outbox_row_unauthenticated`), and never stored on `CUFFLINK_AUDIT`
nor signed and sent to a receiver. Event rows carry no MAC: a guest may
publish on its tenant's subjects anyway.

The key is derived (HKDF-SHA256, a label of the outbox's own) from
`CUFFLINK_SECRETS_KEK`, and without one from `CUFFLINK_CACHE_SIGNING_KEY`;
the tenant's database never holds it. Set the same keys on every platform
replica. `outbox: enforce` is refused, as a tenant's policy and as
`CUFFLINK_POLICY_OUTBOX`, while the platform has neither; a replica that
has none holds the outboxes it books and publishes nothing of them, and
writes to them are answered 503.

The newest key writes and every key given verifies: the current
`CUFFLINK_SECRETS_KEK`, those of `CUFFLINK_SECRETS_KEK_PREVIOUS` and the
cache signing key. To rotate without a row going unbelieved, give every
replica the new key as a previous one first, then make it the current one
and the old one previous, and drop the old one once every outbox is
drained.

The MAC tells the platform's rows from any other. It does not stop a row
written by the platform from being written again as it was: the copy is
published under the same `Nats-Msg-Id`, which the stream takes once within
its duplicate window, and a webhook receiver sees the same
`X-Cufflink-Delivery`. With `db_roles` off a guest can also delete rows
before they are published.

## Unconditional controls

- **Module integrity**: a WASM binary loaded from storage must match its
  deployment's SHA-256 (`cufflink_wasm_hash_mismatch_total`); rollback
  verifies the binary it restores. Workers take a job's tenant, schema and
  hash from the deployment, never the message
  (`cufflink_worker_job_identity_mismatch_total`), and answer a
  request/reply job only on an `_INBOX.` inbox, refusing any other reply
  subject unrun (`cufflink_worker_reply_refused_total`).
- **Platform storage prefixes**: no guest may touch `wasm/`, `backups/`,
  `files/` or `web/` in the platform bucket, use `..` key segments, or name
  a bucket that is empty or contains `/`, whatever the `storage_scope` mode
  (recorded as `platform_prefix`, `path_traversal` and `invalid_bucket`). The web-runner verifies every artifact it extracts
  against its deployment's SHA-256.
- **Metadata endpoints**: cloud metadata hosts and addresses are never
  reachable (`cufflink_egress_denied_total{reason}`): all of IPv4
  link-local `169.254.0.0/16` (AWS, GCP, Azure, Hetzner and DigitalOcean
  metadata, ECS and EKS credential agents), `fd00:ec2::254`, `fd00:ec2::23`,
  `fd20:ce::254`, `168.63.129.16`, `192.0.0.192` and `100.100.100.200`,
  also when written as IPv4-mapped, IPv4-compatible or NAT64 IPv6. Every
  redirect hop is re-checked, connections go only to the addresses that
  were checked (no HTTP proxy is used), and one deadline covers every hop.
- **Reserved names**: guest Redis keys under `auth:`, `cufflink:` or `t:`,
  and guest publishes to any `$` subject, `cufflink.`, `deploy.`,
  `undeploy.`, `_INBOX.` or another tenant's `dw.` namespace are
  refused (`cufflink_nats_publish_denied_total{reason="reserved"}`), as is
  a publish on what is not a subject: a wildcard, an empty token or over
  1024 bytes (`reason="invalid"`). A `Cufflink-Reply` header must name a
  platform inbox.
- **Subscription scope**: a deploy or rollback is refused when a
  subscription's subject is outside `dw.{tenant}.` and `cufflink.tick.*`, or
  binds a stream other than `DW_{TENANT}` or `CUFFLINK_TICKS`; a stored
  version with such a subscription never starts it (logged at `error`).
- **Backup restore**: `s3_key` must be `backups/{service_id}/…` of the target
  service or another service the caller controls; table filters may only
  name the service's tables. The worker runs a backup job with its row's
  service, tenant and type, refuses a message naming another service or
  tenant, and runs each job once (a claim from `queued` to `running`).
- **Webhooks**: managing them needs deploy authority or `webhooks:manage`;
  deliveries carry `X-Cufflink-Signature-256: t={unix},v1={hex HMAC of
  "{t}." + body}` and `X-Cufflink-Delivery` beside the legacy
  `X-Cufflink-Signature`, do not follow redirects, and read at most 64 KiB.
- **Platform administration**: platform-realm JWTs need the
  `CUFFLINK_PLATFORM_ADMIN_ROLE` realm role for `/api/tenants*` and to act
  inside any tenant with `X-Tenant-Slug` (`cufflink-deploy` alone is not
  enough); every platform-admin call is written to the audit log with the
  client address `RATE_LIMIT_CLIENT_IP_SOURCE` resolves
  (`cufflink_platform_admin_calls_total{route}`).
- **Issuer**: tenant tokens must carry `{keycloak_issuer or KEYCLOAK_ISSUER}/realms/{realm}`
  (discovered from Keycloak when neither is set), never an issuer taken from
  the token. A failed discovery refuses the token and is retried with
  backoff; no fallback issuer is ever assumed.
- **API keys**: tenant keys expire `CUFFLINK_API_KEY_TTL_DAYS` after they are
  minted; a key without an expiry (minted before the upgrade) gets one
  `CUFFLINK_API_KEY_TTL_DAYS` from the first start after it. The backfill
  runs on every start, so a tenant key cannot be set to never expire. Keys minted by
  `cufflink login` belong to that user's session: any
  `POST /api/auth/logout` by the user revokes them, including a web app's
  sign-out that calls it (intended: signing out of the backoffice ends that
  developer's CLI keys for the tenant too). Keys from
  `POST /api/auth/api-keys` are not tied to a user.
- **Permission cache**: entries are HMAC-sealed (`CUFFLINK_CACHE_SIGNING_KEY`)
  and dropped when role assignments or role definitions change, including a
  deploy whose default roles add a permission or assign the deployer
  `admin`.
- **SSE**: streams check the table's `view` permission, deliver only the
  caller's rows on owner tables, and are capped per principal
  (`SSE_MAX_STREAMS_PER_PRINCIPAL`, 429 beyond it); anonymous streams count
  against the client address `RATE_LIMIT_CLIENT_IP_SOURCE` resolves.

## Rollout

Before upgrading:

1. Grant `cufflink-platform-admin` (or the role named by
   `CUFFLINK_PLATFORM_ADMIN_ROLE`) in the platform realm to every operator
   who uses a platform-realm JWT for `cufflink tenants …` or with
   `X-Tenant-Slug`; `cufflink-deploy` alone no longer gives cross-tenant
   authority. Platform-admin API keys are unaffected.
2. Set the same `CUFFLINK_CACHE_SIGNING_KEY` (at least 16 bytes) on every
   platform and worker replica. Without it each process seals with its own
   key: correct, but replicas overwrite each other's entries, so a
   multi-replica install runs effectively uncached.
3. If the app bucket is also the platform bucket, confirm no application
   object lives under `wasm/`, `backups/`, `files/` or `web/`.
4. Existing tenant API keys get an expiry `CUFFLINK_API_KEY_TTL_DAYS` from
   the first start of the new platform. Move CI to keys from
   `POST /api/auth/api-keys` (or service-account tokens) before then, and
   tell developers that signing out of any app that calls
   `POST /api/auth/logout` also revokes their `cufflink login` keys.
5. Replace `CUFFLINK_WASM_LIMITS` with `CUFFLINK_POLICY_WASM_LIMITS`: the old
   name is no longer read (the platform warns at startup when it is set).

The worker waits for the platform's migrations: started first, it stays
unready (`catalog: loading`) and takes no jobs until they are applied.

After upgrading, per tenant:

- `service_accounts`: seed `service_account_clients`, then `enforce` once
  `env_fallback` stops appearing.
- `audience`: set `allowed` to the clients you expect under `warn` first
  (with an empty list nothing is checked, so nothing is recorded); tokens
  of any other client then appear as `audience_not_allowed`. Add what
  shows up, then `enforce`.
- `tenant_binding`: annotate routes that must forward caller credentials
  with `forward_auth` and `enforce` after two clean weeks. Under `warn` a
  cross-tenant caller is already served as unauthenticated. Once enforced,
  `auth_header_forwarded` is no longer recorded: the credentials stripped
  from routes without `forward_auth` show in
  `cufflink_route_credentials_stripped_total` instead.
- `redis_scope`: `enforce` after one full rewrite cycle of every key (at
  least 24 h). Under `warn`, a key written with a TTL keeps its unprefixed
  pre-upgrade copy, which reads fall back to once the new key expires or
  is deleted: a delete removes the prefixed key only, since the unprefixed
  one may be another tenant's.
- `storage_scope`: set `allowed_buckets` so other tenant groups' buckets
  become foreign. Build the list from the buckets the data references, not
  only from configuration. Previews created before the upgrade are not
  linked to their source: write the same `storage_scope` row to each of
  them in the same change, or recreate them.
- `egress`: prefill `allow_hosts` with the internal hosts `warn` records,
  then `enforce`. Watch `cufflink_egress_denied_total{reason="body_too_large"}`
  for responses past the 10 MiB cap.

`cufflink_policy_violation_total` carries one `tenant` series per tenant,
previews included; drop the label in recording rules if many short-lived
previews make that too many.

### Rolling back

Control-plane migrations are additive. From this release on, a platform
also starts against a schema a newer one migrated further, so a later
rollback to this release works. Releases before it differ:

- The last image without versioned migrations (before R1) runs against the
  R1 schema unchanged: roll back to it.
- An image with versioned migrations but without this release's `V2`
  refuses to start once `V2` is applied (`MissingVersion(2)`), so do not
  roll back to it. If you must, delete the `V2` row from
  `refinery_schema_history` first; the tables and columns it added stay and
  are ignored.
