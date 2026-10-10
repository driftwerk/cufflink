# Database Roles and Stored Secrets (Operator Guide)

Needs a platform that advertises `db_roles` and `secret_encryption` in
`GET /api/version` → `supports`. Both are off until an operator turns them
on; with every setting at its default a platform runs every service's SQL
as its own role and stores secrets in plaintext, as before.

The rules each mode follows are in [policies](policies.md#database-roles)
(roles) and [deployment](deployment.md#stored-secrets) (secrets); the
variables in [setup](setup.md#core) and the metrics in
[observability](observability.md). This page is the order to do things in.

## Stored secrets

Service configs, webhook secrets, tenants' Keycloak ops secrets and
database-role passwords can be sealed (AES-256-GCM) under a data key per
tenant, itself wrapped under a key-encryption key (KEK) the platform is
given. Service roles the platform makes need it, so turn this on first.

1. Make a KEK and give it the same value on every platform, worker,
   web-runner and ext-runner replica:

   ```sh
   CUFFLINK_SECRETS_KEK=$(openssl rand -base64 32)
   CUFFLINK_SECRETS_KEK_ID=kek-2026-09
   ```

   With a key and no `CUFFLINK_SECRET_STORAGE`, storage is `dual`: every
   write stores the sealed value beside the plaintext, so a replica of an
   older release still reads it during the rollout.
2. Seal what was stored before:

   ```sh
   cufflink admin secrets reseal --operation encrypt
   ```

   (`--tenant <slug>` limits it to one tenant; the same operation is
   `POST /api/admin/secrets/reseal`, platform admin only.) It reports the
   rows checked and those rewritten (sealed, or their plaintext written
   back or cleared), the data keys rewrapped and the tenants rotated.
3. Set `CUFFLINK_SECRET_STORAGE=encrypted` and roll the replicas. Writes now
   store the sealed value alone. The platform refuses to start while a
   secret is still stored in plaintext alone.
4. Run `reseal --operation encrypt` once more under `encrypted`: it clears
   the plaintext kept beside sealed values written under `dual`. Until then
   `cufflink_secret_plaintext_rows{table}` counts them at startup.

Stay on `dual` until rolling the platform back below v0.28.0 is ruled out:
an earlier release reads only the plaintext column, which `encrypted`
storage empties: it fails on a service config or webhook secret it finds
empty, and takes an empty Keycloak ops secret for none.

**Leaving `encrypted`** (before any such rollback):

1. Set `CUFFLINK_SECRET_STORAGE=dual` on every platform, worker,
   web-runner and ext-runner replica and roll them: a replica still on `encrypted` empties
   the plaintext of each secret it writes.
2. `cufflink admin secrets reseal --operation encrypt`: under `dual` it
   writes each secret's plaintext back beside its sealed value.
3. Check that no secret an earlier release reads is left sealed alone; this
   counts 0:

   ```sql
   SELECT (SELECT count(*) FROM service_configs WHERE value IS NULL AND value_enc IS NOT NULL)
        + (SELECT count(*) FROM webhooks WHERE secret IS NULL AND secret_enc IS NOT NULL)
        + (SELECT count(*) FROM tenants
           WHERE keycloak_ops_client_secret IS NULL AND keycloak_ops_client_secret_enc IS NOT NULL);
   ```

4. Roll the platform back.

**Rotating the KEK:**

1. Add the new key to `CUFFLINK_SECRETS_KEK_PREVIOUS` (`id:base64,…`) on
   every replica that reads it (platform, worker, shared web runner) and
   roll them.
2. Make it `CUFFLINK_SECRETS_KEK` everywhere, with the old one in
   `_PREVIOUS`, and roll them.
3. `cufflink admin secrets reseal --operation rotate_kek`.
4. `cufflink admin secrets reseal --operation verify`, of every tenant (no
   `--tenant`): it must exit 0 and print `0 not under the current
   key-encryption key` ([verifying a key ring](#verifying-a-key-ring)).
5. Wait until every outbox is drained: its rows are authenticated with keys
   derived from the ring ([policies](policies.md#rows-the-platform-wrote)).
6. Drop the old key from `_PREVIOUS` on every replica and roll them.
7. Verify again: it must exit 0. If it does not, put the key back.

**Rotating a tenant's data key:** `reseal --operation
rotate_dek --tenant <slug>`, then `encrypt` a minute later, once every
replica follows the new key.

A reseal that meets a row it cannot open stops with a 409 naming the table
and row id; earlier batches stay done. Mend or delete the row and run it
again. A sealed value that does not open is always an error, never a
fallback to plaintext.

### Verifying a key ring

```sh
cufflink admin secrets reseal --operation verify [--tenant <slug>]
```

answers "can this platform's key ring open everything the database holds?"
and changes nothing (`POST /api/admin/secrets/reseal` with `"operation":
"verify"`; the answer gains `verified`, which the other operations' answers
do not have). It unwraps every data key from the database anew, not from
what the replica remembers, so a wrap that went bad after the replica
started is found. It then opens every sealed value with those keys. It
reads one snapshot of the database, so a key or a secret written while it
runs is neither missed nor taken for broken. It writes nothing, makes no
data key for a tenant that has none, locks no row, and does not stop at
what it cannot open: it counts.

```
Verified secrets (every tenant): 1204 rows checked
  Data keys: 31 checked, 0 unopenable, 0 not under the current key-encryption key
  Rows:      0 unopenable, 0 not sealed, 0 whose plaintext differs, 0 under a retired data key
```

| Count (`verified.…`) | Meaning | What to do | Fails the command |
|---|---|---|---|
| `data_keys_unopenable` | a data key the ring cannot unwrap: its key-encryption key is not in the ring, or the wrap was altered. One no row is sealed under is checked too | the log's `kek_id` names the key to put back in `CUFFLINK_SECRETS_KEK_PREVIOUS`. Rows under such a key are counted `unopenable` too: mend keys first, then verify again | yes |
| `unopenable`, by table | a sealed value that does not open: altered, sealed for another row, or under a data key that is missing or unopenable | mend or delete the row, as for a reseal that stops at one | yes |
| `not_sealed`, by table | under `encrypted`: a secret stored in plaintext alone, which the platform refuses to read | return to `dual`, `reseal --operation encrypt`, then `encrypted` again | yes |
| `plain_differs`, by table | under `dual`: the sealed and plain forms disagree (an earlier release wrote the plaintext) | `reseal --operation encrypt` seals it again | yes |
| `data_keys_under_previous_kek` (printed as "not under the current key-encryption key") | data keys wrapped under a key other than the current one. **Must be 0, for every tenant, before a previous key is dropped** | `reseal --operation rotate_kek` | no |
| `rows_under_retired_data_key` | rows sealed under a data key that is no longer its tenant's active one: a `rotate_dek` whose `encrypt` has not run yet | `reseal --operation encrypt` | no |

A run that finds something prints, on stdout, a line for each table under
the counts, grouped by what is wrong (unopenable, not sealed, whose
plaintext differs) and a `Not finished:` line for each of the last two
counts that is not 0; then one line on stderr that says what to do, and
exits 1:

```
Verified secrets (every tenant): 1204 rows checked
  Data keys: 31 checked, 1 unopenable, 1 not under the current key-encryption key
  Rows:      40 unopenable, 0 not sealed, 0 whose plaintext differs, 0 under a retired data key
    service_configs: 38 unopenable
    webhooks: 2 unopenable
  Not finished: 1 data keys are not under the current key-encryption key. Run `reseal --operation rotate_kek` before dropping a previous key.
Not verified: 1 data keys do not unwrap: put the key-encryption key each names back in CUFFLINK_SECRETS_KEK_PREVIOUS (…); 40 rows do not open (…)
```

A count is always followed by its plural (`1 rows checked`), so a line
reads the same to a script whatever it counts: `grep -q ' 0 not under the
current key-encryption key$'` is the check of step 4 above.

- **Exit codes.** 0: everything is read (a rotation may be unfinished). 1
  with a `Verified secrets` line on stdout: the verify ran and found
  something. 1 without that line: it did not run (no credential, an unknown
  tenant, a platform older than v0.30.0, which answers 422, `plaintext`
  storage, a timeout).
- **Where to look.** The replica that answered logs, at `warn`,
  `secret_verify_data_key_unopenable` (`tenant_id`, `key_id`, `kek_id`,
  `error`) for each data key, and `secret_verify_row` (`table`, `id`,
  `tenant_id`, `fault`: `unopenable`, `not_sealed` or `plain_differs`) for
  the first 20 rows of each table whose own value is at fault. Rows under a
  data key that did not unwrap are counted and not named: the key's line
  says why. It also logs one `Verified secrets` line with every count
  (`event`: `secret_verify_clean` or `secret_verify_failed`). No log line
  and no answer holds a secret. The run is audited as `secrets.reseal`,
  with what it found.
- **Scope.** A verify answers for the key ring of the platform replica that
  served it: every replica, worker and runner must carry the same ring.
  With `--tenant` it reads that tenant's keys and rows alone, so its `0 not
  under the current key-encryption key` is no licence to drop a key. A
  scope that holds nothing prints `0 checked` and exits 0: a drill should
  assert `Data keys: N checked` with N above 0.
- **How long.** It runs inside its request, reading 500 rows a query and
  writing nothing. A release build verified 100,000 config rows in under
  half a second against a local PostgreSQL 16 (about 200,000 rows a
  second); a database further away adds its round trip for each 500 rows.
  It holds one read-only snapshot for the run. Where a run nears the
  proxy's timeout, run it tenant by tenant with `--tenant`.
- A database error ends the run with that error: it is not a verification.
  Under `plaintext` storage a verify is refused as a reseal is. It does not
  cover the outbox (step 5 above). A CLI knows the counts of its own
  release.

## Per-service database roles

With `db_roles` off every service runs its SQL as the platform's role, which
reaches every schema of the tenant's database. The policy gives each service
a role of its own.

### Prerequisites

- `CUFFLINK_SECRETS_KEK` set on every platform, worker, web-runner and
  ext-runner replica, and what was stored before resealed, **before `warn`**: a role
  the platform makes logs in with a password the platform mints and keeps
  sealed ([stored secrets](#stored-secrets)).
- The platform's database role has `CREATEROLE` and is not a superuser.
- **PostgreSQL 16 and later:** `CREATEROLE` no longer lets the platform grant
  a role it did not make. As a superuser, give it every read-only role a
  binding names, `reports_ro` for one:

  ```sql
  GRANT reports_ro TO cufflink WITH ADMIN OPTION;
  ```

  Without it binding the read-only role is refused, with this statement in
  the error.
- Tenant pools reached directly keep `DB_POOL_RECYCLE=clean` (the default);
  the policy is refused while one recycles otherwise.
- Under `enforce`, room in `max_connections`: each platform replica and
  worker holds up to `login_pool_max` (4 by default) connections for every
  service in use, logged in as its role and never through a pooler, beside
  its `MAX_CONNS_PER_TENANT`.

### Rollout for a tenant

A role the platform makes exists only once the policy is `warn`, so `warn`
comes before the roles are verified:

1. **`warn`.** `cufflink tenants policy set harbor db_roles warn`, with the
   roles that read every service schema in its config (below). Each service
   whose schema the platform made is given a role
   (`svc_{tenant}_{schema}_{digest}`) at its next deploy; deploy each one.
   SQL runs as each service's role inside the platform's connection; what
   the role is refused runs again as the platform and is counted in
   `cufflink_db_role_fallback_total` and logged (`db_role_fallback`, with
   Postgres's message). `warn` costs a savepoint per statement, so it is for
   the weeks of mending, not for good.
2. **Bind the services whose schema belongs to a role that exists** (a
   service whose schema was adopted, say), and the read-only roles:

   ```sh
   printf '%s' "$ROLE_PASSWORD" | \
     cufflink services db-role set harbor catalog-service --role catalog_service_user --password-stdin
   cufflink services db-role set harbor reports-service --read-only-role reports_ro
   ```

   A binding needs the service to have been deployed once. Every statement
   of a service can switch to its read-only role, not only
   `query_read_only`: bind one that reaches no more than that service may
   read in its tenant's database
   ([policies](policies.md#database-roles)).
3. **Soak.** Grant what is missing until
   `cufflink_policy_violation_total{key="db_roles"}` and
   `cufflink_db_role_fallback_total` stay flat, and watch
   `cufflink_db_role_provision_failures_total` and
   `cufflink_db_role_verify_unanswered_total`.
4. **Verify** each service:

   ```sh
   cufflink services db-role verify harbor catalog-service
   ```

   It logs in as the role and checks, in order, that the role exists and
   can log in, the platform's role is a member of it, the stored password
   works, it may connect to the database, use and create in its schema,
   read and write every served table, switch to its read-only role, and
   use `_cufflink` and the outbox function. Of a
   [private table](schema-ownership.md#private-tables) it checks only that
   the table exists: the platform runs no statement of its own there, so a
   role that holds nothing on one, or reaches it only through a function,
   stays verified. It exits non-zero while any
   check fails and prints, for each, the `GRANT` or `ALTER` that mends it.
   A role that is a superuser, may create roles or databases, replicate or
   bypass row-level security, or could create in `public`, is refused.
5. **`enforce`.** `cufflink tenants policy set harbor db_roles enforce`.
   The policy is refused while any service of the tenant with a schema lacks
   a verified role. Each service's SQL, data routes and `on_migrate` hook
   now run on a login as its role, which no statement can leave.

`readers` in the policy's config lists roles that may read every service
schema of the tenant, now and as tables are added:

```sh
cufflink tenants policy set harbor db_roles enforce \
  --config '{"readers": ["database_reader"], "login_pool_max": 4}'
```

`cufflink services db-role rotate <tenant> <service>` gives a role the
platform made a new password; `show` prints the binding and its last
verification. Deleting a service makes its platform-made role `NOLOGIN` and
leaves its schema.

### Rolling back

Setting the policy back to `off` runs everything as the platform's role
again. The roles keep what they own: to hand a schema back, run
`REASSIGN OWNED BY <service role> TO <platform role>` in the tenant's
database. [Policies](policies.md#database-roles) lists the statements that
take back what a removed reader was granted.
