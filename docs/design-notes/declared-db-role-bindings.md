# Declared database-role bindings and stream-prefix claims

A service can adopt a schema that an existing role owns, for example a
schema made by a migration tool that runs as its own role. This note lets
the service declare that role alongside its other deploy settings. Its
first deploy then binds the role with the tenant's deploy credential alone,
and deploys under `db_roles = enforce`. An administrator acts once per
tenant, to allow role names. Whoever creates each role also grants the
platform membership of it, once (§2). No step is needed at deploy time,
and none needs the admin key.

Part 2 does the same for NATS. After one administrator claim of a
stream-name prefix, a new service that declares a new stream under it needs
no further step.

The implementation follows these rules:

- It is complete: no TODOs and no partial paths.
- It is written in the codebase's functional style: pure decisions,
  effects at the edges.
- It adds no comment that is not needed and duplicates no logic.

## 1. Problem

Under `db_roles: enforce`, the first deploy of a service whose schema is
already owned by a role other than the platform's cannot succeed:

- With no binding, `db_roles::provision::before_deploy` provisions a
  managed role (`svc_{tenant}_{schema}_{digest}`). `provision::statements`
  builds every statement before any runs, and for a schema owned by
  another role, `schema_ownership` refuses with
  `ProvisionError::ForeignSchema` ("schema X belongs to Y, not to the
  platform: bind the service…") before `CREATE ROLE` is attempted. When the
  schema is the platform's or absent, `CREATE ROLE` itself fails on a
  platform role without `CREATEROLE`.
- `before_deploy` only logs that failure: its `(_, false, true)` arm
  returns `Ok`. The deploy goes on until the DDL session finds no verified
  binding (503). A deploy that runs no DDL is activated, and every request
  to the service then answers 503.
- `cufflink services db-role set` takes the platform admin key
  (`PlatformAdmin`), and needs a service that a deploy has already
  recorded. `warn` does not help, because `ForeignSchema` is refused under
  every mode.

So each such service, on each environment, needs four steps:

1. A first deploy, which fails.
2. A binding made with the admin key.
3. A verification, with a policy change where needed.
4. A second deploy.

CI cannot do this without the admin key.

### What exists already

No release has this. `v0.40.6` is the newest tag, and `main` is at it.

Pull request #91 (`feat/db-role-bind-before-deploy`) is open and unmerged,
based on v0.38.1. It adds `db-role set --register`: an administrator
records a service and binds it before its first deploy. That still needs
the admin key and a step per service.

This design folds in three parts of #91 (§4.6):

- `Provisioning::require_verified`;
- the `enforce` gate that refuses a first version without a verified
  binding;
- the `--inactive` rule for an active service that has never run.

#91 rejects a role named at deploy because "a deployer could pick any role
in the tenant database". §5 answers that objection with:

- an allowlist that an administrator sets once;
- proof of possession of the role's password;
- a confinement check, which no current check makes.

## 2. Decision

### Part 1: a declared binding

1. **An administrator opts the tenant in once.** A new key in the tenant's
   `db_roles` policy config, `declared_roles`, lists the role names the
   tenant's deployers may declare. Each entry is an exact name or a stem
   followed by `*`:

   ```sh
   cufflink tenants policy set harbor db_roles enforce \
     --config '{"readers": [], "declared_roles": ["catalog_service_user", "loan_*"]}'
   ```

   The default is `[]`, which admits no declaration.
2. **Whoever creates the role meets one precondition once.** The role
   exists and owns the service's schema. `pg_hba.conf` admits it from the
   platform's hosts with password authentication (`scram-sha-256`),
   because the platform proves the password by logging in as the role
   (gate 3). The platform's role is a member of it:

   ```sql
   GRANT <role> TO <platform role> WITH INHERIT TRUE, SET TRUE;  -- PostgreSQL 16 and later
   GRANT <role> TO <platform role>;                              -- earlier
   ```

   The tenant key cannot grant this, and neither can a platform role
   without `CREATEROLE`. Without the membership, under `enforce`, both the
   deploy and `?plan=true` are refused (422) with that `GRANT`. Under
   `warn`, the binding is written unverified and SQL falls back to the
   platform's role.

   Through the membership, the platform's role inherits the bound role's
   reach. §5.2 confines that reach to the service's schema, which the
   platform reaches already.
3. **The service declares the role per environment** in `Cufflink.toml`.
   It names the password by its vault entry, as `[secrets]` names its
   values.
4. **The deploy carries the declaration.** The CLI reads the vault entry
   before it sends anything, and sends `{role, password}` in a new
   deploy-request field. The field is outside the manifest and outside
   `deploy_hash`.
5. **The platform binds the role under the deploy lock**, after the
   service is recorded and before anything runs as any role. Every check
   passes before the platform makes any grant or writes anything (§4.3).
6. **A binding records where it came from:** the platform (managed), an
   administrator, or a declaration. An administrator's binding takes
   precedence over a declaration.

### Part 2: stream claims by prefix

Subject claims are already patterns: one administrator claim of
`catalog.>` covers every subject under it (`Namespace::covers`). Stream
claims are exact names, so every new stream needs a new claim. Part 2 lets
an administrator claim a stream-name prefix once (`CATALOG_*`). The tenant
key gains no way to write claims (§8).

## 3. Declaration shape

```toml
[service]
name = "catalog-service"

[environments.staging]
api_url = "https://cufflink.example.com"
tenant = "harbor"

[environments.staging.db_role]
role = "catalog_service_user"
password_secret = "CATALOG_DB_ROLE_PASSWORD"
```

- **`role`** is validated by `names::validate_role_name`.
- **`password_secret`** names an entry of the environment's vault
  (`secrets/<env>.json`). It never holds the value itself; the name is not
  `password`, so a literal password is not mistaken for one. A missing
  entry fails before anything is sent.
- **`ProjectConfig::EnvironmentConfig`** gains `db_role:
  Option<DeclaredRole { role: String, password_secret: String }>`.
  `DeclaredRole` is `deny_unknown_fields`.
- **`[service].name` is required** and must equal the manifest's name.
  Otherwise the declaration is refused before anything is built or sent:
  `[environments.staging.db_role] needs [service].name = "<manifest
  name>"`. A `Cufflink.toml` found in a parent directory therefore cannot
  bind another service's role. This holds for `cufflink deploy` and for
  `cufflink deploy --bundle`, which reads the `Cufflink.toml` where it runs,
  as the config step does.
- **The vault entry is the binding's alone:**
  - `unreferenced_vault_secrets` counts `password_secret` as referenced, so
    neither its warning nor the refusal of `config apply --prune` fires.
  - A `[secrets]` key that maps the same entry is refused by every command
    that reads the environment: `CATALOG_DB_ROLE_PASSWORD is the db_role's
    password; [secrets] would hand it to the service's code`. Service
    config is readable by the guest (`config::get`).
- **Why not the manifest:**
  - Roles and passwords belong to an environment.
  - A bundle is built once for every environment.
  - The manifest is hashed into `deploy_hash`.
- **Older CLIs.** `EnvironmentConfig` does not deny unknown fields, so a
  CLI older than v0.41.0 ignores `[db_role]` and sends nothing. In
  particular, it never sends a rotated password. Pin the CLI in CI to
  v0.41.0 or later wherever a declaration is used. The release notes say
  so.

### Deploy request

`DeployRequest` gains one field:

```json
{ "manifest": { … }, "db_role": { "role": "catalog_service_user", "password": "…" } }
```

- **The password is never held as a plain string.**
  - **On the platform**, `db_role.password` deserializes into
    `SecretString`. `cufflink-secrets` gains `impl Deserialize for
    SecretString`, which reads into its `Zeroizing<String>`. Both the
    platform and `cufflink-secrets` are unpublished, so this changes no
    published crate.
  - **In the CLI**, which is published and so cannot depend on the
    unpublished `cufflink-secrets`, the vault value is held in
    `zeroize::Zeroizing<String>`. `zeroize` is already a workspace
    dependency; it is published, small, and the type `SecretString` wraps
    anyway. No new crate is needed, and no type is moved.
  - **The CLI serializes the request** with `serde_json::to_vec`, from a
    request struct that borrows the password as `&str`, into a
    `Zeroizing<Vec<u8>>` body. The password never enters a
    `serde_json::Value`. The copy that the HTTP client makes of the body is
    outside the CLI's reach, and the doc for the CLI says so.
  - **The platform's request buffers are not cleared.** The body arrives
    as hyper's `Bytes`, which are shared and immutable and freed by hyper,
    and `serde_json` unescapes a string with escapes in a scratch buffer
    of its own. Neither is the platform's to clear, and a copy made to
    clear would be one more. What the platform keeps is the
    `SecretString`, cleared on drop; the buffers live only for the
    request, in the platform's own memory, as the tenant key in its
    header does.
- **Admission refuses** an empty password and one holding a NUL byte,
  which PostgreSQL cannot store.
- **What keeps the role name only.** `Admitted` holds the declaration.
  `Admitted::request()` carries only `db_role.role`; it is stored in
  `deploy_jobs.request` and returned by `GET /api/deploys/{id}`. The
  job's `result` and `error`, the plan, the answer, logs and audit rows
  likewise carry only the role.
- **The CLI never prints the request body.** It builds its error messages
  from the answer and the role, never from the body.
- **Feature gate.** `DeployRequest` is `deny_unknown_fields`, so an older
  platform answers 422 to the field. The platform advertises
  `db_role_declarations` (`cufflink_types::features::DB_ROLE_DECLARATIONS`,
  in `api::deployments::FEATURES`). The CLI sends `db_role` only to a
  platform that advertises it. For any other platform it refuses before
  building: `[environments.staging.db_role] needs a platform that
  advertises db_role_declarations (v0.41.0 or later)`.

## 4. Deploy-time flow

### 4.1 Admission (`api::deployments::admit`, before any write)

These checks are pure. Each refusal is a 422 and leaves nothing written:

- The mode runs on the platform (`ServiceMode::runs_on_platform`).
- The role name is valid and does not start with `svc_`.
- The password is neither empty nor holding a NUL byte.
- Secret storage can seal, which needs a key-encryption key.

### 4.2 Claim and lock

`claim` records the service (`find_or_create`) and takes its deploy lock,
as today. A binding is sealed with the AAD
`DB_ROLE_PASSWORD.aad([service.id])`, so the service must exist first.

### 4.3 Taking the declaration (`db_roles::declared::take`)

**Where it runs.** `take` runs at the head of `pipeline::run_locked`, when
`plan_only` is false and a declaration was sent. It runs before the skip
check, so an unchanged deploy still takes a changed password. It also runs
before `before_deploy`, `eventing::prepare`, `schema::apply`, the
migrations, `on_migrate` and activation.

**What it calls.**

```rust
provision::apply(
    state,
    &Provisioning { deploying: Some(manifest), declaration: Some(declared), require_verified, .. },
    Change { role: Some(role), password: Some(password), read_only_role: None, rotate: false, origin: Some(Origin::Declared) },
)
```

- `require_verified` is `true` under `enforce`.
- `Change::origin` is new (§4.3, origin through later provisioning).

**The decision is made inside `apply_locked`.** `apply_locked` already
holds the per-database turn and the advisory lock. There it reads the
binding in place and computes the decision `taking`, a pure function, so a
concurrent `db-role set` cannot slip between the decision and the write.

```rust
pub enum Taking {
    Unchanged,
    Bind { replaces: Option<Origin> },
    Refresh,
}

pub fn taking(existing: Option<&Binding>, declared: &Declared<'_>, same_password: bool) -> Result<Taking, Refusal>
```

| Binding in place | Declaration | `taking` |
|---|---|---|
| none | any | `Bind { replaces: None }` |
| managed (`svc_…`) | any role | `Bind { replaces: Some(Platform) }` |
| admin, same role | same password, verified | `Unchanged` |
| admin, same role | otherwise | `Refresh`: the password only. Origin `Admin` and `read_only_role` are kept. `confined` reports and does not refuse, because the administrator vouched for the role |
| admin, another role | any | refused, 409: `the service is bound by an administrator to role X; an administrator changes it with cufflink services db-role set` |
| declared, same role | same password, verified | `Unchanged` |
| declared, same role | otherwise | `Bind { replaces: Some(Declared) }` |
| declared, another role | any | `Bind { replaces: Some(Declared) }` |

- **`same_password`.** The stored sealed password is opened
  (`verify::open_password`) and compared with the declared one in constant
  time. A password that does not open counts as different.
- **`Unchanged`** writes nothing and audits no `db_role.declare`.
- **Every other outcome** passes the gates below in order. Steps 1 to 6
  write nothing anywhere and grant nothing.

**The gates.** Each gate reads the catalog as the platform. A refusal
stops the request: it is counted and audited as `db_role.declare_refused`
(§7), and the binding in place stays as it was.

1. **Allowlist and name.** These checks are pure: `declared_allowed`
   (`declared_roles` admits the name, for `Refresh` too) and
   `declared_not_platforms` (no `svc_` prefix). They refuse with 422.
   `declared_roles` admits no name the platform or the server reserves
   (below), so neither does this gate.
2. **Existence, from the catalog** (`role_facts`, plus `CONNECT` on this
   database):
   - `role_exists`;
   - `can_login`: a `NOLOGIN` role is refused here, never sent to a login
     probe that would read it as a wrong password;
   - `database_connect`: the remedy is `GRANT CONNECT ON DATABASE … TO …`,
     and the role is never probed into a 42501.

   Each refuses with 422, and with the answer a refused login gives (gate
   3): until the password is proven, a deploy learns nothing of a role but
   that it does not log in with the password given. Which check failed,
   with its remedy, goes to the refusal's detail: the audit row and the
   `warn` log. Only allowlisted names are looked up.
3. **Proof of possession.** `login_pools::probe` logs in with the declared
   password.
   - `probe` classifies 42501 (`insufficient_privilege`) after
     authentication as `Accepted`, because the password was proven. Gate 2
     has already judged `CONNECT`.
   - Class 28 (`invalid_authorization`) is `Refused`, and the request gets
     a 422. This covers 28P01 (wrong password) and 28000 (for example, a
     `pg_hba.conf` reject). Both, and every refusal of gate 2, carry one
     wording: `role X does not log in to the tenant's database with the
     password given: check that the role exists, may log in and has
     CONNECT on the database, that its password matches the vault entry,
     and that pg_hba.conf admits it`. The server's message goes to the
     detail.
   - Everything else is `Unanswered` and gets a retryable 503. That
     includes a server that cannot be reached, 53300
     (`too_many_connections`), 57P03 (`cannot_connect_now`) and 3D000
     (`invalid_catalog_name`). None of these proves or disproves the
     password. The tenant is told only that a login could not be tried;
     the error goes to the detail.
   - This holds under every mode.
4. **The declaration checks of §5.2**, run only once the password is
   proven. `declared::facts` reads the catalog once, and
   `declared::refusals(&facts, &expected) -> Vec<Refusal>` judges it as a
   pure function. A refusal is a 422 that lists every finding with its
   remedy. Findings outside the tenant's own database are redacted (§5.4).
5. **Readiness, before any grant.** `verify::inspect` runs over the
   candidate with the checks that do not depend on the platform's own
   grants: `platform_membership`, `database_connect`, `schema`,
   `public_schema`, `table` and `read_only_role`. The `cufflink_*` checks
   are not judged here, because they are what step 7 grants.
   - Under `enforce` (`require_verified`), any failure refuses the request
     with 422 and the report's remedies.
   - Under `warn` and `off`, the failures are carried to step 9.
6. **The outbox bootstrap.** `outbox::bootstrap::ensure` runs only now.
   It used to run before every check in `apply_locked`; now no refusal
   follows anything it wrote.
7. **The platform's own grants, additive only.** The `_cufflink`
   statements for the role run: `USAGE` on `_cufflink`, `EXECUTE` on `OWN`,
   and the `service_roles` row for the role. `cufflink_statements` is split
   in two:
   - `cufflink_grants`, which only adds: the `INSERT … ON CONFLICT (role)`
     of `map` and the grants, or for a service that never enqueues, the
     revocations of the role's own excess;
   - `unmap_others`, which deletes every other role's mapping for the
     service.

   `unmap_others` is reached from two places today: `map` (a service that
   enqueues), and `narrowing` when the mapping is stale (a service that
   never enqueues, `enqueue.rs` `narrowing`). Both move out of step 7.
   `map` keeps only its `INSERT`, and `narrowing` keeps only the
   revocation of `SHARED` from the role. `unmap_others` runs once, at step
   9.4, for both kinds of service. Step 7 runs `cufflink_grants` alone, so
   the role bound before keeps its mapping and its enqueues go on working.

   The same order holds for every write in `apply_locked`: managed
   provisioning, every `refresh`, `db-role set`, and a declaration.
   `unmap_others` for the bound role runs after `service_db_roles::write`,
   and never before it. A `service_roles` row that a crashed step 7 left
   for another role is therefore removed by the next write of this
   service's binding, whichever path makes it.
8. **Full verification.** `verify::inspect` runs again with every check.
   - **Under `enforce`, any failure here** can only follow a concurrent
     change to the catalog, because gates 2 and 5 judged everything else.
     The request is a 503, and nothing is written to `service_db_roles`.
   - **Taking back the grants.** When role B was not bound to this
     service X before the request, that refusal takes back what step 7
     granted, through `cufflink_ungrants`.
     - `cufflink_ungrants` is derived from the same `Enqueue` facts as
       `cufflink_grants`. Each `GRANT` that `cufflink_grants` issues
       becomes its `REVOKE`. Its `INSERT` into `service_roles` becomes
       `DELETE FROM _cufflink.service_roles WHERE role = B AND service = X`.
     - It is not a full inverse. The revocation of `SHARED` that
       `cufflink_grants` makes for a service that never enqueues is not
       re-granted, because the platform never wants B to hold it. For such
       a service, `cufflink_ungrants` is therefore empty.
     - It may remove a grant B held before the request on those objects,
       left by a crashed attempt or made by hand. That is harmless, because
       B is not bound to any service (`bound_elsewhere` passed under the
       lock), so nothing the platform runs uses B.
     - It never touches another role's row, so the role bound before keeps
       its mapping.
   - **When ungrants run, and when they do not.** They run only at this
     step, or on a refusal at gate 4 or later of a later `take` for the
     same role and service, once `bound_elsewhere` has passed under the
     lock. A refusal at gates 1 to 3 never runs them, so a deployer who has
     not proven the password cannot use it to touch any role's grants.
   - **A role that was bound before** keeps its grants, which it held
     already.
   - **After a crash between steps 7 and 9**, nothing runs the ungrants.
     The additive grants stay, and they are bounded: the role is
     allowlisted, proven and confined to the service's schema. The next
     `take` for that role either completes them, or, if it is refused at
     gate 4 or later, takes them back. A stray `service_roles` row of the
     role is also removed by the next binding write of the service, by
     `unmap_others`.
9. **The write.** In order:
   1. If the binding is being replaced, its `Retiring` is captured from the
      binding as read under the lock, before the overwrite.
   2. `service_db_roles::write` runs with origin `Declared`.
   3. `forget_role(service.id)` runs.
   4. The captured `Retiring` runs as `retire_in(&tenant_db)`. This runs
      under the turn and advisory lock that `apply_locked` already holds
      (§7, retiring a binding).
   5. Now that the role is the binding, `unmap_others` runs.
   6. `db_role.declare` and `db_role.verify` are audited, the first from
      the binding read under the lock.

   Steps 4 and 5 follow a binding that is written, so neither fails the
   request. A failure is logged, with what to run by hand, and counted
   (`cufflink_db_role_retire_failures_total`,
   `cufflink_db_role_unmap_failures_total`). A mapping left behind is
   reported by the next verification (`outbox_mapped_elsewhere`, §5.2).

   A write that loses to another tenant's (the unique index, §6), or a
   role that changes between the gates and the write, is a refusal of the
   `declaration` gate like any other: audited, counted, and answered 422.

#### Origin through later provisioning

`Change` gains `origin: Option<Origin>`:

| `origin` | Set by |
|---|---|
| `Some(Declared)` | a declaration |
| `Some(Admin)` | `db-role set` |
| `None` | everything else: `verify`, `rotate`, and the `refresh` that `before_deploy` and `after_deploy` run with `Change::default()` |

`target` takes `origin.unwrap_or(existing origin)`, and `Platform` when it
makes a managed role. So a declared binding stays declared through every
later deploy.

### 4.4 The rest of the deploy

`before_deploy` finds a verified binding, so no managed role is attempted.
It provisions again only in its existing arms:

- the coming version enqueues and the running one did not;
- the coming version creates in the schema and the running one did not;
- the binding is unverified.

`schema::apply` then runs, in order:

1. the verify pass;
2. the checks of settled adopted declarations;
3. the migrations, as the bound role under `enforce`;
4. the adoption check that follows the migrations
   ([adoption after migrations](adoption-after-migrations.md));
5. activation.

`after_deploy` verifies the binding again against the activated version's
tables. That verification writes `verified_at`, `verify_report`,
`updated_at` and a `db_role.verify` audit row after every deploy, as it
does today. Tables that a migration made are owned by the bound role, so
they pass the table check.

### 4.5 Plan (`?plan=true`)

- **A service already recorded:** `run_locked` runs `declared::preview` in
  place of `take`.
- **A service not yet recorded:** the plan never calls `run_locked`, so
  `api::deployments::plan` calls `preview` in its `None` branch, and asks
  `bound_elsewhere` with `Uuid::nil()`.

`preview` runs gates 1 to 5 and `taking` read-only. It seals nothing,
writes nothing, grants nothing and runs no outbox bootstrap.

- **Refused probes are audited.** A probe that `preview` makes and that is
  refused is audited (`db_role.declare_refused`, with `plan: true`) and
  counted. A plan is therefore no unaudited password oracle.
- **The plan gains:**

  ```json
  "db_role": { "role": "catalog_service_user", "taking": "bind", "failed": [] }
  ```

- **A refusal makes the plan a 422** (CLI exit 3).

### 4.6 What is folded in from #91

- **`Provisioning::require_verified: bool`.** When it is true, `apply_locked`
  writes no binding that does not verify, and returns the report's
  failures as a 422. It is true for a declaration under `enforce`, and for
  `before_deploy` of a service under `enforce` that runs no version yet.
- **The first-version gate.** In that second case, `before_deploy` returns
  provisioning's error as a 422, where today it only logs it. For a foreign
  schema the reason also names the declaration: `…or declare it:
  [environments.<env>.db_role] role = "Y"`. A redeploy of a running service
  is never refused this way, so a deploy that mends it is never blocked.
  **This is a behaviour change:** a first deploy under `enforce` that used
  to be activated unservable is now refused. The release notes call it out.
- **`--inactive` before the first run.** An active service with no
  deployment may be deployed `--inactive`, as #91 specifies.
- **The `enforce` precondition counts deployed services only.**
  `services::without_verified_role` skips services that have no
  deployment. A service recorded by a refused first deploy no longer stops
  a tenant from moving from `warn` to `enforce`. Nothing runs as such a
  service, and its first deploy must pass the first-version gate.

A refused first deploy leaves the service recorded, as every failed first
deploy does, and its binding as it was. Running the deploy again is safe.

## 5. Security

### 5.1 What the deploy credential may and may not do

**The precondition.** §2 item 2 is met by whoever creates the role, before
any deploy or plan. The platform's membership in the role is never the
tenant's to grant.

A credential with deploy authority (`DeployAuth`) for tenant T **may**:

- bind its own service to a role that meets every condition below:
  - `declared_roles` admits it;
  - it exists, may log in and may connect;
  - it owns the service's schema and reaches nothing else;
  - T proves its password;
- refresh the password of a declared or admin binding of that same role.
  For an admin binding, `declared_roles` must still admit the role.

It **may not**:

- bind a role that `declared_roles` does not admit. Only `PlatformAdmin`
  sets that config, through `put_policies`;
- bind a platform-made role (an `svc_` name, or a `cufflink:` marker
  comment);
- bind the platform's role, a superuser or `postgres`;
- bind a role that has `CREATEDB`, `CREATEROLE`, `REPLICATION` or
  `BYPASSRLS`;
- bind a role that is a member of any role;
- bind a role already bound to any service;
- name a `read_only_role`;
- replace an administrator's binding with another role;
- rotate a managed role's password;
- unbind;
- change any policy;
- have any grant made, or any mapping changed, for a role whose password
  it has not just proven (§4.3 gates 1 to 6).

### 5.2 Declaration checks (gate 4)

| Check | Refused when | Remedy |
|---|---|---|
| `declared_not_platforms` | a `cufflink:` marker is on the role (`shobj_description`) | none |
| `role_attributes`, `own_role` | as in `verify::role_checks` | as there |
| `declared_memberships` | the role is a member of another role, recursively through `pg_auth_members` | `REVOKE <granted role> FROM <role>`. The tenant-facing reason gives the count of memberships, and names each one only when it is a role this tenant's services are bound to or one of the built-in `pg_*` roles. Other roles' names go to the log and the audit |
| `bound_elsewhere` | another service is bound to the role | none (422, as `bound_elsewhere` answers today) |
| `schema_owner` | the service's schema is missing, or another role owns it | `ALTER SCHEMA … OWNER TO …` |
| `confined` | the role reaches anything outside the schema (below) | the objects found, up to 20, then a count |

#### `confined`

Roles are cluster-wide, and no existing check reads `pg_shdepend`. A new
query in `cufflink_pgcodec::roles`, `REACH_OF_THE_ROLE`, sits beside
`OWNED_BY_THE_USER`. It reads every `pg_shdepend` row whose `refobjid` is
the role (`refclassid = 'pg_authid'::regclass`), across every `dbid`.

For a row in this database, it reads
`pg_identify_object(classid, objid, objsubid)`, which gives `type`,
`schema` and `identity` for every catalog. The query keeps no list of
catalogs by hand. Two classes have no `schema` there, so the query takes
their namespace from their own catalog:

- **A policy row** (`pg_policy`, `deptype = 'r'`): the namespace of its
  table, `pg_class.relnamespace` of `polrelid`.
- **A `pg_default_acl` row:** its `defaclnamespace`, where 0 means "every
  schema".

A row is **allowed** when one of these holds:

1. Its schema, from `pg_identify_object` or the two rules above, is the
   service's schema. This covers any object type, collations and policies
   included, and any of `deptype` `o`, `a`, `r` and `i`.
2. It is the service's schema itself: `type = 'schema'`, and `identity`
   is the schema.
3. It is a `pg_default_acl` row (`deptype = 'o'`) whose `defaclrole` is
   the role and whose `defaclnamespace` is 0. Such a row governs only
   objects the role creates and grants it nothing.
4. It is `type = 'schema'` and `identity = 'public'`, and the role's ACL
   entry there (`aclexplode(nspacl)`) holds `USAGE` alone.
   `public_schema` already judges `CREATE`.
5. It is one of the platform's own grants, each an exact ACL entry:
   `USAGE` on schema `_cufflink`, or `EXECUTE` on `enqueue::OWN` or
   `enqueue::SHARED`.

For a shared object (`dbid = 0`), the one allowed row is the role's ACL
entry on this database (`CONNECT`, `TEMP`).

Every other row is reported, and so is every row of another database that
the role holds itself: what it owns (`deptype = 'o'`) and the policies
naming it (`'r'`). A privilege granted to it in another database
(`deptype = 'a'`) is that database's owner's doing, not the role's: any
tenant whose role may grant can grant to any role it can name, and so could
otherwise make another tenant's binding fail at its next verification. Such
grants are counted in `granted_elsewhere`, an `EXCESS` check that fails
nothing.

A privilege on an object outside the schema counts as harmless only
without grant option: `USAGE` on `public`, and the platform's own grants
(rules 4 and 5), are allowed only where they are not `is_grantable`, and
so are `CONNECT` and `TEMP` on the database. A row that
`pg_identify_object` cannot name is counted, never dropped.

Privileges that come only through `PUBLIC` are every role's, managed
roles' included, so `confined` does not judge them.

#### Re-checked at later verifications

`confined` and `declared_memberships` become checks of `verify::report` for
every binding:

- **For a declared binding they fail.** A role that is granted more after
  it was bound loses its verification at the next `cufflink services
  db-role verify` or `after_deploy`. It also loses it at any `before_deploy`
  arm of §4.4, and at any `take` that is not `Unchanged`.
- **For an admin binding they are reported `EXCESS` and fail nothing.** The
  `REVOKE` statements that can be computed are printed with them.
- **`declared_memberships` never counts the binding's own
  `read_only_role`.**
- **Its remedy is the operator's.** It names the roles the role is a member
  of, which can be other tenants'. The 422 a first version gets under
  `enforce` (`unverified`) carries every other remedy but this one, for
  which it points to `db-role verify`.

`outbox_mapped_elsewhere` is an `EXCESS` check of every binding of a
service that enqueues: another role mapped to the service in
`_cufflink.service_roles` enqueues as it. It is reported, never refused,
since the role bound before keeps its mapping until the new one is written
(§4.3 step 9).

### 5.3 Threat model

| Threat | Why it fails |
|---|---|
| Tenant A declares tenant B's role | the name is not in A's `declared_roles`. A lacks B's password (gate 3). B's role does not own A's schema, it reaches B's database (`confined`), and it is bound to B's service (`bound_elsewhere`) |
| A deployer names an allowlisted role without its password, to have grants made for it or to move the outbox mapping | gate 3 comes before any grant (step 7) and before any change to a mapping (step 9.4) |
| A refused declaration breaks a running service | nothing is written or unmapped before step 9. Step 7 only adds, so the role bound before keeps its mapping and its enqueues |
| A privileged role is declared | `unbindable`, `role_attributes` and `declared_memberships` |
| A sibling service's role is declared | `bound_elsewhere`. Before that role is bound, `schema_owner` refuses it |
| A role that owns this schema also reaches another schema or database | `confined`, at the declaration and at every later verification |
| The deploy or the plan is used to learn about other tenants | gates 1 to 3 come first, and no tenant-facing refusal names anything outside the tenant's database (§5.4) |
| The plan is used as a password oracle | every refused probe is audited and counted, plans included. Gate 2 reveals only whether an allowlisted name exists |
| Two tenants bind one role at once (the lock is per database, roles are cluster-wide) | a unique index on `service_db_roles (role_name)`. The losing write gets the `bound_elsewhere` 422 |
| The password leaks | `SecretString` on the platform, and `Zeroizing<String>` in the CLI. `Admitted::request()`, the job's `result` and `error`, audit rows and logs carry the role only. Tests check this (§10) |
| An older CLI or platform drops the declaration | the CLI gates on `db_role_declarations`, and an older platform refuses the field. An older CLI ignores `[db_role]`: pin v0.41.0 (§3) |

### 5.4 What a refusal says

- **To the tenant**, a refusal names nothing outside its own database:
  - `bound_elsewhere` answers `role X is bound to another service`;
  - `confined` lists objects of this database by identity, and counts the
    rest as `objects in N other databases`;
  - `schema_owner` names the owning role only, which is in the tenant's
    own database.
- **To the operator**, the full detail (the other tenant and service, the
  database names) goes to the platform log, at `warn`
  (`db_role_declare_refused`), and to the audit row's details. The tenant
  can read neither.

## 6. Data

`platform/migrations/V11__declared_db_roles.sql`:

```sql
ALTER TABLE service_db_roles ADD COLUMN declared BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE service_db_roles ADD CONSTRAINT service_db_roles_origin
    CHECK (NOT (managed AND declared));
CREATE UNIQUE INDEX service_db_roles_role_name_key ON service_db_roles (role_name);
```

**`declared` and `Origin`.** A replica of an older release still writes
this table during a rolling upgrade. It knows only `managed`, and the
column's default keeps its writes valid.

- `Binding` gains `origin: Origin { Platform, Admin, Declared }`, which
  `row_to_binding` derives from the two columns.
- `NewBinding` takes `origin` and writes both columns.
- The wire keeps `managed` beside the new `origin`.

**The unique index.** `bound_elsewhere` already refuses a second binding of
a role, so no installation should hold two. If one does, the migration
fails and the platform does not start. The release notes give the query to
run before upgrading:

```sql
SELECT role_name FROM service_db_roles GROUP BY 1 HAVING count(*) > 1
```

`write` maps a violation of the index to the `bound_elsewhere` 422. V11
checks first, in a `DO` block, and fails naming the roles bound more than
once, with what to run, rather than with a bare unique violation.

**`DbRolesConfig` gains `declared_roles: Vec<String>`**, with
`#[serde(default)]`. `validate` refuses:

- an entry that is neither a role name nor a stem followed by `*`;
- `*`, and a stem shorter than three characters;
- a name or stem that admits a name the platform or the server reserves:
  one that begins with `svc_` (the platform's), `pg_` (the server's
  predefined roles) or `postgres` (its administrator). A stem is refused
  when it begins with one of them, or one of them begins with it.

One function decides a match: `declared::admits`.

**Ordering rule for `declared_roles`.**

- `DbRolesConfig` is `deny_unknown_fields`, and `policy::read` falls back
  to the default config when it cannot read one. A replica older than
  v0.41.0 that reads a config holding `declared_roles` would therefore lose
  `readers` and `login_pool_max` as well.
- So: set `declared_roles` only after every platform, worker, web-runner
  and ext-runner replica runs v0.41.0. Before rolling any replica back
  below v0.41.0, remove `declared_roles` from every tenant's config.
- `put_policies` cannot check what other replicas run, so the release
  notes carry the rule.
- Keeping `deny_unknown_fields` is deliberate: it keeps catching typos in
  the config. A tolerant parse in v0.41.0 would not mend the replicas that
  are already older.

## 7. Modes, idempotency, change and removal

**Modes.** Gates 1 to 4 refuse under every mode. Readiness (gate 5, and
step 8) follows the mode:

| Mode | Readiness failure |
|---|---|
| `off` | the binding is written, verified or not |
| `warn` | the same, and SQL runs as the role, with fallback |
| `enforce` | the binding must verify before it is written, and a first version needs a verified binding (§4.6) |

**Idempotency.**

- **`Unchanged`.** The same role and password, with a verified binding,
  is `Unchanged`. It writes no binding, no `db_role.declare` row and no
  grant.
- **Every deploy still verifies.** `after_deploy`'s verification writes
  `verified_at`, `verify_report`, `updated_at` and a `db_role.verify` row,
  as it does today. It does so because the time of the last verification
  is meaningful.
- **A skipped deploy** still takes a changed password.

**Rotation.** Change the password in the database, then in the vault, then
deploy. Any deploy will do, including one that is skipped. Logins already
made keep working. New logins fail between the first step and the deploy,
as for any binding.

**Changing the role.** Declare the other role. Under `enforce`, it is
verified before the swap.

- A managed role that is replaced is retired (`NOLOGIN`).
- A declared role that is replaced loses only the platform's own grants
  (below, retiring a binding).

**Removing the declaration.** A request without `db_role` means "no
change", which is also what every older CLI sends, so removal never
unbinds.

- The deploy answer carries `db_role: {role, origin, verified}`.
- When the project declares no role and the origin is `declared`, the CLI
  prints: `note: catalog-service stays bound to the declared role
  catalog_service_user; an administrator changes it with db-role set`.

**Administrator precedence.** `db-role set` makes any binding the
administrator's (`Change::origin = Some(Admin)`).

**Retiring a binding.**

- **Today**, `retiring` acts only on managed bindings. It now acts on
  every binding:
  - **Managed:** as today, `NOLOGIN` and `enqueue::unmap`.
  - **Declared or admin:** the platform takes back only what it granted:
    `enqueue::unmap`, `REVOKE USAGE ON SCHEMA _cufflink`, and `REVOKE
    EXECUTE` on `OWN` and `SHARED`. The role itself is left alone.
- **`Retiring` is split in two:**
  - `Retiring::retire_in(self, tenant_db)` does the work on a session it is
    given, and takes no lock.
  - `Retiring::retire(self, state)` is the locked wrapper. It takes the
    database's turn and advisory lock through `in_turn`, then calls
    `retire_in`.
- **Who calls which:**
  - Deleting a service calls `retire`, as today.
  - Replacing a binding calls `retire_in` under the turn and lock that
    `apply_locked` already holds.

  The turn is a tokio `Mutex` that cannot be re-entered (`turns.rs`), and
  the advisory lock sits on a second session. Calling `retire` from inside
  `apply_locked` would therefore wait forever.
- **It runs whenever a binding is replaced by another role:**
  - by a declaration (§4.3 step 9.5);
  - by `db-role set --role C` over a binding of another role, from the
    same step of `apply_locked`, since both paths share it.

  So every role that loses its service loses the platform's grants with
  it, whichever path replaced it.
- **Its log lines name why it ran:** "the deleted service's" or "the
  replaced binding's".
- **This changes behaviour for admin bindings**, which kept these grants
  before. The platform's `REVOKE` acts on the role's grants on those three
  objects, whoever made them: a grant a superuser made by hand on
  `_cufflink`, `OWN` or `SHARED` to that role goes with it. The release
  notes say so.

**Backwards compatibility.**

- Existing bindings read as `Platform` or `Admin`.
- A service bound with `db-role set` that adds a declaration of the same
  role is either `Unchanged`, or `Refresh` when the allowlist admits the
  role. `confined` never refuses it.
- With `declared_roles` empty, nothing changes.
- Rolling back below v0.41.0 leaves declared bindings as ordinary bindings,
  after `declared_roles` is removed (§6).

**Audit and metrics.**

- `db_role.declare` records `{role, previous_role, previous_origin, taking,
  verified, failed}`.
- `db_role.declare_refused` records `{role, gate, plan}`, with the full
  detail of §5.4.
- Neither ever holds the password.
- `cufflink_db_role_declarations_total{tenant, result}` counts each
  outcome, where `result` is `bound`, `refreshed`, `unchanged`,
  `refused_login` or `refused`.

## 8. Part 2: stream claims by prefix

### Should the tenant key extend its own claims?

No.

- **Claims are a cross-tenant allocation.** They hand out a namespace that
  every tenant shares, first come, first served. `validate_claims` refuses
  overlaps across tenants, and subjects outside `dw.{tenant}.>` are where
  systems outside Cufflink publish.
- **A writable path invites squatting.** Claims that deployers could write
  would let a tenant's CI take names that other tenants need, and would
  make the deploy credential the allocator of that shared space.
- **A granted prefix is already a claim.** "A prefix that an administrator
  granted once, within which the tenant claims freely" gives no more than
  claiming the prefix itself. For subjects, that works today: `catalog.>`
  admits every subject under it.

Only streams lack this. The design widens what one administrator claim can
express, and the deploy path stays read-only on claims.

### Design

- **Grammar.** A stream claim is either a name (`CATALOG_EVENTS`) or a
  stem followed by `*` (`CATALOG_*`). The stem is non-empty, valid under
  `is_valid_stream_name`, and at most 63 characters long.
- **One test decides a match.**
  - `Claim::matches_stream(&self, name)` is equality for a name and
    `starts_with(stem)` for a stem.
  - `Namespace::admits_stream` (below) is the only caller of
    `Claim::matches_stream` that admits a stream. It replaces
    `Namespace::has_stream`, and also the equality with
    `Claim::stream(name)` that `stream_scope_violation` uses today.
- **Two kinds of claim, two meanings.**
  - **An exact name is an adoption:** "this stream is the tenant's,
    whoever made it". It admits the stream with no further condition, as
    every stream claim does today.
  - **A prefix is an allocation:** "streams under this stem are the
    tenant's when its own services declare them". It admits a stream only
    on that condition (below).

  So an exact name beside a prefix that covers it (`CATALOG_LEGACY` beside
  `CATALOG_*`) is never redundant. It adopts a stream the tenant's services
  do not declare, such as one made outside Cufflink. It is allowed.
- **Collisions.**
  - **Between tenants:** two stream claims collide when either admits the
    other's name, or when one stem begins the other. A collision is
    `Taken`, as today.
  - **Within one tenant:** only a prefix that another of its prefixes
    covers is refused, as `Covered` (`streams: 'CATALOG_X*' is covered by
    'CATALOG_*'`). This is the same rule as `Repeated`, so the list of
    prefixes stays minimal.
- **Reserved stems.** A stem is refused (`ReservedStream`) when it begins,
  or is begun by, one of:
  - the platform's prefixes, `DW_` and `CUFFLINK_`;
  - JetStream's own, `KV_` (key-value buckets) and `OBJ_` (object stores),
    whose streams a prefix must never admit.

  Exact names keep today's rule.
- **Live streams are checked when the claim is set.**
  - In `put_nats_claims`, before `replace`, the platform lists the
    JetStream streams that each prefix in the request admits.
  - **Every prefix of the request is checked**, both new prefixes and ones
    the tenant already held, so a later put re-checks what is there now.
  - **A live stream under a prefix is the tenant's when either:**
    - it is in the tenant's declared-stream set (below); or
    - the same request claims it by name, as an adoption.
  - **Any other live stream under the prefix** refuses the claim with 409.
    The refusal names those streams, since an administrator is the caller,
    and gives the remedy: claim each one by name in the same request to
    adopt it, or have its owner claim or remove it.
  - **When JetStream cannot be reached**, the put answers 503 and changes
    nothing. The check is never skipped.
- **The declared-stream set.**
  - **What it holds:** the streams named in `streams` by the manifest of
    each recorded service's current version (`services.current_version`
    joined to `deployments`), whatever the service's status. An inactive
    service still declares its stream.
  - **Where it comes from:** one query in `repo::deployments`,
    `declared_streams(client, tenant_id) -> BTreeSet<String>`. It is read
    from the platform database, never from a replica's in-memory registry,
    which may lag.
  - **At deploy,** the set is the query's result with the deploying
    service's own entries replaced by those of the manifest being deployed,
    so a stream that this version stops declaring no longer counts. The set
    is `Namespace::declared` beside the claims, and
    `Namespace::admits_stream(name)` is the one test: an exact claim, or a
    prefix claim together with membership of the set.
- **Enforced at deploy as well.** A claim-time check cannot see a stream
  made later, out of band, under the prefix. So
  `subscription_scope_violation` and `stream_scope_violation` both go
  through `Namespace::admits_stream`.
  - A stream held through an exact claim binds as today.
  - A stream held through a prefix alone binds only when it is in the
    declared-stream set.
  - The effect is that a subscriber deployed before the service that
    declares its stream is refused until that service deploys. The
    refusal names the stream and says so: `stream 'CATALOG_EVENTS' is
    claimed only through 'CATALOG_*' and no service of this tenant
    declares it yet; deploy the service that declares it first, or claim
    it by name`.
- **A declaring service takes over a live stream of the same name.**
  Declaring a stream that already exists under a held prefix, or under an
  exact claim, makes the deploy plan that stream with `plan_stream` against
  the server's configuration, as for any declared stream. So:
  - its retention and storage must equal the declaration's, or the deploy
    is refused;
  - declared subjects are added, and subjects the server holds are kept,
    each with a warning;
  - `max_age`, `max_msgs` and `max_bytes` are raised and never lowered;
  - `duplicate_window`, `discard` and a declared `description` are set as
    declared;
  - the stream's messages and other consumers are left as they are.
- **A stream that a version stops declaring.**
  - The deploy neither deletes the stream nor changes it, as today.
  - Other services' running subscribers keep their consumers.
  - Once no current-version manifest declares the stream, a subscriber
    that holds it only through a prefix is refused at its next deploy,
    with the refusal above. A subscriber that holds it through an exact
    claim is not.
- **`stranded_by`** builds a `Namespace` for each affected tenant from
  the claims after the put, together with that tenant's declared-stream
  set from `declared_streams`. It then calls `validate_tenant_scope` with
  that namespace, as today. So:
  - a put that swaps exact claims for a prefix strands every running
    service that binds a stream the prefix admits but that no manifest
    declares, and refuses with 409;
  - a put that keeps the exact name beside the prefix strands nothing.

  One function builds the namespace for the deploy path, the claim path
  and `stranded_by` alike.
- **Unchanged:** `replace` and the `tenant_nats_claims` rows. Their
  pattern column is text.
- **CLI:**

  ```sh
  cufflink tenants nats-claims set harbor --subject 'catalog.>' --stream 'CATALOG_*'
  ```

  An older platform refuses `CATALOG_*` as an invalid stream name, so no
  feature is advertised for prefix claims.

## 8a. Alternatives not taken

- **The platform reads the password from the service's own secrets.**
  - The vault is a file local to the CLI. The platform's store is
    `service_configs`, which cannot be written before the first deploy:
    `config/apply` answers 404, and the config step runs after the deploy.
  - The guest can read any config value (`config::get`).
  - CI holds the vault key, as `config apply` already requires, and never
    the admin key.
- **The role in the manifest.** It belongs to an environment, and the
  manifest is hashed (§3).
- **A pending binding written before the service exists.** The password is
  sealed against the service's id, so a pending binding would need a
  second sealed table for every reseal, verify and storage mode.
- **Granting first, then revoking on refusal.** A grant that has run (for
  example `map`'s `unmap_others`) can already have removed the mapping of
  the role bound before. A crash would also leave the grants in place.
  Instead, every gate runs before any grant (§4.3). Only the rare refusal
  at step 8 takes anything back, and it takes back only what step 7 gave
  the new role.
- **Tenant-written NATS claims** (§8).

## 9. Code layout

| Where | What |
|---|---|
| `platform/src/db_roles/declared.rs` | `Declared`, `Taking`, `Refusal`, `taking`, `refusals`, `admits`, `facts`, `take`, `preview` |
| `platform/src/db_roles/provision.rs` | `Provisioning::{declaration, require_verified}`; `Change::origin`; the gates of §4.3 in `apply_locked`; `ensure` moved after them; `cufflink_grants` and `cufflink_ungrants`, with `unmap_others` moved to step 9.4; `Retiring` for every origin, split into `retire` and `retire_in`; the first-version gate in `before_deploy` |
| `platform/src/db_roles/login_pools.rs` | `probe` classifies 42501 as `Accepted` |
| `platform/src/db_roles/verify.rs` | `confined` and `declared_memberships` |
| `platform/src/db_roles/enqueue.rs` | `map` and `narrowing` without `unmap_others`; the revocations of the platform's own grants, for `Retiring` and `cufflink_ungrants` |
| `platform/src/repo/service_db_roles.rs` | `Origin`; `declared`; the index violation mapped to 422 |
| `platform/src/repo/services.rs` | `without_verified_role` skips services that have no deployment |
| `platform/src/policy/config.rs` | `declared_roles` |
| `platform/src/api/deployments.rs`, `deployer/pipeline.rs` | `DeployRequest::db_role`; admission; `take` and `preview`; the plan and answer fields; `FEATURES` |
| `platform/src/api/db_roles.rs` | `show` carries `origin` |
| `platform/src/api/tenants.rs`, `nats_manager/subjects.rs`, `deployer/validator/subscriptions.rs`, `repo/deployments.rs` | prefix stream claims; `declared_streams` (each recorded service's current-version manifest); `Namespace::admits_stream`, used by the claim check, `stranded_by` and the deploy-time rule |
| `pgcodec/src/roles.rs` | `REACH_OF_THE_ROLE` |
| `secrets/src/string.rs` | `Deserialize` for `SecretString` (platform only) |
| `types/src/features.rs` | `DB_ROLE_DECLARATIONS` |
| `cli/src/project_config.rs`, `cli/src/commands/{deploy,config_cmd,db_role}.rs` | the declaration; the `[service].name` rule; the vault rules; the feature gate; the output |

## 10. Tests

### Unit tests (no database)

- **The decision.** `taking`: every row of §4.3, including a stored
  password that does not open. `Change::origin` through `target`:
  - `Declared` and `Admin` are set as given;
  - `None` keeps the existing origin;
  - a managed role gets `Platform`.
- **The checks.** `refusals`: one fixture of facts per check in §5.2, and
  one that passes. The redaction of §5.4: no other tenant, service or
  database is named.
- **The allowlist.** `admits`, and `DbRolesConfig::validate`.
- **Verification.** `verify::report`:
  - `confined` and `declared_memberships` fail for `Declared`;
  - they are `EXCESS` for `Admin`;
  - a binding's own `read_only_role` is not counted.
- **The probe** is tested against a real server, since a
  `tokio_postgres::Error` with a code cannot be built by hand (below).
- **The grants.** `cufflink_grants` holds no `DELETE`, and `unmap_others`
  holds nothing else.
- **The request.** `SecretString` deserializes. `Debug` of `DeployRequest`
  and `Admitted::request()` hold no password. Admission refuses:
  - a web or native mode;
  - an `svc_` name;
  - an empty password;
  - a password holding a NUL byte.
- **Stream claims.** `admits_stream`; collisions both ways; stem on stem;
  `Covered`; the reserved stems (`DW_`, `CUFFLINK_`, `KV_`, `OBJ_`, `K*`,
  `O*`); `stream_scope_violation` under a prefix.
- **CLI.**
  - Parsing `[db_role]`, which is refused without `[service].name` or with
    a `[service].name` other than the manifest's.
  - A missing vault entry fails before anything is sent.
  - `unreferenced_vault_secrets` counts `password_secret`, and `--prune`
    accepts it.
  - A `[secrets]` mapping of the same entry is refused.
  - The request body, with and without the declaration.
  - A platform without the feature is refused.

### Integration tests

These run against real Postgres with the platform's harness
(`cufflink_testkit::PlatformProcess`, `TestDb`), in
`platform/tests/db_roles_declared.rs`, and as a platform that is no
superuser in `platform/tests/db_roles_unprivileged.rs`. Roles are made as
the operator, and the tenant runs `enforce` with `declared_roles` set.

- **The probe.** `probe` classifies 42501 (no `CONNECT`) as `Accepted`;
  28P01 (a wrong password) and 28000 (a `NOLOGIN` role, the code a
  `pg_hba.conf` reject also gives) as `Refused`; 53300 (`CONNECTION LIMIT
  0`), 3D000 and a server it cannot reach as `Unanswered`. 57P03 cannot be
  provoked on a running server; it takes the arm of every code outside
  class 28 and 42501.

- **One-pass first deploy.**
  - The first deploy succeeds.
  - Migrations run as the role.
  - The origin is `declared`, and the binding is verified.
  - No `svc_` role exists.
- **Origin survives.** After a second deploy that changes the
  manifest, so that `before_deploy` and `after_deploy` provision, the
  origin is still `declared`.
- **Unchanged.** A deploy identical to the last one writes no new
  `db_role.declare` row. `role_name`, `password_enc` and the origin are
  unchanged.
- **Rotation.** A changed password, sent on a skipped deploy, is taken.
- **Refusals.** Each of these is refused with nothing written and no grant
  made, and the binding in place stays as it was:
  - a wrong password;
  - a role outside `declared_roles`, including the `Refresh` of an admin
    binding while the list is empty;
  - an `svc_` role;
  - a role carrying a marker;
  - a superuser;
  - a member of `pg_read_all_data`;
  - a role bound to another tenant's service;
  - a schema that another role owns;
  - a `NOLOGIN` role, refused by `can_login` before any probe, and a role
    without `CONNECT`, refused with no 503; both answer as a wrong
    password does, the check that failed in the audit row;
  - a `CREATEROLE` role.
- **Errors that leave the password open.** A role at its connection limit
  (53300) makes the request a 503 that does not quote the server, and
  nothing is written.
- **No disclosure.**
  - A refusal for a role bound in another tenant names neither that tenant
    nor its service.
  - A role reaching another database is counted, and the database is not
    named.
  - The log and the audit row hold the full detail.
- **Refused probes are audited.** A wrong password is audited as
  `db_role.declare_refused` and counted, from a deploy and from a plan
  alike.
- **`confined`.**
  - These are refused, each naming its object:
    - a role that owns a table in a second database;
    - a role that holds `SELECT` on a sibling service's table;
    - a role that owns a function in `public`.
  - These are accepted:
    - a role granted a privilege in another database, which is reported
      as `granted_elsewhere`, including one another tenant's role grants
      it;
    - a role whose policy or collation sits in its own schema, with the
      policy's namespace taken from `polrelid`;
    - a role whose default privileges are global
      (`ALTER DEFAULT PRIVILEGES FOR ROLE … GRANT SELECT ON TABLES TO …`
      with no schema);
    - a role holding the platform's three grants.
  - A role holding `CREATE` on `_cufflink` is refused.
  - A grant made after the binding un-verifies it at the next `db-role
    verify`.
- **No grant before the gates.**
  - A running service that enqueues is bound to role A.
  - A declaration of role B is refused, once at gate 3 (wrong password)
    and once at gate 5 (a missing table grant).
  - After each, A is still mapped in `_cufflink.service_roles`, B holds no
    `_cufflink` grant, and a write through the service still enqueues.
- **A failure between grant and write.**
  - The new failpoint `CUFFLINK_TEST_FAIL_AFTER_DECLARED_GRANTS` fails the
    request after step 7. Its pattern is `fail_after_schema`'s.
  - A still serves, and B holds only its additive grants.
  - The next deploy completes the binding.
- **A refusal at step 8 takes back B's grants.** A failpoint revokes `USAGE`
  on `_cufflink` from B between steps 7 and 8, standing in for a
  concurrent change to the catalog. The request is a 503. B is left with
  no `_cufflink` grant and no `service_roles` row, while A's row and its
  enqueues are untouched.
- **Readiness.**
  - Without platform membership, `enforce` refuses with the `GRANT`, both
    for the deploy and for `?plan=true`, on a platform that is no
    superuser (`db_roles_unprivileged`), and once the operator runs it
    the deploy binds the role.
  - Under `warn`, a binding that fails a readiness check (`CREATE` on
    `public` through `PUBLIC`) is written unverified.
- **Precedence.**
  - An admin binding of another role refuses the declaration with 409.
  - An admin binding of the same role is refreshed, and keeps
    `read_only_role` and origin `Admin`.
  - A later `db-role set` makes the binding `Admin`.
  - A managed binding that a declaration replaces leaves its role
    `NOLOGIN`.
- **Replacing a binding completes, and retires the old role.**
  - A declaration of B replaces a declared binding of A.
  - Separately, `db-role set --role C` replaces a declared binding of B.
  - Each completes within a 10-second timeout, proving that `retire_in`
    runs under the held lock and no deadlock occurs.
  - Each leaves the replaced role with no `_cufflink` grant and no
    `service_roles` row. The role itself still logs in and still owns what
    it owned.
- **A concurrent admin `db-role set`.**
  - The test runs a `db-role set` and a declaration of another role at
    once.
  - The outcome is one consistent binding: either the admin's, with the
    declaration refused with 409, or the declared one followed by the
    admin's.
  - It is never a declared binding written over an admin binding of
    another role.
- **Two tenants declare one role at once.** A role cannot pass the gates
  in two tenants, since what it owns in one database is reach in the
  other; the race is therefore between a declaration and another
  tenant's binding of the role. The test holds that binding uncommitted
  until the declaration waits on the unique index, then commits it. The
  declaration gets the `bound_elsewhere` 422, audited, and what it was
  granted is taken back.
- **A mapping left behind.** Another role mapped to the service is
  reported as `outbox_mapped_elsewhere` by `db-role verify`.
- **V11** names the roles bound more than once, and applies once each
  carries one service.
- **The first-version gate.** Without a declaration, under `enforce`, the
  first deploy of a schema owned by another role is a 422 that names the
  declaration. No version is activated. The recorded service does not
  block `warn → enforce` for the tenant's other services.
- **Deleting a service.**
  - Deleting a declared service unmaps it.
  - It revokes `USAGE` on `_cufflink` and `EXECUTE` on `OWN` and `SHARED`.
  - The role can still log in and still owns its schema.
- **The plan.** `?plan=true` reports `taking` and the refusals. It writes
  nothing, seals nothing, grants nothing and runs no outbox bootstrap.
- **No password anywhere.**
  - An `--async` deploy (`?async=true`) holds the password in neither
    `deploy_jobs.request`, `result` nor `error`, nor in `GET
    /api/deploys/{id}`.
  - Across every test, the password's bytes appear in no row of
    `audit_log` or `service_db_roles.verify_report`, and in no answer.

#### Other integration suites

- **`platform/tests/db_roles_unprivileged.rs`.** The platform role is
  `NOCREATEROLE` on PostgreSQL 16 and later, and the membership is granted
  by the operator. A declared first deploy succeeds, with no `CREATE ROLE`
  attempted. On PostgreSQL 15, the remedy uses that version's `GRANT`.
- **`platform/tests/consumers_deploy.rs`.**
  - With `CATALOG_*` claimed, a service that declares `CATALOG_EVENTS`
    deploys and creates the stream.
  - A stream outside the prefix is refused.
  - Another tenant's `CATALOG_EVENTS` refuses the prefix with 409, and so
    does a live `CATALOG_OLD` that no current-version manifest of the tenant
    declares.
  - A later put that keeps `CATALOG_*` is refused once such a stream
    appears.
  - With JetStream unreachable, the put is a 503 and changes nothing.
  - **Adopting beside a prefix.** A live `CATALOG_LEGACY`, made outside
    Cufflink and declared by no manifest, with `CATALOG_*` and an exact
    `CATALOG_LEGACY` in one put: the put succeeds, and a subscription
    binds to `CATALOG_LEGACY`.
  - **Swapping claims.** A put that replaces the exact claims
    `CATALOG_EVENTS` (declared by a running service) and `CATALOG_LEGACY`
    (bound by a running subscriber) with `CATALOG_*` alone is refused by
    `stranded_by` because of `CATALOG_LEGACY`. The same put that keeps
    `CATALOG_LEGACY` by name strands nothing.
  - **Order of deploys.** A subscriber of `CATALOG_EVENTS`, held through
    the prefix alone, is refused until the service that declares the
    stream deploys. An inactive declaring service counts.
  - `CATALOG_X*` beside `CATALOG_*` is refused as `Covered`, and
    `CATALOG_LEGACY` beside `CATALOG_*` is not.
  - A stream made out of band under the prefix, after the claim, cannot be
    bound by a subscription until a service of the tenant declares it.

### End-to-end test

`cli/tests/db_role_declared.rs`, with `PlatformProcess`:

1. As the operator, the test makes the role, its schema and the
   platform's membership. This is the work of an invented migration tool.
2. It makes a project with a `Cufflink.toml` that declares the role and
   sets `[service].name`. The vault is made by the test with `cufflink
   secrets init` and `secrets set`; the test never opens a committed vault
   or key.
3. `cufflink deploy --bundle`, with the tenant's deploy key alone, on a
   tenant under `enforce`, succeeds.
4. `db-role show` reports `declared, verified`, and a data route answers
   200.
5. A second run reports the binding as unchanged.

The suite also covers these refusals, each made before any upload:

- a platform whose `supports` lacks the feature;
- a `Cufflink.toml` in a parent directory that has no `[service].name`;
- a `[secrets]` mapping of the binding's entry.

All tests use the repository's invented fixtures (`acme`, `harbor`,
`catalog-service`, `loan-service`), run concurrently, and each uses a
database of its own.

## 11. Docs

- **`docs/db-roles-and-secrets.md`** gains a section, "Declaring a role
  with the service". It covers:
  - the precondition of §2 item 2;
  - `declared_roles` and its ordering rule;
  - the `Cufflink.toml` shape and its vault rules;
  - rotation, change, removal and deletion.

  Rollout step 2 is amended to match.
- **`docs/policies.md`:** `declared_roles`, `confined` and
  `declared_memberships`.
- **`docs/deployment.md`:** the `db_role` request field, and the plan and
  answer fields.
- **`docs/schema-ownership.md`:** adopting a schema that a role owns links
  to the declaration.
- **`docs/subscriptions.md`:** prefix stream claims.
- **`docs/observability.md`:** the new metric and log events.
- **`docs/versions.md`:** the `db_role_declarations` row, and the release
  notes, which cover:
  - the first-version gate (§4.6), called out as a behaviour change;
  - the ordering rule for `declared_roles` (§6);
  - the duplicate-binding query to run before upgrading;
  - the change in retirement for admin bindings, on deletion and on
    replacement. The platform's `REVOKE` on `_cufflink`, `OWN` and `SHARED`
    also removes grants on those objects that a superuser made to the role
    by hand;
  - platforms before CLIs, and v0.41.0 as the minimum CLI for declarations.

## 12. Release

Following `CONTRIBUTING.md`, there are three focused pull requests against
`main`, none of which changes a crate version:

1. The platform part, with the folded-in parts of #91, the
   `cufflink-types` and `cufflink-pgcodec` changes, the unpublished
   `cufflink-secrets` change (`Deserialize`), and the platform docs.
2. The CLI part, which holds the password in `zeroize::Zeroizing` and so
   adds no dependency on an unpublished crate, and the end-to-end test.
3. Prefix stream claims.

Each passes `cargo fmt --all -- --check`, `cargo clippy --workspace
--all-targets -- -D warnings` and `cargo test --workspace`.

A maintainer then cuts a minor release, `v0.41.0`, by tag. Its release
pull request bumps the published crates that changed: `cufflink-types`,
`cufflink-pgcodec` and `cufflink-cli` (`cufflink-secrets` is unpublished).
It also records the release in `docs/versions.md`.

Roll out platforms first and CLIs second, then set `declared_roles`.

## 13. Open questions and defaults

1. **#91.** Fold `require_verified`, the first-version gate and the
   `--inactive` rule into PR 1, and close #91 (default).
2. **The `declared_roles` opt-in.** It is empty by default: one
   administrator act per tenant (default).
3. **Memberships.** A declared role may be a member of no role at all
   (default).
4. **A missing schema.** The declaration is refused, and such a service is
   given a managed role (default).
5. **A failed first deploy** leaves the service recorded, as today. It no
   longer blocks `enforce` (§4.6) (default).
6. **The password's source.** The CLI reads it from the vault and sends it
   in the deploy request (default, §8a).
7. **Retiring an admin binding.** Deleting a service bound by an
   administrator, or replacing that binding, now takes back the platform's
   own `_cufflink` grants (default, §7). The alternative is to do this
   only for declared bindings.

## 14. As implemented

Where the implementation of Part 1 settles what the design left open:

- **`preview`** is called by `api::deployments::plan` for both a recorded
  and an unrecorded service, under the deploy lock for a recorded one,
  rather than from `run_locked`. The plan's `skipped` branch reports the
  declaration too.
- **Taking back the grants** is one function, `enqueue::relinquish`, shared
  by retiring a binding and by a refused declaration. It revokes `USAGE` on
  `_cufflink` and `EXECUTE` on `OWN` and `SHARED`, and deletes the role's
  mapping to the service: a superset of `cufflink_ungrants`, harmless for
  the reasons given in §4.3 step 8. It runs on a refusal at gate 5, at
  step 8, and when the write loses the unique index to another tenant,
  each only when the role was not already the service's binding. A gate 4
  refusal never runs it, since the role may then be the platform's own or a
  superuser.
- **`confined` and `declared_memberships`** are checked for declared and
  admin bindings only; a role the platform made is not read for them.
- **`REACH_OF_THE_ROLE`** lives in `cufflink_pgcodec::roles`, as designed.
  A row of another database, and any shared object but the role's
  `CONNECT` or `TEMPORARY` on its own database, is counted as "outside this
  database" and never named to the tenant.
- **The failpoint** is `CUFFLINK_TEST_REVOKE_AFTER_DECLARED_GRANTS=1`, which
  revokes `USAGE` on `_cufflink` from the declared role between steps 7 and
  8, standing in for a concurrent change of the catalog. No failpoint fails
  a request between the grants and the write: the step-8 refusal covers
  the same state, and a crash there leaves only the bounded, additive
  grants described in §4.3.
- **The deploy answer's `db_role`** is read after activation and is best
  effort: a failed read logs and answers `null`, never failing a deploy
  that succeeded.
