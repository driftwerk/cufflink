# Schema Ownership, SQL Migrations and Deploy Plans

Needs a platform that advertises `schema_adopted` (adopted schemas and
`schema_name`), `sql_migrations` (`migrations:`), `schema_guard` (the guard
and `--accept-drift`/`--purge`), `async_deploy` (`--plan` and deploys
that outlive their request) and `private_tables` (`private_tables:`) in
`GET /api/version` → `supports`. A manifest
that uses one demands it, so an older platform refuses the deploy (422)
instead of ignoring it ([versions](versions.md)).

By default the platform owns a service's schema: it creates the schema named
after the service (`note-service` → `note_service`) and changes its tables to
match the manifest at every deploy. Everything below is opt-in; a manifest
that declares none of it deploys exactly as before.

## Adopting a schema that exists

A service moved onto Cufflink from elsewhere keeps the schema it has:

```rust
cufflink::service! {
    name: "note-service",
    tables: [Note],
    schema: adopted,
    schema_name: "note_service",
}
```

- `schema: adopted` — the platform emits no DDL for the service's schema.
  At each deploy it reads the database and checks that every declared table
  and column is there with a type that holds it; a table or column that is
  missing or of another type refuses the deploy (422) naming it. Columns the
  database has beyond the manifest are left alone.
- `schema_name` — the Postgres schema, when it is not the service name with
  `-` as `_`. It is fixed at the first deploy: a later deploy that changes
  it, or names a schema another service of the tenant uses, is refused.
- An adopted table may not declare `index(...)` or `check(...)` (the
  platform would never create them), a `soft_delete` table needs its
  `deleted_at` column, and a view cannot be adopted.
- The data routes read and write adopted enum, domain and `citext` columns
  with the casts they need; a create leaves out the columns that have a
  default or take nulls when the body omits them.

`#[table(managed = false)]` adopts one table of a service whose other tables
the platform keeps: it is never created, altered or dropped. A table that
goes from adopted to managed is from then on changed as the manifest says,
and dropped when a later manifest leaves it out; the deploy plan warns of it.

A rollback to a version that adopted checks the adoption again and is
refused, like a deploy, when the database no longer holds it.

## Private tables

A table only the service's own SQL reaches is declared in `private_tables`:

```rust
#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "notes")]
pub struct Note { ... }

cufflink::service! {
    name: "note-service",
    schema: adopted,
    private_tables: [Note],
    routing: strict,
    custom_routes: [
        ("GET", "/notes/{note_id}", "get_note", perm("notes", "view")),
    ],
}
```

The platform checks a private table and does not serve it:

- **Checked.** Every deploy and rollback checks its declared columns against
  the database, as for any adopted table, and refuses (422) one that is
  missing or of another type. The version's schema snapshot records it.
  A scoped route reads the column types of its scope from it.
- **Not served.** It has no CRUD route, no `/{table}/stream`, no
  `/{table}/batch`, no GraphQL type and no OpenAPI path or schema. A request
  for one answers as for a table the service does not declare.
  `_meta/health` does not count it. `cufflink status` prints the tables of
  the project it is run in, as its manifest builds there (not those of the
  deployed version), and lists a private one by name under `Private tables
  (checked, not served)`, with no endpoints.

Rules, each a 422 at deploy:

- A private table is one that exists: the schema is `adopted`, or the table
  is `#[table(managed = false)]`. The platform never creates, alters or
  drops one. Create it in an SQL migration.
- It declares nothing only data routes read: `hooks`, `auth_required`,
  `permission_area` (unless it equals `scoped_by`, which it must where
  both are set), `owner_field` without `scoped_by`, `owner_self_service`,
  `search`, `pagination = keyset`, `sensitive`, `audit_redact`, or a
  column's `#[validate(...)]` or `#[restricted(...)]`. `cufflink deploy` refuses the same before
  it uploads. `scoped_by` with its `owner_field` and `soft_delete` are
  allowed.
- It needs no primary key. It may flag one column as the key, of any type;
  a table with a composite key declares none.
- Its name, compared as Postgres folds it, is declared once across both
  lists.
- `mode: native` and `mode: web` services declare none.

Moving a table between the lists is an ordinary deploy that runs no DDL for
it. A served table the platform made must also become `managed = false` when
it moves; the platform then leaves it as it is, search vector and indexes
included. `cufflink deploy --plan` lists the private tables under their own
heading and prints `table <t>: no longer served` for one the running version
serves. Moving it back to `tables` makes it a served table again, held to the
served rules (one key of a key type). A table that also goes back to
managed, by a deploy or a rollback, is taken over as any adopted table is:
the platform runs its `CREATE … IF NOT EXISTS` statements, rebuilds the
search vector it keeps, and changes the table as the manifest says from
then on. Its rows are kept, and the plan warns of the takeover.

What follows from "not served":

- **Seed.** `cufflink seed` naming a private table is `Unknown table`.
- **Backups.** A private table is in no platform backup; its owner backs it
  up. `cufflink backup restore --clear` never empties one. A backup taken
  while the table was served does not restore once it is private
  (`Unknown table in backup`): restore it before the move, or restore the
  other tables with `--tables`. A preview clone copies no private rows, and
  a `preview_tables` entry naming one is refused as an unknown table.
- **Database roles.** Verification asks only that a private table exists.
  What the service's role may do there is its owner's to grant
  ([database roles](db-roles-and-secrets.md#rollout-for-a-tenant)).
- **Events.** Nothing publishes CRUD events for one. A subscription to such
  a subject is accepted: the service may publish it itself.
- **Built-in handlers.** `delete_cascade` and `update_field` write to the
  table their config names, private or not.

`GET /api/services/{id}/schema` answers `private_tables` beside `tables`.

On a platform older than v0.30.0 the key is dropped ([versions](versions.md#rolling-a-platform-back)).

## SQL migrations in deploys

A service whose schema changes are written by hand ships them with its
deploys:

```rust
cufflink::service! {
    name: "note-service",
    tables: [Note],
    schema: adopted,
    migrations: "migrations",
    migration_history: ("schema_history", baseline = 6, lock = legacy_table_hash, mode = apply),
}
```

`migrations:` is a directory of the crate holding `V{version}__{name}.sql`
files. The macro embeds them byte for byte at build time (the checksum covers
every byte), and refuses at compile time a name that is not ASCII letters,
digits and `_`, an empty file, or versions that are not distinct and
ascending. `cufflink deploy` checks that a directory
of the project holds exactly the migrations the manifest carries, so a
module built from stale files is not deployed.

A deploy runs its phases in this order, each reported in the deploy's
progress: the additive schema changes, the SQL migrations, the `on_migrate`
hook, the destructive changes. A deploy with neither migrations nor
`on_migrate` runs both schema phases in one transaction, as before.

Each migration runs in a transaction of its own, unless its first line is

```sql
-- cufflink:no-transaction
CREATE INDEX CONCURRENTLY notes_created ON note_service.notes (created_at);
```

which runs it outside one, as `CREATE INDEX CONCURRENTLY` requires. An index
such a migration left invalid (the deploy was interrupted) is rebuilt by the
next deploy.

### The history

| `migration_history` key | Default | Meaning |
|---|---|---|
| table (first) | `refinery_schema_history` | The table the applied migrations are recorded in, in the service's schema, or `schema.table` for one elsewhere |
| `baseline = N` | *(none)* | Versions up to `N` are taken as applied without a local file. With no history yet, `baseline = 0` begins one for a service that was deployed before it had migrations |
| `lock = legacy_table_hash` | `platform` | Take the advisory lock a refinery runner derives from the history table's name, so the deploy and a runner that has not been retired never run at once |
| `mode = verify_only` | `apply` | Check the history against the files and apply nothing: another runner still owns the migrations |

The history is read before the deploy changes anything. A deploy is refused
(422, naming the version) when an applied migration's checksum differs from
its file, when a version is applied that the manifest lacks and is above the
baseline, or when a history is expected and not found: a history is begun
only by the first deploy of a service that makes its own schema, or by a
manifest with a `baseline` under every migration it declares. Anything else
names the table and schema looked in, which catches a history named wrongly.

A rollback never runs migrations; the migrations of the version being left
stay applied and are named in a warning.

Timeouts are `SQL_MIGRATION_LOCK_WAIT_SECS`, `SQL_MIGRATION_STATEMENT_TIMEOUT_SECS`
and `SQL_MIGRATION_LOCK_TIMEOUT_SECS` ([setup](setup.md#platform-configuration));
the metrics are `cufflink_sql_migrations_applied_total` and
`cufflink_sql_migration_duration_seconds` ([observability](observability.md)).

### Taking over from refinery

1. Declare the runner's history table, `lock = legacy_table_hash` and
   `mode = verify_only`, and deploy: the deploy only checks that its files
   and the history agree, while the service's own runner keeps migrating.
2. Remove the runner from the service, then switch to `mode = apply`.
   From then on each new migration is applied by the deploy that ships it.

## The schema guard

The tenant policy `schema_guard` ([policies](policies.md#keys)) decides what
a deploy does where the database already disagrees with the manifest:

| Mode | A deploy |
|---|---|
| `off` (default) | Runs the DDL of the manifest's diff, as before |
| `warn` | Reads the database first and reports in its plan: changes the database already holds (`satisfied`, skipped), places where it contradicts the manifest (`drift`), objects outside the manifest that depend on what a drop would remove (`dependents`) and, with `accept_extra_columns: false`, undeclared columns. It then runs what `off` runs; what it could not read is a warning |
| `enforce` | Refuses a deploy that meets drift or dependents (422), and changes how drops happen: a dropped column or table is archived, not dropped |

Under `enforce`:

- A dropped column is renamed `_archived_v{version}_{name}` and loses
  `NOT NULL`, and a dropped table is renamed the same way; a later deploy
  that declares the column or table again restores it. A
  name too long for Postgres is shortened and the original kept in a
  comment `cufflink:archived:{name}`.
- `--allow-destructive` is still needed to drop anything; `--purge` drops
  with `DROP … RESTRICT` instead of archiving.
- `--accept-drift` deploys the rest and leaves what the database contradicts
  as it is.
- An index declared under an existing name in another shape is replaced
  (the plan warns that writes wait for the rebuild); under `off` and `warn`
  it is left as it is, with a warning.
- A rollback is never refused by the guard; what it contradicts is reported.

A drop of something already gone (a column dropped by hand) is no change and
needs no permission. `cufflink_schema_guard_findings_total` counts the
findings.

## Plans and deploys that outlive their request

```sh
cufflink deploy --plan
```

asks the platform what the deploy would do — schema changes, SQL migrations
to apply, what the guard found, warnings — and deploys nothing. It changes
neither database, takes no migration lock and records nothing. It exits 3
when the platform refuses the deploy (422) and 1 when no plan could be made
(another deploy running, the network).

Against a platform that advertises `async_deploy` the CLI deploys with
`?async=true`: the platform answers 202 with the deploy's id at once, runs it
detached from the request (a CLI that is interrupted no longer stops it),
and the CLI follows `GET /api/deploys/{id}` every 2 s, printing each phase.
A second deploy of the same service is a 409 until the first ends. A deploy
whose replica stops heartbeating is marked lost by another replica within a
minute or so; the CLI keeps following a lost deploy for two minutes, since
its replica may still finish it. Finished deploys are kept
`DEPLOY_JOB_RETENTION_DAYS`.

The response of every deploy carries a `plan` key with what the deploy did.
