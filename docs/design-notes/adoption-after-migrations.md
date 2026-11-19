# Checking adopted tables after the deploy's own migrations

A service may ship SQL migrations (`migrations:`, with
`migration_history … mode = apply`) for tables it adopts: a `schema:
adopted` service, or a managed one with `#[table(managed = false)]` or
`private_tables`. Its manifest declares those tables as they stand once the
migrations have run. This note moves the deploy's adoption check after
those migrations, so that a release can ship a migration and the
declaration it makes true together.

## 1. Problem

A deploy checks the adopted declaration against the database before it
applies the migrations:

- `deployer/schema.rs` `apply` runs the verify pass of
  `sql_migrations::run`, then `prepare`, which calls
  `executor::prepare_schema_changes`. That function reads the live schema
  (`look`) and refuses the deploy there with
  `validate_adoption(...).accepted()?` whenever `SchemaRequest::adopts()`
  holds.
- Only after that does `run_phases` apply the pending migrations.
- `plan` calls `prepare` too, so a plan of that deploy fails the same way
  (422, CLI exit 3).

So a release whose migration adds a column or a table, and whose manifest
declares it, is always refused with "the database does not hold what the
manifest adopts", unless something else applied the migration first. That
holds for a managed schema's adopted tables as well: the
[schema ownership guide](../schema-ownership.md#private-tables) tells a
service to create a private table "in an SQL migration", and the deploy
that ships both is refused. The only way through is two releases, the
migration first and the declaration second, which relies on
`accept_extra_columns` and on the first release not reading what it adds.

Two related faults follow from the same order:

- A migration that drops, renames or retypes a declared column passes the
  check made before it. The version is then activated over a schema that
  no longer holds what it declares, and its data routes fail at run time.
- The version's schema snapshot records the adopted column types the check
  read (`AdoptionReport::adopted`, stored by `activate_module` as
  `Adoption::Read`). The data routes take their casts and
  `may_be_omitted` from it. A column that a migration adds or retypes is
  therefore recorded as it was before the migration, or not at all.

## 2. Decision

One predicate chooses when the adoption check is made:

> The deploy adopts a table, its history's mode is `apply`, and the verify
> pass leaves migrations pending.

When it holds, the check is made on a fresh read of the live schema after
the migrations have run, and before `on_migrate`, the destructive phase and
activation. Otherwise it is made where it is made today.

### 2.1 Executor

`SchemaRequest` gains `adoption: AdoptionCheck`:

```rust
pub enum AdoptionCheck {
    Now,
    AfterMigrations,
}
```

- `prepare_schema_changes` still reads the live schema and runs
  `validate_adoption`. Under `Now` it refuses on errors (`accepted()`), as
  today. Under `AfterMigrations` it returns the report as found, marked
  `deferred: true`, and refuses nothing.
- A new `executor::check_adoption(db, adopted)` reads the live schema
  (`introspect::live_schema`) and returns
  `validate_adoption(db.schema(), adopted, &live).accepted()`.
  `validate_adoption` stays the only place that judges a declaration.
- `AdoptionReport` gains
  `#[serde(skip_serializing_if = "std::ops::Not::not")] deferred: bool`,
  as `MigrationsReport::locked` is. A report that is not deferred
  serializes exactly as it does today.
- The rollback path and every existing caller pass `AdoptionCheck::Now`. A
  rollback runs no migrations.

### 2.2 Deployer

`deployer/schema.rs` derives the check from the verify pass's report, in
one function used by both paths:

```rust
fn adoption_check(verified: Option<&MigrationsReport>) -> AdoptionCheck
```

It returns `AfterMigrations` exactly when the report's mode is
`HistoryMode::Apply` and its `run.pending` is not empty, and `Now`
otherwise. Whether the deploy adopts anything needs no extra test: a
request that adopts nothing produces no adoption report, so there is
nothing to defer.

- **`plan`** runs the verify pass first and then `prepare`, the order
  `apply` already uses. The verify pass is read-only and takes no lock, so
  the reordering changes only which error a plan reports when the history
  and the adoption are both wrong: the history's, as a deploy reports.
  - With migrations pending under `apply`, the plan succeeds. Its
    `adoption` carries `deferred: true` and, in `errors`, what the current
    schema lacks: what the pending migrations are expected to provide.
  - With nothing pending, or under `verify_only`, the plan checks adoption
    as today, and errors fail it.
- **`apply`** passes the same check to `prepare`. In `run_phases`, right
  after the SQL migrations ran and before `on_migrate` and the destructive
  phase, a deferred adoption is checked with `check_adoption` over the
  adopted tables of `tables_of(manifest, left)`. `Phases` carries the
  resulting report. `SchemaReport::of` takes it in place of the deferred
  one from `prepare`, so the report returned, and the snapshot
  `activate_module` stores, come from the read made after the migrations.
- When that check refuses, the error stays `PlatformError::Validation`
  (422). When the deploy applied migrations (`run.applied`), it names them,
  in the manner of `stayed_applied`:
  `…adopts: <findings>; the migrations this deploy applied stay applied:
  V2__add_loan_kind`. When it applied none, the message is today's,
  unchanged.
- `MigrationRef` gains `file_stem()` (`V{version}__{name}`), which both
  this message and `MigrationsReport::warnings` use.

### 2.3 CLI

`plan_lines` prints nothing about `adoption` today, since an adoption
error used to fail the plan. With `adoption.deferred`, it prints one line
per error, `adopted once the pending migrations apply: <finding>`, so that
a plan that used to exit 3 does not go green without saying what it
expects the migrations to make.

### 2.4 Why the plan does not apply and roll back

Postgres DDL is transactional, so a plan could apply the pending
migrations in a transaction, check adoption and roll back. That would be
exact, but:

- it takes the migrations' locks on live tables during a plan;
- it cannot run a `-- cufflink:no-transaction` migration;
- it consumes sequence values;
- it breaks the plan's promise that it changes neither database and takes
  no migration lock.

The deploy checks again after applying in any case, so the plan's part is
to say what it expects, not to prove it.

## 3. Failure semantics

- **A migration fails.** Unchanged: it rolls back, the migrations before it
  stay applied and recorded, and the deploy stops before the adoption
  check and before activation. The previous version keeps serving.
- **Every migration succeeds and the check refuses** (the declaration does
  not match what the migrations made):
  - the deploy stops before `on_migrate`, the destructive phase and
    activation, and the previous version keeps serving;
  - what ran stays: a managed schema's additive phase, and the migrations,
    recorded in the history. Both are expand-only, and so safe for the
    previous version, as they are when a later phase fails today;
  - the error names the findings and the migrations applied;
  - the remedy is a release whose declaration matches. A retry of the same
    release finds nothing pending and is refused before anything runs, as
    today.
- **Another runner holds the migration lock.** The apply waits as today,
  then applies what is still pending. The check reads the schema after the
  lock is released, so it sees what the other runner made too.
- **Retry after a partial failure.** The verify pass sees what was
  applied, and only the rest runs.

## 4. Behaviour changes

- A deploy that is refused today because its declaration is ahead of the
  live schema, and whose pending migrations make it true, now succeeds.
  A plan of it succeeds with `adoption.deferred: true`.
- A deploy whose pending migrations break a declared table (drop, rename
  or retype a declared column, drop a declared table) is now refused after
  the migrations ran. Today it is activated over a schema its version
  cannot serve.
- The snapshot stored for a version deployed with pending migrations under
  `apply` records the adopted columns as they are after the migrations.
- Deploys with no migration pending, `verify_only` histories, rollbacks,
  and services that adopt nothing are unchanged, as are
  `accept_extra_columns`, the schema guard's live read and the unserved
  checks.
- Wire: a plan's or deploy's `adoption` gains `deferred`, present only when
  true. Older CLIs ignore it. No platform feature is advertised for it: an
  older platform never defers, and a CLI reads the key only when it is
  there.

## 5. Tests

Platform integration tests, in `platform/tests/sql_migrations_deploys.rs`:

1. A `schema: adopted` service, history in `apply` mode, V1 applied by
   another runner, deploying V1–V2 and declaring the column V2 adds. The
   deploy succeeds, the history records V2, and a create through the data
   route that omits the column succeeds, which proves the stored snapshot
   comes from the read after V2 (the column has a default).
2. The same with a table that a migration creates, declared in
   `private_tables`.
3. A managed schema whose private table a migration creates, in the same
   release. The deploy succeeds.
4. A `schema: adopted` deploy whose migration does not make the declared
   column. The deploy is refused (422) with the finding and
   `V…__…` of the migration applied. The previous version is still served
   and the migration stays recorded. A second attempt finds nothing pending
   and is refused with today's message, which names no migration.
5. A managed schema, refused after its migrations. The additive phase and
   the migration ran. `on_migrate` (a hook that writes a row) did not, nor
   did the destructive phase (a dropped column is still there). The
   previous version is still served.
6. A failing migration under an adopted schema. The error is the
   migration's, as today.
7. A plan with a migration pending that makes a declared column. It
   succeeds with `adoption.deferred: true`, the missing column is in
   `adoption.errors`, and nothing is applied.
8. A plan with nothing pending and a declared column missing. It is
   refused (422), as today.
9. `verify_only` with migrations pending and a declared column missing.
   The deploy and the plan are refused before anything runs, as today.
10. The migration lock is held by another runner, which applies the
    pending migration itself, records it and releases the lock. The deploy
    waits, applies nothing, and the check, made on a fresh read, passes.

Unit tests:

- `adoption_check` for no report, `verify_only`, `apply` with and without
  pending migrations.
- `AdoptionCheck::AfterMigrations` returns findings without refusing, and
  `Now` refuses as before. A report that is not deferred serializes
  without `deferred`.
- The error after migrations names the applied migrations, and keeps
  today's message when none were applied.
- `plan_lines` prints a deferred adoption's findings and nothing for one
  that is not deferred.

The managed-schema tests that exist (`migrations_run_after_the_tables_the_manifest_adds`
and the rest of the file) stay as they are and must pass unchanged.

The SDK's test host (`cufflink_fn::testing::TestDb`) applies migrations but
has no adoption check, so it has nothing to reorder, and this change leaves
it as it is.

## 6. Quality bar

- No todos and no partial paths: plan, apply, the CLI and the docs change
  together.
- The judgement stays in `validate_adoption` and the predicate in
  `adoption_check`. Each path calls them once.
- No comments beyond what the order needs, and no duplicated formatting of
  migration names.
- Docs: the phase order and the plan in
  [schema ownership](../schema-ownership.md#sql-migrations-in-deploys),
  and the release notes under "Changes in v0.39.0" in
  [versions](../versions.md).
