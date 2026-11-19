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

When it holds, the check is made in two steps:

1. Before anything runs, the declarations that are **settled** are checked
   and any finding on them refuses the deploy, as today. A settled
   declaration is an adopted table that the version being left adopted,
   declared the same in everything the check reads: its name, its columns
   and their types, its primary key, `owner_field`, `soft_delete` and
   `search.vector_column`. A finding there is drift that no migration of
   this release was written to repair, so nothing is applied for it. A
   first deploy has no version to leave, so nothing is settled. The
   findings on the other declarations, new or changed, are reported as
   deferred. With nothing settled, nothing is refused up front, not even
   a schema that does not exist yet: the first plan of a managed service
   runs before `ensure_schema` makes it, and reports `schema … is missing`
   as deferred.
2. After the migrations have run, and before `on_migrate`, the destructive
   phase and activation, every declaration is checked again on a fresh
   read of the live schema, settled ones included. A settled declaration
   that passed in step 1 proves nothing about what the migrations then did
   to it.

Otherwise the check is made where it is made today.

### 2.1 Executor

`SchemaRequest` gains `adoption: AdoptionCheck`:

```rust
pub enum AdoptionCheck<'a> {
    Now,
    AfterMigrations { settled: &'a [TableDefinition] },
}
```

- `prepare_schema_changes` still reads the live schema and runs
  `validate_adoption`. Under `Now` it refuses on errors (`accepted()`), as
  today. Under `AfterMigrations` it refuses on the errors of the `settled`
  tables when there are any (`validate_adoption` reports a missing schema
  even for no tables, so an empty `settled` is not checked), and otherwise returns the report of every adopted table, marked
  `deferred: true`. Its errors are then only those of new or changed
  declarations.
- `adopt::settled(declared, left_tables, left_adopted)` picks the settled
  declarations. It compares the projection the check reads (`Checked`,
  beside `validate_adoption`, so the two change together), with names
  folded and columns unordered. A change to anything else in a table,
  such as its hooks, leaves its declaration settled.
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
fn adoption_check<'a>(
    verified: Option<&MigrationsReport>,
    settled: &'a [TableDefinition],
) -> AdoptionCheck<'a>
```

`prepare` computes `settled` from the version being left (`left.tables`
and `left.adopted`). `adoption_check` returns `AfterMigrations` exactly when the report's mode is
`HistoryMode::Apply` and its `run.pending` is not empty, and `Now`
otherwise. Whether the deploy adopts anything needs no extra test: a
request that adopts nothing produces no adoption report, so there is
nothing to defer.

- **`plan`** runs the verify pass first and then `prepare`, the order
  `apply` already uses. The verify pass is read-only and takes no lock, so
  the reordering changes only which error a plan reports when the history
  and the adoption are both wrong: the history's, as a deploy reports.
  - With migrations pending under `apply`, the plan is refused for a
    finding on a settled declaration, as the deploy is. Otherwise it
    succeeds, and its `adoption` carries `deferred: true` and, in
    `errors`, what the new or changed declarations lack: what the pending
    migrations are expected to provide.
  - With nothing pending, or under `verify_only`, the plan checks adoption
    as today, and errors fail it.
- **`apply`** passes the same check to `prepare`. In `run_phases`, right
  after the SQL migrations ran and before `on_migrate` and the destructive
  phase, a deferred adoption is checked with `check_adoption` over every
  adopted table of `tables_of(manifest, left)`. `Phases` carries the
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

### 2.5 Why the migrations and the check are not one transaction

Making the migrations and the check after them atomic, so that a refusal
rolls the migrations back, was considered and not taken:

- `migrate_exec` runs each migration in a transaction of its own and
  records it in the history there, so that a failure leaves the ones
  before it applied and recorded. A refinery runner that shares the
  history relies on that, and so does the retry after a partial failure.
- A `-- cufflink:no-transaction` migration (`CREATE INDEX CONCURRENTLY`)
  cannot run inside a transaction at all, so one transaction could not
  cover every release.
- One transaction would hold every migration's locks on live tables until
  the check ends, where each now holds its own only while it runs.

The check after the migrations is therefore a safety net, not a rollback.

## 3. Failure semantics

- **A migration fails.** Unchanged: it rolls back, the migrations before it
  stay applied and recorded, and the deploy stops before the adoption
  check and before activation. The previous version keeps serving.
- **Every migration succeeds and the check refuses** (the declaration does
  not match what the migrations made):
  - the deploy stops before `on_migrate`, the destructive phase and
    activation, and the previous version keeps serving;
  - what ran stays: a managed schema's additive phase, and the migrations,
    recorded in the history. The previous version keeps serving over
    whatever the migrations made. SQL migrations are arbitrary: one that
    breaks a declaration can leave the previous version unable to serve
    that table. Writing migrations that the version being left can run
    over (expand only) is the author's responsibility, as it is whenever
    a later phase fails today, and the refusal keeps the new version from
    being activated over a schema it does not match;
  - the error names the findings and the migrations applied;
  - the remedy is a release whose declaration matches. A retry of the same
    release finds nothing pending and is refused before anything runs, as
    today.
- **A settled declaration has drifted.** The deploy and its plan are
  refused before anything runs, as today, whatever migrations are pending.
- **Another runner holds the migration lock.** The apply waits as today,
  then applies what is still pending. The check reads the schema after the
  lock is released, so it sees what the other runner made too.
- **Retry after a partial failure.** The verify pass sees what was
  applied, and only the rest runs.

## 4. Behaviour changes

- A deploy that is refused today because a new or changed declaration is
  ahead of the live schema, and whose pending migrations make it true, now
  succeeds. Drift on a settled declaration is refused up front as today.
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

Platform integration tests, in `platform/tests/sql_migrations_deploys.rs`.
Each fails on the platform before this change, except the `verify_only`
one, which pins what stays as it is:

1. `an_adopted_schema_is_checked_after_the_migrations_that_make_it`: a
   `schema: adopted` service, history in `apply` mode, V1 applied by
   another runner. V2 adds an enum column with a default and creates a
   table, and the manifest declares the column and, in `private_tables`,
   the table.
   - The plan succeeds with `adoption.deferred: true` and both findings in
     `adoption.errors`, and applies nothing.
   - The deploy succeeds, records V2 and reports an adoption that is not
     deferred.
   - Creates through the data route, one naming the enum value and one
     omitting the column, store `closed` and `open`. Both depend on the
     snapshot holding the column as V2 made it (the enum's cast, the
     default), so they prove that the report read after the migrations is
     the one stored.
2. `an_adoption_the_migrations_do_not_make_is_refused_after_them`: the
   migration does not make the declared column or table. The deploy is
   refused (422) with both findings and `V2__loan_stage`. The previous
   version is still served and V2 stays recorded. The same deploy again,
   and its plan, find nothing pending and are refused with today's
   message, which names no migration.
3. `a_managed_schema_checks_the_private_table_its_migration_makes`: a
   managed schema with `on_migrate` (a hook that writes a row), a column
   dropped and a column added, and a private table its migration creates
   without a declared column.
   - The deploy is refused after the migration. The added column exists,
     the dropped one is still there, the hook wrote nothing, and the
     previous version is still served.
   - A release whose migration makes the column then deploys, and its
     hook runs.
4. `a_failing_migration_under_an_adopted_schema_fails_as_it_did`: the
   error is the migration's (500, `V2__broken`), with no adoption finding.
5. `a_verified_history_checks_adoption_before_anything_runs`: under
   `verify_only` with a migration pending, the deploy and the plan are
   refused with the findings and run no DDL.
6. `adoption_is_checked_on_what_a_runner_holding_the_lock_made`: another
   runner holds the migration lock, applies and records V2 itself, then
   releases it. The deploy waits, applies nothing, and passes on its fresh
   read.
7. `drift_on_a_settled_declaration_is_refused_before_the_migrations`: a
   changed declaration that its migration makes true passes on a second
   deploy. A column of that table is then dropped by hand, and a release
   declaring the same tables ships an unrelated migration. The plan and
   the deploy are refused with today's message, the history gains no row,
   the unrelated table is not made, and the previous version is still
   served.
8. `a_migration_that_breaks_a_settled_declaration_is_refused_after_it`:
   the declaration is unchanged and passes before the migrations, and V2
   drops the declared `memo`, or retypes it to `integer`. The deploy is
   refused (422) naming `memo` and V2, V2 stays recorded, and the previous
   version is still served.
9. `the_first_plan_of_a_schema_not_made_yet_waits_for_the_migrations`: a
   managed service whose private table its migration makes, planned
   before its schema exists. The plan succeeds with `schema loan_service
   is missing` deferred, and the deploy makes the schema, applies the
   migration and passes the check after it.

The suite's other tests, which cover managed schemas, `verify_only` and
the lock, pass unchanged.

Unit tests:

- `adoption_check` for no report, `verify_only`, `apply` with and without
  pending migrations.
- `AdoptionCheck::AfterMigrations` refuses a finding on a settled
  declaration, returns the others without refusing, and `Now` refuses as
  before. With nothing settled, a schema that does not exist is deferred;
  with a settled table, it is refused.
- `adopt::settled` keeps an unchanged declaration (names folded, columns
  in any order) and drops a table the version left did not adopt, one it
  did not declare, and one whose columns, column types, owner field,
  `soft_delete` or primary key changed. A report that is not deferred serializes
  without `deferred`.
- The error after migrations names the applied migrations, and keeps
  today's message when none were applied.
- `plan_lines` prints a deferred adoption's findings and nothing for one
  that is not deferred.

The SDK's test host (`cufflink_fn::testing::TestDb`) applies migrations but
has no adoption check, so it has nothing to reorder, and this change leaves
it as it is.

## 6. Quality bar

- No todos and no partial paths: plan, apply, the CLI and the docs change
  together.
- The judgement stays in `validate_adoption`, the predicate in
  `adoption_check`, and what counts as settled in `adopt::settled`, beside
  the check whose inputs it compares.
- No comments beyond what the order needs, and no duplicated formatting of
  migration names.
- Docs: the phase order and the plan in
  [schema ownership](../schema-ownership.md#sql-migrations-in-deploys),
  and the release notes under "Changes in v0.39.0" in
  [versions](../versions.md).
