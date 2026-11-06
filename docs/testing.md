# Testing Services

Two ways to test a service before it is deployed:

- **The test host** (`cufflink-fn` feature `test-host`): a handler is a
  plain function called in `cargo test`, against a real Postgres and fakes
  of everything else the platform gives it.
- **The testkit** (`cufflink-testkit`): the platform itself, started inside
  the test's process, deployed to and called over HTTP.

Without `test-host` every host function outside WASM answers what it
always did (`db::query` → `[]`, `http::*` → `None`, …), which suits only
logic that does not depend on them ([WASM guide](wasm-guide.md#testing)).

## The test host

```toml
[lib]
crate-type = ["cdylib", "rlib"]

[dependencies]
cufflink-fn = "<version>"

[dev-dependencies]
cufflink-fn = { version = "<version>", features = ["test-host"] }
```

`test-host` is in `cufflink-fn` from R3 on (the release notes in
[versions](versions.md) name its version); v0.27 has no `test-host`.
Enable it under `[dev-dependencies]` only. Cargo then builds the guest for
`wasm32` without it, byte for byte the module it builds with no test host at
all; enabled under `[dependencies]` it would change the module's symbol
hashes, and with them the order of its constants, though never its imports
or code.

Write each handler as a function and export it with the path form of
`handler!`, so the test calls what is deployed:

```rust
pub fn create_loan(req: Request) -> Response { /* ... */ }

handler!(create_loan_handler, create_loan);
```

```rust
use cufflink_fn::testing::{AuthBuilder, MigrationHistory, TestDb, TestHost, UrlMatch};

#[test]
fn a_loan_is_created() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDb::start()?;
    db.migrate("loan_service", Path::new("migrations"), &MigrationHistory::default())?;
    let host = TestHost::builder().tenant("harbor").service("loan-service").db(&db).build()?;
    host.config.set("CATALOG_URL", "https://catalog.example.com");
    host.http
        .expect("GET", UrlMatch::prefix("https://catalog.example.com/books/"))
        .respond(200, &[("content-type", "application/json")], r#"{"status":"Available"}"#);

    let request = host
        .request()
        .auth(AuthBuilder::user("kc-1").claim("user_id", ADA).permissions(&["loans:create"]))
        .json(&json!({"book_id": BOOK, "memo": "Reserved at the desk"}))
        .build()?;
    let response = host.call(create_loan, request);

    assert_eq!(response.status(), 200);
    assert_eq!(db.query("SELECT memo FROM loan_service.loans")?.len(), 1);
    assert_eq!(host.nats.published_to("audit.logs").len(), 1);
    assert_eq!(host.verify(), Ok(()));
    Ok(())
}
```

`cufflink-fn/tests/library_example` is a whole service, an invented
library's loans, tested this way.

### What the host holds

| Field | What it is |
|-------|------------|
| `db` | `db::*` and `sql::*` (`db-v2`) on the `TestDb`, with the platform's rules: the schema on the search path, a transaction per legacy call, at most two interactive transactions, the same errors and row JSON |
| `http` | `MockHttp`: `expect(method, UrlMatch::exact/prefix/matching).respond(...)` or `.fail(...)`, `.times(n)`; an unmatched request is refused and recorded; `requests()` |
| `nats` | `NatsRecorder`: `published()`, `published_to(subject)`, `requests()`; `captures(stream, pattern)` declares streams, `reply(pattern, bytes)` scripts `nats::request` |
| `storage` | `InMemoryStorage`: `put`, `object` (with the ACL it was stored with), `keys`, `deny`; `admit_acl(bucket, acl)` admits a canned ACL as a tenant bucket would, and an upload naming any other is refused `denied`; `calls()` lists every call in order |
| `cache` | `InMemoryCache`, with the platform's key and size limits; `set_unavailable` |
| `config` | `ConfigMap`: `set`, `remove` |
| `clock` | `FakeClock`: `at`, `set`, `advance`; `util::sleep_ms` advances it (`slept_ms`) |
| `jobs` | `JobRecorder`: `enqueued()`; `refuse(handler)` |
| `images` | `ImageRecorder`: stores the source at each output key and records what was asked |
| `services` | `MockServiceCalls` (feature `service-call`) |
| `ext` | `MockExtensions` (feature `ext-call`): `expect(name).respond(value)`, `.fail(code, message)` (answered as the platform answers it: `Failed`, or the kind of a reserved code `unavailable`, `timeout`, `too_large`) or `.refuse(CallErrorKind::…)`, `.times(n)`; `calls()`. An unscripted call answers `not_found` and is recorded; with `TestHost::builder().extensions([...])` a call to any other extension is refused `not_declared` and never recorded, as the platform refuses one the manifest does not list |

With the features `test-host` and `sql-parse`, `sql::parse` is answered by `PgQueryParser`, which parses in the test's own process with the platform's `pg_query` release, bounds and response cap, so a test gets the platform's tree and refusals. It builds `pg_query`, which needs a C compiler and libclang, and while it parses `pg_query` holds the process's stderr.

None of the fakes panics: what a test did not script is an error the
handler receives, and `host.verify()` lists the expectations that were not
met. A transaction a handler leaves open is discarded when `host.call`
returns, as the platform discards it.

The host is installed on the thread that runs the handler. A thread the
handler spawns has none: its `db::*`, `http::*` and the rest answer as they
do outside the platform (`db::query` → `[]`), silently. The platform runs a
handler on one thread too, so a handler that needs a capability calls it
from the thread it runs on.

### Requests

`host.request()` builds what the platform sends: a route's request by
default (`.method`, `.path`, `.path_param`, `.query`, `.header`, `.json`,
`.raw_body`, `.auth(AuthBuilder)`), a subscription's
(`.subscription(SubscriptionBuilder::on(subject).stream(..).attempt(2))`),
a schedule's (`.schedule(name, fire_at)`), a hook's (`.hook()`) or a
job's (`.job(attempt)`, `.enqueued()`). `build()` refuses a request the
platform never sends: a path parameter without a path, and anything only a
route's request carries (a method, headers, a raw body, a query, a posture,
a path, parts, a calling service, a partner key, a scope) on a delivery or
an enqueued job. A contract test pins the JSON to the platform's own
builders.

A route's request declares its posture as the manifest writes it
(`.posture(json!({"kind": "permission", "area": "loans", "operation":
"delete"}))`), and `host.call` applies it before the handler runs, as the
platform does: no caller is 401, a caller without the permission 403
(`Missing permission: loans:delete`), an `internal` route another service
may not call 403, and so on; a service account passes a permission. The
answer carries its message under `error`, or under the key the builder's
`.error_key("error_message")` names, as the service's error profile
(`errors: { key: … }`) has it. A request without a posture is a legacy
route's, which admits anyone. The owner's rows (`owner(...)`) and the
scopes of a `scoped` route are still the handler's to apply: the platform
compares the token's `sub`, so a test of a handler that compares a
`user_id` claim sets both.

### The database

`TestDb::start()` creates a database on the server
`CUFFLINK_TEST_DATABASE_URL` names, else on an embedded Postgres the
process starts once and stops at exit (installed where `postgresql_embedded`
installs it, or under `CUFFLINK_TEST_POSTGRES_DIR`). The database is dropped
with the `TestDb`. `migrate(schema, dir, history)` applies SQL migrations as
a deploy does, under the history's table and lock, so a service that keeps
a refinery history is migrated the way it is in production; `execute`,
`query` and `execute_file` seed and inspect as the server's user.

A handler's statements run as the platform runs them at its strictest, so
what fails there fails in the test:

| | Default | Set with |
|---|---|---|
| Role | a login as the service's own role, as under `db_roles: enforce`: made by `TestDb::service_role(schema)` as the platform makes it (no superuser, owns its schema and what the server's user made in it, no `CREATE` in `public`, nothing of another schema). `migrate` runs the migrations as it. | `.role(PgRole::Login(ServiceRole::existing(name, password)))` logs in as a role the test set up; `.role(PgRole::Set(name))` sets a role on each transaction; `.role(PgRole::Connection)` runs as the server's user, as under `db_roles: off` |
| Pooler | `pgbouncer_transaction`: a statement that must run outside a transaction, and `SET`/`RESET`, are refused with the platform's messages | `.pooler(Pooler::Direct)` |
| `db::query_read_only` | the service's role; with `.read_only_role(name)`, that role, which the service's role is granted | |
| Statement timeout | 30 s, set again before each statement, savepoint and commit of a transaction as the platform does; a guest's own shorter value (`guest-statement-timeout`) wins, and one past the timeout plus 0.5 s is refused, as the platform refuses one past the route's budget | `.statement_timeout(d)` |

`TestDb::service_role` needs a server user that may create roles (a
superuser, or `CREATEROLE`); the roles are dropped with the database.

A service bound to a role of its own (`cufflink services db-role set
--role R --password-stdin`) logs in as that role under `db_roles:
enforce`, and the role's own settings (`ALTER ROLE … SET work_mem`,
`temp_file_limit`, …) apply. A test logs in the same way:

```rust,ignore
db.execute_file(Path::new("tests/fixtures/roles.sql"))?; // creates loan_reader, its grants
let host = TestHost::builder()
    .service("loan-service")
    .db(&db)
    .role(PgRole::Login(ServiceRole::existing("loan_reader", "its-password")))
    .build()?;
```

Nothing is done to an existing role: it is not created, it is not given
the schema or what is in it, it is not granted `.read_only_role(name)`, and
it is not dropped with the database. The test, or the migrations it ran,
makes every grant, as the operator does in production. Build the schema
with `execute` or `execute_file`, not `migrate`: `migrate` runs as a role
it makes, `svc_<uuid>`, and gives that role the schema, so the existing role
would no longer hold what the operator granted it. The session runs the
same prelude as any login (`search_path` and `statement_timeout`), so every
other setting is the role's. A role that does not exist, or a wrong
password, fails `build()` with `TestHostError::Role`, which names the role
and never the password; `ServiceRole`'s `Debug` prints no password either.
Roles are the server's, so give a role a name of its own in each test that
creates one and drop it after the databases whose objects it owns.

A test of a handler may be a plain `#[test]` or a `#[tokio::test]` of
either flavour. The client is synchronous and drives a runtime of its own,
so every statement, connection and drop runs on a thread of its own and the
test's runtime never sees it; a handler still runs to completion inside
`call`, as it does on the platform.

#### Template databases

A schema that takes seconds to build, such as another tool's migrations
or a large dump, is built once and cloned for each test with
`CREATE DATABASE … TEMPLATE`:

```rust
use cufflink_fn::testing::{TestDb, TestTemplate};
use std::path::Path;

fn orders() -> TestTemplate {
    TestTemplate::get_or_create("orders_v42", |db| {
        db.execute("CREATE SCHEMA order_service")?;
        db.execute_file(Path::new("schema.sql"))
    })
    .expect("template")
}

#[test]
fn an_order_is_placed() {
    let db = TestDb::from_template(&orders()).expect("database");
    // …
}
```

- `get_or_create(name, build)` builds the template on the server
  `TestDb::start` uses, the first time it is asked for, in a database of
  its own and as the server's user. When `build` returns, the database is
  closed to connections (`ALLOW_CONNECTIONS false`), made a template
  (`IS_TEMPLATE true`) and renamed `tmpl_<name>`. Later calls return it,
  and concurrent first calls wait for one build, in the process and, under
  an advisory lock, across processes on one server. The lock is taken in
  the database the server's URL names, so processes share it when they
  reach the server through the same database; a call waits at most ten
  minutes for another's build before it fails with
  `TestHostError::Template`.
- A name is 1 to 58 lowercase letters, digits and underscores. A server
  that outlives the process keeps the template and a later run uses it
  without building it, so name it after what `build` makes (a hash of the
  migration files, say). A database of that name that is not a template is
  refused with `TestHostError::Template`.
- `build` gets a `TemplateDb`, with `execute`, `execute_file`, `query` and
  `config`. Every connection it opens must be closed before it returns: a
  build that leaves one open is refused with `TestHostError::Template`,
  rather than waited on. A build that fails or is refused is dropped, and
  the next call builds again.
- `TestDb::from_template(&template)` clones it as `test_<uuid>`; the clone
  is otherwise a `TestDb` like any other. Its objects keep the template's
  owners and grants, whose roles are the server's, and `service_role`
  gives the schema to a fresh role as on a database `start` creates.
  `template.strategy(CloneStrategy::FileCopy)` clones by copying files
  (`STRATEGY FILE_COPY`, Postgres 15 and later) rather than through the
  write-ahead log.
- The embedded server takes its templates with it when it stops. On a
  server `CUFFLINK_TEST_DATABASE_URL` names they stay, for the next run;
  drop one with `ALTER DATABASE tmpl_<name> IS_TEMPLATE false` and
  `DROP DATABASE tmpl_<name>`.
  A process killed while it builds leaves its build database,
  `tmpl__<uuid>`, which nothing uses again; list them with
  `SELECT datname FROM pg_database WHERE datname LIKE 'tmpl\_\_%'` and
  drop each the same way when no test run is building.

## The testkit

`cufflink-testkit` starts the platform inside the test's process, on a
runtime of its own, with a database of its own and an OIDC issuer it trusts
as it trusts Keycloak:

```rust
use cufflink_testkit::{Platform, PlatformOptions, TenantOptions};

let platform = Platform::start(PlatformOptions::default()).await?;
let tenant = platform.tenant(&Platform::unique_slug("acme"), TenantOptions::default()).await?;
platform.deploy(&tenant, &manifest, Some(&wasm)).await?.success("deploy")?;
let token = platform.token(&tenant).subject("kc-1").permissions(&["loans:create"]).sign().await?;
let user = platform.client(&tenant)?.with_token(&token);
let created = user.post(&user.svc("loan-service", "/loans"), &body).await?;
assert_eq!(created.status, 200);
platform.shutdown().await?;
```

Tokens are RS256 JWTs verified through the real JWKS path, so postures and
permissions are enforced as in production. `TenantOptions` sets a tenant's
database name, policies and NATS claims; `platform.nats()` connects to its
NATS; `tenant_database_url(&tenant)` is where to seed. Every call answers a
`Result`.

It needs Postgres and, unless `PlatformOptions::without_nats()`, NATS:

| Variable | Used for |
|----------|----------|
| `CUFFLINK_TEST_DATABASE_URL` | the Postgres server; else an embedded one |
| `CUFFLINK_TEST_NATS_URL` | the NATS server; else a `nats-server` binary on `PATH` |
| `CUFFLINK_TEST_SPARE_NATS_URL` | a second NATS, for tests that leave theirs unusable |
| `CUFFLINK_TEST_REDIS_URL` | Redis, for `PlatformOptions::with_redis()` |
| `CUFFLINK_TEST_PORT_LEASES` | a directory in which test processes of several checkouts on one machine lease the ports they hand out (default `target/tmp/ports`) |
| `CUFFLINK_CI` | `1` turns every skip for missing infrastructure into a failure |

The same harness runs the repository's own integration tests
([setup](setup.md#local-development-setup)).

To try a service against a whole platform on your machine instead, use
[`cufflink dev`](dev.md).
