# SQL with Bound Parameters (`db_v2`)

`cufflink_fn::sql` runs SQL with bound parameters, returns structured errors
and exact row types, and offers interactive transactions. The legacy
`cufflink_fn::db` functions are unchanged (an error is still `[]`, `None` or
`-1`); the two can be used side by side while a service migrates.

Enable it in the guest's `Cargo.toml`:

```toml
cufflink-fn = { version = "…", features = ["db-v2"] }
```

The feature records `db_v2` in the module, so a platform without it refuses
the deploy (see [versions](versions.md)). With the feature on, the deprecated
`db::sql_escape` warns at every remaining call site. Cargo unifies features
across a workspace, so one member enabling `db-v2` turns the warning on in
every member built with it, and `cargo clippy --workspace -- -D warnings`
fails until each has migrated: enable the feature in every member at once,
or put `#![allow(deprecated)]` at the top of each member's `lib.rs` while it
migrates and remove it when its last `sql_escape` is gone.

## Statements

```rust
use cufflink_fn::prelude::*;

let rows = sql::query("SELECT id, total FROM orders WHERE customer = $1", &params![customer])?;
let total: Option<String> = rows[0].get("total")?;            // NUMERIC is exact text
let order = sql::query_one("SELECT * FROM orders WHERE id = $1", &params![id])?;   // Ok(None) when absent
let orders: Vec<Order> = sql::query_as("SELECT * FROM orders", &[])?;              // serde-decoded
let touched = sql::execute("UPDATE orders SET status = $1 WHERE id = $2", &params!["paid", id])?;
```

- `params![…]` binds Rust values (`&str`, `String`, integers, floats, `bool`,
  `serde_json::Value`, bytes, `Vec`s of strings, `i32` or `i64`, `Option`s as
  NULL, chrono dates and times). A `uuid::Uuid` binds only in native code
  (the test host): the `uuid` crate is not a dependency of a wasm32 build, so
  a guest binds `Param::Uuid(id.to_string())`, or passes the text and casts
  it (`$1::text::uuid`). Arrays of other types are bound as their `Param`
  variant (`Param::UuidArray`, `BoolArray`, `F64Array`, `NumericArray`);
  there is none for `bytea[]` or `jsonb[]`, which are built in SQL
  (`ARRAY[$1::jsonb]`). The platform prepares the statement and converts each value
  to the parameter's Postgres type **losslessly**, or refuses the call with
  kind `InvalidParameter` naming the parameter; enums take their label as
  text, domains their base type. An unsupported type is `UnsupportedType`
  with a suggested cast (`$1::text::<type>`).
- More than one row from `query_one` is `TooManyRows`.
- `Row::get`/`try_get`/`decode` deserialize columns; `columns()` lists names and
  SQL types.

Rows are typed exactly: integers as numbers, `numeric` as exact strings,
`timestamptz` as RFC 3339 UTC, `timestamp` without an offset, `date`/`time`,
`interval` as ISO 8601 durations, `bytea` as base64, JSON embedded, 1-D arrays
as arrays; a type without a rendering is an `UnsupportedType` error naming
the column, never a silent `null`.

## Errors

Every call returns `Result<_, DbError>`. `DbError` carries the SQLSTATE
`code`, a `kind`, `message`, and Postgres's `detail`, `hint`, `constraint`,
`table`, `column`, `schema` and `position`. Kinds include
`UniqueViolation`, `ForeignKeyViolation`, `CheckViolation`,
`NotNullViolation`, `SerializationFailure`, `DeadlockDetected`,
`QueryCanceled` (the statement timeout), `RaiseException`, `PoolTimeout`,
`TransactionLimit` and `Unavailable` (outside the platform, where every call
fails rather than returning empty data). `is_retryable()` is true for
serialization failures and deadlocks; `is_constraint_violation()` for the
integrity kinds.

## Transactions

```rust
sql::transaction(|tx| {
    let row = tx.query_one("SELECT stock FROM items WHERE id = $1 FOR UPDATE", &params![item])?;
    let stock: i64 = row.map(|r| r.get("stock")).transpose()?.unwrap_or(0);
    if stock > 0 {
        tx.execute("UPDATE items SET stock = stock - 1 WHERE id = $1", &params![item])?;
    }
    Ok(stock > 0)
})?;
```

- `Ok` commits (a failed commit is the error); `Err` rolls back and returns the
  closure's error.
- `transaction_with(TxOptions { isolation, read_only }, …)` picks the isolation
  level; `retry_transaction(max_attempts, options, f)` reruns on retryable
  errors.
- Inside, `tx.set_local(name, value)`, `tx.advisory_xact_lock(key)`,
  `tx.try_advisory_xact_lock(key)` and `tx.savepoint(|tx| …)` (released on
  `Ok`, rolled back to on `Err`).
- A transaction holds one tenant-pool connection until it ends; at most
  `WASM_MAX_OPEN_TX` (default 2) are open per invocation, and statements in it
  are bounded by the invocation's remaining budget. One the guest abandons
  (a trap, the deadline) is discarded and rolled back by the server.
- Session advisory locks are deliberately not offered.

Every statement runs inside a transaction, so `db_v2` works unchanged behind
PgBouncer in transaction mode ([deployment](deployment.md#tenant-databases-and-pgbouncer)).
