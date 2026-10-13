# Writing Custom Business Logic with cufflink-fn

This guide shows how to build Cufflink services that go beyond auto-generated CRUD by writing custom handlers in Rust that compile to WebAssembly. Your code runs inside the platform's sandboxed WASM runtime with access to the database, object storage, Redis, outbound HTTP and NATS messaging through a small set of host functions.

It covers everything a handler author needs: project setup, how requests reach a handler, route postures, the full `cufflink-fn` API, lifecycle hooks, subscriptions and long-running jobs.

Versions this guide describes: `cufflink-fn` 0.29.x, `cufflink` (SDK) 0.13.x, `cufflink-cli` 0.20.x.

---

## Table of Contents

1. [When to Use WASM Mode](#when-to-use-wasm-mode)
2. [Getting Started](#getting-started)
3. [How Requests Reach Your Handler](#how-requests-reach-your-handler)
4. [Route Postures](#route-postures)
5. [Architecture: Handlers, Operations, Repos](#architecture-handlers-operations-repos)
6. [Data Access Guidance](#data-access-guidance)
7. [Example 1: E-Commerce Order Service](#example-1-e-commerce-order-service)
8. [Example 2: Approval Workflow](#example-2-approval-workflow)
9. [Example 3: Analytics & Aggregation](#example-3-analytics--aggregation)
10. [Example 4: Notification Service](#example-4-notification-service)
11. [CRUD Lifecycle Hooks](#crud-lifecycle-hooks)
12. [NATS Subscriptions](#nats-subscriptions)
13. [Long-Running Jobs](#long-running-jobs)
14. [Building & Deploying](#building--deploying)
15. [Testing](#testing)
16. [API Reference](#api-reference)

---

## When to Use WASM Mode

Most services only need auto-generated CRUD — define your tables, deploy, and get REST + GraphQL endpoints automatically. Use WASM mode when you need logic the database can't express:

| Scenario | Use |
|----------|-----|
| Store and retrieve data | CRUD mode |
| Filter, sort, paginate, search | CRUD mode |
| Multi-step workflows (checkout, approval) | **WASM mode** |
| Computed values across tables | **WASM mode** |
| Validate business rules beyond column constraints | **WASM mode** |
| Transform data before storage | **WASM mode** |
| Trigger side effects (send emails, publish events) | **WASM mode** |
| Aggregate data for dashboards | **WASM mode** |

A service can have both — CRUD tables for standard data access plus custom WASM handlers for business logic. The auto-generated CRUD endpoints still work alongside your custom routes.

---

## Getting Started

### Scaffold a project

```bash
cufflink init my-service --mode wasm
cd my-service

# One-time: the CLI builds handlers for this target
rustup target add wasm32-unknown-unknown
```

`cufflink init` generates four files. The **manifest** lives in `src/main.rs` (a normal binary that the CLI runs with `--emit-manifest`), and the **handlers** live in `src/lib.rs` (compiled to a `cdylib` for `wasm32-unknown-unknown`).

`Cargo.toml`:

```toml
[package]
name = "my-service"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
cufflink = { version = "0.18" }
cufflink-fn = { version = "0.34" }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
# `v4` is intentionally omitted: the platform auto-generates row IDs, and
# pulling in `getrandom`/`v4` makes the wasm32 build fail without a JS shim.
uuid = { version = "1", features = ["serde"] }
chrono = { version = "0.4", features = ["serde"] }
```

Add `"rlib"` to `crate-type` (`["cdylib", "rlib"]`) if you want to unit-test the library natively with `cargo test --lib`; the CRUD-with-hooks template does this.

`src/main.rs` — the manifest:

```rust
use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "items")]
pub struct Item {
    #[key]
    pub id: Uuid,
    pub name: String,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

cufflink::service! {
    name: "my-service",
    mode: wasm,
    tables: [Item],
    custom_routes: [
        // Every route declares a posture: public("why"), auth, perm("area", "op")
        // or owner("field"). Without one the platform runs it unenforced.
        ("GET", "/hello", "hello", public("demo endpoint")),
    ],
}
```

`src/lib.rs` — the handlers:

```rust
use cufflink_fn::handler;
use cufflink_fn::prelude::*;

cufflink_fn::init!();

// Reachable at GET/POST /svc/{tenant}/{service}/_fn/hello.
// A JSON POST body arrives as req.body(); GET query parameters arrive under
// req.body()["params"].
handler!(hello, |req: Request| {
    let body = req.body();
    let name = body["name"]
        .as_str()
        .or_else(|| body["params"]["name"].as_str())
        .unwrap_or("world");
    Response::json(&json!({"message": format!("Hello, {}!", name)}))
});
```

`Cufflink.toml` — where to deploy. The generated file targets `http://localhost:8080` and tenant `default`; point it at your tenant (the bundled `docker compose` stack creates `acme`) and add the Keycloak settings `cufflink login` needs:

```toml
[service]
default_env = "local"

[environments.local]
api_url = "http://localhost:8080"
tenant = "acme"
keycloak_url = "http://localhost:8180"
keycloak_realm = "acme"
```

### Deploy

```bash
cufflink login      # OAuth device flow; or set CUFFLINK_API_KEY
cufflink deploy
```

`cufflink deploy` does everything in one request: it runs the manifest binary (`cargo run -- --emit-manifest`), builds the handlers with `cargo build --lib --target wasm32-unknown-unknown --release`, base64-encodes the `.wasm` and posts it together with the manifest to `POST /api/services/deploy` as `wasm_base64`. There is no separate "upload the module" step — the deploy API rejects a WASM manifest that arrives without its module.

```
Environment: local
Building service...
Deploying my-service (mode: wasm)...
  WASM required (mode: wasm); building...
Building WASM module...
  WASM artifact: target/wasm32-unknown-unknown/release/my_service.wasm (412345 bytes)
  Bundled WASM: 412345 bytes, hash: 3f9c2a1b7d4e5f60
Deployed my-service v2
  Schema changes: 1
  Deployment ID:  "98e338c3-..."
  Tenant:         "acme"
  Commit:         f1af9c6...
  WASM hash:      3f9c2a1b7d4e5f60
  Deploy verified: db_state=true, local_cache=true
```

Then call it:

```bash
curl "http://localhost:8080/svc/acme/my-service/_fn/hello?name=Ada"
# {"message":"Hello, Ada!"}

curl -X POST http://localhost:8080/svc/acme/my-service/_fn/hello \
  -H 'Content-Type: application/json' -d '{"name": "Ada"}'
# {"message":"Hello, Ada!"}
```

The first deploy of a service reports `v2` (v1 is the registration), so don't expect `v1` in the output.

---

## How Requests Reach Your Handler

### Handlers are addressed by name

Every custom route is served at:

```
/svc/{tenant}/{service}/_fn/{handler}
```

where `{handler}` is the **handler name** — the third element of the `custom_routes` tuple, which must equal the identifier you gave `handler!`. The route accepts `GET`, `POST`, `PUT`, `DELETE` and `PATCH`; the platform looks the route up by handler name and does **not** check that the method matches the one declared in the manifest. The `path` element (`"/hello"`) is informational — it appears in `cufflink routes` and the OpenAPI output but is not what the URL is matched on — unless the service declares `routing: strict`, which also serves each route at its own path, per method, with path parameters ([routing](routing.md#strict-routing)).

Only handlers declared in `custom_routes` are reachable over HTTP. Hook handlers, subscription handlers and the `on_migrate` handler are not exported through `_fn` even if they are compiled into the same module — a `_fn` call to one of them returns 404.

### What the handler receives

The platform serializes one JSON object and passes it to your handler; `Request` is the typed view of it.

| Field | Where it comes from |
|-------|---------------------|
| `req.method()` | The HTTP verb. Lifecycle hooks arrive with method `"HOOK"`, subscriptions with `"SUBSCRIPTION"` — neither can be forged through `_fn`, which only ever passes real verbs. |
| `req.handler()` | The handler name from the URL. |
| `req.body()` | The request body parsed as JSON (`Value::Null` if empty). A body that isn't valid JSON arrives as a JSON string. |
| `req.body()["params"]` | Query-string parameters, as a map of strings. Merged into the body object; for a bodiless GET the body becomes `{"params": {...}}`. If the body was a non-object JSON value it becomes `{"data": <value>, "params": {...}}`. |
| `req.raw_body()` | The exact bytes the caller sent — use for HMAC-signed webhooks. |
| `req.headers()` / `req.header("x-foo")` | All request headers. Names are **lowercased**. |
| `req.tenant()` / `req.service()` | Tenant slug and service name from the URL. |
| `req.auth()` | The validated principal, or `None` (see [Route Postures](#route-postures)). |
| `req.posture()` / `req.owner_field()` | The route's declared posture, if any. |
| `req.job()` | Set only when the handler runs as a long-running job. |

### Limits

- **Body limit**: 4 MB on `_fn` routes (larger bodies get 413), read only once the caller is authorized; a route can set its own `body_limit` and `timeout` ([route limits](routing.md#route-limits)).
- **Execution budget**: a route handler has **30 seconds** end to end, including every host call it makes. Hooks also get 30 s; subscription handlers get 60 s. Timing out returns a 500 `WASM execution timed out after 30s`. Anything longer belongs in a [job](#long-running-jobs).
- Outbound HTTP defaults to a 28 s request budget and a 20 s connect budget, and is always clamped to the handler's remaining time (never below 500 ms).

### Responses

`Response::json(&value)` produces a 200 with `application/json`. Every other constructor (`error`, `not_found`, `unauthorized`, `forbidden`, `created`, `status_json`, `error_message`, `validation_errors`, `with_status`) sets a real HTTP status line, which is what clients, WAFs and edge rate-limiters key on. With the `response-v2` feature a handler can also answer 204, files, plain text, redirects, stored objects and event streams, with its own headers ([responses](routing.md#responses-cufflink-fn-feature-response-v2)). Each `_fn` invocation is also written to the tenant audit log (`dw.{tenant}.logs`); for unauthenticated calls the body is redacted from the audit entry.

---

## Route Postures

A **posture** is the authorization requirement a route declares in the manifest. The platform enforces it *before* your code runs, so a handler cannot forget to check. Declare exactly one per route, after the handler name:

```rust
custom_routes: [
    ("POST", "/webhooks/stripe", "stripe_webhook", public("Stripe signs the payload; verified in-handler")),
    ("GET",  "/me",              "whoami",         auth),
    ("POST", "/refund",          "handle_refund",  perm("orders", "refund")),
    ("GET",  "/my-orders",       "my_orders",      owner("customer_id")),
],
```

| Modifier | Requirement enforced by the platform | `req.auth()` |
|----------|--------------------------------------|--------------|
| `public("reason")` | None. The reason is mandatory and non-empty; it shows up in `cufflink routes`. An invalid `Authorization` header is tolerated and treated as anonymous (so a third-party webhook carrying its own bearer token still works). | `Some` if valid credentials were sent, else `None` |
| `auth` | A valid JWT or API key. Missing → 401; invalid → 401. | Always `Some` |
| `perm("area", "op")` | A valid principal holding the Cufflink permission `area:op` (or `area:*` / `*`). Missing permission → 403. Keycloak service accounts bypass the permission check. | Always `Some` |
| `owner("field")` | A valid principal — and **nothing more**. The platform does not scope rows for you; it passes `field` through as `req.owner_field()` and your query must constrain by it (see the `Request::owner_field` docs for the direct and indirect patterns). | Always `Some` |

### Legacy (undeclared) routes

A route with no posture runs in **legacy, unenforced mode**: the platform authenticates on a best-effort basis, hands whatever it found to `req.auth()`, and lets the request through. Each such request increments the `cufflink_route_posture_undeclared_total{service}` metric and logs at debug level:

```
Custom route has no declared posture — running in legacy (unenforced) mode.
```

Two things push you toward declaring postures everywhere:

- **All-or-nothing per manifest.** Once any route in a manifest declares a posture, the deploy validator rejects a manifest that still has an undeclared route. The current SDK always emits a posture when you write one, so mixing is only possible by hand-editing the manifest.
- **`CUFFLINK_ENFORCE_ROUTE_POSTURE=true`** (or `1`) on the platform rejects *every* deploy with an undeclared route, regardless of the manifest's age.

A manifest with postures also declares the `route_posture` platform feature as required, so deploying it to a platform that predates postures fails instead of silently running the routes as public.

### Auditing

```bash
cufflink routes           # list the routes of the service in this directory
cufflink routes --audit   # exit non-zero if any route is public or undeclared
```

### Job routes

`job(...)` is a fourth modifier that turns the route into an asynchronous job — see [Long-Running Jobs](#long-running-jobs).

### Forwarding the caller's credentials

Once a tenant's `tenant_binding` policy is enforced, the caller's
`authorization`, `cookie` and `proxy-authorization` headers are removed from
`req.headers()` unless the route declares `forward_auth` — needed only by a
handler that calls another service *as the caller*:

```rust
("POST", "/proxy", "handle_proxy", auth, forward_auth),
```

---

## Architecture: Handlers, Operations, Repos

Every example in this guide follows the **thin handlers / fat operations / repos** pattern:

```
HTTP Request
    │
    ▼
┌──────────────────────────────────────┐
│  HANDLER (thin)                      │
│  - Parse request                     │
│  - Call operation                    │
│  - Return response                   │
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│  OPERATION (fat)                     │
│  - Validate inputs                   │
│  - Check business rules              │
│  - Orchestrate repo calls            │
│  - Publish events                    │
│  - Return result or error            │
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│  REPO (data access)                  │
│  - Execute SQL via db::query/execute │
│  - Map rows to domain types          │
│  - No business logic                 │
└──────────────────────────────────────┘
```

**Why this matters:**

- **Handlers** are trivial — if a handler is more than 5 lines, logic belongs in an operation
- **Operations** contain all the interesting code — validation, rules, orchestration. They're reusable across handlers
- **Repos** are pure data access — easy to test, easy to swap out, no coupling to HTTP

Project layout:

```
my-service/
├── Cargo.toml
├── Cufflink.toml
└── src/
    ├── main.rs             # service! manifest (tables, routes, postures)
    ├── lib.rs              # init!() + handler! definitions (thin)
    ├── operations/         # Business logic (fat)
    │   ├── mod.rs
    │   ├── checkout.rs
    │   └── refund.rs
    ├── repo/               # Data access (SQL)
    │   ├── mod.rs
    │   ├── orders.rs
    │   └── products.rs
    └── models.rs           # Shared types
```

---

## Data Access Guidance

The `db` module runs plain SQL strings, without bound parameters (for those, see [Moving to bound parameters](#moving-to-bound-parameters-db-v2)). Three rules keep that safe and correct.

### 1. Never format user input into SQL without escaping

Every string that came from a request must go through `db::sql_escape`, which doubles embedded single quotes so it can sit inside a `'...'` literal:

```rust
// WRONG — a name containing a quote breaks the statement, or worse.
db::query(&format!("SELECT * FROM users WHERE name = '{}'", name));

// RIGHT
db::query(&format!(
    "SELECT * FROM users WHERE name = '{}'",
    db::sql_escape(name)
));
```

Numbers should be parsed to a numeric type first and formatted from that (`quantity as i64`), never interpolated as strings. Identifiers (table and column names) must come from your code, never from the request.

### 2. Generate ids yourself instead of "select the newest row"

`db::execute` returns an affected-row count, not the inserted row, and each `db` call takes its own pooled connection. Reading back with `ORDER BY created_at DESC LIMIT 1` is a race under concurrent requests. Mint the id in the handler and insert it explicitly:

```rust
let order_id = util::generate_uuid();
db::execute(&format!(
    "INSERT INTO orders (id, customer_email, status) VALUES ('{}', '{}', 'pending')",
    order_id, db::sql_escape(customer_email)
));
// order_id is now known without a second query.
```

### 3. Know which schema you're in

`db::query`, `db::query_one`, `db::execute` and `db::execute_all` run with `search_path` set to your **service's schema**, so table names need no prefix. `db::query_read_only` deliberately does *not* set a search path (see the reference) — name schemas explicitly there.

### Transactions

`query`, `query_one` and `execute` each take a fresh connection and run as their own implicit transaction; nothing carries between them — no held lock, no atomicity. `db::execute_all(&[...])` is the one primitive that gives you a transaction: one connection, one `BEGIN`/`COMMIT`, statements in order, everything rolled back on the first failure.

```rust
db::execute_all(&[
    &format!("SELECT pg_advisory_xact_lock(hashtext('{}')::bigint)", db::sql_escape(item_id)),
    &insert_sql,
])?;
```

`nats::publish` is a separate host call and cannot join the transaction — this is not a transactional outbox. With `db-v2`, `sql::transaction(|tx| ...)` holds an interactive transaction in which statements can read what earlier ones wrote.

### Moving to bound parameters (`db-v2`)

The `db-v2` feature of `cufflink-fn` adds `cufflink_fn::sql`, which binds parameters instead of formatting them into SQL (`sql::query("SELECT * FROM users WHERE name = $1", &params![name])`), and deprecates `db::sql_escape` so every remaining interpolation shows up at compile time.

Cargo unifies features across a workspace: as soon as one crate enables `db-v2` on the shared `cufflink-fn` dependency, every crate in the workspace builds with it, and `cargo clippy --workspace -- -D warnings` fails on each `db::sql_escape` call in all of them. While migrating, allow the deprecation at the top of each affected crate's `lib.rs` or `main.rs`, and remove it once the crate no longer calls `sql_escape`:

```rust
#![allow(deprecated)]
```

A workspace in which no crate enables `db-v2` is unaffected.

---

## Example 1: E-Commerce Order Service

A complete order service with CRUD tables for products, orders, and order items — plus custom handlers for checkout and refunds.

### `src/main.rs` — Manifest

```rust
use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "products")]
pub struct Product {
    #[key]
    pub id: Uuid,
    pub name: String,
    pub price_cents: i64,
    pub stock: i32,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "orders")]
pub struct Order {
    #[key]
    pub id: Uuid,
    pub customer_email: String,
    pub status: String,
    pub total_cents: i64,
    pub item_count: i32,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "order_items")]
pub struct OrderItem {
    #[key]
    pub id: Uuid,
    #[references("orders.id")]
    #[on_delete("cascade")]
    pub order_id: Uuid,
    #[references("products.id")]
    pub product_id: Uuid,
    pub product_name: String,
    pub quantity: i32,
    pub unit_price_cents: i64,
    pub line_total_cents: i64,
}

cufflink::service! {
    name: "shop",
    mode: wasm,
    tables: [Product, Order, OrderItem],
    custom_routes: [
        ("POST", "/checkout",      "handle_checkout",      auth),
        ("GET",  "/order-summary", "handle_order_summary", auth),
        ("POST", "/refund",        "handle_refund",        perm("orders", "refund")),
    ],
}
```

### `src/models.rs` — Shared types

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartItem {
    pub product_id: String,
    pub quantity: i32,
}

#[derive(Debug, Clone)]
pub struct ProductInfo {
    pub id: String,
    pub name: String,
    pub price_cents: i64,
    pub stock: i32,
}

impl ProductInfo {
    pub fn from_row(row: &Value) -> Option<Self> {
        Some(Self {
            id: row["id"].as_str()?.to_string(),
            name: row["name"].as_str()?.to_string(),
            price_cents: row["price_cents"].as_i64()?,
            stock: row["stock"].as_i64()? as i32,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Order {
    pub id: String,
    pub customer_email: String,
    pub status: String,
    pub total_cents: i64,
    pub item_count: i32,
}

impl Order {
    pub fn from_row(row: &Value) -> Option<Self> {
        Some(Self {
            id: row["id"].as_str()?.to_string(),
            customer_email: row["customer_email"].as_str()?.to_string(),
            status: row["status"].as_str()?.to_string(),
            total_cents: row["total_cents"].as_i64()?,
            item_count: row["item_count"].as_i64()? as i32,
        })
    }
}
```

### `src/repo/products.rs` — Product data access

```rust
use cufflink_fn::prelude::*;
use crate::models::ProductInfo;

/// Get a product by ID. Returns None if not found.
pub fn get_by_id(product_id: &str) -> Option<ProductInfo> {
    let row = db::query_one(&format!(
        "SELECT id, name, price_cents, stock FROM products WHERE id = '{}'",
        db::sql_escape(product_id)
    ))?;
    ProductInfo::from_row(&row)
}

/// Decrement stock for a product. Returns false if insufficient stock
/// (or if the statement failed — db::execute returns -1 on error).
pub fn decrement_stock(product_id: &str, quantity: i32) -> bool {
    let affected = db::execute(&format!(
        "UPDATE products SET stock = stock - {} WHERE id = '{}' AND stock >= {}",
        quantity, db::sql_escape(product_id), quantity
    ));
    affected > 0
}

/// Restore stock for a product (used in refunds).
pub fn restore_stock(product_id: &str, quantity: i32) {
    db::execute(&format!(
        "UPDATE products SET stock = stock + {} WHERE id = '{}'",
        quantity, db::sql_escape(product_id)
    ));
}
```

### `src/repo/orders.rs` — Order data access

```rust
use cufflink_fn::prelude::*;
use crate::models::Order;

/// Create an order and return its ID. The id is minted here, so no
/// read-back query is needed (and no race with concurrent checkouts).
pub fn create(customer_email: &str, total_cents: i64, item_count: i32) -> Option<String> {
    let id = util::generate_uuid();
    let affected = db::execute(&format!(
        "INSERT INTO orders (id, customer_email, status, total_cents, item_count) \
         VALUES ('{}', '{}', 'pending', {}, {})",
        id, db::sql_escape(customer_email), total_cents, item_count
    ));
    (affected > 0).then_some(id)
}

/// Add a line item to an order.
pub fn add_item(
    order_id: &str,
    product_id: &str,
    product_name: &str,
    quantity: i32,
    unit_price_cents: i64,
) {
    let line_total = quantity as i64 * unit_price_cents;
    db::execute(&format!(
        "INSERT INTO order_items \
         (id, order_id, product_id, product_name, quantity, unit_price_cents, line_total_cents) \
         VALUES ('{}', '{}', '{}', '{}', {}, {}, {})",
        util::generate_uuid(),
        db::sql_escape(order_id),
        db::sql_escape(product_id),
        db::sql_escape(product_name),
        quantity,
        unit_price_cents,
        line_total
    ));
}

/// Get an order by ID.
pub fn get_by_id(order_id: &str) -> Option<Order> {
    let row = db::query_one(&format!(
        "SELECT id, customer_email, status, total_cents, item_count FROM orders WHERE id = '{}'",
        db::sql_escape(order_id)
    ))?;
    Order::from_row(&row)
}

/// Get all line items for an order.
pub fn get_items(order_id: &str) -> Vec<Value> {
    db::query(&format!(
        "SELECT product_id, product_name, quantity, unit_price_cents, line_total_cents \
         FROM order_items WHERE order_id = '{}'",
        db::sql_escape(order_id)
    ))
}

/// Update order status.
pub fn update_status(order_id: &str, status: &str) -> bool {
    let affected = db::execute(&format!(
        "UPDATE orders SET status = '{}' WHERE id = '{}'",
        db::sql_escape(status), db::sql_escape(order_id)
    ));
    affected > 0
}
```

### `src/repo/mod.rs`

```rust
pub mod orders;
pub mod products;
```

### `src/operations/checkout.rs` — Checkout business logic

```rust
use cufflink_fn::prelude::*;
use crate::models::{CartItem, ProductInfo};
use crate::repo;

/// Validated line item ready for insertion.
struct ValidatedItem {
    product: ProductInfo,
    quantity: i32,
    line_total: i64,
}

/// Process a checkout: validate items, check stock, create order, decrement stock.
pub fn process(tenant: &str, customer_email: &str, items: &[CartItem]) -> Result<Value, String> {
    // 1. Validate inputs
    if customer_email.is_empty() {
        return Err("Customer email is required".into());
    }
    if items.is_empty() {
        return Err("Cart is empty".into());
    }
    if items.iter().any(|i| i.quantity <= 0) {
        return Err("All quantities must be positive".into());
    }

    // 2. Look up each product and validate stock
    let mut validated: Vec<ValidatedItem> = Vec::new();
    for item in items {
        let product = repo::products::get_by_id(&item.product_id)
            .ok_or_else(|| format!("Product '{}' not found", item.product_id))?;

        if product.stock < item.quantity {
            return Err(format!(
                "Insufficient stock for '{}': requested {}, available {}",
                product.name, item.quantity, product.stock
            ));
        }

        let line_total = item.quantity as i64 * product.price_cents;
        validated.push(ValidatedItem {
            product,
            quantity: item.quantity,
            line_total,
        });
    }

    // 3. Calculate total
    let total_cents: i64 = validated.iter().map(|v| v.line_total).sum();
    let item_count: i32 = validated.iter().map(|v| v.quantity).sum();

    // 4. Create the order
    let order_id = repo::orders::create(customer_email, total_cents, item_count)
        .ok_or("Failed to create order")?;

    // 5. Add line items
    for v in &validated {
        repo::orders::add_item(
            &order_id,
            &v.product.id,
            &v.product.name,
            v.quantity,
            v.product.price_cents,
        );
    }

    // 6. Decrement stock
    for v in &validated {
        if !repo::products::decrement_stock(&v.product.id, v.quantity) {
            // Stock changed between check and decrement. For all-or-nothing
            // semantics, put the inserts and decrements in one db::execute_all.
            log::warn(&format!("Stock race condition for product {}", v.product.id));
        }
    }

    // 7. Publish event — always inside the service's own tenant namespace
    nats::publish(
        &format!("dw.{}.shop.orders.created", tenant),
        &json!({
            "order_id": order_id,
            "customer_email": customer_email,
            "total_cents": total_cents,
            "item_count": item_count,
        }).to_string(),
    );

    log::info(&format!(
        "Checkout complete: order {} for {} ({} cents, {} items)",
        order_id, customer_email, total_cents, item_count
    ));

    // 8. Return result
    Ok(json!({
        "order_id": order_id,
        "status": "pending",
        "total_cents": total_cents,
        "item_count": item_count,
    }))
}
```

### `src/operations/refund.rs` — Refund business logic

```rust
use cufflink_fn::prelude::*;
use crate::repo;

/// Process a refund: validate eligibility, update status, restore stock.
pub fn process(tenant: &str, order_id: &str, reason: &str) -> Result<Value, String> {
    // 1. Validate
    if order_id.is_empty() {
        return Err("Order ID is required".into());
    }

    // 2. Get order
    let order = repo::orders::get_by_id(order_id)
        .ok_or_else(|| format!("Order '{}' not found", order_id))?;

    // 3. Check business rules
    match order.status.as_str() {
        "refunded" => return Err("Order is already refunded".into()),
        "cancelled" => return Err("Cannot refund a cancelled order".into()),
        "pending" | "confirmed" | "shipped" | "delivered" => {} // allowed
        other => return Err(format!("Cannot refund order in '{}' status", other)),
    }

    // 4. Restore stock for each item
    let items = repo::orders::get_items(order_id);
    for item in &items {
        if let (Some(product_id), Some(qty)) = (
            item["product_id"].as_str(),
            item["quantity"].as_i64(),
        ) {
            repo::products::restore_stock(product_id, qty as i32);
        }
    }

    // 5. Update order status
    repo::orders::update_status(order_id, "refunded");

    // 6. Publish event
    nats::publish(
        &format!("dw.{}.shop.orders.refunded", tenant),
        &json!({
            "order_id": order_id,
            "reason": reason,
            "refund_amount_cents": order.total_cents,
        }).to_string(),
    );

    log::info(&format!("Refund processed for order {}: {}", order_id, reason));

    Ok(json!({
        "order_id": order_id,
        "status": "refunded",
        "refund_amount_cents": order.total_cents,
        "items_restocked": items.len(),
    }))
}
```

### `src/operations/mod.rs`

```rust
pub mod checkout;
pub mod refund;
```

### `src/lib.rs` — Thin handlers

```rust
mod models;
mod operations;
mod repo;

use cufflink_fn::handler;
use cufflink_fn::prelude::*;

cufflink_fn::init!();

// ─── Checkout ───────────────────────────────────────────

handler!(handle_checkout, |req: Request| {
    // Route posture is `auth`, so the platform already rejected anonymous
    // callers; require_auth() is belt-and-braces and gives us the principal.
    let auth = match req.require_auth() {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let body = req.body();

    // Trust the token for the email, not the request body.
    let customer_email = match auth.email.as_deref() {
        Some(e) => e,
        None => return Response::error("token has no email claim"),
    };

    let items: Vec<models::CartItem> = match serde_json::from_value(body["items"].clone()) {
        Ok(items) => items,
        Err(_) => return Response::error("items must be an array of {product_id, quantity}"),
    };

    match operations::checkout::process(req.tenant(), customer_email, &items) {
        Ok(result) => Response::json(&result),
        Err(msg) => Response::error(&msg),
    }
});

// ─── Order Summary ──────────────────────────────────────

handler!(handle_order_summary, |req: Request| {
    // GET: the id arrives as a query parameter.
    let order_id = match req.body()["params"]["order_id"].as_str() {
        Some(id) => id,
        None => return Response::error("order_id is required"),
    };

    let order = match repo::orders::get_by_id(order_id) {
        Some(o) => o,
        None => return Response::not_found("Order not found"),
    };

    let items = repo::orders::get_items(order_id);

    Response::json(&json!({
        "order": {
            "id": order.id,
            "customer_email": order.customer_email,
            "status": order.status,
            "total_cents": order.total_cents,
            "item_count": order.item_count,
        },
        "items": items,
    }))
});

// ─── Refund ─────────────────────────────────────────────

handler!(handle_refund, |req: Request| {
    let body = req.body();

    let order_id = match body["order_id"].as_str() {
        Some(id) => id,
        None => return Response::error("order_id is required"),
    };

    let reason = body["reason"].as_str().unwrap_or("No reason provided");

    match operations::refund::process(req.tenant(), order_id, reason) {
        Ok(result) => Response::json(&result),
        Err(msg) => Response::error(&msg),
    }
});
```

Notice how every handler is just: parse input, call operation, format response. All the interesting logic lives in `operations/`.

### Calling the handlers

```bash
TOKEN=...   # JWT from Keycloak, or use -H 'Authorization: ApiKey ck_...'

# Checkout
curl -X POST http://localhost:8080/svc/acme/shop/_fn/handle_checkout \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "items": [
      {"product_id": "PRODUCT_UUID_1", "quantity": 2},
      {"product_id": "PRODUCT_UUID_2", "quantity": 1}
    ]
  }'

# Response:
# {"order_id": "...", "status": "pending", "total_cents": 7500, "item_count": 3}

# Order summary (GET — query parameters land in body["params"])
curl "http://localhost:8080/svc/acme/shop/_fn/handle_order_summary?order_id=ORDER_UUID" \
  -H "Authorization: Bearer $TOKEN"

# Refund (requires the orders:refund permission)
curl -X POST http://localhost:8080/svc/acme/shop/_fn/handle_refund \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"order_id": "ORDER_UUID", "reason": "Customer changed mind"}'
```

---

## Example 2: Approval Workflow

A service that enforces a state machine for approval requests. The database stores the data, but the business rules (who can approve, valid state transitions, escalation) live in operations.

### `src/main.rs` — Manifest

```rust
use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "requests")]
pub struct ApprovalRequest {
    #[key]
    pub id: Uuid,
    pub title: String,
    pub description: String,
    pub requester: String,
    pub amount_cents: i64,
    pub status: String,
    pub decided_by: Option<String>,
    pub decision_note: Option<String>,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "approval_history")]
pub struct ApprovalHistory {
    #[key]
    pub id: Uuid,
    #[references("requests.id")]
    #[on_delete("cascade")]
    pub request_id: Uuid,
    pub action: String,
    pub actor: String,
    pub note: Option<String>,
    #[timestamp]
    pub created_at: DateTime<Utc>,
}

cufflink::service! {
    name: "approvals",
    mode: wasm,
    tables: [ApprovalRequest, ApprovalHistory],
    custom_routes: [
        ("POST", "/submit",  "handle_submit",  auth),
        ("POST", "/approve", "handle_approve", perm("approvals", "decide")),
        ("POST", "/reject",  "handle_reject",  perm("approvals", "decide")),
        ("GET",  "/status",  "handle_status",  auth),
    ],
}
```

### `src/repo/requests.rs`

```rust
use cufflink_fn::prelude::*;

pub fn create(title: &str, description: &str, requester: &str, amount_cents: i64) -> Option<String> {
    let id = util::generate_uuid();
    let affected = db::execute(&format!(
        "INSERT INTO requests (id, title, description, requester, amount_cents, status) \
         VALUES ('{}', '{}', '{}', '{}', {}, 'pending')",
        id,
        db::sql_escape(title),
        db::sql_escape(description),
        db::sql_escape(requester),
        amount_cents
    ));
    (affected > 0).then_some(id)
}

pub fn get_by_id(id: &str) -> Option<Value> {
    db::query_one(&format!(
        "SELECT * FROM requests WHERE id = '{}'",
        db::sql_escape(id)
    ))
}

pub fn update_status(id: &str, status: &str, decided_by: &str, note: &str) {
    db::execute(&format!(
        "UPDATE requests SET status = '{}', decided_by = '{}', decision_note = '{}' WHERE id = '{}'",
        db::sql_escape(status),
        db::sql_escape(decided_by),
        db::sql_escape(note),
        db::sql_escape(id)
    ));
}

pub fn record_history(request_id: &str, action: &str, actor: &str, note: &str) {
    db::execute(&format!(
        "INSERT INTO approval_history (id, request_id, action, actor, note) \
         VALUES ('{}', '{}', '{}', '{}', '{}')",
        util::generate_uuid(),
        db::sql_escape(request_id),
        db::sql_escape(action),
        db::sql_escape(actor),
        db::sql_escape(note)
    ));
}

pub fn get_history(request_id: &str) -> Vec<Value> {
    db::query(&format!(
        "SELECT action, actor, note, created_at FROM approval_history \
         WHERE request_id = '{}' ORDER BY created_at ASC",
        db::sql_escape(request_id)
    ))
}
```

### `src/operations/workflow.rs` — State machine logic

```rust
use cufflink_fn::prelude::*;
use crate::repo::requests;

/// Amount threshold above which approval requires a senior approver.
const ESCALATION_THRESHOLD: i64 = 100_000; // $1,000.00

/// Submit a new approval request.
pub fn submit(title: &str, description: &str, requester: &str, amount_cents: i64) -> Result<Value, String> {
    if title.is_empty() {
        return Err("Title is required".into());
    }
    if amount_cents <= 0 {
        return Err("Amount must be positive".into());
    }

    let requires_escalation = amount_cents > ESCALATION_THRESHOLD;
    let id = requests::create(title, description, requester, amount_cents)
        .ok_or("Failed to create request")?;

    requests::record_history(&id, "submitted", requester, "");

    if requires_escalation {
        log::info(&format!(
            "Request {} requires escalation (amount: {} cents)",
            id, amount_cents
        ));
    }

    Ok(json!({
        "request_id": id,
        "status": "pending",
        "requires_escalation": requires_escalation,
    }))
}

/// Approve a request. Enforces state transition rules.
pub fn approve(tenant: &str, request_id: &str, approver: &str, note: &str) -> Result<Value, String> {
    let request = requests::get_by_id(request_id)
        .ok_or("Request not found")?;

    // State machine: only pending requests can be approved
    let status = request["status"].as_str().unwrap_or("");
    match status {
        "pending" => {} // valid transition
        "approved" => return Err("Request is already approved".into()),
        "rejected" => return Err("Cannot approve a rejected request".into()),
        other => return Err(format!("Invalid status for approval: '{}'", other)),
    }

    // Business rule: requester cannot approve their own request
    let requester = request["requester"].as_str().unwrap_or("");
    if requester == approver {
        return Err("You cannot approve your own request".into());
    }

    // Business rule: high-value requests need a senior approver
    let amount = request["amount_cents"].as_i64().unwrap_or(0);
    if amount > ESCALATION_THRESHOLD {
        log::info(&format!(
            "High-value approval by {} for request {} ({} cents)",
            approver, request_id, amount
        ));
    }

    requests::update_status(request_id, "approved", approver, note);
    requests::record_history(request_id, "approved", approver, note);

    nats::publish(
        &format!("dw.{}.approvals.requests.approved", tenant),
        &json!({"request_id": request_id, "approver": approver, "amount_cents": amount}).to_string(),
    );

    Ok(json!({
        "request_id": request_id,
        "status": "approved",
        "approved_by": approver,
    }))
}

/// Reject a request. Enforces state transition rules.
pub fn reject(request_id: &str, rejector: &str, reason: &str) -> Result<Value, String> {
    let request = requests::get_by_id(request_id)
        .ok_or("Request not found")?;

    let status = request["status"].as_str().unwrap_or("");
    match status {
        "pending" => {}
        "approved" => return Err("Cannot reject an already approved request".into()),
        "rejected" => return Err("Request is already rejected".into()),
        other => return Err(format!("Invalid status for rejection: '{}'", other)),
    }

    if reason.is_empty() {
        return Err("Rejection reason is required".into());
    }

    requests::update_status(request_id, "rejected", rejector, reason);
    requests::record_history(request_id, "rejected", rejector, reason);

    Ok(json!({
        "request_id": request_id,
        "status": "rejected",
        "rejected_by": rejector,
        "reason": reason,
    }))
}

/// Get the full status of a request including its history.
pub fn get_status(request_id: &str) -> Result<Value, String> {
    let request = requests::get_by_id(request_id)
        .ok_or("Request not found")?;
    let history = requests::get_history(request_id);

    Ok(json!({
        "request": request,
        "history": history,
    }))
}
```

### `src/lib.rs` — Thin handlers

The actor is always taken from the validated token (`auth.sub`), never from the request body — a caller must not be able to approve as someone else.

```rust
mod operations;
mod repo;

use cufflink_fn::handler;
use cufflink_fn::prelude::*;

cufflink_fn::init!();

handler!(handle_submit, |req: Request| {
    let auth = match req.require_auth() {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let b = req.body();
    match operations::workflow::submit(
        b["title"].as_str().unwrap_or(""),
        b["description"].as_str().unwrap_or(""),
        &auth.sub,
        b["amount_cents"].as_i64().unwrap_or(0),
    ) {
        Ok(result) => Response::json(&result),
        Err(msg) => Response::error(&msg),
    }
});

handler!(handle_approve, |req: Request| {
    let auth = match req.require_auth() {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let b = req.body();
    match operations::workflow::approve(
        req.tenant(),
        b["request_id"].as_str().unwrap_or(""),
        &auth.sub,
        b["note"].as_str().unwrap_or(""),
    ) {
        Ok(result) => Response::json(&result),
        Err(msg) => Response::error(&msg),
    }
});

handler!(handle_reject, |req: Request| {
    let auth = match req.require_auth() {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let b = req.body();
    match operations::workflow::reject(
        b["request_id"].as_str().unwrap_or(""),
        &auth.sub,
        b["reason"].as_str().unwrap_or(""),
    ) {
        Ok(result) => Response::json(&result),
        Err(msg) => Response::error(&msg),
    }
});

handler!(handle_status, |req: Request| {
    let request_id = req.body()["params"]["request_id"].as_str().unwrap_or("");
    match operations::workflow::get_status(request_id) {
        Ok(result) => Response::json(&result),
        Err(msg) if msg == "Request not found" => Response::not_found(&msg),
        Err(msg) => Response::error(&msg),
    }
});
```

---

## Example 3: Analytics & Aggregation

Custom handlers that compute aggregated stats from raw event data — things that simple CRUD endpoints can't do. Assume an `events(id, event_type, actor, metadata, created_at)` table declared in `main.rs`, with routes:

```rust
custom_routes: [
    ("GET",  "/dashboard", "get_dashboard", perm("analytics", "view")),
    ("GET",  "/report",    "get_report",    perm("analytics", "view")),
    ("POST", "/events",    "record_event",  auth),
],
```

### `src/repo/stats.rs`

`days` and `limit` are integers parsed in the handler, so they can be formatted directly.

```rust
use cufflink_fn::prelude::*;

pub fn count_events_today() -> i64 {
    db::query_one("SELECT COUNT(*) as count FROM events WHERE created_at >= CURRENT_DATE")
        .and_then(|r| r["count"].as_i64())
        .unwrap_or(0)
}

pub fn events_by_type(days: i32) -> Vec<Value> {
    db::query(&format!(
        "SELECT event_type, COUNT(*) as count \
         FROM events \
         WHERE created_at >= CURRENT_DATE - INTERVAL '{} days' \
         GROUP BY event_type \
         ORDER BY count DESC",
        days
    ))
}

pub fn daily_counts(days: i32) -> Vec<Value> {
    db::query(&format!(
        "SELECT DATE(created_at) as date, COUNT(*) as count \
         FROM events \
         WHERE created_at >= CURRENT_DATE - INTERVAL '{} days' \
         GROUP BY DATE(created_at) \
         ORDER BY date DESC",
        days
    ))
}

pub fn top_actors(days: i32, limit: i32) -> Vec<Value> {
    db::query(&format!(
        "SELECT actor, COUNT(*) as event_count \
         FROM events \
         WHERE created_at >= CURRENT_DATE - INTERVAL '{} days' \
         GROUP BY actor \
         ORDER BY event_count DESC \
         LIMIT {}",
        days, limit
    ))
}
```

### `src/operations/analytics.rs`

```rust
use cufflink_fn::prelude::*;
use crate::repo::stats;

pub fn dashboard() -> Value {
    let today_count = stats::count_events_today();
    let by_type = stats::events_by_type(7);
    let daily = stats::daily_counts(30);
    let top = stats::top_actors(7, 10);

    json!({
        "today": today_count,
        "by_type_7d": by_type,
        "daily_30d": daily,
        "top_actors_7d": top,
    })
}

pub fn report(event_type: &str, days: i32) -> Value {
    let filtered = db::query(&format!(
        "SELECT DATE(created_at) as date, COUNT(*) as count \
         FROM events \
         WHERE event_type = '{}' AND created_at >= CURRENT_DATE - INTERVAL '{} days' \
         GROUP BY DATE(created_at) \
         ORDER BY date ASC",
        db::sql_escape(event_type), days
    ));

    let total: i64 = filtered.iter()
        .filter_map(|r| r["count"].as_i64())
        .sum();

    json!({
        "event_type": event_type,
        "period_days": days,
        "total": total,
        "daily": filtered,
    })
}
```

### `src/lib.rs`

```rust
mod operations;
mod repo;

use cufflink_fn::handler;
use cufflink_fn::prelude::*;

cufflink_fn::init!();

handler!(get_dashboard, |_req: Request| {
    Response::json(&operations::analytics::dashboard())
});

handler!(get_report, |req: Request| {
    // GET: query parameters are strings under body["params"].
    let p = &req.body()["params"];
    let event_type = p["event_type"].as_str().unwrap_or("page_view");
    let days: i32 = p["days"].as_str().and_then(|d| d.parse().ok()).unwrap_or(30);
    Response::json(&operations::analytics::report(event_type, days.clamp(1, 365)))
});

handler!(record_event, |req: Request| {
    let auth = match req.require_auth() {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let b = req.body();
    let event_type = b["event_type"].as_str().unwrap_or("");
    let metadata = &b["metadata"];

    if event_type.is_empty() {
        return Response::error("event_type is required");
    }

    db::execute(&format!(
        "INSERT INTO events (id, event_type, actor, metadata) VALUES ('{}', '{}', '{}', '{}')",
        util::generate_uuid(),
        db::sql_escape(event_type),
        db::sql_escape(&auth.sub),
        db::sql_escape(&metadata.to_string())
    ));

    Response::json(&json!({"recorded": true}))
});
```

---

## Example 4: Notification Service

A service that renders notification templates and publishes them for async delivery via NATS. Assume `templates(id, name, subject_template, body_template)` and `notifications(id, recipient, channel, subject, body, template_name, status, created_at)` tables, with routes:

```rust
custom_routes: [
    ("POST", "/send",   "send_notification",   perm("notifications", "send")),
    ("GET",  "/status", "get_delivery_status", auth),
],
```

### `src/repo/templates.rs`

```rust
use cufflink_fn::prelude::*;

pub fn get_by_name(name: &str) -> Option<Value> {
    db::query_one(&format!(
        "SELECT id, name, subject_template, body_template FROM templates WHERE name = '{}'",
        db::sql_escape(name)
    ))
}
```

### `src/repo/notifications.rs`

```rust
use cufflink_fn::prelude::*;

pub fn create(
    recipient: &str,
    channel: &str,
    subject: &str,
    body: &str,
    template_name: &str,
) -> Option<String> {
    let id = util::generate_uuid();
    let affected = db::execute(&format!(
        "INSERT INTO notifications (id, recipient, channel, subject, body, template_name, status) \
         VALUES ('{}', '{}', '{}', '{}', '{}', '{}', 'queued')",
        id,
        db::sql_escape(recipient),
        db::sql_escape(channel),
        db::sql_escape(subject),
        db::sql_escape(body),
        db::sql_escape(template_name)
    ));
    (affected > 0).then_some(id)
}

pub fn update_status(id: &str, status: &str) {
    db::execute(&format!(
        "UPDATE notifications SET status = '{}' WHERE id = '{}'",
        db::sql_escape(status), db::sql_escape(id)
    ));
}

pub fn get_by_id(id: &str) -> Option<Value> {
    db::query_one(&format!(
        "SELECT * FROM notifications WHERE id = '{}'",
        db::sql_escape(id)
    ))
}
```

### `src/operations/notify.rs`

```rust
use cufflink_fn::prelude::*;
use crate::repo::{templates, notifications};

/// Simple template rendering — replaces {{key}} with values.
fn render_template(template: &str, vars: &Value) -> String {
    let mut result = template.to_string();
    if let Some(obj) = vars.as_object() {
        for (key, value) in obj {
            let placeholder = format!("{{{{{}}}}}", key);
            let replacement = value.as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| value.to_string());
            result = result.replace(&placeholder, &replacement);
        }
    }
    result
}

/// Send a notification using a template.
pub fn send(
    template_name: &str,
    recipient: &str,
    channel: &str,
    variables: &Value,
) -> Result<Value, String> {
    // 1. Validate
    if recipient.is_empty() {
        return Err("Recipient is required".into());
    }
    if !["email", "sms", "push"].contains(&channel) {
        return Err(format!("Invalid channel '{}'. Use: email, sms, push", channel));
    }

    // 2. Load template
    let template = templates::get_by_name(template_name)
        .ok_or_else(|| format!("Template '{}' not found", template_name))?;

    // 3. Render
    let subject = render_template(
        template["subject_template"].as_str().unwrap_or(""),
        variables,
    );
    let body = render_template(
        template["body_template"].as_str().unwrap_or(""),
        variables,
    );

    // 4. Create notification record
    let notification_id = notifications::create(recipient, channel, &subject, &body, template_name)
        .ok_or("Failed to create notification")?;

    // 5. Publish for async delivery. context::tenant() gives the tenant slug
    //    without threading a Request through every layer.
    nats::publish(
        &format!("dw.{}.notifications.deliver.{}", context::tenant(), channel),
        &json!({
            "notification_id": notification_id,
            "recipient": recipient,
            "channel": channel,
            "subject": subject,
            "body": body,
        }).to_string(),
    );

    log::info(&format!("Notification {} queued for {} via {}", notification_id, recipient, channel));

    Ok(json!({
        "notification_id": notification_id,
        "status": "queued",
        "channel": channel,
        "recipient": recipient,
    }))
}
```

### `src/lib.rs`

```rust
mod operations;
mod repo;

use cufflink_fn::handler;
use cufflink_fn::prelude::*;

cufflink_fn::init!();

handler!(send_notification, |req: Request| {
    let b = req.body();
    match operations::notify::send(
        b["template"].as_str().unwrap_or(""),
        b["recipient"].as_str().unwrap_or(""),
        b["channel"].as_str().unwrap_or("email"),
        &b["variables"],
    ) {
        Ok(result) => Response::json(&result),
        Err(msg) => Response::error(&msg),
    }
});

handler!(get_delivery_status, |req: Request| {
    let id = req.body()["params"]["notification_id"].as_str().unwrap_or("");
    match repo::notifications::get_by_id(id) {
        Some(n) => Response::json(&n),
        None => Response::not_found("Notification not found"),
    }
});
```

---

## CRUD Lifecycle Hooks

Tables can declare hooks that call your WASM handlers before or after the auto-generated CRUD operations. Hooks work in CRUD mode too (`cufflink init my-service --template hooks` scaffolds one): the platform sees that the manifest declares hooks and requires a WASM module in the deploy.

```rust
#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "items", auth_required, permission_area = "items")]
#[hooks(
    before_create = "validate_item",
    after_create = "audit_item_created",
    before_delete = "check_owner_can_delete"
)]
pub struct Item {
    #[key] pub id: Uuid,
    pub title: String,
}
```

### What a hook receives

Hook handlers are ordinary `handler!` functions. They arrive with `req.method() == "HOOK"`, and `req.body()` is one of the envelopes below. The caller's principal is passed as `req.auth()` (not inside the body); it is `None` for anonymous CRUD calls.

| Hook | Timing | `req.body()` | Can |
|------|--------|--------------|-----|
| `before_create` | Before INSERT | `{ "hook": "before_create", "table", "data" }` | Modify data, reject |
| `after_create` | After INSERT | `{ "hook": "after_create", "table", "data", "result" }` | Side effects |
| `before_update` | Before UPDATE | `{ "hook": "before_update", "table", "id", "data", "existing" }` | Modify data, reject |
| `after_update` | After UPDATE | `{ "hook": "after_update", "table", "id", "data", "result" }` | Side effects |
| `before_delete` | Before DELETE | `{ "hook": "before_delete", "table", "id", "existing" }` | Reject |
| `after_delete` | After DELETE | `{ "hook": "after_delete", "table", "id" }` | Side effects |

`data` is the incoming payload, `existing` the current row, `result` the row as written. Batch CRUD calls invoke the same hooks per item.

### Modifying and rejecting

A **before** hook modifies the operation by returning a body with a `data` key — the platform replaces the incoming data with it:

```rust
handler!(validate_item, |req: Request| {
    let mut data = req.body()["data"].clone();
    let title = data["title"].as_str().unwrap_or("").trim().to_string();
    if title.is_empty() {
        return Response::error("title is required");   // rejects with HTTP 400
    }
    data["title"] = json!(title);          // normalise before the INSERT
    Response::json(&json!({"data": data}))
});
```

A **before** hook **rejects** the operation by returning a non-2xx response — `Response::error` (400), `Response::unauthorized` (401), `Response::forbidden` (403), `Response::not_found` (404) or `Response::with_status` — and the client receives that status with `Hook error: <message>`. A 2xx body whose top-level `error` key is set is treated as a 400 rejection too, so hand-written replies keep working.

```rust
handler!(check_owner_can_delete, |req: Request| {
    let auth = match req.auth() {
        Some(a) => a,
        None => return Response::unauthorized("authentication required"),
    };
    let existing = &req.body()["existing"];
    if existing["owner_id"].as_str() != Some(auth.sub.as_str()) && !auth.can("items", "delete") {
        return Response::forbidden("not the owner");
    }
    Response::empty()
});
```

**After** hooks run for side effects only, are fire-and-forget (the HTTP response has already been sent), and their return value is ignored. A hook has the same 30 s budget as a route.

See [Guide 14b](guides.md#guide-14b-crud-lifecycle-hooks) for a full walkthrough, and [WASM Workers](wasm-workers.md) for running after-hooks on the worker pool with retries.

---

## NATS Subscriptions

A service can react to NATS messages with a WASM handler. Declare subscriptions in `service!` as `(subject, handler, wasm)` with an optional fourth element naming the JetStream stream:

```rust
cufflink::service! {
    name: "circulation",
    mode: wasm,
    tables: [Hold],
    subscriptions: [
        // `{tenant}` is replaced with the deploying tenant's slug.
        ("dw.{tenant}.catalog.loans.created", "on_loan_created", wasm),
        ("dw.{tenant}.catalog.loans.*",       "on_any_loan_event", wasm),
    ],
}
```

Each subscription becomes a durable pull consumer on the tenant's stream (`DW_{TENANT}`, which captures `dw.{tenant}.>`), or on the stream you name. Besides `wasm`, the handler type can be one of the built-ins `webhook`, `delete_cascade` or `update_field`.

### Event subjects

Every CRUD write publishes an event you can subscribe to:

```
dw.{tenant}.{service}.{table}.created
dw.{tenant}.{service}.{table}.updated
dw.{tenant}.{service}.{table}.deleted
```

The payload is a JSON object carrying `action`, `table` and `data` (the row as written, or the deleted row). Your own `nats::publish` calls inside `dw.{tenant}.*` land on the same stream.

### Subject matching

Subjects are `.`-separated tokens. In a subscription pattern `*` matches exactly one token and a trailing `>` matches the rest of the subject (one or more tokens), so `dw.acme.catalog.loans.*` matches `...loans.created` but not `...loans.created.v2`, and `dw.acme.catalog.>` matches everything under `catalog`.

### What the handler receives

```json
{
  "method": "SUBSCRIPTION",
  "handler": "on_loan_created",
  "headers": {},
  "body": { ...the published message, parsed as JSON... },
  "tenant": "acme",
  "service": "circulation",
  "auth": null
}
```

```rust
handler!(on_loan_created, |req: Request| {
    let event = req.body();
    let loan_id = event["loan_id"].as_str().unwrap_or("");
    db::execute(&format!(
        "INSERT INTO holds (id, loan_id, status) VALUES ('{}', '{}', 'pending')",
        util::generate_uuid(), db::sql_escape(loan_id)
    ));
    Response::empty()
});
```

Subscription handlers get a 60 s budget (`timeout = N` sets another), are not reachable over `_fn`, and always see `req.auth() == None`. Acknowledgement modes, redelivery and dead letters, message metadata (`req.subscription()`), declared streams, acknowledged publishes and schedules are in [Subscriptions, streams and schedules](subscriptions.md). A message published with a `Cufflink-Reply` header gets the handler's response sent back to that inbox, which is what `nats::request` uses to call another service.

### Publishing: what a handler may send to

`nats::publish` and `nats::request` are policed by the host. A service may always send to its own tenant namespace, `dw.{tenant}.*`. Anything else must be listed in the manifest's `publishes` allowlist (patterns with `*` / `>`), otherwise the call is denied — `nats::publish` returns `false`, `nats::request` returns `None` — and the platform logs a warning and increments `cufflink_nats_publish_denied_total`.

```rust
cufflink::service! {
    name: "shop",
    mode: wasm,
    tables: [Product, Order],
    publishes: ["billing.invoices.>"],   // beyond dw.{tenant}.*
}
```

The current SDK always emits a `publishes` list (possibly empty), so enforcement is on for every service you build today. Manifests built before publish authorization run in a legacy mode that allows everything and counts `cufflink_nats_publish_unscoped_total`. Always build subjects from `req.tenant()` or `context::tenant()` — never hard-code a tenant slug.

---

## Long-Running Jobs

A route handler has 30 seconds. For work that needs more — AI calls, exports, backfills — mark the route as a **job**:

```rust
custom_routes: [
    ("POST", "/reports", "generate_report", perm("reports", "run"), job(timeout = 900, max_attempts = 2)),
    ("POST", "/backfill/chunk", "handle_backfill_chunk", auth, job),   // defaults: 600 s, 3 attempts
],
```

Calling a job route does not run the handler inline. The platform snapshots the request (body, headers, principal), stores a job row, publishes it to the `CUFFLINK_LONG_JOBS` stream and immediately answers:

```
HTTP/1.1 202 Accepted
{"job_id": "11111111-...", "status": "pending"}
```

A [worker](wasm-workers.md) then executes the handler with the job's own timeout, retrying up to `max_attempts` times. Inside the handler, `req.job()` is `Some`:

```rust
handler!(generate_report, |req: Request| {
    if let Some(job) = req.job() {
        if job.is_retry() {
            log::warn(&format!("job {} attempt {}/{}", job.id, job.attempt, job.max_attempts));
            // skip side effects the earlier attempt already paid for
        }
    }
    // Slow upstreams get a real budget here — the host clamps it to what the
    // job has left, so this can never outlive the handler.
    let resp = http::post_with_timeout("https://api.example.com/render", &[], &payload, 300_000);
    // ...
    Response::empty()
});
```

A handler can fan out from inside a job with `jobs::enqueue("handle_backfill_chunk", &json!({"chunk_id": id}))`, which enqueues another `job(...)`-annotated route of the same service, running as the same caller. `jobs::enqueue_with` overrides the timeout and attempts (`0` = platform default).

A tenant with 1000 jobs pending or running is refused with `429`. A job the platform recorded but could not publish is still accepted (`202`) and published by a worker's reclaimer.

### Jobs v2: retries and dead letters

A crash (the handler traps at runtime, or its worker dies) is attempted again while attempts remain. Anything else ends the job on its first attempt as it always did — an error or a timeout fails it, and any response, a 5xx included, completes it — unless the route opts in (needs a platform with `jobs_v2`):

```rust
("POST", "/sync", "sync_crm", auth, job(max_attempts = 5, retry_on = [error, timeout], backoff(base = 10, max = 600))),
```

- `retry_on = [error]` retries a handler error or a 5xx response; `timeout` retries a run past `timeout`.
- `backoff(base, max)` is the delay between attempts, doubling from `base` to `max` seconds (default 30 s to an hour).
- `req.job().and_then(|job| job.last_error())` is what the previous attempt failed with.
- With the `delivery-control` feature a handler decides for itself: `Response::retry_after(Duration)` is another attempt that long from now while attempts remain, and never later than the job's longest backoff (`backoff.max`, an hour unless declared); `Response::terminal(reason)` fails the job at once with `reason` as its error.

A job that fails for good is dead-lettered to `CUFFLINK_DLQ` (see `cufflink dlq`). Run it again with `cufflink jobs retry <id>` (`POST /svc/{tenant}/_jobs/{id}/retry`), which answers the job as `GET /svc/{tenant}/_jobs/{id}` does and refuses a job that has not failed (409). Jobs enqueued with `jobs::enqueue` retry a crash only.

---

## Building & Deploying

`cufflink deploy` is the only supported way to ship a WASM service. Under the hood it:

1. Runs `cargo run -- --emit-manifest` to produce the manifest JSON from `src/main.rs`.
2. Checks that the `wasm32-unknown-unknown` target is installed (`rustup target add wasm32-unknown-unknown` if not).
3. Runs `cargo build --lib --target wasm32-unknown-unknown --release` and reads `target/wasm32-unknown-unknown/release/<crate>.wasm`.
4. Sends one `POST /api/services/deploy` with `manifest`, `wasm_base64`, `commit_hash`, `cli_version` and `api_version`.
5. Verifies the platform stored the same hash it built, and polls `wasm/status` until the binary is durable in object storage (`Deploy verified: db_state=true, ...`).

The platform rejects a WASM-mode manifest (or one that declares hooks, an `on_migrate` handler, or a `wasm` subscription) that arrives without `wasm_base64`, and refuses to deploy against a platform whose `/api/version` is older than the CLI's deploy API version. The deploy validator also checks that every handler named in the manifest — routes, hooks, subscriptions, `on_migrate` — is exported by the module.

A manual `cargo build` is still useful for a fast compile check:

```bash
cargo build --lib --target wasm32-unknown-unknown --release
```

### Schema migrations with `on_migrate`

When a deploy changes the schema you can run data-migration SQL between the additive and destructive phases:

```rust
cufflink::service! {
    name: "ferry",
    mode: wasm,
    tables: [SailingRequest],
    on_migrate: "handle_on_migrate",
}
```

```rust
handler!(handle_on_migrate, |req: Request| {
    migrate::run(req, |diff| {
        if diff.added_column("sailing_requests", "item_count_min")
            && diff.dropped_column("sailing_requests", "item_count")
        {
            db::execute("UPDATE sailing_requests \
                         SET item_count_min = item_count \
                         WHERE item_count_min IS NULL");
        }
        Ok(())
    })
});
```

The hook runs on **every** deploy that is not skipped, including one that changes only the module and a first deploy against an empty database (a deploy whose manifest and module are both unchanged is skipped, and runs nothing), so the closure must be idempotent — guard every `UPDATE` with `WHERE … IS NULL` or similar. A hook that traps, times out, is missing from the module, or returns `Err` from its closure aborts the deploy (the deploy fails with `on_migrate hook '<name>' failed: Hook error: <message>`); the additive phase that already ran is left in place, so keep the closure idempotent and re-run the deploy after fixing it.

---

## Testing

### Unit test your operations and repos natively

To test handlers against a real Postgres, with HTTP, NATS, storage, the cache, config and the clock faked, use the test host ([Testing services](testing.md)). Without it, operations and repos that are plain Rust functions can still be tested on your normal machine (not in WASM), because on non-wasm targets every host call is inert: `db::query` returns `[]`, `db::execute` returns `0`, `db::execute_all` returns `Ok` with zero counts, `nats::publish` returns `true`, `http::*` and `storage::*` return `None`, `config::get` returns `None`, `context::tenant()` returns `""`, `util::generate_uuid()` returns a real v4 UUID. Test business logic that doesn't depend on database results, or structure your code so the SQL layer is separate from the logic layer.

```rust
// src/operations/checkout.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::CartItem;

    #[test]
    fn test_empty_cart_rejected() {
        let result = process("acme", "alice@example.com", &[]);
        assert_eq!(result.unwrap_err(), "Cart is empty");
    }

    #[test]
    fn test_missing_email_rejected() {
        let items = vec![CartItem { product_id: "x".into(), quantity: 1 }];
        let result = process("acme", "", &items);
        assert_eq!(result.unwrap_err(), "Customer email is required");
    }

    #[test]
    fn test_zero_quantity_rejected() {
        let items = vec![CartItem { product_id: "x".into(), quantity: 0 }];
        let result = process("acme", "alice@example.com", &items);
        assert_eq!(result.unwrap_err(), "All quantities must be positive");
    }
}
```

```rust
// src/operations/workflow.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_title_rejected() {
        let result = submit("", "desc", "alice", 5000);
        assert_eq!(result.unwrap_err(), "Title is required");
    }

    #[test]
    fn test_negative_amount_rejected() {
        let result = submit("Request", "desc", "alice", -100);
        assert_eq!(result.unwrap_err(), "Amount must be positive");
    }
}
```

Run them with `cargo test --lib` (add `"rlib"` to `crate-type` so the library is testable natively).

### Test pure logic separately

Extract business logic into standalone functions that don't touch the database:

```rust
// This function is easy to test — no database dependency
fn render_template(template: &str, vars: &Value) -> String {
    // ...
}

#[test]
fn test_template_rendering() {
    let vars = json!({"name": "Alice", "amount": "$50"});
    let result = render_template("Hello {{name}}, you owe {{amount}}", &vars);
    assert_eq!(result, "Hello Alice, you owe $50");
}

#[test]
fn test_template_missing_var() {
    let vars = json!({"name": "Alice"});
    let result = render_template("Hello {{name}}, amount: {{amount}}", &vars);
    assert_eq!(result, "Hello Alice, amount: {{amount}}");
}
```

### Integration test deployed handlers

Once deployed, test end-to-end against the compose stack (tenant `acme`; get a token with `cufflink login` or use an API key):

```bash
#!/bin/bash
set -euo pipefail
BASE="http://localhost:8080/svc/acme/shop"
AUTH="Authorization: ApiKey ${CUFFLINK_API_KEY}"

# Seed a product first (via CRUD endpoint)
PRODUCT=$(curl -sf -X POST "$BASE/products" -H "$AUTH" \
  -H 'Content-Type: application/json' \
  -d '{"name": "Widget", "price_cents": 1500, "stock": 100}')
PRODUCT_ID=$(echo "$PRODUCT" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

# Test checkout
CHECKOUT=$(curl -sf -X POST "$BASE/_fn/handle_checkout" -H "$AUTH" \
  -H 'Content-Type: application/json' \
  -d "{\"items\": [{\"product_id\": \"$PRODUCT_ID\", \"quantity\": 2}]}")

ORDER_ID=$(echo "$CHECKOUT" | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")
TOTAL=$(echo "$CHECKOUT" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_cents'])")
[ "$TOTAL" = "3000" ] || { echo "FAIL: expected 3000, got $TOTAL"; exit 1; }
echo "Checkout OK: order $ORDER_ID, total $TOTAL cents"

# Verify stock decremented
PRODUCT_AFTER=$(curl -sf "$BASE/products/$PRODUCT_ID" -H "$AUTH")
STOCK=$(echo "$PRODUCT_AFTER" | python3 -c "import sys,json; print(json.load(sys.stdin)['stock'])")
[ "$STOCK" = "98" ] || { echo "FAIL: expected stock 98, got $STOCK"; exit 1; }
echo "Stock decremented OK: $STOCK"

# Test refund
REFUND=$(curl -sf -X POST "$BASE/_fn/handle_refund" -H "$AUTH" \
  -H 'Content-Type: application/json' \
  -d "{\"order_id\": \"$ORDER_ID\", \"reason\": \"test\"}")
STATUS=$(echo "$REFUND" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
[ "$STATUS" = "refunded" ] || { echo "FAIL: expected refunded, got $STATUS"; exit 1; }
echo "Refund OK"

echo "All tests passed!"
```

---

## API Reference

Everything below is exported by `use cufflink_fn::prelude::*;` (plus `use cufflink_fn::handler;` for the macro). Signatures are those of `cufflink-fn` 0.29.0.

### Macros

#### `cufflink_fn::init!()`

Required once per module. Exports the `alloc` function the platform needs to pass data into your WASM module.

#### `handler!(name, |req: Request| { ... })`

Defines a handler. Generates the `#[no_mangle] extern "C"` wrapper so your code is a plain Rust closure taking a `Request` and returning a `Response`. The identifier is the exported symbol and must equal the handler name used in the manifest (route tuple, hook attribute, subscription tuple or `on_migrate`).

```rust
handler!(my_handler, |req: Request| {
    Response::json(&json!({"ok": true}))
});
```

### Request

| Method | Returns | Description |
|--------|---------|-------------|
| `req.method()` | `&str` | `GET`, `POST`, `PUT`, `DELETE`, `PATCH`; `HOOK` for lifecycle hooks; `SUBSCRIPTION` for NATS subscriptions |
| `req.handler()` | `&str` | Handler name from the URL |
| `req.body()` | `&Value` | Parsed JSON body (`Value::Null` if empty); query params under `["params"]` |
| `req.raw_body()` | `&[u8]` | Exact request bytes as sent — for HMAC signature checks (Stripe, etc.). Empty when no body |
| `req.headers()` | `&HashMap<String, String>` | All HTTP headers, names lowercased |
| `req.header("name")` | `Option<&str>` | Get a specific header (use the lowercase name) |
| `req.tenant()` | `&str` | Tenant slug from the URL |
| `req.service()` | `&str` | Service name from the URL |
| `req.auth()` | `Option<&Auth>` | Validated auth context, or `None` |
| `req.require_auth()` | `Result<&Auth, Response>` | Auth, or a ready-made 401 response |
| `req.job()` | `Option<&JobContext>` | Set when running as a long-running job on a worker |
| `req.posture()` | `Option<&Value>` | The route's declared posture as JSON (`{"kind": "owner", "field": "..."}` etc.); `None` on legacy routes |
| `req.owner_field()` | `Option<&str>` | For `owner("field")` routes, the column to scope by; `None` otherwise |

### Auth

The platform validates JWTs (Keycloak) and API keys, resolves Cufflink permissions for the service's tenant, and passes the result to your handler. See [Guide 31](guides.md#guide-31-authentication--authorization) for full details.

| Field/Method | Type | Description |
|-------------|------|-------------|
| `auth.sub` | `String` | Keycloak subject (user id); for an API key, the key's subject |
| `auth.preferred_username` | `Option<String>` | Username |
| `auth.name` | `Option<String>` | Display name |
| `auth.email` | `Option<String>` | Email address |
| `auth.realm_roles` | `Vec<String>` | Keycloak realm roles |
| `auth.claims` | `HashMap<String, Value>` | All other JWT claims |
| `auth.permissions` | `Vec<String>` | Cufflink permissions (e.g. `"staff:create"`, `"items:*"`) |
| `auth.role_names` | `Vec<String>` | Cufflink role names assigned to the user |
| `auth.is_service_account` | `bool` | Keycloak service account (client-credentials grant). Service accounts bypass `perm(...)` checks at the platform level |
| `auth.has_role("admin")` | `bool` | Check for a Keycloak realm role |
| `auth.can("area", "op")` | `bool` | Check for a Cufflink permission (`area:*` and `*` wildcards) |
| `auth.has_cufflink_role("name")` | `bool` | Check for a Cufflink role by name |
| `auth.claim("key")` | `Option<&Value>` | Get a custom claim |

### JobContext

Present via `req.job()` only when the handler was dispatched by the long-running jobs runtime.

| Field/Method | Type | Description |
|-------------|------|-------------|
| `job.id` | `String` | Job id (the one returned in the 202 response) |
| `job.attempt` | `u32` | 1-based attempt number |
| `job.max_attempts` | `u32` | Configured maximum |
| `job.is_retry()` | `bool` | `attempt > 1` |

### Response

| Method | Status | Body |
|--------|--------|------|
| `Response::json(&value)` | 200 | The value as JSON |
| `Response::text("string")` | 200 | The string as a JSON string literal (`"string"`), not `text/plain` |
| `Response::empty()` | 200 | `{"ok": true}` |
| `Response::error("message")` | 400 | `{"error": "message"}` |
| `Response::not_found("message")` | 404 | `{"error": "message"}` |
| `Response::unauthorized("message")` | 401 | `{"error": "message"}` |
| `Response::forbidden("message")` | 403 | `{"error": "message"}` |
| `.with_status(u16)` | any | Override the status of any response, e.g. `Response::json(&v).with_status(201)` |

Non-200 responses are wrapped as `{"__status": n, "__body": ...}` on the wire; the `_fn` router unwraps this and sets the real HTTP status.

### db module

| Function | Returns | Description |
|----------|---------|-------------|
| `db::query(sql)` | `Vec<Value>` | Run a SELECT; rows as JSON objects keyed by column. Empty on error |
| `db::query_one(sql)` | `Option<Value>` | First row of a SELECT |
| `db::execute(sql)` | `i32` | Run INSERT/UPDATE/DELETE; affected-row count, **`-1` on error** |
| `db::execute_all(&[sql, ...])` | `Result<Vec<i32>, String>` | Run N statements in one transaction on one connection — all commit or none. Per-statement counts on success; `Err` names the 1-based failing statement |
| `db::query_read_only(sql)` | `Result<Value, String>` | Run one caller-supplied SELECT under host-enforced guards (below). `Ok({"ok": true, "row_count", "truncated", "rows": [...]})`; `Err` carries the database's message for an invalid query |
| `db::sql_escape(s)` | `String` | Double single quotes for safe use inside a `'...'` literal. Pure, no host call |

`query`, `query_one`, `execute` and `execute_all` run with `search_path` set to the **service schema**, so bare table names work. Each of the first three takes its own pooled connection and implicit transaction; only `execute_all` spans statements (one statement per slot — the extended query protocol rejects several commands in one string; no rows are returned, read them back after the batch commits).

`query_read_only` exists for SQL your service did not write (an assistant turning a question into a query). The host — never the guest — fixes every safety property: a `READ ONLY` transaction that is always rolled back, `SET LOCAL ROLE` to a least-privilege role, a statement timeout and a row cap. They are platform settings, not arguments:

| Platform env | Default | Meaning |
|--------------|---------|---------|
| `DB_READ_ONLY_ROLE` | `reports_ro` | Role the statement runs as |
| `DB_READ_ONLY_TIMEOUT_SECS` | `30` | `statement_timeout` |
| `DB_READ_ONLY_MAX_ROWS` | `10000` | Row cap; `truncated: true` when hit |

No `search_path` is set on this path, so name schemas explicitly (`SELECT … FROM reporting.item_overview`); your own tables are not reachable by bare name here.

```rust
let out = db::query_read_only("SELECT count(*) FROM reporting.item_overview")?;
let rows = out["rows"].as_array().cloned().unwrap_or_default();
if out["truncated"].as_bool().unwrap_or(false) { /* say so */ }
```

### nats module

| Function | Returns | Description |
|----------|---------|-------------|
| `nats::publish(subject, payload)` | `bool` | Publish; `true` on success, `false` on failure **or when the publish policy denies the subject** |
| `nats::request(subject, payload, timeout_ms: i32)` | `Option<String>` | Request/reply; the reply payload, or `None` on timeout, failure or denial. The host enforces a minimum timeout of 1000 ms |
| `nats::publish_with(subject, &bytes, &PublishOptions)` | `Result<PubAck, PublishError>` | Feature `nats-publish-v2`: publish and wait for the stream's acknowledgement; headers and a `msg_id` for deduplication ([details](subscriptions.md#publishing-with-acknowledgements)) |
| `nats::publish_json(subject, &value, &PublishOptions)` | `Result<PubAck, PublishError>` | The same with a JSON payload |
| `nats::publish_core(subject, &bytes, &headers)` | `Result<(), PublishError>` | Send without a stream; `Ok` means sent |

Policy: a handler may always send to `dw.{tenant}.*`; other subjects must match a pattern in the manifest's `publishes` list (see [Subscriptions](#publishing-what-a-handler-may-send-to)).

```rust
let tenant = req.tenant();
nats::publish(
    &format!("dw.{}.shop.orders.created", tenant),
    &json!({"order_id": order_id}).to_string(),
);
let reply = nats::request(
    &format!("dw.{}.users.lookup", tenant),
    &json!({"customer_id": id}).to_string(),
    5000,
);
```

### http module

| Function | Returns | Description |
|----------|---------|-------------|
| `http::get(url, headers)` | `Option<FetchResponse>` | GET |
| `http::post(url, headers, body)` | `Option<FetchResponse>` | POST with a body |
| `http::put(url, headers, body)` | `Option<FetchResponse>` | PUT with a body |
| `http::delete(url, headers)` | `Option<FetchResponse>` | DELETE |
| `http::patch(url, headers, body)` | `Option<FetchResponse>` | PATCH with a body |
| `http::fetch(method, url, headers, body: Option<&str>)` | `Option<FetchResponse>` | Generic request |
| `http::fetch_with_timeout(method, url, headers, body, timeout_ms: u64)` | `Option<FetchResponse>` | `fetch` with an explicit request budget; `0` = host default. **Needs platform 0.25+** (imports `http_fetch_timeout`; on an older platform the module fails to instantiate) |
| `http::post_with_timeout(url, headers, body, timeout_ms: u64)` | `Option<FetchResponse>` | POST with an explicit budget |
| `http::fetch_many(&[FetchRequest { method, url, headers, body }])` | `Vec<Result<FetchResponse, String>>` | N requests in parallel on the host (bounded, default 32 concurrent); per-item errors, other slots still succeed |
| `http::fetch_bytes(&HttpRequest)` | `Result<HttpResponse, HttpError>` | Feature `http-v2`: binary bodies both ways, repeated headers, redirects on or off, `max_response_bytes` up to `HOST_HTTP_V2_MAX_RESPONSE_BYTES`; errors carry a kind (`denied`, `timeout`, `too_large`, …) |
| `http::multipart(&HttpRequest, &[Part::text(..), Part::file(..)])` | `Result<HttpResponse, HttpError>` | Feature `http-v2`: a `multipart/form-data` request |

`headers` is `&[(&str, &str)]`. Default budgets are 28 s per request and 20 s to connect; every budget, requested or defaulted, is clamped to the handler's remaining execution time (never below 500 ms), so inside a `job(timeout = 900)` handler you can ask for minutes and inside a route you cannot exceed 30 s.

`FetchResponse` fields and methods:

| Member | Type | Description |
|--------|------|-------------|
| `.status` | `i32` | HTTP status code |
| `.body` | `String` | Body text — base64-encoded when the content is binary |
| `.body_encoding` | `String` | `"utf8"` or `"base64"` |
| `.headers` | `HashMap<String, String>` | Response headers |
| `.json()` | `Option<Value>` | Parse the body as JSON |
| `.is_success()` | `bool` | 2xx |
| `.is_base64()` | `bool` | Body is base64 |

```rust
let resp = http::post(
    "https://api.example.com/notify",
    &[("Authorization", "Bearer token123"), ("Content-Type", "application/json")],
    &json!({"message": "Hello"}).to_string(),
);
match resp {
    Some(r) if r.is_success() => log::info("Notification sent"),
    Some(r) => log::warn(&format!("upstream returned {}: {}", r.status, r.body)),
    None => log::error("request failed"),
}

let results = http::fetch_many(&[
    http::FetchRequest { method: "GET", url: "https://a.example.com", headers: &[], body: None },
    http::FetchRequest { method: "GET", url: "https://b.example.com", headers: &[], body: None },
]);
```

### storage module

Object storage through the platform's own credentials (S3-compatible or GCS, whatever the platform is configured with).

| Function | Returns | Description |
|----------|---------|-------------|
| `storage::download(bucket, key)` | `Option<String>` | Object contents **base64-encoded** |
| `storage::download_many(&[(bucket, key), ...])` | `Vec<Result<Vec<u8>, String>>` | N parallel downloads (bounded, default 32); decoded **bytes** per slot, per-item errors |
| `storage::presign_upload(bucket, key, content_type, expires_secs: u64)` | `Option<String>` | Presigned PUT URL for direct client upload |
| `storage::presign_download(bucket, key, expires_secs: u64)` | `Option<String>` | Presigned GET URL |
| `storage::delete(bucket, key)` | `bool` | `true` when the object is confirmed absent (including already gone); `false` when the host could not delete it |
| `storage::upload(bucket, key, content_type, &bytes)` | `Result<Uploaded, StorageError>` | Feature `storage-v2`: store bytes (up to `HOST_STORAGE_MAX_UPLOAD_BYTES`, ≈48 MiB at the defaults); `upload_with` adds `content_disposition` and `cache_control` |
| `storage::head(bucket, key)` | `Result<Option<ObjectMeta>, StorageError>` | Feature `storage-v2`: an object's size, type and etag without its bytes; `None` when absent |

```rust
handler!(upload_url, |req: Request| {
    let key = format!("uploads/{}.jpg", util::generate_uuid());
    match storage::presign_upload("media", &key, "image/jpeg", 300) {
        Some(url) => Response::json(&json!({"key": key, "url": url})),
        None => Response::error("storage not configured"),
    }
});
```

### image module

Image transforms run on the host: download from object storage, decode (EXIF orientation honoured), optionally downscale the longest edge to `max_dim` (`0` = no resize), encode at `quality` (1–100) and write back to the same bucket. Returns bytes written, or `None` on failure.

| Function | Returns | Description |
|----------|---------|-------------|
| `image::transform_jpeg(bucket, in_key, out_key, max_dim: u32, quality: u8)` | `Option<u32>` | JPEG output |
| `image::transform_webp(bucket, in_key, out_key, max_dim, quality)` | `Option<u32>` | Lossy WebP (~25–30% smaller than JPEG) |
| `image::transform_avif(bucket, in_key, out_key, max_dim, quality)` | `Option<u32>` | Lossy AVIF (smaller still, CPU-heavy — prefer jobs/backfills over hot requests) |
| `image::transform_multi(bucket, in_key, &[VariantSpec { out_key, max_dim, quality, format }])` | `Option<Vec<u32>>` | Decode once, write several variants (`format` is `"jpeg"`, `"webp"` or `"avif"`) |

```rust
let written = image::transform_multi("media", "raw/photo.jpg", &[
    image::VariantSpec { out_key: "raw/photo.thumb.webp",   max_dim: 256,  quality: 75, format: "webp" },
    image::VariantSpec { out_key: "raw/photo.display.webp", max_dim: 1024, quality: 80, format: "webp" },
]);
```

### jobs module

| Function | Returns | Description |
|----------|---------|-------------|
| `jobs::enqueue(handler, &payload)` | `Option<String>` | Enqueue a `job(...)`-annotated route of the current service with `payload` as its body, as the current caller. Returns the job id |
| `jobs::enqueue_with(handler, &payload, JobOptions { timeout_secs, max_attempts })` | `Option<String>` | Same with overrides; `0` means the platform default (600 s, 3 attempts) |

```rust
for chunk in chunks {
    jobs::enqueue("handle_backfill_chunk", &json!({"chunk_id": chunk.id}));
}
```

### redis module

Backed by the platform's Redis connection. `None`/`false` when Redis is not configured. **Redis is disabled on the worker**, so handlers that run there (jobs, worker-routed handlers) see a miss on every read and `false` on every write — treat it as a cache, never as the source of truth.

| Function | Returns | Description |
|----------|---------|-------------|
| `redis::get(key)` | `Option<String>` | Value, or `None` if absent or on error |
| `redis::set(key, value, ttl_secs: i32)` | `bool` | Set with TTL (`0` = no expiry) |
| `redis::del(key)` | `bool` | Delete |
| `redis::get_with_status(key)` | `Result<Option<String>, String>` | `Ok(Some)` hit, `Ok(None)` miss, `Err` Redis unavailable — when you need to tell the two apart |
| `redis::mget(&[key, ...])` | `Result<Vec<Option<String>>, String>` | Several keys in one round trip, results in input order |

```rust
let key = format!("perms:{}:{}", context::tenant(), auth.sub);
match redis::get_with_status(&key) {
    Ok(Some(cached)) => { /* use it */ }
    Ok(None) => { let fresh = compute(); redis::set(&key, &fresh, 300); }
    Err(e) => { log::warn(&format!("redis unavailable: {}", e)); let fresh = compute(); }
}
```

### config module

| Function | Returns | Description |
|----------|---------|-------------|
| `config::get(key)` | `Option<String>` | Read a value set with `cufflink config set KEY VALUE [--secret]`, scoped to this service |

```rust
let api_key = match config::get("ANTHROPIC_API_KEY") {
    Some(k) => k,
    None => return Response::error("ANTHROPIC_API_KEY is not configured"),
};
```

### context module

| Function | Returns | Description |
|----------|---------|-------------|
| `context::tenant()` | `String` | Tenant slug of the executing service — for helpers that don't receive a `Request`. Empty string outside WASM |

### util module

| Function | Returns | Description |
|----------|---------|-------------|
| `util::generate_uuid()` | `String` | Random UUID v4 (from the host; a real UUID natively too) |
| `util::current_time()` | `String` | Current UTC time as RFC 3339 |
| `util::now_millis()` | `i64` | Feature `util-v2`: milliseconds since the Unix epoch |
| `util::sleep_ms(ms)` | `u32` | Feature `util-v2`: wait on the host, up to `UTIL_SLEEP_MAX_MS` and never past the handler's budget; the milliseconds slept |

WASM modules cannot access system randomness or clocks directly; these call the host. `util::current_time()` is the platform's clock at the moment of the call, while SQL `now()` is the database server's clock at the start of the transaction, the same for every statement of one `db::execute_all` or `sql::transaction`. Use one or the other for a column, not both, when values are compared.

```rust
handler!(create_order, |req: Request| {
    let order_id = util::generate_uuid();
    let now = util::current_time();
    db::execute(&format!(
        "INSERT INTO orders (id, status, updated_at) VALUES ('{}', 'pending', '{}')",
        order_id, now
    ));
    Response::json(&json!({ "id": order_id, "updated_at": now })).with_status(201)
});
```

### log module

| Function | Platform level | Description |
|----------|---------------|-------------|
| `log::error(msg)` | ERROR | Log an error |
| `log::warn(msg)` | WARN | Log a warning |
| `log::info(msg)` | INFO | Log info |
| `log::debug(msg)` | DEBUG | Log debug output |

Messages appear in the platform (or worker) logs prefixed with `[wasm]`.

### migrate module

| Function | Returns | Description |
|----------|---------|-------------|
| `migrate::run(req, \|diff: SchemaDiff\| -> Result<(), String>)` | `Response` | Parse the `SchemaDiff` the deployer sends to the `on_migrate` handler and invoke the closure; `200 {"ok": true}` on success, 400 with the message on `Err` or a parse failure |

`SchemaDiff` offers `added_column(table, column)`, `dropped_column(table, column)` and `is_empty()`. See [Schema migrations with `on_migrate`](#schema-migrations-with-on_migrate).

### stripe feature

`cufflink-fn = { version = "0.34", features = ["stripe"] }` adds a `stripe` module (webhook signature verification over `req.raw_body()`, form encoding and a small REST client); it is opt-in so the crypto dependencies stay out of services that don't need it.

### Compatibility

- `cufflink-fn` 0.25.x targets platform 0.25+. `http::fetch_with_timeout` / `post_with_timeout` import a host function that only exists there — roll the platform before deploying a service that calls them.
- Route postures require a platform with the `route_posture` feature; the manifest declares it as required so an older platform rejects the deploy instead of running the routes unenforced.

### Scaling WASM handlers

`job(...)` routes and `jobs::enqueue` always run on the worker pool; compute-heavy synchronous handlers and after-hooks can be routed there too with `WORKER_ROUTES`. See [WASM Workers](wasm-workers.md).
