# Cufflink Service Guides

A progressive series of guides that take you from a simple single-table service to a production-grade application using every feature the platform offers.

---

## Table of Contents

1. [Guide 1: Hello World — Your First Service](#guide-1-hello-world--your-first-service)
2. [Guide 2: Multiple Tables & Relationships](#guide-2-multiple-tables--relationships)
3. [Guide 3: Data Validation & Constraints](#guide-3-data-validation--constraints)
4. [Guide 4: Soft Deletes & Filtering](#guide-4-soft-deletes--filtering)
5. [Guide 5: Full-Text Search & Pagination](#guide-5-full-text-search--pagination)
6. [Guide 6: Real-Time Streaming with SSE](#guide-6-real-time-streaming-with-sse)
7. [Guide 7: Webhooks for External Integrations](#guide-7-webhooks-for-external-integrations)
8. [Guide 8: Row-Level Security](#guide-8-row-level-security)
9. [Guide 9: File Storage](#guide-9-file-storage)
10. [Guide 10: Configuration & Secrets](#guide-10-configuration--secrets)
11. [Guide 11: Data Seeding & Backups](#guide-11-data-seeding--backups)
12. [Guide 12: GraphQL Queries](#guide-12-graphql-queries)
13. [Guide 13: Batch Operations & Transactions](#guide-13-batch-operations--transactions)
14. [Guide 14: RBAC & Permissions](#guide-14-rbac--permissions)
14b. [Guide 14b: CRUD Lifecycle Hooks](#guide-14b-crud-lifecycle-hooks)
15. [Guide 15: The Full-Stack Application](#guide-15-the-full-stack-application)

**Custom Business Logic & Advanced Features**

16. [Guide 16: WASM Mode — Custom Business Logic](#guide-16-wasm-mode--custom-business-logic)
17. [Guide 17: Event Subscriptions & Reactive Patterns](#guide-17-event-subscriptions--reactive-patterns)
18. [Guide 18: Custom Routes & API Design](#guide-18-custom-routes--api-design)
19. [Guide 19: Multi-Tenant Architecture Patterns](#guide-19-multi-tenant-architecture-patterns)
20. [Guide 20: Advanced Filtering, JSON Operators & Aggregation](#guide-20-advanced-filtering-json-operators--aggregation)
21. [Guide 21: OpenAPI & TypeScript Client Generation](#guide-21-openapi--typescript-client-generation)
22. [Guide 22: Audit Trails & Compliance](#guide-22-audit-trails--compliance)
23. [Guide 23: Production Deployment Patterns](#guide-23-production-deployment-patterns)

**Testing**

24. [Guide 24: Testing Your Service — Unit Tests](#guide-24-testing-your-service--unit-tests)
25. [Guide 25: Integration Testing with a Running Platform](#guide-25-integration-testing-with-a-running-platform)
26. [Guide 26: Testing WASM Handlers](#guide-26-testing-wasm-handlers)
27. [Guide 27: End-to-End Testing with CI/CD](#guide-27-end-to-end-testing-with-cicd)
28. [Guide 28: Load Testing & Performance Validation](#guide-28-load-testing--performance-validation)

**Environments & CI/CD**

29. [Guide 29: Environments — Staging, Production & Testing](#guide-29-environments--staging-production--testing)
30. [Guide 30: CI/CD Pipeline — Automated Deploy](#guide-30-cicd-pipeline--automated-deploy)

**Authentication**

31. [Guide 31: Authentication & Authorization](#guide-31-authentication--authorization)

**Operations**

32. [Guide 32: Multi-Replica Deployment](#guide-32-multi-replica-deployment)
33. [Guide 33: Observability & Monitoring](#guide-33-observability--monitoring)

**Scaling**

- [WASM Worker Pool](wasm-workers.md) — offload heavy WASM handlers to dedicated worker processes

---

## Guide 1: Hello World — Your First Service

This guide creates a simple todo list service with a single table.

### Prerequisites

1. **A running platform.** From a clone of this repo, `docker compose up -d` starts Postgres, NATS, Keycloak, object storage and the platform, and registers a dev tenant with slug `acme` (Keycloak realm `acme`, user `dev` / `dev`). Check it with `curl localhost:8080/health` (prints `cufflink-platform ok`).
2. **The CLI.** `cargo install cufflink-cli` (or `cargo build -p cufflink-cli` from this repo, which puts the binary at `target/debug/cufflink`).
3. **Credentials.** Every deploy needs an API key for the tenant. Run `cufflink login` inside your project directory (it needs the `Cufflink.toml` shown below) and complete the device-flow prompt in your browser as `dev` / `dev`; the key is saved to `~/.config/cufflink/credentials.json`. Alternatively export `CUFFLINK_API_KEY=ck_...`.

### Scaffold the project

```bash
cufflink init my-todos
cd my-todos
```

This creates three files:

- `Cufflink.toml` — environment configuration (local, staging, production)
- `Cargo.toml` — Rust dependencies
- `src/main.rs` — your service definition

Or create the files manually:

**Cargo.toml**
```toml
[package]
name = "my-todos"
version = "0.1.0"
edition = "2021"

[dependencies]
cufflink = { version = "0.15" }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
```

**Cufflink.toml**
```toml
[service]
default_env = "local"

[environments.local]
api_url = "http://localhost:8080"
tenant = "acme"
keycloak_url = "http://localhost:8180"
keycloak_realm = "acme"
```

The `tenant` here must match a tenant registered on the platform — the compose stack creates `acme`. The `keycloak_url` / `keycloak_realm` pair is what `cufflink login` uses for the device flow.

**src/main.rs**
```rust
use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "todos")]
pub struct Todo {
    #[key]
    pub id: Uuid,
    pub title: String,
    pub completed: bool,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

cufflink::service! {
    name: "my-todos",
    tables: [Todo],
}
```

### What each attribute does

- `#[derive(Table)]` — Generates the schema metadata for deployment
- `#[table(name = "todos")]` — The SQL table name
- `#[key]` — Primary key, auto-generates a UUID on creation
- `#[timestamp]` — Auto-generated; `created_at` is set on INSERT, `updated_at` on UPDATE
- `cufflink::service!` — Generates the `main()` function that emits the manifest

### Deploy

```bash
cufflink deploy
# => Environment: local
#    Building service...
#    Deploying my-todos (mode: crud)...
#    Deployed my-todos vN
#      Schema changes: 1
#      Deployment ID:  "98e338c3-..."
#      Tenant:         "acme"
#      Commit:         f1af9c6...
```

The version number is assigned by the platform and increments on every deploy. `cufflink services` lists what is deployed (NAME, VERSION, MODE, STATUS, TENANT, COMMIT) and `cufflink status` shows the current service in detail.

### Use the API

CRUD routes live at `/svc/{tenant}/{service}/{table}` — for this guide, `/svc/acme/my-todos/todos`. Tables without `auth_required` accept anonymous requests.

**Create a todo:**
```bash
curl -X POST http://localhost:8080/svc/acme/my-todos/todos \
  -H 'Content-Type: application/json' \
  -d '{"title": "Buy groceries", "completed": false}'
```

Response:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "title": "Buy groceries",
  "completed": false,
  "created_at": "2026-02-19T10:30:00Z",
  "updated_at": "2026-02-19T10:30:00Z"
}
```

**List all todos:**
```bash
curl http://localhost:8080/svc/acme/my-todos/todos
```

Response (lists are always wrapped with pagination metadata; default 50 per page, newest first):
```json
{
  "results": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "title": "Buy groceries",
      "completed": false,
      "created_at": "2026-02-19T10:30:00Z",
      "updated_at": "2026-02-19T10:30:00Z"
    }
  ],
  "pagination": {
    "current_page": 1,
    "per_page": 50,
    "total_records": 1,
    "total_pages": 1
  }
}
```

**Get a single todo:**
```bash
curl http://localhost:8080/svc/acme/my-todos/todos/550e8400-e29b-41d4-a716-446655440000
```

**Update a todo:**
```bash
curl -X PUT http://localhost:8080/svc/acme/my-todos/todos/550e8400-... \
  -H 'Content-Type: application/json' \
  -d '{"completed": true}'
```

**Delete a todo:**
```bash
curl -X DELETE http://localhost:8080/svc/acme/my-todos/todos/550e8400-...
```

A GET for the deleted id now returns 404.

---

## Guide 2: Multiple Tables & Relationships

Build a blog service with posts, comments, and tags — demonstrating foreign keys and cascading deletes.

**src/main.rs**
```rust
use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "authors")]
pub struct Author {
    #[key]
    pub id: Uuid,
    pub name: String,
    pub email: String,
    pub bio: Option<String>,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "posts")]
pub struct Post {
    #[key]
    pub id: Uuid,
    pub title: String,
    pub slug: Option<String>,
    pub body: String,
    #[references("authors.id")]
    #[on_delete("cascade")]
    pub author_id: Uuid,
    pub published: bool,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "comments")]
pub struct Comment {
    #[key]
    pub id: Uuid,
    #[references("posts.id")]
    #[on_delete("cascade")]
    pub post_id: Uuid,
    pub author_name: String,
    pub body: String,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "tags")]
pub struct Tag {
    #[key]
    pub id: Uuid,
    pub name: String,
    #[references("posts.id")]
    #[on_delete("cascade")]
    pub post_id: Uuid,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

cufflink::service! {
    name: "blog-service",
    tables: [Author, Post, Comment, Tag],
}
```

### Foreign key behavior

- `#[references("authors.id")]` — Creates a `FOREIGN KEY` constraint referencing the `authors` table
- `#[on_delete("cascade")]` — When the referenced row is deleted, all referencing rows are automatically deleted

**Available `on_delete` actions:**
| Action | Behavior |
|--------|----------|
| `cascade` | Delete referencing rows |
| `set_null` | Set the FK column to NULL |
| `restrict` | Prevent deletion if references exist |
| `no_action` | Same as restrict (deferred) |

### Usage

```bash
# Create an author
curl -X POST http://localhost:8080/svc/acme/blog-service/authors \
  -H 'Content-Type: application/json' \
  -d '{"name": "Alice", "email": "alice@example.com"}'

# Create a post (use the author's ID)
curl -X POST http://localhost:8080/svc/acme/blog-service/posts \
  -H 'Content-Type: application/json' \
  -d '{"title": "My First Post", "body": "Hello world!", "author_id": "AUTHOR_ID", "published": true}'

# Add a comment
curl -X POST http://localhost:8080/svc/acme/blog-service/comments \
  -H 'Content-Type: application/json' \
  -d '{"post_id": "POST_ID", "author_name": "Bob", "body": "Great post!"}'

# Delete the author — cascades to posts, then to comments
curl -X DELETE http://localhost:8080/svc/acme/blog-service/authors/AUTHOR_ID
```

---

## Guide 3: Data Validation & Constraints

Add validation rules and unique constraints to ensure data integrity.

Each rule is a **string literal** of the form `"key=value"`; pass several to one `#[validate(...)]` attribute separated by commas. (The derive silently ignores anything that is not a string literal, so `#[validate(min_length = 3)]` compiles but produces no rule.)

```rust
use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "users")]
pub struct User {
    #[key]
    pub id: Uuid,

    #[unique]
    #[validate("min_length=3", "max_length=50")]
    pub username: String,

    #[unique]
    #[validate("regex=^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")]
    pub email: String,

    #[validate("min_length=8", "max_length=128")]
    pub display_name: String,

    #[validate("min=0", "max=150")]
    pub age: Option<i32>,

    #[default("free")]
    pub plan: String,

    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "products")]
pub struct Product {
    #[key]
    pub id: Uuid,
    pub name: String,

    #[validate("min=0.01")]
    pub price: f64,

    #[validate("min=0")]
    pub stock: i32,

    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

cufflink::service! {
    name: "validated-service",
    tables: [User, Product],
}
```

### Validation rules reference

| Rule | Applies to | Example | Description |
|------|-----------|---------|-------------|
| `min_length=N` | `String` | `#[validate("min_length=3")]` | Minimum character count |
| `max_length=N` | `String` | `#[validate("max_length=255")]` | Maximum character count |
| `regex=...` | `String` | `#[validate("regex=^[A-Z]")]` | Regex pattern match |
| `min=N` | numeric | `#[validate("min=0")]` | Minimum value |
| `max=N` | numeric | `#[validate("max=100")]` | Maximum value |

An enumerated-values rule (`one_of`) exists in the platform's manifest format (`{"rule": "one_of", "values": ["draft", "published"]}` in a column's `validations` array of a JSON manifest) but is **not** supported by the `#[validate]` derive. To constrain a `String` to a fixed set from Rust, use a regex such as `#[validate("regex=^(draft|published)$")]`, or enforce it in a `before_create` / `before_update` hook (Guide 14b).

### What happens on invalid data

```bash
curl -X POST http://localhost:8080/svc/acme/validated-service/users \
  -H 'Content-Type: application/json' \
  -d '{"username": "ab", "email": "not-an-email", "display_name": "Alice", "age": 200}'
```

Response (422):
```json
{"error": "Field 'username' must be at least 3 characters"}
```

Validations are checked on both CREATE and UPDATE operations.

### Other column attributes

| Attribute | Description |
|-----------|-------------|
| `#[unique]` | SQL `UNIQUE` constraint |
| `#[default("value")]` | Default value in SQL |
| `#[index]` | Create an index on this column |
| `#[restricted(read = "op", write = "op")]` | Read and written through the data routes only with a grant ([column permissions](column-permissions.md)) |
| `Option<T>` | Makes the column nullable |

---

## Guide 4: Soft Deletes & Filtering

Enable soft deletes so records are hidden but never actually removed from the database.

```rust
use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "documents", soft_delete)]
pub struct Document {
    #[key]
    pub id: Uuid,
    pub title: String,
    pub content: String,
    pub summary: Option<String>,
    pub category: String,
    pub priority: i32,
    pub archived: bool,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

cufflink::service! {
    name: "doc-service",
    tables: [Document],
}
```

### How soft delete works

Adding `soft_delete` to `#[table(...)]`:
- Adds a `deleted_at TIMESTAMPTZ` column to the table
- DELETE requests set `deleted_at = now()` instead of removing the row
- LIST and GET requests automatically exclude rows where `deleted_at IS NOT NULL`

### Querying deleted records

```bash
# Normal list — deleted records are hidden
curl http://localhost:8080/svc/acme/doc-service/documents

# Include deleted records
curl "http://localhost:8080/svc/acme/doc-service/documents?include_deleted=true"
```

### Column-specific filtering

```bash
# Filter by category
curl "http://localhost:8080/svc/acme/doc-service/documents?category=reports"

# Filter by priority
curl "http://localhost:8080/svc/acme/doc-service/documents?priority=1"

# Filter by boolean
curl "http://localhost:8080/svc/acme/doc-service/documents?archived=false"
```

### Advanced filtering with JSON

The `filter` parameter takes a JSON **array of objects**, each with `n` (column), `f` (operator) and `v` (value). Any other shape — including an array of arrays — is silently ignored and returns the unfiltered list.

```bash
# Complex filter: priority >= 3 AND category = "reports"
curl "http://localhost:8080/svc/acme/doc-service/documents?filter=\
[{\"n\":\"priority\",\"f\":\">=\",\"v\":\"3\"},{\"n\":\"category\",\"f\":\"=\",\"v\":\"reports\"}]"

# Pattern matching
curl "http://localhost:8080/svc/acme/doc-service/documents?filter=\
[{\"n\":\"title\",\"f\":\"LIKE\",\"v\":\"%quarterly%\"}]"

# NULL checks (no value needed)
curl "http://localhost:8080/svc/acme/doc-service/documents?filter=\
[{\"n\":\"summary\",\"f\":\"IS NULL\"}]"
```

How filters combine:

- Filters on **different** columns are AND'd together.
- Several filters on the **same** column are OR'd — `[{"n":"category","f":"=","v":"reports"},{"n":"category","f":"=","v":"memos"}]` means `category = 'reports' OR category = 'memos'`. This also means a range on one column (`priority >= 3 AND priority <= 5`) cannot be expressed with `?filter=`; use two different columns, or a WASM handler (Guide 18).
- Operators are matched case-insensitively; `eq`, `neq`, `gt`, `lt`, `gte`, `lte` are accepted as aliases for the symbols. See Guide 20 for the full operator list.
- Only declared columns can be filtered. `deleted_at` is managed by the platform and is not a declared column, so it cannot be filtered on — use `?include_deleted=true` to see soft-deleted rows.

### Sorting

```bash
# Sort by priority descending, then title ascending
curl "http://localhost:8080/svc/acme/doc-service/documents?sort=-priority,title"

# Sort by creation date descending (default)
curl "http://localhost:8080/svc/acme/doc-service/documents?sort=-created_at"
```

---

## Guide 5: Full-Text Search & Pagination

Use PostgreSQL's full-text search for powerful querying, plus different pagination strategies.

### Setup

```rust
use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "articles")]
pub struct Article {
    #[key]
    pub id: Uuid,
    pub title: String,
    pub body: String,
    pub summary: Option<String>,
    pub author: String,
    pub tags: Option<String>,
    pub published: bool,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

cufflink::service! {
    name: "articles-service",
    tables: [Article],
}
```

### Full-text search

Full-text search builds a `tsvector` from all text columns and matches against a `tsquery`:

```bash
# Search for articles about "rust programming"
curl "http://localhost:8080/svc/acme/articles-service/articles?fts=rust+programming"
```

This generates SQL like:
```sql
WHERE to_tsvector('english', coalesce(title,'') || ' ' || coalesce(body,'') || ' ' || ...)
  @@ plainto_tsquery('english', 'rust programming')
```

### Simple substring search

For basic case-insensitive substring matching across all text columns:

```bash
curl "http://localhost:8080/svc/acme/articles-service/articles?search=kubernetes"
```

### Offset-based pagination (default)

```bash
# Page 1 with 10 items
curl "http://localhost:8080/svc/acme/articles-service/articles?page=1&per_page=10"
```

Response:
```json
{
  "results": [...],
  "pagination": {
    "current_page": 1,
    "per_page": 10,
    "total_records": 47,
    "total_pages": 5
  }
}
```

### Cursor-based pagination

For large datasets or real-time feeds, cursor-based pagination is more efficient. Cursor mode is selected by the presence of the `cursor` parameter — pass it empty to start from the first row:

```bash
# First page
curl "http://localhost:8080/svc/acme/articles-service/articles?cursor=&limit=10"

# Response includes next_cursor:
# { "results": [...], "next_cursor": "NTUwZTg0MDA...", "has_more": true }

# Next page
curl "http://localhost:8080/svc/acme/articles-service/articles?cursor=NTUwZTg0MDA...&limit=10"
```

Cursor mode always orders by primary key ascending (`sort` is ignored) and returns `next_cursor` / `has_more` instead of the `pagination` object. A request without `cursor` uses offset mode and never returns `next_cursor`.

### Column selection

Only return the columns you need:

```bash
curl "http://localhost:8080/svc/acme/articles-service/articles?select=id,title,author"
```

### Combining everything

```bash
# Full-text search for "kubernetes", sorted by date, page 2, only specific columns
curl "http://localhost:8080/svc/acme/articles-service/articles?\
fts=kubernetes&sort=-created_at&page=2&per_page=20&select=id,title,author,created_at"
```

---

## Guide 6: Real-Time Streaming with SSE

Subscribe to live CRUD events using Server-Sent Events.

### Connect to the stream

```bash
curl -N http://localhost:8080/svc/acme/my-todos/todos/stream
```

### Event format

```
event: connected
data: {"service":"my-todos","table":"todos","tenant":"acme"}

event: created
data: {"action":"created","table":"todos","data":{"id":"...","title":"New todo",...}}

event: updated
data: {"action":"updated","table":"todos","data":{"id":"...","completed":true,...}}

event: deleted
data: {"action":"deleted","table":"todos","data":{"id":"...","title":"Old todo",...}}

:ping
```

### JavaScript client example

```javascript
const source = new EventSource(
  'http://localhost:8080/svc/acme/my-todos/todos/stream'
);

source.addEventListener('created', (event) => {
  const data = JSON.parse(event.data);
  console.log('New todo created:', data.data.title);
});

source.addEventListener('updated', (event) => {
  const data = JSON.parse(event.data);
  console.log('Todo updated:', data.data.id);
});

source.addEventListener('deleted', (event) => {
  const data = JSON.parse(event.data);
  console.log('Todo deleted:', data.data.id);
});
```

### How it works

1. The client connects to the SSE endpoint
2. The platform subscribes to the NATS subject `dw.{tenant}.{service}.{table}.>`
3. Any CRUD operation on that table publishes an event to NATS
4. The SSE handler forwards NATS messages to the connected client
5. A keep-alive ping is sent every 15 seconds

---

## Guide 7: Webhooks for External Integrations

Register HTTP webhooks to be notified when records are created, updated, or deleted.

### Register a webhook

```bash
curl -X POST http://localhost:8080/api/services/SERVICE_ID/webhooks \
  -H 'Content-Type: application/json' \
  -H 'Authorization: ApiKey ck_...' \
  -d '{
    "table_name": "todos",
    "event": "create",
    "url": "https://your-app.com/webhook",
    "secret": "whsec_your_secret_key"
  }'
```

Events: `create`, `update`, `delete`

Managing webhooks needs deploy authority (a tenant API key or the deploy role)
or the `webhooks:manage` permission. A URL that is not `http(s)`, does not
resolve, or points at a cloud metadata endpoint is refused at registration;
deliveries to private or internal addresses follow the tenant's `egress`
policy (see [policies](policies.md)).

### Webhook delivery

When a matching CRUD event occurs, the platform sends an HTTP POST to your URL:

```http
POST /webhook HTTP/1.1
Content-Type: application/json
X-Cufflink-Signature: sha256=abc123...
X-Cufflink-Signature-256: t=1700000000,v1=def456...
X-Cufflink-Event: create
X-Cufflink-Delivery: 5f0c3a4e-...

{
  "action": "created",
  "service": "my-todos",
  "table": "todos",
  "data": {
    "id": "...",
    "title": "New todo",
    "completed": false,
    ...
  },
  "tenant_id": "..."
}
```

`X-Cufflink-Event` carries the registration event name (`create`, `update` or `delete`); the table name is in the body's `table` field, and the body's `action` uses the past-tense form (`created`, `updated`, `deleted`).

### Verify the signature

`X-Cufflink-Signature-256` binds the delivery time: `t` is the Unix time the
request was signed and `v1` the hex HMAC-SHA256, with your secret, of `"{t}."`
followed by the raw body. Reject a `t` too far from now to refuse replays, and
use `X-Cufflink-Delivery` (the same for every retry of a delivery) to drop
duplicates.

```python
import hmac, hashlib, time

def verify(body: bytes, secret: str, header: str, tolerance: int = 300) -> bool:
    parts = dict(item.split("=", 1) for item in header.split(","))
    t = int(parts["t"])
    expected = hmac.new(secret.encode(), f"{t}.".encode() + body, hashlib.sha256).hexdigest()
    return abs(time.time() - t) <= tolerance and hmac.compare_digest(expected, parts["v1"])
```

The older `X-Cufflink-Signature` header, an HMAC-SHA256 of the body alone, is
still sent:

```python
import hmac
import hashlib

def verify_signature(body: bytes, secret: str, signature: str) -> bool:
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature)
```

```javascript
const crypto = require('crypto');

function verifySignature(body, secret, signature) {
  const expected = crypto
    .createHmac('sha256', secret)
    .update(body)
    .digest('hex');
  return signature === `sha256=${expected}`;
}
```

### Retry behavior

- 3 delivery attempts in total, with a 1s pause after the first failure and 2s after the second
- 10-second timeout per request; redirects are not followed; at most 64 KiB of the reply is read
- A target refused by the egress guard or policy is not retried (`cufflink_webhook_dispatch_denied_total{reason}`); a name that does not resolve is retried like any other failed attempt
- Failed deliveries are logged with metrics

### Manage webhooks

```bash
# List webhooks
curl -H 'Authorization: ApiKey ck_...' \
  http://localhost:8080/api/services/SERVICE_ID/webhooks

# Pause a webhook
curl -X PUT http://localhost:8080/api/services/SERVICE_ID/webhooks/WEBHOOK_ID \
  -H 'Content-Type: application/json' \
  -H 'Authorization: ApiKey ck_...' \
  -d '{"active": false}'

# Resume a webhook
curl -X PUT http://localhost:8080/api/services/SERVICE_ID/webhooks/WEBHOOK_ID \
  -H 'Content-Type: application/json' \
  -H 'Authorization: ApiKey ck_...' \
  -d '{"active": true}'

# Delete a webhook
curl -X DELETE -H 'Authorization: ApiKey ck_...' \
  http://localhost:8080/api/services/SERVICE_ID/webhooks/WEBHOOK_ID
```

---

## Guide 8: Row-Level Security

Scope records to the authenticated user so each user only sees their own data.

### Define a table with an owner field

The `owner_field` names the column that stores the owning user's ID. Set it either via the Rust `#[table]` attribute or in a JSON manifest deploy — the platform handles the rest.

**Rust SDK:**
```rust
#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "notes", auth_required, owner_field = "user_id")]
pub struct Note {
    #[key] pub id: Uuid,
    pub user_id: String,
    pub title: String,
    pub body: String,
    #[timestamp] pub created_at: DateTime<Utc>,
    #[timestamp] pub updated_at: DateTime<Utc>,
}
```

**Manifest approach (JSON deploy):**
```json
{
  "name": "notes-service",
  "tables": [{
    "name": "notes",
    "owner_field": "user_id",
    "columns": [
      {"name": "id", "column_type": "uuid", "primary_key": true, "auto_generate": true},
      {"name": "user_id", "column_type": "text"},
      {"name": "title", "column_type": "text"},
      {"name": "body", "column_type": "text"},
      {"name": "created_at", "column_type": "timestamp", "auto_generate": true},
      {"name": "updated_at", "column_type": "timestamp", "auto_generate": true}
    ]
  }]
}
```

### How RLS works

When a table has `owner_field` set:

| Operation | Behavior |
|-----------|----------|
| **CREATE** | Automatically sets the owner field to the authenticated user's subject ID |
| **LIST** | Only returns records where `owner_field = user.subject` |
| **GET** | Returns 404 if the record belongs to another user |
| **UPDATE** | Returns 404 if the record belongs to another user |
| **DELETE** | Returns 404 if the record belongs to another user |

### Example

```bash
# Alice creates a note (her subject ID is auto-set as user_id)
curl -X POST http://localhost:8080/svc/acme/notes-service/notes \
  -H 'Authorization: Bearer alice_jwt_token' \
  -H 'Content-Type: application/json' \
  -d '{"title": "Alice private note", "body": "Secret stuff"}'

# Alice lists notes — sees only her notes
curl -H 'Authorization: Bearer alice_jwt_token' \
  http://localhost:8080/svc/acme/notes-service/notes
# => {"results":[{"title":"Alice private note", ...}],"pagination":{"total_records":1, ...}}

# Bob lists notes — sees only his notes (empty)
curl -H 'Authorization: Bearer bob_jwt_token' \
  http://localhost:8080/svc/acme/notes-service/notes
# => {"results":[],"pagination":{"total_records":0, ...}}

# Bob tries to read Alice's note — 404
curl -H 'Authorization: Bearer bob_jwt_token' \
  http://localhost:8080/svc/acme/notes-service/notes/ALICE_NOTE_ID
# => {"error": "notes not found"}
```

### Without authentication

If no valid auth header is provided and the table has `owner_field`, every operation on it (list, get, create, update, delete) is rejected with `401 {"error": "Authentication required for this resource"}`. In this guide the table also declares `auth_required`, which rejects anonymous requests for the same reason.

---

## Guide 9: File Storage

Upload and manage files attached to a service.

### Upload a file

```bash
curl -X POST http://localhost:8080/api/services/SERVICE_ID/files \
  -H 'Authorization: ApiKey ck_...' \
  -F 'file=@document.pdf'
```

Response:
```json
{
  "files": [{
    "id": "abc123-...",
    "filename": "document.pdf",
    "content_type": "application/pdf",
    "size_bytes": 102400
  }]
}
```

### List files

```bash
curl -H 'Authorization: ApiKey ck_...' \
  http://localhost:8080/api/services/SERVICE_ID/files
```

### Download a file

```bash
curl -o document.pdf \
  -H 'Authorization: ApiKey ck_...' \
  http://localhost:8080/api/services/SERVICE_ID/files/FILE_ID
```

### Delete a file

```bash
curl -X DELETE -H 'Authorization: ApiKey ck_...' \
  http://localhost:8080/api/services/SERVICE_ID/files/FILE_ID
```

### Storage

The supported backend is S3-compatible object storage, configured on the platform with `STORAGE_BACKEND=s3` plus `S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_BUCKET` (the compose stack runs RustFS and creates the `cufflink` bucket for you). Files are stored under the key `files/<service_id>/<file_id>.<ext>`.

If no object storage is configured, uploads fall back to the local filesystem at `$CUFFLINK_STORAGE_DIR/<service_id>/<file_id>.<ext>` (default `./data/files/`). This fallback is for quick local experiments only: the stored path is not the same as the upload path, so download and delete may not find the file, and nothing else on the platform (WASM modules, backups) works without object storage.

---

## Guide 10: Configuration & Secrets

Manage environment-specific configuration for your services.

### Set a config value

```bash
# Plain value
cufflink config set DATABASE_HOST production-db.example.com

# Secret value (masked in API responses)
cufflink config set STRIPE_KEY sk_live_abc123 --secret

# Or via API
curl -X PUT http://localhost:8080/api/services/SERVICE_ID/config \
  -H 'Content-Type: application/json' \
  -H 'Authorization: ApiKey ck_...' \
  -d '{"key": "FEATURE_FLAGS", "value": "{\"beta\": true}", "is_secret": false}'
```

### List config

```bash
cufflink config list

# Output:
# KEY                VALUE               SECRET
# DATABASE_HOST      production-db...    no
# STRIPE_KEY         ***                 yes
# FEATURE_FLAGS      {"beta": true}      no
```

### Delete a config key

```bash
cufflink config delete FEATURE_FLAGS
```

---

## Guide 11: Data Seeding & Backups

### Seed data from a JSON file

Create a `seed.json`. Seeding is an **upsert** keyed on the primary key: a row whose `id` already exists is updated in place, and a row without an `id` gets a fresh UUID every time the seed runs. Give rows explicit ids whenever another row references them (the `posts.author_id` below), and whenever you want re-running the seed to be idempotent:

```json
{
  "tables": {
    "authors": [
      {"id": "11111111-1111-4111-8111-111111111111", "name": "Alice", "email": "alice@example.com"},
      {"id": "22222222-2222-4222-8222-222222222222", "name": "Bob", "email": "bob@example.com"}
    ],
    "posts": [
      {
        "id": "33333333-3333-4333-8333-333333333333",
        "title": "Getting Started with Cufflink",
        "body": "Cufflink makes it easy to...",
        "author_id": "11111111-1111-4111-8111-111111111111",
        "published": true
      }
    ]
  }
}
```

Tables are inserted in foreign-key dependency order (referenced tables first), so the order of keys in the file does not matter.

```bash
# Seed data (upsert into existing rows)
cufflink seed seed.json

# Seed data (clear existing rows first)
cufflink seed seed.json --clear

# Seed only specific tables
cufflink seed seed.json --clear --tables authors

# Seed multiple specific tables
cufflink seed seed.json --clear --tables authors,posts
```

Via API:

```bash
curl -X POST http://localhost:8080/api/services/SERVICE_ID/seed \
  -H 'Content-Type: application/json' \
  -H 'Authorization: ApiKey ck_...' \
  -d @seed.json
```

Response:
```json
{
  "seeded": true,
  "total_rows": 3,
  "tables": {"authors": 2, "posts": 1}
}
```

### Export a backup

Backups are asynchronous jobs. The platform enqueues the job, the worker streams every table to object storage as NDJSON (one JSON row per line), and the CLI polls until the job completes and then downloads the file. Object storage must be configured on the platform and worker (see Guide 9) — without it the export request fails with `S3 storage not configured`.

```bash
# Default output is backup.jsonl in the current directory
cufflink backup export

# Choose the file name, or export only some tables
cufflink backup export --output backup-2026-02-19.jsonl --tables authors,posts

# Return the job id immediately instead of waiting
cufflink backup export --async
# => Export job started: <job-id>
#    Check status: cufflink backup jobs
```

The `.jsonl` file contains one row per line; the worker stores it at `backups/<service_id>/<job_id>/export.jsonl` in the bucket, and `cufflink backup list` shows completed exports that can be restored directly.

### Restore from backup

```bash
# Restore from a local file (existing rows are kept — conflicting ids are skipped)
cufflink backup restore backup-2026-02-19.jsonl

# Restore (clear existing data first)
cufflink backup restore backup-2026-02-19.jsonl --clear

# Restore straight from a previous export job without downloading it
cufflink backup restore --from-backup <job-id>
```

Restore is also a job: the CLI uploads the file to object storage, the worker inserts rows with `ON CONFLICT DO NOTHING`, and `cufflink backup jobs` lists every export/restore job with its status.

---

## Guide 12: GraphQL Queries

Every deployed service automatically gets a GraphQL endpoint.

### Endpoint

```
POST /svc/{tenant}/{service}/graphql
```

### Query examples

```bash
# List all todos
curl -X POST http://localhost:8080/svc/acme/my-todos/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query": "{ todos { id title completed created_at } }"}'
```

Response:
```json
{
  "data": {
    "todos": [
      {"id": "...", "title": "Buy groceries", "completed": false, "created_at": "..."},
      {"id": "...", "title": "Write docs", "completed": true, "created_at": "..."}
    ]
  }
}
```

```bash
# Query with filtering
curl -X POST http://localhost:8080/svc/acme/my-todos/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query": "{ todos(completed: false) { id title } }"}'
```

### Get the GraphQL schema

```bash
curl http://localhost:8080/svc/acme/my-todos/graphql/schema
```

Returns the auto-generated GraphQL schema with all types and queries.

---

## Guide 13: Batch Operations & Transactions

Perform multiple create, update, and delete operations in a single atomic transaction.

### Batch request

```bash
curl -X POST http://localhost:8080/svc/acme/my-todos/todos/batch \
  -H 'Content-Type: application/json' \
  -d '{
    "operations": [
      {
        "action": "create",
        "data": {"title": "Task 1", "completed": false}
      },
      {
        "action": "create",
        "data": {"title": "Task 2", "completed": false}
      },
      {
        "action": "update",
        "id": "EXISTING_TODO_ID",
        "data": {"completed": true}
      },
      {
        "action": "delete",
        "id": "OLD_TODO_ID"
      }
    ]
  }'
```

Response:
```json
{
  "results": [
    {"id": "...", "title": "Task 1", ...},
    {"id": "...", "title": "Task 2", ...},
    {"id": "EXISTING_TODO_ID", "completed": true, ...}
  ],
  "summary": {
    "created": 2,
    "updated": 1,
    "deleted": 1,
    "total": 4
  }
}
```

### Atomicity

All operations in a batch run inside a PostgreSQL transaction. If any operation fails, the entire batch is rolled back — no partial updates.

---

## Guide 14: RBAC & Permissions

Set up roles and permissions for tenant-level access control.

### Permission model

Permissions use structured `"area:operation"` strings (e.g. `"staff:create"`, `"items:view"`). Wildcards are supported:
- `"staff:*"` — all operations in the "staff" area
- `"*"` — super-admin wildcard, grants everything

### Declaring permissions in your service

Use `permission_area` on tables to enable automatic CRUD permission enforcement, and the `authorization` block in `service!` to declare permission areas and seed default roles on deploy:

```rust
use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "staff", auth_required, permission_area = "staff")]
pub struct Staff {
    #[key] pub id: Uuid,
    pub name: String,
    pub email: String,
    #[timestamp] pub created_at: DateTime<Utc>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "items", auth_required, permission_area = "items")]
pub struct Item {
    #[key] pub id: Uuid,
    pub title: String,
    #[timestamp] pub created_at: DateTime<Utc>,
}

cufflink::service! {
    name: "my-service",
    mode: wasm,
    tables: [Staff, Item],
    custom_routes: [
        // Handlers are only reachable when listed here. The posture modifier
        // (auth / perm / owner / public) is enforced by the platform before
        // the handler runs; see Guide 18.
        ("POST", "/invite-staff", "invite_staff", perm("staff", "create")),
    ],
    authorization: [
        areas: [
            ("staff", ["create", "view", "edit", "delete"]),
            ("items", ["create", "view", "edit", "delete"]),
        ],
        default_roles: [
            ("admin", "Full access", ["staff:*", "items:*"]),
            ("viewer", "Read-only", ["staff:view", "items:view"]),
        ],
    ],
}
```

When you run `cufflink deploy`:
1. The `default_roles` are upserted into the `roles` table (idempotent — permissions are updated on redeploy)
2. The deploying user is auto-assigned to the "admin" role if they don't already have it
3. CRUD endpoints automatically enforce permissions based on `permission_area`:
   - GET (list/get) requires `"area:view"`
   - POST (create) requires `"area:create"`
   - PUT (update) requires `"area:edit"`
   - DELETE requires `"area:delete"`

### Checking permissions in WASM handlers

The `perm("staff", "create")` posture on the route above already rejects callers without that permission. Inside a handler you can make finer-grained decisions with `auth.can()` (prefer it over `auth.has_role()`):

```rust
use cufflink_fn::handler;
use cufflink_fn::prelude::*;

cufflink_fn::init!();

// Reachable at POST /svc/acme/my-service/_fn/invite_staff
handler!(invite_staff, |req: Request| {
    let auth = match req.require_auth() {
        Ok(auth) => auth,
        Err(resp) => return resp,
    };

    if !auth.can("staff", "create") {
        return Response::forbidden("missing staff:create permission");
    }

    // ... create staff member
    Response::json(&json!({"ok": true}))
});
```

### Managing roles via API

Create roles manually (in addition to or instead of `default_roles`):

```bash
# Create a custom role
curl -X POST http://localhost:8080/api/tenants/TENANT_ID/roles \
  -H 'Content-Type: application/json' \
  -H 'Authorization: ApiKey ck_...' \
  -d '{
    "name": "manager",
    "description": "Operations manager",
    "permissions": ["staff:view", "items:create", "items:view", "items:edit"]
  }'
```

### Assign roles to users

```bash
curl -X POST http://localhost:8080/api/tenants/TENANT_ID/roles/assign \
  -H 'Content-Type: application/json' \
  -H 'Authorization: ApiKey ck_...' \
  -d '{"user_id": "alice-keycloak-subject-id", "role_id": "ROLE_ID"}'
```

### Check a user's effective permissions

```bash
curl -H 'Authorization: ApiKey ck_...' \
  http://localhost:8080/api/tenants/TENANT_ID/users/alice-keycloak-subject-id/permissions
```

Response:
```json
{
  "user_id": "alice-keycloak-subject-id",
  "roles": ["admin"],
  "permissions": ["items:*", "staff:*"]
}
```

### Unassign a role

```bash
curl -X DELETE http://localhost:8080/api/tenants/TENANT_ID/roles/unassign \
  -H 'Content-Type: application/json' \
  -H 'Authorization: ApiKey ck_...' \
  -d '{"user_id": "alice-keycloak-subject-id", "role_id": "ROLE_ID"}'
```

### List roles

```bash
curl -H 'Authorization: ApiKey ck_...' \
  http://localhost:8080/api/tenants/TENANT_ID/roles
```

### Delete a role

```bash
curl -X DELETE -H 'Authorization: ApiKey ck_...' \
  http://localhost:8080/api/tenants/TENANT_ID/roles/ROLE_ID
```

---

## Guide 14b: CRUD Lifecycle Hooks

Run custom WASM functions before or after CRUD operations for validation, side effects, audit logging, and more.

### Declaring hooks

Add `#[hooks(...)]` to your table struct. Each hook names a WASM handler function:

```rust
#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "items", auth_required, permission_area = "items")]
#[hooks(
    before_create = "validate_item",
    after_create = "audit_item_created",
    before_update = "validate_item_update",
    after_update = "audit_item_updated",
    after_delete = "audit_item_deleted"
)]
pub struct Item {
    #[key] pub id: Uuid,
    pub title: String,
    pub status: String,
    #[timestamp] pub created_at: DateTime<Utc>,
}
```

### Available hooks

| Hook | Timing | Receives | Can |
|------|--------|----------|-----|
| `before_create` | Before INSERT | `{ data, auth }` | Modify data, reject with error |
| `after_create` | After INSERT | `{ data, result, auth }` | Side effects (audit, notify) |
| `before_update` | Before UPDATE | `{ id, data, existing, auth }` | Modify data, reject with error |
| `after_update` | After UPDATE | `{ id, data, result, auth }` | Side effects |
| `before_delete` | Before DELETE | `{ id, existing, auth }` | Reject with error |
| `after_delete` | After DELETE | `{ id, auth }` | Side effects |

### Writing hook handlers

Hook handlers are regular WASM handler functions defined with the `handler!` macro:

```rust
// Validate before creating an item
handler!(validate_item, |req: Request| {
    let body = req.body();

    let title = body["data"]["title"].as_str().unwrap_or("");
    if title.len() < 3 {
        return Response::error("Title must be at least 3 characters");
    }

    // Return the (optionally modified) data to proceed
    Response::json(body)
});

// Audit log after creation
handler!(audit_item_created, |req: Request| {
    let result = &req.body()["result"];
    let auth = req.auth();
    let user = auth.map(|a| a.sub.as_str()).unwrap_or("anonymous");

    db::execute(&format!(
        "INSERT INTO audit_log (action, entity_id, user_id) VALUES ('item_created', '{}', '{}')",
        result["id"].as_str().unwrap_or(""),
        user
    ));

    Response::empty()
});
```

### How hooks work

- **Before hooks** run synchronously — if they return an error, the operation is aborted and the error is returned to the client
- **Before hooks** can modify the data by returning a modified JSON payload
- **After hooks** run after the database operation completes
- Hook handlers use the same `handler!` macro as custom routes and subscriptions
- All hooks receive the authenticated user context in `auth`

---

## Guide 15: The Full-Stack Application

This guide combines everything into a complete project management application with all Cufflink features.

### Define the service

```rust
use cufflink::prelude::*;

// ─── Users ───────────────────────────────────────────────

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "users")]
pub struct User {
    #[key]
    pub id: Uuid,

    #[unique]
    #[validate("min_length=3", "max_length=30")]
    pub username: String,

    #[unique]
    #[validate("regex=^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")]
    pub email: String,

    #[validate("max_length=100")]
    pub display_name: String,

    pub avatar_url: Option<String>,

    #[validate("regex=^(active|suspended|deactivated)$")]
    #[default("active")]
    pub status: String,

    pub metadata: Option<serde_json::Value>,

    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

// ─── Projects ────────────────────────────────────────────

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "projects", soft_delete)]
pub struct Project {
    #[key]
    pub id: Uuid,

    #[validate("min_length=1", "max_length=100")]
    pub name: String,

    pub description: Option<String>,

    #[references("users.id")]
    #[on_delete("set_null")]
    pub owner_id: Option<Uuid>,

    #[validate("regex=^(planning|active|paused|completed|archived)$")]
    #[default("planning")]
    pub status: String,

    #[validate("min=0")]
    pub budget_cents: Option<i64>,

    pub start_date: Option<chrono::NaiveDate>,
    pub due_date: Option<chrono::NaiveDate>,

    pub settings: Option<serde_json::Value>,

    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

// ─── Tasks ───────────────────────────────────────────────

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "tasks", soft_delete)]
pub struct Task {
    #[key]
    pub id: Uuid,

    #[references("projects.id")]
    #[on_delete("cascade")]
    pub project_id: Uuid,

    #[validate("min_length=1", "max_length=255")]
    pub title: String,

    pub description: Option<String>,

    #[references("users.id")]
    #[on_delete("set_null")]
    pub assignee_id: Option<Uuid>,

    #[validate("regex=^(todo|in_progress|review|done)$")]
    #[default("todo")]
    pub status: String,

    #[validate("regex=^(low|medium|high|critical)$")]
    #[default("medium")]
    pub priority: String,

    #[validate("min=0", "max=100")]
    pub story_points: Option<i32>,

    pub due_date: Option<chrono::NaiveDate>,

    pub labels: Option<serde_json::Value>,

    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

// ─── Comments ────────────────────────────────────────────

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "comments")]
pub struct Comment {
    #[key]
    pub id: Uuid,

    #[references("tasks.id")]
    #[on_delete("cascade")]
    pub task_id: Uuid,

    #[references("users.id")]
    #[on_delete("set_null")]
    pub author_id: Option<Uuid>,

    #[validate("min_length=1", "max_length=5000")]
    pub body: String,

    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

// ─── Time Entries ────────────────────────────────────────

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "time_entries")]
pub struct TimeEntry {
    #[key]
    pub id: Uuid,

    #[references("tasks.id")]
    #[on_delete("cascade")]
    pub task_id: Uuid,

    #[references("users.id")]
    #[on_delete("cascade")]
    pub user_id: Uuid,

    #[validate("min=0.25")]
    pub hours: f64,

    pub description: Option<String>,

    pub logged_date: chrono::NaiveDate,

    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

cufflink::service! {
    name: "project-manager",
    tables: [User, Project, Task, Comment, TimeEntry],
}
```

### Deploy and seed

```bash
cufflink deploy
```

Create `seed.json`:

```json
{
  "tables": {
    "users": [
      {
        "username": "alice",
        "email": "alice@example.com",
        "display_name": "Alice Johnson",
        "status": "active"
      },
      {
        "username": "bob",
        "email": "bob@example.com",
        "display_name": "Bob Smith",
        "status": "active"
      }
    ]
  }
}
```

```bash
cufflink seed seed.json
```

### Set up webhooks for Slack notifications

```bash
# Notify Slack when tasks are created
curl -X POST http://localhost:8080/api/services/SERVICE_ID/webhooks \
  -H 'Content-Type: application/json' \
  -H 'Authorization: ApiKey ck_...' \
  -d '{
    "table_name": "tasks",
    "event": "create",
    "url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
    "secret": "whsec_slack_integration"
  }'

# Notify when tasks are completed
curl -X POST http://localhost:8080/api/services/SERVICE_ID/webhooks \
  -H 'Content-Type: application/json' \
  -H 'Authorization: ApiKey ck_...' \
  -d '{
    "table_name": "tasks",
    "event": "update",
    "url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
    "secret": "whsec_slack_integration"
  }'
```

### Store service configuration

```bash
cufflink config set SLACK_CHANNEL "#project-updates"
cufflink config set JIRA_TOKEN xoxb-your-token --secret
cufflink config set MAX_STORY_POINTS 21
```

### Set up RBAC

```bash
# Create project manager role
curl -X POST http://localhost:8080/api/tenants/TENANT_ID/roles \
  -H 'Content-Type: application/json' \
  -H 'Authorization: ApiKey ck_...' \
  -d '{
    "name": "project-manager",
    "permissions": ["project:read", "project:write", "task:read", "task:write", "task:assign"]
  }'

# Create developer role
curl -X POST http://localhost:8080/api/tenants/TENANT_ID/roles \
  -H 'Content-Type: application/json' \
  -H 'Authorization: ApiKey ck_...' \
  -d '{
    "name": "developer",
    "permissions": ["project:read", "task:read", "task:write", "time:write"]
  }'
```

### Real-time task board

```javascript
// Connect to live task updates for a project
const source = new EventSource(
  'http://localhost:8080/svc/acme/project-manager/tasks/stream'
);

source.addEventListener('created', (e) => {
  const { data } = JSON.parse(e.data);
  addTaskToBoard(data);
});

source.addEventListener('updated', (e) => {
  const { data } = JSON.parse(e.data);
  if (data.status === 'done') {
    moveToCompleted(data);
    showConfetti();
  } else {
    updateTaskCard(data);
  }
});

source.addEventListener('deleted', (e) => {
  const { data } = JSON.parse(e.data);
  removeFromBoard(data.id);
});
```

### Complex queries

```bash
# Tasks assigned to Alice, high priority, in progress, sorted by due date
curl "http://localhost:8080/svc/acme/project-manager/tasks?\
assignee_id=ALICE_UUID&priority=high&status=in_progress&sort=due_date"

# Full-text search for tasks mentioning "database migration"
curl "http://localhost:8080/svc/acme/project-manager/tasks?\
fts=database+migration&sort=-created_at"

# Overdue high-priority tasks with story points
# (two filters on the SAME column are OR'd, so a due-date range needs a
#  WASM handler — see Guide 4; here each column appears once, so they AND)
curl "http://localhost:8080/svc/acme/project-manager/tasks?\
filter=[{\"n\":\"due_date\",\"f\":\"<\",\"v\":\"2026-02-17\"},{\"n\":\"priority\",\"f\":\"IN\",\"v\":\"high,critical\"}]&\
select=id,title,status,priority,story_points,due_date"

# Batch update: move all "review" tasks to "done"
curl -X POST http://localhost:8080/svc/acme/project-manager/tasks/batch \
  -H 'Content-Type: application/json' \
  -d '{
    "operations": [
      {"action": "update", "id": "TASK_1_ID", "data": {"status": "done"}},
      {"action": "update", "id": "TASK_2_ID", "data": {"status": "done"}},
      {"action": "update", "id": "TASK_3_ID", "data": {"status": "done"}}
    ]
  }'
```

### Upload attachments

```bash
# Attach a file to the service
curl -X POST http://localhost:8080/api/services/SERVICE_ID/files \
  -H 'Authorization: ApiKey ck_...' \
  -F 'file=@design-mockup.png'
```

### GraphQL queries

```bash
# Query tasks with their comments
curl -X POST http://localhost:8080/svc/acme/project-manager/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query": "{ tasks(status: \"in_progress\") { id title priority assignee_id } }"}'
```

### Backup and restore

```bash
# Nightly backup (async job, NDJSON output — see Guide 11)
cufflink backup export --output "backup-$(date +%Y-%m-%d).jsonl"

# Restore to a different environment defined in Cufflink.toml
cufflink backup restore backup-2026-02-19.jsonl --env staging
```

Use `--env` to target another environment. `CUFFLINK_API_URL` is only consulted when there is no `Cufflink.toml` in the project — once the file exists, its `api_url` wins.

### Deploy a new version

When you add a new column or table:

```bash
# Schema changes are applied automatically
cufflink deploy

# If you need to drop a column (destructive change)
cufflink deploy --allow-destructive

# Something went wrong? Rollback
cufflink rollback
```

### OpenAPI spec for frontend team

```bash
# Generate and save the OpenAPI spec
cufflink openapi --output api-spec.json

# Generate a TypeScript client
cufflink generate-client --output ./frontend/src/api
```

### Inspect with the CLI

```bash
cufflink status        # current service: id, version, status, tenant, timestamps
cufflink services      # every deployed service in the tenant
cufflink logs --follow # stream the service's logs (filter with --level, --search, --since)
cufflink sql "SELECT status, COUNT(*) FROM tasks GROUP BY status"
cufflink deployments   # deployment history for rollback
```

---

## SDK Quick Reference

### Column Type Mapping

| Rust Type | SQL Type | JSON Type |
|-----------|----------|-----------|
| `Uuid` | `UUID` | `string` |
| `String` | `TEXT` | `string` |
| `bool` | `BOOLEAN` | `boolean` |
| `i32` | `INTEGER` | `number` |
| `i64` | `BIGINT` | `number` |
| `f32` | `REAL` | `number` |
| `f64` | `DOUBLE PRECISION` | `number` |
| `DateTime<Utc>` | `TIMESTAMPTZ` | `string` (ISO 8601) |
| `NaiveDate` | `DATE` | `string` (YYYY-MM-DD) |
| `serde_json::Value` | `JSONB` | `any` |
| `Option<T>` | nullable `T` | `T \| null` |

### Attribute Reference

| Attribute | Level | Description |
|-----------|-------|-------------|
| `#[table(name = "...")]` | struct | SQL table name (required) |
| `#[table(soft_delete)]` | struct | Enable soft deletes |
| `#[key]` | field | Primary key (auto UUID) |
| `#[timestamp]` | field | Auto-managed timestamp |
| `#[unique]` | field | UNIQUE constraint |
| `#[index]` | field | Create index |
| `#[default("val")]` | field | SQL DEFAULT value |
| `#[references("table.col")]` | field | Foreign key |
| `#[on_delete("action")]` | field | FK delete action |
| `#[validate("min_length=3", "regex=...")]` | field | Validation rules (string literals; see Guide 3) |
| `#[hooks(before_create = "fn", ...)]` | struct | CRUD lifecycle hooks (Guide 14b) |

### Available Templates

`cufflink init <name> [--template <template>] [--mode crud|wasm]`; run `cufflink templates` to list them.

CRUD mode (the default):

| Template | Tables | Features |
|----------|--------|----------|
| `default` | one item table named after the project | Minimal starting point |
| `todo` | categories, todos | Soft delete, FK cascade |
| `blog` | authors, posts, comments | Soft delete, FK cascade |
| `ecommerce` | customers, products, orders, order_items | Soft delete, FK cascade, validation |
| `cms` | pages, media, navigation | Soft delete, optional fields |
| `hooks` | items | CRUD service with before/after lifecycle hooks in `src/lib.rs` |

WASM mode (`--mode wasm`):

| Template | Tables | Features |
|----------|--------|----------|
| `default` | one item table named after the project | A `hello` custom route with a `public(...)` posture |
| `ecommerce` | customers, products, orders, order_items | Checkout and order-summary handlers |

---

## Guide 16: WASM Mode — Custom Business Logic

Move beyond auto-generated CRUD by writing custom handlers in Rust that compile to WebAssembly. This lets you implement business rules, computed fields, workflows, and any logic that plain CRUD can't express — all running inside a sandboxed WASM runtime.

### The `cufflink-fn` crate

The `cufflink-fn` crate provides a clean Rust SDK for writing WASM handlers. You write normal Rust code with access to database queries, NATS messaging, and structured logging — no raw pointer manipulation required.

`cufflink init <name> --mode wasm` scaffolds a project with the right dependencies (`cufflink = "0.15"` for the manifest in `src/main.rs`, `cufflink-fn = "0.31"` for the handlers in `src/lib.rs`, plus a `[lib] crate-type = ["cdylib", "rlib"]` section) and the `wasm32-unknown-unknown` target is used by `cufflink deploy`.

```rust
use cufflink_fn::handler;
use cufflink_fn::prelude::*;

cufflink_fn::init!();

handler!(handle_checkout, |req: Request| {
    let body = req.body();
    let customer_id = body["customer_id"].as_str().unwrap();
    let items = body["items"].as_array().unwrap();

    let total: i64 = items.iter()
        .map(|i| i["qty"].as_i64().unwrap() * i["price"].as_i64().unwrap())
        .sum();

    db::execute(&format!(
        "INSERT INTO orders (customer_id, total_cents, status) VALUES ('{}', {}, 'pending')",
        customer_id, total
    ));

    nats::publish("orders.created", &json!({"total": total}).to_string());
    log::info(&format!("Order created for customer {}", customer_id));

    Response::json(&json!({ "total_cents": total, "status": "pending" }))
});
```

### When to use WASM mode vs CRUD mode

| Scenario | Mode |
|----------|------|
| Simple data storage and retrieval | CRUD |
| Need computed fields or derived values | WASM |
| Multi-step workflows (checkout, approval chains) | WASM |
| Cross-table transactions with business rules | WASM |
| Input transformation before storage | WASM |
| Conditional logic (pricing rules, eligibility) | WASM |

### Full guide

For complete documentation including project setup, architecture patterns (thin handlers / fat operations / db repos), four full real-world service examples, building & deploying, and testing strategies, see **[docs/wasm-guide.md](wasm-guide.md)**.

---

## Guide 17: Event Subscriptions & Reactive Patterns

Build event-driven architectures where changes in one table automatically trigger actions in another — without writing custom code.

### How subscriptions work

When a CRUD operation happens, the platform publishes a NATS event. Subscriptions listen for these events and execute handlers automatically:

```
CRUD Operation → NATS Event → JetStream → Subscription Handler → Side Effect
```

Events follow the subject pattern: `dw.<tenant>.<service>.<table>.<action>`

For example: `dw.acme.blog-service.posts.created`

### Subscription handler types

#### 1. DeleteCascade — Automatic cleanup

Automatically delete records in a related table when a parent record is removed. This is useful for cross-service relationships that can't use SQL foreign keys.

```json
{
  "name": "blog-service",
  "tables": [...],
  "subscriptions": [
    {
      "subject": "dw.{tenant}.blog-service.authors.deleted",
      "handler": "delete_cascade",
      "handler_type": "delete_cascade",
      "config": {
        "table": "drafts",
        "foreign_key": "author_id"
      }
    }
  ]
}
```

When an author is deleted, this automatically runs:
```sql
DELETE FROM drafts WHERE author_id = '<deleted_author_id>'
```

#### 2. UpdateField — Flags and derived fields on the emitting row

Update a column in response to an event. The handler runs

```sql
UPDATE <table> SET <set_column> = <set_value> WHERE <where_column> = <event.data.id>
```

— the `where_column` is **always compared against the `id` of the row that emitted the event**, never against another field of the payload. So `update_field` can only target rows whose `where_column` holds that same id: in practice the emitting row itself (`where_column: "id"`), or a row in another table that stores the emitting row's id in `where_column`.

```json
{
  "subscriptions": [
    {
      "subject": "dw.{tenant}.blog-service.posts.updated",
      "handler": "bump_revision",
      "handler_type": "update_field",
      "config": {
        "table": "posts",
        "where_column": "id",
        "set_column": "revision",
        "set_value": "revision + 1"
      }
    },
    {
      "subject": "dw.{tenant}.blog-service.posts.created",
      "handler": "init_post_stats",
      "handler_type": "update_field",
      "config": {
        "table": "post_stats",
        "where_column": "post_id",
        "set_column": "view_count",
        "set_value": "0"
      }
    }
  ]
}
```

The second example only affects a `post_stats` row whose `post_id` equals the new post's id, i.e. one that already exists — `update_field` never inserts. `set_value` is restricted to simple arithmetic expressions and is validated at deploy time.

A comment counter on `posts` ("when a comment is created, increment the parent post's `comment_count`") is **not** expressible with `update_field`, because the event's `data.id` is the comment's id, not the post's. Use a WASM subscription handler for cross-table updates like that (see Guide 16 / [docs/wasm-guide.md](wasm-guide.md)).

#### 3. Webhook — Forward events to external services

Send CRUD events to an external HTTP endpoint as they happen.

```json
{
  "subscriptions": [
    {
      "subject": "dw.{tenant}.blog-service.posts.created",
      "handler": "notify_slack",
      "handler_type": "webhook",
      "config": {
        "url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
        "method": "POST"
      }
    }
  ]
}
```

The entire event payload is forwarded as the request body.

### Event payload format

Every CRUD event published to NATS has this structure:

```json
{
  "action": "created",
  "service": "blog-service",
  "table": "posts",
  "data": {
    "id": "550e8400-...",
    "title": "My Post",
    "body": "Content here...",
    "author_id": "uuid-...",
    "created_at": "2026-02-19T10:30:00Z"
  },
  "tenant_id": "uuid-..."
}
```

### Subject patterns

| Pattern | Matches |
|---------|---------|
| `dw.acme.blog-service.posts.created` | Exact match — posts created in acme tenant |
| `dw.{tenant}.blog-service.posts.created` | Any tenant, posts created |
| `dw.{tenant}.blog-service.posts.*` | Any action on posts (created, updated, deleted) |
| `dw.*.*.posts.>` | Any service, any tenant, anything on posts |

### Practical example: Moderation flag

A `comments` table with a `flagged` column and a `moderation_queue` table that stores the comment id. When a comment is edited, mark its queue entry for re-review:

```json
{
  "name": "blog-service",
  "tables": [
    {
      "name": "comments",
      "columns": [
        {"name": "id", "column_type": "uuid", "primary_key": true, "auto_generate": true},
        {"name": "body", "column_type": "text"},
        {"name": "created_at", "column_type": "timestamp", "auto_generate": true}
      ]
    },
    {
      "name": "moderation_queue",
      "columns": [
        {"name": "id", "column_type": "uuid", "primary_key": true, "auto_generate": true},
        {"name": "comment_id", "column_type": "uuid", "unique": true},
        {"name": "needs_review", "column_type": "boolean"},
        {"name": "updated_at", "column_type": "timestamp", "auto_generate": true}
      ]
    }
  ],
  "subscriptions": [
    {
      "subject": "dw.{tenant}.blog-service.comments.updated",
      "handler": "flag_for_review",
      "handler_type": "update_field",
      "config": {
        "table": "moderation_queue",
        "where_column": "comment_id",
        "set_column": "needs_review",
        "set_value": "true"
      }
    }
  ]
}
```

This works because `moderation_queue.comment_id` holds exactly the id the `comments.updated` event carries in `data.id`. Aggregations that need a *different* id from the payload (a per-page view counter driven by `page_views.created`, say) belong in a WASM subscription handler, where you can read any field of the event and run arbitrary SQL.

### NATS infrastructure

- Each tenant gets a JetStream stream: `DW_<TENANT_SLUG>`
- Subjects: `dw.<tenant_slug>.>`
- Retention: Interest-based, 7-day max age
- Durable consumers ensure at-least-once delivery
- Messages are acknowledged after successful handler execution

---

## Guide 18: Custom Routes & API Design

Design custom API endpoints that go beyond standard CRUD — implement search, aggregation, workflow, and composite endpoints.

### Defining custom routes

Custom routes are declared in the `service!` macro of a `mode: wasm` service and handled by functions you write with the `cufflink-fn` crate. Each entry is a tuple of `(method, path, handler_name, posture)`:

```rust
// src/main.rs
use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "products")]
pub struct Product {
    #[key] pub id: Uuid,
    pub name: String,
    pub description: String,
    pub category: String,
    pub price_cents: i64,
    pub stock: i32,
    #[timestamp] pub created_at: DateTime<Utc>,
    #[timestamp] pub updated_at: DateTime<Utc>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "orders")]
pub struct Order {
    #[key] pub id: Uuid,
    pub total_cents: i64,
    pub status: String,
    #[timestamp] pub created_at: DateTime<Utc>,
    #[timestamp] pub updated_at: DateTime<Utc>,
}

cufflink::service! {
    name: "product-catalog",
    mode: wasm,
    tables: [Product, Order],
    custom_routes: [
        ("POST", "/search", "search_products", public("catalog search is anonymous")),
        ("GET", "/dashboard", "get_dashboard", perm("orders", "view")),
        ("POST", "/bulk-import", "bulk_import", auth),
    ],
}
```

The posture modifier is what the platform enforces before your handler runs. One is required per route (a route without one is logged as *"Custom route has no declared posture — running in legacy (unenforced) mode"* and `cufflink routes` reports it):

| Modifier | Who can call the route |
|----------|------------------------|
| `public("reason")` | Anyone; the reason is recorded in the manifest for audit |
| `auth` | Any authenticated user (JWT or API key) |
| `perm("area", "op")` | Users holding the RBAC permission `area:op` (Guide 14) |
| `owner("field")` | Authenticated users, with `req.auth()` available for per-row ownership checks in the handler |

### URL pattern

Handlers are addressed **by name**, not by the declared path:
```
/svc/{tenant}/{service}/_fn/{handler}
```

So the routes above are reachable at `POST /svc/acme/product-catalog/_fn/search_products`, `GET /svc/acme/product-catalog/_fn/get_dashboard` and so on. The `path` in the tuple is informational (it is shown by `cufflink routes` and stored in the manifest) and is not matched by the router; path parameters such as `/categories/:slug` are not supported — pass values in the JSON body or as query parameters instead.

### Reading input, writing output

Handlers are declared with `handler!(name, |req: Request| { ... })` in `src/lib.rs`, after a single `cufflink_fn::init!();`:

- `req.body()` — the parsed JSON body (`Value::Null` when none was sent). For GET requests the query string is exposed as `req.body()["params"]`, so `?q=rust` is `req.body()["params"]["q"]`.
- `req.auth()` / `req.require_auth()` — the caller's identity (`sub`, roles, `can(area, op)`).
- `db::query(sql)` returns `Vec<Value>`, `db::query_one(sql)` returns `Option<Value>`, `db::execute(sql)` returns the affected-row count. SQL runs in the service's schema, so bare table names work.
- `Response::json(&value)` (200), `Response::error(msg)` (400), `Response::not_found`, `Response::unauthorized`, `Response::forbidden`, and `.with_status(n)` to override the code.

### Example: Search endpoint

A handler that combines full-text search with custom scoring and facets:

```rust
// src/lib.rs
use cufflink_fn::handler;
use cufflink_fn::prelude::*;

cufflink_fn::init!();

// POST /svc/acme/product-catalog/_fn/search_products
// {"query": "wool socks", "category": "apparel", "min_price": 500, "max_price": 5000}
handler!(search_products, |req: Request| {
    let body = req.body();
    let query = body["query"].as_str().unwrap_or("").replace('\'', "''");
    let category = body["category"].as_str().map(|c| c.replace('\'', "''"));
    let min_price = body["min_price"].as_i64();
    let max_price = body["max_price"].as_i64();

    let mut sql = format!(
        "SELECT id, name, price_cents, category, \
         ts_rank(to_tsvector('english', name || ' ' || description), \
         plainto_tsquery('english', '{q}')) AS relevance \
         FROM products WHERE to_tsvector('english', name || ' ' || description) \
         @@ plainto_tsquery('english', '{q}')",
        q = query
    );

    if let Some(cat) = category {
        sql.push_str(&format!(" AND category = '{}'", cat));
    }
    if let Some(min) = min_price {
        sql.push_str(&format!(" AND price_cents >= {}", min));
    }
    if let Some(max) = max_price {
        sql.push_str(&format!(" AND price_cents <= {}", max));
    }
    sql.push_str(" ORDER BY relevance DESC LIMIT 50");

    let results = db::query(&sql);
    let facets = db::query("SELECT category, COUNT(*) AS count FROM products GROUP BY category");

    Response::json(&json!({
        "results": results,
        "facets": {"categories": facets},
        "query": body["query"],
    }))
});
```

Note that this is exactly the kind of same-column range (`price_cents >= min AND price_cents <= max`) that the CRUD `?filter=` parameter cannot express (Guide 4). Handler SQL is a plain string, so escape or validate anything you interpolate from the request, as above.

### Example: Composite endpoint

An endpoint that joins data across multiple tables, gated by the `perm("orders", "view")` posture declared in `service!`:

```rust
// GET /svc/acme/product-catalog/_fn/get_dashboard
handler!(get_dashboard, |_req: Request| {
    let recent_orders = db::query("SELECT * FROM orders ORDER BY created_at DESC LIMIT 10");

    let today = db::query_one(
        "SELECT COUNT(*) AS count, COALESCE(SUM(total_cents), 0) AS revenue \
         FROM orders WHERE created_at >= CURRENT_DATE",
    )
    .unwrap_or(json!({"count": 0, "revenue": 0}));

    let low_stock = db::query("SELECT * FROM products WHERE stock < 10 ORDER BY stock ASC");

    Response::json(&json!({
        "recent_orders": recent_orders,
        "today": today,
        "low_stock_alerts": low_stock,
    }))
});
```

Deploy with `cufflink deploy` — it compiles the crate to `wasm32-unknown-unknown` and uploads the module with the manifest in one step. The full handler API (HTTP client, NATS publishing, config, storage, long-running `job(...)` routes, testing) is documented in **[docs/wasm-guide.md](wasm-guide.md)**.

---

## Guide 19: Multi-Tenant Architecture Patterns

Cufflink is multi-tenant by default. Every service deployment is scoped to a tenant, and all data is isolated at the database level.

### How tenancy works

Each tenant gets:
- A dedicated PostgreSQL **database**: `cufflink_<slug>` (hyphens in the slug become underscores). Inside it, each deployed service gets its own **schema** named after the service (`blog-service` → `blog_service`)
- A dedicated NATS JetStream stream: `DW_<SLUG>`
- Complete data isolation — no cross-tenant queries are possible, because the platform opens a separate connection pool per tenant database

### URL structure

```
/svc/{tenant_slug}/{service_name}/{table}
```

The tenant slug appears in every API URL, making it explicit which tenant's data is being accessed.

### Deploying a service to multiple tenants

Use `Cufflink.toml` to define environments that map to different tenants:

```toml
# Cufflink.toml
[service]
default_env = "acme"

[environments.acme]
api_url = "https://cufflink.example.com"
tenant = "acme"
api_key_env = "CUFFLINK_ACME_API_KEY"

[environments.globex]
api_url = "https://cufflink.example.com"
tenant = "globex"
api_key_env = "CUFFLINK_GLOBEX_API_KEY"
```

```bash
# Deploy to the "acme" tenant
cufflink deploy --env acme

# Deploy to the "globex" tenant
cufflink deploy --env globex
```

Each tenant gets its own database with independent data.

### Per-tenant configuration

Store tenant-specific settings using the CLI:

```bash
# Set config for acme
cufflink config set FEATURE_BETA true --env acme

# Different config for globex
cufflink config set FEATURE_BETA false --env globex

# Secrets are masked in API responses
cufflink config set STRIPE_KEY sk_live_... --secret --env acme
```

### Cross-tenant event patterns

If service A in tenant "acme" needs to notify service B in tenant "globex", use webhook subscriptions:

```json
{
  "subscriptions": [
    {
      "subject": "dw.acme.order-service.orders.created",
      "handler": "forward_to_fulfillment",
      "handler_type": "webhook",
      "config": {
        "url": "http://localhost:8080/svc/globex/fulfillment-service/_fn/new_order",
        "method": "POST"
      }
    }
  ]
}
```

### Tenant isolation checklist

| Layer | Isolation Method |
|-------|-----------------|
| Database | Separate PostgreSQL database per tenant (`cufflink_<slug>`), schema per service |
| Messaging | Separate NATS JetStream stream per tenant |
| API | Tenant slug in URL path |
| Auth | JWT/API key scoped to tenant |
| Files | Object-storage keys prefixed by service id (`files/<service_id>/...`); a service id belongs to exactly one tenant |
| Audit | Tenant ID recorded on every audit entry |

---

## Guide 20: Advanced Filtering, JSON Operators & Aggregation

Master the query system to build powerful data retrieval patterns without custom code.

### JSON filter syntax

The `filter` query parameter accepts a JSON array of filter objects:

```
?filter=[{"n":"column","f":"operator","v":"value"}]
```

| Field | Description |
|-------|-------------|
| `n` | Column name (must be a declared column; unknown names are ignored) |
| `f` | Operator (case-insensitive) |
| `v` | Value (omit for `IS NULL` / `IS NOT NULL`; comma-separated list for `IN`) |

How filters combine is fixed by column, not by a per-filter flag: filters on **different columns are AND'd**, and multiple filters on the **same column are OR'd**. (A `c` field is accepted for backwards compatibility but has no effect.)

### Available operators

| Operator | SQL | Example |
|----------|-----|---------|
| `=` | `=` | Exact match |
| `!=` | `!=` | Not equal |
| `>` | `>` | Greater than |
| `<` | `<` | Less than |
| `>=` | `>=` | Greater than or equal |
| `<=` | `<=` | Less than or equal |
| `LIKE` | `LIKE` | Pattern match (case-sensitive, use `%` wildcard) |
| `ILIKE` | `ILIKE` | Pattern match (case-insensitive) |
| `IN` | `IN` | Value in list (comma-separated) |
| `IS NULL` | `IS NULL` | Column is null |
| `IS NOT NULL` | `IS NOT NULL` | Column is not null |

### Practical filtering examples

```bash
# Products that are either under $10 or over $50
# (same column twice = OR; a "between" range on ONE column is not expressible
#  with ?filter= — use a WASM handler for that)
curl "http://localhost:8080/svc/acme/shop/products?\
filter=[{\"n\":\"price_cents\",\"f\":\"<\",\"v\":\"1000\"},{\"n\":\"price_cents\",\"f\":\">\",\"v\":\"5000\"}]"

# Products in a category AND under $50 (different columns = AND)
curl "http://localhost:8080/svc/acme/shop/products?\
filter=[{\"n\":\"category\",\"f\":\"=\",\"v\":\"books\"},{\"n\":\"price_cents\",\"f\":\"<\",\"v\":\"5000\"}]"

# Users created in the last 7 days
curl "http://localhost:8080/svc/acme/app/users?\
filter=[{\"n\":\"created_at\",\"f\":\">=\",\"v\":\"2026-02-12T00:00:00Z\"}]"

# Orders with status "shipped" or "delivered"
curl "http://localhost:8080/svc/acme/shop/orders?\
filter=[{\"n\":\"status\",\"f\":\"IN\",\"v\":\"shipped,delivered\"}]"

# Tasks with no assignee
curl "http://localhost:8080/svc/acme/pm/tasks?\
filter=[{\"n\":\"assignee_id\",\"f\":\"IS NULL\"}]"

# Case-insensitive name search
curl "http://localhost:8080/svc/acme/app/users?\
filter=[{\"n\":\"name\",\"f\":\"ILIKE\",\"v\":\"%alice%\"}]"
```

### JSONB column querying

For tables with JSONB columns (like `metadata` or `settings`), you can store and retrieve structured data:

```bash
# Create a record with JSONB data
curl -X POST http://localhost:8080/svc/acme/app/users \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Alice",
    "metadata": {"role": "admin", "preferences": {"theme": "dark", "notifications": true}}
  }'

# The JSONB column is returned as a full JSON object in responses
```

### Combining search and filters

```bash
# Full-text search + column filter + sort + pagination
curl "http://localhost:8080/svc/acme/blog/posts?\
fts=kubernetes+deployment\
&filter=[{\"n\":\"published\",\"f\":\"=\",\"v\":\"true\"}]\
&sort=-created_at\
&page=1&per_page=20\
&select=id,title,author,created_at"
```

### Multi-column sorting

```bash
# Sort by priority (descending), then by due_date (ascending), then by title
curl "http://localhost:8080/svc/acme/pm/tasks?sort=-priority,due_date,title"
```

Prefix a column with `-` for descending order.

### Cursor-based pagination for large datasets

For datasets with millions of records, cursor pagination avoids the performance penalty of `OFFSET`. Send an empty `cursor=` to enter cursor mode from the first row:

```bash
# First page
curl "http://localhost:8080/svc/acme/app/events?cursor=&limit=100"
# Response: {"results": [...], "next_cursor": "base64...", "has_more": true}

# Next page
curl "http://localhost:8080/svc/acme/app/events?cursor=base64...&limit=100"
```

Cursor mode walks the table by primary key ascending; `sort` is ignored, but `filter`, `search` and `select` still apply. `has_more` is `false` once a page comes back shorter than `limit`.

---

## Guide 21: OpenAPI & TypeScript Client Generation

Generate API documentation and type-safe clients automatically from your deployed service.

### Generate an OpenAPI spec

```bash
# Print to stdout (reads the deployed service for the current environment)
cufflink openapi

# Save to file
cufflink openapi --output api-spec.json

# Generate from the local manifest without deploying first
cufflink openapi --local --output api-spec.json

# --local builds paths as /svc/local/...; override the slug if you want real URLs
cufflink openapi --local --tenant-slug acme --output api-spec.json
```

Both `openapi` and `generate-client` accept `--local` (build the spec from `cargo run -- --emit-manifest` instead of the platform) and `--tenant-slug` (the tenant used in the generated `/svc/{tenant}/...` paths when running locally).

### What's included in the spec

For each table in your service, the spec includes:

- **Paths**: GET (list), POST (create), GET (by id), PUT (update), DELETE
- **Schemas**: Full schema with types, a "Create" schema (excludes auto-generated fields)
- **Query parameters**: page, per_page, sort, filter
- **Pagination schema**: `current_page`, `per_page`, `total_records`, `total_pages` (matches the `pagination` object in list responses)

**Type mapping**:

| Column Type | OpenAPI Type |
|------------|--------------|
| uuid | `string` (format: uuid) |
| text | `string` |
| integer | `integer` (format: int32) |
| biginteger | `integer` (format: int64) |
| float | `number` (format: float) |
| double | `number` (format: double) |
| boolean | `boolean` |
| timestamp | `string` (format: date-time) |
| date | `string` (format: date) |
| jsonb | `object` |

### Generate a TypeScript client

```bash
# Writes openapi.json, client.ts and index.ts into the output directory
# (default: ./generated)
cufflink generate-client --output ./frontend/src/api

# Or without a deployment
cufflink generate-client --local --tenant-slug acme --output ./frontend/src/api
```

This generates a typed TypeScript client. Simplified, the output looks like:

```typescript
// Generated types
interface Todo {
  id: string;
  title: string;
  completed: boolean;
  created_at: string;
  updated_at: string;
}

interface TodoCreate {
  title: string;
  completed: boolean;
}

interface Pagination {
  current_page: number;
  per_page: number;
  total_records: number;
  total_pages: number;
}

// Generated client
export class CufflinkClient {
  constructor(baseUrl: string, headers: Record<string, string> = {}) { /* ... */ }

  async listTodos(params?: Record<string, string>): Promise<{ results: Todo[]; pagination: Pagination }> {
    return this.request('GET', '/svc/acme/my-todos/todos', params);
  }

  async createTodo(data: TodoCreate): Promise<Todo> {
    return this.request('POST', '/svc/acme/my-todos/todos', data);
  }

  async getTodo(id: string): Promise<Todo> {
    return this.request('GET', `/svc/acme/my-todos/todos/${id}`);
  }

  async updateTodo(id: string, data: Partial<TodoCreate>): Promise<Todo> {
    return this.request('PUT', `/svc/acme/my-todos/todos/${id}`, data);
  }

  async deleteTodo(id: string): Promise<void> {
    return this.request('DELETE', `/svc/acme/my-todos/todos/${id}`);
  }
}
```

### Using the generated client

```typescript
import { CufflinkClient } from './api';

// Pass an Authorization header for tables that declare auth_required
const api = new CufflinkClient('http://localhost:8080', {
  Authorization: 'ApiKey ck_...',
});

// Full type safety
const todo = await api.createTodo({
  title: "Write docs",
  completed: false
});

console.log(todo.id); // string - typed!

const { results, pagination } = await api.listTodos({ per_page: '20' });
results.forEach(t => console.log(t.title));
console.log(pagination.total_records);
```

### Importing into tools

The OpenAPI spec works with any standard tool:
- **Swagger UI**: Upload `api-spec.json` for interactive API docs
- **Postman**: Import as a collection
- **openapi-generator**: Generate clients in any language
- **Stoplight**: Build documentation portals

---

## Guide 22: Audit Trails & Compliance

Keep a record of who deployed what, and when, with the platform audit log.

### What gets logged

The platform audit log (`/api/audit`) records service deployments:

| Action | Resource Type | Details |
|--------|--------------|---------|
| `deploy` | `service` | `version` (the new deployment version) and `schema_changes` (number of SQL statements applied) |

Other management operations (webhook and config changes, file uploads, seeding, backup/restore) are **not** written to the platform audit log today. Data changes are covered separately: every CRUD write and every custom-route call is published as an audit event to the tenant's NATS stream (`dw.{tenant}.logs`), which you can consume with your own audit service — see [Audit Trail](audit.md).

### Querying the audit log

```bash
# All audit entries (paginated)
curl -H 'Authorization: ApiKey ck_...' \
  "http://localhost:8080/api/audit"

# Filter by action
curl -H 'Authorization: ApiKey ck_...' \
  "http://localhost:8080/api/audit?action=deploy"

# Filter by resource type
curl -H 'Authorization: ApiKey ck_...' \
  "http://localhost:8080/api/audit?resource_type=service"

# Filter by actor
curl -H 'Authorization: ApiKey ck_...' \
  "http://localhost:8080/api/audit?actor=alice@example.com"

# Combine filters with pagination
curl -H 'Authorization: ApiKey ck_...' \
  "http://localhost:8080/api/audit?action=deploy&actor=ci-bot&page=1&per_page=100"
```

### Audit response format

```json
{
  "entries": [
    {
      "id": "550e8400-...",
      "tenant_id": "tenant-uuid",
      "actor": "alice@example.com",
      "action": "deploy",
      "resource_type": "service",
      "resource_id": "service-uuid",
      "details": {
        "version": 2,
        "schema_changes": 3
      },
      "ip_address": null,
      "created_at": "2026-02-19T10:30:45Z"
    }
  ],
  "pagination": {
    "total": 150,
    "page": 1,
    "per_page": 50,
    "total_pages": 3
  }
}
```

`resource_id` is the service's ID and `actor` is the subject of the credential that deployed (a Keycloak user ID, or the API key's subject). Note that this endpoint uses its own `{"entries", "pagination": {"total", "page", ...}}` envelope, not the `{"results", "pagination": {"current_page", ...}}` shape of CRUD list responses.

### Compliance patterns

**Periodic export** — Dump the audit log for long-term archival:
```bash
# Export last 30 days of audit entries
curl -H 'Authorization: ApiKey ck_...' \
  "http://localhost:8080/api/audit?per_page=200" > audit-feb-2026.json
```

**Monitoring deployments** — Alert on unexpected deployments:
```bash
# Check recent deploys
curl -H 'Authorization: ApiKey ck_...' \
  "http://localhost:8080/api/audit?action=deploy&per_page=10"
```

---

## Guide 23: Production Deployment Patterns

Strategies for running Cufflink reliably in production.

### Schema evolution

When you redeploy a service with schema changes, the platform automatically:

1. **Detects changes** — Computes a SHA-256 hash of the manifest and compares to the last deployment
2. **Generates migrations** — Diffs the old and new schemas to produce SQL statements
3. **Applies non-destructive changes** — Adds new tables, adds new columns, creates indexes
4. **Blocks destructive changes** — Column drops and table drops require `--allow-destructive`

```bash
# Normal deploy — safe changes only
cufflink deploy --env production

# Allow destructive changes (column/table drops)
cufflink deploy --env production --allow-destructive

# Roll back to previous version
cufflink rollback --env production
```

### Zero-downtime deployments

Since schema changes are additive by default (new columns are nullable or have defaults), you can deploy new versions without downtime:

1. Deploy the new schema — adds columns, never removes
2. Update your application code to use new columns
3. Once old columns are no longer used, deploy with `--allow-destructive` to clean up

### Restructuring data during a deploy (`on_migrate` hook)

When a schema change requires *transforming* data — splitting one column into two, renaming a column losslessly, converting types — the simple expand/contract pattern above isn't enough. Cufflink supports an optional WASM hook that runs **between** additive and destructive phases of the deploy, with access to both the old and new columns at the same time.

#### How phases work

A deploy that declares `on_migrate` runs in three phases inside the platform:

1. **Phase 1 — Additive.** `CREATE TABLE`, `ADD COLUMN` (always nullable in this phase, even if your struct says non-null), `CREATE INDEX`, `ALTER FOREIGN KEY`. Committed in its own transaction.
2. **Phase 2 — Hook.** Your WASM `on_migrate` handler is invoked with a `SchemaDiff` describing exactly which tables and columns just changed. You backfill new columns from old ones, transform values, etc. The hook runs in its own database connection — failures abort the deploy without touching phase 3.
3. **Phase 3 — Destructive.** `DROP COLUMN`, `DROP TABLE`, `ALTER COLUMN TYPE`, and the deferred `SET NOT NULL` for non-nullable columns added in phase 1. Committed in its own transaction.

Services without `on_migrate` declared continue to use the legacy single-transaction path — no behaviour change for existing services.

#### Declaring the hook

Add `on_migrate` to your `service!` macro and implement a handler that delegates to `cufflink_fn::migrate::run`:

```rust
use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize)]
#[table(name = "sailing_requests")]
pub struct SailingRequest {
    #[key]
    pub id: Uuid,
    #[validate("min=1")]
    pub approximate_item_count_min: i32,
    pub approximate_item_count_max: Option<i32>,
    // ... other fields ...
}

cufflink::service! {
    name: "ferry-service",
    mode: wasm,
    tables: [SailingRequest],
    on_migrate: "handle_on_migrate",
}
```

```rust
// src/migrations/on_migrate.rs
use cufflink_fn::prelude::*;

handler!(handle_on_migrate, |req: Request| {
    migrate::run(req, |diff| {
        // Split approximate_item_count into a min/max pair.
        // Idempotent: WHERE … IS NULL guards skip already-backfilled rows.
        if diff.added_column("sailing_requests", "approximate_item_count_min")
            && diff.dropped_column("sailing_requests", "approximate_item_count")
        {
            db::execute(
                "UPDATE sailing_requests
                 SET approximate_item_count_min = CASE approximate_item_count
                         WHEN 3  THEN 1   WHEN 10 THEN 6
                         WHEN 23 THEN 16  WHEN 40 THEN 30
                         ELSE approximate_item_count
                     END,
                     approximate_item_count_max = CASE approximate_item_count
                         WHEN 3  THEN 5   WHEN 10 THEN 15
                         WHEN 23 THEN 30  WHEN 40 THEN NULL
                         ELSE approximate_item_count
                     END
                 WHERE approximate_item_count_min IS NULL"
            );
        }

        Ok(())
    })
})
```

#### The lifecycle: declare once, accumulate `if` blocks

The `on_migrate` declaration in `service!` is **persistent** — set it up once and never remove it. Migrations accumulate as `if` blocks inside the handler. Each block is gated by a `SchemaDiff` predicate, so it only fires on the deploy that actually performs that change. After that deploy runs on a given environment, the column is no longer "added" in the diff and the block becomes a silent no-op.

Adding a second migration months later just means appending another `if` block to the same handler:

```rust
handler!(handle_on_migrate, |req: Request| {
    migrate::run(req, |diff| {
        // First migration: split approximate_item_count into min/max
        if diff.added_column("sailing_requests", "approximate_item_count_min") {
            db::execute(/* ... */);
        }

        // Second migration, added later: rename special_notes → driver_notes
        if diff.added_column("sailing_requests", "driver_notes")
            && diff.dropped_column("sailing_requests", "special_notes")
        {
            db::execute(
                "UPDATE sailing_requests
                 SET driver_notes = special_notes
                 WHERE driver_notes IS NULL"
            );
        }

        Ok(())
    })
})
```

You can leave old blocks in the handler indefinitely as historical documentation — they cost nothing at deploy time once their predicates stop matching.

#### Idempotency is mandatory

Cufflink may invoke the hook on retried deploys, on deploys that change only the code, and on first-time deploys against a fresh database (a deploy whose manifest and code are both unchanged is skipped and does not run it). Every UPDATE in the hook **must** be idempotent, typically with a `WHERE … IS NULL` guard so previously-backfilled rows are skipped.

#### When phase 2 fails

If your hook returns `Err(...)` or panics:

- Phase 1 changes are already committed (they ran in a separate transaction).
- Phase 3 doesn't run.
- The deploy reports a clear error pointing at the hook name and message.

To recover: fix the bug in the hook, run `cufflink deploy` again. Phase 1 is a no-op the second time (the columns already exist), the hook re-runs (and skips already-backfilled rows thanks to your `IS NULL` guards), then phase 3 runs.

#### What `on_migrate` is *not* for

The hook fires only when there's a schema change to gate it. **Pure data migrations with no schema change** — e.g. "reformat all phone numbers to E.164" — don't have an idempotency anchor and would either re-run on every deploy forever or never run at all. For those, use a one-shot custom route handler invoked manually via `curl` after deploy and deleted in a follow-up commit.

### Backup strategy

```bash
# Scheduled nightly backup (exports are JSON Lines; default file is backup.jsonl)
cufflink backup export --output "backups/$(date +%Y-%m-%d).jsonl"

# Restore to a staging environment
cufflink backup restore backups/2026-02-19.jsonl --env staging
```

### Health monitoring

The platform exposes liveness, readiness and metrics endpoints:

```bash
# Liveness — plain text, checks only that the process is up
curl http://localhost:8080/health
# => cufflink-platform ok

# Readiness — JSON; 200 when the platform database answers, 503 otherwise.
# NATS and Redis are reported but do not gate the status.
curl http://localhost:8080/ready
# => {"db":"ok","nats":"connected","redis":"ok"}

# Prometheus metrics
curl http://localhost:8080/metrics
```

Point load-balancer health checks at `/ready` and container liveness probes at `/health`.

### Docker Compose production setup

The platform needs PostgreSQL, NATS (JetStream), a Keycloak realm for authentication, an S3-compatible object store for files and WASM modules, and (optionally) Redis for rate limiting shared across replicas. The repository's `docker-compose.yml` is the reference; a trimmed production shape looks like this:

```yaml
services:
  platform:
    image: kingsleyh/cufflink-platform:latest
    ports:
      - "8080:8080"
    environment:
      DATABASE_URL: postgres://cufflink:secret@db:5432/cufflink
      NATS_URL: nats://nats:4222
      # Keycloak as the platform reaches it, plus the browser-facing issuer
      KEYCLOAK_URL: http://keycloak:8080
      KEYCLOAK_REALM: cufflink
      KEYCLOAK_ISSUER: https://auth.example.com
      # Object storage — any S3-compatible store (RustFS shown; use your provider in prod)
      STORAGE_BACKEND: s3
      S3_ENDPOINT: http://rustfs:9000
      S3_REGION: us-east-1
      S3_ACCESS_KEY: rustfsadmin
      S3_SECRET_KEY: rustfsadmin
      S3_BUCKET: cufflink
      # Redis for distributed rate limiting (see below)
      REDIS_URL: redis://redis:6379
      RUST_LOG: info
    depends_on:
      - db
      - nats
      - keycloak
      - rustfs
      - redis
    restart: always

  db:
    image: postgres:16
    volumes:
      - pgdata:/var/lib/postgresql/data
    environment:
      POSTGRES_DB: cufflink
      POSTGRES_USER: cufflink
      POSTGRES_PASSWORD: secret
    restart: always

  nats:
    image: nats:2.10
    command: ["--jetstream", "--store_dir", "/data"]
    volumes:
      - natsdata:/data
    restart: always

  redis:
    image: redis:6.2-alpine
    restart: always

  rustfs:
    image: rustfs/rustfs:latest
    environment:
      RUSTFS_ACCESS_KEY: rustfsadmin
      RUSTFS_SECRET_KEY: rustfsadmin
    volumes:
      - rustfsdata:/data
    restart: always

  keycloak:
    image: quay.io/keycloak/keycloak:26.0
    # See docker-compose.yml for the database and realm-import configuration
    restart: always

volumes:
  pgdata:
  natsdata:
  rustfsdata:
```

There is no `JWT_SECRET`: tokens are validated against the Keycloak realm's JWKS, so the platform only needs to know where Keycloak is. The web-runner (`kingsleyh/cufflink-web-runner`) is a separate optional service for hosting web-mode services; see `docker-compose.yml` for its configuration.

### Rate limiting

The platform includes a per-client-IP rate limiter for the public API. It is **off by default** — enable it explicitly:

```
RATE_LIMIT_ENABLED=true     # default: false
RATE_LIMIT_PER_SECOND=20    # sustained requests per second per client IP (default: 20)
RATE_LIMIT_BURST=60         # burst allowance per client IP (default: 60)
RATE_LIMIT_EXEMPT_PRIVATE_PEERS=true   # skip callers on private addresses (default: true)
RATE_LIMIT_EXEMPT_IPS=203.0.113.7,198.51.100.0/24   # addresses or CIDR ranges never limited (default: none)
```

Health, readiness and metrics probes, in-cluster callers on private addresses (unless `RATE_LIMIT_EXEMPT_PRIVATE_PEERS=false`), and the addresses listed in `RATE_LIMIT_EXEMPT_IPS` are exempt. With `REDIS_URL` set the counters are shared across replicas; without it (or while Redis is unavailable) each instance limits with its own in-memory buckets.

### Observability

Enable OpenTelemetry export for traces and logs:

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
```

Outside a tenant's service the platform reports as `cufflink-platform`; inside one, the canonical collector names the tenant's service. See [Guide 33](#guide-33-observability--monitoring) for the full stack.

---

## Guide 24: Testing Your Service — Unit Tests

Test your service schema and manifest generation locally before deploying.

### Running tests with the CLI

The `cufflink test` command provides smart incremental testing:

```bash
# Run tests (skips if no source changes since last run)
cufflink test

# Force run all tests
cufflink test --all
```

The CLI computes a SHA-256 hash of all `.rs` files in `src/` plus `Cargo.toml`. If the hash matches the last run (stored in `.cufflink-test-hash`), tests are skipped — saving time during rapid iteration.

### Testing manifest generation

The most important test for any service is verifying that the manifest is correctly generated:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_todo_table_definition() {
        let def = Todo::__cufflink_table_def();

        assert_eq!(def.name, "todos");
        assert_eq!(def.columns.len(), 5); // id, title, completed, created_at, updated_at

        // Check primary key
        let pk = def.primary_key().unwrap();
        assert_eq!(pk.name, "id");
        assert!(pk.auto_generate);
        assert_eq!(pk.column_type, cufflink_types::ColumnType::Uuid);

        // Check regular column
        let title = def.columns.iter().find(|c| c.name == "title").unwrap();
        assert_eq!(title.column_type, cufflink_types::ColumnType::Text);
        assert!(!title.nullable);

        // Check boolean column
        let completed = def.columns.iter().find(|c| c.name == "completed").unwrap();
        assert_eq!(completed.column_type, cufflink_types::ColumnType::Boolean);
    }

    #[test]
    fn test_manifest_emits_valid_json() {
        // ServiceManifest does not implement Default, so every field is spelled out.
        let manifest = cufflink_types::ServiceManifest {
            name: "my-todos".into(),
            version: Some(env!("CARGO_PKG_VERSION").into()),
            tables: vec![Todo::__cufflink_table_def()],
            cells: vec![],
            events: vec![],
            subscriptions: vec![],
            custom_routes: vec![],
            mode: cufflink_types::ServiceMode::Crud,
            authorization: None,
            on_migrate: None,
            publishes: None,
            required_platform_features: vec![],
        };

        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["name"], "my-todos");
        assert_eq!(parsed["tables"][0]["name"], "todos");
    }

    #[test]
    fn test_schema_hash_stability() {
        let manifest1 = build_manifest();
        let manifest2 = build_manifest();
        assert_eq!(manifest1.schema_hash(), manifest2.schema_hash());
    }

    fn build_manifest() -> cufflink_types::ServiceManifest {
        cufflink_types::ServiceManifest {
            name: "my-todos".into(),
            version: Some("1.0.0".into()),
            tables: vec![Todo::__cufflink_table_def()],
            cells: vec![],
            events: vec![],
            subscriptions: vec![],
            custom_routes: vec![],
            mode: cufflink_types::ServiceMode::Crud,
            authorization: None,
            on_migrate: None,
            publishes: None,
            required_platform_features: vec![],
        }
    }
}
```

### Testing foreign key relationships

```rust
#[test]
fn test_comment_references_post() {
    let def = Comment::__cufflink_table_def();
    let post_id = def.columns.iter().find(|c| c.name == "post_id").unwrap();

    assert_eq!(post_id.references.as_deref(), Some("posts.id"));
    assert_eq!(post_id.on_delete, Some(cufflink_types::ForeignKeyAction::Cascade));
}
```

### Testing validation rules

```rust
#[test]
fn test_user_validations() {
    let def = User::__cufflink_table_def();

    let email = def.columns.iter().find(|c| c.name == "email").unwrap();
    assert!(email.unique);
    assert!(!email.validations.is_empty());

    // Check regex validation exists
    assert!(email.validations.iter().any(|v| matches!(v, cufflink_types::ValidationRule::Regex { .. })));

    let age = def.columns.iter().find(|c| c.name == "age").unwrap();
    assert!(age.nullable);

    // Check min/max validations
    assert!(age.validations.iter().any(|v| matches!(v, cufflink_types::ValidationRule::Min { value } if *value == 0.0)));
    assert!(age.validations.iter().any(|v| matches!(v, cufflink_types::ValidationRule::Max { value } if *value == 150.0)));
}
```

### Testing soft delete configuration

```rust
#[test]
fn test_document_soft_delete() {
    let def = Document::__cufflink_table_def();
    assert!(def.soft_delete);
}

#[test]
fn test_todo_hard_delete() {
    let def = Todo::__cufflink_table_def();
    assert!(!def.soft_delete);
}
```

### Testing the --emit-manifest flag

Verify that your service binary produces valid manifest JSON:

```rust
#[test]
fn test_emit_manifest_output() {
    let output = std::process::Command::new("cargo")
        .args(["run", "--", "--emit-manifest"])
        .output()
        .expect("Failed to run service");

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).unwrap();
    let manifest: cufflink_types::ServiceManifest =
        serde_json::from_str(&stdout).expect("Manifest is not valid JSON");

    assert_eq!(manifest.name, "my-todos");
    assert!(!manifest.tables.is_empty());
}
```

---

## Guide 25: Integration Testing with a Running Platform

Test your service against a live Cufflink platform to verify the full deployment and CRUD cycle.

### Prerequisites

Start the platform locally:

```bash
docker compose up -d
```

Verify it's running:

```bash
curl http://localhost:8080/health
# => cufflink-platform ok

curl http://localhost:8080/ready
# => {"db":"ok","nats":"connected","redis":"ok"}
```

The scripts below call `cufflink deploy`, which needs a `Cufflink.toml` with a `local` environment (tenant `acme`) and credentials — either run `cufflink login` once, or export `CUFFLINK_API_KEY`. The CRUD calls assume the `todos` table does not declare `auth_required`; if it does, add `-H 'Authorization: ApiKey ck_...'` to each `curl`.

### Deploy and test script

Create a `test-integration.sh`:

```bash
#!/bin/bash
set -euo pipefail

BASE_URL="${CUFFLINK_API_URL:-http://localhost:8080}"
TENANT="${CUFFLINK_TENANT:-acme}"
SERVICE="my-todos"
TABLE="todos"
API="$BASE_URL/svc/$TENANT/$SERVICE/$TABLE"

echo "=== Deploying service ==="
cufflink deploy

echo "=== Creating a todo ==="
CREATE_RESP=$(curl -sf -X POST "$API" \
  -H 'Content-Type: application/json' \
  -d '{"title": "Integration test todo", "completed": false}')

TODO_ID=$(echo "$CREATE_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Created: $TODO_ID"

echo "=== Reading the todo ==="
GET_RESP=$(curl -sf "$API/$TODO_ID")
TITLE=$(echo "$GET_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['title'])")
[ "$TITLE" = "Integration test todo" ] || { echo "FAIL: title mismatch"; exit 1; }
echo "Read OK: $TITLE"

echo "=== Updating the todo ==="
curl -sf -X PUT "$API/$TODO_ID" \
  -H 'Content-Type: application/json' \
  -d '{"completed": true}' > /dev/null
GET_RESP=$(curl -sf "$API/$TODO_ID")
COMPLETED=$(echo "$GET_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['completed'])")
[ "$COMPLETED" = "True" ] || { echo "FAIL: not completed"; exit 1; }
echo "Update OK"

echo "=== Listing todos ==="
LIST_RESP=$(curl -sf "$API")
COUNT=$(echo "$LIST_RESP" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['results']))")
[ "$COUNT" -ge 1 ] || { echo "FAIL: no results"; exit 1; }
echo "List OK: $COUNT results"

echo "=== Filtering ==="
FILTER_RESP=$(curl -sf "$API?completed=true")
F_COUNT=$(echo "$FILTER_RESP" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['results']))")
echo "Filtered: $F_COUNT completed todos"

echo "=== Deleting the todo ==="
curl -sf -X DELETE "$API/$TODO_ID" > /dev/null
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API/$TODO_ID")
[ "$HTTP_CODE" = "404" ] || { echo "FAIL: todo not deleted (got $HTTP_CODE)"; exit 1; }
echo "Delete OK"

echo ""
echo "=== All integration tests passed ==="
```

```bash
chmod +x test-integration.sh
./test-integration.sh
```

### Testing schema migrations

Verify that schema changes are applied correctly by deploying two versions of the same service. Start with a minimal table:

```rust
// src/main.rs — v1
#[derive(Table, Serialize, Deserialize)]
#[table(name = "items")]
pub struct Item {
    #[key]
    pub id: Uuid,
    pub name: String,
}

cufflink::service! {
    name: "migration-test",
    tables: [Item],
}
```

```bash
API="http://localhost:8080/svc/acme/migration-test/items"

echo "=== Deploy v1 ==="
cufflink deploy

# Create a record against the v1 schema
curl -sf -X POST "$API" \
  -H 'Content-Type: application/json' \
  -d '{"name": "test item"}'
```

Now add a nullable column and redeploy. Adding a column is a non-destructive change, so no `--allow-destructive` is needed:

```rust
// src/main.rs — v2
pub struct Item {
    #[key]
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,   // new
}
```

```bash
echo "=== Deploy v2 (add column) ==="
cufflink deploy
# => Deployed migration-test vN
#      Schema changes: 1

# Verify old data still works
RESP=$(curl -sf "$API")
echo "Old data preserved: $RESP"

# Verify the new column works
curl -sf -X POST "$API" \
  -H 'Content-Type: application/json' \
  -d '{"name": "new item", "description": "with description"}'

echo "=== Migration test passed ==="
```

**Under the hood.** `cufflink deploy` runs `cargo run -- --emit-manifest`, then POSTs the manifest to `/api/services/deploy`. That endpoint requires a deploy-capable credential (`Authorization: ApiKey ck_...` or a Keycloak bearer token with the tenant's deploy role), and the body wraps the manifest rather than being the manifest itself:

```bash
curl -sf -X POST "http://localhost:8080/api/services/deploy" \
  -H 'Authorization: ApiKey ck_...' \
  -H 'Content-Type: application/json' \
  -d '{
    "manifest": {
      "name": "migration-test",
      "tables": [{
        "name": "items",
        "columns": [
          {"name": "id", "column_type": "uuid", "primary_key": true, "auto_generate": true},
          {"name": "name", "column_type": "text"}
        ]
      }]
    },
    "allow_destructive": false,
    "commit_hash": "optional-git-sha"
  }'
```

WASM services also send `wasm_base64` in the same request. Prefer the CLI: it fills in the manifest, commit hash and WASM bundle for you.

### Testing validation

```bash
#!/bin/bash
set -euo pipefail

BASE_URL="http://localhost:8080"
API="$BASE_URL/svc/acme/validated-service/users"

echo "=== Testing validation rejection ==="

# Should fail: username too short
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API" \
  -H 'Content-Type: application/json' \
  -d '{"username": "ab", "email": "test@example.com", "display_name": "Test"}')
[ "$HTTP_CODE" = "422" ] || { echo "FAIL: expected 422, got $HTTP_CODE"; exit 1; }
echo "Short username rejected: OK"

# Should fail: invalid email
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API" \
  -H 'Content-Type: application/json' \
  -d '{"username": "validuser", "email": "not-an-email", "display_name": "Test"}')
[ "$HTTP_CODE" = "422" ] || { echo "FAIL: expected 422, got $HTTP_CODE"; exit 1; }
echo "Invalid email rejected: OK"

# Should succeed
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API" \
  -H 'Content-Type: application/json' \
  -d '{"username": "validuser", "email": "valid@example.com", "display_name": "Valid User"}')
[ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ] || { echo "FAIL: expected 200/201, got $HTTP_CODE"; exit 1; }
echo "Valid data accepted: OK"

echo "=== Validation tests passed ==="
```

### Testing batch operations

```bash
#!/bin/bash
set -euo pipefail

BASE_URL="http://localhost:8080"
API="$BASE_URL/svc/acme/my-todos/todos"

echo "=== Testing batch operations ==="

# Create initial record
RESP=$(curl -sf -X POST "$API" \
  -H 'Content-Type: application/json' \
  -d '{"title": "Existing todo", "completed": false}')
EXISTING_ID=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

# Batch: create 2, update 1, delete 1
BATCH_RESP=$(curl -sf -X POST "$API/batch" \
  -H 'Content-Type: application/json' \
  -d "{
    \"operations\": [
      {\"action\": \"create\", \"data\": {\"title\": \"Batch 1\", \"completed\": false}},
      {\"action\": \"create\", \"data\": {\"title\": \"Batch 2\", \"completed\": false}},
      {\"action\": \"update\", \"id\": \"$EXISTING_ID\", \"data\": {\"completed\": true}},
      {\"action\": \"delete\", \"id\": \"$EXISTING_ID\"}
    ]
  }")

CREATED=$(echo "$BATCH_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['summary']['created'])")
[ "$CREATED" = "2" ] || { echo "FAIL: expected 2 created, got $CREATED"; exit 1; }
echo "Batch operations: OK"

echo "=== Batch tests passed ==="
```

---

## Guide 26: Testing WASM Handlers

Test your custom WASM business logic before deploying.

### Unit testing handler logic

Separate your business logic from the WASM host function calls so it can be tested natively:

```rust
// src/lib.rs — Business logic (testable)

pub fn calculate_order_total(items: &[OrderItem]) -> i64 {
    items.iter()
        .map(|item| item.quantity as i64 * item.unit_price_cents)
        .sum()
}

pub fn validate_checkout(request: &CheckoutRequest) -> Result<(), String> {
    if request.items.is_empty() {
        return Err("Cart is empty".into());
    }
    if request.items.iter().any(|i| i.quantity <= 0) {
        return Err("Quantity must be positive".into());
    }
    if request.items.iter().any(|i| i.unit_price_cents <= 0) {
        return Err("Price must be positive".into());
    }
    Ok(())
}

pub struct CheckoutRequest {
    pub customer_id: String,
    pub items: Vec<OrderItem>,
}

pub struct OrderItem {
    pub product_name: String,
    pub quantity: i32,
    pub unit_price_cents: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_total_calculation() {
        let items = vec![
            OrderItem { product_name: "Widget".into(), quantity: 2, unit_price_cents: 1500 },
            OrderItem { product_name: "Gadget".into(), quantity: 1, unit_price_cents: 4500 },
        ];
        assert_eq!(calculate_order_total(&items), 7500);
    }

    #[test]
    fn test_order_total_empty() {
        assert_eq!(calculate_order_total(&[]), 0);
    }

    #[test]
    fn test_validate_checkout_ok() {
        let req = CheckoutRequest {
            customer_id: "cust-1".into(),
            items: vec![
                OrderItem { product_name: "Widget".into(), quantity: 1, unit_price_cents: 1000 },
            ],
        };
        assert!(validate_checkout(&req).is_ok());
    }

    #[test]
    fn test_validate_checkout_empty_cart() {
        let req = CheckoutRequest {
            customer_id: "cust-1".into(),
            items: vec![],
        };
        assert_eq!(validate_checkout(&req).unwrap_err(), "Cart is empty");
    }

    #[test]
    fn test_validate_checkout_zero_quantity() {
        let req = CheckoutRequest {
            customer_id: "cust-1".into(),
            items: vec![
                OrderItem { product_name: "Widget".into(), quantity: 0, unit_price_cents: 1000 },
            ],
        };
        assert!(validate_checkout(&req).is_err());
    }

    #[test]
    fn test_validate_checkout_negative_price() {
        let req = CheckoutRequest {
            customer_id: "cust-1".into(),
            items: vec![
                OrderItem { product_name: "Widget".into(), quantity: 1, unit_price_cents: -100 },
            ],
        };
        assert!(validate_checkout(&req).is_err());
    }
}
```

### Testing request/response parsing

Test that your handler correctly parses the platform's request format and produces valid responses:

```rust
use serde_json::json;

#[test]
fn test_parse_checkout_request() {
    let platform_request = json!({
        "method": "POST",
        "handler": "handle_checkout",
        "headers": {"Content-Type": "application/json"},
        "body": {
            "customer_id": "cust-123",
            "items": [
                {"product_name": "Widget", "quantity": 2, "unit_price_cents": 1500}
            ]
        },
        "tenant": "acme",
        "service": "order-service"
    });

    let body = &platform_request["body"];
    let customer_id = body["customer_id"].as_str().unwrap();
    assert_eq!(customer_id, "cust-123");

    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["quantity"].as_i64().unwrap(), 2);
}

#[test]
fn test_response_format() {
    let response = json!({
        "order_id": "order-uuid",
        "status": "pending",
        "total_cents": 7500,
        "item_count": 2
    });

    // Verify the response is valid JSON the platform can return
    let serialized = serde_json::to_string(&response).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap();
    assert_eq!(parsed["status"], "pending");
}
```

### Integration testing WASM handlers

After deploying your WASM module, test the custom routes end-to-end:

```bash
#!/bin/bash
set -euo pipefail

BASE_URL="http://localhost:8080"
SERVICE_API="$BASE_URL/svc/acme/order-service/_fn"

echo "=== Testing checkout handler ==="
RESP=$(curl -sf -X POST "$SERVICE_API/handle_checkout" \
  -H 'Content-Type: application/json' \
  -d '{
    "customer_id": "cust-123",
    "items": [
      {"product_name": "Widget", "quantity": 2, "unit_price_cents": 1500},
      {"product_name": "Gadget", "quantity": 1, "unit_price_cents": 4500}
    ]
  }')

TOTAL=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_cents'])")
[ "$TOTAL" = "7500" ] || { echo "FAIL: expected 7500, got $TOTAL"; exit 1; }
echo "Checkout total: OK ($TOTAL cents)"

ORDER_ID=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")
echo "Order created: $ORDER_ID"

echo "=== Testing stats handler ==="
STATS=$(curl -sf "$SERVICE_API/get_order_stats")
echo "Stats: $STATS"

echo "=== All WASM handler tests passed ==="
```

---

## Guide 27: End-to-End Testing with CI/CD

Set up automated testing in your CI/CD pipeline.

### Quick start: generate a CI workflow

The fastest way to set up CI/CD is to use the built-in generator:

```bash
cufflink ci init
```

This generates `.github/workflows/cufflink.yml` with a full pipeline: test, deploy staging, integration test, deploy production. See [Guide 30](#guide-30-cicd-pipeline--automated-deploy) for the complete walkthrough.

### GitHub Actions workflow for service testing

If you prefer to write your own workflow, here's a minimal example that runs unit tests and integration tests against a Cufflink platform started in the job.

A few things the job needs that are easy to miss:

- **`Cufflink.toml` committed** with a `local` environment. Every CLI command except `cufflink tenants` reads it, and `cufflink test --env local` fails with `Environment 'local' not found in Cufflink.toml` if the environment is missing.
- **A `tests/` directory.** `cufflink test --env <env>` runs `cargo test --test '*'`, so there must be at least one integration test target (see [Guide 30](#guide-30-cicd-pipeline--automated-deploy) for an example).
- **Credentials.** `cufflink deploy` and `cufflink test --env` need an API key: set `CUFFLINK_API_KEY`, or name a variable via `api_key_env` in `Cufflink.toml`.
- **Keycloak and a tenant.** API keys are minted by exchanging a tenant Keycloak token at `POST /api/auth/device/token`, so a bare platform container with only Postgres and NATS is not enough. The simplest option is the repository's `docker-compose.yml`, which imports the `acme` realm (user `dev`/`dev`) and registers the `acme` tenant.

```toml
# Cufflink.toml (committed)
[service]
default_env = "local"

[environments.local]
api_url = "http://localhost:8080"
tenant = "acme"
keycloak_url = "http://localhost:8180"
keycloak_realm = "acme"
```

```yaml
name: Service Tests

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  unit-tests:
    name: Unit Tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@stable

      - name: Run unit tests
        run: cargo test

      - name: Verify manifest generation
        run: cargo run -- --emit-manifest | python3 -m json.tool

  integration-tests:
    name: Integration Tests
    runs-on: ubuntu-latest
    needs: unit-tests
    steps:
      - uses: actions/checkout@v4

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@stable

      - name: Start the Cufflink stack
        run: |
          git clone --depth 1 https://github.com/driftwerk/cufflink.git /tmp/cufflink
          docker compose -f /tmp/cufflink/docker-compose.yml up -d
          # Wait for the platform, then for the acme tenant bootstrap to finish
          for i in $(seq 1 60); do
            curl -sf http://localhost:8080/health && break
            sleep 2
          done
          docker compose -f /tmp/cufflink/docker-compose.yml wait tenant-init

      - name: Mint an API key for the acme tenant
        run: |
          TOKEN=$(curl -sf -X POST \
            http://localhost:8180/realms/acme/protocol/openid-connect/token \
            -d grant_type=password -d client_id=cufflink-cli \
            -d username=dev -d password=dev | jq -r .access_token)
          API_KEY=$(curl -sf -X POST http://localhost:8080/api/auth/device/token \
            -H 'Content-Type: application/json' \
            -d "{\"access_token\":\"$TOKEN\",\"tenant_slug\":\"acme\"}" | jq -r .api_key)
          echo "::add-mask::$API_KEY"
          echo "CUFFLINK_API_KEY=$API_KEY" >> "$GITHUB_ENV"

      - name: Deploy service
        run: |
          cargo install cufflink-cli
          cufflink deploy --env local

      - name: Run integration tests
        run: cufflink test --env local

      - name: Cleanup
        if: always()
        run: docker compose -f /tmp/cufflink/docker-compose.yml down -v || true
```

The password grant on the `cufflink-cli` client is enabled in the bundled dev realms only; against a real Keycloak, create the API key once with `cufflink login` and store it as a repository secret instead.

### Testing with docker-compose

The repository's `docker-compose.yml` is the reference test stack: it brings up PostgreSQL, NATS, Keycloak (with the `cufflink` and `acme` realms imported), Redis, RustFS object storage, the platform and the WASM worker, and registers the `acme` tenant. Copy it into your service repository as `docker-compose.test.yml` (or point at a checkout) and trim what you don't need — but keep Keycloak and the `tenant-init` job, because deploys need a tenant and an API key.

Two things to keep in mind if you slim it down:

- The platform reads `KEYCLOAK_URL` / `KEYCLOAK_REALM` / `KEYCLOAK_ISSUER` and `STORAGE_BACKEND` + `S3_*`; without object storage, WASM deploys fail.
- `RUST_LOG: warn` is fine for CI; the shipped file uses `info,cufflink_platform=debug`.

```bash
# Start test infrastructure
docker compose -f docker-compose.test.yml up -d
docker compose -f docker-compose.test.yml wait tenant-init

# Mint a key for the acme tenant (dev realm only — see the workflow above)
export CUFFLINK_API_KEY=ck_...

# Deploy and test
cufflink deploy --env local
./test-integration.sh

# Tear down
docker compose -f docker-compose.test.yml down -v
```

### Makefile for test automation

```makefile
.PHONY: test test-unit test-integration test-all clean

test-unit:
	cargo test
	@echo "Unit tests passed"

test-manifest:
	cargo run -- --emit-manifest | python3 -m json.tool > /dev/null
	@echo "Manifest generation OK"

test-integration: test-infra-up
	cufflink deploy --env local
	./test-integration.sh
	$(MAKE) test-infra-down

test-infra-up:
	docker compose -f docker-compose.test.yml up -d
	docker compose -f docker-compose.test.yml wait tenant-init

test-infra-down:
	docker compose -f docker-compose.test.yml down -v

test-all: test-unit test-manifest test-integration
	@echo "All tests passed"

clean:
	cargo clean
	rm -f .cufflink-test-hash
```

### Pre-commit hook

Add a git pre-commit hook to catch issues early:

```bash
#!/bin/bash
# .git/hooks/pre-commit

echo "Running pre-commit checks..."

# Format check
cargo fmt -- --check || { echo "Run 'cargo fmt' first"; exit 1; }

# Clippy
cargo clippy --all-targets -- -D warnings || exit 1

# Unit tests
cargo test || exit 1

# Manifest check
cargo run -- --emit-manifest > /dev/null 2>&1 || { echo "Manifest generation failed"; exit 1; }

echo "Pre-commit checks passed"
```

---

## Guide 28: Load Testing & Performance Validation

Ensure your service handles production traffic levels.

### Quick load test with curl

```bash
#!/bin/bash
# Simple concurrent load test

API="http://localhost:8080/svc/acme/my-todos/todos"
CONCURRENCY=10
REQUESTS=100

echo "Running $REQUESTS requests with $CONCURRENCY concurrent..."

for i in $(seq 1 $REQUESTS); do
  curl -sf -X POST "$API" \
    -H 'Content-Type: application/json' \
    -d "{\"title\": \"Load test $i\", \"completed\": false}" \
    -o /dev/null -w "%{http_code} %{time_total}s\n" &

  # Limit concurrency
  if (( i % CONCURRENCY == 0 )); then
    wait
  fi
done
wait

echo "Load test complete"
echo "Verifying record count..."
COUNT=$(curl -sf "$API?per_page=1" | python3 -c "import sys,json; print(json.load(sys.stdin)['pagination']['total_records'])")
echo "Total records: $COUNT"
```

### Load testing with hey

[hey](https://github.com/rakyll/hey) is a simple HTTP load generator:

```bash
# Install hey
go install github.com/rakyll/hey@latest

# Test read performance (GET list)
hey -n 1000 -c 50 \
  "http://localhost:8080/svc/acme/my-todos/todos?per_page=10"

# Test write performance (POST create)
hey -n 500 -c 20 -m POST \
  -H "Content-Type: application/json" \
  -d '{"title": "load test", "completed": false}' \
  "http://localhost:8080/svc/acme/my-todos/todos"

# Test with filtering
hey -n 1000 -c 50 \
  "http://localhost:8080/svc/acme/my-todos/todos?completed=false&sort=-created_at&per_page=20"
```

### Load testing with k6

For more sophisticated load testing, use [k6](https://k6.io/):

```javascript
// load-test.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

export const options = {
  stages: [
    { duration: '30s', target: 20 },   // Ramp up to 20 users
    { duration: '1m', target: 50 },    // Ramp up to 50 users
    { duration: '30s', target: 100 },  // Spike to 100 users
    { duration: '1m', target: 50 },    // Back to 50
    { duration: '30s', target: 0 },    // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'],  // 95% of requests under 500ms
    errors: ['rate<0.01'],             // Error rate under 1%
  },
};

const BASE = 'http://localhost:8080/svc/acme/my-todos/todos';

export default function () {
  // Create
  const createRes = http.post(BASE, JSON.stringify({
    title: `k6 test ${Date.now()}`,
    completed: false,
  }), { headers: { 'Content-Type': 'application/json' } });

  check(createRes, { 'create status 200': (r) => r.status === 200 });
  errorRate.add(createRes.status !== 200);

  if (createRes.status === 200) {
    const id = createRes.json('id');

    // Read
    const getRes = http.get(`${BASE}/${id}`);
    check(getRes, { 'get status 200': (r) => r.status === 200 });
    errorRate.add(getRes.status !== 200);

    // Update
    const updateRes = http.put(`${BASE}/${id}`, JSON.stringify({
      completed: true,
    }), { headers: { 'Content-Type': 'application/json' } });
    check(updateRes, { 'update status 200': (r) => r.status === 200 });

    // Delete
    const deleteRes = http.del(`${BASE}/${id}`);
    check(deleteRes, { 'delete status 200': (r) => r.status === 200 });
  }

  // List with pagination
  const listRes = http.get(`${BASE}?per_page=20&sort=-created_at`);
  check(listRes, { 'list status 200': (r) => r.status === 200 });

  sleep(0.1);
}
```

```bash
k6 run load-test.js
```

### Performance benchmarks to target

| Operation | Target (p95) | Notes |
|-----------|-------------|-------|
| GET single record | < 10ms | Single row by primary key |
| GET list (50 records) | < 50ms | With pagination |
| GET list with filters | < 100ms | With filter + sort |
| GET full-text search | < 200ms | Depends on data volume |
| POST create | < 20ms | Single record insert |
| PUT update | < 20ms | Single record update |
| DELETE | < 10ms | Single record delete |
| POST batch (10 ops) | < 100ms | Transactional batch |
| GraphQL query | < 100ms | Single table query |

### Monitoring during load tests

While the load test runs, monitor the platform:

```bash
# Watch CRUD request counters
watch -n 1 'curl -s http://localhost:8080/metrics | grep cufflink_crud_requests_total'

# Watch rate-limiter rejections (only if RATE_LIMIT_ENABLED=true)
watch -n 1 'curl -s http://localhost:8080/metrics | grep cufflink_rate_limited_total'

# Check readiness (database, NATS, Redis)
watch -n 5 'curl -s http://localhost:8080/ready'
```

---

## Guide 29: Environments — Staging, Production & Testing

Manage multiple deployment targets from a single codebase using `Cufflink.toml`.

### The environment model

In Cufflink, **environments = tenants**. Each environment (local, staging, production) maps to a tenant on the same or different platform instance. The platform already provides full isolation — separate database schema, separate NATS stream — so no special configuration is needed on the server side.

### Setting up environments

When you run `cufflink init`, a `Cufflink.toml` is generated with a local environment. Add staging and production:

```toml
# Cufflink.toml
[service]
default_env = "local"

[environments.local]
api_url = "http://localhost:8080"
tenant = "acme"
keycloak_url = "http://localhost:8180"   # used by `cufflink login` (device flow)
keycloak_realm = "acme"

[environments.staging]
api_url = "https://cufflink.example.com"
tenant = "myapp-staging"
api_key_env = "CUFFLINK_STAGING_API_KEY"

[environments.production]
api_url = "https://cufflink.example.com"
tenant = "myapp-prod"
api_key_env = "CUFFLINK_PROD_API_KEY"
```

Key fields:
- `api_url` — the Cufflink platform URL
- `tenant` — the tenant slug for this environment
- `keycloak_url` / `keycloak_realm` — the tenant's Keycloak realm, used by `cufflink login`
- `api_key` — inline key (for local dev only, don't commit to version control)
- `api_key_env` — name of the environment variable holding the API key (for CI/CD)
- `default_env` — which environment to use when `--env` is not specified

### Using the `--env` flag

Every CLI command accepts `--env` (or `-e`) to target a specific environment:

```bash
# Deploy to staging
cufflink deploy --env staging

# Check status of production
cufflink status --env production

# View logs from staging
cufflink logs --env staging

# Set a config value on production
cufflink config set STRIPE_KEY sk_live_... --secret --env production

# Export a backup from staging
cufflink backup export --output staging-backup.jsonl --env staging

# Run integration tests against staging
cufflink test --env staging
```

Without `--env`, the CLI uses `default_env` from `Cufflink.toml`. Asking for an environment that isn't declared fails with `Environment 'staging' not found in Cufflink.toml. Available: ["local"]`.

### API key resolution

The CLI resolves API keys in this order:

1. `api_key` field in the environment config (inline, for local dev)
2. Environment variable named in `api_key_env` (for CI/CD)
3. `CUFFLINK_API_KEY` environment variable
4. Stored credentials from `cufflink login` (`~/.config/cufflink/credentials.json`, keyed by `api_url` + `tenant`)

If none of these yields a key, commands fail with `Error: No credentials found for tenant '<slug>'` and a hint to run `cufflink login` or set `CUFFLINK_API_KEY`.

For CI/CD, always use `api_key_env` — it keeps secrets out of your config file:

```toml
[environments.staging]
api_url = "https://cufflink.example.com"
tenant = "myapp-staging"
api_key_env = "CUFFLINK_STAGING_API_KEY"
```

Then in your CI environment, set `CUFFLINK_STAGING_API_KEY` as a secret.

### Environment isolation

Each environment is completely isolated:

| Aspect | Isolation level |
|--------|----------------|
| Database | Separate PostgreSQL schema per tenant |
| Event streams | Separate NATS JetStream per tenant |
| Config & secrets | Per-tenant key-value store |
| Service versions | Independent deployment history |
| Data | No cross-tenant access possible |

This means you can safely test destructive schema changes on staging without affecting production.

### Typical workflow

```bash
# 1. Develop locally
cufflink deploy                    # uses default_env = "local"
cargo test                         # unit tests

# 2. Deploy to staging
cufflink deploy --env staging
cufflink test --env staging        # integration tests against staging

# 3. Deploy to production
cufflink deploy --env production

# 4. If something goes wrong
cufflink rollback --env production
```

### Multiple platform instances

Environments can point to different platform instances. For example, local development uses a local platform while staging and production use a hosted instance:

```toml
[environments.local]
api_url = "http://localhost:8080"
tenant = "acme"
keycloak_url = "http://localhost:8180"
keycloak_realm = "acme"

[environments.staging]
api_url = "https://us-east.cufflink.example.com"
tenant = "myapp-staging"
api_key_env = "CUFFLINK_STAGING_API_KEY"

[environments.production]
api_url = "https://eu-west.cufflink.example.com"
tenant = "myapp-prod"
api_key_env = "CUFFLINK_PROD_API_KEY"
```

### `Cufflink.toml` is required

Service commands (`deploy`, `status`, `logs`, `test`, `backup`, `config`, …) resolve their target from `Cufflink.toml`; without one they stop with "No Cufflink.toml found. Run from a cufflink project directory or use `cufflink init` to create one." The CLI searches the current directory and its parents, so running from a subdirectory of the service works. The only exception is `cufflink tenants ...`, which is a platform-admin command and accepts `--api-url` (or `CUFFLINK_API_URL`) when no `Cufflink.toml` is present.

---

## Guide 30: CI/CD Pipeline — Automated Deploy

Automate the full workflow: push code, run tests, deploy to staging, run integration tests, deploy to production.

### Generate the pipeline

```bash
cufflink ci init
```

This creates `.github/workflows/cufflink.yml` with four jobs:

1. **test** — runs `cargo test` on every push and PR
2. **deploy-staging** — deploys to staging on pushes to `main`
3. **integration-test** — runs `cufflink test --env staging` against the live staging environment
4. **deploy-production** — deploys to production after integration tests pass

### Prerequisites

Before using the generated pipeline:

1. **Configure `Cufflink.toml`** with staging and production environments:

```toml
[service]
default_env = "local"

[environments.local]
api_url = "http://localhost:8080"
tenant = "acme"
keycloak_url = "http://localhost:8180"
keycloak_realm = "acme"

[environments.staging]
api_url = "https://cufflink.example.com"
tenant = "myapp-staging"
api_key_env = "CUFFLINK_STAGING_API_KEY"

[environments.production]
api_url = "https://cufflink.example.com"
tenant = "myapp-prod"
api_key_env = "CUFFLINK_PROD_API_KEY"
```

2. **Add GitHub secrets** in your repository settings:
   - `CUFFLINK_STAGING_API_KEY` — API key for the staging tenant
   - `CUFFLINK_PROD_API_KEY` — API key for the production tenant

3. **Create GitHub environments** named `staging` and `production` in your repository settings. The production environment can have required reviewers for manual approval before deploying.

### Pipeline flow

```
push to main
    │
    ▼
┌─────────┐     ┌────────────────┐     ┌──────────────────┐     ┌───────────────────┐
│  test   │────▶│ deploy-staging │────▶│ integration-test │────▶│ deploy-production │
│         │     │                │     │                  │     │                   │
│cargo    │     │cufflink deploy │     │cufflink test     │     │cufflink deploy    │
│test     │     │--env staging   │     │--env staging     │     │--env production   │
└─────────┘     └────────────────┘     └──────────────────┘     └───────────────────┘
```

Pull requests only run the `test` job. The full deploy pipeline runs on pushes to `main`.

### Writing integration tests

Integration tests live in the `tests/` directory and run against a deployed service. The CLI sets environment variables so your tests know where to connect:

```rust
// tests/integration.rs
use reqwest::Client;
use serde_json::json;

fn api_url() -> String {
    std::env::var("CUFFLINK_API_URL").expect("CUFFLINK_API_URL must be set")
}

fn tenant() -> String {
    std::env::var("CUFFLINK_TENANT").expect("CUFFLINK_TENANT must be set")
}

#[tokio::test]
async fn test_create_and_read() {
    let client = Client::new();
    let base = format!(
        "{}/svc/{}/my-todos/todos",
        api_url(),
        tenant()
    );

    // Create a record
    let resp = client
        .post(&base)
        .json(&json!({ "title": "Integration test", "completed": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let created: serde_json::Value = resp.json().await.unwrap();
    let id = created["id"].as_str().unwrap();

    // Read it back
    let resp = client
        .get(&format!("{}/{}", base, id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["title"], "Integration test");

    // Clean up
    client.delete(&format!("{}/{}", base, id)).send().await.unwrap();
}
```

Run them locally against staging:

```bash
cufflink test --env staging
```

This sets `CUFFLINK_API_URL`, `CUFFLINK_TENANT`, `CUFFLINK_API_KEY`, and `CUFFLINK_ENV`, then runs `cargo test --test '*'`.

### WASM services in CI

The generated pipeline already includes the `wasm32-unknown-unknown` target. For WASM services, `cufflink deploy` automatically:

1. Builds the native binary to extract the manifest
2. Builds the WASM module (`cargo build --lib --target wasm32-unknown-unknown --release`)
3. Sends the manifest and the WASM binary (`wasm_base64`) to the platform in a single deploy request, which applies schema changes and activates the module together

No additional CI configuration is needed — one `cufflink deploy --env staging` handles everything.

### Customizing the pipeline

The generated workflow is a starting point. Common customizations:

**Add manual approval for production:**

In your GitHub repository, go to Settings > Environments > production and add "Required reviewers". The `deploy-production` job will wait for approval before running.

**Add a smoke test after production deploy:**

```yaml
  smoke-test:
    name: Production Smoke Test
    needs: deploy-production
    runs-on: ubuntu-latest
    steps:
      - run: |
          curl -sf "https://cufflink.example.com/svc/myapp-prod/my-todos/todos?per_page=1" \
            || (echo "Smoke test failed" && exit 1)
```

**Pin the CLI version:**

The generated workflow runs `cargo install cufflink-cli`, which picks up the latest release. To pin:

```yaml
      - name: Install Cufflink CLI
        run: cargo install cufflink-cli@0.20
```

### Troubleshooting

| Issue | Solution |
|-------|----------|
| `Environment 'staging' not found in Cufflink.toml. Available: ["local"]` | Add `[environments.staging]` to `Cufflink.toml` and commit it |
| `No Cufflink.toml found. Run from a cufflink project directory or use cufflink init to create one.` | The job must run in the service directory (or a subdirectory of it) with `Cufflink.toml` checked in |
| `Error: No credentials found for tenant 'myapp-staging'` | Add `CUFFLINK_STAGING_API_KEY` to GitHub secrets and pass it to the job via `env:` |
| `error[E0463]: can't find crate for core` / target not installed | The generated workflow includes `targets: wasm32-unknown-unknown`; add it to any custom Rust setup step |
| `401 Unauthorized` on deploy | The API key belongs to a different tenant, or the Keycloak user lacks the tenant's deploy role (`cufflink-deploy` by default) |
| `no test target matches pattern` | `cufflink test --env` runs `cargo test --test '*'`; add at least one file under `tests/` |
| Integration tests time out | Ensure staging is reachable from GitHub Actions runners |

---

## Guide 31: Authentication & Authorization

Protect your endpoints and access JWT claims in business logic.

### How authentication works

When a request arrives at the Cufflink platform with an `Authorization` header, the platform validates it before your code ever sees it:

1. **Bearer JWT** — validated against Keycloak's JWKS (RS256 signature, expiry, issuer)
2. **ApiKey** — looked up in the database (SHA256 hash match, expiry check)

If valid, the platform extracts user claims and makes them available to your handlers. Invalid tokens are rejected with `401 Unauthorized` before reaching your code.

### Authentication for CRUD services

By default, CRUD tables allow unauthenticated access. To require authentication, add `auth_required` to your table:

```rust
#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "orders", auth_required)]
pub struct Order {
    #[key]
    pub id: Uuid,
    pub customer_id: String,
    pub total: i64,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}
```

With `auth_required`, all CRUD operations (list, create, get, update, delete, batch) on this table return `401 Unauthorized` if no valid auth is provided.

### Combining `auth_required` with `owner_field`

Use both for per-user data isolation:

```rust
#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "notes", auth_required)]
pub struct Note {
    #[key]
    pub id: Uuid,
    pub user_id: String,  // set via owner_field in manifest
    pub title: String,
    pub body: String,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}
```

- `auth_required` rejects unauthenticated requests with 401
- `owner_field` scopes data so users only see their own records
- `owner_field` is auto-set to the authenticated user's subject on create

### Authentication in WASM handlers

The platform validates the JWT and passes the extracted claims in the `auth` field of the request. You never validate tokens yourself — just check the claims.

```rust
use cufflink_fn::prelude::*;

cufflink_fn::init!();

handler!(get_profile, |req: Request| {
    // Get auth context (None if unauthenticated)
    let auth = match req.require_auth() {
        Ok(auth) => auth,
        Err(resp) => return resp,  // Returns 401 error response
    };

    // Access user info
    let user_id = &auth.sub;
    let name = auth.name.as_deref().unwrap_or("Unknown");
    let email = auth.email.as_deref().unwrap_or("N/A");

    Response::json(&json!({
        "user_id": user_id,
        "name": name,
        "email": email,
    }))
});
```

### Role-based access control

Check Cufflink permissions in your handlers (recommended):

```rust
handler!(admin_action, |req: Request| {
    let auth = match req.require_auth() {
        Ok(auth) => auth,
        Err(resp) => return resp,
    };

    if !auth.can("settings", "edit") {
        return Response::error("Forbidden: missing settings:edit permission");
    }

    // Authorized logic here
    db::execute("UPDATE settings SET maintenance = true");
    Response::json(&json!({"status": "maintenance enabled"}))
});
```

You can also check Keycloak realm roles directly if needed:

```rust
if !auth.has_role("realm-admin") {
    return Response::error("Forbidden: realm-admin role required");
}
```

### Custom JWT claims

If you configure custom Keycloak protocol mappers (e.g., to add department, organization, or subscription tier to the JWT), access them via `auth.claim()`:

```rust
handler!(check_tier, |req: Request| {
    let auth = match req.require_auth() {
        Ok(auth) => auth,
        Err(resp) => return resp,
    };

    let tier = auth.claim("subscription_tier")
        .and_then(|v| v.as_str())
        .unwrap_or("free");

    let max_items = match tier {
        "enterprise" => 10000,
        "pro" => 1000,
        _ => 100,
    };

    Response::json(&json!({"tier": tier, "max_items": max_items}))
});
```

To add custom claims in Keycloak:
1. Go to your realm > Client Scopes > create a new scope
2. Add a protocol mapper (type: User Attribute or Hardcoded Claim)
3. Set "Add to ID token" and "Add to access token" to ON
4. Assign the scope to your client

### The `Auth` struct

The full `Auth` struct available in WASM handlers:

```rust
pub struct Auth {
    pub sub: String,                        // Keycloak user ID
    pub preferred_username: Option<String>,  // Username
    pub name: Option<String>,                // Display name
    pub email: Option<String>,               // Email
    pub realm_roles: Vec<String>,            // Roles from realm_access
    pub claims: HashMap<String, Value>,      // All other JWT claims
    pub permissions: Vec<String>,            // Cufflink permissions (e.g. "staff:create")
    pub role_names: Vec<String>,             // Cufflink role names
    pub is_service_account: bool,            // true for machine credentials (service-to-service calls)
}

impl Auth {
    fn has_role(&self, role: &str) -> bool;              // Check Keycloak realm role
    fn can(&self, area: &str, op: &str) -> bool;         // Check Cufflink permission (supports wildcards)
    fn has_cufflink_role(&self, role: &str) -> bool;      // Check Cufflink role name
    fn claim(&self, key: &str) -> Option<&Value>;         // Get custom claim
}

impl Request {
    fn auth(&self) -> Option<&Auth>;                  // Get auth (None if unauthenticated)
    fn require_auth(&self) -> Result<&Auth, Response>; // Get auth or return 401
}
```

**Cufflink permissions** use structured `"area:operation"` strings. Wildcards are supported:
- `auth.can("staff", "create")` — checks for `"staff:create"` or `"staff:*"` or `"*"`
- Permissions are resolved from the Cufflink `roles` and `user_roles` tables, not from Keycloak

```rust
handler!(admin_action, |req: Request| {
    // require_auth returns Result<&Auth, Response>; the Err is a ready-made 401
    let auth = match req.require_auth() {
        Ok(auth) => auth,
        Err(resp) => return resp,
    };

    // Keycloak realm role (from JWT)
    if auth.has_role("realm-admin") { /* ... */ }

    // Cufflink permission (from database)
    if !auth.can("staff", "create") {
        return Response::error("Forbidden: missing staff:create permission");
    }

    Response::json(&json!({"ok": true}))
});
```

The macro takes a bare identifier (it becomes the exported WASM symbol) and the closure parameter must be typed `Request`. Because the handler body returns a `Response`, not a `Result`, `?` on `require_auth()` does not compile — use `match` as above.

### Using with an API gateway (Tyk, Kong)

You can place Tyk or Kong in front of the Cufflink platform. Both approaches work:

**Gateway validates + Platform validates (recommended):**

The gateway validates the JWT and forwards the request with the `Authorization` header intact. The Cufflink platform also validates the JWT (double validation is fine — both use the same Keycloak JWKS). Your WASM handlers receive the validated claims from the platform.

```
Client → Tyk (validates JWT) → Cufflink Platform (validates JWT) → WASM Handler
```

This gives you defense in depth — if either layer is misconfigured, the other still protects you.

**Gateway validates + Platform trusts:**

If you want the gateway to be the sole auth enforcement point, you can run Cufflink without Keycloak. `KEYCLOAK_URL` defaults to `http://localhost:8180`, so this has to be explicit: set `KEYCLOAK_URL=""` (empty). The platform then skips bearer-token validation, and `req.auth()` returns `None` in handlers. You'd need to read claims from a custom header set by the gateway. Existing API keys still work (they are looked up in the database, not validated against Keycloak), but minting new ones through `cufflink login` requires a tenant Keycloak realm.

### Docker Compose with Keycloak

The standard `docker-compose.yml` includes Keycloak and imports two realms from `infra/keycloak/` on first start: `cufflink` (the platform realm; user `admin`/`admin`) and `acme` (a dev tenant realm; user `dev`/`dev`). Both contain a public `cufflink-cli` client with the device grant enabled and a `cufflink-deploy` realm role.

```yaml
keycloak:
  image: quay.io/keycloak/keycloak:26.0
  command: start-dev --import-realm
  environment:
    KC_DB: postgres
    KC_DB_URL: jdbc:postgresql://keycloak-db:5432/keycloak
    KC_DB_USERNAME: keycloak
    KC_DB_PASSWORD: keycloak
    KEYCLOAK_ADMIN: admin
    KEYCLOAK_ADMIN_PASSWORD: admin
    # Fix the issuer to the browser-facing address
    KC_HOSTNAME: http://localhost:8180
    KC_HOSTNAME_BACKCHANNEL_DYNAMIC: "true"
  volumes:
    - ./infra/keycloak:/opt/keycloak/data/import:ro
  ports:
    - "8180:8080"
```

The platform connects to Keycloak via environment variables:

```yaml
platform:
  environment:
    KEYCLOAK_URL: http://keycloak:8080        # as the platform reaches it (JWKS)
    KEYCLOAK_REALM: cufflink                  # platform realm
    KEYCLOAK_ISSUER: http://localhost:8180    # issuer as it appears in tokens
```

Each tenant has its own realm, registered with the tenant (`cufflink tenants create --name ... --slug ... --tenant-keycloak-url ... --tenant-keycloak-realm ...`); the platform fetches that realm's JWKS to validate tenant tokens.

### Setting up Keycloak for development

1. Start Docker Compose: `docker compose up -d` — the realms are imported and the `acme` tenant is registered automatically
2. Open Keycloak: `http://localhost:8180` (admin/admin) to add users or roles in the `acme` realm as needed
3. Make sure `Cufflink.toml` has `keycloak_url = "http://localhost:8180"` and `keycloak_realm = "acme"` in the environment you're using
4. Run `cufflink login` and log in as `dev`/`dev` via the device code flow; the CLI exchanges the token for an API key and stores it

For an additional tenant, create a realm (with a `cufflink-cli` public client and a `cufflink-deploy` role), then register it with `cufflink tenants create`.

### CLI authentication

```bash
# Login via Keycloak device code flow
cufflink login

# Or use an API key directly
export CUFFLINK_API_KEY=ck_your-api-key-here

# Or set it in Cufflink.toml per environment
# [environments.staging]
# api_key_env = "CUFFLINK_STAGING_API_KEY"
```

---

## Guide 32: Multi-Replica Deployment

Running multiple Cufflink platform instances behind a load balancer for high availability and horizontal scaling.

### Architecture

In a multi-replica deployment, all platform instances are **stateless** and share:

- **PostgreSQL** — single source of truth for all data
- **NATS** — event bus and inter-replica coordination
- **Redis** — distributed rate limiting
- **S3-compatible storage** — file and WASM binary storage

```
                    ┌─────────────────┐
                    │  Load Balancer  │
                    └────────┬────────┘
               ┌─────────┬──┴──┬─────────┐
               ▼         ▼     ▼         ▼
          ┌─────────┐┌─────────┐┌─────────┐
          │Platform ││Platform ││Platform │
          │Replica 1││Replica 2││Replica 3│
          └────┬────┘└────┬────┘└────┬────┘
               │         │          │
    ┌──────────┼─────────┼──────────┼──────────┐
    │          ▼         ▼          ▼          │
    │  ┌────────────┐ ┌──────┐ ┌───────┐ ┌──┐ │
    │  │ PostgreSQL │ │ NATS │ │ Redis │ │S3│ │
    │  └────────────┘ └──────┘ └───────┘ └──┘ │
    └──────────────────────────────────────────┘
```

### What makes it safe

| Concern | Solution |
|---------|----------|
| File storage | S3-compatible storage (all replicas read/write the same bucket) |
| WASM modules | Persisted to S3, lazy-loaded on cache miss after restart or on new replica |
| Service registry | In-memory cache + NATS pub/sub invalidation (deploy on replica A notifies B and C to reload) |
| Webhook dispatch | NATS queue group ensures each event processed by exactly one replica |
| Deployment races | PostgreSQL advisory locks prevent concurrent deploys to the same service |
| Rate limiting | Redis-backed counter shared across all replicas |
| JWKS cache | Shared per-instance (each replica caches independently, which is fine) |

### S3 storage configuration

Cufflink supports any S3-compatible object storage (and Google Cloud Storage via `STORAGE_BACKEND=gcs` + `GCS_BUCKET`). Set these environment variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `STORAGE_BACKEND` | `s3` (default when unset) or `gcs` | `s3` |
| `S3_ENDPOINT` | Storage endpoint URL | `https://hel1.your-objectstorage.com` |
| `S3_REGION` | Storage region | `eu-central-1` |
| `S3_ACCESS_KEY` | Access key ID | `your-access-key` |
| `S3_SECRET_KEY` | Secret access key | `your-secret-key` |
| `S3_BUCKET` | Bucket name | `cufflink` |
| `OBJECT_STORE_RETRY_TIMEOUT_SECS` | Max wall-clock budget, across retries, for one storage op. Bounded under the deploy gateway timeout so a provider outage fails deploys fast instead of holding the deploy lock. Applies to S3 and GCS. | `20` (default) |

**Hetzner Object Storage:**
```bash
S3_ENDPOINT=https://hel1.your-objectstorage.com
S3_REGION=eu-central-1
S3_ACCESS_KEY=your-hetzner-access-key
S3_SECRET_KEY=your-hetzner-secret-key
S3_BUCKET=cufflink
```

**RustFS (local development, as shipped in `docker-compose.yml`):**
```bash
STORAGE_BACKEND=s3
S3_ENDPOINT=http://localhost:9000
S3_REGION=us-east-1
S3_ACCESS_KEY=rustfsadmin
S3_SECRET_KEY=rustfsadmin
S3_BUCKET=cufflink
```

When none of the `S3_*` variables are set (and `STORAGE_BACKEND` is not `gcs`), the platform starts without object storage: file uploads and WASM deploys are unavailable, but CRUD services still work. That is only suitable for a single-instance dev setup.

### Docker Compose (local dev)

The default `docker-compose.yml` includes RustFS (an S3-compatible store) for object storage and Redis for rate limiting:

```bash
docker compose up -d
```

This starts: PostgreSQL, NATS, Keycloak (with its own Postgres), Redis, RustFS (plus a one-shot `bucket-init` that creates the `cufflink` bucket), the platform, the WASM worker, a one-shot `tenant-init` that registers the `acme` tenant, the web-runner, the ext-runner, and the monitoring stack (OTel collector, Tempo, Loki, Prometheus, Grafana).

The RustFS console is available at `http://localhost:9001` (rustfsadmin/rustfsadmin).

### Docker Swarm deployment

```yaml
# docker-compose.prod.yml
services:
  platform:
    image: kingsleyh/cufflink-platform:latest
    environment:
      DATABASE_URL: postgres://cufflink:secret@postgres:5432/cufflink
      NATS_URL: nats://nats:4222
      KEYCLOAK_URL: http://keycloak:8080
      KEYCLOAK_REALM: cufflink
      KEYCLOAK_ISSUER: https://auth.example.com
      STORAGE_BACKEND: s3
      S3_ENDPOINT: https://hel1.your-objectstorage.com
      S3_REGION: eu-central-1
      S3_ACCESS_KEY: your-access-key      # or S3_ACCESS_KEY_FILE: a mounted secret's path (deployment.md, "Secrets as files")
      S3_SECRET_KEY: your-secret-key
      S3_BUCKET: cufflink
      REDIS_URL: redis://redis:6379
      RATE_LIMIT_ENABLED: "true"
    deploy:
      replicas: 3
      update_config:
        parallelism: 1
        delay: 10s
      restart_policy:
        condition: on-failure
```

### Redis configuration

Redis is used for distributed rate limiting. Set:

```bash
REDIS_URL=redis://redis:6379
```

Rate limiting is off unless `RATE_LIMIT_ENABLED=true`; the limits are per client IP, `RATE_LIMIT_PER_SECOND` (default 20) sustained with `RATE_LIMIT_BURST` (default 60). When Redis is not configured, or a Redis check fails, each replica limits independently with its own in-memory buckets at the same per-IP settings — so with three replicas a single client may get up to three times the configured rate.

### WASM Worker Pool

For compute-heavy WASM handlers that would block platform CRUD throughput, deploy dedicated worker processes that scale independently:

```bash
# Scale workers separately from the platform
docker service scale cufflink_platform=3
docker service scale cufflink_wasm-worker=5
```

Workers connect to the same NATS cluster and pick up WASM jobs via queue groups. See [WASM Workers](wasm-workers.md) for full configuration and deployment details.

### Graceful behavior

- **No object storage configured**: Platform starts, but file uploads and WASM deploys are unavailable (single-instance dev only)
- **No Redis configured**: Falls back to the in-memory rate limiter (when rate limiting is enabled)
- **Redis goes down**: Automatically falls back to the in-memory rate limiter; Redis never turns a request into a rejection on its own
- **NATS registry sync**: If a notification is missed, the registry is still correct after the next deploy or restart (loaded from DB at startup)

---

## Guide 33: Observability & Monitoring

Full observability stack with Grafana, Prometheus, Loki, Tempo, and OpenTelemetry Collector. This guide is the tour; the contract every signal follows, the settings and the metrics reference are in [observability](observability.md).

### Architecture

```
  ┌──────────────────────────────────────────────────────┐
  │                  Cufflink Platform                    │
  │  ┌────────────┐  ┌────────────┐  ┌────────────────┐ │
  │  │  tracing    │  │  metrics   │  │ JSON logs      │ │
  │  │  (OTel)     │  │(Prometheus)│  │ (tracing-sub)  │ │
  │  └──────┬─────┘  └──────┬─────┘  └───────┬────────┘ │
  └─────────┼───────────────┼─────────────────┼──────────┘
            │               │                 │
     OTLP HTTP        Scrape /metrics    OTLP HTTP
            │               │                 │
            ▼               │                 ▼
  ┌──────────────────┐      │     ┌──────────────────┐
  │  OTEL Collector  │──────┼────▶│  OTEL Collector  │
  └────┬────────┬────┘      │     └────┬────────┬────┘
       │        │           │          │        │
  Traces    Logs            │     Traces    Logs
       │        │           │          │        │
       ▼        ▼           ▼          ▼        ▼
  ┌────────┐ ┌──────┐ ┌───────────┐
  │ Tempo  │ │ Loki │ │Prometheus │
  └────┬───┘ └──┬───┘ └─────┬─────┘
       │        │            │
       └────────┼────────────┘
                ▼
          ┌──────────┐
          │ Grafana  │
          └──────────┘
```

### Quick start

The monitoring stack is included in the default `docker-compose.yml`:

```bash
docker compose up -d
```

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3001 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| NATS Monitor | http://localhost:8222 | — |
| RustFS Console | http://localhost:9001 | rustfsadmin / rustfsadmin |

### What's collected

**Metrics** (Prometheus scrapes `/metrics` every 15s):

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `cufflink_crud_requests_total` | counter | `tenant`, `service`, `operation`, `table` | CRUD operations (`list`, `create`, `get`, `update`, `delete`, `batch`) |
| `cufflink_deployments_total` | counter | `result`, `mode` | Deploy attempts by outcome (`attempted`, `success`, `rejected`, …) and service mode |
| `cufflink_webhooks_delivered_total` | counter | `event` | Successful webhook deliveries |
| `cufflink_webhooks_failed_total` | counter | `event` | Failed webhook deliveries |
| `cufflink_rate_limited_total` | counter | — | Requests rejected by the rate limiter (when `RATE_LIMIT_ENABLED=true`, or unset under the hardened HTTP profile) |
| `cufflink_services_total` | gauge | — | Currently active services |

The full list is in [observability](observability.md#metrics). Request rates, errors and latency of each tenant's service come from the spans: Tempo writes them to Prometheus as [span metrics](observability.md#span-metrics).

**Traces** (OpenTelemetry → Tempo):
- HTTP request spans (method, route name, status, latency); a request for a tenant's service is named `{tenant}/{service}/{route}`, never by its path
- Distributed trace context across service calls
- Viewable in Grafana → Explore → Tempo

**Logs** (OpenTelemetry → Loki):
- Each line under the tenant and service it was logged for, or under the binary
- Searchable by service, level, message content
- Linked to traces via `trace_id`, which Loki holds beside the line (structured metadata)
- `cufflink logs` reads the same lines for the service of the current directory

### Pre-built dashboards

Three dashboards are provisioned in Grafana ([observability](observability.md#dashboards)):

- **Tenants → Tenant services**: each service of a tenant with its version, requests, errors, latency, logs and traces
- **Tenants → Service**: one service by handler, with its subscriptions, schedules, logs and traces
- **Platform → Cufflink platform**: the platform, the worker and the runners themselves

### Querying logs in Loki

Open Grafana → Explore → select **Loki** datasource.

```logql
# A tenant's service, whichever binary ran it
{service_namespace="acme", service_name="notes"}

# Its errors only
{service_namespace="acme", service_name="notes"} | detected_level="error"

# What the platform logs outside any service
{service_name="cufflink-platform"}
```

### Viewing traces in Tempo

Open Grafana → Explore → select **Tempo** datasource.

- Search by service name: a tenant's service (`{resource.service.namespace="acme" && resource.service.name="notes"}`), or `cufflink-platform` for what belongs to no service
- Filter by HTTP method, status code, or duration
- Click a trace to see the full span waterfall
- Click "Logs for this span" to jump to correlated Loki logs

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (none) | OTLP HTTP endpoint for traces + logs. Set to `http://otel-collector:4318` in docker-compose |
| `RUST_LOG` | `info,cufflink_platform=debug` | Log level filter |
| `OTEL_RESOURCE_ATTRIBUTES` | (none) | Added to what every signal carries; name the environment with `deployment.environment.name=<env>` (`dev` in docker-compose) |
| `LOKI_URL` | (none) | The Loki `cufflink logs` reads, set on the platform |
| `LOKI_LABEL_MODEL` | `legacy` | How `cufflink logs` finds a service's lines; `scoped` with the collector and Loki of `infra/monitoring`, as docker-compose sets it ([observability](observability.md#loki_label_model)) |
| `CUFFLINK_CHILD_TELEMETRY` | `legacy` | On the runners: `scoped` makes a web app or native service with an OpenTelemetry SDK report as its service ([deployment](deployment.md#childrens-telemetry)) |
| `CUFFLINK_CHILD_OUTPUT_LEVEL` | `debug` | On the runners: `info` logs what apps and native services print, under their service |

A line or span outside a tenant's service is the binary's, `cufflink-platform` (there is no `OTEL_SERVICE_NAME` override); inside one, the collector of `infra/monitoring` names it as the tenant's service ([whose telemetry it is](observability.md#whose-telemetry-it-is)). When `OTEL_EXPORTER_OTLP_ENDPOINT` is not set, the platform falls back to plain JSON logging to stdout (no traces or logs sent to the collector).

### Production notes

For production on any cloud provider:

1. Vendor `infra/monitoring/` as it is at the tag of the release you run, and keep your own scrape targets, labels, storage and alert rules in files beside it ([vendoring](observability.md#vendoring)). The files name the compose stack's hosts (`tempo`, `loki`, `prometheus`, `platform`, `wasm-worker`)
2. Consider external Grafana Cloud or a dedicated Grafana instance with persistent storage
3. For high-volume deployments, switch Loki and Tempo to S3-backed storage instead of local filesystem
4. The compose stack is one developer's: its collector is on the runner network and its ports are published. Where more than one tenant's code runs, read [who can write a tenant's lines](observability.md#who-can-write-a-tenants-lines) before setting `LOKI_LABEL_MODEL=scoped`: each tenant's runners need a collector of their own, each tenant's `egress` policy must be enforced, and the collector, Loki, Tempo and Prometheus must be on private addresses or behind authentication, since an enforced `egress` refuses only internal addresses
5. Add Prometheus alerting rules. A minimal starting point:

```yaml
# prometheus-alerts.yml
groups:
  - name: cufflink
    rules:
      - alert: CufflinkPlatformDown
        expr: up{job="cufflink-platform"} == 0
        for: 2m
        labels: { severity: critical }
        annotations: { summary: "Cufflink platform scrape failing" }

      - alert: CufflinkDeployFailures
        expr: increase(cufflink_deployments_total{result="rejected"}[15m]) > 0
        for: 0m
        labels: { severity: warning }
        annotations: { summary: "Deploys rejected in the last 15 minutes" }

      - alert: CufflinkWebhookFailures
        expr: rate(cufflink_webhooks_failed_total[5m]) > 0.1
        for: 10m
        labels: { severity: warning }
        annotations: { summary: "Sustained webhook delivery failures" }
```

Reference it from `prometheus.yml` with `rule_files: ["prometheus-alerts.yml"]`; the `job` label must match the scrape job name in your config.
