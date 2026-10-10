# Audit Logging in Cufflink

Cufflink automatically publishes audit events for all CRUD operations and custom route invocations. A dedicated audit-service can subscribe to these events and store a complete audit trail.

There are two audit mechanisms, and it is worth keeping them apart:

| Mechanism | What it records | Where it lives | Who reads it |
|-----------|-----------------|----------------|--------------|
| **Data audit events** (this page, mostly) | Every CRUD write and every `_fn` custom-route call | NATS JetStream, subject `dw.{tenant_slug}.logs` | Your own audit-service subscription |
| **Platform audit log** | Control-plane actions — today only `deploy` | `audit_log` table in the platform DB | `GET /api/audit` |

## How It Works

```
Client ──> Platform ──> CRUD/WASM Handler
                │
                ├──> NATS: dw.{tenant}.{service}.{table}.{action}  (domain event)
                │
                └──> NATS: dw.{tenant}.logs                         (audit event)
                           │
                           ▼
                     audit-service (JetStream consumer on DW_{TENANT})
                           │
                           ▼
                     audit_logs table
```

### Automatic Publishing

The platform automatically publishes audit events to `dw.{tenant_slug}.logs` after:

1. **CRUD operations** — create, update, delete on any table (`platform/src/router/crud.rs`)
2. **Custom WASM routes** — any `_fn` handler invocation (`platform/src/router/custom/`)

No configuration needed. The subject sits under the tenant's `dw.{tenant}.>`
wildcard, so it lands on the per-tenant JetStream stream **`DW_{TENANT}`**
(slug upper-cased, `-` → `_`), which the platform creates when the tenant is set
up (`platform/src/nats_manager/events.rs`):

- **Retention: Interest.** A message is kept only while a consumer is interested
  in it. If no service subscribes to `dw.{tenant}.logs`, audit events are dropped
  immediately — there is no backlog waiting for an audit-service you deploy later.
- **Max age: 7 days.** Even with a lagging consumer, events older than seven days
  are discarded.

Deploy your audit-service *before* you need the history.

### Audit Event Payload

```json
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "tenant": "acme",
  "service": "stock-service",
  "table": "items",
  "action": "create",
  "user_id": "keycloak-subject-id",
  "user_name": "Jane Doe",
  "record_id": "550e8400-e29b-41d4-a716-446655440000",
  "data": { "id": "...", "name": "Widget", "status": "active" },
  "previous": null
}
```

| Field | Description |
|-------|-------------|
| `timestamp` | When the event occurred (ISO 8601) |
| `tenant` | Tenant slug from the URL |
| `service` | Service name |
| `table` | Table name (null for custom routes) |
| `action` | `create`, `update`, `delete`, or `custom:{handler_name}` |
| `user_id` | Keycloak subject ID (or the API key's subject), or `"anonymous"` |
| `user_name` | User's display name, or `""` when unknown (API keys, anonymous callers) |
| `record_id` | Primary key of the affected record (null for custom routes) |
| `data` | New data (create/update), the request body (custom routes), or null |
| `previous` | Previous data (delete) or null |

For an **unauthenticated** `_fn` call the body is not recorded: `data` is
`{"redacted": "unauthenticated request body not recorded"}`. There is nobody to
attribute the action to, and public-route bodies are routinely third-party PII
(a payment webhook carries customer emails and amounts), so the entry is kept
for forensics but the payload is dropped.

## Building an Audit Service

Create a Cufflink WASM service that subscribes to audit events:

### Service Definition

```rust
cufflink::service! {
    name: "audit-service",
    mode: wasm,
    tables: [AuditLog],
    custom_routes: [
        ("GET", "/logs/search",             "handle_search_logs", perm("audit", "view")),
        ("GET", "/logs/user/:user_id",      "handle_user_logs",   perm("audit", "view")),
        ("GET", "/logs/record/:record_id",  "handle_record_logs", perm("audit", "view")),
    ],
    subscriptions: [
        ("dw.{tenant}.logs", "handle_audit_event", wasm),
    ],
    authorization: [
        areas: [("audit", ["view"])],
        default_roles: [("admin", "Audit access", ["audit:view"])],
    ],
}
```

Every custom route declares a posture (`perm("audit", "view")` here — audit
data is sensitive, so don't leave these routes in legacy unenforced mode; see
[authorization.md](authorization.md#custom-route-postures)).

Subscription tuples are `(subject, handler, wasm)` with an optional fourth
element naming the JetStream **stream** to consume from. `{tenant}` in the subject
is replaced with the tenant slug at deploy time. When the stream is omitted the
consumer is created on the tenant stream `DW_{TENANT}`, which is exactly where
`dw.{tenant}.logs` lands, so the three-element form is enough here. Pass the
fourth element when subscribing to a subject that another stream captures:

```rust
subscriptions: [
    ("dw.{tenant}.logs", "handle_audit_event", wasm, "DW_ACME"),
],
```

### Table Schema

```rust
#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "audit_logs")]
pub struct AuditLog {
    #[key]
    pub id: Uuid,
    pub tenant: String,
    pub service: String,
    pub table_name: Option<String>,
    pub action: String,
    pub user_id: String,
    pub user_name: Option<String>,
    pub record_id: Option<String>,
    pub data: Option<serde_json::Value>,
    pub previous: Option<serde_json::Value>,
    pub event_time: DateTime<Utc>,
    #[timestamp]
    pub created_at: DateTime<Utc>,
}
```

### NATS Subscription Handler

The handler receives audit events from NATS and inserts them into the database:

```rust
handler!(handle_audit_event, |req: Request| {
    let body = req.body();
    let id = uuid::Uuid::new_v4();

    db::execute(&format!(
        "INSERT INTO audit_logs (id, tenant, service, table_name, action, user_id, ...)
         VALUES ('{}', '{}', '{}', ...)",
        id,
        body["tenant"].as_str().unwrap_or(""),
        body["service"].as_str().unwrap_or(""),
        // ... other fields
    ));

    Response::empty()
});
```

### Query Endpoints

**Search logs:**
```
GET /svc/{tenant}/audit-service/_fn/logs/search?service=stock-service&action=create&from=2024-01-01
```

**User activity:**
```
GET /svc/{tenant}/audit-service/_fn/logs/user/{keycloak_sub}
```

**Record history:**
```
GET /svc/{tenant}/audit-service/_fn/logs/record/{record_uuid}
```

## Deployment

1. Deploy the audit-service like any other Cufflink service:
   ```bash
   cd backend/audit-service
   cufflink deploy
   ```

2. The platform automatically publishes events — no other changes needed.

3. The durable consumer for `dw.{tenant}.logs` on `DW_{TENANT}` is created
   automatically during deployment. From that moment the stream retains events
   for the consumer (up to 7 days); anything published before has already been
   dropped.

## Platform Audit Log (`GET /api/audit`)

Independently of the NATS events, the platform keeps a small control-plane audit
table (`audit_log`) in its own database (`platform/src/repo/audit.rs`). Today the
only action written is **`deploy`**: one row per successful deployment, with

| Column | Value |
|--------|-------|
| `actor` | The deployer's subject |
| `action` | `deploy` |
| `resource_type` | `service` |
| `resource_id` | The service id |
| `details` | `{"version": <int>, "schema_changes": <int>}` |
| `ip_address` | Not recorded (null) |

Query it with `GET /api/audit` (`platform/src/api/audit.rs`). It requires
**DeployAuth** and is scoped to the caller's tenant.

| Query parameter | Meaning |
|-----------------|---------|
| `page` | 1-based page number (default 1) |
| `per_page` | Rows per page (default 50, max 200) |
| `action` | Exact match on `action` |
| `resource_type` | Exact match on `resource_type` |
| `actor` | Exact match on `actor` |

```bash
curl "http://localhost:8080/api/audit?action=deploy&per_page=20" \
  -H "Authorization: ApiKey ck_..."
```

Response, newest first:

```json
{
  "entries": [
    {
      "id": "…",
      "tenant_id": "…",
      "actor": "keycloak-subject-id",
      "action": "deploy",
      "resource_type": "service",
      "resource_id": "…",
      "details": { "version": 3, "schema_changes": 1 },
      "ip_address": null,
      "created_at": "2024-01-15T10:30:00+00:00"
    }
  ],
  "pagination": { "total": 1, "page": 1, "per_page": 20, "total_pages": 1 }
}
```

### SQL console audit

Queries run through the SQL console (`POST /api/services/{id}/query`,
`cufflink sql`) are not written to `audit_log`. Instead each one is published to
JetStream on **`cufflink.audit.sql.{tenant_slug}`** before execution — the intent
is recorded whether or not the query succeeds (`platform/src/api/query.rs`):

```json
{
  "action": "sql.query",
  "actor": "keycloak-subject-id",
  "tenant_slug": "acme",
  "service_name": "stock-service",
  "service_id": "…",
  "sql": "SELECT count(*) FROM items"
}
```

This subject is not under `dw.{tenant}.>`, so it does not land on `DW_{TENANT}`,
and the platform creates no stream for it. Until a JetStream stream covering
`cufflink.audit.sql.>` exists the publish only logs a warning and the event is
lost. Create one, then subscribe with the explicit fourth stream element if you
want SQL activity in the same trail.

## Notes

- Audit events are published **after** the operation completes (not before), so they always reflect the actual state change.
- Custom route audit events use `custom:{handler_name}` as the action and include the request body as `data` — redacted when the caller is unauthenticated.
- The `previous` field is populated for delete operations, containing the record data before deletion.
- Audit event publishing is fire-and-forget — a failed publish does not affect the original operation.
- Interest retention means an audit-service outage longer than the 7-day max age loses events; keep the consumer healthy.
