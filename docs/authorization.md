# Authorization in Cufflink

Cufflink provides a layered authorization system: built-in platform-level RBAC for CRUD endpoints and custom routes, plus a Redis-backed permission cache that dedicated authorization services can leverage.

## Architecture Overview

```
                           ┌─────────────┐
                           │  Keycloak   │
                           │  (JWT/OIDC) │
                           └──────┬──────┘
                                  │ JWT
┌─────────────┐            ┌──────▼──────┐          ┌─────────┐
│   Client     │──request──>│  Platform   │──cache──>│  Redis  │
└─────────────┘            └──────┬──────┘          └─────────┘
                                  │                       ▲
                           ┌──────▼──────┐          ┌─────┴─────┐
                           │  WASM svc   │──r/w────>│ auth-svc  │
                           │ (your code) │          │  (WASM)   │
                           └─────────────┘          └───────────┘
```

## Who Can Do What: the Extractors

Every platform route is guarded by one of four Axum extractors
(`platform/src/auth/extractor.rs`). Knowing which one a route uses tells you what
credential it needs.

| Extractor | Accepts | Used by |
|-----------|---------|---------|
| `AuthUser` | Any valid tenant credential: tenant-realm JWT, tenant API key, or a platform admin (JWT/API key) with `X-Tenant-Slug` | Data routes (bound to the route's tenant, see [tenant binding](policies.md)), jobs, OpenAPI, RBAC **reads** (`GET .../roles`, `.../permission-areas`, `.../users/{id}/permissions`) |
| `DeployAuth` | `AuthUser` **with deploy authority**: the tenant's deploy role on a JWT, a tenant API key, a platform admin, or a service account | Deploy, services list/get/delete/enable/disable, logs, config, backups/restore/schema, files, seed, SQL console, WASM/web upload, `GET /api/audit`, `POST /api/auth/api-keys` |
| `RbacAdmin` | Deploy authority **or** the `roles:manage` permission | RBAC **writes**: create/update/delete role, assign/unassign; tenant user search |
| `PlatformAdmin` | Platform-realm JWT holding `CUFFLINK_PLATFORM_ADMIN_ROLE` (default `cufflink-platform-admin`) or a platform admin API key; every call is audited | `/api/tenants*` (including policies), `/api/platform/api-keys*`, `/api/users/lookup` |

Webhook management (`/api/services/{id}/webhooks*`) needs deploy authority or
the `webhooks:manage` permission.

`OptionalAuth` is the anonymous-tolerant variant of `AuthUser` used by public
CRUD routes and `/api/auth/me`: no `Authorization` header means anonymous, but a
header that **fails** validation (expired token, wrong realm, bad key) is a `401`,
never silently downgraded to anonymous. The only routes that tolerate an invalid
credential are custom routes with the `public` posture or no posture at all.

Deploy authority is deliberately *not* required for tenant data. A portal user with
a valid tenant JWT reaches `/svc/...` and is authorized per route by the table's
`permission_area` / `owner_self_service`, the custom route's posture, or the
handler's own `auth.can` checks. RBAC management is likewise an app-admin function
(`roles:manage`), not a tenant-operator one — deploy authority is accepted there
only so the bootstrap deployer can provision the first admins.

## Built-In Platform RBAC

### Declaring Permissions

In your service manifest (`src/main.rs`), declare permission areas and default roles:

```rust
cufflink::service! {
    name: "my-service",
    mode: wasm,
    tables: [Item, Order],
    authorization: [
        areas: [
            ("items", ["create", "view", "edit", "delete"]),
            ("orders", ["create", "view", "edit", "cancel"]),
        ],
        default_roles: [
            ("admin", "Full access", ["items:*", "orders:*", "roles:manage"]),
            ("manager", "Manage items", ["items:*", "orders:view"]),
            ("viewer", "Read-only", ["items:view", "orders:view"]),
        ],
    ],
}
```

### Protecting Tables

Add `permission_area` to any table that needs permission checks:

```rust
#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "items", auth_required, permission_area = "items")]
pub struct Item { ... }
```

When `permission_area = "items"` is set, the platform automatically checks permissions:

| Operation | Required Permission |
|-----------|-------------------|
| `GET /svc/.../items` | `items:view` |
| `POST /svc/.../items` | `items:create` |
| `PUT /svc/.../items/:id` | `items:edit` |
| `DELETE /svc/.../items/:id` | `items:delete` |

A [private table](schema-ownership.md#private-tables) is outside this: the
platform serves no route for it, so there is nothing for `auth_required`,
`permission_area` or `owner_self_service` to guard, and a deploy that
declares one of them on a private table is refused (a `permission_area`
equal to the table's `scoped_by`, which it must be where both are set, is
the exception). Access to its rows is what the service's own routes and
their postures allow.

### Owner-Scoped Tables

Two more `#[table(...)]` attributes give you row-level security without a
permission grant (`sdk-macros/src/table_derive.rs`, `platform/src/router/crud.rs`):

```rust
#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "todos", owner_field = "user_id", owner_self_service)]
pub struct Todo {
    #[key] pub id: Uuid,
    pub user_id: String,   // or Uuid — see below
    pub title: String,
}
```

| Attribute | Effect |
|-----------|--------|
| `owner_field = "col"` | Every auto-CRUD handler scopes the operation to rows where `col` equals the caller's subject: `POST` sets `col` to the subject (a client-supplied value is overwritten), `GET` list/detail add `col = subject` to the `WHERE`, `PUT`/`DELETE` only touch the caller's own rows (others come back `404`). Reads, updates and deletes with no principal are `401` even without `auth_required`; an anonymous `POST` merely skips the column, so pair `owner_field` with `auth_required` or `owner_self_service`. `col` must be a declared column; if it is a `Uuid` the subject must be a UUID or the request is rejected. |
| `owner_self_service` | Declares that ownership **is** the authorization: the `permission_area` check is skipped for auto-CRUD on this table, and anonymous access is refused. Only meaningful together with `owner_field` — without one there is nothing to scope by, so the flag is ignored and the permission check still applies. |

Grants can also carry a scope (`items:view@own`, `@field(column=claim)`, `@all`) that decides which rows they reach on a table declaring `scoped_by`; see [scoped permissions](scoped-permissions.md).

A column can ask a grant of its own (`#[restricted(read = "internal")]` asks `items:internal`), so a caller that reaches a row is answered it without the columns it is not granted; see [column permissions](column-permissions.md).

Use `owner_field` alone when staff with a permission grant should still be limited
to their own rows; add `owner_self_service` for customer-facing tables where every
authenticated user manages their own records. Cross-owner access (an admin reading
everyone's todos) does not flow through owner-scoped CRUD — write a custom route
with a `perm(...)` posture for it.

### How Permission Resolution Works

1. User makes a request with `Authorization: Bearer <jwt>` (or an API key)
2. Platform validates the JWT against the tenant's Keycloak realm
3. Platform resolves permissions by querying `user_roles JOIN roles` in the platform DB
4. **Redis cache**: the resolved `{permissions, role_names}` are cached at `auth:perms:{tenant_id}:{user_subject}` for 5 minutes (`platform/src/auth/permissions.rs`)
5. Platform checks if the user has the required `area:operation` permission
6. Wildcard support: `items:*` grants all operations in the `items` area; `*` grants everything

### Permission Format

Permissions follow the `area:operation` format:

- `items:create` — specific permission
- `items:*` — wildcard: all operations in the "items" area
- `*` — super-wildcard: all permissions
- `roles:manage` — reserved by the platform: lets a user create, edit, delete and assign roles through the RBAC API (`RbacAdmin`)

### Default Role Seeding

When a service is deployed, the platform seeds the manifest's `default_roles`
(`platform/src/api/deployments.rs`, `seed_default_roles`):

- Roles are unique per `(tenant, name)`. If a role already exists, its permissions
  are **merged** (set union), never overwritten, so multiple services can
  contribute permissions to the same role name and a redeploy never drops a
  permission an admin added by hand.
- If the manifest defines a role named **`admin`**, the deploying user is
  automatically assigned to it (skipped for bootstrap API-key identities). This is
  the only automatic assignment the platform makes — nothing happens at login.
- Everything else is assigned explicitly: `cufflink roles assign --user … --role …`
  or `POST /api/tenants/{tenant_id}/roles/assign`.

### Tenant-Level Permission Areas

The platform aggregates permission areas across all services for a tenant:

```
GET /api/tenants/{tenant_id}/permission-areas
```

Returns all areas from all deployed services, with a `service` field indicating which service declared each area.

## Custom Route Postures

Every entry in `custom_routes` should carry a **posture** — a fourth tuple element
that declares who may call the route. The platform enforces it in one place
(`platform/src/router/postures/mod.rs`, `authorize_route`) before your handler
runs, so a guest handler cannot forget an auth check.

```rust
cufflink::service! {
    name: "orders-service",
    mode: wasm,
    tables: [Order],
    custom_routes: [
        ("POST", "/webhooks/payments", "handle_payment_webhook", public("signed payment-provider webhook")),
        ("GET",  "/me/orders",         "handle_my_orders",       owner("customer_id")),
        ("POST", "/orders/:id/cancel", "handle_cancel_order",    perm("orders", "cancel")),
        ("GET",  "/profile",           "handle_profile",         auth),
    ],
}
```

| Posture | Requirement | Invalid credential |
|---------|-------------|--------------------|
| `public("reason")` | None; the caller may be anonymous. The reason is mandatory so every public route is greppable and reviewable. | Tolerated — treated as anonymous |
| `auth` | Any authenticated tenant principal | `401` |
| `perm("area", "op")` | Authenticated **and** holds `area:op` (wildcards apply; service accounts pass) | `401` / `403 Missing permission: area:op` |
| `owner("field")` | Authenticated. The platform only enforces the principal; **your handler** must scope its queries to rows whose `field` equals `auth.sub`. | `401` |
| *(none)* | **Legacy, unenforced.** The route behaves like `public`, the platform logs `Custom route has no declared posture — running in legacy (unenforced) mode.` and increments `cufflink_route_posture_undeclared_total`. | Tolerated |

A route may declare at most one posture. Declaring any posture adds
`route_posture` to the manifest's required platform features, so an old platform
that would ignore it refuses the deploy instead of silently exposing the route.
`cufflink routes` (add `--all-services` for every deployed service) lists each
route with its posture so you can find the undeclared ones.

### Permission checks inside handlers

Postures cover the common cases; for anything finer, use `auth.can(area, operation)`
in the handler. Return **403** for a missing permission (`Response::forbidden`),
not `Response::error`, which is a 400:

```rust
handler!(handle_cancel_order, |req: Request| {
    let auth = match req.require_auth() {
        Ok(a) => a,
        Err(resp) => return resp,   // 401 Authentication required
    };

    if !auth.can("orders", "cancel") {
        return Response::forbidden("Missing orders:cancel permission");
    }

    // ... cancel logic
    Response::json(&json!({"ok": true}))
});
```

### The `Auth` struct in guest code

`req.auth()` / `req.require_auth()` return the caller's identity as the platform
resolved it (`cufflink-fn/src/auth.rs`):

| Field / method | Meaning |
|----------------|---------|
| `sub` | Keycloak subject (or the API key's tenant subject) |
| `preferred_username`, `name`, `email` | Profile claims from the token; `None` for API keys |
| `realm_roles` | Keycloak realm roles from the token (empty for API keys) |
| `claims` | All remaining JWT claims (`auth.claim("azp")`) |
| `permissions` | Cufflink permissions resolved from the tenant's roles, e.g. `["items:*", "orders:view"]` |
| `role_names` | Cufflink role names assigned to the user, e.g. `["admin"]` |
| `is_service_account` | `true` when the token came from an allowlisted machine client (see below) |
| `has_role(r)` | Keycloak realm role check |
| `has_cufflink_role(r)` | Cufflink role-name check |
| `can(area, op)` | Permission check with `area:*` and `*` wildcards |

`permissions` and `role_names` are populated for `_fn` custom routes and for table
hooks (`before_create` etc.). They are resolved from the tenant that owns the
service and go through the same 5-minute Redis cache as CRUD.

## Service Accounts

Keycloak service accounts (client credentials grant) bypass per-user permission
checks: `permission_area` enforcement on CRUD, `perm(...)` postures on custom
routes, and `RbacAdmin`. Because that is a full RBAC bypass, detection is
**fail-closed and allowlist-based** (`platform/src/auth/extractor.rs`,
`platform/src/auth/service_accounts.rs`):

- A token is a service account **only if** its `azp` (authorized party — the
  client id Keycloak minted it for) is on the tenant's `service_account_clients`
  (`cufflink tenants update <slug> --service-account-clients a,b`), or — while
  the tenant's `service_accounts` policy is `off` or `warn` — in the
  platform-wide list built from `CUFFLINK_SERVICE_ACCOUNT_CLIENTS`
  (comma-separated) plus `CUFFLINK_DEFAULT_KC_OPS_CLIENT_ID`. A platform-list
  match is recorded under `warn` so it can be moved onto the tenant first.
- `azp` is not user-controllable: obtaining a token with a given `azp` requires
  authenticating as that confidential client.
- `preferred_username` is **deliberately ignored**. A `service-account-*` username
  is user data — anywhere self-registration or username edits are allowed, a
  human could set it and gain the bypass.
- Merely having an `azp` claim means nothing; every token has one. A token from
  a human-login client (portal, backoffice) that is not on the allowlist goes
  through normal RBAC.
- An empty allowlist denies everyone the bypass.

```bash
# platform environment
CUFFLINK_SERVICE_ACCOUNT_CLIENTS=acme-ops,billing-sync
CUFFLINK_DEFAULT_KC_OPS_CLIENT_ID=acme-ops          # always allowlisted
CUFFLINK_DEFAULT_KC_OPS_CLIENT_SECRET=...
```

Never list a client that humans log in with. In guest code:

```rust
if auth.is_service_account {
    // Full access — this is a trusted machine client
}
```

## Building a Dedicated Auth Service

For complex authorization needs (cross-service roles, fine-grained permissions, custom hierarchies), build a dedicated authorization service as a Cufflink WASM service.

### Table Design

```rust
cufflink::service! {
    name: "auth-service",
    mode: wasm,
    tables: [Role, Permission, RolePermission, UserRole],
    custom_routes: [
        ("GET",    "/users/:user_id/permissions",                "handle_get_user_permissions",         perm("authz", "view")),
        ("POST",   "/users/:user_id/roles",                      "handle_assign_role",                  perm("authz", "manage")),
        ("DELETE", "/users/:user_id/roles/:role_id",             "handle_unassign_role",                perm("authz", "manage")),
        ("POST",   "/roles/:role_id/permissions",                "handle_add_permission_to_role",       perm("authz", "manage")),
        ("DELETE", "/roles/:role_id/permissions/:permission_id", "handle_remove_permission_from_role",  perm("authz", "manage")),
        ("POST",   "/cache/invalidate",                          "handle_invalidate_cache",             perm("authz", "manage")),
        ("GET",    "/check",                                     "handle_check_permission",             auth),
    ],
    authorization: [
        areas: [("authz", ["view", "manage"])],
        default_roles: [("admin", "Authorization admin", ["authz:*"])],
    ],
}
```

**Tables:**

- `roles` — Role definitions (name, description)
- `permissions` — Permission definitions (area, operation)
- `role_permissions` — Join table: which permissions belong to which roles
- `user_roles` — Join table: which roles are assigned to which users

### Redis Write-Through Cache

The auth-service uses Redis to cache resolved permissions per user:

```rust
// Cache key format — use your OWN prefix (see warning below)
let key = format!("authsvc:perms:{}", user_id);

// On permission resolution: check cache first
if let Some(cached) = redis::get(&key) {
    return serde_json::from_str(&cached).unwrap_or_default();
}

// Cache miss: resolve from DB
let perms = resolve_from_db(user_id);

// Cache with 1-hour TTL
redis::set(&key, &serde_json::to_string(&perms).unwrap(), 3600);
```

> **Do not reuse the platform's key shape.** The platform owns
> `auth:perms:{tenant_id}:{subject}` and stores a
> `{"permissions": [...], "role_names": [...]}` object there with a 5-minute TTL.
> Writing your own payload under that prefix would be read back by the platform's
> CRUD and posture checks — and a plain `Vec<String>` fails to deserialize, which
> is treated as a cache miss but can also mask real grants until the TTL expires.
> Pick a distinct prefix such as `authsvc:perms:`.

**Cache invalidation** happens automatically when:
- A role is assigned/unassigned from a user → invalidate that user's cache
- A permission is added/removed from a role → invalidate all users with that role

### Checking Permissions From Other Services

**Option 1: HTTP call to auth-service**

```rust
let resp = http::get(
    &format!(
        "http://localhost:8080/svc/{}/auth-service/_fn/check?user_id={}&area=items&operation=edit",
        req.tenant(), user_id
    ),
    &[("Authorization", auth_header)],
);
```

**Option 2: Read Redis cache directly**

Since the auth-service writes to Redis, any service can read from Redis:

```rust
if let Some(perms_json) = redis::get(&format!("authsvc:perms:{}", user_id)) {
    let perms: Vec<String> = serde_json::from_str(&perms_json).unwrap_or_default();
    let has_perm = perms.iter().any(|p| p == "items:edit" || p == "items:*" || p == "*");
}
```

This is the preferred approach for low-latency checks — no HTTP overhead, just a Redis GET.

## Redis Host Functions

The `redis` module in `cufflink-fn` provides:

```rust
use cufflink_fn::prelude::*;

// Get a value
let value: Option<String> = redis::get("my:key");

// Set a value with TTL (seconds), use 0 for no expiry
redis::set("my:key", "value", 3600);

// Delete a key
redis::del("my:key");
```

Redis is optional. If not configured in the platform, `redis::get()` returns `None` and `redis::set()`/`redis::del()` are no-ops.

### Platform Configuration

Redis is configured only through the `REDIS_URL` environment variable on the
platform — there is no TOML setting:

```bash
REDIS_URL=redis://redis:6379
```

The platform uses it for two things: the permissions cache described above
(`auth:perms:*`, 5-minute TTL) and distributed rate limiting. Without it,
permissions are resolved from the database on every request and rate limits are
per-process.

## Frontend Integration

The platform exposes RBAC management endpoints:

```
GET    /api/tenants/{tenant_id}/permission-areas       — All permission areas        (AuthUser)
GET    /api/tenants/{tenant_id}/roles                  — List roles                  (AuthUser)
POST   /api/tenants/{tenant_id}/roles                  — Create role                 (RbacAdmin)
PUT    /api/tenants/{tenant_id}/roles/{id}             — Update role                 (RbacAdmin)
DELETE /api/tenants/{tenant_id}/roles/{id}             — Delete role                 (RbacAdmin)
POST   /api/tenants/{tenant_id}/roles/assign           — Assign role to user         (RbacAdmin)
DELETE /api/tenants/{tenant_id}/roles/unassign         — Unassign role from user     (RbacAdmin)
GET    /api/tenants/{tenant_id}/users/search?q=        — Search Keycloak users       (AuthUser)
GET    /api/tenants/{tenant_id}/users/{id}/permissions — User's permissions + roles  (AuthUser)
```

`RbacAdmin` means deploy authority *or* the `roles:manage` permission, so give
your admin role `roles:manage` and a backoffice role editor works for app admins
without handing them the deploy role.

For dedicated auth-service integration, call its custom routes via the standard service API (`/svc/{tenant}/auth-service/_fn/...`).
