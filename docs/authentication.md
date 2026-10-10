# Cufflink Authentication & Authorization Guide

## Overview

Cufflink uses a **dual-realm Keycloak model** with two distinct authentication paths:

- **Platform realm** — for platform administrators who manage tenants
- **Tenant realms** — for developers and end users within each tenant

```
┌──────────────────────────────────────────────────────────────┐
│                          Keycloak                            │
│                                                              │
│  ┌──────────────────────┐    ┌──────────────────────────┐    │
│  │  Platform realm      │    │  Tenant realm            │    │
│  │  ("cufflink")        │    │  (e.g. "acme")           │    │
│  │                      │    │                          │    │
│  │  Users:              │    │  Users:                  │    │
│  │   - Platform admins  │    │   - Developers           │    │
│  │                      │    │   - End users            │    │
│  │  Clients:            │    │  Clients:                │    │
│  │   - cufflink-cli     │    │   - cufflink-cli         │    │
│  │                      │    │                          │    │
│  │  Roles:              │    │  Roles:                  │    │
│  │   - cufflink-deploy  │    │   - cufflink-deploy      │    │
│  └──────────────────────┘    └──────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
             │                              │
             ▼                              ▼
   PlatformAdmin extractor          AuthUser extractor
   (tenant CRUD endpoints)          (service endpoints)
```

---

## Table of Contents

- [Concepts](#concepts)
- [Setup Guide](#setup-guide)
  - [1. Docker Compose (realms imported for you)](#1-docker-compose-realms-imported-for-you)
  - [2. Manual setup for your own Keycloak](#2-manual-setup-for-your-own-keycloak)
  - [3. Tenant Configuration](#3-tenant-configuration)
  - [4. Issuer vs. JWKS URL](#4-issuer-vs-jwks-url)
- [Authentication Flows](#authentication-flows)
  - [Platform Admin (Tenant Management)](#platform-admin-tenant-management)
  - [Developer Login (CLI)](#developer-login-cli)
  - [API Key Authentication](#api-key-authentication)
  - [JWT Authentication (Web Apps)](#jwt-authentication-web-apps)
- [Authorization (RBAC)](#authorization-rbac)
- [Fresh Install Walkthrough](#fresh-install-walkthrough)
- [Platform Configuration](#platform-configuration)
- [API Reference](#api-reference)
- [Troubleshooting](#troubleshooting)

---

## Concepts

### Platform Admin
A user in the **platform realm** who can create, update, list, and delete tenants
(`/api/tenants*`, `/api/platform/api-keys*`). Those endpoints use the
`PlatformAdmin` extractor, which accepts a platform-realm JWT holding the
`cufflink-platform-admin` realm role (`CUFFLINK_PLATFORM_ADMIN_ROLE`) or a platform
admin API key, and records every call in the audit log.

The same role is needed in two more places, because both give a platform-realm
JWT authority across tenants:

- To act **inside a tenant** with a platform-realm JWT (send `X-Tenant-Slug: <slug>`
  to any tenant-scoped endpoint such as `/api/services`). The platform rejects the
  request with `You need the 'cufflink-platform-admin' role in the platform realm.`
  otherwise; `cufflink-deploy` alone no longer suffices.
- For the `/api/auth/me` fallback (a platform-realm JWT without `X-Tenant-Slug`).

### Tenant
An isolated workspace within the platform. Each tenant has its own:
- Database schema per service
- Keycloak realm for user authentication
- API keys
- Roles and permissions
- Deployed services

### Deploy Role
A Keycloak realm role (default: `cufflink-deploy`, configurable per tenant) that a
developer must have in the **tenant's** Keycloak realm to obtain an API key via
`cufflink login`. Holding it also grants *deploy authority* on a Bearer request —
see [Authorization](authorization.md) for what that gates.

### API Key
A long-lived credential (`ck_...`) stored in `~/.config/cufflink/credentials.json`.
Used by the CLI for all service operations (deploy, status, rollback, etc.). Created
during `cufflink login`.

There are two types of API keys:
- **Tenant API keys** — scoped to a single tenant, created by `cufflink login` or
  `POST /api/auth/api-keys`. A tenant API key always carries deploy authority.
- **Platform admin API keys** — cross-tenant, created by
  `cufflink tenants create-api-key --name <name>`. Accepted by the `PlatformAdmin`
  endpoints directly, and by tenant-scoped endpoints together with an
  `X-Tenant-Slug` header. Required for CI/CD and preview environments.

---

## Setup Guide

### 1. Docker Compose (realms imported for you)

`docker compose up -d` starts Keycloak with `--import-realm`, loading every file in
`infra/keycloak/` on first start. Nothing has to be clicked together by hand:

| File | Realm | Contents |
|------|-------|----------|
| `infra/keycloak/cufflink-realm.json` | `cufflink` (platform realm) | Public client `cufflink-cli` (Device Authorization Grant + password grant enabled — the latter for local bootstrap only); realm roles `cufflink-deploy` and `cufflink-platform-admin`; user `admin` / `admin` holding both |
| `infra/keycloak/acme-realm.json` | `acme` (dev tenant realm) | Same shape: public client `cufflink-cli`, realm role `cufflink-deploy`, user `dev` / `dev` holding `cufflink-deploy` |

Keycloak listens on `http://localhost:8180` (admin console `admin` / `admin`). The
compose file sets `KC_HOSTNAME=http://localhost:8180` so every token — including
ones minted through the Docker-internal address `http://keycloak:8080` — carries
`http://localhost:8180/realms/<realm>` as its issuer. The platform container is
configured to match: `KEYCLOAK_URL=http://keycloak:8080` (where it fetches JWKS),
`KEYCLOAK_REALM=cufflink`, `KEYCLOAK_ISSUER=http://localhost:8180`.

A one-shot `tenant-init` service (`infra/compose/bootstrap-tenant.sh`) then logs in
as `admin` and registers the tenant:

| Field | Value |
|-------|-------|
| slug / name | `acme` / `Acme` |
| `keycloak_url` | `http://keycloak:8080` — the address the **platform** reaches |
| `keycloak_realm` | `acme` |
| `deploy_role` | `cufflink-deploy` |

Re-running the script is safe (a `409` means the tenant already exists). After
that, `cufflink login` as `dev` / `dev` and `cufflink deploy` work immediately.

### 2. Manual setup for your own Keycloak

If you run your own Keycloak, mirror what the compose realms contain. The platform's
expectations are:

**Platform realm**

1. Create a realm whose name equals the platform's `KEYCLOAK_REALM` (default:
   **`cufflink`**). If you pick another name, set `KEYCLOAK_REALM` to match.
2. Create client **`cufflink-cli`**
   - Client authentication: **Off** (public client)
   - Authentication flow: enable **OAuth 2.0 Device Authorization Grant**
   - Standard flow can stay enabled; the CLI never uses a redirect URI
3. Create realm roles **`cufflink-deploy`** and **`cufflink-platform-admin`**
4. Create your platform admin user and assign both: `cufflink-platform-admin` for
   `/api/tenants*`, `cufflink-deploy` to act inside a tenant via `X-Tenant-Slug`
   and for `/api/auth/me`

**Tenant realm** (one per tenant, e.g. **`acme`**)

1. Create the realm
2. Create client **`cufflink-cli`** exactly as above (public, device grant enabled)
3. Create realm role **`cufflink-deploy`** (or whatever you pass as `--deploy-role`)
4. Create users for your developers and assign the deploy role to each:
   **Users** → select user → **Role mapping** → **Assign role** → `cufflink-deploy`

Web or backoffice apps that log users in with their own Keycloak client need no
extra registration on the platform — any client in the tenant realm works for
Bearer requests, as long as it is *not* listed in `CUFFLINK_SERVICE_ACCOUNT_CLIENTS`.

### 3. Tenant Configuration

Register the tenant in the platform with its Keycloak configuration:

```bash
cufflink tenants create \
  --name "Acme" \
  --slug acme \
  --tenant-keycloak-url http://keycloak:8080 \
  --tenant-keycloak-realm acme \
  --deploy-role cufflink-deploy
```

Or update an existing tenant:

```bash
cufflink tenants update acme \
  --tenant-keycloak-url http://keycloak:8080 \
  --tenant-keycloak-realm acme
```

**`--tenant-keycloak-url` must be reachable from the platform process.** When a
developer runs `cufflink login`, the CLI posts the tenant-realm JWT to
`POST /api/auth/device/token`, and the platform validates it by fetching that
realm's JWKS from the tenant's stored `keycloak_url` (`platform/src/api/auth.rs`,
`exchange_token`). Inside Docker that is `http://keycloak:8080`, not
`http://localhost:8180`; the CLI's own device flow uses the browser-facing address
from `Cufflink.toml` instead.

Tenants can also carry Keycloak *ops* credentials — a confidential client with
admin rights on the tenant realm — used for `cufflink roles assign` user search
and `POST /api/auth/logout`:

```bash
cufflink tenants update acme \
  --keycloak-ops-client-id acme-ops \
  --keycloak-ops-client-secret '<secret>'
```

Both must be set together. If a tenant has none, the platform falls back to
`CUFFLINK_DEFAULT_KC_OPS_CLIENT_ID` / `CUFFLINK_DEFAULT_KC_OPS_CLIENT_SECRET`.

### 4. Issuer vs. JWKS URL

A JWT's `iss` claim is `{keycloak_public_url}/realms/{realm}` and must match
exactly. Two settings control this on the platform:

| Setting | Meaning |
|---------|---------|
| `KEYCLOAK_URL` | Where the platform **fetches JWKS** for the platform realm and for every tenant realm's Bearer requests, on `/svc/...` and on the API (`http://keycloak:8080` in Docker), and where it calls Keycloak's admin API. |
| `KEYCLOAK_ISSUER` | The **browser-facing** Keycloak base URL. The platform expects platform-realm tokens to carry `{KEYCLOAK_ISSUER}/realms/{KEYCLOAK_REALM}`, and `GET /api/auth/config` advertises it so the CLI runs the device flow against a URL the user's browser can open. Optional when `KEYCLOAK_URL` is already the public address (native dev loop). |

For tenant-realm tokens the expected issuer is
`{tenant keycloak_issuer, else KEYCLOAK_ISSUER}/realms/{realm}`; with neither set
it is the issuer the realm's OIDC discovery reports. A discovery that fails
refuses the token and is retried with backoff (1 s doubling to 30 s); no issuer is
ever guessed or taken from the token. JWKS come from `KEYCLOAK_URL` for every
Bearer request. The tenant's `keycloak_url` is read only by the `cufflink login`
exchange, so a tenant registered with another Keycloak can mint CLI keys, but its
Bearer tokens are always validated against `KEYCLOAK_URL`: every tenant's realm
has to be served there ([tenants](tenants.md)). A tenant with no `keycloak_url` at
all is not matched to its realm, so the value must be set even though Bearer
validation does not fetch from it. Keycloak itself must
mint stable issuers regardless of which hostname the request arrived on — that is
what `KC_HOSTNAME` (plus `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true`) does in the
compose file.

---

## Authentication Flows

### Platform Admin (Tenant Management)

Used by: `cufflink tenants create|list|update|delete` (and the `*-api-key`
subcommands when `CUFFLINK_PLATFORM_API_KEY` is not set)

```
┌──────────┐                     ┌──────────────┐              ┌──────────┐
│   CLI    │ ──device code─────> │  Keycloak    │              │ Platform │
│          │ <──user code──────  │  (platform   │              │          │
│          │                     │   realm)     │              │          │
│          │ ──poll token──────> │              │              │          │
│          │ <──JWT────────────  │              │              │          │
│          │                     └──────────────┘              │          │
│          │                                                   │          │
│          │ ──GET /api/tenants (Bearer JWT)─────────────────> │          │
│          │ <──200 OK + tenant list───────────────────────── │          │
└──────────┘                                                   └──────────┘
```

1. CLI calls `GET /api/auth/config` to discover the platform realm's browser-facing
   Keycloak URL, realm and client id (falls back to `Cufflink.toml` / env / defaults)
2. CLI initiates the OAuth 2.0 Device Code flow against the **platform realm** and
   prints a verification URL plus a user code
3. User opens the URL and authenticates in a browser
4. CLI receives a JWT from the platform realm
5. CLI sends the JWT as `Bearer` token to tenant management endpoints
6. Platform validates the JWT via the `PlatformAdmin` extractor (JWKS from the
   platform realm, issuer `{KEYCLOAK_ISSUER}/realms/{KEYCLOAK_REALM}`)
7. Tokens are cached in `~/.config/cufflink/platform-tokens.json` and refreshed
   automatically

**No Cufflink.toml required** — tenant commands work from any directory. Use
`--api-url` to specify a non-default platform URL.

### Developer Login (CLI)

Used by: `cufflink login` (run inside a project directory — it reads
`keycloak_url`, `keycloak_realm` and `tenant` from `Cufflink.toml`)

```
┌──────────┐                     ┌──────────────┐              ┌──────────┐
│   CLI    │ ──device code─────> │  Keycloak    │              │ Platform │
│          │ <──user code──────  │  (tenant     │              │          │
│          │                     │   realm)     │              │          │
│          │ ──poll token──────> │              │              │          │
│          │ <──JWT────────────  │              │              │          │
│          │                     └──────────────┘              │          │
│          │                                                   │          │
│          │ ──POST /api/auth/device/token ──────────────────> │          │
│          │    { access_token, tenant_slug }                  │          │
│          │                                                   │          │
│          │    Platform fetches JWKS from tenant.keycloak_url │          │
│          │    Platform validates JWT against tenant realm    │          │
│          │    Platform checks deploy_role in realm roles     │          │
│          │    Platform creates API key                       │          │
│          │                                                   │          │
│          │ <──{ api_key: "ck_...", tenant_slug, username }─ │          │
└──────────┘                                                   └──────────┘
```

1. CLI initiates the OAuth 2.0 Device Code flow against the **tenant's realm**
   (`{keycloak_url}/realms/{keycloak_realm}/protocol/openid-connect/auth/device`)
2. CLI prints the verification URL and user code and polls Keycloak — it does
   **not** open a browser for you:
   ```
   Environment: local
   Authenticating with Cufflink...
     Open this URL in your browser:
     http://localhost:8180/realms/acme/device?user_code=XXXX-XXXX
     Enter code: XXXX-XXXX
   ```
3. User authenticates in the browser
4. CLI receives a JWT from the tenant realm
5. CLI sends the JWT to `POST /api/auth/device/token` with the `tenant_slug`
6. Platform looks up the tenant's Keycloak config from the database
7. Platform validates the JWT against the **tenant's realm**, fetching JWKS from
   the tenant's stored `keycloak_url` (not the platform realm)
8. Platform checks that the user has the tenant's `deploy_role` (default:
   `cufflink-deploy`) in their realm roles
9. Platform creates an API key named `cli-{username}` and returns
   `{ api_key, tenant_slug, username }` — no role assignment happens here
10. CLI stores the API key in `~/.config/cufflink/credentials.json`

If the device-authorization endpoint is unreachable (Keycloak not configured), the
CLI falls back to prompting for an API key, verifies it with `GET /api/auth/me`,
and stores it the same way. Alternatively set `CUFFLINK_API_KEY` and skip login.

Credential precedence for service commands: `Cufflink.toml` `api_key` →
`api_key_env` → `CUFFLINK_API_KEY` env → stored credentials.

### API Key Authentication

Used by: `cufflink deploy|status|rollback|services|...` and any HTTP client

```
Authorization: ApiKey ck_f47ac10b58cc4372a5670e4df22460a2
```

- API keys are SHA256-hashed before storage (never stored in plaintext)
- Format: `ck_` + UUID v4 (e.g., `ck_f47ac10b58cc4372a5670e4df22460a2`)
- Optional expiration date
- Resolved to a tenant via the `api_keys` table
- No Keycloak interaction at request time — purely database lookup
- The caller's subject is the tenant's `keycloak_user_id`; the key carries deploy
  authority. Platform admin keys need `X-Tenant-Slug` and their subject is
  `apikey:{name}`.

### JWT Authentication (Web Apps)

Used by: Web applications (a customer portal, a staff backoffice) that
authenticate users via Keycloak

```
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
```

1. Web app authenticates user via Keycloak (standard OIDC flow)
2. Web app sends requests to `/svc/{tenant}/{service}/...` with the JWT
3. Platform extracts the `iss` (issuer) claim from the JWT without signature validation
4. Platform parses the realm name from the issuer (format: `{url}/realms/{realm}`)
5. Platform looks up the tenant whose `keycloak_realm` matches (with
   `X-Tenant-Slug` the slug *and* realm must match; without it the oldest tenant
   for that realm wins — previews share their source's realm)
6. Platform validates the JWT signature against the tenant realm's JWKS, fetched
   from `KEYCLOAK_URL`
7. The caller is now an authenticated tenant principal. Reaching tenant **data**
   requires no pre-existing role assignment — authorization happens per-route
   downstream (table `permission_area` / `owner_self_service`, custom-route
   postures, or the handler's own `auth.can` checks). Control-plane routes
   (deploy, service management, logs, SQL, backups) additionally require the
   tenant's deploy role via the `DeployAuth` guard.

A token whose realm matches neither a tenant nor the platform realm is rejected
with `No tenant configured for realm '<realm>'`.

---

## Authorization (RBAC)

Cufflink has a role-based access control system that operates at the tenant level.
This section is the short version; see [authorization.md](authorization.md) for
route postures, owner-scoped tables, service accounts and the permission cache.

### Roles

Each tenant can have multiple roles. A role has:
- **name** — unique per tenant, e.g. "admin", "viewer", "editor"
- **permissions** — JSON array of permission strings
- **service_id** (optional) — the service whose manifest seeded it

### Permissions

Permission strings use the format `area:operation`:

| Permission | Grants |
|-----------|--------|
| `todos:create` | Create todos |
| `todos:*` | All operations on todos |
| `*` | Everything |
| `roles:manage` | Manage roles and assignments through the RBAC API |

### Where roles come from

Roles are **seeded at deploy time** from the service manifest's `authorization`
block (`default_roles`). On every deploy the platform upserts each role; if it
already exists its permissions are merged, never overwritten. If the manifest
defines a role named **`admin`**, the deploying user is automatically assigned to
it (`platform/src/api/deployments.rs`, `seed_default_roles`). Nothing is assigned
on login, and there is no built-in "member" role.

```rust
cufflink::service! {
    name: "todo-service",
    tables: [Todo],
    authorization: [
        areas: [("todos", ["create", "view", "edit", "delete"])],
        default_roles: [
            ("admin", "Full access", ["todos:*", "roles:manage"]),
            ("viewer", "Read-only", ["todos:view"]),
        ],
    ],
}
```

### Permission Checking in Services

Define permission areas on your tables:

```rust
#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "todos", auth_required, permission_area = "todos")]
pub struct Todo {
    #[key] pub id: Uuid,
    pub title: String,
    pub completed: bool,
}
```

With `auth_required`, CRUD operations require authentication. With `permission_area = "todos"`, the platform checks the user's permissions against:
- `todos:view` for GET
- `todos:create` for POST
- `todos:edit` for PUT
- `todos:delete` for DELETE

### Managing Roles via CLI

```bash
# List all roles and their permissions
cufflink roles list

# Assign a role to a user (searches the tenant realm by email/username)
cufflink roles assign --user user@example.com --role admin

# Remove a role from a user
cufflink roles unassign --user user@example.com --role admin
```

`roles assign` / `unassign` look the user up through
`GET /api/tenants/{tenant_id}/users/search?q=`, which needs Keycloak ops
credentials on the tenant (or the platform-wide defaults).

### Managing Roles via API

Role writes require **`RbacAdmin`**: deploy authority (deploy role, tenant API
key, platform admin, service account) *or* the `roles:manage` permission.

```bash
# Create a role
curl -X POST http://localhost:8080/api/tenants/{tenant_id}/roles \
  -H "Authorization: ApiKey ck_..." \
  -H "Content-Type: application/json" \
  -d '{"name": "editor", "description": "Can edit content", "permissions": ["posts:edit", "posts:create"]}'

# List roles
curl http://localhost:8080/api/tenants/{tenant_id}/roles \
  -H "Authorization: ApiKey ck_..."

# Assign role to user
curl -X POST http://localhost:8080/api/tenants/{tenant_id}/roles/assign \
  -H "Authorization: ApiKey ck_..." \
  -H "Content-Type: application/json" \
  -d '{"user_id": "keycloak-subject-id", "role_id": "role-uuid"}'
```

---

## Fresh Install Walkthrough

Starting from nothing:

```bash
# 1. Install the CLI
cargo install cufflink-cli

# 2. Start infrastructure (PostgreSQL, NATS, Keycloak, Redis, object storage, platform)
#    Keycloak imports the `cufflink` and `acme` realms from infra/keycloak/,
#    and tenant-init registers the `acme` tenant for you.
docker compose up -d

# 3. (Own Keycloak only) create the realms by hand — see "Manual setup" above —
#    and register the tenant from any directory (no Cufflink.toml needed):
cufflink tenants create \
  --name "Acme" \
  --slug acme \
  --tenant-keycloak-url http://keycloak:8080 \
  --tenant-keycloak-realm acme \
  --deploy-role cufflink-deploy

# 4. Initialize a service project
cufflink init my-service
cd my-service

# 5. Configure the environment in Cufflink.toml
#    (cufflink init creates this with defaults)

# 6. Log in as a developer (device flow; dev/dev on the compose stack)
cufflink login

# 7. Deploy
cufflink deploy
```

---

## Platform Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KEYCLOAK_URL` | `http://localhost:8180` | Keycloak URL used to fetch JWKS and call the Admin API (must be reachable from the platform process; `http://keycloak:8080` in Docker) |
| `KEYCLOAK_REALM` | `cufflink` | Platform realm name (for `PlatformAdmin` JWT validation) |
| `KEYCLOAK_ISSUER` | *(unset — uses `KEYCLOAK_URL`)* | Browser-facing Keycloak base URL. Expected issuer for platform-realm tokens and the URL advertised by `GET /api/auth/config` |
| `CUFFLINK_SERVICE_ACCOUNT_CLIENTS` | *(empty)* | Comma-separated Keycloak client IDs whose tokens (matched on `azp`) are treated as service accounts and bypass permission checks while a tenant's `service_accounts` policy is `off` or `warn`; prefer each tenant's `service_account_clients`. Never list human-login clients |
| `CUFFLINK_PLATFORM_ADMIN_ROLE` | `cufflink-platform-admin` | Platform-realm role required by `PlatformAdmin` JWTs |
| `CUFFLINK_DEFAULT_KC_OPS_CLIENT_ID` | *(unset)* | Fallback confidential client for Keycloak Admin API calls (user search, logout) when a tenant has no ops credentials. Always added to the service-account allowlist |
| `CUFFLINK_DEFAULT_KC_OPS_CLIENT_SECRET` | *(unset)* | Secret for the default ops client |
| `REDIS_URL` | *(unset — Redis disabled)* | Redis for the permissions cache and distributed rate limiting |
| `DEFAULT_TENANT_SLUG` | `default` | Read into the platform config as the default tenant slug; not consumed by any auth path today |

### CLI Configuration (Cufflink.toml)

```toml
[service]
name = "my-service"
default_env = "local"

[environments.local]
api_url = "http://localhost:8080"
tenant = "acme"
keycloak_url = "http://localhost:8180"   # browser-facing; used by `cufflink login`
keycloak_realm = "acme"                  # the TENANT realm, not the platform realm

[environments.staging]
api_url = "https://api.staging.example.com"
tenant = "acme"
keycloak_url = "https://auth.staging.example.com"
keycloak_realm = "acme"
```

`keycloak_realm` matters: without it `cufflink login` falls back to the realm
`cufflink`, whose users are platform admins, not tenant developers.

### CLI Environment Variables

| Variable | Description |
|----------|-------------|
| `CUFFLINK_API_URL` | Override platform API URL |
| `CUFFLINK_KEYCLOAK_URL` | Override Keycloak URL |
| `CUFFLINK_KEYCLOAK_REALM` | Override Keycloak realm |
| `CUFFLINK_CLIENT_ID` | Override Keycloak client ID (default: `cufflink-cli`) |
| `CUFFLINK_API_KEY` | Tenant API key; skips `cufflink login` (after `Cufflink.toml` `api_key` / `api_key_env`, before stored credentials) |
| `CUFFLINK_PLATFORM_API_KEY` | Platform admin API key used by `cufflink tenants create-api-key / list-api-keys / revoke-api-key` instead of the device flow |

### Credential Storage

| File | Purpose |
|------|---------|
| `~/.config/cufflink/credentials.json` | API keys per environment (from `cufflink login`) |
| `~/.config/cufflink/platform-tokens.json` | Platform JWT + refresh tokens per api_url (from `cufflink tenants` commands) |

Both files are created with `0600` permissions (owner read/write only).

---

## API Reference

Extractor legend: **None** = public; **AuthUser** = any tenant credential;
**DeployAuth** = AuthUser with deploy authority; **RbacAdmin** = deploy authority
or `roles:manage`; **PlatformAdmin** = platform-realm JWT or platform admin API
key.

### Auth Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/api/auth/config` | None | Keycloak URL (`KEYCLOAK_ISSUER` or `KEYCLOAK_URL`), platform realm and client ids for CLI discovery |
| `POST` | `/api/auth/device/token` | None | Exchange a tenant Keycloak JWT for an API key (requires the tenant's deploy role) |
| `POST` | `/api/auth/api-keys` | DeployAuth | Create an additional tenant API key (the key carries deploy authority, so minting one is a control-plane operation) |
| `GET` | `/api/auth/me` | OptionalAuth | Authenticated user info; falls back to platform-realm validation (requires `CUFFLINK_PLATFORM_ADMIN_ROLE`) when no tenant context is present |
| `POST` | `/api/auth/logout` | AuthUser | Back-channel termination of the caller's Keycloak session via the tenant's ops client; also revokes the API keys that user's `cufflink login` minted for the tenant, so a web app's sign-out that calls it ends them too |

### Tenant Management (Platform Admin)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/api/tenants` | PlatformAdmin | Create a tenant. Body: `name`, `slug`, `keycloak_url`, `keycloak_realm`, `deploy_role`, `keycloak_ops_client_id`, `keycloak_ops_client_secret`, `clone_rbac_from` (slug whose roles and assignments are copied into the new tenant). 409 when another tenant that is not a preview (made with `clone_rbac_from`) uses the same `keycloak_realm` with the same `deploy_role`, since a deployer of either would deploy to both; the same holds for `PUT /api/tenants/{slug}` |
| `GET` | `/api/tenants` | PlatformAdmin | List all tenants |
| `PUT` | `/api/tenants/{slug}` | PlatformAdmin | Update a tenant (same fields except `clone_rbac_from`) |
| `DELETE` | `/api/tenants/{slug}` | PlatformAdmin | Delete a tenant |
| `POST` | `/api/tenants/{slug}/rbac/sync` | PlatformAdmin | Copy roles and user-role assignments from `{"source_tenant": "<slug>"}`; idempotent top-up |
| `POST` | `/api/platform/api-keys` | PlatformAdmin | Create platform admin API key |
| `GET` | `/api/platform/api-keys` | PlatformAdmin | List platform admin API keys |
| `DELETE` | `/api/platform/api-keys/{id}` | PlatformAdmin | Revoke platform admin API key |

Matching CLI: `cufflink tenants create|update|list|delete`,
`cufflink tenants update <slug> --keycloak-ops-client-id … --keycloak-ops-client-secret …`,
`cufflink tenants create-api-key --name <name>`, `cufflink tenants list-api-keys`,
`cufflink tenants revoke-api-key <id>`. `clone_rbac_from` and `rbac/sync` have no
CLI wrapper; call the API directly.

### RBAC

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/api/tenants/{tenant_id}/roles` | RbacAdmin | Create role |
| `GET` | `/api/tenants/{tenant_id}/roles` | AuthUser | List roles |
| `PUT` | `/api/tenants/{tenant_id}/roles/{role_id}` | RbacAdmin | Update role |
| `DELETE` | `/api/tenants/{tenant_id}/roles/{role_id}` | RbacAdmin | Delete role |
| `POST` | `/api/tenants/{tenant_id}/roles/assign` | RbacAdmin | Assign role to user |
| `DELETE` | `/api/tenants/{tenant_id}/roles/unassign` | RbacAdmin | Unassign role from user |
| `GET` | `/api/tenants/{tenant_id}/permission-areas` | AuthUser | Permission areas declared by all deployed services |
| `GET` | `/api/tenants/{tenant_id}/users/{user_id}/permissions` | AuthUser | Get effective permissions and role names |
| `GET` | `/api/tenants/{tenant_id}/users/search?q=` | AuthUser | Search users in the tenant's Keycloak realm by email/username (needs ops credentials) |
| `GET` | `/api/users/lookup?q=` | AuthUser | Searches **tenants** by name or slug, not Keycloak users |

All tenant-scoped RBAC routes also require `{tenant_id}` to equal the caller's
tenant.

---

## Troubleshooting

### "No matching JWK for kid=... after JWKS refresh"
The platform fetched the JWKS of a realm that never issued this token — its key id
is unknown even after a forced refresh. For a platform-realm token, check that
`KEYCLOAK_REALM` names the realm that issued it. For a tenant token, check the
tenant's `keycloak_realm` (and, for `cufflink login`, that its `keycloak_url`
points at the same Keycloak the browser used).

### "InvalidIssuer"
The JWT's issuer claim doesn't match what the platform expects. The issuer is
`{keycloak_public_url}/realms/{realm}`. For platform-realm tokens set
`KEYCLOAK_ISSUER` to the browser-facing base URL; make sure Keycloak's
`KC_HOSTNAME` is fixed so the issuer does not change with the request hostname.

### "No tenant configured for realm 'xxx'"
A JWT was presented from a realm that doesn't match any tenant's `keycloak_realm`
(nor the platform realm). Update the tenant with the correct realm:
```bash
cufflink tenants update acme --tenant-keycloak-realm correct-realm
```

### "You need the 'cufflink-deploy' role in the 'acme' realm..."
`cufflink login` was refused because the user lacks the tenant's deploy role. In
Keycloak admin:
1. Go to the tenant realm → **Realm roles** → create `cufflink-deploy` if it doesn't exist
2. Go to **Users** → select user → **Role mapping** → assign `cufflink-deploy`

### "You need the 'cufflink-platform-admin' role in the platform realm."
A platform-realm JWT tried to administer tenants (`/api/tenants*`), act inside a
tenant (`X-Tenant-Slug`) or call `/api/auth/me` without holding the
platform-admin role (`CUFFLINK_PLATFORM_ADMIN_ROLE`, default
`cufflink-platform-admin`) in the platform realm. Assign the role there.

### "This operation requires deploy authority..."
The credential is valid but the caller lacks control-plane authority for a deploy /
service-management route (`DeployAuth`). Control-plane authority is the tenant's
deploy role, a tenant API key, a platform admin, or a service account — a plain
tenant user has data access but not this. Run `cufflink login` (which mints a
deploy-capable API key) or have an admin grant the deploy role.

### "Managing roles requires the 'roles:manage' permission."
RBAC writes need `RbacAdmin`: deploy authority or a role granting `roles:manage`.

### "Missing Authorization header"
The endpoint requires authentication. Use either:
- `Authorization: Bearer <jwt>` for JWT auth
- `Authorization: ApiKey <key>` for API key auth

### `cufflink login` fails with a connection error during token exchange
The tenant's stored `keycloak_url` is where the platform fetches JWKS for the
exchange. If it is `http://localhost:8180` but the platform runs in Docker, that
address is unreachable from the container — set it to the Docker-internal address:
```bash
cufflink tenants update acme --tenant-keycloak-url http://keycloak:8080
```
Bearer requests to `/svc/...` use the platform's `KEYCLOAK_URL` instead, so they
may keep working while login fails.

### "Tenant 'xxx' is not configured for login"
The tenant has no `keycloak_url` / `keycloak_realm`. Set both with
`cufflink tenants update`.

### Platform tokens not refreshing
Delete `~/.config/cufflink/platform-tokens.json` and re-run the tenants command to re-authenticate.

### API key not working after re-deploy
API keys are tied to tenants. If the tenant was deleted and recreated, old API keys are invalid. Run `cufflink login` again.
