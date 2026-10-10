# Cufflink Workspace

A workspace is a monorepo-level orchestration layer for deploying, testing, and seeding multiple cufflink services in one command. It's defined by a `cufflink-workspace.toml` file at the monorepo root.

## Configuration

Create `cufflink-workspace.toml` at your monorepo root:

```toml
[workspace]
name = "my-project"

[[services]]
name = "stock-service"
path = "backend/stock-service"
seed = "backend/stock-service/seed.json"

[[services]]
name = "user-service"
path = "backend/user-service"

[[services]]
name = "portal"
path = "apps/portal"

[[services]]
name = "backoffice"
path = "apps/backoffice"

[environments.local]
deploy = ["stock-service", "user-service"]
seed = ["stock-service"]

[environments.staging]
deploy = ["stock-service", "user-service", "portal", "backoffice"]
seed = ["stock-service"]

[environments.production]
deploy = ["stock-service", "user-service", "portal", "backoffice"]
seed = []
```

### Fields

- **`[workspace].name`** — workspace display name
- **`[[services]].name`** — service name (must match `[service].name` in the service's `Cufflink.toml`)
- **`[[services]].path`** — relative path from workspace root to service directory
- **`[[services]].seed`** — optional path to seed data JSON file
- **`[environments.<name>].deploy`** — which services to deploy for this env (omit to deploy all)
- **`[environments.<name>].seed`** — which services to seed after deploy (omit or `[]` for none)

All services are always listed at the top level so `workspace test` can find them all, regardless of the deploy list.

Service type (Rust/WASM vs web) is auto-detected from each service's `Cufflink.toml` `mode` field.

## Commands

### Deploy

Deploy all services for the current environment:

```bash
cufflink workspace deploy --env staging
```

Skip specific services:

```bash
cufflink workspace deploy --env local --skip portal,backoffice
```

Deploy in parallel (all services concurrently):

```bash
cufflink workspace deploy --env staging --parallel
```

Deploy to a specific tenant:

```bash
CUFFLINK_PLATFORM_API_KEY=<platform-admin-key> \
cufflink workspace deploy --env staging --tenant preview-pr-42
```

`--tenant` is passed through to each service's `cufflink deploy`. When `CUFFLINK_PLATFORM_API_KEY` is set, requests are sent with that platform key plus an `X-Tenant-Slug` header so the deploy lands in the named tenant. When it is **not** set, the CLI silently falls back to the environment's ordinary tenant API key, which can only deploy into its own tenant — so set the platform key whenever you mean a different tenant.

After deploying, services listed in the environment's `seed` config are automatically seeded.

### Test

Run tests for all services in the workspace:

```bash
cufflink workspace test
```

Skip specific services:

```bash
cufflink workspace test --skip shared-lib
```

Show full test output:

```bash
cufflink workspace test --all
```

Services without a `Cargo.toml` (shared libraries, web apps) are automatically skipped. Output shows a summary table:

```
 SERVICE              RESULT    TIME
 stock-service        passed    3.0s
 user-service         passed    1.5s
 portal               skipped   -

2 passed, 1 skipped
```

### Status

Show deploy status of all workspace services:

```bash
cufflink workspace status --env staging
```

### Seed

Run seeding for configured services (without deploying):

```bash
cufflink workspace seed --env local
```

## Preview Environments

Create ephemeral preview environments on existing infrastructure by spinning up a new tenant, deploying all services, and cloning data from a source tenant.

### Create

```bash
CUFFLINK_PLATFORM_API_KEY=<platform-admin-key> \
CUFFLINK_API_KEY=<source-tenant-key> \
cufflink workspace preview create \
  --name pr-42 \
  --source-tenant acme \
  --env staging
```

This:
1. Creates tenant `preview-pr-42` with the same Keycloak config as the source
   tenant, cloning the source's RBAC (roles + user-role assignments) so
   existing staff can log in immediately
2. Deploys all services in parallel to the new tenant
3. Exports all data from the source tenant's services
4. Restores that data into the preview tenant

Re-running `preview create` for an existing preview (e.g. a new commit on a
labeled PR) reuses the tenant: services are redeployed, RBAC is topped up
from the source, and `preview_tables` are re-cloned.

### Minimal data clones with `preview_tables`

By default the full source tenant is cloned. To keep previews fast and clean,
declare `preview_tables` on the services whose data previews actually need:

```toml
[[services]]
name = "user-service"
path = "backend/user-service"
preview_tables = ["staff", "site_settings"]

[[services]]
name = "stock-service"
path = "backend/stock-service"
```

As soon as **any** service declares `preview_tables`, preview cloning switches
to allowlist mode:

- Declared services clone **only the listed tables**. The tables are cleared
  first (`clear_existing`), so source-tenant values win over rows seeded by
  `on_migrate` hooks at deploy time.
- Undeclared services are not cloned at all — they start from their deployed
  schema plus whatever `on_migrate`/seed files create.

With no `preview_tables` anywhere in the workspace, the previous full-clone
behavior is unchanged.

### Destroy

```bash
CUFFLINK_PLATFORM_API_KEY=<platform-admin-key> \
cufflink workspace preview destroy --name pr-42 --env staging
```

Permanently deletes the preview tenant, drops its database, and removes its
JetStream stream.

**Always pass `--env`**: like every workspace command, destroy resolves the
platform API URL from the env's `Cufflink.toml` — without `--env` it falls
back to the workspace `default_env` (often `local`) and the delete silently
targets the wrong platform.

### CI Integration

`cufflink ci init github` scaffolds `.github/workflows/cufflink.yml` for a single service: test → deploy staging → integration test → deploy production, reading `CUFFLINK_STAGING_API_KEY` and `CUFFLINK_PROD_API_KEY` from repository secrets. Use it as the starting point for a workspace pipeline by swapping the `cufflink deploy` steps for `cufflink workspace deploy --env <name>`.

A preview workflow (for example, triggered by a `Preview` label on a pull request) needs two secrets:

- a **platform admin API key** (exported as `CUFFLINK_PLATFORM_API_KEY`) so the job can create and destroy tenants
- the **source tenant's API key** (exported as `CUFFLINK_API_KEY`) so it can export the source data

Wire the `preview create` command above into the label-added event and `preview destroy` into the PR-closed event. If your services use `cufflink secrets`, the job will also need the vault key for the environment it deploys to.
