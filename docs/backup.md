# Backup & Restore

Cufflink provides object-storage-backed, async backup and restore for service data. Jobs are orchestrated via NATS JetStream so they survive platform restarts and can be cancelled mid-flight.

## Prerequisites

Object storage must be configured on the **worker** — the worker is what reads and writes the backup files. The platform needs the same configuration to hand out presigned upload/download URLs, and refuses backup requests with an error if it has none.

| Variable | Default | Notes |
|----------|---------|-------|
| `STORAGE_BACKEND` | `s3` | `s3` (any S3-compatible store, e.g. AWS S3 or RustFS) or `gcs` |
| `S3_ENDPOINT` | — | required for `s3` |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | — | required for `s3` |
| `S3_REGION` | `us-east-1` | |
| `S3_BUCKET` | `cufflink` | |
| `GCS_BUCKET` | — | required for `gcs`; credentials via `GOOGLE_APPLICATION_CREDENTIALS` |

The compose stack sets these on both the platform and the worker against the bundled RustFS instance (`S3_ENDPOINT=http://rustfs:9000`, credentials `rustfsadmin` / `rustfsadmin`).

## What a backup contains

An export is **row data only**, for the tables in the service's **latest deployed manifest** (optionally narrowed with `--tables`). It does not contain:

- the service's [private tables](schema-ownership.md#private-tables): the export reads the manifest's `tables` list alone, so a table the platform checks and does not serve is in no export, and `restore --clear` never empties one
- the schema itself (tables, columns, indexes, FKs) — that comes from redeploying the service
- service config or secrets (`cufflink config`, `cufflink secrets`)
- files in object storage referenced by rows
- RBAC roles and assignments
- the audit log
- anything in the platform database (tenants, deployments, API keys) or Keycloak

A restore therefore needs the **service already deployed to the target tenant at a schema compatible with the export**. Rows are inserted with `ON CONFLICT DO NOTHING`, so restoring over existing data keeps the existing rows; use `--clear` to replace them.

## Schema

View tables, columns, types, and foreign key dependencies for the current service:

```bash
cufflink backup schema
```

Output:

```
Service: stock-service

  categories
    id uuid [PK]
    name text
    parent_id uuid [nullable, FK] -> categories.id
    depends on: categories

  products
    id uuid [PK]
    name text
    category_id uuid [FK] -> categories.id
    price float
    depends on: categories
```

## Export

Export all tables:

```bash
cufflink backup export
```

Export specific tables:

```bash
cufflink backup export --tables categories,products
```

The CLI warns if selected tables have FK dependencies on tables not included in the export.

Export with a custom output path:

```bash
cufflink backup export --output my-backup.jsonl
```

Fire and forget (returns job ID immediately):

```bash
cufflink backup export --async
```

### Export Format

Exports are NDJSON (newline-delimited JSON) files. Each line is a record with a `_table` field:

```json
{"_table": "categories", "id": "abc-123", "name": "Shoes"}
{"_table": "categories", "id": "def-456", "name": "Bags"}
{"_table": "products", "id": "ghi-789", "name": "Sneakers", "category_id": "abc-123"}
```

## Restore

Restore from a local file:

```bash
cufflink backup restore my-backup.jsonl
```

Restore specific tables only:

```bash
cufflink backup restore my-backup.jsonl --tables categories
```

Clear existing data before restoring:

```bash
cufflink backup restore my-backup.jsonl --clear
```

Skip the CLI's `--tables` check:

```bash
cufflink backup restore my-backup.jsonl --tables legacy_items --force
```

Before submitting a restore with `--tables`, the CLI checks that every named table exists in the service's current schema and aborts with `Tables not in current schema: ... Use --force to skip this check.` if not. `--force` skips **only that client-side name check**: the platform itself refuses (422) a `tables` filter naming a table outside the service's deployed manifest, and on the worker rows for a table or column that does not exist in the deployed schema still fail the restore.

### Restore from a Previous Export

List completed backups:

```bash
cufflink backup list
```

Restore directly from object storage without downloading:

```bash
cufflink backup restore --from-backup <job-id>
```

The `--from-backup` flag looks up the export job's object key and creates a restore job pointing to the same file. No local download needed.

A restore may only read a backup under `backups/{service_id}/` of the target service or of another service the caller controls (a platform admin may restore any service's export, which is how previews copy their source's data).

## Job Management

List all backup/restore jobs for the current service:

```bash
cufflink backup jobs
```

Output includes job ID, type (export/restore), status, tables filter, row count, and creation time.

Cancel a running job:

```bash
cufflink backup cancel <job-id>
```

The worker checks for cancellation between table iterations (export) and every 1000 rows (restore). Cancelled restores are rolled back.

## How It Works

1. CLI sends a request to the platform API to create a backup job (for a local-file restore it first uploads the file through a presigned URL to `backups/{service_id}/restore-{uuid}.jsonl`)
2. Platform creates a `backup_jobs` row and publishes a message to the `CUFFLINK_BACKUP` NATS JetStream stream (subjects `cufflink.backup.>`, messages retained for 24 hours)
3. A worker picks up the message from the durable consumer
4. **Export:** worker streams rows via DB cursors in batches of 1000 and writes NDJSON to `backups/{service_id}/{job_id}/export.jsonl`
5. **Restore:** worker downloads the NDJSON and inserts rows in batches of 1000 with `ON CONFLICT DO NOTHING`. The **whole restore is one transaction** (including the `--clear` deletes): it commits only when every row is in, and any error or cancellation rolls the entire restore back
6. Worker updates job progress and status in the `backup_jobs` table
7. CLI polls the job status endpoint until completion, then downloads the file (for export)

Jobs have a 5-minute ack timeout and a JetStream `max_deliver` of 3, i.e. up to two redeliveries if a worker dies mid-job.

## Cross-Tenant Backup/Restore

Platform admins can export from one tenant and restore into another using the `CUFFLINK_PLATFORM_API_KEY` and `X-Tenant-Slug` header. This is how `cufflink workspace preview create` clones data from the source tenant into a preview tenant (see `docs/workspace.md`).

## Backup and disaster recovery

`cufflink backup` covers the rows of one service's served tables. It is a way to copy a service's data, not a disaster-recovery backup: it holds no schema, no private table, no config, and nothing of the platform. Everything below lives outside Cufflink's own tooling and needs a backup of its own.

### What state exists

| What | Where | Holds | If lost |
|------|-------|-------|---------|
| Platform database | PostgreSQL, at `DATABASE_URL` | tenants and their policies, services, deployments and their manifests, schema snapshots, config and secrets (`service_configs`), API, partner and runner keys, roles and assignments, webhooks and their deliveries, job and backup-job records, schedule state, NATS claims and declared streams, each service's database role binding (`service_db_roles`) and each tenant's wrapped data keys (`tenant_data_keys`) | nothing is deployed: every service has to be deployed again, and every key and secret set again |
| Tenant databases | PostgreSQL; `cufflink_{tenant}` where the platform made the database, or the database a tenant adopted (`cufflink tenants create --database-name`) | every service's schema and rows, private tables included, and the schema `_cufflink`: the outbox with what is not yet published, and the map of service roles | the tenant's data |
| Database roles | the PostgreSQL cluster, not a database | the role of each service under `db_roles` (`svc_{tenant}_{service}` where the platform made it); its password is sealed in the platform database | services cannot log in until the roles are made again ([database roles](db-roles-and-secrets.md)) |
| Object storage bucket | S3-compatible store or GCS | WASM modules and their precompiled artefacts (`wasm/`), native binaries (`native/`), web app artifacts (`web/`), uploaded files (`files/`), backup files (`backups/`) | a deployment whose module is gone cannot run: the platform loads a module only when its bytes match the hash its deployment recorded (`cufflink_wasm_hash_mismatch_total`) |
| The key-encryption key | the environment, or the files `CUFFLINK_SECRETS_KEK_FILE` and `CUFFLINK_SECRETS_KEK_PREVIOUS_FILE` name; never the database | what unwraps every tenant's data key, under `dual` or `encrypted` secret storage | every sealed secret is unreadable, and no backup of the database brings it back. Keep every key that wrapped a data key at any time a backup you may restore was taken |
| JetStream file store | NATS `--store_dir` (the `natsdata` volume in compose) | the tenants' streams (`DW_{TENANT}` and those their manifests declare) and their durables' positions; `CUFFLINK_AUDIT` (durable audit records); `CUFFLINK_DLQ` (dead letters); `CUFFLINK_WEBHOOKS`; job, schedule and backup messages in flight (`CUFFLINK_WASM_JOBS`, `CUFFLINK_LONG_JOBS`, `CUFFLINK_SCHEDULES`, `CUFFLINK_TICKS`, `CUFFLINK_BACKUP`) | the platform makes its own streams again as it starts, and a deploy the declared ones; what they held is gone: events not yet consumed, audit records, dead letters. Long jobs are rows in the platform database and are published again |
| Keycloak database | PostgreSQL (`keycloak-db` in compose) | realms, users, roles, clients | nobody can log in |
| Redis | — | caches and rate-limit counters | nothing: it is rebuilt |

### A dedicated platform

The platform made its tenants' databases, on the server `DATABASE_URL` names (or the one `TENANT_DATABASE_URL` reaches).

- **Back up the cluster, not the databases one by one.** The platform database and the tenant databases describe each other: a deployment's schema snapshot is of the tenant schema as that deploy left it, a role binding's sealed password is the password the cluster's role has, and a relay's lease is for an outbox in a tenant database. A physical backup with point-in-time recovery (pgBackRest, WAL-G, a managed service's) restores all of them, and the roles, to one moment. Keep them on one cluster so that one backup can.
- **A logical backup** (`pg_dump` of each database) must be of all of them at one time, with the cluster's roles (`pg_dumpall --globals-only`), since no database dump carries a role. A tenant database restored to another moment than the platform's is restored again, not reconciled by hand.
- **Back up the bucket** with the database, and keep every object for as long as a database backup that names it may be restored: the modules of retained versions, not only the running ones, since a rollback loads them.
- **Escrow the key-encryption keys** outside the platform's own storage, each for as long as any backup holds a data key wrapped under it.
- **NATS** is backed up where what its streams hold matters to you (durable audit records; tenants' streams with long retention). A stream's backup is of another moment than the database's: consumers may see again what they had handled, which `Nats-Msg-Id` drops only inside a stream's duplicate window.

### An adopted database

A tenant created with `--database-name` uses a database that existed. The platform never creates or drops it, and with `schema: adopted` services and private tables it makes no table in it either ([schema ownership](schema-ownership.md)). What it does put there is the schema `_cufflink` (under `outbox: enforce` or `db_roles`), and under `db_roles` it grants and, for roles it made, creates roles in that database's cluster.

- **The database stays in its owner's backup**, which then also holds `_cufflink`. Do not exclude that schema from a backup meant for recovery: its outbox holds messages that were committed and not yet published. Do exclude it from a copy made to seed another environment, which has a platform, roles and an outbox of its own.
- **The platform database is backed up with it, to the same point in time.** It is small, and it is what says which services run on the adopted database, under which roles, with which sealed passwords and config. The simplest way to keep the two at one moment is to put the platform database on the adopted database's cluster, under the same physical backup. Where they are on different servers, restore both to the same time, and expect to run the checks below.
- **Restoring the adopted database alone, to an earlier time**, as its owner may for reasons of their own, leaves the platform ahead of it: deployments whose schema the database no longer has, and outbox rows that were published already and are published again (repeats are dropped only inside a stream's duplicate window). Redeploy each service, or roll each back to the version of that time; where the whole cluster went back, verify each role too, since a role's password is the cluster's and the sealed copy is the platform's.
- **`cufflink backup export` is not part of this.** It reads the served tables of one service; an adopted database's other tables, its private tables and its schema are not in it.

### Restoring

Order after a total loss: PostgreSQL (platform, tenant and Keycloak databases, and the roles) → object storage → the key-encryption keys in place, as variables or files → NATS → the platform, then the worker, web-runner and ext-runner. The platform re-creates missing JetStream streams on startup, so a lost NATS store costs what the streams held and no more.

Then check, before sending traffic:

1. **The platform is ready**: `/ready` answers 200. It loads every active service's manifest from the restored database; a row it cannot read is counted in `cufflink_registry_load_errors_total`.
2. **The key ring opens everything the database holds**:

   ```sh
   cufflink admin secrets reseal --operation verify
   ```

   It changes nothing, and exits 0 only when every data key unwraps and every sealed secret opens; it exits 1 and says what does not, by table. Run it for every tenant (no `--tenant`), and have a drill assert `Data keys: N checked` with N above 0, since a scope that holds nothing also exits 0. A `Not finished:` line is a rotation that was under way when the backup was taken, not a failed restore. Under `plaintext` storage there is nothing to verify and the command is refused. The counts, the exit codes and what to do about each are under [verifying a key ring](db-roles-and-secrets.md#verifying-a-key-ring).
3. **The modules are the deployments' own**: call a route of each service that has handlers, and watch `cufflink_wasm_hash_mismatch_total` stay at 0.
4. **Each service's role still logs in** (under `db_roles`): `cufflink services db-role verify <tenant> <service>`. After a restore that did not bring the roles back with their passwords, `cufflink services db-role rotate <tenant> <service>` gives a role the platform made a new one.
5. **The consumers are where the manifests want them**: `cufflink consumers ls`.

A restore is only known to work once it has been done: run these on a restored copy on a schedule, not for the first time after a loss.
