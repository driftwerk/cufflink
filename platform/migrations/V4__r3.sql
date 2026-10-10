-- R3: NATS namespace claims, streams and consumer handovers, the outbox relay
-- lease, schedules, job leases and retries, webhook deliveries, the tenant
-- database and service schema names, deploy jobs, service database roles and
-- secret encryption. Additive only, so an R2 binary runs against this schema.
--
-- Every change to a table that has rows is one that rewrites none of them,
-- so the lock it takes is held for an instant. What a table of any size
-- cannot have done to it inside this transaction without blocking its
-- writers (the indexes of wasm_jobs) is done after it, outside any
-- transaction, by repo/migrations.rs. A change that cannot take its lock
-- within ten seconds fails, and startup applies the migration again, rather
-- than holding every query on the table behind it.

SET LOCAL lock_timeout = '10s';

-- First, so that no other table is locked while the lock of wasm_jobs,
-- the table long readers hold, is waited for. One statement adds the
-- columns under one lock, and no column is given a value per row: a
-- default that is a constant or now() is stored once, for the table. The
-- jobs an earlier release enqueued were published when they were created,
-- which published_at's default says of each of them; a job enqueued later
-- has none until it is published.
ALTER TABLE wasm_jobs
    ADD COLUMN IF NOT EXISTS lease_owner       TEXT,
    ADD COLUMN IF NOT EXISTS lease_until       TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS published_at      TIMESTAMPTZ DEFAULT now(),
    ADD COLUMN IF NOT EXISTS last_error        TEXT,
    ADD COLUMN IF NOT EXISTS error_class       TEXT,
    ADD COLUMN IF NOT EXISTS retry_on          TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS backoff_base_secs INTEGER,
    ADD COLUMN IF NOT EXISTS backoff_max_secs  INTEGER,
    ADD COLUMN IF NOT EXISTS trace_context     JSONB,
    ADD COLUMN IF NOT EXISTS dead_lettered_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS idempotency_key   TEXT,
    ADD COLUMN IF NOT EXISTS schedule_name     TEXT;
ALTER TABLE wasm_jobs ALTER COLUMN published_at DROP DEFAULT;

CREATE TABLE IF NOT EXISTS tenant_nats_claims (
    tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    kind       TEXT NOT NULL CHECK (kind IN ('subject', 'stream')),
    pattern    TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, pattern)
);
CREATE INDEX IF NOT EXISTS tenant_nats_claims_tenant_idx ON tenant_nats_claims (tenant_id);

CREATE TABLE IF NOT EXISTS tenant_streams (
    tenant_id   UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    declared_by UUID REFERENCES services(id) ON DELETE SET NULL,
    config_hash TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS consumer_handovers (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_id     UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    deploy_version INTEGER NOT NULL,
    stream         TEXT NOT NULL,
    from_name      TEXT NOT NULL,
    to_name        TEXT NOT NULL,
    mode           TEXT NOT NULL CHECK (mode IN ('create_then_delete', 'delete_then_create')),
    start_sequence BIGINT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox_relay_leases (
    tenant_id  UUID PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    holder     TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS schedule_state (
    service_id    UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    last_fired_at TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (service_id, name)
);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id           UUID PRIMARY KEY,
    webhook_id   UUID NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
    tenant_id    UUID NOT NULL,
    event        TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('pending', 'delivered', 'failed', 'dropped')),
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_status  INTEGER,
    last_error   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS webhook_deliveries_webhook_idx ON webhook_deliveries (webhook_id, created_at DESC);
CREATE INDEX IF NOT EXISTS webhook_deliveries_updated_idx ON webhook_deliveries (updated_at);

ALTER TABLE tenants  ADD COLUMN IF NOT EXISTS database_name TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS tenants_database_name_idx ON tenants (database_name) WHERE database_name IS NOT NULL;
ALTER TABLE services ADD COLUMN IF NOT EXISTS schema_name TEXT;
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM services GROUP BY tenant_id, lower(COALESCE(schema_name, replace(name, '-', '_'))) HAVING count(*) > 1) THEN
    CREATE UNIQUE INDEX IF NOT EXISTS services_schema_idx ON services (tenant_id, lower(COALESCE(schema_name, replace(name, '-', '_'))));
  ELSE
    RAISE WARNING 'services_schema_idx not created: two services of one tenant share a schema; the deploy validator still rejects new collisions';
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS deploy_jobs (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_id   UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    status       TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    replica      TEXT NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    request      JSONB NOT NULL,
    progress     JSONB NOT NULL DEFAULT '{}'::jsonb,
    result       JSONB,
    error        TEXT,
    created_by   TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS deploy_jobs_running_idx ON deploy_jobs (heartbeat_at) WHERE status = 'running';

CREATE TABLE IF NOT EXISTS service_db_roles (
    service_id     UUID PRIMARY KEY REFERENCES services(id) ON DELETE CASCADE,
    role_name      TEXT NOT NULL,
    managed        BOOLEAN NOT NULL,
    login          BOOLEAN NOT NULL DEFAULT false,
    password_enc   BYTEA,
    read_only_role TEXT,
    verified_at    TIMESTAMPTZ,
    verify_report  JSONB,
    updated_by     TEXT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tenant_data_keys (
    key_id      UUID PRIMARY KEY,
    tenant_id   UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    kek_id      TEXT NOT NULL,
    wrapped_dek BYTEA NOT NULL,
    active      BOOLEAN NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS tenant_data_keys_active_idx ON tenant_data_keys (tenant_id) WHERE active;
ALTER TABLE service_configs ADD COLUMN IF NOT EXISTS value_enc BYTEA;
ALTER TABLE service_configs ALTER COLUMN value DROP NOT NULL;
ALTER TABLE tenants  ADD COLUMN IF NOT EXISTS keycloak_ops_client_secret_enc BYTEA;
ALTER TABLE webhooks ADD COLUMN IF NOT EXISTS secret_enc BYTEA;
ALTER TABLE webhooks ALTER COLUMN secret DROP NOT NULL;
