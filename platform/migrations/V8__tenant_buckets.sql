-- Buckets a platform admin registers to one tenant: the keys and canned
-- ACLs its guests may use there, and the credential that reaches it when
-- the platform's own does not. A bucket belongs to one tenant at most.
-- Additive only, so an earlier binary runs against this schema and never
-- reads it.

SET LOCAL lock_timeout = '10s';
CREATE TABLE IF NOT EXISTS tenant_buckets (
    id                    UUID PRIMARY KEY,
    tenant_id             UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    bucket                TEXT NOT NULL UNIQUE,
    key_prefixes          TEXT[],
    acls                  TEXT[] NOT NULL DEFAULT '{}',
    access_key_id         TEXT,
    secret_access_key     TEXT,
    secret_access_key_enc BYTEA,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by            TEXT,
    CHECK (access_key_id IS NOT NULL
           OR (secret_access_key IS NULL AND secret_access_key_enc IS NULL))
);
CREATE INDEX IF NOT EXISTS tenant_buckets_tenant_idx ON tenant_buckets (tenant_id);
