-- V2: per-tenant policies, tenant-scoped service-account clients and JWT
-- issuer, preview tenant groups, and user-bound tenant API keys (their
-- expiry is backfilled at startup from CUFFLINK_API_KEY_TTL_DAYS).

CREATE TABLE IF NOT EXISTS tenant_policies (
    tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    key        TEXT NOT NULL,
    mode       TEXT NOT NULL CHECK (mode IN ('off','warn','enforce')),
    config     JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by TEXT,
    PRIMARY KEY (tenant_id, key)
);

ALTER TABLE tenants ADD COLUMN IF NOT EXISTS service_account_clients TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS keycloak_issuer TEXT;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS source_tenant_id UUID REFERENCES tenants(id) ON DELETE SET NULL;

ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS user_sub TEXT;
CREATE INDEX IF NOT EXISTS api_keys_tenant_user_idx ON api_keys (tenant_id, user_sub) WHERE user_sub IS NOT NULL;
