-- Runner keys: each runner's credential, bound to one tenant and one kind
-- of runner (`native`, the extension runner; `web`, a tenant-scoped web
-- runner) and accepted by that kind's runner API alone. A table of their
-- own, apart from api_keys, so no route that reads API keys can ever
-- accept one. Additive only, so an R3 binary runs against this schema.

SET LOCAL lock_timeout = '10s';
CREATE TABLE IF NOT EXISTS runner_keys (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    runs         TEXT NOT NULL CHECK (runs IN ('native', 'web')),
    key_prefix   TEXT NOT NULL UNIQUE,
    secret_hash  TEXT NOT NULL,
    created_by   TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at   TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS runner_keys_tenant_idx ON runner_keys (tenant_id);
