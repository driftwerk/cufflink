-- R2: partner keys, precompiled module artefacts and API key usage.
-- Additive only, so an R1 binary runs against this schema.

CREATE TABLE IF NOT EXISTS partner_keys (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    key_prefix    TEXT NOT NULL UNIQUE,
    secret_hash   TEXT NOT NULL,
    scopes        TEXT[] NOT NULL CHECK (cardinality(scopes) > 0),
    created_by    TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL,
    revoked_at    TIMESTAMPTZ,
    last_used_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS partner_keys_tenant_idx ON partner_keys (tenant_id);

CREATE TABLE IF NOT EXISTS module_artifacts (
    service_id          UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    version             INTEGER NOT NULL,
    engine_fingerprint  TEXT NOT NULL,
    wasm_sha256         TEXT NOT NULL,
    sha256              TEXT NOT NULL,
    size_bytes          BIGINT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (service_id, version, engine_fingerprint)
);

ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;
