-- Extensions: what each tenant registers, served by a native service or a
-- NATS provider. Additive only, so an R3 binary runs against this schema.

SET LOCAL lock_timeout = '10s';
CREATE TABLE IF NOT EXISTS tenant_extensions (
    tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name       TEXT NOT NULL,
    backend    TEXT NOT NULL CHECK (backend IN ('nats', 'native')),
    service_id UUID REFERENCES services(id) ON DELETE CASCADE,
    callers    TEXT[] NOT NULL CHECK (cardinality(callers) > 0),
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, name),
    CHECK ((backend = 'native') = (service_id IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS tenant_extensions_service_idx ON tenant_extensions (service_id);
