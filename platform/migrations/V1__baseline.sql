-- V1: the platform schema as it stood before versioned migrations. It is
-- byte-for-byte the idempotent DDL the platform used to run on every start,
-- so an existing install records it as applied without changing anything.
-- Frozen: refinery verifies its checksum on every startup, so any change to
-- this file goes into a new V<n>__ file instead.

CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    keycloak_user_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS services (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    current_version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(tenant_id, name)
);

CREATE TABLE IF NOT EXISTS deployments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    manifest_hash TEXT NOT NULL,
    manifest JSONB NOT NULL,
    wasm_hash TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    deployed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(service_id, version)
);

CREATE TABLE IF NOT EXISTS schema_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    schema_json JSONB NOT NULL,
    hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(service_id, version)
);

CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    key_hash TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID REFERENCES tenants(id) ON DELETE SET NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT,
    details JSONB,
    ip_address TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS service_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    is_secret BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(service_id, key)
);

CREATE TABLE IF NOT EXISTS roles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    permissions JSONB NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(tenant_id, name)
);

CREATE TABLE IF NOT EXISTS user_roles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    role_id UUID NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(tenant_id, user_id, role_id)
);

CREATE TABLE IF NOT EXISTS file_uploads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    table_name TEXT,
    record_id UUID,
    filename TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    size_bytes BIGINT NOT NULL,
    storage_path TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS webhooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    event TEXT NOT NULL,
    url TEXT NOT NULL,
    secret TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(service_id, table_name, event, url)
);

ALTER TABLE roles ADD COLUMN IF NOT EXISTS service_id UUID REFERENCES services(id) ON DELETE CASCADE;
ALTER TABLE roles ADD COLUMN IF NOT EXISTS is_default BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS artifact_s3_key TEXT;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS artifact_hash TEXT;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS artifact_size BIGINT;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS keycloak_url TEXT;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS keycloak_realm TEXT;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS deploy_role TEXT NOT NULL DEFAULT 'cufflink-deploy';
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS keycloak_ops_client_id TEXT;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS keycloak_ops_client_secret TEXT;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS commit_hash TEXT;

CREATE TABLE IF NOT EXISTS backup_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    tenant_id UUID NOT NULL,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    tables_filter TEXT[],
    clear_existing BOOLEAN NOT NULL DEFAULT false,
    s3_key TEXT,
    total_rows BIGINT NOT NULL DEFAULT 0,
    processed_rows BIGINT NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS is_platform_admin BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE api_keys ALTER COLUMN tenant_id DROP NOT NULL;

CREATE TABLE IF NOT EXISTS wasm_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    tenant_slug TEXT NOT NULL,
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    service_slug TEXT NOT NULL,
    service_version INTEGER NOT NULL,
    handler TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    request_json JSONB NOT NULL,
    auth_subject TEXT,
    auth_claims JSONB,
    result_json JSONB,
    error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    timeout_secs INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (status IN ('pending','running','completed','failed'))
);

CREATE INDEX IF NOT EXISTS wasm_jobs_tenant_status_idx
    ON wasm_jobs (tenant_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS wasm_jobs_sweep_idx
    ON wasm_jobs (expires_at)
    WHERE status IN ('completed','failed');
CREATE INDEX IF NOT EXISTS wasm_jobs_zombie_idx
    ON wasm_jobs (started_at)
    WHERE status = 'running';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'deployments_wasm_hash_required'
      AND conrelid = 'deployments'::regclass
  ) THEN
    ALTER TABLE deployments
      ADD CONSTRAINT deployments_wasm_hash_required
      CHECK (manifest->>'mode' != 'wasm' OR wasm_hash IS NOT NULL)
      NOT VALID;
  END IF;
END $$;
