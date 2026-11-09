-- Web bundles uploaded for a service, stored by content, before or after
-- a release records them. A row no deployment records is removed, with
-- its object, once older than WEB_UPLOAD_RETENTION_SECS.
-- Additive only, so an earlier binary runs against this schema and never
-- reads it.

SET LOCAL lock_timeout = '10s';
CREATE TABLE IF NOT EXISTS web_uploads (
    service_id  UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    sha256      TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    s3_key      TEXT NOT NULL,
    size        BIGINT NOT NULL CHECK (size > 0),
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    uploaded_by TEXT NOT NULL,
    PRIMARY KEY (service_id, sha256)
);
CREATE INDEX IF NOT EXISTS web_uploads_uploaded_at_idx ON web_uploads (uploaded_at);
