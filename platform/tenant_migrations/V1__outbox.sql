CREATE SCHEMA IF NOT EXISTS _cufflink;
REVOKE ALL ON SCHEMA _cufflink FROM PUBLIC;

CREATE TABLE _cufflink.outbox (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    msg_id       UUID        NOT NULL,
    kind         TEXT        NOT NULL CHECK (kind IN ('event', 'audit', 'webhook')),
    service      TEXT        NOT NULL,
    source_role  NAME        NOT NULL DEFAULT session_user,
    subject      TEXT        NOT NULL,
    payload      BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    attempts     INTEGER     NOT NULL DEFAULT 0,
    last_error   TEXT
);
CREATE INDEX outbox_due_idx ON _cufflink.outbox (available_at, id);

CREATE FUNCTION _cufflink.outbox_notify() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('_cufflink_outbox', '');
    RETURN NULL;
END $$;
CREATE TRIGGER outbox_notify AFTER INSERT ON _cufflink.outbox
    FOR EACH STATEMENT EXECUTE FUNCTION _cufflink.outbox_notify();

CREATE FUNCTION _cufflink.outbox_enqueue_many(
    p_kind TEXT[], p_service TEXT[], p_subject TEXT[],
    p_payload BYTEA[], p_headers JSONB[], p_msg_id UUID[]
) RETURNS VOID LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, _cufflink AS $$
    INSERT INTO _cufflink.outbox (kind, service, subject, payload, headers, msg_id)
    SELECT * FROM unnest(p_kind, p_service, p_subject, p_payload, p_headers, p_msg_id)
$$;
REVOKE ALL ON FUNCTION _cufflink.outbox_enqueue_many(TEXT[], TEXT[], TEXT[], BYTEA[], JSONB[], UUID[]) FROM PUBLIC;
