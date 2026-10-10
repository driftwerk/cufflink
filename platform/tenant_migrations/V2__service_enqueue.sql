CREATE TABLE _cufflink.service_roles (
    role    NAME PRIMARY KEY,
    service TEXT NOT NULL
);
REVOKE ALL ON _cufflink.service_roles FROM PUBLIC;

CREATE FUNCTION _cufflink.outbox_enqueue_own(
    p_kind TEXT[], p_subject TEXT[], p_payload BYTEA[], p_headers JSONB[], p_msg_id UUID[]
) RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    caller NAME := CASE current_setting('role') WHEN 'none' THEN session_user
                                                ELSE current_setting('role') END;
    own TEXT;
BEGIN
    SELECT service INTO own FROM _cufflink.service_roles WHERE role = caller;
    IF own IS NULL THEN
        RAISE EXCEPTION 'role % enqueues for no service', caller
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    INSERT INTO _cufflink.outbox (kind, service, subject, payload, headers, msg_id)
    SELECT kind, own, subject, payload, headers, msg_id
    FROM unnest(p_kind, p_subject, p_payload, p_headers, p_msg_id)
        AS rows (kind, subject, payload, headers, msg_id);
END $$;
REVOKE ALL ON FUNCTION _cufflink.outbox_enqueue_own(TEXT[], TEXT[], BYTEA[], JSONB[], UUID[]) FROM PUBLIC;
