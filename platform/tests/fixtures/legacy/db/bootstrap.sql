DO $$
DECLARE
    service_role text;
BEGIN
    FOREACH service_role IN ARRAY ARRAY[
        'loan_service_user', 'audit_service_user', 'catalog_service_user',
        'report_service_user', 'kiosk_report_user'
    ] LOOP
        BEGIN
            EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', service_role, 'service_password_placeholder');
        EXCEPTION WHEN duplicate_object OR unique_violation THEN
            NULL;
        END;
    END LOOP;
    FOREACH service_role IN ARRAY ARRAY['database_reader', 'service_reader'] LOOP
        BEGIN
            EXECUTE format('CREATE ROLE %I NOLOGIN', service_role);
        EXCEPTION WHEN duplicate_object OR unique_violation THEN
            NULL;
        END;
    END LOOP;
END $$;

CREATE SCHEMA IF NOT EXISTS loan_service AUTHORIZATION loan_service_user;
CREATE SCHEMA IF NOT EXISTS audit_service AUTHORIZATION audit_service_user;
CREATE SCHEMA IF NOT EXISTS catalog_service AUTHORIZATION catalog_service_user;
CREATE SCHEMA IF NOT EXISTS reporting;

GRANT USAGE ON SCHEMA public TO loan_service_user, audit_service_user, catalog_service_user;
GRANT ALL ON SCHEMA reporting TO report_service_user;
GRANT USAGE ON SCHEMA reporting TO kiosk_report_user, database_reader;
GRANT USAGE ON SCHEMA loan_service, audit_service, catalog_service TO database_reader, service_reader;
