CREATE OR REPLACE VIEW reporting.book_overview AS
SELECT
    b.id AS book_id,
    b.created_at AS book_created_at,
    b.updated_at AS book_updated_at,
    b.status AS book_status,
    b.format AS book_format,
    b.tags,
    b.replacement_cost,
    l.loan_count,
    l.last_loan_at
FROM catalog_service.book b
LEFT JOIN (
    SELECT book_id, count(*) AS loan_count, max(created_at) AS last_loan_at
    FROM loan_service.loans
    GROUP BY book_id
) l ON l.book_id = b.id;

CREATE OR REPLACE VIEW reporting.book_overview_scoped AS
SELECT *
FROM reporting.book_overview
WHERE
       current_setting('app.scope', true) = 'global'
    OR (current_setting('app.scope', true) = 'self'
        AND book_id IN (
            SELECT book_id FROM loan_service.loans
            WHERE created_by = nullif(current_setting('app.user_id', true), '')::uuid
        ));

GRANT SELECT ON ALL TABLES IN SCHEMA reporting TO report_service_user;
GRANT SELECT ON ALL TABLES IN SCHEMA reporting TO database_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA reporting GRANT SELECT ON TABLES TO report_service_user;
GRANT SELECT ON reporting.book_overview_scoped TO kiosk_report_user;
REVOKE ALL ON reporting.book_overview FROM kiosk_report_user;
