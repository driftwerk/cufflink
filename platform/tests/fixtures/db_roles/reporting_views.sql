
SET ROLE {reporting_owner};

CREATE VIEW reporting.item_overview AS
    SELECT n.id AS loan_id, n.title, a.body AS stock
    FROM loan_service.items n
    LEFT JOIN stock_service.items a ON a.body = n.title;

CREATE VIEW reporting.item_overview_scoped AS
    SELECT loan_id, title, stock
    FROM reporting.item_overview
    WHERE title = current_setting('app.scope', true);

CREATE VIEW reporting.service_totals AS
    SELECT 'loan-service' AS service, count(*) AS items FROM loan_service.items
    UNION ALL
    SELECT 'stock-service', count(*) FROM stock_service.items
    UNION ALL
    SELECT 'reports-service', count(*) FROM reports_service.items;

RESET ROLE;
