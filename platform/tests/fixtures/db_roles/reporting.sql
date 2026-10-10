
CREATE ROLE {reporting_owner} NOLOGIN;
CREATE ROLE {reports_ro} NOLOGIN;
CREATE ROLE {dashboard_ro} LOGIN PASSWORD 'dashboard';
GRANT {reporting_owner} TO CURRENT_USER;

CREATE SCHEMA reporting AUTHORIZATION {reporting_owner};
GRANT USAGE ON SCHEMA reporting TO {reports_ro}, {dashboard_ro};
ALTER DEFAULT PRIVILEGES FOR ROLE {reporting_owner} IN SCHEMA reporting
    GRANT SELECT ON TABLES TO {reports_ro}, {dashboard_ro};
