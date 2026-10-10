use crate::infra::{with_dbname, Tenant, TestDb};
use cufflink_types::SqlMigration;
use std::path::PathBuf;
use tokio_postgres::Client;

pub const BOOTSTRAP: &str = "db/bootstrap.sql";
pub const CATALOG_SUBSET: &str = "db/catalog_subset.sql";
pub const REPORTING: &str = "db/reporting.sql";
pub const DEFERRED_HISTORY: &str = "db/deferred_history.sql";
pub const LOAN_MIGRATIONS: &str = "migrations/loan_service";

pub const MIGRATED_THROUGH: i64 = 2;

pub fn path(fixture: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../platform/tests/fixtures/legacy")
        .join(fixture)
}

pub fn sql(fixture: &str) -> String {
    std::fs::read_to_string(path(fixture))
        .unwrap_or_else(|e| panic!("fixture {fixture} cannot be read: {e}"))
}

pub fn loan_migrations() -> Vec<SqlMigration> {
    cufflink_migrate::scan_dir(&path(LOAN_MIGRATIONS)).expect("loan_service migrations")
}

pub async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect to {url}: {e}"));
    tokio::spawn(connection);
    client
}

pub async fn tenant_database(db: &TestDb, tenant: &Tenant) -> Client {
    let name = cufflink_platform::tenant_db::TenantPoolManager::managed_db_name(&tenant.slug);
    connect(&with_dbname(&db.url, &name)).await
}

pub async fn apply(client: &Client, files: &[&str]) {
    for file in files {
        client
            .batch_execute(&sql(file))
            .await
            .unwrap_or_else(|e| panic!("fixture {file} failed: {e}"));
    }
}

pub async fn migrated(client: &Client) {
    apply(client, &[BOOTSTRAP]).await;
    migrated_after_bootstrap(client).await;
}

pub async fn migrated_after_bootstrap(client: &Client) {
    client
        .batch_execute("SET ROLE loan_service_user")
        .await
        .expect("loan_service_user");
    for migration in loan_migrations()
        .iter()
        .filter(|m| i64::from(m.version) <= MIGRATED_THROUGH)
    {
        client
            .batch_execute(&migration.sql)
            .await
            .unwrap_or_else(|e| panic!("{} failed: {e}", migration.file_stem()));
    }
    client.batch_execute("RESET ROLE").await.expect("reset");
    apply(client, &[CATALOG_SUBSET, REPORTING]).await;
}

const DDL_RECORDER: &str = "\
CREATE TABLE public.ddl_recorded (
    at timestamptz NOT NULL DEFAULT clock_timestamp(),
    tag text NOT NULL,
    schema_name text,
    object text);
CREATE TABLE public.ddl_statements (
    at timestamptz NOT NULL DEFAULT clock_timestamp(),
    tag text NOT NULL,
    statement text NOT NULL);
CREATE SEQUENCE public.ddl_statements_begun;
CREATE FUNCTION public.record_statement() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM nextval('public.ddl_statements_begun');
    INSERT INTO public.ddl_statements (tag, statement) VALUES (tg_tag, current_query());
END $$;
CREATE FUNCTION public.record_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO public.ddl_recorded (tag, schema_name, object)
    SELECT command_tag, schema_name, object_identity FROM pg_event_trigger_ddl_commands();
END $$;
CREATE FUNCTION public.record_drop() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO public.ddl_recorded (tag, schema_name, object)
    SELECT 'DROP ' || object_type, schema_name, object_identity
      FROM pg_event_trigger_dropped_objects() WHERE original;
END $$;
CREATE EVENT TRIGGER record_statement ON ddl_command_start
    EXECUTE FUNCTION public.record_statement();
CREATE EVENT TRIGGER record_ddl ON ddl_command_end EXECUTE FUNCTION public.record_ddl();
CREATE EVENT TRIGGER record_drop ON sql_drop EXECUTE FUNCTION public.record_drop();";

pub async fn record_ddl(client: &Client) {
    client
        .batch_execute(DDL_RECORDER)
        .await
        .expect("the DDL recorder needs a superuser");
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DdlStatements {
    pub begun: i64,
    pub committed: Vec<String>,
}

impl DdlStatements {
    pub const NONE: Self = Self {
        begun: 0,
        committed: Vec::new(),
    };
}

pub async fn ddl_statements(client: &Client) -> DdlStatements {
    let begun = client
        .query_one(
            "SELECT CASE WHEN is_called THEN last_value ELSE 0 END \
             FROM public.ddl_statements_begun",
            &[],
        )
        .await
        .expect("DDL statements begun")
        .get(0);
    let committed = client
        .query(
            "SELECT tag || ': ' || statement FROM public.ddl_statements ORDER BY at",
            &[],
        )
        .await
        .expect("DDL statements")
        .iter()
        .map(|row| row.get(0))
        .collect();
    DdlStatements { begun, committed }
}

pub async fn recorded_ddl(client: &Client, schema: &str) -> Vec<String> {
    client
        .query(
            "SELECT tag || ' ' || coalesce(object, '') FROM public.ddl_recorded \
             WHERE schema_name = $1 ORDER BY at",
            &[&schema],
        )
        .await
        .expect("recorded DDL")
        .iter()
        .map(|row| row.get(0))
        .collect()
}
