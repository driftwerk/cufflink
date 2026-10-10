use crate::db_roles::enqueue::{self, Enqueue, CUFFLINK_SCHEMA};
use crate::db_roles::login_pools;
use crate::deployer::manifest::{ServiceManifest, TableDefinition};
use crate::errors::PlatformResult;
use crate::repo::service_db_roles::Binding;
use crate::repo::services::Service;
use crate::repo::tenants;
use crate::secrets::{SecretBox, SecretString, DB_ROLE_PASSWORD};
use crate::tenant_sessions::TenantSessions;
use crate::AppState;
use chrono::{DateTime, Utc};
use cufflink_db::quote_ident;
use cufflink_types::manifest::validate::{folded, folded_ident};
use deadpool_postgres::GenericClient;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const PG16: i32 = 160000;

pub const PRIVILEGED_ROLES: [&str; 10] = [
    "pg_checkpoint",
    "pg_create_subscription",
    "pg_database_owner",
    "pg_execute_server_program",
    "pg_maintain",
    "pg_read_all_data",
    "pg_read_server_files",
    "pg_signal_backend",
    "pg_write_all_data",
    "pg_write_server_files",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Check {
    pub check: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub inconclusive: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remedy: Option<String>,
}

impl Check {
    fn passed(check: &str) -> Self {
        Self {
            check: check.to_string(),
            ok: true,
            inconclusive: false,
            detail: None,
            remedy: None,
        }
    }

    fn failed(check: &str, detail: impl Into<String>, remedy: Option<String>) -> Self {
        Self {
            check: check.to_string(),
            ok: false,
            inconclusive: false,
            detail: Some(detail.into()),
            remedy,
        }
    }

    fn unanswered(check: &str, detail: impl Into<String>) -> Self {
        Self {
            inconclusive: true,
            ..Self::failed(check, detail, None)
        }
    }

    pub fn of(check: &str, ok: bool, failure: impl FnOnce() -> (String, Option<String>)) -> Self {
        match ok {
            true => Self::passed(check),
            false => {
                let (detail, remedy) = failure();
                Self::failed(check, detail, remedy)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Verified,
    Refuted,
    Undetermined,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyReport {
    pub role: String,
    pub verified: bool,
    pub checked_at: DateTime<Utc>,
    pub server_version_num: i32,
    pub checks: Vec<Check>,
    pub remediation: Vec<String>,
}

impl VerifyReport {
    fn of(role: &str, server_version_num: i32, now: DateTime<Utc>, checks: Vec<Check>) -> Self {
        Self {
            role: role.to_string(),
            verified: checks.iter().all(|c| c.ok),
            checked_at: now,
            server_version_num,
            remediation: checks.iter().filter_map(|c| c.remedy.clone()).collect(),
            checks,
        }
    }

    pub fn unanswered(role: &str, reason: impl Into<String>, now: DateTime<Utc>) -> Self {
        Self::of(
            role,
            0,
            now,
            vec![Check::unanswered("database_reachable", reason)],
        )
    }

    pub fn verdict(&self) -> Verdict {
        let refuted = self.checks.iter().any(|c| !c.ok && !c.inconclusive);
        match (refuted, self.verified) {
            (true, _) => Verdict::Refuted,
            (false, true) => Verdict::Verified,
            (false, false) => Verdict::Undetermined,
        }
    }

    pub fn failed(&self) -> Vec<&str> {
        self.checks
            .iter()
            .filter(|c| !c.ok)
            .map(|c| c.check.as_str())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Probe {
    Accepted,
    Refused(String),
    Unanswered(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicCreate {
    No,
    ThroughPublic,
    Granted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleFacts {
    pub can_login: bool,
    pub attributes: Vec<String>,
    pub is_platform: bool,
    pub privileged: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Facts {
    pub server_version_num: i32,
    pub platform_role: String,
    pub database: String,
    pub role: Option<RoleFacts>,
    pub member: bool,
    pub inherits: bool,
    pub can_set: Option<bool>,
    pub login: Option<Probe>,
    pub connect: bool,
    pub schema: Option<(bool, bool)>,
    pub public_create: PublicCreate,
    pub tables: Vec<(String, bool, bool)>,
    pub read_only: Option<(bool, bool)>,
    pub cufflink: Option<(bool, Enqueue)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expected<'a> {
    pub role: &'a str,
    pub managed: bool,
    pub service: &'a str,
    pub schema: &'a str,
    pub tables: &'a [(String, Holds)],
    pub read_only_role: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Holds {
    Dml,
    Nothing,
}

impl Expected<'_> {
    fn holds(&self, table: &str) -> Holds {
        self.tables
            .iter()
            .find(|(declared, _)| declared == table)
            .map_or(Holds::Dml, |(_, holds)| *holds)
    }
}

const TABLE_FACTS: &str = "SELECT to_regclass($1) IS NOT NULL,
                        CASE WHEN to_regclass($1) IS NOT NULL
                             THEN has_table_privilege($2, to_regclass($1), 'SELECT, INSERT, UPDATE, DELETE') END";

pub fn membership_grant(
    server_version_num: i32,
    granted: &str,
    member: &str,
    inherit: bool,
) -> String {
    let (granted, member) = (quote_ident(granted), quote_ident(member));
    match server_version_num >= PG16 {
        true => format!(
            "GRANT {granted} TO {member} WITH INHERIT {}, SET TRUE",
            if inherit { "TRUE" } else { "FALSE" }
        ),
        false => format!("GRANT {granted} TO {member}"),
    }
}

fn role_checks(expected: &Expected<'_>, facts: &RoleFacts) -> [Check; 4] {
    let role = quote_ident(expected.role);
    [
        Check::of("can_login", facts.can_login, || {
            (
                "the role cannot log in".into(),
                Some(format!("ALTER ROLE {role} LOGIN;")),
            )
        }),
        Check::of("role_attributes", facts.attributes.is_empty(), || {
            (
                format!("the role is {}", facts.attributes.join(", ")),
                Some(format!(
                    "ALTER ROLE {role} {};",
                    facts
                        .attributes
                        .iter()
                        .map(|attribute| format!("NO{attribute}"))
                        .collect::<Vec<_>>()
                        .join(" ")
                )),
            )
        }),
        Check::of("own_role", !facts.is_platform, || {
            (
                "the role is the one the platform connects as; bind a role of the service's own"
                    .into(),
                None,
            )
        }),
        Check::of("role_memberships", facts.privileged.is_empty(), || {
            (
                format!("the role is a member of {}", facts.privileged.join(", ")),
                Some(format!(
                    "REVOKE {} FROM {role};",
                    facts
                        .privileged
                        .iter()
                        .map(|held| quote_ident(held))
                        .collect::<Vec<_>>()
                        .join(", ")
                )),
            )
        }),
    ]
}

fn credentials_check(expected: &Expected<'_>, login: Option<&Probe>) -> Check {
    let new_password = || {
        Some(
            match expected.managed {
                true => "cufflink services db-role rotate <tenant> <service>",
                false => "cufflink services db-role set <tenant> <service> --password-stdin",
            }
            .to_string(),
        )
    };
    match login {
        None => Check::failed(
            "credentials",
            "no password is stored for the role",
            new_password(),
        ),
        Some(Probe::Accepted) => Check::passed("credentials"),
        Some(Probe::Refused(error)) => Check::failed(
            "credentials",
            format!("logging in as the role failed: {error}"),
            new_password(),
        ),
        Some(Probe::Unanswered(error)) => Check::unanswered(
            "credentials",
            format!("a login as the role could not be tried: {error}"),
        ),
    }
}

fn read_only_check(
    expected: &Expected<'_>,
    facts: &Facts,
    read_only: &str,
    state: (bool, bool),
) -> Check {
    match state {
        (false, _) => Check::failed(
            "read_only_role",
            format!("role {read_only} does not exist"),
            Some(format!("CREATE ROLE {} NOLOGIN;", quote_ident(read_only))),
        ),
        (true, false) => Check::failed(
            "read_only_role",
            format!("the role may not switch to {read_only}"),
            Some(format!(
                "{};",
                membership_grant(facts.server_version_num, read_only, expected.role, false)
            )),
        ),
        (true, true) => Check::passed("read_only_role"),
    }
}

fn cufflink_checks(expected: &Expected<'_>, (usage, enqueue): (bool, Enqueue)) -> Vec<Check> {
    std::iter::once(Check::of("cufflink_schema", usage, || {
        (
            format!("the role lacks USAGE on schema {CUFFLINK_SCHEMA}"),
            Some(format!(
                "GRANT USAGE ON SCHEMA {CUFFLINK_SCHEMA} TO {};",
                quote_ident(expected.role)
            )),
        )
    }))
    .chain(enqueue::checks(expected.role, expected.service, enqueue))
    .collect()
}

pub fn report(expected: &Expected<'_>, facts: &Facts, now: DateTime<Utc>) -> VerifyReport {
    let role = quote_ident(expected.role);
    let schema = folded_ident(expected.schema);
    let exists = match &facts.role {
        None => vec![Check::failed(
            "role_exists",
            format!("role {} does not exist", expected.role),
            Some(format!("CREATE ROLE {role} LOGIN;")),
        )],
        Some(role_facts) => std::iter::once(Check::passed("role_exists"))
            .chain(role_checks(expected, role_facts))
            .collect(),
    };
    let membership = facts.member && facts.inherits && facts.can_set.unwrap_or(true);
    let reach = [
        Check::of("platform_membership", membership, || {
            (
                format!(
                    "{} is not a member of the role with INHERIT and SET",
                    facts.platform_role
                ),
                Some(format!(
                    "{};",
                    membership_grant(
                        facts.server_version_num,
                        expected.role,
                        &facts.platform_role,
                        true
                    )
                )),
            )
        }),
        credentials_check(expected, facts.login.as_ref()),
        Check::of("database_connect", facts.connect, || {
            (
                format!("the role cannot connect to {}", facts.database),
                Some(format!(
                    "GRANT CONNECT ON DATABASE {} TO {role};",
                    quote_ident(&facts.database)
                )),
            )
        }),
        match facts.schema {
            None => Check::failed(
                "schema",
                format!("schema {schema} does not exist"),
                Some(format!("CREATE SCHEMA {schema} AUTHORIZATION {role};")),
            ),
            Some((true, true)) => Check::passed("schema"),
            Some(_) => Check::failed(
                "schema",
                format!("the role lacks USAGE or CREATE on schema {schema}"),
                Some(format!("GRANT USAGE, CREATE ON SCHEMA {schema} TO {role};")),
            ),
        },
        match facts.public_create {
            PublicCreate::No => Check::passed("public_schema"),
            PublicCreate::ThroughPublic => Check::failed(
                "public_schema",
                "every role may create in schema public, which is on every search path",
                Some("REVOKE CREATE ON SCHEMA public FROM PUBLIC;".to_string()),
            ),
            PublicCreate::Granted => Check::failed(
                "public_schema",
                "the role may create in schema public, which is on every search path",
                Some(format!("REVOKE CREATE ON SCHEMA public FROM {role};")),
            ),
        },
    ];
    let tables = facts.tables.iter().map(|(table, exists, privileges)| {
        let qualified = format!("{schema}.{}", folded_ident(table));
        match (exists, privileges, expected.holds(table)) {
            (false, _, _) => {
                Check::failed("table", format!("table {qualified} does not exist"), None)
            }
            (true, true, _) | (true, _, Holds::Nothing) => Check::passed("table"),
            (true, false, Holds::Dml) => Check::failed(
                "table",
                format!("the role lacks SELECT, INSERT, UPDATE or DELETE on {qualified}"),
                Some(format!(
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON {qualified} TO {role};"
                )),
            ),
        }
    });
    let read_only = expected
        .read_only_role
        .zip(facts.read_only)
        .map(|(read_only, state)| read_only_check(expected, facts, read_only, state));
    let cufflink = facts
        .cufflink
        .map(|granted| cufflink_checks(expected, granted))
        .unwrap_or_default();
    VerifyReport::of(
        expected.role,
        facts.server_version_num,
        now,
        exists
            .into_iter()
            .chain(reach)
            .chain(tables)
            .chain(read_only)
            .chain(cufflink)
            .collect(),
    )
}

pub async fn open_password(
    secrets: &SecretBox,
    platform: &impl GenericClient,
    tenant_id: Uuid,
    service_id: Uuid,
    sealed: &[u8],
) -> Result<SecretString, crate::secrets::SecretError> {
    secrets
        .open(
            platform,
            tenant_id,
            &DB_ROLE_PASSWORD.aad([service_id.to_string()]),
            sealed,
        )
        .await
}

pub async fn password(
    secrets: &SecretBox,
    platform: &impl GenericClient,
    tenant_id: Uuid,
    binding: &Binding,
) -> Result<Option<SecretString>, crate::secrets::SecretError> {
    match &binding.password_enc {
        Some(sealed) => open_password(secrets, platform, tenant_id, binding.service_id, sealed)
            .await
            .map(Some),
        None => Ok(None),
    }
}

pub async fn role_facts(
    client: &impl GenericClient,
    role: &str,
) -> Result<Option<RoleFacts>, tokio_postgres::Error> {
    let Some(row) = client
        .query_opt(
            "SELECT rolcanlogin, rolname IN (current_user, session_user),
                    array_remove(ARRAY[CASE WHEN rolsuper THEN 'SUPERUSER' END,
                                       CASE WHEN rolcreatedb THEN 'CREATEDB' END,
                                       CASE WHEN rolcreaterole THEN 'CREATEROLE' END,
                                       CASE WHEN rolreplication THEN 'REPLICATION' END,
                                       CASE WHEN rolbypassrls THEN 'BYPASSRLS' END], NULL)
             FROM pg_roles WHERE rolname = $1",
            &[&role],
        )
        .await?
    else {
        return Ok(None);
    };
    let privileged = client
        .query(
            "WITH RECURSIVE held(oid) AS (
                 SELECT m.roleid FROM pg_auth_members m
                 JOIN pg_roles r ON r.oid = m.member WHERE r.rolname = $1
                 UNION
                 SELECT m.roleid FROM pg_auth_members m JOIN held ON m.member = held.oid)
             SELECT r.rolname::text FROM held JOIN pg_roles r ON r.oid = held.oid
             WHERE r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication
                OR r.rolbypassrls OR r.rolname IN (current_user, session_user)
                OR r.rolname = ANY($2)
             ORDER BY 1",
            &[&role, &PRIVILEGED_ROLES.as_slice()],
        )
        .await?;
    Ok(Some(RoleFacts {
        can_login: row.get(0),
        is_platform: row.get(1),
        attributes: row.get(2),
        privileged: privileged.iter().map(|row| row.get(0)).collect(),
    }))
}

async fn login(
    sessions: &TenantSessions,
    tenant_id: Uuid,
    role: &str,
    password: &SecretString,
) -> Probe {
    match sessions.login_config(tenant_id).await {
        Ok((config, settings)) => {
            login_pools::probe(
                login_pools::login_config(&config, role, password),
                &settings,
            )
            .await
        }
        Err(e) => Probe::Unanswered(e.to_string()),
    }
}

pub async fn facts(
    client: &impl GenericClient,
    sessions: &TenantSessions,
    tenant_id: Uuid,
    expected: &Expected<'_>,
    password: Option<&SecretString>,
) -> Result<Facts, tokio_postgres::Error> {
    let Expected {
        role,
        schema,
        tables,
        read_only_role,
        ..
    } = *expected;
    let head = client
        .query_one(
            "SELECT current_setting('server_version_num')::int, current_user::text, current_database()::text",
            &[],
        )
        .await?;
    let server_version_num: i32 = head.get(0);
    let platform_role: String = head.get(1);
    let database: String = head.get(2);
    let Some(role_facts) = role_facts(client, role).await? else {
        return Ok(Facts {
            server_version_num,
            platform_role,
            database,
            role: None,
            member: false,
            inherits: false,
            can_set: None,
            login: password.map(|_| Probe::Refused("the role does not exist".to_string())),
            connect: false,
            schema: None,
            public_create: PublicCreate::No,
            tables: tables
                .iter()
                .map(|(table, _)| (table.clone(), false, false))
                .collect(),
            read_only: read_only_role.map(|_| (false, false)),
            cufflink: None,
        });
    };
    let folded = folded(schema);
    let privileges = client
        .query_one(
            "SELECT pg_has_role(current_user, $1, 'MEMBER'),
                    pg_has_role(current_user, $1, 'USAGE'),
                    CASE WHEN current_setting('server_version_num')::int >= $4
                         THEN pg_has_role(current_user, $1, 'SET') END,
                    has_database_privilege($1, current_database(), 'CONNECT'),
                    EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $2),
                    CASE WHEN EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $2)
                         THEN has_schema_privilege($1, $2, 'USAGE') END,
                    CASE WHEN EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $2)
                         THEN has_schema_privilege($1, $2, 'CREATE') END,
                    EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $3),
                    CASE WHEN EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $3)
                         THEN has_schema_privilege($1, $3, 'USAGE') END,
                    CASE WHEN EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'public')
                         THEN has_schema_privilege($1, 'public', 'CREATE') END,
                    CASE WHEN EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'public')
                         THEN has_schema_privilege('public', 'public', 'CREATE') END",
            &[&role, &folded, &CUFFLINK_SCHEMA, &PG16],
        )
        .await?;
    let granted = |index: usize| privileges.get::<_, Option<bool>>(index).unwrap_or(false);
    let schema_exists: bool = privileges.get(4);
    let cufflink_exists: bool = privileges.get(7);
    let mut table_facts = Vec::with_capacity(tables.len());
    for (table, _) in tables {
        let row = client
            .query_one(
                TABLE_FACTS,
                &[
                    &format!("{}.{}", folded_ident(schema), folded_ident(table)),
                    &role,
                ],
            )
            .await?;
        table_facts.push((
            table.clone(),
            row.get(0),
            row.get::<_, Option<bool>>(1).unwrap_or(false),
        ));
    }
    let read_only = match read_only_role {
        Some(read_only) => {
            let row = client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $2),
                            CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $2)
                                 THEN pg_has_role($1, $2, $3) END",
                    &[&role, &read_only, &switch_privilege(server_version_num)],
                )
                .await?;
            Some((row.get(0), row.get::<_, Option<bool>>(1).unwrap_or(false)))
        }
        None => None,
    };
    let login = match password {
        Some(password) => Some(login(sessions, tenant_id, role, password).await),
        None => None,
    };
    Ok(Facts {
        server_version_num,
        platform_role,
        database,
        role: Some(role_facts),
        member: privileges.get(0),
        inherits: privileges.get(1),
        can_set: privileges.get(2),
        login,
        connect: privileges.get(3),
        schema: schema_exists.then(|| (granted(5), granted(6))),
        public_create: match (granted(9), granted(10)) {
            (false, _) => PublicCreate::No,
            (true, true) => PublicCreate::ThroughPublic,
            (true, false) => PublicCreate::Granted,
        },
        tables: table_facts,
        read_only,
        cufflink: match cufflink_exists {
            true => Some((
                granted(8),
                enqueue::facts(client, Some(role), expected.service).await?,
            )),
            false => None,
        },
    })
}

pub fn switch_privilege(server_version_num: i32) -> &'static str {
    match server_version_num >= PG16 {
        true => "SET",
        false => "MEMBER",
    }
}

pub(crate) fn requirements(manifest: &ServiceManifest) -> Vec<(String, Holds)> {
    let held = |holds| move |table: &TableDefinition| (table.name.clone(), holds);
    manifest
        .tables
        .iter()
        .map(held(Holds::Dml))
        .chain(manifest.private_tables.iter().map(held(Holds::Nothing)))
        .collect()
}

async fn declared_tables(
    client: &impl GenericClient,
    service_id: Uuid,
) -> PlatformResult<Vec<(String, Holds)>> {
    Ok(client
        .query_opt(
            "SELECT d.manifest FROM services s
             JOIN deployments d ON d.service_id = s.id AND d.version = s.current_version
             WHERE s.id = $1",
            &[&service_id],
        )
        .await?
        .map(|row| serde_json::from_value::<ServiceManifest>(row.get(0)))
        .transpose()?
        .map(|manifest| requirements(&manifest))
        .unwrap_or_default())
}

pub async fn inspect(
    state: &AppState,
    platform: &impl GenericClient,
    tenant: &tenants::Tenant,
    service: &Service,
    binding: &Candidate<'_>,
) -> PlatformResult<VerifyReport> {
    let tables = declared_tables(platform, service.id).await?;
    let schema = service.schema();
    let expected = Expected {
        role: binding.role,
        managed: binding.managed,
        service: &service.name,
        schema: &schema,
        tables: &tables,
        read_only_role: binding.read_only_role,
    };
    let asked = async {
        let tenant_db = state
            .tenant_sessions
            .platform(tenant.id, None)
            .await
            .map_err(|e| e.reason())?;
        facts(
            &tenant_db,
            &state.tenant_sessions,
            tenant.id,
            &expected,
            binding.password,
        )
        .await
        .map_err(|e| crate::db_error::from_pg(&e).message.clone())
    };
    Ok(match asked.await {
        Ok(facts) => report(&expected, &facts, Utc::now()),
        Err(reason) => VerifyReport::unanswered(binding.role, reason, Utc::now()),
    })
}

#[derive(Debug, Clone, Copy)]
pub struct Candidate<'a> {
    pub role: &'a str,
    pub managed: bool,
    pub read_only_role: Option<&'a str>,
    pub password: Option<&'a SecretString>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sound() -> RoleFacts {
        RoleFacts {
            can_login: true,
            attributes: vec![],
            is_platform: false,
            privileged: vec![],
        }
    }

    fn all_good() -> Facts {
        Facts {
            server_version_num: 160004,
            platform_role: "cufflink".into(),
            database: "harbor".into(),
            role: Some(sound()),
            member: true,
            inherits: true,
            can_set: Some(true),
            login: Some(Probe::Accepted),
            connect: true,
            schema: Some((true, true)),
            public_create: PublicCreate::No,
            tables: vec![("loans".into(), true, true)],
            read_only: Some((true, true)),
            cufflink: Some((true, Enqueue::Shared { execute: true })),
        }
    }

    fn expected() -> Expected<'static> {
        Expected {
            role: "loan_service_user",
            managed: false,
            service: "loan-service",
            schema: "loan_service",
            tables: &[],
            read_only_role: Some("kiosk_report_user"),
        }
    }

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_790_000_000, 0).expect("time")
    }

    #[test]
    fn a_role_that_can_do_everything_is_verified() {
        let report = report(&expected(), &all_good(), now());
        assert!(report.verified, "{report:?}");
        assert!(report.remediation.is_empty());
        assert_eq!(
            report
                .checks
                .iter()
                .map(|c| c.check.as_str())
                .collect::<Vec<_>>(),
            [
                "role_exists",
                "can_login",
                "role_attributes",
                "own_role",
                "role_memberships",
                "platform_membership",
                "credentials",
                "database_connect",
                "schema",
                "public_schema",
                "table",
                "read_only_role",
                "cufflink_schema",
                "outbox_enqueue"
            ]
        );
    }

    #[test]
    fn each_failure_names_the_statement_that_mends_it() {
        let facts = Facts {
            role: Some(RoleFacts {
                can_login: false,
                ..sound()
            }),
            member: true,
            inherits: false,
            can_set: Some(false),
            login: Some(Probe::Refused(
                "password authentication failed for user \"loan_service_user\"".into(),
            )),
            connect: false,
            schema: Some((true, false)),
            tables: vec![("loans".into(), true, false), ("Old".into(), false, false)],
            read_only: Some((true, false)),
            cufflink: Some((false, Enqueue::Shared { execute: false })),
            ..all_good()
        };
        let report = report(&expected(), &facts, now());
        assert!(!report.verified);
        assert_eq!(
            report.remediation,
            [
                "ALTER ROLE \"loan_service_user\" LOGIN;",
                "GRANT \"loan_service_user\" TO \"cufflink\" WITH INHERIT TRUE, SET TRUE;",
                "cufflink services db-role set <tenant> <service> --password-stdin",
                "GRANT CONNECT ON DATABASE \"harbor\" TO \"loan_service_user\";",
                "GRANT USAGE, CREATE ON SCHEMA \"loan_service\" TO \"loan_service_user\";",
                "GRANT SELECT, INSERT, UPDATE, DELETE ON \"loan_service\".\"loans\" TO \"loan_service_user\";",
                "GRANT \"kiosk_report_user\" TO \"loan_service_user\" WITH INHERIT FALSE, SET TRUE;",
                "GRANT USAGE ON SCHEMA _cufflink TO \"loan_service_user\";",
                "GRANT EXECUTE ON FUNCTION _cufflink.outbox_enqueue_many(TEXT[], TEXT[], TEXT[], BYTEA[], JSONB[], UUID[]) TO \"loan_service_user\";",
            ]
        );
        let missing_table = report
            .checks
            .iter()
            .find(|c| c.detail.as_deref() == Some("table \"loan_service\".\"old\" does not exist"))
            .expect("a missing table is reported");
        assert_eq!(missing_table.remedy, None);
    }

    #[test]
    fn before_postgres_16_membership_is_a_plain_grant_and_set_is_not_asked() {
        let facts = Facts {
            server_version_num: 150008,
            member: false,
            inherits: false,
            can_set: None,
            ..all_good()
        };
        let mended = report(&expected(), &facts, now());
        assert_eq!(
            mended.remediation,
            ["GRANT \"loan_service_user\" TO \"cufflink\";"]
        );
        let unread = report(
            &expected(),
            &Facts {
                server_version_num: 150008,
                can_set: None,
                read_only: Some((true, false)),
                ..all_good()
            },
            now(),
        );
        assert_eq!(
            unread.remediation,
            ["GRANT \"kiosk_report_user\" TO \"loan_service_user\";"]
        );
        assert_eq!(switch_privilege(150008), "MEMBER");
        assert_eq!(switch_privilege(160000), "SET");
        let verified = report(
            &expected(),
            &Facts {
                server_version_num: 150008,
                can_set: None,
                ..all_good()
            },
            now(),
        );
        assert!(verified.verified);
    }

    #[test]
    fn a_missing_role_stops_at_its_creation_and_a_stored_password_is_required() {
        let facts = Facts {
            role: None,
            member: false,
            inherits: false,
            can_set: None,
            login: None,
            connect: false,
            schema: None,
            tables: vec![],
            read_only: None,
            cufflink: None,
            ..all_good()
        };
        assert_eq!(
            super::report(&expected(), &facts, now()).verdict(),
            Verdict::Refuted
        );
        let report = report(
            &Expected {
                read_only_role: None,
                ..expected()
            },
            &facts,
            now(),
        );
        assert_eq!(
            report.checks[0].remedy.as_deref(),
            Some("CREATE ROLE \"loan_service_user\" LOGIN;")
        );
        assert!(report.checks.iter().any(|c| c.check == "credentials"
            && c.detail.as_deref() == Some("no password is stored for the role")
            && c.remedy.as_deref()
                == Some("cufflink services db-role set <tenant> <service> --password-stdin")));
        let managed = super::report(
            &Expected {
                managed: true,
                ..expected()
            },
            &facts,
            now(),
        );
        assert!(
            managed.checks.iter().any(|c| c.check == "credentials"
                && c.remedy.as_deref()
                    == Some("cufflink services db-role rotate <tenant> <service>")),
            "a password the platform mints is mended by minting another"
        );
        assert!(report.checks.iter().any(|c| c.check == "schema"
            && c.remedy.as_deref()
                == Some("CREATE SCHEMA \"loan_service\" AUTHORIZATION \"loan_service_user\";")));
        assert!(!report
            .checks
            .iter()
            .any(|c| c.check == "read_only_role" || c.check == "cufflink_schema"));
    }

    #[test]
    fn a_role_that_reaches_beyond_its_own_is_refused_with_what_takes_it_back() {
        let facts = Facts {
            role: Some(RoleFacts {
                can_login: true,
                attributes: vec!["SUPERUSER".into(), "BYPASSRLS".into()],
                is_platform: true,
                privileged: vec!["cufflink".into(), "pg_read_all_data".into()],
            }),
            public_create: PublicCreate::ThroughPublic,
            ..all_good()
        };
        let report = report(&expected(), &facts, now());
        assert_eq!(report.verdict(), Verdict::Refuted);
        assert_eq!(
            report.failed(),
            [
                "role_attributes",
                "own_role",
                "role_memberships",
                "public_schema"
            ]
        );
        assert_eq!(
            report.remediation,
            [
                "ALTER ROLE \"loan_service_user\" NOSUPERUSER NOBYPASSRLS;",
                "REVOKE \"cufflink\", \"pg_read_all_data\" FROM \"loan_service_user\";",
                "REVOKE CREATE ON SCHEMA public FROM PUBLIC;",
            ]
        );
        let granted = super::report(
            &expected(),
            &Facts {
                public_create: PublicCreate::Granted,
                ..all_good()
            },
            now(),
        );
        assert_eq!(
            granted.remediation,
            ["REVOKE CREATE ON SCHEMA public FROM \"loan_service_user\";"]
        );
    }

    #[test]
    fn a_login_that_was_never_judged_leaves_the_verdict_open() {
        let unanswered = report(
            &expected(),
            &Facts {
                login: Some(Probe::Unanswered("connection timed out".into())),
                ..all_good()
            },
            now(),
        );
        assert!(!unanswered.verified);
        assert_eq!(unanswered.verdict(), Verdict::Undetermined);
        assert_eq!(unanswered.failed(), ["credentials"]);
        assert!(unanswered.remediation.is_empty());
        let refuted = report(
            &expected(),
            &Facts {
                login: Some(Probe::Unanswered("connection timed out".into())),
                connect: false,
                ..all_good()
            },
            now(),
        );
        assert_eq!(
            refuted.verdict(),
            Verdict::Refuted,
            "what Postgres did answer refuses the role"
        );
        assert_eq!(
            report(&expected(), &all_good(), now()).verdict(),
            Verdict::Verified
        );
        let away = VerifyReport::unanswered("loan_service_user", "pool is closed", now());
        assert_eq!(away.verdict(), Verdict::Undetermined);
        assert_eq!(away.failed(), ["database_reachable"]);
    }

    #[test]
    fn a_report_round_trips_as_json() {
        let report = report(&expected(), &all_good(), now());
        let json = serde_json::to_value(&report).expect("json");
        assert_eq!(json["verified"], true);
        assert_eq!(
            json["checks"][0],
            serde_json::json!({"check": "role_exists", "ok": true})
        );
        let back: VerifyReport = serde_json::from_value(json).expect("report");
        assert_eq!(back, report);
    }

    #[test]
    fn a_served_table_is_asked_what_it_was() {
        assert_eq!(
            TABLE_FACTS,
            [
                "SELECT to_regclass($1) IS NOT NULL,",
                "                        CASE WHEN to_regclass($1) IS NOT NULL",
                "                             THEN has_table_privilege($2, to_regclass($1), \
                 'SELECT, INSERT, UPDATE, DELETE') END",
            ]
            .join("\n"),
            "the text v0.29.2 asked, which is true of a role holding any of the four"
        );
        let manifest: ServiceManifest = serde_json::from_value(serde_json::json!({
            "name": "loan-service",
            "tables": [{"name": "loans", "columns": []}, {"name": "labels", "columns": []}]
        }))
        .expect("manifest");
        let tables = requirements(&manifest);
        assert_eq!(
            tables,
            [
                ("loans".to_string(), Holds::Dml),
                ("labels".to_string(), Holds::Dml)
            ]
        );
        let facts = Facts {
            tables: vec![("loans".into(), true, true), ("labels".into(), true, false)],
            ..all_good()
        };
        let report = report(
            &Expected {
                tables: &tables,
                ..expected()
            },
            &facts,
            now(),
        );
        assert_eq!(report.failed(), ["table"]);
        assert_eq!(
            report.remediation,
            ["GRANT SELECT, INSERT, UPDATE, DELETE ON \"loan_service\".\"labels\" TO \"loan_service_user\";"]
        );
    }

    #[test]
    fn a_private_table_the_role_holds_nothing_on_is_verified() {
        let manifest: ServiceManifest = serde_json::from_value(serde_json::json!({
            "name": "loan-service",
            "schema": "adopted",
            "tables": [{"name": "labels", "columns": []}],
            "private_tables": [{"name": "loans", "columns": []}, {"name": "gone", "columns": []}]
        }))
        .expect("manifest");
        let tables = requirements(&manifest);
        assert_eq!(
            tables,
            [
                ("labels".to_string(), Holds::Dml),
                ("loans".to_string(), Holds::Nothing),
                ("gone".to_string(), Holds::Nothing)
            ]
        );
        let expected = Expected {
            tables: &tables,
            ..expected()
        };
        let held = |tables: &[(&str, bool, bool)]| Facts {
            tables: tables
                .iter()
                .map(|(table, exists, privileges)| (table.to_string(), *exists, *privileges))
                .collect(),
            ..all_good()
        };
        let verified = report(
            &expected,
            &held(&[("labels", true, true), ("loans", true, false)]),
            now(),
        );
        assert!(verified.verified, "{verified:?}");
        let missing = report(
            &expected,
            &held(&[("labels", true, true), ("gone", false, false)]),
            now(),
        );
        assert_eq!(missing.failed(), ["table"]);
        assert!(
            missing.checks.iter().any(|check| check.detail.as_deref()
                == Some("table \"loan_service\".\"gone\" does not exist")),
            "{missing:?}"
        );
        let served = report(
            &expected,
            &held(&[("labels", true, false), ("loans", true, false)]),
            now(),
        );
        assert_eq!(served.failed(), ["table"], "the served table is refuted");
    }
}
