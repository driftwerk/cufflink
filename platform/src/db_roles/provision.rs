use crate::db_roles::enqueue::{self, Enqueue, CUFFLINK_SCHEMA};
use crate::db_roles::names;
use crate::db_roles::turns::Turns;
use crate::db_roles::verify::{
    self, membership_grant, switch_privilege, Candidate, Verdict, VerifyReport, PG16,
};
use crate::errors::{PlatformError, PlatformResult};
use crate::policy::config::DbRolesConfig;
use crate::policy::{PolicyKey, PolicyMode};
use crate::repo::advisory_lock::AdvisoryLock;
use crate::repo::service_db_roles::{self, Binding, NewBinding, Verification};
use crate::repo::services::Service;
use crate::repo::tenants::Tenant;
use crate::secrets::{random_bytes, SecretString, DB_ROLE_PASSWORD};
use crate::tenant_catalog::TenantEntry;
use crate::tenant_db::TenantPoolManager;
use crate::AppState;
use cufflink_db::{quote_ident, quote_literal};
use cufflink_types::manifest::validate::{folded, folded_ident};
use deadpool_postgres::GenericClient;
use serde_json::json;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio_postgres::error::SqlState;
use tracing::{error, info, warn};
use uuid::Uuid;

pub const PLATFORM_ACTOR: &str = "platform";

const ATTEMPTS: u32 = 5;
const FIRST_BACKOFF: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owned {
    pub kind: String,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadOnly {
    pub member: bool,
    pub grantable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    pub server_version_num: i32,
    pub platform_role: String,
    pub database: String,
    pub role_exists: bool,
    pub role_marker: Option<String>,
    pub role_statement_timeout: Option<String>,
    pub schema_owner: Option<String>,
    pub owned_by_platform: Vec<Owned>,
    pub cufflink_schema: bool,
    pub enqueue: Enqueue,
    pub missing_roles: Vec<String>,
    pub public_open: bool,
    pub read_only: Option<ReadOnly>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec<'a> {
    pub role: &'a str,
    pub managed: bool,
    pub marker: &'a str,
    pub service: &'a str,
    pub schema: &'a str,
    pub own_database: bool,
    pub readers: &'a [String],
    pub read_only_role: Option<&'a str>,
    pub verifier: Option<&'a str>,
    pub statement_timeout: Option<Duration>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProvisionError {
    #[error(
        "role {0} exists and was not made by the platform for this service, which changes no role it did not make; drop or rename it, or bind the service to it with `cufflink services db-role set <tenant> <service> --role {0} --password-stdin`"
    )]
    ForeignRole(String),
    #[error(
        "role {0} does not exist; create it before naming it in db_roles.readers or read_only_role"
    )]
    MissingRole(String),
    #[error(
        "schema {schema} belongs to {owner}, not to the platform: bind the service to the role that owns it with `cufflink services db-role set <tenant> <service> --role {owner} --password-stdin`, or hand the schema to the platform with `REASSIGN OWNED BY {owner} TO {platform}`"
    )]
    ForeignSchema {
        schema: String,
        owner: String,
        platform: String,
    },
    #[error(
        "the platform may not grant {read_only}; as a role that may, run `GRANT {read_only} TO {platform} WITH ADMIN OPTION`"
    )]
    NotGrantable { read_only: String, platform: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub sql: String,
    shown_as: Option<&'static str>,
}

impl Step {
    fn secret(shown_as: &'static str, sql: String) -> Self {
        Self {
            sql,
            shown_as: Some(shown_as),
        }
    }

    pub fn shown(&self) -> &str {
        self.shown_as.unwrap_or(&self.sql)
    }
}

impl From<String> for Step {
    fn from(sql: String) -> Self {
        Self {
            sql,
            shown_as: None,
        }
    }
}

fn schema_ownership(spec: &Spec<'_>, catalog: &Catalog) -> Result<Option<String>, ProvisionError> {
    let (role, schema) = (quote_ident(spec.role), folded_ident(spec.schema));
    match catalog.schema_owner.as_deref() {
        None => Ok(Some(format!("CREATE SCHEMA {schema} AUTHORIZATION {role}"))),
        Some(owner) if owner == spec.role => Ok(None),
        Some(owner) if owner == catalog.platform_role => {
            Ok(Some(format!("ALTER SCHEMA {schema} OWNER TO {role}")))
        }
        Some(owner) => Err(ProvisionError::ForeignSchema {
            schema: spec.schema.to_string(),
            owner: owner.to_string(),
            platform: catalog.platform_role.clone(),
        }),
    }
}

fn read_only_grant(spec: &Spec<'_>, catalog: &Catalog) -> Result<Option<String>, ProvisionError> {
    match (spec.read_only_role, catalog.read_only) {
        (Some(_), Some(ReadOnly { member: true, .. })) | (None, _) | (Some(_), None) => Ok(None),
        (
            Some(read_only),
            Some(ReadOnly {
                grantable: true, ..
            }),
        ) => Ok(Some(membership_grant(
            catalog.server_version_num,
            read_only,
            spec.role,
            false,
        ))),
        (Some(read_only), Some(_)) => Err(ProvisionError::NotGrantable {
            read_only: read_only.to_string(),
            platform: catalog.platform_role.clone(),
        }),
    }
}

fn creation(spec: &Spec<'_>, catalog: &Catalog) -> Result<Option<[String; 2]>, ProvisionError> {
    let role = quote_ident(spec.role);
    match (catalog.role_exists, catalog.role_marker.as_deref()) {
        (false, _) => Ok(Some([
            format!(
                "CREATE ROLE {role} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
            ),
            format!("COMMENT ON ROLE {role} IS {}", quote_literal(spec.marker)),
        ])),
        (true, Some(marker)) if marker == spec.marker => Ok(None),
        (true, _) => Err(ProvisionError::ForeignRole(spec.role.to_string())),
    }
}

fn statement_timeout(spec: &Spec<'_>, catalog: &Catalog) -> Option<String> {
    let role = quote_ident(spec.role);
    let wanted = spec
        .statement_timeout
        .map(|timeout| format!("{}ms", timeout.as_millis()));
    match (wanted, catalog.role_statement_timeout.as_deref()) {
        (Some(wanted), Some(held)) if wanted == held => None,
        (Some(wanted), _) => Some(format!(
            "ALTER ROLE {role} SET statement_timeout = {}",
            quote_literal(&wanted)
        )),
        (None, Some(_)) => Some(format!("ALTER ROLE {role} RESET statement_timeout")),
        (None, None) => None,
    }
}

fn managed_statements(spec: &Spec<'_>, catalog: &Catalog) -> Result<Vec<Step>, ProvisionError> {
    let role = quote_ident(spec.role);
    let create = creation(spec, catalog)?
        .into_iter()
        .flatten()
        .map(Step::from);
    let password = spec.verifier.map(|verifier| {
        Step::secret(
            "ALTER ROLE … PASSWORD",
            format!(
                "ALTER ROLE {role} LOGIN PASSWORD {}",
                quote_literal(verifier)
            ),
        )
    });
    let timeout = statement_timeout(spec, catalog).map(Step::from);
    let reach = [
        membership_grant(
            catalog.server_version_num,
            spec.role,
            &catalog.platform_role,
            true,
        ),
        format!(
            "GRANT CONNECT ON DATABASE {} TO {role}",
            quote_ident(&catalog.database)
        ),
        format!("GRANT USAGE ON SCHEMA public TO {role}"),
    ];
    let closed = (spec.own_database && catalog.public_open)
        .then(|| "REVOKE CREATE ON SCHEMA public FROM PUBLIC".to_string());
    let owned = catalog
        .owned_by_platform
        .iter()
        .map(|owned| format!("ALTER {} {} OWNER TO {role}", owned.kind, owned.name));
    Ok(create
        .chain(password)
        .chain(timeout)
        .chain(reach.map(Step::from))
        .chain(closed.map(Step::from))
        .chain(schema_ownership(spec, catalog)?.map(Step::from))
        .chain(owned.map(Step::from))
        .collect())
}

fn reader_statements<'a>(spec: &'a Spec<'_>) -> impl Iterator<Item = String> + use<'a> {
    let role = quote_ident(spec.role);
    let schema = folded_ident(spec.schema);
    spec.readers.iter().flat_map(move |reader| {
        let reader = quote_ident(reader);
        [
            format!("GRANT USAGE ON SCHEMA {schema} TO {reader}"),
            format!("GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {reader}"),
            format!("GRANT SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO {reader}"),
            format!(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA {schema} GRANT SELECT ON TABLES TO {reader}"
            ),
            format!(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA {schema} GRANT SELECT ON SEQUENCES TO {reader}"
            ),
        ]
    })
}

fn cufflink_statements(spec: &Spec<'_>, catalog: &Catalog) -> Vec<String> {
    match catalog.cufflink_schema && (spec.managed || catalog.role_exists) {
        true => std::iter::once(format!(
            "GRANT USAGE ON SCHEMA {CUFFLINK_SCHEMA} TO {}",
            quote_ident(spec.role)
        ))
        .chain(enqueue::statements(
            spec.role,
            spec.service,
            catalog.enqueue,
        ))
        .collect(),
        false => vec![],
    }
}

pub fn statements(spec: &Spec<'_>, catalog: &Catalog) -> Result<Vec<Step>, ProvisionError> {
    let cufflink = cufflink_statements(spec, catalog)
        .into_iter()
        .map(Step::from);
    if !spec.managed {
        return Ok(cufflink.collect());
    }
    let managed = managed_statements(spec, catalog)?;
    if let Some(missing) = catalog.missing_roles.first() {
        return Err(ProvisionError::MissingRole(missing.clone()));
    }
    Ok(managed
        .into_iter()
        .chain(cufflink)
        .chain(reader_statements(spec).map(Step::from))
        .chain(read_only_grant(spec, catalog)?.map(Step::from))
        .collect())
}

fn generate_password() -> Result<SecretString, PlatformError> {
    random_bytes::<32>()
        .map(|bytes| SecretString::new(hex::encode(bytes)))
        .map_err(PlatformError::from)
}

pub fn scram_verifier(password: &SecretString) -> String {
    postgres_protocol::password::scram_sha_256(password.expose().as_bytes())
}

async fn read_only(
    client: &impl GenericClient,
    role: &str,
    read_only: &str,
    server_version_num: i32,
) -> Result<Option<ReadOnly>, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $2),
                    CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)
                          AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $2)
                         THEN pg_has_role($1, $2, $3) END,
                    CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $2)
                         THEN pg_has_role(current_user, $2, 'USAGE WITH ADMIN OPTION')
                              OR ($4 AND (SELECT rolcreaterole FROM pg_roles
                                          WHERE rolname = current_user)) END",
            &[
                &role,
                &read_only,
                &switch_privilege(server_version_num),
                &(server_version_num < PG16),
            ],
        )
        .await?;
    Ok(row.get::<_, bool>(0).then(|| ReadOnly {
        member: row.get::<_, Option<bool>>(1).unwrap_or(false),
        grantable: row.get::<_, Option<bool>>(2).unwrap_or(false),
    }))
}

pub async fn catalog(
    client: &impl GenericClient,
    spec: &Spec<'_>,
) -> Result<Catalog, tokio_postgres::Error> {
    let schema = folded(spec.schema);
    let named: Vec<&str> = spec
        .readers
        .iter()
        .map(String::as_str)
        .chain(spec.read_only_role)
        .collect();
    let head = client
        .query_one(
            "SELECT current_setting('server_version_num')::int, current_user::text, current_database()::text,
                    EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1),
                    (SELECT shobj_description(oid, 'pg_authid') FROM pg_roles WHERE rolname = $1),
                    (SELECT split_part(setting, '=', 2)
                     FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole,
                          unnest(s.setconfig) AS setting
                     WHERE r.rolname = $1 AND s.setdatabase = 0
                       AND starts_with(setting, 'statement_timeout=')),
                    (SELECT r.rolname::text FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner
                     WHERE n.nspname = $2),
                    EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $3),
                    ARRAY(SELECT name FROM unnest($4::text[]) AS name
                          WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = name)),
                    COALESCE((SELECT has_schema_privilege('public', n.oid, 'CREATE')
                                     AND pg_has_role(current_user, n.nspowner, 'USAGE')
                              FROM pg_namespace n WHERE n.nspname = 'public'), false)",
            &[&spec.role, &schema, &CUFFLINK_SCHEMA, &named],
        )
        .await?;
    let owned = client
        .query(cufflink_pgcodec::roles::OWNED_BY_THE_USER, &[&schema])
        .await?
        .iter()
        .map(|row| Owned {
            kind: row.get(0),
            name: row.get(1),
        })
        .collect();
    let server_version_num = head.get(0);
    let role_exists: bool = head.get(3);
    let enqueue = enqueue::facts(client, role_exists.then_some(spec.role), spec.service).await?;
    let read_only = match spec.read_only_role {
        Some(named) => read_only(client, spec.role, named, server_version_num).await?,
        None => None,
    };
    Ok(Catalog {
        server_version_num,
        platform_role: head.get(1),
        database: head.get(2),
        role_exists,
        role_marker: head.get(4),
        role_statement_timeout: head.get(5),
        schema_owner: head.get(6),
        owned_by_platform: owned,
        cufflink_schema: head.get(7),
        enqueue,
        missing_roles: head.get(8),
        public_open: head.get(9),
        read_only,
    })
}

#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error(transparent)]
    Refused(#[from] ProvisionError),
    #[error("provisioning the database role failed at `{at}`: {}", crate::db_error::from_pg(.source).message)]
    Statement {
        at: String,
        #[source]
        source: tokio_postgres::Error,
    },
    #[error(transparent)]
    Database(#[from] tokio_postgres::Error),
}

impl Failure {
    fn contended(&self) -> bool {
        match self {
            Failure::Statement { source, .. } => source.as_db_error().is_some_and(|e| {
                (e.code() == &SqlState::INTERNAL_ERROR
                    && e.message() == "tuple concurrently updated")
                    || e.code() == &SqlState::T_R_DEADLOCK_DETECTED
                    || e.code() == &SqlState::T_R_SERIALIZATION_FAILURE
            }),
            Failure::Refused(_) | Failure::Database(_) => false,
        }
    }
}

impl From<Failure> for PlatformError {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Refused(refusal) => PlatformError::Validation(refusal.to_string()),
            Failure::Database(e) => PlatformError::Database(e),
            statement => PlatformError::Internal(statement.to_string()),
        }
    }
}

async fn run_once(
    client: &mut deadpool_postgres::Object,
    spec: &Spec<'_>,
) -> Result<(Catalog, usize), Failure> {
    let tx = client.transaction().await?;
    let catalog = catalog(&tx, spec).await?;
    let steps = statements(spec, &catalog)?;
    for step in &steps {
        tx.batch_execute(&step.sql)
            .await
            .map_err(|source| Failure::Statement {
                at: step.shown().to_string(),
                source,
            })?;
    }
    tx.commit().await?;
    Ok((catalog, steps.len()))
}

fn backoff(attempt: u32) -> Duration {
    FIRST_BACKOFF.saturating_mul(1 << attempt.saturating_sub(1).min(8))
}

async fn run(
    client: &mut deadpool_postgres::Object,
    spec: &Spec<'_>,
) -> Result<(Catalog, usize), Failure> {
    let mut attempt = 1;
    loop {
        match run_once(client, spec).await {
            Err(failure) if failure.contended() && attempt < ATTEMPTS => {
                warn!(role = spec.role, attempt, error = %failure, "Provisioning met a concurrent change of the catalog; running it again");
                tokio::time::sleep(backoff(attempt)).await;
                attempt += 1;
            }
            outcome => return outcome,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Change<'a> {
    pub role: Option<&'a str>,
    pub password: Option<&'a str>,
    pub read_only_role: Option<&'a str>,
    pub rotate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Password {
    Minted {
        stored: Option<Vec<u8>>,
        again: bool,
    },
    Given(SecretString),
    Kept(Option<Vec<u8>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    role: String,
    managed: bool,
    password: Password,
    read_only_role: Option<String>,
}

fn target(
    existing: Option<&Binding>,
    change: &Change<'_>,
    policy: PolicyMode,
    managed_role: impl FnOnce() -> String,
) -> Result<Target, String> {
    let read_only_role = change
        .read_only_role
        .map(str::to_string)
        .or_else(|| existing.and_then(|b| b.read_only_role.clone()));
    let stored = |role: &str| {
        existing
            .filter(|b| b.role_name == role)
            .and_then(|b| b.password_enc.clone())
    };
    let adopted = |role: &str| {
        match change.rotate {
        true => Err(format!(
            "role '{role}' was bound by an admin, who changes its password in the database and gives it with `db-role set --password-stdin`"
        )),
        false => Ok(Target {
            role: role.to_string(),
            managed: false,
            password: change
                .password
                .map_or_else(|| Password::Kept(stored(role)), |given| {
                    Password::Given(SecretString::new(given))
                }),
            read_only_role: read_only_role.clone(),
        }),
    }
    };
    let managed = |role: String| {
        match change.password {
        Some(_) => Err(
            "a role the platform manages has a password of the platform's own; rotate it with `db-role rotate`".to_string(),
        ),
        None => Ok(Target {
            password: Password::Minted {
                stored: stored(&role),
                again: change.rotate,
            },
            role,
            managed: true,
            read_only_role: read_only_role.clone(),
        }),
    }
    };
    match (change.role, existing) {
        (Some(role), _) => adopted(role),
        (None, Some(binding)) if binding.managed => managed(binding.role_name.clone()),
        (None, Some(binding)) => adopted(&binding.role_name),
        (None, None) if policy != PolicyMode::Off => managed(managed_role()),
        (None, None) => Err(
            "role is required: the service has no database role yet, and its tenant's db_roles policy is off, so the platform makes none".to_string(),
        ),
    }
}

fn mints(stored: bool, again: bool, role_exists: bool) -> bool {
    again || !stored || !role_exists
}

fn verification(verdict: Verdict, previous: Option<&Binding>, target: &Target) -> Verification {
    let unchanged = |previous: &Binding| {
        previous.role_name == target.role
            && previous.managed == target.managed
            && (target.managed
                || (matches!(&target.password, Password::Kept(_))
                    && previous.read_only_role == target.read_only_role))
    };
    match verdict {
        Verdict::Verified => Verification::Verified,
        Verdict::Refuted => Verification::Unverified,
        Verdict::Undetermined if previous.is_some_and(unchanged) => Verification::AsBefore,
        Verdict::Undetermined => Verification::Unverified,
    }
}

static TURNS: LazyLock<Turns<String>> = LazyLock::new(Turns::default);

pub fn lock_key(database: &str) -> i64 {
    cufflink_migrate::platform_key("db_roles", database)
}

pub struct Provisioning<'a> {
    pub tenant: &'a Tenant,
    pub service: &'a Service,
    pub policy: PolicyMode,
    pub config: &'a DbRolesConfig,
    pub actor: &'a str,
}

pub async fn apply(
    state: &AppState,
    provisioning: &Provisioning<'_>,
    change: Change<'_>,
) -> PlatformResult<(Binding, VerifyReport)> {
    in_turn(state, provisioning.tenant, async |platform| {
        apply_locked(state, platform, provisioning, change).await
    })
    .await
}

async fn in_turn<T>(
    state: &AppState,
    tenant: &Tenant,
    work: impl AsyncFnOnce(&deadpool_postgres::Client) -> PlatformResult<T>,
) -> PlatformResult<T> {
    let database = TenantPoolManager::db_name(tenant);
    let _turn = TURNS.take(&database).await;
    let lock = AdvisoryLock::acquire(
        &state.platform_db,
        lock_key(&database),
        "cufflink_db_role_lock_dropped_total",
    )
    .await?;
    let done = match lock.client() {
        Ok(platform) => work(platform).await,
        Err(e) => Err(e),
    };
    lock.release().await;
    done
}

pub struct Retiring {
    tenant_id: Uuid,
    service_id: Uuid,
    service: String,
    role: String,
}

pub async fn retiring(state: &AppState, service: &Service) -> Option<Retiring> {
    let binding = async {
        service_db_roles::get(&state.platform_db.get().await?, service.id)
            .await
            .map_err(PlatformError::from)
    };
    match binding.await {
        Ok(binding) => binding
            .filter(|binding| binding.managed)
            .map(|binding| Retiring {
                tenant_id: service.tenant_id,
                service_id: service.id,
                service: service.name.clone(),
                role: binding.role_name,
            }),
        Err(e) => {
            retire_failed(&service.name, &e);
            None
        }
    }
}

fn retire_failed(service: &str, error: &PlatformError) {
    metrics::counter!("cufflink_db_role_retire_failures_total").increment(1);
    warn!(service = %service, error = %error, "The deleted service's database role could not be retired; `ALTER ROLE … NOLOGIN` it by hand");
}

impl Retiring {
    pub async fn retire(self, state: &AppState) {
        let retired = async {
            let Some(entry) = state.tenants.get_or_load(self.tenant_id).await? else {
                return Ok(false);
            };
            in_turn(state, &entry.tenant, async |_| {
                let tenant_db = state.tenant_sessions.platform(self.tenant_id, None).await?;
                let ours: bool = tenant_db
                    .query_one(
                        "SELECT COALESCE((SELECT shobj_description(oid, 'pg_authid') = $2
                                          FROM pg_roles WHERE rolname = $1), false)",
                        &[&self.role, &names::marker(self.tenant_id, self.service_id)],
                    )
                    .await?
                    .get(0);
                if ours {
                    tenant_db
                        .batch_execute(&format!("ALTER ROLE {} NOLOGIN", quote_ident(&self.role)))
                        .await?;
                    enqueue::unmap(&tenant_db, &self.role).await?;
                }
                Ok(ours)
            })
            .await
        };
        match retired.await {
            Ok(true) => {
                info!(service = %self.service, role = %self.role, "The deleted service's database role may log in no more")
            }
            Ok(false) => {}
            Err(e) => retire_failed(&self.service, &e),
        }
    }
}

async fn apply_locked(
    state: &AppState,
    platform: &deadpool_postgres::Client,
    provisioning: &Provisioning<'_>,
    change: Change<'_>,
) -> PlatformResult<(Binding, VerifyReport)> {
    let Provisioning {
        tenant,
        service,
        policy,
        config,
        actor,
    } = *provisioning;
    let existing = service_db_roles::get(platform, service.id).await?;
    let schema = service.schema();
    let target = target(existing.as_ref(), &change, policy, || {
        names::managed_role(&tenant.slug, &schema, tenant.id, service.id)
    })
    .map_err(PlatformError::Validation)?;
    if let Some((other_tenant, other_service)) =
        service_db_roles::bound_elsewhere(platform, &target.role, service.id).await?
    {
        return Err(PlatformError::Validation(format!(
            "role '{}' is bound to the service '{other_service}' of tenant '{other_tenant}'; a role carries one service",
            target.role
        )));
    }
    crate::outbox::bootstrap::ensure(&state.tenant_sessions, tenant.id).await?;
    let mut tenant_db = state.tenant_sessions.platform(tenant.id, None).await?;
    let role_facts = verify::role_facts(&tenant_db, &target.role).await?;
    if let (Some(_), Some(refusal)) = (
        change.role,
        role_facts
            .as_ref()
            .and_then(|facts| unbindable(&target.role, facts)),
    ) {
        return Err(PlatformError::Validation(refusal));
    }
    let role_exists = role_facts.is_some();
    let password = match &target.password {
        Password::Minted { stored, again } if mints(stored.is_some(), *again, role_exists) => {
            Some(generate_password()?)
        }
        Password::Given(given) => Some(given.clone()),
        Password::Minted { .. } | Password::Kept(_) => None,
    };
    let password_enc = match (&password, &target.password) {
        (Some(password), _) => Some(
            state
                .secrets
                .seal(
                    platform,
                    tenant.id,
                    &DB_ROLE_PASSWORD.aad([service.id.to_string()]),
                    password.expose().as_bytes(),
                )
                .await?,
        ),
        (None, Password::Minted { stored, .. } | Password::Kept(stored)) => stored.clone(),
        (None, Password::Given(_)) => None,
    };
    let verifier = password
        .as_ref()
        .filter(|_| target.managed)
        .map(scram_verifier);
    let marker = names::marker(tenant.id, service.id);
    let spec = Spec {
        role: &target.role,
        managed: target.managed,
        marker: &marker,
        service: &service.name,
        schema: &schema,
        own_database: tenant.database_name.is_none(),
        readers: &config.readers,
        read_only_role: target.read_only_role.as_deref(),
        verifier: verifier.as_deref(),
        statement_timeout: state.tenant_sessions.statement_timeout(),
    };
    let (_, steps) = run(&mut tenant_db, &spec).await?;
    drop(tenant_db);
    info!(
        tenant = %tenant.slug,
        service = %service.name,
        role = %target.role,
        managed = target.managed,
        statements = steps,
        "Database role provisioned"
    );
    let opened = match password {
        Some(password) => Some(password),
        None => match &password_enc {
            Some(sealed) => Some(
                verify::open_password(&state.secrets, platform, tenant.id, service.id, sealed)
                    .await?,
            ),
            None => None,
        },
    };
    let report = verify::inspect(
        state,
        platform,
        tenant,
        service,
        &Candidate {
            role: &target.role,
            managed: target.managed,
            read_only_role: target.read_only_role.as_deref(),
            password: opened.as_ref(),
        },
    )
    .await?;
    let verdict = report.verdict();
    if verdict == Verdict::Undetermined {
        metrics::counter!("cufflink_db_role_verify_unanswered_total", "tenant" => tenant.slug.clone(), "service" => service.name.clone()).increment(1);
        warn!(
            event = "db_role_verify_unanswered",
            tenant = %tenant.slug,
            service = %service.name,
            role = %target.role,
            unanswered = ?report.failed(),
            "The database role could not be verified; what was known of it stands"
        );
    }
    let binding = service_db_roles::write(
        platform,
        &NewBinding {
            service_id: service.id,
            role_name: &target.role,
            managed: target.managed,
            password_enc: password_enc.as_deref(),
            read_only_role: target.read_only_role.as_deref(),
            updated_by: actor,
            verification: verification(verdict, existing.as_ref(), &target),
            verify_report: &serde_json::to_value(&report)?,
        },
    )
    .await?;
    state.tenant_sessions.forget_role(service.id);
    crate::repo::audit::log_action(crate::repo::audit::AuditEntry {
        client: platform,
        tenant_id: Some(tenant.id),
        actor,
        action: "db_role.verify",
        resource_type: "service",
        resource_id: Some(&service.name),
        details: Some(json!({
            "role": report.role,
            "verified": binding.verified_at.is_some(),
            "failed": report.failed(),
        })),
        ip_address: None,
    })
    .await?;
    Ok((binding, report))
}

const ADMINISTRATOR: &str = "postgres";

fn unbindable(name: &str, role: &verify::RoleFacts) -> Option<String> {
    let why = match (
        role.is_platform,
        role.attributes.iter().any(|a| a == "SUPERUSER"),
        name == ADMINISTRATOR,
    ) {
        (true, _, _) => "the one the platform connects as",
        (false, true, _) => "a superuser, which no schema confines",
        (false, false, true) => "the server's administrator, which no schema confines",
        (false, false, false) => return None,
    };
    Some(format!(
        "the role is {why}; a service is bound to a role of its own"
    ))
}

async fn asking(state: &AppState, tenant_id: Uuid) -> Option<Arc<TenantEntry>> {
    state
        .tenants
        .get_or_load(tenant_id)
        .await
        .ok()
        .flatten()
        .filter(|entry| entry.mode(PolicyKey::DbRoles) != PolicyMode::Off)
}

async fn verified(state: &AppState, service_id: Uuid) -> PlatformResult<bool> {
    Ok(
        service_db_roles::get(&state.platform_db.get().await?, service_id)
            .await?
            .is_some_and(|binding| binding.verified_at.is_some()),
    )
}

async fn refresh(state: &AppState, entry: &TenantEntry, service_id: Uuid) {
    let service = match crate::repo::services::get_by_id(&state.platform_db, service_id).await {
        Ok(Some(service)) => service,
        _ => return,
    };
    let refreshed = apply(
        state,
        &Provisioning {
            tenant: &entry.tenant,
            service: &service,
            policy: entry.mode(PolicyKey::DbRoles),
            config: entry.config::<DbRolesConfig>(),
            actor: PLATFORM_ACTOR,
        },
        Change::default(),
    )
    .await;
    if let Err(e) = refreshed {
        metrics::counter!("cufflink_db_role_provision_failures_total", "tenant" => entry.tenant.slug.clone(), "service" => service.name.clone()).increment(1);
        error!(
            tenant = %entry.tenant.slug,
            service = %service.name,
            error = %e,
            "Database role could not be provisioned for the deploy; `cufflink services db-role verify` reports what is missing"
        );
    }
}

pub async fn before_deploy(state: &AppState, tenant_id: Uuid, service_id: Uuid) {
    let Some(entry) = asking(state, tenant_id).await else {
        return;
    };
    if !verified(state, service_id).await.unwrap_or(false) {
        refresh(state, &entry, service_id).await;
    }
}

pub async fn after_deploy(state: &AppState, tenant_id: Uuid, service_id: Uuid) {
    if let Some(entry) = asking(state, tenant_id).await {
        refresh(state, &entry, service_id).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn catalog() -> Catalog {
        Catalog {
            server_version_num: 160004,
            platform_role: "cufflink".into(),
            database: "cufflink_default".into(),
            role_exists: false,
            role_marker: None,
            role_statement_timeout: None,
            schema_owner: Some("cufflink".into()),
            owned_by_platform: vec![
                Owned {
                    kind: "TABLE".into(),
                    name: "\"stock_service\".\"items\"".into(),
                },
                Owned {
                    kind: "FUNCTION".into(),
                    name: "\"stock_service\".\"touch\"(uuid, integer)".into(),
                },
            ],
            cufflink_schema: true,
            enqueue: Enqueue::Shared { execute: false },
            missing_roles: vec![],
            public_open: false,
            read_only: Some(ReadOnly {
                member: false,
                grantable: true,
            }),
        }
    }

    const MARKER: &str = "cufflink:t:s";

    fn spec<'a>(readers: &'a [String]) -> Spec<'a> {
        Spec {
            role: "svc_default_stock_service",
            managed: true,
            marker: MARKER,
            service: "stock-service",
            schema: "stock_service",
            own_database: true,
            readers,
            read_only_role: Some("reports_ro"),
            verifier: Some("SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy"),
            statement_timeout: Some(Duration::from_secs(30)),
        }
    }

    fn sql(spec: &Spec<'_>, catalog: &Catalog) -> Vec<String> {
        statements(spec, catalog)
            .expect("statements")
            .into_iter()
            .map(|step| step.sql)
            .collect()
    }

    #[test]
    fn a_managed_role_is_made_owned_granted_and_read() {
        let readers = ["reporting_owner".to_string(), "dashboard_ro".to_string()];
        assert_eq!(
            sql(&spec(&readers), &catalog()),
            [
                "CREATE ROLE \"svc_default_stock_service\" LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS",
                "COMMENT ON ROLE \"svc_default_stock_service\" IS 'cufflink:t:s'",
                "ALTER ROLE \"svc_default_stock_service\" LOGIN PASSWORD 'SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy'",
                "ALTER ROLE \"svc_default_stock_service\" SET statement_timeout = '30000ms'",
                "GRANT \"svc_default_stock_service\" TO \"cufflink\" WITH INHERIT TRUE, SET TRUE",
                "GRANT CONNECT ON DATABASE \"cufflink_default\" TO \"svc_default_stock_service\"",
                "GRANT USAGE ON SCHEMA public TO \"svc_default_stock_service\"",
                "ALTER SCHEMA \"stock_service\" OWNER TO \"svc_default_stock_service\"",
                "ALTER TABLE \"stock_service\".\"items\" OWNER TO \"svc_default_stock_service\"",
                "ALTER FUNCTION \"stock_service\".\"touch\"(uuid, integer) OWNER TO \"svc_default_stock_service\"",
                "GRANT USAGE ON SCHEMA _cufflink TO \"svc_default_stock_service\"",
                "GRANT EXECUTE ON FUNCTION _cufflink.outbox_enqueue_many(TEXT[], TEXT[], TEXT[], BYTEA[], JSONB[], UUID[]) TO \"svc_default_stock_service\"",
                "GRANT USAGE ON SCHEMA \"stock_service\" TO \"reporting_owner\"",
                "GRANT SELECT ON ALL TABLES IN SCHEMA \"stock_service\" TO \"reporting_owner\"",
                "GRANT SELECT ON ALL SEQUENCES IN SCHEMA \"stock_service\" TO \"reporting_owner\"",
                "ALTER DEFAULT PRIVILEGES FOR ROLE \"svc_default_stock_service\" IN SCHEMA \"stock_service\" GRANT SELECT ON TABLES TO \"reporting_owner\"",
                "ALTER DEFAULT PRIVILEGES FOR ROLE \"svc_default_stock_service\" IN SCHEMA \"stock_service\" GRANT SELECT ON SEQUENCES TO \"reporting_owner\"",
                "GRANT USAGE ON SCHEMA \"stock_service\" TO \"dashboard_ro\"",
                "GRANT SELECT ON ALL TABLES IN SCHEMA \"stock_service\" TO \"dashboard_ro\"",
                "GRANT SELECT ON ALL SEQUENCES IN SCHEMA \"stock_service\" TO \"dashboard_ro\"",
                "ALTER DEFAULT PRIVILEGES FOR ROLE \"svc_default_stock_service\" IN SCHEMA \"stock_service\" GRANT SELECT ON TABLES TO \"dashboard_ro\"",
                "ALTER DEFAULT PRIVILEGES FOR ROLE \"svc_default_stock_service\" IN SCHEMA \"stock_service\" GRANT SELECT ON SEQUENCES TO \"dashboard_ro\"",
                "GRANT \"reports_ro\" TO \"svc_default_stock_service\" WITH INHERIT FALSE, SET TRUE",
            ]
        );
    }

    #[test]
    fn a_verifier_is_in_the_statement_and_in_nothing_that_is_shown() {
        let steps = statements(&spec(&[]), &catalog()).expect("statements");
        let shown: Vec<&str> = steps.iter().map(Step::shown).collect();
        assert_eq!(shown[2], "ALTER ROLE … PASSWORD");
        assert!(steps[2].sql.contains("SCRAM-SHA-256$"));
        assert!(
            !shown.iter().any(|shown| shown.contains("SCRAM-SHA-256")),
            "{shown:?}"
        );
        assert_eq!(
            shown[0], steps[0].sql,
            "every other statement is shown whole"
        );
    }

    #[test]
    fn a_service_without_a_schema_is_given_one_that_is_its_roles() {
        let readers = ["reporting_owner".to_string()];
        let statements = sql(
            &Spec {
                verifier: None,
                read_only_role: None,
                ..spec(&readers)
            },
            &Catalog {
                schema_owner: None,
                owned_by_platform: vec![],
                cufflink_schema: false,
                enqueue: Enqueue::Absent,
                read_only: None,
                ..catalog()
            },
        );
        assert_eq!(
            statements[3..8],
            [
                "GRANT \"svc_default_stock_service\" TO \"cufflink\" WITH INHERIT TRUE, SET TRUE",
                "GRANT CONNECT ON DATABASE \"cufflink_default\" TO \"svc_default_stock_service\"",
                "GRANT USAGE ON SCHEMA public TO \"svc_default_stock_service\"",
                "CREATE SCHEMA \"stock_service\" AUTHORIZATION \"svc_default_stock_service\"",
                "GRANT USAGE ON SCHEMA \"stock_service\" TO \"reporting_owner\"",
            ]
        );
        assert_eq!(
            statements.len(),
            12,
            "and its readers read it from the first"
        );
    }

    #[test]
    fn a_role_in_order_is_only_granted_again() {
        let settled = Catalog {
            role_exists: true,
            role_marker: Some(MARKER.into()),
            role_statement_timeout: Some("30000ms".into()),
            schema_owner: Some("svc_default_stock_service".into()),
            owned_by_platform: vec![],
            cufflink_schema: false,
            enqueue: Enqueue::Absent,
            read_only: Some(ReadOnly {
                member: true,
                grantable: false,
            }),
            ..catalog()
        };
        assert_eq!(
            sql(
                &Spec {
                    verifier: None,
                    ..spec(&[])
                },
                &settled,
            ),
            [
                "GRANT \"svc_default_stock_service\" TO \"cufflink\" WITH INHERIT TRUE, SET TRUE",
                "GRANT CONNECT ON DATABASE \"cufflink_default\" TO \"svc_default_stock_service\"",
                "GRANT USAGE ON SCHEMA public TO \"svc_default_stock_service\"",
            ],
            "a read-only role the service's role may switch to is not granted again, which the platform may be unable to"
        );
    }

    #[test]
    fn a_managed_role_is_given_the_platforms_statement_timeout_once() {
        let held = |timeout: Option<&str>| Catalog {
            role_exists: true,
            role_marker: Some(MARKER.into()),
            role_statement_timeout: timeout.map(str::to_string),
            ..catalog()
        };
        let wanted = |timeout: Option<Duration>| Spec {
            verifier: None,
            statement_timeout: timeout,
            ..spec(&[])
        };
        for (spec, catalog, expected) in [
            (
                wanted(Some(Duration::from_secs(30))),
                held(None),
                Some("ALTER ROLE \"svc_default_stock_service\" SET statement_timeout = '30000ms'"),
            ),
            (
                wanted(Some(Duration::from_millis(1500))),
                held(Some("30s")),
                Some("ALTER ROLE \"svc_default_stock_service\" SET statement_timeout = '1500ms'"),
            ),
            (
                wanted(Some(Duration::from_secs(30))),
                held(Some("30000ms")),
                None,
            ),
            (
                wanted(None),
                held(Some("30000ms")),
                Some("ALTER ROLE \"svc_default_stock_service\" RESET statement_timeout"),
            ),
            (wanted(None), held(None), None),
        ] {
            assert_eq!(
                statement_timeout(&spec, &catalog).as_deref(),
                expected,
                "{:?} over {:?}",
                spec.statement_timeout,
                catalog.role_statement_timeout
            );
        }
    }

    #[test]
    fn a_role_of_the_name_the_platform_did_not_make_for_the_service_is_never_changed() {
        for marker in [None, Some("cufflink:t:another"), Some("made by hand")] {
            let error = statements(
                &spec(&[]),
                &Catalog {
                    role_exists: true,
                    role_marker: marker.map(str::to_string),
                    ..catalog()
                },
            )
            .expect_err("a role of another's");
            assert_eq!(
                error,
                ProvisionError::ForeignRole("svc_default_stock_service".into()),
                "{marker:?}"
            );
            assert!(error
                .to_string()
                .contains("--role svc_default_stock_service"));
        }
    }

    #[test]
    fn a_schema_of_another_owner_is_refused_with_the_ways_out() {
        let error = statements(
            &spec(&[]),
            &Catalog {
                schema_owner: Some("asset_owner".into()),
                ..catalog()
            },
        )
        .expect_err("a schema the platform does not own");
        assert_eq!(
            error,
            ProvisionError::ForeignSchema {
                schema: "stock_service".into(),
                owner: "asset_owner".into(),
                platform: "cufflink".into(),
            }
        );
        let said = error.to_string();
        assert!(said.contains("--role asset_owner"), "{said}");
        assert!(
            said.contains("REASSIGN OWNED BY asset_owner TO cufflink"),
            "{said}"
        );
    }

    #[test]
    fn a_read_only_role_the_platform_may_not_grant_is_refused_with_the_grant_it_needs() {
        let error = statements(
            &spec(&[]),
            &Catalog {
                read_only: Some(ReadOnly {
                    member: false,
                    grantable: false,
                }),
                ..catalog()
            },
        )
        .expect_err("no ADMIN OPTION");
        assert_eq!(
            error.to_string(),
            "the platform may not grant reports_ro; as a role that may, run `GRANT reports_ro TO cufflink WITH ADMIN OPTION`"
        );
    }

    #[test]
    fn a_public_schema_every_role_may_create_in_is_closed_in_a_database_of_the_platforms() {
        const REVOKE: &str = "REVOKE CREATE ON SCHEMA public FROM PUBLIC";
        let open = Catalog {
            public_open: true,
            ..catalog()
        };
        assert!(sql(&spec(&[]), &open).contains(&REVOKE.to_string()));
        let adopted_database = Spec {
            own_database: false,
            ..spec(&[])
        };
        assert!(
            !sql(&adopted_database, &open).contains(&REVOKE.to_string()),
            "what others do in a database the tenant adopted is theirs; the verification reports it"
        );
        assert!(!sql(&spec(&[]), &catalog()).contains(&REVOKE.to_string()));
    }

    #[test]
    fn before_postgres_16_the_membership_grants_name_no_options() {
        let statements = sql(
            &Spec {
                verifier: None,
                ..spec(&[])
            },
            &Catalog {
                server_version_num: 150008,
                ..catalog()
            },
        );
        assert!(
            statements.contains(&"GRANT \"svc_default_stock_service\" TO \"cufflink\"".to_string())
        );
        assert!(statements
            .contains(&"GRANT \"reports_ro\" TO \"svc_default_stock_service\"".to_string()));
        assert!(!statements.iter().any(|s| s.contains("WITH INHERIT")));
    }

    #[test]
    fn an_adopted_role_is_granted_only_what_the_platform_owns() {
        let statements = sql(
            &Spec {
                role: "loan_service_user",
                managed: false,
                marker: MARKER,
                service: "loan-service",
                schema: "loan_service",
                own_database: true,
                readers: &["reporting_owner".to_string()],
                read_only_role: Some("kiosk_report_user"),
                verifier: None,
                statement_timeout: None,
            },
            &Catalog {
                role_exists: true,
                schema_owner: Some("db_migrator".into()),
                public_open: true,
                read_only: Some(ReadOnly {
                    member: false,
                    grantable: false,
                }),
                ..catalog()
            },
        );
        assert_eq!(
            statements,
            [
                "GRANT USAGE ON SCHEMA _cufflink TO \"loan_service_user\"",
                "GRANT EXECUTE ON FUNCTION _cufflink.outbox_enqueue_many(TEXT[], TEXT[], TEXT[], BYTEA[], JSONB[], UUID[]) TO \"loan_service_user\"",
            ]
        );
    }

    #[test]
    fn an_adopted_role_that_does_not_exist_is_granted_nothing() {
        let statements = sql(
            &Spec {
                role: "nobody",
                managed: false,
                verifier: None,
                ..spec(&[])
            },
            &Catalog {
                role_exists: false,
                ..catalog()
            },
        );
        assert_eq!(statements, Vec::<String>::new());
    }

    #[test]
    fn a_reader_that_does_not_exist_is_refused_by_name_for_a_managed_role_only() {
        let readers = ["reporting_owner".to_string()];
        let missing = Catalog {
            missing_roles: vec!["reporting_owner".into()],
            ..catalog()
        };
        let error = statements(&spec(&readers), &missing).expect_err("missing reader");
        assert_eq!(
            error.to_string(),
            "role reporting_owner does not exist; create it before naming it in db_roles.readers or read_only_role"
        );
        let adopted = Spec {
            managed: false,
            ..spec(&readers)
        };
        assert!(statements(&adopted, &missing).is_ok());
    }

    fn binding(managed: bool, verified: bool) -> Binding {
        Binding {
            service_id: Uuid::nil(),
            role_name: "loan_service_user".into(),
            managed,
            login: true,
            password_enc: Some(vec![1]),
            read_only_role: Some("reports_ro".into()),
            verified_at: verified.then(Utc::now),
            verify_report: None,
            updated_by: "admin".into(),
            updated_at: Utc::now(),
        }
    }

    fn managed_role() -> String {
        "svc_default_loan_service".to_string()
    }

    #[test]
    fn a_service_without_a_binding_gets_the_platforms_role_where_roles_are_asked_for() {
        let made = target(None, &Change::default(), PolicyMode::Warn, managed_role)
            .expect("a managed role");
        assert_eq!(
            made,
            Target {
                role: "svc_default_loan_service".into(),
                managed: true,
                password: Password::Minted {
                    stored: None,
                    again: false
                },
                read_only_role: None,
            }
        );
        let refused = target(None, &Change::default(), PolicyMode::Off, managed_role)
            .expect_err("no role and none asked for");
        assert!(refused.starts_with("role is required"), "{refused}");
    }

    #[test]
    fn a_role_an_admin_names_is_the_admins_and_keeps_the_password_of_the_same_role() {
        let existing = binding(false, true);
        let named = |role| Change {
            role: Some(role),
            ..Change::default()
        };
        let same = target(
            Some(&existing),
            &named("loan_service_user"),
            PolicyMode::Off,
            managed_role,
        )
        .expect("the same role");
        assert_eq!(same.password, Password::Kept(Some(vec![1])));
        assert_eq!(same.read_only_role.as_deref(), Some("reports_ro"));
        assert!(!same.managed);
        let other = target(
            Some(&existing),
            &named("other_user"),
            PolicyMode::Off,
            managed_role,
        )
        .expect("another role");
        assert_eq!(
            other.password,
            Password::Kept(None),
            "the password on file was another role's"
        );
        let given = target(
            Some(&existing),
            &Change {
                password: Some("s3cret"),
                read_only_role: Some("report_ro"),
                ..Change::default()
            },
            PolicyMode::Enforce,
            managed_role,
        )
        .expect("a password");
        assert_eq!(given.role, "loan_service_user");
        assert_eq!(given.password, Password::Given(SecretString::new("s3cret")));
        assert_eq!(given.read_only_role.as_deref(), Some("report_ro"));
    }

    #[test]
    fn the_password_of_a_managed_role_is_the_platforms_and_of_an_adopted_one_its_admins() {
        let managed = binding(true, true);
        let given = target(
            Some(&managed),
            &Change {
                password: Some("mine"),
                ..Change::default()
            },
            PolicyMode::Warn,
            managed_role,
        )
        .expect_err("a password for a managed role");
        assert!(given.contains("db-role rotate"), "{given}");
        let rotated = target(
            Some(&managed),
            &Change {
                rotate: true,
                ..Change::default()
            },
            PolicyMode::Warn,
            managed_role,
        )
        .expect("a rotation");
        assert_eq!(
            rotated.password,
            Password::Minted {
                stored: Some(vec![1]),
                again: true
            }
        );
        let adopted = target(
            Some(&binding(false, true)),
            &Change {
                rotate: true,
                ..Change::default()
            },
            PolicyMode::Warn,
            managed_role,
        )
        .expect_err("a rotation of an adopted role");
        assert!(adopted.contains("was bound by an admin"), "{adopted}");
    }

    #[test]
    fn a_managed_role_is_given_its_first_password_and_another_when_rotated_or_made_again() {
        assert!(mints(false, false, true));
        assert!(mints(true, true, true));
        assert!(
            mints(true, false, false),
            "a role that is made again has no password until it is given one"
        );
        assert!(!mints(true, false, true), "a refresh keeps the password");
    }

    #[test]
    fn a_verification_that_could_not_be_made_leaves_what_was_known() {
        let kept = |managed| Target {
            role: "loan_service_user".into(),
            managed,
            password: match managed {
                true => Password::Minted {
                    stored: Some(vec![1]),
                    again: true,
                },
                false => Password::Kept(Some(vec![1])),
            },
            read_only_role: Some("reports_ro".into()),
        };
        for managed in [true, false] {
            let previous = binding(managed, true);
            for (verdict, expected) in [
                (Verdict::Verified, Verification::Verified),
                (Verdict::Refuted, Verification::Unverified),
                (Verdict::Undetermined, Verification::AsBefore),
            ] {
                assert_eq!(
                    verification(verdict, Some(&previous), &kept(managed)),
                    expected,
                    "managed={managed} {verdict:?}"
                );
            }
        }
        assert_eq!(
            verification(Verdict::Undetermined, None, &kept(true)),
            Verification::Unverified,
            "a binding that is new was never verified"
        );
        let previous = binding(false, true);
        for changed in [
            Target {
                role: "other_user".into(),
                ..kept(false)
            },
            Target {
                password: Password::Given(SecretString::new("new")),
                ..kept(false)
            },
            Target {
                read_only_role: None,
                ..kept(false)
            },
        ] {
            assert_eq!(
                verification(Verdict::Undetermined, Some(&previous), &changed),
                Verification::Unverified,
                "what an admin changed was never verified: {changed:?}"
            );
        }
    }

    #[test]
    fn only_a_change_of_the_same_catalog_rows_is_run_again() {
        assert!(!Failure::Refused(ProvisionError::MissingRole("r".into())).contended());
        assert_eq!(backoff(1), Duration::from_millis(50));
        assert_eq!(backoff(3), Duration::from_millis(200));
    }

    #[test]
    fn each_tenant_database_has_a_lock_of_its_own() {
        assert_eq!(lock_key("cufflink_acme"), 7182794317239927779);
        assert_ne!(lock_key("cufflink_acme"), lock_key("cufflink_acme_2"));
    }

    #[test]
    fn a_role_that_confines_nothing_is_never_bound() {
        let facts = |is_platform, attributes: &[&str]| verify::RoleFacts {
            can_login: true,
            attributes: attributes.iter().map(|a| a.to_string()).collect(),
            is_platform,
            privileged: vec![],
        };
        for (name, role, why) in [
            (
                "cufflink",
                facts(true, &[]),
                "the one the platform connects as",
            ),
            ("boss", facts(false, &["SUPERUSER"]), "a superuser"),
            (
                "postgres",
                facts(false, &["CREATEROLE"]),
                "the server's administrator",
            ),
        ] {
            let refusal = unbindable(name, &role).expect(name);
            assert!(refusal.contains(why), "{refusal}");
        }
        assert_eq!(
            unbindable("loan_service_user", &facts(false, &["CREATEDB"])),
            None,
            "a role with less reach is bound, and its verification names what to take back"
        );
    }

    #[test]
    fn a_verifier_is_scram_and_never_the_password() {
        let password = SecretString::new("correct horse battery staple");
        let verifier = scram_verifier(&password);
        assert!(verifier.starts_with("SCRAM-SHA-256$4096:"), "{verifier}");
        assert!(!verifier.contains("horse"));
        assert_ne!(
            verifier,
            scram_verifier(&password),
            "a fresh salt each time"
        );
        let generated = generate_password().expect("password");
        assert_eq!(generated.expose().len(), 64);
        assert_ne!(generated, generate_password().expect("password"));
    }
}
