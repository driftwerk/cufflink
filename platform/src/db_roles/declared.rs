use crate::db_roles::names::{self, MANAGED_PREFIX};
use crate::db_roles::provision::{self, Change, Provisioned, Provisioning};
use crate::db_roles::verify::{self, Candidate, Probe, Reach, RoleFacts, VerifyReport, READINESS};
use crate::deployer::manifest::ServiceManifest;
use crate::errors::{PlatformError, PlatformResult};
use crate::policy::config::DbRolesConfig;
use crate::policy::{PolicyKey, PolicyMode};
use crate::repo::service_db_roles::{self, Binding, Origin};
use crate::repo::services::Service;
use crate::repo::tenants::Tenant;
use crate::secrets::SecretString;
use crate::tenant_catalog::TenantEntry;
use crate::AppState;
use cufflink_db::quote_ident;
use cufflink_types::manifest::validate::{folded, is_lowercase_identifier};
use deadpool_postgres::GenericClient;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tracing::warn;
use uuid::Uuid;

/// A service's own database role, carried by its deploy.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Declaration {
    pub role: String,
    pub password: SecretString,
}

impl Declaration {
    /// What a deploy is refused for before anything is written.
    pub fn admission(&self) -> Result<(), String> {
        names::validate_role_name("db_role.role", &self.role)?;
        match (
            names::is_managed_name(&self.role),
            self.password.expose().is_empty(),
            self.password.expose().contains('\0'),
        ) {
            (true, _, _) => Err(format!(
                "db_role.role '{}' is named as the platform names its own roles ('{MANAGED_PREFIX}…'); declare a role of the service's own",
                self.role
            )),
            (_, true, _) => Err("db_role.password is empty".into()),
            (_, _, true) => Err("db_role.password holds a NUL byte, which PostgreSQL cannot store".into()),
            (false, false, false) => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Taking {
    Unchanged,
    Bind { replaces: Option<Origin> },
    Refresh,
}

impl Taking {
    pub fn as_str(self) -> &'static str {
        match self {
            Taking::Unchanged => "unchanged",
            Taking::Bind { .. } => "bind",
            Taking::Refresh => "refresh",
        }
    }

    pub fn result(self) -> &'static str {
        match self {
            Taking::Unchanged => "unchanged",
            Taking::Bind { .. } => "bound",
            Taking::Refresh => "refreshed",
        }
    }
}

impl Serialize for Taking {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    Precedence,
    Allowlist,
    Existence,
    Login,
    Declaration,
    Readiness,
    Verification,
}

impl Gate {
    pub fn as_str(self) -> &'static str {
        match self {
            Gate::Precedence => "precedence",
            Gate::Allowlist => "allowlist",
            Gate::Existence => "existence",
            Gate::Login => "login",
            Gate::Declaration => "declaration",
            Gate::Readiness => "readiness",
            Gate::Verification => "verification",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    Refused,
    Conflict,
    Unavailable,
}

/// Why a declaration was not taken. `reasons` is what the tenant is told;
/// `detail` is the operator's, for the log and the audit row alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub gate: Gate,
    pub answer: Answer,
    pub reasons: Vec<String>,
    pub detail: Vec<String>,
}

impl Refusal {
    fn of(gate: Gate, answer: Answer, reason: String) -> Self {
        Refusal {
            gate,
            answer,
            reasons: vec![reason],
            detail: vec![],
        }
    }

    pub(crate) fn refused(gate: Gate, reason: String) -> Self {
        Self::of(gate, Answer::Refused, reason)
    }

    /// Gates 2 and 3 answer alike until the password is proven, so a
    /// refusal tells the tenant nothing about roles it cannot log in as.
    fn unproven(gate: Gate, role: &str, detail: Vec<String>) -> Self {
        Refusal {
            detail,
            ..Self::refused(
                gate,
                format!(
                    "role {role} does not log in to the tenant's database with the password given: \
                     check that the role exists, may log in and has CONNECT on the database, \
                     that its password matches the vault entry, and that pg_hba.conf admits it"
                ),
            )
        }
    }

    pub fn unsettled(report: &VerifyReport) -> Self {
        Self::of(
            Gate::Verification,
            Answer::Unavailable,
            format!(
                "the role changed while it was being bound ({}); deploy again",
                report.failed().join(", ")
            ),
        )
    }

    pub fn into_error(self, role: &str) -> PlatformError {
        let told = format!("db_role {role} was not taken: {}", self.reasons.join("; "));
        match self.answer {
            Answer::Refused => PlatformError::Validation(told),
            Answer::Conflict => PlatformError::Conflict(told),
            Answer::Unavailable => PlatformError::Unavailable(told),
        }
    }
}

/// The stored password opens and is the declared one, compared in constant
/// time; one that does not open counts as another.
pub fn same_password(stored: Option<&SecretString>, declared: &SecretString) -> bool {
    stored.is_some_and(|stored| {
        bool::from(
            stored
                .expose()
                .as_bytes()
                .ct_eq(declared.expose().as_bytes()),
        )
    })
}

pub fn taking(
    existing: Option<&Binding>,
    role: &str,
    same_password: bool,
) -> Result<Taking, Refusal> {
    let settled = |binding: &Binding| same_password && binding.verified_at.is_some();
    match existing {
        None => Ok(Taking::Bind { replaces: None }),
        Some(binding) => match (binding.origin, binding.role_name == role) {
            (Origin::Platform, _) => Ok(Taking::Bind {
                replaces: Some(Origin::Platform),
            }),
            (Origin::Admin, true) if settled(binding) => Ok(Taking::Unchanged),
            (Origin::Admin, true) => Ok(Taking::Refresh),
            (Origin::Admin, false) => Err(Refusal::of(
                Gate::Precedence,
                Answer::Conflict,
                format!(
                    "the service is bound by an administrator to role {}; an administrator changes it with cufflink services db-role set",
                    binding.role_name
                ),
            )),
            (Origin::Declared, true) if settled(binding) => Ok(Taking::Unchanged),
            (Origin::Declared, _) => Ok(Taking::Bind {
                replaces: Some(Origin::Declared),
            }),
        },
    }
}

/// What the catalog says of a declared role beyond [`verify::role_facts`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Facts {
    pub role: RoleFacts,
    pub marked: bool,
    pub schema_owner: Option<String>,
    pub reach: Reach,
    pub bound_elsewhere: Option<(String, String)>,
    pub tenant_roles: Vec<String>,
}

pub struct Expected<'a> {
    pub role: &'a str,
    pub schema: &'a str,
    pub taking: Taking,
}

fn told_membership(held: &str, tenant_roles: &[String]) -> bool {
    held.starts_with("pg_") || tenant_roles.iter().any(|role| role == held)
}

fn memberships_refusal(facts: &Facts, role: &str) -> Option<(String, String)> {
    let held = &facts.reach.memberships;
    (!held.is_empty()).then(|| {
        let named: Vec<&str> = held
            .iter()
            .map(String::as_str)
            .filter(|held| told_membership(held, &facts.tenant_roles))
            .collect();
        let unnamed = held.len() - named.len();
        let told = std::iter::once(format!("the role is a member of {} roles", held.len()))
            .chain((!named.is_empty()).then(|| format!("among them {}", named.join(", "))))
            .chain((unnamed > 0).then(|| format!("{unnamed} more that are not this tenant's")))
            .chain(
                (!named.is_empty())
                    .then(|| format!("run {}", verify::revoke_memberships(role, &named))),
            )
            .collect::<Vec<_>>()
            .join(", ");
        (told, format!("memberships: {}", held.join(", ")))
    })
}

fn confined_refusal(reach: &Reach) -> Option<(String, String)> {
    reach.described().map(|told| {
        let elsewhere = reach
            .objects
            .iter()
            .filter(|object| !object.here && object.held())
            .map(|object| object.place.as_str())
            .collect::<Vec<_>>();
        (
            format!("{told}; a declared role reaches its service's schema alone"),
            format!("reached outside this database: {}", elsewhere.join(", ")),
        )
    })
}

/// Gate 4: each finding of the declaration checks, judged from `facts`.
/// A refresh keeps a binding as it is, an administrator's included, so it is
/// judged only on what no binding may hold: the platform's marker, the
/// platform's or the administrator's role, and the attributes. What else the
/// role reaches is reported by verification.
pub fn refusals(facts: &Facts, expected: &Expected<'_>) -> Option<Refusal> {
    let role = quote_ident(expected.role);
    let binds = expected.taking != Taking::Refresh;
    let attributes = (!facts.role.attributes.is_empty()).then(|| {
        (
            format!(
                "the role is {}; run {}",
                facts.role.attributes.join(", "),
                verify::alter_to_none_of(expected.role, &facts.role.attributes)
            ),
            String::new(),
        )
    });
    let marked = facts.marked.then(|| {
        (
            "the role was made by the platform for a service; declare a role of the service's own"
                .to_string(),
            String::new(),
        )
    });
    let platform = verify::unbindable(expected.role, &facts.role).map(|why| {
        (
            format!("{why}; declare a role of the service's own"),
            String::new(),
        )
    });
    let elsewhere = facts.bound_elsewhere.as_ref().map(|(tenant, service)| {
        (
            format!("role {} is bound to another service", expected.role),
            format!("bound to service {service} of tenant {tenant}"),
        )
    });
    let schema = folded(expected.schema);
    let owner = match facts.schema_owner.as_deref() {
        Some(owner) if owner == expected.role => None,
        Some(owner) => Some((
            format!(
                "schema {schema} belongs to {owner}; run ALTER SCHEMA {} OWNER TO {role}",
                quote_ident(&schema)
            ),
            String::new(),
        )),
        None => Some((
            format!(
                "schema {schema} does not exist; the role that declares it creates it: CREATE SCHEMA {} AUTHORIZATION {role}",
                quote_ident(&schema)
            ),
            String::new(),
        )),
    };
    let binding = [
        elsewhere,
        owner,
        memberships_refusal(facts, expected.role),
        confined_refusal(&facts.reach),
    ]
    .into_iter()
    .filter(|_| binds);
    let findings: Vec<(String, String)> = [marked, platform, attributes]
        .into_iter()
        .chain(binding)
        .flatten()
        .collect();
    (!findings.is_empty()).then(|| Refusal {
        gate: Gate::Declaration,
        answer: Answer::Refused,
        detail: findings
            .iter()
            .map(|(_, detail)| detail.clone())
            .filter(|detail| !detail.is_empty())
            .collect(),
        reasons: findings.into_iter().map(|(told, _)| told).collect(),
    })
}

pub async fn facts(
    platform: &impl GenericClient,
    tenant_db: &impl GenericClient,
    tenant: &Tenant,
    service: &Service,
    role: RoleFacts,
    name: &str,
    read_only_role: Option<&str>,
) -> Result<Facts, PlatformError> {
    let schema = folded(&service.schema());
    let row = tenant_db
        .query_one(
            "SELECT COALESCE((SELECT shobj_description(oid, 'pg_authid') LIKE 'cufflink:%'
                              FROM pg_roles WHERE rolname = $1), false),
                    (SELECT pg_get_userbyid(nspowner)::text FROM pg_namespace WHERE nspname = $2)",
            &[&name, &schema],
        )
        .await?;
    let tenant_roles = platform
        .query(
            "SELECT r.role_name FROM service_db_roles r JOIN services s ON s.id = r.service_id
             WHERE s.tenant_id = $1 ORDER BY 1",
            &[&tenant.id],
        )
        .await?;
    Ok(Facts {
        role,
        marked: row.get(0),
        schema_owner: row.get(1),
        reach: verify::reach(tenant_db, name, &schema, read_only_role).await?,
        bound_elsewhere: service_db_roles::bound_elsewhere(platform, name, service.id).await?,
        tenant_roles: tenant_roles.iter().map(|row| row.get(0)).collect(),
    })
}
pub struct Subject<'a> {
    pub tenant: &'a Tenant,
    pub service: &'a Service,
    pub config: &'a DbRolesConfig,
    pub require_verified: bool,
    pub enqueues: bool,
    pub creates: bool,
}

pub struct Gated {
    pub taking: Taking,
    pub unready: Vec<String>,
}

impl Gated {
    pub fn change<'a>(&self, declaration: &'a Declaration) -> Change<'a> {
        Change {
            role: Some(&declaration.role),
            password: Some(declaration.password.expose()),
            read_only_role: None,
            rotate: false,
            origin: match self.taking {
                Taking::Refresh => None,
                Taking::Unchanged | Taking::Bind { .. } => Some(Origin::Declared),
            },
        }
    }
}

fn unready(report: &VerifyReport) -> Vec<&verify::Check> {
    report
        .checks
        .iter()
        .filter(|check| !check.ok && READINESS.contains(&check.check.as_str()))
        .collect()
}

fn told(check: &verify::Check) -> String {
    [check.detail.as_deref(), check.remedy.as_deref()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(": ")
}

/// Gates 1 to 5: what a declaration must pass before the platform grants
/// it anything or writes anything. It reads alone.
pub async fn gates(
    state: &AppState,
    platform: &impl GenericClient,
    subject: &Subject<'_>,
    declaration: &Declaration,
    existing: Option<&Binding>,
) -> PlatformResult<Result<Gated, Refusal>> {
    let Subject {
        tenant,
        service,
        config,
        ..
    } = *subject;
    let role = declaration.role.as_str();
    let stored = match existing.filter(|binding| binding.role_name == role) {
        Some(binding) => verify::password(&state.secrets, platform, tenant.id, binding)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    let taking = match taking(
        existing,
        role,
        same_password(stored.as_ref(), &declaration.password),
    ) {
        Ok(Taking::Unchanged) => {
            return Ok(Ok(Gated {
                taking: Taking::Unchanged,
                unready: vec![],
            }))
        }
        Ok(taking) => taking,
        Err(refusal) => return Ok(Err(refusal)),
    };
    if !admits(&config.declared_roles, role) {
        return Ok(Err(Refusal::refused(
            Gate::Allowlist,
            format!(
                "role {role} is not among the roles tenant {}'s db_roles policy lets a deploy declare (declared_roles); an administrator adds it with cufflink tenants policy set",
                tenant.slug
            ),
        )));
    }
    let read_only_role = existing.and_then(|binding| binding.read_only_role.as_deref());
    let facts = {
        let tenant_db = state.tenant_sessions.platform(tenant.id, None).await?;
        let role_facts = verify::role_facts(&tenant_db, role).await?;
        let connect = tenant_db
            .query_one(
                "SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)
                             THEN has_database_privilege($1, current_database(), 'CONNECT')
                             ELSE false END,
                        current_database()::text",
                &[&role],
            )
            .await?;
        let existing_role = match role_facts {
            None => Err(format!(
                "role {role} does not exist; whoever manages the service's schema creates it"
            )),
            Some(facts) if !facts.can_login => Err(format!(
                "role {role} may not log in; run ALTER ROLE {} LOGIN",
                quote_ident(role)
            )),
            Some(_) if !connect.get::<_, bool>(0) => Err(format!(
                "role {role} may not connect to the tenant's database; run GRANT CONNECT ON DATABASE {} TO {}",
                quote_ident(connect.get::<_, &str>(1)),
                quote_ident(role)
            )),
            Some(facts) => Ok(facts),
        };
        let probed = verify::login(
            &state.tenant_sessions,
            tenant.id,
            role,
            &declaration.password,
        )
        .await;
        let role_facts = match (probed, existing_role) {
            (Probe::Unanswered(error), _) => {
                return Ok(Err(Refusal {
                    detail: vec![error],
                    ..Refusal::of(
                        Gate::Login,
                        Answer::Unavailable,
                        format!("a login as role {role} could not be tried; deploy again"),
                    )
                }))
            }
            (_, Err(why)) => return Ok(Err(Refusal::unproven(Gate::Existence, role, vec![why]))),
            (Probe::Refused(error), Ok(_)) => {
                return Ok(Err(Refusal::unproven(Gate::Login, role, vec![error])))
            }
            (Probe::Accepted, Ok(facts)) => facts,
        };
        facts(
            platform,
            &tenant_db,
            tenant,
            service,
            role_facts,
            role,
            read_only_role,
        )
        .await?
    };
    if let Some(refusal) = refusals(
        &facts,
        &Expected {
            role,
            schema: &service.schema(),
            taking,
        },
    ) {
        return Ok(Err(refusal));
    }
    let report = verify::inspect(
        state,
        platform,
        tenant,
        service,
        &Candidate {
            role,
            origin: match taking {
                Taking::Refresh => Origin::Admin,
                Taking::Unchanged | Taking::Bind { .. } => Origin::Declared,
            },
            enqueues: subject.enqueues,
            creates: subject.creates,
            read_only_role,
            password: Some(&declaration.password),
            replacing: existing
                .map(|binding| binding.role_name.as_str())
                .filter(|previous| *previous != role),
        },
    )
    .await?;
    let unready = unready(&report);
    match (subject.require_verified, unready.is_empty()) {
        (true, false) => Ok(Err(Refusal {
            gate: Gate::Readiness,
            answer: Answer::Refused,
            reasons: unready.into_iter().map(told).collect(),
            detail: vec![],
        })),
        _ => Ok(Ok(Gated {
            taking,
            unready: unready
                .into_iter()
                .map(|check| check.check.clone())
                .collect(),
        })),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Previewed {
    pub taking: Taking,
    pub unready: Vec<String>,
}

async fn entry(state: &AppState, tenant_id: Uuid) -> PlatformResult<Arc<TenantEntry>> {
    state
        .tenants
        .get_or_load(tenant_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("Tenant not found".into()))
}

/// A row for `tenant` is the tenant's to read (`/api/audit`); one for no
/// tenant is the operator's alone, and may name what lies outside it.
async fn audit(
    state: &AppState,
    tenant: Option<&Tenant>,
    service: &str,
    actor: &str,
    action: &str,
    details: serde_json::Value,
) -> PlatformResult<()> {
    crate::repo::audit::log_action(crate::repo::audit::AuditEntry {
        client: &state.platform_db.get().await?,
        tenant_id: tenant.map(|tenant| tenant.id),
        actor,
        action,
        resource_type: "service",
        resource_id: Some(service),
        details: Some(details),
        ip_address: None,
    })
    .await?;
    Ok(())
}

fn counted(tenant: &Tenant, result: &'static str) {
    metrics::counter!("cufflink_db_role_declarations_total", "tenant" => tenant.slug.clone(), "result" => result).increment(1);
}

async fn refused(
    state: &AppState,
    tenant: &Tenant,
    service: &str,
    actor: &str,
    declaration: &Declaration,
    refusal: Refusal,
    plan: bool,
) -> PlatformError {
    counted(
        tenant,
        match (refusal.gate, refusal.answer) {
            (Gate::Login, Answer::Refused) => "refused_login",
            _ => "refused",
        },
    );
    warn!(
        event = "db_role_declare_refused",
        tenant = %tenant.slug,
        service = %service,
        role = %declaration.role,
        gate = refusal.gate.as_str(),
        plan,
        reasons = ?refusal.reasons,
        detail = ?refusal.detail,
        "A declared database role was not taken"
    );
    let audited = async {
        audit(
            state,
            Some(tenant),
            service,
            actor,
            "db_role.declare_refused",
            json!({
                "role": declaration.role,
                "plan": plan,
                "reasons": refusal.reasons,
            }),
        )
        .await?;
        audit(
            state,
            None,
            service,
            actor,
            "db_role.declare_refused",
            json!({
                "tenant": tenant.slug,
                "role": declaration.role,
                "gate": refusal.gate.as_str(),
                "plan": plan,
                "reasons": refusal.reasons,
                "detail": refusal.detail,
            }),
        )
        .await
    };
    match audited.await {
        Ok(()) => refusal.into_error(&declaration.role),
        Err(e) => e,
    }
}

/// Binds the service to the role its deploy declares, under the deploy lock
/// and the database's turn; [`Taking::Unchanged`] writes nothing.
pub async fn take(
    state: &AppState,
    service: &Service,
    actor: &str,
    deploying: &ServiceManifest,
    declaration: &Declaration,
) -> PlatformResult<Provisioned> {
    let entry = entry(state, service.tenant_id).await?;
    let tenant = &entry.tenant;
    let policy = entry.mode(PolicyKey::DbRoles);
    let outcome = provision::declare(
        state,
        &Provisioning {
            tenant,
            service,
            policy,
            config: entry.config::<DbRolesConfig>(),
            actor,
            deploying: Some(deploying),
            declaration: Some(declaration),
            require_verified: policy == PolicyMode::Enforce,
        },
        Change::default(),
    )
    .await?;
    let provisioned = match outcome {
        Ok(provisioned) => provisioned,
        Err(refusal) => {
            return Err(refused(
                state,
                tenant,
                &service.name,
                actor,
                declaration,
                refusal,
                false,
            )
            .await)
        }
    };
    let taking = provisioned.taking.unwrap_or(Taking::Unchanged);
    counted(tenant, taking.result());
    if taking != Taking::Unchanged {
        audit(
            state,
            Some(tenant),
            &service.name,
            actor,
            "db_role.declare",
            json!({
                "role": declaration.role,
                "previous_role": provisioned.previous.as_ref().map(|binding| &binding.role_name),
                "previous_origin": provisioned.previous.as_ref().map(|binding| binding.origin),
                "taking": taking,
                "verified": provisioned.binding.verified_at.is_some(),
                "failed": provisioned.report.failed(),
            }),
        )
        .await?;
    }
    Ok(provisioned)
}

/// What [`take`] would decide, by gates 1 to 5 alone: it seals, grants and
/// writes nothing. A service not yet recorded is given with a nil id.
pub async fn preview(
    state: &AppState,
    service: &Service,
    actor: &str,
    deploying: &ServiceManifest,
    declaration: &Declaration,
) -> PlatformResult<Previewed> {
    let entry = entry(state, service.tenant_id).await?;
    let tenant = &entry.tenant;
    let platform = state.platform_db.get().await?;
    let existing = service_db_roles::get(&platform, service.id).await?;
    let (enqueues, creates) = provision::needs(&platform, service.id, Some(deploying)).await?;
    let subject = Subject {
        tenant,
        service,
        config: entry.config::<DbRolesConfig>(),
        require_verified: entry.mode(PolicyKey::DbRoles) == PolicyMode::Enforce,
        enqueues,
        creates,
    };
    match gates(state, &platform, &subject, declaration, existing.as_ref()).await? {
        Ok(gated) => Ok(Previewed {
            taking: gated.taking,
            unready: gated.unready,
        }),
        Err(refusal) => Err(refused(
            state,
            tenant,
            &service.name,
            actor,
            declaration,
            refusal,
            true,
        )
        .await),
    }
}

const STEM: char = '*';

fn stem(entry: &str) -> Option<&str> {
    entry.strip_suffix(STEM)
}

/// Whether an entry of `declared_roles` admits `role`: an exact name, or a
/// stem followed by `*` that begins it.
pub fn admits(entries: &[String], role: &str) -> bool {
    entries.iter().any(|entry| match stem(entry) {
        Some(stem) => role.starts_with(stem),
        None => entry == role,
    })
}

/// The shortest stem a `declared_roles` entry may have.
pub const MIN_STEM: usize = 3;

/// Names no declared role takes: the platform's own roles, the server's
/// predefined roles and its administrator.
const RESERVED_ROLE_PREFIXES: [&str; 3] = [MANAGED_PREFIX, "pg_", verify::ADMINISTRATOR];

pub fn refusal_of_entry(entry: &str) -> Option<String> {
    let stemmed = stem(entry);
    let name = stemmed.unwrap_or(entry);
    let reserved = RESERVED_ROLE_PREFIXES
        .into_iter()
        .find(|prefix| name.starts_with(prefix) || (stemmed.is_some() && prefix.starts_with(name)));
    match (stemmed, is_lowercase_identifier(name), reserved) {
        (Some(""), _, _) => Some(format!(
            "declared_roles entry '{entry}' would admit every role; name the roles or a stem of them"
        )),
        (_, false, _) => Some(format!(
            "declared_roles entry '{entry}' must be a role name of lowercase letters, digits and '_', \
             not starting with a digit, or such a name followed by '*'"
        )),
        (_, true, Some(prefix)) => Some(format!(
            "declared_roles entry '{entry}' would admit roles whose names begin with '{prefix}', which are the platform's own or the server's"
        )),
        (Some(stem), true, None) if stem.len() < MIN_STEM => Some(format!(
            "declared_roles entry '{entry}' is too wide a stem; a stem has at least {MIN_STEM} characters"
        )),
        (_, true, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(entries: &[&str]) -> Vec<String> {
        entries.iter().map(|entry| entry.to_string()).collect()
    }

    #[test]
    fn an_entry_admits_its_name_or_what_its_stem_begins() {
        let allowed = entries(&["catalog_service_user", "loan_*"]);
        assert!(admits(&allowed, "catalog_service_user"));
        assert!(admits(&allowed, "loan_"));
        assert!(admits(&allowed, "loan_service_user"));
        assert!(!admits(&allowed, "catalog_service_user2"));
        assert!(!admits(&allowed, "catalog_service"));
        assert!(!admits(&allowed, "loans"));
        assert!(!admits(&[], "catalog_service_user"));
    }

    #[test]
    fn an_entry_is_a_role_name_or_a_stem_that_spares_the_platforms_roles() {
        for good in [
            "catalog_service_user",
            "loan_*",
            "loa*",
            "_xy*",
            "s",
            "svc",
            "sales_*",
            "pgx_*",
        ] {
            assert_eq!(refusal_of_entry(good), None, "{good}");
        }
        for (bad, why) in [
            ("*", "every role"),
            ("s*", "platform's own"),
            ("svc*", "platform's own"),
            ("svc_*", "platform's own"),
            ("svc_loans*", "platform's own"),
            ("svc_loans", "platform's own"),
            ("pg_*", "'pg_'"),
            ("pg_read_all_data", "'pg_'"),
            ("p*", "'pg_'"),
            ("pos*", "'postgres'"),
            ("postgres*", "'postgres'"),
            ("postgres", "'postgres'"),
            ("postgres_app", "'postgres'"),
            ("l*", "at least 3"),
            ("lo*", "at least 3"),
            ("Loan_*", "lowercase"),
            ("loan-*", "lowercase"),
            ("loan_**", "lowercase"),
            ("*loan", "lowercase"),
            ("", "lowercase"),
            ("9loan", "lowercase"),
        ] {
            let refusal = refusal_of_entry(bad).unwrap_or_default();
            assert!(refusal.contains(why), "{bad}: {refusal}");
        }
    }

    fn binding(origin: Origin, role: &str, verified: bool) -> Binding {
        Binding {
            service_id: Uuid::nil(),
            role_name: role.into(),
            origin,
            login: true,
            password_enc: Some(vec![1]),
            read_only_role: None,
            verified_at: verified.then(chrono::Utc::now),
            verify_report: None,
            updated_by: "admin".into(),
            updated_at: chrono::Utc::now(),
        }
    }

    const ROLE: &str = "catalog_service_user";

    #[test]
    fn what_is_taken_follows_the_binding_in_place() {
        let bind = |replaces| Ok(Taking::Bind { replaces });
        let admin = Origin::Admin;
        let declared = Origin::Declared;
        for (existing, same, expected) in [
            (None, true, bind(None)),
            (
                Some(binding(Origin::Platform, "svc_x", true)),
                true,
                bind(Some(Origin::Platform)),
            ),
            (
                Some(binding(admin, ROLE, true)),
                true,
                Ok(Taking::Unchanged),
            ),
            (Some(binding(admin, ROLE, true)), false, Ok(Taking::Refresh)),
            (Some(binding(admin, ROLE, false)), true, Ok(Taking::Refresh)),
            (
                Some(binding(declared, ROLE, true)),
                true,
                Ok(Taking::Unchanged),
            ),
            (
                Some(binding(declared, ROLE, true)),
                false,
                bind(Some(declared)),
            ),
            (
                Some(binding(declared, ROLE, false)),
                true,
                bind(Some(declared)),
            ),
            (
                Some(binding(declared, "loan_user", true)),
                true,
                bind(Some(declared)),
            ),
        ] {
            assert_eq!(
                taking(existing.as_ref(), ROLE, same),
                expected,
                "{existing:?} {same}"
            );
        }
        let refused = taking(Some(&binding(admin, "loan_user", true)), ROLE, true)
            .expect_err("an administrator's other role");
        assert_eq!(
            (refused.gate, refused.answer),
            (Gate::Precedence, Answer::Conflict)
        );
        assert!(refused.reasons[0].contains("bound by an administrator to role loan_user"));
    }

    #[test]
    fn a_password_is_the_same_only_when_the_stored_one_opens_and_matches() {
        let declared = SecretString::new("s3cret");
        assert!(same_password(Some(&SecretString::new("s3cret")), &declared));
        assert!(!same_password(
            Some(&SecretString::new("s3cret!")),
            &declared
        ));
        assert!(!same_password(None, &declared));
    }

    fn declaration(role: &str, password: &str) -> Declaration {
        Declaration {
            role: role.into(),
            password: SecretString::new(password),
        }
    }

    #[test]
    fn admission_refuses_a_platform_name_and_a_password_postgres_cannot_hold() {
        assert_eq!(declaration(ROLE, "s3cret").admission(), Ok(()));
        for (bad, why) in [
            (
                declaration("svc_catalog", "s3cret"),
                "platform names its own",
            ),
            (declaration("Catalog", "s3cret"), "must be a role name"),
            (declaration(ROLE, ""), "empty"),
            (declaration(ROLE, "a\0b"), "NUL"),
        ] {
            let refused = bad.admission().expect_err("refused");
            assert!(refused.contains(why), "{refused}");
        }
    }

    #[test]
    fn a_declaration_never_shows_its_password() {
        let read: Declaration =
            serde_json::from_str(r#"{"role": "catalog_service_user", "password": "hunter2"}"#)
                .expect("a declaration");
        assert_eq!(read.password.expose(), "hunter2");
        assert!(!format!("{read:?}").contains("hunter2"));
        assert!(serde_json::from_str::<Declaration>(
            r#"{"role": "x", "password": "p", "extra": 1}"#
        )
        .is_err());
    }

    fn sound() -> Facts {
        Facts {
            role: RoleFacts {
                can_login: true,
                attributes: vec![],
                is_platform: false,
                privileged: vec![],
            },
            marked: false,
            schema_owner: Some(ROLE.into()),
            reach: Reach::default(),
            bound_elsewhere: None,
            tenant_roles: vec!["loan_service_user".into()],
        }
    }

    fn judged(facts: &Facts, taking: Taking) -> Option<Refusal> {
        refusals(
            facts,
            &Expected {
                role: ROLE,
                schema: "catalog_service",
                taking,
            },
        )
    }

    const BIND: Taking = Taking::Bind { replaces: None };

    #[test]
    fn a_role_that_owns_its_schema_and_reaches_nothing_else_passes() {
        assert_eq!(judged(&sound(), BIND), None);
    }

    #[test]
    fn each_declaration_check_refuses_with_its_reason() {
        let reached = |here: bool, place: &str| verify::Reached {
            here,
            place: place.into(),
            kind: here.then(|| "table".to_string()),
            identity: here.then(|| "public.ledger".to_string()),
            received: false,
        };
        for (facts, told) in [
            (
                Facts {
                    marked: true,
                    ..sound()
                },
                "made by the platform",
            ),
            (
                Facts {
                    role: RoleFacts {
                        is_platform: true,
                        ..sound().role
                    },
                    ..sound()
                },
                "the one the platform connects as",
            ),
            (
                Facts {
                    role: RoleFacts {
                        attributes: vec!["SUPERUSER".into(), "CREATEROLE".into()],
                        ..sound().role
                    },
                    ..sound()
                },
                "NOSUPERUSER NOCREATEROLE",
            ),
            (
                Facts {
                    bound_elsewhere: Some(("orchard".into(), "loan-service".into())),
                    ..sound()
                },
                "is bound to another service",
            ),
            (
                Facts {
                    schema_owner: Some("migrator".into()),
                    ..sound()
                },
                "belongs to migrator",
            ),
            (
                Facts {
                    schema_owner: None,
                    ..sound()
                },
                "does not exist",
            ),
            (
                Facts {
                    reach: Reach {
                        memberships: vec!["pg_read_all_data".into()],
                        objects: vec![],
                    },
                    ..sound()
                },
                "member of 1 roles, among them pg_read_all_data",
            ),
            (
                Facts {
                    reach: Reach {
                        memberships: vec![],
                        objects: vec![reached(true, "tenant_harbor")],
                    },
                    ..sound()
                },
                "reaches table public.ledger",
            ),
        ] {
            let refusal = judged(&facts, BIND).expect("refused");
            assert_eq!(
                (refusal.gate, refusal.answer),
                (Gate::Declaration, Answer::Refused)
            );
            assert!(
                refusal.reasons.iter().any(|reason| reason.contains(told)),
                "{told}: {refusal:?}"
            );
        }
    }

    #[test]
    fn a_refusal_names_nothing_outside_the_tenants_database() {
        let facts = Facts {
            bound_elsewhere: Some(("orchard".into(), "ledger-service".into())),
            reach: Reach {
                memberships: vec![
                    "loan_service_user".into(),
                    "orchard_owner".into(),
                    "pg_write_all_data".into(),
                ],
                objects: vec![verify::Reached {
                    here: false,
                    place: "tenant_orchard".into(),
                    kind: None,
                    identity: None,
                    received: false,
                }],
            },
            ..sound()
        };
        let refusal = judged(&facts, BIND).expect("refused");
        let told = refusal.reasons.join(" ");
        for hidden in ["orchard", "ledger-service", "tenant_orchard"] {
            assert!(!told.contains(hidden), "{hidden} in {told}");
        }
        assert!(
            told.contains("among them loan_service_user, pg_write_all_data"),
            "{told}"
        );
        assert!(told.contains("1 more that are not this tenant's"), "{told}");
        assert!(told.contains("1 objects outside this database"), "{told}");
        let detail = refusal.detail.join(" ");
        for shown in ["orchard_owner", "tenant_orchard", "ledger-service"] {
            assert!(detail.contains(shown), "{shown} in {detail}");
        }
    }

    #[test]
    fn an_administrators_role_kept_by_a_refresh_is_vouched_for_beyond_its_schema() {
        let reaching = Facts {
            reach: Reach {
                memberships: vec!["reporting".into()],
                objects: vec![],
            },
            ..sound()
        };
        assert_eq!(judged(&reaching, Taking::Refresh), None);
        assert!(judged(&reaching, BIND).is_some());

        let administered = Facts {
            schema_owner: Some("harbor_owner".into()),
            bound_elsewhere: Some(("orchard".into(), "ledger-service".into())),
            ..sound()
        };
        assert_eq!(
            judged(&administered, Taking::Refresh),
            None,
            "an administrator's binding keeps its schema's owner, as it was set"
        );
        assert!(judged(&administered, BIND).is_some());

        for barred in [
            Facts {
                marked: true,
                ..sound()
            },
            Facts {
                role: RoleFacts {
                    attributes: vec!["CREATEROLE".into()],
                    ..sound().role
                },
                ..sound()
            },
            Facts {
                role: RoleFacts {
                    is_platform: true,
                    ..sound().role
                },
                ..sound()
            },
        ] {
            assert!(
                judged(&barred, Taking::Refresh).is_some(),
                "no refresh binds what no binding may hold: {barred:?}"
            );
        }
    }

    #[test]
    fn a_refusal_answers_as_its_gate_decided() {
        let refusal = |answer| Refusal::of(Gate::Login, answer, "why".into()).into_error(ROLE);
        assert!(
            matches!(refusal(Answer::Refused), PlatformError::Validation(m) if m == "db_role catalog_service_user was not taken: why")
        );
        assert!(matches!(
            refusal(Answer::Conflict),
            PlatformError::Conflict(_)
        ));
        assert!(matches!(
            refusal(Answer::Unavailable),
            PlatformError::Unavailable(_)
        ));
    }
}
