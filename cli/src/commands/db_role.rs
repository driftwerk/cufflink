//! `cufflink services db-role` — the database role a service runs as
//! (platform admin only).

use crate::commands::secret_from;
use crate::commands::tenants::admin_request;
use clap::Subcommand;
use serde_json::{json, Value};

#[derive(Subcommand)]
pub enum DbRoleAction {
    /// Bind a service to a database role, or change what its binding holds
    Set {
        /// Tenant slug
        tenant: String,
        /// Service name
        service: String,
        /// The role the service runs as; kept when omitted
        #[arg(long)]
        role: Option<String>,
        /// Read the role's password from stdin
        #[arg(long)]
        password_stdin: bool,
        /// A role the service switches to for read-only queries
        #[arg(long)]
        read_only_role: Option<String>,
        /// Record the service first when it has not been deployed, so its
        /// first deploy runs as the role
        #[arg(long, requires = "role")]
        register: bool,
        /// The schema the service's manifest declares (`schema_name`), when
        /// registering it; by default its name with `-` as `_`
        #[arg(long, requires = "register")]
        schema: Option<String>,
    },
    /// Check the binding against the tenant database and record the report
    Verify {
        /// Tenant slug
        tenant: String,
        /// Service name
        service: String,
    },
    /// Give the role the platform made for a service a new password
    Rotate {
        /// Tenant slug
        tenant: String,
        /// Service name
        service: String,
    },
    /// Show the binding and its last verification
    Show {
        /// Tenant slug
        tenant: String,
        /// Service name
        service: String,
    },
}

fn path(tenant: &str, service: &str) -> String {
    format!("/api/tenants/{tenant}/services/{service}/db-role")
}

struct SetRequest<'a> {
    role: Option<&'a str>,
    password: Option<&'a str>,
    read_only_role: Option<&'a str>,
    register: bool,
    schema: Option<&'a str>,
}

/// The body of a set. `register` and `schema` are sent only when asked
/// for, so a set without them is the one a platform before them reads.
fn set_body(request: &SetRequest<'_>) -> Value {
    let mut body = json!({
        "role": request.role,
        "password": request.password,
        "read_only_role": request.read_only_role,
    });
    if request.register {
        body["register"] = json!(true);
    }
    if let Some(schema) = request.schema {
        body["schema"] = json!(schema);
    }
    body
}

/// A failure while the report says the binding is not verified, so the
/// command exits non-zero.
fn verified(report: &Value) -> eyre::Result<()> {
    match report["verified"].as_bool() {
        Some(true) => Ok(()),
        _ => eyre::bail!("the binding is not verified"),
    }
}

fn print_binding(body: &Value) {
    let binding = &body["binding"];
    if binding.is_null() {
        println!(
            "{}/{}: no database role bound",
            body["tenant"].as_str().unwrap_or_default(),
            body["service"].as_str().unwrap_or_default()
        );
        return;
    }
    println!(
        "{}/{}: role {} ({}, {})",
        body["tenant"].as_str().unwrap_or_default(),
        body["service"].as_str().unwrap_or_default(),
        binding["role"].as_str().unwrap_or_default(),
        if binding["managed"].as_bool().unwrap_or(false) {
            "managed by the platform"
        } else {
            "bound by an admin"
        },
        if binding["login"].as_bool().unwrap_or(false) {
            "password stored"
        } else {
            "no password stored"
        },
    );
    if let Some(read_only) = binding["read_only_role"].as_str() {
        println!("  read-only role: {read_only}");
    }
    match binding["verified_at"].as_str() {
        Some(at) => println!("  verified at {at}"),
        None => println!("  not verified"),
    }
}

fn print_report(report: &Value) {
    let checks: Vec<&Value> = report["checks"].as_array().into_iter().flatten().collect();
    let detail = |check: &Value| check["detail"].as_str().unwrap_or_default().to_string();
    for check in &checks {
        let name = check["check"].as_str().unwrap_or_default();
        match (
            check["ok"].as_bool().unwrap_or(false),
            check["excess"].as_bool().unwrap_or(false),
        ) {
            (true, false) => println!("  ok      {name}"),
            (true, true) => println!("  EXCESS  {name}: {}", detail(check)),
            (false, _) => println!("  FAILED  {name}: {}", detail(check)),
        }
    }
    print_statements(
        "To mend, run on the tenant database:",
        report["remediation"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str),
    );
    print_statements(
        "To take back what the role holds beyond its need, run on the tenant database:",
        checks
            .iter()
            .filter(|check| check["excess"].as_bool().unwrap_or(false))
            .filter_map(|check| check["remedy"].as_str()),
    );
}

fn print_statements<'a>(heading: &str, statements: impl Iterator<Item = &'a str>) {
    let statements: Vec<&str> = statements.collect();
    if !statements.is_empty() {
        println!("{heading}");
        statements
            .iter()
            .for_each(|statement| println!("  {statement}"));
    }
}

/// Ask the platform for `step` of the binding and print the verification
/// it answers with, which fails the command while the binding is not
/// verified.
async fn check(
    what: &str,
    step: &str,
    tenant: &str,
    service: &str,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let body = admin_request(
        what,
        reqwest::Method::POST,
        &format!("{}/{step}", path(tenant, service)),
        None,
        api_url,
        env,
    )
    .await?;
    print_binding(&body);
    print_report(&body["report"]);
    verified(&body["report"])
}

pub async fn run(
    action: DbRoleAction,
    api_url: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    match action {
        DbRoleAction::Set {
            tenant,
            service,
            role,
            password_stdin,
            read_only_role,
            register,
            schema,
        } => {
            let password = password_stdin
                .then(|| secret_from(std::io::stdin(), "--password-stdin"))
                .transpose()?;
            let body = admin_request(
                "Database role",
                reqwest::Method::PUT,
                &path(&tenant, &service),
                Some(set_body(&SetRequest {
                    role: role.as_deref(),
                    password: password.as_deref(),
                    read_only_role: read_only_role.as_deref(),
                    register,
                    schema: schema.as_deref(),
                })),
                api_url,
                env,
            )
            .await?;
            if body["registered"].as_bool().unwrap_or(false) {
                println!(
                    "Registered {service} with schema {}; its first deploy runs as the role.",
                    body["schema"].as_str().unwrap_or_default()
                );
            }
            print_binding(&body);
            println!("Run `cufflink services db-role verify {tenant} {service}` to check it.");
        }
        DbRoleAction::Verify { tenant, service } => {
            let checked = check(
                "Database role verification",
                "verify",
                &tenant,
                &service,
                api_url,
                env,
            );
            checked.await?;
        }
        DbRoleAction::Rotate { tenant, service } => {
            let checked = check(
                "Database role rotation",
                "rotate",
                &tenant,
                &service,
                api_url,
                env,
            );
            checked.await?;
            println!("The role has a new password; sessions open before it keep theirs.");
        }
        DbRoleAction::Show { tenant, service } => {
            let body = admin_request(
                "Database role",
                reqwest::Method::GET,
                &path(&tenant, &service),
                None,
                api_url,
                env,
            )
            .await?;
            print_binding(&body);
            if !body["binding"]["verify_report"].is_null() {
                print_report(&body["binding"]["verify_report"]);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_password_on_stdin_loses_its_line_ending_and_nothing_else() {
        for (input, password) in [
            ("s3cret\n", "s3cret"),
            ("s3cret\r\n", "s3cret"),
            ("s3cret", "s3cret"),
            (" spaced out \n", " spaced out "),
            ("two\nlines\n", "two\nlines"),
        ] {
            assert_eq!(
                secret_from(input.as_bytes(), "--password-stdin").expect(input),
                password
            );
        }
    }

    #[test]
    fn an_empty_password_is_refused() {
        for input in ["", "\n", "\r\n\n"] {
            let refused = secret_from(input.as_bytes(), "--password-stdin").expect_err(input);
            assert!(
                refused.to_string().contains("nothing was read"),
                "{refused}"
            );
        }
    }

    fn request<'a>(
        role: Option<&'a str>,
        password: Option<&'a str>,
        read_only_role: Option<&'a str>,
    ) -> SetRequest<'a> {
        SetRequest {
            role,
            password,
            read_only_role,
            register: false,
            schema: None,
        }
    }

    #[test]
    fn what_is_not_given_is_sent_as_null_and_kept() {
        assert_eq!(
            set_body(&request(None, Some("pw"), None)),
            json!({"role": null, "password": "pw", "read_only_role": null})
        );
        assert_eq!(
            set_body(&request(
                Some("loan_service_user"),
                None,
                Some("reports_ro")
            )),
            json!({"role": "loan_service_user", "password": null, "read_only_role": "reports_ro"})
        );
    }

    #[test]
    fn a_registration_is_sent_only_when_asked_for() {
        assert_eq!(
            set_body(&SetRequest {
                register: true,
                ..request(Some("catalog_service_user"), Some("pw"), None)
            }),
            json!({"role": "catalog_service_user", "password": "pw", "read_only_role": null,
                   "register": true})
        );
        assert_eq!(
            set_body(&SetRequest {
                register: true,
                schema: Some("catalog"),
                ..request(Some("catalog_service_user"), None, None)
            }),
            json!({"role": "catalog_service_user", "password": null, "read_only_role": null,
                   "register": true, "schema": "catalog"})
        );
    }

    #[test]
    fn a_registration_names_its_role_and_a_schema_is_given_only_with_one() {
        use clap::Parser;
        #[derive(Parser)]
        struct Command {
            #[command(subcommand)]
            action: DbRoleAction,
        }
        let parse = |args: &[&str]| {
            Command::try_parse_from(
                ["db-role", "set", "acme", "catalog-service"]
                    .iter()
                    .chain(args),
            )
        };
        assert!(parse(&["--register", "--role", "catalog_service_user"]).is_ok());
        assert!(parse(&["--register", "--schema", "catalog", "--role", "r"]).is_ok());
        assert!(
            parse(&["--register"]).is_err(),
            "a registration names its role"
        );
        assert!(
            parse(&["--schema", "catalog", "--role", "r"]).is_err(),
            "a schema is given only with --register"
        );
    }

    #[test]
    fn a_binding_not_verified_fails_the_command() {
        assert!(verified(&json!({"verified": true})).is_ok());
        for report in [json!({"verified": false}), json!({}), Value::Null] {
            assert!(verified(&report).is_err(), "{report}");
        }
    }

    #[test]
    fn the_routes_are_the_tenants_and_the_services() {
        assert_eq!(
            path("acme", "loan-service"),
            "/api/tenants/acme/services/loan-service/db-role"
        );
    }
}
