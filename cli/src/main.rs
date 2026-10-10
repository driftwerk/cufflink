use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use std::path::PathBuf;

mod bundle;
mod commands;
mod config;
mod credentials;
mod digest;
mod github;
mod package_manifest;
mod pnpm;
mod project_config;
mod workspace_config;

use commands::services::Status;

#[derive(Parser)]
#[command(
    name = "cufflink",
    about = "Cufflink CLI — Deploy CRUD microservices in seconds",
    subcommand_required = false,
    arg_required_else_help = true
)]
struct Cli {
    /// Target environment (from Cufflink.toml)
    #[arg(short, long, global = true)]
    env: Option<String>,

    /// Print the CLI's version, and with --env the platform's and what it supports
    #[arg(short = 'V', long)]
    version: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new cufflink service project
    Init {
        /// Service name
        name: String,
        /// Template to use (todo, blog, ecommerce, cms)
        #[arg(short, long)]
        template: Option<String>,
        /// Service mode: crud (default) or wasm
        #[arg(short, long, default_value = "crud")]
        mode: String,
        /// Initialize from a GitHub starter kit (e.g., github:owner/repo)
        #[arg(long)]
        from: Option<String>,
    },
    /// List available service templates
    Templates,
    /// Install a package from GitHub or local path
    Install {
        /// Package source (e.g., github:owner/repo, github:owner/repo@v1.0.0, or /local/path)
        source: String,
    },
    /// Deploy all components in an installed package
    DeployPackage {
        /// Package name (as listed in cufflink-packages/)
        name: String,
        /// Skip specific components by name (repeatable)
        #[arg(long, value_delimiter = ',')]
        skip: Option<Vec<String>>,
    },
    /// Deploy a service to the platform
    Deploy {
        /// Allow destructive schema changes
        #[arg(long)]
        allow_destructive: bool,
        /// Under an enforced schema guard, leave what the database
        /// contradicts as it is and deploy the rest
        #[arg(long)]
        accept_drift: bool,
        /// Under an enforced schema guard, drop the tables and columns the
        /// deploy removes instead of archiving them
        #[arg(long)]
        purge: bool,
        /// Print what the deploy would do (schema changes, migrations, what
        /// the guard found) and deploy nothing; exits 3 when the platform
        /// refuses the deploy (422), 1 when the plan could not be made
        #[arg(long)]
        plan: bool,
        /// Deploy without starting the service. A new service is created
        /// inactive; a service that is active is refused
        ///
        /// Start it with `cufflink services enable <name>`. A service that
        /// is already inactive is deployed and stays so, with or without
        /// the flag. Not for web apps, which their runner starts whatever
        /// their status.
        #[arg(long)]
        inactive: bool,
        /// Git commit hash to associate with this deployment (auto-detected
        /// if omitted; with --bundle, the commit the bundle was built at)
        #[arg(long)]
        commit_hash: Option<String>,
        /// Deploy to a specific tenant instead of Cufflink.toml's. Takes
        /// effect only with CUFFLINK_PLATFORM_API_KEY set (a platform
        /// admin's key); without it the deploy goes to the tenant of
        /// Cufflink.toml
        #[arg(long)]
        tenant: Option<String>,
        /// Deploy this bundle, written by `cufflink build --bundle`, instead
        /// of building the working directory: no sources are needed, only
        /// a Cufflink.toml (for --env)
        #[arg(long, value_name = "FILE")]
        bundle: Option<PathBuf>,
    },
    /// Build the service into a bundle, without deploying it
    Build {
        /// Write the service as a bundle: one file `cufflink deploy
        /// --bundle` deploys. The same manifest, module, commit and CLI
        /// give the same file. For WASM and CRUD services; not for native
        /// services or web apps.
        #[arg(long, value_name = "FILE")]
        bundle: PathBuf,
        /// Git commit hash the bundle is of (auto-detected if omitted)
        #[arg(long)]
        commit_hash: Option<String>,
    },
    /// Rollback to a previous deployment version
    Rollback {
        /// Roll back to this version (default: the one before the running
        /// version)
        #[arg(short, long, conflicts_with = "commit")]
        version: Option<i32>,
        /// Roll back to the newest retained version deployed at this
        /// commit: its hash, or at least 7 characters of it (a label
        /// shorter than that, given as --commit-hash, cannot be named here)
        #[arg(long, value_name = "COMMIT_HASH", value_parser = commands::rollback::commit_argument)]
        commit: Option<String>,
        /// The service to roll back, by name, without building it
        /// (default: the one this directory builds)
        #[arg(long, value_name = "NAME", value_parser = clap::builder::NonEmptyStringValueParser::new())]
        service: Option<String>,
        /// Allow destructive schema changes (dropping tables/columns)
        #[arg(long)]
        allow_destructive: bool,
    },
    /// Show service status
    Status,
    /// Stream service logs
    Logs {
        /// Follow log output in real-time
        #[arg(short, long)]
        follow: bool,
        /// Show logs from this duration ago (e.g., 1h, 30m, 24h)
        #[arg(long)]
        since: Option<String>,
        /// Start time (RFC3339)
        #[arg(long)]
        from: Option<String>,
        /// End time (RFC3339)
        #[arg(long)]
        to: Option<String>,
        /// Filter logs containing this text
        #[arg(short, long)]
        search: Option<String>,
        /// Filter by log level (debug, info, warn, error)
        #[arg(short, long)]
        level: Option<String>,
        /// Maximum number of log lines (default: 100)
        #[arg(short = 'n', long)]
        limit: Option<usize>,
        /// Save logs to a file
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Manage API keys (list, revoke, rotate)
    Auth {
        #[command(subcommand)]
        action: commands::auth::AuthAction,
    },
    /// Manage partner keys for `api_key` routes
    Keys {
        #[command(subcommand)]
        action: commands::keys::KeysAction,
    },
    /// List all deployed services
    Services {
        #[command(subcommand)]
        action: Option<ServicesAction>,
    },
    /// Audit custom-route authorization postures (local service, or all deployed)
    Routes {
        /// Audit every deployed service instead of the local one
        #[arg(long)]
        all_services: bool,
        /// Exit non-zero if any anonymous or undeclared route is found
        #[arg(long)]
        audit: bool,
    },
    /// Inspect and clean up deployment history for the current service
    Deployments {
        #[command(subcommand)]
        action: DeploymentsAction,
    },
    /// The durable consumers of the current service
    Consumers {
        #[command(subcommand)]
        action: ConsumersAction,
    },
    /// A platform on this machine to develop against
    Dev {
        #[command(subcommand)]
        action: commands::dev::DevAction,
    },
    /// The tenant's dead-letter queue: what its consumers gave up on
    Dlq {
        #[command(subcommand)]
        action: DlqAction,
    },
    /// Print or save the OpenAPI spec for the current service
    Openapi {
        /// Write spec to a file instead of stdout
        #[arg(short, long)]
        output: Option<String>,
        /// Generate locally from the manifest (no deployment required)
        #[arg(long)]
        local: bool,
        /// Tenant slug for URL paths (used with --local, default: "local")
        #[arg(long)]
        tenant_slug: Option<String>,
    },
    /// Generate a TypeScript client from the OpenAPI spec
    GenerateClient(commands::openapi::GenerateClientArgs),
    /// Platform administration (platform admin only)
    Admin {
        /// Platform API URL (overrides Cufflink.toml and CUFFLINK_API_URL)
        #[arg(long)]
        api_url: Option<String>,
        #[command(subcommand)]
        action: commands::admin::AdminAction,
    },
    /// The extensions a tenant registers (platform admin only)
    Ext {
        /// Platform API URL (overrides Cufflink.toml and CUFFLINK_API_URL)
        #[arg(long)]
        api_url: Option<String>,
        #[command(subcommand)]
        action: commands::ext::ExtAction,
    },
    /// Manage tenants (platform admin only)
    Tenants {
        /// Platform API URL (overrides Cufflink.toml and CUFFLINK_API_URL)
        #[arg(long)]
        api_url: Option<String>,
        #[command(subcommand)]
        action: TenantsAction,
    },
    /// Authenticate with the platform
    Login,
    /// Run tests (hash-based, only changed)
    Test {
        /// Run all tests regardless of hash
        #[arg(long)]
        all: bool,
    },
    /// Seed data into a deployed service from a JSON file
    Seed {
        /// Path to seed JSON file (default: seed.json)
        #[arg(default_value = "seed.json")]
        file: String,
        /// Clear existing data before seeding
        #[arg(long)]
        clear: bool,
        /// Only seed specific tables (comma-separated)
        #[arg(long, value_delimiter = ',')]
        tables: Option<Vec<String>>,
    },
    /// Manage service configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Backup and restore service data
    Backup {
        #[command(subcommand)]
        action: BackupAction,
    },
    /// Manage long-running jobs
    Jobs {
        #[command(subcommand)]
        action: JobsAction,
    },
    /// Workspace-wide deploy, test, seed, and status
    Workspace {
        #[command(subcommand)]
        action: WorkspaceAction,
    },
    /// Generate CI/CD pipeline configuration
    Ci {
        #[command(subcommand)]
        action: CiAction,
    },
    /// Manage roles and role assignments
    Roles {
        #[command(subcommand)]
        action: RolesAction,
    },
    /// Manage encrypted secrets (securestore)
    Secrets {
        #[command(subcommand)]
        action: SecretsAction,
    },
    /// Run a read-only SQL query against a service's tenant database
    Sql {
        /// SQL query string
        query: String,
        /// Service name (defaults to current service if run inside one)
        #[arg(long)]
        service: Option<String>,
        /// Tenant slug override (defaults to tenant from Cufflink.toml for --env)
        #[arg(long)]
        tenant: Option<String>,
        /// Output format: table (default), jsonl, csv, tsv
        #[arg(long)]
        format: Option<String>,
        /// Render/display all rows (default truncates to 1000 in TTY mode). TTY + --all auto-pages.
        #[arg(long)]
        all: bool,
        /// Row cap of one SELECT, WITH, VALUES or TABLE query that writes
        /// nothing, applied as `SELECT * FROM (<query>) q LIMIT N`; anything
        /// else is sent as typed. The server still enforces its own max.
        #[arg(long)]
        limit: Option<u64>,
        /// Disable the auto-pager when rendering large tables on a TTY.
        #[arg(long)]
        no_pager: bool,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// List all config values
    List,
    /// Set a config value
    Set {
        /// Config key
        key: String,
        /// Config value
        value: String,
        /// Mark as secret (value masked in API responses)
        #[arg(long)]
        secret: bool,
    },
    /// Delete a config value
    Delete {
        /// Config key to delete
        key: String,
    },
    /// Sync configs and secrets from Cufflink.toml to the platform: an
    /// apply that removes nothing
    Sync,
    /// Apply the configs and secrets of Cufflink.toml as one set: every
    /// key is written or none is
    Apply {
        /// Remove the keys the platform holds that Cufflink.toml does not declare
        #[arg(long)]
        prune: bool,
        /// Change nothing: print what an apply would change and exit 2 if
        /// anything would (1 if the check itself failed). With --prune, a
        /// key Cufflink.toml does not declare counts as a difference
        #[arg(long)]
        check: bool,
        /// The service to apply to, by name, without building it (default:
        /// the one this directory builds). Refused where Cufflink.toml
        /// names another service as its own
        #[arg(long, value_name = "NAME", value_parser = clap::builder::NonEmptyStringValueParser::new())]
        service: Option<String>,
    },
}

#[derive(Subcommand)]
enum SecretsAction {
    /// Initialize a new secrets vault for an environment
    Init,
    /// Initialize empty vaults in the current service using another workspace service's keys
    InitFrom {
        /// Name of the sibling service to copy key files from (must appear in cufflink-workspace.toml)
        #[arg(long)]
        from: String,
        /// Limit to a single environment (e.g., staging). If omitted, all envs with a key in the source are copied.
        #[arg(long)]
        store: Option<String>,
    },
    /// Re-encrypt existing vaults in the current service using another service's keys
    Rekey {
        /// Name of the sibling service whose key(s) should become the new encryption key
        #[arg(long)]
        from: String,
        /// Limit to a single environment. If omitted, every local vault with a matching source key is rekeyed.
        #[arg(long)]
        store: Option<String>,
    },
    /// Set a secret value
    Set {
        /// Secret name (e.g., "keycloak:client_secret")
        name: String,
        /// Secret value
        value: String,
    },
    /// List secret names (values are not shown unless --reveal is used)
    List {
        /// Show secret values
        #[arg(long)]
        reveal: bool,
    },
    /// Delete a secret from the vault
    Delete {
        /// Secret name to delete
        name: String,
    },
}

#[derive(Subcommand)]
enum ConsumersAction {
    /// List the durables the manifest wants, what the server has of each,
    /// and the handovers on record
    Ls,
}

#[derive(Subcommand)]
enum DlqAction {
    /// List the newest dead letters
    Ls {
        /// Only the letters of this service
        #[arg(long)]
        service: Option<String>,
        /// How many to show (at most 200)
        #[arg(short = 'n', long)]
        limit: Option<usize>,
    },
    /// Publish a dead letter again where it came from, then drop it
    Replay {
        /// The letter's sequence in the queue
        sequence: u64,
    },
    /// Drop a dead letter
    Drop {
        /// The letter's sequence in the queue
        sequence: u64,
    },
}

#[derive(Subcommand)]
enum DeploymentsAction {
    /// List deployment history for the current service
    List {
        /// Show only the most recent N deployments
        #[arg(short = 'n', long)]
        limit: Option<usize>,
    },
    /// Delete oldest deployments, retaining the N most recent (current always kept)
    Prune {
        /// Number of recent deployments to retain (>= 1)
        #[arg(long)]
        keep: usize,
        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
    },
    /// Delete a single deployment by version (cannot delete the active version)
    Delete {
        /// Deployment version to remove
        version: i32,
        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum ServicesAction {
    /// List deployed services
    List {
        /// Also show each service's required platform features
        #[arg(long)]
        features: bool,
    },
    /// Delete a deployed service and all its data (a native service as a
    /// platform admin, with CUFFLINK_PLATFORM_API_KEY)
    Delete {
        /// Service name to delete
        name: String,
        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
        /// Delete a native service even while a deployed service calls one
        /// of its extensions
        #[arg(long)]
        force: bool,
    },
    /// The database role a service runs as (platform admin only)
    DbRole {
        /// Platform API URL (overrides Cufflink.toml and CUFFLINK_API_URL)
        #[arg(long)]
        api_url: Option<String>,
        #[command(subcommand)]
        action: commands::db_role::DbRoleAction,
    },
    /// Enable a service (make it active and routable; a native service as a
    /// platform admin)
    Enable {
        /// Service name
        name: String,
        /// The service of a specific tenant instead of Cufflink.toml's.
        /// Needs CUFFLINK_PLATFORM_API_KEY (a platform admin's key), and is
        /// refused without it
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Disable a service (make it inactive — stops routing traffic; a
    /// native service as a platform admin)
    Disable {
        /// Service name
        name: String,
        /// The service of a specific tenant instead of Cufflink.toml's.
        /// Needs CUFFLINK_PLATFORM_API_KEY (a platform admin's key), and is
        /// refused without it
        #[arg(long)]
        tenant: Option<String>,
    },
}

#[derive(Subcommand)]
enum JobsAction {
    /// Give a failed job one more attempt
    Retry {
        /// The job's id
        job_id: uuid::Uuid,
    },
}

#[derive(Subcommand)]
enum BackupAction {
    /// Show service tables, columns, and FK dependencies
    Schema,
    /// Export service data to a file
    Export {
        /// Output file path
        #[arg(short, long, default_value = "backup.jsonl")]
        output: String,
        /// Export only specific tables (comma-separated)
        #[arg(long, value_delimiter = ',')]
        tables: Option<Vec<String>>,
        /// Return job ID immediately without waiting
        #[arg(long, rename_all = "kebab-case")]
        r#async: bool,
    },
    /// Restore service data from a file or a previous backup
    Restore {
        /// Input file path (optional if --from-backup is used)
        file: Option<String>,
        /// Restore directly from a previous backup job ID (skip local file)
        #[arg(long)]
        from_backup: Option<String>,
        /// Restore only specific tables (comma-separated)
        #[arg(long, value_delimiter = ',')]
        tables: Option<Vec<String>>,
        /// Clear existing data before restoring
        #[arg(long)]
        clear: bool,
        /// Skip schema and FK validation checks
        #[arg(long)]
        force: bool,
        /// Return job ID immediately without waiting
        #[arg(long, rename_all = "kebab-case")]
        r#async: bool,
    },
    /// List completed backups available for restore
    List,
    /// List all backup/restore jobs (including in-progress and failed)
    Jobs,
    /// Cancel a running backup/restore job
    Cancel {
        /// Job ID to cancel
        job_id: String,
    },
}

#[derive(Subcommand)]
enum WorkspaceAction {
    /// Deploy all services for the current environment
    Deploy {
        /// Skip specific services (comma-separated)
        #[arg(long, value_delimiter = ',')]
        skip: Option<Vec<String>>,
        /// Deploy to a specific tenant instead of Cufflink.toml's. Takes
        /// effect only with CUFFLINK_PLATFORM_API_KEY set (a platform
        /// admin's key); without it the deploy goes to the tenant of
        /// Cufflink.toml
        #[arg(long)]
        tenant: Option<String>,
        /// Deploy services in parallel
        #[arg(long)]
        parallel: bool,
    },
    /// Run tests for all services
    Test {
        /// Skip specific services (comma-separated)
        #[arg(long, value_delimiter = ',')]
        skip: Option<Vec<String>>,
        /// Show full test output
        #[arg(long)]
        all: bool,
    },
    /// Show deploy status of all services
    Status,
    /// Seed data for configured services
    Seed,
    /// Create or destroy a preview environment
    Preview {
        #[command(subcommand)]
        action: PreviewAction,
    },
}

#[derive(Subcommand)]
enum PreviewAction {
    /// Create a preview environment (new tenant + deploy + clone data)
    Create {
        /// Preview name (used as tenant slug: preview-<name>)
        #[arg(long)]
        name: String,
        /// Source tenant to clone data from
        #[arg(long, default_value = "default")]
        source_tenant: String,
    },
    /// Destroy a preview environment
    Destroy {
        /// Preview name
        #[arg(long)]
        name: String,
    },
}

#[derive(Subcommand)]
enum CiAction {
    /// Generate GitHub Actions workflow for deploy pipeline
    Init {
        /// CI provider (github)
        #[arg(long, default_value = "github")]
        provider: String,
    },
}

#[derive(Subcommand)]
enum TenantsAction {
    /// Create a new tenant
    Create {
        /// Tenant display name
        #[arg(long)]
        name: String,
        /// Tenant slug (URL-safe identifier)
        #[arg(long)]
        slug: String,
        /// Keycloak URL for this tenant's realm
        #[arg(long)]
        tenant_keycloak_url: Option<String>,
        /// Keycloak realm name for this tenant
        #[arg(long)]
        tenant_keycloak_realm: Option<String>,
        /// Required Keycloak realm role for deploy access (default: cufflink-deploy)
        #[arg(long)]
        deploy_role: Option<String>,
        /// An existing database the tenant adopts; the platform never
        /// creates or drops it, and it cannot be changed afterwards
        #[arg(long)]
        database_name: Option<String>,
    },
    /// Update a tenant's configuration
    Update {
        /// Tenant slug to update
        slug: String,
        /// New display name
        #[arg(long)]
        name: Option<String>,
        /// Keycloak URL for this tenant's realm
        #[arg(long)]
        tenant_keycloak_url: Option<String>,
        /// Keycloak realm name for this tenant
        #[arg(long)]
        tenant_keycloak_realm: Option<String>,
        /// Required Keycloak realm role for deploy access
        #[arg(long)]
        deploy_role: Option<String>,
        /// Keycloak client ID for admin API operations (user search)
        #[arg(long)]
        keycloak_ops_client_id: Option<String>,
        /// Keycloak client secret for admin API operations (user search)
        #[arg(long)]
        keycloak_ops_client_secret: Option<String>,
        /// Comma-separated Keycloak client ids treated as service accounts on
        /// this tenant; an empty value clears the list
        #[arg(long)]
        service_account_clients: Option<String>,
        /// Browser-facing Keycloak base URL this tenant's tokens are issued
        /// under, when it differs from the platform's; an empty value clears it
        #[arg(long)]
        keycloak_issuer: Option<String>,
    },
    /// Show or change a tenant's policies
    Policy {
        #[command(subcommand)]
        action: PolicyAction,
    },
    /// Show or replace the NATS subjects and streams a tenant claims
    /// beyond its own
    NatsClaims {
        #[command(subcommand)]
        action: NatsClaimsAction,
    },
    /// Create, list or revoke the keys a tenant's runners (its extension
    /// runner and a tenant-scoped web runner) authenticate with
    RunnerKeys {
        #[command(subcommand)]
        action: commands::runner_keys::RunnerKeysAction,
    },
    /// List all tenants
    List,
    /// Delete a tenant and all its data
    Delete {
        /// Tenant slug to delete
        slug: String,
        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
    },
    /// Create a platform admin API key
    CreateApiKey {
        /// Key name (for identification)
        #[arg(long)]
        name: String,
    },
    /// List platform admin API keys
    ListApiKeys,
    /// Revoke a platform admin API key
    RevokeApiKey {
        /// API key ID to revoke
        id: String,
    },
}

#[derive(Subcommand)]
enum NatsClaimsAction {
    /// Show the tenant's claims
    Show {
        /// Tenant slug
        slug: String,
    },
    /// Replace the tenant's claims with the ones given
    Set {
        /// Tenant slug
        slug: String,
        /// A subject pattern the tenant claims (repeatable)
        #[arg(
            long = "subject",
            required_unless_present_any = ["streams", "clear"]
        )]
        subjects: Vec<String>,
        /// A stream the tenant claims (repeatable)
        #[arg(long = "stream")]
        streams: Vec<String>,
        /// Leave the tenant with no claim
        #[arg(long, conflicts_with_all = ["subjects", "streams"])]
        clear: bool,
    },
}

#[derive(Subcommand)]
enum PolicyAction {
    /// Show every policy's mode, config and whether it is the tenant's own
    Get {
        /// Tenant slug
        slug: String,
    },
    /// Set a policy's mode (and optionally its config) for a tenant
    Set {
        /// Tenant slug
        slug: String,
        /// Policy key (tenant_binding, graphql, storage_scope, egress,
        /// wasm_limits, service_accounts, audience, redis_scope, strict_filters,
        /// outbox, db_roles, schema_guard)
        key: cufflink_types::policy::PolicyKey,
        /// off, warn or enforce
        mode: cufflink_types::policy::PolicyMode,
        /// Policy config as JSON; omitted keeps the current config
        #[arg(long, value_parser = parse_json)]
        config: Option<serde_json::Value>,
    },
    /// Drop the tenant's own setting, returning to the platform default
    Reset {
        /// Tenant slug
        slug: String,
        /// Policy key
        key: cufflink_types::policy::PolicyKey,
    },
}

fn parse_json(raw: &str) -> Result<serde_json::Value, String> {
    serde_json::from_str(raw).map_err(|e| format!("not valid JSON: {e}"))
}

#[derive(Subcommand)]
enum RolesAction {
    /// List all roles and their permissions
    List,
    /// Assign a role to a user
    Assign {
        /// User email or username to search for
        #[arg(long)]
        user: String,
        /// Role name to assign
        #[arg(long)]
        role: String,
    },
    /// Remove a role from a user
    Unassign {
        /// User email or username to search for
        #[arg(long)]
        user: String,
        /// Role name to remove
        #[arg(long)]
        role: String,
    },
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    run().await.map_or_else(
        |error| {
            eprintln!("{}", told(&error));
            std::process::ExitCode::from(exit_code(&error))
        },
        |()| std::process::ExitCode::SUCCESS,
    )
}

/// What a command that did not succeed ends with. A config that differs
/// and a verify that found something are answers, not faults of the
/// CLI's: each is told without where in the CLI it was found.
fn told(error: &eyre::Report) -> String {
    let answer = error
        .downcast_ref::<commands::config_cmd::Drifted>()
        .map(ToString::to_string)
        .or_else(|| {
            error
                .downcast_ref::<commands::admin::Unverified>()
                .map(ToString::to_string)
        });
    answer.unwrap_or_else(|| format!("Error: {error:?}"))
}

/// 3 for a plan the platform refused, 2 for a config check that found a
/// difference, 1 for any other failure.
fn exit_code(error: &eyre::Report) -> u8 {
    [
        (
            error.downcast_ref::<commands::deploy::Refused>().is_some(),
            commands::deploy::Refused::EXIT_CODE,
        ),
        (
            error
                .downcast_ref::<commands::config_cmd::Drifted>()
                .is_some(),
            commands::config_cmd::Drifted::EXIT_CODE,
        ),
    ]
    .into_iter()
    .find_map(|(found, code)| found.then_some(code))
    .unwrap_or(1)
}

async fn run() -> eyre::Result<()> {
    let matches = Cli::command().get_matches();
    if matches.get_flag("version") {
        return commands::version::print(matches.get_one::<String>("env").map(String::as_str))
            .await;
    }
    let cli = Cli::from_arg_matches(&matches)
        .unwrap_or_else(|error| error.format(&mut Cli::command()).exit());
    let env = cli.env.as_deref();

    match cli.command {
        Commands::Init {
            name,
            template,
            mode,
            from,
        } => {
            commands::init::run(&name, template.as_deref(), &mode, from.as_deref()).await?;
        }
        Commands::Install { source } => {
            commands::install::run(&source).await?;
        }
        Commands::DeployPackage { name, skip } => {
            let skip_list = skip.unwrap_or_default();
            commands::deploy_package::run(&name, &skip_list, env).await?;
        }
        Commands::Templates => {
            commands::init::list_templates();
        }
        Commands::Deploy {
            allow_destructive,
            accept_drift,
            purge,
            plan,
            inactive,
            commit_hash,
            tenant,
            bundle,
        } => {
            let flags = commands::deploy::DeployFlags {
                allow_destructive,
                accept_drift,
                purge,
                plan,
                inactive,
            };
            let tenant = tenant.as_deref();
            match bundle {
                Some(bundle) => {
                    commands::deploy::run_bundle(flags, &bundle, commit_hash, env, tenant).await?
                }
                None => commands::deploy::run_with(flags, commit_hash, env, tenant).await?,
            }
        }
        Commands::Build {
            bundle,
            commit_hash,
        } => {
            commands::build::bundle(&bundle, commit_hash)?;
        }
        Commands::Rollback {
            version,
            commit,
            service,
            allow_destructive,
        } => {
            let to = commands::rollback::To::asked(version, commit);
            commands::rollback::run(to, service.as_deref(), allow_destructive, env).await?;
        }
        Commands::Status => {
            commands::status::run(env).await?;
        }
        Commands::Logs {
            follow,
            since,
            from,
            to,
            search,
            level,
            limit,
            output,
        } => {
            commands::logs::run(
                commands::logs::LogParams {
                    follow,
                    since: since.as_deref(),
                    from: from.as_deref(),
                    to: to.as_deref(),
                    search: search.as_deref(),
                    level: level.as_deref(),
                    limit,
                    output: output.as_deref(),
                },
                env,
            )
            .await?;
        }
        Commands::Auth { action } => commands::auth::run(action, env).await?,
        Commands::Keys { action } => commands::keys::run(action, env).await?,
        Commands::Services { action } => match action {
            None => commands::services::run(false, env).await?,
            Some(ServicesAction::List { features }) => {
                commands::services::run(features, env).await?
            }
            Some(ServicesAction::Delete { name, yes, force }) => {
                commands::services::delete(&name, yes, force, env).await?
            }
            Some(ServicesAction::DbRole { api_url, action }) => {
                commands::db_role::run(action, api_url.as_deref(), env).await?
            }
            Some(ServicesAction::Enable { name, tenant }) => {
                commands::services::set_status(&name, Status::Active, env, tenant.as_deref())
                    .await?
            }
            Some(ServicesAction::Disable { name, tenant }) => {
                commands::services::set_status(&name, Status::Inactive, env, tenant.as_deref())
                    .await?
            }
        },
        Commands::Routes {
            all_services,
            audit,
        } => commands::routes::run(all_services, audit, env).await?,
        Commands::Consumers { action } => match action {
            ConsumersAction::Ls => commands::consumers::ls(env).await?,
        },
        Commands::Dev { action } => commands::dev::run(action).await?,
        Commands::Dlq { action } => match action {
            DlqAction::Ls { service, limit } => {
                commands::dlq::ls(service.as_deref(), limit, env).await?
            }
            DlqAction::Replay { sequence } => commands::dlq::replay(sequence, env).await?,
            DlqAction::Drop { sequence } => commands::dlq::drop(sequence, env).await?,
        },
        Commands::Deployments { action } => match action {
            DeploymentsAction::List { limit } => commands::deployments::list(limit, env).await?,
            DeploymentsAction::Prune { keep, yes } => {
                commands::deployments::prune(keep, yes, env).await?
            }
            DeploymentsAction::Delete { version, yes } => {
                commands::deployments::delete(version, yes, env).await?
            }
        },
        Commands::Openapi {
            output,
            local,
            tenant_slug,
        } => {
            if let Some(path) = output {
                commands::openapi::save(&path, local, tenant_slug.as_deref(), env).await?;
            } else {
                commands::openapi::run(local, tenant_slug.as_deref(), env).await?;
            }
        }
        Commands::GenerateClient(args) => {
            commands::openapi::generate_client(&args, env).await?;
        }
        Commands::Admin { action, api_url } => {
            commands::admin::run(action, api_url.as_deref(), env).await?
        }
        Commands::Ext { action, api_url } => {
            commands::ext::run(action, api_url.as_deref(), env).await?
        }
        Commands::Tenants { action, api_url } => match action {
            TenantsAction::Create {
                name,
                slug,
                tenant_keycloak_url,
                tenant_keycloak_realm,
                deploy_role,
                database_name,
            } => {
                commands::tenants::create(
                    &commands::tenants::NewTenant {
                        name: &name,
                        slug: &slug,
                        keycloak_url: tenant_keycloak_url.as_deref(),
                        keycloak_realm: tenant_keycloak_realm.as_deref(),
                        deploy_role: deploy_role.as_deref(),
                        database_name: database_name.as_deref(),
                    },
                    api_url.as_deref(),
                    env,
                )
                .await?
            }
            TenantsAction::Update {
                slug,
                name,
                tenant_keycloak_url,
                tenant_keycloak_realm,
                deploy_role,
                keycloak_ops_client_id,
                keycloak_ops_client_secret,
                service_account_clients,
                keycloak_issuer,
            } => {
                let changes = commands::tenants::TenantChanges {
                    name: name.as_deref(),
                    keycloak_url: tenant_keycloak_url.as_deref(),
                    keycloak_realm: tenant_keycloak_realm.as_deref(),
                    deploy_role: deploy_role.as_deref(),
                    keycloak_ops_client_id: keycloak_ops_client_id.as_deref(),
                    keycloak_ops_client_secret: keycloak_ops_client_secret.as_deref(),
                    service_account_clients: service_account_clients.as_deref(),
                    keycloak_issuer: keycloak_issuer.as_deref(),
                };
                commands::tenants::update(&slug, &changes, api_url.as_deref(), env).await?
            }
            TenantsAction::Policy { action } => match action {
                PolicyAction::Get { slug } => {
                    commands::tenants::policy_get(&slug, api_url.as_deref(), env).await?
                }
                PolicyAction::Set {
                    slug,
                    key,
                    mode,
                    config,
                } => {
                    commands::tenants::policy_set(&slug, key, mode, config, api_url.as_deref(), env)
                        .await?
                }
                PolicyAction::Reset { slug, key } => {
                    commands::tenants::policy_reset(&slug, key, api_url.as_deref(), env).await?
                }
            },
            TenantsAction::NatsClaims { action } => match action {
                NatsClaimsAction::Show { slug } => {
                    commands::tenants::nats_claims_show(&slug, api_url.as_deref(), env).await?
                }
                NatsClaimsAction::Set {
                    slug,
                    subjects,
                    streams,
                    ..
                } => {
                    commands::tenants::nats_claims_set(
                        &slug,
                        &subjects,
                        &streams,
                        api_url.as_deref(),
                        env,
                    )
                    .await?
                }
            },
            TenantsAction::RunnerKeys { action } => {
                commands::runner_keys::run(action, api_url.as_deref(), env).await?
            }
            TenantsAction::List => commands::tenants::list(api_url.as_deref(), env).await?,
            TenantsAction::Delete { slug, yes } => {
                commands::tenants::delete(&slug, yes, api_url.as_deref(), env).await?
            }
            TenantsAction::CreateApiKey { name } => {
                commands::tenants::create_platform_api_key(&name, api_url.as_deref(), env).await?
            }
            TenantsAction::ListApiKeys => {
                commands::tenants::list_platform_api_keys(api_url.as_deref(), env).await?
            }
            TenantsAction::RevokeApiKey { id } => {
                commands::tenants::revoke_platform_api_key(&id, api_url.as_deref(), env).await?
            }
        },
        Commands::Login => {
            commands::login::run(env).await?;
        }
        Commands::Test { all } => {
            commands::test::run(all, env).await?;
        }
        Commands::Seed {
            file,
            clear,
            tables,
        } => {
            commands::seed::run(&file, clear, tables, env).await?;
        }
        Commands::Config { action } => match action {
            ConfigAction::List => commands::config_cmd::list(env).await?,
            ConfigAction::Set { key, value, secret } => {
                commands::config_cmd::set(&key, &value, secret, env).await?
            }
            ConfigAction::Delete { key } => commands::config_cmd::delete(&key, env).await?,
            ConfigAction::Sync => commands::config_cmd::sync(env).await?,
            ConfigAction::Apply {
                prune,
                check,
                service,
            } => {
                let flags = commands::config_cmd::ApplyFlags { prune, check };
                commands::config_cmd::apply(flags, service.as_deref(), env).await?
            }
        },
        Commands::Jobs { action } => match action {
            JobsAction::Retry { job_id } => commands::jobs::retry(job_id, env).await?,
        },
        Commands::Backup { action } => match action {
            BackupAction::Schema => commands::backup::schema(env).await?,
            BackupAction::Export {
                output,
                tables,
                r#async,
            } => commands::backup::export(&output, tables, r#async, env).await?,
            BackupAction::Restore {
                file,
                from_backup,
                tables,
                clear,
                force,
                r#async,
            } => {
                commands::backup::restore(
                    file.as_deref(),
                    from_backup.as_deref(),
                    tables,
                    clear,
                    force,
                    r#async,
                    env,
                )
                .await?
            }
            BackupAction::List => commands::backup::list(env).await?,
            BackupAction::Jobs => commands::backup::jobs(env).await?,
            BackupAction::Cancel { job_id } => commands::backup::cancel(&job_id, env).await?,
        },
        Commands::Workspace { action } => match action {
            WorkspaceAction::Deploy {
                skip,
                tenant,
                parallel,
            } => {
                let skip_list = skip.unwrap_or_default();
                commands::workspace::deploy(&skip_list, env, tenant.as_deref(), parallel).await?
            }
            WorkspaceAction::Test { skip, all } => {
                let skip_list = skip.unwrap_or_default();
                commands::workspace::test(&skip_list, all).await?
            }
            WorkspaceAction::Status => commands::workspace::status(env).await?,
            WorkspaceAction::Seed => commands::workspace::seed(env).await?,
            WorkspaceAction::Preview { action } => match action {
                PreviewAction::Create {
                    name,
                    source_tenant,
                } => commands::workspace::preview_create(&name, &source_tenant, env).await?,
                PreviewAction::Destroy { name } => {
                    commands::workspace::preview_destroy(&name, env).await?
                }
            },
        },
        Commands::Ci { action } => match action {
            CiAction::Init { provider } => commands::ci::init(&provider)?,
        },
        Commands::Secrets { action } => match action {
            SecretsAction::Init => commands::secrets_cmd::init(env)?,
            SecretsAction::InitFrom { from, store } => {
                commands::secrets_cmd::init_from(&from, store.as_deref(), env)?
            }
            SecretsAction::Rekey { from, store } => {
                commands::secrets_cmd::rekey(&from, store.as_deref(), env)?
            }
            SecretsAction::Set { name, value } => commands::secrets_cmd::set(&name, &value, env)?,
            SecretsAction::List { reveal } => commands::secrets_cmd::list(env, reveal)?,
            SecretsAction::Delete { name } => commands::secrets_cmd::delete(&name, env)?,
        },
        Commands::Sql {
            query,
            service,
            tenant,
            format,
            all,
            limit,
            no_pager,
        } => {
            commands::sql::run(
                &query,
                service.as_deref(),
                tenant.as_deref(),
                format.as_deref(),
                all,
                limit,
                no_pager,
                env,
            )
            .await?
        }
        Commands::Roles { action } => match action {
            RolesAction::List => commands::roles::list(env).await?,
            RolesAction::Assign { user, role } => {
                commands::roles::assign(&user, &role, env).await?
            }
            RolesAction::Unassign { user, role } => {
                commands::roles::unassign(&user, &role, env).await?
            }
        },
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cufflink_types::policy::{PolicyKey, PolicyMode};

    #[test]
    fn the_version_is_asked_for_with_or_without_an_environment() {
        Cli::command().debug_assert();
        let asked = |argv: &[&str]| {
            Cli::command()
                .try_get_matches_from(argv)
                .map(|matches| {
                    (
                        matches.get_flag("version"),
                        matches.get_one::<String>("env").cloned(),
                    )
                })
                .ok()
        };
        assert_eq!(asked(&["cufflink", "--version"]), Some((true, None)));
        assert_eq!(
            asked(&["cufflink", "-V", "--env", "dev"]),
            Some((true, Some("dev".to_string())))
        );
        assert!(Cli::try_parse_from(["cufflink", "--env", "dev"]).is_err());
        assert!(Cli::try_parse_from(["cufflink"]).is_err());
        assert!(matches!(
            Cli::try_parse_from(["cufflink", "--env", "dev", "status"]).map(|cli| cli.command),
            Ok(Commands::Status)
        ));
    }

    #[test]
    fn the_development_stack_is_started_from_images_or_from_source() {
        use commands::dev::DevAction;
        let dev = |arguments: &[&str]| {
            Cli::try_parse_from(["cufflink", "dev"].iter().chain(arguments)).map(|cli| cli.command)
        };
        assert!(matches!(
            dev(&["up"]),
            Ok(Commands::Dev {
                action: DevAction::Up {
                    from_source: None,
                    platform_image: None,
                    worker_image: None,
                    web_runner_image: None,
                    ext_runner_image: None,
                    with_web_runner: false,
                    with_ext_runner: false,
                }
            })
        ));
        assert!(matches!(
            dev(&["up", "--platform-image", "registry.test/platform:1", "--with-web-runner"]),
            Ok(Commands::Dev {
                action: DevAction::Up {
                    platform_image: Some(image),
                    with_web_runner: true,
                    with_ext_runner: false,
                    ..
                }
            }) if image == "registry.test/platform:1"
        ));
        assert!(matches!(
            dev(&["up", "--ext-runner-image", "registry.test/ext-runner:1", "--with-ext-runner"]),
            Ok(Commands::Dev {
                action: DevAction::Up {
                    ext_runner_image: Some(image),
                    with_web_runner: false,
                    with_ext_runner: true,
                    ..
                }
            }) if image == "registry.test/ext-runner:1"
        ));
        assert!(matches!(
            dev(&["up", "--from-source", "../cufflink"]),
            Ok(Commands::Dev {
                action: DevAction::Up {
                    from_source: Some(repo),
                    ..
                }
            }) if repo == std::path::Path::new("../cufflink")
        ));
        for image in [
            "--platform-image",
            "--worker-image",
            "--web-runner-image",
            "--ext-runner-image",
        ] {
            assert!(
                dev(&["up", "--from-source", ".", image, "x"]).is_err(),
                "{image}"
            );
        }
    }

    #[test]
    fn the_development_stack_is_stopped_read_and_deployed_to() {
        use commands::dev::DevAction;
        let dev = |arguments: &[&str]| {
            Cli::try_parse_from(["cufflink", "dev"].iter().chain(arguments)).map(|cli| cli.command)
        };
        assert!(matches!(
            dev(&["down", "--volumes"]),
            Ok(Commands::Dev {
                action: DevAction::Down { volumes: true }
            })
        ));
        assert!(matches!(
            dev(&["logs", "platform", "-f"]),
            Ok(Commands::Dev {
                action: DevAction::Logs { service: Some(service), follow: true }
            }) if service == "platform"
        ));
        assert!(matches!(
            dev(&["logs"]),
            Ok(Commands::Dev {
                action: DevAction::Logs {
                    service: None,
                    follow: false
                }
            })
        ));
        assert!(matches!(
            dev(&["token", "--user", "alice"]),
            Ok(Commands::Dev {
                action: DevAction::Token { user, password: None }
            }) if user == "alice"
        ));
        assert!(dev(&["token"]).is_err());
        assert!(matches!(
            dev(&["deploy", "--watch"]),
            Ok(Commands::Dev {
                action: DevAction::Deploy { watch: true }
            })
        ));
        assert!(matches!(
            dev(&["doctor"]),
            Ok(Commands::Dev {
                action: DevAction::Doctor {
                    with_web_runner: false,
                    with_ext_runner: false
                }
            })
        ));
        assert!(matches!(
            dev(&["doctor", "--with-web-runner"]),
            Ok(Commands::Dev {
                action: DevAction::Doctor {
                    with_web_runner: true,
                    with_ext_runner: false
                }
            })
        ));
        assert!(matches!(
            dev(&["doctor", "--with-ext-runner", "--with-web-runner"]),
            Ok(Commands::Dev {
                action: DevAction::Doctor {
                    with_web_runner: true,
                    with_ext_runner: true
                }
            })
        ));
    }

    #[test]
    fn a_job_is_retried_by_its_id() {
        let retry = |id: &'static str| {
            Cli::try_parse_from(["cufflink", "jobs", "retry", id]).map(|cli| cli.command)
        };
        assert!(matches!(
            retry("97c9e5c1-3f4a-4d0f-a53a-2273c4be3aad"),
            Ok(Commands::Jobs {
                action: JobsAction::Retry { job_id }
            }) if job_id.to_string() == "97c9e5c1-3f4a-4d0f-a53a-2273c4be3aad"
        ));
        assert!(retry("not-a-job").is_err());
        assert!(Cli::try_parse_from(["cufflink", "jobs", "retry"]).is_err());
    }

    #[test]
    fn nats_claims_take_repeated_subjects_and_streams() {
        let argv = [
            "cufflink",
            "tenants",
            "nats-claims",
            "set",
            "harbor",
            "--subject",
            "books.>",
            "--subject",
            "audit.logs",
            "--stream",
            "BOOKS",
        ];
        let parsed = Cli::try_parse_from(argv).expect("parses").command;
        assert!(matches!(
            parsed,
            Commands::Tenants {
                action: TenantsAction::NatsClaims {
                    action: NatsClaimsAction::Set { slug, subjects, streams, clear }
                },
                ..
            } if slug == "harbor"
                && subjects == ["books.>", "audit.logs"]
                && streams == ["BOOKS"]
                && !clear
        ));
        let set = |flags: &[&str]| {
            Cli::try_parse_from(
                ["cufflink", "tenants", "nats-claims", "set", "harbor"]
                    .iter()
                    .chain(flags),
            )
            .map(|cli| cli.command)
        };
        assert!(matches!(
            set(&["--clear"]),
            Ok(Commands::Tenants {
                action: TenantsAction::NatsClaims {
                    action: NatsClaimsAction::Set { subjects, streams, clear, .. }
                },
                ..
            }) if subjects.is_empty() && streams.is_empty() && clear
        ));
        assert!(matches!(
            set(&["--stream", "BOOKS"]),
            Ok(Commands::Tenants {
                action: TenantsAction::NatsClaims {
                    action: NatsClaimsAction::Set { subjects, streams, .. }
                },
                ..
            }) if subjects.is_empty() && streams == ["BOOKS"]
        ));
        assert!(
            set(&[]).is_err(),
            "every claim is never dropped by leaving the flags out"
        );
        for both in [
            ["--clear", "--subject", "books.>"],
            ["--clear", "--stream", "BOOKS"],
        ] {
            assert!(set(&both).is_err(), "{both:?}");
        }
    }

    #[test]
    fn a_deploy_is_asked_not_to_start_its_service() {
        let inactive = |arguments: &[&str]| {
            Cli::try_parse_from(["cufflink", "deploy"].iter().chain(arguments))
                .ok()
                .map(|cli| match cli.command {
                    Commands::Deploy { inactive, plan, .. } => (inactive, plan),
                    _ => panic!("not a deploy"),
                })
        };
        assert_eq!(inactive(&[]), Some((false, false)));
        assert_eq!(inactive(&["--inactive"]), Some((true, false)));
        assert_eq!(inactive(&["--plan", "--inactive"]), Some((true, true)));
        assert_eq!(inactive(&["--inactive=false"]), None);
        let workspace: Result<Cli, clap::Error> =
            Cli::try_parse_from(["cufflink", "workspace", "deploy", "--inactive"]);
        assert!(
            workspace.is_err(),
            "only a service's own deploy creates it inactive"
        );
        let help = Cli::command()
            .find_subcommand("deploy")
            .and_then(|deploy| {
                deploy
                    .get_arguments()
                    .find(|argument| argument.get_id() == "inactive")
                    .and_then(|argument| argument.get_help().map(ToString::to_string))
            })
            .expect("help");
        assert_eq!(
            help,
            "Deploy without starting the service. A new service is created inactive; \
             a service that is active is refused"
        );
        let long_help = Cli::command()
            .find_subcommand("deploy")
            .and_then(|deploy| {
                deploy
                    .get_arguments()
                    .find(|argument| argument.get_id() == "inactive")
                    .and_then(|argument| argument.get_long_help().map(ToString::to_string))
            })
            .expect("long help");
        assert!(
            long_help.starts_with(&help)
                && long_help.contains("Start it with `cufflink services enable <name>`")
                && long_help.contains("Not for web apps"),
            "{long_help}"
        );
    }

    #[test]
    fn a_service_is_enabled_or_disabled_in_the_tenant_a_deploy_was_sent_to() {
        let switched = |arguments: &[&str]| {
            Cli::try_parse_from(["cufflink", "services"].iter().chain(arguments))
                .ok()
                .and_then(|cli| match cli.command {
                    Commands::Services {
                        action: Some(ServicesAction::Enable { name, tenant }),
                    } => Some((Status::Active, name, tenant, cli.env)),
                    Commands::Services {
                        action: Some(ServicesAction::Disable { name, tenant }),
                    } => Some((Status::Inactive, name, tenant, cli.env)),
                    _ => None,
                })
        };
        assert_eq!(
            switched(&["enable", "loans"]),
            Some((Status::Active, "loans".to_string(), None, None))
        );
        assert_eq!(
            switched(&["disable", "loans", "--env", "staging", "--tenant", "acme"]),
            Some((
                Status::Inactive,
                "loans".to_string(),
                Some("acme".to_string()),
                Some("staging".to_string())
            )),
            "the command a deploy's hint spells"
        );
    }

    fn policy_action(args: &[&str]) -> Result<PolicyAction, clap::Error> {
        let argv = ["cufflink", "tenants", "policy"].iter().chain(args);
        match Cli::try_parse_from(argv)?.command {
            Commands::Tenants {
                action: TenantsAction::Policy { action },
                ..
            } => Ok(action),
            _ => panic!("not a policy command"),
        }
    }

    fn generate_client_args(args: &[&str]) -> commands::openapi::GenerateClientArgs {
        let argv = ["cufflink", "generate-client"].iter().chain(args);
        match Cli::try_parse_from(argv).expect("parses").command {
            Commands::GenerateClient(args) => args,
            _ => panic!("not generate-client"),
        }
    }

    #[test]
    fn generate_client_flags_are_unchanged() {
        let defaults = generate_client_args(&[]);
        assert_eq!(defaults.output, "./generated");
        assert!(!defaults.local);
        assert_eq!(defaults.tenant_slug, None);
        let local = generate_client_args(&[
            "--local",
            "--output",
            "clients/typescript",
            "--tenant-slug",
            "acme",
        ]);
        assert_eq!(local.output, "clients/typescript");
        assert!(local.local);
        assert_eq!(local.tenant_slug.as_deref(), Some("acme"));
        assert_eq!(generate_client_args(&["-o", "out"]).output, "out");
        let with_env = Cli::try_parse_from(["cufflink", "generate-client", "--env", "staging"])
            .expect("parses");
        assert_eq!(with_env.env.as_deref(), Some("staging"));
    }

    #[test]
    fn policy_set_parses_key_mode_and_config() {
        match policy_action(&[
            "set",
            "harbor",
            "storage_scope",
            "enforce",
            "--config",
            r#"{"scope":"strict"}"#,
        ])
        .expect("parses")
        {
            PolicyAction::Set {
                slug,
                key,
                mode,
                config,
            } => {
                assert_eq!(slug, "harbor");
                assert_eq!(key, PolicyKey::StorageScope);
                assert_eq!(mode, PolicyMode::Enforce);
                assert_eq!(config, Some(serde_json::json!({"scope": "strict"})));
            }
            _ => panic!("expected set"),
        }
    }

    #[test]
    fn policy_set_rejects_unknown_keys_modes_and_bad_json() {
        assert!(policy_action(&["set", "t", "secret_rotation", "warn"]).is_err());
        assert!(policy_action(&["set", "t", "egress", "loud"]).is_err());
        assert!(policy_action(&["set", "t", "egress", "warn", "--config", "{"]).is_err());
        assert!(matches!(
            policy_action(&["reset", "t", "graphql"]).expect("parses"),
            PolicyAction::Reset {
                key: PolicyKey::Graphql,
                ..
            }
        ));
    }

    #[test]
    fn the_r3_policies_are_settable_keys() {
        for (name, key) in [
            ("outbox", PolicyKey::Outbox),
            ("db_roles", PolicyKey::DbRoles),
            ("schema_guard", PolicyKey::SchemaGuard),
        ] {
            assert!(
                matches!(
                    policy_action(&["set", "harbor", name, "enforce"]).expect("parses"),
                    PolicyAction::Set { key: parsed, mode: PolicyMode::Enforce, .. } if parsed == key
                ),
                "{name}"
            );
        }
    }

    #[test]
    fn a_refused_plan_exits_3_a_config_that_differs_2_and_any_other_failure_1() {
        let refused: eyre::Report = commands::deploy::Refused { body: "{}".into() }.into();
        assert_eq!(exit_code(&refused), 3);
        assert_eq!(exit_code(&refused.wrap_err("while planning")), 3);
        let drifted: eyre::Report = commands::config_cmd::Drifted {
            service: "loans".into(),
        }
        .into();
        assert_eq!(exit_code(&drifted), 2);
        assert_eq!(
            told(&drifted),
            "the config of loans differs from what Cufflink.toml declares; \
             `cufflink config apply` brings the platform to it",
            "an answer, told without the place in the CLI that found it"
        );
        assert_eq!(exit_code(&drifted.wrap_err("while checking")), 2);
        assert!(told(&eyre::eyre!("Plan failed (409 Conflict)"))
            .starts_with("Error: Plan failed (409 Conflict)"));
        assert_eq!(exit_code(&eyre::eyre!("Plan failed (409 Conflict)")), 1);
        assert_eq!(exit_code(&eyre::eyre!("Failed to apply config (404)")), 1);
    }

    #[test]
    fn a_bundle_is_built_and_deployed_by_its_file() {
        let parsed = |arguments: &[&str]| {
            Cli::try_parse_from(["cufflink"].iter().chain(arguments)).map(|cli| cli.command)
        };
        assert!(matches!(
            parsed(&["build", "--bundle", "out/loans.bundle", "--commit-hash", "4f2a9c1"]),
            Ok(Commands::Build { bundle, commit_hash: Some(commit) })
                if bundle == std::path::Path::new("out/loans.bundle") && commit == "4f2a9c1"
        ));
        assert_eq!(
            parsed(&["build"]).err().map(|usage| usage.kind()),
            Some(clap::error::ErrorKind::MissingRequiredArgument),
            "a build writes a bundle or does nothing"
        );
        assert!(matches!(
            parsed(&["deploy", "--bundle", "loans.bundle", "--inactive", "--plan"]),
            Ok(Commands::Deploy { bundle: Some(bundle), inactive: true, plan: true, commit_hash: None, .. })
                if bundle == std::path::Path::new("loans.bundle")
        ));
        assert!(matches!(
            parsed(&["deploy"]),
            Ok(Commands::Deploy { bundle: None, .. })
        ));
    }

    #[test]
    fn a_rollback_is_to_a_version_or_a_commit_and_never_both() {
        let rollback = |arguments: &[&str]| {
            Cli::try_parse_from(["cufflink", "rollback"].iter().chain(arguments))
                .map(|cli| cli.command)
        };
        assert!(matches!(
            rollback(&[]),
            Ok(Commands::Rollback {
                version: None,
                commit: None,
                service: None,
                allow_destructive: false,
            })
        ));
        assert!(matches!(
            rollback(&["--commit", "4f2a9c1", "--service", "loans"]),
            Ok(Commands::Rollback { version: None, commit: Some(commit), service: Some(service), .. })
                if commit == "4f2a9c1" && service == "loans"
        ));
        assert!(matches!(
            rollback(&["-v", "3", "--allow-destructive"]),
            Ok(Commands::Rollback {
                version: Some(3),
                commit: None,
                allow_destructive: true,
                ..
            })
        ));
        assert!(rollback(&["--version", "3", "--commit", "4f2a9c1"]).is_err());
        assert!(
            rollback(&["--commit", "4f2a9c"])
                .err()
                .is_some_and(|usage| usage.to_string().contains(
                    "'4f2a9c' is too short to name a commit: give at least 7 characters of its \
                     hash"
                )),
            "refused before anything is built or asked"
        );
        assert!(rollback(&["--service", ""]).is_err());
    }

    #[test]
    fn a_config_is_applied_pruned_checked_and_to_a_named_service() {
        let apply = |arguments: &[&str]| {
            Cli::try_parse_from(["cufflink", "config", "apply"].iter().chain(arguments))
                .map(|cli| cli.command)
        };
        assert!(matches!(
            apply(&[]),
            Ok(Commands::Config {
                action: ConfigAction::Apply {
                    prune: false,
                    check: false,
                    service: None,
                }
            })
        ));
        assert!(matches!(
            apply(&["--prune", "--check", "--service", "loans"]),
            Ok(Commands::Config {
                action: ConfigAction::Apply { prune: true, check: true, service: Some(service) }
            }) if service == "loans"
        ));
        assert!(apply(&["--dry-run"]).is_err());
        assert!(apply(&["--service", ""]).is_err());
        assert!(apply(&["--service"]).is_err());
        assert!(matches!(
            Cli::try_parse_from(["cufflink", "config", "apply", "--check", "--env", "staging"])
                .map(|cli| (cli.env, cli.command)),
            Ok((Some(env), Commands::Config { action: ConfigAction::Apply { check: true, .. } }))
                if env == "staging"
        ));
        assert!(matches!(
            Cli::try_parse_from(["cufflink", "config", "sync"]).map(|cli| cli.command),
            Ok(Commands::Config {
                action: ConfigAction::Sync
            })
        ));
    }

    #[test]
    fn strict_filters_is_a_settable_key() {
        assert!(matches!(
            policy_action(&["set", "harbor", "strict_filters", "enforce"]).expect("parses"),
            PolicyAction::Set {
                key: PolicyKey::StrictFilters,
                mode: PolicyMode::Enforce,
                config: None,
                ..
            }
        ));
    }
}
