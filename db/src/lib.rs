use deadpool_postgres::{Hook, HookError, Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use eyre::{eyre, WrapErr};
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

pub const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_POOL_TIMEOUT: Duration = Duration::from_millis(5_000);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_LIFETIME: Duration = Duration::from_secs(1_800);

const TLS_ENV: &str = "DATABASE_TLS";
const TLS_VERIFY_ENV: &str = "DATABASE_TLS_VERIFY";
const TLS_CA_CERT_ENV: &str = "DATABASE_TLS_CA_CERT";
const POOL_TIMEOUT_ENV: &str = "DB_POOL_TIMEOUT_MS";
const STATEMENT_TIMEOUT_ENV: &str = "DB_STATEMENT_TIMEOUT_MS";
const RECYCLE_ENV: &str = "DB_POOL_RECYCLE";
const KEEPALIVE_IDLE_ENV: &str = "DB_TCP_KEEPALIVE_IDLE_SECS";
const KEEPALIVE_INTERVAL_ENV: &str = "DB_TCP_KEEPALIVE_INTERVAL_SECS";
const KEEPALIVE_RETRIES_ENV: &str = "DB_TCP_KEEPALIVE_RETRIES";
const USER_TIMEOUT_ENV: &str = "DB_TCP_USER_TIMEOUT_MS";
const MAX_LIFETIME_ENV: &str = "DB_CONN_MAX_LIFETIME_SECS";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recycling {
    Fast,
    Verified,
    Clean,
}

impl FromStr for Recycling {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "fast" => Ok(Recycling::Fast),
            "verified" => Ok(Recycling::Verified),
            "clean" => Ok(Recycling::Clean),
            other => Err(format!("'{other}' is not one of fast, verified, clean")),
        }
    }
}

impl From<Recycling> for RecyclingMethod {
    fn from(recycling: Recycling) -> Self {
        match recycling {
            Recycling::Fast => RecyclingMethod::Fast,
            Recycling::Verified => RecyclingMethod::Verified,
            Recycling::Clean => RecyclingMethod::Clean,
        }
    }
}

pub use cufflink_pgcodec::pooler::Pooler;
pub use cufflink_settings::{env_secs, env_var, parse_var, positive_var, Vars};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsMode {
    Off,
    Encrypt,
    VerifyCa { ca_pem: PathBuf },
    VerifyFull { ca_pem: Option<PathBuf> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Keepalive {
    pub idle: Duration,
    pub interval: Duration,
    pub retries: u32,
    pub user_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolSettings {
    pub max_size: usize,
    pub wait_timeout: Duration,
    pub connect_timeout: Duration,
    pub statement_timeout: Option<Duration>,
    pub recycling: Recycling,
    pub keepalive: Keepalive,
    pub max_lifetime: Option<Duration>,
    pub tls: TlsMode,
    pub pooler: Pooler,
}

impl PoolSettings {
    pub fn from_env(max_size: usize) -> eyre::Result<Self> {
        Self::from_vars(max_size, |name| std::env::var(name).ok())
    }

    pub fn from_vars(
        max_size: usize,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> eyre::Result<Self> {
        let vars = Vars(lookup);
        let wait_timeout =
            vars.positive(POOL_TIMEOUT_ENV, DEFAULT_POOL_TIMEOUT.as_millis() as u64)?;
        Ok(Self {
            max_size,
            wait_timeout: Duration::from_millis(wait_timeout),
            connect_timeout: CONNECT_TIMEOUT,
            statement_timeout: zero_is_none(
                vars.parsed(
                    STATEMENT_TIMEOUT_ENV,
                    DEFAULT_STATEMENT_TIMEOUT.as_millis() as u64,
                )?,
                Duration::from_millis,
            ),
            recycling: vars.parsed(RECYCLE_ENV, Recycling::Clean)?,
            keepalive: Keepalive {
                idle: Duration::from_secs(vars.parsed(KEEPALIVE_IDLE_ENV, 30)?),
                interval: Duration::from_secs(vars.parsed(KEEPALIVE_INTERVAL_ENV, 10)?),
                retries: vars.parsed(KEEPALIVE_RETRIES_ENV, 3)?,
                user_timeout: Duration::from_millis(vars.parsed(USER_TIMEOUT_ENV, 30_000)?),
            },
            max_lifetime: zero_is_none(
                vars.parsed(MAX_LIFETIME_ENV, DEFAULT_MAX_LIFETIME.as_secs())?,
                Duration::from_secs,
            ),
            tls: tls_mode(&vars)?,
            pooler: Pooler::Direct,
        })
    }

    pub fn statement_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.statement_timeout = timeout;
        self
    }

    pub fn max_size(mut self, max_size: usize) -> Self {
        self.max_size = max_size;
        self
    }

    pub fn pooler(mut self, pooler: Pooler) -> Self {
        self.pooler = pooler;
        self
    }

    fn recycling_method(&self) -> RecyclingMethod {
        match self.pooler {
            Pooler::Direct => self.recycling.into(),
            Pooler::PgBouncerTransaction => Recycling::Verified.into(),
        }
    }
}

fn zero_is_none(value: u64, to_duration: fn(u64) -> Duration) -> Option<Duration> {
    (value > 0).then(|| to_duration(value))
}

fn tls_mode(vars: &Vars<impl Fn(&str) -> Option<String>>) -> eyre::Result<TlsMode> {
    if !is_truthy(&vars.get(TLS_ENV).unwrap_or_default()) {
        return Ok(TlsMode::Off);
    }
    let ca_pem = vars.get(TLS_CA_CERT_ENV).map(PathBuf::from);
    let verify = vars.get(TLS_VERIFY_ENV).unwrap_or_else(|| "off".into());
    match verify.trim().to_ascii_lowercase().as_str() {
        "off" => Ok(TlsMode::Encrypt),
        "ca" => ca_pem
            .map(|ca_pem| TlsMode::VerifyCa { ca_pem })
            .ok_or_else(|| {
                eyre!(
                    "{TLS_VERIFY_ENV}=ca requires {TLS_CA_CERT_ENV} to point at the CA certificate"
                )
            }),
        "full" => Ok(TlsMode::VerifyFull { ca_pem }),
        other => Err(eyre!(
            "{TLS_VERIFY_ENV} has an invalid value {other:?}: expected off, ca or full"
        )),
    }
}

fn is_truthy(value: &str) -> bool {
    let v = value.trim();
    v.eq_ignore_ascii_case("true")
        || v.eq_ignore_ascii_case("yes")
        || v.eq_ignore_ascii_case("on")
        || v == "1"
}

pub fn parse_config(database_url: &str) -> eyre::Result<tokio_postgres::Config> {
    database_url
        .parse()
        .wrap_err("parse Postgres connection URL")
}

pub fn with_dbname(base: &tokio_postgres::Config, dbname: &str) -> tokio_postgres::Config {
    let mut config = base.clone();
    config.dbname(dbname);
    config
}

pub fn build_pool(database_url: &str, max_size: usize) -> eyre::Result<Pool> {
    build_pool_with(
        parse_config(database_url)?,
        PoolSettings::from_env(max_size)?,
    )
}

pub fn build_pool_with(
    config: tokio_postgres::Config,
    settings: PoolSettings,
) -> eyre::Result<Pool> {
    let config = apply_connection_settings(config, &settings);
    let manager_config = ManagerConfig {
        recycling_method: settings.recycling_method(),
    };
    let manager = match tls_connector(&settings.tls)? {
        Some(tls) => Manager::from_config(config, tls, manager_config),
        None => Manager::from_config(config, tokio_postgres::NoTls, manager_config),
    };
    let mut builder = Pool::builder(manager)
        .max_size(settings.max_size)
        .runtime(Runtime::Tokio1)
        .wait_timeout(Some(settings.wait_timeout))
        .create_timeout(Some(settings.connect_timeout))
        .recycle_timeout(Some(settings.connect_timeout));
    if let Some(max_lifetime) = settings.max_lifetime {
        builder = builder.pre_recycle(Hook::sync_fn(move |_, metrics| {
            if lifetime_exceeded(metrics.created, Instant::now(), max_lifetime) {
                Err(HookError::message("max lifetime exceeded"))
            } else {
                Ok(())
            }
        }));
    }
    builder.build().map_err(Into::into)
}

fn lifetime_exceeded(created: Instant, now: Instant, max_lifetime: Duration) -> bool {
    now.saturating_duration_since(created) > max_lifetime
}

fn apply_connection_settings(
    mut config: tokio_postgres::Config,
    settings: &PoolSettings,
) -> tokio_postgres::Config {
    if let (Some(timeout), Pooler::Direct) = (settings.statement_timeout, settings.pooler) {
        config.options(options_with_statement_timeout(
            config.get_options(),
            timeout,
        ));
    }
    config
        .connect_timeout(settings.connect_timeout)
        .keepalives(true)
        .keepalives_idle(settings.keepalive.idle)
        .keepalives_interval(settings.keepalive.interval)
        .keepalives_retries(settings.keepalive.retries)
        .tcp_user_timeout(settings.keepalive.user_timeout);
    config
}

fn options_with_statement_timeout(existing: Option<&str>, timeout: Duration) -> String {
    let directive = format!("-c statement_timeout={}", timeout.as_millis());
    match existing.map(str::trim).filter(|s| !s.is_empty()) {
        Some(existing) => format!("{existing} {directive}"),
        None => directive,
    }
}

fn tls_connector(mode: &TlsMode) -> eyre::Result<Option<MakeTlsConnector>> {
    let mut builder = TlsConnector::builder();
    match mode {
        TlsMode::Off => return Ok(None),
        TlsMode::Encrypt => {
            builder.danger_accept_invalid_certs(true);
        }
        TlsMode::VerifyCa { ca_pem } => {
            builder
                .add_root_certificate(read_certificate(ca_pem)?)
                .danger_accept_invalid_hostnames(true);
        }
        TlsMode::VerifyFull { ca_pem } => {
            if let Some(ca_pem) = ca_pem {
                builder.add_root_certificate(read_certificate(ca_pem)?);
            }
        }
    }
    let connector = builder.build().wrap_err("build native-tls connector")?;
    Ok(Some(MakeTlsConnector::new(connector)))
}

fn read_certificate(path: &PathBuf) -> eyre::Result<Certificate> {
    let pem = std::fs::read(path)
        .wrap_err_with(|| format!("read {TLS_CA_CERT_ENV} {}", path.display()))?;
    Certificate::from_pem(&pem)
        .wrap_err_with(|| format!("parse {TLS_CA_CERT_ENV} {}", path.display()))
}

pub async fn connect_single(
    config: tokio_postgres::Config,
    settings: &PoolSettings,
) -> eyre::Result<tokio_postgres::Client> {
    open(config, settings, None).await
}

pub struct Listener {
    _client: tokio_postgres::Client,
    notified: tokio::sync::mpsc::Receiver<()>,
}

impl Listener {
    pub async fn notified(&mut self) -> bool {
        self.notified.recv().await.is_some()
    }
}

pub async fn listen(
    config: tokio_postgres::Config,
    settings: &PoolSettings,
    channel: &str,
) -> eyre::Result<Listener> {
    let (wake, notified) = tokio::sync::mpsc::channel(1);
    let client = open(config, settings, Some(wake)).await?;
    client
        .batch_execute(&format!("LISTEN {}", quote_ident(channel)))
        .await?;
    Ok(Listener {
        _client: client,
        notified,
    })
}

async fn open(
    config: tokio_postgres::Config,
    settings: &PoolSettings,
    wake: Option<tokio::sync::mpsc::Sender<()>>,
) -> eyre::Result<tokio_postgres::Client> {
    let config = apply_connection_settings(config, settings);
    let client = match tls_connector(&settings.tls)? {
        Some(tls) => {
            let (client, connection) = config.connect(tls).await?;
            tokio::spawn(run_connection(connection, wake));
            client
        }
        None => {
            let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
            tokio::spawn(run_connection(connection, wake));
            client
        }
    };
    Ok(client)
}

pub async fn cancel_statement(
    token: &tokio_postgres::CancelToken,
    tls: &TlsMode,
) -> eyre::Result<()> {
    match tls_connector(tls)? {
        Some(tls) => token.cancel_query(tls).await,
        None => token.cancel_query(tokio_postgres::NoTls).await,
    }
    .wrap_err("ask the server to stop a statement")
}

async fn run_connection<S, T>(
    connection: tokio_postgres::Connection<S, T>,
    wake: Option<tokio::sync::mpsc::Sender<()>>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let ended = match wake {
        Some(wake) => forward_notifications(connection, wake).await,
        None => connection.await,
    };
    if let Err(e) = ended {
        tracing::warn!(error = %e, "Postgres connection ended with an error");
    }
}

async fn forward_notifications<S, T>(
    mut connection: tokio_postgres::Connection<S, T>,
    wake: tokio::sync::mpsc::Sender<()>,
) -> Result<(), tokio_postgres::Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    while let Some(message) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
        if let tokio_postgres::AsyncMessage::Notification(_) = message? {
            let _ = wake.try_send(());
        }
    }
    Ok(())
}

pub use cufflink_pgcodec::session::quote_ident;

pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn pool_status_gauges(name: &str, pool: &Pool) {
    record_status_gauges(name, &pool.status());
}

pub fn record_status_gauges(name: &str, status: &deadpool_postgres::Status) {
    for (metric, value) in [
        ("cufflink_db_pool_max", status.max_size),
        ("cufflink_db_pool_size", status.size),
        ("cufflink_db_pool_available", status.available),
        ("cufflink_db_pool_waiting", status.waiting),
    ] {
        metrics::gauge!(metric, "pool" => name.to_string()).set(value as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_quoted_with_their_quotes_doubled() {
        assert_eq!(quote_ident("loan_service"), "\"loan_service\"");
        assert_eq!(quote_ident("odd\"name"), "\"odd\"\"name\"");
        assert_eq!(quote_ident(""), "\"\"");
    }

    #[test]
    fn literals_are_quoted_with_their_quotes_doubled() {
        assert_eq!(quote_literal("cufflink:t:s"), "'cufflink:t:s'");
        assert_eq!(quote_literal("it's"), "'it''s'");
        assert_eq!(quote_literal("back\\slash"), "'back\\slash'");
    }
    use std::collections::HashMap;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn settings_from(pairs: &[(&str, &str)]) -> eyre::Result<PoolSettings> {
        let vars = vars(pairs);
        PoolSettings::from_vars(4, |name| vars.get(name).cloned())
    }

    #[test]
    fn truthy_recognizes_common_yes_values() {
        for s in [
            "true", "True", "TRUE", "yes", "YES", "on", "ON", "1", " true ", "\ttrue\n",
        ] {
            assert!(is_truthy(s), "expected {s:?} to be truthy");
        }
    }

    #[test]
    fn truthy_rejects_falsy_and_unknown_values() {
        for s in [
            "", "false", "False", "no", "off", "0", "2", "yep", "truex", " ",
        ] {
            assert!(!is_truthy(s), "expected {s:?} to be falsy");
        }
    }

    #[test]
    fn settings_defaults_when_nothing_is_set() {
        let settings = settings_from(&[]).expect("defaults");
        assert_eq!(
            settings,
            PoolSettings {
                max_size: 4,
                wait_timeout: Duration::from_millis(5_000),
                connect_timeout: CONNECT_TIMEOUT,
                statement_timeout: Some(DEFAULT_STATEMENT_TIMEOUT),
                recycling: Recycling::Clean,
                keepalive: Keepalive {
                    idle: Duration::from_secs(30),
                    interval: Duration::from_secs(10),
                    retries: 3,
                    user_timeout: Duration::from_millis(30_000),
                },
                max_lifetime: Some(Duration::from_secs(1_800)),
                tls: TlsMode::Off,
                pooler: Pooler::Direct,
            }
        );
    }

    #[test]
    fn a_transaction_pooler_gets_no_startup_options_and_verified_recycling() {
        let direct = settings_from(&[]).unwrap();
        let pooled = direct.clone().pooler(Pooler::PgBouncerTransaction);
        let url = || parse_config("postgres://u:p@localhost/db").unwrap();
        assert_eq!(
            apply_connection_settings(url(), &pooled).get_options(),
            None
        );
        assert!(apply_connection_settings(url(), &direct)
            .get_options()
            .is_some_and(|o| o.contains("statement_timeout")));
        assert!(matches!(
            pooled.recycling_method(),
            RecyclingMethod::Verified
        ));
        assert!(matches!(direct.recycling_method(), RecyclingMethod::Clean));
    }

    #[test]
    fn zero_disables_statement_timeout_and_max_lifetime() {
        let settings = settings_from(&[
            (STATEMENT_TIMEOUT_ENV, "0"),
            (MAX_LIFETIME_ENV, "0"),
            (POOL_TIMEOUT_ENV, " 1500 "),
        ])
        .expect("parses");
        assert_eq!(settings.statement_timeout, None);
        assert_eq!(settings.max_lifetime, None);
        assert_eq!(settings.wait_timeout, Duration::from_millis(1_500));
    }

    #[test]
    fn every_parse_failure_names_its_variable() {
        for (name, bad) in [
            (POOL_TIMEOUT_ENV, "soon"),
            (POOL_TIMEOUT_ENV, "0"),
            (STATEMENT_TIMEOUT_ENV, "30s"),
            (RECYCLE_ENV, "sometimes"),
            (KEEPALIVE_IDLE_ENV, "x"),
            (KEEPALIVE_INTERVAL_ENV, "-1"),
            (KEEPALIVE_RETRIES_ENV, "three"),
            (USER_TIMEOUT_ENV, "1.5"),
            (MAX_LIFETIME_ENV, "1h"),
        ] {
            let err = settings_from(&[(name, bad)]).expect_err(name);
            assert!(err.to_string().contains(name), "{name}: {err}");
        }
        let err =
            settings_from(&[(TLS_ENV, "true"), (TLS_VERIFY_ENV, "maybe")]).expect_err("verify");
        assert!(err.to_string().contains(TLS_VERIFY_ENV), "{err}");
        let err = settings_from(&[(TLS_ENV, "true"), (TLS_VERIFY_ENV, "ca")]).expect_err("ca cert");
        assert!(err.to_string().contains(TLS_CA_CERT_ENV), "{err}");
    }

    #[test]
    fn recycling_and_tls_modes_parse() {
        assert_eq!("Fast".parse::<Recycling>().unwrap(), Recycling::Fast);
        assert_eq!(
            " verified ".parse::<Recycling>().unwrap(),
            Recycling::Verified
        );
        assert_eq!("clean".parse::<Recycling>().unwrap(), Recycling::Clean);
        assert!("lazy".parse::<Recycling>().is_err());
        assert_eq!(
            settings_from(&[(RECYCLE_ENV, "fast")]).unwrap().recycling,
            Recycling::Fast
        );

        assert_eq!(
            settings_from(&[(TLS_ENV, "false")]).unwrap().tls,
            TlsMode::Off
        );
        assert_eq!(
            settings_from(&[
                (TLS_ENV, "true"),
                (TLS_VERIFY_ENV, "junk"),
                (TLS_ENV, "false")
            ])
            .unwrap()
            .tls,
            TlsMode::Off,
            "verify mode is ignored while TLS is off"
        );
        assert_eq!(
            settings_from(&[(TLS_ENV, "true")]).unwrap().tls,
            TlsMode::Encrypt
        );
        assert_eq!(
            settings_from(&[
                (TLS_ENV, "1"),
                (TLS_VERIFY_ENV, "ca"),
                (TLS_CA_CERT_ENV, "/etc/ssl/ca.pem")
            ])
            .unwrap()
            .tls,
            TlsMode::VerifyCa {
                ca_pem: PathBuf::from("/etc/ssl/ca.pem")
            }
        );
        assert_eq!(
            settings_from(&[(TLS_ENV, "yes"), (TLS_VERIFY_ENV, "full")])
                .unwrap()
                .tls,
            TlsMode::VerifyFull { ca_pem: None }
        );
    }

    #[test]
    fn statement_timeout_is_appended_to_existing_options() {
        assert_eq!(
            options_with_statement_timeout(None, Duration::from_secs(30)),
            "-c statement_timeout=30000"
        );
        assert_eq!(
            options_with_statement_timeout(Some("-c search_path=app"), Duration::from_millis(250)),
            "-c search_path=app -c statement_timeout=250"
        );
        assert_eq!(
            options_with_statement_timeout(Some("  "), Duration::from_secs(1)),
            "-c statement_timeout=1000"
        );
    }

    #[test]
    fn connection_settings_land_on_the_config() {
        let base = parse_config("postgres://u:p@localhost:5432/db?options=-c%20search_path%3Dapp")
            .expect("url parses");
        let settings = settings_from(&[]).unwrap();
        let config = apply_connection_settings(base, &settings);
        assert_eq!(
            config.get_options(),
            Some("-c search_path=app -c statement_timeout=30000")
        );
        assert_eq!(config.get_connect_timeout(), Some(&CONNECT_TIMEOUT));
        assert!(config.get_keepalives());
        assert_eq!(config.get_keepalives_idle(), Duration::from_secs(30));
        assert_eq!(
            config.get_keepalives_interval(),
            Some(Duration::from_secs(10))
        );
        assert_eq!(config.get_keepalives_retries(), Some(3));
        assert_eq!(
            config.get_tcp_user_timeout(),
            Some(&Duration::from_millis(30_000))
        );

        let none = apply_connection_settings(
            parse_config("postgres://u:p@localhost/db").unwrap(),
            &settings.statement_timeout(None),
        );
        assert_eq!(none.get_options(), None);
    }

    #[test]
    fn with_dbname_preserves_everything_but_the_database() {
        let base = parse_config("postgres://app:s3cret@db.internal:6432/platform?sslmode=require")
            .expect("url parses");
        let tenant = with_dbname(&base, "cufflink_acme");
        assert_eq!(tenant.get_dbname(), Some("cufflink_acme"));
        assert_eq!(tenant.get_user(), Some("app"));
        assert_eq!(tenant.get_password(), Some(b"s3cret".as_slice()));
        assert_eq!(tenant.get_ports(), &[6432]);
        assert_eq!(tenant.get_ssl_mode(), base.get_ssl_mode());
        assert!(matches!(
            tenant.get_ssl_mode(),
            tokio_postgres::config::SslMode::Require
        ));
        assert_eq!(base.get_dbname(), Some("platform"));
    }

    #[test]
    fn max_lifetime_decision_is_strictly_after_the_lifetime() {
        let created = Instant::now();
        let lifetime = Duration::from_secs(60);
        assert!(!lifetime_exceeded(created, created, lifetime));
        assert!(!lifetime_exceeded(created, created + lifetime, lifetime));
        assert!(lifetime_exceeded(
            created,
            created + lifetime + Duration::from_millis(1),
            lifetime
        ));
    }

    #[test]
    fn build_pool_constructs_for_a_valid_url() {
        let pool = build_pool("postgres://user:pass@localhost:5432/db", 4)
            .expect("pool builder should succeed for a valid URL");
        assert_eq!(pool.status().max_size, 4);
        let timeouts = pool.timeouts();
        assert!(timeouts.wait.is_some());
        assert_eq!(timeouts.create, Some(CONNECT_TIMEOUT));
        assert_eq!(timeouts.recycle, Some(CONNECT_TIMEOUT));
    }

    #[test]
    fn build_pool_rejects_a_malformed_url() {
        assert!(build_pool("not-a-url", 4).is_err());
    }

    #[test]
    fn tls_modes_build_connectors_and_missing_ca_files_are_named() {
        assert!(tls_connector(&TlsMode::Off).unwrap().is_none());
        assert!(tls_connector(&TlsMode::Encrypt).unwrap().is_some());
        assert!(tls_connector(&TlsMode::VerifyFull { ca_pem: None })
            .unwrap()
            .is_some());
        let missing = PathBuf::from("/nonexistent/cufflink-ca.pem");
        let err = tls_connector(&TlsMode::VerifyCa { ca_pem: missing })
            .err()
            .expect("missing CA file must error");
        assert!(err.to_string().contains("cufflink-ca.pem"), "{err}");
    }
}
