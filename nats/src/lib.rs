//! NATS connections shared by the platform, worker and web-runner: the
//! `NATS_URL` server list, the component's credentials and the connect
//! options every component uses.

use async_nats::{Client, ConnectOptions, ServerAddr};
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(2);
const PING_INTERVAL: Duration = Duration::from_secs(10);

/// The options every component connects with: its credentials and name, a
/// first connect that happens in the background (so a NATS outage never
/// stops a process from starting), pings that notice a dead server in
/// ~20s, and reconnects that never give up.
pub async fn connect_options(client_name: &str, auth: &NatsAuth) -> eyre::Result<ConnectOptions> {
    Ok(auth
        .apply(ConnectOptions::new())
        .await?
        .name(client_name)
        .retry_on_initial_connect()
        .connection_timeout(CONNECTION_TIMEOUT)
        .ping_interval(PING_INTERVAL)
        .max_reconnects(None::<usize>))
}

/// A client for `raw_urls` with [`connect_options`]. Only a malformed
/// `NATS_URL` (or unreadable credentials) is an error; an unreachable server
/// is not.
pub async fn connect(raw_urls: &str, client_name: &str, auth: &NatsAuth) -> eyre::Result<Client> {
    let servers = parse_server_list(raw_urls)?;
    Ok(connect_options(client_name, auth)
        .await?
        .connect(servers)
        .await?)
}

/// Parse `NATS_URL`: a single server or a comma-separated list.
pub fn parse_server_list(raw: &str) -> eyre::Result<Vec<ServerAddr>> {
    let servers = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<ServerAddr>()
                .map_err(|e| eyre::eyre!("invalid NATS server URL '{}': {}", redact_urls(s), e))
        })
        .collect::<eyre::Result<Vec<_>>>()?;
    if servers.is_empty() {
        eyre::bail!("NATS_URL lists no servers");
    }
    Ok(servers)
}

/// `NATS_URL` with any `user:password@` credentials replaced, safe to log.
pub fn redact_urls(raw: &str) -> String {
    raw.split(',')
        .map(|entry| {
            let trimmed = entry.trim();
            let (scheme, rest) = match trimmed.find("://") {
                Some(i) => trimmed.split_at(i + 3),
                None => ("", trimmed),
            };
            let authority_end = rest.find('/').unwrap_or(rest.len());
            match rest[..authority_end].rfind('@') {
                Some(at) => format!("{}***@{}", scheme, &rest[at + 1..]),
                None => trimmed.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// How a component authenticates to NATS: `NATS_CREDS_FILE` (a `.creds`
/// file), `NATS_NKEY_SEED`, or `NATS_USER` with `NATS_PASSWORD`; at most one.
/// Without any the connection is anonymous, as before.
#[derive(Clone, Default, PartialEq, Eq)]
pub enum NatsAuth {
    #[default]
    Anonymous,
    CredentialsFile(PathBuf),
    NKeySeed(String),
    UserPassword {
        user: String,
        password: String,
    },
}

impl fmt::Debug for NatsAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NatsAuth::Anonymous => f.write_str("Anonymous"),
            NatsAuth::CredentialsFile(path) => {
                f.debug_tuple("CredentialsFile").field(path).finish()
            }
            NatsAuth::NKeySeed(_) => f.write_str("NKeySeed(***)"),
            NatsAuth::UserPassword { user, .. } => f
                .debug_struct("UserPassword")
                .field("user", user)
                .field("password", &"***")
                .finish(),
        }
    }
}

impl NatsAuth {
    pub fn from_env() -> eyre::Result<Self> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    /// The credentials the variables `vars` give; an empty one counts as
    /// unset.
    pub fn from_vars(vars: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let lookup = |name: &str| vars(name).filter(|value| !value.is_empty());
        let creds = lookup("NATS_CREDS_FILE").map(|path| NatsAuth::CredentialsFile(path.into()));
        let nkey = lookup("NATS_NKEY_SEED").map(NatsAuth::NKeySeed);
        let user = match (lookup("NATS_USER"), lookup("NATS_PASSWORD")) {
            (Some(user), Some(password)) => Some(NatsAuth::UserPassword { user, password }),
            (None, None) => None,
            _ => eyre::bail!("NATS_USER and NATS_PASSWORD must be set together"),
        };
        let mut configured = [creds, nkey, user].into_iter().flatten();
        match (configured.next(), configured.next()) {
            (None, _) => Ok(NatsAuth::Anonymous),
            (Some(auth), None) => Ok(auth),
            (Some(_), Some(_)) => eyre::bail!(
                "set only one of NATS_CREDS_FILE, NATS_NKEY_SEED or NATS_USER/NATS_PASSWORD"
            ),
        }
    }

    /// `options` carrying these credentials.
    pub async fn apply(&self, options: ConnectOptions) -> eyre::Result<ConnectOptions> {
        Ok(match self {
            NatsAuth::Anonymous => options,
            NatsAuth::CredentialsFile(path) => {
                options.credentials_file(path).await.map_err(|e| {
                    eyre::eyre!("cannot read NATS credentials file {}: {e}", path.display())
                })?
            }
            NatsAuth::NKeySeed(seed) => options.nkey(seed.clone()),
            NatsAuth::UserPassword { user, password } => {
                options.user_and_password(user.clone(), password.clone())
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn single_server_url_parses() {
        assert_eq!(parse_server_list("nats://nats:4222").unwrap().len(), 1);
    }

    #[test]
    fn comma_separated_servers_parse_with_whitespace_and_blanks() {
        assert_eq!(
            parse_server_list(" nats://a:4222 , ,nats://b:4222,")
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn empty_server_list_is_an_error() {
        assert!(parse_server_list("").is_err());
        assert!(parse_server_list(" , ,").is_err());
    }

    #[test]
    fn redacts_credentials_only() {
        assert_eq!(redact_urls("nats://nats:4222"), "nats://nats:4222");
        assert_eq!(
            redact_urls("nats://user:s3cret@nats-0:4222, tls://tok@nats-1:4222"),
            "nats://***@nats-0:4222,tls://***@nats-1:4222"
        );
        assert_eq!(redact_urls("user:pw@host:4222"), "***@host:4222");
    }

    #[test]
    fn an_empty_credential_variable_is_unset() {
        assert_eq!(
            NatsAuth::from_vars(vars(&[
                ("NATS_CREDS_FILE", ""),
                ("NATS_NKEY_SEED", ""),
                ("NATS_USER", ""),
                ("NATS_PASSWORD", ""),
            ]))
            .unwrap(),
            NatsAuth::Anonymous
        );
        assert_eq!(
            NatsAuth::from_vars(vars(&[("NATS_NKEY_SEED", "SUAB"), ("NATS_USER", "")])).unwrap(),
            NatsAuth::NKeySeed("SUAB".into())
        );
        assert!(NatsAuth::from_vars(vars(&[("NATS_USER", "app"), ("NATS_PASSWORD", "")])).is_err());
    }

    #[test]
    fn auth_is_anonymous_unless_configured() {
        assert_eq!(NatsAuth::from_vars(vars(&[])).unwrap(), NatsAuth::Anonymous);
        assert_eq!(
            NatsAuth::from_vars(vars(&[("NATS_CREDS_FILE", "/etc/nats/platform.creds")])).unwrap(),
            NatsAuth::CredentialsFile("/etc/nats/platform.creds".into())
        );
        assert_eq!(
            NatsAuth::from_vars(vars(&[("NATS_USER", "platform"), ("NATS_PASSWORD", "pw")]))
                .unwrap(),
            NatsAuth::UserPassword {
                user: "platform".into(),
                password: "pw".into()
            }
        );
    }

    #[test]
    fn auth_rejects_half_or_several_methods() {
        assert!(NatsAuth::from_vars(vars(&[("NATS_USER", "platform")])).is_err());
        assert!(NatsAuth::from_vars(vars(&[
            ("NATS_NKEY_SEED", "SUA"),
            ("NATS_CREDS_FILE", "/x.creds")
        ]))
        .is_err());
    }

    #[test]
    fn debug_never_prints_secrets() {
        let rendered = format!(
            "{:?} {:?}",
            NatsAuth::NKeySeed("SUASECRET".into()),
            NatsAuth::UserPassword {
                user: "u".into(),
                password: "hunter2".into()
            }
        );
        assert!(
            !rendered.contains("SUASECRET") && !rendered.contains("hunter2"),
            "{rendered}"
        );
    }

    #[tokio::test]
    async fn missing_credentials_file_is_an_error() {
        let err = NatsAuth::CredentialsFile("/nonexistent/cufflink.creds".into())
            .apply(ConnectOptions::new())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("/nonexistent/cufflink.creds"),
            "{err}"
        );
    }
}
