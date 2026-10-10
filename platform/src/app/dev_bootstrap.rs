use super::Permanence;
use crate::auth::runner_keys;
use crate::config::RunEnv;
use crate::repo::api_keys::{self, KeyLookup};
use crate::repo::runner_services::Runs;
use crate::repo::tenants::{self, NewTenant, Tenant};
use crate::AppState;
use eyre::{bail, eyre, Result, WrapErr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tracing::info;

pub const VARIABLE: &str = "CUFFLINK_DEV_BOOTSTRAP";
const KEY_NAME: &str = "cufflink-dev";
const RUNNER_KEY_NAME: &str = "cufflink-dev-runner";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevBootstrap {
    pub tenant: String,
    pub realm: String,
    pub key_file: PathBuf,
    pub runner_key_file: Option<PathBuf>,
}

impl FromStr for DevBootstrap {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let settings = raw
            .split(',')
            .map(str::trim)
            .filter(|setting| !setting.is_empty())
            .map(|setting| {
                setting
                    .split_once('=')
                    .map(|(key, value)| (key.trim(), value.trim()))
                    .filter(|(_, value)| !value.is_empty())
                    .ok_or_else(|| format!("'{setting}' is not key=value"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if let Some((unknown, _)) = settings
            .iter()
            .find(|(key, _)| !matches!(*key, "tenant" | "realm" | "key_file" | "runner_key_file"))
        {
            return Err(format!(
                "'{unknown}' is not one of tenant, realm, key_file, runner_key_file"
            ));
        }
        let optional = |name: &str| match settings
            .iter()
            .filter(|(key, _)| *key == name)
            .collect::<Vec<_>>()
            .as_slice()
        {
            [(_, value)] => Ok(Some(*value)),
            [] => Ok(None),
            _ => Err(format!("{name} is given more than once")),
        };
        let setting = |name: &str| optional(name)?.ok_or_else(|| format!("{name} is missing"));
        let absolute = |name: &str, value: &str| {
            let file = PathBuf::from(value);
            match file.is_absolute() {
                true => Ok(file),
                false => Err(format!("{name} '{value}' is not an absolute path")),
            }
        };
        let tenant = setting("tenant")?;
        if !cufflink_types::slug::is_valid_tenant_slug(tenant) {
            return Err(format!("tenant '{tenant}' is not a tenant slug"));
        }
        let realm = setting("realm")?;
        let key_file = absolute("key_file", setting("key_file")?)?;
        let runner_key_file = optional("runner_key_file")?
            .map(|value| absolute("runner_key_file", value))
            .transpose()?;
        Ok(Self {
            tenant: tenant.to_string(),
            realm: realm.to_string(),
            key_file,
            runner_key_file,
        })
    }
}

impl DevBootstrap {
    pub fn from_setting(declared: Option<RunEnv>, raw: Option<&str>) -> Result<Option<Self>> {
        match (raw.map(str::trim).filter(|raw| !raw.is_empty()), declared) {
            (None, _) => Ok(None),
            (Some(raw), Some(RunEnv::Development)) => raw
                .parse()
                .map(Some)
                .map_err(|reason| eyre!("{VARIABLE}: {reason}")),
            (Some(_), declared) => bail!(
                "{VARIABLE} is refused unless RUN_ENV=development (it is {}): it creates a \
                 tenant and writes its API key to a file",
                declared.map_or("unset", RunEnv::name)
            ),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Failure {
    #[error("{VARIABLE}: {0}")]
    Refused(String),
    #[error(transparent)]
    Failed(#[from] eyre::Report),
}

impl Permanence for Failure {
    fn is_permanent(&self) -> bool {
        match self {
            Self::Refused(_) => true,
            Self::Failed(error) => error.is_permanent(),
        }
    }
}

impl From<crate::errors::PlatformError> for Failure {
    fn from(error: crate::errors::PlatformError) -> Self {
        Self::Failed(error.into())
    }
}

impl From<deadpool_postgres::PoolError> for Failure {
    fn from(error: deadpool_postgres::PoolError) -> Self {
        Self::Failed(error.into())
    }
}

fn key_file_refused(key_file: &Path, doing: &str, error: &std::io::Error) -> Failure {
    Failure::Refused(format!("{doing} {}: {error}", key_file.display()))
}

fn kept(found: &KeyLookup, tenant: &Tenant) -> bool {
    matches!(found, KeyLookup::Active(key) if key.tenant_id == Some(tenant.id))
}

async fn tenant(state: &AppState, dev: &DevBootstrap) -> Result<Tenant, Failure> {
    let existing = || tenants::get_by_slug(&state.platform_db, &dev.tenant);
    let tenant = match existing().await? {
        Some(tenant) => tenant,
        None => {
            let created = tenants::create_tenant(
                &state.platform_db.get().await?,
                &NewTenant {
                    name: &dev.tenant,
                    slug: &dev.tenant,
                    keycloak_url: Some(state.config.keycloak_url.as_str())
                        .filter(|url| !url.is_empty()),
                    keycloak_realm: Some(&dev.realm),
                    deploy_role: None,
                    keycloak_ops_client_id: None,
                    keycloak_ops_client_secret: None,
                    service_account_clients: &[],
                    keycloak_issuer: None,
                    source_tenant_id: None,
                    database_name: None,
                },
            )
            .await;
            match created {
                Ok(tenant) => {
                    info!(tenant = %tenant.slug, realm = %dev.realm, "Created the development tenant");
                    tenant
                }
                Err(crate::errors::PlatformError::Conflict(_)) => existing()
                    .await?
                    .ok_or_else(|| eyre!("tenant '{}' is taken and cannot be read", dev.tenant))?,
                Err(e) => return Err(e.into()),
            }
        }
    };
    if tenant.keycloak_realm.as_deref() != Some(dev.realm.as_str()) {
        return Err(Failure::Refused(format!(
            "tenant '{}' exists with realm {:?}, not '{}'",
            tenant.slug, tenant.keycloak_realm, dev.realm
        )));
    }
    if tenant.database_name.is_none() {
        state
            .tenant_sessions
            .create_database(&state.platform_db, &tenant)
            .await
            .wrap_err("creating the development tenant's database")?;
    }
    state.tenants.refresh().await;
    Ok(tenant)
}

async fn held(key_file: &Path) -> Result<Option<String>, Failure> {
    match tokio::fs::read_to_string(key_file).await {
        Ok(key) => Ok(Some(key.trim().to_string()).filter(|key| !key.is_empty())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(key_file_refused(key_file, "reading", &e)),
    }
}

async fn write(key_file: &Path, key: &str) -> Result<(), Failure> {
    let partial = key_file.with_extension("partial");
    let written = async {
        if let Some(directory) = key_file.parent() {
            tokio::fs::create_dir_all(directory).await?;
        }
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&partial).await?;
        tokio::io::AsyncWriteExt::write_all(&mut file, format!("{key}\n").as_bytes()).await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&partial, key_file).await
    };
    written
        .await
        .map_err(|e| key_file_refused(key_file, "writing", &e))
}

pub async fn run(state: &AppState, dev: &DevBootstrap) -> Result<(), Failure> {
    let tenant = tenant(state, dev).await?;
    api_key(state, &tenant, &dev.key_file).await?;
    match &dev.runner_key_file {
        Some(file) => runner_key(state, &tenant, file).await,
        None => Ok(()),
    }
}

async fn api_key(state: &AppState, tenant: &Tenant, key_file: &Path) -> Result<(), Failure> {
    if let Some(key) = held(key_file).await? {
        let found = api_keys::find(&state.platform_db, &api_keys::hash(&key)).await?;
        if kept(&found, tenant) {
            info!(tenant = %tenant.slug, "The development tenant's API key is in place");
            return Ok(());
        }
    }
    let minted = api_keys::create_tenant_key(
        &state.platform_db,
        tenant.id,
        KEY_NAME,
        None,
        state.config.api_key_ttl,
    )
    .await?;
    write(key_file, &minted.raw).await?;
    info!(
        tenant = %tenant.slug,
        key_id = %minted.id,
        key_file = %key_file.display(),
        "Wrote the development tenant's API key"
    );
    Ok(())
}

async fn runner_key(state: &AppState, tenant: &Tenant, key_file: &Path) -> Result<(), Failure> {
    if let Some(key) = held(key_file).await? {
        let found = runner_keys::authenticate(&state.platform_db, &key).await?;
        if found.is_some_and(|runner| runner.tenant_id == tenant.id && runner.runs == Runs::Native)
        {
            info!(tenant = %tenant.slug, "The development tenant's runner key is in place");
            return Ok(());
        }
    }
    let (minted, key) = runner_keys::mint(
        &state.platform_db,
        tenant.id,
        RUNNER_KEY_NAME,
        Runs::Native,
        VARIABLE,
    )
    .await?;
    write(key_file, &key).await?;
    info!(
        tenant = %tenant.slug,
        key_id = %minted.id,
        key_file = %key_file.display(),
        "Wrote the development tenant's runner key"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::api_keys::ApiKeyAuth;
    use crate::tenant_catalog::tests::tenant as a_tenant;
    use uuid::Uuid;

    fn bootstrap(raw: &str) -> Result<DevBootstrap, String> {
        raw.parse()
    }

    #[test]
    fn a_bootstrap_names_its_tenant_realm_and_key_file() {
        let expected = DevBootstrap {
            tenant: "dev".into(),
            realm: "cufflink-dev".into(),
            key_file: "/run/cufflink/dev-key".into(),
            runner_key_file: None,
        };
        for raw in [
            "tenant=dev,realm=cufflink-dev,key_file=/run/cufflink/dev-key",
            " key_file = /run/cufflink/dev-key , realm=cufflink-dev, tenant=dev ,",
        ] {
            assert_eq!(bootstrap(raw), Ok(expected.clone()), "{raw}");
        }
        assert_eq!(
            bootstrap(
                "tenant=dev,realm=cufflink-dev,key_file=/run/cufflink/dev-key,\
                 runner_key_file=/run/cufflink/runner-key"
            ),
            Ok(DevBootstrap {
                runner_key_file: Some("/run/cufflink/runner-key".into()),
                ..expected
            })
        );
    }

    #[test]
    fn a_bootstrap_that_is_not_one_says_why() {
        for (raw, reason) in [
            ("tenant=dev,realm=r", "key_file is missing"),
            ("realm=r,key_file=/k", "tenant is missing"),
            ("tenant=dev,key_file=/k", "realm is missing"),
            (
                "tenant=dev,tenant=qa,realm=r,key_file=/k",
                "tenant is given more than once",
            ),
            (
                "tenant=dev,realm=r,key_file=/k,admin=yes",
                "'admin' is not one of tenant, realm, key_file, runner_key_file",
            ),
            (
                "tenant=dev,realm=r,key_file=/k,runner_key_file=run/key",
                "runner_key_file 'run/key' is not an absolute path",
            ),
            (
                "tenant=dev,realm=r,key_file=/k,runner_key_file=/a,runner_key_file=/b",
                "runner_key_file is given more than once",
            ),
            ("tenant=dev,realm,key_file=/k", "'realm' is not key=value"),
            ("tenant=dev,realm=,key_file=/k", "'realm=' is not key=value"),
            (
                "tenant=Dev_1,realm=r,key_file=/k",
                "tenant 'Dev_1' is not a tenant slug",
            ),
            (
                "tenant=dev,realm=r,key_file=run/key",
                "key_file 'run/key' is not an absolute path",
            ),
        ] {
            assert_eq!(bootstrap(raw), Err(reason.to_string()), "{raw}");
        }
    }

    #[test]
    fn only_an_explicit_development_platform_admits_a_bootstrap() {
        let raw = "tenant=dev,realm=cufflink-dev,key_file=/run/cufflink/dev-key";
        let dev =
            DevBootstrap::from_setting(Some(RunEnv::Development), Some(raw)).expect("a bootstrap");
        assert_eq!(dev.map(|dev| dev.tenant), Some("dev".to_string()));
        for (declared, named) in [
            (None, "unset"),
            (Some(RunEnv::Staging), "staging"),
            (Some(RunEnv::Production), "production"),
        ] {
            for value in [raw, "nonsense"] {
                let refused = DevBootstrap::from_setting(declared, Some(value))
                    .expect_err("refused")
                    .to_string();
                assert!(
                    refused.starts_with(&format!(
                        "CUFFLINK_DEV_BOOTSTRAP is refused unless RUN_ENV=development (it is \
                         {named})"
                    )),
                    "{refused}"
                );
            }
        }
        for declared in [
            None,
            Some(RunEnv::Development),
            Some(RunEnv::Staging),
            Some(RunEnv::Production),
        ] {
            for unset in [None, Some(""), Some("  ")] {
                assert_eq!(
                    DevBootstrap::from_setting(declared, unset).expect("unset"),
                    None
                );
            }
        }
        let named = DevBootstrap::from_setting(Some(RunEnv::Development), Some("tenant=dev"))
            .expect_err("malformed")
            .to_string();
        assert_eq!(named, "CUFFLINK_DEV_BOOTSTRAP: realm is missing");
    }

    #[test]
    fn only_an_active_key_of_the_tenant_is_kept() {
        let tenant = a_tenant("dev", Some("cufflink-dev"));
        let key = |tenant_id: Option<Uuid>| {
            KeyLookup::Active(ApiKeyAuth {
                id: Uuid::new_v4(),
                tenant_id,
                is_platform_admin: tenant_id.is_none(),
                name: KEY_NAME.into(),
                user_sub: None,
            })
        };
        assert!(kept(&key(Some(tenant.id)), &tenant));
        for replaced in [
            key(Some(Uuid::new_v4())),
            key(None),
            KeyLookup::Expired,
            KeyLookup::Unknown,
        ] {
            assert!(!kept(&replaced, &tenant), "{replaced:?}");
        }
    }

    #[tokio::test]
    async fn a_key_is_written_whole_for_its_owner_alone() {
        let directory = tempfile::tempdir().expect("directory");
        let key_file = directory.path().join("run/dev-key");
        assert_eq!(held(&key_file).await.expect("read"), None);
        write(&key_file, "ck_first").await.expect("write");
        write(&key_file, "ck_second").await.expect("write");
        assert_eq!(
            held(&key_file).await.expect("read"),
            Some("ck_second".to_string())
        );
        assert!(!key_file.with_extension("partial").exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&key_file)
                .expect("metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        tokio::fs::write(&key_file, " \n").await.expect("blank");
        assert_eq!(held(&key_file).await.expect("read"), None);
    }

    #[tokio::test]
    async fn what_waiting_cannot_change_ends_the_startup() {
        let directory = tempfile::tempdir().expect("directory");
        let file = directory.path().join("taken");
        tokio::fs::write(&file, "x").await.expect("a file");
        let refused = write(&file.join("dev-key"), "ck_key")
            .await
            .expect_err("a file is no directory");
        assert!(refused.is_permanent(), "{refused}");
        assert!(
            refused
                .to_string()
                .starts_with("CUFFLINK_DEV_BOOTSTRAP: writing "),
            "{refused}"
        );
        assert!(held(directory.path())
            .await
            .expect_err("a directory is no key")
            .is_permanent());
        assert!(!Failure::Failed(eyre!("the database is not there yet")).is_permanent());
    }
}
