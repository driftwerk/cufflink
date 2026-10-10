use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
struct StoredCredentials {
    api_key: String,
    username: String,
}

type CredentialsFile = HashMap<String, HashMap<String, StoredCredentials>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Credentials {
    pub api_key: String,
    pub tenant_slug: String,
    pub username: String,
    pub api_url: String,
}

fn credentials_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home)
        .join(".config")
        .join("cufflink")
        .join("credentials.json")
}

fn read_file() -> eyre::Result<CredentialsFile> {
    let path = credentials_path();
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let json = std::fs::read_to_string(&path)?;
    Ok(serde_json::from_str(&json).unwrap_or_default())
}

fn write_file(file: &CredentialsFile) -> eyre::Result<()> {
    let path = credentials_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(file)?;
    std::fs::write(&path, json)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }

    Ok(())
}

pub fn save(creds: &Credentials) -> eyre::Result<()> {
    let mut file = read_file()?;
    file.entry(creds.api_url.clone()).or_default().insert(
        creds.tenant_slug.clone(),
        StoredCredentials {
            api_key: creds.api_key.clone(),
            username: creds.username.clone(),
        },
    );
    write_file(&file)
}

pub fn load_for(api_url: &str, tenant_slug: &str) -> eyre::Result<Option<Credentials>> {
    let file = read_file()?;
    Ok(file
        .get(api_url)
        .and_then(|tenants| tenants.get(tenant_slug))
        .map(|stored| Credentials {
            api_key: stored.api_key.clone(),
            tenant_slug: tenant_slug.to_string(),
            username: stored.username.clone(),
            api_url: api_url.to_string(),
        }))
}

#[allow(dead_code)]
pub fn clear() -> eyre::Result<()> {
    let path = credentials_path();
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(())
}
