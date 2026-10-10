use crate::project_config::ProjectConfig;
use crate::workspace_config::WorkspaceConfig;
use securestore::{KeySource, SecretsManager};
use std::path::{Path, PathBuf};

fn secrets_dir(project: &ProjectConfig) -> PathBuf {
    project.project_dir.join("secrets")
}

pub(crate) fn vault_path(project: &ProjectConfig, env_name: &str) -> PathBuf {
    secrets_dir(project).join(format!("{}.json", env_name))
}

pub(crate) fn vault_secret_names(project: &ProjectConfig, env_name: &str) -> Option<Vec<String>> {
    let raw = std::fs::read_to_string(vault_path(project, env_name)).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let names = parsed.get("secrets")?.as_object()?;
    Some(names.keys().cloned().collect())
}

fn key_path(project: &ProjectConfig, env_name: &str) -> PathBuf {
    secrets_dir(project).join(format!("{}.key", env_name))
}

pub fn load_store(project: &ProjectConfig, env_name: &str) -> eyre::Result<SecretsManager> {
    let vault = vault_path(project, env_name);
    if !vault.exists() {
        eyre::bail!(
            "Secrets vault not found at {}. Run: cufflink secrets init --env {}",
            vault.display(),
            env_name
        );
    }

    if let Ok(key_b64) = std::env::var("CUFFLINK_SECRETS_KEY") {
        let key_bytes =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64.trim())
                .map_err(|e| eyre::eyre!("Failed to decode CUFFLINK_SECRETS_KEY: {}", e))?;

        let tmp = tempfile::NamedTempFile::new()?;
        std::fs::write(tmp.path(), &key_bytes)?;
        let store = SecretsManager::load(vault, KeySource::Path(tmp.path()))
            .map_err(|e| eyre::eyre!("Failed to load secrets vault: {}", e))?;
        return Ok(store);
    }

    let key = key_path(project, env_name);
    if !key.exists() {
        eyre::bail!(
            "Secrets key not found. Either:\n  \
             - Set CUFFLINK_SECRETS_KEY env var (base64-encoded)\n  \
             - Place key file at {}",
            key.display()
        );
    }

    let store = SecretsManager::load(vault, KeySource::Path(&key))
        .map_err(|e| eyre::eyre!("Failed to load secrets vault: {}", e))?;
    Ok(store)
}

pub fn init(env: Option<&str>) -> eyre::Result<()> {
    let project =
        ProjectConfig::find_and_load()?.ok_or_else(|| eyre::eyre!("No Cufflink.toml found"))?;

    let env_name = resolve_env_name(&project, env)?;

    let dir = secrets_dir(&project);
    std::fs::create_dir_all(&dir)?;

    let vault = vault_path(&project, &env_name);
    let key = key_path(&project, &env_name);

    if vault.exists() {
        eyre::bail!("Secrets vault already exists at {}", vault.display());
    }

    let store = SecretsManager::new(KeySource::Csprng)
        .map_err(|e| eyre::eyre!("Failed to create secrets vault: {}", e))?;
    store
        .export_key(&key)
        .map_err(|e| eyre::eyre!("Failed to export key: {}", e))?;
    store
        .save_as(&vault)
        .map_err(|e| eyre::eyre!("Failed to save vault: {}", e))?;

    ensure_gitignore(&dir)?;

    println!("Secrets initialized for '{}':", env_name);
    println!("  Vault:  {}", vault.display());
    println!("  Key:    {}", key.display());
    println!("  .gitignore created (key files excluded)");
    println!();
    println!("Add secrets with:");
    println!(
        "  cufflink secrets set \"name\" \"value\" --env {}",
        env_name
    );

    Ok(())
}

pub fn set(name: &str, value: &str, env: Option<&str>) -> eyre::Result<()> {
    let project =
        ProjectConfig::find_and_load()?.ok_or_else(|| eyre::eyre!("No Cufflink.toml found"))?;

    let env_name = resolve_env_name(&project, env)?;
    let mut store = load_store(&project, &env_name)?;

    store.set(name, value);
    store
        .save()
        .map_err(|e| eyre::eyre!("Failed to save vault: {}", e))?;

    println!("Secret '{}' set for '{}'", name, env_name);
    Ok(())
}

pub fn delete(name: &str, env: Option<&str>) -> eyre::Result<()> {
    let project =
        ProjectConfig::find_and_load()?.ok_or_else(|| eyre::eyre!("No Cufflink.toml found"))?;

    let env_name = resolve_env_name(&project, env)?;
    let mut store = load_store(&project, &env_name)?;

    store
        .remove(name)
        .map_err(|e| eyre::eyre!("Secret '{}' not found: {}", name, e))?;
    store
        .save()
        .map_err(|e| eyre::eyre!("Failed to save vault: {}", e))?;

    println!("Secret '{}' deleted from '{}'", name, env_name);
    Ok(())
}

pub fn list(env: Option<&str>, reveal: bool) -> eyre::Result<()> {
    let project =
        ProjectConfig::find_and_load()?.ok_or_else(|| eyre::eyre!("No Cufflink.toml found"))?;

    let env_name = resolve_env_name(&project, env)?;
    let store = load_store(&project, &env_name)?;

    let names: Vec<&str> = store.keys().collect();
    if names.is_empty() {
        println!("No secrets stored for '{}'.", env_name);
    } else {
        println!("Secrets for '{}':", env_name);
        for name in names {
            if reveal {
                let value: String = store
                    .get(name)
                    .map_err(|e| eyre::eyre!("Failed to read '{}': {}", name, e))?;
                println!("  {} = {}", name, value);
            } else {
                println!("  {}", name);
            }
        }
    }

    Ok(())
}

fn resolve_env_name(project: &ProjectConfig, env: Option<&str>) -> eyre::Result<String> {
    let name = env
        .map(|s| s.to_string())
        .or_else(|| project.service.default_env.clone())
        .ok_or_else(|| {
            eyre::eyre!(
                "No environment specified. Use --env <name> or set default_env in Cufflink.toml"
            )
        })?;
    Ok(name)
}

fn ensure_gitignore(dir: &Path) -> eyre::Result<()> {
    let gitignore = dir.join(".gitignore");
    if !gitignore.exists() {
        std::fs::write(&gitignore, "*.key\n")?;
    }
    Ok(())
}

fn resolve_workspace_service(from: &str) -> eyre::Result<PathBuf> {
    let (ws, root) = WorkspaceConfig::find_and_load()?;
    let svc = ws.services.iter().find(|s| s.name == from).ok_or_else(|| {
        let names: Vec<&str> = ws.services.iter().map(|s| s.name.as_str()).collect();
        eyre::eyre!(
            "Service '{}' not found in cufflink-workspace.toml. Available: {:?}",
            from,
            names,
        )
    })?;
    Ok(root.join(&svc.path))
}

fn list_store_keys(service_dir: &Path) -> eyre::Result<Vec<(String, PathBuf)>> {
    let dir = service_dir.join("secrets");
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("key") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                out.push((stem.to_string(), path));
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

pub fn init_from(from: &str, store: Option<&str>, env: Option<&str>) -> eyre::Result<()> {
    let project =
        ProjectConfig::find_and_load()?.ok_or_else(|| eyre::eyre!("No Cufflink.toml found"))?;

    let target = store.or(env);
    let source_dir = resolve_workspace_service(from)?;
    let mut src_keys = list_store_keys(&source_dir)?;

    if let Some(t) = target {
        src_keys.retain(|(name, _)| name == t);
    }

    if src_keys.is_empty() {
        match target {
            Some(t) => eyre::bail!("Source service '{}' has no secrets/{}.key", from, t),
            None => eyre::bail!("Source service '{}' has no *.key files in secrets/", from),
        }
    }

    let dir = secrets_dir(&project);
    std::fs::create_dir_all(&dir)?;
    ensure_gitignore(&dir)?;

    for (env_name, src_key_path) in src_keys {
        let dst_vault = vault_path(&project, &env_name);
        if dst_vault.exists() {
            eyre::bail!(
                "Vault already exists at {}. Remove it or run `cufflink secrets rekey --from {}` instead.",
                dst_vault.display(),
                from,
            );
        }
        let dst_key = key_path(&project, &env_name);

        std::fs::copy(&src_key_path, &dst_key).map_err(|e| {
            eyre::eyre!(
                "Failed to copy {} → {}: {}",
                src_key_path.display(),
                dst_key.display(),
                e,
            )
        })?;

        let store = SecretsManager::new(KeySource::Path(&dst_key))
            .map_err(|e| eyre::eyre!("Failed to create vault for '{}': {}", env_name, e))?;
        store
            .save_as(&dst_vault)
            .map_err(|e| eyre::eyre!("Failed to save vault for '{}': {}", env_name, e))?;

        println!("Initialized '{}' vault from '{}'", env_name, from);
        println!("  Vault: {}", dst_vault.display());
        println!("  Key:   {} (copied)", dst_key.display());
    }

    Ok(())
}

pub fn rekey_vault_in_place(
    vault: &Path,
    current_key: &Path,
    new_key: &Path,
) -> eyre::Result<usize> {
    if !vault.exists() {
        eyre::bail!("No vault at {}", vault.display());
    }
    let old = SecretsManager::load(vault, KeySource::Path(current_key))
        .map_err(|e| eyre::eyre!("Failed to load vault with current key: {}", e))?;

    let pairs: Vec<(String, String)> = old
        .keys()
        .map(String::from)
        .map(|name| {
            old.get(&name)
                .map(|value: String| (name.clone(), value))
                .map_err(|e| eyre::eyre!("Failed to read '{}' during rekey: {}", name, e))
        })
        .collect::<eyre::Result<_>>()?;

    let mut new_store = SecretsManager::new(KeySource::Path(new_key))
        .map_err(|e| eyre::eyre!("Failed to create new vault: {}", e))?;
    for (name, value) in &pairs {
        new_store.set(name, value.as_str());
    }
    new_store
        .save_as(vault)
        .map_err(|e| eyre::eyre!("Failed to save rekeyed vault: {}", e))?;

    Ok(pairs.len())
}

pub fn rekey(from: &str, store: Option<&str>, env: Option<&str>) -> eyre::Result<()> {
    let project =
        ProjectConfig::find_and_load()?.ok_or_else(|| eyre::eyre!("No Cufflink.toml found"))?;

    let target = store.or(env);
    let source_dir = resolve_workspace_service(from)?;
    let mut candidates = list_store_keys(&source_dir)?;

    if let Some(t) = target {
        candidates.retain(|(name, _)| name == t);
    } else {
        candidates.retain(|(name, _)| vault_path(&project, name).exists());
    }

    if candidates.is_empty() {
        match target {
            Some(t) => eyre::bail!(
                "Nothing to rekey: source service '{}' has no secrets/{}.key, or current service has no matching vault.",
                from,
                t,
            ),
            None => eyre::bail!(
                "Nothing to rekey: no env names have both a source `.key` in '{}' and a local vault.",
                from,
            ),
        }
    }

    let env_tmp = materialize_env_key()?;

    for (env_name, src_key_path) in candidates {
        let current_vault = vault_path(&project, &env_name);
        if !current_vault.exists() {
            eyre::bail!(
                "No vault to rekey at {}. Use `cufflink secrets init --from {} --store {}` first.",
                current_vault.display(),
                from,
                env_name,
            );
        }

        let current_key = key_path(&project, &env_name);
        let key_for_decrypt: &Path = match env_tmp.as_ref() {
            Some(tmp) => tmp.path(),
            None => &current_key,
        };

        let preserved = rekey_vault_in_place(&current_vault, key_for_decrypt, &src_key_path)?;

        std::fs::copy(&src_key_path, &current_key).map_err(|e| {
            eyre::eyre!(
                "Failed to persist new key to {}: {}",
                current_key.display(),
                e
            )
        })?;

        println!(
            "Rekeyed '{}' vault — now uses '{}' key ({} secrets preserved)",
            env_name, from, preserved,
        );
    }

    Ok(())
}

fn materialize_env_key() -> eyre::Result<Option<tempfile::NamedTempFile>> {
    let Ok(b64) = std::env::var("CUFFLINK_SECRETS_KEY") else {
        return Ok(None);
    };
    let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64.trim())
        .map_err(|e| eyre::eyre!("Failed to decode CUFFLINK_SECRETS_KEY: {}", e))?;
    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), &bytes)?;
    Ok(Some(tmp))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_key(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(format!("{name}.key"));
        let store = SecretsManager::new(KeySource::Csprng).unwrap();
        store.export_key(&path).unwrap();
        path
    }

    #[test]
    fn list_store_keys_returns_sorted_env_names() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = tmp.path().join("svc_a");
        let secrets = svc.join("secrets");
        std::fs::create_dir_all(&secrets).unwrap();
        write_key(&secrets, "staging");
        write_key(&secrets, "local");
        write_key(&secrets, "production");
        std::fs::write(secrets.join("README.md"), "ignore me").unwrap();

        let keys = list_store_keys(&svc).unwrap();
        let names: Vec<&str> = keys.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["local", "production", "staging"]);
    }

    #[test]
    fn list_store_keys_empty_when_no_secrets_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let keys = list_store_keys(tmp.path()).unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn new_vault_with_existing_keyfile_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let key = write_key(tmp.path(), "staging");
        let vault = tmp.path().join("staging.json");

        let mut store = SecretsManager::new(KeySource::Path(&key)).unwrap();
        store.set("NEXTAUTH_SECRET", "super-secret");
        store.save_as(&vault).unwrap();

        let loaded = SecretsManager::load(&vault, KeySource::Path(&key)).unwrap();
        let got: String = loaded.get("NEXTAUTH_SECRET").unwrap();
        assert_eq!(got, "super-secret");
    }

    #[test]
    fn rekey_vault_in_place_preserves_values_and_leaves_keyfiles_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let key_old = write_key(tmp.path(), "staging");
        let key_new = write_key(tmp.path(), "staging-new-source");
        let vault = tmp.path().join("staging.json");

        let mut seed = SecretsManager::new(KeySource::Path(&key_old)).unwrap();
        seed.set("A", "alpha");
        seed.set("B", "bravo");
        seed.save_as(&vault).unwrap();

        let key_old_before = std::fs::read(&key_old).unwrap();
        let key_new_before = std::fs::read(&key_new).unwrap();

        let preserved = rekey_vault_in_place(&vault, &key_old, &key_new).unwrap();
        assert_eq!(preserved, 2);

        assert_eq!(std::fs::read(&key_old).unwrap(), key_old_before);
        assert_eq!(std::fs::read(&key_new).unwrap(), key_new_before);

        let reloaded = SecretsManager::load(&vault, KeySource::Path(&key_new)).unwrap();
        let a: String = reloaded.get("A").unwrap();
        let b: String = reloaded.get("B").unwrap();
        assert_eq!(a, "alpha");
        assert_eq!(b, "bravo");
    }

    #[test]
    fn rekey_vault_in_place_errors_when_vault_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let key = write_key(tmp.path(), "staging");
        let err = rekey_vault_in_place(&tmp.path().join("missing.json"), &key, &key).unwrap_err();
        assert!(err.to_string().contains("No vault"));
    }
}
