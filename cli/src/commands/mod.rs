pub mod admin;
pub mod auth;
pub mod backup;
pub mod build;
pub mod ci;
pub mod config_cmd;
pub mod consumers;
pub mod db_role;
pub mod deploy;
pub mod deploy_package;
pub mod deployments;
pub mod dev;
pub mod dlq;
pub mod ext;
pub mod init;
pub mod install;
pub mod jobs;
pub mod keys;
pub mod login;
pub mod logs;
pub mod native;
pub mod openapi;
pub mod roles;
pub mod rollback;
pub mod routes;
pub mod runner_keys;
pub mod secrets_cmd;
pub mod seed;
pub mod services;
pub mod sql;
pub mod status;
#[cfg(test)]
pub(crate) mod stub;
pub mod tenant_buckets;
pub mod tenants;
pub mod test;
pub mod version;
pub mod workspace;

pub(crate) async fn json_answer(
    resp: reqwest::Response,
    what: &str,
) -> eyre::Result<serde_json::Value> {
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to {what} ({status}): {body}");
    }
    Ok(resp.json().await?)
}

/// The secret `input` holds, without the line ending a shell or a file
/// leaves after it; nothing at all is refused, naming the `flag` it was
/// read for.
pub(crate) fn secret_from(mut input: impl std::io::Read, flag: &str) -> eyre::Result<String> {
    let mut secret = String::new();
    input.read_to_string(&mut secret)?;
    let secret = secret.trim_end_matches(['\r', '\n']);
    if secret.is_empty() {
        eyre::bail!("{flag}: nothing was read from stdin");
    }
    Ok(secret.to_string())
}

pub(crate) fn listing(header: &[&str]) -> comfy_table::Table {
    use comfy_table::{presets::NOTHING, Table, TableComponent};
    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.set_style(TableComponent::HeaderLines, '-');
    table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
    table.set_header(header.to_vec());
    table
}
