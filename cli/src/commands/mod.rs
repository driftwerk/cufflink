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

pub(crate) fn listing(header: &[&str]) -> comfy_table::Table {
    use comfy_table::{presets::NOTHING, Table, TableComponent};
    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.set_style(TableComponent::HeaderLines, '-');
    table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
    table.set_header(header.to_vec());
    table
}
