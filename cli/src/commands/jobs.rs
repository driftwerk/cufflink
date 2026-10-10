use super::json_answer;
use crate::config::CliConfig;
use serde_json::Value;
use uuid::Uuid;

fn retry_url(api_url: &str, tenant: &str, job_id: Uuid) -> String {
    format!("{api_url}/svc/{tenant}/_jobs/{job_id}/retry")
}

fn retried(job: &Value) -> String {
    let text = |key: &str| job[key].as_str().unwrap_or("?");
    format!(
        "Job {} ({}/{}) is {}: attempt {} of {} follows",
        text("job_id"),
        text("service"),
        text("handler"),
        text("status"),
        job["attempts"].as_i64().map_or(0, |made| made + 1),
        job["max_attempts"]
    )
}

pub async fn retry(job_id: Uuid, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    if let Some(name) = &config.env_name {
        println!("Environment: {name}");
    }
    let tenant = config
        .tenant_override
        .as_deref()
        .unwrap_or(&config.tenant_slug);
    let response = config
        .auth_request(
            &config.http_client(),
            reqwest::Method::POST,
            &retry_url(&config.api_url, tenant, job_id),
        )
        .send()
        .await?;
    let job = json_answer(response, "retry the job").await?;
    println!("{}", retried(&job));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_job_is_retried_where_it_is_polled() {
        assert_eq!(
            retry_url("https://cufflink.example", "harbor", Uuid::nil()),
            "https://cufflink.example/svc/harbor/_jobs/00000000-0000-0000-0000-000000000000/retry"
        );
    }

    #[test]
    fn the_operator_is_told_which_attempt_follows() {
        let job = json!({
            "job_id": "00000000-0000-0000-0000-000000000000",
            "status": "pending",
            "handler": "handle_run_report",
            "service": "stock-service",
            "attempts": 2,
            "max_attempts": 3,
        });
        assert_eq!(
            retried(&job),
            "Job 00000000-0000-0000-0000-000000000000 (stock-service/handle_run_report) is pending: \
             attempt 3 of 3 follows"
        );
        assert_eq!(
            retried(&json!({})),
            "Job ? (?/?) is ?: attempt 0 of null follows"
        );
    }
}
