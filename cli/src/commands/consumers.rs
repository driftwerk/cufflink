use super::{json_answer, listing};

pub async fn ls(env: Option<&str>) -> eyre::Result<()> {
    let (config, service_id, service_name) = super::deployments::resolve_service(env).await?;
    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!("{}/api/services/{}/consumers", config.api_url, service_id),
        )
        .send()
        .await?;
    let body = json_answer(resp, "list consumers").await?;
    print_consumers(&service_name, &body);
    Ok(())
}

fn print_consumers(service_name: &str, body: &serde_json::Value) {
    let mut table = listing(&[
        "HANDLER",
        "SUBJECT",
        "STREAM",
        "DURABLE",
        "STATE",
        "PENDING",
        "IN_FLIGHT",
        "REDELIVERED",
        "HERE",
    ]);
    for consumer in body["consumers"].as_array().into_iter().flatten() {
        let server = &consumer["server"];
        let count = |key: &str| {
            server[key]
                .as_u64()
                .map_or_else(|| "-".to_string(), |n| n.to_string())
        };
        let state = match (server.is_null(), consumer["bound"].as_bool()) {
            (true, Some(true)) => "missing (bound)",
            (true, _) => "missing",
            (false, Some(true)) => "bound",
            (false, _) => "managed",
        };
        table.add_row(vec![
            consumer["handler"].as_str().unwrap_or("?").to_string(),
            consumer["subject"].as_str().unwrap_or("?").to_string(),
            consumer["stream"].as_str().unwrap_or("?").to_string(),
            consumer["durable"].as_str().unwrap_or("?").to_string(),
            state.to_string(),
            count("num_pending"),
            count("num_ack_pending"),
            count("num_redelivered"),
            if consumer["running_here"] == true {
                "✓"
            } else {
                ""
            }
            .to_string(),
        ]);
    }
    println!("{table}");
    let handovers = body["handovers"].as_array().cloned().unwrap_or_default();
    println!(
        "\n{} consumer(s) of '{}' v{}",
        body["consumers"].as_array().map_or(0, Vec::len),
        service_name,
        body["version"]
    );
    if !handovers.is_empty() {
        let mut table = listing(&["DEPLOY", "STREAM", "FROM", "TO", "MODE", "START", "AT"]);
        for handover in &handovers {
            table.add_row(vec![
                format!("v{}", handover["deploy_version"]),
                handover["stream"].as_str().unwrap_or("?").to_string(),
                handover["from_name"].as_str().unwrap_or("?").to_string(),
                handover["to_name"].as_str().unwrap_or("?").to_string(),
                handover["mode"].as_str().unwrap_or("?").to_string(),
                handover["start_sequence"]
                    .as_i64()
                    .map_or_else(|| "-".to_string(), |n| n.to_string()),
                handover["created_at"]
                    .as_str()
                    .map(|at| at.chars().take(19).collect())
                    .unwrap_or_default(),
            ]);
        }
        println!("\nHandovers:\n{table}");
    }
}
