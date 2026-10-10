use super::{json_answer, listing};
use crate::config::CliConfig;
use serde_json::Value;

fn dlq_path(config: &CliConfig, rest: &str) -> String {
    format!(
        "{}/api/tenants/{}/dlq{rest}",
        config.api_url, config.tenant_slug
    )
}

fn listing_query(service: Option<&str>, limit: Option<usize>) -> Vec<(&'static str, String)> {
    service
        .map(|service| ("service", service.to_string()))
        .into_iter()
        .chain(limit.map(|limit| ("limit", limit.to_string())))
        .collect()
}

fn letter_row(letter: &Value) -> Vec<String> {
    let text = |key: &str| letter[key].as_str().unwrap_or("?").to_string();
    vec![
        letter["sequence"].to_string(),
        text("service"),
        text("kind"),
        text("handler"),
        letter["deliveries"].to_string(),
        letter["at"]
            .as_str()
            .map(|at| at.chars().take(19).collect())
            .unwrap_or_default(),
        text("original_subject"),
        text("reason"),
    ]
}

fn delivered(body: &Value) -> String {
    match &body["delivered"] {
        delivered if delivered["core"] == true => "core, to whoever listens now".to_string(),
        delivered if delivered["duplicate"] == true => format!(
            "stream {} had taken an earlier replay of it",
            delivered["stream"].as_str().unwrap_or("?")
        ),
        delivered => format!(
            "stream {} sequence {}",
            delivered["stream"].as_str().unwrap_or("?"),
            delivered["sequence"]
        ),
    }
}

pub async fn ls(
    service: Option<&str>,
    limit: Option<usize>,
    env: Option<&str>,
) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    let client = config.http_client();
    let resp = config
        .auth_request(&client, reqwest::Method::GET, &dlq_path(&config, ""))
        .query(&listing_query(service, limit))
        .send()
        .await?;
    let body = json_answer(resp, "list dead letters").await?;
    let letters = body["letters"].as_array().cloned().unwrap_or_default();
    let mut table = listing(&[
        "SEQ",
        "SERVICE",
        "KIND",
        "HANDLER",
        "DELIVERIES",
        "AT",
        "SUBJECT",
        "REASON",
    ]);
    for letter in &letters {
        table.add_row(letter_row(letter));
    }
    println!("{table}");
    println!(
        "\n{} dead letter(s) of tenant '{}'{}",
        letters.len(),
        config.tenant_slug,
        if letters.len() == body["limit"].as_u64().unwrap_or_default() as usize {
            " (the newest; pass --limit for more)"
        } else {
            ""
        }
    );
    Ok(())
}

pub async fn replay(sequence: u64, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::POST,
            &dlq_path(&config, &format!("/{sequence}/replay")),
        )
        .send()
        .await?;
    let body = json_answer(resp, "replay the dead letter").await?;
    println!(
        "Dead letter {sequence} published again on '{}' ({})",
        body["subject"].as_str().unwrap_or("?"),
        delivered(&body)
    );
    Ok(())
}

pub async fn drop(sequence: u64, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::DELETE,
            &dlq_path(&config, &format!("/{sequence}")),
        )
        .send()
        .await?;
    json_answer(resp, "drop the dead letter").await?;
    println!("Dead letter {sequence} dropped.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_listing_asks_for_what_was_named() {
        assert_eq!(listing_query(None, None), []);
        assert_eq!(
            listing_query(Some("mail service&x=1"), Some(20)),
            [
                ("service", "mail service&x=1".to_string()),
                ("limit", "20".to_string())
            ]
        );
    }

    #[test]
    fn a_letter_is_one_row() {
        assert_eq!(
            letter_row(&json!({
                "sequence": 42, "service": "mail-service", "kind": "sub", "handler": "fail",
                "deliveries": 3, "at": "2026-09-28T10:00:00.250Z",
                "original_subject": "dw.acme.mail.fail", "reason": "handler answered 503",
            })),
            [
                "42",
                "mail-service",
                "sub",
                "fail",
                "3",
                "2026-09-28T10:00:00",
                "dw.acme.mail.fail",
                "handler answered 503"
            ]
        );
        assert_eq!(
            letter_row(&json!({})),
            ["null", "?", "?", "?", "null", "", "?", "?"]
        );
    }

    #[test]
    fn a_replay_says_how_it_was_delivered() {
        for (answer, told) in [
            (json!({"core": true}), "core, to whoever listens now"),
            (
                json!({"stream": "DW_ACME", "sequence": 9, "duplicate": true}),
                "stream DW_ACME had taken an earlier replay of it",
            ),
            (
                json!({"stream": "DW_ACME", "sequence": 9, "duplicate": false}),
                "stream DW_ACME sequence 9",
            ),
        ] {
            assert_eq!(delivered(&json!({ "delivered": answer })), told);
        }
    }
}
