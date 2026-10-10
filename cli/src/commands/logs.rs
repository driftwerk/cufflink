use crate::config::CliConfig;
use crate::project_config::ProjectConfig;

pub struct LogParams<'a> {
    pub follow: bool,
    pub since: Option<&'a str>,
    pub from: Option<&'a str>,
    pub to: Option<&'a str>,
    pub search: Option<&'a str>,
    pub level: Option<&'a str>,
    pub limit: Option<usize>,
    pub output: Option<&'a str>,
}

fn resolve_service_name() -> eyre::Result<String> {
    if let Some(project) = ProjectConfig::find_and_load()? {
        if let Some(name) = project.service.name.as_deref() {
            return Ok(name.to_string());
        }
    }

    let output = std::process::Command::new("cargo")
        .args(["run", "--", "--emit-manifest"])
        .output()?;

    if !output.status.success() {
        eyre::bail!("Cannot determine service name. Run from a cufflink service directory.");
    }

    let stdout = String::from_utf8(output.stdout)?;
    let manifest: serde_json::Value = serde_json::from_str(stdout.trim())?;
    manifest["name"]
        .as_str()
        .map(String::from)
        .ok_or_else(|| eyre::eyre!("No service name in manifest"))
}

pub async fn run(params: LogParams<'_>, env: Option<&str>) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;
    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let service_name = resolve_service_name()?;
    let service_id = config.find_service_id(&service_name).await?;

    if params.follow && params.since.is_none() && params.from.is_none() && params.to.is_none() {
        return tail_logs(&config, &service_id, params.search, params.level).await;
    }

    query_logs(&config, &service_id, &params).await
}

async fn query_logs(
    config: &CliConfig,
    service_id: &str,
    params: &LogParams<'_>,
) -> eyre::Result<()> {
    let client = config.http_client();
    let mut url = format!("{}/api/services/{}/logs", config.api_url, service_id);
    let mut query_params = Vec::new();

    if let Some(s) = params.since {
        query_params.push(format!("since={}", s));
    }
    if let Some(f) = params.from {
        query_params.push(format!("from={}", f));
    }
    if let Some(t) = params.to {
        query_params.push(format!("to={}", t));
    }
    if let Some(s) = params.search {
        query_params.push(format!("search={}", urlencoding::encode(s)));
    }
    if let Some(l) = params.level {
        query_params.push(format!("level={}", l));
    }
    if let Some(n) = params.limit {
        query_params.push(format!("limit={}", n));
    }

    if !query_params.is_empty() {
        url.push('?');
        url.push_str(&query_params.join("&"));
    }

    let resp = config
        .auth_request(&client, reqwest::Method::GET, &url)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to fetch logs ({}): {}", status, body);
    }

    let body: serde_json::Value = resp.json().await?;
    let logs = body["logs"].as_array();
    let form = Form::of(&body);

    let mut file_output = params
        .output
        .map(|p| std::fs::File::create(p).expect("Failed to create output file"));

    let mut count = 0;
    if let Some(logs) = logs {
        for entry in logs {
            println!("{}", format_log_entry(entry));

            if let Some(ref mut f) = file_output {
                use std::io::Write;
                writeln!(f, "{}", form.saved(entry)).ok();
            }

            count += 1;
        }
    }

    if count == 0 {
        println!("No logs found.");
    } else if let Some(path) = params.output {
        println!("\n{} log lines saved to {}", count, path);
    }

    Ok(())
}

async fn tail_logs(
    config: &CliConfig,
    service_id: &str,
    search: Option<&str>,
    level: Option<&str>,
) -> eyre::Result<()> {
    let client = config.http_client();
    let mut url = format!("{}/api/services/{}/logs/tail", config.api_url, service_id);
    let mut params = Vec::new();

    if let Some(s) = search {
        params.push(format!("search={}", urlencoding::encode(s)));
    }
    if let Some(l) = level {
        params.push(format!("level={}", l));
    }

    if !params.is_empty() {
        url.push('?');
        url.push_str(&params.join("&"));
    }

    println!("Streaming logs (Ctrl+C to stop)...\n");

    let resp = config
        .auth_request(&client, reqwest::Method::GET, &url)
        .header("Accept", "text/event-stream")
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Failed to stream logs ({}): {}", status, body);
    }

    use futures_util::StreamExt;
    let mut stream = resp.bytes_stream();

    let mut buffer = String::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(pos) = buffer.find("\n\n") {
            let event = buffer[..pos].to_string();
            buffer = buffer[pos + 2..].to_string();

            if let Some(data) = event.strip_prefix("data:") {
                let data = data.trim();
                if let Ok(entry) = serde_json::from_str::<serde_json::Value>(data) {
                    if entry["line"].as_str().is_some_and(|line| !line.is_empty()) {
                        println!("{}", format_log_entry(&entry));
                    }
                }
            }
        }
    }

    Ok(())
}

fn format_timestamp(ts_nanos: &str) -> String {
    ts_nanos
        .parse::<i64>()
        .ok()
        .and_then(|ns| {
            chrono::DateTime::from_timestamp(ns / 1_000_000_000, (ns % 1_000_000_000) as u32)
        })
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default()
}

#[derive(Debug, PartialEq, Eq)]
struct Shown {
    message: String,
    level: String,
    target: Option<String>,
    fields: Vec<(String, String)>,
}

fn is_labelled(entry: &serde_json::Value) -> bool {
    entry["level"].is_string() || entry["fields"].is_object()
}

fn labelled(entry: &serde_json::Value) -> Option<Shown> {
    is_labelled(entry).then(|| Shown {
        level: entry["level"].as_str().unwrap_or("INFO").to_uppercase(),
        message: entry["line"].as_str().unwrap_or_default().to_string(),
        target: None,
        fields: entry["fields"]
            .as_object()
            .into_iter()
            .flatten()
            .filter_map(|(name, value)| Some((name.clone(), value.as_str()?.to_string())))
            .collect(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Form {
    Lines,
    Entries,
}

impl Form {
    fn of(body: &serde_json::Value) -> Self {
        match body["label_model"].as_str() {
            Some("scoped") => Self::Entries,
            _ => Self::Lines,
        }
    }

    fn saved(self, entry: &serde_json::Value) -> String {
        match self {
            Self::Entries => entry.to_string(),
            Self::Lines => entry["line"].as_str().unwrap_or_default().to_string(),
        }
    }
}

fn parsed(line: &str) -> Option<Shown> {
    let parsed = serde_json::from_str::<serde_json::Value>(line).ok()?;
    let attrs = &parsed["attributes"];
    Some(Shown {
        message: parsed["body"]
            .as_str()
            .or_else(|| parsed["message"].as_str())
            .or_else(|| parsed["msg"].as_str())
            .unwrap_or(line)
            .to_string(),
        level: parsed["severity"]
            .as_str()
            .or_else(|| parsed["level"].as_str())
            .unwrap_or("INFO")
            .to_uppercase(),
        target: parsed["instrumentation_scope"]["name"]
            .as_str()
            .filter(|scope| !scope.is_empty())
            .map(str::to_string),
        fields: ["tenant", "service", "table", "operation"]
            .into_iter()
            .filter_map(|key| Some((key.to_string(), attrs[key].as_str()?.to_string())))
            .collect(),
    })
}

fn format_log_entry(entry: &serde_json::Value) -> String {
    let line = entry["line"].as_str().unwrap_or("");
    let Some(shown) = labelled(entry).or_else(|| parsed(line)) else {
        return line.to_string();
    };
    let level_colored = match shown.level.as_str() {
        "ERROR" | "CRITICAL" | "FATAL" => format!("\x1b[31m{}\x1b[0m", shown.level),
        "WARN" | "WARNING" => format!("\x1b[33m{}\x1b[0m", shown.level),
        "DEBUG" | "TRACE" => format!("\x1b[36m{}\x1b[0m", shown.level),
        _ => format!("\x1b[32m{}\x1b[0m", shown.level),
    };
    let target = shown
        .target
        .map(|target| format!(" \x1b[90m{target}\x1b[0m"))
        .unwrap_or_default();
    let ts = format_timestamp(entry["timestamp"].as_str().unwrap_or("0"));
    let ts_prefix = match ts.is_empty() {
        true => String::new(),
        false => format!("\x1b[90m{ts}\x1b[0m "),
    };
    let fields = shown
        .fields
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join(" ");
    let head = format!("{ts_prefix}[{level_colored}]{target} {}", shown.message);
    match fields.is_empty() {
        true => head,
        false => format!("{head} \x1b[90m{fields}\x1b[0m"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const AT: &str = "1700000000000000000";

    #[test]
    fn a_json_line_is_shown_by_what_it_says_of_itself() {
        let line = json!({
            "body": "crud request",
            "severity": "info",
            "attributes": {"tenant": "acme", "service": "orders", "operation": "list"},
            "instrumentation_scope": {"name": "cufflink_platform::router::crud"},
        })
        .to_string();
        assert_eq!(
            format_log_entry(&json!({"timestamp": AT, "line": line})),
            "\x1b[90m2023-11-14 22:13:20\x1b[0m [\x1b[32mINFO\x1b[0m] \x1b[90mcufflink_platform::router::crud\x1b[0m crud request \x1b[90mtenant=acme service=orders operation=list\x1b[0m"
        );
        assert_eq!(
            format_log_entry(
                &json!({"timestamp": AT, "line": r#"{"msg":"failed","level":"error"}"#})
            ),
            "\x1b[90m2023-11-14 22:13:20\x1b[0m [\x1b[31mERROR\x1b[0m] failed"
        );
    }

    #[test]
    fn a_line_with_its_level_and_fields_beside_it_is_shown_by_them() {
        assert_eq!(
            format_log_entry(&json!({
                "timestamp": AT,
                "line": "[wasm] slow",
                "level": "warn",
                "fields": {"cufflink_handler": "list_orders", "trace_id": "97c9e5c1"},
            })),
            "\x1b[90m2023-11-14 22:13:20\x1b[0m [\x1b[33mWARN\x1b[0m] [wasm] slow \x1b[90mcufflink_handler=list_orders trace_id=97c9e5c1\x1b[0m"
        );
        assert_eq!(
            format_log_entry(
                &json!({"timestamp": AT, "line": r#"{"msg":"a JSON message"}"#, "level": "info"})
            ),
            "\x1b[90m2023-11-14 22:13:20\x1b[0m [\x1b[32mINFO\x1b[0m] {\"msg\":\"a JSON message\"}",
            "the line is the message when the platform gives the level"
        );
    }

    #[test]
    fn fields_without_a_level_are_shown_and_a_level_past_error_is_an_error() {
        assert_eq!(
            format_log_entry(&json!({
                "timestamp": AT,
                "line": r#"{"msg":"kept whole"}"#,
                "fields": {"trace_id": "97c9e5c1", "count": 3},
            })),
            "\x1b[90m2023-11-14 22:13:20\x1b[0m [\x1b[32mINFO\x1b[0m] {\"msg\":\"kept whole\"} \x1b[90mtrace_id=97c9e5c1\x1b[0m"
        );
        for level in ["critical", "fatal"] {
            assert_eq!(
                format_log_entry(&json!({"timestamp": AT, "line": "gone", "level": level})),
                format!(
                    "\x1b[90m2023-11-14 22:13:20\x1b[0m [\x1b[31m{}\x1b[0m] gone",
                    level.to_uppercase()
                )
            );
        }
    }

    #[test]
    fn a_file_is_all_lines_or_all_entries_as_the_answer_declares_its_model() {
        let labelled = json!({
            "timestamp": AT,
            "line": "[wasm] slow",
            "level": "warn",
            "fields": {"cufflink_handler": "list_orders", "trace_id": "97c9e5c1"},
        });
        let bare = json!({"timestamp": AT, "line": "plain"});
        let record = r#"{"body":"crud request","severity":"info"}"#;
        let legacy = json!({"timestamp": AT, "line": record});

        let scoped = Form::of(&json!({"logs": [labelled, bare], "label_model": "scoped"}));
        assert_eq!(scoped, Form::Entries);
        for entry in [&labelled, &bare] {
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&scoped.saved(entry)).expect("JSON"),
                *entry,
                "an entry Loki holds nothing of is JSON too"
            );
        }

        for answer in [
            json!({"logs": [legacy]}),
            json!({"logs": [legacy], "label_model": "legacy"}),
            json!({"logs": [labelled]}),
        ] {
            let lines = Form::of(&answer);
            assert_eq!(lines, Form::Lines, "{answer}");
            assert_eq!(lines.saved(&legacy), record);
            assert_eq!(
                lines.saved(&labelled),
                "[wasm] slow",
                "an answer that declares no model is lines, whatever an entry carries"
            );
        }
        assert_eq!(Form::Lines.saved(&json!({})), "");
    }

    #[test]
    fn a_line_that_says_nothing_of_itself_is_shown_as_it_is() {
        assert_eq!(
            format_log_entry(&json!({"timestamp": AT, "line": "plain text"})),
            "plain text"
        );
        assert_eq!(format_log_entry(&json!({})), "");
    }
}
