use crate::auth::DeployAuth;
use crate::errors::{PlatformError, PlatformResult};
use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    response::sse::{Event, KeepAlive, Sse},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct LogsQuery {
    pub since: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub search: Option<String>,
    pub level: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Deserialize)]
pub struct TailQuery {
    pub search: Option<String>,
    pub level: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LokiLabelModel {
    #[default]
    Legacy,
    Scoped,
}

impl FromStr for LokiLabelModel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "legacy" => Ok(Self::Legacy),
            "scoped" => Ok(Self::Scoped),
            other => Err(format!("'{other}' is not one of legacy, scoped")),
        }
    }
}

impl LokiLabelModel {
    fn declared(self) -> Option<&'static str> {
        match self {
            Self::Legacy => None,
            Self::Scoped => Some("scoped"),
        }
    }
}

fn logs_answer(model: LokiLabelModel, entries: Vec<Entry>) -> Value {
    let mut answer = json!({ "logs": entries });
    if let Some(declared) = model.declared() {
        answer["label_model"] = Value::from(declared);
    }
    answer
}

fn logql_string(value: &str) -> String {
    Value::from(value).to_string()
}

fn build_logql(
    model: LokiLabelModel,
    service_name: &str,
    tenant_slug: &str,
    search: Option<&str>,
    level: Option<&str>,
) -> String {
    match model {
        LokiLabelModel::Legacy => {
            let field = |key: &str, value: &str| format!("\"{key}\":{}", Value::from(value));
            [
                Some(field("service", service_name)),
                Some(field("tenant", tenant_slug)),
                level.map(|level| field("severity", &level.to_uppercase())),
                search.map(str::to_string),
            ]
            .into_iter()
            .flatten()
            .fold(
                r#"{service_name="cufflink-platform"}"#.to_string(),
                |query, filter| format!("{query} |= {}", logql_string(&filter)),
            )
        }
        LokiLabelModel::Scoped => [
            Some(format!(
                "{{service_namespace={}, service_name={}}}",
                logql_string(tenant_slug),
                logql_string(service_name)
            )),
            level.map(|level| format!("| detected_level={}", logql_string(&level.to_lowercase()))),
            search.map(|search| format!("|= {}", logql_string(search))),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" "),
    }
}

const FIELDS: [&str; 3] = ["cufflink_handler", "cufflink_kind", "trace_id"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Entry {
    timestamp: String,
    line: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<BTreeMap<&'static str, String>>,
}

impl Entry {
    fn logged_at(&self) -> Option<i64> {
        self.timestamp.parse().ok()
    }
}

fn entries(model: LokiLabelModel, answer: &Value) -> Vec<Entry> {
    let text = |value: &Value| value.as_str().map(str::to_string);
    let mut entries: Vec<Entry> = answer["data"]["result"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|stream| {
            let labels = &stream["stream"];
            let (level, fields) = match model {
                LokiLabelModel::Legacy => (None, None),
                LokiLabelModel::Scoped => (
                    text(&labels["detected_level"]),
                    Some(
                        FIELDS
                            .into_iter()
                            .filter_map(|name| Some((name, text(&labels[name])?)))
                            .collect::<BTreeMap<_, _>>(),
                    )
                    .filter(|fields| !fields.is_empty()),
                ),
            };
            stream["values"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|pair| match pair.as_array().map(Vec::as_slice) {
                    Some([timestamp, line, ..]) => Some((text(timestamp)?, text(line)?)),
                    _ => None,
                })
                .map(move |(timestamp, line)| Entry {
                    timestamp,
                    line,
                    level: level.clone(),
                    fields: fields.clone(),
                })
        })
        .collect();
    entries.sort_by_cached_key(|entry| {
        let logged_at = entry.logged_at();
        (logged_at.is_none(), logged_at)
    });
    entries
}

fn next_start(start: i64, batch: &[Entry]) -> i64 {
    batch
        .iter()
        .filter_map(Entry::logged_at)
        .max()
        .map_or(start, |latest| start.max(latest.saturating_add(1)))
}

const DEFAULT_WINDOW_NS: i64 = 3_600_000_000_000;

fn parse_duration_to_nanos(s: &str) -> eyre::Result<i64> {
    let s = s.trim();
    let (num_str, multiplier) = if let Some(n) = s.strip_suffix("ms") {
        (n, 1_000_000i64)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1_000_000_000i64)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60_000_000_000i64)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3_600_000_000_000i64)
    } else if let Some(n) = s.strip_suffix('d') {
        (n, 86_400_000_000_000i64)
    } else {
        eyre::bail!("Invalid duration: {}", s);
    };
    Ok(num_str.parse::<i64>()? * multiplier)
}

pub async fn query_logs(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<Uuid>,
    Query(params): Query<LogsQuery>,
) -> PlatformResult<Json<Value>> {
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let loki_url = state
        .config
        .loki_url
        .as_deref()
        .ok_or_else(|| PlatformError::Internal("LOKI_URL not configured".into()))?;

    let model = state.config.loki_label_model;
    let query = build_logql(
        model,
        &service.name,
        &user.tenant_slug,
        params.search.as_deref(),
        params.level.as_deref(),
    );

    let now = chrono::Utc::now();
    let end_ns = params
        .to
        .as_deref()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.timestamp_nanos_opt().unwrap_or(0))
        .unwrap_or_else(|| now.timestamp_nanos_opt().unwrap_or(0));

    let start_ns = if let Some(ref from) = params.from {
        chrono::DateTime::parse_from_rfc3339(from)
            .map(|t| t.timestamp_nanos_opt().unwrap_or(0))
            .map_err(|e| PlatformError::BadRequest(format!("Invalid from timestamp: {}", e)))?
    } else if let Some(ref since) = params.since {
        let duration_ns = parse_duration_to_nanos(since)
            .map_err(|e| PlatformError::BadRequest(format!("Invalid duration: {}", e)))?;
        end_ns - duration_ns
    } else {
        end_ns - DEFAULT_WINDOW_NS
    };

    let limit = params.limit.unwrap_or(100).min(5000);

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/loki/api/v1/query_range", loki_url))
        .query(&[
            ("query", query.as_str()),
            ("start", &start_ns.to_string()),
            ("end", &end_ns.to_string()),
            ("limit", &limit.to_string()),
            ("direction", "forward"),
        ])
        .send()
        .await
        .map_err(|e| PlatformError::Internal(format!("Loki query failed: {}", e)))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(PlatformError::Internal(format!(
            "Loki returned {}: {}",
            status, body
        )));
    }

    let loki_resp: Value = resp
        .json()
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to parse Loki response: {}", e)))?;

    Ok(Json(logs_answer(model, entries(model, &loki_resp))))
}

pub async fn tail_logs(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<Uuid>,
    Query(params): Query<TailQuery>,
) -> PlatformResult<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>> {
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let loki_url = state
        .config
        .loki_url
        .as_deref()
        .ok_or_else(|| PlatformError::Internal("LOKI_URL not configured".into()))?
        .to_string();

    let model = state.config.loki_label_model;
    let query = build_logql(
        model,
        &service.name,
        &user.tenant_slug,
        params.search.as_deref(),
        params.level.as_deref(),
    );

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(64);

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut start = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        loop {
            let now_ns = chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(0)
                .to_string();

            let resp = client
                .get(format!("{}/loki/api/v1/query_range", loki_url))
                .query(&[
                    ("query", query.as_str()),
                    ("start", start.to_string().as_str()),
                    ("end", now_ns.as_str()),
                    ("limit", "100"),
                    ("direction", "forward"),
                ])
                .send()
                .await;

            if let Ok(resp) = resp {
                if let Ok(body) = resp.json::<Value>().await {
                    let batch = entries(model, &body);
                    start = next_start(start, &batch);
                    for entry in batch {
                        let event = Event::default().data(json!(entry).to_string());
                        if tx.send(Ok(event)).await.is_err() {
                            return;
                        }
                    }
                }
            }

            tokio::select! {
                _ = tx.closed() => return,
                _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use LokiLabelModel::{Legacy, Scoped};

    #[test]
    fn a_legacy_query_names_the_service_and_tenant_as_it_always_has() {
        assert_eq!(
            build_logql(Legacy, "orders", "acme", None, None),
            r#"{service_name="cufflink-platform"} |= "\"service\":\"orders\"" |= "\"tenant\":\"acme\"""#
        );
        assert_eq!(
            build_logql(Legacy, "orders", "acme", Some("timeout"), Some("warn")),
            r#"{service_name="cufflink-platform"} |= "\"service\":\"orders\"" |= "\"tenant\":\"acme\"" |= "\"severity\":\"WARN\"" |= "timeout""#
        );
    }

    #[test]
    fn a_scoped_query_selects_the_tenants_service_by_its_labels() {
        assert_eq!(
            build_logql(Scoped, "orders", "acme", None, None),
            r#"{service_namespace="acme", service_name="orders"}"#
        );
        assert_eq!(
            build_logql(Scoped, "orders", "acme", None, Some("WARN")),
            r#"{service_namespace="acme", service_name="orders"} | detected_level="warn""#
        );
        assert_eq!(
            build_logql(Scoped, "orders", "acme", Some("timeout"), Some("error")),
            r#"{service_namespace="acme", service_name="orders"} | detected_level="error" |= "timeout""#
        );
    }

    #[test]
    fn nothing_a_caller_sends_leaves_its_string() {
        let (search, level) = ("x` |~ `.*\" \\", "info` |= `\"");
        assert_eq!(
            build_logql(Legacy, "orders", "acme", Some(search), Some(level)),
            r#"{service_name="cufflink-platform"} |= "\"service\":\"orders\"" |= "\"tenant\":\"acme\"" |= "\"severity\":\"INFO` |= `\\\"\"" |= "x` |~ `.*\" \\""#
        );
        assert_eq!(
            build_logql(
                Scoped,
                "ord\"} |= \"ers",
                "ac\"me",
                Some(search),
                Some(level)
            ),
            r#"{service_namespace="ac\"me", service_name="ord\"} |= \"ers"} | detected_level="info` |= `\"" |= "x` |~ `.*\" \\""#
        );
    }

    #[test]
    fn the_label_model_is_legacy_unless_set_and_refuses_what_it_does_not_know() {
        assert_eq!(LokiLabelModel::default(), Legacy);
        assert_eq!(" Scoped ".parse(), Ok(Scoped));
        assert_eq!("legacy".parse(), Ok(Legacy));
        assert_eq!(
            "otlp".parse::<LokiLabelModel>(),
            Err("'otlp' is not one of legacy, scoped".to_string())
        );
    }

    fn answer() -> Value {
        json!({"data": {"result": [
            {
                "stream": {
                    "service_namespace": "acme",
                    "service_name": "orders",
                    "detected_level": "warn",
                    "cufflink_handler": "list_orders",
                    "cufflink_kind": "route",
                    "trace_id": "97c9e5c13f4a4d0fa53a2273c4be3aad",
                    "span_id": "00f067aa0ba902b7",
                },
                "values": [["1700000000000000001", "[wasm] slow"], ["1700000000000000002", "[wasm] slower"]],
            },
            {
                "stream": {"service_namespace": "acme", "service_name": "orders"},
                "values": [["1700000000000000003", "plain"], ["malformed"], "junk"],
            },
        ]}})
    }

    #[test]
    fn a_legacy_answer_is_the_timestamp_and_the_line_alone() {
        assert_eq!(
            json!(entries(Legacy, &answer())),
            json!([
                {"timestamp": "1700000000000000001", "line": "[wasm] slow"},
                {"timestamp": "1700000000000000002", "line": "[wasm] slower"},
                {"timestamp": "1700000000000000003", "line": "plain"},
            ])
        );
        assert_eq!(entries(Legacy, &json!({"data": {}})), []);
    }

    #[test]
    fn an_answer_names_its_label_model_only_where_the_entries_are_labelled() {
        let entries = entries(Legacy, &answer());
        assert_eq!(
            logs_answer(Legacy, entries.clone()),
            json!({"logs": entries}),
            "a legacy answer is what it always was"
        );
        assert_eq!(
            logs_answer(Scoped, Vec::new()),
            json!({"logs": [], "label_model": "scoped"})
        );
    }

    #[test]
    fn a_scoped_answer_gains_the_level_and_fields_loki_holds_beside_the_line() {
        let fields = json!({
            "cufflink_handler": "list_orders",
            "cufflink_kind": "route",
            "trace_id": "97c9e5c13f4a4d0fa53a2273c4be3aad",
        });
        assert_eq!(
            json!(entries(Scoped, &answer())),
            json!([
                {"timestamp": "1700000000000000001", "line": "[wasm] slow", "level": "warn", "fields": fields},
                {"timestamp": "1700000000000000002", "line": "[wasm] slower", "level": "warn", "fields": fields},
                {"timestamp": "1700000000000000003", "line": "plain"},
            ])
        );
    }

    fn streams(streams: &[&[(&str, &str)]]) -> Value {
        let streams: Vec<_> = streams
            .iter()
            .zip(0..)
            .map(|(values, trace)| json!({"stream": {"trace_id": trace.to_string()}, "values": values}))
            .collect();
        json!({"data": {"result": streams}})
    }

    fn told(model: LokiLabelModel, answer: &Value) -> Vec<(String, String)> {
        entries(model, answer)
            .into_iter()
            .map(|entry| (entry.timestamp, entry.line))
            .collect()
    }

    #[test]
    fn the_lines_of_several_streams_are_answered_oldest_first() {
        let answer = streams(&[
            &[("1700000000000000002", "b"), ("1700000000000000009", "e")],
            &[("999999999999999999", "a"), ("1700000000000000005", "c")],
            &[("1700000000000000005", "d"), ("soon", "f"), ("", "g")],
        ]);
        let expected = [
            ("999999999999999999", "a"),
            ("1700000000000000002", "b"),
            ("1700000000000000005", "c"),
            ("1700000000000000005", "d"),
            ("1700000000000000009", "e"),
            ("soon", "f"),
            ("", "g"),
        ]
        .map(|(timestamp, line)| (timestamp.to_string(), line.to_string()));
        assert_eq!(told(Scoped, &answer), expected);
        assert_eq!(told(Legacy, &answer), expected);
    }

    #[test]
    fn a_tail_reads_on_from_past_the_latest_line_of_its_batch() {
        let batch = entries(
            Scoped,
            &streams(&[
                &[("1700000000000000007", "late")],
                &[("1700000000000000003", "early"), ("unreadable", "?")],
            ]),
        );
        assert_eq!(
            batch.last().map(|entry| entry.line.as_str()),
            Some("?"),
            "the last line of a batch is not always its latest"
        );
        assert_eq!(
            next_start(1_700_000_000_000_000_000, &batch),
            1_700_000_000_000_000_008
        );
        assert_eq!(next_start(5, &[]), 5, "nothing read: from the same instant");
        assert_eq!(next_start(5, &batch[2..]), 5, "no line with a time");
        assert_eq!(
            next_start(1_700_000_000_000_000_100, &batch),
            1_700_000_000_000_000_100,
            "never back"
        );
        let last = entries(Scoped, &streams(&[&[(&i64::MAX.to_string(), "last")]]));
        assert_eq!(next_start(0, &last), i64::MAX);
    }
}
