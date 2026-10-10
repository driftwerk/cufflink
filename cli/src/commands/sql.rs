use crate::config::CliConfig;
use crate::project_config::ProjectConfig;
use crate::workspace_config::WorkspaceConfig;
use comfy_table::{presets::NOTHING, Table, TableComponent};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use std::io::{IsTerminal, Write};

const TTY_PREVIEW_ROWS: usize = 1000;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    query: &str,
    service: Option<&str>,
    tenant_override: Option<&str>,
    format: Option<&str>,
    all: bool,
    limit: Option<u64>,
    no_pager: bool,
    env: Option<&str>,
) -> eyre::Result<()> {
    let mut config = CliConfig::load_with_env(env)?;
    if let Some(t) = tenant_override {
        config.tenant_override = Some(t.to_string());
    }

    let service_name = resolve_service_name(service)?;
    let service_id = config.find_service_id(&service_name).await?;

    let effective_query = match limited(query, limit) {
        Limited::Wrapped(wrapped) => wrapped,
        Limited::AsTyped(why) => {
            if let Some(why) = why {
                eprintln!("--limit does not apply ({why}); the statement is sent as typed");
            }
            query.to_string()
        }
    };
    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::POST,
            &format!("{}/api/services/{}/query", config.api_url, service_id),
        )
        .json(&serde_json::json!({ "sql": effective_query }))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("SQL request failed ({}): {}", status, body);
    }

    let format = format.unwrap_or("table");
    let tty = std::io::stdout().is_terminal();

    match (format, tty) {
        ("jsonl", _) => stream_flat(resp, RowFormat::Jsonl).await,
        ("csv", _) => stream_flat(resp, RowFormat::Csv).await,
        ("tsv", _) => stream_flat(resp, RowFormat::Tsv).await,
        ("table", true) => render_table(resp, all, no_pager).await,
        ("table", false) => stream_flat(resp, RowFormat::Jsonl).await,
        (other, _) => eyre::bail!(
            "Unknown --format '{}'. Use one of: table, jsonl, csv, tsv",
            other
        ),
    }
}

fn resolve_service_name(explicit: Option<&str>) -> eyre::Result<String> {
    if let Some(name) = explicit {
        return Ok(name.to_string());
    }
    let project = ProjectConfig::find_and_load()?.ok_or_else(|| {
        eyre::eyre!(
            "--service not given and no Cufflink.toml found in current directory or any parent."
        )
    })?;

    if let Some(name) = project.service.name.clone() {
        return Ok(name);
    }

    if let Some(name) = lookup_name_from_workspace(&project.project_dir) {
        return Ok(name);
    }

    eyre::bail!(
        "Current service has no `name` in Cufflink.toml and cufflink-workspace.toml has no matching entry. Pass --service explicitly."
    )
}

fn lookup_name_from_workspace(project_dir: &std::path::Path) -> Option<String> {
    let (ws, root) = WorkspaceConfig::find_and_load().ok()?;
    match_service_by_path(&ws.services, &root, project_dir)
}

fn match_service_by_path(
    services: &[crate::workspace_config::WorkspaceService],
    root: &std::path::Path,
    project_dir: &std::path::Path,
) -> Option<String> {
    let project_canon = project_dir.canonicalize().ok()?;
    services
        .iter()
        .find(|s| {
            root.join(&s.path)
                .canonicalize()
                .is_ok_and(|p| p == project_canon)
        })
        .map(|s| s.name.clone())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Word,
    Quoted,
    Semicolon,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Token {
    kind: Kind,
    start: usize,
    end: usize,
}

fn is_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn literal_end(bytes: &[u8], start: usize, escapes: bool) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        match (bytes[i], bytes.get(i + 1)) {
            (b'\\', Some(_)) if escapes => i += 2,
            (b'\'', Some(b'\'')) => i += 2,
            (b'\'', _) => return i + 1,
            _ => i += 1,
        }
    }
    bytes.len()
}

fn quoted_name_end(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        match (bytes[i], bytes.get(i + 1)) {
            (b'"', Some(b'"')) => i += 2,
            (b'"', _) => return i + 1,
            _ => i += 1,
        }
    }
    bytes.len()
}

fn dollar_tag(query: &str, start: usize) -> Option<&str> {
    let rest = &query.as_bytes()[start + 1..];
    let length = rest.iter().take_while(|b| is_word(**b)).count();
    let opens = rest.first().is_none_or(|b| !b.is_ascii_digit()) && rest.get(length) == Some(&b'$');
    opens.then(|| &query[start..start + length + 2])
}

fn block_comment_end(bytes: &[u8], start: usize) -> usize {
    let (mut i, mut depth) = (start + 2, 1);
    while i < bytes.len() {
        match (bytes[i], bytes.get(i + 1)) {
            (b'*', Some(b'/')) if depth == 1 => return i + 2,
            (b'*', Some(b'/')) => (i, depth) = (i + 2, depth - 1),
            (b'/', Some(b'*')) => (i, depth) = (i + 2, depth + 1),
            _ => i += 1,
        }
    }
    bytes.len()
}

fn tokens(query: &str) -> Vec<Token> {
    let bytes = query.as_bytes();
    let mut found: Vec<Token> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let token = |kind, end| Token {
            kind,
            start: i,
            end,
        };
        let next = bytes.get(i + 1).copied();
        let (skipped, token) = match bytes[i] {
            b if b.is_ascii_whitespace() => (i + 1, None),
            b'-' if next == Some(b'-') => (
                bytes[i..]
                    .iter()
                    .position(|b| *b == b'\n')
                    .map_or(bytes.len(), |line| i + line + 1),
                None,
            ),
            b'/' if next == Some(b'*') => (block_comment_end(bytes, i), None),
            b'\'' => {
                let escaped = found.last().is_some_and(|word| {
                    word.end == i && query[word.start..i].eq_ignore_ascii_case("e")
                });
                let end = literal_end(bytes, i, escaped);
                (end, Some(token(Kind::Quoted, end)))
            }
            b'"' => {
                let end = quoted_name_end(bytes, i);
                (end, Some(token(Kind::Quoted, end)))
            }
            b'$' => match dollar_tag(query, i) {
                Some(tag) => {
                    let body = i + tag.len();
                    let end = query[body..]
                        .find(tag)
                        .map_or(bytes.len(), |close| body + close + tag.len());
                    (end, Some(token(Kind::Quoted, end)))
                }
                None => (i + 1, Some(token(Kind::Other, i + 1))),
            },
            b';' => (i + 1, Some(token(Kind::Semicolon, i + 1))),
            b if is_word(b) => {
                let end = i + bytes[i..].iter().take_while(|b| is_word(**b)).count();
                (end, Some(token(Kind::Word, end)))
            }
            _ => (i + 1, Some(token(Kind::Other, i + 1))),
        };
        found.extend(token);
        i = skipped;
    }
    found
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Scan {
    words: Vec<String>,
    statements: usize,
    end: usize,
}

fn scan(query: &str) -> Scan {
    let tokens = tokens(query);
    let statements = tokens
        .split(|token| token.kind == Kind::Semicolon)
        .filter(|statement| !statement.is_empty())
        .count();
    Scan {
        words: tokens
            .iter()
            .filter(|token| token.kind == Kind::Word)
            .map(|word| query[word.start..word.end].to_ascii_uppercase())
            .collect(),
        statements,
        end: tokens
            .iter()
            .rev()
            .find(|token| token.kind != Kind::Semicolon)
            .map_or(0, |last| last.end),
    }
}

const WRITES: [&str; 5] = ["INSERT", "UPDATE", "DELETE", "MERGE", "INTO"];

#[derive(Debug, PartialEq, Eq)]
enum Limited {
    Wrapped(String),
    AsTyped(Option<&'static str>),
}

fn limited(query: &str, limit: Option<u64>) -> Limited {
    let Some(limit) = limit else {
        return Limited::AsTyped(None);
    };
    let scan = scan(query);
    let answers_rows = scan
        .words
        .first()
        .is_some_and(|first| ["SELECT", "WITH", "VALUES", "TABLE"].contains(&first.as_str()));
    if scan.statements > 1 {
        Limited::AsTyped(Some("it is several statements"))
    } else if !answers_rows {
        Limited::AsTyped(Some("it is not a query"))
    } else if scan
        .words
        .iter()
        .any(|word| WRITES.contains(&word.as_str()))
    {
        Limited::AsTyped(Some("it writes"))
    } else {
        Limited::Wrapped(format!(
            "SELECT * FROM (\n{}\n) q LIMIT {limit}",
            &query[..scan.end]
        ))
    }
}

#[derive(Debug, Deserialize)]
struct ColumnMeta {
    name: String,
}

#[derive(Copy, Clone)]
enum RowFormat {
    Jsonl,
    Csv,
    Tsv,
}

enum Frame {
    Columns(Vec<ColumnMeta>),
    Row(Vec<Value>),
    Done {
        rows: u64,
        truncated: bool,
        elapsed_ms: u64,
    },
    Error(String),
}

fn parse_frame(line: &str) -> eyre::Result<Frame> {
    let v: Value = serde_json::from_str(line)
        .map_err(|e| eyre::eyre!("Malformed NDJSON frame: {} — line was: {}", e, line))?;
    match v.get("type").and_then(|t| t.as_str()) {
        Some("columns") => {
            let cols: Vec<ColumnMeta> =
                serde_json::from_value(v.get("columns").cloned().unwrap_or(Value::Array(vec![])))?;
            Ok(Frame::Columns(cols))
        }
        Some("row") => {
            let values: Vec<Value> = v
                .get("values")
                .and_then(|x| x.as_array())
                .cloned()
                .unwrap_or_default();
            Ok(Frame::Row(values))
        }
        Some("done") => Ok(Frame::Done {
            rows: v.get("rows").and_then(|x| x.as_u64()).unwrap_or(0),
            truncated: v
                .get("truncated")
                .and_then(|x| x.as_bool())
                .unwrap_or(false),
            elapsed_ms: v.get("elapsed_ms").and_then(|x| x.as_u64()).unwrap_or(0),
        }),
        Some("error") => {
            let msg = v
                .get("message")
                .and_then(|x| x.as_str())
                .unwrap_or("(no message)")
                .to_string();
            Ok(Frame::Error(msg))
        }
        other => eyre::bail!("Unknown NDJSON frame type: {:?}", other),
    }
}

async fn stream_flat(resp: reqwest::Response, kind: RowFormat) -> eyre::Result<()> {
    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let mut columns: Vec<ColumnMeta> = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buf.extend_from_slice(&chunk);
        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let line_bytes: Vec<u8> = buf.drain(..=pos).collect();
            let line = std::str::from_utf8(&line_bytes[..line_bytes.len() - 1])?.trim();
            if line.is_empty() {
                continue;
            }
            match parse_frame(line)? {
                Frame::Columns(c) => {
                    match kind {
                        RowFormat::Csv => writeln!(out, "{}", join_header(&c, ','))?,
                        RowFormat::Tsv => writeln!(out, "{}", join_header(&c, '\t'))?,
                        RowFormat::Jsonl => {}
                    }
                    columns = c;
                }
                Frame::Row(values) => match kind {
                    RowFormat::Jsonl => {
                        let obj = if columns.is_empty() {
                            Value::Array(values)
                        } else {
                            let mut map = serde_json::Map::with_capacity(columns.len());
                            for (c, v) in columns.iter().zip(values.into_iter()) {
                                map.insert(c.name.clone(), v);
                            }
                            Value::Object(map)
                        };
                        writeln!(out, "{}", serde_json::to_string(&obj)?)?;
                    }
                    RowFormat::Csv => writeln!(out, "{}", encode_row(&values, ','))?,
                    RowFormat::Tsv => writeln!(out, "{}", encode_row(&values, '\t'))?,
                },
                Frame::Done {
                    rows, truncated, ..
                } => {
                    if truncated {
                        eprintln!("(server row cap reached after {rows} rows)");
                    }
                }
                Frame::Error(msg) => eyre::bail!("Server error during streaming: {msg}"),
            }
        }
    }
    Ok(())
}

async fn render_table(resp: reqwest::Response, all: bool, no_pager: bool) -> eyre::Result<()> {
    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    let mut columns: Vec<ColumnMeta> = Vec::new();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut skipped: u64 = 0;
    let mut total: u64 = 0;
    let mut server_truncated = false;
    let mut elapsed_ms: u64 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buf.extend_from_slice(&chunk);
        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let line_bytes: Vec<u8> = buf.drain(..=pos).collect();
            let line = std::str::from_utf8(&line_bytes[..line_bytes.len() - 1])?.trim();
            if line.is_empty() {
                continue;
            }
            match parse_frame(line)? {
                Frame::Columns(c) => columns = c,
                Frame::Row(values) => {
                    total += 1;
                    if all || rows.len() < TTY_PREVIEW_ROWS {
                        rows.push(values);
                    } else {
                        skipped += 1;
                    }
                }
                Frame::Done {
                    rows: server_rows,
                    truncated,
                    elapsed_ms: elapsed,
                } => {
                    server_truncated = truncated;
                    elapsed_ms = elapsed;
                    if server_rows > 0 && server_rows != total {
                        total = server_rows;
                    }
                }
                Frame::Error(msg) => eyre::bail!("Server error during streaming: {msg}"),
            }
        }
    }

    if columns.is_empty() && rows.is_empty() {
        println!("(empty result)");
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.set_style(TableComponent::HeaderLines, '-');
    table.set_style(TableComponent::MiddleHeaderIntersections, ' ');
    let header: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    table.set_header(header);
    for row in &rows {
        let cells: Vec<String> = row.iter().map(cell_string).collect();
        table.add_row(cells);
    }

    let rendered = format!("{table}");

    if all && !no_pager && !rendered.is_empty() {
        pipe_to_pager(&rendered).unwrap_or_else(|_| println!("{rendered}"));
    } else {
        println!("{rendered}");
    }

    let hint = match (skipped > 0, server_truncated) {
        (true, true) => Some(format!(
            "{skipped} rows hidden. Server cap hit at {total}. Use --format csv|jsonl to stream, or reduce the query."
        )),
        (true, false) => Some(format!(
            "{skipped} rows hidden (showing first {TTY_PREVIEW_ROWS}). Use --all, --limit N, or --format csv|jsonl."
        )),
        (false, true) => Some(format!(
            "Server cap hit at {total} rows. Query returned more than the platform allows."
        )),
        (false, false) => None,
    };
    if let Some(h) = hint {
        eprintln!("{h}");
    }
    if elapsed_ms > 0 {
        eprintln!("({total} rows, {elapsed_ms} ms)");
    }

    Ok(())
}

fn cell_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn join_header(cols: &[ColumnMeta], sep: char) -> String {
    cols.iter()
        .map(|c| csv_escape(&c.name, sep))
        .collect::<Vec<_>>()
        .join(&sep.to_string())
}

fn encode_row(values: &[Value], sep: char) -> String {
    values
        .iter()
        .map(|v| csv_escape(&cell_string(v), sep))
        .collect::<Vec<_>>()
        .join(&sep.to_string())
}

fn csv_escape(s: &str, sep: char) -> String {
    if s.contains(sep) || s.contains('"') || s.contains('\n') || s.contains('\r') {
        let escaped = s.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        s.to_string()
    }
}

fn pipe_to_pager(content: &str) -> eyre::Result<()> {
    let pager = std::env::var("PAGER").unwrap_or_else(|_| "less -SRX".to_string());
    let mut parts = pager.split_whitespace();
    let program = parts
        .next()
        .ok_or_else(|| eyre::eyre!("PAGER env var is empty"))?;
    let args: Vec<&str> = parts.collect();

    let mut child = std::process::Command::new(program)
        .args(&args)
        .stdin(std::process::Stdio::piped())
        .spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(content.as_bytes())?;
    }
    child.wait()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_limit_wraps_the_query() {
        for (query, limited_to) in [
            (
                "select * from items",
                "SELECT * FROM (\nselect * from items\n) q LIMIT 10",
            ),
            (
                "select * from items; \n",
                "SELECT * FROM (\nselect * from items\n) q LIMIT 10",
            ),
            (
                "SELECT * FROM items LIMIT 500",
                "SELECT * FROM (\nSELECT * FROM items LIMIT 500\n) q LIMIT 10",
            ),
            (
                "select * from items -- all of them",
                "SELECT * FROM (\nselect * from items\n) q LIMIT 10",
            ),
            (
                "select 1; -- note",
                "SELECT * FROM (\nselect 1\n) q LIMIT 10",
            ),
            (
                "select 1 /* one */ ; ; /* done */\n",
                "SELECT * FROM (\nselect 1\n) q LIMIT 10",
            ),
            (
                "select name from limits where note = ' LIMIT '",
                "SELECT * FROM (\nselect name from limits where note = ' LIMIT '\n) q LIMIT 10",
            ),
            (
                "select 'it''s; -- not a comment' as a",
                "SELECT * FROM (\nselect 'it''s; -- not a comment' as a\n) q LIMIT 10",
            ),
            (
                "select \"delete\", 'insert into x' from \"update\"",
                "SELECT * FROM (\nselect \"delete\", 'insert into x' from \"update\"\n) q LIMIT 10",
            ),
            (
                "select $$ ; delete $$, $tag$ it's $tag$",
                "SELECT * FROM (\nselect $$ ; delete $$, $tag$ it's $tag$\n) q LIMIT 10",
            ),
            (
                "select E'\\'; delete'",
                "SELECT * FROM (\nselect E'\\'; delete'\n) q LIMIT 10",
            ),
            (
                "-- the newest\n/* first */ WITH n AS (SELECT 1) SELECT * FROM n",
                "SELECT * FROM (\n-- the newest\n/* first */ WITH n AS (SELECT 1) SELECT * FROM n\n) q LIMIT 10",
            ),
            (
                "values (1), (2)",
                "SELECT * FROM (\nvalues (1), (2)\n) q LIMIT 10",
            ),
            (
                "TABLE items",
                "SELECT * FROM (\nTABLE items\n) q LIMIT 10",
            ),
            (
                "( select 1) union (select 2)",
                "SELECT * FROM (\n( select 1) union (select 2)\n) q LIMIT 10",
            ),
            (
                "select * from t where id = $1",
                "SELECT * FROM (\nselect * from t where id = $1\n) q LIMIT 10",
            ),
        ] {
            assert_eq!(
                limited(query, Some(10)),
                Limited::Wrapped(limited_to.to_string()),
                "{query}"
            );
        }
    }

    #[test]
    fn only_one_query_that_writes_nothing_is_limited() {
        for (statement, why) in [
            ("show search_path", "it is not a query"),
            ("explain select * from items", "it is not a query"),
            ("selector", "it is not a query"),
            ("-- select\nshow search_path", "it is not a query"),
            ("/* unterminated select", "it is not a query"),
            ("/* nested /* select */ */ show all", "it is not a query"),
            ("", "it is not a query"),
            (
                "WITH d AS (DELETE FROM items RETURNING *) SELECT * FROM d",
                "it writes",
            ),
            (
                "with moved as (update items set n = 1 returning id) select * from moved",
                "it writes",
            ),
            ("SELECT * INTO copy FROM items", "it writes"),
            ("select 1; select 2", "it is several statements"),
            ("select 1; -- one\nselect 2;", "it is several statements"),
        ] {
            assert_eq!(
                limited(statement, Some(10)),
                Limited::AsTyped(Some(why)),
                "{statement}"
            );
        }
        assert_eq!(limited("select 1;", None), Limited::AsTyped(None));
    }

    #[test]
    fn parses_columns_frame() {
        let line = r#"{"type":"columns","columns":[{"name":"id","type_name":"uuid"},{"name":"email","type_name":"text"}]}"#;
        match parse_frame(line).unwrap() {
            Frame::Columns(c) => {
                assert_eq!(c.len(), 2);
                assert_eq!(c[0].name, "id");
                assert_eq!(c[1].name, "email");
            }
            _ => panic!("expected Columns"),
        }
    }

    #[test]
    fn parses_row_frame() {
        let line = r#"{"type":"row","values":["abc",42,null,true]}"#;
        match parse_frame(line).unwrap() {
            Frame::Row(vs) => {
                assert_eq!(vs.len(), 4);
                assert_eq!(vs[1].as_i64().unwrap(), 42);
                assert!(vs[2].is_null());
            }
            _ => panic!("expected Row"),
        }
    }

    #[test]
    fn parses_done_frame() {
        let line = r#"{"type":"done","rows":17,"truncated":true,"elapsed_ms":250}"#;
        match parse_frame(line).unwrap() {
            Frame::Done {
                rows,
                truncated,
                elapsed_ms,
            } => {
                assert_eq!(rows, 17);
                assert!(truncated);
                assert_eq!(elapsed_ms, 250);
            }
            _ => panic!("expected Done"),
        }
    }

    #[test]
    fn csv_escape_quotes_embedded_commas() {
        assert_eq!(csv_escape("hello", ','), "hello");
        assert_eq!(csv_escape("a,b", ','), "\"a,b\"");
        assert_eq!(csv_escape("a\"b", ','), "\"a\"\"b\"");
        assert_eq!(csv_escape("multi\nline", ','), "\"multi\nline\"");
        assert_eq!(csv_escape("a,b", '\t'), "a,b");
        assert_eq!(csv_escape("a\tb", '\t'), "\"a\tb\"");
    }

    #[test]
    fn cell_string_flattens_null_to_empty() {
        assert_eq!(cell_string(&Value::Null), "");
        assert_eq!(cell_string(&Value::String("x".into())), "x");
        assert_eq!(cell_string(&Value::Bool(true)), "true");
    }

    #[test]
    fn match_service_by_path_resolves_rust_service() {
        use crate::workspace_config::WorkspaceService;

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join("backend/stock-service")).unwrap();
        std::fs::create_dir_all(root.join("apps/portal")).unwrap();

        let services = vec![
            WorkspaceService {
                name: "stock-service".into(),
                path: "backend/stock-service".into(),
                seed: None,
                preview_tables: None,
            },
            WorkspaceService {
                name: "portal".into(),
                path: "apps/portal".into(),
                seed: None,
                preview_tables: None,
            },
        ];

        let got = match_service_by_path(&services, root, &root.join("backend/stock-service"));
        assert_eq!(got.as_deref(), Some("stock-service"));

        let got = match_service_by_path(&services, root, &root.join("apps/portal"));
        assert_eq!(got.as_deref(), Some("portal"));

        let got = match_service_by_path(&services, root, &root.join("does/not/exist"));
        assert!(got.is_none());
    }
}
