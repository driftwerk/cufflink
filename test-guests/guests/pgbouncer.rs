use cufflink_fn::params;
use cufflink_fn::prelude::*;
use cufflink_fn::sql::DbError;

cufflink_fn::init!();

fn respond(result: Result<Value, DbError>) -> Response {
    match result {
        Ok(value) => Response::json(&value),
        Err(e) => Response::json(&json!({"error": {"kind": e.kind.as_str(), "message": e.message}}))
            .with_status(500),
    }
}

fn kind<T>(result: Result<T, DbError>) -> Value {
    result.map_or_else(|e| json!(e.kind.as_str()), |_| json!("ok"))
}

cufflink_fn::handler!(setup, |_req: Request| {
    respond(
        [
            "CREATE TABLE counters (id int PRIMARY KEY, n int NOT NULL)",
            "INSERT INTO counters VALUES (1, 0)",
        ]
        .iter()
        .try_for_each(|statement| sql::execute(statement, &[]).map(drop))
        .map(|()| json!({"ok": true})),
    )
});

cufflink_fn::handler!(bump, |req: Request| {
    let marker = req.body()["marker"].as_str().unwrap_or_default().to_string();
    respond(
        sql::transaction(|tx| {
            tx.set_local("app.marker", &marker)?;
            tx.advisory_xact_lock(7)?;
            tx.savepoint(|sp| sp.execute("UPDATE counters SET n = n + 1 WHERE id = 1", &[]))?;
            let seen: String = tx
                .query("SELECT current_setting('app.marker') AS m", &[])?
                .first()
                .map(|row| row.get("m"))
                .transpose()?
                .unwrap_or_default();
            Ok(json!({"marker_kept": seen == marker}))
        }),
    )
});

cufflink_fn::handler!(leak, |_req: Request| {
    respond((|| {
        let marker: Option<String> = sql::query("SELECT current_setting('app.marker', true) AS m", &[])?
            .first()
            .map(|row| row.try_get("m"))
            .transpose()?
            .flatten();
        let leaked: Option<String> = sql::query("SELECT current_setting('app.leaked', true) AS l", &[])?
            .first()
            .map(|row| row.try_get("l"))
            .transpose()?
            .flatten();
        let read_only = db::query_read_only(
            "SELECT current_setting('search_path') AS sp, current_setting('app.marker', true) AS m",
        );
        Ok(json!({
            "marker": marker,
            "leaked": leaked,
            "read_only": read_only.unwrap_or_else(|e| json!({"error": e})),
        }))
    })())
});

cufflink_fn::handler!(count, |_req: Request| {
    respond(
        sql::query("SELECT n FROM counters WHERE id = $1", &params![1])
            .and_then(|rows| rows[0].get::<i64>("n"))
            .map(|n| json!({"n": n})),
    )
});

cufflink_fn::handler!(legacy, |_req: Request| {
    Response::json(&json!({
        "execute": db::execute("INSERT INTO counters VALUES (2, 5)"),
        "query": db::query("SELECT n FROM counters WHERE id = 2"),
        "execute_all": db::execute_all(&[
            "UPDATE counters SET n = n + 1 WHERE id = 2",
            "UPDATE counters SET n = n + 1 WHERE id = 2",
        ]),
        "read_only": db::query_read_only("SELECT 1 AS one").map(|v| v["rows"].clone()),
    }))
});

cufflink_fn::handler!(concurrently, |_req: Request| {
    Response::json(&json!({
        "v2": kind(sql::execute("CREATE INDEX CONCURRENTLY counters_n ON counters (n)", &[])),
        "legacy": db::execute("CREATE INDEX CONCURRENTLY counters_n ON counters (n)"),
    }))
});

cufflink_fn::handler!(timeout, |_req: Request| {
    Response::json(&json!({ "timeout": kind(sql::query("SELECT pg_sleep(30)", &[])) }))
});

cufflink_fn::handler!(timeout_in_transaction, |_req: Request| {
    Response::json(&json!({
        "timeout": kind(sql::transaction(|tx| tx.query("SELECT pg_sleep(30)", &[]))),
    }))
});

cufflink_fn::handler!(session_state, |_req: Request| {
    Response::json(&json!({
        "v2": kind(sql::execute("SET app.leaked = 'v2'", &[])),
        "v2_role": kind(sql::execute("SET ROLE postgres", &[])),
        "v2_reset": kind(sql::execute("RESET ALL", &[])),
        "v2_in_transaction": kind(sql::transaction(|tx| tx.execute("SET SESSION app.leaked = 'tx'", &[]))),
        "set_local": kind(sql::transaction(|tx| tx.execute("SET LOCAL app.local = 'ok'", &[]))),
        "legacy": db::execute("SET app.leaked = 'legacy'"),
        "legacy_query": db::query("SET app.leaked = 'legacy'"),
        "execute_all": db::execute_all(&[
            "UPDATE counters SET n = n WHERE id = 1",
            "SET app.leaked = 'batch'",
        ])
        .map_err(|e| e.starts_with("statement 2 of 2 failed")),
    }))
});
