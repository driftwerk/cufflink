use cufflink_fn::prelude::*;
use cufflink_fn::sql::{DbError, Row};

cufflink_fn::init!();

fn sql_of(req: &Request) -> String {
    req.body()["sql"].as_str().unwrap_or_default().to_string()
}

fn statements_of(req: &Request) -> Vec<String> {
    req.body()["statements"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|statement| statement.as_str().map(str::to_string))
        .collect()
}

fn failure(e: DbError) -> Value {
    json!({"error": {"kind": e.kind.as_str(), "message": e.message}})
}

fn rows(result: Result<Vec<Row>, DbError>) -> Value {
    result.map_or_else(failure, |rows| {
        json!({"rows": rows.into_iter().map(Row::into_value).collect::<Vec<_>>()})
    })
}

fn affected(result: Result<u64, DbError>) -> Value {
    result.map_or_else(failure, |affected| json!({"affected": affected}))
}

cufflink_fn::handler!(query, |req: Request| {
    Response::json(&rows(sql::query(&sql_of(&req), &[])))
});

cufflink_fn::handler!(execute, |req: Request| {
    Response::json(&affected(sql::execute(&sql_of(&req), &[])))
});

cufflink_fn::handler!(transaction, |req: Request| {
    let mut outcomes = Vec::new();
    let committed = sql::transaction(|tx| {
        outcomes = statements_of(&req)
            .iter()
            .map(|statement| rows(tx.query(statement, &[])))
            .collect();
        Ok(())
    });
    Response::json(&json!({
        "statements": outcomes,
        "committed": committed.map_or_else(failure, |()| json!(true)),
    }))
});

cufflink_fn::handler!(legacy, |req: Request| {
    Response::json(&json!({"rows": db::query(&sql_of(&req))}))
});

cufflink_fn::handler!(legacy_execute, |req: Request| {
    Response::json(&json!({"rc": db::execute(&sql_of(&req))}))
});

cufflink_fn::handler!(batch, |req: Request| {
    let statements = statements_of(&req);
    let statements: Vec<&str> = statements.iter().map(String::as_str).collect();
    Response::json(&match db::execute_all(&statements) {
        Ok(counts) => json!({"counts": counts}),
        Err(error) => json!({"error": error}),
    })
});

cufflink_fn::handler!(read_only, |req: Request| {
    Response::json(&match db::query_read_only(&sql_of(&req)) {
        Ok(answer) => json!({"rows": answer["rows"]}),
        Err(error) => json!({"error": error}),
    })
});

cufflink_fn::handler!(migrate, |req: Request| {
    cufflink_fn::migrate::run(req, |_diff| {
        [
            "CREATE TABLE IF NOT EXISTS migrated (who text NOT NULL, login text NOT NULL)",
            "INSERT INTO migrated SELECT current_user, session_user",
        ]
        .iter()
        .try_for_each(|statement| sql::execute(statement, &[]).map(drop))
        .map_err(|e| e.message.clone())
    })
});
