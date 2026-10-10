use cufflink_fn::params;
use cufflink_fn::prelude::*;
use cufflink_fn::sql::{DbError, Param};

cufflink_fn::init!();

fn respond(result: Result<Value, DbError>) -> Response {
    match result {
        Ok(value) => Response::json(&value),
        Err(e) => Response::json(&json!({"error": {"kind": e.kind.as_str(), "message": e.message}}))
            .with_status(500),
    }
}

fn outcome<T>(result: Result<T, DbError>) -> Value {
    match result {
        Ok(_) => json!({"ok": true}),
        Err(e) => json!({"kind": e.kind.as_str(), "code": e.code, "constraint": e.constraint}),
    }
}

const SCHEMA: &[&str] = &[
    "CREATE TYPE mood AS ENUM ('sad', 'happy')",
    "CREATE TABLE kinds (id uuid PRIMARY KEY, amount numeric(30, 6), local timestamp, \
     tags text[], counts int[], mood mood, doc jsonb, raw bytea, pt point)",
    "CREATE TABLE accounts (id int PRIMARY KEY, balance int NOT NULL)",
    "INSERT INTO accounts VALUES (1, 100)",
    "CREATE TABLE owned (id int PRIMARY KEY, owner text NOT NULL)",
    "INSERT INTO owned VALUES (1, 'a'), (2, 'b'), (3, 'a')",
    "CREATE VIEW scoped AS SELECT * FROM owned WHERE owner = current_setting('app.scope', true)",
    "CREATE TABLE parents (id int PRIMARY KEY)",
    "INSERT INTO parents VALUES (1)",
    "CREATE TABLE children (id int PRIMARY KEY, parent int REFERENCES parents (id), \
     email text NOT NULL CONSTRAINT children_email_key UNIQUE, \
     age int CONSTRAINT children_age_check CHECK (age >= 0))",
    "INSERT INTO children VALUES (1, 1, 'a@x', 1)",
    "CREATE TABLE ledger (id int PRIMARY KEY, note text NOT NULL)",
];

cufflink_fn::handler!(setup, |_req: Request| {
    respond(
        SCHEMA
            .iter()
            .try_for_each(|statement| sql::execute(statement, &[]).map(drop))
            .map(|()| json!({"ok": true})),
    )
});

cufflink_fn::handler!(types, |_req: Request| {
    let id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    respond((|| {
        sql::execute(
            "INSERT INTO kinds (id, amount, local, tags, counts, mood, doc, raw, pt) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '(1,2)')",
            &params![
                Param::Uuid(id.into()),
                Param::Numeric("12345678901234567890.123456".into()),
                Param::NaiveTimestamp("2026-03-04 05:06:07".into()),
                vec!["a", "b"],
                vec![1i64, 2],
                "happy",
                json!({"a": [1, null]}),
                vec![0u8, 255],
            ],
        )?;
        let row = sql::query_one(
            "SELECT id, amount, local, tags, counts, mood, doc, raw FROM kinds WHERE id = $1",
            &params![id],
        )?
        .map(|row| row.into_value());
        let unsupported = sql::query("SELECT pt FROM kinds", &[]);
        let amount: String = sql::query("SELECT amount FROM kinds", &[])?[0].get("amount")?;
        Ok(json!({"row": row, "amount": amount, "unsupported": outcome(unsupported)}))
    })())
});

cufflink_fn::handler!(errors, |_req: Request| {
    let insert = |id: i32, parent: i32, email: Option<&str>, age: i32| {
        sql::execute(
            "INSERT INTO children (id, parent, email, age) VALUES ($1, $2, $3, $4)",
            &params![id, parent, email, age],
        )
    };
    respond(Ok(json!({
        "unique": outcome(insert(2, 1, Some("a@x"), 1)),
        "foreign_key": outcome(insert(2, 9, Some("b@x"), 1)),
        "check": outcome(insert(2, 1, Some("b@x"), -1)),
        "not_null": outcome(insert(2, 1, None, 1)),
        "raise": outcome(sql::execute("DO $$ BEGIN RAISE EXCEPTION 'not today'; END $$", &[])),
        "timeout": outcome(sql::query("SELECT pg_sleep(30)", &[])),
        "legacy_query": db::query("SELECT * FROM missing"),
        "legacy_execute": db::execute("INSERT INTO missing VALUES (1)"),
    })))
});

cufflink_fn::handler!(withdraw, |req: Request| {
    let amount = req.body()["amount"].as_i64().unwrap_or(0);
    respond(
        sql::transaction(|tx| {
            let balance: i64 = tx
                .query_one("SELECT balance FROM accounts WHERE id = 1 FOR UPDATE", &[])?
                .map(|row| row.get("balance"))
                .transpose()?
                .unwrap_or(0);
            tx.query("SELECT pg_sleep(0.2)", &[])?;
            let withdrew = balance >= amount;
            if withdrew {
                tx.execute(
                    "UPDATE accounts SET balance = balance - $1 WHERE id = 1",
                    &params![amount],
                )?;
            }
            Ok(withdrew)
        })
        .map(|withdrew| json!({"withdrew": withdrew})),
    )
});

cufflink_fn::handler!(balance, |_req: Request| {
    respond(
        sql::query("SELECT balance FROM accounts WHERE id = 1", &[])
            .and_then(|rows| rows[0].get::<i64>("balance"))
            .map(|balance| json!({"balance": balance})),
    )
});

cufflink_fn::handler!(scoped, |_req: Request| {
    respond((|| {
        let ids: Vec<i64> = sql::transaction(|tx| {
            tx.set_local("app.scope", "a")?;
            tx.query("SELECT id FROM scoped ORDER BY id", &[])?
                .iter()
                .map(|row| row.get("id"))
                .collect()
        })?;
        let after = sql::query("SELECT current_setting('app.scope', true) AS s", &[])?[0]
            .try_get::<String>("s")?;
        Ok(json!({"ids": ids, "after": after}))
    })())
});

cufflink_fn::handler!(advisory, |_req: Request| {
    respond((|| {
        let inner_got = sql::transaction(|outer| {
            outer.advisory_xact_lock(42)?;
            sql::transaction(|inner| inner.try_advisory_xact_lock(42))
        })?;
        let later_got = sql::transaction(|tx| tx.try_advisory_xact_lock(42))?;
        Ok(json!({"inner_got": inner_got, "later_got": later_got}))
    })())
});

fn notes() -> Result<Vec<String>, DbError> {
    sql::query("SELECT note FROM ledger ORDER BY id", &[])?
        .iter()
        .map(|row| row.get("note"))
        .collect()
}

cufflink_fn::handler!(savepoints, |_req: Request| {
    respond((|| {
        let inner = sql::transaction(|tx| {
            tx.execute("INSERT INTO ledger VALUES (10, 'outer')", &[])?;
            let inner = tx.savepoint(|sp| sp.execute("INSERT INTO ledger VALUES (10, 'dup')", &[]));
            tx.execute("INSERT INTO ledger VALUES (11, 'after')", &[])?;
            Ok(outcome(inner))
        })?;
        Ok(json!({"inner": inner, "notes": notes()?}))
    })())
});

cufflink_fn::handler!(ledger, |_req: Request| {
    respond(notes().map(|notes| json!({"notes": notes})))
});

cufflink_fn::handler!(limit, |_req: Request| {
    respond(Ok(outcome(sql::transaction(|_| {
        sql::transaction(|_| sql::transaction(|_| Ok(())))
    }))))
});

cufflink_fn::handler!(abandon, |_req: Request| {
    let _ = sql::transaction(|tx| -> Result<(), DbError> {
        tx.execute("INSERT INTO ledger VALUES (99, 'abandoned')", &[])?;
        loop {
            std::hint::spin_loop();
        }
    });
    Response::json(&json!({"unreachable": true}))
});

cufflink_fn::handler!(titles_in_scope, |req: Request| {
    let Some(scope) = req.scope() else {
        return Response::error("no scope");
    };
    let mut params = Vec::new();
    let condition = scope.to_sql(&mut params);
    respond(
        sql::query(
            &format!("SELECT title FROM accounts WHERE {condition} ORDER BY title"),
            &params,
        )
        .and_then(|rows| rows.iter().map(|row| row.get::<String>("title")).collect())
        .map(|titles: Vec<String>| json!({"titles": titles})),
    )
});
