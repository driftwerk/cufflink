use cufflink_testkit as common;

use common::Deployment;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::BTreeMap;

fn tickets() -> Value {
    json!({
        "name": "desk", "version": "0.1.0", "mode": "crud", "publishes": [],
        "tables": [{
            "name": "tickets",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                {"name": "queue", "column_type": "text", "primary_key": false, "nullable": false},
                {"name": "rank", "column_type": "integer", "primary_key": false, "nullable": false},
                {"name": "note", "column_type": "text", "primary_key": false, "nullable": true},
                {"name": "created_at", "column_type": "timestamp", "primary_key": false,
                 "nullable": false, "auto_generate": true},
            ],
            "indexes": [{"name": "tickets_by_queue", "columns": ["queue", "rank", "id"], "unique": false}],
            "pagination": "keyset",
        }]
    })
}

struct Desk(Deployment);

impl Desk {
    async fn start() -> Option<Self> {
        let d = Deployment::start(&[]).await?;
        d.deployed(json!({"manifest": tickets()})).await;
        Some(Self(d))
    }

    async fn insert(&self, from: i32, count: i32, queue: impl Fn(i32) -> String) {
        for n in from..from + count {
            self.0
                .database
                .execute(
                    "INSERT INTO desk.tickets (id, queue, rank, note) VALUES ($1, $2, $3, $4)",
                    &[&uuid::Uuid::new_v4(), &queue(n), &(n % 7), &format!("t{n}")],
                )
                .await
                .expect("insert");
        }
    }

    async fn page(&self, rest: &str) -> (u16, Value) {
        let path = format!("/svc/{}/desk/tickets{rest}", self.0.tenant.slug);
        self.0.send(Method::GET, &path, None).await
    }

    async fn walk(&self, query: &str, added_at: Option<usize>) -> BTreeMap<String, usize> {
        let mut seen = BTreeMap::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0;
        loop {
            let rest = match &cursor {
                Some(cursor) => format!("{query}&cursor={cursor}"),
                None => query.to_string(),
            };
            let (status, body) = self.page(&rest).await;
            assert_eq!(status, 200, "{body}");
            for row in body["results"].as_array().expect("results") {
                *seen
                    .entry(row["note"].as_str().expect("note").to_string())
                    .or_insert(0) += 1;
            }
            pages += 1;
            if added_at == Some(pages) {
                self.insert(1000, 5, |_| "zzz".into()).await;
            }
            let pagination = &body["pagination"];
            assert_eq!(pagination["per_page"], 37, "{pagination}");
            match (
                pagination["has_more"].as_bool(),
                pagination["next_cursor"].as_str(),
            ) {
                (Some(true), Some(next)) => cursor = Some(next.to_string()),
                (Some(false), None) => return seen,
                other => panic!("{other:?}: {pagination}"),
            }
        }
    }
}

#[tokio::test]
async fn keyset_pagination_walks_all_rows() {
    let Some(d) = Desk::start().await else {
        return;
    };
    d.insert(0, 1000, |n| format!("q{}", n % 13)).await;

    let seen = d.walk("?sort=queue,-rank&per_page=37", Some(3)).await;
    assert_eq!(seen.len(), 1005, "every row, those added at the end too");
    assert!(seen.values().all(|times| *times == 1), "each row once");

    let ordered = d.walk("?per_page=37", None).await;
    assert_eq!(ordered.len(), 1005);
    let by_key: i64 =
        d.0.database
            .query_one("SELECT count(*) FROM desk.tickets", &[])
            .await
            .expect("count")
            .get(0);
    assert_eq!(by_key, 1005);
}

#[tokio::test]
async fn a_keyset_page_says_what_it_is_and_refuses_what_it_is_not() {
    let Some(d) = Desk::start().await else {
        return;
    };
    d.insert(0, 5, |_| "q".into()).await;

    let (status, first) = d.page("?per_page=2&select=note").await;
    assert_eq!(status, 200, "{first}");
    assert_eq!(first["results"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        first["results"][0]
            .as_object()
            .map(|row| row.keys().collect::<Vec<_>>()),
        Some(vec![&"note".to_string()]),
        "the keys are read, not rendered: {first}"
    );
    assert_eq!(first["pagination"]["has_more"], true);
    assert!(
        first["pagination"].get("total_records").is_none(),
        "{first}"
    );
    let cursor = first["pagination"]["next_cursor"]
        .as_str()
        .expect("cursor")
        .to_string();
    let (status, second) = d.page(&format!("?per_page=2&cursor={cursor}")).await;
    assert_eq!(status, 200, "{second}");
    assert_ne!(second["results"][0]["note"], first["results"][0]["note"]);

    let (status, counted) = d.page("?per_page=2&count=exact").await;
    assert_eq!(status, 200, "{counted}");
    assert_eq!(counted["pagination"]["total_records"], 5, "{counted}");

    let (status, body) = d.page("?page=2").await;
    assert_eq!(status, 400, "{body}");
    let (status, body) = d.page("?sort=note").await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("may be null")),
        "{body}"
    );
    let (status, body) = d
        .page(&format!("?per_page=2&sort=rank&cursor={cursor}"))
        .await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("of another sort")),
        "{body}"
    );
    let (status, body) = d.page("?cursor=nonsense").await;
    assert_eq!(status, 400, "{body}");

    let (status, last) = d.page("?per_page=10").await;
    assert_eq!(status, 200, "{last}");
    assert_eq!(last["pagination"]["has_more"], false);
    assert_eq!(last["pagination"]["next_cursor"], Value::Null);
    let (_, version) =
        d.0.platform
            .get_json("/api/version")
            .await
            .expect("version");
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|s| s.contains(&json!("keyset_lists"))),
        "{version}"
    );
}

const BOARD: &str = "\
    CREATE SCHEMA board; \
    CREATE TYPE board.stage AS ENUM ('new', 'open', 'won', 'lost'); \
    CREATE TABLE board.cards ( \
        id uuid PRIMARY KEY DEFAULT gen_random_uuid(), \
        stage board.stage NOT NULL, \
        title text NOT NULL); \
    INSERT INTO board.cards (stage, title) \
        SELECT (ARRAY['lost', 'won', 'open', 'new']::board.stage[])[1 + n % 4], 'card ' || n \
        FROM generate_series(1, 30) AS n; \
    CREATE TABLE board.lanes (id uuid PRIMARY KEY, stage board.stage NOT NULL, title text NOT NULL); \
    INSERT INTO board.lanes SELECT * FROM board.cards";

#[tokio::test]
async fn an_adopted_table_is_paged_by_the_order_of_its_enum() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    d.database
        .batch_execute(BOARD)
        .await
        .expect("a table made before the platform adopts it");
    let column = |name: &str, key: bool| {
        json!({"name": name, "column_type": if key { "uuid" } else { "text" },
               "primary_key": key, "nullable": false})
    };
    d.deployed(json!({"manifest": {
        "name": "board", "version": "0.1.0", "mode": "crud", "publishes": [],
        "schema": "adopted",
        "tables": [
            {
                "name": "cards",
                "columns": [column("id", true), column("stage", false), column("title", false)],
                "pagination": "keyset",
            },
            {
                "name": "lanes",
                "columns": [column("id", true), column("stage", false), column("title", false)],
            },
        ],
    }}))
    .await;
    let (mut stages, mut titles) = (Vec::new(), std::collections::BTreeSet::new());
    let mut cursor: Option<String> = None;
    loop {
        let rest = cursor
            .as_ref()
            .map_or(String::new(), |cursor| format!("&cursor={cursor}"));
        let path = format!(
            "/svc/{}/board/cards?sort=stage&per_page=7{rest}",
            d.tenant.slug
        );
        let (status, body) = d.send(Method::GET, &path, None).await;
        assert_eq!(status, 200, "{body}");
        for row in body["results"].as_array().expect("results") {
            stages.push(row["stage"].as_str().expect("stage").to_string());
            titles.insert(row["title"].as_str().expect("title").to_string());
        }
        match body["pagination"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_string()),
            None => break,
        }
    }
    let expected: Vec<String> = [("new", 7), ("open", 8), ("won", 8), ("lost", 7)]
        .into_iter()
        .flat_map(|(stage, count)| std::iter::repeat_n(stage.to_string(), count))
        .collect();
    assert_eq!(stages, expected, "in the order of the enum");
    assert_eq!(titles.len(), 30, "every card once");

    let listed = |select: &'static str| {
        let path = format!(
            "/svc/{}/board/lanes?sort=stage,title&per_page=30{select}",
            d.tenant.slug
        );
        let d = &d;
        async move {
            let (status, body) = d.send(Method::GET, &path, None).await;
            assert_eq!(status, 200, "{body}");
            body["results"]
                .as_array()
                .expect("results")
                .iter()
                .map(|row| row["title"].as_str().expect("title").to_string())
                .collect::<Vec<_>>()
        }
    };
    let all = listed("").await;
    assert_eq!(all, listed("&select=title").await);
    let first: Vec<&str> = all.iter().take(2).map(String::as_str).collect();
    assert_eq!(
        first,
        ["card 11", "card 15"],
        "a card that is new comes first"
    );
}
