use cufflink_testkit as common;

use common::{PlatformProcess, Tenant, TestDb};
use reqwest::Method;
use serde_json::{json, Value};

struct Crud {
    platform: PlatformProcess,
    tenant: Tenant,
    db: TestDb,
}

impl Crud {
    async fn start(manifest: &Value) -> Option<Self> {
        let infra = common::infra().await?;
        let mut db = infra.create_database().await;
        let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let (status, body) = platform.deploy(&tenant, manifest).await;
        assert_eq!(status, 200, "deploy failed: {body}");
        Some(Self {
            platform,
            tenant,
            db,
        })
    }

    async fn set_policy(&self, key: &str, mode: &str) {
        let admin = format!("ApiKey {}", self.db.mint_admin_key().await);
        let (status, body) = self
            .platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", self.tenant.slug),
                &admin,
                Some(&json!({"policies": {key: {"mode": mode}}})),
            )
            .await;
        assert_eq!(status, 200, "{body}");
    }

    async fn call(&self, method: Method, path: &str, body: Option<&Value>) -> (u16, Value) {
        self.platform
            .send(
                method,
                &format!("/svc/{}/shop{path}", self.tenant.slug),
                &format!("ApiKey {}", self.tenant.api_key),
                body,
            )
            .await
    }
}

fn column(name: &str, column_type: &str, nullable: bool) -> Value {
    json!({"name": name, "column_type": column_type, "primary_key": false, "nullable": nullable, "auto_generate": false})
}

fn keyed(extra: Vec<Value>) -> Vec<Value> {
    [
        json!({"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true}),
        json!({"name": "created_at", "column_type": "timestamp", "primary_key": false, "nullable": false, "auto_generate": true}),
    ]
    .into_iter()
    .chain(extra)
    .collect()
}

fn constraint_manifest() -> Value {
    let mut name = column("name", "text", false);
    name["unique"] = json!(true);
    let mut parent = column("parent_id", "uuid", false);
    parent["references"] = json!("parents.id");
    json!({
        "name": "shop",
        "version": "0.1.0",
        "mode": "crud",
        "tables": [
            {"name": "parents", "columns": keyed(vec![name])},
            {"name": "children", "columns": keyed(vec![parent, column("title", "text", false)])}
        ],
        "publishes": [],
    })
}

fn error_only(body: &Value) -> &str {
    let fields = body.as_object().expect("a JSON object body");
    assert_eq!(fields.len(), 1, "only the error key: {body}");
    fields["error"].as_str().expect("error string")
}

#[tokio::test]
async fn constraint_errors_keep_error_key() {
    let Some(crud) = Crud::start(&constraint_manifest()).await else {
        return;
    };
    let (status, parent) = crud
        .call(Method::POST, "/parents", Some(&json!({"name": "a"})))
        .await;
    assert_eq!(status, 200, "{parent}");

    let (status, body) = crud
        .call(Method::POST, "/parents", Some(&json!({"name": "a"})))
        .await;
    assert_eq!(status, 409, "{body}");
    let message = error_only(&body);
    assert!(
        message.starts_with("conflict: ") && message.contains("name"),
        "{message}"
    );

    let (status, body) = crud
        .call(
            Method::POST,
            "/children",
            Some(&json!({"parent_id": uuid::Uuid::new_v4(), "title": "orphan"})),
        )
        .await;
    assert_eq!(status, 422, "{body}");
    assert!(
        error_only(&body).starts_with("invalid reference ("),
        "{body}"
    );

    let (status, body) = crud
        .call(
            Method::POST,
            "/children",
            Some(&json!({"parent_id": parent["id"], "title": null})),
        )
        .await;
    assert_eq!(status, 422, "{body}");
    assert_eq!(error_only(&body), "constraint violated: title");

    let (status, child) = crud
        .call(
            Method::POST,
            "/children",
            Some(&json!({"parent_id": parent["id"], "title": "kept"})),
        )
        .await;
    assert_eq!(status, 200, "{child}");
    let parent_path = format!("/parents/{}", parent["id"].as_str().expect("id"));
    let (status, body) = crud.call(Method::DELETE, &parent_path, None).await;
    assert_eq!(status, 409, "{body}");
    assert!(
        error_only(&body).starts_with("conflict: row is referenced ("),
        "{body}"
    );

    let (status, body) = crud
        .call(
            Method::POST,
            "/parents/batch",
            Some(&json!({"operations": [
                {"action": "create", "data": {"name": "b"}},
                {"action": "create", "data": {"name": "b"}}
            ]})),
        )
        .await;
    assert_eq!(status, 409, "{body}");
    error_only(&body);
    let (_, listed) = crud.call(Method::GET, "/parents?name=b", None).await;
    assert_eq!(listed["results"], json!([]), "the batch rolled back");

    let child_path = format!("/children/{}", child["id"].as_str().expect("id"));
    let (status, body) = crud.call(Method::DELETE, &child_path, None).await;
    assert_eq!((status, body), (200, json!({"deleted": true})));
    let (status, body) = crud.call(Method::DELETE, &parent_path, None).await;
    assert_eq!(
        (status, body),
        (200, json!({"deleted": true})),
        "delete still answers a JSON body"
    );
}

fn items_manifest() -> Value {
    json!({
        "name": "shop",
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{
            "name": "items",
            "columns": keyed(vec![
                column("title", "text", false),
                column("status", "text", true),
                column("rank", "integer", true),
            ]),
        }],
        "publishes": [],
    })
}

async fn seeded() -> Option<Crud> {
    let crud = Crud::start(&items_manifest()).await?;
    for (title, status, rank) in [
        ("a", "new", 1),
        ("b", "open", 2),
        ("c", "open", 3),
        ("d", "closed", 4),
        ("e", "closed", 5),
    ] {
        let (code, body) = crud
            .call(
                Method::POST,
                "/items",
                Some(&json!({"title": title, "status": status, "rank": rank})),
            )
            .await;
        assert_eq!(code, 200, "{body}");
    }
    Some(crud)
}

fn encode(query: &[(&str, &str)]) -> String {
    let pairs: Vec<String> = query
        .iter()
        .map(|(k, v)| {
            let value: String = v
                .bytes()
                .map(|b| match b {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => {
                        char::from(b).to_string()
                    }
                    _ => format!("%{b:02X}"),
                })
                .collect();
            format!("{k}={value}")
        })
        .collect();
    format!("?{}", pairs.join("&"))
}

impl Crud {
    async fn list(&self, query: &[(&str, &str)]) -> (u16, Value) {
        self.call(Method::GET, &format!("/items{}", encode(query)), None)
            .await
    }
}

fn titles(body: &Value) -> Vec<String> {
    let mut titles: Vec<String> = body["results"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|r| r["title"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    titles.sort();
    titles
}

#[tokio::test]
async fn crud_v1_unchanged() {
    let Some(crud) = seeded().await else {
        return;
    };
    let (status, body) = crud.list(&[("per_page", "1000")]).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["pagination"],
        json!({"current_page": 1, "per_page": 200, "total_records": 5, "total_pages": 1}),
        "per_page clamps silently"
    );
    let (_, body) = crud.list(&[("per_page", "2"), ("page", "99")]).await;
    assert_eq!(
        body["pagination"]["current_page"], 3,
        "page clamps to the last"
    );
    assert_eq!(
        titles(&body),
        vec!["a"],
        "newest first, so the oldest is last"
    );
    let (_, body) = crud.list(&[("ghost", "1"), ("filter", "{not json")]).await;
    assert_eq!(
        titles(&body).len(),
        5,
        "unknown columns and bad filters are ignored"
    );
    let same_column = r#"[{"n":"status","f":"=","v":"new"},{"n":"status","f":"=","v":"closed"}]"#;
    let (_, body) = crud.list(&[("filter", same_column)]).await;
    assert_eq!(
        titles(&body),
        vec!["a", "d", "e"],
        "same-column filters are OR-ed"
    );
    let (_, body) = crud
        .list(&[
            ("status", "open"),
            ("filter", r#"[{"n":"rank","f":">=","v":"3"}]"#),
        ])
        .await;
    assert_eq!(
        titles(&body),
        vec!["c"],
        "?column=value merges with the filter"
    );
    let (status, body) = crud
        .list(&[("filter", r#"[{"n":"rank","f":"=","v":"x"}]"#)])
        .await;
    assert_eq!(
        (status, titles(&body)),
        (200, vec![]),
        "a bad value is coerced, not refused"
    );
    let (_, body) = crud
        .list(&[("filter", r#"[{"n":"rank","f":"IN","v":"1, 5"}]"#)])
        .await;
    assert_eq!(titles(&body), vec!["a", "e"]);
    let (status, row) = crud
        .call(
            Method::POST,
            "/items",
            Some(&json!({"title": "f", "rank": "x"})),
        )
        .await;
    assert_eq!(
        (status, row["rank"].clone()),
        (200, json!(0)),
        "v1 writes coerce"
    );
}

#[tokio::test]
async fn crud_v2_filters_and_errors() {
    let Some(crud) = seeded().await else {
        return;
    };
    let v2 = |extra: &[(&'static str, &'static str)]| {
        let query: Vec<(&str, &str)> = [("filter_mode", "v2")]
            .into_iter()
            .chain(extra.iter().copied())
            .collect();
        let crud = &crud;
        async move { crud.list(&query).await }
    };
    let (status, body) = v2(&[(
        "filter",
        r#"{"or": [{"f": "status", "op": "in", "v": ["new", "closed"]}, {"f": "rank", "op": "eq", "v": 2}]}"#,
    )])
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(titles(&body), vec!["a", "b", "d", "e"]);
    let (_, body) = v2(&[(
        "filter",
        r#"[{"f": "created_at", "op": "gte", "v": "2020-01-01T00:00:00Z"}, {"f": "created_at", "op": "lt", "v": "2021-01-01T00:00:00Z"}]"#,
    )])
    .await;
    assert_eq!(titles(&body), Vec::<String>::new(), "a date range");
    let (_, body) = v2(&[(
        "filter",
        r#"[{"n":"status","f":"=","v":"new"},{"n":"status","f":"=","v":"open","c":"OR"},{"n":"rank","f":">","v":"1"}]"#,
    )])
    .await;
    assert_eq!(titles(&body), vec!["b", "c"], "v1 connectors are honoured");
    let (_, body) = v2(&[(
        "filter",
        r#"{"not": {"f": "status", "op": "eq", "v": "closed"}}"#,
    )])
    .await;
    assert_eq!(titles(&body), vec!["a", "b", "c"]);

    for (query, message) in [
        (
            vec![("filter", r#"{"f": "ghost", "op": "eq", "v": 1}"#)],
            "filter: unknown field 'ghost'",
        ),
        (
            vec![("filter", r#"{"f": "rank", "op": "eq", "v": "x"}"#)],
            "filter: column 'rank'",
        ),
        (vec![("filter", "{bad")], "filter: not valid JSON"),
        (vec![("rank", "x")], "filter: column 'rank'"),
        (
            vec![("per_page", "501")],
            "per_page must be between 1 and 500",
        ),
        (vec![("count", "some")], "count must be"),
    ] {
        let (status, body) = v2(&query).await;
        assert_eq!(status, 400, "{query:?}: {body}");
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|e| e.starts_with(message)),
            "{body}"
        );
    }
    let (status, body) = v2(&[("per_page", "2"), ("page", "4")]).await;
    assert_eq!(status, 404, "{body}");
    assert_eq!(body["error"], "page 4 is past the last page (3)");

    let (status, body) = v2(&[
        ("per_page", "2"),
        ("page", "2"),
        ("count", "none"),
        ("sort", "rank"),
    ])
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(titles(&body), vec!["c", "d"]);
    assert_eq!(
        body["pagination"],
        json!({"current_page": 2, "per_page": 2, "has_more": true})
    );
    let (_, body) = v2(&[("per_page", "2"), ("page", "3"), ("count", "none")]).await;
    assert_eq!(body["pagination"]["has_more"], false);
    let (status, body) = crud
        .list(&[("count", "estimate"), ("status", "open")])
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["pagination"]["total_records_estimated"], true);
    assert!(body["pagination"]["total_records"].is_i64(), "{body}");
    let (_, body) = crud.list(&[("count", "estimate")]).await;
    assert_eq!(
        body["pagination"]["total_records_estimated"], true,
        "{body}"
    );

    let (status, body) = crud
        .call(
            Method::POST,
            "/items?filter_mode=v2",
            Some(&json!({"title": "f", "rank": "x"})),
        )
        .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body["error"],
        r#"column 'rank': expected a 32-bit integer, got "x""#
    );

    let (status, row) = crud
        .call(
            Method::POST,
            "/items?filter_mode=v2",
            Some(&json!({"title": "padded", "rank": " 7"})),
        )
        .await;
    assert_eq!((status, row["rank"].clone()), (200, json!(7)), "{row}");
    let path = format!("/items/{}?filter_mode=v2", row["id"].as_str().expect("id"));
    let (status, body) = crud
        .call(Method::PUT, &path, Some(&json!({"rank": "7x"})))
        .await;
    assert_eq!(status, 400, "a strict update is typed too: {body}");
    let (status, body) = crud
        .call(Method::PUT, &path, Some(&json!({"rank": " 8"})))
        .await;
    assert_eq!((status, body["rank"].clone()), (200, json!(8)), "{body}");
    let (status, body) = crud
        .call(
            Method::POST,
            "/items/batch?filter_mode=v2",
            Some(&json!({"operations": [
                {"action": "create", "data": {"title": "g", "rank": 1}},
                {"action": "create", "data": {"title": "h", "rank": "1.5"}}
            ]})),
        )
        .await;
    assert_eq!(status, 400, "{body}");
    let (_, body) = crud.list(&[("title", "g")]).await;
    assert!(titles(&body).is_empty(), "a refused batch writes nothing");
    let (status, row) = crud
        .call(
            Method::POST,
            "/items",
            Some(&json!({"title": "legacy", "rank": "7x"})),
        )
        .await;
    assert_eq!(
        (status, row["rank"].clone()),
        (200, json!(0)),
        "v1 writes keep their coercion: {row}"
    );

    let far = i64::MAX.to_string();
    for count in ["none", "estimate"] {
        let query = [
            ("filter_mode", "v2"),
            ("count", count),
            ("page", far.as_str()),
        ];
        let (status, body) = crud.list(&query).await;
        assert_eq!(status, 400, "{count}: {body}");
    }
}

#[tokio::test]
async fn strict_filters_policy_warns_then_enforces() {
    let Some(crud) = seeded().await else {
        return;
    };
    let malformed = [("filter", "{bad")];
    crud.set_policy("strict_filters", "warn").await;
    let (status, body) = crud.list(&malformed).await;
    assert_eq!((status, titles(&body).len()), (200, 5), "warn keeps v1");
    let recorded = crud
        .platform
        .metric(
            "cufflink_policy_violation_total",
            &[("key", "strict_filters"), ("reason", "would_reject")],
        )
        .await;
    assert_eq!(recorded, Some(1.0));

    crud.set_policy("strict_filters", "enforce").await;
    let (status, body) = crud.list(&malformed).await;
    assert_eq!(status, 400, "enforce makes every listing v2: {body}");
    let (status, _) = crud
        .list(&[("filter_mode", "v1"), ("filter", "{bad")])
        .await;
    assert_eq!(status, 400, "the parameter cannot opt out");
    let (status, _) = crud
        .call(
            Method::POST,
            "/items",
            Some(&json!({"title": "f", "rank": "x"})),
        )
        .await;
    assert_eq!(status, 400, "writes are checked too");
}
