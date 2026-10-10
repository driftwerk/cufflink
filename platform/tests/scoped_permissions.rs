use cufflink_testkit as common;

use common::keycloak::MockKeycloak;
use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::repo::tenants::NewTenant;
use reqwest::Method;
use serde_json::{json, Value};

struct Scenario {
    keycloak: MockKeycloak,
    platform: PlatformProcess,
    tenant: Tenant,
    realm: String,
    _db: TestDb,
}

impl Scenario {
    async fn start() -> Option<Self> {
        let infra = common::infra().await?;
        let keycloak = MockKeycloak::start().await;
        let mut db = infra.create_database().await;
        let platform = PlatformProcess::start(
            &db.url,
            infra.nats_url(),
            &[
                ("KEYCLOAK_URL", keycloak.url.as_str()),
                ("KEYCLOAK_ISSUER", keycloak.issuer_base.as_str()),
            ],
        )
        .await;
        platform.wait_ready().await;
        let realm = format!("realm-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        let tenant = db
            .mint_tenant_with(NewTenant {
                keycloak_url: Some(&keycloak.url),
                keycloak_realm: Some(&realm),
                ..Default::default()
            })
            .await;
        Some(Self {
            keycloak,
            platform,
            tenant,
            realm,
            _db: db,
        })
    }

    fn key(&self) -> String {
        format!("ApiKey {}", self.tenant.api_key)
    }

    fn bearer(&self, claims: Value) -> String {
        format!("Bearer {}", self.keycloak.token(&self.realm, claims))
    }

    async fn deploy(&self, manifest: &Value) {
        let (status, body) = self.platform.deploy(&self.tenant, manifest).await;
        assert_eq!(status, 200, "deploy failed: {body}");
    }

    async fn create_role(&self, name: &str, permissions: &[&str]) -> (u16, Value) {
        self.platform
            .send(
                Method::POST,
                &format!("/api/tenants/{}/roles", self.tenant.id),
                &self.key(),
                Some(&json!({"name": name, "permissions": permissions})),
            )
            .await
    }

    async fn grant(&self, subject: &str, permissions: &[&str]) {
        let (status, role) = self
            .create_role(&format!("role-{subject}"), permissions)
            .await;
        assert_eq!(status, 200, "{role}");
        let (status, body) = self
            .platform
            .send(
                Method::POST,
                &format!("/api/tenants/{}/roles/assign", self.tenant.id),
                &self.key(),
                Some(&json!({"user_id": subject, "role_id": role["id"]})),
            )
            .await;
        assert_eq!(status, 200, "{body}");
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        authorization: &str,
        body: Option<&Value>,
    ) -> (u16, Value) {
        let path = format!("/svc/{}{path}", self.tenant.slug);
        self.platform.send(method, &path, authorization, body).await
    }
}

fn column(name: &str, column_type: &str) -> Value {
    json!({"name": name, "column_type": column_type, "primary_key": false, "nullable": true, "auto_generate": false})
}

fn id_column() -> Value {
    json!({"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true})
}

fn created_at() -> Value {
    json!({"name": "created_at", "column_type": "timestamp", "primary_key": false, "nullable": false, "auto_generate": true})
}

#[tokio::test]
async fn scoped_role_strings_back_compat() {
    let Some(s) = Scenario::start().await else {
        return;
    };
    s.deploy(&json!({
        "name": "catalog",
        "version": "0.1.0",
        "mode": "wasm",
        "tables": [{
            "name": "items",
            "columns": [id_column(), column("title", "text"), created_at()],
            "auth_required": true,
            "permission_area": "items"
        }],
        "custom_routes": [{
            "method": "POST",
            "path": "/echo",
            "handler": "echo",
            "posture": {"kind": "permission", "area": "items", "operation": "view"}
        }],
        "publishes": [],
    }))
    .await;

    let (status, body) = s.create_role("broken", &["items:view@everyone"]).await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("items:view@everyone")),
        "{body}"
    );

    s.grant("alice", &["items:view@all", "notes:edit@own"])
        .await;
    let alice = s.bearer(json!({"sub": "alice"}));

    let (status, body) = s.call(Method::GET, "/catalog/items", &alice, None).await;
    assert_eq!(
        status, 200,
        "an @all grant covers an unscoped table: {body}"
    );
    let (status, _) = s
        .call(
            Method::POST,
            "/catalog/items",
            &alice,
            Some(&json!({"title": "x"})),
        )
        .await;
    assert_eq!(status, 403, "it grants only its own operation");

    let (status, request) = s
        .call(Method::POST, "/catalog/_fn/echo", &alice, Some(&json!({})))
        .await;
    assert_eq!(
        status, 200,
        "a perm(...) route accepts the scoped grant: {request}"
    );
    assert_eq!(
        request["auth"]["permissions"],
        json!(["items:view", "notes:edit"])
    );
    assert_eq!(
        request["auth"]["scoped_permissions"],
        json!(["items:view@all", "notes:edit@own"])
    );

    s.grant("bob", &["items:view"]).await;
    let (status, request) = s
        .call(
            Method::POST,
            "/catalog/_fn/echo",
            &s.bearer(json!({"sub": "bob"})),
            Some(&json!({})),
        )
        .await;
    assert_eq!(status, 200, "{request}");
    assert_eq!(request["auth"]["permissions"], json!(["items:view"]));
    assert!(
        request["auth"].get("scoped_permissions").is_none(),
        "no scoped grants, no new key: {request}"
    );
}

fn scoped_manifest() -> Value {
    let text = |name: &str| column(name, "text");
    json!({
        "name": "library",
        "version": "0.1.0",
        "mode": "crud",
        "graphql": true,
        "authorization": {
            "areas": [
                {"name": "books", "operations": ["view", "create", "edit", "delete"]},
                {"name": "tickets", "operations": ["view", "create"]}
            ]
        },
        "tables": [
            {
                "name": "books",
                "columns": [id_column(), text("user_id"), text("branch_id"), text("title"), created_at()],
                "auth_required": true,
                "owner_field": "user_id",
                "scoped_by": "books"
            },
            {
                "name": "tickets",
                "columns": [id_column(), text("customer_id"), text("title"), created_at()],
                "auth_required": true,
                "owner_field": "customer_id",
                "owner_self_service": true,
                "scoped_by": "tickets"
            }
        ],
        "publishes": [],
    })
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
async fn scoped_crud_matrix() {
    let Some(s) = Scenario::start().await else {
        return;
    };
    s.deploy(&scoped_manifest()).await;
    s.grant("gina", &["books:*@all"]).await;
    s.grant("agnes", &["books:*@field(branch_id=branch_id)"])
        .await;
    s.grant("olga", &["books:*"]).await;
    s.grant("nomad", &["books:view@field(branch_id=branch_id)"])
        .await;
    let global = s.bearer(json!({"sub": "gina"}));
    let branch = s.bearer(json!({"sub": "agnes", "branch_id": "branch-a"}));
    let own = s.bearer(json!({"sub": "olga"}));
    let without_claim = s.bearer(json!({"sub": "nomad"}));
    let stranger = s.bearer(json!({"sub": "sam"}));

    let mut ids = std::collections::HashMap::new();
    for (title, user, org) in [
        ("olga-a", "olga", "branch-a"),
        ("other-a", "oscar", "branch-a"),
        ("other-b", "oscar", "branch-b"),
    ] {
        let (status, row) = s
            .call(
                Method::POST,
                "/library/books",
                &global,
                Some(&json!({"title": title, "user_id": user, "branch_id": org})),
            )
            .await;
        assert_eq!(status, 200, "an @all grant creates any row as given: {row}");
        assert_eq!(row["user_id"], user);
        ids.insert(title, row["id"].as_str().expect("id").to_string());
    }
    let row_path = |title: &str| format!("/library/books/{}", ids[title]);

    let list = |authorization: &str, query: &str| {
        let path = format!("/library/books{query}");
        let authorization = authorization.to_string();
        let s = &s;
        async move { s.call(Method::GET, &path, &authorization, None).await }
    };
    for (who, authorization, expected) in [
        ("global", &global, vec!["olga-a", "other-a", "other-b"]),
        ("branch", &branch, vec!["olga-a", "other-a"]),
        ("own", &own, vec!["olga-a"]),
        ("missing claim", &without_claim, vec![]),
    ] {
        let (status, body) = list(authorization, "").await;
        assert_eq!(status, 200, "{who}: {body}");
        assert_eq!(titles(&body), expected, "{who}");
        assert_eq!(body["pagination"]["total_records"], expected.len(), "{who}");
    }
    let (status, body) = list(&stranger, "").await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(body["error"], "Missing permission: books:view");
    let (status, body) = list(&own, "?scope=all").await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(body["error"], "scope=all not granted for books:view");
    let (status, body) = list(&global, "?scope=own").await;
    assert_eq!(
        (status, titles(&body)),
        (200, vec![]),
        "narrowed to own rows"
    );
    let (status, body) = list(&global, "?scope=all&branch_id=branch-b").await;
    assert_eq!((status, titles(&body)), (200, vec!["other-b".to_string()]));
    let (status, _) = list(&global, "?scope=mine").await;
    assert_eq!(status, 400);

    let (status, _) = s
        .call(Method::GET, &row_path("other-b"), &branch, None)
        .await;
    assert_eq!(status, 404, "a row outside the scope does not exist");
    let (status, row) = s
        .call(Method::GET, &row_path("other-a"), &branch, None)
        .await;
    assert_eq!((status, row["title"].clone()), (200, json!("other-a")));

    let (status, row) = s
        .call(
            Method::POST,
            "/library/books",
            &branch,
            Some(&json!({"title": "filled"})),
        )
        .await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(
        row["branch_id"], "branch-a",
        "a lone claim fills its column"
    );
    let (status, body) = s
        .call(
            Method::POST,
            "/library/books",
            &branch,
            Some(&json!({"title": "elsewhere", "branch_id": "branch-b"})),
        )
        .await;
    assert_eq!(status, 403, "{body}");
    let (status, row) = s
        .call(
            Method::POST,
            "/library/books",
            &own,
            Some(&json!({"title": "mine", "user_id": "oscar", "branch_id": "branch-z"})),
        )
        .await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["user_id"], "olga", "an own grant forces the owner");

    let (status, row) = s
        .call(
            Method::PUT,
            &row_path("other-a"),
            &branch,
            Some(&json!({"title": "other-a", "user_id": "oscar", "branch_id": "branch-a"})),
        )
        .await;
    assert_eq!(
        status, 200,
        "re-sending the row unchanged is allowed: {row}"
    );
    let (status, body) = s
        .call(
            Method::PUT,
            &row_path("other-a"),
            &branch,
            Some(&json!({"branch_id": "branch-b"})),
        )
        .await;
    assert_eq!(status, 403, "a row cannot leave the scope: {body}");
    let (_, row) = s
        .call(Method::GET, &row_path("other-a"), &global, None)
        .await;
    assert_eq!(
        row["branch_id"], "branch-a",
        "and the refused update rolled back"
    );
    let (status, _) = s
        .call(
            Method::PUT,
            &row_path("olga-a"),
            &own,
            Some(&json!({"user_id": "oscar"})),
        )
        .await;
    assert_eq!(status, 403);
    let (status, _) = s
        .call(
            Method::PUT,
            &row_path("other-b"),
            &branch,
            Some(&json!({"title": "x"})),
        )
        .await;
    assert_eq!(status, 404);
    let (status, row) = s
        .call(
            Method::PUT,
            &row_path("other-b"),
            &global,
            Some(&json!({"branch_id": "branch-c"})),
        )
        .await;
    assert_eq!((status, row["branch_id"].clone()), (200, json!("branch-c")));

    let batch = |authorization: &str, operations: Value| {
        let authorization = authorization.to_string();
        let s = &s;
        async move {
            s.call(
                Method::POST,
                "/library/books/batch",
                &authorization,
                Some(&json!({"operations": operations})),
            )
            .await
        }
    };
    let (status, body) = batch(
        &branch,
        json!([
            {"action": "create", "data": {"title": "batched"}},
            {"action": "update", "id": ids["other-b"], "data": {"title": "x"}}
        ]),
    )
    .await;
    assert_eq!(status, 404, "{body}");
    let (status, body) = batch(
        &branch,
        json!([
            {"action": "create", "data": {"title": "batched"}},
            {"action": "create", "data": {"title": "stray", "branch_id": "branch-b"}}
        ]),
    )
    .await;
    assert_eq!(status, 403, "{body}");
    let (_, body) = list(&global, "?title=batched").await;
    assert!(titles(&body).is_empty(), "a refused batch writes nothing");
    let (status, body) = batch(
        &branch,
        json!([
            {"action": "create", "data": {"title": "batched"}},
            {"action": "update", "id": ids["other-a"], "data": {"title": "renamed"}}
        ]),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["results"][0]["branch_id"], "branch-a");

    let (status, body) = s
        .call(
            Method::POST,
            "/library/graphql",
            &branch,
            Some(&json!({"query": "{ books { title } }"})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["books"].as_array().map(Vec::len), Some(4));

    let (status, _) = s
        .call(Method::DELETE, &row_path("other-b"), &branch, None)
        .await;
    assert_eq!(status, 404);
    let (status, body) = s
        .call(Method::DELETE, &row_path("olga-a"), &branch, None)
        .await;
    assert_eq!((status, body), (200, json!({"deleted": true})));

    let customer = s.bearer(json!({"sub": "cora"}));
    let (status, ticket) = s
        .call(
            Method::POST,
            "/library/tickets",
            &customer,
            Some(&json!({"title": "help"})),
        )
        .await;
    assert_eq!(status, 200, "a self-service owner needs no role: {ticket}");
    assert_eq!(ticket["customer_id"], "cora");
    let (status, body) = s
        .call(Method::GET, "/library/tickets", &stranger, None)
        .await;
    assert_eq!((status, titles(&body)), (200, vec![]));
    let (status, body) = s
        .call(Method::GET, "/library/tickets", &customer, None)
        .await;
    assert_eq!((status, titles(&body)), (200, vec!["help".to_string()]));
}

async fn stream_contains(
    response: &mut reqwest::Response,
    needle: &str,
    within: std::time::Duration,
) -> bool {
    let mut seen = String::new();
    let deadline = tokio::time::Instant::now() + within;
    while let Ok(Ok(Some(chunk))) = tokio::time::timeout_at(deadline, response.chunk()).await {
        seen.push_str(&String::from_utf8_lossy(&chunk));
        if seen.contains(needle) {
            return true;
        }
    }
    false
}

#[tokio::test]
async fn scoped_streams_deliver_only_rows_in_scope() {
    let Some(infra) = common::infra().await else {
        return;
    };
    if infra.require_nats().is_none() {
        return;
    }
    let Some(s) = Scenario::start().await else {
        return;
    };
    s.deploy(&scoped_manifest()).await;
    s.grant("gina", &["books:*@all"]).await;
    s.grant("agnes", &["books:view@field(branch_id=branch_id)"])
        .await;
    let global = s.bearer(json!({"sub": "gina"}));
    let branch = s.bearer(json!({"sub": "agnes", "branch_id": "branch-a"}));
    let open = |authorization: &str| {
        s.platform
            .http
            .get(format!(
                "{}/svc/{}/library/books/stream",
                s.platform.base_url, s.tenant.slug
            ))
            .header("authorization", authorization)
            .send()
    };
    let mut stream = open(&branch).await.expect("stream");
    assert_eq!(stream.status().as_u16(), 200);
    assert!(
        stream_contains(
            &mut stream,
            "event: connected",
            std::time::Duration::from_secs(5)
        )
        .await
    );
    for (title, org) in [("hidden-b", "branch-b"), ("shown-a", "branch-a")] {
        let (status, body) = s
            .call(
                Method::POST,
                "/library/books",
                &global,
                Some(&json!({"title": title, "user_id": "gina", "branch_id": org})),
            )
            .await;
        assert_eq!(status, 200, "{body}");
    }
    let mut seen = String::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while let Ok(Ok(Some(chunk))) = tokio::time::timeout_at(deadline, stream.chunk()).await {
        seen.push_str(&String::from_utf8_lossy(&chunk));
        if seen.contains("shown-a") {
            break;
        }
    }
    assert!(seen.contains("shown-a"), "{seen}");
    assert!(
        !seen.contains("hidden-b"),
        "events are published in order: {seen}"
    );
}

#[tokio::test]
async fn scoped_custom_routes_hand_the_guest_its_scope() {
    let Some(s) = Scenario::start().await else {
        return;
    };
    let mut manifest = common::wasm_manifest("reports", &[]);
    manifest["authorization"] = json!({
        "areas": [{"name": "books", "operations": ["view"]}],
        "default_roles": [],
    });
    manifest["custom_routes"] = json!([
        {"method": "POST", "path": "/mine", "handler": "echo",
            "posture": {"kind": "scoped", "area": "books", "operation": "view", "owner_field": "user_id"}},
        {"method": "POST", "path": "/branch", "handler": "echo_forwarded",
            "posture": {"kind": "scoped", "area": "books", "operation": "view"}},
    ]);
    s.deploy(&manifest).await;
    s.grant("gina", &["books:view@all"]).await;
    s.grant("agnes", &["books:view@field(branch_id=branch_id)"])
        .await;
    s.grant("olga", &["books:view"]).await;
    let call = |handler: &str, authorization: String| {
        let path = format!("/reports/_fn/{handler}");
        let s = &s;
        async move {
            s.call(Method::POST, &path, &authorization, Some(&json!({})))
                .await
        }
    };

    for (handler, claims, scope) in [
        ("echo", json!({"sub": "gina"}), json!({"kind": "all"})),
        (
            "echo",
            json!({"sub": "agnes", "branch_id": "branch-a"}),
            json!({"kind": "rows", "predicates": [
                {"claim": {"column": "branch_id", "values": ["branch-a"]}}
            ]}),
        ),
        (
            "echo",
            json!({"sub": "olga"}),
            json!({"kind": "rows", "predicates": [
                {"owner": {"column": "user_id", "subject": "olga"}}
            ]}),
        ),
    ] {
        let (status, body) = call(handler, s.bearer(claims.clone())).await;
        assert_eq!(status, 200, "{claims}: {body}");
        assert_eq!(body["scope"], scope, "{claims}");
        assert_eq!(body["auth"]["sub"], claims["sub"]);
    }
    let (status, body) = call("echo_forwarded", s.bearer(json!({"sub": "olga"}))).await;
    assert_eq!(
        (status, &body["error"]),
        (403, &json!("Missing permission: books:view")),
        "an @own grant reaches nothing on a route without an owner column"
    );
    let (status, _) = call("echo", s.bearer(json!({"sub": "sam"}))).await;
    assert_eq!(status, 403, "no grant on the area");
    let (status, _) = s
        .call(Method::POST, "/reports/_fn/echo", "", Some(&json!({})))
        .await;
    assert_eq!(status, 401);
}

#[tokio::test]
async fn guest_scopes_bind_as_their_columns_type() {
    let Some(wasm) = common::sdk::sdk_guest("db-v2", cufflink_test_guests::Profile::Debug) else {
        return;
    };
    let Some(s) = Scenario::start().await else {
        return;
    };
    let mut manifest = numbered_manifest();
    manifest["mode"] = json!("wasm");
    manifest["authorization"]["default_roles"] = json!([]);
    manifest["custom_routes"] = json!([
        {"method": "POST", "path": "/titles", "handler": "titles_in_scope",
            "posture": {"kind": "scoped", "area": "accounts", "operation": "view",
                "owner_field": "user_id"}},
    ]);
    let (status, body) = s.platform.deploy_module(&s.tenant, &manifest, &wasm).await;
    assert_eq!(status, 200, "deploy failed: {body}");
    s.grant("gina", &["accounts:*@all"]).await;
    s.grant("ina", &["accounts:view@field(org_no=org_no)"])
        .await;
    s.grant("olga", &["accounts:view@own"]).await;
    let global = s.bearer(json!({"sub": "gina"}));
    for (title, user, org) in [
        ("seven", "olga", 7),
        ("eight", "sam", 8),
        ("also seven", "sam", 7),
    ] {
        let (status, body) = s
            .call(
                Method::POST,
                "/desk/accounts",
                &global,
                Some(&json!({"title": title, "user_id": user, "org_no": org, "rank": 1})),
            )
            .await;
        assert_eq!(status, 200, "{body}");
    }

    for (claims, titles) in [
        (
            json!({"sub": "ina", "org_no": "7"}),
            json!(["also seven", "seven"]),
        ),
        (json!({"sub": "ina", "org_no": "seven"}), json!([])),
        (json!({"sub": "olga"}), json!(["seven"])),
        (
            json!({"sub": "gina"}),
            json!(["also seven", "eight", "seven"]),
        ),
    ] {
        let (status, body) = s
            .call(
                Method::POST,
                "/desk/_fn/titles_in_scope",
                &s.bearer(claims.clone()),
                Some(&json!({})),
            )
            .await;
        assert_eq!(
            (status, &body["titles"]),
            (200, &titles),
            "{claims}: {body}"
        );
    }
}

fn numbered_manifest() -> Value {
    json!({
        "name": "desk",
        "version": "0.1.0",
        "mode": "crud",
        "authorization": {
            "areas": [{"name": "accounts", "operations": ["view", "create", "edit", "delete"]}]
        },
        "tables": [{
            "name": "accounts",
            "columns": [
                id_column(),
                column("user_id", "text"),
                column("org_no", "integer"),
                column("rank", "integer"),
                column("title", "text"),
                created_at()
            ],
            "auth_required": true,
            "owner_field": "user_id",
            "scoped_by": "accounts"
        }],
        "publishes": [],
    })
}

#[tokio::test]
async fn scoped_writes_store_the_value_the_scope_checked() {
    let Some(s) = Scenario::start().await else {
        return;
    };
    s.deploy(&numbered_manifest()).await;
    s.grant("gina", &["accounts:*@all"]).await;
    s.grant("ina", &["accounts:*@field(org_no=org.no)"]).await;
    s.grant("mo", &["accounts:*@field(org_no=orgs)"]).await;
    let global = s.bearer(json!({"sub": "gina"}));
    let nested = s.bearer(json!({"sub": "ina", "org": {"no": 5}}));
    let several = s.bearer(json!({"sub": "mo", "orgs": [5, 6]}));
    let unnested = s.bearer(json!({"sub": "ina", "org": {"name": "five"}}));
    let post = |authorization: &str, path: &'static str, body: Value| {
        let authorization = authorization.to_string();
        let s = &s;
        async move {
            s.call(Method::POST, path, &authorization, Some(&body))
                .await
        }
    };
    let titles_of = |authorization: &str| {
        let authorization = authorization.to_string();
        let s = &s;
        async move {
            let (status, body) = s
                .call(Method::GET, "/desk/accounts", &authorization, None)
                .await;
            assert_eq!(status, 200, "{body}");
            titles(&body)
        }
    };

    let (status, row) = post(
        &nested,
        "/desk/accounts",
        json!({"title": "padded", "org_no": " 5"}),
    )
    .await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["org_no"], 5, "stored as the scope saw it, never 0");
    for (who, authorization, body, expected) in [
        (
            "nested",
            &nested,
            json!({"title": "moved", "org_no": " 6"}),
            403,
        ),
        (
            "nested",
            &nested,
            json!({"title": "junk", "org_no": "5x"}),
            403,
        ),
        (
            "nested",
            &nested,
            json!({"title": "typed", "rank": "5x"}),
            400,
        ),
        (
            "several",
            &several,
            json!({"title": "stray", "org_no": "7"}),
            403,
        ),
        ("several", &several, json!({"title": "none"}), 403),
    ] {
        let (status, body) = post(authorization, "/desk/accounts", body).await;
        assert_eq!(status, expected, "{who}: {body}");
    }
    let (status, row) = post(
        &several,
        "/desk/accounts",
        json!({"title": "second", "org_no": " 6"}),
    )
    .await;
    assert_eq!((status, row["org_no"].clone()), (200, json!(6)), "{row}");

    let (status, body) = post(
        &nested,
        "/desk/accounts/batch",
        json!({"operations": [
            {"action": "create", "data": {"title": "batched", "org_no": " 5"}},
            {"action": "create", "data": {"title": "escaped", "org_no": " 6"}}
        ]}),
    )
    .await;
    assert_eq!(status, 403, "{body}");
    let (status, body) = post(
        &nested,
        "/desk/accounts/batch",
        json!({"operations": [
            {"action": "create", "data": {"title": "batched", "org_no": " 5"}}
        ]}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["results"][0]["org_no"], 5);
    assert_eq!(
        titles_of(&global).await,
        vec!["batched", "padded", "second"],
        "refused writes wrote nothing"
    );
    assert_eq!(titles_of(&nested).await, vec!["batched", "padded"]);
    assert!(
        titles_of(&unnested).await.is_empty(),
        "a missing nested claim reaches no rows"
    );

    let (_, all) = s.call(Method::GET, "/desk/accounts", &global, None).await;
    let id_of = |title: &str| {
        all["results"]
            .as_array()
            .and_then(|rows| rows.iter().find(|r| r["title"] == title))
            .and_then(|r| r["id"].as_str())
            .map(str::to_string)
            .expect("row")
    };
    let (status, body) = post(
        &nested,
        "/desk/accounts/batch",
        json!({"operations": [
            {"action": "delete", "id": id_of("padded")},
            {"action": "delete", "id": id_of("second")}
        ]}),
    )
    .await;
    assert_eq!(
        status, 404,
        "a row outside the scope is not found, as a single delete: {body}"
    );
    assert_eq!(
        titles_of(&global).await,
        vec!["batched", "padded", "second"],
        "and the batch rolled back"
    );
    let (status, body) = post(
        &nested,
        "/desk/accounts/batch",
        json!({"operations": [{"action": "delete", "id": id_of("padded")}]}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["summary"]["deleted"], 1);
}

fn listings_table(name: &str, scoped: bool) -> Value {
    let restricted = |name: &str, default: &str, access: Value| {
        let mut column = column(name, "text");
        column["default_value"] = json!(default);
        column["access"] = access;
        column
    };
    let mut table = json!({
        "name": name,
        "columns": [
            id_column(),
            column("customer_id", "text"),
            column("title", "text"),
            restricted("ops_notes", "seeded", json!({"read": "internal", "write": "internal"})),
            restricted("status", "draft", json!({"write": "manage"})),
            created_at()
        ],
        "auth_required": true,
        "owner_field": "customer_id",
        "owner_self_service": true,
    });
    let area = if scoped {
        "scoped_by"
    } else {
        "permission_area"
    };
    table[area] = json!("listings");
    table
}

fn restricted_manifest() -> Value {
    json!({
        "name": "shop",
        "version": "0.1.0",
        "mode": "crud",
        "graphql": true,
        "authorization": {
            "areas": [{
                "name": "listings",
                "operations": ["view", "create", "edit", "delete", "internal", "manage"]
            }]
        },
        "tables": [listings_table("listings", false), listings_table("offers", true)],
        "publishes": [],
    })
}

#[tokio::test]
async fn column_permissions_are_refused_where_nothing_could_grant_them() {
    let Some(s) = Scenario::start().await else {
        return;
    };
    let mut misspelt = restricted_manifest();
    misspelt["authorization"]["areas"][0]["operations"] = json!(["view", "internal"]);
    let (status, body) = s.platform.deploy(&s.tenant, &misspelt).await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body.to_string()
            .contains("column 'status' asks listings:manage, and 'manage' is not an operation"),
        "{body}"
    );
    let mut keyed = restricted_manifest();
    keyed["tables"][0]["columns"][0]["access"] = json!({"read": "internal"});
    let (status, body) = s.platform.deploy(&s.tenant, &keyed).await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body.to_string()
            .contains("tables.listings.columns.id.access: the primary key"),
        "{body}"
    );
}

#[tokio::test]
async fn column_permissions_matrix() {
    let Some(s) = Scenario::start().await else {
        return;
    };
    s.deploy(&restricted_manifest()).await;
    s.grant("sara", &["listings:internal", "listings:manage"])
        .await;
    let member = s.bearer(json!({"sub": "mia"}));
    let staff = s.bearer(json!({"sub": "sara"}));

    for table in ["listings", "offers"] {
        let path = |rest: &str| format!("/shop/{table}{rest}");
        let create = |authorization: &str, body: Value| {
            let (path, authorization) = (path(""), authorization.to_string());
            let s = &s;
            async move {
                s.call(Method::POST, &path, &authorization, Some(&body))
                    .await
            }
        };
        let list = |authorization: &str, query: &str| {
            let (path, authorization) = (path(query), authorization.to_string());
            let s = &s;
            async move { s.call(Method::GET, &path, &authorization, None).await }
        };

        let (status, body) = create(&member, json!({"title": "lamp", "ops_notes": "mine"})).await;
        assert_eq!(status, 403, "{table}: {body}");
        assert_eq!(
            body["error"], "Missing permission: listings:internal (to set 'ops_notes')",
            "{table}"
        );
        let (status, body) = create(&member, json!({"title": "lamp", "status": "live"})).await;
        assert_eq!(status, 403, "{table}: {body}");
        assert_eq!(
            body["error"], "Missing permission: listings:manage (to set 'status')",
            "{table}"
        );
        let (_, body) = list(&member, "").await;
        assert!(
            titles(&body).is_empty(),
            "{table}: a refused create writes nothing"
        );

        let (status, lamp) = create(&member, json!({"title": "lamp"})).await;
        assert_eq!(status, 200, "{table}: {lamp}");
        assert_eq!(lamp["status"], "draft", "{table}");
        assert_eq!(lamp["customer_id"], "mia", "{table}");
        assert!(lamp.get("ops_notes").is_none(), "{table}: {lamp}");
        let (status, desk) = create(
            &staff,
            json!({"title": "desk", "ops_notes": "fragile", "status": "live"}),
        )
        .await;
        assert_eq!(status, 200, "{table}: {desk}");
        assert_eq!(
            (&desk["ops_notes"], &desk["status"]),
            (&json!("fragile"), &json!("live")),
            "{table}"
        );
        let row = |of: &Value| path(&format!("/{}", of["id"].as_str().expect("id")));

        let (status, got) = s.call(Method::GET, &row(&lamp), &member, None).await;
        assert_eq!(status, 200, "{table}: {got}");
        assert!(got.get("ops_notes").is_none(), "{table}: {got}");
        assert_eq!(got["title"], "lamp", "{table}");
        let (_, got) = s.call(Method::GET, &row(&desk), &staff, None).await;
        assert_eq!(got["ops_notes"], "fragile", "{table}");

        for (query, shown) in [
            ("", true),
            ("?ops_notes=nothing", true),
            ("?sort=-ops_notes", true),
            ("?search=seeded", false),
        ] {
            let (status, body) = list(&member, query).await;
            assert_eq!(status, 200, "{table} {query}: {body}");
            assert_eq!(titles(&body) == ["lamp"], shown, "{table} {query}: {body}");
            assert!(
                !body.to_string().contains("ops_notes") && !body.to_string().contains("seeded"),
                "{table} {query}: {body}"
            );
        }
        let (_, body) = list(&member, "?select=title,ops_notes").await;
        assert_eq!(body["results"], json!([{"title": "lamp"}]), "{table}");
        for (query, expected) in [
            ("?ops_notes=nothing", vec![]),
            ("?ops_notes=fragile", vec!["desk"]),
            ("?search=fragile", vec!["desk"]),
        ] {
            let (status, body) = list(&staff, query).await;
            assert_eq!(status, 200, "{table} {query}: {body}");
            assert_eq!(titles(&body), expected, "{table} {query}");
        }
        let by_notes = "?filter_mode=v2&filter=%7B%22f%22%3A%22ops_notes%22%2C%22op%22%3A%22eq%22%2C%22v%22%3A%22seeded%22%7D";
        let (status, body) = list(&member, by_notes).await;
        assert_eq!(status, 400, "{table}: {body}");
        let (status, body) = list(&staff, by_notes).await;
        assert_eq!((status, titles(&body)), (200, vec![]), "{table}");

        let (status, body) = s
            .call(
                Method::PUT,
                &row(&lamp),
                &member,
                Some(&json!({"title": "lamp", "status": "live"})),
            )
            .await;
        assert_eq!(status, 403, "{table}: {body}");
        let (status, body) = s
            .call(
                Method::PUT,
                &row(&lamp),
                &member,
                Some(&json!({"title": "floor lamp"})),
            )
            .await;
        assert_eq!(status, 200, "{table}: {body}");
        assert_eq!(
            (&body["title"], &body["status"], body.get("ops_notes")),
            (&json!("floor lamp"), &json!("draft"), None),
            "{table}: a refused update changed nothing"
        );
        let (status, body) = s
            .call(
                Method::PUT,
                &row(&desk),
                &staff,
                Some(&json!({"ops_notes": "sturdy"})),
            )
            .await;
        assert_eq!(status, 200, "{table}: {body}");
        assert_eq!(body["ops_notes"], "sturdy", "{table}");

        let batch = |authorization: &str, operations: Value| {
            let (path, authorization) = (path("/batch"), authorization.to_string());
            let s = &s;
            async move {
                s.call(
                    Method::POST,
                    &path,
                    &authorization,
                    Some(&json!({"operations": operations})),
                )
                .await
            }
        };
        let (status, body) = batch(
            &member,
            json!([
                {"action": "create", "data": {"title": "batched"}},
                {"action": "update", "id": lamp["id"], "data": {"ops_notes": "mine"}},
            ]),
        )
        .await;
        assert_eq!(status, 403, "{table}: {body}");
        let (_, body) = list(&member, "?title=batched").await;
        assert!(
            titles(&body).is_empty(),
            "{table}: a refused batch writes nothing"
        );
        let (status, body) = batch(
            &member,
            json!([{"action": "create", "data": {"title": "batched"}}]),
        )
        .await;
        assert_eq!(status, 200, "{table}: {body}");
        assert_eq!(body["results"][0]["title"], "batched", "{table}");
        assert!(!body.to_string().contains("ops_notes"), "{table}: {body}");

        let graphql = |authorization: &str, query: String| {
            let authorization = authorization.to_string();
            let s = &s;
            async move {
                s.call(
                    Method::POST,
                    "/shop/graphql",
                    &authorization,
                    Some(&json!({"query": query})),
                )
                .await
            }
        };
        let (status, body) = graphql(&member, format!("{{ {table} {{ title ops_notes }} }}")).await;
        assert_eq!(status, 400, "{table}: {body}");
        assert_eq!(
            body["error"],
            format!("Unknown field 'ops_notes' on table '{table}'"),
            "{table}"
        );
        let (status, body) =
            graphql(&member, format!("{{ {table}(ops_notes: \"nothing\") }}")).await;
        assert_eq!(status, 200, "{table}: {body}");
        assert_eq!(
            body["data"][table].as_array().map(Vec::len),
            Some(2),
            "{table}"
        );
        assert!(!body.to_string().contains("ops_notes"), "{table}: {body}");
        let (status, body) = graphql(&staff, format!("{{ {table} {{ title ops_notes }} }}")).await;
        assert_eq!(status, 200, "{table}: {body}");
        assert_eq!(
            body["data"][table],
            json!([{"title": "desk", "ops_notes": "sturdy"}]),
            "{table}"
        );
    }

    let (status, spec) = s
        .call(Method::GET, "/shop/_meta/openapi.json", &member, None)
        .await;
    assert_eq!(status, 200, "{spec}");
    assert_eq!(
        spec["components"]["schemas"]["Listings"]["properties"]["ops_notes"]["x-cufflink-access"],
        json!({"read": "listings:internal", "write": "listings:internal"})
    );
}

#[tokio::test]
async fn restricted_columns_are_left_out_of_a_stream_for_a_caller_without_the_grant() {
    let Some(infra) = common::infra().await else {
        return;
    };
    if infra.require_nats().is_none() {
        return;
    }
    let Some(s) = Scenario::start().await else {
        return;
    };
    s.deploy(&restricted_manifest()).await;
    s.grant("sara", &["listings:internal"]).await;
    let within = std::time::Duration::from_secs(5);
    for table in ["listings", "offers"] {
        for (subject, reads_notes) in [("mia", false), ("sara", true)] {
            let authorization = s.bearer(json!({"sub": subject}));
            let mut stream = s
                .platform
                .http
                .get(format!(
                    "{}/svc/{}/shop/{table}/stream",
                    s.platform.base_url, s.tenant.slug
                ))
                .header("authorization", &authorization)
                .send()
                .await
                .expect("stream");
            assert_eq!(stream.status().as_u16(), 200);
            assert!(stream_contains(&mut stream, "event: connected", within).await);
            let title = format!("{table}-{subject}");
            let (status, body) = s
                .call(
                    Method::POST,
                    &format!("/shop/{table}"),
                    &authorization,
                    Some(&json!({"title": title})),
                )
                .await;
            assert_eq!(status, 200, "{body}");
            let mut seen = String::new();
            let deadline = tokio::time::Instant::now() + within;
            while let Ok(Ok(Some(chunk))) = tokio::time::timeout_at(deadline, stream.chunk()).await
            {
                seen.push_str(&String::from_utf8_lossy(&chunk));
                if seen.contains(&title) {
                    break;
                }
            }
            assert!(seen.contains(&title), "{title}: {seen}");
            assert_eq!(seen.contains("seeded"), reads_notes, "{title}: {seen}");
        }
    }
}
