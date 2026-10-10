use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_test_guests::Profile;
use serde_json::{json, Value};

const HANDLERS: [&str; 11] = [
    "setup",
    "types",
    "errors",
    "withdraw",
    "balance",
    "scoped",
    "advisory",
    "savepoints",
    "limit",
    "abandon",
    "ledger",
];

fn guest() -> Option<Vec<u8>> {
    match cufflink_test_guests::UNAVAILABLE {
        Some(reason) => {
            common::skip(reason);
            None
        }
        None => Some(cufflink_test_guests::wasm("db-v2", Profile::Debug).expect("built guest")),
    }
}

async fn post(
    platform: &PlatformProcess,
    tenant: &common::Tenant,
    handler: &str,
    body: Value,
) -> (u16, Value) {
    let response = platform
        .http
        .post(platform.fn_url(tenant, "dbv2-sdk", handler))
        .json(&body)
        .send()
        .await
        .expect("fn request");
    (
        response.status().as_u16(),
        response.json().await.unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn sdk_guests_run_the_db_v2_cases() {
    let Some(wasm) = guest() else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[("WASM_TEST_ROUTE_BUDGET_SECS", "3")],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let routes: Vec<(&str, String)> = HANDLERS.iter().map(|h| (*h, format!("/{h}"))).collect();
    let routes: Vec<(&str, &str)> = routes.iter().map(|(h, p)| (*h, p.as_str())).collect();
    let (status, body) = platform
        .deploy_module(&tenant, &common::wasm_manifest("dbv2-sdk", &routes), &wasm)
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let call = |handler: &'static str| post(&platform, &tenant, handler, json!({}));

    assert_eq!(call("setup").await, (200, json!({"ok": true})));

    let (status, body) = call("types").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body,
        json!({
            "row": {
                "id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                "amount": "12345678901234567890.123456",
                "local": "2026-03-04T05:06:07",
                "tags": ["a", "b"], "counts": [1, 2], "mood": "happy",
                "doc": {"a": [1, null]}, "raw": "AP8="
            },
            "amount": "12345678901234567890.123456",
            "unsupported": {"kind": "unsupported_type", "code": null, "constraint": null}
        })
    );

    let (status, body) = call("errors").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body,
        json!({
            "unique": {"kind": "unique_violation", "code": "23505", "constraint": "children_email_key"},
            "foreign_key": {"kind": "foreign_key_violation", "code": "23503", "constraint": "children_parent_fkey"},
            "check": {"kind": "check_violation", "code": "23514", "constraint": "children_age_check"},
            "not_null": {"kind": "not_null_violation", "code": "23502", "constraint": null},
            "raise": {"kind": "raise_exception", "code": "P0001", "constraint": null},
            "timeout": {"kind": "query_canceled", "code": "57014", "constraint": null},
            "legacy_query": [],
            "legacy_execute": -1
        })
    );

    let withdraw = || post(&platform, &tenant, "withdraw", json!({"amount": 60}));
    let (a, b) = tokio::join!(withdraw(), withdraw());
    assert_eq!((a.0, b.0), (200, 200), "{a:?} {b:?}");
    let withdrew = [&a.1, &b.1].map(|body| body["withdrew"] == true);
    assert!(
        withdrew[0] ^ withdrew[1],
        "exactly one withdrawal: {a:?} {b:?}"
    );
    assert_eq!(call("balance").await, (200, json!({"balance": 40})));

    assert_eq!(
        call("scoped").await,
        (200, json!({"ids": [1, 3], "after": null}))
    );
    assert_eq!(
        call("advisory").await,
        (200, json!({"inner_got": false, "later_got": true}))
    );
    assert_eq!(
        call("savepoints").await,
        (
            200,
            json!({
                "inner": {"kind": "unique_violation", "code": "23505", "constraint": "ledger_pkey"},
                "notes": ["outer", "after"]
            })
        )
    );
    assert_eq!(
        call("limit").await,
        (
            200,
            json!({"kind": "transaction_limit", "code": null, "constraint": null})
        )
    );

    let (status, body) = call("abandon").await;
    assert_eq!(status, 500, "{body}");
    assert_eq!(body["error"], "WASM execution timed out after 3s");
    assert_eq!(
        call("ledger").await,
        (200, json!({"notes": ["outer", "after"]})),
        "the abandoned insert committed"
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while platform
        .metric("cufflink_db_tx_abandoned_total", &[("reason", "deadline")])
        .await
        != Some(1.0)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the trapped guest's transaction was never counted as abandoned"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
