use cufflink_testkit as common;

use common::{PlatformProcess, Tenant};
use cufflink_test_guests::Profile;
use serde_json::{json, Value};

const HANDLERS: [&str; 9] = [
    "put",
    "fetch",
    "fetch_many",
    "lock",
    "count",
    "forget",
    "extend",
    "json_round_trip",
    "raw",
];

fn guest() -> Option<Vec<u8>> {
    match cufflink_test_guests::UNAVAILABLE {
        Some(reason) => {
            common::skip(reason);
            None
        }
        None => Some(cufflink_test_guests::wasm("cache", Profile::Debug).expect("built guest")),
    }
}

struct Cache<'a> {
    platform: &'a PlatformProcess,
    tenant: &'a Tenant,
    service: &'static str,
}

impl Cache<'_> {
    async fn call(&self, handler: &str, body: Value) -> Value {
        let response = self
            .platform
            .http
            .post(self.platform.fn_url(self.tenant, self.service, handler))
            .json(&body)
            .send()
            .await
            .expect("fn request");
        assert_eq!(response.status().as_u16(), 200, "{handler}");
        response.json().await.expect("json body")
    }
}

#[tokio::test]
async fn cache_namespaced() {
    let Some(wasm) = guest() else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(redis_url) = infra.require_redis() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(
        &db.url,
        infra.nats_url(),
        &[
            ("REDIS_URL", redis_url),
            ("CACHE_MGET_MAX_KEYS", "3"),
            ("CACHE_MGET_MAX_BYTES", "20"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let (first, second) = (db.mint_tenant().await, db.mint_tenant().await);
    let routes: Vec<(&str, String)> = HANDLERS.iter().map(|h| (*h, format!("/{h}"))).collect();
    let routes: Vec<(&str, &str)> = routes.iter().map(|(h, p)| (*h, p.as_str())).collect();
    for (tenant, service) in [
        (&first, "cache-a"),
        (&first, "cache-b"),
        (&second, "cache-a"),
    ] {
        let (status, body) = platform
            .deploy_module(tenant, &common::wasm_manifest(service, &routes), &wasm)
            .await;
        assert_eq!(status, 200, "deploy failed: {body}");
    }
    let a = Cache {
        platform: &platform,
        tenant: &first,
        service: "cache-a",
    };
    let b = Cache {
        platform: &platform,
        tenant: &first,
        service: "cache-b",
    };
    let other_tenant = Cache {
        platform: &platform,
        tenant: &second,
        service: "cache-a",
    };

    for (cache, value) in [
        (&a, "first-a"),
        (&b, "first-b"),
        (&other_tenant, "second-a"),
    ] {
        assert_eq!(
            cache
                .call("put", json!({"key": "shared", "value": value}))
                .await,
            json!({"result": null})
        );
    }
    for (cache, value) in [
        (&a, "first-a"),
        (&b, "first-b"),
        (&other_tenant, "second-a"),
    ] {
        assert_eq!(
            cache.call("fetch", json!({"key": "shared"})).await,
            json!({"result": value})
        );
    }
    assert_eq!(
        a.call("fetch_many", json!({"keys": ["shared", "absent"]}))
            .await,
        json!({"result": ["first-a", null]})
    );
    assert_eq!(
        a.call("fetch_many", json!({"keys": []})).await,
        json!({"result": []})
    );
    assert_eq!(
        a.call("fetch_many", json!({"keys": ["shared", "shared"]}))
            .await,
        json!({"result": ["first-a", "first-a"]}),
        "14 bytes of values fit the 20-byte cap"
    );
    for keys in [
        json!(["shared", "shared", "shared"]),
        json!(["a", "b", "c", "d"]),
    ] {
        assert_eq!(
            a.call("fetch_many", json!({ "keys": keys })).await["kind"],
            "too_large",
            "{keys}"
        );
    }

    let physical = |tenant: &Tenant, service: &str, key: &str| {
        format!("cufflink:cache:{}:{service}:{key}", tenant.id)
    };
    let client = redis::Client::open(redis_url).expect("redis url");
    let mut redis = client
        .get_multiplexed_async_connection()
        .await
        .expect("redis connection");
    let stored: Option<String> = redis::cmd("GET")
        .arg(physical(&first, "cache-a", "shared"))
        .query_async(&mut redis)
        .await
        .expect("get");
    assert_eq!(stored.as_deref(), Some("first-a"));
    let ttl_of = |key: String| {
        let mut redis = redis.clone();
        async move {
            redis::cmd("PTTL")
                .arg(key)
                .query_async::<i64>(&mut redis)
                .await
                .expect("pttl")
        }
    };
    let ttl = ttl_of(physical(&first, "cache-a", "shared")).await;
    assert!((1..=60_000).contains(&ttl), "{ttl}");

    assert_eq!(
        a.call("lock", json!({"key": "invoice:42", "ttl_ms": 30_000}))
            .await,
        json!({"result": true})
    );
    assert_eq!(
        a.call("lock", json!({"key": "invoice:42"})).await,
        json!({"result": false})
    );
    assert_eq!(
        b.call("lock", json!({"key": "invoice:42"})).await,
        json!({"result": true}),
        "another service's lock of the same name is its own"
    );

    assert_eq!(
        a.call("count", json!({"key": "hits", "by": 2, "ttl_ms": 5_000}))
            .await,
        json!({"result": 2})
    );
    let first_ttl = ttl_of(physical(&first, "cache-a", "hits")).await;
    assert!((1..=5_000).contains(&first_ttl), "{first_ttl}");
    assert_eq!(
        a.call("count", json!({"key": "hits", "by": 3, "ttl_ms": 600_000}))
            .await,
        json!({"result": 5})
    );
    let second_ttl = ttl_of(physical(&first, "cache-a", "hits")).await;
    assert!(
        second_ttl <= first_ttl,
        "incr on an existing key keeps its expiry: {first_ttl} then {second_ttl}"
    );
    assert_eq!(
        a.call("extend", json!({"key": "hits", "ttl_ms": 600_000}))
            .await,
        json!({"result": true})
    );
    assert!(ttl_of(physical(&first, "cache-a", "hits")).await > 5_000);
    assert_eq!(
        a.call("extend", json!({"key": "absent"})).await,
        json!({"result": false})
    );

    assert_eq!(
        a.call(
            "json_round_trip",
            json!({"key": "doc", "value": {"n": [1, 2]}})
        )
        .await,
        json!({"result": {"n": [1, 2]}})
    );
    assert_eq!(
        a.call("forget", json!({"key": "doc"})).await,
        json!({"result": true})
    );
    assert_eq!(
        a.call("forget", json!({"key": "doc"})).await,
        json!({"result": false})
    );

    for (body, kind) in [
        (json!({"key": "has space", "value": "v"}), "invalid_key"),
        (
            json!({"key": "k", "value": "v", "ttl_ms": 0}),
            "invalid_ttl",
        ),
        (
            json!({"key": "k", "value": "v", "ttl_ms": 31u64 * 24 * 3_600_000}),
            "invalid_ttl",
        ),
        (
            json!({"key": "k", "value": "x".repeat(1024 * 1024 + 1)}),
            "too_large",
        ),
    ] {
        assert_eq!(a.call("put", body).await["kind"], kind);
    }

    assert_eq!(
        a.call("raw", json!({"key": physical(&first, "cache-a", "shared")}))
            .await,
        json!({"error": "reserved key"}),
        "raw guest Redis access cannot read the cache"
    );
    assert_eq!(
        platform
            .metric(
                "cufflink_cache_calls_total",
                &[("op", "set_nx"), ("result", "ok")]
            )
            .await,
        Some(3.0)
    );
}
