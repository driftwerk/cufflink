use cufflink_testkit as common;

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method as HttpMethod, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::{PlatformProcess, Tenant};
use cufflink_test_guests::Profile;
use percent_encoding::percent_decode_str;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SERVICE: &str = "documents";
const PLATFORM_BUCKET: &str = "example-platform";
const PLATFORM_KEY: &str = "AKPLATFORM";
const HARBOR_KEY: &str = "AKHARBOR";
const HARBOR_SECRET: &str = "harbor-example-secret";
const EXACT: &str = "leads/7/Café façade #2 50%+off?.pdf";
const ROUTES: [(&str, &str); 9] = [
    ("upload", "/upload"),
    ("head", "/head"),
    ("download", "/download"),
    ("download_many", "/download-many"),
    ("presign", "/presign"),
    ("presign_upload", "/presign-upload"),
    ("transform", "/transform"),
    ("delete", "/delete"),
    ("object", "/object"),
];

#[derive(Debug, Clone, PartialEq)]
struct Seen {
    method: String,
    bucket: String,
    key: String,
    access_key: String,
    acl: Option<String>,
}

#[derive(Default)]
struct Store {
    objects: HashMap<(String, String), Vec<u8>>,
    seen: Vec<Seen>,
}

type Shared = Arc<Mutex<Store>>;

fn decoded(raw: &str) -> String {
    percent_decode_str(raw).decode_utf8_lossy().into_owned()
}

fn access_key(headers: &HeaderMap) -> String {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split("Credential=").nth(1))
        .and_then(|credential| credential.split('/').next())
        .unwrap_or_default()
        .to_string()
}

fn meta(size: usize) -> [(&'static str, String); 4] {
    [
        ("etag", "\"example-etag\"".to_string()),
        ("last-modified", "Thu, 05 Nov 2026 10:00:00 GMT".to_string()),
        ("content-length", size.to_string()),
        ("content-type", "application/pdf".to_string()),
    ]
}

fn with_meta(status: StatusCode, size: usize, body: Vec<u8>) -> Response {
    let mut response = (status, body).into_response();
    meta(size).into_iter().for_each(|(name, value)| {
        if let Ok(value) = HeaderValue::from_str(&value) {
            response.headers_mut().insert(name, value);
        }
    });
    response
}

fn deleted_keys(body: &str) -> Vec<String> {
    body.split("<Key>")
        .skip(1)
        .filter_map(|rest| rest.split("</Key>").next())
        .map(str::to_string)
        .collect()
}

async fn s3(State(store): State<Shared>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let body: Bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .unwrap_or_default();
    let path = parts.uri.path().trim_start_matches('/');
    let (bucket, raw_key) = path.split_once('/').unwrap_or((path, ""));
    let bucket = bucket.to_string();
    let signer = access_key(&parts.headers);
    let acl = parts
        .headers
        .get("x-amz-acl")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let mut store = store.lock().expect("the stand-in's store");
    let mut seen = |method: &str, key: &str| {
        store.seen.push(Seen {
            method: method.to_string(),
            bucket: bucket.clone(),
            key: key.to_string(),
            access_key: signer.clone(),
            acl: acl.clone(),
        })
    };
    let key = decoded(raw_key);
    match parts.method {
        HttpMethod::POST if parts.uri.query() == Some("delete") => {
            let keys = deleted_keys(&String::from_utf8_lossy(&body));
            keys.iter().for_each(|key| seen("DELETE", key));
            let deleted: String = keys
                .iter()
                .map(|key| format!("<Deleted><Key>{key}</Key></Deleted>"))
                .collect();
            keys.iter().for_each(|key| {
                store.objects.remove(&(bucket.clone(), key.clone()));
            });
            (
                StatusCode::OK,
                format!("<DeleteResult>{deleted}</DeleteResult>"),
            )
                .into_response()
        }
        HttpMethod::PUT => {
            seen("PUT", &key);
            store.objects.insert((bucket, key), body.to_vec());
            with_meta(StatusCode::OK, 0, Vec::new())
        }
        HttpMethod::GET | HttpMethod::HEAD => {
            let method = parts.method.as_str().to_string();
            seen(&method, &key);
            match store.objects.get(&(bucket, key)) {
                Some(object) if parts.method == HttpMethod::HEAD => {
                    with_meta(StatusCode::OK, object.len(), Vec::new())
                }
                Some(object) => with_meta(StatusCode::OK, object.len(), object.clone()),
                None => StatusCode::NOT_FOUND.into_response(),
            }
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn stand_in() -> (String, Shared) {
    let store = Shared::default();
    let app = Router::new().fallback(s3).with_state(store.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the S3 stand-in");
    let url = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (url, store)
}

fn seen_count(store: &Shared) -> usize {
    store.lock().expect("the stand-in's store").seen.len()
}

fn png() -> Vec<u8> {
    let mut png = Vec::new();
    image::RgbImage::new(4, 4)
        .write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
        .expect("a png");
    png
}

const INVALID_NAMES: [&str; 12] = [
    ".",
    "..",
    "example-documents\\",
    "x\\..\\example-documents",
    "example-platform\\wasm",
    "example-documents?x",
    "example-documents#x",
    "example-documents\t",
    "example-documents\n",
    "Example-Documents",
    "%2e",
    "example..documents",
];

fn seen_on(store: &Shared, bucket: &str) -> Vec<Seen> {
    store
        .lock()
        .expect("the stand-in's store")
        .seen
        .iter()
        .filter(|seen| seen.bucket == bucket)
        .cloned()
        .collect()
}

struct Guest<'a> {
    platform: &'a PlatformProcess,
    tenant: &'a Tenant,
}

impl Guest<'_> {
    async fn answer(&self, handler: &str, body: Value) -> reqwest::Response {
        self.platform
            .http
            .post(self.platform.fn_url(self.tenant, SERVICE, handler))
            .json(&body)
            .send()
            .await
            .expect("fn request")
    }

    async fn call(&self, handler: &str, body: Value) -> Value {
        let response = self.answer(handler, body).await;
        assert_eq!(response.status().as_u16(), 200, "{handler}");
        response.json().await.expect("json body")
    }

    async fn upload(&self, bucket: &str, key: &str, acl: Option<&str>) -> Value {
        self.call(
            "upload",
            json!({"bucket": bucket, "key": key, "body": "hello", "acl": acl}),
        )
        .await
    }

    async fn refuses_every_call(&self, bucket: &str, key: &str) {
        let at = json!({"bucket": bucket, "key": key});
        let case = format!("{bucket:?}/{key}");
        assert_eq!(
            self.upload(bucket, key, None).await["error"],
            "denied",
            "{case}"
        );
        assert_eq!(
            self.call("head", at.clone()).await["error"],
            "denied",
            "{case}"
        );
        assert_eq!(
            self.call("download", at.clone()).await,
            json!({"base64": null}),
            "{case}"
        );
        assert_eq!(
            self.call("download_many", json!({"items": [at.clone()]}))
                .await,
            json!({"slots": [{"error": "storage access denied"}]}),
            "{case}"
        );
        for presign in ["presign", "presign_upload"] {
            assert_eq!(
                self.call(presign, at.clone()).await,
                json!({"url": null}),
                "{case}"
            );
        }
        assert_eq!(
            self.call("delete", at.clone()).await,
            json!({"deleted": false}),
            "{case}"
        );
        assert_eq!(
            self.call(
                "transform",
                json!({"bucket": bucket, "key": key, "out_key": format!("{key}.jpg")})
            )
            .await,
            json!({"written": null}),
            "{case}"
        );
        assert_ne!(
            self.answer("object", at).await.status().as_u16(),
            200,
            "{case}"
        );
    }

    async fn refused(&self, bucket: &str, key: &str, acl: Option<&str>) -> String {
        let refused = self.upload(bucket, key, acl).await;
        assert_eq!(refused["error"], "denied", "{bucket}/{key}: {refused}");
        refused["message"].as_str().unwrap_or_default().to_string()
    }
}

struct Admin<'a> {
    platform: &'a PlatformProcess,
    key: String,
}

impl Admin<'_> {
    async fn send(
        &self,
        method: Method,
        slug: &str,
        bucket: Option<&str>,
        body: Option<Value>,
    ) -> (u16, Value) {
        let path = match bucket {
            Some(bucket) => format!("/api/tenants/{slug}/buckets/{bucket}"),
            None => format!("/api/tenants/{slug}/buckets"),
        };
        self.platform
            .send(method, &path, &self.key, body.as_ref())
            .await
    }

    async fn put(&self, slug: &str, bucket: &str, body: Value) -> (u16, Value) {
        self.send(Method::PUT, slug, Some(bucket), Some(body)).await
    }
}

fn documents_rules(prefix: &str) -> Value {
    json!({
        "key_prefixes": [prefix],
        "credential": {"access_key_id": HARBOR_KEY, "secret_access_key": HARBOR_SECRET},
    })
}

async fn started(db_url: &str, nats_url: &str, endpoint: &str, kek: &str) -> PlatformProcess {
    let platform = PlatformProcess::start(
        db_url,
        Some(nats_url),
        &[
            ("STORAGE_BACKEND", "s3"),
            ("S3_ENDPOINT", endpoint),
            ("S3_REGION", "us-east-1"),
            ("S3_ACCESS_KEY", PLATFORM_KEY),
            ("S3_SECRET_KEY", "platform-example-secret"),
            ("S3_BUCKET", PLATFORM_BUCKET),
            ("CUFFLINK_SECRETS_KEK", kek),
            ("CUFFLINK_SECRETS_KEK_ID", "a"),
            ("CUFFLINK_SECRET_STORAGE", "encrypted"),
        ],
    )
    .await;
    platform.wait_ready().await;
    platform
}

#[tokio::test]
async fn tenant_buckets() {
    let Some(wasm) = common::sdk::sdk_guest("tenant-buckets", Profile::Debug) else {
        return;
    };
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let (endpoint, store) = stand_in().await;
    let kek = STANDARD.encode([7; 32]);
    let mut db = infra.create_database().await;
    let platform = started(&db.url, nats_url, &endpoint, &kek).await;
    let (_, version) = platform.get_json("/api/version").await.expect("version");
    for feature in ["tenant_buckets", "storage_acl"] {
        assert!(
            version["supports"]
                .as_array()
                .is_some_and(|supports| supports.contains(&json!(feature))),
            "{feature}: {version}"
        );
    }
    let harbor = db.mint_tenant().await;
    let globex = db.mint_tenant().await;
    let admin = Admin {
        platform: &platform,
        key: format!("ApiKey {}", db.mint_admin_key().await),
    };
    for tenant in [&harbor, &globex] {
        let (status, body) = platform
            .deploy_module(tenant, &common::wasm_manifest(SERVICE, &ROUTES), &wasm)
            .await;
        assert_eq!(status, 200, "deploy failed: {body}");
    }

    let (status, set) = admin
        .put(&harbor.slug, "example-documents", documents_rules("leads/"))
        .await;
    assert_eq!(status, 200, "{set}");
    assert_eq!(set["bucket"], "example-documents");
    assert_eq!(set["key_prefixes"], json!(["leads/"]));
    assert_eq!(set["credential"], json!({"access_key_id": HARBOR_KEY}));
    let (status, assets) = admin
        .put(
            &harbor.slug,
            "example-assets",
            json!({"acls": ["public-read"]}),
        )
        .await;
    assert_eq!(status, 200, "{assets}");
    assert_eq!(assets["credential"], Value::Null);
    let (status, listed) = admin.send(Method::GET, &harbor.slug, None, None).await;
    assert_eq!(status, 200, "{listed}");
    let buckets: Vec<&str> = listed["buckets"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|bucket| bucket["bucket"].as_str())
        .collect();
    assert_eq!(buckets, ["example-assets", "example-documents"]);

    let (status, conflict) = admin
        .put(&globex.slug, "example-documents", json!({}))
        .await;
    assert_eq!(status, 409, "{conflict}");
    assert!(conflict.to_string().contains(&harbor.slug), "{conflict}");
    for (bucket, body) in [
        (PLATFORM_BUCKET, json!({})),
        ("Example-Documents", json!({})),
        ("example-other", json!({"key_prefixes": []})),
        ("example-other", json!({"key_prefixes": ["/leads"]})),
        ("example-other", json!({"acls": ["public-read-write"]})),
        (
            "example-other",
            json!({"credential": {"access_key_id": "AK"}}),
        ),
        ("example-other", json!({"region": "us-east-1"})),
    ] {
        let (status, refused) = admin.put(&harbor.slug, bucket, body.clone()).await;
        assert_eq!(status, 422, "{bucket} {body}: {refused}");
    }

    let pool = db.pool();
    let client = pool.get().await.expect("checkout");
    let sealed = client
        .query_one(
            "SELECT secret_access_key, secret_access_key_enc IS NOT NULL AS sealed
             FROM tenant_buckets WHERE bucket = 'example-documents'",
            &[],
        )
        .await
        .expect("the bucket's row");
    assert_eq!(sealed.get::<_, Option<String>>("secret_access_key"), None);
    assert!(sealed.get::<_, bool>("sealed"));
    let audited: Vec<String> = client
        .query(
            "SELECT action, details::text AS details FROM audit_log WHERE action LIKE 'tenant.bucket.%'",
            &[],
        )
        .await
        .expect("audit records")
        .iter()
        .map(|row| format!("{} {}", row.get::<_, String>("action"), row.get::<_, Option<String>>("details").unwrap_or_default()))
        .collect();
    assert_eq!(audited.len(), 2, "{audited:?}");
    assert!(
        audited.iter().all(|record| !record.contains(HARBOR_SECRET)),
        "{audited:?}"
    );
    for answer in [&set, &listed] {
        assert!(!answer.to_string().contains(HARBOR_SECRET), "{answer}");
    }
    drop(client);

    let ours = Guest {
        platform: &platform,
        tenant: &harbor,
    };
    let theirs = Guest {
        platform: &platform,
        tenant: &globex,
    };
    let exact = json!({"bucket": "example-documents", "key": EXACT});

    assert_eq!(
        ours.upload("example-documents", EXACT, None).await,
        json!({"size": 5})
    );
    assert_eq!(ours.call("head", exact.clone()).await, json!({"size": 5}));
    assert_eq!(
        ours.call("download", exact.clone()).await,
        json!({"base64": STANDARD.encode("hello")})
    );
    assert_eq!(
        ours.call(
            "download_many",
            json!({"items": [
                {"bucket": "example-documents", "key": EXACT},
                {"bucket": "example-documents", "key": "other/a.pdf"},
            ]})
        )
        .await,
        json!({"slots": [{"size": 5}, {"error": "storage access denied"}]})
    );
    let streamed = ours.answer("object", exact.clone()).await;
    assert_eq!(streamed.status().as_u16(), 200);
    assert_eq!(
        streamed.bytes().await.expect("streamed"),
        "hello".as_bytes()
    );
    let presigned = ours.call("presign", exact.clone()).await;
    let url = presigned["url"].as_str().expect("a presigned url");
    let parsed = reqwest::Url::parse(url).expect("a url");
    assert_eq!(
        decoded(parsed.path()),
        format!("/example-documents/{EXACT}"),
        "{url}"
    );
    assert!(
        parsed
            .query_pairs()
            .any(|(name, value)| name == "X-Amz-Credential"
                && value.starts_with(&format!("{HARBOR_KEY}/"))),
        "{url}"
    );
    assert!(ours
        .refused("example-documents", "other/a.pdf", None)
        .await
        .contains("key_outside_prefix"));
    assert!(ours
        .refused("example-documents", "leads//a.pdf", None)
        .await
        .contains("key_not_exact"));
    assert!(ours
        .refused("example-documents", "leads/a.pdf", Some("public-read"))
        .await
        .contains("acl_not_admitted"));

    let before = seen_on(&store, "example-documents").len();
    assert_eq!(
        theirs.upload("example-documents", EXACT, None).await["error"],
        "denied"
    );
    assert_eq!(theirs.call("head", exact.clone()).await["error"], "denied");
    assert_eq!(
        theirs.call("download", exact.clone()).await,
        json!({"base64": null})
    );
    assert_eq!(
        theirs
            .call("download_many", json!({"items": [exact.clone()]}))
            .await,
        json!({"slots": [{"error": "storage access denied"}]})
    );
    assert_eq!(
        theirs.call("presign", exact.clone()).await,
        json!({"url": null})
    );
    assert_eq!(
        theirs.call("delete", exact.clone()).await,
        json!({"deleted": false})
    );
    assert_eq!(
        theirs
            .answer("object", exact.clone())
            .await
            .status()
            .as_u16(),
        500
    );
    assert_eq!(
        seen_on(&store, "example-documents").len(),
        before,
        "no call of another tenant reaches the bucket"
    );

    assert_eq!(
        ours.upload("example-assets", "logos/Café.png", Some("public-read"))
            .await,
        json!({"size": 5})
    );
    assert!(ours
        .refused("example-media", "logos/a.png", Some("public-read"))
        .await
        .contains("acl_not_admitted"));
    assert_eq!(
        ours.upload("example-media", "media/Café.pdf", None).await,
        json!({"size": 5})
    );
    assert_eq!(
        ours.call("delete", exact.clone()).await,
        json!({"deleted": true})
    );
    assert_eq!(
        ours.call("head", exact.clone()).await,
        json!({"size": null})
    );

    let documents = seen_on(&store, "example-documents");
    assert!(
        documents
            .iter()
            .all(|seen| seen.key == EXACT && seen.access_key == HARBOR_KEY && seen.acl.is_none()),
        "{documents:#?}"
    );
    for method in ["PUT", "HEAD", "GET", "DELETE"] {
        assert!(
            documents.iter().any(|seen| seen.method == method),
            "{method}: {documents:#?}"
        );
    }
    assert_eq!(
        seen_on(&store, "example-assets"),
        [Seen {
            method: "PUT".into(),
            bucket: "example-assets".into(),
            key: "logos/Café.png".into(),
            access_key: PLATFORM_KEY.into(),
            acl: Some("public-read".into()),
        }]
    );
    let media = seen_on(&store, "example-media");
    assert_eq!(media.len(), 1, "{media:#?}");
    assert_eq!(
        media[0].key, "media/Caf%C3%A9.pdf",
        "a bucket no tenant registers keeps its encoding"
    );
    assert_eq!(
        (media[0].access_key.as_str(), media[0].acl.as_deref()),
        (PLATFORM_KEY, None)
    );
    assert_eq!(
        platform
            .metric(
                "cufflink_host_storage_acl_total",
                &[("acl", "public-read"), ("result", "ok")]
            )
            .await,
        Some(1.0)
    );

    let replica = started(&db.url, nats_url, &endpoint, &kek).await;
    let elsewhere = Guest {
        platform: &replica,
        tenant: &harbor,
    };
    assert_eq!(
        elsewhere
            .upload("example-documents", "leads/b.pdf", None)
            .await,
        json!({"size": 5})
    );
    let (status, moved) = admin
        .put(
            &harbor.slug,
            "example-documents",
            documents_rules("archive/"),
        )
        .await;
    assert_eq!(status, 200, "{moved}");
    common::until(
        "the replica reloads the bucket's prefixes",
        Duration::from_secs(20),
        async || {
            elsewhere
                .upload("example-documents", "leads/c.pdf", None)
                .await["error"]
                == "denied"
        },
    )
    .await
    .expect("the change reaches the replica without a restart");
    assert_eq!(
        elsewhere
            .upload("example-documents", "archive/c.pdf", None)
            .await,
        json!({"size": 5})
    );
    let (status, _) = admin
        .send(Method::DELETE, &harbor.slug, Some("example-assets"), None)
        .await;
    assert_eq!(status, 204);
    let (status, _) = admin
        .send(Method::DELETE, &harbor.slug, Some("example-assets"), None)
        .await;
    assert_eq!(status, 204, "retiring is idempotent");
    let (status, _) = admin
        .send(Method::DELETE, &harbor.slug, Some("example-unknown"), None)
        .await;
    assert_eq!(status, 404);
    common::until(
        "the replica forgets the deleted bucket",
        Duration::from_secs(20),
        async || {
            elsewhere
                .upload("example-assets", "logos/b.png", Some("public-read"))
                .await["error"]
                == "denied"
        },
    )
    .await
    .expect("the deletion reaches the replica without a restart");

    let ours = Guest {
        platform: &platform,
        tenant: &harbor,
    };
    let theirs = Guest {
        platform: &platform,
        tenant: &globex,
    };

    let before = seen_count(&store);
    for bucket in INVALID_NAMES {
        for guest in [&ours, &theirs] {
            guest.refuses_every_call(bucket, "archive/a.pdf").await;
        }
    }
    assert_eq!(
        seen_count(&store),
        before,
        "no invalid bucket name reaches the store"
    );

    store.lock().expect("the stand-in's store").objects.insert(
        ("example-documents".into(), "archive/Café logo.png".into()),
        png(),
    );
    let written = ours
        .call(
            "transform",
            json!({"bucket": "example-documents", "key": "archive/Café logo.png", "out_key": "archive/Café logo.jpg"}),
        )
        .await;
    assert!(
        written["written"].as_u64().is_some_and(|n| n > 0),
        "{written}"
    );
    assert!(
        seen_on(&store, "example-documents")
            .iter()
            .any(|seen| seen.method == "PUT"
                && seen.key == "archive/Café logo.jpg"
                && seen.access_key == HARBOR_KEY),
        "the transform writes the exact key with the bucket's credential"
    );
    let upload_url = ours
        .call(
            "presign_upload",
            json!({"bucket": "example-documents", "key": "archive/up load.pdf"}),
        )
        .await;
    let upload_url =
        reqwest::Url::parse(upload_url["url"].as_str().expect("a presigned url")).expect("a url");
    assert_eq!(
        decoded(upload_url.path()),
        "/example-documents/archive/up load.pdf"
    );
    assert!(upload_url
        .query_pairs()
        .any(|(name, value)| name == "X-Amz-Credential"
            && value.starts_with(&format!("{HARBOR_KEY}/"))));
    let streamed = ours
        .answer(
            "object",
            json!({"bucket": "example-documents", "key": "archive/Café logo.png"}),
        )
        .await;
    assert_eq!(streamed.status().as_u16(), 200);
    let before = seen_count(&store);
    theirs
        .refuses_every_call("example-documents", "archive/Café logo.png")
        .await;
    assert_eq!(seen_count(&store), before);

    let (status, revived) = admin
        .put(
            &harbor.slug,
            "example-assets",
            json!({"acls": ["public-read"]}),
        )
        .await;
    assert_eq!(status, 200, "{revived}");
    assert_eq!(revived["retired_at"], Value::Null);
    let allowlist =
        json!({"scope": "legacy", "allowed_buckets": ["example-assets", "example-documents"]});
    let (status, refused) = platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}/policies", globex.slug),
            &admin.key,
            Some(&json!({"policies": {"storage_scope": {"mode": "enforce", "config": allowlist}}})),
        )
        .await;
    assert_eq!(status, 409, "{refused}");
    let client = pool.get().await.expect("checkout");
    client
        .execute(
            "INSERT INTO tenant_policies (tenant_id, key, mode, config, updated_by)
             VALUES ($1, 'storage_scope', 'enforce', $2, 'test')",
            &[&globex.id, &allowlist],
        )
        .await
        .expect("an allowlist written before the registration");
    let (status, _) = admin
        .put(
            &harbor.slug,
            "example-assets",
            json!({"acls": ["public-read"]}),
        )
        .await;
    assert_eq!(status, 200);
    let before = seen_count(&store);
    theirs
        .refuses_every_call("example-assets", "logos/a.png")
        .await;
    assert!(theirs
        .refused("example-assets", "logos/a.png", Some("public-read"))
        .await
        .contains("foreign_tenant_bucket"));
    assert_eq!(
        seen_count(&store),
        before,
        "a listed bucket of another tenant stays refused"
    );

    client
        .execute(
            "UPDATE tenant_buckets SET secret_access_key_enc = '\\x00'::bytea WHERE bucket = 'example-documents'",
            &[],
        )
        .await
        .expect("a secret that cannot be opened");
    let (status, listed) = admin
        .put(
            &harbor.slug,
            "example-assets",
            json!({"acls": ["public-read"]}),
        )
        .await;
    assert_eq!(status, 409, "another tenant group lists it: {listed}");
    let (status, unlisted) = platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}/policies", globex.slug),
            &admin.key,
            Some(&json!({"policies": {"storage_scope": {"mode": "enforce", "config": {"scope": "legacy"}}}})),
        )
        .await;
    assert_eq!(status, 200, "{unlisted}");
    let (status, _) = admin
        .put(
            &harbor.slug,
            "example-assets",
            json!({"acls": ["public-read"]}),
        )
        .await;
    assert_eq!(status, 200);
    let unopenable = ours
        .upload("example-documents", "archive/b.pdf", None)
        .await;
    assert_eq!(unopenable["error"], "not_configured", "{unopenable}");
    assert_eq!(
        ours.upload("example-assets", "logos/c.png", Some("public-read"))
            .await,
        json!({"size": 5}),
        "the rest of the catalog still loads"
    );

    let (status, _) = admin
        .send(
            Method::DELETE,
            &harbor.slug,
            Some("example-documents"),
            None,
        )
        .await;
    assert_eq!(status, 204);
    assert!(ours
        .refused("example-documents", "archive/b.pdf", None)
        .await
        .contains("bucket_retired"));
    let (status, held) = admin
        .put(&globex.slug, "example-documents", json!({}))
        .await;
    assert_eq!(status, 409, "{held}");
    assert!(held.to_string().contains("release"), "{held}");
    let release = "/api/buckets/example-documents/release";
    let (status, early) = platform.send(Method::POST, release, &admin.key, None).await;
    assert_eq!(
        status, 409,
        "released before every replica reloaded: {early}"
    );
    client
        .execute(
            "UPDATE tenant_buckets SET retired_at = now() - interval '10 minutes' WHERE bucket = 'example-documents'",
            &[],
        )
        .await
        .expect("backdated");
    let (status, _) = platform.send(Method::POST, release, &admin.key, None).await;
    assert_eq!(status, 204);
    let (status, _) = platform.send(Method::POST, release, &admin.key, None).await;
    assert_eq!(status, 404);
    let (status, moved) = admin
        .put(&globex.slug, "example-documents", json!({}))
        .await;
    assert_eq!(status, 200, "{moved}");
    assert_eq!(
        theirs.upload("example-documents", "a.pdf", None).await,
        json!({"size": 5})
    );
    assert!(ours
        .refused("example-documents", "archive/b.pdf", None)
        .await
        .contains("foreign_tenant_bucket"));

    let orchard = db.mint_tenant().await;
    let (status, _) = admin.put(&orchard.slug, "example-orchard", json!({})).await;
    assert_eq!(status, 200);
    let (status, deleted) = platform
        .send(
            Method::DELETE,
            &format!("/api/tenants/{}", orchard.slug),
            &admin.key,
            None,
        )
        .await;
    assert_eq!(status, 200, "{deleted}");
    let orphan = client
        .query_one(
            "SELECT tenant_id IS NULL AS orphan, retired_at IS NOT NULL AS retired
             FROM tenant_buckets WHERE bucket = 'example-orchard'",
            &[],
        )
        .await
        .expect("the deleted tenant's claim");
    assert!(orphan.get::<_, bool>("orphan") && orphan.get::<_, bool>("retired"));
    assert!(ours
        .refused("example-orchard", "a.pdf", None)
        .await
        .contains("foreign_tenant_bucket"));
    let (status, held) = admin.put(&harbor.slug, "example-orchard", json!({})).await;
    assert_eq!(status, 409, "{held}");
    assert!(held.to_string().contains("deleted tenant"), "{held}");
}
