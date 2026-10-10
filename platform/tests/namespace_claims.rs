use cufflink_testkit as common;

use common::{PlatformProcess, Tenant};
use reqwest::Method;
use serde_json::{json, Value};

fn subscribing(service: &str, subject: &str) -> Value {
    json!({
        "name": service,
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{"name": "loans", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
            {"name": "book_id", "column_type": "uuid", "primary_key": false, "nullable": false,
             "auto_generate": false},
        ]}],
        "subscriptions": [{
            "subject": subject,
            "handler": "drop_loans",
            "handler_type": "delete_cascade",
            "config": {"table": "loans", "foreign_key": "book_id"},
        }],
        "publishes": [],
    })
}

fn publishing(service: &str, pattern: &str) -> Value {
    json!({
        "name": service,
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{"name": "notes", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
        ]}],
        "publishes": [pattern],
    })
}

struct Claims<'a> {
    platform: &'a PlatformProcess,
    admin: String,
}

impl Claims<'_> {
    async fn put(&self, tenant: &Tenant, body: Value) -> (u16, Value) {
        self.platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/nats-claims", tenant.slug),
                &self.admin,
                Some(&body),
            )
            .await
    }

    async fn get(&self, tenant: &Tenant) -> Value {
        let (status, body) = self
            .platform
            .send(
                Method::GET,
                &format!("/api/tenants/{}/nats-claims", tenant.slug),
                &self.admin,
                None,
            )
            .await;
        assert_eq!(status, 200, "{body}");
        body
    }
}

#[tokio::test]
async fn namespace_claims_enforced() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let harbor = db.mint_tenant().await;
    let acme = db.mint_tenant().await;
    let claims = admin_claims(&platform, db.mint_admin_key().await);
    let books = format!("books{}", uuid::Uuid::new_v4().simple());
    let stream = books.to_uppercase();

    assert_eq!(
        claims.get(&harbor).await,
        json!({"tenant": harbor.slug, "subjects": [], "streams": []})
    );
    let subscription = subscribing("loan-service", &format!("{books}.status"));
    let (status, refused) = platform.deploy(&harbor, &subscription).await;
    assert_eq!(status, 422, "{refused}");
    assert_eq!(
        refused["error"],
        json!(format!(
            "Subscription 'drop_loans': subject '{books}.status' is outside this tenant's \
             dw.{}.* namespace",
            harbor.slug
        ))
    );

    let (status, set) = claims
        .put(
            &harbor,
            json!({"subjects": [format!("{books}.>"), format!("audit{books}.logs")], "streams": [stream]}),
        )
        .await;
    assert_eq!(status, 200, "{set}");
    let listed = json!({
        "tenant": harbor.slug,
        "subjects": [format!("audit{books}.logs"), format!("{books}.>")],
        "streams": [stream],
    });
    assert_eq!(set, listed);
    assert_eq!(claims.get(&harbor).await, listed);
    let (status, subscriber) = platform.deploy(&harbor, &subscription).await;
    assert_eq!(
        status, 200,
        "a claimed subject is the tenant's: {subscriber}"
    );

    let (status, foreign) = platform.deploy(&acme, &subscription).await;
    assert_eq!(
        status, 422,
        "another tenant's claim is not this one's: {foreign}"
    );

    for (body, status, reason) in [
        (
            json!({"subjects": [format!("{books}.status")]}),
            409,
            format!(
                "subjects: '{books}.status' overlaps '{books}.>', which tenant '{}' claims",
                harbor.slug
            ),
        ),
        (
            json!({"subjects": ["*.logs"]}),
            422,
            "subjects: '*.logs' reaches into 'dw.>', which no tenant claims".to_string(),
        ),
        (
            json!({"streams": [stream]}),
            409,
            format!(
                "streams: '{stream}' overlaps '{stream}', which tenant '{}' claims",
                harbor.slug
            ),
        ),
        (
            json!({"subjects": ["dw.elsewhere.>"]}),
            422,
            "subjects: 'dw.elsewhere.>' reaches into 'dw.>', which no tenant claims".to_string(),
        ),
        (
            json!({"streams": ["CUFFLINK_DLQ"]}),
            422,
            "streams: 'CUFFLINK_DLQ' is named like the streams the platform creates (CUFFLINK_*)"
                .to_string(),
        ),
        (
            json!({"subjects": ["receipts.>", "receipts.>"]}),
            422,
            "subjects: 'receipts.>' is claimed twice".to_string(),
        ),
    ] {
        let (answered, refused) = claims.put(&acme, body.clone()).await;
        assert_eq!(answered, status, "{body}: {refused}");
        assert_eq!(refused["error"], json!(reason), "{body}");
    }
    assert_eq!(
        claims.get(&acme).await,
        json!({"tenant": acme.slug, "subjects": [], "streams": []}),
        "a refused write changes nothing"
    );

    let (status, overlapping) = platform
        .deploy(&acme, &publishing("spoofer", &format!("{books}.created")))
        .await;
    assert_eq!(status, 422, "{overlapping}");
    assert_eq!(
        overlapping["error"],
        json!(format!(
            "publishes: '{books}.created' overlaps '{books}.>', which tenant '{}' claims",
            harbor.slug
        ))
    );
    let (status, own) = platform
        .deploy(
            &harbor,
            &publishing("catalog-service", &format!("{books}.created")),
        )
        .await;
    assert_eq!(status, 200, "{own}");

    let (status, set) = claims.put(&acme, json!({"subjects": ["receipts.>"]})).await;
    assert_eq!(status, 200, "{set}");
    let given_up = json!({"subjects": [format!("audit{books}.logs")]});
    let (status, stranding) = claims.put(&harbor, given_up.clone()).await;
    assert_eq!(
        status, 409,
        "a claim a deployed service subscribes under is not given up: {stranding}"
    );
    for service in [&subscriber, &own] {
        let (status, deleted) = platform
            .send(
                Method::DELETE,
                &format!(
                    "/api/services/{}",
                    service["service_id"].as_str().expect("service id")
                ),
                &format!("ApiKey {}", harbor.api_key),
                None,
            )
            .await;
        assert_eq!(status, 200, "{deleted}");
    }
    let (status, replaced) = claims.put(&harbor, given_up).await;
    assert_eq!(status, 200, "{replaced}");
    assert_eq!(
        replaced,
        json!({"tenant": harbor.slug, "subjects": [format!("audit{books}.logs")], "streams": []})
    );
    let (status, released) = platform
        .deploy(
            &harbor,
            &subscribing("loan-service-2", &format!("{books}.status")),
        )
        .await;
    assert_eq!(
        status, 422,
        "a claim given up is no longer the tenant's: {released}"
    );
    let (status, taken) = claims
        .put(&acme, json!({"subjects": [format!("{books}.>")]}))
        .await;
    assert_eq!(status, 200, "and another tenant may claim it: {taken}");

    let (status, _) = platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}/nats-claims", harbor.slug),
            &format!("ApiKey {}", harbor.api_key),
            Some(&json!({"subjects": ["anything.>"]})),
        )
        .await;
    assert_eq!(status, 401, "a tenant cannot claim for itself");
    let (status, _) = claims
        .platform
        .send(
            Method::PUT,
            "/api/tenants/no-such-tenant/nats-claims",
            &claims.admin,
            Some(&json!({})),
        )
        .await;
    assert_eq!(status, 404);
    let (status, body) = claims.put(&harbor, json!({"subject": ["a.b"]})).await;
    assert!(
        (400..500).contains(&status),
        "a misspelt field is refused: {status} {body}"
    );
}

fn admin_claims<'a>(platform: &'a PlatformProcess, key: String) -> Claims<'a> {
    Claims {
        platform,
        admin: format!("ApiKey {key}"),
    }
}

fn publisher(service: &str, publishes: Option<&[&str]>) -> Value {
    let mut manifest = common::wasm_manifest(
        service,
        &[
            ("publish_claimed", "/claimed"),
            ("publish_unclaimed", "/unclaimed"),
        ],
    );
    match (publishes, manifest.as_object_mut()) {
        (Some(allowed), Some(fields)) => {
            fields.insert("publishes".into(), json!(allowed));
        }
        (None, Some(fields)) => {
            fields.remove("publishes");
        }
        (_, None) => {}
    }
    manifest
}

async fn arrivals(
    subscription: &mut async_nats::Subscriber,
    expected: usize,
    publish: impl std::future::Future<Output = (u16, Value)>,
) -> (Value, usize) {
    use futures_util::StreamExt;
    let (status, answer) = publish.await;
    assert_eq!(status, 200, "{answer}");
    let mut arrived = 0;
    while tokio::time::timeout(
        if arrived < expected {
            std::time::Duration::from_secs(10)
        } else {
            std::time::Duration::from_millis(500)
        },
        subscription.next(),
    )
    .await
    .is_ok_and(|message| message.is_some())
    {
        arrived += 1;
    }
    (answer, arrived)
}

#[tokio::test]
async fn a_guest_cannot_publish_into_what_another_tenant_claims() {
    const CLAIMED: &str = "itclaimed.books.created";
    const UNCLAIMED: &str = "itunclaimed.books.created";

    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let owner = db.mint_tenant().await;
    let other = db.mint_tenant().await;
    let claims = admin_claims(&platform, db.mint_admin_key().await);
    let nats = async_nats::connect(nats_url).await.expect("nats");
    let (mut claimed, mut unclaimed) = (
        nats.subscribe(CLAIMED).await.expect("subscribe"),
        nats.subscribe(UNCLAIMED).await.expect("subscribe"),
    );
    nats.flush().await.expect("flush");

    let (status, deployed) = platform
        .deploy(&other, &publisher("legacy-publisher", None))
        .await;
    assert_eq!(status, 200, "{deployed}");
    let (status, deployed) = platform
        .deploy(
            &other,
            &publisher("declared-publisher", Some(&["itunclaimed.>"])),
        )
        .await;
    assert_eq!(status, 200, "{deployed}");

    for service in ["legacy-publisher", "declared-publisher"] {
        let (answer, arrived) = arrivals(
            &mut unclaimed,
            1,
            platform.call_fn(&other, service, "publish_unclaimed"),
        )
        .await;
        assert_eq!((answer, arrived), (json!({"rc": 0}), 1), "{service}");
    }
    let (answer, arrived) = arrivals(
        &mut claimed,
        1,
        platform.call_fn(&other, "legacy-publisher", "publish_claimed"),
    )
    .await;
    assert_eq!(
        (answer, arrived),
        (json!({"rc": 0}), 1),
        "before anyone claims it, a legacy guest publishes where it always could"
    );

    let (status, set) = claims
        .put(&owner, json!({"subjects": ["itclaimed.>"]}))
        .await;
    assert_eq!(status, 200, "{set}");

    for service in ["legacy-publisher", "declared-publisher"] {
        let (answer, arrived) = arrivals(
            &mut claimed,
            0,
            platform.call_fn(&other, service, "publish_claimed"),
        )
        .await;
        assert_eq!((answer, arrived), (json!({"rc": -1}), 0), "{service}");
        let (answer, arrived) = arrivals(
            &mut unclaimed,
            1,
            platform.call_fn(&other, service, "publish_unclaimed"),
        )
        .await;
        assert_eq!(
            (answer, arrived),
            (json!({"rc": 0}), 1),
            "{service}: what no tenant claims is published as before"
        );
    }
    assert_eq!(
        platform
            .metric(
                "cufflink_nats_publish_denied_total",
                &[("reason", "claimed")]
            )
            .await,
        Some(2.0)
    );

    let (status, deployed) = platform
        .deploy(&owner, &publisher("owner-publisher", Some(&[])))
        .await;
    assert_eq!(status, 200, "{deployed}");
    let (answer, arrived) = arrivals(
        &mut claimed,
        1,
        platform.call_fn(&owner, "owner-publisher", "publish_claimed"),
    )
    .await;
    assert_eq!(
        (answer, arrived),
        (json!({"rc": 0}), 1),
        "what a tenant claims is its namespace"
    );
}

#[tokio::test]
async fn claims_strand_no_deployed_service_and_end_with_their_tenant() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let harbor = db.mint_tenant().await;
    let acme = db.mint_tenant().await;
    let claims = admin_claims(&platform, db.mint_admin_key().await);
    let root = format!("strand{}", uuid::Uuid::new_v4().simple());
    let claimed = json!({"subjects": [format!("{root}.>")]});

    let (status, set) = claims.put(&harbor, claimed.clone()).await;
    assert_eq!(status, 200, "{set}");
    let (status, deployed) = platform
        .deploy(
            &harbor,
            &subscribing("loan-service", &format!("{root}.status")),
        )
        .await;
    assert_eq!(status, 200, "{deployed}");

    for shrunk in [json!({}), json!({"subjects": [format!("{root}.other")]})] {
        let (status, refused) = claims.put(&harbor, shrunk.clone()).await;
        assert_eq!(status, 409, "{shrunk}: {refused}");
        assert_eq!(
            refused["error"],
            json!(format!(
                "service 'loan-service' of tenant '{slug}' is deployed with what these claims \
                 take from it: Subscription 'drop_loans': subject '{root}.status' is outside \
                 this tenant's dw.{slug}.* namespace{and_claims}",
                slug = harbor.slug,
                and_claims = if shrunk == json!({}) {
                    ""
                } else {
                    " and its claims"
                },
            ))
        );
    }
    assert_eq!(claims.get(&harbor).await["subjects"], claimed["subjects"]);
    let (status, widened) = claims
        .put(
            &harbor,
            json!({"subjects": [format!("{root}.>"), format!("{root}extra.>")]}),
        )
        .await;
    assert_eq!(status, 200, "{widened}");

    let publishes = format!("pub{root}.created");
    let (status, deployed) = platform
        .deploy(&acme, &publishing("notes", &publishes))
        .await;
    assert_eq!(status, 200, "{deployed}");
    let (status, refused) = claims
        .put(
            &harbor,
            json!({"subjects": [format!("{root}.>"), format!("pub{root}.>")]}),
        )
        .await;
    assert_eq!(status, 409, "{refused}");
    assert_eq!(
        refused["error"],
        json!(format!(
            "service 'notes' of tenant '{}' is deployed with what these claims take from it: \
             publishes: '{publishes}' overlaps 'pub{root}.>', which tenant '{}' claims",
            acme.slug, harbor.slug
        ))
    );

    let (status, taken) = claims.put(&acme, claimed.clone()).await;
    assert_eq!(status, 409, "{taken}");
    let (status, deleted) = platform
        .send(
            Method::DELETE,
            &format!("/api/tenants/{}", harbor.slug),
            &claims.admin,
            None,
        )
        .await;
    assert_eq!(status, 200, "{deleted}");
    let (status, freed) = claims.put(&acme, claimed.clone()).await;
    assert_eq!(status, 200, "a deleted tenant claims nothing: {freed}");
    assert_eq!(claims.get(&acme).await["subjects"], claimed["subjects"]);
}

#[tokio::test]
async fn overlapping_claims_made_at_once_are_one_tenants() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let (harbor, acme) = (db.mint_tenant().await, db.mint_tenant().await);
    let claims = admin_claims(&platform, db.mint_admin_key().await);
    let root = format!("race{}", uuid::Uuid::new_v4().simple());

    for round in 0..5 {
        let (wide, narrow) = (
            json!({"subjects": [format!("{root}{round}.>")]}),
            json!({"subjects": [format!("{root}{round}.books.*")]}),
        );
        let (one, other) = tokio::join!(claims.put(&harbor, wide), claims.put(&acme, narrow));
        let mut statuses = [one.0, other.0];
        statuses.sort_unstable();
        assert_eq!(statuses, [200, 409], "{} / {}", one.1, other.1);
        let holders = [&harbor, &acme]
            .into_iter()
            .zip([one.0, other.0])
            .filter(|(_, status)| *status == 200)
            .count();
        assert_eq!(holders, 1);
        for tenant in [&harbor, &acme] {
            let (status, cleared) = claims.put(tenant, json!({})).await;
            assert_eq!(status, 200, "{cleared}");
        }
    }
}
