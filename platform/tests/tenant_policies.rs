use cufflink_testkit as common;

use common::PlatformProcess;
use cufflink_platform::policy::{PolicyKey, PolicyMode, PolicySet, PolicySource};
use cufflink_platform::repo::tenant_policies;
use cufflink_platform::repo::tenants::{self, NewTenant};
use cufflink_platform::shutdown::Shutdown;
use cufflink_platform::tenant_catalog::TenantCatalog;
use reqwest::Method;
use serde_json::json;
use uuid::Uuid;

async fn tenant(pool: &deadpool_postgres::Pool, slug: &str) -> tenants::Tenant {
    tenants::create_tenant(
        &pool.get().await.expect("checkout"),
        &NewTenant {
            name: slug,
            slug,
            keycloak_realm: Some("orchard"),
            ..Default::default()
        },
    )
    .await
    .expect("tenant")
}

#[tokio::test]
async fn repository_upserts_lists_deletes_and_clones() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_migrated_database().await;
    let pool = db.pool();
    let source = tenant(&pool, "source").await;
    let target = tenant(&pool, "target").await;
    let client = pool.get().await.expect("checkout");

    tenant_policies::upsert(
        &client,
        source.id,
        PolicyKey::Egress,
        PolicyMode::Warn,
        &json!({"allow_hosts": ["keycloak"]}),
        "admin",
    )
    .await
    .expect("insert");
    let updated = tenant_policies::upsert(
        &client,
        source.id,
        PolicyKey::Egress,
        PolicyMode::Enforce,
        &json!({"allow_hosts": ["keycloak", "dashboards"]}),
        "admin-2",
    )
    .await
    .expect("update");
    assert_eq!(updated.mode, PolicyMode::Enforce);
    assert_eq!(updated.updated_by.as_deref(), Some("admin-2"));
    tenant_policies::upsert(
        &client,
        source.id,
        PolicyKey::Graphql,
        PolicyMode::Enforce,
        &json!({}),
        "admin",
    )
    .await
    .expect("insert");
    tenant_policies::upsert(
        &client,
        target.id,
        PolicyKey::Graphql,
        PolicyMode::Off,
        &json!({}),
        "admin",
    )
    .await
    .expect("target's own row");

    let rows = tenant_policies::list_for_tenant(&client, source.id)
        .await
        .expect("list");
    assert_eq!(
        rows.iter().map(|r| r.key.as_str()).collect::<Vec<_>>(),
        vec!["egress", "graphql"]
    );

    assert_eq!(
        tenant_policies::clone_from(&client, source.id, target.id)
            .await
            .expect("clone"),
        1,
        "only the row the target lacks is copied"
    );
    let target_rows = tenant_policies::list_for_tenant(&client, target.id)
        .await
        .expect("list");
    let target_set = PolicySet::builtin_defaults().with_tenant_rows(&target_rows);
    assert_eq!(target_set.mode(PolicyKey::Egress), PolicyMode::Enforce);
    assert_eq!(target_set.mode(PolicyKey::Graphql), PolicyMode::Off);
    assert_eq!(
        tenant_policies::list_all(&pool).await.expect("all").len(),
        4
    );

    assert!(tenant_policies::delete(&pool, source.id, PolicyKey::Egress)
        .await
        .expect("delete"));
    assert!(
        !tenant_policies::delete(&pool, source.id, PolicyKey::Egress)
            .await
            .expect("delete again")
    );
}

#[tokio::test]
async fn catalog_reloads_loads_new_tenants_and_keeps_its_snapshot_on_error() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_migrated_database().await;
    let pool = db.pool();
    let known = tenant(&pool, "known").await;
    tenant_policies::upsert(
        &pool.get().await.expect("checkout"),
        known.id,
        PolicyKey::TenantBinding,
        PolicyMode::Enforce,
        &json!({}),
        "admin",
    )
    .await
    .expect("policy");

    let nats = cufflink_platform::nats_conn::connect(
        "nats://127.0.0.1:1",
        "catalog-test",
        &cufflink_platform::nats_conn::NatsAuth::Anonymous,
    )
    .await
    .expect("offline nats client");
    let catalog = TenantCatalog::new(pool.clone(), nats, PolicySet::builtin_defaults());
    assert!(
        catalog.get(known.id).is_none(),
        "nothing is read before a reload"
    );
    catalog.reload().await.expect("reload");
    let entry = catalog.get_by_slug("known").expect("known tenant");
    assert_eq!(entry.mode(PolicyKey::TenantBinding), PolicyMode::Enforce);
    assert_eq!(
        entry.policies.get(PolicyKey::TenantBinding).source,
        PolicySource::Tenant
    );

    let created_later = tenant(&pool, "created-later").await;
    assert!(catalog.get(created_later.id).is_none());
    let loaded = catalog
        .get_or_load(created_later.id)
        .await
        .expect("load")
        .expect("tenant exists");
    assert_eq!(loaded.tenant.slug, "created-later");
    assert!(
        catalog.get(created_later.id).is_some(),
        "the load is cached"
    );
    let by_slug = tenant(&pool, "by-slug").await;
    assert_eq!(
        catalog
            .get_or_load_by_slug("by-slug")
            .await
            .expect("load")
            .map(|entry| entry.tenant.id),
        Some(by_slug.id)
    );
    assert!(catalog
        .get_or_load_by_slug("no-such-tenant")
        .await
        .expect("load")
        .is_none());
    assert!(catalog
        .get_or_load(Uuid::new_v4())
        .await
        .expect("load")
        .is_none());
    assert_eq!(
        *catalog
            .policies_or_load(Uuid::new_v4())
            .await
            .expect("defaults"),
        PolicySet::builtin_defaults()
    );

    let client = pool.get().await.expect("checkout");
    client
        .batch_execute("ALTER TABLE tenant_policies RENAME TO tenant_policies_gone")
        .await
        .expect("break the table");
    assert!(catalog.reload().await.is_err());
    assert_eq!(
        catalog
            .get(known.id)
            .expect("still cached")
            .mode(PolicyKey::TenantBinding),
        PolicyMode::Enforce,
        "a failed reload keeps the previous snapshot"
    );
    client
        .batch_execute("ALTER TABLE tenant_policies_gone RENAME TO tenant_policies")
        .await
        .expect("restore the table");
}

#[tokio::test]
async fn a_policy_change_on_one_replica_reaches_another_at_once() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let db = infra.create_migrated_database().await;
    let pool = db.pool();
    let target = tenant(&pool, "shared").await;
    let replica = || async {
        let nats = cufflink_platform::nats_conn::connect(
            nats_url,
            "catalog-replica",
            &cufflink_platform::nats_conn::NatsAuth::Anonymous,
        )
        .await
        .expect("nats client");
        let catalog = TenantCatalog::new(pool.clone(), nats, PolicySet::builtin_defaults());
        catalog.reload().await.expect("reload");
        catalog
    };
    let (writer, reader) = (replica().await, replica().await);
    let shutdown = Shutdown::new(std::time::Duration::from_secs(1));
    reader.start_sync_listener(&shutdown);
    tenant_policies::upsert(
        &pool.get().await.expect("checkout"),
        target.id,
        PolicyKey::Graphql,
        PolicyMode::Enforce,
        &json!({}),
        "admin",
    )
    .await
    .expect("policy");

    let seen = async {
        loop {
            writer.refresh().await;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            if reader
                .get(target.id)
                .is_some_and(|entry| entry.mode(PolicyKey::Graphql) == PolicyMode::Enforce)
            {
                return;
            }
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), seen)
        .await
        .expect("the notification reloads the other replica well before its 60 s timer");
    shutdown.trigger();
}

#[tokio::test]
async fn admin_api_flips_a_policy_without_a_restart() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let policies = format!("/api/tenants/{}/policies", tenant.slug);

    let (status, body) = platform
        .deploy(&tenant, &common::wasm_manifest("hog", &[("hog", "/hog")]))
        .await;
    assert_eq!(status, 200, "deploy failed: {body}");
    let (status, body) = platform.call_fn(&tenant, "hog", "hog").await;
    assert_eq!(status, 200, "warn is the platform default: {body}");

    let (status, body) = platform.send(Method::GET, &policies, &admin, None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["policies"]["wasm_limits"]["mode"], "warn");
    assert_eq!(
        body["policies"]["wasm_limits"]["source"],
        "platform_default"
    );

    let enforce = json!({"policies": {"wasm_limits": {"mode": "enforce"}}});
    let (status, body) = platform
        .send(Method::PUT, &policies, &admin, Some(&enforce))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["policies"]["wasm_limits"]["mode"], "enforce");
    assert_eq!(body["policies"]["wasm_limits"]["source"], "tenant");
    let (status, body) = platform.call_fn(&tenant, "hog", "hog").await;
    assert_eq!(status, 500, "enforced without a restart: {body}");

    let (status, _) = platform
        .send(
            Method::DELETE,
            &format!("{policies}/wasm_limits"),
            &admin,
            None,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = platform.call_fn(&tenant, "hog", "hog").await;
    assert_eq!(status, 200, "back to the platform default: {body}");
    let (status, _) = platform
        .send(
            Method::DELETE,
            &format!("{policies}/wasm_limits"),
            &admin,
            None,
        )
        .await;
    assert_eq!(status, 404, "no tenant row left to delete");

    let invalid = [
        json!({"policies": {"secret_rotation": {"mode": "warn"}}}),
        json!({"policies": {"audience": {"mode": "enforce"}}}),
        json!({"policies": {"outbox": {"mode": "warn"}}}),
    ];
    for body in &invalid {
        let (status, response) = platform
            .send(Method::PUT, &policies, &admin, Some(body))
            .await;
        assert_eq!(status, 422, "{response}");
    }

    let settable = json!({"policies": {
        "outbox": {"mode": "enforce"},
        "db_roles": {"mode": "warn", "config": {"readers": ["reporting_owner"]}},
        "schema_guard": {"mode": "warn"},
    }});
    let (status, body) = platform
        .send(Method::PUT, &policies, &admin, Some(&settable))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["policies"]["outbox"],
        json!({"mode": "enforce", "config": {}, "source": "tenant"})
    );
    assert_eq!(
        body["policies"]["db_roles"],
        json!({
            "mode": "warn",
            "config": {"readers": ["reporting_owner"]},
            "source": "tenant",
        })
    );
    assert_eq!(
        body["policies"]["schema_guard"]["config"],
        json!({"accept_extra_columns": true})
    );
    let (status, body) = platform
        .send(
            Method::PUT,
            &policies,
            &admin,
            Some(&json!({"policies": {"db_roles": {"mode": "enforce"}}})),
        )
        .await;
    assert_eq!(status, 422, "{body}");
    assert_eq!(
        body["error"],
        "db_roles: enforce needs a verified database role for every service of the tenant; \
         without one: hog"
    );
    let (status, body) = platform.call_fn(&tenant, "hog", "hog").await;
    assert_eq!(status, 200, "the service runs as it did: {body}");
    let (status, _) = platform
        .send(
            Method::GET,
            "/api/tenants/no-such-tenant/policies",
            &admin,
            None,
        )
        .await;
    assert_eq!(status, 404);
    let tenant_key = format!("ApiKey {}", tenant.api_key);
    let (status, _) = platform
        .send(Method::GET, &policies, &tenant_key, None)
        .await;
    assert_eq!(status, 401, "a tenant key is not a platform admin");
}

#[tokio::test]
async fn previews_inherit_policies_service_accounts_and_issuer() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);

    let source = format!("src-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let preview = format!("preview-{source}");
    db.adopt_tenant_db(&source);
    db.adopt_tenant_db(&preview);
    let (status, body) = platform
        .send(
            Method::POST,
            "/api/tenants",
            &admin,
            Some(&json!({
                "name": "Source",
                "slug": source,
                "keycloak_realm": "orchard",
                "service_account_clients": ["orchard-ops"],
                "keycloak_issuer": "https://auth.example.test",
            })),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["service_account_clients"], json!(["orchard-ops"]));
    assert_eq!(body["source_tenant_id"], serde_json::Value::Null);
    let source_id = body["id"].clone();
    let (status, _) = platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{source}/policies"),
            &admin,
            Some(&json!({"policies": {"graphql": {"mode": "enforce"}}})),
        )
        .await;
    assert_eq!(status, 200);

    let (status, body) = platform
        .send(
            Method::POST,
            "/api/tenants",
            &admin,
            Some(&json!({
                "name": "Preview",
                "slug": preview,
                "keycloak_realm": "orchard",
                "clone_rbac_from": source,
            })),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["service_account_clients"], json!(["orchard-ops"]));
    assert_eq!(body["keycloak_issuer"], "https://auth.example.test");
    assert_eq!(
        body["source_tenant_id"], source_id,
        "joins the source's group"
    );

    let nested = format!("{preview}-2");
    db.adopt_tenant_db(&nested);
    let (status, body) = platform
        .send(
            Method::POST,
            "/api/tenants",
            &admin,
            Some(&json!({
                "name": "Nested",
                "slug": nested,
                "keycloak_realm": "orchard",
                "clone_rbac_from": preview,
            })),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["source_tenant_id"], source_id,
        "a preview of a preview joins the root group"
    );
    let (_, body) = platform
        .send(
            Method::GET,
            &format!("/api/tenants/{preview}/policies"),
            &admin,
            None,
        )
        .await;
    assert_eq!(body["policies"]["graphql"]["mode"], "enforce");
    assert_eq!(body["policies"]["graphql"]["source"], "tenant");

    let (status, body) = platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{preview}"),
            &admin,
            Some(&json!({"service_account_clients": [], "keycloak_issuer": ""})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["service_account_clients"], json!([]));
    assert_eq!(body["keycloak_issuer"], serde_json::Value::Null);

    for slug in ["Preview_1", "../etc", ""] {
        let (status, body) = platform
            .send(
                Method::POST,
                "/api/tenants",
                &admin,
                Some(&json!({"name": "x", "slug": slug})),
            )
            .await;
        assert_eq!(status, 422, "{slug}: {body}");
    }
}
