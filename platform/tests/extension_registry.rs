use cufflink_testkit as common;

use common::TestDb;
use cufflink_platform::errors::PlatformError;
use cufflink_platform::repo::deployments::{self, ActivateVersion, Artifact, NativeVersion};
use cufflink_platform::repo::{extensions, services};
use cufflink_types::{ProvidedExtension, ServiceManifest, ServiceMode};
use deadpool_postgres::Pool;
use std::time::Duration;
use uuid::Uuid;

fn calling(name: &str, calls: &[&str]) -> ServiceManifest {
    ServiceManifest {
        name: name.into(),
        extensions: calls.iter().map(|call| call.to_string()).collect(),
        ..Default::default()
    }
}

fn providing(name: &str, provides: &[(&str, &[&str])]) -> ServiceManifest {
    ServiceManifest {
        name: name.into(),
        mode: ServiceMode::Native,
        provides: provides
            .iter()
            .map(|(name, callers)| ProvidedExtension {
                name: name.to_string(),
                callers: callers.iter().map(|caller| caller.to_string()).collect(),
            })
            .collect(),
        ..Default::default()
    }
}

async fn activate(
    pool: &Pool,
    service_id: Uuid,
    version: i32,
    manifest: &ServiceManifest,
) -> Result<(), PlatformError> {
    let artifact = Artifact {
        s3_key: format!("native/{service_id}/{version}"),
        hash: version.to_string(),
        size: 1,
    };
    deployments::activate_version(
        pool,
        ActivateVersion {
            service_id,
            version,
            manifest_hash: "m",
            manifest,
            wasm_hash: None,
            commit_hash: None,
            schema_json: None,
            schema_hash: "",
            native: matches!(manifest.mode, ServiceMode::Native).then_some(NativeVersion {
                artifact: &artifact,
                actor: "admin",
            }),
        },
    )
    .await
    .map(drop)
}

async fn register(pool: &Pool, tenant_id: Uuid, name: &str, callers: &[&str]) {
    let callers: Vec<String> = callers.iter().map(|caller| caller.to_string()).collect();
    extensions::register_nats(
        &pool.get().await.expect("checkout"),
        tenant_id,
        name,
        &callers,
        "admin",
    )
    .await
    .expect("registered")
    .expect("a NATS provider");
}

async fn current_version(pool: &Pool, service_id: Uuid) -> i32 {
    services::get_by_id(pool, service_id)
        .await
        .expect("lookup")
        .expect("service")
        .current_version
}

async fn world() -> Option<(TestDb, Uuid)> {
    let infra = common::infra().await?;
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    Some((db, tenant.id))
}

#[tokio::test]
async fn a_version_is_activated_only_while_its_registrations_admit_it() {
    let Some((db, tenant_id)) = world().await else {
        return;
    };
    let pool = db.pool();
    let service = cufflink_testkit::active_service(&pool, tenant_id, "reports", "reports")
        .await
        .expect("service");
    let manifest = calling("reports", &["google.token"]);
    let next = service.current_version + 1;

    let refused = activate(&pool, service.id, next, &manifest).await;
    assert!(
        matches!(&refused, Err(PlatformError::Validation(m))
            if m == "extensions: 'google.token' is not registered for this tenant"),
        "{refused:?}"
    );
    register(&pool, tenant_id, "google.token", &["billing"]).await;
    let refused = activate(&pool, service.id, next, &manifest).await;
    assert!(
        matches!(&refused, Err(PlatformError::Validation(m)) if m.contains("does not list 'reports'")),
        "{refused:?}"
    );
    assert_eq!(
        current_version(&pool, service.id).await,
        service.current_version
    );
    assert!(
        deployments::list_by_service(&pool, service.id)
            .await
            .expect("deployments")
            .is_empty(),
        "a refused activation records nothing"
    );

    register(&pool, tenant_id, "google.token", &["reports"]).await;
    activate(&pool, service.id, next, &manifest)
        .await
        .expect("admitted");
    assert_eq!(current_version(&pool, service.id).await, next);
}

#[tokio::test]
async fn an_activation_holds_the_registrations_it_was_admitted_by() {
    let Some((db, tenant_id)) = world().await else {
        return;
    };
    let pool = db.pool();
    register(&pool, tenant_id, "google.token", &["*"]).await;
    let names = vec!["google.token".to_string()];

    let mut activating = pool.get().await.expect("checkout");
    let activation = activating.transaction().await.expect("transaction");
    let held = extensions::shared(&activation, tenant_id, &names)
        .await
        .expect("shared");
    assert_eq!(held.len(), 1);
    let operator = pool.get().await.expect("checkout");
    operator
        .batch_execute("SET lock_timeout = '200ms'")
        .await
        .expect("lock timeout");
    let removal = operator
        .execute(
            "DELETE FROM tenant_extensions WHERE tenant_id = $1 AND name = 'google.token'",
            &[&tenant_id],
        )
        .await;
    assert!(
        removal.is_err(),
        "the operator waits for the activation to end"
    );
    activation.commit().await.expect("commit");

    let mut removing = pool.get().await.expect("checkout");
    let removal = removing.transaction().await.expect("transaction");
    extensions::locked(&removal, tenant_id, "google.token")
        .await
        .expect("locked")
        .expect("registered");
    let deploying = {
        let pool = pool.clone();
        let names = names.clone();
        tokio::spawn(async move {
            let mut client = pool.get().await.expect("checkout");
            let activation = client.transaction().await.expect("transaction");
            let found = extensions::shared(&activation, tenant_id, &names)
                .await
                .expect("shared");
            activation.commit().await.expect("commit");
            found
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !deploying.is_finished(),
        "the activation waits for the operator"
    );
    assert!(
        extensions::unregister_nats(&removal, tenant_id, "google.token")
            .await
            .expect("unregistered")
    );
    removal.commit().await.expect("commit");
    assert!(
        deploying.await.expect("activation").is_empty(),
        "and then finds the registration gone"
    );
}

#[tokio::test]
async fn a_native_activation_neither_strands_a_caller_nor_stores_callers_unsorted() {
    let Some((db, tenant_id)) = world().await else {
        return;
    };
    let pool = db.pool();
    let native = cufflink_testkit::active_service(&pool, tenant_id, "ext-google", "ext_google")
        .await
        .expect("native service");
    let caller = cufflink_testkit::active_service(&pool, tenant_id, "reports", "reports")
        .await
        .expect("caller");

    activate(
        &pool,
        native.id,
        2,
        &providing(
            "ext-google",
            &[("google.token", &["reports", "*", "reports"])],
        ),
    )
    .await
    .expect("provided");
    let stored = extensions::registrations(&pool.get().await.expect("checkout"), tenant_id)
        .await
        .expect("registrations");
    assert_eq!(stored["google.token"].callers, ["*", "reports"]);

    activate(&pool, caller.id, 2, &calling("reports", &["google.token"]))
        .await
        .expect("the caller is admitted");
    for (version, narrowed) in [
        (
            3,
            providing("ext-google", &[("google.token", &["billing"])]),
        ),
        (3, providing("ext-google", &[("other.one", &["*"])])),
    ] {
        let refused = activate(&pool, native.id, version, &narrowed).await;
        assert!(
            matches!(&refused, Err(PlatformError::Conflict(m))
                if m == "provides: service 'reports' is deployed calling extension \
                         'google.token'; redeploy it without the extension first"),
            "{refused:?}"
        );
    }
    assert_eq!(current_version(&pool, native.id).await, 2);
    let kept = extensions::registrations(&pool.get().await.expect("checkout"), tenant_id)
        .await
        .expect("registrations");
    assert_eq!(kept.keys().collect::<Vec<_>>(), ["google.token"]);

    services::update_status(&pool, caller.id, services::Status::Inactive)
        .await
        .expect("inactive");
    activate(
        &pool,
        native.id,
        3,
        &providing("ext-google", &[("other.one", &["*"])]),
    )
    .await
    .expect("an inactive service calls nothing");
}
