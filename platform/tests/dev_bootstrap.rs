use cufflink_testkit as common;

use common::PlatformProcess;
use reqwest::Method;
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

const REALM: &str = "cufflink-dev";

fn setting(tenant: &str, key_file: &Path) -> String {
    format!(
        "tenant={tenant},realm={REALM},key_file={}",
        key_file.display()
    )
}

async fn started(database_url: &str, setting: &str) -> PlatformProcess {
    let platform = PlatformProcess::start(
        database_url,
        None,
        &[
            ("RUN_ENV", "development"),
            ("CUFFLINK_DEV_BOOTSTRAP", setting),
        ],
    )
    .await;
    platform.wait_ready().await;
    platform
}

fn key_in(key_file: &Path) -> String {
    std::fs::read_to_string(key_file)
        .expect("the key file")
        .trim()
        .to_string()
}

async fn services(platform: &PlatformProcess, key: &str) -> u16 {
    platform
        .send(Method::GET, "/api/services", &format!("ApiKey {key}"), None)
        .await
        .0
}

#[tokio::test]
async fn a_development_platform_makes_its_tenant_and_a_key_once() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let tenant = format!("dev-{}", &Uuid::new_v4().simple().to_string()[..8]);
    db.adopt_tenant_db(&tenant);
    let directory = tempfile::tempdir().expect("directory");
    let key_file = directory.path().join("run/dev-key");
    let setting = setting(&tenant, &key_file);

    let first = started(&db.url, &setting).await;
    let key = key_in(&key_file);
    assert!(key.starts_with("ck_"), "{key}");
    assert_eq!(services(&first, &key).await, 200);
    let made = db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT keycloak_realm, database_name, \
                    (SELECT count(*) FROM api_keys WHERE tenant_id = tenants.id) AS keys \
             FROM tenants WHERE slug = $1",
            &[&tenant],
        )
        .await
        .expect("the tenant");
    assert_eq!(
        (
            made.get::<_, Option<String>>("keycloak_realm").as_deref(),
            made.get::<_, Option<String>>("database_name"),
            made.get::<_, i64>("keys"),
        ),
        (Some(REALM), None, 1)
    );
    assert!(
        db.database_exists(
            &cufflink_platform::tenant_db::TenantPoolManager::managed_db_name(&tenant)
        )
        .await
    );
    drop(first);

    let second = started(&db.url, &setting).await;
    assert_eq!(key_in(&key_file), key, "a key that works is kept");
    assert_eq!(services(&second, &key).await, 200);
    drop(second);

    std::fs::remove_file(&key_file).expect("remove the key file");
    let third = started(&db.url, &setting).await;
    let replaced = key_in(&key_file);
    assert_ne!(replaced, key);
    assert_eq!(services(&third, &replaced).await, 200);
    drop(third);

    let other_realm = format!(
        "tenant={tenant},realm=another,key_file={}",
        key_file.display()
    );
    let mut refused = PlatformProcess::launch(
        &db.url,
        None,
        &[
            ("RUN_ENV", "development"),
            ("CUFFLINK_DEV_BOOTSTRAP", other_realm.as_str()),
        ],
    );
    let status = refused.wait_exit(Duration::from_secs(60)).await;
    let output = refused.output_tail();
    assert!(status.is_some_and(|status| !status.success()), "{output}");
    assert!(
        output.contains(&format!("tenant '{tenant}' exists with realm")),
        "{output}"
    );
}

async fn native_set(platform: &PlatformProcess, runner_key: &str) -> (u16, serde_json::Value) {
    platform
        .send(
            Method::GET,
            "/api/runner/native",
            &format!("RunnerKey {runner_key}"),
            None,
        )
        .await
}

#[tokio::test]
async fn a_development_platform_writes_its_tenants_runner_key_once() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let tenant = format!("dev-{}", &Uuid::new_v4().simple().to_string()[..8]);
    db.adopt_tenant_db(&tenant);
    let directory = tempfile::tempdir().expect("directory");
    let runner_key_file = directory.path().join("run/runner-key");
    let setting = format!(
        "{},runner_key_file={}",
        setting(&tenant, &directory.path().join("run/dev-key")),
        runner_key_file.display()
    );

    let first = started(&db.url, &setting).await;
    let runner_key = key_in(&runner_key_file);
    assert!(runner_key.starts_with("cfr_"), "{runner_key}");
    let (status, set) = native_set(&first, &runner_key).await;
    assert_eq!((status, &set["tenant"]), (200, &serde_json::json!(tenant)));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&runner_key_file)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "for its owner alone");
    }
    drop(first);

    let second = started(&db.url, &setting).await;
    assert_eq!(
        key_in(&runner_key_file),
        runner_key,
        "a key in force is kept"
    );
    drop(second);

    std::fs::write(&runner_key_file, "cfr_AAAAAAAAAAAA_notakey").expect("a stale key");
    let third = started(&db.url, &setting).await;
    let replaced = key_in(&runner_key_file);
    assert!(
        replaced.starts_with("cfr_") && replaced != runner_key,
        "{replaced}"
    );
    assert_eq!(native_set(&third, &replaced).await.0, 200);
    let keys: i64 = db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT count(*) FROM runner_keys k JOIN tenants t ON t.id = k.tenant_id
             WHERE t.slug = $1",
            &[&tenant],
        )
        .await
        .expect("the runner keys")
        .get(0);
    assert_eq!(keys, 2, "one minted at first, one for the stale file");
}

#[tokio::test]
async fn a_platform_not_declared_a_development_one_refuses_to_start_with_a_bootstrap() {
    for run_env in [None, Some("staging"), Some("production")] {
        let directory = tempfile::tempdir().expect("directory");
        let key_file = directory.path().join("dev-key");
        let bootstrap = setting("dev", &key_file);
        let env: Vec<(&str, &str)> = run_env
            .map(|run_env| ("RUN_ENV", run_env))
            .into_iter()
            .chain([("CUFFLINK_DEV_BOOTSTRAP", bootstrap.as_str())])
            .collect();
        let mut platform = PlatformProcess::launch(
            "postgres://cufflink:secret@127.0.0.1:1/cufflink",
            None,
            &env,
        );
        let status = platform.wait_exit(Duration::from_secs(30)).await;
        let output = platform.output_tail();
        assert!(
            status.is_some_and(|status| !status.success()),
            "{run_env:?}: {output}"
        );
        assert!(
            output.contains(&format!(
                "CUFFLINK_DEV_BOOTSTRAP is refused unless RUN_ENV=development (it is {})",
                run_env.unwrap_or("unset")
            )),
            "{output}"
        );
        assert!(!key_file.exists());
    }
}
