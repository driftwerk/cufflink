use cufflink_testkit as common;

use cufflink_db::{PoolSettings, Recycling};
use std::time::Duration;

#[tokio::test]
async fn clean_recycle_keeps_statement_timeout() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;

    let settings = PoolSettings::from_vars(1, |_| None)
        .expect("defaults")
        .statement_timeout(Some(Duration::from_secs(30)));
    assert_eq!(settings.recycling, Recycling::Clean);
    let config = cufflink_db::parse_config(&db.url).expect("url");
    let pool = cufflink_db::build_pool_with(config, settings).expect("pool");

    let key: i64 = 424_242;
    let objid = key as u32;
    {
        let client = pool.get().await.expect("first checkout");
        client
            .batch_execute("SET statement_timeout = '1ms'; SET search_path TO nowhere")
            .await
            .expect("session changes");
        client
            .execute("SELECT pg_advisory_lock($1)", &[&key])
            .await
            .expect("advisory lock");
    }

    let client = pool.get().await.expect("second checkout");
    let timeout: String = client
        .query_one("SHOW statement_timeout", &[])
        .await
        .expect("show")
        .get(0);
    assert_eq!(timeout, "30s");
    let search_path: String = client
        .query_one("SHOW search_path", &[])
        .await
        .expect("show")
        .get(0);
    assert!(
        !search_path.contains("nowhere"),
        "search_path leaked: {search_path}"
    );
    let held: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = $1",
            &[&objid],
        )
        .await
        .expect("pg_locks")
        .get(0);
    assert_eq!(held, 0, "advisory lock survived the recycle");
    assert_eq!(
        pool.status().size,
        1,
        "the pool reused its single connection"
    );
    drop(client);
}

#[tokio::test]
async fn connect_single_applies_the_settings_outside_a_pool() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let settings = PoolSettings::from_vars(1, |_| None).expect("defaults");
    let config = cufflink_db::parse_config(&db.url).expect("url");

    let client = cufflink_db::connect_single(config.clone(), &settings)
        .await
        .expect("connect");
    let timeout: String = client
        .query_one("SHOW statement_timeout", &[])
        .await
        .expect("show")
        .get(0);
    assert_eq!(timeout, "30s");

    let unbounded = cufflink_db::connect_single(config, &settings.statement_timeout(None))
        .await
        .expect("connect without a statement timeout");
    let timeout: String = unbounded
        .query_one("SHOW statement_timeout", &[])
        .await
        .expect("show")
        .get(0);
    assert_eq!(timeout, "0");
}
