use crate::errors::PlatformResult;
use crate::repo::deployments::Artifact;
use deadpool_postgres::GenericClient;
use std::time::Duration;
use uuid::Uuid;

fn seconds(retention: Duration) -> i64 {
    i64::try_from(retention.as_secs()).unwrap_or(i64::MAX)
}

pub async fn record(
    client: &impl GenericClient,
    service_id: Uuid,
    upload: &Artifact,
    uploaded_by: &str,
) -> PlatformResult<()> {
    client
        .execute(
            "INSERT INTO web_uploads (service_id, sha256, s3_key, size, uploaded_by)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (service_id, sha256) DO UPDATE SET
                 s3_key = EXCLUDED.s3_key,
                 size = EXCLUDED.size,
                 uploaded_by = EXCLUDED.uploaded_by,
                 uploaded_at = now()",
            &[
                &service_id,
                &upload.hash,
                &upload.s3_key,
                &upload.size,
                &uploaded_by,
            ],
        )
        .await?;
    Ok(())
}

pub async fn claim(
    client: &impl GenericClient,
    service_id: Uuid,
    sha256: &str,
) -> PlatformResult<Option<Artifact>> {
    Ok(client
        .query_opt(
            "SELECT s3_key, sha256, size FROM web_uploads WHERE service_id = $1 AND sha256 = $2",
            &[&service_id, &sha256],
        )
        .await?
        .map(|row| Artifact {
            s3_key: row.get(0),
            hash: row.get(1),
            size: row.get(2),
        }))
}

pub async fn expired(
    client: &impl GenericClient,
    service_id: Uuid,
    retention: Duration,
) -> PlatformResult<Vec<String>> {
    Ok(client
        .query(
            "DELETE FROM web_uploads u
             WHERE u.service_id = $1
               AND u.uploaded_at < now() - make_interval(secs => $2::bigint)
               AND NOT EXISTS (SELECT 1 FROM deployments d WHERE d.artifact_s3_key = u.s3_key)
             RETURNING u.s3_key",
            &[&service_id, &seconds(retention)],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect())
}

pub async fn holds(
    client: &impl GenericClient,
    key: &str,
    retention: Duration,
) -> PlatformResult<bool> {
    Ok(client
        .query_one(
            "SELECT EXISTS (
                 SELECT 1 FROM web_uploads
                 WHERE s3_key = $1 AND uploaded_at >= now() - make_interval(secs => $2::bigint)
             )",
            &[&key, &seconds(retention)],
        )
        .await?
        .get(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn migrated() -> Option<(
        cufflink_testkit::TestDb,
        deadpool_postgres::Pool,
        Uuid,
        Uuid,
    )> {
        let infra = cufflink_testkit::infra().await?;
        let mut db = infra.create_migrated_database().await;
        let tenant = db.mint_tenant().await;
        let pool = cufflink_db::build_pool(&db.url, 2).expect("pool");
        let service = |name: &'static str| {
            let pool = pool.clone();
            async move {
                crate::repo::services::find_or_create(
                    &pool,
                    tenant.id,
                    name,
                    name,
                    crate::repo::services::Status::Active,
                )
                .await
                .expect("service")
                .id
            }
        };
        let (site, shop) = (service("site").await, service("shop").await);
        Some((db, pool, site, shop))
    }

    fn bundle(service_id: Uuid, byte: u8, size: i64) -> Artifact {
        let hash = format!("{byte:02x}").repeat(32);
        Artifact {
            s3_key: format!("web/{service_id}/{hash}.tar.gz"),
            hash,
            size,
        }
    }

    async fn age(pool: &deadpool_postgres::Pool, service_id: Uuid, sha256: &str, hours: i32) {
        pool.get()
            .await
            .expect("checkout")
            .execute(
                "UPDATE web_uploads SET uploaded_at = now() - make_interval(hours => $3)
                 WHERE service_id = $1 AND sha256 = $2",
                &[&service_id, &sha256, &hours],
            )
            .await
            .expect("aged");
    }

    #[tokio::test]
    async fn an_upload_is_recorded_once_and_refreshed_by_the_next() {
        let Some((_db, pool, site, shop)) = migrated().await else {
            return;
        };
        let client = pool.get().await.expect("checkout");
        let upload = bundle(site, 0x3f, 41);
        assert_eq!(
            claim(&client, site, &upload.hash).await.expect("read"),
            None
        );

        record(&client, site, &upload, "ci")
            .await
            .expect("recorded");
        age(&pool, site, &upload.hash, 30).await;
        record(&client, site, &upload, "ci")
            .await
            .expect("recorded again");

        assert_eq!(
            claim(&client, site, &upload.hash).await.expect("read"),
            Some(upload.clone())
        );
        assert_eq!(
            claim(&client, shop, &upload.hash).await.expect("read"),
            None,
            "an upload belongs to its service"
        );
        let (rows, refreshed): (i64, bool) = client
            .query_one(
                "SELECT count(*), bool_and(uploaded_at > now() - interval '1 hour')
                 FROM web_uploads WHERE service_id = $1",
                &[&site],
            )
            .await
            .map(|row| (row.get(0), row.get(1)))
            .expect("rows");
        assert_eq!((rows, refreshed), (1, true));
        assert!(holds(&client, &upload.s3_key, Duration::from_secs(3600))
            .await
            .expect("read"));
    }

    #[tokio::test]
    async fn expired_returns_only_unrecorded_uploads_past_the_retention() {
        let Some((_db, pool, site, shop)) = migrated().await else {
            return;
        };
        let client = pool.get().await.expect("checkout");
        let retention = Duration::from_secs(24 * 3600);
        let (old, fresh, released, elsewhere) = (
            bundle(site, 0x01, 10),
            bundle(site, 0x02, 20),
            bundle(site, 0x03, 30),
            bundle(shop, 0x04, 40),
        );
        for (service_id, upload) in [
            (site, &old),
            (site, &fresh),
            (site, &released),
            (shop, &elsewhere),
        ] {
            record(&client, service_id, upload, "ci")
                .await
                .expect("recorded");
        }
        for (service_id, upload) in [(site, &old), (site, &released), (shop, &elsewhere)] {
            age(&pool, service_id, &upload.hash, 25).await;
        }
        client
            .execute(
                "INSERT INTO deployments (service_id, version, manifest_hash, manifest,
                                          artifact_s3_key, artifact_hash, artifact_size)
                 VALUES ($1, 1, 'm', '{\"name\": \"site\", \"mode\": \"web\"}', $2, $3, $4)",
                &[&site, &released.s3_key, &released.hash, &released.size],
            )
            .await
            .expect("a release");

        assert_eq!(
            expired(&client, site, retention).await.expect("swept"),
            vec![old.s3_key.clone()]
        );
        assert_eq!(claim(&client, site, &old.hash).await.expect("read"), None);
        assert!(!holds(&client, &old.s3_key, retention).await.expect("read"));
        for (service_id, upload) in [(site, &fresh), (site, &released), (shop, &elsewhere)] {
            assert_eq!(
                claim(&client, service_id, &upload.hash)
                    .await
                    .expect("read"),
                Some(upload.clone())
            );
        }
        assert!(!holds(&client, &released.s3_key, retention)
            .await
            .expect("read"));
        assert!(expired(&client, site, retention)
            .await
            .expect("swept")
            .is_empty());
    }
}
