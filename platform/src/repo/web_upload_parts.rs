use crate::errors::PlatformResult;
use crate::repo::deployments::Artifact;
use deadpool_postgres::GenericClient;
use std::time::Duration;
use uuid::Uuid;

fn artifact(row: &tokio_postgres::Row) -> Artifact {
    Artifact {
        s3_key: row.get(0),
        hash: row.get(1),
        size: row.get(2),
    }
}

pub async fn record(
    client: &impl GenericClient,
    service_id: Uuid,
    part: &Artifact,
) -> PlatformResult<()> {
    client
        .execute(
            "INSERT INTO web_upload_parts (service_id, sha256, s3_key, size)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (service_id, sha256) DO UPDATE SET
                 s3_key = EXCLUDED.s3_key,
                 size = EXCLUDED.size,
                 uploaded_at = now()",
            &[&service_id, &part.hash, &part.s3_key, &part.size],
        )
        .await?;
    Ok(())
}

pub async fn find(
    client: &impl GenericClient,
    service_id: Uuid,
    sha256s: &[String],
) -> PlatformResult<Vec<Artifact>> {
    Ok(client
        .query(
            "SELECT s3_key, sha256, size FROM web_upload_parts
             WHERE service_id = $1 AND sha256 = ANY($2)",
            &[&service_id, &sha256s],
        )
        .await?
        .iter()
        .map(artifact)
        .collect())
}

pub async fn forget(
    client: &impl GenericClient,
    service_id: Uuid,
    sha256s: &[String],
) -> PlatformResult<Vec<String>> {
    Ok(client
        .query(
            "DELETE FROM web_upload_parts WHERE service_id = $1 AND sha256 = ANY($2)
             RETURNING s3_key",
            &[&service_id, &sha256s],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect())
}

pub async fn expired(
    client: &impl GenericClient,
    service_id: Uuid,
    retention: Duration,
) -> PlatformResult<Vec<String>> {
    Ok(client
        .query(
            "DELETE FROM web_upload_parts
             WHERE service_id = $1 AND uploaded_at < now() - make_interval(secs => $2::bigint)
             RETURNING s3_key",
            &[&service_id, &super::web_uploads::seconds(retention)],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect())
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

    fn part(service_id: Uuid, byte: u8, size: i64) -> Artifact {
        let hash = format!("{byte:02x}").repeat(32);
        Artifact {
            s3_key: format!("web/{service_id}/parts/{hash}"),
            hash,
            size,
        }
    }

    async fn age(pool: &deadpool_postgres::Pool, service_id: Uuid, sha256: &str, hours: i32) {
        pool.get()
            .await
            .expect("checkout")
            .execute(
                "UPDATE web_upload_parts SET uploaded_at = now() - make_interval(hours => $3)
                 WHERE service_id = $1 AND sha256 = $2",
                &[&service_id, &sha256, &hours],
            )
            .await
            .expect("aged");
    }

    fn hashes(parts: &[&Artifact]) -> Vec<String> {
        parts.iter().map(|part| part.hash.clone()).collect()
    }

    fn sorted(mut parts: Vec<Artifact>) -> Vec<Artifact> {
        parts.sort_by(|a, b| a.hash.cmp(&b.hash));
        parts
    }

    #[tokio::test]
    async fn a_part_is_found_by_its_service_and_forgotten_once() {
        let Some((_db, pool, site, shop)) = migrated().await else {
            return;
        };
        let client = pool.get().await.expect("checkout");
        let (first, second) = (part(site, 0x1a, 8), part(site, 0x2b, 3));
        record(&client, site, &first).await.expect("recorded");
        record(&client, site, &second).await.expect("recorded");
        record(&client, site, &second)
            .await
            .expect("recorded again");

        let wanted = hashes(&[&first, &second, &part(site, 0x3c, 1)]);
        assert_eq!(
            sorted(find(&client, site, &wanted).await.expect("read")),
            vec![first.clone(), second.clone()]
        );
        assert_eq!(
            find(&client, shop, &wanted).await.expect("read"),
            vec![],
            "a part belongs to its service"
        );

        assert_eq!(
            forget(&client, site, &hashes(&[&first]))
                .await
                .expect("forgotten"),
            vec![first.s3_key.clone()]
        );
        assert_eq!(
            forget(&client, site, &hashes(&[&first]))
                .await
                .expect("forgotten again"),
            Vec::<String>::new()
        );
        assert_eq!(
            find(&client, site, &wanted).await.expect("read"),
            vec![second]
        );
    }

    #[tokio::test]
    async fn expired_removes_only_the_services_parts_past_the_retention() {
        let Some((_db, pool, site, shop)) = migrated().await else {
            return;
        };
        let client = pool.get().await.expect("checkout");
        let (old, recent, others) = (
            part(site, 0x1a, 8),
            part(site, 0x2b, 8),
            part(shop, 0x1a, 8),
        );
        for (service_id, part) in [(site, &old), (site, &recent), (shop, &others)] {
            record(&client, service_id, part).await.expect("recorded");
        }
        age(&pool, site, &old.hash, 30).await;
        age(&pool, shop, &others.hash, 30).await;

        assert_eq!(
            expired(&client, site, Duration::from_secs(86_400))
                .await
                .expect("swept"),
            vec![old.s3_key.clone()]
        );
        assert_eq!(
            find(&client, site, &hashes(&[&old, &recent]))
                .await
                .expect("read"),
            vec![recent]
        );
        assert_eq!(
            find(&client, shop, &hashes(&[&others]))
                .await
                .expect("read"),
            vec![others],
            "another service's parts are its own sweep's"
        );
    }
}
