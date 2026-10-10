use crate::errors::PlatformResult;
use chrono::{DateTime, Utc};
use deadpool_postgres::GenericClient;
use std::collections::HashMap;
use uuid::Uuid;

pub type LastFired = HashMap<(Uuid, String), DateTime<Utc>>;

pub async fn all(client: &impl GenericClient) -> PlatformResult<LastFired> {
    Ok(client
        .query(
            "SELECT service_id, name, last_fired_at FROM schedule_state",
            &[],
        )
        .await?
        .iter()
        .map(|row| {
            (
                (row.get("service_id"), row.get("name")),
                row.get("last_fired_at"),
            )
        })
        .collect())
}

pub async fn introduce(
    client: &impl GenericClient,
    service_id: Uuid,
    names: &[String],
    at: DateTime<Utc>,
) -> PlatformResult<()> {
    client
        .execute(
            "INSERT INTO schedule_state (service_id, name, last_fired_at)
             SELECT $1, name, $3 FROM unnest($2::text[]) AS name
             ON CONFLICT (service_id, name) DO NOTHING",
            &[&service_id, &names, &at],
        )
        .await?;
    Ok(())
}

pub async fn fired(
    client: &impl GenericClient,
    service_id: Uuid,
    name: &str,
    at: DateTime<Utc>,
) -> PlatformResult<()> {
    client
        .execute(
            "INSERT INTO schedule_state (service_id, name, last_fired_at)
             VALUES ($1, $2, $3)
             ON CONFLICT (service_id, name) DO UPDATE
             SET last_fired_at = GREATEST(schedule_state.last_fired_at, EXCLUDED.last_fired_at),
                 updated_at = now()",
            &[&service_id, &name, &at],
        )
        .await?;
    Ok(())
}

pub async fn keep_only(
    client: &impl GenericClient,
    service_id: Uuid,
    names: &[String],
) -> PlatformResult<u64> {
    Ok(client
        .execute(
            "DELETE FROM schedule_state WHERE service_id = $1 AND name <> ALL($2::text[])",
            &[&service_id, &names],
        )
        .await?)
}
