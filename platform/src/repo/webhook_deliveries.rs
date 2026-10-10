use chrono::{DateTime, Utc};
use deadpool_postgres::GenericClient;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    Pending,
    Delivered,
    Failed,
    Dropped,
}

impl DeliveryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            DeliveryStatus::Pending => "pending",
            DeliveryStatus::Delivered => "delivered",
            DeliveryStatus::Failed => "failed",
            DeliveryStatus::Dropped => "dropped",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attempt<'a> {
    pub id: Uuid,
    pub webhook_id: Uuid,
    pub tenant_id: Uuid,
    pub event: &'a str,
    pub status: DeliveryStatus,
    pub attempts: i32,
    pub last_status: Option<i32>,
    pub last_error: Option<&'a str>,
}

pub async fn record(
    client: &impl GenericClient,
    attempt: &Attempt<'_>,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "INSERT INTO webhook_deliveries
                 (id, webhook_id, tenant_id, event, status, attempts, last_status, last_error,
                  delivered_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CASE WHEN $5 = 'delivered' THEN now() END)
             ON CONFLICT (id) DO UPDATE SET
                 status = EXCLUDED.status,
                 attempts = GREATEST(webhook_deliveries.attempts, EXCLUDED.attempts),
                 last_status = EXCLUDED.last_status,
                 last_error = EXCLUDED.last_error,
                 delivered_at = COALESCE(webhook_deliveries.delivered_at, EXCLUDED.delivered_at),
                 updated_at = now()
             WHERE webhook_deliveries.webhook_id = EXCLUDED.webhook_id
               AND webhook_deliveries.status <> 'delivered'",
            &[
                &attempt.id,
                &attempt.webhook_id,
                &attempt.tenant_id,
                &attempt.event,
                &attempt.status.as_str(),
                &attempt.attempts,
                &attempt.last_status,
                &attempt.last_error,
            ],
        )
        .await
        .map(drop)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recorded {
    Delivered,
    Attempted(i32),
}

pub async fn recorded(
    client: &impl GenericClient,
    id: Uuid,
) -> Result<Recorded, tokio_postgres::Error> {
    let row = client
        .query_opt(
            "SELECT status, attempts FROM webhook_deliveries WHERE id = $1",
            &[&id],
        )
        .await?;
    Ok(row.map_or(Recorded::Attempted(0), |row| {
        match row.get::<_, &str>("status") == DeliveryStatus::Delivered.as_str() {
            true => Recorded::Delivered,
            false => Recorded::Attempted(row.get("attempts")),
        }
    }))
}

pub async fn prune(
    client: &impl GenericClient,
    before: DateTime<Utc>,
    batch: i64,
) -> Result<u64, tokio_postgres::Error> {
    client
        .execute(
            "DELETE FROM webhook_deliveries WHERE id IN (
                 SELECT id FROM webhook_deliveries WHERE updated_at < $1
                 LIMIT $2 FOR UPDATE SKIP LOCKED)",
            &[&before, &batch],
        )
        .await
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Delivery {
    pub id: Uuid,
    pub event: String,
    pub status: String,
    pub attempts: i32,
    pub last_status: Option<i32>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub delivered_at: Option<DateTime<Utc>>,
}

pub async fn list(
    client: &impl GenericClient,
    webhook_id: Uuid,
    limit: i64,
) -> Result<Vec<Delivery>, tokio_postgres::Error> {
    let rows = client
        .query(
            "SELECT id, event, status, attempts, last_status, last_error, created_at,
                    updated_at, delivered_at
             FROM webhook_deliveries WHERE webhook_id = $1
             ORDER BY created_at DESC, id LIMIT $2",
            &[&webhook_id, &limit],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| Delivery {
            id: row.get("id"),
            event: row.get("event"),
            status: row.get("status"),
            attempts: row.get("attempts"),
            last_status: row.get("last_status"),
            last_error: row.get("last_error"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            delivered_at: row.get("delivered_at"),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_status_is_the_text_the_table_checks() {
        let texts: Vec<&str> = [
            DeliveryStatus::Pending,
            DeliveryStatus::Delivered,
            DeliveryStatus::Failed,
            DeliveryStatus::Dropped,
        ]
        .into_iter()
        .map(DeliveryStatus::as_str)
        .collect();
        assert_eq!(texts, ["pending", "delivered", "failed", "dropped"]);
    }
}
