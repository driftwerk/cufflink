use crate::errors::{PlatformError, PlatformResult};
use crate::nats_manager::subjects::{validate_claims, Claim, ClaimError, ClaimKind};
use deadpool_postgres::GenericClient;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRow {
    pub tenant_id: Uuid,
    pub tenant_slug: String,
    pub claim: Claim,
}

const SELECT: &str = "SELECT c.tenant_id, t.slug, c.kind, c.pattern
                      FROM tenant_nats_claims c JOIN tenants t ON t.id = c.tenant_id";

fn row_to_claim(row: &tokio_postgres::Row) -> PlatformResult<ClaimRow> {
    let kind: String = row.get("kind");
    Ok(ClaimRow {
        tenant_id: row.get("tenant_id"),
        tenant_slug: row.get("slug"),
        claim: Claim {
            kind: kind.parse().map_err(PlatformError::Internal)?,
            pattern: row.get("pattern"),
        },
    })
}

pub async fn list_all(client: &impl GenericClient) -> PlatformResult<Vec<ClaimRow>> {
    client
        .query(&format!("{SELECT} ORDER BY t.slug, c.kind, c.pattern"), &[])
        .await?
        .iter()
        .map(row_to_claim)
        .collect()
}

pub async fn list_for_tenant(
    client: &impl GenericClient,
    tenant_id: Uuid,
) -> PlatformResult<Vec<Claim>> {
    client
        .query(
            &format!("{SELECT} WHERE c.tenant_id = $1 ORDER BY c.kind, c.pattern"),
            &[&tenant_id],
        )
        .await?
        .iter()
        .map(|row| row_to_claim(row).map(|row| row.claim))
        .collect()
}

fn refused(error: ClaimError) -> PlatformError {
    if error.is_conflict() {
        PlatformError::Conflict(error.to_string())
    } else {
        PlatformError::Validation(error.to_string())
    }
}

pub async fn replace(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    claims: &[Claim],
    actor: &str,
) -> PlatformResult<Vec<Claim>> {
    let mut client = pool.get().await?;
    let tx = client.transaction().await?;
    tx.batch_execute("LOCK TABLE tenant_nats_claims IN SHARE ROW EXCLUSIVE MODE")
        .await?;
    let others: Vec<(String, Claim)> = list_all(&tx)
        .await?
        .into_iter()
        .filter(|row| row.tenant_id != tenant_id)
        .map(|row| (row.tenant_slug, row.claim))
        .collect();
    validate_claims(claims, &others).map_err(refused)?;
    tx.execute(
        "DELETE FROM tenant_nats_claims WHERE tenant_id = $1",
        &[&tenant_id],
    )
    .await?;
    let (kinds, patterns): (Vec<&str>, Vec<&str>) = claims
        .iter()
        .map(|claim| (claim.kind.as_str(), claim.pattern.as_str()))
        .unzip();
    tx.execute(
        "INSERT INTO tenant_nats_claims (tenant_id, kind, pattern, created_by)
         SELECT $1, kind, pattern, $4 FROM unnest($2::text[], $3::text[]) AS c(kind, pattern)",
        &[&tenant_id, &kinds, &patterns, &actor],
    )
    .await?;
    let stored = list_for_tenant(&tx, tenant_id).await?;
    tx.commit().await?;
    Ok(stored)
}

pub fn patterns(claims: &[Claim], kind: ClaimKind) -> Vec<&str> {
    claims
        .iter()
        .filter(|claim| claim.kind == kind)
        .map(|claim| claim.pattern.as_str())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_claim_another_tenant_holds_is_a_conflict_and_a_malformed_one_invalid() {
        let taken = refused(ClaimError::Taken {
            kind: ClaimKind::Subject,
            pattern: "books.*".into(),
            tenant: "harbor".into(),
            theirs: "books.>".into(),
        });
        assert!(
            matches!(&taken, PlatformError::Conflict(m)
                if m == "subjects: 'books.*' overlaps 'books.>', which tenant 'harbor' claims"),
            "{taken}"
        );
        assert!(matches!(
            refused(ClaimError::InvalidStream("books".into())),
            PlatformError::Validation(_)
        ));
    }

    #[test]
    fn patterns_are_listed_by_kind() {
        let claims = [
            Claim::subject("books.>"),
            Claim::stream("BOOKS"),
            Claim::subject("audit.logs"),
        ];
        assert_eq!(
            patterns(&claims, ClaimKind::Subject),
            ["books.>", "audit.logs"]
        );
        assert_eq!(patterns(&claims, ClaimKind::Stream), ["BOOKS"]);
    }
}
