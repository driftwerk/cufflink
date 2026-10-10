use serde_json::{json, Value};
use uuid::Uuid;

pub struct AuditEntry<'a> {
    pub client: &'a deadpool_postgres::Client,
    pub tenant_id: Option<Uuid>,
    pub actor: &'a str,
    pub action: &'a str,
    pub resource_type: &'a str,
    pub resource_id: Option<&'a str>,
    pub details: Option<Value>,
    pub ip_address: Option<&'a str>,
}

pub async fn log_action(entry: AuditEntry<'_>) -> eyre::Result<()> {
    entry.client
        .execute(
            "INSERT INTO audit_log (tenant_id, actor, action, resource_type, resource_id, details, ip_address)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &entry.tenant_id,
                &entry.actor,
                &entry.action,
                &entry.resource_type,
                &entry.resource_id,
                &entry.details,
                &entry.ip_address,
            ],
        )
        .await?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Credential {
    Token,
    Key { id: Uuid, name: String },
}

impl Credential {
    fn to_json(&self) -> Value {
        match self {
            Credential::Token => json!({"method": "jwt"}),
            Credential::Key { id, name } => {
                json!({"method": "api_key", "key_id": id, "key_name": name})
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Change<'a> {
    pub tenant_id: Option<Uuid>,
    pub actor: &'a str,
    pub credential: Credential,
    pub action: &'a str,
    pub resource_type: &'a str,
    pub resource_id: Option<&'a str>,
    pub details: Option<Value>,
}

impl Change<'_> {
    fn details(&self) -> Value {
        let credential = self.credential.to_json();
        match self.details.clone() {
            None => json!({ "credential": credential }),
            Some(Value::Object(mut details)) => {
                details.insert("credential".to_string(), credential);
                Value::Object(details)
            }
            Some(details) => json!({ "details": details, "credential": credential }),
        }
    }
}

pub async fn record(db: &deadpool_postgres::Pool, change: Change<'_>) {
    let recorded = async {
        let client = db.get().await?;
        log_action(AuditEntry {
            client: &client,
            tenant_id: change.tenant_id,
            actor: change.actor,
            action: change.action,
            resource_type: change.resource_type,
            resource_id: change.resource_id,
            details: Some(change.details()),
            ip_address: None,
        })
        .await
    }
    .await;
    if let Err(e) = recorded {
        tracing::warn!(
            action = change.action,
            resource = change.resource_id.unwrap_or_default(),
            error = %e,
            "Could not audit a change to the control plane"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(credential: Credential, details: Option<Value>) -> Change<'static> {
        Change {
            tenant_id: None,
            actor: "owner-1",
            credential,
            action: "webhook.create",
            resource_type: "webhook",
            resource_id: None,
            details,
        }
    }

    #[test]
    fn an_entry_names_the_key_it_was_made_with_and_never_holds_it() {
        let key = Credential::Key {
            id: Uuid::from_u128(9),
            name: "ci".into(),
        };
        assert_eq!(
            change(key.clone(), Some(json!({"host": "hooks.example"}))).details(),
            json!({"host": "hooks.example", "credential": {
                "method": "api_key", "key_id": "00000000-0000-0000-0000-000000000009",
                "key_name": "ci"}})
        );
        assert_eq!(
            change(Credential::Token, None).details(),
            json!({"credential": {"method": "jwt"}})
        );
        assert_eq!(
            change(key, Some(json!("set"))).details()["details"],
            json!("set")
        );
    }
}
