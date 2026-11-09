use crate::errors::PlatformResult;
use crate::AppState;
use tracing::warn;
use uuid::Uuid;

async fn held(state: &AppState, key: &str) -> PlatformResult<bool> {
    let client = state.platform_db.get().await?;
    Ok(
        crate::repo::deployments::records_artifact(&client, key).await?
            || crate::repo::web_uploads::holds(&client, key, state.config.web_upload_retention)
                .await?,
    )
}

pub(crate) async fn remove_orphan(state: &AppState, key: &str, context: &str) {
    let Some(storage) = state.storage.as_ref() else {
        return;
    };
    match held(state, key).await {
        Ok(true) => {}
        Ok(false) => {
            warn!(key, context, "Removing an artifact no deployment records");
            if let Err(e) = storage.delete(key).await {
                warn!(key, context, error = %e, "Could not remove the artifact");
            }
        }
        Err(e) => warn!(
            key,
            context,
            error = %e,
            "Could not tell whether a deployment or a recent upload holds the artifact; leaving it"
        ),
    }
}

pub(crate) async fn delete_objects(state: &AppState, service_id: Uuid) {
    let Some(storage) = state.storage.as_ref() else {
        return;
    };
    for prefix in [
        super::native::object_prefix(service_id),
        super::web::object_prefix(service_id),
    ] {
        if let Err(e) = storage.delete_prefix(&prefix).await {
            warn!(prefix = %prefix, error = %e, "Failed to delete a service's artifacts");
        }
    }
}
