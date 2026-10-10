use crate::auth::AuthUser;
use crate::errors::{PlatformError, PlatformResult};
use crate::registry::RegisteredService;
use crate::AppState;
use axum::{
    extract::{Path, State},
    response::Html,
    Json,
};
use cufflink_types::openapi::service_spec;
use serde_json::{json, Value};
use std::sync::Arc;

fn resolve(service: &RegisteredService, leaf: &str) {
    crate::telemetry::resolve(&service.scope(), &format!("_meta/{leaf}"));
}

pub async fn health_handler(
    State(state): State<Arc<AppState>>,
    Path((tenant_slug, service_name)): Path<(String, String)>,
) -> PlatformResult<Json<Value>> {
    let service = state
        .registry
        .get_by_slug(&tenant_slug, &service_name)
        .await
        .ok_or_else(|| PlatformError::NotFound("Service not found".into()))?;
    resolve(&service, "health");

    let current = crate::repo::deployments::current(&state.platform_db, service.service_id).await?;

    Ok(Json(json!({
        "name": service.name,
        "tenant": service.tenant_slug,
        "version": current.as_ref().map_or(service.version, |d| d.version),
        "commit_hash": current.as_ref().and_then(|d| d.commit_hash.clone()),
        "status": current.as_ref().map_or_else(|| "active".into(), |d| d.status.clone()),
        "table_count": service.manifest.tables.len(),
        "deployed_at": current.as_ref().map(|d| d.deployed_at.to_rfc3339()),
        "now": chrono::Utc::now().to_rfc3339(),
    })))
}

fn require_same_tenant(user: &AuthUser, tenant_slug: &str) -> PlatformResult<()> {
    if user.tenant_slug == tenant_slug {
        Ok(())
    } else {
        Err(PlatformError::NotFound("Service not found".into()))
    }
}

pub async fn openapi_handler(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path((tenant_slug, service_name)): Path<(String, String)>,
) -> PlatformResult<Json<Value>> {
    require_same_tenant(&user, &tenant_slug)?;
    let service = state
        .registry
        .get_by_slug(&tenant_slug, &service_name)
        .await
        .ok_or_else(|| PlatformError::NotFound("Service not found".into()))?;
    resolve(&service, "openapi.json");

    service_spec(&service.manifest, &service.tenant_slug)
        .map(Json)
        .map_err(PlatformError::from)
}

pub async fn docs_handler(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path((tenant_slug, service_name)): Path<(String, String)>,
) -> PlatformResult<Html<String>> {
    require_same_tenant(&user, &tenant_slug)?;
    let service = state
        .registry
        .get_by_slug(&tenant_slug, &service_name)
        .await
        .ok_or_else(|| PlatformError::NotFound("Service not found".into()))?;
    resolve(&service, "docs");

    let spec_url = format!("/svc/{}/{}/_meta/openapi.json", tenant_slug, service_name);
    let title = format!("{} — API docs", service_name);
    Ok(Html(swagger_ui_html(&spec_url, &title)))
}

fn swagger_ui_html(spec_url: &str, title: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{title}</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
  <style>body {{ margin: 0; }}</style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = () => {{
      window.ui = SwaggerUIBundle({{
        url: {spec_url:?},
        dom_id: '#swagger-ui',
        deepLinking: true,
      }});
    }};
  </script>
</body>
</html>"#
    )
}

#[cfg(test)]
mod tests {
    use super::swagger_ui_html;

    #[test]
    fn swagger_ui_html_embeds_spec_url_as_json_literal() {
        let html = swagger_ui_html(
            "/svc/default/audit-service/_meta/openapi.json",
            "audit-service — API docs",
        );
        assert!(html.contains(r#"url: "/svc/default/audit-service/_meta/openapi.json""#));
        assert!(html.contains("<title>audit-service — API docs</title>"));
        assert!(html.contains("swagger-ui-dist@5"));
    }

    #[test]
    fn swagger_ui_html_escapes_quotes_in_spec_url() {
        let html = swagger_ui_html(r#"/svc/foo"injected/bar/_meta/openapi.json"#, "x");
        assert!(html.contains(r#"\"injected"#));
    }
}
