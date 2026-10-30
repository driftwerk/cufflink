use crate::registry::RegisteredService;
use crate::AppState;
use axum::{extract::State, response::IntoResponse};
use std::collections::BTreeSet;
use std::sync::Arc;

pub async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let services = state.registry.all_services().await;
    metrics::gauge!("cufflink_services_total").set(services.len() as f64);
    if let Some(engine) = &state.wasm_engine {
        metrics::gauge!("cufflink_wasm_module_cache_entries").set(engine.cache_len() as f64);
        engine.publish_gauges();
    }
    metrics::gauge!("cufflink_consumer_loops").set(state.subscription_tasks.loop_count() as f64);
    crate::telemetry::record_pool_gauges(&state);
    format!(
        "{}{}",
        state.metrics_handle.render(),
        service_info(&services)
    )
}

fn label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn service_info(services: &[Arc<RegisteredService>]) -> String {
    let series: BTreeSet<String> = services
        .iter()
        .map(|service| {
            format!(
                "cufflink_service_info{{tenant=\"{}\",service=\"{}\",version=\"{}\",commit=\"{}\",mode=\"{}\"}} 1\n",
                label_value(&service.tenant_slug),
                label_value(&service.name),
                service.scope().version_label().unwrap_or_default(),
                label_value(service.commit_hash.as_deref().unwrap_or_default()),
                service.manifest.mode.as_str(),
            )
        })
        .collect();
    match series.is_empty() {
        true => String::new(),
        false => std::iter::once("# TYPE cufflink_service_info gauge\n".to_string())
            .chain(series)
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cufflink_types::{ServiceManifest, ServiceMode};
    use uuid::Uuid;

    fn service(
        tenant: &str,
        name: &str,
        version: i32,
        commit: Option<&str>,
        mode: ServiceMode,
    ) -> Arc<RegisteredService> {
        Arc::new(RegisteredService {
            service_id: Uuid::new_v4(),
            tenant_id: Uuid::new_v4(),
            tenant_slug: tenant.into(),
            name: name.into(),
            manifest: ServiceManifest {
                name: name.into(),
                mode,
                ..Default::default()
            },
            version,
            wasm_hash: None,
            commit_hash: commit.map(str::to_string),
        })
    }

    #[test]
    fn an_empty_registry_prints_nothing() {
        assert_eq!(service_info(&[]), "");
    }

    #[test]
    fn each_service_is_one_sorted_line_with_its_version_commit_and_mode() {
        let info = service_info(&[
            service("globex", "notes", 2, None, ServiceMode::Crud),
            service("acme", "portal", 12, Some("9f3c2ab"), ServiceMode::Web),
            service("acme", "notes", 7, Some("0a1b2c3"), ServiceMode::Wasm),
        ]);
        assert_eq!(
            info,
            "# TYPE cufflink_service_info gauge\n\
             cufflink_service_info{tenant=\"acme\",service=\"notes\",version=\"v7\",commit=\"0a1b2c3\",mode=\"wasm\"} 1\n\
             cufflink_service_info{tenant=\"acme\",service=\"portal\",version=\"v12\",commit=\"9f3c2ab\",mode=\"web\"} 1\n\
             cufflink_service_info{tenant=\"globex\",service=\"notes\",version=\"v2\",commit=\"\",mode=\"crud\"} 1\n"
        );
    }

    #[test]
    fn a_label_value_is_escaped_as_the_exposition_format_asks() {
        let info = service_info(&[service(
            "acme",
            "notes",
            1,
            Some("a\"b\\c\nd"),
            ServiceMode::Wasm,
        )]);
        assert_eq!(
            info.lines().nth(1),
            Some(
                r#"cufflink_service_info{tenant="acme",service="notes",version="v1",commit="a\"b\\c\nd",mode="wasm"} 1"#
            )
        );
    }
}
