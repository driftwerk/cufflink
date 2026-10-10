use cufflink_telemetry::scope::TENANT;
use cufflink_telemetry::{resource, Component, COMPONENT};
use opentelemetry::{Key, Value};

#[test]
fn the_resource_names_its_component_and_merges_the_environment() {
    std::env::set_var(
        "OTEL_RESOURCE_ATTRIBUTES",
        "deployment.environment.name=dev,cufflink.component=spoofed,service.name=spoofed",
    );
    std::env::set_var("OTEL_SERVICE_NAME", "spoofed");
    let attribute = |resource: &opentelemetry_sdk::Resource, name: &'static str| {
        resource
            .get(&Key::from_static_str(name))
            .as_ref()
            .map(Value::as_str)
            .map(std::borrow::Cow::into_owned)
    };

    [
        (Component::Platform, "cufflink-platform", "platform"),
        (Component::Worker, "cufflink-worker", "worker"),
        (Component::WebRunner, "cufflink-web-runner", "web-runner"),
        (Component::ExtRunner, "cufflink-ext-runner", "ext-runner"),
    ]
    .into_iter()
    .for_each(|(component, service_name, name)| {
        let resource = resource(component, &[]);
        assert_eq!(component.service_name(), service_name);
        assert_eq!(
            attribute(&resource, "service.name").as_deref(),
            Some(service_name)
        );
        assert_eq!(attribute(&resource, COMPONENT).as_deref(), Some(name));
        assert_eq!(
            attribute(&resource, "deployment.environment.name").as_deref(),
            Some("dev")
        );
        assert_eq!(attribute(&resource, TENANT), None);
    });

    let tenant_runner = resource(Component::ExtRunner, &[(TENANT, "acme".to_owned())]);
    assert_eq!(attribute(&tenant_runner, TENANT).as_deref(), Some("acme"));
    assert_eq!(
        attribute(&tenant_runner, COMPONENT).as_deref(),
        Some("ext-runner")
    );
}
