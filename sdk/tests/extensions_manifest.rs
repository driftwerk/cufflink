include!("ui/pass/extensions.rs");

#[test]
fn declared_extensions_reach_the_manifest_and_demand_ext_call() {
    let manifest = __cufflink_manifest().expect("manifest");
    assert_eq!(manifest.extensions, ["google.token", "pdf.render-v2"]);
    assert_eq!(
        manifest.required_platform_features,
        ["ext_call", "route_posture"]
    );
    let wire = serde_json::to_value(&manifest).expect("serializes");
    assert_eq!(
        wire["extensions"],
        serde_json::json!(["google.token", "pdf.render-v2"])
    );
}
