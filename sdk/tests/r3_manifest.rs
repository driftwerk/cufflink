include!("ui/pass/r3_keywords.rs");

fn manifest() -> cufflink::manifest::ServiceManifest {
    cufflink::manifest::ServiceManifest {
        version: Some("0.0.0".to_string()),
        ..__cufflink_manifest().expect("manifest")
    }
}

#[test]
fn r3_keywords_manifest_matches_the_golden() {
    assert_eq!(
        serde_json::to_string_pretty(&manifest()).expect("serializes"),
        include_str!("golden/r3_keywords.manifest.json").trim_end()
    );
}

#[test]
fn r3_keywords_demand_their_platform_features() {
    assert_eq!(
        manifest().required_platform_features,
        [
            "audit_redaction",
            "column_types_v2",
            "jobs_v2",
            "keyset_lists",
            "nats_consumer_config",
            "route_posture",
            "schedules",
            "schema_adopted",
            "streams",
        ]
    );
}

#[test]
fn the_emitted_manifest_passes_the_shared_validators() {
    use cufflink::manifest::validate::{
        validate_schedules, validate_streams, validate_subscription_delivery,
    };
    let manifest = manifest();
    assert_eq!(validate_streams("streams", &manifest.streams), Ok(()));
    assert_eq!(validate_schedules("schedules", &manifest.schedules), Ok(()));
    for subscription in &manifest.subscriptions {
        assert_eq!(
            validate_subscription_delivery("subscription", subscription),
            Ok(())
        );
    }
    assert_eq!(manifest.schema(), "loan_service");
}
