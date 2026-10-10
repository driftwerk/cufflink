include!("ui/pass/every_keyword.rs");

#[test]
fn every_keyword_manifest_matches_the_golden() {
    let manifest = cufflink::manifest::ServiceManifest {
        version: Some("0.0.0".to_string()),
        ..__cufflink_manifest().expect("manifest")
    };
    assert_eq!(
        serde_json::to_string_pretty(&manifest).expect("serializes"),
        include_str!("golden/every_keyword.manifest.json").trim_end()
    );
}
