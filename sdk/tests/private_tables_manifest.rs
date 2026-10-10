include!("ui/pass/private_tables.rs");

fn manifest() -> cufflink::manifest::ServiceManifest {
    cufflink::manifest::ServiceManifest {
        version: Some("0.0.0".to_string()),
        ..__cufflink_manifest().expect("manifest")
    }
}

#[test]
fn private_tables_manifest_matches_the_golden() {
    assert_eq!(
        serde_json::to_string_pretty(&manifest()).expect("serializes"),
        include_str!("golden/private_tables.manifest.json").trim_end()
    );
}

#[test]
fn private_tables_fill_their_own_list_and_demand_their_feature() {
    use cufflink::manifest::validate::validate_private_table;
    let manifest = manifest();
    let names = |tables: &[cufflink::manifest::TableDefinition]| -> Vec<String> {
        tables.iter().map(|table| table.name.clone()).collect()
    };
    assert_eq!(names(&manifest.tables), ["labels"]);
    assert_eq!(names(&manifest.private_tables), ["loans", "loan_tags"]);
    assert!(manifest.get_table("loans").is_none());
    assert_eq!(
        manifest.required_platform_features,
        [
            "private_tables",
            "route_matching",
            "route_posture",
            "schema_adopted"
        ]
    );
    for table in &manifest.private_tables {
        assert_eq!(validate_private_table(table), Ok(()));
    }
}
