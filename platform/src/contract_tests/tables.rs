use super::sample_manifest;
use crate::db_roles::verify::{requirements, Holds};
use crate::deployer::schema::{declared, SchemaReport};
use crate::deployer::validator::fixtures::accepted_by_v0_27_0;
use crate::deployer::validator::validate_manifest;
use crate::repo::deployments::stored;
use crate::schema_manager::adopt::AdoptedSchema;
use cufflink_types::features::PRIVATE_TABLES;
use cufflink_types::openapi::{generate_openapi, PathStyle};
use cufflink_types::ServiceManifest;
use serde_json::json;

fn manifests() -> Vec<ServiceManifest> {
    std::iter::once(sample_manifest())
        .chain(accepted_by_v0_27_0())
        .collect()
}

#[test]
fn a_manifest_without_the_key_hashes_as_v0_29_2_did() {
    let sample: ServiceManifest =
        serde_json::from_str(include_str!("../../../types/fixtures/sample_manifest.json"))
            .expect("fixture");
    assert_eq!(
        sample.deploy_hash().expect("deploy hash"),
        "32b79ba0e4b4697a505007ae19a1ab55a1f4fbefb26fe8fcd86a53847eccba28"
    );
    assert_eq!(
        sample.schema_hash().expect("schema hash"),
        "422f205e16c949bd532f63707b78d8f6ccc4c9ba4eda0b2bd275c479a8d998f2"
    );
    for manifest in manifests() {
        let wire = serde_json::to_value(&manifest).expect("wire");
        assert_eq!(wire.get("private_tables"), None, "{}", manifest.name);
        assert!(
            !manifest.derived_features().contains(PRIVATE_TABLES),
            "{}",
            manifest.name
        );
    }
}

#[test]
fn a_manifest_without_the_key_deploys_as_v0_29_2_did() {
    for manifest in manifests() {
        validate_manifest(&manifest).unwrap_or_else(|e| panic!("{}: {e}", manifest.name));
        let tables = declared(&manifest);
        assert_eq!(
            serde_json::to_value(&tables).expect("declared"),
            serde_json::to_value(&manifest.tables).expect("tables"),
            "{}: the schema is its tables",
            manifest.name
        );
        assert_eq!(
            stored(&tables, &AdoptedSchema::default()).expect("snapshot"),
            serde_json::to_value(&manifest.tables).expect("tables"),
            "{}: the snapshot is the list every release stored",
            manifest.name
        );
        assert!(
            requirements(&manifest)
                .iter()
                .all(|(_, holds)| *holds == Holds::Dml),
            "{}: the role is asked of every table what it was",
            manifest.name
        );
    }
    assert_eq!(
        serde_json::to_value(SchemaReport::default()).expect("report"),
        json!({"schema_changes": []}),
        "a plan gains no key"
    );
}

#[test]
fn a_manifest_without_the_key_serves_as_v0_29_2_did() {
    for manifest in manifests() {
        assert_eq!(manifest.declared_tables().count(), manifest.tables.len());
        let spec = generate_openapi(&manifest, PathStyle::Absolute { tenant: "default" })
            .unwrap_or_else(|e| panic!("{}: {e:?}", manifest.name));
        for table in &manifest.tables {
            assert!(
                manifest.get_table(&table.name).is_some(),
                "{}.{} is served",
                manifest.name,
                table.name
            );
            let collection = format!("/svc/default/{}/{}", manifest.name, table.name);
            assert!(
                spec["paths"].get(&collection).is_some(),
                "{collection} is documented"
            );
        }
    }
}
