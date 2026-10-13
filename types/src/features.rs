//! Platform capability names: what `/api/version` advertises in `supports`
//! and a manifest may demand in `required_platform_features`.

use crate::{
    CustomRouteDefinition, ScheduleDefinition, ServiceManifest, SubscriptionDefinition,
    TableDefinition,
};
use std::collections::BTreeSet;

pub const ROUTE_POSTURE: &str = "route_posture";
pub const DB_V2: &str = "db_v2";
pub const CACHE: &str = "cache";
pub const SERVICE_CALL: &str = "service_call";
pub const RESPONSE_V2: &str = "response_v2";
pub const SSE_BRIDGE: &str = "sse_bridge";
pub const ROUTE_MATCHING: &str = "route_matching";
pub const ROUTE_LIMITS: &str = "route_limits";
pub const ERROR_PROFILE: &str = "error_profile";
pub const INTERNAL_POSTURE: &str = "internal_posture";
pub const API_KEY_POSTURE: &str = "api_key_posture";
pub const SCOPED_PERMISSIONS: &str = "scoped_permissions";
pub const SCOPED_POSTURE: &str = "scoped_posture";
pub const TYPED_ROUTES: &str = "typed_routes";
pub const NATS_CONSUMER_CONFIG: &str = "nats_consumer_config";
pub const NATS_MESSAGE_META: &str = "nats_message_meta";
pub const NATS_PUBLISH_V2: &str = "nats_publish_v2";
pub const STREAMS: &str = "streams";
pub const SCHEDULES: &str = "schedules";
pub const JOBS_V2: &str = "jobs_v2";
pub const DELIVERY_CONTROL: &str = "delivery_control";
pub const HTTP_V2: &str = "http_v2";
pub const STORAGE_V2: &str = "storage_v2";
pub const UTIL_V2: &str = "util_v2";
pub const SCHEMA_ADOPTED: &str = "schema_adopted";
pub const SQL_MIGRATIONS: &str = "sql_migrations";
pub const COLUMN_TYPES_V2: &str = "column_types_v2";
pub const AUDIT_REDACTION: &str = "audit_redaction";
pub const KEYSET_LISTS: &str = "keyset_lists";
pub const OUTBOX: &str = "outbox";
pub const DB_ROLES: &str = "db_roles";
pub const SCHEMA_GUARD: &str = "schema_guard";
pub const NAMESPACE_CLAIMS: &str = "namespace_claims";
pub const SECRET_ENCRYPTION: &str = "secret_encryption";
pub const CONSUMER_RECONCILER: &str = "consumer_reconciler";
pub const ASYNC_DEPLOY: &str = "async_deploy";
pub const EXT_CALL: &str = "ext_call";
pub const NATIVE_SERVICES: &str = "native_services";
pub const PRIVATE_TABLES: &str = "private_tables";
pub const INACTIVE_DEPLOY: &str = "inactive_deploy";
pub const CONFIG_APPLY: &str = "config_apply";
pub const COLUMN_PERMISSIONS: &str = "column_permissions";

impl ServiceManifest {
    /// Features this manifest's declared shape requires, sorted and
    /// deduplicated. A manifest that needs a capability always demands it, so
    /// a platform too old to provide it rejects the deploy instead of silently
    /// running the service without it.
    pub fn derived_features(&self) -> BTreeSet<&'static str> {
        self.custom_routes
            .iter()
            .flat_map(CustomRouteDefinition::derived_features)
            .chain(
                self.declared_tables()
                    .flat_map(TableDefinition::derived_features),
            )
            .chain(
                self.subscriptions
                    .iter()
                    .flat_map(SubscriptionDefinition::derived_features),
            )
            .chain(
                self.schedules
                    .iter()
                    .flat_map(ScheduleDefinition::derived_features),
            )
            .chain(self.service_features())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::fixtures::sample_manifest;
    use crate::RoutePosture;
    use serde_json::Value;

    const SAMPLE: &str = include_str!("../fixtures/sample_manifest.json");
    const TODO: &str = include_str!("../fixtures/todo_manifest.json");
    const BLOG: &str = include_str!("../fixtures/blog_manifest.json");
    /// Every shape v0.27.0 accepted, canonicalised and hashed by v0.27.0.
    /// It wrote a subscription's `config` in an order that changed from
    /// one process to the next; the hashes are those of the runs that
    /// wrote it in the order of its keys, as every run does now.
    const LEGACY_SHAPES: &str = include_str!("../fixtures/legacy_shapes_manifest.json");

    fn manifest(json: &str) -> ServiceManifest {
        serde_json::from_str(json).expect("fixture manifest")
    }

    fn route(handler: &str, posture: Option<RoutePosture>) -> CustomRouteDefinition {
        CustomRouteDefinition {
            method: "GET".into(),
            path: format!("/{handler}"),
            handler: handler.into(),
            job: None,
            posture,
            forward_auth: false,
            ..Default::default()
        }
    }

    #[test]
    fn sample_manifest_derives_only_route_posture() {
        let sample = manifest(SAMPLE);
        assert_eq!(sample.derived_features(), BTreeSet::from([ROUTE_POSTURE]));
        assert_eq!(sample.required_platform_features, vec![ROUTE_POSTURE]);
        for crud_only in [TODO, BLOG] {
            assert!(manifest(crud_only).derived_features().is_empty());
        }
    }

    #[test]
    fn route_posture_is_derived_once_from_any_declared_posture() {
        let mut m = sample_manifest();
        assert!(m.derived_features().is_empty());
        m.custom_routes.push(route("legacy", None));
        assert!(m.derived_features().is_empty());
        m.custom_routes
            .push(route("me", Some(RoutePosture::Authenticated)));
        m.custom_routes.push(route(
            "hook",
            Some(RoutePosture::Public {
                reason: "signed webhook".into(),
            }),
        ));
        assert_eq!(m.derived_features(), BTreeSet::from([ROUTE_POSTURE]));
    }

    #[test]
    fn strict_routing_demands_route_matching() {
        let mut m = sample_manifest();
        m.routing = crate::RoutingMode::Strict;
        assert_eq!(m.derived_features(), BTreeSet::from([ROUTE_MATCHING]));
    }

    #[test]
    fn an_error_profile_demands_error_profile() {
        let mut m = sample_manifest();
        m.errors = Some(crate::ErrorProfile {
            key: "error_message".into(),
        });
        assert_eq!(m.derived_features(), BTreeSet::from([ERROR_PROFILE]));
    }

    fn sql_migration() -> crate::SqlMigration {
        crate::SqlMigration {
            version: 1,
            name: "initial_tables".into(),
            sql: "CREATE TABLE loans ();".into(),
            no_transaction: false,
        }
    }

    fn stream() -> crate::StreamDefinition {
        serde_json::from_value(serde_json::json!({
            "name": "LOANS", "subjects": ["loans.created"], "retention": "limits"
        }))
        .expect("stream")
    }

    fn schedule() -> crate::ScheduleDefinition {
        serde_json::from_value(serde_json::json!({
            "name": "order_retry", "handler": "retry_orders", "spec": {"kind": "every", "secs": 30}
        }))
        .expect("schedule")
    }

    fn private_table() -> TableDefinition {
        serde_json::from_value(serde_json::json!({
            "name": "loans", "managed": false,
            "columns": [{"name": "body", "column_type": "text"}]
        }))
        .expect("table")
    }

    /// One case per R3 field: where the field lands in the manifest JSON,
    /// the manifest before it is set, the same manifest with only that
    /// field set, and the features it demands.
    fn r3_fields() -> Vec<(
        &'static str,
        ServiceManifest,
        ServiceManifest,
        Vec<&'static str>,
    )> {
        use crate::{
            AckMode, ConsumerNaming, HandlerType, JobConfig, MigrationHistory, Pagination, RetryOn,
            SchemaOwnership,
        };
        let base = || manifest(SAMPLE);
        let subscribed = || {
            let mut m = base();
            m.subscriptions.insert(
                0,
                SubscriptionDefinition::new("dw.{tenant}.items.created", "h", HandlerType::Wasm),
            );
            m
        };
        let service =
            |pointer, after: ServiceManifest, features| (pointer, base(), after, features);
        let table = |pointer, edit: fn(&mut TableDefinition), features| {
            let mut after = base();
            edit(&mut after.tables[0]);
            (pointer, base(), after, features)
        };
        let route = |pointer, edit: fn(&mut crate::CustomRouteDefinition), features| {
            let mut after = base();
            edit(&mut after.custom_routes[0]);
            (pointer, base(), after, features)
        };
        let subscription = |pointer, edit: fn(&mut SubscriptionDefinition), features| {
            let mut after = subscribed();
            edit(&mut after.subscriptions[0]);
            (pointer, subscribed(), after, features)
        };
        vec![
            service(
                "/consumers",
                ServiceManifest {
                    consumers: ConsumerNaming::Scoped,
                    ..base()
                },
                vec![NATS_CONSUMER_CONFIG],
            ),
            service(
                "/streams",
                ServiceManifest {
                    streams: vec![stream()],
                    ..base()
                },
                vec![STREAMS],
            ),
            service(
                "/schedules",
                ServiceManifest {
                    schedules: vec![schedule()],
                    ..base()
                },
                vec![SCHEDULES],
            ),
            service(
                "/schedules/0/job/retry_on",
                ServiceManifest {
                    schedules: vec![crate::ScheduleDefinition {
                        job: Some(JobConfig {
                            retry_on: vec![RetryOn::Timeout],
                            ..Default::default()
                        }),
                        ..schedule()
                    }],
                    ..base()
                },
                vec![SCHEDULES, JOBS_V2],
            ),
            service(
                "/schema",
                ServiceManifest {
                    schema: SchemaOwnership::Adopted,
                    ..base()
                },
                vec![SCHEMA_ADOPTED],
            ),
            service(
                "/schema_name",
                ServiceManifest {
                    schema_name: Some("loan_service".into()),
                    ..base()
                },
                vec![SCHEMA_ADOPTED],
            ),
            service(
                "/migrations",
                ServiceManifest {
                    migrations: vec![sql_migration()],
                    ..base()
                },
                vec![SQL_MIGRATIONS],
            ),
            service(
                "/extensions",
                ServiceManifest {
                    extensions: vec!["google.token".into()],
                    ..base()
                },
                vec![EXT_CALL],
            ),
            service(
                "/provides",
                ServiceManifest {
                    mode: crate::ServiceMode::Native,
                    provides: vec![crate::ProvidedExtension {
                        name: "google.token".into(),
                        callers: vec!["reports-service".into()],
                    }],
                    ..base()
                },
                vec![NATIVE_SERVICES],
            ),
            service(
                "/migration_history",
                ServiceManifest {
                    migration_history: Some(MigrationHistory::default()),
                    ..base()
                },
                vec![],
            ),
            subscription(
                "/subscriptions/0/ack",
                |s| s.ack = AckMode::OnSuccess,
                vec![NATS_CONSUMER_CONFIG],
            ),
            subscription(
                "/subscriptions/0/consumer",
                |s| s.consumer = Some(Default::default()),
                vec![NATS_CONSUMER_CONFIG],
            ),
            subscription(
                "/subscriptions/0/timeout_secs",
                |s| s.timeout_secs = Some(30),
                vec![NATS_CONSUMER_CONFIG],
            ),
            table(
                "/tables/0/managed",
                |t| t.managed = false,
                vec![SCHEMA_ADOPTED],
            ),
            table(
                "/tables/0/checks",
                |t| {
                    t.checks.push(crate::CheckConstraint {
                        name: "c".into(),
                        expr: "a > 0".into(),
                    })
                },
                vec![COLUMN_TYPES_V2],
            ),
            table(
                "/tables/0/search",
                |t| t.search = Some(Default::default()),
                vec![COLUMN_TYPES_V2],
            ),
            table(
                "/tables/0/indexes/0/predicate",
                |t| {
                    t.indexes.insert(
                        0,
                        crate::IndexDefinition {
                            name: "i".into(),
                            columns: vec!["id".into()],
                            predicate: Some("id IS NOT NULL".into()),
                            ..Default::default()
                        },
                    )
                },
                vec![COLUMN_TYPES_V2],
            ),
            table(
                "/tables/0/indexes/0/method",
                |t| {
                    t.indexes.insert(
                        0,
                        crate::IndexDefinition {
                            name: "i".into(),
                            columns: vec!["id".into()],
                            method: crate::IndexMethod::Hash,
                            ..Default::default()
                        },
                    )
                },
                vec![COLUMN_TYPES_V2],
            ),
            table(
                "/tables/0/audit_redact",
                |t| t.audit_redact.push("dob".into()),
                vec![AUDIT_REDACTION],
            ),
            table(
                "/tables/0/sensitive",
                |t| t.sensitive = true,
                vec![AUDIT_REDACTION],
            ),
            route(
                "/custom_routes/0/audit_redact",
                |r| r.audit_redact.push("card.number".into()),
                vec![AUDIT_REDACTION],
            ),
            table(
                "/tables/0/pagination",
                |t| t.pagination = Pagination::Keyset,
                vec![KEYSET_LISTS],
            ),
            table(
                "/tables/0/columns/1/access",
                |t| {
                    t.columns[1].access = Some(crate::ColumnAccess {
                        read: Some("internal".into()),
                        write: None,
                    })
                },
                vec![COLUMN_PERMISSIONS],
            ),
            service(
                "/private_tables",
                ServiceManifest {
                    private_tables: vec![private_table()],
                    ..base()
                },
                vec![PRIVATE_TABLES, SCHEMA_ADOPTED],
            ),
        ]
    }

    fn json(manifest: &ServiceManifest) -> Value {
        serde_json::to_value(manifest).expect("serialize")
    }

    #[test]
    fn every_r3_field_is_absent_until_it_is_set() {
        for (pointer, before, after, _) in r3_fields() {
            assert_eq!(json(&before).pointer(pointer), None, "{pointer}");
            assert!(json(&after).pointer(pointer).is_some(), "{pointer}");
        }
    }

    #[test]
    fn every_r3_field_changes_the_hash_and_round_trips() {
        for (pointer, before, after, _) in r3_fields() {
            assert_ne!(
                after.deploy_hash().expect("hash"),
                before.deploy_hash().expect("hash"),
                "{pointer}"
            );
            let wire = json(&after);
            let reparsed: ServiceManifest = serde_json::from_value(wire.clone()).expect("parse");
            assert_eq!(json(&reparsed), wire, "{pointer}");
        }
    }

    #[test]
    fn every_r3_field_demands_its_features() {
        for (pointer, before, after, demanded) in r3_fields() {
            assert_eq!(
                before.derived_features(),
                BTreeSet::from([ROUTE_POSTURE]),
                "{pointer}"
            );
            let expected: BTreeSet<&str> = demanded.into_iter().chain([ROUTE_POSTURE]).collect();
            assert_eq!(after.derived_features(), expected, "{pointer}");
        }
    }

    #[test]
    fn deploy_hash_unchanged_for_legacy_manifest() {
        for (json, deploy, schema) in [
            (
                SAMPLE,
                "eee68954141745273b4157ffa4da676f33db4a908f63949bc9a4d7a2a437a17e",
                "c1bfff7742704c2d5c1849a6eb964cece595080d25bcddf9e899dbd137ce5978",
            ),
            (
                TODO,
                "927dfafc66a426e311dc92eee91f2964e6ad450f7d57266a8cab4300a1359bab",
                "aa0463d8a7e1fd9e3c6bca62fa0480e4ee6ce4083d17441f02061dca699e9907",
            ),
            (
                BLOG,
                "9d931c651c3bff3d9be5105b7867ffae01913757bdd8db0a13b1337dd17b7ae5",
                "98cd636af82779af1d42ccc7f7daff5b1fd0e70cb553a15233ce4a4e3655e43f",
            ),
            (
                LEGACY_SHAPES,
                "6bcd77c75c7980173facd21b6032b82636e6ec4119c0c6636acd1a67a1f2343a",
                "11dc0ab100791653dc63e43a52d5c870ccf2a9d6997f64775664887400107439",
            ),
        ] {
            let m = manifest(json);
            assert_eq!(m.deploy_hash().expect("hash"), deploy, "{}", m.name);
            assert_eq!(m.schema_hash().expect("schema hash"), schema, "{}", m.name);
        }
    }

    #[test]
    fn a_manifest_without_private_tables_serialises_as_before() {
        for json in [SAMPLE, TODO, BLOG, LEGACY_SHAPES] {
            let stored: Value = serde_json::from_str(json).expect("fixture json");
            let m = manifest(json);
            assert!(m.private_tables.is_empty(), "{}", m.name);
            let wire = serde_json::to_value(&m).expect("serialize");
            assert_eq!(wire.get("private_tables"), None, "{}", m.name);
            assert_eq!(wire, stored, "{}", m.name);
            assert_eq!(m.declared_tables().count(), m.tables.len(), "{}", m.name);
        }
    }

    #[test]
    fn private_tables_demand_their_feature() {
        let mut m = sample_manifest();
        assert!(m.derived_features().is_empty());
        m.private_tables = vec![private_table()];
        assert_eq!(
            m.derived_features(),
            BTreeSet::from([PRIVATE_TABLES, SCHEMA_ADOPTED])
        );
        m.private_tables[0].columns[0].column_type = crate::ColumnType::JsonbArray;
        assert_eq!(
            m.derived_features(),
            BTreeSet::from([COLUMN_TYPES_V2, PRIVATE_TABLES, SCHEMA_ADOPTED])
        );
    }

    #[test]
    fn legacy_manifest_json_round_trips_unchanged() {
        for json in [SAMPLE, TODO, BLOG, LEGACY_SHAPES] {
            let stored: Value = serde_json::from_str(json).expect("fixture json");
            let reserialized = serde_json::to_value(manifest(json)).expect("serialize");
            assert_eq!(reserialized, stored);
        }
    }

    #[test]
    fn a_legacy_manifest_demands_what_it_demanded_of_v0_27_0() {
        assert_eq!(
            manifest(LEGACY_SHAPES).derived_features(),
            BTreeSet::from([
                "api_key_posture",
                "error_profile",
                "internal_posture",
                "route_limits",
                "route_matching",
                "route_posture",
                "scoped_permissions",
                "scoped_posture",
                "sse_bridge",
            ])
        );
    }
}
