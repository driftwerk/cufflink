use cufflink_testkit::monitoring::{assets, dashboards, Dashboard, Query, Source, TENANTS};
use std::collections::{BTreeMap, BTreeSet};

const MIXED: &str = "-- Mixed --";

const LOKI: &[&str] = &[
    "service_namespace",
    "service_name",
    "cufflink_component",
    "deployment_environment_name",
    "detected_level",
    "service_version",
    "cufflink_handler",
    "cufflink_kind",
    "trace_id",
    "span_id",
];
const TEMPO: &[&str] = &[
    "resource.service.namespace",
    "resource.service.name",
    "resource.service.version",
    "resource.cufflink.component",
    "span.cufflink.handler",
    "span.cufflink.kind",
];
const SPAN_METRICS: &[&str] = &[
    "service_namespace",
    "service",
    "service_version",
    "span_name",
    "span_kind",
    "status_code",
    "cufflink_kind",
    "le",
];
const NATIVE: &[&str] = &[
    "tenant",
    "service",
    "handler",
    "route",
    "kind",
    "version",
    "commit",
    "mode",
    "le",
    "durable",
    "key",
    "operation",
    "outcome",
    "phase",
    "pool",
    "reason",
    "result",
    "schedule",
    "state",
    "status",
    "status_class",
    "table",
];

fn all() -> Vec<Dashboard> {
    dashboards().expect("the dashboards parse")
}

fn allowed(query: &Query) -> &'static [&'static str] {
    match query.source {
        Source::Loki => LOKI,
        Source::Tempo => TEMPO,
        Source::Prometheus if query.text.contains("traces_") => SPAN_METRICS,
        Source::Prometheus => NATIVE,
    }
}

fn faults<T: Ord>(
    dashboards: &[Dashboard],
    wrong: impl Fn(&Dashboard, &Query) -> Vec<T>,
) -> BTreeMap<String, BTreeSet<T>> {
    dashboards
        .iter()
        .map(|dashboard| {
            let found = dashboard
                .queries()
                .iter()
                .flat_map(|query| wrong(dashboard, query))
                .collect::<BTreeSet<_>>();
            (dashboard.file.clone(), found)
        })
        .filter(|(_, found)| !found.is_empty())
        .collect()
}

#[test]
fn the_dashboards_are_the_three_each_in_its_folder_and_named_by_its_uid() {
    let found: Vec<_> = all()
        .iter()
        .map(|dashboard| {
            (
                dashboard.folder.clone(),
                dashboard.file.clone(),
                dashboard.uid().to_string(),
            )
        })
        .collect();
    let named = |folder: &str, uid: &str| (folder.into(), format!("{uid}.json"), uid.into());
    assert_eq!(
        found,
        [
            named("Platform", "cufflink-platform"),
            named(TENANTS, "cufflink-service"),
            named(TENANTS, "cufflink-tenant"),
        ]
    );
}

#[test]
fn a_dashboard_names_only_the_provisioned_datasources() {
    let provisioned =
        std::fs::read_to_string(assets().join("grafana-datasources.yaml")).expect("datasources");
    let uids: BTreeSet<&str> = provisioned
        .lines()
        .filter_map(|line| line.trim().strip_prefix("uid: "))
        .collect();
    assert_eq!(uids, Source::ALL.map(Source::uid).into());

    for dashboard in all() {
        let foreign: Vec<_> = dashboard
            .datasources()
            .into_iter()
            .filter(|uid| *uid != MIXED && !uids.contains(uid))
            .collect();
        assert_eq!(foreign, Vec::<&str>::new(), "{}", dashboard.file);
        assert!(!dashboard.queries().is_empty(), "{}", dashboard.file);
    }
}

#[test]
fn a_query_uses_only_the_label_names_of_the_contract() {
    let unknown = faults(&all(), |_, query| {
        query
            .labels()
            .into_iter()
            .filter(|label| !allowed(query).contains(label))
            .map(|label| format!("{label} in {}", query.text))
            .collect()
    });
    assert_eq!(unknown, BTreeMap::new());
}

#[test]
fn a_metric_a_dashboard_reads_is_one_the_docs_describe() {
    let docs = std::fs::read_to_string(assets().join("../../docs/observability.md")).expect("docs");
    let undocumented = faults(&all(), |_, query| {
        query
            .metrics()
            .into_iter()
            .filter(|metric| !docs.contains(&format!("`{metric}`")))
            .map(str::to_string)
            .collect()
    });
    assert_eq!(undocumented, BTreeMap::new());
}

#[test]
fn a_link_to_a_dashboard_leads_to_one_that_exists() {
    let dashboards = all();
    let uids: BTreeSet<&str> = dashboards.iter().map(Dashboard::uid).collect();
    let linked: BTreeSet<&str> = dashboards
        .iter()
        .flat_map(Dashboard::linked_dashboards)
        .collect();
    assert_eq!(
        linked.difference(&uids).collect::<Vec<_>>(),
        Vec::<&&str>::new()
    );
    assert_eq!(
        linked,
        BTreeSet::from(["cufflink-service", "cufflink-tenant"]),
        "the tenant's table opens a service, and a service books back"
    );
}

#[test]
fn a_joined_table_names_every_column_it_keeps() {
    let dashboards = all();
    let tables: Vec<_> = dashboards
        .iter()
        .flat_map(Dashboard::panels)
        .filter(|panel| panel["type"] == "table" && panel["targets"][1].is_object())
        .collect();
    assert_eq!(tables.len(), 1, "the tenant's services");
    for table in tables {
        let kept = table["transformations"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|step| step["id"] == "filterFieldsByName")
            .and_then(|step| step["options"]["include"]["pattern"].as_str())
            .map(|pattern| regex::Regex::new(pattern).expect("a pattern"))
            .expect("the table filters its fields");
        let values: BTreeSet<String> = table["targets"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|target| target["refId"].as_str())
            .map(|query| format!("Value #{query}"))
            .filter(|value| kept.is_match(value))
            .collect();
        let named: BTreeSet<String> = table["fieldConfig"]["overrides"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|column| {
                column["properties"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|property| property["id"] == "displayName")
            })
            .filter_map(|column| column["matcher"]["options"].as_str())
            .filter(|name| name.starts_with("Value #"))
            .map(str::to_string)
            .collect();
        assert_eq!(values, named);
        assert!(!kept.is_match("Value #version"), "the version is a label");
    }
}

fn names_the_tenant(query: &Query) -> bool {
    let labels: &[&str] = match query.source {
        Source::Loki => &["service_namespace"],
        Source::Tempo => &["resource.service.namespace"],
        Source::Prometheus => &["service_namespace", "tenant"],
    };
    let names = |selector: &str| {
        labels.iter().any(|label| {
            let matcher = format!(r#"{label}="$tenant""#);
            selector
                .match_indices(&matcher)
                .any(|(at, _)| selector[..at].ends_with(['{', ',', ' ']))
        })
    };
    let selectors = query.selectors();
    !selectors.is_empty()
        && selectors.iter().all(|selector| names(selector))
        && !query.reads_a_bare_metric()
}

#[test]
fn nothing_a_dashboard_asks_goes_unread() {
    let unread: BTreeMap<_, _> = all()
        .into_iter()
        .map(|dashboard| (dashboard.file.clone(), dashboard.unread()))
        .filter(|(_, unread)| !unread.is_empty())
        .collect();
    assert_eq!(unread, BTreeMap::new());
}

#[test]
fn every_query_of_a_tenants_dashboard_names_the_tenant() {
    const TENANTS_LISTED: &str = r#"label_values({service_namespace=~".+"}, service_namespace)"#;
    let dashboards: Vec<_> = all()
        .into_iter()
        .filter(|dashboard| dashboard.folder == TENANTS)
        .collect();
    assert_eq!(dashboards.len(), 2);
    let unscoped = faults(&dashboards, |_, query| {
        Some(query.text.clone())
            .filter(|text| text != TENANTS_LISTED && !names_the_tenant(query))
            .into_iter()
            .collect()
    });
    assert_eq!(unscoped, BTreeMap::new());
}

#[test]
fn a_query_without_the_tenant_is_found() {
    let query = |source, text: &str| Query {
        source,
        text: text.to_string(),
    };
    assert!(!names_the_tenant(&query(
        Source::Prometheus,
        r#"sum(rate(cufflink_crud_requests_total{service="$service"}[5m]))"#
    )));
    assert!(!names_the_tenant(&query(
        Source::Loki,
        r#"{service_name="$service", tenant="$tenant"}"#
    )));
    for (source, text) in [
        (Source::Tempo, r#"{resource.service.name="$service"}"#),
        (
            Source::Prometheus,
            r#"rate(a_total{service_namespace="$tenant"}[5m]) / rate(b_total{service="$service"}[5m])"#,
        ),
        (
            Source::Prometheus,
            r#"sum(cufflink_dlq_messages_total{other_tenant="$tenant"})"#,
        ),
        (
            Source::Prometheus,
            r#"cufflink_outbox_depth{tenant="$tenant"} / cufflink_services_total"#,
        ),
        (
            Source::Prometheus,
            r#"cufflink_job_backlog{tenant=~"$tenant"}"#,
        ),
        (Source::Prometheus, "vector(1)"),
        (
            Source::Loki,
            r#"{service_name="$service"} | line_format "service_namespace=\"$tenant\"""#,
        ),
    ] {
        assert!(!names_the_tenant(&query(source, text)), "{text}");
    }
    let mixed = query(
        Source::Prometheus,
        r#"sum by (span_name, le) (rate(traces_spanmetrics_latency_bucket{service_namespace="$tenant", colour!="red"}[5m]))"#,
    );
    assert!(names_the_tenant(&mixed));
    assert_eq!(
        mixed.labels(),
        BTreeSet::from(["colour", "le", "service_namespace", "span_name"])
    );
    assert_eq!(
        mixed.metrics(),
        BTreeSet::from(["traces_spanmetrics_latency"])
    );
}
