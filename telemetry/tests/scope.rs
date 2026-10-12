mod support;

use cufflink_telemetry::scope::{HANDLER, KIND, SERVICE, TENANT, VERSION};
use cufflink_telemetry::{scopable_span, scoped_span, Kind, Scope};
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use opentelemetry::Context;
use serde_json::{json, Value};
use support::{exported_by, log_attribute, log_scope, log_trace, pairs, span_scope, span_trace};
use tracing::{info, info_span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

fn list_notes() -> Scope {
    Scope {
        service: Some("notes".into()),
        version: Some(7),
        handler: Some("list".into()),
        kind: Some(Kind::Route),
        ..Scope::tenant("acme")
    }
}

fn list_notes_exported() -> Vec<(String, String)> {
    pairs(&[
        (TENANT, "acme"),
        (SERVICE, "notes"),
        (VERSION, "v7"),
        (HANDLER, "list"),
        (KIND, "route"),
    ])
}

fn propagated() -> (TraceId, Context) {
    let trace = TraceId::from(0x0af7_6519_16cd_43dd_8448_eb21_1c80_319c_u128);
    let parent = SpanContext::new(
        trace,
        SpanId::from(0x00f0_67aa_0ba9_02b7_u64),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    );
    (trace, Context::new().with_remote_span_context(parent))
}

#[test]
fn a_child_span_inherits_its_parents_scope() {
    let exported = exported_by(|| {
        let _handler = scoped_span!("wasm_handler", list_notes()).entered();
        assert_eq!(Scope::current(), Some(list_notes()));
        let _query = info_span!("db.query").entered();
        let _rows = info_span!("db.rows", rows = 3).entered();
        assert_eq!(Scope::current(), Some(list_notes()));
    });

    ["wasm_handler", "db.query", "db.rows"]
        .into_iter()
        .for_each(|name| {
            assert_eq!(
                span_scope(exported.span(name)),
                list_notes_exported(),
                "{name}"
            )
        });
}

#[test]
fn a_span_outside_any_scope_carries_none() {
    let exported = exported_by(|| {
        drop(scoped_span!("wasm_handler", list_notes()).entered());
        let _deploy = info_span!("deploy").entered();
        let _step = info_span!("migrate").entered();
        assert_eq!(Scope::current(), None);
    });

    ["deploy", "migrate"]
        .into_iter()
        .for_each(|name| assert_eq!(span_scope(exported.span(name)), [], "{name}"));
}

#[test]
fn a_tenant_only_scope_exports_the_tenant_alone() {
    let exported = exported_by(|| {
        let _relay = scoped_span!("outbox_relay", Scope::tenant("acme"), batch = 4).entered();
        assert_eq!(Scope::current(), Some(Scope::tenant("acme")));
        info!("relayed");
    });

    let tenant_only = pairs(&[(TENANT, "acme")]);
    assert_eq!(span_scope(exported.span("outbox_relay")), tenant_only);
    assert_eq!(log_scope(exported.log("relayed")), tenant_only);
}

#[test]
fn a_declared_field_overrides_the_inherited_one() {
    let before_create = Scope {
        handler: Some("before_create".into()),
        kind: Some(Kind::Hook),
        ..Scope::tenant("acme")
    };
    let exported = exported_by(|| {
        let _request = scoped_span!("request", list_notes()).entered();
        let _hook = scoped_span!("wasm_handler", before_create).entered();
        info_span!("db.query").in_scope(|| info!("in the hook"));
    });

    let hook = pairs(&[
        (TENANT, "acme"),
        (SERVICE, "notes"),
        (VERSION, "v7"),
        (HANDLER, "before_create"),
        (KIND, "hook"),
    ]);
    assert_eq!(span_scope(exported.span("request")), list_notes_exported());
    assert_eq!(span_scope(exported.span("wasm_handler")), hook);
    assert_eq!(span_scope(exported.span("db.query")), hook);
    assert_eq!(log_scope(exported.log("in the hook")), hook);
}

#[test]
fn a_recorded_field_overrides_the_inherited_one() {
    let before_create = Scope {
        handler: Some("before_create".into()),
        kind: Some(Kind::Hook),
        ..Scope::tenant("acme")
    };
    let after_create = Scope {
        handler: Some("after_create".into()),
        ..before_create.clone()
    };
    let exported = exported_by(|| {
        let _request = scoped_span!("request", list_notes()).entered();
        let hook = scopable_span!("hook").entered();
        assert_eq!(Scope::current(), Some(list_notes()));

        before_create.record_on(&hook);
        drop(info_span!("first").entered());

        after_create.record_on(&hook);
        assert_eq!(
            Scope::current(),
            Some(Scope {
                handler: Some("after_create".into()),
                kind: Some(Kind::Hook),
                ..list_notes()
            })
        );
        let _second = info_span!("second").entered();
        info!("ran");
    });

    let hooked = |handler| {
        pairs(&[
            (TENANT, "acme"),
            (SERVICE, "notes"),
            (VERSION, "v7"),
            (HANDLER, handler),
            (KIND, "hook"),
        ])
    };
    assert_eq!(span_scope(exported.span("request")), list_notes_exported());
    assert_eq!(span_scope(exported.span("hook")), hooked("after_create"));
    assert_eq!(span_scope(exported.span("first")), hooked("before_create"));
    assert_eq!(span_scope(exported.span("second")), hooked("after_create"));
    assert_eq!(log_scope(exported.log("ran")), hooked("after_create"));
}

#[test]
fn a_scope_recorded_late_reaches_children_opened_after_it() {
    let exported = exported_by(|| {
        let request = scopable_span!("request", http.method = "GET").entered();
        assert_eq!(Scope::current(), None);
        let early = info_span!("rate_limit");
        early.in_scope(|| info!("admitted"));

        list_notes().record_on(&request);
        assert_eq!(Scope::current(), Some(list_notes()));
        early.in_scope(|| info!("still unscoped"));
        info_span!("db.query").in_scope(|| info!("queried"));
    });

    assert_eq!(span_scope(exported.span("request")), list_notes_exported());
    assert_eq!(span_scope(exported.span("db.query")), list_notes_exported());
    assert_eq!(log_scope(exported.log("queried")), list_notes_exported());
    assert_eq!(span_scope(exported.span("rate_limit")), []);
    assert_eq!(log_scope(exported.log("admitted")), []);
    assert_eq!(log_scope(exported.log("still unscoped")), []);
}

#[test]
fn a_name_recorded_late_renames_the_exported_span() {
    let exported = exported_by(|| {
        let request = scopable_span!("request", otel.name = "GET unmatched").entered();
        list_notes().record_on(&request);
        request.record("otel.name", "acme/notes/list_notes");
    });

    assert_eq!(
        span_scope(exported.span("acme/notes/list_notes")),
        list_notes_exported()
    );
    assert!(exported
        .spans
        .iter()
        .all(|span| span.name != "GET unmatched" && span.name != "request"));
}

#[test]
fn the_diagnostics_of_exporting_are_logged_and_never_exported() {
    let _ = tracing_log::LogTracer::init();
    let exported = exported_by(|| {
        info!(target: "opentelemetry_sdk", "a record could not be emitted");
        info!(target: "opentelemetry-otlp", "the collector refused a batch");
        info!(target: "hyper_util::client::legacy::pool", "reuse idle connection");
        info!(target: "hyper", "a connection of the exporter's");
        tracing_log::log::info!(target: "reqwest::connect", "starting new connection");
        info!(target: "cufflink_platform", "a line of the platform's");
        info!(target: "hyperdrive::engine", "a line of another crate's");
        tracing_log::log::info!(target: "cufflink_native", "a line through log");
    });

    let kept = [
        "a line of the platform's",
        "a line of another crate's",
        "a line through log",
    ];
    for message in kept {
        exported.log(message);
    }
    assert_eq!(exported.logs.len(), kept.len());
    for message in [
        "a record could not be emitted",
        "the collector refused a batch",
        "reuse idle connection",
        "a connection of the exporter's",
        "starting new connection",
        "a line of the platform's",
    ] {
        exported.line(message);
    }
}

#[test]
fn a_log_inside_a_scope_carries_the_five_attributes() {
    let exported = exported_by(|| {
        let _handler = scoped_span!("wasm_handler", list_notes()).entered();
        info!(tenant = "acme", service = "notes", "[wasm] listing");
        info_span!("db.query").in_scope(|| warn!(rows = 0, "nothing found"));
    });

    let listing = exported.log("[wasm] listing");
    assert_eq!(log_scope(listing), list_notes_exported());
    assert_eq!(log_attribute(listing, "tenant").as_deref(), Some("acme"));
    assert_eq!(log_attribute(listing, "service").as_deref(), Some("notes"));
    assert_eq!(
        log_scope(exported.log("nothing found")),
        list_notes_exported()
    );
}

#[test]
fn an_attribute_the_event_set_wins() {
    let exported = exported_by(|| {
        let _handler = scoped_span!("wasm_handler", list_notes()).entered();
        info!(cufflink.handler = "replayed", "named by the event");
    });

    assert_eq!(
        log_scope(exported.log("named by the event")),
        pairs(&[
            (TENANT, "acme"),
            (SERVICE, "notes"),
            (VERSION, "v7"),
            (HANDLER, "replayed"),
            (KIND, "route"),
        ])
    );
}

#[test]
fn a_log_outside_any_scope_carries_none() {
    let exported = exported_by(|| {
        info!(tenant = "acme", service = "notes", "Deploying");
        info_span!("deploy").in_scope(|| info!("migrated"));
        drop(scoped_span!("wasm_handler", list_notes()).entered());
        info!("after the handler");
    });

    let deploying = exported.log("Deploying");
    assert_eq!(log_scope(deploying), []);
    assert_eq!(log_attribute(deploying, "tenant").as_deref(), Some("acme"));
    assert_eq!(
        log_attribute(deploying, "service").as_deref(),
        Some("notes")
    );
    assert_eq!(log_trace(deploying), None);
    assert_eq!(log_scope(exported.log("migrated")), []);
    assert_eq!(log_scope(exported.log("after the handler")), []);
    assert_eq!(log_trace(exported.log("after the handler")), None);
}

#[test]
fn a_log_carries_the_trace_and_span_of_its_span() {
    let exported = exported_by(|| {
        let _request = info_span!("request").entered();
        info!("in the request");
        info_span!("db.query").in_scope(|| info!("in the query"));
        info!(parent: None, "detached");
    });

    let (request, query) = (exported.span("request"), exported.span("db.query"));
    assert_eq!(
        log_trace(exported.log("in the request")),
        Some(span_trace(request))
    );
    assert_eq!(
        log_trace(exported.log("in the query")),
        Some(span_trace(query))
    );
    assert_eq!(span_trace(query).0, span_trace(request).0);
    assert_eq!(query.parent_span_id, span_trace(request).1);
    assert_eq!(log_trace(exported.log("detached")), None);
}

#[test]
fn a_log_under_a_propagated_parent_carries_the_parents_trace() {
    let (trace, parent) = propagated();
    let exported = exported_by(|| {
        let request = info_span!("request");
        request.set_parent(parent).expect("set parent");
        let _request = request.entered();
        info!("in the request");
        info_span!("db.query").in_scope(|| info!("in the query"));
    });

    let (request, query) = (exported.span("request"), exported.span("db.query"));
    assert_eq!(span_trace(request).0, trace);
    assert_eq!(span_trace(query).0, trace);
    assert_eq!(
        log_trace(exported.log("in the request")),
        Some((trace, span_trace(request).1))
    );
    assert_eq!(
        log_trace(exported.log("in the query")),
        Some((trace, span_trace(query).1))
    );
}

#[test]
fn a_log_under_a_task_span_carries_its_message_spans_trace() {
    let (trace, parent) = propagated();
    let exported = exported_by(|| {
        let _pump = info_span!("task", task = "subscription").entered();
        info!("pumping");

        info_span!(parent: None, "tick").in_scope(|| {
            info!("in the tick");
            info_span!("db.query").in_scope(|| info!("in the tick's query"));
        });

        let published = info_span!(parent: None, "published");
        published.set_parent(parent).expect("set parent");
        published.in_scope(|| info!("in the published message"));
    });

    let span = |name| span_trace(exported.span(name));
    let (pump, tick, published) = (span("task"), span("tick"), span("published"));
    assert_eq!(log_trace(exported.log("pumping")), Some(pump));
    assert_ne!(tick.0, pump.0);
    assert_eq!(exported.span("tick").parent_span_id, SpanId::INVALID);
    assert_eq!(log_trace(exported.log("in the tick")), Some(tick));
    assert_eq!(
        log_trace(exported.log("in the tick's query")),
        Some((tick.0, span("db.query").1))
    );
    assert_eq!(published.0, trace);
    assert_eq!(
        log_trace(exported.log("in the published message")),
        Some(published)
    );
}

fn without_timestamp(line: &Value) -> Value {
    let mut line = line.clone();
    let timestamp = line
        .as_object_mut()
        .and_then(|keys| keys.remove("timestamp"));
    assert!(matches!(timestamp, Some(Value::String(_))), "{line}");
    line
}

fn without_scope(span: &Value) -> (Value, Vec<(String, String)>) {
    let (scope, own) = span
        .as_object()
        .expect("a span object")
        .clone()
        .into_iter()
        .partition::<serde_json::Map<_, _>, _>(|(key, _)| key.starts_with("cufflink."));
    (
        Value::Object(own),
        scope
            .into_iter()
            .map(|(key, value)| (key, value.as_str().expect("a string").to_owned()))
            .collect(),
    )
}

#[test]
fn stdout_lines_keep_their_top_level_keys_and_event_fields() {
    let request_span = json!({
        "name": "request",
        "otel.name": "GET acme/notes/books",
        "otel.kind": "server",
        "http.method": "GET",
        "url.path": "/svc/acme/notes/books",
        "request.id": "req-1",
    });
    let request_line = json!({
        "level": "INFO",
        "fields": {
            "message": "crud request",
            "tenant": "acme",
            "service": "notes",
            "table": "books",
            "operation": "list",
        },
        "target": "cufflink_platform::router::crud",
        "span": request_span,
        "spans": [request_span],
    });
    let subscription_span = json!({
        "name": "subscription",
        "otel.name": "acme/notes/on_book",
        "otel.kind": "consumer",
        "messaging.subject": "dw.acme.books.created",
    });
    let pump_line = json!({
        "level": "WARN",
        "fields": { "message": "Invalid JSON in event: EOF while parsing a value" },
        "target": "cufflink_platform::nats_manager::subscriptions",
        "span": subscription_span,
        "spans": [subscription_span],
    });
    let crud_request = || {
        info!(
            target: "cufflink_platform::router::crud",
            tenant = %"acme", service = %"notes", table = %"books", operation = "list",
            "crud request"
        )
    };
    let invalid_json = || {
        warn!(
            target: "cufflink_platform::nats_manager::subscriptions",
            "Invalid JSON in event: {}", "EOF while parsing a value"
        )
    };

    let unscoped = exported_by(|| {
        let request = info_span!(
            "request",
            otel.name = %"GET acme/notes/books",
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
            http.method = %"GET",
            http.response.status_code = tracing::field::Empty,
            url.path = %"/svc/acme/notes/books",
            request.id = tracing::field::Empty,
        );
        request.record("request.id", "req-1");
        request.in_scope(crud_request);

        let _pump = info_span!("task", task = "subscription").entered();
        info_span!(
            parent: None,
            "subscription",
            otel.name = %"acme/notes/on_book",
            otel.kind = "consumer",
            otel.status_code = tracing::field::Empty,
            messaging.subject = %"dw.acme.books.created",
        )
        .in_scope(invalid_json);
    });
    assert_eq!(
        without_timestamp(unscoped.line("crud request")),
        request_line
    );
    assert_eq!(
        without_timestamp(unscoped.line("Invalid JSON in event: EOF while parsing a value")),
        pump_line
    );

    let on_book = Scope {
        handler: Some("on_book".into()),
        kind: Some(Kind::Subscription),
        ..list_notes()
    };
    let scoped = exported_by(|| {
        let request = scopable_span!(
            "request",
            otel.name = %"GET acme/notes/books",
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
            http.method = %"GET",
            http.response.status_code = tracing::field::Empty,
            url.path = %"/svc/acme/notes/books",
            request.id = tracing::field::Empty,
        );
        request.record("request.id", "req-1");
        list_notes().record_on(&request);
        request.in_scope(crud_request);

        let _pump = info_span!("task", task = "subscription").entered();
        scoped_span!(
            parent: None,
            "subscription",
            on_book,
            otel.name = %"acme/notes/on_book",
            otel.kind = "consumer",
            otel.status_code = tracing::field::Empty,
            messaging.subject = %"dw.acme.books.created",
        )
        .in_scope(invalid_json);
    });
    let on_book_exported = pairs(&[
        (TENANT, "acme"),
        (SERVICE, "notes"),
        (VERSION, "v7"),
        (HANDLER, "on_book"),
        (KIND, "subscription"),
    ]);
    [
        ("crud request", request_line, list_notes_exported()),
        (
            "Invalid JSON in event: EOF while parsing a value",
            pump_line,
            on_book_exported,
        ),
    ]
    .into_iter()
    .for_each(|(message, golden, scope)| {
        let mut line = without_timestamp(scoped.line(message));
        let (span, gained) = without_scope(&line["span"]);
        assert_eq!(gained, scope, "{message}");
        assert_eq!(line["spans"], json!([line["span"]]), "{message}");
        line["span"] = span.clone();
        line["spans"] = json!([span]);
        assert_eq!(line, golden, "{message}");
    });
}
