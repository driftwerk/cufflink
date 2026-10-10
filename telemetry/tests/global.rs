mod support;

use cufflink_telemetry::scope::{HANDLER, KIND, SERVICE, TENANT, VERSION};
use cufflink_telemetry::{scoped_span, Kind, Scope};
use support::{log_scope, log_trace, pairs, span_scope, span_trace};
use tracing::{info, info_span};

struct Chatty;

impl std::fmt::Debug for Chatty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        info_span!(parent: None, "formatting").in_scope(|| info!("while formatting"));
        f.write_str("chatty")
    }
}

#[test]
fn a_log_carries_the_trace_and_span_of_its_span() {
    let (subscriber, sinks) = support::subscriber();
    tracing::subscriber::set_global_default(subscriber).expect("the only global default");
    let list_notes = Scope {
        service: Some("notes".into()),
        version: Some(7),
        handler: Some("list".into()),
        kind: Some(Kind::Route),
        ..Scope::tenant("acme")
    };

    info!("starting");
    scoped_span!("wasm_handler", list_notes).in_scope(|| {
        assert_eq!(Scope::current(), Some(list_notes.clone()));
        info!("in the handler");
        info_span!("db.query").in_scope(|| info!("in the query"));
        info!(value = ?Chatty, "after a nested event");
    });
    info!("stopping");

    let exported = sinks.exported();
    let scope = pairs(&[
        (TENANT, "acme"),
        (SERVICE, "notes"),
        (VERSION, "v7"),
        (HANDLER, "list"),
        (KIND, "route"),
    ]);
    let (handler, query) = (exported.span("wasm_handler"), exported.span("db.query"));
    assert_eq!(span_scope(query), scope);
    [
        ("in the handler", handler),
        ("in the query", query),
        ("after a nested event", handler),
    ]
    .into_iter()
    .for_each(|(message, span)| {
        let record = exported.log(message);
        assert_eq!(log_scope(record), scope, "{message}");
        assert_eq!(log_trace(record), Some(span_trace(span)), "{message}");
    });

    let nested = exported.logs("while formatting");
    assert!(!nested.is_empty());
    nested.into_iter().for_each(|record| {
        assert_eq!(log_scope(record), []);
        assert_ne!(log_trace(record).map(|(trace, _)| trace), None);
        assert_ne!(
            log_trace(record).map(|(trace, _)| trace),
            Some(span_trace(handler).0)
        );
    });

    ["starting", "stopping"].into_iter().for_each(|message| {
        let record = exported.log(message);
        assert_eq!(log_scope(record), [], "{message}");
        assert_eq!(log_trace(record), None, "{message}");
    });
}
