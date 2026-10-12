use super::Fields;
use opentelemetry::logs::{AnyValue, LogRecord};
use opentelemetry::trace::{SpanId, TraceContextExt, TraceId};
use opentelemetry::{KeyValue, StringValue};
use opentelemetry_sdk::logs::SdkLogRecord;
use std::cell::RefCell;
use std::sync::OnceLock;
use tracing::dispatcher::WeakDispatch;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Dispatch, Event, Subscriber};
use tracing_log::NormalizeEvent;
use tracing_opentelemetry::get_otel_context;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

pub struct ScopeLayer<B> {
    bridge: Option<B>,
    dispatch: OnceLock<WeakDispatch>,
}

impl<B> ScopeLayer<B> {
    pub fn new(bridge: Option<B>) -> Self {
        Self {
            bridge,
            dispatch: OnceLock::new(),
        }
    }

    fn otel_context(&self, id: &Id) -> Option<opentelemetry::Context> {
        let dispatch = self.dispatch.get()?.upgrade()?;
        get_otel_context(id, &dispatch)
    }
}

impl<S, B> Layer<S> for ScopeLayer<B>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
    B: Layer<S>,
{
    fn on_register_dispatch(&self, subscriber: &Dispatch) {
        let _ = self.dispatch.set(subscriber.downgrade());
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let inherited = span
            .parent()
            .and_then(|parent| parent.extensions().get::<Fields>().cloned())
            .unwrap_or_default();
        let declared = Fields::declared(|visitor| attrs.record(visitor));
        if inherited.is_empty() && declared.is_empty() {
            return;
        }
        span.extensions_mut().insert(declared.or(&inherited));
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let declared = Fields::declared(|visitor| values.record(visitor));
        let mut renamed = Renamed(None);
        values.record(&mut renamed);
        let Some(span) = ctx.span(id) else {
            return;
        };
        if !declared.is_empty() {
            let mut extensions = span.extensions_mut();
            let effective = declared.or(&extensions.remove::<Fields>().unwrap_or_default());
            extensions.insert(effective);
        }
        if let Some(name) = renamed.0 {
            if let Some(cx) = self.otel_context(id) {
                cx.span().update_name(name);
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let Some(bridge) = self.bridge.as_ref().filter(|_| !is_an_exports_own(event)) else {
            return;
        };
        let _restore = EventScope::enter(ctx.event_span(event).map(|span| {
            let fields = span
                .extensions()
                .get::<Fields>()
                .cloned()
                .unwrap_or_default();
            EventScope {
                fields,
                trace: self.otel_context(&span.id()).and_then(trace_context),
            }
        }));
        bridge.on_event(event, ctx);
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let fields = ctx
            .span(&id)
            .and_then(|span| span.extensions().get::<Fields>().cloned())
            .filter(|fields| !fields.is_empty());
        if let Some((fields, cx)) = fields.zip(self.otel_context(&id)) {
            cx.span().set_attributes(fields.attributes().map(key_value));
        }
    }
}

const EXPORT_PATH: [&str; 6] = ["h2", "hyper", "hyper_util", "reqwest", "rustls", "tonic"];

fn is_within(target: &str, krate: &str) -> bool {
    target
        .strip_prefix(krate)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with("::"))
}

fn is_an_exporters(target: &str) -> bool {
    target.starts_with("opentelemetry") || EXPORT_PATH.iter().any(|krate| is_within(target, krate))
}

fn is_an_exports_own(event: &Event<'_>) -> bool {
    event.normalized_metadata().map_or_else(
        || is_an_exporters(event.metadata().target()),
        |metadata| is_an_exporters(metadata.target()),
    )
}

fn key_value((name, value): (&'static str, StringValue)) -> KeyValue {
    KeyValue::new(name, value)
}

fn trace_context(cx: opentelemetry::Context) -> Option<(TraceId, SpanId)> {
    let span = cx.span();
    let context = span.span_context();
    context
        .is_valid()
        .then(|| (context.trace_id(), context.span_id()))
}

struct Renamed(Option<String>);

impl Visit for Renamed {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "otel.name" {
            self.0 = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "otel.name" {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

struct EventScope {
    fields: Fields,
    trace: Option<(TraceId, SpanId)>,
}

thread_local! {
    static EVENT: RefCell<Option<EventScope>> = const { RefCell::new(None) };
}

struct Restore(Option<EventScope>);

impl EventScope {
    fn enter(event: Option<Self>) -> Restore {
        Restore(
            EVENT
                .try_with(|current| current.replace(event))
                .ok()
                .flatten(),
        )
    }
}

impl Drop for Restore {
    fn drop(&mut self) {
        let previous = self.0.take();
        let _ = EVENT.try_with(|current| current.replace(previous));
    }
}

pub(super) fn stamp(record: &mut SdkLogRecord) {
    let _ = EVENT.try_with(|current| {
        if let Some(EventScope { fields, trace }) = &*current.borrow() {
            fields.attributes().for_each(|(name, value)| {
                if !record
                    .attributes_iter()
                    .any(|(key, _)| key.as_str() == name)
                {
                    record.add_attribute(name, AnyValue::String(value));
                }
            });
            if let Some((trace_id, span_id)) = *trace {
                record.set_trace_context(trace_id, span_id, None);
            }
        }
    });
}
