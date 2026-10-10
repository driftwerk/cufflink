use super::Fields;
use opentelemetry::logs::{AnyValue, LogRecord};
use opentelemetry::trace::{SpanBuilder, SpanId, TraceContextExt, TraceId};
use opentelemetry::{KeyValue, StringValue};
use opentelemetry_sdk::logs::SdkLogRecord;
use std::cell::RefCell;
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_log::NormalizeEvent;
use tracing_opentelemetry::OtelData;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{LookupSpan, SpanRef};
use tracing_subscriber::Layer;

pub struct ScopeLayer<B> {
    bridge: Option<B>,
}

impl<B> ScopeLayer<B> {
    pub fn new(bridge: Option<B>) -> Self {
        Self { bridge }
    }
}

impl<S, B> Layer<S> for ScopeLayer<B>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
    B: Layer<S>,
{
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
        let (added, effective) = match declared.is_empty() {
            true => (None, inherited),
            false => (Some(inherited.except(&declared)), declared.or(&inherited)),
        };
        export(&span, |builder| {
            builder.attributes.get_or_insert_default().extend(
                added
                    .as_ref()
                    .unwrap_or(&effective)
                    .attributes()
                    .map(key_value),
            )
        });
        span.extensions_mut().insert(effective);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let declared = Fields::declared(|visitor| values.record(visitor));
        let Some(span) = ctx.span(id).filter(|_| !declared.is_empty()) else {
            return;
        };
        export(&span, |builder| {
            let attributes = builder.attributes.get_or_insert_default();
            attributes.retain(|attribute| !declared.has(attribute.key.as_str()));
            attributes.extend(declared.attributes().map(key_value));
        });
        let mut extensions = span.extensions_mut();
        let effective = declared.or(&extensions.remove::<Fields>().unwrap_or_default());
        extensions.insert(effective);
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let Some(bridge) = self.bridge.as_ref().filter(|_| !is_an_exports_own(event)) else {
            return;
        };
        let _restore = EventScope::enter(ctx.event_span(event).map(|span| {
            let extensions = span.extensions();
            EventScope {
                fields: extensions.get::<Fields>().cloned().unwrap_or_default(),
                trace: extensions.get::<OtelData>().and_then(trace_context),
            }
        }));
        bridge.on_event(event, ctx);
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

fn export<'a, S>(span: &SpanRef<'a, S>, write: impl FnOnce(&mut SpanBuilder))
where
    S: LookupSpan<'a>,
{
    if let Some(OtelData {
        builder,
        parent_cx: _,
    }) = span.extensions_mut().get_mut::<OtelData>()
    {
        write(builder);
    }
}

fn trace_context(data: &OtelData) -> Option<(TraceId, SpanId)> {
    let OtelData { builder, parent_cx } = data;
    match parent_cx.has_active_span() {
        true => Some(parent_cx.span().span_context().trace_id()),
        false => builder.trace_id,
    }
    .zip(builder.span_id)
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
