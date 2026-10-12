use super::NAMES;
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{Span, SpanData, SpanProcessor};
use opentelemetry_sdk::Resource;
use std::time::Duration;

#[derive(Debug)]
pub struct ScopeSpanProcessor<P> {
    inner: P,
}

impl<P> ScopeSpanProcessor<P> {
    pub fn new(inner: P) -> Self {
        Self { inner }
    }
}

impl<P: SpanProcessor> SpanProcessor for ScopeSpanProcessor<P> {
    fn on_start(&self, span: &mut Span, cx: &Context) {
        self.inner.on_start(span, cx);
    }

    fn on_end(&self, mut span: SpanData) {
        last_scope_wins(&mut span.attributes);
        self.inner.on_end(span);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn shutdown(&self) -> OTelSdkResult {
        self.inner.shutdown()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

fn last_scope_wins(attributes: &mut Vec<KeyValue>) {
    let mut seen = [false; NAMES.len()];
    let mut kept = Vec::with_capacity(attributes.len());
    for attribute in attributes.drain(..).rev() {
        if let Some(at) = NAMES
            .iter()
            .position(|name| *name == attribute.key.as_str())
        {
            if std::mem::replace(&mut seen[at], true) {
                continue;
            }
        }
        kept.push(attribute);
    }
    kept.reverse();
    *attributes = kept;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scope::{HANDLER, TENANT};

    #[test]
    fn a_scope_key_recorded_twice_keeps_its_last_value_in_place() {
        let mut attributes = vec![
            KeyValue::new(TENANT, "acme"),
            KeyValue::new(HANDLER, "before_create"),
            KeyValue::new("rows", 3),
            KeyValue::new(HANDLER, "after_create"),
            KeyValue::new("rows", 4),
        ];
        last_scope_wins(&mut attributes);
        assert_eq!(
            attributes
                .iter()
                .map(|attribute| (attribute.key.as_str(), attribute.value.to_string()))
                .collect::<Vec<_>>(),
            [
                (TENANT, "acme".to_owned()),
                ("rows", "3".to_owned()),
                (HANDLER, "after_create".to_owned()),
                ("rows", "4".to_owned()),
            ]
        );
    }
}
