#![allow(dead_code)]

use cufflink_telemetry::{layers, Component, Exporters, Providers};
use opentelemetry::logs::AnyValue;
use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{LogBatch, LogExporter, SdkLogRecord};
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use serde_json::Value;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tracing::Subscriber;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
struct Kept<T>(Arc<Mutex<Vec<T>>>);

impl<T> Clone for Kept<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: Clone> Kept<T> {
    fn new() -> Self {
        Self(Arc::default())
    }

    fn keep(&self, items: impl IntoIterator<Item = T>) {
        self.0.lock().expect("kept").extend(items);
    }

    fn all(&self) -> Vec<T> {
        self.0.lock().expect("kept").clone()
    }
}

impl SpanExporter for Kept<SpanData> {
    fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        self.keep(batch);
        std::future::ready(Ok(()))
    }
}

impl LogExporter for Kept<SdkLogRecord> {
    fn export(&self, batch: LogBatch<'_>) -> impl Future<Output = OTelSdkResult> + Send {
        self.keep(batch.iter().map(|(record, _)| record.clone()));
        std::future::ready(Ok(()))
    }
}

impl std::io::Write for Kept<u8> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.keep(bytes.iter().copied());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl MakeWriter<'_> for Kept<u8> {
    type Writer = Self;

    fn make_writer(&self) -> Self {
        self.clone()
    }
}

pub struct Sinks {
    providers: Providers,
    spans: Kept<SpanData>,
    logs: Kept<SdkLogRecord>,
    stdout: Kept<u8>,
}

pub fn subscriber() -> (impl Subscriber + Send + Sync + 'static, Sinks) {
    let (spans, logs, stdout) = (Kept::new(), Kept::new(), Kept::new());
    let (subscriber, providers) = layers(
        Component::Platform,
        EnvFilter::new("trace"),
        cufflink_telemetry::resource(Component::Platform, &[]),
        Exporters {
            stdout: stdout.clone(),
            spans: Some(spans.clone()),
            logs: Some(logs.clone()),
        },
    );
    (
        subscriber,
        Sinks {
            providers,
            spans,
            logs,
            stdout,
        },
    )
}

pub fn exported_by(run: impl FnOnce()) -> Exported {
    let (subscriber, sinks) = subscriber();
    tracing::subscriber::with_default(subscriber, run);
    sinks.exported()
}

impl Sinks {
    pub fn exported(self) -> Exported {
        self.providers
            .shutdown()
            .into_iter()
            .for_each(|(what, outcome)| {
                assert!(
                    matches!(outcome, Some(Ok(()))),
                    "{what} exporter: {outcome:?}"
                )
            });
        Exported {
            spans: self.spans.all(),
            logs: self.logs.all(),
            lines: String::from_utf8(self.stdout.all())
                .expect("stdout is UTF-8")
                .lines()
                .map(|line| serde_json::from_str(line).expect("a JSON line"))
                .collect(),
        }
    }
}

pub struct Exported {
    pub spans: Vec<SpanData>,
    pub logs: Vec<SdkLogRecord>,
    pub lines: Vec<Value>,
}

impl Exported {
    pub fn span(&self, name: &str) -> &SpanData {
        match self
            .spans
            .iter()
            .filter(|span| span.name == name)
            .collect::<Vec<_>>()[..]
        {
            [span] => span,
            ref found => panic!("{} spans named {name}", found.len()),
        }
    }

    pub fn logs(&self, message: &str) -> Vec<&SdkLogRecord> {
        self.logs
            .iter()
            .filter(|record| body(record) == message)
            .collect()
    }

    pub fn log(&self, message: &str) -> &SdkLogRecord {
        match self.logs(message)[..] {
            [record] => record,
            ref found => panic!("{} log records saying {message}", found.len()),
        }
    }

    pub fn line(&self, message: &str) -> &Value {
        self.lines
            .iter()
            .find(|line| line["fields"]["message"] == message)
            .unwrap_or_else(|| panic!("no stdout line saying {message}"))
    }
}

fn text(value: &AnyValue) -> String {
    match value {
        AnyValue::String(text) => text.as_str().to_owned(),
        other => format!("{other:?}"),
    }
}

fn body(record: &SdkLogRecord) -> String {
    record.body().map(text).unwrap_or_default()
}

fn sorted(mut scope: Vec<(String, String)>) -> Vec<(String, String)> {
    scope.sort();
    scope
}

pub fn log_scope(record: &SdkLogRecord) -> Vec<(String, String)> {
    sorted(
        record
            .attributes_iter()
            .filter(|(key, _)| key.as_str().starts_with("cufflink."))
            .map(|(key, value)| (key.as_str().to_owned(), text(value)))
            .collect(),
    )
}

pub fn log_attribute(record: &SdkLogRecord, name: &str) -> Option<String> {
    record
        .attributes_iter()
        .find(|(key, _)| key.as_str() == name)
        .map(|(_, value)| text(value))
}

pub fn span_scope(span: &SpanData) -> Vec<(String, String)> {
    sorted(
        span.attributes
            .iter()
            .filter(|attribute| attribute.key.as_str().starts_with("cufflink."))
            .map(|attribute| {
                (
                    attribute.key.as_str().to_owned(),
                    attribute.value.as_str().into_owned(),
                )
            })
            .collect(),
    )
}

pub fn log_trace(record: &SdkLogRecord) -> Option<(TraceId, SpanId)> {
    record
        .trace_context()
        .map(|context| (context.trace_id, context.span_id))
}

pub fn span_trace(span: &SpanData) -> (TraceId, SpanId) {
    (span.span_context.trace_id(), span.span_context.span_id())
}

pub fn pairs(scope: &[(&str, &str)]) -> Vec<(String, String)> {
    sorted(
        scope
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect(),
    )
}
