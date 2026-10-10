use crate::runner::Runner;
use cufflink_telemetry::{scoped_span, Kind};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::time::Instant;
use tracing::Span;

pub(crate) const MAX_LINE_BYTES: usize = 8 * 1024;
const LOG_LINES_PER_WINDOW: u32 = 100;
const LOG_WINDOW: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

#[derive(Debug)]
struct LineBudget {
    window_start: Instant,
    logged: u32,
    dropped: u64,
}

impl LineBudget {
    fn new(now: Instant) -> Self {
        Self {
            window_start: now,
            logged: 0,
            dropped: 0,
        }
    }

    fn window_end(&self) -> Instant {
        self.window_start + LOG_WINDOW
    }

    fn admit(&mut self, now: Instant) -> bool {
        if now >= self.window_end() {
            self.window_start = now;
            self.logged = 0;
        }
        let admitted = self.logged < LOG_LINES_PER_WINDOW;
        if admitted {
            self.logged += 1;
        } else {
            self.dropped += 1;
        }
        admitted
    }

    fn take_dropped(&mut self) -> u64 {
        std::mem::take(&mut self.dropped)
    }
}

struct ChildLog<R: Runner> {
    runner: Arc<R>,
    key: R::Key,
    info: R::Info,
    stream: Stream,
    budget: LineBudget,
    span: Option<Span>,
}

impl<R: Runner> ChildLog<R> {
    fn window_end(&self) -> Option<Instant> {
        self.span.as_ref().map(|_| self.budget.window_end())
    }

    fn end_window(&mut self) {
        let dropped = self.budget.take_dropped();
        if let Some(span) = self.span.take() {
            if dropped > 0 {
                span.in_scope(|| {
                    self.runner
                        .output_dropped(&self.key, &self.info, self.stream, dropped)
                });
            }
        }
    }

    fn line(&mut self, line: &[u8]) {
        let now = Instant::now();
        if now >= self.budget.window_end() {
            self.end_window();
        }
        if self.budget.admit(now) {
            let span = self.span.get_or_insert_with(|| {
                scoped_span!(
                    parent: None,
                    "child_output",
                    self.runner.scope(&self.key, &self.info).of_kind(Kind::Child),
                    stream = self.stream.as_str(),
                )
            });
            span.in_scope(|| {
                self.runner
                    .output_line(&self.key, &self.info, self.stream, line)
            });
        }
    }
}

async fn reached(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

pub(crate) async fn forward_output<R: Runner>(
    output: impl AsyncRead + Unpin,
    stream: Stream,
    runner: Arc<R>,
    key: R::Key,
    info: R::Info,
) {
    let mut log = ChildLog {
        runner,
        key,
        info,
        stream,
        budget: LineBudget::new(Instant::now()),
        span: None,
    };
    let mut reader = BufReader::new(output);
    let mut line: Vec<u8> = Vec::new();
    loop {
        let filled = tokio::select! {
            filled = reader.fill_buf() => filled,
            () = reached(log.window_end()) => {
                log.end_window();
                continue;
            }
        };
        let consumed = match filled {
            Ok([]) | Err(_) => break,
            Ok(buffer) => {
                let end = buffer.iter().position(|b| *b == b'\n');
                let chunk = &buffer[..end.unwrap_or(buffer.len())];
                let room = MAX_LINE_BYTES.saturating_sub(line.len());
                line.extend_from_slice(&chunk[..chunk.len().min(room)]);
                if end.is_some() {
                    log.line(&line);
                    line.clear();
                }
                end.map_or(buffer.len(), |end| end + 1)
            }
        };
        reader.consume(consumed);
    }
    if !line.is_empty() {
        log.line(&line);
    }
    log.end_window();
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::testing::{support, Probe};
    use opentelemetry::trace::SpanId;
    use std::process::Stdio;
    use std::sync::atomic::Ordering;
    use tokio::io::AsyncWriteExt;
    use tokio::process::Command;

    #[tokio::test]
    async fn a_chatty_child_never_blocks_on_its_output() {
        let mut child = Command::new("sh")
            .args([
                "-c",
                "head -c 200000 /dev/zero | tr '\\0' x; echo; i=0; while [ $i -lt 2000 ]; do echo line $i; i=$((i+1)); done",
            ])
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn sh");
        let stdout = child.stdout.take().expect("piped stdout");
        let (probe, _exits) = Probe::new();
        let forwarded = tokio::spawn(forward_output(
            stdout,
            Stream::Stdout,
            probe.clone(),
            "web",
            1,
        ));
        let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .expect("the child is never blocked")
            .expect("status");
        assert!(status.success());
        tokio::time::timeout(Duration::from_secs(5), forwarded)
            .await
            .expect("forwarding ends at EOF")
            .expect("join");
        assert_eq!(
            probe.lines.load(Ordering::SeqCst),
            u64::from(LOG_LINES_PER_WINDOW)
        );
        assert_eq!(
            probe.dropped.load(Ordering::SeqCst),
            2001 - u64::from(LOG_LINES_PER_WINDOW),
            "the lines over the budget are reported when the stream ends"
        );
        assert_eq!(probe.longest.load(Ordering::SeqCst), MAX_LINE_BYTES);
    }

    #[test]
    fn the_log_takes_a_budget_of_lines_per_window() {
        let start = Instant::now();
        let mut budget = LineBudget::new(start);
        let admitted = (0..LOG_LINES_PER_WINDOW + 5)
            .filter(|_| budget.admit(start))
            .count();
        assert_eq!(admitted as u32, LOG_LINES_PER_WINDOW);
        assert!(!budget.admit(start + LOG_WINDOW - Duration::from_nanos(1)));
        assert_eq!(budget.take_dropped(), 6);
        assert_eq!(budget.take_dropped(), 0, "told once");
        assert!(budget.admit(start + LOG_WINDOW), "a new window logs again");
        assert_eq!(
            budget.window_end(),
            start + 2 * LOG_WINDOW,
            "from its first line"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn each_window_of_output_is_a_span_of_its_own_exported_when_the_window_ends() {
        let (subscriber, sinks) = support::subscriber();
        let _subscribed = tracing::subscriber::set_default(subscriber);
        let (mut child, output) = tokio::io::duplex(64 * 1024);
        let (probe, _exits) = Probe::new();
        let forwarded = tokio::spawn(forward_output(
            output,
            Stream::Stderr,
            probe.clone(),
            "web",
            1,
        ));
        let over = 50;
        let first: String = (0..LOG_LINES_PER_WINDOW + over)
            .map(|line| format!("first {line}\n"))
            .collect();
        for (lines, quiet_for) in [
            (first.as_str(), LOG_WINDOW),
            ("second 0\nsecond 1\nsecond 2\n", 3 * LOG_WINDOW),
        ] {
            child.write_all(lines.as_bytes()).await.expect("written");
            tokio::time::sleep(quiet_for + Duration::from_millis(1)).await;
        }
        assert_eq!(
            (
                probe.lines.load(Ordering::SeqCst),
                probe.dropped.load(Ordering::SeqCst)
            ),
            (u64::from(LOG_LINES_PER_WINDOW) + 3, u64::from(over)),
            "what a window did not log is said when the window ends"
        );
        assert!(!forwarded.is_finished(), "the child still runs");

        let exported = sinks.exported();
        let windows: Vec<_> = exported
            .spans
            .iter()
            .filter(|span| span.name == "child_output")
            .map(|span| {
                assert_eq!(
                    support::span_scope(span),
                    support::pairs(&[
                        ("cufflink.kind", "child"),
                        ("cufflink.service", "web"),
                        ("cufflink.tenant", "acme"),
                        ("cufflink.version", "v1"),
                    ])
                );
                assert_eq!(span.parent_span_id, SpanId::INVALID, "a trace of its own");
                (
                    span.events.len(),
                    span.events
                        .iter()
                        .next()
                        .map(|event| event.name.to_string()),
                )
            })
            .collect();
        assert_eq!(
            windows,
            [
                (
                    LOG_LINES_PER_WINDOW as usize + 1,
                    Some("first 0".to_string())
                ),
                (3, Some("second 0".to_string())),
            ],
            "one span a window that had a line, holding no more than the window logged"
        );
        drop(child);
        forwarded.await.expect("forwarding ends at EOF");
    }
}
