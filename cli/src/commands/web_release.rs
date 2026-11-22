use crate::commands::version::PlatformVersion;
use crate::config::CliConfig;
use cufflink_types::features::{WEB_BUNDLE_PARTS, WEB_RELEASES};
use futures_util::{StreamExt, TryStreamExt};
use reqwest::{Method, StatusCode};
use serde_json::{json, Value};
use std::future::Future;
use std::time::{Duration, Instant};

fn unrouted(status: StatusCode) -> &'static str {
    match status {
        StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED => {
            " (a platform replica without this route answers so: \
             is a platform upgrade in progress?)"
        }
        _ => "",
    }
}

pub(crate) fn require_web_releases(platform: &PlatformVersion) -> eyre::Result<()> {
    if platform.offers(WEB_RELEASES) {
        Ok(())
    } else {
        eyre::bail!(
            "platform {} cannot release a web app with its bundle (needs {WEB_RELEASES}). \
             Upgrade the platform, or deploy with a CLI older than v0.38.0",
            platform.label()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Bundle {
    sha256: String,
    bytes: bytes::Bytes,
}

impl Bundle {
    pub(crate) fn of(bytes: Vec<u8>) -> Self {
        Self {
            sha256: crate::digest::sha256_hex(&bytes),
            bytes: bytes.into(),
        }
    }

    fn size(&self) -> usize {
        self.bytes.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Parts {
    threshold: usize,
    size: usize,
    in_flight: usize,
    attempts: u32,
    backoff: Duration,
    assembly_timeout: Duration,
}

const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

impl Default for Parts {
    fn default() -> Self {
        Self {
            threshold: 32 * 1024 * 1024,
            size: 8 * 1024 * 1024,
            in_flight: 4,
            attempts: 3,
            backoff: Duration::from_secs(2),
            assembly_timeout: Duration::from_secs(600),
        }
    }
}

struct Failed {
    status: Option<StatusCode>,
    retry_after: Option<Duration>,
    body: String,
    report: eyre::Report,
}

impl Failed {
    fn transient(&self) -> bool {
        self.status.is_none_or(transient)
    }

    fn reason(&self) -> String {
        self.status
            .map_or_else(|| "no answer".to_string(), |status| status.to_string())
    }
}

fn transient(status: StatusCode) -> bool {
    match status {
        StatusCode::NOT_IMPLEMENTED | StatusCode::HTTP_VERSION_NOT_SUPPORTED => false,
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS => true,
        status => status.is_server_error(),
    }
}

fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
        .map(Duration::from_secs)
}

fn distinct(parts: &[Bundle]) -> Vec<(usize, &Bundle)> {
    let mut seen = std::collections::HashSet::new();
    parts
        .iter()
        .enumerate()
        .filter(|(_, part)| seen.insert(part.sha256.as_str()))
        .collect()
}

fn carried(parts: &[(usize, &Bundle)]) -> usize {
    parts.iter().map(|(_, part)| part.size()).sum()
}

async fn timed<T>(work: impl Future<Output = T>) -> (T, Duration) {
    let started = Instant::now();
    let done = work.await;
    (done, started.elapsed())
}

fn megabytes(bytes: usize) -> f64 {
    bytes as f64 / 1_048_576.0
}

fn seconds(took: Duration) -> String {
    format!("{:.1} s", took.as_secs_f64())
}

fn rate(bytes: usize, took: Duration) -> String {
    if took.is_zero() {
        String::new()
    } else {
        format!(" ({:.1} MB/s)", megabytes(bytes) / took.as_secs_f64())
    }
}

fn answered_line(
    label: &str,
    carrying: Option<usize>,
    took: Duration,
    (attempt, attempts): (u32, u32),
) -> String {
    let answered = match carrying {
        Some(bytes) => format!("{bytes} bytes in {}{}", seconds(took), rate(bytes, took)),
        None => format!("answered in {}", seconds(took)),
    };
    let retried = if attempt > 1 {
        format!(", on attempt {attempt} of {attempts}")
    } else {
        String::new()
    };
    format!("  {label}: {answered}{retried}")
}

fn failed_line(label: &str, reason: &str, took: Duration, again: Option<(u32, u32)>) -> String {
    let again = again.map_or_else(String::new, |(attempt, attempts)| {
        format!("; trying again ({attempt} of {attempts})")
    });
    format!("  {label}: {reason} after {}{again}", seconds(took))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Sent {
    bytes: usize,
    phases: Option<Phases>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Phases {
    parts: Duration,
    assembly: Duration,
}

fn sent_line(sent: Sent, total: Duration) -> String {
    let phases = sent.phases.map_or_else(String::new, |phases| {
        format!(
            ": parts {}, assembly {}",
            seconds(phases.parts),
            seconds(phases.assembly)
        )
    });
    format!(
        "  Sent {:.1} MB in {}{}{phases}",
        megabytes(sent.bytes),
        seconds(total),
        rate(sent.bytes, total)
    )
}

struct Asking<'a> {
    doing: &'a str,
    label: &'a str,
    carrying: Option<usize>,
}

impl Parts {
    pub(crate) fn offered(platform: &PlatformVersion) -> Option<Self> {
        platform.offers(WEB_BUNDLE_PARTS).then(Self::default)
    }

    fn of(self, bundle: &Bundle) -> Option<Vec<Bundle>> {
        (bundle.size() > self.threshold).then(|| {
            (0..bundle.size())
                .step_by(self.size)
                .map(|start| {
                    let part = bundle
                        .bytes
                        .slice(start..(start + self.size).min(bundle.size()));
                    Bundle {
                        sha256: crate::digest::sha256_hex(&part),
                        bytes: part,
                    }
                })
                .collect()
        })
    }
}

fn short(sha256: &str) -> &str {
    &sha256[..12.min(sha256.len())]
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Released {
    version: i64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reserved {
    service_id: String,
    running: Option<Released>,
}

impl Reserved {
    fn of(service_id: String, status: &Value) -> Self {
        let running = status["artifact_hash"]
            .as_str()
            .zip(status["version"].as_i64())
            .map(|(sha256, version)| Released {
                version,
                sha256: sha256.to_string(),
            });
        Self {
            service_id,
            running,
        }
    }

    fn unreleased(&self, name: &str) -> String {
        match &self.running {
            Some(Released { version, sha256 }) => format!(
                "nothing was released; {name} still runs v{version} (bundle {})",
                short(sha256)
            ),
            None => "nothing was released".to_string(),
        }
    }

    fn runs(&self, bundle: &Bundle) -> Option<i64> {
        self.running
            .as_ref()
            .filter(|running| running.sha256 == bundle.sha256)
            .map(|running| running.version)
    }
}

pub(crate) struct WebApp<'a> {
    pub(crate) config: &'a CliConfig,
    pub(crate) client: &'a reqwest::Client,
    pub(crate) name: &'a str,
    pub(crate) parts: Option<Parts>,
    pub(crate) out: &'a (dyn Fn(String) + Sync),
}

enum Release {
    Answered(Value),
    NotClaimed(String),
}

impl WebApp<'_> {
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.config.api_url)
    }

    fn say(&self, line: String) {
        (self.out)(line)
    }

    async fn exchanged(
        &self,
        request: reqwest::RequestBuilder,
        unanswered: impl FnOnce() -> String,
    ) -> eyre::Result<(StatusCode, Option<Duration>, Result<Value, String>)> {
        let answered = async {
            let answer = request.send().await?;
            let status = answer.status();
            let after = retry_after(answer.headers());
            if status.is_success() {
                Ok((status, after, Ok(answer.json().await?)))
            } else {
                Ok((status, after, Err(answer.text().await.unwrap_or_default())))
            }
        }
        .await;
        answered.map_err(|lost: reqwest::Error| eyre::Report::new(lost).wrap_err(unanswered()))
    }

    async fn sent(
        &self,
        request: reqwest::RequestBuilder,
        unanswered: impl FnOnce() -> String,
    ) -> eyre::Result<(StatusCode, Result<Value, String>)> {
        self.exchanged(request, unanswered)
            .await
            .map(|(status, _, answer)| (status, answer))
    }

    async fn reserve(&self) -> eyre::Result<Reserved> {
        let request = self
            .config
            .auth_request(self.client, Method::POST, &self.url("/api/services/web"))
            .json(&json!({ "name": self.name }));
        let reserved = match self
            .sent(request, || {
                format!(
                    "Reserving {} got no answer; nothing was released",
                    self.name
                )
            })
            .await?
        {
            (_, Ok(body)) => body,
            (status, Err(body)) => eyre::bail!(
                "Reserving {} failed ({status}): {body}{}; nothing was released",
                self.name,
                unrouted(status)
            ),
        };
        let service_id = reserved["service_id"]
            .as_str()
            .ok_or_else(|| {
                eyre::eyre!(
                    "No service_id in the platform's reservation of {}",
                    self.name
                )
            })?
            .to_string();
        let request = self.config.auth_request(
            self.client,
            Method::GET,
            &self.url(&format!("/api/services/{service_id}/web/status")),
        );
        let unanswered = || {
            format!(
                "Reading what {} runs got no answer; nothing was released",
                self.name
            )
        };
        match self.sent(request, unanswered).await? {
            (_, Ok(status)) => Ok(Reserved::of(service_id, &status)),
            (status, Err(body)) => eyre::bail!(
                "Reading what {} runs failed ({status}): {body}; nothing was released",
                self.name
            ),
        }
    }

    async fn answered(
        &self,
        request: reqwest::RequestBuilder,
        doing: &str,
        reserved: &Reserved,
    ) -> Result<Value, Failed> {
        let unreleased = reserved.unreleased(self.name);
        match self
            .exchanged(request, || format!("{doing} got no answer; {unreleased}"))
            .await
        {
            Ok((_, _, Ok(body))) => Ok(body),
            Ok((status, retry_after, Err(body))) => Err(Failed {
                status: Some(status),
                retry_after,
                report: eyre::eyre!(
                    "{doing} failed ({status}): {body}{}; {unreleased}",
                    unrouted(status)
                ),
                body,
            }),
            Err(report) => Err(Failed {
                status: None,
                retry_after: None,
                body: String::new(),
                report,
            }),
        }
    }

    async fn persisted(
        &self,
        parts: Parts,
        request: impl Fn() -> reqwest::RequestBuilder,
        asking: Asking<'_>,
        reserved: &Reserved,
    ) -> Result<Value, Failed> {
        let mut attempt = 1;
        loop {
            match timed(self.answered(request(), asking.doing, reserved)).await {
                (Err(failed), took) if failed.transient() && attempt < parts.attempts => {
                    attempt += 1;
                    self.say(failed_line(
                        asking.label,
                        &failed.reason(),
                        took,
                        Some((attempt, parts.attempts)),
                    ));
                    let backoff = parts.backoff * (attempt - 1);
                    tokio::time::sleep(
                        failed
                            .retry_after
                            .map_or(backoff, |after| after.min(MAX_RETRY_AFTER).max(backoff)),
                    )
                    .await;
                }
                (answered, took) => {
                    self.say(match &answered {
                        Ok(_) => answered_line(
                            asking.label,
                            asking.carrying,
                            took,
                            (attempt, parts.attempts),
                        ),
                        Err(failed) => failed_line(asking.label, &failed.reason(), took, None),
                    });
                    return answered;
                }
            }
        }
    }

    fn whole_request(&self, reserved: &Reserved, bundle: &Bundle) -> reqwest::RequestBuilder {
        self.config
            .auth_request(
                self.client,
                Method::PUT,
                &self.url(&format!(
                    "/api/services/{}/web/artifacts/{}",
                    reserved.service_id, bundle.sha256
                )),
            )
            .header("Content-Type", "application/gzip")
            .body(bundle.bytes.clone())
    }

    fn part_request(
        &self,
        reserved: &Reserved,
        bundle: &Bundle,
        (position, part): (usize, &Bundle),
    ) -> reqwest::RequestBuilder {
        self.config
            .auth_request(
                self.client,
                Method::PUT,
                &self.url(&format!(
                    "/api/services/{}/web/parts/{}?bundle={}&index={}",
                    reserved.service_id,
                    part.sha256,
                    bundle.sha256,
                    position + 1
                )),
            )
            .header("Content-Type", "application/octet-stream")
            .body(part.bytes.clone())
    }

    fn assembly_request(
        &self,
        reserved: &Reserved,
        bundle: &Bundle,
        split: &[Bundle],
        parts: Parts,
    ) -> reqwest::RequestBuilder {
        self.config
            .auth_request(
                self.client,
                Method::POST,
                &self.url(&format!(
                    "/api/services/{}/web/artifacts/{}/parts",
                    reserved.service_id, bundle.sha256
                )),
            )
            .timeout(parts.assembly_timeout)
            .json(&json!({
                "size": bundle.size(),
                "parts": split.iter().map(|part| &part.sha256).collect::<Vec<_>>(),
            }))
    }

    async fn put_parts(
        &self,
        reserved: &Reserved,
        bundle: &Bundle,
        (parts, of): (Parts, usize),
        sent: &[(usize, &Bundle)],
    ) -> eyre::Result<()> {
        futures_util::stream::iter(sent)
            .map(|&(position, part)| async move {
                let place = format!("part {} of {of} of bundle", position + 1);
                self.persisted(
                    parts,
                    || self.part_request(reserved, bundle, (position, part)),
                    Asking {
                        doing: &format!("Uploading {place} {}", bundle.sha256),
                        label: &format!("Uploading {place} {}", short(&bundle.sha256)),
                        carrying: Some(part.size()),
                    },
                    reserved,
                )
                .await
                .map(drop)
                .map_err(|failed| failed.report)
            })
            .buffer_unordered(parts.in_flight)
            .try_collect()
            .await
    }

    async fn assembled(
        &self,
        reserved: &Reserved,
        bundle: &Bundle,
        parts: Parts,
        split: &[Bundle],
    ) -> eyre::Result<(Value, Option<Sent>)> {
        let (doing, label) = (
            format!("Assembling bundle {}", bundle.sha256),
            format!("Assembling bundle {}", short(&bundle.sha256)),
        );
        let assemble = || {
            timed(self.persisted(
                parts,
                || self.assembly_request(reserved, bundle, split, parts),
                Asking {
                    doing: &doing,
                    label: &label,
                    carrying: None,
                },
                reserved,
            ))
        };
        let (asked, mut assembly) = assemble().await;
        match asked {
            Err(failed) if failed.status == Some(StatusCode::CONFLICT) => {}
            answered => {
                return answered
                    .map(|answer| (answer, None))
                    .map_err(|failed| failed.report)
            }
        }
        let unique = distinct(split);
        self.say(format!("  In {} parts", unique.len()));
        let (put_all, mut parts_took) =
            timed(self.put_parts(reserved, bundle, (parts, split.len()), &unique)).await;
        put_all?;
        let (asked, took) = assemble().await;
        assembly += took;
        let (answer, bytes) = match asked {
            Err(failed) if failed.status == Some(StatusCode::CONFLICT) => {
                let missing = unique
                    .iter()
                    .copied()
                    .filter(|(_, part)| failed.body.contains(&part.sha256))
                    .collect::<Vec<_>>();
                if missing.is_empty() {
                    return Err(failed.report);
                }
                self.say(format!(
                    "  The platform no longer stores {} of them; uploading again",
                    missing.len()
                ));
                let (put_missing, took) =
                    timed(self.put_parts(reserved, bundle, (parts, split.len()), &missing)).await;
                parts_took += took;
                put_missing?;
                let (asked, took) = assemble().await;
                assembly += took;
                (
                    asked.map_err(|failed| failed.report)?,
                    carried(&unique) + carried(&missing),
                )
            }
            answered => (answered.map_err(|failed| failed.report)?, carried(&unique)),
        };
        Ok((
            answer,
            Some(Sent {
                bytes,
                phases: Some(Phases {
                    parts: parts_took,
                    assembly,
                }),
            }),
        ))
    }

    async fn upload(&self, reserved: &Reserved, bundle: &Bundle) -> eyre::Result<()> {
        self.say(format!("Uploading bundle {}...", short(&bundle.sha256)));
        let started = Instant::now();
        let split = self
            .parts
            .and_then(|parts| parts.of(bundle).map(|split| (parts, split)));
        let (answer, sent) = match split {
            Some((parts, split)) => self.assembled(reserved, bundle, parts, &split).await?,
            None => (
                self.answered(
                    self.whole_request(reserved, bundle),
                    &format!("Uploading bundle {}", bundle.sha256),
                    reserved,
                )
                .await
                .map_err(|failed| failed.report)?,
                Some(Sent {
                    bytes: bundle.size(),
                    phases: None,
                }),
            ),
        };
        self.say(
            if answer["stored"].as_str() == Some("present") {
                "  Already stored"
            } else {
                "  Stored"
            }
            .to_string(),
        );
        if let Some(sent) = sent {
            self.say(sent_line(sent, started.elapsed()));
        }
        Ok(())
    }

    fn unsure(&self) -> String {
        format!(
            "{name} may or may not have been released; check cufflink deployments list for \
             {name}, or deploy again, which releases the bundle or finds it running",
            name = self.name
        )
    }

    async fn release(&self, reserved: &Reserved, request: &Value) -> eyre::Result<Release> {
        let sent =
            self.config
                .auth_request(self.client, Method::POST, &self.url("/api/services/deploy"));
        let answered = self
            .sent(sent.json(request), || {
                format!("the release got no answer; {}", self.unsure())
            })
            .await?;
        match answered {
            (_, Ok(body)) => Ok(Release::Answered(body)),
            (StatusCode::CONFLICT, Err(body)) => Ok(Release::NotClaimed(body)),
            (StatusCode::UNPROCESSABLE_ENTITY, Err(body))
                if body.contains("unknown field `web_artifact`") =>
            {
                eyre::bail!(
                    "an older platform replica refused the release; {}. Is a platform \
                     upgrade in progress? Deploy again once it has finished",
                    reserved.unreleased(self.name)
                )
            }
            (status, Err(body)) if status.is_client_error() => eyre::bail!(
                "Releasing {} failed ({status}): {body}; {}",
                self.name,
                reserved.unreleased(self.name)
            ),
            (status, Err(body)) => eyre::bail!(
                "Releasing {} failed ({status}): {body}; {}",
                self.name,
                self.unsure()
            ),
        }
    }

    pub(crate) async fn deploy(
        &self,
        request: Value,
        build: impl AsyncFnOnce(&str) -> eyre::Result<Vec<u8>>,
    ) -> eyre::Result<()> {
        let reserved = self.reserve().await?;
        let bundle = build(&reserved.service_id)
            .await
            .map(Bundle::of)
            .map_err(|failed| {
                failed.wrap_err(format!(
                    "the build failed; {}",
                    reserved.unreleased(self.name)
                ))
            })?;
        self.say(format!(
            "  Bundle {}: {} bytes ({:.1} MB)",
            short(&bundle.sha256),
            bundle.size(),
            megabytes(bundle.size())
        ));
        match reserved.runs(&bundle) {
            Some(version) => self.say(format!(
                "  {} v{version} already runs this bundle; not uploading it again",
                self.name
            )),
            None => self.upload(&reserved, &bundle).await?,
        }

        let request = released_with(request, &bundle);
        self.say(format!("Releasing {}...", self.name));
        let answer = match self.release(&reserved, &request).await? {
            Release::Answered(answer) => answer,
            Release::NotClaimed(refusal) => {
                self.say(format!(
                    "  The platform refused the release ({refusal}); uploading again"
                ));
                self.upload(&reserved, &bundle).await?;
                match self.release(&reserved, &request).await? {
                    Release::Answered(answer) => answer,
                    Release::NotClaimed(refusal) => eyre::bail!(
                        "Releasing {} failed (409 Conflict): {refusal}; {}",
                        self.name,
                        reserved.unreleased(self.name)
                    ),
                }
            }
        };
        reported(self.name, &bundle, &answer)?
            .into_iter()
            .for_each(|line| self.say(line));
        Ok(())
    }
}

fn released_with(mut request: Value, bundle: &Bundle) -> Value {
    request["web_artifact"] = json!({ "sha256": bundle.sha256, "size": bundle.size() });
    request
}

fn reported(name: &str, bundle: &Bundle, answer: &Value) -> eyre::Result<Vec<String>> {
    let released = answer["web_artifact"]["sha256"].as_str().ok_or_else(|| {
        eyre::eyre!(
            "the platform answered without the bundle it released; \
             check cufflink deployments list for {name}"
        )
    })?;
    if released != bundle.sha256 {
        eyre::bail!(
            "the platform's current version of {name} records bundle {released}, not \
             {}, which this deploy built and uploaded; check cufflink deployments list \
             for {name}",
            bundle.sha256
        );
    }
    let version = &answer["version"];
    let headline = if answer["skipped"].as_bool() == Some(true) {
        format!(
            "{name} v{version} already runs bundle {}; nothing to release",
            short(released)
        )
    } else {
        format!("Released {name} v{version} (bundle {})", short(released))
    };
    let details = [
        ("Deployment ID", answer["deployment_id"].as_str()),
        ("Tenant", answer["tenant_slug"].as_str()),
        ("Commit", answer["commit_hash"].as_str()),
    ]
    .into_iter()
    .filter_map(|(label, value)| {
        value.map(|value| format!("  {:<15}{value}", format!("{label}:")))
    });
    let warnings = answer["warnings"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(|warning| format!("warning: {warning}"));
    let untold = (answer["runners_told"].as_bool() == Some(false)).then(|| {
        format!(
            "warning: the web runners were not told of v{version}; each picks it up at its \
             next reconcile (WEB_RECONCILE_SECS, 30 s by default)"
        )
    });
    Ok(std::iter::once(headline)
        .chain(details)
        .chain(warnings)
        .chain(untold)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::stub::{answering, Asked};

    const BUNDLE: &[u8] = b"a gzip tarball of site";

    fn sha() -> String {
        crate::digest::sha256_hex(BUNDLE)
    }

    fn reserved() -> (u16, Value) {
        (
            200,
            json!({"service_id": "7", "name": "site", "current_version": 1, "created": true}),
        )
    }

    fn running(version: i64, sha256: Option<&str>) -> (u16, Value) {
        (
            200,
            json!({"service_id": "7", "version": version, "artifact_hash": sha256}),
        )
    }

    fn stored(stored: &str) -> (u16, Value) {
        (
            200,
            json!({"sha256": sha(), "size": BUNDLE.len(), "stored": stored}),
        )
    }

    fn released(version: i64, skipped: bool) -> (u16, Value) {
        (
            200,
            json!({"service_id": "7", "name": "site", "version": version, "skipped": skipped,
                   "deployment_id": "d", "tenant_slug": "acme",
                   "web_artifact": {"sha256": sha(), "size": BUNDLE.len()},
                   "runners_told": true}),
        )
    }

    fn refused(status: u16, error: &str) -> (u16, Value) {
        (status, json!({ "error": error }))
    }

    fn request() -> Value {
        json!({"manifest": {"name": "site", "mode": "web", "version": "0.1.0", "tables": []},
               "commit_hash": "4f2a9c1"})
    }

    fn upload() -> String {
        format!("PUT /api/services/7/web/artifacts/{}", sha())
    }

    const RESERVE: &str = "POST /api/services/web";
    const STATUS: &str = "GET /api/services/7/web/status";
    const RELEASE: &str = "POST /api/services/deploy";

    async fn deployed(
        answers: Vec<(u16, Value)>,
        build: eyre::Result<Vec<u8>>,
    ) -> (eyre::Result<()>, Asked, Option<(String, usize)>) {
        deployed_or_lost(answers.into_iter().map(Some).collect(), build).await
    }

    async fn deployed_or_lost(
        answers: Vec<Option<(u16, Value)>>,
        build: eyre::Result<Vec<u8>>,
    ) -> (eyre::Result<()>, Asked, Option<(String, usize)>) {
        deployed_with(None, answers, build).await
    }

    async fn deployed_with(
        parts: Option<Parts>,
        answers: Vec<Option<(u16, Value)>>,
        build: eyre::Result<Vec<u8>>,
    ) -> (eyre::Result<()>, Asked, Option<(String, usize)>) {
        let (ended, asked, built, _) = deployed_saying(parts, answers, build).await;
        (ended, asked, built)
    }

    async fn deployed_saying(
        parts: Option<Parts>,
        answers: Vec<Option<(u16, Value)>>,
        build: eyre::Result<Vec<u8>>,
    ) -> (
        eyre::Result<()>,
        Asked,
        Option<(String, usize)>,
        Vec<String>,
    ) {
        let (config, asked) = answering(answers).await;
        let client = config.http_client();
        let said = std::sync::Mutex::new(Vec::new());
        let app = WebApp {
            config: &config,
            client: &client,
            name: "site",
            parts,
            out: &|line: String| {
                said.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(line)
            },
        };
        let mut built = None;
        let ended = app
            .deploy(request(), async |service_id: &str| {
                built = Some((service_id.to_string(), asked.count()));
                build
            })
            .await;
        let said = said
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (ended, asked, built, said)
    }

    fn timeless(line: &str) -> String {
        let mut out = String::new();
        let mut rest = line;
        while let Some(start) = rest.find(|c: char| c.is_ascii_digit()) {
            out.push_str(&rest[..start]);
            let digits = |from: &str| {
                from.find(|c: char| !c.is_ascii_digit())
                    .unwrap_or(from.len())
            };
            let tail = &rest[start..];
            let whole = digits(tail);
            let fraction = tail[whole..]
                .strip_prefix('.')
                .map(digits)
                .filter(|fraction| *fraction > 0);
            let end = fraction.map_or(whole, |fraction| whole + 1 + fraction);
            out.push_str(if fraction.is_some() {
                "_"
            } else {
                &tail[..end]
            });
            rest = &tail[end..];
        }
        out.push_str(rest);
        out
    }

    #[tokio::test]
    async fn a_web_deploy_reserves_builds_uploads_then_releases() {
        let (ended, asked, built) = deployed(
            vec![
                reserved(),
                running(1, None),
                stored("uploaded"),
                released(1, false),
            ],
            Ok(BUNDLE.to_vec()),
        )
        .await;

        ended.expect("released");
        assert_eq!(
            built,
            Some(("7".to_string(), 2)),
            "built for the reserved service, after the reservation and before any upload"
        );
        assert_eq!(asked.requests(), [RESERVE, STATUS, &upload(), RELEASE]);
        let bodies = asked.bodies();
        assert_eq!(bodies[0], json!({"name": "site"}));
        assert_eq!(
            bodies[3],
            json!({"manifest": request()["manifest"], "commit_hash": "4f2a9c1",
                   "web_artifact": {"sha256": sha(), "size": BUNDLE.len()}}),
            "the release names the bundle uploaded"
        );
    }

    #[tokio::test]
    async fn a_failed_build_releases_nothing() {
        let (ended, asked, _) = deployed(
            vec![reserved(), running(7, Some(&"3f".repeat(32)))],
            Err(eyre::eyre!("Build failed:\nType error")),
        )
        .await;

        let failed = format!("{:#}", ended.expect_err("the build failed"));
        assert_eq!(
            failed,
            "the build failed; nothing was released; site still runs v7 (bundle 3f3f3f3f3f3f): \
             Build failed:\nType error"
        );
        assert_eq!(asked.requests(), [RESERVE, STATUS]);
    }

    #[tokio::test]
    async fn a_reserved_app_with_no_release_is_not_said_to_run_v1() {
        let (ended, asked, _) = deployed(
            vec![reserved(), running(1, None)],
            Err(eyre::eyre!("Build failed")),
        )
        .await;

        let failed = format!("{:#}", ended.expect_err("the build failed"));
        assert_eq!(
            failed,
            "the build failed; nothing was released: Build failed"
        );
        assert_eq!(asked.count(), 2);
    }

    #[tokio::test]
    async fn a_failed_upload_releases_nothing() {
        let (ended, asked, _) = deployed(
            vec![
                reserved(),
                running(7, Some(&"3f".repeat(32))),
                refused(500, "Failed to store web/7/….tar.gz"),
            ],
            Ok(BUNDLE.to_vec()),
        )
        .await;

        let failed = ended.expect_err("the upload failed").to_string();
        assert!(
            failed.starts_with(&format!("Uploading bundle {} failed (500 ", sha())),
            "{failed}"
        );
        assert!(
            failed.ends_with("; nothing was released; site still runs v7 (bundle 3f3f3f3f3f3f)"),
            "{failed}"
        );
        assert_eq!(asked.requests(), [RESERVE, STATUS, &upload()]);
    }

    #[tokio::test]
    async fn a_bundle_whose_bytes_are_not_its_hash_is_refused_in_the_platforms_words() {
        let mismatch = format!("the bundle's SHA-256 is {}, not {}", "cd".repeat(32), sha());
        let (ended, asked, _) = deployed(
            vec![reserved(), running(1, None), refused(422, &mismatch)],
            Ok(BUNDLE.to_vec()),
        )
        .await;

        let failed = ended.expect_err("refused").to_string();
        assert!(failed.contains(&mismatch), "{failed}");
        assert!(failed.ends_with("; nothing was released"), "{failed}");
        assert_eq!(asked.requests(), [RESERVE, STATUS, &upload()]);
    }

    #[tokio::test]
    async fn the_bundle_that_runs_is_not_uploaded_again() {
        let (ended, asked, _) = deployed(
            vec![reserved(), running(7, Some(&sha())), released(7, true)],
            Ok(BUNDLE.to_vec()),
        )
        .await;

        ended.expect("skipped");
        assert_eq!(asked.requests(), [RESERVE, STATUS, RELEASE]);
    }

    #[tokio::test]
    async fn a_bundle_no_longer_stored_is_uploaded_again_once() {
        let not_stored = format!(
            "bundle {} of site was not uploaded, or is no longer stored; upload it again",
            sha()
        );
        let (ended, asked, _) = deployed(
            vec![
                reserved(),
                running(7, Some(&sha())),
                refused(409, &not_stored),
                stored("uploaded"),
                released(8, false),
            ],
            Ok(BUNDLE.to_vec()),
        )
        .await;
        ended.expect("released after one more upload");
        assert_eq!(
            asked.requests(),
            [RESERVE, STATUS, RELEASE, &upload(), RELEASE]
        );

        let (ended, asked, _) = deployed(
            vec![
                reserved(),
                running(1, None),
                stored("uploaded"),
                refused(409, &not_stored),
                stored("present"),
                refused(409, &not_stored),
            ],
            Ok(BUNDLE.to_vec()),
        )
        .await;
        let failed = ended.expect_err("a second refusal").to_string();
        assert!(
            failed.starts_with("Releasing site failed (409 Conflict): "),
            "{failed}"
        );
        assert!(failed.contains(&not_stored), "{failed}");
        assert!(failed.ends_with("; nothing was released"), "{failed}");
        assert_eq!(
            asked.requests(),
            [RESERVE, STATUS, &upload(), RELEASE, &upload(), RELEASE]
        );
    }

    #[tokio::test]
    async fn a_release_an_older_replica_refuses_says_nothing_was_released() {
        let (ended, _, _) = deployed(
            vec![
                reserved(),
                running(7, Some(&"3f".repeat(32))),
                stored("uploaded"),
                refused(
                    422,
                    "Failed to deserialize the JSON body into the target type: unknown field \
                     `web_artifact`",
                ),
            ],
            Ok(BUNDLE.to_vec()),
        )
        .await;

        assert_eq!(
            ended.expect_err("refused").to_string(),
            "an older platform replica refused the release; nothing was released; site still \
             runs v7 (bundle 3f3f3f3f3f3f). Is a platform upgrade in progress? Deploy again \
             once it has finished"
        );
    }

    #[tokio::test]
    async fn a_release_that_failed_on_the_platform_is_not_said_to_have_released_nothing() {
        let (ended, _, _) = deployed(
            vec![
                reserved(),
                running(7, Some(&"3f".repeat(32))),
                stored("uploaded"),
                refused(500, "Internal error"),
            ],
            Ok(BUNDLE.to_vec()),
        )
        .await;
        let failed = ended.expect_err("failed").to_string();
        assert!(
            failed.ends_with(
                "; site may or may not have been released; check cufflink deployments list \
                 for site, or deploy again, which releases the bundle or finds it running"
            ),
            "{failed}"
        );

        let (ended, _, _) = deployed_or_lost(
            vec![
                Some(reserved()),
                Some(running(1, None)),
                Some(stored("uploaded")),
                None,
            ],
            Ok(BUNDLE.to_vec()),
        )
        .await;
        let lost = ended.expect_err("no answer");
        assert!(
            lost.to_string()
                .starts_with("the release got no answer; site may or may not have been released"),
            "{lost}"
        );
    }

    #[tokio::test]
    async fn a_release_refused_for_another_reason_releases_nothing() {
        for (status, refusal) in [
            (403, "Forbidden: deploy needs the deploy role"),
            (422, "web_artifact.size: invalid type: string, expected u64"),
        ] {
            let (ended, asked, _) = deployed(
                vec![
                    reserved(),
                    running(7, Some(&"3f".repeat(32))),
                    stored("uploaded"),
                    refused(status, refusal),
                ],
                Ok(BUNDLE.to_vec()),
            )
            .await;

            let failed = ended.expect_err("refused").to_string();
            assert!(
                failed.starts_with(&format!("Releasing site failed ({status} ")),
                "{failed}"
            );
            assert!(failed.contains(refusal), "{failed}");
            assert!(
                failed
                    .ends_with("; nothing was released; site still runs v7 (bundle 3f3f3f3f3f3f)"),
                "{failed}"
            );
            assert_eq!(asked.requests(), [RESERVE, STATUS, &upload(), RELEASE]);
        }
    }

    #[tokio::test]
    async fn a_lost_reservation_or_upload_releases_nothing() {
        let (ended, asked, built) = deployed_or_lost(vec![None], Ok(BUNDLE.to_vec())).await;
        let lost = format!("{:#}", ended.expect_err("no answer"));
        assert!(
            lost.starts_with("Reserving site got no answer; nothing was released: "),
            "{lost}"
        );
        assert_eq!(asked.requests(), [RESERVE]);
        assert_eq!(built, None, "nothing is built");

        let (ended, asked, _) = deployed_or_lost(
            vec![
                Some(reserved()),
                Some(running(7, Some(&"3f".repeat(32)))),
                None,
            ],
            Ok(BUNDLE.to_vec()),
        )
        .await;
        assert_eq!(
            ended.expect_err("no answer").to_string(),
            format!(
                "Uploading bundle {} got no answer; nothing was released; site still runs v7 \
                 (bundle 3f3f3f3f3f3f)",
                sha()
            )
        );
        assert_eq!(asked.requests(), [RESERVE, STATUS, &upload()]);
    }

    #[tokio::test]
    async fn a_platform_without_the_reserve_route_is_named_as_one_being_upgraded() {
        for status in [404, 405] {
            let (ended, asked, built) =
                deployed(vec![(status, Value::Null)], Ok(BUNDLE.to_vec())).await;

            let failed = ended.expect_err("refused").to_string();
            assert!(
                failed.contains("is a platform upgrade in progress?)"),
                "{failed}"
            );
            assert!(failed.ends_with("; nothing was released"), "{failed}");
            assert_eq!(asked.requests(), [RESERVE]);
            assert_eq!(built, None, "nothing is built");
        }
    }

    const IN_PARTS: Parts = Parts {
        threshold: 8,
        size: 8,
        in_flight: 1,
        attempts: 3,
        backoff: Duration::ZERO,
        assembly_timeout: Duration::from_secs(5),
    };

    fn slices() -> Vec<&'static [u8]> {
        BUNDLE.chunks(IN_PARTS.size).collect()
    }

    fn put_part(index: usize, slice: &[u8]) -> String {
        format!(
            "PUT /api/services/7/web/parts/{}?bundle={}&index={}",
            crate::digest::sha256_hex(slice),
            sha(),
            index + 1
        )
    }

    fn part_stored(slice: &[u8]) -> Option<(u16, Value)> {
        Some((
            200,
            json!({"sha256": crate::digest::sha256_hex(slice), "size": slice.len(),
                   "stored": "uploaded"}),
        ))
    }

    fn parts_stored() -> Vec<Option<(u16, Value)>> {
        slices().into_iter().map(part_stored).collect()
    }

    fn parts_put() -> Vec<String> {
        slices()
            .into_iter()
            .enumerate()
            .map(|(index, slice)| put_part(index, slice))
            .collect()
    }

    fn assemble() -> String {
        format!("POST /api/services/7/web/artifacts/{}/parts", sha())
    }

    fn missing(part: &[u8]) -> Option<(u16, Value)> {
        Some(refused(
            409,
            &format!(
                "part {} of bundle {} was not uploaded, or is no longer stored; upload it again",
                crate::digest::sha256_hex(part),
                sha()
            ),
        ))
    }

    fn answers(groups: Vec<Vec<Option<(u16, Value)>>>) -> Vec<Option<(u16, Value)>> {
        [vec![Some(reserved()), Some(running(1, None))]]
            .into_iter()
            .chain(groups)
            .flatten()
            .collect()
    }

    fn requests(groups: &[&[String]]) -> Vec<String> {
        groups.concat()
    }

    fn fixed(requests: &[&str]) -> Vec<String> {
        requests.iter().map(|request| request.to_string()).collect()
    }

    fn hashes() -> Vec<String> {
        slices()
            .into_iter()
            .map(crate::digest::sha256_hex)
            .collect()
    }

    #[tokio::test]
    async fn a_bundle_above_the_threshold_is_asked_for_then_sent_in_parts_and_assembled() {
        let (ended, asked, _) = deployed_with(
            Some(IN_PARTS),
            answers(vec![
                vec![missing(slices()[0])],
                parts_stored(),
                vec![Some(stored("uploaded")), Some(released(1, false))],
            ]),
            Ok(BUNDLE.to_vec()),
        )
        .await;

        ended.expect("released");
        assert_eq!(
            asked.requests(),
            requests(&[
                &fixed(&[RESERVE, STATUS]),
                &[assemble()],
                &parts_put(),
                &[assemble()],
                &fixed(&[RELEASE]),
            ])
        );
        for at in [2, 6] {
            assert_eq!(
                asked.bodies()[at],
                json!({"size": BUNDLE.len(), "parts": hashes()}),
                "the assembly names the parts in order"
            );
        }
        assert_eq!(
            asked.bodies()[7]["web_artifact"],
            json!({"sha256": sha(), "size": BUNDLE.len()}),
            "the release names the assembled bundle"
        );
    }

    #[tokio::test]
    async fn a_bundle_the_platform_already_stores_sends_no_part() {
        let (ended, asked, _) = deployed_with(
            Some(IN_PARTS),
            answers(vec![vec![
                Some(stored("present")),
                Some(released(1, false)),
            ]]),
            Ok(BUNDLE.to_vec()),
        )
        .await;

        ended.expect("released");
        assert_eq!(
            asked.requests(),
            requests(&[
                &fixed(&[RESERVE, STATUS]),
                &[assemble()],
                &fixed(&[RELEASE])
            ])
        );
    }

    #[tokio::test]
    async fn parts_in_flight_together_all_reach_the_platform_before_the_assembly() {
        let (ended, asked, _) = deployed_with(
            Some(Parts {
                in_flight: 3,
                ..IN_PARTS
            }),
            answers(vec![
                vec![missing(slices()[0])],
                parts_stored(),
                vec![Some(stored("uploaded")), Some(released(1, false))],
            ]),
            Ok(BUNDLE.to_vec()),
        )
        .await;

        ended.expect("released");
        let asked = asked.requests();
        let mut sent = asked[3..6].to_vec();
        sent.sort();
        let mut expected = parts_put();
        expected.sort();
        assert_eq!(sent, expected);
        assert_eq!(asked[6..], [assemble(), RELEASE.to_string()]);
    }

    #[tokio::test]
    async fn a_bundle_at_the_threshold_or_on_a_platform_without_parts_goes_in_one_request() {
        for parts in [
            Some(Parts {
                threshold: BUNDLE.len(),
                ..IN_PARTS
            }),
            None,
        ] {
            let (ended, asked, _) = deployed_with(
                parts,
                answers(vec![vec![
                    Some(stored("uploaded")),
                    Some(released(1, false)),
                ]]),
                Ok(BUNDLE.to_vec()),
            )
            .await;

            ended.expect("released");
            assert_eq!(asked.requests(), [RESERVE, STATUS, &upload(), RELEASE]);
        }
    }

    #[tokio::test]
    async fn a_part_lost_three_times_or_refused_stops_the_upload_and_releases_nothing() {
        let (ended, asked, _) = deployed_with(
            Some(IN_PARTS),
            [
                vec![
                    Some(reserved()),
                    Some(running(7, Some(&"3f".repeat(32)))),
                    missing(slices()[0]),
                    part_stored(slices()[0]),
                ],
                vec![None; 3],
            ]
            .concat(),
            Ok(BUNDLE.to_vec()),
        )
        .await;
        assert_eq!(
            ended.expect_err("no answer").to_string(),
            format!(
                "Uploading part 2 of 3 of bundle {} got no answer; nothing was released; site \
                 still runs v7 (bundle 3f3f3f3f3f3f)",
                sha()
            )
        );
        let parts = parts_put();
        assert_eq!(
            asked.requests(),
            requests(&[
                &fixed(&[RESERVE, STATUS]),
                &[assemble(), parts[0].clone()],
                &[parts[1].clone(), parts[1].clone(), parts[1].clone()],
            ]),
            "a part that gets no answer is sent three times in all"
        );

        for (status, refusal, hint) in [
            (422, "the part's SHA-256 is not its path's", ""),
            (409, "service 'site' is not a web app", ""),
            (404, "", " (a platform replica without this route"),
            (501, "", ""),
        ] {
            let (ended, asked, _) = deployed_with(
                Some(IN_PARTS),
                answers(vec![vec![
                    missing(slices()[0]),
                    Some(refused(status, refusal)),
                ]]),
                Ok(BUNDLE.to_vec()),
            )
            .await;
            let failed = ended.expect_err("refused").to_string();
            assert!(
                failed.starts_with(&format!(
                    "Uploading part 1 of 3 of bundle {} failed ({status} ",
                    sha()
                )),
                "{failed}"
            );
            assert!(
                failed.contains(refusal) && failed.contains(hint),
                "{failed}"
            );
            assert!(failed.ends_with("; nothing was released"), "{failed}");
            assert_eq!(
                asked.requests(),
                requests(&[&fixed(&[RESERVE, STATUS]), &[assemble()], &parts_put()[..1]])
            );
        }
    }

    #[tokio::test]
    async fn a_part_the_platform_no_longer_stores_at_the_assembly_is_sent_again_once() {
        let (ended, asked, _) = deployed_with(
            Some(IN_PARTS),
            answers(vec![
                vec![missing(slices()[0])],
                parts_stored(),
                vec![
                    missing(slices()[1]),
                    part_stored(slices()[1]),
                    Some(stored("uploaded")),
                    Some(released(1, false)),
                ],
            ]),
            Ok(BUNDLE.to_vec()),
        )
        .await;

        ended.expect("released after sending the part again");
        let parts = parts_put();
        assert_eq!(
            asked.requests(),
            requests(&[
                &fixed(&[RESERVE, STATUS]),
                &[assemble()],
                &parts,
                &[assemble(), parts[1].clone(), assemble()],
                &fixed(&[RELEASE]),
            ])
        );
    }

    #[tokio::test]
    async fn every_part_a_conflict_names_is_sent_again() {
        let hashes = hashes();
        let (ended, asked, _) = deployed_with(
            Some(IN_PARTS),
            answers(vec![
                vec![missing(slices()[0])],
                parts_stored(),
                vec![
                    Some(refused(
                        409,
                        &format!(
                            "parts {}, {} of bundle {} were not uploaded, or are no longer \
                             stored; upload them again",
                            hashes[0],
                            hashes[2],
                            sha()
                        ),
                    )),
                    part_stored(slices()[0]),
                    part_stored(slices()[2]),
                    Some(stored("uploaded")),
                    Some(released(1, false)),
                ],
            ]),
            Ok(BUNDLE.to_vec()),
        )
        .await;

        ended.expect("released after sending both parts again");
        let parts = parts_put();
        assert_eq!(
            asked.requests(),
            requests(&[
                &fixed(&[RESERVE, STATUS]),
                &[assemble()],
                &parts,
                &[assemble(), parts[0].clone(), parts[2].clone(), assemble()],
                &fixed(&[RELEASE]),
            ])
        );
    }

    #[tokio::test]
    async fn an_assembly_refused_lost_or_missing_a_part_twice_releases_nothing() {
        let mismatch = format!("the bundle's SHA-256 is {}, not {}", "cd".repeat(32), sha());
        let cases = [
            (
                vec![Some(refused(422, &mismatch))],
                format!(
                    "Assembling bundle {} failed (422 Unprocessable Entity): ",
                    sha()
                ),
                requests(&[&[assemble()], &parts_put(), &[assemble()]]),
            ),
            (
                vec![
                    missing(slices()[1]),
                    part_stored(slices()[1]),
                    missing(slices()[1]),
                ],
                format!("Assembling bundle {} failed (409 Conflict): ", sha()),
                requests(&[
                    &[assemble()],
                    &parts_put(),
                    &[assemble(), parts_put()[1].clone(), assemble()],
                ]),
            ),
        ];
        for (after, starts, sent) in cases {
            let (ended, asked, _) = deployed_with(
                Some(IN_PARTS),
                answers(vec![vec![missing(slices()[0])], parts_stored(), after]),
                Ok(BUNDLE.to_vec()),
            )
            .await;
            let failed = ended.expect_err("refused").to_string();
            assert!(failed.starts_with(&starts), "{failed}");
            assert!(failed.ends_with("; nothing was released"), "{failed}");
            assert_eq!(
                asked.requests(),
                requests(&[&fixed(&[RESERVE, STATUS]), &sent])
            );
        }

        let (ended, _, _) = deployed_with(
            Some(IN_PARTS),
            answers(vec![vec![None; 3]]),
            Ok(BUNDLE.to_vec()),
        )
        .await;
        assert_eq!(
            ended.expect_err("no answer").to_string(),
            format!(
                "Assembling bundle {} got no answer; nothing was released",
                sha()
            )
        );
    }

    #[tokio::test]
    async fn a_part_or_assembly_lost_or_unavailable_is_sent_again_and_released() {
        let (ended, asked, _) = deployed_with(
            Some(IN_PARTS),
            answers(vec![
                vec![missing(slices()[0])],
                vec![
                    part_stored(slices()[0]),
                    None,
                    Some(refused(503, "the platform is starting")),
                    part_stored(slices()[1]),
                    part_stored(slices()[2]),
                ],
                vec![None, Some(stored("present")), Some(released(2, false))],
            ]),
            Ok(BUNDLE.to_vec()),
        )
        .await;

        ended.expect("released after sending again");
        let parts = parts_put();
        assert_eq!(
            asked.requests(),
            requests(&[
                &fixed(&[RESERVE, STATUS]),
                &[assemble()],
                &[
                    parts[0].clone(),
                    parts[1].clone(),
                    parts[1].clone(),
                    parts[1].clone(),
                    parts[2].clone(),
                ],
                &[assemble(), assemble()],
                &fixed(&[RELEASE]),
            ])
        );
    }

    #[test]
    fn only_a_lost_answer_or_a_passing_refusal_is_worth_sending_again() {
        let worth = [500, 502, 503, 504, 408, 429];
        let lasting = [400, 401, 403, 404, 405, 409, 413, 422, 501, 505];
        assert!(worth
            .into_iter()
            .all(|status| transient(StatusCode::from_u16(status).expect("a status"))));
        assert!(!lasting
            .into_iter()
            .any(|status| transient(StatusCode::from_u16(status).expect("a status"))));
    }

    #[test]
    fn a_retry_after_in_seconds_is_read_and_any_other_form_ignored() {
        let after = |value: &str| {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::RETRY_AFTER,
                value.parse().expect("a header value"),
            );
            retry_after(&headers)
        };
        assert_eq!(after("7"), Some(Duration::from_secs(7)));
        assert_eq!(after(" 30 "), Some(Duration::from_secs(30)));
        assert_eq!(after("Wed, 21 Oct 2026 07:28:00 GMT"), None);
        assert_eq!(retry_after(&reqwest::header::HeaderMap::new()), None);
    }

    #[test]
    fn each_distinct_part_is_sent_once_in_the_order_it_first_appears() {
        let parts = IN_PARTS
            .of(&Bundle::of(b"same, 8!tail of same, 8!it".to_vec()))
            .expect("in parts");
        assert_eq!(parts.len(), 4);
        assert_eq!(
            distinct(&parts)
                .into_iter()
                .map(|(position, part)| (position, part.bytes.to_vec()))
                .collect::<Vec<_>>(),
            [
                (0, b"same, 8!".to_vec()),
                (1, b"tail of ".to_vec()),
                (3, b"it".to_vec())
            ]
        );
    }

    #[tokio::test]
    async fn a_release_refused_as_not_claimed_uploads_the_parts_again() {
        let not_stored = format!(
            "bundle {} of site was not uploaded, or is no longer stored; upload it again",
            sha()
        );
        let (ended, asked, _) = deployed_with(
            Some(IN_PARTS),
            answers(vec![
                vec![missing(slices()[0])],
                parts_stored(),
                vec![Some(stored("uploaded")), Some(refused(409, &not_stored))],
                vec![missing(slices()[0])],
                parts_stored(),
                vec![Some(stored("present")), Some(released(2, false))],
            ]),
            Ok(BUNDLE.to_vec()),
        )
        .await;

        ended.expect("released after uploading the parts again");
        let once = requests(&[
            &[assemble()],
            &parts_put(),
            &[assemble()],
            &fixed(&[RELEASE]),
        ]);
        assert_eq!(
            asked.requests(),
            requests(&[&fixed(&[RESERVE, STATUS]), &once, &once])
        );
    }

    #[test]
    fn a_bundle_splits_into_slices_in_order_of_which_only_the_last_is_shorter() {
        let parts = |bytes: &[u8]| {
            IN_PARTS.of(&Bundle::of(bytes.to_vec())).map(|parts| {
                parts
                    .into_iter()
                    .map(|part| (part.bytes.to_vec(), part.sha256))
                    .collect::<Vec<_>>()
            })
        };
        let expected = |slices: &[&[u8]]| {
            Some(
                slices
                    .iter()
                    .map(|slice| (slice.to_vec(), crate::digest::sha256_hex(slice)))
                    .collect::<Vec<_>>(),
            )
        };
        assert_eq!(parts(BUNDLE), expected(&slices()));
        assert_eq!(
            parts(&b"sixteen bytes!!!"[..]),
            expected(&[&b"sixteen "[..], &b"bytes!!!"[..]])
        );
        assert_eq!(parts(&b"8 bytes!"[..]), None, "a bundle at the threshold");
    }

    #[test]
    fn parts_are_used_only_on_a_platform_that_offers_them() {
        let platform = |supports: &[&str]| PlatformVersion {
            api_version: 1,
            platform_version: Some("0.40.0".into()),
            supports: supports.iter().map(|s| s.to_string()).collect(),
        };
        assert_eq!(
            Parts::offered(&platform(&[WEB_RELEASES, WEB_BUNDLE_PARTS])),
            Some(Parts::default())
        );
        assert_eq!(Parts::offered(&platform(&[WEB_RELEASES])), None);
    }

    fn answer(answer: Value) -> eyre::Result<Vec<String>> {
        reported("site", &Bundle::of(BUNDLE.to_vec()), &answer)
    }

    #[test]
    fn a_deployed_answer_without_the_bundle_fails() {
        for unnamed in [
            json!({"version": 8}),
            json!({"version": 8, "web_artifact": null}),
        ] {
            assert_eq!(
                answer(unnamed).expect_err("no bundle").to_string(),
                "the platform answered without the bundle it released; check cufflink \
                 deployments list for site"
            );
        }
    }

    #[test]
    fn an_answer_naming_another_bundle_fails() {
        let other = "cd".repeat(32);
        let failed = answer(json!({"version": 8, "web_artifact": {"sha256": other, "size": 1}}))
            .expect_err("another bundle")
            .to_string();
        assert_eq!(
            failed,
            format!(
                "the platform's current version of site records bundle {other}, not {}, \
                 which this deploy built and uploaded; check cufflink deployments list for site",
                sha()
            )
        );
    }

    #[test]
    fn a_release_says_what_runs_and_warns_when_the_runners_were_not_told() {
        let (_, mut deployed) = released(8, false);
        assert_eq!(
            answer(deployed.clone()).expect("released"),
            [
                format!("Released site v8 (bundle {})", short(&sha())),
                "  Deployment ID: d".to_string(),
                "  Tenant:        acme".to_string(),
            ]
        );
        deployed["runners_told"] = json!(false);
        deployed["commit_hash"] = json!("4f2a9c1");
        assert_eq!(
            answer(deployed)
                .expect("released")
                .last()
                .map(String::as_str),
            Some(
                "warning: the web runners were not told of v8; each picks it up at its next \
                 reconcile (WEB_RECONCILE_SECS, 30 s by default)"
            )
        );
        let (_, skipped) = released(7, true);
        assert_eq!(
            answer(skipped).expect("skipped")[0],
            format!(
                "site v7 already runs bundle {}; nothing to release",
                short(&sha())
            )
        );
    }

    #[test]
    fn only_a_platform_with_web_releases_is_asked_to_release() {
        let platform = |supports: &[&str]| PlatformVersion {
            api_version: 1,
            platform_version: Some("0.37.1".into()),
            supports: supports.iter().map(|s| s.to_string()).collect(),
        };
        assert!(require_web_releases(&platform(&["db_v2", WEB_RELEASES])).is_ok());
        assert_eq!(
            require_web_releases(&platform(&["db_v2"]))
                .expect_err("refused")
                .to_string(),
            "platform v0.37.1 cannot release a web app with its bundle (needs web_releases). \
             Upgrade the platform, or deploy with a CLI older than v0.38.0"
        );
    }

    #[tokio::test]
    async fn each_attempt_of_a_part_or_assembly_says_how_long_it_took_and_the_upload_sums_up() {
        let (ended, _, _, said) = deployed_saying(
            Some(IN_PARTS),
            answers(vec![
                vec![missing(slices()[0])],
                vec![
                    part_stored(slices()[0]),
                    None,
                    part_stored(slices()[1]),
                    part_stored(slices()[2]),
                ],
                vec![Some(stored("uploaded")), Some(released(1, false))],
            ]),
            Ok(BUNDLE.to_vec()),
        )
        .await;

        ended.expect("released");
        let bundle = short(&sha()).to_string();
        let part = |index: usize| format!("  Uploading part {index} of 3 of bundle {bundle}: ");
        assert_eq!(
            said.iter().map(|line| timeless(line)).collect::<Vec<_>>(),
            [
                format!("  Bundle {bundle}: 22 bytes (_ MB)"),
                format!("Uploading bundle {bundle}..."),
                format!("  Assembling bundle {bundle}: 409 Conflict after _ s"),
                "  In 3 parts".to_string(),
                format!("{}8 bytes in _ s (_ MB/s)", part(1)),
                format!("{}no answer after _ s; trying again (2 of 3)", part(2)),
                format!("{}8 bytes in _ s (_ MB/s), on attempt 2 of 3", part(2)),
                format!("{}6 bytes in _ s (_ MB/s)", part(3)),
                format!("  Assembling bundle {bundle}: answered in _ s"),
                "  Stored".to_string(),
                "  Sent _ MB in _ s (_ MB/s): parts _ s, assembly _ s".to_string(),
                "Releasing site...".to_string(),
                format!("Released site v1 (bundle {bundle})"),
                "  Deployment ID: d".to_string(),
                "  Tenant:        acme".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn a_part_that_fails_for_good_says_how_long_its_last_attempt_took() {
        let (ended, _, _, said) = deployed_saying(
            Some(IN_PARTS),
            answers(vec![vec![missing(slices()[0]), None, None, None]]),
            Ok(BUNDLE.to_vec()),
        )
        .await;

        ended.expect_err("no answer");
        let part = format!("  Uploading part 1 of 3 of bundle {}: ", short(&sha()));
        assert_eq!(
            said[4..]
                .iter()
                .map(|line| timeless(line))
                .collect::<Vec<_>>(),
            [
                format!("{part}no answer after _ s; trying again (2 of 3)"),
                format!("{part}no answer after _ s; trying again (3 of 3)"),
                format!("{part}no answer after _ s"),
            ]
        );
    }

    #[tokio::test]
    async fn a_bundle_sent_whole_says_how_long_it_took_and_one_already_stored_sends_nothing() {
        let (ended, _, _, said) = deployed_saying(
            None,
            answers(vec![vec![
                Some(stored("uploaded")),
                Some(released(1, false)),
            ]]),
            Ok(BUNDLE.to_vec()),
        )
        .await;
        ended.expect("released");
        assert_eq!(
            said[1..4]
                .iter()
                .map(|line| timeless(line))
                .collect::<Vec<_>>(),
            [
                format!("Uploading bundle {}...", short(&sha())),
                "  Stored".to_string(),
                "  Sent _ MB in _ s (_ MB/s)".to_string(),
            ]
        );

        let (ended, _, _, said) = deployed_saying(
            Some(IN_PARTS),
            answers(vec![vec![
                Some(stored("present")),
                Some(released(1, false)),
            ]]),
            Ok(BUNDLE.to_vec()),
        )
        .await;
        ended.expect("released");
        assert_eq!(
            said[2..4]
                .iter()
                .map(|line| timeless(line))
                .collect::<Vec<_>>(),
            [
                format!("  Assembling bundle {}: answered in _ s", short(&sha())),
                "  Already stored".to_string(),
            ]
        );
        assert_eq!(said[4], "Releasing site...", "nothing was sent to sum up");
    }

    #[test]
    fn an_attempt_reads_as_its_bytes_time_and_rate_or_its_reason() {
        let label = "Uploading part 7 of 20 of bundle 3f9a1c2b4d5e";
        let took = Duration::from_millis(4_000);
        assert_eq!(
            answered_line(label, Some(8_388_608), took, (1, 3)),
            format!("  {label}: 8388608 bytes in 4.0 s (2.0 MB/s)")
        );
        assert_eq!(
            answered_line(label, Some(8_388_608), took, (2, 3)),
            format!("  {label}: 8388608 bytes in 4.0 s (2.0 MB/s), on attempt 2 of 3")
        );
        assert_eq!(
            answered_line(label, Some(8_388_608), Duration::ZERO, (1, 3)),
            format!("  {label}: 8388608 bytes in 0.0 s"),
            "no rate for an attempt that took no time"
        );
        assert_eq!(
            answered_line("Assembling bundle 3f9a1c2b4d5e", None, took, (1, 3)),
            "  Assembling bundle 3f9a1c2b4d5e: answered in 4.0 s"
        );
        assert_eq!(
            failed_line(label, "no answer", Duration::from_secs(120), Some((2, 3))),
            format!("  {label}: no answer after 120.0 s; trying again (2 of 3)")
        );
        assert_eq!(
            failed_line(label, "503 Service Unavailable", took, None),
            format!("  {label}: 503 Service Unavailable after 4.0 s")
        );
    }

    #[test]
    fn an_upload_sums_up_its_bytes_time_rate_and_phases() {
        assert_eq!(
            sent_line(
                Sent {
                    bytes: 100 * 1_048_576,
                    phases: Some(Phases {
                        parts: Duration::from_millis(45_300),
                        assembly: Duration::from_secs(4),
                    }),
                },
                Duration::from_secs(50),
            ),
            "  Sent 100.0 MB in 50.0 s (2.0 MB/s): parts 45.3 s, assembly 4.0 s"
        );
        assert_eq!(
            sent_line(
                Sent {
                    bytes: 3 * 1_048_576,
                    phases: None,
                },
                Duration::from_millis(1_500),
            ),
            "  Sent 3.0 MB in 1.5 s (2.0 MB/s)"
        );
    }

    #[test]
    fn timeless_erases_only_decimals() {
        assert_eq!(
            timeless("  Part 12 of 3f9a1c2b4d52...: 8 bytes in 0.31 s (25.0 MB/s) v1"),
            "  Part 12 of 3f9a1c2b4d52...: 8 bytes in _ s (_ MB/s) v1"
        );
    }
}
