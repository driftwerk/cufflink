use crate::commands::version::PlatformVersion;
use crate::config::CliConfig;
use cufflink_types::features::WEB_RELEASES;
use reqwest::{Method, StatusCode};
use serde_json::{json, Value};

fn unrouted(status: StatusCode) -> &'static str {
    match status {
        StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED => {
            " (a platform replica that does not take web bundles by content answers so: \
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
}

enum Release {
    Answered(Value),
    NotClaimed(String),
}

impl WebApp<'_> {
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.config.api_url)
    }

    async fn sent(
        &self,
        request: reqwest::RequestBuilder,
    ) -> eyre::Result<(StatusCode, Result<Value, String>)> {
        let answer = request.send().await?;
        let status = answer.status();
        if status.is_success() {
            Ok((status, Ok(answer.json().await?)))
        } else {
            Ok((status, Err(answer.text().await.unwrap_or_default())))
        }
    }

    async fn reserve(&self) -> eyre::Result<Reserved> {
        let request = self
            .config
            .auth_request(self.client, Method::POST, &self.url("/api/services/web"))
            .json(&json!({ "name": self.name }));
        let reserved = match self.sent(request).await? {
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
        match self.sent(request).await? {
            (_, Ok(status)) => Ok(Reserved::of(service_id, &status)),
            (status, Err(body)) => eyre::bail!(
                "Reading what {} runs failed ({status}): {body}; nothing was released",
                self.name
            ),
        }
    }

    async fn upload(&self, reserved: &Reserved, bundle: &Bundle) -> eyre::Result<()> {
        println!("Uploading bundle {}...", short(&bundle.sha256));
        let request = self
            .config
            .auth_request(
                self.client,
                Method::PUT,
                &self.url(&format!(
                    "/api/services/{}/web/artifacts/{}",
                    reserved.service_id, bundle.sha256
                )),
            )
            .header("Content-Type", "application/gzip")
            .body(bundle.bytes.clone());
        match self.sent(request).await? {
            (_, Ok(body)) if body["stored"].as_str() == Some("present") => {
                println!("  Already stored");
                Ok(())
            }
            (_, Ok(_)) => {
                println!("  Stored");
                Ok(())
            }
            (status, Err(body)) => eyre::bail!(
                "Uploading bundle {} failed ({status}): {body}{}; {}",
                bundle.sha256,
                unrouted(status),
                reserved.unreleased(self.name)
            ),
        }
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
        let answered = self.sent(sent.json(request)).await.map_err(|lost| {
            lost.wrap_err(format!("the release got no answer; {}", self.unsure()))
        })?;
        match answered {
            (_, Ok(body)) => Ok(Release::Answered(body)),
            (StatusCode::CONFLICT, Err(body)) => Ok(Release::NotClaimed(body)),
            (StatusCode::UNPROCESSABLE_ENTITY, Err(body)) if body.contains("web_artifact") => {
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
        println!(
            "  Bundle {}: {} bytes ({:.1} MB)",
            short(&bundle.sha256),
            bundle.size(),
            bundle.size() as f64 / 1_048_576.0
        );
        match reserved.runs(&bundle) {
            Some(version) => println!(
                "  {} v{version} already runs this bundle; not uploading it again",
                self.name
            ),
            None => self.upload(&reserved, &bundle).await?,
        }

        let request = released_with(request, &bundle);
        println!("Releasing {}...", self.name);
        let answer = match self.release(&reserved, &request).await? {
            Release::Answered(answer) => answer,
            Release::NotClaimed(refusal) => {
                println!("  The platform refused the release ({refusal}); uploading again");
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
            .iter()
            .for_each(|line| println!("{line}"));
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
        let (config, asked) = answering(answers.into_iter().map(Some).collect()).await;
        let client = config.http_client();
        let app = WebApp {
            config: &config,
            client: &client,
            name: "site",
        };
        let mut built = None;
        let ended = app
            .deploy(request(), async |service_id: &str| {
                built = Some((service_id.to_string(), asked.count()));
                build
            })
            .await;
        (ended, asked, built)
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

        let (config, _) = answering(
            [reserved(), running(1, None), stored("uploaded")]
                .into_iter()
                .map(Some)
                .chain([None])
                .collect(),
        )
        .await;
        let client = config.http_client();
        let app = WebApp {
            config: &config,
            client: &client,
            name: "site",
        };
        let lost = app
            .deploy(request(), async |_: &str| Ok(BUNDLE.to_vec()))
            .await
            .expect_err("no answer");
        assert!(
            lost.to_string()
                .starts_with("the release got no answer; site may or may not have been released"),
            "{lost}"
        );
    }

    #[tokio::test]
    async fn a_platform_without_the_reserve_route_is_named_as_one_being_upgraded() {
        let (ended, asked, built) = deployed(vec![(405, Value::Null)], Ok(BUNDLE.to_vec())).await;

        let failed = ended.expect_err("refused").to_string();
        assert!(
            failed.contains("is a platform upgrade in progress?)"),
            "{failed}"
        );
        assert!(failed.ends_with("; nothing was released"), "{failed}");
        assert_eq!(asked.requests(), [RESERVE]);
        assert_eq!(built, None, "nothing is built");
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
}
