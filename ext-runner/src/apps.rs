use crate::child::BINARY;
use crate::deployment::{Deployment, Desired};
use cufflink_supervisor::runner_api::RunnerApi;
use cufflink_supervisor::Executable;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{info, warn};

pub async fn installed(
    platform: &RunnerApi,
    apps_dir: &Path,
    deployment: &Deployment,
) -> eyre::Result<PathBuf> {
    let dir = [&deployment.tenant, &deployment.service, &deployment.sha256]
        .iter()
        .fold(apps_dir.to_path_buf(), |dir, part| dir.join(part));
    let binary = dir.join(BINARY);
    if !tokio::fs::try_exists(&binary).await? {
        let data = platform
            .native_binary(deployment.service_id, &deployment.sha256, deployment.size)
            .await?;
        platform
            .cap()
            .install(
                &deployment.service,
                data,
                &dir,
                Some(&deployment.sha256),
                Executable(BINARY),
            )
            .await?;
    }
    Ok(binary)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Kept(BTreeMap<(String, String), Option<BTreeSet<String>>>);

impl Kept {
    pub fn of<'a>(
        desired: &'a Desired,
        running: impl IntoIterator<Item = &'a Arc<Deployment>>,
    ) -> Self {
        let unreadable = desired
            .unreadable
            .values()
            .map(|names| (names.clone(), None))
            .collect();
        Self(desired.run.values().chain(running).fold(
            unreadable,
            |mut kept: BTreeMap<_, Option<BTreeSet<String>>>, deployment| {
                let names = (deployment.tenant.clone(), deployment.service.clone());
                if let Some(versions) = kept.entry(names).or_insert_with(|| Some(BTreeSet::new())) {
                    versions.insert(deployment.sha256.clone());
                }
                kept
            },
        ))
    }

    fn keeps(&self, tenant: &str, service: &str, version: &str) -> bool {
        self.0
            .get(&(tenant.to_string(), service.to_string()))
            .is_some_and(|versions| {
                versions
                    .as_ref()
                    .is_none_or(|versions| versions.contains(version))
            })
    }
}

fn entries(dir: &Path) -> Vec<(String, PathBuf)> {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .map(|entry| {
                    (
                        entry.file_name().to_string_lossy().into_owned(),
                        entry.path(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn remove(path: &Path) {
    let removed = match path.is_dir() {
        true => std::fs::remove_dir_all(path),
        false => std::fs::remove_file(path),
    };
    match removed {
        Ok(()) => info!(path = %path.display(), "Removed a binary no child runs any more"),
        Err(e) => warn!(path = %path.display(), error = %e, "Cannot remove a binary no child runs"),
    }
}

pub fn prune(apps_dir: &Path, kept: &Kept, tenant: &str) {
    let tenant_dir = apps_dir.join(tenant);
    for (service, service_dir) in entries(&tenant_dir) {
        entries(&service_dir)
            .into_iter()
            .filter(|(version, _)| !kept.keeps(tenant, &service, version))
            .for_each(|(_, path)| remove(&path));
        let _ = std::fs::remove_dir(&service_dir);
    }
    let _ = std::fs::remove_dir(&tenant_dir);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cufflink_supervisor::runner_api::{RunnerKey, RunnerTenant};
    use cufflink_supervisor::ArtifactCap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use uuid::Uuid;

    fn deployment(tenant: &str, service: &str, sha256: &str) -> Arc<Deployment> {
        Arc::new(Deployment {
            service_id: Uuid::new_v4(),
            tenant: tenant.into(),
            service: service.into(),
            version: 1,
            sha256: sha256.into(),
            size: 4,
            extensions: ["a.b".to_string()].into(),
        })
    }

    fn tree(root: &Path) -> Vec<String> {
        let mut found: Vec<String> = entries(root)
            .into_iter()
            .flat_map(|(tenant, dir)| {
                entries(&dir).into_iter().flat_map(move |(service, dir)| {
                    let tenant = tenant.clone();
                    entries(&dir)
                        .into_iter()
                        .map(move |(version, _)| format!("{tenant}/{service}/{version}"))
                })
            })
            .collect();
        found.sort();
        found
    }

    #[test]
    fn what_no_child_needs_is_pruned_of_the_runners_tenant_alone() {
        let root = TempDir::new().expect("apps dir");
        for path in [
            "acme/ext-pdf/aaa/service",
            "acme/ext-pdf/bbb/service",
            "acme/ext-pdf/.tmpX1/service",
            "acme/ext-gone/ccc/service",
            "acme/ext-odd/ddd/service",
            "acme/ext-odd/eee/service",
            "globex/ext-mail/fff/service",
        ] {
            let path = root.path().join(path);
            std::fs::create_dir_all(path.parent().expect("dir")).expect("dir");
            std::fs::write(path, "bin").expect("binary");
        }
        std::fs::write(root.path().join("acme/ext-pdf/stray"), "x").expect("stray");
        let desired = Desired {
            run: [deployment("acme", "ext-pdf", "bbb")]
                .into_iter()
                .map(|d| (d.service_id, d))
                .collect(),
            unreadable: [(Uuid::new_v4(), ("acme".to_string(), "ext-odd".to_string()))].into(),
        };
        let running = [deployment("acme", "ext-pdf", "aaa")];
        prune(root.path(), &Kept::of(&desired, &running), "acme");
        assert_eq!(
            tree(root.path()),
            [
                "acme/ext-odd/ddd",
                "acme/ext-odd/eee",
                "acme/ext-pdf/aaa",
                "acme/ext-pdf/bbb",
                "globex/ext-mail/fff"
            ],
            "another tenant's binaries are left alone"
        );

        prune(root.path(), &Kept::default(), "acme");
        assert_eq!(tree(root.path()), ["globex/ext-mail/fff"]);
        assert!(!root.path().join("acme").exists(), "emptied dirs go too");
    }

    #[test]
    fn an_unreadable_service_keeps_every_version() {
        let desired = Desired {
            run: [deployment("acme", "ext-pdf", "bbb")]
                .into_iter()
                .map(|d| (d.service_id, d))
                .collect(),
            unreadable: [(Uuid::new_v4(), ("acme".to_string(), "ext-pdf".to_string()))].into(),
        };
        let kept = Kept::of(&desired, &[]);
        assert!(kept.keeps("acme", "ext-pdf", "anything"));
        assert!(!kept.keeps("acme", "ext-other", "bbb"));
    }

    async fn serving(body: &'static [u8]) -> (RunnerApi, Arc<AtomicUsize>) {
        let served = Arc::new(AtomicUsize::new(0));
        let counted = served.clone();
        let app = axum::Router::new().route(
            "/api/runner/native/{service_id}/{sha256}",
            axum::routing::get(move || {
                counted.fetch_add(1, Ordering::SeqCst);
                std::future::ready(body)
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port");
        let url = format!("http://{}", listener.local_addr().expect("an address"));
        tokio::spawn(async move { axum::serve(listener, app).await });
        let platform = RunnerApi::new(
            &url,
            RunnerKey::Value("cfr_AAAAAAAAAAAA_s3cret".into()),
            RunnerTenant::parse("EXT_TENANT", "acme".into()).expect("a slug"),
            ArtifactCap {
                max_bytes: 1 << 20,
                setting: "EXT_ARTIFACT_MAX_BYTES",
            },
        )
        .expect("a client");
        (platform, served)
    }

    const TEST_SHA: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    #[tokio::test]
    async fn a_binary_is_installed_once() {
        let root = TempDir::new().expect("apps dir");
        let (platform, served) = serving(b"test").await;
        let deployment = deployment("acme", "ext-pdf", TEST_SHA);
        let binary = installed(&platform, root.path(), &deployment)
            .await
            .expect("installed");
        assert_eq!(
            binary,
            root.path()
                .join("acme/ext-pdf")
                .join(TEST_SHA)
                .join("service")
        );
        assert_eq!(std::fs::read(&binary).expect("binary"), b"test");
        assert_eq!(
            installed(&platform, root.path(), &deployment)
                .await
                .expect("reused"),
            binary
        );
        assert_eq!(
            served.load(Ordering::SeqCst),
            1,
            "an installed binary is not downloaded again"
        );
    }

    #[tokio::test]
    async fn a_binary_that_is_not_its_hash_is_not_installed() {
        let root = TempDir::new().expect("apps dir");
        let (platform, _) = serving(b"tampered").await;
        let deployment = deployment("acme", "ext-pdf", TEST_SHA);
        let refused = installed(&platform, root.path(), &deployment)
            .await
            .expect_err("a mismatch")
            .to_string();
        assert!(refused.contains("hash mismatch"), "{refused}");
        assert!(tree(root.path()).is_empty(), "nothing is installed");
    }
}
