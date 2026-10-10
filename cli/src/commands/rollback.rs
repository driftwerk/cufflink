use super::services::{self, Deployed};
use crate::config::CliConfig;
use serde::Deserialize;
use std::collections::BTreeSet;

const SHORTEST_COMMIT: usize = 7;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum To {
    Previous,
    Version(i32),
    Commit(String),
}

impl To {
    pub fn asked(version: Option<i32>, commit: Option<String>) -> Self {
        match (version, commit) {
            (Some(version), _) => To::Version(version),
            (None, Some(commit)) => To::Commit(commit),
            (None, None) => To::Previous,
        }
    }
}

pub fn commit_argument(commit: &str) -> Result<String, String> {
    match commit.len() < SHORTEST_COMMIT {
        true => Err(NoSuchCommit::TooShort.told(commit)),
        false => Ok(commit.to_string()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct Deployment {
    version: i32,
    #[serde(default)]
    commit_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Retained {
    current_version: i32,
    deployments: Vec<Deployment>,
}

#[derive(Debug, PartialEq, Eq)]
enum NoSuchCommit {
    TooShort,
    NotRetained,
    Running,
    Ambiguous(BTreeSet<String>),
}

impl NoSuchCommit {
    fn told(&self, commit: &str) -> String {
        match self {
            NoSuchCommit::TooShort => format!(
                "'{commit}' is too short to name a commit: give at least {SHORTEST_COMMIT} \
                 characters of its hash"
            ),
            NoSuchCommit::NotRetained => format!(
                "no retained version was deployed at {commit}\n  `cufflink deployments list` \
                 shows what is retained; deploying the commit's bundle gives the same code as \
                 a new version"
            ),
            NoSuchCommit::Running => format!("{commit} is the running version"),
            NoSuchCommit::Ambiguous(commits) => format!(
                "{commit} names more than one deployed commit ({}): give more of its hash",
                commits.iter().cloned().collect::<Vec<_>>().join(", ")
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct At {
    version: i32,
    commit: String,
}

fn begins_with(deployed: &str, commit: &str) -> bool {
    deployed
        .get(..commit.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(commit))
}

fn version_at_commit(
    deployments: &[Deployment],
    commit: &str,
    current: i32,
) -> Result<At, NoSuchCommit> {
    if commit.len() < SHORTEST_COMMIT {
        return Err(NoSuchCommit::TooShort);
    }
    let labelled = || {
        deployments
            .iter()
            .filter_map(|deployment| Some((deployment.commit_hash.as_deref()?, deployment.version)))
    };
    let exactly: Vec<(&str, i32)> = labelled()
        .filter(|(deployed, _)| deployed.eq_ignore_ascii_case(commit))
        .collect();
    let at_commit = match exactly.is_empty() {
        true => labelled()
            .filter(|(deployed, _)| begins_with(deployed, commit))
            .collect(),
        false => exactly,
    };
    let commits: BTreeSet<String> = at_commit
        .iter()
        .map(|(deployed, _)| deployed.to_ascii_lowercase())
        .collect();
    if commits.len() > 1 {
        return Err(NoSuchCommit::Ambiguous(commits));
    }
    if at_commit.iter().any(|(_, version)| *version == current) {
        return Err(NoSuchCommit::Running);
    }
    at_commit
        .into_iter()
        .max_by_key(|(_, version)| *version)
        .map(|(deployed, version)| At {
            version,
            commit: deployed.to_string(),
        })
        .ok_or(NoSuchCommit::NotRetained)
}

async fn retained(
    config: &CliConfig,
    client: &reqwest::Client,
    service_id: &str,
) -> eyre::Result<Retained> {
    let resp = config
        .auth_request(
            client,
            reqwest::Method::GET,
            &format!("{}/api/services/{service_id}/deployments", config.api_url),
        )
        .send()
        .await?;
    serde_json::from_value(super::json_answer(resp, "list deployments").await?)
        .map_err(|e| eyre::eyre!("Failed to list deployments: the answer is not a listing ({e})"))
}

#[derive(Debug, PartialEq, Eq)]
struct Target {
    from: i32,
    version: i32,
    commit: Option<String>,
}

async fn target(
    config: &CliConfig,
    client: &reqwest::Client,
    service: &Deployed,
    to: &To,
) -> eyre::Result<Target> {
    let from = service.current_version;
    let to_version = |version| Target {
        from,
        version,
        commit: None,
    };
    match to {
        To::Version(version) => Ok(to_version(*version)),
        To::Previous if from <= 1 => {
            eyre::bail!("Cannot rollback: only one version exists (v{from})")
        }
        To::Previous => Ok(to_version(from - 1)),
        To::Commit(commit) => {
            let retained = retained(config, client, &service.id).await?;
            version_at_commit(&retained.deployments, commit, retained.current_version)
                .map(|at| Target {
                    from: retained.current_version,
                    version: at.version,
                    commit: Some(at.commit),
                })
                .map_err(|none| eyre::eyre!(none.told(commit)))
        }
    }
}

fn announced(name: &str, target: &Target) -> String {
    let commit = target
        .commit
        .as_ref()
        .map(|commit| format!(" (commit {commit})"))
        .unwrap_or_default();
    format!(
        "Rolling back {name} from v{} to v{}{commit}...",
        target.from, target.version
    )
}

fn forced(name: &str, version: i32, env: Option<&str>) -> String {
    let env = env.map(|env| format!(" --env {env}")).unwrap_or_default();
    format!("cufflink rollback --version {version} --service {name}{env} --allow-destructive")
}

pub async fn run(
    to: To,
    service: Option<&str>,
    allow_destructive: bool,
    env: Option<&str>,
) -> eyre::Result<()> {
    let config = CliConfig::load_with_env(env)?;

    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }

    let client = config.http_client();
    let service = services::acted_on(&config, &client, service).await?;
    let target = target(&config, &client, &service, &to).await?;
    println!("{}", announced(&service.name, &target));
    let target = target.version;

    let rollback_req = serde_json::json!({
        "target_version": target,
        "allow_destructive": allow_destructive,
    });

    let resp = config
        .auth_request(
            &client,
            reqwest::Method::POST,
            &format!("{}/api/services/{}/rollback", config.api_url, service.id),
        )
        .json(&rollback_req)
        .send()
        .await?;

    if resp.status().is_success() {
        let body: serde_json::Value = resp.json().await?;
        println!(
            "Rolled back {} to v{} (now v{})",
            body["name"].as_str().unwrap_or("?"),
            body["rolled_back_to"],
            body["new_version"]
        );
        println!("  Schema changes: {}", body["schema_changes"]);
        println!("  Deployment ID:  {}", body["deployment_id"]);
        if body["wasm_restored"].as_bool() == Some(true) {
            println!("  WASM binary:    restored from v{}", target);
        }
        if let Some(hash) = body["native_hash"].as_str() {
            println!(
                "  Native binary:  restored from v{} ({})",
                target,
                &hash[..16.min(hash.len())]
            );
        }
        if let Some(commit) = body["rolled_back_from_commit"].as_str() {
            println!("  Source commit:  {}", commit);
        }
    } else {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();

        if body.contains("Destructive changes detected") {
            eprintln!("Rollback blocked: {}", body);
            eprintln!();
            eprintln!("Re-run with --allow-destructive to force:");
            eprintln!("  {}", forced(&service.name, target, env));
            std::process::exit(1);
        }

        eyre::bail!("Rollback failed ({}): {}", status, body);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::stub::answering;
    use super::*;
    use serde_json::json;

    const A: &str = "aaaaaaa1111111111111111111111111111111aa";
    const B: &str = "bbbbbbb2222222222222222222222222222222bb";

    fn deployment(version: i32, commit: Option<&str>) -> Deployment {
        Deployment {
            version,
            commit_hash: commit.map(str::to_string),
        }
    }

    fn history() -> Vec<Deployment> {
        vec![
            deployment(4, Some(B)),
            deployment(3, Some(A)),
            deployment(2, None),
            deployment(1, Some(A)),
        ]
    }

    fn at(version: i32, commit: &str) -> Result<At, NoSuchCommit> {
        Ok(At {
            version,
            commit: commit.to_string(),
        })
    }

    #[test]
    fn a_commit_names_the_newest_version_deployed_at_it_other_than_the_running_one() {
        assert_eq!(version_at_commit(&history(), A, 4), at(3, A), "newest wins");
        assert_eq!(
            version_at_commit(&history(), &A[..7], 4),
            at(3, A),
            "a prefix, answered with the commit it names"
        );
        assert_eq!(
            version_at_commit(&history(), &A[..12].to_ascii_uppercase(), 4),
            at(3, A),
            "in either case"
        );
        let oldest_first: Vec<_> = history().into_iter().rev().collect();
        assert_eq!(
            version_at_commit(&oldest_first, A, 4),
            at(3, A),
            "whatever order they are listed in"
        );
        assert_eq!(
            version_at_commit(&history(), B, 4),
            Err(NoSuchCommit::Running)
        );
        assert_eq!(
            version_at_commit(&history(), A, 3),
            Err(NoSuchCommit::Running),
            "a commit the running version carries is not rolled back to at an older one"
        );
        assert_eq!(
            version_at_commit(&history(), "ccccccc", 4),
            Err(NoSuchCommit::NotRetained)
        );
        assert_eq!(version_at_commit(&[], A, 0), Err(NoSuchCommit::NotRetained));
        assert_eq!(
            version_at_commit(&history(), &A[..6], 4),
            Err(NoSuchCommit::TooShort)
        );
    }

    #[test]
    fn a_prefix_of_two_deployed_commits_is_refused_unless_it_is_one_of_them() {
        let alike = [
            deployment(3, Some("abcdef1000")),
            deployment(2, Some("abcdef1999")),
            deployment(1, Some("abcdef1000")),
        ];
        assert_eq!(
            version_at_commit(&alike, "abcdef1", 3),
            Err(NoSuchCommit::Ambiguous(
                ["abcdef1000".to_string(), "abcdef1999".to_string()].into()
            ))
        );
        assert_eq!(
            version_at_commit(&alike, "abcdef19", 3),
            at(2, "abcdef1999")
        );

        let abbreviated = [
            deployment(3, Some("abcdef1999")),
            deployment(2, Some("abcdef1")),
            deployment(1, Some("0000000")),
        ];
        assert_eq!(
            version_at_commit(&abbreviated, "abcdef1", 1),
            at(2, "abcdef1"),
            "a version labelled with exactly what was given is the one meant"
        );
    }

    #[test]
    fn a_commit_that_is_not_rolled_back_to_is_told_why() {
        let told = |none: NoSuchCommit| none.told("abcdef1");
        assert_eq!(
            told(NoSuchCommit::NotRetained),
            "no retained version was deployed at abcdef1\n  `cufflink deployments list` shows \
             what is retained; deploying the commit's bundle gives the same code as a new \
             version"
        );
        assert_eq!(
            told(NoSuchCommit::Running),
            "abcdef1 is the running version"
        );
        assert_eq!(
            told(NoSuchCommit::Ambiguous(
                ["abcdef1000".to_string(), "abcdef1999".to_string()].into()
            )),
            "abcdef1 names more than one deployed commit (abcdef1000, abcdef1999): \
             give more of its hash"
        );
        let too_short =
            "'abc' is too short to name a commit: give at least 7 characters of its hash";
        assert_eq!(NoSuchCommit::TooShort.told("abc"), too_short);
        assert_eq!(commit_argument("abc"), Err(too_short.to_string()));
        assert_eq!(commit_argument("abcdef1"), Ok("abcdef1".to_string()));
    }

    #[test]
    fn a_rollback_is_to_the_version_or_the_commit_asked_for_or_the_one_before() {
        assert_eq!(To::asked(None, None), To::Previous);
        assert_eq!(To::asked(Some(3), None), To::Version(3));
        assert_eq!(
            To::asked(None, Some("abcdef1".into())),
            To::Commit("abcdef1".into())
        );
    }

    #[test]
    fn a_rollback_says_where_it_goes_and_how_to_force_the_same_one() {
        let by_version = Target {
            from: 7,
            version: 3,
            commit: None,
        };
        assert_eq!(
            announced("notes", &by_version),
            "Rolling back notes from v7 to v3..."
        );
        assert_eq!(
            announced(
                "notes",
                &Target {
                    commit: Some(A.to_string()),
                    ..by_version
                }
            ),
            format!("Rolling back notes from v7 to v3 (commit {A})...")
        );
        assert_eq!(
            forced("notes", 3, Some("production")),
            "cufflink rollback --version 3 --service notes --env production --allow-destructive",
            "the version that was found, not the flags it was found by"
        );
        assert_eq!(
            forced("notes", 3, None),
            "cufflink rollback --version 3 --service notes --allow-destructive"
        );
    }

    fn service(current_version: i32) -> Deployed {
        Deployed {
            name: "notes".into(),
            id: "7".into(),
            mode: Some(cufflink_types::ServiceMode::Wasm),
            current_version,
        }
    }

    #[tokio::test]
    async fn a_target_is_found_without_asking_unless_it_is_a_commit() {
        let listing = || {
            Some((
                200,
                json!({"deployments": [
                    {"version": 4, "commit_hash": B, "manifest": {"name": "notes"}},
                    {"version": 3, "commit_hash": A},
                    {"version": 2, "commit_hash": null},
                    {"version": 1},
                ], "total": 4, "current_version": 4}),
            ))
        };
        let (config, asked) =
            answering(vec![listing(), listing(), Some((200, json!({"total": 0})))]).await;
        let client = config.http_client();
        let found = |to: To, current: i32| {
            let (config, client) = (&config, &client);
            async move {
                target(config, client, &service(current), &to)
                    .await
                    .map(|target| (target.from, target.version, target.commit))
                    .map_err(|e| e.to_string())
            }
        };

        assert_eq!(found(To::Previous, 4).await, Ok((4, 3, None)));
        assert_eq!(found(To::Version(1), 4).await, Ok((4, 1, None)));
        assert_eq!(
            found(To::Previous, 1).await,
            Err("Cannot rollback: only one version exists (v1)".to_string())
        );
        assert_eq!(asked.count(), 0);

        assert_eq!(
            found(To::Commit(A[..9].to_string()), 4).await,
            Ok((4, 3, Some(A.to_string())))
        );
        assert_eq!(asked.requests(), ["GET /api/services/7/deployments"]);
        assert_eq!(
            found(To::Commit(B.to_string()), 3).await,
            Err(format!("{B} is the running version")),
            "the running version is the one the listing of versions names, not one read earlier"
        );
        assert!(found(To::Commit(A.to_string()), 4)
            .await
            .is_err_and(|unlisted| unlisted
                .starts_with("Failed to list deployments: the answer is not a listing (")));
    }
}
