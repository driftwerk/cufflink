use eyre::Result;
use std::path::Path;
use std::process::Command;

#[derive(Debug)]
pub struct GitHubSource {
    pub owner: String,
    pub repo: String,
    pub tag: Option<String>,
}

impl GitHubSource {
    pub fn parse(input: &str) -> Result<Self> {
        let rest = input.strip_prefix("github:").ok_or_else(|| {
            eyre::eyre!("Package source must start with 'github:' (e.g., github:owner/repo)")
        })?;

        let (repo_path, tag) = match rest.split_once('@') {
            Some((path, tag)) => (path, Some(tag.to_string())),
            None => (rest, None),
        };

        let (owner, repo) = repo_path
            .split_once('/')
            .ok_or_else(|| eyre::eyre!("Invalid format. Expected github:owner/repo[@tag]"))?;

        if owner.is_empty() || repo.is_empty() {
            eyre::bail!("Owner and repo cannot be empty. Expected github:owner/repo[@tag]");
        }

        Ok(Self {
            owner: owner.to_string(),
            repo: repo.to_string(),
            tag,
        })
    }

    pub fn display(&self) -> String {
        match &self.tag {
            Some(tag) => format!("github:{}/{}@{}", self.owner, self.repo, tag),
            None => format!("github:{}/{}", self.owner, self.repo),
        }
    }
}

fn gh_token() -> Option<String> {
    std::env::var("GH_TOKEN")
        .ok()
        .or_else(|| std::env::var("GITHUB_TOKEN").ok())
}

pub async fn download(source: &GitHubSource, dest: &Path) -> Result<()> {
    match download_tarball(source, dest).await {
        Ok(_) => Ok(()),
        Err(tarball_err) => {
            eprintln!("Tarball download failed, trying git clone...");
            clone_repo(source, dest).map_err(|clone_err| {
                eyre::eyre!(
                    "Failed to download package:\n  Tarball: {}\n  Clone: {}",
                    tarball_err,
                    clone_err
                )
            })
        }
    }
}

async fn download_tarball(source: &GitHubSource, dest: &Path) -> Result<()> {
    let git_ref = source.tag.as_deref().unwrap_or("HEAD");
    let url = format!(
        "https://api.github.com/repos/{}/{}/tarball/{}",
        source.owner, source.repo, git_ref
    );

    let client = reqwest::Client::new();
    let mut req = client
        .get(&url)
        .header("User-Agent", "cufflink-cli")
        .header("Accept", "application/vnd.github+json");

    if let Some(token) = gh_token() {
        req = req.header("Authorization", format!("Bearer {}", token));
    }

    let resp = req.send().await?;

    if resp.status() == reqwest::StatusCode::FORBIDDEN
        && resp
            .headers()
            .get("x-ratelimit-remaining")
            .and_then(|v| v.to_str().ok())
            == Some("0")
    {
        eyre::bail!(
            "GitHub API rate limit exceeded. Set GH_TOKEN or GITHUB_TOKEN to authenticate."
        );
    }

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        let hint = if gh_token().is_none() {
            " If this is a private repo, set GH_TOKEN or GITHUB_TOKEN."
        } else {
            ""
        };
        eyre::bail!("Repository or tag not found: {}.{}", source.display(), hint);
    }

    if !resp.status().is_success() {
        eyre::bail!("GitHub API error: {}", resp.status());
    }

    let bytes = resp.bytes().await?;

    let tarball = dest.join("_download.tar.gz");
    std::fs::create_dir_all(dest)?;
    std::fs::write(&tarball, &bytes)?;

    let output = Command::new("tar")
        .args(["xzf", &tarball.to_string_lossy(), "--strip-components=1"])
        .current_dir(dest)
        .output()?;

    std::fs::remove_file(&tarball)?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        eyre::bail!("Failed to extract tarball: {}", stderr);
    }

    Ok(())
}

fn clone_repo(source: &GitHubSource, dest: &Path) -> Result<()> {
    let url = format!("https://github.com/{}/{}.git", source.owner, source.repo);

    let dest_str = dest.to_string_lossy().to_string();
    let mut args = vec!["clone", "--depth", "1"];
    if let Some(tag) = &source.tag {
        args.extend(["--branch", tag]);
    }
    args.push(&url);
    args.push(&dest_str);

    let output = Command::new("git").args(&args).output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        eyre::bail!("git clone failed: {}", stderr);
    }

    let git_dir = dest.join(".git");
    if git_dir.exists() {
        std::fs::remove_dir_all(&git_dir)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic() {
        let source = GitHubSource::parse("github:driftwerk/cufflink-web").unwrap();
        assert_eq!(source.owner, "driftwerk");
        assert_eq!(source.repo, "cufflink-web");
        assert!(source.tag.is_none());
    }

    #[test]
    fn test_parse_with_tag() {
        let source = GitHubSource::parse("github:driftwerk/cufflink-web@v1.0.0").unwrap();
        assert_eq!(source.owner, "driftwerk");
        assert_eq!(source.repo, "cufflink-web");
        assert_eq!(source.tag.as_deref(), Some("v1.0.0"));
    }

    #[test]
    fn test_parse_missing_prefix() {
        assert!(GitHubSource::parse("driftwerk/cufflink-web").is_err());
    }

    #[test]
    fn test_parse_missing_repo() {
        assert!(GitHubSource::parse("github:driftwerk").is_err());
    }

    #[test]
    fn test_parse_empty_parts() {
        assert!(GitHubSource::parse("github:/repo").is_err());
        assert!(GitHubSource::parse("github:owner/").is_err());
    }

    #[test]
    fn test_display() {
        let source = GitHubSource::parse("github:owner/repo@v2").unwrap();
        assert_eq!(source.display(), "github:owner/repo@v2");

        let source = GitHubSource::parse("github:owner/repo").unwrap();
        assert_eq!(source.display(), "github:owner/repo");
    }
}
