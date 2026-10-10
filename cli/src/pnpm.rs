use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum WorkspaceCheck {
    NoWorkspace,
    Included,
    NotIncluded {
        workspace_file: PathBuf,
        suggested_glob: String,
    },
}

pub fn check_workspace_membership(dir: &Path) -> WorkspaceCheck {
    let dir = match dir.canonicalize() {
        Ok(d) => d,
        Err(_) => return WorkspaceCheck::NoWorkspace,
    };

    let (workspace_root, workspace_file, globs) = match find_workspace_config(&dir) {
        Some(result) => result,
        None => return WorkspaceCheck::NoWorkspace,
    };

    let rel_path = match dir.strip_prefix(&workspace_root) {
        Ok(rel) => rel.to_string_lossy().to_string(),
        Err(_) => return WorkspaceCheck::NoWorkspace,
    };

    let rel_path = rel_path.replace('\\', "/");

    for glob in &globs {
        if glob_match::glob_match(glob, &rel_path) {
            return WorkspaceCheck::Included;
        }
    }

    let suggested_glob = rel_path
        .split('/')
        .next()
        .map(|first| format!("{}/*", first))
        .unwrap_or_else(|| format!("{}/*", rel_path));

    WorkspaceCheck::NotIncluded {
        workspace_file,
        suggested_glob,
    }
}

#[derive(serde::Deserialize, Default)]
struct PnpmWorkspace {
    #[serde(default)]
    packages: Vec<String>,
}

fn find_workspace_config(start: &Path) -> Option<(PathBuf, PathBuf, Vec<String>)> {
    let mut dir = start.to_path_buf();
    loop {
        let candidate = dir.join("pnpm-workspace.yaml");
        if candidate.is_file() {
            let content = std::fs::read_to_string(&candidate).ok()?;
            let workspace: PnpmWorkspace = serde_yaml::from_str(&content).unwrap_or_default();
            return Some((dir, candidate, workspace.packages));
        }
        if !dir.pop() {
            break;
        }
    }
    None
}
