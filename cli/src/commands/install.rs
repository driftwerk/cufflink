use crate::github::GitHubSource;
use crate::package_manifest::PackageManifest;
use std::path::Path;

const PACKAGES_DIR: &str = "cufflink-packages";

pub async fn run(source: &str) -> eyre::Result<()> {
    let (source_dir, _temp) = if source.starts_with("github:") {
        let gh = GitHubSource::parse(source)?;
        println!("Installing {}...", gh.display());
        let temp = tempfile::tempdir()?;
        crate::github::download(&gh, temp.path()).await?;
        (temp.path().to_path_buf(), Some(temp))
    } else {
        let path = std::path::PathBuf::from(shellexpand::tilde(source).as_ref());
        if !path.exists() {
            eyre::bail!("Path '{}' does not exist", path.display());
        }
        println!("Installing from {}...", path.display());
        (path, None)
    };

    let manifest = PackageManifest::load(&source_dir)?;
    manifest.validate_paths(&source_dir)?;

    let packages_dir = std::env::current_dir()?.join(PACKAGES_DIR);
    let dest = packages_dir.join(&manifest.package.name);

    let reinstall = dest.exists();
    if reinstall {
        print!(
            "Package '{}' already installed. Overwrite? [y/N] ",
            manifest.package.name
        );
        use std::io::Write;
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    std::fs::create_dir_all(&packages_dir)?;

    let src = format!("{}/", source_dir.to_string_lossy());
    let dest_str = format!("{}/", dest.to_string_lossy());
    let mut args = vec![
        "-a",
        "--exclude=node_modules",
        "--exclude=.next",
        "--exclude=target",
        "--exclude=.git",
    ];
    if reinstall {
        args.extend(["--delete", "--exclude=Cufflink.toml", "--exclude=secrets"]);
    }
    args.push(&src);
    args.push(&dest_str);

    let output = std::process::Command::new("rsync").args(&args).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        eyre::bail!("Failed to copy package: {}", stderr);
    }

    update_lockfile(&packages_dir, &manifest, source)?;

    println!();
    manifest.print_summary();

    check_pnpm_workspace(&dest);

    println!("Installed to {}/{}/", PACKAGES_DIR, manifest.package.name);
    println!();
    println!("Next steps:");
    println!(
        "  1. See {}/{}/README.md for configuration",
        PACKAGES_DIR, manifest.package.name
    );
    println!("  2. cufflink deploy-package {}", manifest.package.name);

    Ok(())
}

fn check_pnpm_workspace(dest: &Path) {
    use crate::pnpm::{check_workspace_membership, WorkspaceCheck};

    if let WorkspaceCheck::NotIncluded {
        workspace_file,
        suggested_glob,
        ..
    } = check_workspace_membership(dest)
    {
        println!("WARNING: This package was installed inside a pnpm workspace");
        println!(
            "  but \"{}\" is not listed in {}",
            suggested_glob,
            workspace_file.display()
        );
        println!();
        println!("  pnpm install will not install dependencies for this package.");
        println!(
            "  Add \"{}\" to the packages list in {}",
            suggested_glob,
            workspace_file.display()
        );
        println!();
    }
}

fn update_lockfile(
    packages_dir: &Path,
    manifest: &PackageManifest,
    source: &str,
) -> eyre::Result<()> {
    let lockfile = packages_dir.join("cufflink-packages.toml");
    let mut content = std::fs::read_to_string(&lockfile).unwrap_or_default();

    let version = manifest.package.version.as_deref().unwrap_or("0.0.0");
    let entry = format!(
        "\n[{}]\nversion = \"{}\"\nsource = \"{}\"\ninstalled_at = \"{}\"\n",
        manifest.package.name,
        version,
        source,
        chrono::Utc::now().to_rfc3339(),
    );

    if let Some(start) = content.find(&format!("[{}]", manifest.package.name)) {
        let end = content[start + 1..]
            .find("\n[")
            .map(|i| start + 1 + i)
            .unwrap_or(content.len());
        content.replace_range(start..end, "");
    }

    content.push_str(&entry);
    std::fs::write(&lockfile, content.trim_start())?;
    Ok(())
}
