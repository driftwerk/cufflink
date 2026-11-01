use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitSource {
    Max,
    High,
    V1,
}

impl LimitSource {
    pub fn label(self) -> &'static str {
        match self {
            LimitSource::Max => "max",
            LimitSource::High => "high",
            LimitSource::V1 => "v1",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CgroupLimit {
    pub bytes: u64,
    pub source: LimitSource,
    pub file: PathBuf,
    pub dir: Option<PathBuf>,
}

struct Mount {
    root: String,
    point: String,
    fstype: String,
    options: String,
}

fn mounts(root: &Path) -> Vec<Mount> {
    read(root, "/proc/self/mountinfo")
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            let (before, after) = line.split_once(" - ")?;
            let fields: Vec<&str> = before.split_whitespace().collect();
            let mut after = after.split_whitespace();
            Some(Mount {
                root: fields.get(3)?.to_string(),
                point: fields.get(4)?.to_string(),
                fstype: after.next()?.to_string(),
                options: after.nth(1).unwrap_or_default().to_string(),
            })
        })
        .collect()
}

fn read(root: &Path, path: &str) -> Option<String> {
    std::fs::read_to_string(root.join(path.trim_start_matches('/'))).ok()
}

fn lists_memory(controllers: &str) -> bool {
    controllers.split([' ', ',', '\n']).any(|c| c == "memory")
}

fn cgroup_dir(mount: &Mount, path: &str) -> String {
    let relative = match mount.root.as_str() {
        "/" => path,
        root => path.strip_prefix(root).unwrap_or(path),
    };
    format!(
        "{}/{}",
        mount.point.trim_end_matches('/'),
        relative.trim_start_matches('/')
    )
    .trim_end_matches('/')
    .to_string()
}

fn own_cgroup(cgroups: &str, v2: bool) -> Option<&str> {
    cgroups.lines().find_map(|line| {
        let mut parts = line.splitn(3, ':');
        let (id, controllers, path) = (parts.next()?, parts.next()?, parts.next()?);
        let matches = match v2 {
            true => id == "0" && controllers.is_empty(),
            false => controllers.split(',').any(|c| c == "memory"),
        };
        matches.then_some(path)
    })
}

fn parse_limit(text: &str) -> Option<u64> {
    match text.trim() {
        "max" => None,
        value => value.parse().ok(),
    }
}

fn v2_limit(root: &Path, mount: &Mount, path: &str) -> Option<CgroupLimit> {
    let top = mount.point.trim_end_matches('/').to_string();
    let mut dir = cgroup_dir(mount, path);
    if !read(root, &format!("{dir}/cgroup.controllers")).is_some_and(|c| lists_memory(&c)) {
        return None;
    }
    let mut best: Option<CgroupLimit> = None;
    loop {
        for (file, source) in [
            ("memory.max", LimitSource::Max),
            ("memory.high", LimitSource::High),
        ] {
            let at = format!("{dir}/{file}");
            if let Some(bytes) = read(root, &at).as_deref().and_then(parse_limit) {
                if best.as_ref().is_none_or(|best| bytes < best.bytes) {
                    best = Some(CgroupLimit {
                        bytes,
                        source,
                        file: PathBuf::from(at),
                        dir: Some(root.join(dir.trim_start_matches('/'))),
                    });
                }
            }
        }
        if dir.len() <= top.len() {
            return best;
        }
        dir = dir
            .rsplit_once('/')
            .map_or_else(|| top.clone(), |(parent, _)| parent.to_string());
        if dir.len() < top.len() {
            dir = top.clone();
        }
    }
}

fn mem_total(root: &Path) -> Option<u64> {
    read(root, "/proc/meminfo")?.lines().find_map(|line| {
        let kib = line.strip_prefix("MemTotal:")?.trim().strip_suffix("kB")?;
        kib.trim().parse::<u64>().ok().map(|kib| kib * 1024)
    })
}

fn v1_limit(root: &Path, mount: &Mount, path: &str) -> Option<CgroupLimit> {
    let file = format!("{}/memory.stat", cgroup_dir(mount, path));
    let bytes = read(root, &file)?.lines().find_map(|line| {
        line.strip_prefix("hierarchical_memory_limit ")?
            .trim()
            .parse::<u64>()
            .ok()
    })?;
    let unlimited = mem_total(root).is_none_or(|total| bytes >= total);
    (!unlimited).then(|| CgroupLimit {
        bytes,
        source: LimitSource::V1,
        file: PathBuf::from(file),
        dir: None,
    })
}

pub fn detect(root: &Path) -> Option<CgroupLimit> {
    let mounts = mounts(root);
    let cgroups = read(root, "/proc/self/cgroup")?;
    let unified = mounts.iter().find(|mount| {
        mount.fstype == "cgroup2"
            && read(root, &format!("{}/cgroup.controllers", mount.point))
                .is_some_and(|c| lists_memory(&c))
    });
    match unified {
        Some(mount) => v2_limit(root, mount, own_cgroup(&cgroups, true)?),
        None => {
            let mount = mounts
                .iter()
                .find(|mount| mount.fstype == "cgroup" && lists_memory(&mount.options))?;
            v1_limit(root, mount, own_cgroup(&cgroups, false)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn tree(files: &[(&str, &str)]) -> tempfile::TempDir {
        let root = tempfile::tempdir().expect("tempdir");
        for (path, content) in files {
            let at = root.path().join(path.trim_start_matches('/'));
            std::fs::create_dir_all(at.parent().expect("parent")).expect("dirs");
            std::fs::write(at, content).expect("write");
        }
        root
    }

    const UNIFIED: &str =
        "30 25 0:26 / /sys/fs/cgroup rw,nosuid shared:4 - cgroup2 cgroup2 rw,nsdelegate\n";

    #[test]
    fn v2_takes_the_smallest_max_or_high_over_the_cgroup_and_its_ancestors() {
        let root = tree(&[
            ("/proc/self/mountinfo", UNIFIED),
            ("/proc/self/cgroup", "0::/system.slice/app.scope\n"),
            ("/sys/fs/cgroup/cgroup.controllers", "cpu memory pids"),
            ("/sys/fs/cgroup/system.slice/cgroup.controllers", "memory"),
            (
                "/sys/fs/cgroup/system.slice/memory.max",
                &format!("{}\n", 1024 * MIB),
            ),
            ("/sys/fs/cgroup/system.slice/memory.high", "max\n"),
            (
                "/sys/fs/cgroup/system.slice/app.scope/cgroup.controllers",
                "memory",
            ),
            ("/sys/fs/cgroup/system.slice/app.scope/memory.max", "max\n"),
            ("/sys/fs/cgroup/system.slice/app.scope/memory.high", "max\n"),
        ]);
        let limit = detect(root.path()).expect("a limit");
        assert_eq!(limit.bytes, 1024 * MIB, "the parent's limit binds");
        assert_eq!(limit.source, LimitSource::Max);
        assert!(limit.file.ends_with("system.slice/memory.max"), "{limit:?}");
        assert_eq!(
            limit.dir,
            Some(root.path().join("sys/fs/cgroup/system.slice")),
            "the gate reads the cgroup whose limit binds"
        );

        std::fs::write(
            root.path()
                .join("sys/fs/cgroup/system.slice/app.scope/memory.high"),
            format!("{}", 768 * MIB),
        )
        .expect("write");
        let limit = detect(root.path()).expect("a limit");
        assert_eq!(
            (limit.bytes, limit.source),
            (768 * MIB, LimitSource::High),
            "memory.high below memory.max binds"
        );
        assert_eq!(
            limit.dir,
            Some(root.path().join("sys/fs/cgroup/system.slice/app.scope"))
        );
    }

    #[test]
    fn v2_resolves_the_cgroup_against_a_mount_of_a_namespace_root() {
        let root = tree(&[
            (
                "/proc/self/mountinfo",
                "30 25 0:26 /docker/abc /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n",
            ),
            ("/proc/self/cgroup", "0::/docker/abc\n"),
            ("/sys/fs/cgroup/cgroup.controllers", "memory"),
            ("/sys/fs/cgroup/memory.max", &format!("{}", 512 * MIB)),
        ]);
        assert_eq!(detect(root.path()).map(|l| l.bytes), Some(512 * MIB));
    }

    #[test]
    fn unlimited_or_absent_cgroups_have_no_limit() {
        let unlimited = tree(&[
            ("/proc/self/mountinfo", UNIFIED),
            ("/proc/self/cgroup", "0::/\n"),
            ("/sys/fs/cgroup/cgroup.controllers", "memory"),
            ("/sys/fs/cgroup/memory.max", "max\n"),
        ]);
        assert_eq!(detect(unlimited.path()), None);
        assert_eq!(detect(tree(&[]).path()), None, "no cgroup at all");
    }

    #[test]
    fn a_hybrid_host_uses_the_v1_memory_hierarchy() {
        let hybrid = "30 25 0:26 / /sys/fs/cgroup/unified rw - cgroup2 cgroup2 rw\n\
                      31 25 0:27 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n";
        let root = tree(&[
            ("/proc/self/mountinfo", hybrid),
            ("/proc/self/cgroup", "5:memory:/app\n0::/app\n"),
            ("/sys/fs/cgroup/unified/cgroup.controllers", ""),
            ("/proc/meminfo", "MemTotal:       8388608 kB\n"),
            (
                "/sys/fs/cgroup/memory/app/memory.stat",
                &format!("cache 0\nhierarchical_memory_limit {}\n", 2048 * MIB),
            ),
        ]);
        let limit = detect(root.path()).expect("a limit");
        assert_eq!((limit.bytes, limit.source), (2048 * MIB, LimitSource::V1));

        std::fs::write(
            root.path().join("sys/fs/cgroup/memory/app/memory.stat"),
            "hierarchical_memory_limit 9223372036854771712\n",
        )
        .expect("write");
        assert_eq!(
            detect(root.path()),
            None,
            "at or above MemTotal is unlimited"
        );
    }
}
