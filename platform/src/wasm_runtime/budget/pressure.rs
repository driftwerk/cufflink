use super::cgroup::CgroupLimit;
use super::ledger::Ledger;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

const EVERY: Duration = Duration::from_millis(250);
const EVENTS: [&str; 4] = ["high", "max", "oom", "oom_kill"];

fn field(text: &str, name: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (key, value) = line.split_once(' ')?;
        (key == name).then(|| value.trim().parse().ok())?
    })
}

fn unreclaimable(stat: &str, current: Option<u64>) -> Option<u64> {
    let of = |name| field(stat, name);
    match (of("anon"), of("shmem"), of("kernel")) {
        (Some(anon), Some(shmem), Some(kernel)) => Some(anon + shmem + kernel),
        _ => Some(
            current?
                .saturating_sub(of("active_file")?)
                .saturating_sub(of("inactive_file")?),
        ),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Reading {
    current: Option<u64>,
    unreclaimable: u64,
    events: Vec<(&'static str, u64)>,
}

fn read(dir: &Path) -> Option<Reading> {
    let text = |file: &str| std::fs::read_to_string(dir.join(file)).ok();
    let current = text("memory.current").and_then(|value| value.trim().parse().ok());
    let unreclaimable = unreclaimable(&text("memory.stat")?, current)?;
    let events = text("memory.events").unwrap_or_default();
    Some(Reading {
        current,
        unreclaimable,
        events: EVENTS
            .into_iter()
            .filter_map(|event| Some((event, field(&events, event)?)))
            .collect(),
    })
}

fn publish(reading: &Reading) {
    if let Some(current) = reading.current {
        metrics::gauge!("cufflink_process_memory_current_bytes").set(current as f64);
    }
    metrics::gauge!("cufflink_process_memory_unreclaimable_bytes")
        .set(reading.unreclaimable as f64);
    for (event, count) in &reading.events {
        metrics::counter!("cufflink_process_memory_events_total", "event" => *event)
            .absolute(*count);
    }
}

fn watch(dir: &Path, ledger: &Weak<Ledger>, ceiling: Option<u64>) {
    while let Some(ledger) = ledger.upgrade() {
        if let Some(reading) = read(dir) {
            publish(&reading);
            ledger.set_pressure(ceiling.is_some_and(|ceiling| reading.unreclaimable > ceiling));
        }
        drop(ledger);
        std::thread::sleep(EVERY);
    }
}

pub(super) fn start(
    limit: &CgroupLimit,
    ledger: &Arc<Ledger>,
    reserve: usize,
    gated: bool,
) -> eyre::Result<()> {
    metrics::gauge!("cufflink_process_memory_limit_bytes", "source" => limit.source.label())
        .set(limit.bytes as f64);
    let Some(dir) = limit.dir.clone() else {
        return Ok(());
    };
    let ceiling = gated.then(|| limit.bytes.saturating_sub(reserve as u64 / 2));
    let ledger = Arc::downgrade(ledger);
    spawn(dir, ledger, ceiling)
}

fn spawn(dir: PathBuf, ledger: Weak<Ledger>, ceiling: Option<u64>) -> eyre::Result<()> {
    std::thread::Builder::new()
        .name("wasm-pressure".into())
        .spawn(move || watch(&dir, &ledger, ceiling))
        .map(drop)
        .map_err(|e| eyre::eyre!("spawn the memory pressure gate: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_gate_reads_what_the_kernel_cannot_reclaim() {
        let stat = "anon 100\nfile 900\nkernel 20\nshmem 5\nactive_file 300\ninactive_file 200\n";
        assert_eq!(unreclaimable(stat, Some(1000)), Some(125));
        let no_kernel = "anon 100\nshmem 5\nactive_file 300\ninactive_file 200\n";
        assert_eq!(
            unreclaimable(no_kernel, Some(1000)),
            Some(500),
            "without `kernel`, memory.current less the file cache"
        );
        assert_eq!(unreclaimable(no_kernel, None), None);
    }

    #[test]
    fn a_cgroup_directory_is_read_into_one_reading() {
        let dir = tempfile::tempdir().expect("tempdir");
        for (file, text) in [
            ("memory.current", "1000\n"),
            ("memory.stat", "anon 10\nshmem 2\nkernel 3\n"),
            ("memory.events", "low 0\nhigh 4\nmax 2\noom 0\noom_kill 1\n"),
        ] {
            std::fs::write(dir.path().join(file), text).expect("write");
        }
        assert_eq!(
            read(dir.path()),
            Some(Reading {
                current: Some(1000),
                unreclaimable: 15,
                events: vec![("high", 4), ("max", 2), ("oom", 0), ("oom_kill", 1)],
            })
        );
        assert_eq!(read(&dir.path().join("gone")), None);
    }
}
