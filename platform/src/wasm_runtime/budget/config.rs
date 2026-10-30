use super::caps::HostCaps;
use super::cgroup::CgroupLimit;
use super::ledger::Limits;
use crate::policy::PolicyMode;
use cufflink_db::Vars;
use eyre::{eyre, Result};
use std::time::Duration;

const MIB: usize = 1024 * 1024;
const MIN_RESERVE: usize = 256 * MIB;
const MIN_THREAD_STACK_KB: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetConfig {
    pub mode: PolicyMode,
    pub limits: Option<Limits>,
    pub limit: Option<CgroupLimit>,
    pub reserve: usize,
    pub per_service: usize,
    pub cold_estimate: usize,
    pub estimate_max_age: Duration,
    pub estimate_map_max: usize,
    pub committed_wait: Duration,
    pub background_aging: Duration,
    pub job_floor: usize,
    pub capacity_retries: u32,
    pub capacity_retries_job: u32,
    pub pressure_gate: bool,
    pub thread_stack: usize,
    pub caps: HostCaps,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            mode: PolicyMode::Warn,
            limits: None,
            limit: None,
            reserve: 0,
            per_service: 32,
            cold_estimate: 32 * MIB,
            estimate_max_age: Duration::from_secs(600),
            estimate_map_max: 4096,
            committed_wait: Duration::from_secs(300),
            background_aging: Duration::from_secs(30),
            job_floor: 32 * MIB,
            capacity_retries: 3,
            capacity_retries_job: 10,
            pressure_gate: true,
            thread_stack: 2048 * 1024,
            caps: HostCaps::default(),
        }
    }
}

fn percent(
    vars: &Vars<impl Fn(&str) -> Option<String>>,
    name: &str,
    default: usize,
) -> Result<usize> {
    match vars.positive(name, default)? {
        value @ 1..=100 => Ok(value),
        value => Err(eyre!("{name} must be between 1 and 100, not {value}")),
    }
}

pub fn thread_stack_bytes(var: impl Fn(&str) -> Option<String>) -> Result<usize> {
    match Vars(var).positive("WASM_THREAD_STACK_KB", 2048usize)? {
        kb @ MIN_THREAD_STACK_KB.. => Ok(kb * 1024),
        kb => Err(eyre!(
            "WASM_THREAD_STACK_KB must be at least {MIN_THREAD_STACK_KB}, not {kb}"
        )),
    }
}

impl BudgetConfig {
    pub fn from_vars(
        var: impl Fn(&str) -> Option<String>,
        limit: Option<CgroupLimit>,
        max_concurrent: usize,
    ) -> Result<Self> {
        let thread_stack = thread_stack_bytes(&var)?;
        let vars = Vars(var);
        let defaults = Self::default();
        let mode = vars.parsed("WASM_MEMORY_BUDGET_MODE", PolicyMode::Warn)?;
        let explicit = vars.get("WASM_MEMORY_BUDGET_MB").is_some();
        let budget_mb: usize = vars.positive("WASM_MEMORY_BUDGET_MB", 1)?;
        let limit_bytes = limit
            .as_ref()
            .map(|limit| usize::try_from(limit.bytes).unwrap_or(usize::MAX));
        let reserve = match vars.get("WASM_MEMORY_HOST_RESERVE_MB") {
            Some(_) => vars.positive("WASM_MEMORY_HOST_RESERVE_MB", 1usize)? * MIB,
            None => limit_bytes.map_or(MIN_RESERVE, |limit| (limit / 10 * 3).max(MIN_RESERVE)),
        };
        let budget = match (explicit, limit_bytes) {
            (true, _) => Some(budget_mb * MIB),
            (false, Some(limit)) => limit.checked_sub(reserve).filter(|budget| *budget > 0),
            (false, None) => None,
        };
        let service = percent(&vars, "WASM_MEMORY_SERVICE_SHARE_PERCENT", 50)?;
        let background = percent(&vars, "WASM_MEMORY_BACKGROUND_SHARE_PERCENT", 75)?;
        let admit = percent(&vars, "WASM_MEMORY_ADMIT_PERCENT", 90)?;
        let share = |bytes: usize, percent: usize| bytes / 100 * percent;
        Ok(Self {
            limits: budget
                .filter(|_| mode != PolicyMode::Off)
                .map(|budget| Limits {
                    budget,
                    admit: share(budget, admit),
                    service: share(budget, service),
                    background: share(budget, background),
                }),
            mode,
            limit,
            reserve,
            per_service: vars
                .positive("WASM_MAX_CONCURRENT_PER_SERVICE", defaults.per_service)?
                .min(max_concurrent),
            cold_estimate: vars.positive("WASM_MEMORY_COLD_ESTIMATE_MB", 32usize)? * MIB,
            estimate_max_age: Duration::from_secs(
                vars.positive("WASM_ESTIMATE_MAX_AGE_SECS", 600u64)?,
            ),
            estimate_map_max: vars.positive("WASM_ESTIMATE_MAP_MAX", defaults.estimate_map_max)?,
            committed_wait: Duration::from_secs(vars.positive("WASM_COMMITTED_WAIT_SECS", 300u64)?),
            background_aging: Duration::from_secs(
                vars.positive("WASM_BACKGROUND_AGING_SECS", 30u64)?,
            ),
            job_floor: vars.positive("WASM_MEMORY_JOB_FLOOR_MB", 32usize)? * MIB,
            capacity_retries: vars.positive("WASM_CAPACITY_RETRIES", defaults.capacity_retries)?,
            capacity_retries_job: vars
                .positive("WASM_CAPACITY_RETRIES_JOB", defaults.capacity_retries_job)?,
            pressure_gate: vars.parsed("WASM_MEMORY_PRESSURE_GATE", defaults.pressure_gate)?,
            thread_stack,
            caps: HostCaps::from_vars(&vars, mode)?,
        })
    }

    pub fn enforces(&self) -> bool {
        self.mode == PolicyMode::Enforce
    }

    pub fn clamped_memory_cap(&self, max_memory: usize) -> usize {
        match self.limits {
            Some(limits) if self.enforces() => max_memory.min(limits.service),
            _ => max_memory,
        }
    }

    pub fn log_startup(&self, max_memory: usize) {
        match (&self.limits, &self.limit) {
            (None, _) if self.mode == PolicyMode::Off => {
                tracing::info!(target: "cufflink::wasm::budget", "Guest memory budget is off")
            }
            (None, None) => tracing::warn!(
                target: "cufflink::wasm::budget",
                mode = %self.mode,
                "No memory limit found for this process and WASM_MEMORY_BUDGET_MB is unset: the guest memory budget is off"
            ),
            (None, Some(limit)) => tracing::warn!(
                target: "cufflink::wasm::budget",
                limit = limit.bytes,
                reserve = self.reserve,
                "The memory limit leaves nothing beside the host reserve: the guest memory budget is off"
            ),
            (Some(limits), limit) => {
                tracing::info!(
                    target: "cufflink::wasm::budget",
                    mode = %self.mode,
                    limit = limit.as_ref().map(|limit| limit.bytes),
                    source = limit.as_ref().map(|limit| limit.file.display().to_string()),
                    reserve = self.reserve,
                    budget = limits.budget,
                    service_share = limits.service,
                    background_share = limits.background,
                    admit = limits.admit,
                    "Guest memory budget"
                );
                if max_memory > limits.service {
                    let clamped = self.enforces();
                    tracing::warn!(
                        target: "cufflink::wasm::budget",
                        max_memory,
                        service_share = limits.service,
                        clamped,
                        "WASM_MAX_MEMORY_MB is above a service's share of the guest memory budget; \
                         under enforce the per-guest cap is clamped to the share"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::cgroup::LimitSource;
    use super::*;
    use std::path::PathBuf;

    fn limit(mib: u64) -> Option<CgroupLimit> {
        Some(CgroupLimit {
            bytes: mib * MIB as u64,
            source: LimitSource::Max,
            file: PathBuf::from("/sys/fs/cgroup/memory.max"),
            dir: None,
        })
    }

    fn config(pairs: &[(&str, &str)], cgroup: Option<CgroupLimit>) -> Result<BudgetConfig> {
        BudgetConfig::from_vars(cufflink_settings::fixed(pairs), cgroup, 256)
    }

    #[test]
    fn the_budget_is_the_limit_less_the_reserve() {
        let derived = config(&[], limit(2048)).expect("config");
        assert_eq!(derived.mode, PolicyMode::Warn);
        assert_eq!(derived.reserve, 2048 * MIB / 10 * 3);
        let limits = derived.limits.expect("a budget");
        assert_eq!(limits.budget, 2048 * MIB - 2048 * MIB / 10 * 3);
        assert_eq!(limits.service, limits.budget / 100 * 50);
        assert_eq!(limits.background, limits.budget / 100 * 75);
        assert_eq!(limits.admit, limits.budget / 100 * 90);

        let small = config(&[], limit(768)).expect("config");
        assert_eq!(small.reserve, 256 * MIB, "the reserve is at least 256 MiB");
        assert_eq!(small.limits.map(|l| l.budget), Some(512 * MIB));

        let explicit = config(
            &[
                ("WASM_MEMORY_BUDGET_MB", "128"),
                ("WASM_MEMORY_HOST_RESERVE_MB", "64"),
            ],
            None,
        )
        .expect("config");
        assert_eq!(explicit.limits.map(|l| l.budget), Some(128 * MIB));
        assert_eq!(explicit.reserve, 64 * MIB);
    }

    #[test]
    fn no_limit_or_nothing_left_turns_the_budget_off_but_an_explicit_zero_refuses() {
        assert_eq!(config(&[], None).expect("config").limits, None);
        assert_eq!(
            config(&[], limit(200)).expect("config").limits,
            None,
            "a limit below the reserve leaves no budget"
        );
        assert_eq!(
            config(&[("WASM_MEMORY_BUDGET_MODE", "off")], limit(2048))
                .expect("config")
                .limits,
            None
        );
        let err = config(&[("WASM_MEMORY_BUDGET_MB", "0")], limit(2048)).unwrap_err();
        assert!(err.to_string().contains("WASM_MEMORY_BUDGET_MB"), "{err}");
    }

    #[test]
    fn malformed_values_refuse_startup() {
        for (name, value) in [
            ("WASM_MEMORY_BUDGET_MODE", "strict"),
            ("WASM_MEMORY_BUDGET_MB", "lots"),
            ("WASM_MEMORY_SERVICE_SHARE_PERCENT", "0"),
            ("WASM_MEMORY_ADMIT_PERCENT", "101"),
            ("WASM_MAX_CONCURRENT_PER_SERVICE", "0"),
            ("WASM_THREAD_STACK_KB", "512"),
            ("WASM_COMMITTED_WAIT_SECS", "-1"),
        ] {
            let err = config(&[(name, value)], limit(2048)).unwrap_err();
            assert!(err.to_string().contains(name), "{name}: {err}");
        }
    }

    #[test]
    fn the_per_guest_cap_is_clamped_to_the_service_share_under_enforce_only() {
        let warn = config(&[("WASM_MEMORY_BUDGET_MB", "512")], None).expect("config");
        assert_eq!(warn.clamped_memory_cap(512 * MIB), 512 * MIB);
        let enforce = config(
            &[
                ("WASM_MEMORY_BUDGET_MB", "512"),
                ("WASM_MEMORY_BUDGET_MODE", "enforce"),
            ],
            None,
        )
        .expect("config");
        assert_eq!(enforce.clamped_memory_cap(512 * MIB), 512 * MIB / 100 * 50);
        assert_eq!(enforce.clamped_memory_cap(64 * MIB), 64 * MIB);
    }

    #[test]
    fn the_per_service_limit_is_at_most_the_global_one() {
        let config =
            BudgetConfig::from_vars(cufflink_settings::fixed(&[]), None, 8).expect("config");
        assert_eq!(config.per_service, 8);
    }
}
