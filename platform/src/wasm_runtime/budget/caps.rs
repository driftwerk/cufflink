use crate::policy::record::{LogThrottle, LOG_THROTTLE};
use crate::policy::PolicyMode;
use cufflink_db::Vars;
use eyre::Result;
use std::sync::LazyLock;

const MIB: usize = 1024 * 1024;
pub(crate) const S3_HOST_MULTIPLE: (usize, usize) = (7, 3);
pub(crate) const IMAGE_BYTES_PER_PIXEL: u64 = 20;
pub(crate) const IMAGE_MAX_SIDE: u32 = 16_384;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostCaps {
    pub mode: PolicyMode,
    pub response: usize,
    pub fanout_items: usize,
    pub image_input: usize,
    pub image_pixels: u64,
    pub image_targets: usize,
}

impl Default for HostCaps {
    fn default() -> Self {
        Self {
            mode: PolicyMode::Warn,
            response: 64 * MIB,
            fanout_items: 128,
            image_input: 32 * MIB,
            image_pixels: 40_000_000,
            image_targets: 16,
        }
    }
}

static LOGGED: LazyLock<LogThrottle<(&'static str, &'static str)>> =
    LazyLock::new(|| LogThrottle::new(LOG_THROTTLE));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapExceeded {
    pub path: &'static str,
    pub cap: &'static str,
    pub size: u64,
    pub limit: u64,
}

impl std::fmt::Display for CapExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} is over the host {} cap: {} above {}",
            self.path, self.cap, self.size, self.limit
        )
    }
}

impl HostCaps {
    pub(super) fn from_vars(
        vars: &Vars<impl Fn(&str) -> Option<String>>,
        mode: PolicyMode,
    ) -> Result<Self> {
        let defaults = Self::default();
        Ok(Self {
            mode,
            response: vars.positive("WASM_MAX_HOST_RESPONSE_MB", defaults.response / MIB)? * MIB,
            fanout_items: vars.positive("HOST_FANOUT_MAX_ITEMS", defaults.fanout_items)?,
            image_input: vars.positive("IMAGE_MAX_INPUT_MB", defaults.image_input / MIB)? * MIB,
            image_pixels: vars.positive("IMAGE_MAX_PIXELS", defaults.image_pixels)?,
            image_targets: vars.positive("IMAGE_MAX_TARGETS", defaults.image_targets)?,
        })
    }

    pub fn enforces(&self) -> bool {
        self.mode == PolicyMode::Enforce
    }

    pub fn check(
        &self,
        path: &'static str,
        cap: &'static str,
        size: u64,
        limit: u64,
    ) -> Result<(), CapExceeded> {
        if self.mode == PolicyMode::Off || size <= limit {
            return Ok(());
        }
        let exceeded = CapExceeded {
            path,
            cap,
            size,
            limit,
        };
        metrics::counter!("cufflink_wasm_host_cap_total", "path" => path, "cap" => cap)
            .increment(1);
        if LOGGED.should_log((path, cap), std::time::Instant::now()) {
            tracing::warn!(
                target: "cufflink::wasm::budget",
                path,
                cap,
                size,
                limit,
                mode = self.mode.as_str(),
                "A host call passed a host cap"
            );
        }
        match self.enforces() {
            true => Err(exceeded),
            false => Ok(()),
        }
    }

    pub fn response(&self, path: &'static str, size: usize) -> Result<(), CapExceeded> {
        self.check(path, "response", size as u64, self.response as u64)
    }

    pub fn fanout(&self, path: &'static str, items: usize) -> Result<(), CapExceeded> {
        self.check(path, "fanout", items as u64, self.fanout_items as u64)
    }

    pub fn response_limit(&self) -> Option<usize> {
        self.enforces().then_some(self.response)
    }

    pub fn image_pixels(&self, share: Option<usize>) -> u64 {
        share.map_or(self.image_pixels, |share| {
            self.image_pixels.min(share as u64 / IMAGE_BYTES_PER_PIXEL)
        })
    }
}

pub(crate) fn s3_host_bytes(size: u64) -> usize {
    let (num, den) = S3_HOST_MULTIPLE;
    usize::try_from(size)
        .unwrap_or(usize::MAX)
        .saturating_mul(num)
        .div_ceil(den)
}

pub(crate) fn base64_len(len: usize) -> usize {
    len.div_ceil(3).saturating_mul(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps(mode: PolicyMode) -> HostCaps {
        HostCaps {
            mode,
            ..HostCaps::default()
        }
    }

    #[test]
    fn the_mode_governs_every_cap() {
        let over = 64 * MIB + 1;
        assert!(caps(PolicyMode::Enforce)
            .response("db_query", over)
            .is_err());
        assert!(caps(PolicyMode::Warn).response("db_query", over).is_ok());
        assert!(caps(PolicyMode::Off).response("db_query", over).is_ok());
        assert!(caps(PolicyMode::Enforce)
            .response("db_query", 64 * MIB)
            .is_ok());
        assert!(caps(PolicyMode::Enforce)
            .fanout("http_fetch_many", 128)
            .is_ok());
        assert!(caps(PolicyMode::Enforce)
            .fanout("http_fetch_many", 129)
            .is_err());
        assert!(caps(PolicyMode::Warn)
            .fanout("http_fetch_many", 129)
            .is_ok());
        assert_eq!(caps(PolicyMode::Warn).response_limit(), None);
        assert_eq!(caps(PolicyMode::Enforce).response_limit(), Some(64 * MIB));
    }

    #[test]
    fn the_pixel_cap_is_the_smaller_of_the_setting_and_the_share() {
        let caps = caps(PolicyMode::Enforce);
        assert_eq!(caps.image_pixels(None), 40_000_000);
        assert_eq!(caps.image_pixels(Some(4096 * MIB)), 40_000_000);
        let share = 742 * MIB;
        assert_eq!(caps.image_pixels(Some(share)), share as u64 / 20);
    }

    #[test]
    fn the_declared_multiples() {
        assert_eq!(s3_host_bytes(3), 7);
        assert_eq!(s3_host_bytes(48 * MIB as u64), 112 * MIB);
        assert_eq!(base64_len(0), 0);
        assert_eq!(base64_len(1), 4);
        assert_eq!(base64_len(48 * MIB), 64 * MIB);
    }

    #[test]
    fn variables_override_and_zero_refuses() {
        let vars = Vars(cufflink_settings::fixed(&[
            ("WASM_MAX_HOST_RESPONSE_MB", "8"),
            ("HOST_FANOUT_MAX_ITEMS", "4"),
            ("IMAGE_MAX_TARGETS", "2"),
        ]));
        let caps = HostCaps::from_vars(&vars, PolicyMode::Enforce).expect("caps");
        assert_eq!(
            (caps.response, caps.fanout_items, caps.image_targets),
            (8 * MIB, 4, 2)
        );
        for name in [
            "WASM_MAX_HOST_RESPONSE_MB",
            "HOST_FANOUT_MAX_ITEMS",
            "IMAGE_MAX_INPUT_MB",
            "IMAGE_MAX_PIXELS",
            "IMAGE_MAX_TARGETS",
        ] {
            let vars = Vars(cufflink_settings::fixed(&[(name, "0")]));
            let err = HostCaps::from_vars(&vars, PolicyMode::Warn).unwrap_err();
            assert!(err.to_string().contains(name), "{name}: {err}");
        }
    }
}
