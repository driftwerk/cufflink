//! Utility functions for common operations in WASM handlers.

#[cfg(target_arch = "wasm32")]
use crate::{ffi, host_buffer};

/// Get the current UTC time as an RFC3339 string.
///
/// In WASM, this calls the platform host function. Outside WASM, it is the
/// system's time, or the installed test host's.
///
/// ```rust,ignore
/// let now = util::current_time();
/// log::info(&format!("Current time: {}", now));
/// ```
pub fn current_time() -> String {
    #[cfg(target_arch = "wasm32")]
    {
        if unsafe { ffi::current_time() } < 0 {
            return String::new();
        }
        host_buffer::read_host_string().unwrap_or_default()
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::clock().map_or_else(|| rfc3339(system_millis()), |clock| clock.now_rfc3339())
    }
}

/// Generate a new random UUID v4 string.
///
/// ```rust,ignore
/// let id = util::generate_uuid();
/// log::info(&format!("New ID: {}", id));
/// ```
pub fn generate_uuid() -> String {
    #[cfg(target_arch = "wasm32")]
    {
        if unsafe { ffi::generate_uuid() } < 0 {
            return String::new();
        }
        host_buffer::read_host_string().unwrap_or_default()
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::clock().map_or_else(|| uuid::Uuid::new_v4().to_string(), |clock| clock.uuid())
    }
}

/// The longest a native [`sleep_ms`] lasts, as the platform's default
/// `UTIL_SLEEP_MAX_MS`.
#[cfg(all(feature = "util-v2", not(target_arch = "wasm32")))]
const NATIVE_SLEEP_MAX_MS: u32 = 30_000;

/// Sleep for `ms` milliseconds, or for as long as the platform allows:
/// never past what is left of the handler's budget, 30 seconds at most by
/// default, and no longer once the invocation is cancelled. Returns the
/// milliseconds slept.
///
/// Needs a platform that supports `util_v2`; the `util-v2` feature declares
/// that in the module.
///
/// ```rust,ignore
/// let slept = util::sleep_ms(500 * 2u32.pow(attempt));
/// ```
#[cfg(feature = "util-v2")]
pub fn sleep_ms(ms: u32) -> u32 {
    #[cfg(target_arch = "wasm32")]
    {
        #[link(wasm_import_module = "env")]
        extern "C" {
            fn util_sleep_ms(ms: i32) -> i32;
        }
        let slept = unsafe { util_sleep_ms(i32::try_from(ms).unwrap_or(i32::MAX)) };
        u32::try_from(slept).unwrap_or(0)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::clock().map_or_else(
            || {
                let granted = ms.min(NATIVE_SLEEP_MAX_MS);
                std::thread::sleep(std::time::Duration::from_millis(u64::from(granted)));
                granted
            },
            |clock| clock.sleep_ms(ms),
        )
    }
}

/// Milliseconds since the Unix epoch.
#[cfg(feature = "util-v2")]
pub fn now_millis() -> i64 {
    #[cfg(target_arch = "wasm32")]
    {
        #[link(wasm_import_module = "env")]
        extern "C" {
            fn util_now_millis() -> i64;
        }
        unsafe { util_now_millis() }
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::clock().map_or_else(system_millis, |clock| clock.now_millis())
    }
}

/// The system's milliseconds since the Unix epoch.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn system_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let since = |later: SystemTime, earlier: SystemTime| {
        later
            .duration_since(earlier)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
    };
    let now = SystemTime::now();
    since(now, UNIX_EPOCH)
        .or_else(|| since(UNIX_EPOCH, now).map(|before| -before))
        .unwrap_or(0)
}

/// `millis` since the Unix epoch as RFC 3339 in UTC, offset written as the
/// platform writes it.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn rfc3339(millis: i64) -> String {
    const DAY: i64 = 86_400_000;
    let (year, month, day) = civil(millis.div_euclid(DAY));
    let of_day = millis.rem_euclid(DAY);
    let (seconds, fraction) = (of_day / 1_000, of_day % 1_000);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{fraction:03}+00:00",
        seconds / 3_600,
        seconds / 60 % 60,
        seconds % 60
    )
}

/// The proleptic Gregorian date of a day counted from 1970-01-01, by
/// eras of 400 years that begin on 1 March.
#[cfg(not(target_arch = "wasm32"))]
fn civil(days: i64) -> (i64, i64, i64) {
    const ERA: i64 = 146_097;
    let shifted = days + 719_468;
    let (era, day_of_era) = (shifted.div_euclid(ERA), shifted.rem_euclid(ERA));
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / (ERA - 1)) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let march_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * march_month + 2) / 5 + 1;
    let month = if march_month < 10 {
        march_month + 3
    } else {
        march_month - 9
    };
    (year_of_era + era * 400 + i64::from(month <= 2), month, day)
}

#[cfg(test)]
mod tests {
    use crate::util;

    #[test]
    fn generate_uuid_is_unique_across_rapid_calls() {
        let ids: std::collections::HashSet<String> =
            (0..10_000).map(|_| util::generate_uuid()).collect();
        assert_eq!(ids.len(), 10_000);
    }

    #[test]
    fn generate_uuid_is_rfc4122_v4() {
        let id = util::generate_uuid();
        let bytes = id.as_bytes();
        assert_eq!(id.len(), 36);
        assert!([8, 13, 18, 23].iter().all(|&i| bytes[i] == b'-'));
        assert!(id
            .chars()
            .all(|c| c == '-' || matches!(c, '0'..='9' | 'a'..='f')));
        assert_eq!(bytes[14], b'4');
        assert!(matches!(bytes[19], b'8' | b'9' | b'a' | b'b'));
    }

    #[test]
    fn dates_are_the_calendars() {
        for (millis, written) in [
            (0, "1970-01-01T00:00:00.000+00:00"),
            (-1, "1969-12-31T23:59:59.999+00:00"),
            (951_782_400_000, "2000-02-29T00:00:00.000+00:00"),
            (951_868_799_999, "2000-02-29T23:59:59.999+00:00"),
            (951_868_800_000, "2000-03-01T00:00:00.000+00:00"),
            (4_107_542_400_000, "2100-03-01T00:00:00.000+00:00"),
            (1_790_589_600_123, "2026-09-28T10:00:00.123+00:00"),
            (1_798_761_599_000, "2026-12-31T23:59:59.000+00:00"),
            (-2_208_988_800_000, "1900-01-01T00:00:00.000+00:00"),
        ] {
            assert_eq!(util::rfc3339(millis), written);
        }
    }

    #[test]
    fn the_native_time_is_the_systems_in_rfc_3339() {
        let before = util::system_millis();
        let now = util::current_time();
        let bytes = now.as_bytes();
        assert_eq!(now.len(), "2026-09-28T10:00:00.000+00:00".len(), "{now}");
        assert!(
            [
                (4, b'-'),
                (7, b'-'),
                (10, b'T'),
                (13, b':'),
                (16, b':'),
                (19, b'.')
            ]
            .iter()
            .all(|(at, separator)| bytes[*at] == *separator),
            "{now}"
        );
        assert!(now.ends_with("+00:00"), "{now}");
        let year: i64 = now[..4].parse().expect("year");
        assert!((2026..3000).contains(&year), "{now}");
        assert!(before > 1_790_000_000_000, "{before}");
    }

    #[cfg(all(feature = "util-v2", not(feature = "test-host")))]
    #[test]
    fn a_native_sleep_is_a_real_one() {
        let started = std::time::Instant::now();
        let before = util::now_millis();
        assert_eq!(util::sleep_ms(20), 20);
        assert!(started.elapsed() >= std::time::Duration::from_millis(20));
        assert!(util::now_millis() >= before + 20);
        assert_eq!(util::sleep_ms(0), 0);
    }

    #[cfg(feature = "test-host")]
    mod hosted {
        use crate::host::{self, HostSet};
        use crate::testing::FakeClock;
        use crate::util;
        use std::rc::Rc;

        fn hosted<T>(clock: &FakeClock, body: impl FnOnce() -> T) -> T {
            let hosts = HostSet {
                clock: Some(Rc::new(clock.clone())),
                ..Default::default()
            };
            host::with(hosts, body)
        }

        #[test]
        fn the_legacy_functions_read_the_installed_clock() {
            let clock = FakeClock::at(1_790_589_600_000);
            let (now, first, second) = hosted(&clock, || {
                (
                    util::current_time(),
                    util::generate_uuid(),
                    util::generate_uuid(),
                )
            });
            assert_eq!(now, "2026-09-28T10:00:00.000+00:00");
            assert_eq!(first, "00000000-0000-4000-8000-000000000001");
            assert_eq!(second, "00000000-0000-4000-8000-000000000002");
            assert_ne!(util::current_time(), now);
        }

        #[cfg(feature = "util-v2")]
        #[test]
        fn a_backoff_under_a_fake_clock_takes_no_time() {
            let clock = FakeClock::at(1_000);
            let started = std::time::Instant::now();
            let slept: Vec<u32> = hosted(&clock, || {
                (0..4)
                    .map(|attempt| util::sleep_ms(500 << attempt))
                    .collect()
            });
            assert_eq!(slept, [500, 1_000, 2_000, 4_000]);
            assert_eq!(hosted(&clock, util::now_millis), 8_500);
            assert_eq!(clock.slept_ms(), 7_500);
            assert!(started.elapsed() < std::time::Duration::from_secs(1));
        }
    }
}
