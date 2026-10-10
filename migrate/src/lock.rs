//! Advisory-lock keys. Those of a lock only this platform's processes take
//! are [`platform_key`]s, apart by namespace. The keys a released binary
//! derives are kept as it derives them, so that it and this one exclude
//! each other: the legacy runner's table key here, the deploy lock's (a
//! service id's bits) and the control plane's migration lock (a constant).
//!
//! A key hashes each of its parts as its UTF-8 bytes and then `0xff`, the
//! bytes std's `Hash` for `str` feeds a hasher, spelled out here because
//! std does not promise them from one Rust release to the next and replicas
//! built by different toolchains must derive the same key.

use siphasher::sip::SipHasher13;
use std::hash::Hasher;

/// SipHash-1-3 under `keys` of `parts`, each its bytes and then `0xff`,
/// which is no byte of UTF-8.
fn sip13((key0, key1): (u64, u64), parts: &[&str]) -> u64 {
    parts
        .iter()
        .fold(
            SipHasher13::new_with_keys(key0, key1),
            |mut hasher, part| {
                hasher.write(part.as_bytes());
                hasher.write_u8(0xff);
                hasher
            },
        )
        .finish()
}

/// The key a legacy refinery runner takes for `table`:
/// `(DefaultHasher(table).finish() as i64).abs() % i64::MAX`, where
/// `DefaultHasher` is SipHash-1-3 with zero keys. Sharing it keeps that
/// runner and this one from touching one history at once.
pub fn legacy_table_lock_key(table: &str) -> i64 {
    (sip13((0, 0), &[table]) as i64).wrapping_abs() % i64::MAX
}

/// The key of a lock only this platform's processes take, on `name`
/// within `namespace`.
pub fn platform_key(namespace: &str, name: &str) -> i64 {
    sip13(
        (
            u64::from_be_bytes(*b"cufflink"),
            u64::from_be_bytes(*b"locks\0\0\0"),
        ),
        &[namespace, name],
    ) as i64
}

/// The key of a history only this platform's runners write.
pub fn platform_lock_key(table: &str) -> i64 {
    platform_key("migrate", table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hash;

    fn legacy_runner_key(table: &str) -> i64 {
        let mut hasher = DefaultHasher::new();
        table.hash(&mut hasher);
        (hasher.finish() as i64).abs() % i64::MAX
    }

    #[test]
    fn the_legacy_key_is_the_one_the_legacy_runner_derives() {
        for (table, key) in [
            ("loan_service.schema_history", 5095852499937019771),
            ("catalog_service.schema_history", 576944167566361902),
            ("public.schema_history_deferred", 315364223451433509),
            ("schema_history", 3568529105731104943),
            ("", 3476900567878811119),
        ] {
            assert_eq!(legacy_table_lock_key(table), key, "{table}");
            assert_eq!(legacy_runner_key(table), key, "{table}");
        }
    }

    #[test]
    fn a_platform_key_is_its_namespace_and_name_together() {
        assert_ne!(
            platform_key("db_roles", "cufflink_acme"),
            platform_key("migrate", "cufflink_acme")
        );
        assert_ne!(platform_key("ab", "c"), platform_key("a", "bc"));
    }

    #[test]
    fn the_platform_keys_are_the_ones_every_replica_derives() {
        assert_eq!(
            platform_key("db_roles", "cufflink_acme"),
            7182794317239927779
        );
        assert_eq!(
            platform_lock_key("_cufflink.schema_history"),
            -1468775211513434424
        );
        assert_eq!(
            platform_lock_key("loan_service.refinery_schema_history"),
            -8958653408802261385
        );
        assert_ne!(
            platform_lock_key("_cufflink.schema_history"),
            legacy_table_lock_key("_cufflink.schema_history")
        );
    }
}
