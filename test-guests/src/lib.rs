mod profile;

pub use profile::Profile;

use std::path::PathBuf;

pub const UNAVAILABLE: Option<&str> = option_env!("CUFFLINK_TEST_GUESTS_UNAVAILABLE");

pub fn names() -> impl Iterator<Item = &'static str> {
    env!("CUFFLINK_TEST_GUESTS").split(',')
}

pub fn wasm(guest: &str, profile: Profile) -> std::io::Result<Vec<u8>> {
    std::fs::read(
        PathBuf::from(env!("CUFFLINK_TEST_GUESTS_DIR"))
            .join(format!("{guest}-{}.wasm", profile.name())),
    )
}
