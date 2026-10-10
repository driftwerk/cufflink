pub fn sdk_guest(guest: &str, profile: cufflink_test_guests::Profile) -> Option<Vec<u8>> {
    match cufflink_test_guests::UNAVAILABLE {
        Some(reason) => {
            crate::skip(reason);
            None
        }
        None => Some(cufflink_test_guests::wasm(guest, profile).expect("built guest")),
    }
}
