//! The legacy functions in a native build: through the installed
//! [`StorageHost`](crate::host::storage::StorageHost), and nothing
//! without one.

use crate::host::storage::PresignOp;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;

pub(super) fn download(bucket: &str, key: &str) -> Option<String> {
    crate::host::storage()?
        .download(bucket, key)
        .map(|bytes| STANDARD.encode(bytes))
}

pub(super) fn download_many(items: &[(&str, &str)]) -> Vec<Result<Vec<u8>, String>> {
    crate::host::storage().map_or_else(Vec::new, |host| {
        items
            .iter()
            .map(|(bucket, key)| {
                host.download(bucket, key)
                    .ok_or_else(|| format!("download failed for {bucket}/{key}"))
            })
            .collect()
    })
}

pub(super) fn presign(op: PresignOp<'_>) -> Option<String> {
    crate::host::storage()?.presign(op)
}

pub(super) fn delete(bucket: &str, key: &str) -> bool {
    crate::host::storage().is_some_and(|host| host.delete(bucket, key))
}

#[cfg(all(test, feature = "test-host"))]
mod tests {
    use crate::host::{self, HostSet};
    use crate::storage;
    use crate::testing::InMemoryStorage;
    use std::rc::Rc;

    #[test]
    fn the_legacy_functions_reach_the_host() {
        let store = InMemoryStorage::new().deny("platform");
        store.put("receipts", "a.pdf", "application/pdf", &[0x25, 0xff]);
        store.put("platform", "wasm/x", "application/wasm", b"\0asm");
        let hosts = HostSet {
            storage: Some(Rc::new(store.clone())),
            ..Default::default()
        };
        host::with(hosts, || {
            assert_eq!(
                storage::download("receipts", "a.pdf").as_deref(),
                Some("Jf8=")
            );
            assert_eq!(storage::download("receipts", "missing"), None);
            assert_eq!(storage::download("platform", "wasm/x"), None);
            assert_eq!(
                storage::download_many(&[("receipts", "a.pdf"), ("receipts", "missing")]),
                [
                    Ok(vec![0x25, 0xff]),
                    Err("download failed for receipts/missing".to_string())
                ]
            );
            assert_eq!(
                storage::presign_upload("receipts", "b.pdf", "application/pdf", 300).as_deref(),
                Some("memory://receipts/b.pdf?expires=300")
            );
            assert_eq!(
                storage::presign_download("receipts", "a.pdf", 60).as_deref(),
                Some("memory://receipts/a.pdf?expires=60")
            );
            assert_eq!(storage::presign_download("platform", "wasm/x", 60), None);
            assert!(storage::delete("receipts", "a.pdf"));
            assert!(storage::delete("receipts", "a.pdf"), "already absent");
            assert!(!storage::delete("platform", "wasm/x"));
        });
        assert!(store.object("receipts", "a.pdf").is_none());
        assert!(store.object("platform", "wasm/x").is_some());
        assert_eq!(storage::download("receipts", "a.pdf"), None);
        assert!(!storage::delete("receipts", "a.pdf"));
        assert!(storage::download_many(&[("receipts", "a.pdf")]).is_empty());
    }
}
