use std::collections::HashMap;

pub(crate) struct Asset {
    pub name: &'static str,
    pub content_type: &'static str,
    pub bytes: &'static [u8],
}

const INDEX: &str = "index.html";
const PAGE_ONLY: [&str; 2] = [INDEX, "swagger-initializer.js"];

macro_rules! asset {
    ($name:literal, $content_type:literal) => {
        Asset {
            name: $name,
            content_type: $content_type,
            bytes: include_bytes!(concat!("../../assets/swagger-ui/", $name)),
        }
    };
}

pub(crate) static ASSETS: [Asset; 8] = [
    asset!("index.html", "text/html; charset=utf-8"),
    asset!("swagger-ui.css", "text/css"),
    asset!("index.css", "text/css"),
    asset!("swagger-ui-bundle.js", "text/javascript"),
    asset!("swagger-ui-standalone-preset.js", "text/javascript"),
    asset!("favicon-32x32.png", "image/png"),
    asset!("favicon-16x16.png", "image/png"),
    asset!("swagger-initializer.js", "text/javascript"),
];

pub(crate) struct Files<T>(HashMap<&'static str, T>);

impl<T> Files<T> {
    pub(crate) fn of(entry: impl Fn(&'static Asset) -> T) -> Self {
        Self(
            ASSETS
                .iter()
                .map(|asset| (asset.name, entry(asset)))
                .collect(),
        )
    }

    pub(crate) fn page(&self, name: &str) -> Option<&T> {
        self.0.get(match name {
            "" => INDEX,
            name => name,
        })
    }

    pub(crate) fn meta(&self, name: &str) -> Option<&T> {
        (!PAGE_ONLY.contains(&name))
            .then(|| self.0.get(name))
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn the_embedded_files_are_the_pinned_bytes() {
        let pinned = [
            (
                "index.html",
                "text/html; charset=utf-8",
                734,
                "bb9928afd0ea8c12e124c42fef58fb080f36770389684badb2a4dcf548624eeb",
            ),
            (
                "swagger-ui.css",
                "text/css",
                152_071,
                "40170f0ee859d17f92131ba707329a88a070e4f66874d11365e9a77d232f6117",
            ),
            (
                "index.css",
                "text/css",
                202,
                "9324807d424565a1639bb29f3754c8d4d45c1009c67674e996e33355f6929ce7",
            ),
            (
                "swagger-ui-bundle.js",
                "text/javascript",
                1_452_753,
                "c2e4a9ef08144839ff47c14202063ecfe4e59e70a4e7154a26bd50d880c88ba1",
            ),
            (
                "swagger-ui-standalone-preset.js",
                "text/javascript",
                230_293,
                "33b7a6f5afcac4902fdf93281be2d2e12db15f241d384606e6e6d17745b7f86f",
            ),
            (
                "favicon-32x32.png",
                "image/png",
                628,
                "3ed612f41e050ca5e7000cad6f1cbe7e7da39f65fca99c02e99e6591056e5837",
            ),
            (
                "favicon-16x16.png",
                "image/png",
                665,
                "af24ad604dd7b3bcda8f975ab973075f4a2f70a4087944a12f8ef8b63a3e07c2",
            ),
            (
                "swagger-initializer.js",
                "text/javascript",
                521,
                "62416f7cd2f7d9a0cdbb7a503e3332e155531ce4bb74ea342c834d06aeec67be",
            ),
        ];
        let embedded: Vec<_> = ASSETS
            .iter()
            .map(|asset| {
                (
                    asset.name,
                    asset.content_type,
                    asset.bytes.len(),
                    hex::encode(Sha256::digest(asset.bytes)),
                )
            })
            .collect();
        let expected: Vec<_> = pinned
            .iter()
            .map(|&(name, content_type, len, hash)| (name, content_type, len, hash.to_string()))
            .collect();
        assert_eq!(embedded, expected);
    }

    #[test]
    fn the_licence_and_notice_ship_beside_the_files() {
        let licence = include_str!("../../assets/swagger-ui/LICENSE");
        let notice = include_str!("../../assets/swagger-ui/NOTICE");
        assert_eq!(licence.len(), 11_358);
        assert!(licence.contains("Apache License"));
        assert_eq!(notice.len(), 55);
        assert!(notice.contains("SmartBear Software"));
    }

    #[test]
    fn the_page_serves_its_index_twice_and_meta_serves_only_the_shared_files() {
        let files = Files::of(|asset| asset.name);
        assert_eq!(files.page(""), Some(&"index.html"));
        assert_eq!(files.page("index.html"), Some(&"index.html"));
        assert_eq!(
            files.page("swagger-initializer.js"),
            Some(&"swagger-initializer.js")
        );
        for missing in [
            "swagger-ui.js",
            "swagger-ui.js.map",
            "oauth2-redirect.html",
            "LICENSE",
            "../index.html",
        ] {
            assert_eq!(files.page(missing), None, "{missing}");
        }
        let shared: Vec<_> = ASSETS
            .iter()
            .filter_map(|asset| files.meta(asset.name))
            .copied()
            .collect();
        assert_eq!(
            shared,
            [
                "swagger-ui.css",
                "index.css",
                "swagger-ui-bundle.js",
                "swagger-ui-standalone-preset.js",
                "favicon-32x32.png",
                "favicon-16x16.png",
            ]
        );
        assert_eq!(files.meta(""), None);
    }
}
