//! Guest-behaviour features a module declares to the platform. Each enabled
//! cargo feature links its name into the `cufflink.features` custom section
//! (the linker concatenates every object's copy), which the CLI and the
//! platform read to require the capability before deploying the module.

macro_rules! declare_features {
    ($($cargo_feature:literal => $name:ident = $marker:literal),* $(,)?) => {
        $(
            #[cfg(all(target_arch = "wasm32", feature = $cargo_feature))]
            #[used]
            #[link_section = "cufflink.features"]
            static $name: [u8; $marker.len()] = *$marker;
        )*

        /// Keeps every enabled marker in the linked module. `#[used]` only
        /// stops rustc discarding a static; the linker still drops the object
        /// file holding it unless something references it, which in debug and
        /// split-codegen builds nothing else does. Every guest exports
        /// `alloc`, which calls this.
        pub(crate) fn anchor() {
            $(
                #[cfg(all(target_arch = "wasm32", feature = $cargo_feature))]
                core::hint::black_box(&$name);
            )*
        }

        #[cfg(test)]
        const MARKERS: &[(&str, &[u8])] = &[$(($cargo_feature, $marker)),*];
    };
}

declare_features! {
    "db-v2" => DB_V2 = b"db_v2\n",
    "cache" => CACHE = b"cache\n",
    "service-call" => SERVICE_CALL = b"service_call\n",
    "response-v2" => RESPONSE_V2 = b"response_v2\n",
    "delivery-control" => DELIVERY_CONTROL = b"delivery_control\n",
    "nats-publish-v2" => NATS_PUBLISH_V2 = b"nats_publish_v2\n",
    "http-v2" => HTTP_V2 = b"http_v2\n",
    "storage-v2" => STORAGE_V2 = b"storage_v2\n",
    "util-v2" => UTIL_V2 = b"util_v2\n",
    "ext-call" => EXT_CALL = b"ext_call\n",
}

#[cfg(test)]
mod tests {
    use super::MARKERS;
    use cufflink_types::features;
    use std::collections::BTreeMap;

    /// Every cargo feature of this crate: the keys of its `[features]`.
    fn cargo_features() -> Vec<&'static str> {
        include_str!("../Cargo.toml")
            .split("\n[")
            .find_map(|section| section.strip_prefix("features]"))
            .into_iter()
            .flat_map(str::lines)
            .filter_map(|line| line.split_once(" = "))
            .map(|(name, _)| name.trim())
            .filter(|name| !name.is_empty() && !name.starts_with('#') && *name != "default")
            .collect()
    }

    /// `docs/versions.md`'s map: each cargo feature, and the platform
    /// feature it requires, `None` for one that requires none.
    fn documented_map() -> BTreeMap<&'static str, Option<&'static str>> {
        include_str!("../../docs/versions.md")
            .split("\n## ")
            .find(|section| section.starts_with("SDK cargo features"))
            .into_iter()
            .flat_map(str::lines)
            .filter_map(|line| line.strip_prefix("| `"))
            .filter_map(|row| row.split_once("` | "))
            .map(|(cargo_feature, required)| {
                let platform = required
                    .strip_prefix('`')
                    .and_then(|rest| rest.split_once('`'))
                    .map(|(platform, _)| platform);
                (cargo_feature, platform)
            })
            .collect()
    }

    #[test]
    fn the_versions_doc_maps_every_cargo_feature_to_what_it_requires() {
        let declared: BTreeMap<&str, &str> = MARKERS
            .iter()
            .map(|(cargo_feature, marker)| {
                let marker = std::str::from_utf8(marker).expect("a marker is text");
                (*cargo_feature, marker.trim_end())
            })
            .collect();
        let mut documented = documented_map();
        let mut features = cargo_features();
        features.sort_unstable();
        assert_eq!(
            documented.keys().copied().collect::<Vec<_>>(),
            features,
            "docs/versions.md maps other cargo features than Cargo.toml has"
        );
        documented.retain(|_, platform| platform.is_some());
        assert_eq!(
            documented,
            declared
                .into_iter()
                .map(|(cargo_feature, platform)| (cargo_feature, Some(platform)))
                .collect::<BTreeMap<_, _>>()
        );
    }

    #[test]
    fn markers_name_platform_features_and_their_cargo_features() {
        let expected = [
            features::DB_V2,
            features::CACHE,
            features::SERVICE_CALL,
            features::RESPONSE_V2,
            features::DELIVERY_CONTROL,
            features::NATS_PUBLISH_V2,
            features::HTTP_V2,
            features::STORAGE_V2,
            features::UTIL_V2,
            features::EXT_CALL,
        ];
        assert_eq!(MARKERS.len(), expected.len());
        for ((cargo_feature, marker), feature) in MARKERS.iter().zip(expected) {
            assert_eq!(*marker, format!("{feature}\n").as_bytes());
            assert_eq!(*cargo_feature, feature.replace('_', "-"));
        }
    }
}
