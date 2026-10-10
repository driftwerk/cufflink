//! Image transforms recorded, not performed.

use super::storage::InMemoryStorage;
use crate::host::image::ImageHost;
use crate::image::VariantSpec;
use std::cell::RefCell;
use std::rc::Rc;

/// One variant a handler asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transformed {
    pub bucket: String,
    pub in_key: String,
    pub out_key: String,
    /// The longest edge asked for; `0` leaves the size as it is.
    pub max_dim: u32,
    pub quality: u8,
    /// `jpeg`, `webp` or `avif`.
    pub format: String,
}

/// Records every transform and stores the source's bytes, unchanged, at
/// each output key under the content type of its format. What the platform
/// refuses is refused: a source that is not there, a quality outside 1 to
/// 100, a format it does not encode, no variant at all. Clones share one
/// record.
#[derive(Clone)]
pub struct ImageRecorder {
    storage: InMemoryStorage,
    transforms: Rc<RefCell<Vec<Transformed>>>,
}

fn content_type(format: &str) -> Option<&'static str> {
    match format {
        "jpeg" => Some("image/jpeg"),
        "webp" => Some("image/webp"),
        "avif" => Some("image/avif"),
        _ => None,
    }
}

fn accepted(variant: &VariantSpec<'_>) -> Option<&'static str> {
    ((1..=100).contains(&variant.quality) && i32::try_from(variant.max_dim).is_ok())
        .then(|| content_type(variant.format))
        .flatten()
}

impl ImageRecorder {
    /// A recorder that reads its sources from `storage` and writes its
    /// variants there.
    pub fn over(storage: InMemoryStorage) -> Self {
        Self {
            storage,
            transforms: Rc::default(),
        }
    }

    /// Every variant written, in order.
    pub fn transforms(&self) -> Vec<Transformed> {
        self.transforms.borrow().clone()
    }
}

impl ImageHost for ImageRecorder {
    fn transform(
        &self,
        bucket: &str,
        in_key: &str,
        variants: &[VariantSpec<'_>],
    ) -> Option<Vec<u32>> {
        let content_types = variants
            .iter()
            .map(accepted)
            .collect::<Option<Vec<_>>>()
            .filter(|accepted| !accepted.is_empty())?;
        let source = self.storage.object(bucket, in_key)?;
        let size = u32::try_from(source.body.len()).ok()?;
        variants
            .iter()
            .zip(content_types)
            .for_each(|(variant, content_type)| {
                self.storage
                    .put(bucket, variant.out_key, content_type, &source.body);
                self.transforms.borrow_mut().push(Transformed {
                    bucket: bucket.to_string(),
                    in_key: in_key.to_string(),
                    out_key: variant.out_key.to_string(),
                    max_dim: variant.max_dim,
                    quality: variant.quality,
                    format: variant.format.to_string(),
                });
            });
        Some(vec![size; variants.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::{self, HostSet};
    use crate::image;

    fn hosted<T>(images: &ImageRecorder, body: impl FnOnce() -> T) -> T {
        let hosts = HostSet {
            image: Some(Rc::new(images.clone())),
            ..Default::default()
        };
        host::with(hosts, body)
    }

    fn variant<'a>(out_key: &'a str, quality: u8, format: &'a str) -> VariantSpec<'a> {
        VariantSpec {
            out_key,
            max_dim: 256,
            quality,
            format,
        }
    }

    #[test]
    fn variants_are_recorded_and_stored_beside_their_source() {
        let storage = InMemoryStorage::new();
        storage.put("media", "raw/a.jpg", "image/jpeg", &[0xff, 0xd8, 0xff]);
        let images = ImageRecorder::over(storage.clone());
        let (single, many) = hosted(&images, || {
            (
                image::transform_webp("media", "raw/a.jpg", "raw/a.display.webp", 1024, 80),
                image::transform_multi(
                    "media",
                    "raw/a.jpg",
                    &[
                        variant("raw/a.thumb.jpg", 75, "jpeg"),
                        variant("raw/a.thumb.avif", 60, "avif"),
                    ],
                ),
            )
        });
        assert_eq!(single, Some(3));
        assert_eq!(many, Some(vec![3, 3]));
        assert_eq!(
            storage.keys("media"),
            [
                "raw/a.display.webp",
                "raw/a.jpg",
                "raw/a.thumb.avif",
                "raw/a.thumb.jpg"
            ]
        );
        let stored = storage.object("media", "raw/a.thumb.avif").expect("stored");
        assert_eq!(
            (stored.content_type.as_str(), stored.body),
            ("image/avif", vec![0xff, 0xd8, 0xff])
        );
        assert_eq!(
            images.transforms(),
            [
                ("raw/a.display.webp", 1024, 80, "webp"),
                ("raw/a.thumb.jpg", 256, 75, "jpeg"),
                ("raw/a.thumb.avif", 256, 60, "avif"),
            ]
            .map(|(out_key, max_dim, quality, format)| Transformed {
                bucket: "media".into(),
                in_key: "raw/a.jpg".into(),
                out_key: out_key.into(),
                max_dim,
                quality,
                format: format.into(),
            })
        );
    }

    #[test]
    fn what_the_platform_refuses_writes_nothing() {
        let storage = InMemoryStorage::new();
        storage.put("media", "raw/a.jpg", "image/jpeg", b"jpeg");
        let images = ImageRecorder::over(storage.clone());
        hosted(&images, || {
            assert_eq!(
                image::transform_jpeg("media", "raw/missing.jpg", "out.jpg", 0, 80),
                None
            );
            assert_eq!(
                image::transform_jpeg("media", "raw/a.jpg", "out.jpg", 0, 0),
                None
            );
            assert_eq!(
                image::transform_jpeg("media", "raw/a.jpg", "out.jpg", 0, 101),
                None
            );
            assert_eq!(
                image::transform_jpeg("media", "raw/a.jpg", "out.jpg", u32::MAX, 80),
                None
            );
            assert_eq!(image::transform_multi("media", "raw/a.jpg", &[]), None);
            assert_eq!(
                image::transform_multi(
                    "media",
                    "raw/a.jpg",
                    &[variant("ok.jpg", 80, "jpeg"), variant("no.png", 80, "png")],
                ),
                None,
                "one variant refused refuses them all"
            );
        });
        assert_eq!(storage.keys("media"), ["raw/a.jpg"]);
        assert!(images.transforms().is_empty());
        assert_eq!(
            image::transform_jpeg("media", "raw/a.jpg", "out.jpg", 0, 80),
            None,
            "no host"
        );
    }
}
