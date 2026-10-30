use super::budget::{Hold, HostCaps, Refused, IMAGE_BYTES_PER_PIXEL, IMAGE_MAX_SIDE};
use super::host::{read_object, Fetch};
use crate::storage::S3Storage;
use eyre::{Result, WrapErr};
use image::{
    codecs::jpeg::JpegEncoder, imageops::FilterType, DynamicImage, ImageDecoder, ImageReader,
    RgbImage,
};
use std::io::Cursor;
use std::sync::Arc;

#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub enum Format {
    Jpeg,
    Webp,
    Avif,
}

impl Format {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        match name {
            "jpeg" | "jpg" => Some(Format::Jpeg),
            "webp" => Some(Format::Webp),
            "avif" => Some(Format::Avif),
            _ => None,
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Format::Jpeg => "image/jpeg",
            Format::Webp => "image/webp",
            Format::Avif => "image/avif",
        }
    }

    pub fn encode(self, img: &RgbImage, quality: u8) -> Result<Vec<u8>> {
        match self {
            Format::Jpeg => encode_jpeg(img, quality),
            Format::Webp => encode_webp(img, quality),
            Format::Avif => encode_avif(img, quality),
        }
    }
}

pub(crate) struct ImageBudget<'a> {
    pub caps: &'a HostCaps,
    pub share: Option<usize>,
    pub path: &'static str,
}

impl ImageBudget<'_> {
    pub(crate) fn source(&self, size: u64, hold: &mut Hold) -> Result<(), Refused> {
        self.caps
            .check(self.path, "image_input", size, self.caps.image_input as u64)
            .map_err(Refused::Cap)?;
        hold.grow(usize::try_from(size).unwrap_or(usize::MAX))
            .map_err(Refused::Budget)
    }

    pub(crate) fn targets(&self, count: usize) -> Result<(), Refused> {
        self.caps
            .check(
                self.path,
                "image_targets",
                count as u64,
                self.caps.image_targets as u64,
            )
            .map_err(Refused::Cap)
    }

    fn decode(&self, source: &[u8], held: &Hold) -> Result<(Hold, image::Limits), Fetch> {
        let (width, height) = ImageReader::new(Cursor::new(source))
            .with_guessed_format()
            .wrap_err("failed to guess image format")
            .and_then(|reader| {
                reader
                    .into_dimensions()
                    .wrap_err("failed to read image dimensions")
            })
            .map_err(Fetch::Failed)?;
        let pixels = u64::from(width) * u64::from(height);
        self.caps
            .check(
                self.path,
                "image_pixels",
                pixels,
                self.caps.image_pixels(self.share),
            )
            .map_err(|cap| Fetch::Refused(Refused::Cap(cap)))?;
        let bytes =
            usize::try_from(pixels.saturating_mul(IMAGE_BYTES_PER_PIXEL)).unwrap_or(usize::MAX);
        let mut decode = held.sibling();
        decode
            .grow(bytes)
            .map_err(|capacity| Fetch::Refused(Refused::Budget(capacity)))?;
        let mut limits = image::Limits::default();
        limits.max_image_width = Some(IMAGE_MAX_SIDE);
        limits.max_image_height = Some(IMAGE_MAX_SIDE);
        limits.max_alloc = Some(pixels.saturating_mul(IMAGE_BYTES_PER_PIXEL));
        Ok((decode, limits))
    }
}

pub(crate) struct Source {
    bytes: Vec<u8>,
    hold: Hold,
}

impl Source {
    pub(crate) async fn read(
        storage: &S3Storage,
        budget: &ImageBudget<'_>,
        bucket: &str,
        key: &str,
        mut hold: Hold,
    ) -> Result<Self, Fetch> {
        let bytes =
            read_object(storage, bucket, key, |size| budget.source(size, &mut hold)).await?;
        Ok(Self { bytes, hold })
    }
}

struct Decoded {
    image: RgbImage,
    _hold: Hold,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn transform(
    storage: &S3Storage,
    budget: &ImageBudget<'_>,
    bucket: &str,
    source: Source,
    out_key: &str,
    max_dim: u32,
    quality: u8,
    format: Format,
) -> Result<u32, Fetch> {
    let (hold, limits) = budget.decode(&source.bytes, &source.hold)?;
    let (encoded, _encoded_hold) = tokio::task::spawn_blocking(move || {
        let Source {
            bytes,
            hold: source_hold,
        } = source;
        let img = decode_oriented(&bytes, limits)?;
        drop((bytes, source_hold));
        format
            .encode(&resize_to(&img, max_dim), quality)
            .map(|encoded| (encoded, hold))
    })
    .await
    .wrap_err("image transform task panicked")
    .and_then(|encoded| encoded)
    .map_err(Fetch::Failed)?;

    let bytes_written = encoded.len() as u32;
    storage
        .upload_to_bucket(bucket, out_key, encoded, format.content_type())
        .await
        .map_err(Fetch::Failed)?;
    Ok(bytes_written)
}

pub(crate) async fn transform_multi(
    storage: &S3Storage,
    budget: &ImageBudget<'_>,
    bucket: &str,
    source: Source,
    targets: Vec<(String, u32, u8, Format)>,
) -> Result<Vec<u32>, Fetch> {
    let (hold, limits) = budget.decode(&source.bytes, &source.hold)?;
    let decoded = tokio::task::spawn_blocking(move || {
        let Source {
            bytes,
            hold: _source,
        } = source;
        decode_oriented(&bytes, limits).map(|image| Arc::new(Decoded { image, _hold: hold }))
    })
    .await
    .wrap_err("image transform task panicked")
    .and_then(|decoded| decoded)
    .map_err(Fetch::Failed)?;

    let mut sizes = Vec::with_capacity(targets.len());
    for (out_key, max_dim, quality, format) in targets {
        let decoded = Arc::clone(&decoded);
        let encoded = tokio::task::spawn_blocking(move || {
            format.encode(&resize_to(&decoded.image, max_dim), quality)
        })
        .await
        .wrap_err("image transform task panicked")
        .and_then(|encoded| encoded)
        .map_err(Fetch::Failed)?;
        sizes.push(encoded.len() as u32);
        storage
            .upload_to_bucket(bucket, &out_key, encoded, format.content_type())
            .await
            .map_err(Fetch::Failed)?;
    }
    Ok(sizes)
}

#[doc(hidden)]
pub fn decode_oriented(input: &[u8], limits: image::Limits) -> Result<RgbImage> {
    let mut decoder = ImageReader::new(Cursor::new(input))
        .with_guessed_format()
        .wrap_err("failed to guess image format")?
        .into_decoder()
        .wrap_err("failed to build image decoder")?;
    decoder
        .set_limits(limits)
        .wrap_err("the image is larger than its reservation")?;
    let orientation = decoder
        .orientation()
        .wrap_err("failed to read image orientation")?;
    let mut decoded =
        DynamicImage::from_decoder(decoder).wrap_err("failed to decode source image")?;
    decoded.apply_orientation(orientation);
    Ok(decoded.to_rgb8())
}

#[doc(hidden)]
pub fn resize_to(img: &RgbImage, max_dim: u32) -> RgbImage {
    let (w, h) = img.dimensions();
    if max_dim > 0 && w.max(h) > max_dim {
        let (new_w, new_h) = scaled_dims(w, h, max_dim);
        image::imageops::resize(img, new_w, new_h, FilterType::Lanczos3)
    } else {
        img.clone()
    }
}

#[cfg(test)]
fn decode_and_resize(input: &[u8], max_dim: u32) -> Result<RgbImage> {
    Ok(resize_to(
        &decode_oriented(input, image::Limits::default())?,
        max_dim,
    ))
}

fn encode_jpeg(img: &RgbImage, quality: u8) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity((img.as_raw().len() / 4).max(1024));
    JpegEncoder::new_with_quality(&mut out, quality)
        .encode(
            img,
            img.width(),
            img.height(),
            image::ExtendedColorType::Rgb8,
        )
        .wrap_err("failed to encode JPEG")?;
    Ok(out)
}

fn encode_webp(img: &RgbImage, quality: u8) -> Result<Vec<u8>> {
    let config = zenwebp::LossyConfig::new().with_quality(quality as f32);
    zenwebp::EncodeRequest::lossy(
        &config,
        img.as_raw(),
        zenwebp::PixelLayout::Rgb8,
        img.width(),
        img.height(),
    )
    .encode()
    .map_err(|e| eyre::eyre!("failed to encode WebP: {e}"))
}

fn encode_avif(img: &RgbImage, quality: u8) -> Result<Vec<u8>> {
    let pixels: Vec<ravif::RGB8> = img
        .as_raw()
        .chunks_exact(3)
        .map(|c| ravif::RGB8::new(c[0], c[1], c[2]))
        .collect();
    let buf = ravif::Img::new(
        pixels.as_slice(),
        img.width() as usize,
        img.height() as usize,
    );
    let encoded = ravif::Encoder::new()
        .with_quality(quality as f32)
        .with_speed(6)
        .encode_rgb(buf)
        .map_err(|e| eyre::eyre!("failed to encode AVIF: {e}"))?;
    Ok(encoded.avif_file)
}

fn scaled_dims(width: u32, height: u32, max_dim: u32) -> (u32, u32) {
    if width >= height {
        let new_w = max_dim;
        let new_h = ((height as u64 * max_dim as u64) / width as u64).max(1) as u32;
        (new_w, new_h)
    } else {
        let new_h = max_dim;
        let new_w = ((width as u64 * max_dim as u64) / height as u64).max(1) as u32;
        (new_w, new_h)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{ImageBuffer, Rgb};

    fn synthetic_jpeg(width: u32, height: u32) -> Vec<u8> {
        let img: ImageBuffer<Rgb<u8>, Vec<u8>> = ImageBuffer::from_fn(width, height, |x, y| {
            Rgb([(x % 256) as u8, (y % 256) as u8, ((x + y) % 256) as u8])
        });
        let mut buf = Vec::new();
        let mut enc = JpegEncoder::new_with_quality(&mut buf, 95);
        enc.encode(&img, width, height, image::ExtendedColorType::Rgb8)
            .unwrap();
        buf
    }

    fn synthetic_jpeg_with_orientation(width: u32, height: u32, exif: u8) -> Vec<u8> {
        let base = synthetic_jpeg(width, height);
        let tiff: [u8; 26] = [
            0x49, 0x49, 0x2A, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x12, 0x01, 0x03, 0x00,
            0x01, 0x00, 0x00, 0x00, exif, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let mut payload = b"Exif\0\0".to_vec();
        payload.extend_from_slice(&tiff);
        let app1_len = (payload.len() + 2) as u16;

        let mut out = vec![0xFF, 0xD8, 0xFF, 0xE1];
        out.extend_from_slice(&app1_len.to_be_bytes());
        out.extend_from_slice(&payload);
        out.extend_from_slice(&base[2..]);
        out
    }

    #[test]
    fn decode_and_resize_applies_exif_orientation() {
        let bytes = synthetic_jpeg_with_orientation(640, 480, 6);
        let img = decode_and_resize(&bytes, 0).expect("decode+orient");
        assert_eq!(
            img.dimensions(),
            (480, 640),
            "expected dimensions swapped by EXIF orientation 6"
        );
    }

    #[test]
    fn decode_and_resize_orientation_1_is_unchanged() {
        let bytes = synthetic_jpeg_with_orientation(640, 480, 1);
        let img = decode_and_resize(&bytes, 0).expect("decode");
        assert_eq!(img.dimensions(), (640, 480));
    }

    #[test]
    fn scaled_dims_landscape_caps_long_edge() {
        assert_eq!(scaled_dims(4000, 3000, 2048), (2048, 1536));
    }

    #[test]
    fn scaled_dims_portrait_caps_long_edge() {
        assert_eq!(scaled_dims(3000, 4000, 2048), (1536, 2048));
    }

    #[test]
    fn scaled_dims_square_keeps_aspect() {
        assert_eq!(scaled_dims(4000, 4000, 1024), (1024, 1024));
    }

    #[test]
    fn decode_and_resize_caps_long_edge() {
        let bytes = synthetic_jpeg(4000, 3000);
        let img = decode_and_resize(&bytes, 2048).expect("decode+resize");
        let (w, h) = img.dimensions();
        assert!(w.max(h) <= 2048, "expected long edge ≤ 2048, got {w}x{h}");
    }

    #[test]
    fn decode_and_resize_skips_when_under_max_dim() {
        let bytes = synthetic_jpeg(800, 600);
        let img = decode_and_resize(&bytes, 2048).expect("decode+resize");
        assert_eq!(img.dimensions(), (800, 600));
    }

    #[test]
    fn decode_and_resize_zero_max_dim_is_no_resize() {
        let bytes = synthetic_jpeg(1200, 800);
        let img = decode_and_resize(&bytes, 0).expect("decode+resize");
        assert_eq!(img.dimensions(), (1200, 800));
    }

    #[test]
    fn encode_jpeg_lower_quality_yields_smaller_output() {
        let img = decode_and_resize(&synthetic_jpeg(1024, 1024), 0).expect("decode");
        let high = encode_jpeg(&img, 90).expect("q90");
        let low = encode_jpeg(&img, 50).expect("q50");
        assert!(low.len() < high.len(), "q50 should be smaller than q90");
    }

    #[test]
    fn encode_webp_produces_decodable_webp() {
        let img = decode_and_resize(&synthetic_jpeg(640, 480), 0).expect("decode");
        let out = encode_webp(&img, 80).expect("webp encode");
        assert_eq!(&out[0..4], b"RIFF", "expected RIFF header");
        assert_eq!(&out[8..12], b"WEBP", "expected WEBP form type");
        let decoded = image::load_from_memory(&out).expect("decode webp");
        assert_eq!(decoded.to_rgb8().dimensions(), (640, 480));
    }

    #[test]
    fn encode_avif_produces_avif_container() {
        let img = decode_and_resize(&synthetic_jpeg(320, 240), 0).expect("decode");
        let out = encode_avif(&img, 70).expect("avif encode");
        assert_eq!(&out[4..8], b"ftyp", "expected ftyp box");
        assert_eq!(&out[8..12], b"avif", "expected avif brand");
    }

    #[test]
    fn resize_to_caps_or_clones() {
        let img =
            decode_oriented(&synthetic_jpeg(4000, 3000), image::Limits::default()).expect("decode");
        assert_eq!(resize_to(&img, 2048).dimensions(), (2048, 1536));
        assert_eq!(resize_to(&img, 0).dimensions(), (4000, 3000));
        assert_eq!(resize_to(&img, 5000).dimensions(), (4000, 3000));
    }

    #[test]
    fn format_from_name_parses_known_and_rejects_unknown() {
        assert!(matches!(Format::from_name("jpeg"), Some(Format::Jpeg)));
        assert!(matches!(Format::from_name("jpg"), Some(Format::Jpeg)));
        assert!(matches!(Format::from_name("webp"), Some(Format::Webp)));
        assert!(matches!(Format::from_name("avif"), Some(Format::Avif)));
        assert!(Format::from_name("gif").is_none());
    }

    #[test]
    fn one_decode_yields_independent_sizes() {
        let img =
            decode_oriented(&synthetic_jpeg(2000, 1000), image::Limits::default()).expect("decode");
        let sizes: Vec<_> = [256u32, 1024, 2048]
            .iter()
            .map(|d| resize_to(&img, *d).dimensions())
            .collect();
        assert_eq!(sizes, vec![(256, 128), (1024, 512), (2000, 1000)]);
    }

    mod budgeted {
        use super::*;
        use crate::policy::PolicyMode;
        use crate::wasm_runtime::budget::testing::{self, MIB};

        fn enforced() -> HostCaps {
            HostCaps {
                mode: PolicyMode::Enforce,
                ..HostCaps::default()
            }
        }

        fn image_budget(caps: &HostCaps, share: Option<usize>) -> ImageBudget<'_> {
            ImageBudget {
                caps,
                share,
                path: "image_transform_multi",
            }
        }

        async fn stored(bytes: Vec<u8>) -> S3Storage {
            let storage = S3Storage::from_inner(cufflink_storage::in_memory("media"));
            storage
                .upload_to_bucket("media", "in.jpg", bytes, "image/jpeg")
                .await
                .expect("stored");
            storage
        }

        fn refused(result: Result<impl Sized, Fetch>) -> Option<Refused> {
            match result {
                Err(Fetch::Refused(refused)) => Some(refused),
                _ => None,
            }
        }

        #[tokio::test]
        async fn targets_are_encoded_and_uploaded_and_every_charge_is_returned() {
            let storage = stored(synthetic_jpeg(800, 600)).await;
            let caps = enforced();
            let (ledger, hold) = testing::hold(256 * MIB);
            let budget = image_budget(&caps, hold.share());
            let source = Source::read(&storage, &budget, "media", "in.jpg", hold)
                .await
                .unwrap_or_else(|_| panic!("read"));
            assert!(ledger.in_use() > 0, "the source is charged while held");
            let targets = vec![
                ("small.jpg".to_string(), 200, 80, Format::Jpeg),
                ("big.webp".to_string(), 0, 80, Format::Webp),
            ];
            let sizes = transform_multi(&storage, &budget, "media", source, targets)
                .await
                .unwrap_or_else(|_| panic!("transformed"));
            assert_eq!(sizes.len(), 2);
            for key in ["small.jpg", "big.webp"] {
                let bytes = storage
                    .download_from_bucket("media", key)
                    .await
                    .expect("uploaded");
                assert!(image::load_from_memory(&bytes).is_ok(), "{key}");
            }
            assert_eq!(ledger.in_use(), 0);
        }

        #[tokio::test]
        async fn a_source_past_the_input_cap_is_refused_before_it_is_read() {
            let storage = stored(synthetic_jpeg(64, 64)).await;
            let caps = HostCaps {
                image_input: 16,
                ..enforced()
            };
            let (ledger, hold) = testing::hold(64 * MIB);
            let budget = image_budget(&caps, None);
            let read = Source::read(&storage, &budget, "media", "in.jpg", hold).await;
            assert!(matches!(refused(read), Some(Refused::Cap(cap)) if cap.cap == "image_input"));
            assert_eq!(ledger.in_use(), 0);
        }

        #[tokio::test]
        async fn the_pixel_cap_and_the_decode_charge_are_read_from_the_header() {
            let storage = stored(synthetic_jpeg(1000, 1000)).await;
            let caps = HostCaps {
                image_pixels: 999_999,
                ..enforced()
            };
            let (_, hold) = testing::hold(64 * MIB);
            let budget = image_budget(&caps, None);
            let source = Source::read(&storage, &budget, "media", "in.jpg", hold)
                .await
                .unwrap_or_else(|_| panic!("read"));
            let result = transform(
                &storage,
                &budget,
                "media",
                source,
                "out.jpg",
                0,
                80,
                Format::Jpeg,
            )
            .await;
            assert!(
                matches!(refused(result), Some(Refused::Cap(cap)) if cap.cap == "image_pixels")
            );

            let caps = enforced();
            let (ledger, hold) = testing::hold(8 * MIB);
            let budget = image_budget(&caps, None);
            let source = Source::read(&storage, &budget, "media", "in.jpg", hold)
                .await
                .unwrap_or_else(|_| panic!("read"));
            let result = transform(
                &storage,
                &budget,
                "media",
                source,
                "out.jpg",
                0,
                80,
                Format::Jpeg,
            )
            .await;
            assert!(
                matches!(refused(result), Some(Refused::Budget(_))),
                "a million pixels need 20 MiB to decode"
            );
            assert_eq!(ledger.in_use(), 0);
            assert!(storage
                .download_from_bucket("media", "out.jpg")
                .await
                .is_err());
        }

        #[test]
        fn the_target_count_is_capped_and_the_share_lowers_the_pixel_cap() {
            let caps = enforced();
            assert!(image_budget(&caps, None).targets(16).is_ok());
            assert!(matches!(
                image_budget(&caps, None).targets(17),
                Err(Refused::Cap(cap)) if cap.cap == "image_targets"
            ));
            assert_eq!(caps.image_pixels(Some(20 * MIB)), MIB as u64);
        }

        #[test]
        fn a_decoder_refuses_dimensions_past_its_limits() {
            let mut limits = image::Limits::default();
            limits.max_image_width = Some(100);
            assert!(decode_oriented(&synthetic_jpeg(200, 50), limits).is_err());
        }
    }
}
