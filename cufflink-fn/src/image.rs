//! Image transforms executed on the platform host.
//!
//! The platform downloads the source from object storage, decodes it,
//! optionally downscales the longest edge to `max_dim` (Lanczos3),
//! JPEG-encodes at the requested quality, and writes the result back to
//! the same bucket. CPU work runs in a host-side blocking task so it
//! does not stall the async runtime.

#[cfg(target_arch = "wasm32")]
use crate::{ffi, host_buffer};
#[cfg(target_arch = "wasm32")]
use serde_json::Value;

/// Resize an image stored at `(bucket, in_key)`, JPEG-encode it, and
/// upload to `(bucket, out_key)`.
///
/// `max_dim == 0` skips resizing. `quality` must be in `1..=100`.
/// EXIF orientation is honoured, so portrait photos are not rotated.
/// Returns the number of bytes written, or `None` on failure (the
/// host logs the underlying error).
///
/// ```rust,ignore
/// let bytes = image::transform_jpeg("media", "raw/photo.jpg", "raw/photo.display.jpg", 1024, 80);
/// ```
pub fn transform_jpeg(
    bucket: &str,
    in_key: &str,
    out_key: &str,
    max_dim: u32,
    quality: u8,
) -> Option<u32> {
    transform(Codec::Jpeg, bucket, in_key, out_key, max_dim, quality)
}

/// Like [`transform_jpeg`] but encodes lossy **WebP** (`zenwebp` on the
/// host). Typically ~25–30% smaller than JPEG at equal quality.
///
/// ```rust,ignore
/// let bytes = image::transform_webp("media", "raw/photo.jpg", "raw/photo.display.webp", 1024, 80);
/// ```
pub fn transform_webp(
    bucket: &str,
    in_key: &str,
    out_key: &str,
    max_dim: u32,
    quality: u8,
) -> Option<u32> {
    transform(Codec::Webp, bucket, in_key, out_key, max_dim, quality)
}

/// Like [`transform_jpeg`] but encodes lossy **AVIF** (`ravif` on the
/// host). Smaller than WebP at equal quality, but encoding is noticeably
/// CPU-heavier — prefer it for backfill/async paths over hot requests.
///
/// ```rust,ignore
/// let bytes = image::transform_avif("media", "raw/photo.jpg", "raw/photo.display.avif", 1024, 80);
/// ```
pub fn transform_avif(
    bucket: &str,
    in_key: &str,
    out_key: &str,
    max_dim: u32,
    quality: u8,
) -> Option<u32> {
    transform(Codec::Avif, bucket, in_key, out_key, max_dim, quality)
}

/// One target for [`transform_multi`]: an output key, the max long-edge in
/// px (`0` = no resize), JPEG-style `quality` (1..=100), and `format`
/// (`"jpeg"`, `"webp"`, or `"avif"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VariantSpec<'a> {
    pub out_key: &'a str,
    pub max_dim: u32,
    pub quality: u8,
    pub format: &'a str,
}

/// Decode the source at `(bucket, in_key)` ONCE and write each
/// [`VariantSpec`] (resized + encoded in its format), returning the bytes
/// written per variant in order, or `None` on failure. Prefer this over
/// multiple single-output calls when several sizes/formats come from one
/// original — it decodes the (potentially large) source a single time.
pub fn transform_multi(bucket: &str, in_key: &str, targets: &[VariantSpec]) -> Option<Vec<u32>> {
    #[cfg(target_arch = "wasm32")]
    {
        let specs: Vec<Value> = targets
            .iter()
            .map(|t| {
                serde_json::json!({
                    "out_key": t.out_key,
                    "max_dim": t.max_dim,
                    "quality": t.quality,
                    "format": t.format,
                })
            })
            .collect();
        let payload = Value::Array(specs).to_string();
        let bucket_bytes = bucket.as_bytes();
        let in_key_bytes = in_key.as_bytes();
        let payload_bytes = payload.as_bytes();
        let result = unsafe {
            ffi::image_transform_multi(
                bucket_bytes.as_ptr() as i32,
                bucket_bytes.len() as i32,
                in_key_bytes.as_ptr() as i32,
                in_key_bytes.len() as i32,
                payload_bytes.as_ptr() as i32,
                payload_bytes.len() as i32,
            )
        };
        if result < 0 {
            return None;
        }
        serde_json::from_str::<Vec<u32>>(&host_buffer::read_host_string().unwrap_or_default()).ok()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::image().and_then(|host| host.transform(bucket, in_key, targets))
    }
}

/// Output codec for [`transform`].
#[derive(Clone, Copy)]
enum Codec {
    Jpeg,
    Webp,
    Avif,
}

#[cfg(not(target_arch = "wasm32"))]
impl Codec {
    /// The `format` of a [`VariantSpec`] that encodes as this codec.
    fn name(self) -> &'static str {
        match self {
            Codec::Jpeg => "jpeg",
            Codec::Webp => "webp",
            Codec::Avif => "avif",
        }
    }
}

/// Shared dispatch over the three `image_transform_*` host functions.
fn transform(
    codec: Codec,
    bucket: &str,
    in_key: &str,
    out_key: &str,
    max_dim: u32,
    quality: u8,
) -> Option<u32> {
    #[cfg(target_arch = "wasm32")]
    {
        let bucket_bytes = bucket.as_bytes();
        let in_key_bytes = in_key.as_bytes();
        let out_key_bytes = out_key.as_bytes();
        let host_fn = match codec {
            Codec::Jpeg => ffi::image_transform_jpeg,
            Codec::Webp => ffi::image_transform_webp,
            Codec::Avif => ffi::image_transform_avif,
        };
        let result = unsafe {
            host_fn(
                bucket_bytes.as_ptr() as i32,
                bucket_bytes.len() as i32,
                in_key_bytes.as_ptr() as i32,
                in_key_bytes.len() as i32,
                out_key_bytes.as_ptr() as i32,
                out_key_bytes.len() as i32,
                max_dim as i32,
                quality as i32,
            )
        };
        if result < 0 {
            return None;
        }
        Some(result as u32)
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let variant = VariantSpec {
            out_key,
            max_dim,
            quality,
            format: codec.name(),
        };
        crate::host::image()
            .and_then(|host| host.transform(bucket, in_key, &[variant]))
            .and_then(|sizes| sizes.first().copied())
    }
}

#[cfg(test)]
mod tests {
    use crate::image;

    #[test]
    fn transform_multi_is_inert_off_wasm() {
        use image::VariantSpec;
        assert!(image::transform_multi(
            "media",
            "raw/photo.jpg",
            &[
                VariantSpec {
                    out_key: "raw/photo.thumb.jpg",
                    max_dim: 256,
                    quality: 75,
                    format: "jpeg",
                },
                VariantSpec {
                    out_key: "raw/photo.thumb.webp",
                    max_dim: 256,
                    quality: 75,
                    format: "webp",
                },
            ],
        )
        .is_none());
    }

    #[test]
    fn test_image_transform_jpeg_noop_on_native() {
        let bytes = image::transform_jpeg("my-bucket", "in.jpg", "out.jpg", 1024, 80);
        assert!(bytes.is_none());
    }
}
