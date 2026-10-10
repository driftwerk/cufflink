//! Image transforms.

use crate::image::VariantSpec;

/// What answers `image::*`.
pub trait ImageHost {
    /// The bytes written for each variant of the object at `in_key`, in
    /// order; `None` when any of them could not be written.
    fn transform(
        &self,
        bucket: &str,
        in_key: &str,
        variants: &[VariantSpec<'_>],
    ) -> Option<Vec<u32>>;
}
