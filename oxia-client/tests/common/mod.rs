//! Shared by the Docker-backed test files.

use testcontainers::GenericImage;

// `nightly-beta` is built daily from Oxia's main branch. CI pre-pulls the same
// image (`.github/workflows/ci.yml`), so change it there too.
const DEFAULT_OXIA_IMAGE: &str = "oxia/oxia";
const DEFAULT_OXIA_TAG: &str = "nightly-beta";

/// The Oxia server image the tests run, unless the `OXIA_IMAGE` and `OXIA_TAG`
/// environment variables override its name and tag.
pub fn oxia_image() -> GenericImage {
    let image = std::env::var("OXIA_IMAGE").unwrap_or_else(|_| DEFAULT_OXIA_IMAGE.to_string());
    let tag = std::env::var("OXIA_TAG").unwrap_or_else(|_| DEFAULT_OXIA_TAG.to_string());
    GenericImage::new(image, tag)
}
