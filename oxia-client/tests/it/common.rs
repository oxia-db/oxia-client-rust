// Copyright 2025-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
