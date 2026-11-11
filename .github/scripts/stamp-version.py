# Copyright 2025-2026 The Oxia Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Writes the version a release tag names into `oxia-client/Cargo.toml`.

The committed version is a placeholder (`0.0.0-dev`): the tag is the single
source of truth for what a release is numbered, and this stamps it in before
anything is built or published. See RELEASING.md.

    python3 .github/scripts/stamp-version.py v0.3.0

Prints the bare version (`0.3.0`) on stdout. Exits non-zero, having changed
nothing, if the tag is not a release tag or the file does not look as
expected — a wrong version published to crates.io cannot be taken back.
"""

import re
import sys
from pathlib import Path

# `vMAJOR.MINOR.PATCH`, with an optional pre-release suffix (`-rc.1`). No
# build metadata: crates.io ignores it, so it cannot distinguish a release.
TAG = re.compile(r"^v(?P<version>[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?)$")

MANIFEST = Path("oxia-client/Cargo.toml")

# The `version` of the `[package]` table, and nothing else in the file: a
# dependency's `version = ` must not be touched.
PACKAGE_VERSION = re.compile(
    r"(?P<before>^\[package\](?:.*\n)*?)^version = \"[^\"]*\"$",
    re.MULTILINE,
)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <tag>", file=sys.stderr)
        return 2

    tag = argv[1]
    matched = TAG.match(tag)
    if not matched:
        print(
            f"error: {tag!r} is not a release tag; expected vMAJOR.MINOR.PATCH"
            " with an optional pre-release suffix, as in v0.3.0 or v0.3.0-rc.1",
            file=sys.stderr,
        )
        return 1
    version = matched.group("version")

    if not MANIFEST.is_file():
        print(f"error: {MANIFEST} not found; run this from the repository root",
              file=sys.stderr)
        return 1
    manifest = MANIFEST.read_text()

    stamped, count = PACKAGE_VERSION.subn(
        lambda m: f'{m.group("before")}version = "{version}"', manifest, count=1
    )
    if count != 1:
        print(f"error: no [package] version to replace in {MANIFEST}",
              file=sys.stderr)
        return 1

    MANIFEST.write_text(stamped)
    print(version)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
