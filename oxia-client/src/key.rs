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

use crate::proto;
use std::cmp::Ordering;

/// The prefix of Oxia's own internal keys.
const INTERNAL_KEY_PREFIX: &str = "__oxia/";

/// The order of the keys in the shards of a namespace, which the shard
/// assignments report as its key sorting. Each shard returns the results of a
/// read in this order, and the client picks or merges the results of a read
/// sent to several shards in the same order, so it has to agree with the
/// server exactly.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum KeyOrder {
    /// The order of the storage format before Oxia v0.15 ([`compare`]),
    /// kept when the server does not report the key sorting.
    #[default]
    Legacy,
    /// `KEY_SORTING_NATURAL`: the keys are sorted by their bytes.
    Natural,
    /// `KEY_SORTING_HIERARCHICAL`: the keys with fewer `/` sort first, and
    /// the keys with as many `/` by their bytes, with `/` after any other
    /// byte.
    Hierarchical,
}

impl KeyOrder {
    /// The order of a namespace with this key sorting.
    pub(crate) fn from_key_sorting(key_sorting: i32) -> Self {
        match proto::KeySorting::try_from(key_sorting) {
            Ok(proto::KeySorting::Natural) => KeyOrder::Natural,
            Ok(proto::KeySorting::Hierarchical) => KeyOrder::Hierarchical,
            // Older servers do not report the key sorting: keep the order
            // that the client has always used, which is also the order of the
            // servers older than v0.15.
            Ok(proto::KeySorting::Unknown) | Err(_) => KeyOrder::Legacy,
        }
    }

    /// Compares two keys in this order.
    pub(crate) fn compare(self, a: &str, b: &str) -> Ordering {
        match self {
            KeyOrder::Legacy => compare(a, b),
            KeyOrder::Natural => compare_natural(a, b),
            KeyOrder::Hierarchical => compare_hierarchical(a, b),
        }
    }
}

/// Compares two keys as the server sorts them in a natural namespace: by
/// their bytes, except that the internal keys sort after all the others. The
/// server stores an internal key with its `__` prefix replaced by
/// `0xff 0xff`, which no UTF-8 key starts with.
fn compare_natural(a: &str, b: &str) -> Ordering {
    let internal = |key: &str| key.starts_with(INTERNAL_KEY_PREFIX);
    internal(a).cmp(&internal(b)).then_with(|| a.cmp(b))
}

/// Compares two keys as the server sorts them in a hierarchical namespace,
/// where it stores a key as its [`hierarchical_level`] on 2 bytes, followed by
/// the key with every `/` replaced by `0xff`, which sorts it after any other
/// byte of a UTF-8 key.
fn compare_hierarchical(a: &str, b: &str) -> Ordering {
    let encode = |byte: u8| if byte == b'/' { 0xff } else { byte };
    hierarchical_level(a)
        .cmp(&hierarchical_level(b))
        .then_with(|| a.bytes().map(encode).cmp(b.bytes().map(encode)))
}

/// The level of a key in a hierarchical namespace: its number of `/`, except
/// the last one of a key that ends with `//`, at most `0x7fff`, with the top
/// bit set for the internal keys, which sort after all the others.
fn hierarchical_level(key: &str) -> u16 {
    const MAX_LEVEL: usize = 0x7fff;
    const INTERNAL_KEYS: u16 = 0x8000;
    let mut separators = key.bytes().filter(|&byte| byte == b'/').count();
    if separators == 0 {
        return 0;
    }
    if key.ends_with("//") {
        // The trailing separator does not open a new level.
        separators -= 1;
    }
    let mut level = separators.min(MAX_LEVEL) as u16;
    if key.starts_with(INTERNAL_KEY_PREFIX) {
        level |= INTERNAL_KEYS;
    }
    level
}

/// Compares two keys using Oxia's slash-aware ordering: the order of the
/// storage format before Oxia v0.15, which the client keeps when the server
/// does not report the key sorting of the namespace ([`KeyOrder::Legacy`]).
///
/// It is a direct port of the Go reference `common/compare.CompareWithSlash`.
/// Comparison proceeds segment by segment, splitting on `/`; at any level a
/// key with no further `/` sorts *before* one that has a deeper segment,
/// regardless of the remaining bytes.
pub fn compare(mut a: &str, mut b: &str) -> Ordering {
    while !a.is_empty() && !b.is_empty() {
        match (a.find('/'), b.find('/')) {
            // Neither has a further separator: plain byte-wise comparison.
            (None, None) => return a.cmp(b),
            // The side that ends first (no more `/`) sorts before the deeper one,
            // independent of the bytes — this is the crux of the ordering.
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            // Both have another segment: compare this segment, else descend.
            (Some(ia), Some(ib)) => {
                let span = a[..ia].cmp(&b[..ib]);
                if span != Ordering::Equal {
                    return span;
                }
                a = &a[ia + 1..];
                b = &b[ib + 1..];
            }
        }
    }
    // One side is exhausted: the shorter key sorts first.
    a.len().cmp(&b.len())
}
#[cfg(test)]
mod tests {
    use crate::key::compare;
    use std::cmp::Ordering;

    #[test]
    fn test_simple_strings() {
        assert_eq!(compare("apple", "banana"), Ordering::Less);
        assert_eq!(compare("zoo", "zebra"), Ordering::Greater);
        assert_eq!(compare("test", "test"), Ordering::Equal);
    }

    #[test]
    fn test_single_slash_paths() {
        assert_eq!(compare("a/b", "a/c"), Ordering::Less);
        assert_eq!(compare("a/b", "a/b"), Ordering::Equal);
        assert_eq!(compare("a/c", "a/b"), Ordering::Greater);
        assert_eq!(compare("apple/fruit", "banana/fruit"), Ordering::Less);
        assert_eq!(compare("apple/fruit", "apple/grape"), Ordering::Less);
    }

    #[test]
    fn test_multi_slash_paths() {
        assert_eq!(compare("a/b/c", "a/b/d"), Ordering::Less);
        assert_eq!(compare("a/b/c", "a/b/c"), Ordering::Equal);
        assert_eq!(compare("a/b/c", "a/c/b"), Ordering::Less);
        assert_eq!(compare("a/b/c/d", "a/b/e/f"), Ordering::Less);
    }

    #[test]
    fn test_prefix_paths() {
        assert_eq!(compare("a/b", "a/b/c"), Ordering::Less);
        assert_eq!(compare("a/b/c", "a/b"), Ordering::Greater);
        assert_eq!(compare("a", "a/b"), Ordering::Less);
        assert_eq!(compare("a/b", "a"), Ordering::Greater);
    }

    #[test]
    fn test_empty_segments() {
        assert_eq!(compare("a//c", "a/b/c"), Ordering::Less);
        assert_eq!(compare("a/b", "a/b/"), Ordering::Less);
        // "a//" descends to "/" vs "b": "/" has a further segment, "b" does not,
        // so "a//" sorts after "a/b". (Matches the Go reference; the pre-fix
        // implementation wrongly returned Less by comparing bytes first.)
        assert_eq!(compare("a//", "a/b"), Ordering::Greater);
        assert_eq!(compare("", "a/b"), Ordering::Less);
    }

    /// Vectors copied verbatim from the Go reference
    /// (`common/compare/compare_with_slash_test.go`). If these diverge, this
    /// client orders `list`/`range_scan` results differently from the server and
    /// every other Oxia client.
    #[test]
    fn matches_go_compare_with_slash_vectors() {
        assert_eq!(compare("aaaaa", "aaaaa"), Ordering::Equal);
        assert_eq!(compare("aaaaa", "zzzzz"), Ordering::Less);
        assert_eq!(compare("bbbbb", "aaaaa"), Ordering::Greater);
        assert_eq!(compare("aaaaa", ""), Ordering::Greater);
        assert_eq!(compare("", "aaaaaa"), Ordering::Less);
        assert_eq!(compare("", ""), Ordering::Equal);
        assert_eq!(compare("aaaaa", "aaaaaaaaaaa"), Ordering::Less);
        assert_eq!(compare("aaaaaaaaaaa", "aaa"), Ordering::Greater);
        assert_eq!(compare("a", "/"), Ordering::Less);
        assert_eq!(compare("/", "a"), Ordering::Greater);
        assert_eq!(compare("/aaaa", "/bbbbb"), Ordering::Less);
        assert_eq!(compare("/aaaa", "/aa/a"), Ordering::Less);
        assert_eq!(compare("/aaaa/a", "/aaaa/b"), Ordering::Less);
        assert_eq!(compare("/aaaa/a/a", "/bbbbbbbbbb"), Ordering::Greater);
        assert_eq!(compare("/aaaa/a/a", "/aaaa/bbbbbbbbbb"), Ordering::Greater);
        assert_eq!(compare("/a/b/a/a/a", "/a/b/a/b"), Ordering::Greater);
    }

    #[test]
    fn test_empty_strings() {
        assert_eq!(compare("", ""), Ordering::Equal);
        assert_eq!(compare("a", ""), Ordering::Greater);
        assert_eq!(compare("", "a"), Ordering::Less);
        assert_eq!(compare("", "/"), Ordering::Less);
    }

    #[test]
    fn test_different_prefixes() {
        assert_eq!(compare("c/a", "a/b"), Ordering::Greater);
        assert_eq!(compare("a/b", "c/a"), Ordering::Less);
    }

    #[test]
    fn test_paths_with_complex_characters() {
        assert_eq!(compare("a-1/b", "a-2/b"), Ordering::Less);
        assert_eq!(compare("a_1/b", "a_2/b"), Ordering::Less);
        assert_eq!(compare("1/2/3", "1/2/4"), Ordering::Less);
    }

    #[test]
    fn test_leading_trailing_slashes() {
        assert_eq!(compare("/a/b", "a/b"), Ordering::Less);
        assert_eq!(compare("a/b/", "a/b"), Ordering::Greater);
        assert_eq!(compare("/a/b/", "a/b"), Ordering::Less);
        assert_eq!(compare("/a/b/", "/a/b"), Ordering::Greater);
    }

    #[test]
    fn test_root_vs_subdirectory() {
        assert_eq!(compare("/", "a/b"), Ordering::Less);
        assert_eq!(compare("a/b", "/"), Ordering::Greater);
    }

    /// Vectors copied from the Go reference (`oxia/key_order_test.go`): the
    /// order of each key sorting, and the legacy order when the server does
    /// not report one.
    #[test]
    fn key_order_matches_go_vectors() {
        use super::KeyOrder;
        use Ordering::{Equal, Greater, Less};

        for (a, b, natural, hierarchical, legacy) in [
            ("a", "a", Equal, Equal, Equal),
            ("a0", "a/y/z", Greater, Less, Less),
            ("a/x", "ab/y", Less, Greater, Less),
            ("b/x", "a/y/z", Greater, Less, Greater),
            ("a/z", "a/y/z", Greater, Less, Less),
            // The internal keys sort after the other keys.
            ("__oxia/x", "z/z", Greater, Greater, Less),
        ] {
            for (order, expected) in [
                (KeyOrder::Natural, natural),
                (KeyOrder::Hierarchical, hierarchical),
                (KeyOrder::Legacy, legacy),
            ] {
                assert_eq!(order.compare(a, b), expected, "{order:?}: {a:?} vs {b:?}");
                assert_eq!(
                    order.compare(b, a),
                    expected.reverse(),
                    "{order:?}: {b:?} vs {a:?}"
                );
            }
        }
    }

    #[test]
    fn key_order_from_key_sorting() {
        use super::KeyOrder;
        use crate::proto::KeySorting;

        let order = |key_sorting: i32| KeyOrder::from_key_sorting(key_sorting);
        assert_eq!(order(KeySorting::Natural as i32), KeyOrder::Natural);
        assert_eq!(
            order(KeySorting::Hierarchical as i32),
            KeyOrder::Hierarchical
        );
        assert_eq!(order(KeySorting::Unknown as i32), KeyOrder::Legacy);
        // A key sorting this client does not know keeps the legacy order.
        assert_eq!(order(99), KeyOrder::Legacy);
    }

    /// Vectors copied from the Go reference
    /// (`common/compare/encode_test.go`, `TestEncodeCompare`), which compares
    /// the keys as the server stores them in a hierarchical namespace.
    #[test]
    fn hierarchical_matches_go_encoder_vectors() {
        let cmp = super::compare_hierarchical;

        assert_eq!(cmp("aaaaa", "aaaaa"), Ordering::Equal);
        assert_eq!(cmp("aaaaa", "zzzzz"), Ordering::Less);
        assert_eq!(cmp("bbbbb", "aaaaa"), Ordering::Greater);

        assert_eq!(cmp("aaaaa", ""), Ordering::Greater);
        assert_eq!(cmp("", "aaaaaa"), Ordering::Less);
        assert_eq!(cmp("", ""), Ordering::Equal);

        assert_eq!(cmp("aaaaa", "aaaaaaaaaaa"), Ordering::Less);
        assert_eq!(cmp("aaaaaaaaaaa", "aaa"), Ordering::Greater);

        assert_eq!(cmp("a", "/"), Ordering::Less);
        assert_eq!(cmp("/", "a"), Ordering::Greater);

        assert_eq!(cmp("/aaaa", "/bbbbb"), Ordering::Less);
        assert_eq!(cmp("/aaaa", "/aa/a"), Ordering::Less);
        assert_eq!(cmp("/aaaa/a", "/aaaa/b"), Ordering::Less);
        assert_eq!(cmp("/aaaa/a/a", "/bbbbbbbbbb"), Ordering::Greater);
        assert_eq!(cmp("/aaaa/a/a", "/aaaa/bbbbbbbbbb"), Ordering::Greater);

        assert_eq!(cmp("/a/b/a/a/a", "/a/b/a/b"), Ordering::Greater);

        assert_eq!(cmp("/a", "/b"), Ordering::Less);
        assert_eq!(cmp("/a", "/a/"), Ordering::Less);
        assert_eq!(cmp("/a/", "/a//"), Ordering::Less);
        assert_eq!(cmp("/a/a-2", "/b/c"), Ordering::Less);
        assert_eq!(cmp("/", "/a"), Ordering::Less);
        assert_eq!(cmp("/a", "//"), Ordering::Less);
        assert_eq!(cmp("/b", "//"), Ordering::Less);
        assert_eq!(cmp("//", "/a/a-1"), Ordering::Less);

        assert_eq!(cmp("/a/a-1", "/a//"), Ordering::Less);
        assert_eq!(cmp("/a//", "/b/c"), Ordering::Less);
    }

    /// The internal keys sort after the other keys in both key sortings
    /// (`TestEncodeInternalKeys` in the Go reference), even after a key that
    /// sorts after their `__` prefix by its bytes.
    #[test]
    fn internal_keys_sort_last() {
        use super::KeyOrder;

        for order in [KeyOrder::Natural, KeyOrder::Hierarchical] {
            for key in ["my-key", "/my-key", "~", "~/~/~"] {
                assert_eq!(
                    order.compare(key, "__oxia/xyz"),
                    Ordering::Less,
                    "{order:?}: {key:?}"
                );
            }
            assert_eq!(
                order.compare("__oxia/a", "__oxia/b"),
                Ordering::Less,
                "{order:?}"
            );
        }
    }

    /// A key deeper than the level can count shares the deepest level, rather
    /// than reaching into the internal keys' bit or wrapping around
    /// (`TestEncodeDeeplyNestedKeys` in the Go reference).
    #[test]
    fn deeply_nested_keys_share_the_deepest_level() {
        let cmp = super::compare_hierarchical;

        for (separators, level) in [
            (0x7ffe, 0x7ffe),
            (0x8000, 0x7fff),
            (0x8001, 0x7fff),
            (1 << 16, 0x7fff),
        ] {
            // The trailing "a" keeps the key from ending with `//`, which
            // would drop the count by one.
            let key = "/".repeat(separators) + "a";
            assert_eq!(super::hierarchical_level(&key), level, "{separators}");
            assert_eq!(cmp(&key, "__oxia/xyz"), Ordering::Less, "{separators}");
            assert_eq!(cmp(&key, "a/b"), Ordering::Greater, "{separators}");
        }
        // Past the deepest level, the keys compare by their bytes alone.
        let deep = |separators: usize, last: &str| "/".repeat(separators) + last;
        assert_eq!(
            cmp(&deep(0x9000, "a"), &deep(0x8000, "b")),
            Ordering::Greater
        );
        assert_eq!(cmp(&deep(0x8000, "a"), &deep(0x8000, "b")), Ordering::Less);
        // An internal key keeps its bit however deep it is.
        let internal = format!("__oxia/{}a", "/".repeat(1 << 16));
        assert_eq!(super::hierarchical_level(&internal), 0xffff);
    }
}
