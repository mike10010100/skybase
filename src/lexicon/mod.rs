//! ATProto Lexicon data models for rich text and facets.
//!
//! Provides Serde representations of `app.bsky.richtext.facet` and its associated
//! `byteSlice`, `#mention`, `#link`, and `#tag` feature types, plus a
//! UTF-8-byte-accurate [`extract_link_facets`] helper for annotating link facets.

use serde::{Deserialize, Serialize};

/// UTF-8 byte slice range for rich text facets (`app.bsky.richtext.facet#byteSlice`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ByteSlice {
    /// Start byte index (inclusive).
    pub byte_start: usize,
    /// End byte index (exclusive).
    pub byte_end: usize,
}

impl ByteSlice {
    /// Creates a new [`ByteSlice`].
    #[must_use]
    pub const fn new(byte_start: usize, byte_end: usize) -> Self {
        Self {
            byte_start,
            byte_end,
        }
    }
}

/// Feature metadata embedded within rich text facets (`app.bsky.richtext.facet#features`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "$type")]
pub enum FacetFeature {
    /// Explicit user mention pointing to a decentralized identifier (DID).
    #[serde(rename = "app.bsky.richtext.facet#mention")]
    Mention {
        /// Decentralized identifier of the mentioned user (`did:plc:...`).
        did: String,
    },
    /// External hyperlink.
    #[serde(rename = "app.bsky.richtext.facet#link")]
    Link {
        /// Target URI string.
        uri: String,
    },
    /// Tag or hashtag.
    #[serde(rename = "app.bsky.richtext.facet#tag")]
    Tag {
        /// Hashtag label without leading `#`.
        tag: String,
    },
    /// Forward-compatible fallback for unrecognized future facet types.
    #[serde(other)]
    Unknown,
}

impl FacetFeature {
    /// Returns the mentioned DID if this feature is a [`FacetFeature::Mention`].
    #[must_use]
    pub fn as_mention_did(&self) -> Option<&str> {
        match self {
            Self::Mention { did } => Some(did.as_str()),
            _ => None,
        }
    }

    /// Returns `true` if this feature is a mention.
    #[must_use]
    pub fn is_mention(&self) -> bool {
        matches!(self, Self::Mention { .. })
    }
}

/// Rich text facet annotation associating a byte slice with features (`app.bsky.richtext.facet`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Facet {
    /// Byte range within the post text.
    pub index: ByteSlice,
    /// Feature annotations applied to this slice.
    pub features: Vec<FacetFeature>,
}

impl Facet {
    /// Creates a new [`Facet`].
    #[must_use]
    pub fn new(index: ByteSlice, features: Vec<FacetFeature>) -> Self {
        Self { index, features }
    }

    /// Creates a new link facet spanning byte range `[byte_start, byte_end)` pointing to `uri`.
    #[must_use]
    pub fn link(byte_start: usize, byte_end: usize, uri: impl Into<String>) -> Self {
        Self {
            index: ByteSlice::new(byte_start, byte_end),
            features: vec![FacetFeature::Link { uri: uri.into() }],
        }
    }

    /// Returns an iterator over all mentioned DIDs in this facet.
    pub fn mentioned_dids(&self) -> impl Iterator<Item = &str> {
        self.features
            .iter()
            .filter_map(FacetFeature::as_mention_did)
    }

    /// Returns `true` if this facet contains an explicit mention targeting `target_did`.
    #[must_use]
    pub fn contains_mention_did(&self, target_did: &str) -> bool {
        self.mentioned_dids().any(|d| d == target_did)
    }
}

/// Alias for [`ByteSlice`] conforming to ATProto rich text byte index terminology.
pub type FacetIndex = ByteSlice;

/// Extracts all HTTP and HTTPS link facets from `text` with accurate UTF-8 byte slice offsets.
///
/// Strips trailing punctuation (such as `.`, `,`, `!`, `?`, `:`, `;`, quotes, backticks, or
/// unbalanced parentheses) and accounts for preceding multi-byte UTF-8 sequences
/// (emojis, CJK characters).
#[must_use]
pub fn extract_link_facets(text: &str) -> Vec<Facet> {
    use std::sync::LazyLock;

    static URL_REGEX: LazyLock<Option<regex::Regex>> =
        LazyLock::new(|| regex::Regex::new(r"(?-u:\b)(?i:https?)://[\x21-\x7E]+").ok());

    let mut facets = Vec::new();
    let Some(ref re) = *URL_REGEX else {
        return facets;
    };
    for mat in re.find_iter(text) {
        let raw = mat.as_str();
        let trimmed = trim_trailing_url_punctuation(raw);
        if trimmed.is_empty() {
            continue;
        }

        if let Ok(parsed) = url::Url::parse(trimmed) {
            if (parsed.scheme() == "http" || parsed.scheme() == "https") && parsed.has_host() {
                let start = mat.start();
                let end = start + trimmed.len();
                facets.push(Facet::link(start, end, trimmed));
            }
        }
    }
    facets
}

/// Trims trailing punctuation, quotes, and unbalanced delimiters from a URL match.
fn trim_trailing_url_punctuation(raw_url: &str) -> &str {
    let mut url = raw_url;
    loop {
        if url.is_empty() {
            break;
        }

        let last_char = match url.chars().next_back() {
            Some(c) => c,
            None => break,
        };

        match last_char {
            // Standard sentence and delimiter punctuation
            '.' | ',' | ';' | ':' | '!' | '?' | '"' | '\'' | '`' | '>' | '<' | '*' | '~'
            // Typographic quotes and brackets
            | '”' | '“' | '’' | '‘' | '»' | '«' | '›' | '‹' | '„' | '‟' | '‚'
            // Ellipses and dashes
            | '…' | '⋯' | '‥' | '—' | '–'
            // Fullwidth & CJK punctuation
            | '。' | '、' | '！' | '？' | '：' | '；' | '，' | '．'
            // Invisible, zero-width, and directional formatting characters
            | '\u{200B}' | '\u{200C}' | '\u{200D}' | '\u{200E}' | '\u{200F}' | '\u{FEFF}' => {
                url = &url[..url.len() - last_char.len_utf8()];
            }
            // ASCII paired delimiters (strip if unbalanced)
            ')' if is_unbalanced_closing(url, '(', ')') => {
                url = &url[..url.len() - 1];
            }
            ']' if is_unbalanced_closing(url, '[', ']') => {
                url = &url[..url.len() - 1];
            }
            '}' if is_unbalanced_closing(url, '{', '}') => {
                url = &url[..url.len() - 1];
            }
            // CJK / Fullwidth paired delimiters (strip if unbalanced)
            '）' if is_unbalanced_closing(url, '（', '）') => {
                url = &url[..url.len() - '）'.len_utf8()];
            }
            '］' if is_unbalanced_closing(url, '［', '］') => {
                url = &url[..url.len() - '］'.len_utf8()];
            }
            '｝' if is_unbalanced_closing(url, '｛', '｝') => {
                url = &url[..url.len() - '｝'.len_utf8()];
            }
            '》' if is_unbalanced_closing(url, '《', '》') => {
                url = &url[..url.len() - '》'.len_utf8()];
            }
            '〉' if is_unbalanced_closing(url, '〈', '〉') => {
                url = &url[..url.len() - '〉'.len_utf8()];
            }
            '」' if is_unbalanced_closing(url, '「', '」') => {
                url = &url[..url.len() - '」'.len_utf8()];
            }
            '』' if is_unbalanced_closing(url, '『', '』') => {
                url = &url[..url.len() - '』'.len_utf8()];
            }
            '】' if is_unbalanced_closing(url, '【', '】') => {
                url = &url[..url.len() - '】'.len_utf8()];
            }
            '〕' if is_unbalanced_closing(url, '〔', '〕') => {
                url = &url[..url.len() - '〕'.len_utf8()];
            }
            '〗' if is_unbalanced_closing(url, '〖', '〗') => {
                url = &url[..url.len() - '〗'.len_utf8()];
            }
            '〙' if is_unbalanced_closing(url, '〘', '〙') => {
                url = &url[..url.len() - '〙'.len_utf8()];
            }
            _ => break,
        }
    }
    url
}

/// Returns `true` if `close` appears more often than `open` in `url`.
fn is_unbalanced_closing(url: &str, open: char, close: char) -> bool {
    let open_count = url.chars().filter(|&c| c == open).count();
    let close_count = url.chars().filter(|&c| c == close).count();
    close_count > open_count
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_link_facets_empty_and_no_links() {
        assert!(extract_link_facets("").is_empty());
        assert!(extract_link_facets("   ").is_empty());
        assert!(extract_link_facets("Hello world! No links here.").is_empty());
        assert!(extract_link_facets("Just http:// or https:// with no host").is_empty());
        assert!(extract_link_facets("ftp://example.com is not http/https").is_empty());
    }

    #[test]
    fn test_extract_link_facets_simple() {
        let text = "Visit https://skybouncer.mike10010100.com/auth to activate";
        let facets = extract_link_facets(text);
        assert_eq!(facets.len(), 1);

        let facet = &facets[0];
        assert_eq!(
            &text[facet.index.byte_start..facet.index.byte_end],
            "https://skybouncer.mike10010100.com/auth"
        );
        match &facet.features[0] {
            FacetFeature::Link { uri } => {
                assert_eq!(uri, "https://skybouncer.mike10010100.com/auth");
            }
            _ => panic!("expected Link feature"),
        }
    }

    #[test]
    fn test_extract_link_facets_emojis_and_multibyte() {
        let text =
            "👋 Welcome to Skybouncer! Auth: https://skybouncer.mike10010100.com/auth 🎉 Enjoy!";
        let facets = extract_link_facets(text);
        assert_eq!(facets.len(), 1);

        let facet = &facets[0];
        // Confirm UTF-8 byte slice matches URL text exactly
        assert_eq!(
            &text[facet.index.byte_start..facet.index.byte_end],
            "https://skybouncer.mike10010100.com/auth"
        );
        // UTF-8 byte start offset is strictly greater than character count due to multi-byte emojis
        let char_count_before = text
            .chars()
            .take_while(|c| *c != 'h' || !text.contains("https://"))
            .count();
        assert!(facet.index.byte_start > char_count_before);

        // Immediately adjacent emoji without whitespace
        let text_adjacent = "Auth: 🔗https://skybouncer.mike10010100.com/auth";
        let facets_adjacent = extract_link_facets(text_adjacent);
        assert_eq!(facets_adjacent.len(), 1);
        assert_eq!(
            &text_adjacent[facets_adjacent[0].index.byte_start..facets_adjacent[0].index.byte_end],
            "https://skybouncer.mike10010100.com/auth"
        );

        // Skin-tone modifier and ZWJ sequence preceding URL
        let text_zwj = "Leader 👨‍💻: https://skybouncer.mike10010100.com/auth";
        let facets_zwj = extract_link_facets(text_zwj);
        assert_eq!(facets_zwj.len(), 1);
        assert_eq!(
            &text_zwj[facets_zwj[0].index.byte_start..facets_zwj[0].index.byte_end],
            "https://skybouncer.mike10010100.com/auth"
        );

        // RTL mark preceding URL
        let text_rtl = "مرحبا \u{200F}https://skybouncer.mike10010100.com/auth";
        let facets_rtl = extract_link_facets(text_rtl);
        assert_eq!(facets_rtl.len(), 1);
        assert_eq!(
            &text_rtl[facets_rtl[0].index.byte_start..facets_rtl[0].index.byte_end],
            "https://skybouncer.mike10010100.com/auth"
        );

        // Immediately adjacent CJK characters without whitespace or punctuation
        let text_cjk_direct = "请访问https://skybouncer.mike10010100.com/auth进行授权";
        let facets_cjk_direct = extract_link_facets(text_cjk_direct);
        assert_eq!(facets_cjk_direct.len(), 1);
        assert_eq!(
            &text_cjk_direct
                [facets_cjk_direct[0].index.byte_start..facets_cjk_direct[0].index.byte_end],
            "https://skybouncer.mike10010100.com/auth"
        );

        let text_jp_direct = "詳細はhttps://skybouncer.mike10010100.com/authです";
        let facets_jp_direct = extract_link_facets(text_jp_direct);
        assert_eq!(facets_jp_direct.len(), 1);
        assert_eq!(
            &text_jp_direct
                [facets_jp_direct[0].index.byte_start..facets_jp_direct[0].index.byte_end],
            "https://skybouncer.mike10010100.com/auth"
        );

        // Uppercase and mixed-case schemes (RFC 3986 case-insensitive scheme matching)
        let text_upper = "Visit HTTPS://skybouncer.mike10010100.com/auth now";
        let facets_upper = extract_link_facets(text_upper);
        assert_eq!(facets_upper.len(), 1);
        assert_eq!(
            &text_upper[facets_upper[0].index.byte_start..facets_upper[0].index.byte_end],
            "HTTPS://skybouncer.mike10010100.com/auth"
        );

        let text_mixed = "Visit Http://skybouncer.mike10010100.com/auth now";
        let facets_mixed = extract_link_facets(text_mixed);
        assert_eq!(facets_mixed.len(), 1);
        assert_eq!(
            &text_mixed[facets_mixed[0].index.byte_start..facets_mixed[0].index.byte_end],
            "Http://skybouncer.mike10010100.com/auth"
        );

        // Immediately trailing emoji without whitespace
        let text_trailing_emoji = "Visit https://skybouncer.mike10010100.com/auth🚀 immediately!";
        let facets_trailing_emoji = extract_link_facets(text_trailing_emoji);
        assert_eq!(facets_trailing_emoji.len(), 1);
        assert_eq!(
            &text_trailing_emoji[facets_trailing_emoji[0].index.byte_start
                ..facets_trailing_emoji[0].index.byte_end],
            "https://skybouncer.mike10010100.com/auth"
        );

        // Rejection of invalid word prefix (not a URL)
        let text_invalid_prefix = "badprefixhttps://skybouncer.mike10010100.com/auth and 123https://skybouncer.mike10010100.com/auth";
        assert!(extract_link_facets(text_invalid_prefix).is_empty());
    }

    #[test]
    fn test_extract_link_facets_trailing_punctuation() {
        let text = "Links: https://example.com/one., https://example.com/two! and https://example.com/three?";
        let facets = extract_link_facets(text);
        assert_eq!(facets.len(), 3);

        assert_eq!(
            &text[facets[0].index.byte_start..facets[0].index.byte_end],
            "https://example.com/one"
        );
        assert_eq!(
            &text[facets[1].index.byte_start..facets[1].index.byte_end],
            "https://example.com/two"
        );
        assert_eq!(
            &text[facets[2].index.byte_start..facets[2].index.byte_end],
            "https://example.com/three"
        );
    }

    #[test]
    fn test_extract_link_facets_markdown_quotes_and_cjk_punctuation() {
        let text = "Markdown: **https://example.com/bold**, *https://example.com/italic*, ~https://example.com/strike~, quotes: “https://example.com/quote”, ‘https://example.com/single’, angle: <https://example.com/angle>, ellipsis: https://example.com/more…, cjk: https://example.com/cjk。, brackets: 《https://example.com/book》, 【https://example.com/notice】, （https://example.com/fullparens）, zwsp: https://example.com/hidden\u{200B}, percent: https://example.com/foo%20bar.";
        let facets = extract_link_facets(text);
        assert_eq!(facets.len(), 13);

        let expected_urls = [
            "https://example.com/bold",
            "https://example.com/italic",
            "https://example.com/strike",
            "https://example.com/quote",
            "https://example.com/single",
            "https://example.com/angle",
            "https://example.com/more",
            "https://example.com/cjk",
            "https://example.com/book",
            "https://example.com/notice",
            "https://example.com/fullparens",
            "https://example.com/hidden",
            "https://example.com/foo%20bar",
        ];

        for (i, expected) in expected_urls.iter().enumerate() {
            let slice = &text[facets[i].index.byte_start..facets[i].index.byte_end];
            assert_eq!(slice, *expected, "failed at index {i}");
            match &facets[i].features[0] {
                FacetFeature::Link { uri } => assert_eq!(uri, *expected),
                _ => panic!("expected Link feature"),
            }
        }
    }

    #[test]
    fn test_extract_link_facets_parentheses_and_brackets() {
        let text = "Auth URL is (https://example.com/auth) or [https://example.com/alt].";
        let facets = extract_link_facets(text);
        assert_eq!(facets.len(), 2);

        assert_eq!(
            &text[facets[0].index.byte_start..facets[0].index.byte_end],
            "https://example.com/auth"
        );
        assert_eq!(
            &text[facets[1].index.byte_start..facets[1].index.byte_end],
            "https://example.com/alt"
        );
    }

    #[test]
    fn test_extract_link_facets_balanced_parentheses_in_url() {
        // Balanced parens in Wikipedia URL should be preserved
        let text = "Read https://en.wikipedia.org/wiki/Rust_(programming_language) today.";
        let facets = extract_link_facets(text);
        assert_eq!(facets.len(), 1);
        assert_eq!(
            &text[facets[0].index.byte_start..facets[0].index.byte_end],
            "https://en.wikipedia.org/wiki/Rust_(programming_language)"
        );

        // Parens surrounding the Wikipedia URL should have only the outer paren stripped
        let text2 = "Read (https://en.wikipedia.org/wiki/Rust_(programming_language)) today.";
        let facets2 = extract_link_facets(text2);
        assert_eq!(facets2.len(), 1);
        assert_eq!(
            &text2[facets2[0].index.byte_start..facets2[0].index.byte_end],
            "https://en.wikipedia.org/wiki/Rust_(programming_language)"
        );

        // Markdown link syntax with Wikipedia URL
        let text3 = "Check [Wikipedia](https://en.wikipedia.org/wiki/Rust_(programming_language)) for details.";
        let facets3 = extract_link_facets(text3);
        assert_eq!(facets3.len(), 1);
        assert_eq!(
            &text3[facets3[0].index.byte_start..facets3[0].index.byte_end],
            "https://en.wikipedia.org/wiki/Rust_(programming_language)"
        );
    }

    #[test]
    fn test_extract_link_facets_adversarial_stress() {
        // Test IPv4 and IPv6 bracketed addresses with ports
        let text_ip = "IPv4: http://127.0.0.1:8080/auth, IPv6: http://[::1]:9090/v1/auth?step=2";
        let facets_ip = extract_link_facets(text_ip);
        assert_eq!(facets_ip.len(), 2);
        assert_eq!(
            &text_ip[facets_ip[0].index.byte_start..facets_ip[0].index.byte_end],
            "http://127.0.0.1:8080/auth"
        );
        assert_eq!(
            &text_ip[facets_ip[1].index.byte_start..facets_ip[1].index.byte_end],
            "http://[::1]:9090/v1/auth?step=2"
        );

        // Test heavy multi-byte emojis (25-byte family emoji 👨‍👩‍👧‍👦, 8-byte flags 🇺🇸 🇬🇧) and RTL text
        let text_heavy_unicode =
            "Family 👨‍👩‍👧‍👦 flag 🇺🇸: https://example.com/family! Arabic مرحبا https://example.com/arabic?lang=ar#welcome Russian Привет: (https://example.com/russian).";
        let facets_heavy = extract_link_facets(text_heavy_unicode);
        assert_eq!(facets_heavy.len(), 3);
        assert_eq!(
            &text_heavy_unicode[facets_heavy[0].index.byte_start..facets_heavy[0].index.byte_end],
            "https://example.com/family"
        );
        assert_eq!(
            &text_heavy_unicode[facets_heavy[1].index.byte_start..facets_heavy[1].index.byte_end],
            "https://example.com/arabic?lang=ar#welcome"
        );
        assert_eq!(
            &text_heavy_unicode[facets_heavy[2].index.byte_start..facets_heavy[2].index.byte_end],
            "https://example.com/russian"
        );

        // Verify non-overlapping and strictly increasing offsets
        for i in 0..facets_heavy.len() {
            assert!(facets_heavy[i].index.byte_start < facets_heavy[i].index.byte_end);
            if i > 0 {
                assert!(facets_heavy[i - 1].index.byte_end <= facets_heavy[i].index.byte_start);
            }
        }

        // Nested and combined punctuation
        let text_punct =
            "Check ((https://example.com/nested?q=1&b=2))... or [https://example.com/bracket]!";
        let facets_punct = extract_link_facets(text_punct);
        assert_eq!(facets_punct.len(), 2);
        assert_eq!(
            &text_punct[facets_punct[0].index.byte_start..facets_punct[0].index.byte_end],
            "https://example.com/nested?q=1&b=2"
        );
        assert_eq!(
            &text_punct[facets_punct[1].index.byte_start..facets_punct[1].index.byte_end],
            "https://example.com/bracket"
        );
    }
}
