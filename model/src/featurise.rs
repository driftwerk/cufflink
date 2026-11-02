//! Turning text into the n-gram features a model scores.
//!
//! This is the `char_wb` featuriser, version 1. The Python toolkit in `ml/`
//! implements the same steps, and the parity tests hold the two together:
//!
//! 1. Lowercase the whole text with Unicode's full lowercase mapping
//!    (`str::to_lowercase`, which handles a word-final capital sigma the way
//!    Python's `str.lower` does).
//! 2. Split it into words on [`is_space`] characters, dropping empty words.
//! 3. Pad each word with one space on each side.
//! 4. For every `n` from `ngram_min` to `ngram_max`, take every window of
//!    `n` consecutive *characters* (Unicode scalar values, not bytes) of the
//!    padded word that fits.
//!
//! The same n-gram can appear more than once; callers count occurrences.

/// The whitespace characters words are split on: Unicode `White_Space`.
/// Spelled out rather than taken from either language's standard library,
/// which disagree on a few control characters.
pub fn is_space(c: char) -> bool {
    matches!(
        c,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{0085}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
    )
}

/// Every n-gram of `text`, in order, with repeats.
pub fn ngrams(text: &str, ngram_min: usize, ngram_max: usize) -> Vec<String> {
    let lower = text.to_lowercase();
    let mut out = Vec::new();
    for word in lower.split(is_space).filter(|w| !w.is_empty()) {
        let padded: Vec<char> = std::iter::once(' ')
            .chain(word.chars())
            .chain(std::iter::once(' '))
            .collect();
        for n in ngram_min.max(1)..=ngram_max {
            if n > padded.len() {
                break;
            }
            for window in padded.windows(n) {
                out.push(window.iter().collect());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_are_padded_and_windowed() {
        assert_eq!(
            ngrams("Ab", 1, 2),
            vec![" ", "a", "b", " ", " a", "ab", "b "]
        );
    }

    #[test]
    fn windows_count_characters_not_bytes() {
        // "é" is two bytes but one character.
        assert_eq!(ngrams("é", 3, 3), vec![" é "]);
    }

    #[test]
    fn short_words_stop_at_their_length() {
        assert_eq!(ngrams("a", 1, 4), vec![" ", "a", " ", " a", "a ", " a "]);
    }

    #[test]
    fn splits_on_unicode_white_space_only() {
        assert_eq!(ngrams("a\u{3000}b", 2, 2), vec![" a", "a ", " b", "b "]);
        // U+001C is a separator to Python's str.split() but not White_Space.
        assert_eq!(ngrams("a\u{1c}b", 4, 4), vec![" a\u{1c}b", "a\u{1c}b "]);
    }

    #[test]
    fn empty_and_blank_text_have_no_ngrams() {
        assert!(ngrams("", 1, 4).is_empty());
        assert!(ngrams(" \t\n", 1, 4).is_empty());
    }

    #[test]
    fn final_sigma_lowercases_like_python() {
        assert_eq!(ngrams("ΟΔΟΣ", 1, 1).concat(), " οδο\u{3c2} ");
    }
}
