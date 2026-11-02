//! # cufflink-model
//!
//! Run small trained text classifiers inside Cufflink services. Training
//! happens offline (the Python toolkit in `ml/`); a service embeds the
//! exported file and classifies with it, natively or in WASM, without
//! calling out to anything.
//!
//! ```rust,ignore
//! use cufflink_model::Model;
//!
//! static GENRES: &[u8] = include_bytes!("../models/genre.cflm");
//!
//! let model = Model::from_bytes(GENRES)?;
//! let p = model.predict("The Dragon of Ember Hill");
//! if p.confidence >= model.threshold().unwrap_or(0.9) {
//!     // use p.label
//! }
//! ```
//!
//! A WASM guest is a fresh instance on every call, so [`Model::from_bytes`]
//! reads the file in place: it checks the layout once and keeps borrowed
//! slices, with no tables rebuilt. Every offset is bounds-checked at load,
//! and nothing in this crate panics on a malformed file or unusual input.
//!
//! The file format is described in `docs/design-notes/models.md`.

pub mod featurise;

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

use serde_json::Value;

/// The first four bytes of every model file.
pub const MAGIC: &[u8; 4] = b"CFLM";
/// The file layout this crate reads.
pub const FORMAT_VERSION: u16 = 1;

/// The kind of model a file holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Character n-gram TF-IDF features scored by a linear layer and softmax.
    NgramLinear,
}

impl Kind {
    fn from_code(code: u16) -> Result<Self, ModelError> {
        match code {
            1 => Ok(Kind::NgramLinear),
            // 2 = transformer and 3 = ensemble are reserved; see the design note.
            other => Err(ModelError::UnsupportedKind(other)),
        }
    }
}

/// Why a model file couldn't be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelError {
    /// The file doesn't start with [`MAGIC`].
    NotAModel,
    /// The file uses a layout version this crate doesn't read.
    UnsupportedVersion(u16),
    /// The file holds a kind of model this crate doesn't run.
    UnsupportedKind(u16),
    /// The file is cut short or an offset points outside it.
    Truncated(&'static str),
    /// The file's contents contradict each other.
    Invalid(String),
}

impl fmt::Display for ModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ModelError::NotAModel => write!(f, "not a cufflink model file"),
            ModelError::UnsupportedVersion(v) => write!(f, "unsupported model format version {v}"),
            ModelError::UnsupportedKind(k) => write!(f, "unsupported model kind {k}"),
            ModelError::Truncated(what) => write!(f, "model file truncated at {what}"),
            ModelError::Invalid(why) => write!(f, "invalid model file: {why}"),
        }
    }
}

impl std::error::Error for ModelError {}

/// A model's answer for one text.
#[derive(Debug, Clone, PartialEq)]
pub struct Prediction {
    /// The most likely label.
    pub label: String,
    /// Its probability, between 0 and 1.
    pub confidence: f64,
    /// The most likely labels, best first, with their probabilities (up to five).
    pub ranked: Vec<(String, f64)>,
}

/// A loaded model, borrowing the file it was read from.
#[derive(Debug, Clone)]
pub struct Model<'a> {
    kind: Kind,
    meta: Value,
    labels: Vec<String>,
    ngram_min: usize,
    ngram_max: usize,
    sublinear_tf: bool,
    l2_norm: bool,
    linear: Linear<'a>,
}

#[derive(Debug, Clone)]
struct Linear<'a> {
    n_features: usize,
    n_classes: usize,
    intercepts: &'a [u8],
    idf: &'a [u8],
    str_offsets: &'a [u8],
    strings: &'a [u8],
    w_offsets: &'a [u8],
    entries: &'a [u8],
}

const ENTRY: usize = 6; // u16 class + f32 weight

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize, what: &'static str) -> Result<&'a [u8], ModelError> {
        let end = self.pos.checked_add(n).ok_or(ModelError::Truncated(what))?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(ModelError::Truncated(what))?;
        self.pos = end;
        Ok(slice)
    }
    fn u16(&mut self, what: &'static str) -> Result<u16, ModelError> {
        let b = self.take(2, what)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }
    fn u32(&mut self, what: &'static str) -> Result<u32, ModelError> {
        let b = self.take(4, what)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn array(
        &mut self,
        count: usize,
        width: usize,
        what: &'static str,
    ) -> Result<&'a [u8], ModelError> {
        let n = count
            .checked_mul(width)
            .ok_or(ModelError::Truncated(what))?;
        self.take(n, what)
    }
}

fn u32_at(slice: &[u8], i: usize) -> Option<u32> {
    let b = slice.get(i * 4..i * 4 + 4)?;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn f32_at(slice: &[u8], i: usize) -> Option<f32> {
    let b = slice.get(i * 4..i * 4 + 4)?;
    Some(f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

/// Offsets must start at 0, never decrease, and end exactly at `len`.
fn check_offsets(offsets: &[u8], count: usize, len: usize, what: &str) -> Result<(), ModelError> {
    let mut prev = 0u32;
    for i in 0..=count {
        let o = u32_at(offsets, i)
            .ok_or_else(|| ModelError::Invalid(format!("{what} offsets short")))?;
        if (i == 0 && o != 0) || o < prev {
            return Err(ModelError::Invalid(format!("{what} offsets out of order")));
        }
        prev = o;
    }
    if prev as usize != len {
        return Err(ModelError::Invalid(format!(
            "{what} offsets don't cover the data"
        )));
    }
    Ok(())
}

impl<'a> Model<'a> {
    /// Read a model file. Checks the whole layout; the returned model borrows `bytes`.
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self, ModelError> {
        let mut r = Reader { bytes, pos: 0 };
        if r.take(4, "magic").map_err(|_| ModelError::NotAModel)? != MAGIC {
            return Err(ModelError::NotAModel);
        }
        let version = r.u16("version")?;
        if version != FORMAT_VERSION {
            return Err(ModelError::UnsupportedVersion(version));
        }
        let kind = Kind::from_code(r.u16("kind")?)?;
        let meta_len = r.u32("metadata length")? as usize;
        let meta: Value = serde_json::from_slice(r.take(meta_len, "metadata")?)
            .map_err(|e| ModelError::Invalid(format!("metadata: {e}")))?;

        let labels: Vec<String> = meta["labels"]
            .as_array()
            .ok_or_else(|| ModelError::Invalid("metadata has no labels".into()))?
            .iter()
            .map(|l| l.as_str().map(String::from))
            .collect::<Option<_>>()
            .ok_or_else(|| ModelError::Invalid("labels must be strings".into()))?;
        let feat = &meta["featuriser"];
        if feat["kind"].as_str() != Some("char_wb") || feat["version"].as_u64() != Some(1) {
            return Err(ModelError::Invalid(
                "featuriser must be char_wb version 1".into(),
            ));
        }
        let ngram_min = feat["ngram_min"].as_u64().unwrap_or(1) as usize;
        let ngram_max = feat["ngram_max"].as_u64().unwrap_or(4) as usize;
        if ngram_min == 0 || ngram_min > ngram_max || ngram_max > 16 {
            return Err(ModelError::Invalid("bad n-gram range".into()));
        }
        let sublinear_tf = feat["sublinear_tf"].as_bool().unwrap_or(true);
        let l2_norm = match feat["norm"].as_str() {
            Some("l2") | None => true,
            Some("none") => false,
            Some(other) => return Err(ModelError::Invalid(format!("unknown norm {other}"))),
        };

        let n_features = r.u32("feature count")? as usize;
        let n_classes = r.u32("class count")? as usize;
        if n_classes != labels.len() || n_classes == 0 || n_classes > u16::MAX as usize {
            return Err(ModelError::Invalid(
                "class count doesn't match labels".into(),
            ));
        }
        let intercepts = r.array(n_classes, 4, "intercepts")?;
        let idf = r.array(n_features, 4, "idf")?;
        let str_offsets = r.array(n_features + 1, 4, "feature offsets")?;
        let str_len = u32_at(str_offsets, n_features)
            .ok_or(ModelError::Truncated("feature offsets"))? as usize;
        let strings = r.take(str_len, "feature strings")?;
        let w_offsets = r.array(n_features + 1, 4, "weight offsets")?;
        let n_entries =
            u32_at(w_offsets, n_features).ok_or(ModelError::Truncated("weight offsets"))? as usize;
        let entries = r.array(n_entries, ENTRY, "weights")?;
        if r.pos != bytes.len() {
            return Err(ModelError::Invalid("trailing bytes after the model".into()));
        }

        check_offsets(str_offsets, n_features, strings.len(), "feature")?;
        check_offsets(w_offsets, n_features, n_entries, "weight")?;
        if std::str::from_utf8(strings).is_err() {
            return Err(ModelError::Invalid("feature strings aren't UTF-8".into()));
        }
        let linear = Linear {
            n_features,
            n_classes,
            intercepts,
            idf,
            str_offsets,
            strings,
            w_offsets,
            entries,
        };
        // Features must be strictly increasing by bytes for the binary search.
        for i in 1..n_features {
            if linear.feature(i - 1) >= linear.feature(i) {
                return Err(ModelError::Invalid("features not sorted and unique".into()));
            }
        }
        for e in 0..n_entries {
            let class = u16::from_le_bytes([entries[e * ENTRY], entries[e * ENTRY + 1]]) as usize;
            if class >= n_classes {
                return Err(ModelError::Invalid(
                    "weight for a class that doesn't exist".into(),
                ));
            }
        }

        Ok(Model {
            kind,
            meta,
            labels,
            ngram_min,
            ngram_max,
            sublinear_tf,
            l2_norm,
            linear,
        })
    }

    /// The kind of model.
    pub fn kind(&self) -> Kind {
        self.kind
    }

    /// The labels the model chooses between, in output order.
    pub fn labels(&self) -> &[String] {
        &self.labels
    }

    /// Everything the trainer recorded about the model (name, version, threshold, ...).
    pub fn meta(&self) -> &Value {
        &self.meta
    }

    /// The model's own `name` and `version`, as `name@version`, for logs.
    pub fn id(&self) -> String {
        format!(
            "{}@{}",
            self.meta["name"].as_str().unwrap_or("model"),
            self.meta["version"].as_str().unwrap_or("?")
        )
    }

    /// The confidence the trainer suggests acting on, if it recorded one.
    pub fn threshold(&self) -> Option<f64> {
        self.meta["threshold"].as_f64()
    }

    /// The probability of every label for `text`, in [`Model::labels`] order.
    pub fn probabilities(&self, text: &str) -> Vec<f64> {
        let lin = &self.linear;
        let mut counts: BTreeMap<usize, u32> = BTreeMap::new();
        for gram in featurise::ngrams(text, self.ngram_min, self.ngram_max) {
            if let Some(i) = lin.find(gram.as_bytes()) {
                *counts.entry(i).or_insert(0) += 1;
            }
        }
        let mut x: Vec<(usize, f64)> = counts
            .into_iter()
            .map(|(i, c)| {
                let tf = if self.sublinear_tf {
                    1.0 + (c as f64).ln()
                } else {
                    c as f64
                };
                (i, tf * f32_at(lin.idf, i).unwrap_or(0.0) as f64)
            })
            .collect();
        if self.l2_norm {
            let norm = x.iter().map(|(_, v)| v * v).sum::<f64>().sqrt();
            if norm > 0.0 {
                for (_, v) in &mut x {
                    *v /= norm;
                }
            }
        }
        let mut scores: Vec<f64> = (0..lin.n_classes)
            .map(|c| f32_at(lin.intercepts, c).unwrap_or(0.0) as f64)
            .collect();
        for (i, v) in x {
            let (start, end) = (
                u32_at(lin.w_offsets, i).unwrap_or(0) as usize,
                u32_at(lin.w_offsets, i + 1).unwrap_or(0) as usize,
            );
            for e in start..end {
                let Some(b) = lin.entries.get(e * ENTRY..e * ENTRY + ENTRY) else {
                    break;
                };
                let class = u16::from_le_bytes([b[0], b[1]]) as usize;
                let w = f32::from_le_bytes([b[2], b[3], b[4], b[5]]) as f64;
                if let Some(s) = scores.get_mut(class) {
                    *s += v * w;
                }
            }
        }
        softmax(&mut scores);
        scores
    }

    /// The model's answer for `text`.
    pub fn predict(&self, text: &str) -> Prediction {
        let probs = self.probabilities(text);
        let mut order: Vec<usize> = (0..probs.len()).collect();
        order.sort_by(|&a, &b| {
            probs[b]
                .partial_cmp(&probs[a])
                .unwrap_or(Ordering::Equal)
                .then(a.cmp(&b))
        });
        let ranked: Vec<(String, f64)> = order
            .iter()
            .take(5)
            .map(|&i| (self.labels[i].clone(), probs[i]))
            .collect();
        let (label, confidence) = ranked.first().cloned().unwrap_or_default();
        Prediction {
            label,
            confidence,
            ranked,
        }
    }
}

impl Linear<'_> {
    fn feature(&self, i: usize) -> &[u8] {
        let start = u32_at(self.str_offsets, i).unwrap_or(0) as usize;
        let end = u32_at(self.str_offsets, i + 1).unwrap_or(0) as usize;
        self.strings.get(start..end).unwrap_or(&[])
    }

    fn find(&self, gram: &[u8]) -> Option<usize> {
        let (mut lo, mut hi) = (0usize, self.n_features);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.feature(mid).cmp(gram) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Some(mid),
            }
        }
        None
    }
}

fn softmax(scores: &mut [f64]) {
    let max = scores.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let mut sum = 0.0;
    for s in scores.iter_mut() {
        *s = (*s - max).exp();
        sum += *s;
    }
    if sum > 0.0 {
        for s in scores.iter_mut() {
            *s /= sum;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A hand-built two-label model: "x" votes for `b`, nothing else is known.
    fn tiny() -> Vec<u8> {
        let meta = serde_json::json!({
            "name": "tiny", "version": "1", "labels": ["a", "b"], "threshold": 0.6,
            "featuriser": {"kind": "char_wb", "version": 1, "ngram_min": 1, "ngram_max": 1,
                           "sublinear_tf": true, "norm": "l2"}
        })
        .to_string();
        let mut f = Vec::new();
        f.extend_from_slice(MAGIC);
        f.extend_from_slice(&1u16.to_le_bytes());
        f.extend_from_slice(&1u16.to_le_bytes());
        f.extend_from_slice(&(meta.len() as u32).to_le_bytes());
        f.extend_from_slice(meta.as_bytes());
        f.extend_from_slice(&1u32.to_le_bytes()); // features
        f.extend_from_slice(&2u32.to_le_bytes()); // classes
        for b in [0.5f32, 0.0] {
            f.extend_from_slice(&b.to_le_bytes());
        }
        f.extend_from_slice(&1.0f32.to_le_bytes()); // idf of "x"
        for o in [0u32, 1] {
            f.extend_from_slice(&o.to_le_bytes());
        }
        f.extend_from_slice(b"x");
        for o in [0u32, 1] {
            f.extend_from_slice(&o.to_le_bytes());
        }
        f.extend_from_slice(&1u16.to_le_bytes());
        f.extend_from_slice(&3.0f32.to_le_bytes());
        f
    }

    #[test]
    fn known_features_move_the_answer() {
        let bytes = tiny();
        let m = Model::from_bytes(&bytes).unwrap();
        assert_eq!(m.id(), "tiny@1");
        assert_eq!(m.threshold(), Some(0.6));
        assert_eq!(m.predict("x").label, "b");
        // Unknown text falls back to the intercepts, which favour "a".
        let p = m.predict("zzz");
        assert_eq!(p.label, "a");
        let expected = 1.0 / (1.0 + (-0.5f64).exp());
        assert!((p.confidence - expected).abs() < 1e-9);
        assert_eq!(p.ranked.len(), 2);
    }

    #[test]
    fn empty_text_is_answered_not_panicked() {
        let bytes = tiny();
        let m = Model::from_bytes(&bytes).unwrap();
        assert_eq!(m.predict("").label, "a");
        assert_eq!(m.predict("   ").label, "a");
    }

    #[test]
    fn every_truncation_is_an_error_not_a_panic() {
        let bytes = tiny();
        for len in 0..bytes.len() {
            assert!(
                Model::from_bytes(&bytes[..len]).is_err(),
                "accepted {len} bytes"
            );
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(Model::from_bytes(&longer).is_err());
    }

    #[test]
    fn corrupt_files_are_rejected() {
        let bytes = tiny();
        assert_eq!(
            Model::from_bytes(b"nope").unwrap_err(),
            ModelError::NotAModel
        );

        let mut v2 = bytes.clone();
        v2[4] = 2;
        assert_eq!(
            Model::from_bytes(&v2).unwrap_err(),
            ModelError::UnsupportedVersion(2)
        );

        let mut transformer = bytes.clone();
        transformer[6] = 2;
        assert_eq!(
            Model::from_bytes(&transformer).unwrap_err(),
            ModelError::UnsupportedKind(2)
        );

        let mut bad_class = bytes.clone();
        let at = bad_class.len() - 6;
        bad_class[at] = 9;
        assert!(matches!(
            Model::from_bytes(&bad_class),
            Err(ModelError::Invalid(_))
        ));
    }
}
