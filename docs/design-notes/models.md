# Model files

Status: `ngram_linear` implemented; `transformer` and `ensemble` reserved.

## Goals

- A service embeds a trained model and classifies short text with no network
  call, natively or in WASM.
- A WASM guest is instantiated fresh for every invocation, so a model is read
  in place: one pass of checks, then borrowed slices. No tables are rebuilt.
- The bytes evaluated offline are the bytes that run. The Python toolkit reads
  and scores the file with the same arithmetic as the crate, and a parity
  test holds them together.
- A malformed file is an error, never a panic.

## Layout (format version 1)

All integers and floats are little-endian.

| field | type |
|---|---|
| magic | `"CFLM"` |
| format version | `u16` = 1 |
| kind | `u16`: 1 `ngram_linear`, 2 `transformer` (reserved), 3 `ensemble` (reserved) |
| metadata length | `u32` |
| metadata | UTF-8 JSON |
| payload | depends on kind |

Metadata holds `labels` (required, output order), `featuriser`, and
optionally `name`, `version`, `threshold` and a free-form `info` object.

### `ngram_linear`

| field | type |
|---|---|
| feature count `F`, label count `C` | `u32`, `u32` |
| intercepts | `f32 × C` |
| idf | `f32 × F` |
| feature offsets | `u32 × (F+1)`, starting at 0 |
| feature strings | UTF-8, strictly increasing by bytes |
| weight offsets | `u32 × (F+1)`, starting at 0 |
| weights | `(u16 label, f32 weight) × last offset` |

Scoring: count each feature's n-grams (featuriser `char_wb` v1, see
`model/src/featurise.rs`), weight by `1 + ln(count)` times idf, L2-normalise,
add each feature's weights to its labels' scores on top of the intercepts,
and softmax. Arithmetic is `f64` over the stored `f32` values.

Features whose weights were all pruned stay in the table with their idf, so
the normalisation matches training.

## Reserved kinds

- **`transformer`**: a small character- or subword-level encoder (embeddings,
  attention blocks, pooled classification head), weights in `f32` or `f16`
  tensors described by a table in the payload. Larger models are expected to
  run on the host rather than in the guest; the file format stays the same.
- **`ensemble`**: member models (each a complete file of any kind) sharing
  one label list, combined by weighted averaging of their probabilities.

Both will get their own format sections when implemented. A reader that
meets an unknown kind returns `UnsupportedKind`.
