# Text classifiers

`cufflink-model` runs small trained text classifiers inside a service: the
model is a file compiled into the service, and classifying a short text takes
well under a millisecond in WASM, with no calls out.

It suits narrow labelling jobs over short text: mapping free-text input onto a
fixed list, routing a message, tagging a title. It isn't a language model;
it can't write text or answer questions.

## Train and export (Python)

The toolkit lives in [`ml/`](../ml). Training happens offline:

```python
from cufflink_ml import ngram

model = ngram.train(texts, labels)                 # TF-IDF char n-grams + logistic regression
model.export("genre.cflm", name="genre", version="2026-01-01", threshold=0.8)

shipped = ngram.load("genre.cflm")                 # scores exactly as the crate does
label, confidence, top5 = shipped.predict("The Dragon of Ember Hill")
```

Evaluate `shipped`, not `model`: the file stores weights as `f32` and keeps
only the strongest `keep_fraction` of them (5% by default), and the loaded
model gives the answers a service will give.

## Use it in a service

```toml
[dependencies]
cufflink-model = "0.1"
```

```rust
use cufflink_model::Model;

static GENRE: &[u8] = include_bytes!("../models/genre.cflm");

fn genre_of(title: &str) -> Option<String> {
    let model = Model::from_bytes(GENRE).ok()?;
    let p = model.predict(title);
    (p.confidence >= model.threshold().unwrap_or(0.9)).then_some(p.label)
}
```

`Model::from_bytes` reads the file in place, so loading it on every call (as a
WASM guest does) costs little. It returns an error for a malformed file rather
than panicking; a service should treat that as "no answer".

### Confidence

Act on `confidence` rather than the label alone. Pick the threshold from an
evaluation on real inputs the model never saw in training, and send anything
below it down the path you'd take with no model at all.

### Shadow runs

Before a model decides anything, run it alongside the existing logic and log
both answers, e.g. with `model.id()`, the label and the confidence. Switch over
once its answers agree on real traffic.

## Parity

`model/tests/parity.rs` checks the crate against the Python reader on an
invented fixture, including empty, non-ASCII and unusual-whitespace input.
Regenerate it with:

```sh
uv run --project ml python ml/examples/genre/build.py
```

The file format, and the reserved transformer and ensemble kinds, are
described in [the design note](design-notes/models.md).
