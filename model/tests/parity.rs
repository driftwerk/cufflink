//! The crate must score exactly as the Python toolkit's reader does, so that
//! what was evaluated offline is what runs in a service.
//! Regenerate the fixtures with `uv run --project ml python ml/examples/genre/build.py`.

use cufflink_model::{Kind, Model};
use serde::Deserialize;

static GENRE: &[u8] = include_bytes!("fixtures/genre.cflm");

#[derive(Deserialize)]
struct Case {
    text: String,
    probabilities: Vec<f64>,
}

#[test]
fn matches_the_python_reader() {
    let model = Model::from_bytes(GENRE).expect("fixture loads");
    assert_eq!(model.kind(), Kind::NgramLinear);
    assert_eq!(model.id(), "genre@1");
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("fixtures/genre_parity.json")).expect("fixture parses");
    assert!(cases.len() >= 10);
    for case in cases {
        let got = model.probabilities(&case.text);
        assert_eq!(got.len(), case.probabilities.len(), "{:?}", case.text);
        for (g, want) in got.iter().zip(&case.probabilities) {
            assert!(
                (g - want).abs() < 1e-9,
                "{:?}: {got:?} vs {:?}",
                case.text,
                case.probabilities
            );
        }
        let best = (0..got.len())
            .max_by(|&a, &b| {
                case.probabilities[a]
                    .total_cmp(&case.probabilities[b])
                    .then(b.cmp(&a))
            })
            .unwrap();
        assert_eq!(
            model.predict(&case.text).label,
            model.labels()[best],
            "{:?}",
            case.text
        );
    }
}

#[test]
fn reads_the_fixture_in_place_quickly() {
    // A WASM guest reloads the model on every call, so loading must stay cheap.
    let start = std::time::Instant::now();
    for _ in 0..100 {
        Model::from_bytes(GENRE).unwrap();
    }
    assert!(
        start.elapsed().as_millis() < 500,
        "100 loads took {:?}",
        start.elapsed()
    );
}
