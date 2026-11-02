import random

from cufflink_ml import featurise, ngram


def test_featuriser_matches_the_rust_spec():
    assert featurise.ngrams("Ab", 1, 2) == [" ", "a", "b", " ", " a", "ab", "b "]
    assert featurise.ngrams("é", 3, 3) == [" é "]
    assert featurise.ngrams("a　b", 2, 2) == [" a", "a ", " b", "b "]
    assert featurise.ngrams("a\u001cb", 4, 4) == [" a\u001cb", "a\u001cb "]
    assert featurise.ngrams("ΟΔΟΣ", 1, 1)[-2] == "ς"
    assert featurise.ngrams(" \t\n") == []


def test_export_and_load_round_trip(tmp_path):
    random.seed(1)
    texts = [f"red {random.random():.3f}" for _ in range(40)] + [f"blue {random.random():.3f}" for _ in range(40)]
    labels = ["red"] * 40 + ["blue"] * 40
    model = ngram.train(texts, labels, min_df=1, keep_fraction=1.0)
    path = tmp_path / "m.cflm"
    model.export(str(path), name="colour", version="t", threshold=0.7)
    loaded = ngram.load(str(path))
    assert loaded.labels == model.labels
    assert loaded.meta["threshold"] == 0.7
    assert loaded.predict("red thing")[0] == "red"
    assert loaded.predict("blue thing")[0] == "blue"
    for t in ["red 1", "blue 2", "", "purple"]:
        a, b = model.probabilities(t), loaded.probabilities(t)
        assert max(abs(x - y) for x, y in zip(a, b)) < 1e-9


def test_pruning_keeps_every_feature():
    texts = ["alpha one", "alpha two", "beta one", "beta two"] * 5
    labels = ["a", "a", "b", "b"] * 5
    full = ngram.train(texts, labels, min_df=1, keep_fraction=1.0)
    pruned = ngram.train(texts, labels, min_df=1, keep_fraction=0.1)
    assert pruned.features == full.features
    assert sum(map(len, pruned.weights)) < sum(map(len, full.weights))
