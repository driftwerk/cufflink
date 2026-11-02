"""Train and export text classifiers that the `cufflink-model` crate runs.

    from cufflink_ml import ngram

    model = ngram.train(texts, labels)
    model.export("genre.cflm", name="genre", version="2026-01-01", threshold=0.8)
    reloaded = ngram.load("genre.cflm")      # scores exactly as the Rust crate does
    reloaded.predict("The Dragon of Ember Hill")
"""
from . import featurise, ngram  # noqa: F401
