"""Character n-gram classifiers: TF-IDF features, a linear layer, softmax.

`train` fits one with scikit-learn. `NgramModel.export` writes the binary
format the `cufflink-model` crate reads (see docs/design-notes/models.md).
`load` reads a file back and `NgramModel.probabilities` scores exactly the
way the crate does (f32 weights, f64 arithmetic), so evaluate the exported
file, not the in-memory scikit-learn objects.
"""
from __future__ import annotations

import json
import math
import struct
from dataclasses import dataclass, field

import numpy as np

from .featurise import ngrams

MAGIC = b"CFLM"
FORMAT_VERSION = 1
KIND_NGRAM_LINEAR = 1


@dataclass
class NgramModel:
    labels: list[str]
    features: list[str]            # sorted by UTF-8 bytes, unique
    idf: np.ndarray                # f32, one per feature
    intercepts: np.ndarray         # f32, one per label
    weights: list[list[tuple[int, float]]]   # per feature: (label index, weight)
    ngram_min: int = 1
    ngram_max: int = 4
    sublinear_tf: bool = True
    meta: dict = field(default_factory=dict)
    _index: dict = field(default=None, repr=False)

    # ─── scoring (mirrors model/src/lib.rs) ──────────────────────────────
    def probabilities(self, text: str) -> list[float]:
        if self._index is None:
            self._index = {f: i for i, f in enumerate(self.features)}
        counts: dict[int, int] = {}
        for g in ngrams(text, self.ngram_min, self.ngram_max):
            i = self._index.get(g)
            if i is not None:
                counts[i] = counts.get(i, 0) + 1
        x = []
        for i in sorted(counts):
            c = counts[i]
            tf = 1.0 + math.log(c) if self.sublinear_tf else float(c)
            x.append((i, tf * float(self.idf[i])))
        norm = math.sqrt(sum(v * v for _, v in x))
        if norm > 0:
            x = [(i, v / norm) for i, v in x]
        scores = [float(b) for b in self.intercepts]
        for i, v in x:
            for c, w in self.weights[i]:
                scores[c] += v * float(np.float32(w))
        m = max(scores)
        exps = [math.exp(s - m) for s in scores]
        total = sum(exps)
        return [e / total for e in exps]

    def predict(self, text: str) -> tuple[str, float, list[tuple[str, float]]]:
        """(label, confidence, top five) — the same answer the crate gives."""
        p = self.probabilities(text)
        order = sorted(range(len(p)), key=lambda i: (-p[i], i))
        ranked = [(self.labels[i], p[i]) for i in order[:5]]
        return ranked[0][0], ranked[0][1], ranked

    # ─── file format ─────────────────────────────────────────────────────
    def export(self, path: str, *, name: str, version: str, threshold: float | None = None,
               info: dict | None = None) -> int:
        """Write the model file. Returns its size in bytes."""
        meta = {
            "name": name,
            "version": version,
            "labels": self.labels,
            "featuriser": {"kind": "char_wb", "version": 1, "ngram_min": self.ngram_min,
                           "ngram_max": self.ngram_max, "sublinear_tf": self.sublinear_tf, "norm": "l2"},
        }
        if threshold is not None:
            meta["threshold"] = threshold
        if info:
            meta["info"] = info
        meta_bytes = json.dumps(meta, ensure_ascii=False, separators=(",", ":")).encode()
        encoded = [f.encode() for f in self.features]
        if any(a >= b for a, b in zip(encoded, encoded[1:])):
            raise ValueError("features must be sorted by UTF-8 bytes and unique")
        if len(self.labels) > 0xFFFF:
            raise ValueError("too many labels")

        out = bytearray(MAGIC)
        out += struct.pack("<HHI", FORMAT_VERSION, KIND_NGRAM_LINEAR, len(meta_bytes)) + meta_bytes
        out += struct.pack("<II", len(self.features), len(self.labels))
        out += np.asarray(self.intercepts, dtype="<f4").tobytes()
        out += np.asarray(self.idf, dtype="<f4").tobytes()
        offs, pos = [0], 0
        for e in encoded:
            pos += len(e)
            offs.append(pos)
        out += struct.pack(f"<{len(offs)}I", *offs) + b"".join(encoded)
        woffs, pos, entries = [0], 0, bytearray()
        for row in self.weights:
            for c, w in sorted(row):
                entries += struct.pack("<Hf", c, w)
            pos += len(row)
            woffs.append(pos)
        out += struct.pack(f"<{len(woffs)}I", *woffs) + entries
        with open(path, "wb") as f:
            f.write(out)
        return len(out)


def load(path: str) -> NgramModel:
    """Read a model file written by `export` (or anything else following the format)."""
    data = open(path, "rb").read()
    if data[:4] != MAGIC:
        raise ValueError("not a cufflink model file")
    version, kind, meta_len = struct.unpack_from("<HHI", data, 4)
    if version != FORMAT_VERSION or kind != KIND_NGRAM_LINEAR:
        raise ValueError(f"unsupported format {version} / kind {kind}")
    pos = 12
    meta = json.loads(data[pos : pos + meta_len]); pos += meta_len
    nf, nc = struct.unpack_from("<II", data, pos); pos += 8
    intercepts = np.frombuffer(data, "<f4", nc, pos); pos += 4 * nc
    idf = np.frombuffer(data, "<f4", nf, pos); pos += 4 * nf
    offs = struct.unpack_from(f"<{nf + 1}I", data, pos); pos += 4 * (nf + 1)
    blob = data[pos : pos + offs[-1]]; pos += offs[-1]
    features = [blob[offs[i] : offs[i + 1]].decode() for i in range(nf)]
    woffs = struct.unpack_from(f"<{nf + 1}I", data, pos); pos += 4 * (nf + 1)
    weights = []
    for i in range(nf):
        row = [struct.unpack_from("<Hf", data, pos + 6 * e) for e in range(woffs[i], woffs[i + 1])]
        weights.append([(c, w) for c, w in row])
    pos += 6 * woffs[-1]
    if pos != len(data):
        raise ValueError("trailing bytes")
    feat = meta["featuriser"]
    return NgramModel(labels=meta["labels"], features=features, idf=idf, intercepts=intercepts,
                      weights=weights, ngram_min=feat["ngram_min"], ngram_max=feat["ngram_max"],
                      sublinear_tf=feat["sublinear_tf"], meta=meta)


def train(texts: list[str], labels: list[str], *, ngram_range: tuple[int, int] = (1, 4), min_df: int = 2,
          C: float = 8.0, keep_fraction: float = 0.05, max_iter: int = 300) -> NgramModel:
    """Fit TF-IDF + multinomial logistic regression, then keep only the strongest
    `keep_fraction` of weights. Every feature keeps its idf even when all its
    weights are pruned, so it still counts toward the L2 norm as it did in training."""
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression

    lo, hi = ngram_range
    vec = TfidfVectorizer(analyzer=lambda s: ngrams(s, lo, hi), sublinear_tf=True, norm="l2", min_df=min_df)
    X = vec.fit_transform(texts)
    lr = LogisticRegression(C=C, max_iter=max_iter)
    lr.fit(X, labels)
    classes = [str(c) for c in lr.classes_]
    coef = lr.coef_
    if coef.shape[0] == 1:  # binary: scikit-learn keeps one row; make it two-class softmax
        coef = np.vstack([-coef[0] / 2, coef[0] / 2])
        intercepts = np.array([-lr.intercept_[0] / 2, lr.intercept_[0] / 2])
    else:
        intercepts = lr.intercept_
    cut = np.quantile(np.abs(coef), 1 - keep_fraction) if keep_fraction < 1 else 0.0

    vocab = vec.vocabulary_
    features = sorted(vocab, key=lambda f: f.encode())
    weights = []
    for f in features:
        col = coef[:, vocab[f]]
        weights.append([(c, float(w)) for c, w in enumerate(col) if w != 0 and abs(w) >= cut])
    idf = np.array([vec.idf_[vocab[f]] for f in features], dtype=np.float32)
    return NgramModel(labels=classes, features=features, idf=idf,
                      intercepts=np.asarray(intercepts, dtype=np.float32), weights=weights,
                      ngram_min=lo, ngram_max=hi)


def parity_fixture(model: NgramModel, texts: list[str]) -> list[dict]:
    """Expected answers for the crate's parity test: each text with every probability."""
    return [{"text": t, "probabilities": model.probabilities(t)} for t in texts]
