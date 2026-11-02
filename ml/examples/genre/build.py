"""Build the invented genre classifier used by the cufflink-model tests.

Titles are generated from word lists, so the data is entirely invented.
Writes model/tests/fixtures/genre.cflm and genre_parity.json.

    uv run --project ml python ml/examples/genre/build.py
"""
import json
import pathlib
import random

from cufflink_ml import ngram

random.seed(7)
WORDS = {
    "fantasy": (["Dragon", "Sword", "Wizard", "Crown", "Elven", "Shadow", "Rune", "Throne"],
                ["of Ember Hill", "Reborn", "and the Last Gate", "Rising", "of the North Wind"]),
    "mystery": (["Murder", "Secret", "Missing", "Silent", "Poisoned", "Locked", "Vanished", "Hidden"],
                ["at the Rectory", "Witness", "in Room Nine", "Clue", "on the Night Train"]),
    "cookery": (["Bread", "Soup", "Pasta", "Baking", "Spice", "Garden Kitchen", "One-Pot", "Sourdough"],
                ["for Beginners", "Every Day", "Made Simple", "Recipes", "in Thirty Minutes"]),
    "travel": (["Island", "Mountain", "Coastal", "Desert", "River", "Railway", "City", "Backroads"],
               ["Journeys", "Guide", "on Foot", "Wanderings", "Atlas"]),
    "history": (["Empire", "Medieval", "Ancient", "Revolution", "Kings", "Industrial", "Victorian", "War"],
                ["A History", "and Its Fall", "Through the Ages", "Chronicles", "1750-1900"]),
}


def title(genre: str) -> str:
    a, b = WORDS[genre]
    t = f"{random.choice(['The ', '', 'A '])}{random.choice(a)} {random.choice(b)}".strip()
    return t.lower() if random.random() < 0.2 else t


texts, labels = [], []
for g in WORDS:
    for _ in range(80):
        texts.append(title(g)); labels.append(g)

root = pathlib.Path(__file__).resolve().parents[3]
out = root / "model/tests/fixtures"
out.mkdir(parents=True, exist_ok=True)
model = ngram.train(texts, labels, min_df=1, keep_fraction=0.3)
size = model.export(str(out / "genre.cflm"), name="genre", version="1", threshold=0.6,
                    info={"note": "invented titles; test fixture only"})
loaded = ngram.load(str(out / "genre.cflm"))

PROBES = [
    "The Dragon Reborn", "murder at the rectory", "Sourdough Every Day", "Coastal Journeys",
    "Empire and Its Fall", "Dragon Recipes", "",  "   \t", "❤", "ΟΔΟΣ", "İstanbul Kitchen",
    "café　guide", "a\u001cb", "THE SILENT WITNESS!!!", "x" * 200, "1750-1900",
]
json.dump(ngram.parity_fixture(loaded, PROBES), open(out / "genre_parity.json", "w"),
          ensure_ascii=False, indent=1)
print(f"exported {size} bytes, {len(loaded.features)} features, labels {loaded.labels}")
for p in PROBES[:6]:
    print(f"  {p!r} -> {loaded.predict(p)[:2]}")
