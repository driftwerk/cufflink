"""The `char_wb` featuriser, version 1. Mirrors `model/src/featurise.rs`; see its docs."""

# Unicode White_Space, spelled out: str.split() also splits on U+001C..U+001F.
_SPACE = set("\t\n\x0b\x0c\r \x85\xa0     　") | {
    chr(c) for c in range(0x2000, 0x200B)
}


def is_space(c: str) -> bool:
    return c in _SPACE


def words(text: str) -> list[str]:
    out, cur = [], []
    for c in text:
        if c in _SPACE:
            if cur:
                out.append("".join(cur))
                cur = []
        else:
            cur.append(c)
    if cur:
        out.append("".join(cur))
    return out


def ngrams(text: str, ngram_min: int = 1, ngram_max: int = 4) -> list[str]:
    """Every n-gram of `text`, in order, with repeats."""
    out = []
    for word in words(text.lower()):
        padded = " " + word + " "
        for n in range(max(ngram_min, 1), ngram_max + 1):
            if n > len(padded):
                break
            out.extend(padded[i : i + n] for i in range(len(padded) - n + 1))
    return out
