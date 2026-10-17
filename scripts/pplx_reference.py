#!/usr/bin/env python3
"""Writes fixtures/pplx_embedding_reference.json: what pplx-embed-context-v1-0.6b
produces for three documents, computed independently of Go.

The Go embedder must reproduce these vectors at cosine >= 0.9999, per document
and per passage. This script is the reference, so it is deliberately written
from the model card's published ONNX pipeline and not from the Go code: a
shared mistake is the one thing a reference cannot be allowed to have.

It differs from the card's pipeline in one way, on purpose. The card joins chunk
*text* with "<|endoftext|>" and tokenises the result. Remem tokenises the whole
content once and slices the *ids* into 256-token passages, inserting the
separator id itself, because re-tokenising a substring can change the tokens at
its edges. The fixture is built the same way, or Go and Python would be fed two
different inputs and agree about nothing.

Everything below is the construction of design decision 2
(openspec/changes/passage-vectors/design.md):

  - passages of 256 token ids, the last one shorter;
  - segments of at most 16 passages, overlapping by 2;
  - a segment's input is p0 SEP p1 SEP ... SEP pk, mask 1 everywhere, batch 1;
  - a passage's vector is the mean of its own tokens' hidden states, separators
    excluded;
  - a passage in two segments takes its vector from the one where it lies
    farther from the edge, ties to the earlier;
  - the document vector is the mean over every content token of the first
    segment;
  - every vector is quantised as round(tanh(x) * 127) clamped to [-128, 127],
    with torch's rounding, which is half to even (st_quantize.py).

The model runs from its fp32 export (onnx/model.onnx and its two data files).
The int8 above is the model's *output* rule, which it was trained with. The
int8-*weight* export (model_quantized.onnx) is a different vector producer, worst
cosine 0.9963 against this one, and it was slower on every input measured on the
development host.

The int8 values are stored as integers, before normalisation: they are exact,
four times smaller than floats in JSON, and normalising them is one line on the
reading side.

Usage (see docs/FIXTURES.md):

  python3 -m venv .tools/pplx-venv
  .tools/pplx-venv/bin/pip install onnxruntime==1.29.0 tokenizers==0.23.3 numpy==2.5.3
  make model
  .tools/pplx-venv/bin/python -I scripts/pplx_reference.py \\
      .models/pplx-embed-context-v1-0.6b fixtures/pplx_embedding_reference.json
"""

import json
import random
import sys

import numpy as np
import onnxruntime as ort
import tokenizers

MODEL = "pplx-embed-context-v1-0.6b"
REVISION = "b42df969d4d78d1840769e45c68c4e4ce763768b"
SEP_ID = 151643
PASSAGE_TOKENS = 256
SEGMENT_PASSAGES = 16
SEGMENT_OVERLAP = 2


def long_english(seed: int, min_tokens: int, tok) -> str:
    """Prose of at least min_tokens tokens, deterministic in the seed."""
    rng = random.Random(seed)
    subjects = ["The harbour authority", "A volunteer archivist", "The night-shift nurse",
                "Our accounting team", "The village council", "A retired geologist",
                "The orchestra's oboist", "The bakery on Elm Street", "The data centre",
                "A first-year apprentice"]
    verbs = ["recorded", "reviewed", "postponed", "photographed", "repaired",
             "catalogued", "measured", "questioned", "replaced", "described"]
    objects = ["the tide tables for October", "the cracked foundation of the east wing",
               "an invoice numbered INV-2024-8871", "the migration of swallows over the estuary",
               "the cooling system after the outage", "a letter written in 1911",
               "the sourdough starter's temperature", "the schedule for the spring recital",
               "the samples taken from the northern quarry", "the backlog of unanswered requests"]
    tails = ["before anyone else arrived.", "and wrote down every detail.",
             "although the weather made it difficult.", "because the deadline had moved.",
             "with help from two colleagues.", "for the third time this year.",
             "and nobody could explain the result.", "while the radio played in the background."]
    out, n = [], 0
    while n < min_tokens:
        para = " ".join(f"{rng.choice(subjects)} {rng.choice(verbs)} {rng.choice(objects)} {rng.choice(tails)}"
                        for _ in range(rng.randint(3, 6)))
        out.append(para)
        n = len(tok.encode("\n\n".join(out), add_special_tokens=False).ids)
    return "\n\n".join(out)


def non_ascii() -> str:
    """German, Japanese and emoji, long enough for at least two passages."""
    german = ("Die Brücke über die Isar wurde im Frühjahr gesperrt, weil Gutachter Risse "
              "in den Trägern gefunden hatten. Anwohner mussten einen Umweg von fünf "
              "Kilometern in Kauf nehmen, und die Straßenbahn fuhr wochenlang nur bis zum "
              "Max-Weber-Platz. ")
    japanese = ("東京の図書館では、古い地図の修復作業が続いている。担当者によると、"
                "江戸時代の資料は湿気に弱く、慎重な取り扱いが必要だという。")
    emoji = " 🙂👍🏽 🇩🇪 "
    parts, i = [], 0
    while True:
        parts.append(german if i % 3 == 0 else japanese if i % 3 == 1 else emoji + german)
        i += 1
        if i >= 9:
            return "".join(parts)


def quantise(v: np.ndarray) -> list:
    q = np.clip(np.round(np.tanh(v) * 127.0), -128, 127)  # np.round is half to even
    return [int(x) for x in q]


def segments(n: int) -> list:
    stride = SEGMENT_PASSAGES - SEGMENT_OVERLAP
    count = max(1, -(-(n - SEGMENT_OVERLAP) // stride))
    return [(s * stride, min(s * stride + SEGMENT_PASSAGES, n)) for s in range(count)]


def owner(p: int, segs: list) -> int:
    best, best_d = -1, -1
    for i, (a, b) in enumerate(segs):
        if a <= p < b:
            d = min(p - a, b - 1 - p)
            if d > best_d:  # strictly farther; ties keep the earlier segment
                best, best_d = i, d
    return best


def char_to_byte(text: str) -> list:
    """Maps a character offset to a UTF-8 byte offset, for every offset 0..len."""
    table, b = [0], 0
    for ch in text:
        b += len(ch.encode("utf-8"))
        table.append(b)
    return table


def embed(sess, tok, name: str, text: str) -> dict:
    enc = tok.encode(text, add_special_tokens=False)
    ids, offsets = enc.ids, enc.offsets
    passages = [(i, min(i + PASSAGE_TOKENS, len(ids))) for i in range(0, len(ids), PASSAGE_TOKENS)] or [(0, 0)]
    segs = segments(len(passages))
    c2b = char_to_byte(text)

    pooled = {}  # (segment, passage) -> mean hidden state
    document = None
    for si, (a, b) in enumerate(segs):
        seq, spans = [], []
        for p in range(a, b):
            if p > a:
                seq.append(SEP_ID)
            start = len(seq)
            seq.extend(ids[passages[p][0]:passages[p][1]])
            spans.append((p, start, len(seq)))
        input_ids = np.array([seq], dtype=np.int64)
        mask = np.ones_like(input_ids)
        hidden = sess.run(None, {"input_ids": input_ids, "attention_mask": mask})[0][0]
        content = []
        for p, s, e in spans:
            pooled[(si, p)] = hidden[s:e].mean(axis=0)
            content.extend(range(s, e))
        if si == 0:
            document = hidden[content].mean(axis=0)

    out = []
    for p, (s, e) in enumerate(passages):
        seg = owner(p, segs)
        out.append({
            "index": p,
            "segment": seg,
            "start": c2b[offsets[s][0]],
            "end": c2b[offsets[e - 1][1]],
            "int8": quantise(pooled[(seg, p)]),
        })
    return {
        "name": name,
        "text": text,
        "ids": ids,
        "segments": [list(s) for s in segs],
        "document": quantise(document),
        "passages": out,
    }


def main() -> None:
    model_dir, dest = sys.argv[1], sys.argv[2]
    opts = ort.SessionOptions()
    opts.intra_op_num_threads = 0
    sess = ort.InferenceSession(f"{model_dir}/onnx/model.onnx", opts,
                                providers=["CPUExecutionProvider"])
    tok = tokenizers.Tokenizer.from_file(f"{model_dir}/tokenizer.json")

    cases = [
        ("short", "The quarterly invoice from the cleaning company was paid on the fourth of March."),
        ("forty-passages", None),
        ("non-ascii", non_ascii()),
    ]
    # Exactly forty passages: more than two segments, so overlap ownership is
    # exercised in both directions, and a last segment shorter than sixteen.
    text = long_english(42, 39 * PASSAGE_TOKENS + 100, tok)
    enc = tok.encode(text, add_special_tokens=False)
    cut = enc.offsets[40 * PASSAGE_TOKENS - 1][1] if len(enc.ids) > 40 * PASSAGE_TOKENS else len(text)
    cases[1] = ("forty-passages", text[:cut])

    fixture = {
        "model": MODEL,
        "revision": REVISION,
        "generator": "scripts/pplx_reference.py",
        "onnxruntime": ort.__version__,
        "tokenizers": tokenizers.__version__,
        "sep_id": SEP_ID,
        "passage_tokens": PASSAGE_TOKENS,
        "segment_passages": SEGMENT_PASSAGES,
        "segment_overlap": SEGMENT_OVERLAP,
        "quantisation": "round_half_even(tanh(x) * 127), clamped to [-128, 127]; not normalised",
        "spans": "UTF-8 byte offsets into text, [start, end)",
        "cases": [embed(sess, tok, n, t) for n, t in cases],
    }
    for c in fixture["cases"]:
        print(f'{c["name"]}: {len(c["ids"])} tokens, {len(c["passages"])} passages, segments {c["segments"]}',
              file=sys.stderr)
    with open(dest, "w") as f:
        json.dump(fixture, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")


if __name__ == "__main__":
    main()
