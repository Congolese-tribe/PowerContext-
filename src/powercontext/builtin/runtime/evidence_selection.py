# Copyright (c) 2026 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Query-driven selection of literal spans under a caller-owned rendering budget."""

from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass

_WORDS = re.compile(r"[a-z]+|\d+|[\u3400-\u9fff]|[^\W\d_]+", re.IGNORECASE)
_BOUNDARY = re.compile(r"(?<=[.!?。！？])\s+|\n+")  # noqa: RUF001 - literal sentence punctuation
_STOP = frozenset([
    "a",
    "an",
    "the",
    "and",
    "or",
    "but",
    "if",
    "as",
    "at",
    "by",
    "for",
    "from",
    "in",
    "into",
    "of",
    "on",
    "to",
    "with",
    "without",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "being",
    "this",
    "that",
    "these",
    "those",
    "it",
    "its",
    "their",
    "they",
    "them",
    "we",
    "our",
    "you",
    "your",
    "which",
    "who",
    "what",
    "when",
    "where",
    "how",
    "than",
    "then",
    "also",
    "can",
    "could",
    "may",
    "might",
    "will",
    "would",
    "should",
    "do",
    "does",
    "did",
    "have",
    "has",
    "had",
    "show",
    "shows",
    "showed",
    "shown",
    "study",
    "studies",
    "result",
    "results",
    "using",
    "used",
    "use",
    "such",
    "both",
    "each",
    "between",
    "during",
    "directly",
    "indirectly",
    "easily",
    "significantly",
    "specifically",
    "substantially",
    "strongly",
    "weakly",
    "generally",
    "particularly",
    "relatively",
    "similarly",
    "approximately",
    "potentially",
    "better",
    "worse",
    "positive",
    "negative",
    "total",
    "impact",
    "association",
    "associated",
    "related",
])


@dataclass(frozen=True)
class EvidenceSpan:
    """An internal character interval in one caller-provided document."""

    document: int
    start: int
    end: int


def _stem(word: str) -> str:
    word = word.casefold()
    if len(word) > 5 and word.endswith("ies"):
        return word[:-3] + "y"
    if len(word) > 5 and word.endswith("ing"):
        word = word[:-3]
        if len(word) > 3 and word[-1] == word[-2]:
            word = word[:-1]
    elif len(word) > 4 and word.endswith("ed"):
        word = word[:-2]
    elif len(word) > 3 and word.endswith("s") and not word.endswith(("ss", "us", "is")):
        word = word[:-1]
    if len(word) > 6 and word.endswith("ion") and word[-4] in "st":
        word = word[:-3]
    # Match a silent-e inflection without changing any delivered source text.
    if len(word) > 4 and word.endswith("e"):
        word = word[:-1]
    return word


def _terms(text: str) -> Counter[str]:
    return Counter(_stem(match[0]) for match in _WORDS.finditer(text) if match[0].casefold() not in _STOP)


def _sentences(text: str, document: int) -> list[EvidenceSpan]:
    spans = []
    start = 0
    for boundary in _BOUNDARY.finditer(text):
        if text[start : boundary.start()].strip():
            spans.append(EvidenceSpan(document, start, boundary.end()))
        # Whitespace may belong to an original sentence. Preserve it on both
        # sides; overlapping selections are merged before delivery.
        start = boundary.start()
    if text[start:].strip():
        spans.append(EvidenceSpan(document, start, len(text)))
    return spans


def _merge(spans: Sequence[EvidenceSpan], texts: Sequence[str]) -> tuple[EvidenceSpan, ...]:
    merged: list[EvidenceSpan] = []
    for span in sorted(spans, key=lambda item: (item.document, item.start, item.end)):
        if merged and merged[-1].document == span.document:
            previous = merged[-1]
            gap = texts[span.document][previous.end : span.start]
            if span.start <= previous.end or not gap.strip():
                merged[-1] = EvidenceSpan(span.document, previous.start, max(previous.end, span.end))
                continue
        merged.append(span)
    return tuple(merged)


def _ranked_sentences(
    texts: Sequence[str], query: str, spans: Sequence[EvidenceSpan]
) -> list[tuple[EvidenceSpan, float]]:
    """Score every sentence using only the query and the provided candidate set."""
    query_terms = _terms(query)
    if not spans or not query_terms:
        return []
    tokens = [_terms(texts[span.document][span.start : span.end]) for span in spans]
    frequencies: Counter[str] = Counter(term for row in tokens for term in row)
    document_terms = [set(_terms(text)) for text in texts]
    document_frequency = Counter(term for row in document_terms for term in row)
    entities = {
        term
        for match in re.finditer(r"[A-Za-z][A-Za-z0-9-]*", query)
        if any(char.isdigit() for char in match[0])
        or sum(char.isupper() for char in match[0]) >= 2
        or (match.start() > 0 and match[0][0].isupper())
        for term in _terms(match[0])
        if not term.isdigit()
    }
    weights = {
        term: math.log(1 + (len(spans) - frequencies[term] + 0.5) / (frequencies[term] + 0.5))
        * (0.3 + math.log(1 + (len(texts) - document_frequency[term] + 0.5) / (document_frequency[term] + 0.5)))
        * (2.5 if term in entities else 1)
        for term in query_terms
    }
    weight_sum = sum(weights.values())
    document_coverage = [sum(weights[term] for term in weights if term in row) / weight_sum for row in document_terms]
    average = sum(sum(row.values()) for row in tokens) / len(tokens) or 1
    scores = []
    for span, row in zip(spans, tokens, strict=True):
        length = sum(row.values())
        coverage = sum(weight for term, weight in weights.items() if term in row) / weight_sum
        relevance = sum(
            weight * row[term] / (row[term] + 0.5 + 0.5 * length / average) for term, weight in weights.items()
        )
        scores.append(relevance * (0.4 + coverage) * (0.2 + document_coverage[span.document]) ** 2)
    contextual = []
    for index, (span, score) in enumerate(zip(spans, scores, strict=True)):
        neighbour = max(
            (
                scores[other]
                for other in (index - 1, index + 1)
                if 0 <= other < len(spans) and spans[other].document == span.document
            ),
            default=0.0,
        )
        contextual.append(score + 0.12 * neighbour)

    # Bound repeated budget exploration while still scoring every source sentence.
    return [(spans[index], contextual[index]) for index in sorted(range(len(spans)), key=lambda i: -contextual[i])[:96]]


def _budget_units(
    texts: Sequence[str], max_bytes: int, measure: Callable[[tuple[EvidenceSpan, ...]], int]
) -> list[EvidenceSpan]:
    units = []
    for document, text in enumerate(texts):
        for span in _sentences(text, document):
            if measure((span,)) <= max_bytes:
                units.append(span)
                continue
            # An oversized sentence still has useful interior evidence. Partition
            # it at nearby word boundaries rather than defaulting to its prefix.
            start = span.start
            while start < span.end:
                end = min(start + 128, span.end)
                if end < span.end:
                    space = text.rfind(" ", start + 64, end)
                    if space >= 0:
                        end = space + 1
                units.append(EvidenceSpan(document, start, end))
                start = end
    return units


def select_evidence(
    texts: Sequence[str],
    query: str,
    *,
    max_bytes: int,
    max_items: int,
    measure: Callable[[tuple[EvidenceSpan, ...]], int],
) -> tuple[EvidenceSpan, ...]:
    """Greedily admit relevant sentences, measuring exact serialized output.

    Local inverse document frequency and length-normalized term frequency rank
    sentences. Adjacent selections merge, sharing citation overhead. Neighbouring
    sentences receive a small context credit to retain explanatory continuations.
    No document identity, external corpus, model, or relevance label is consulted.
    """
    candidates = _ranked_sentences(texts, query, _budget_units(texts, max_bytes, measure))
    selected: tuple[EvidenceSpan, ...] = ()
    selected_bytes = measure(selected)
    while candidates:
        best: tuple[float, int, tuple[EvidenceSpan, ...], int] | None = None
        for index, (span, score) in enumerate(candidates):
            if score <= 0 or any(
                kept.document == span.document and kept.start <= span.start and kept.end >= span.end
                for kept in selected
            ):
                continue
            proposal = _merge((*selected, span), texts)
            if len(proposal) > max_items:
                continue
            size = measure(proposal)
            if size > max_bytes:
                continue
            utility = score / max(32, size - selected_bytes) ** 0.45
            if best is None or utility > best[0]:
                best = (utility, index, proposal, size)
        if best is None:
            break
        _, index, selected, selected_bytes = best
        candidates.pop(index)
    return selected
