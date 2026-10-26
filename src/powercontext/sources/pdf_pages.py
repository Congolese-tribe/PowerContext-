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

"""Complete PDF capture and offline retrieval of exact physical-page evidence.

Install ``powercontext[pdf]`` to parse documents. Retrieval needs the built-in
Runtime and uses only its scoped Memory search and exact-read operations.
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import re
from collections import Counter
from typing import TYPE_CHECKING, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator

from powercontext.artifacts import MemoryCitation
from powercontext.sources.definitions import SourceDefinition, SourceProjection
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY, TextEvidence

if TYPE_CHECKING:
    from pypdf import PageObject, PdfReader
    from pypdf.generic import ContentStream, DictionaryObject

    from powercontext.builtin.artifacts.memory.models import MemoryHit
    from powercontext.builtin.runtime import ScopedMemoryApplication


class PDFDocumentInput(BaseModel):
    """One complete original PDF and its caller-assigned document identity."""

    model_config = ConfigDict(frozen=True)

    document_id: str
    content: bytes

    @field_validator("document_id")
    @classmethod
    def require_document_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("document_id must not be blank")  # noqa: TRY003
        return value


class PDFPageInput(BaseModel):
    """Extracted text from a physical page, numbered from one, including blanks."""

    model_config = ConfigDict(frozen=True)

    document_id: str
    document_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    page_number: int = Field(strict=True, ge=1)
    text: str

    @field_validator("document_id")
    @classmethod
    def require_document_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("document_id must not be blank")  # noqa: TRY003
        return value


def parse_pdf_document(value: PDFDocumentInput) -> tuple[PDFPageInput, ...]:
    """Extract every physical page atomically, or raise ``ValueError``.

    Empty user passwords (including permission-only encryption) are supported.
    Page text is extracted without filtering headers, tables, or empty pages.
    No result is exposed until the entire document has been read successfully.
    """
    from pypdf import PdfReader

    digest = hashlib.sha256(value.content).hexdigest()
    try:
        with PdfReader(io.BytesIO(value.content), strict=True) as reader:
            if reader.is_encrypted and not reader.decrypt(""):
                raise ValueError("PDF requires a password")  # noqa: TRY003, TRY301
            pages = tuple(
                PDFPageInput(
                    document_id=value.document_id,
                    document_sha256=digest,
                    page_number=number,
                    text=_extract_page_text(page, reader),
                )
                for number, page in enumerate(reader.pages, start=1)
            )
            page_tree = cast("DictionaryObject", reader.root_object["/Pages"])
            if len(pages) != page_tree["/Count"]:
                raise ValueError("PDF page tree count is inconsistent")  # noqa: TRY003, TRY301
    except Exception as error:
        # pypdf can also raise decoding, indexing, and decompression errors for
        # damaged objects; the ingestion boundary has one atomic failure contract.
        raise ValueError("Cannot completely parse PDF document") from error  # noqa: TRY003
    if not pages:
        raise ValueError("PDF has no physical pages")  # noqa: TRY003
    return pages


def _extract_page_text(page: PageObject, reader: PdfReader) -> str:
    from pypdf.generic import ContentStream, DictionaryObject, StreamObject

    def validate_forms(content: ContentStream, resources: DictionaryObject, active: set[int]) -> None:
        for operands, operator in content.operations:
            if operator != b"Do":
                continue
            objects = cast(DictionaryObject, resources["/XObject"])
            form = objects[operands[0]]
            if not isinstance(form, StreamObject):
                raise TypeError("PDF XObject must be a stream")  # noqa: TRY003
            if form.get("/Subtype") != "/Form":
                continue
            if id(form) in active:
                raise ValueError("Cyclic PDF form reference")  # noqa: TRY003
            active.add(id(form))
            validate_forms(
                ContentStream(form, reader),
                cast(DictionaryObject, form.get("/Resources", resources).get_object()),
                active,
            )
            active.remove(id(form))

    content = page.get_contents()
    if content is not None:
        # pypdf deliberately logs and skips unreadable Form XObjects during text
        # extraction, even in strict mode. Validate invoked forms first so that
        # a damaged later page cannot silently become an empty or partial page.
        validate_forms(content, cast(DictionaryObject, page.get("/Resources", DictionaryObject()).get_object()), set())
    return page.extract_text() or ""


class PDFPageSource(Source):
    """A captured page whose original bytes are located by document digest."""

    page: PDFPageInput


class _PDFPageTextProjection:
    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class: type[PDFPageSource] = PDFPageSource
    output_class: type[BaseModel] = TextEvidence

    def project(self, source: PDFPageSource, /) -> TextEvidence:
        page = source.page
        return TextEvidence(
            source_type="pdf-page",
            source_id=source.name,
            content=f"{page.document_id}\n{page.text}",
            metadata={
                "document_id": page.document_id,
                "document_sha256": page.document_sha256,
                "page_number": page.page_number,
            },
        )


class _PDFPageDefinition:
    input_class: type[PDFPageInput] = PDFPageInput
    name: str = "pdf-page"
    version: str = "1"
    source_class: type[PDFPageSource] = PDFPageSource
    projections: tuple[SourceProjection[PDFPageSource], ...] = (_PDFPageTextProjection(),)

    async def resolve(self, value: PDFPageInput, /) -> PDFPageSource:
        # Framing prevents delimiter collisions; hashing bounds even arbitrary
        # Unicode document IDs without truncating any identity-bearing component.
        identity = json.dumps(
            [value.document_id, value.document_sha256, value.page_number],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return PDFPageSource(
            name=f"pdf-page:{hashlib.sha256(identity).hexdigest()}",
            materialization=SourceMaterialization.CAPTURED,
            page=value,
        )

    async def read(self, source: PDFPageSource, /) -> PDFPageInput:
        return source.page


PDF_PAGE_SOURCE_DEFINITION: SourceDefinition[PDFPageInput, PDFPageSource, PDFPageInput] = _PDFPageDefinition()

_WORDS = re.compile(r"[a-z]+|\d+")
_STOP_WORDS = frozenset([
    "a",
    "an",
    "the",
    "and",
    "or",
    "for",
    "of",
    "to",
    "in",
    "on",
    "at",
    "by",
    "from",
    "with",
    "as",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "being",
    "what",
    "which",
    "who",
    "how",
    "why",
    "when",
    "where",
    "do",
    "does",
    "did",
    "can",
    "could",
    "should",
    "would",
    "has",
    "have",
    "had",
    "i",
    "we",
    "you",
    "it",
    "its",
    "our",
    "your",
    "this",
    "that",
    "these",
    "those",
    "s",
    "t",
    "fy",
    "fiscal",
    "year",
    "years",
    "please",
    "give",
    "answer",
    "respond",
    "response",
    "question",
    "assuming",
    "assume",
    "perspective",
    "analyst",
    "investment",
    "only",
    "use",
    "using",
    "details",
    "shown",
    "within",
    "rely",
    "relying",
    "based",
    "calculate",
    "define",
    "state",
    "if",
    "then",
    "any",
    "directly",
    "explicitly",
    "outlined",
    "reported",
    "unadjusted",
    "usd",
    "million",
    "millions",
])

# General accounting vocabulary, shared by every document and query. These are
# alternative search terms, never additions to captured page text.
_FINANCIAL_TERMS = (
    (frozenset({"ebitda"}), "operating income depreciation amortization"),
    (frozenset({"capex"}), "capital expenditures purchases property plant equipment"),
    (frozenset({"geography", "geographies", "geographic", "geographical"}), "geographic segments regions countries"),
    (frozenset({"legal", "litigation", "lawsuit", "lawsuits"}), "legal litigation proceedings contingencies"),
    (frozenset({"revenue", "revenues", "sales"}), "revenue revenues sales"),
    (frozenset({"income", "earnings", "profit", "profits"}), "income earnings operations"),
    (frozenset({"cash", "cashflow"}), "cash flows operating activities"),
    (frozenset({"debt", "borrowings"}), "debt borrowings obligations"),
    (frozenset({"restructuring"}), "restructuring charges costs"),
)

# Questions about a statement may concern an item that is absent from that
# statement. Heading evidence must therefore compete with keyword mentions in
# notes and exhibits, even when the requested line item itself is not present.
_REPORT_SECTIONS = (
    (
        r"\bebitda\b|\bincome statements?\b|\bstatements? of (?:income|operations|earnings)\b|\bp\s*&\s*l\b",
        "consolidated income earnings operations",
        r"^(?:consolidated\s+)?statements? of (?:income|operations|earnings)(?:\s*\(continued\))?\s*$",
    ),
    (
        r"\bebitda\b|\bcash\s*flows?\b|\bd\s*&\s*a\b|\bcapex\b",
        "statement cash flows",
        r"^(?:consolidated\s+)?statements? of cash flows?(?:\s*\(continued\))?\s*$",
    ),
    (
        r"\bbalance sheets?\b|\bfinancial position\b|\bcurrent ratio\b|\bworking capital\b",
        "balance sheet financial position",
        r"^(?:consolidated\s+)?(?:balance sheets?|statements? of financial position)(?:\s*\(continued\))?\s*$",
    ),
    (
        r"\bgeograph(?:y|ies|ic|ical)\b",
        "geographic divisions segments",
        r"^(?:our (?:divisions|segments)|geograph(?:ic|ical) (?:information|areas|data))\b",
    ),
    (
        r"\blegal\b|\blitigation\b|\blawsuits?\b",
        "legal proceedings litigation",
        r"^(?:item\s+\d+\.?\s+)?legal proceedings[.:]?\s*$|^(?:note\s+\d+\W+)?commitments and contingencies[.:]?\s*$",
    ),
)


def _query_terms(question: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    text = re.sub(r"\bd\s*&\s*a\b", "depreciation amortization", question.lower())
    text = re.sub(r"\bp\s*&\s*l\b", "income earnings", text)
    words = tuple(dict.fromkeys(word for word in _WORDS.findall(text) if word not in _STOP_WORDS))
    expansions = tuple(phrase for triggers, phrase in _FINANCIAL_TERMS if triggers.intersection(words))
    return words, expansions


def _lexical_terms(text: str) -> list[str]:
    # A small plural normalizer keeps costs/cost and liabilities/liability
    # comparable without changing source text or requiring a language model.
    return [
        word[:-3] + "y"
        if len(word) > 4 and word.endswith("ies")
        else word[:-1]
        if len(word) > 3 and word.endswith("s") and not word.endswith(("ss", "us"))
        else word
        for word in _WORDS.findall(text.lower())
    ]


def _rank_pages(
    hits: tuple[MemoryHit, ...], words: tuple[str, ...], expansions: tuple[str, ...], headings: tuple[str, ...]
) -> list[MemoryHit]:
    """BM25 over a bounded candidate pool, with document and phrase evidence."""
    bodies = [_lexical_terms(hit.text.partition("\n")[2]) for hit in hits]
    counts = [Counter(body) for body in bodies]
    lengths = [sum(count.values()) for count in counts]
    average_length = sum(lengths) / max(len(lengths), 1) or 1.0
    frequencies: Counter[str] = Counter(term for count in counts for term in count)
    weights = dict.fromkeys(_lexical_terms(" ".join(words)), 1.0)
    for term in _lexical_terms(" ".join(expansions)):
        weights.setdefault(term, 0.45)
    phrases = [" ".join(_lexical_terms(phrase)) for phrase in expansions]
    query_sequence = _lexical_terms(" ".join(words))
    phrases.extend(" ".join(query_sequence[index : index + 2]) for index in range(len(query_sequence) - 1))

    def score(index: int) -> float:
        count = counts[index]
        total = 0.0
        for term, weight in weights.items():
            frequency = count[term]
            idf = math.log1p((len(hits) - frequencies[term] + 0.5) / (frequencies[term] + 0.5))
            normalization = 1.2 * (0.45 + 0.55 * lengths[index] / average_length)
            total += weight * idf * frequency * 2.2 / (frequency + normalization)
        body = " ".join(bodies[index])
        total += 0.8 * sum(phrase in body for phrase in phrases if phrase)
        prefix = "\n".join(" ".join(line.lower().split()) for line in hits[index].text.partition("\n")[2].splitlines())[
            :2000
        ]
        total += 12.0 * sum(bool(re.search(heading, prefix, re.MULTILINE)) for heading in headings)
        document = set(_lexical_terms(hits[index].text.partition("\n")[0]))
        # The document identity is present once in the standard projection. It
        # disambiguates companies and revisions without a document-name lookup.
        total += 3.0 * sum(term in document for term in query_sequence if len(term) > 2 and not term.isdigit())
        total += 0.5 * sum(term in document for term in query_sequence if term.isdigit())
        return total

    return [hits[index] for index in sorted(range(len(hits)), key=lambda index: (-score(index), index))]


async def search_pdf_pages(
    memory: ScopedMemoryApplication, question: str, limit: int = 10
) -> tuple[MemoryCitation, ...]:
    """Search scoped, persisted pages offline and return distinct exact citations.

    A bounded pool from FTS and accounting-term expansions is reranked locally.
    No global index, filesystem access, model, or cross-scope cache is used.
    A non-positive limit or a query without searchable terms returns no pages.
    """
    from powercontext.builtin.runtime import GetMemoryEntryRequest, SearchMemoryRequest

    if limit <= 0:
        return ()
    words, expansions = _query_terms(question)
    if not words:
        return ()
    sections = tuple(section for section in _REPORT_SECTIONS if re.search(section[0], question, re.IGNORECASE))
    queries = tuple(dict.fromkeys((" ".join(words[:64]), *expansions, *(section[1] for section in sections))))
    pool: dict[tuple[str, str, str], MemoryHit] = {}
    candidate_limit = max(100, min(400, limit * 10))
    for query in queries:
        page = await memory.search(SearchMemoryRequest(query=query, mode="fts", limit=candidate_limit))
        for hit in page.hits:
            pool.setdefault((hit.memory_ref.artifact_id, hit.entry_id, hit.entry_version_id), hit)
    ranked = _rank_pages(tuple(pool.values()), words, expansions, tuple(section[2] for section in sections))
    citations: list[MemoryCitation] = []
    seen: set[str] = set()
    for hit in ranked:
        citation = MemoryCitation(
            memory_ref=hit.memory_ref, entry_id=hit.entry_id, entry_version_id=hit.entry_version_id
        )
        record = await memory.get(GetMemoryEntryRequest(citation=citation))
        references = record.entry.sources
        if len(references) != 1 or references[0].source_type != "pdf-page" or references[0].source_id in seen:
            continue
        seen.add(references[0].source_id)
        citations.append(citation)
        if len(citations) == limit:
            break
    return tuple(citations)


__all__ = ["PDF_PAGE_SOURCE_DEFINITION", "PDFDocumentInput", "PDFPageInput", "parse_pdf_document", "search_pdf_pages"]
