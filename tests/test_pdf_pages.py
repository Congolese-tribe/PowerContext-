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

import asyncio
import hashlib
import io

import pytest
from pypdf import PdfWriter
from pypdf.generic import DictionaryObject, NameObject, NumberObject

from powercontext.sources import (
    TEXT_EVIDENCE_PROJECTION_KEY,
    SourceDefinitionRegistry,
    SourceMaterialization,
    SourceObservation,
    project_source_for_transport,
)
from powercontext.sources.pdf_pages import (
    PDF_PAGE_SOURCE_DEFINITION,
    PDFDocumentInput,
    PDFPageInput,
    parse_pdf_document,
)
from tests.pdf_support import build_form_pdf, build_pdf


def test_physical_pages_preserve_all_text_numbers_and_blanks() -> None:
    raw = build_pdf("Printed page iv: Revenue 1,234 Expenses 567", "", "Printed page 99: Cash 890")
    pages = parse_pdf_document(PDFDocumentInput(document_id="Report", content=raw))
    assert isinstance(pages, tuple)
    assert [(page.page_number, page.text.strip()) for page in pages] == [
        (1, "Printed page iv: Revenue 1,234 Expenses 567"),
        (2, ""),
        (3, "Printed page 99: Cash 890"),
    ]
    assert all(page.document_id == "Report" for page in pages)
    assert all(page.document_sha256 == hashlib.sha256(raw).hexdigest() for page in pages)


def test_empty_password_permissions_preserve_extractable_text() -> None:
    raw = build_pdf("Operating income 123", "", password="")
    pages = parse_pdf_document(PDFDocumentInput(document_id="Permissions", content=raw))
    assert [page.text.strip() for page in pages] == ["Operating income 123", ""]


@pytest.mark.parametrize(
    "raw",
    [b"", b"not a PDF", b"%PDF-1.7\ntruncated", build_pdf("Secret", password="secret"), build_pdf()],  # noqa: S106
)
def test_unreadable_pdf_fails_with_value_error(raw: bytes) -> None:
    with pytest.raises(ValueError):
        parse_pdf_document(PDFDocumentInput(document_id="Invalid", content=raw))


def test_damaged_later_page_fails_instead_of_returning_partial_document() -> None:
    raw = build_pdf("First page is readable", "Second page is damaged", damaged_page=2)
    with pytest.raises(ValueError):
        parse_pdf_document(PDFDocumentInput(document_id="Damaged", content=raw))


def test_form_text_is_preserved_and_damaged_forms_fail_atomically() -> None:
    pages = parse_pdf_document(PDFDocumentInput(document_id="Forms", content=build_form_pdf()))
    assert "First page" in pages[0].text
    assert "Before form" in pages[1].text and "Text inside form 456" in pages[1].text
    with pytest.raises(ValueError):
        parse_pdf_document(PDFDocumentInput(document_id="Forms", content=build_form_pdf(damaged=True)))


def test_inconsistent_page_tree_count_is_rejected() -> None:
    writer = PdfWriter(io.BytesIO(build_pdf("First page", "Second page")))
    page_tree = writer.root_object["/Pages"]
    assert isinstance(page_tree, DictionaryObject)
    page_tree[NameObject("/Count")] = NumberObject(3)
    buffer = io.BytesIO()
    writer.write(buffer)
    with pytest.raises(ValueError):
        parse_pdf_document(PDFDocumentInput(document_id="Missing page", content=buffer.getvalue()))


@pytest.mark.parametrize("document_id", ["", " ", "\t\n"])
def test_blank_document_id_is_rejected(document_id: str) -> None:
    with pytest.raises(ValueError):
        PDFDocumentInput(document_id=document_id, content=build_pdf("Text"))
    with pytest.raises(ValueError):
        PDFPageInput(document_id=document_id, document_sha256="0" * 64, page_number=1, text="")


@pytest.mark.parametrize("changes", [{"page_number": 0}, {"page_number": 1.5}, {"document_sha256": "ABC"}])
def test_page_provenance_requires_physical_page_and_full_digest(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        PDFPageInput.model_validate(
            {"document_id": "Report", "document_sha256": "0" * 64, "page_number": 1, "text": ""} | changes
        )


def test_captured_identity_projection_and_transport_roundtrip() -> None:
    async def scenario() -> None:
        definition = PDF_PAGE_SOURCE_DEFINITION
        assert definition.name == "pdf-page" and definition.version == "1"
        assert definition.input_class is PDFPageInput
        registry = SourceDefinitionRegistry((definition,))
        raw = build_pdf("Revenue 123", "")
        identifier = "文档:|/" * 100
        pages = parse_pdf_document(PDFDocumentInput(document_id=identifier, content=raw))
        revised = parse_pdf_document(
            PDFDocumentInput(document_id=identifier, content=build_pdf("Revenue 456", "", title="Revision"))
        )
        other = pages[0].model_copy(update={"document_id": identifier + ":1"})
        sources = [await registry.resolve(page) for page in (*pages, *revised, other)]
        assert len({source.name for source in sources}) == 5
        for source, page in zip(sources, (*pages, *revised, other), strict=True):
            assert len(source.name) <= 256
            assert source.materialization is SourceMaterialization.CAPTURED
            assert (await registry.resolve(page)).name == source.name
            assert await registry.read(source) == page
            restored = type(source).model_validate_json(source.model_dump_json())
            assert await registry.read(restored) == page
            observation = project_source_for_transport(registry, source)
            observation = SourceObservation.model_validate_json(observation.model_dump_json())
            assert observation.projection(TEXT_EVIDENCE_PROJECTION_KEY) == {
                "source_type": "pdf-page",
                "source_id": source.name,
                "content": f"{page.document_id}\n{page.text}",
                "metadata": {
                    "document_id": page.document_id,
                    "document_sha256": page.document_sha256,
                    "page_number": page.page_number,
                },
            }

    asyncio.run(scenario())
