---
title: Sources and capture
description: Capture evidence, inspect Source records, and configure processing.
---

# Sources and capture

Sources preserve evidence before it becomes selected knowledge. A Content Source stores captured text; other Source
types can refer to material supplied by a connector. Work Contracts and Task Outcomes also preserve Source evidence.

## Capture and inspect

1. Choose an existing Scope. The [API Quick Start](../develop/api-quickstart.md) demonstrates content capture through
   `POST /v1/sources/content` and retains the returned Source reference.
2. Use `GET /v1/scopes/{scope_id}/sources` to browse the Source journal, then
   `GET /v1/scopes/{scope_id}/sources/{source_type}/{source_id}` to read the selected Source.
3. To process eligible evidence into Memory, configure a generation model and either scheduled processing or an
   explicit Memory flush. See [Enable extraction and vector search](../get-started/configure-models.md).

A successful capture means evidence was stored; it does not mean extraction succeeded or that the result was worth
keeping. Use Source references to trace a generated result back to its input. Do not retry a capture with changed
content under the same identity as if it were a new observation.

## Choose a capture path

- Agent prompt hooks: use the host-specific capture switch in [Connect Agents](../integrations/index.md).
- Files and object stores: use the [OpenDAL connector](ingest-text-files-with-opendal.md).
- Subject evidence for a Scope profile: follow [Scope profiles](use-profiles.md).
- Application or evaluation events: use the application's adapter; [Bub capture](../integrations/evaluation.md) is opt-in.

Source writes and reads are subject to the selected [Scope and access policy](scopes-and-access.md).

## Capture complete PDF pages

Install `powercontext[builtin,pdf]` to use the PDF page Source. The PDF extra pins `pypdf==6.19.0`.
The parser accepts complete original bytes and returns every physical page in order, including blank pages.
It preserves extractable text and table numbers without OCR. Unreadable or password-protected documents that
cannot be opened with an empty password raise `ValueError`; extraction never returns a partial document.

```python
from pathlib import Path

from powercontext import PDF_PAGE_SOURCE_DEFINITION, PDFDocumentInput, parse_pdf_document, search_pdf_pages
from powercontext.builtin.runtime import GetMemoryEntryRequest, SubmitSourceObservation
from powercontext.sources import SourceDefinitionRegistry, manifest_for_definition, project_source_for_transport


async def capture_report(runtime, scope_id: str, path: Path, document_id: str):
    pages = parse_pdf_document(PDFDocumentInput(document_id=document_id, content=path.read_bytes()))
    registry = SourceDefinitionRegistry((PDF_PAGE_SOURCE_DEFINITION,))
    await runtime.ingestion.register(manifest_for_definition(PDF_PAGE_SOURCE_DEFINITION))
    for page in pages:
        source = await registry.resolve(page)
        await runtime.ingestion.submit(SubmitSourceObservation(
            scope_id=scope_id,
            observation=project_source_for_transport(registry, source),
        ))
    return pages


async def retrieve_pages(memory, question: str):
    citations = await search_pdf_pages(memory, question, limit=20)
    return tuple([
        (citation, (await memory.get(GetMemoryEntryRequest(citation=citation))).entry)
        for citation in citations
    ])
```

Configure the Memory extraction pipeline to retain each page as a separate entry with its Source reference,
and finish processing observations before retrieval. Source ingestion alone does not create searchable Memory
entries. Pass `runtime.memory.for_scope(scope_id)` to `retrieve_pages`; the Runtime may be reopened before searching.
The retrieval helper uses FTS, general accounting vocabulary, statement headings, and local lexical reranking.
It returns at most `limit` different page citations; non-positive limits and queries without searchable terms return `()`.

Page identity hashes the exact `(document_id, document_sha256, page_number)` tuple. The SHA-256 covers the complete
original bytes, so a revised PDF receives new identities while existing citations retain their original evidence.
The standard text projection contains `document_id + "\n" + page.text`; metadata contains exactly `document_id`,
`document_sha256`, and `page_number`. Retain original PDFs by digest and use the physical page number starting at
one to locate evidence. Printed page labels can differ. Retrieval does not read PDFs, labels, or external services.
