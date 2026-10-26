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

"""PDF retrieval through persisted Source ingestion and scoped Memory."""

import asyncio
from pathlib import Path

from powercontext.builtin.artifacts.memory import MemoryEntryInput
from powercontext.builtin.persistence.sqlite import SQLiteConfig
from powercontext.builtin.runtime import (
    BuiltinConfig,
    GetMemoryEntryRequest,
    RememberMemoryRequest,
    SubmitSourceObservation,
    open_builtin_contexts,
    open_builtin_runtime,
)
from powercontext.builtin.scope import ScopeDraft
from powercontext.sources import SourceDefinitionRegistry, manifest_for_definition, project_source_for_transport
from powercontext.sources.pdf_pages import (
    PDF_PAGE_SOURCE_DEFINITION,
    PDFDocumentInput,
    parse_pdf_document,
    search_pdf_pages,
)
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY
from tests.pdf_support import build_pdf


class PageEvidencePipeline:
    async def extract(self, request, /) -> tuple[MemoryEntryInput, ...]:
        return tuple(
            MemoryEntryInput(
                kind=kind,
                text=source.projection(TEXT_EVIDENCE_PROJECTION_KEY)["content"],
                sources=(source,),
            )
            for source in request.sources
            for kind in ("source-evidence", "duplicate-page-evidence")
        )


def test_restart_retrieval_limits_scope_and_original_revision(tmp_path: Path) -> None:
    async def scenario() -> None:
        registry = SourceDefinitionRegistry((PDF_PAGE_SOURCE_DEFINITION,))
        raw = build_pdf(
            "Consolidated statements of income\n2024: Net sales 900 Operating income 120",
            "Consolidated statements of cash flows\n2024: Depreciation and amortization 30 Capital expenditures 50",
            "Legal proceedings: Litigation remains pending in federal court",
            "",
            *(
                f"Management discussion {index}: See consolidated statement of income for restructuring costs 2024"
                for index in range(24)
            ),
        )
        pages = parse_pdf_document(PDFDocumentInput(document_id="ACME_2024_10K", content=raw))
        revised = parse_pdf_document(
            PDFDocumentInput(
                document_id="ACME_2024_10K", content=build_pdf("Legal proceedings: Litigation settled 2024")
            )
        )
        observations = tuple([
            project_source_for_transport(registry, await registry.resolve(page)) for page in (*pages, *revised)
        ])
        other_pages = parse_pdf_document(PDFDocumentInput(document_id="OTHER_2024_10K", content=raw))[:3]
        other_observations = tuple([
            project_source_for_transport(registry, await registry.resolve(page)) for page in other_pages
        ])
        config = BuiltinConfig(database=SQLiteConfig(url=f"sqlite+aiosqlite:///{tmp_path / 'pdf.db'}"))
        async with open_builtin_runtime(config) as runtime:
            assert runtime.scopes is not None
            scope = await runtime.scopes.create(
                ScopeDraft(title="Public reports", summary="PDF evidence", idempotency_key="public")
            )
            private_scope = await runtime.scopes.create(
                ScopeDraft(title="Private reports", summary="Separate scope", idempotency_key="private")
            )
            await runtime.ingestion.register(manifest_for_definition(PDF_PAGE_SOURCE_DEFINITION))
            for observation in (*observations, *other_observations):
                await runtime.ingestion.submit(
                    SubmitSourceObservation(scope_id=scope.scope_id, observation=observation)
                )
            await runtime.ingestion.submit(
                SubmitSourceObservation(scope_id=scope.scope_id, observation=observations[0])
            )
            await runtime.ingestion.submit(
                SubmitSourceObservation(scope_id=private_scope.scope_id, observation=observations[1])
            )
        async with open_builtin_runtime(config, candidate_pipeline=PageEvidencePipeline()) as runtime:
            for scope_id in (scope.scope_id, private_scope.scope_id):
                memory = runtime.memory.for_scope(scope_id)
                for _ in range(len(observations) + 1):
                    if not (await memory.flush()).processed:
                        break
                else:
                    raise AssertionError("PDF evidence ingestion did not drain")  # noqa: TRY003
        async with open_builtin_contexts(config) as contexts:
            journal = await (await contexts.get(scope.scope_id)).sources.journal.entries()
            assert len(journal) == len(observations) + len(other_observations)
        async with open_builtin_runtime(config) as runtime:
            memory = runtime.memory.for_scope(scope.scope_id)
            assert await search_pdf_pages(memory, "ACME EBITDA", limit=0) == ()
            assert await search_pdf_pages(memory, "ACME EBITDA", limit=-1) == ()
            assert await search_pdf_pages(memory, "   ") == ()
            citations = await search_pdf_pages(memory, "What was ACME's 2024 EBITDA less capex?", limit=5)
            assert 2 <= len(citations) <= 5
            entries = [(await memory.get(GetMemoryEntryRequest(citation=citation))).entry for citation in citations]
            assert {observations[0].name, observations[1].name} <= {entry.sources[0].source_id for entry in entries}
            assert len({entry.sources[0].source_id for entry in entries}) == len(entries)
            assert len(await search_pdf_pages(memory, "ACME EBITDA", limit=1)) == 1
            income = await search_pdf_pages(
                memory, "What restructuring costs appear in ACME's 2024 income statement?", limit=1
            )
            income_sources = {
                (await memory.get(GetMemoryEntryRequest(citation=citation))).entry.sources[0].source_id
                for citation in income
            }
            assert observations[0].name in income_sources
            legal = await search_pdf_pages(memory, "Does ACME have ongoing legal battles?", limit=20)
            legal_sources = {
                (await memory.get(GetMemoryEntryRequest(citation=citation))).entry.sources[0].source_id
                for citation in legal
            }
            assert {observations[2].name, observations[-1].name} <= legal_sources
            private = await search_pdf_pages(runtime.memory.for_scope(private_scope.scope_id), "ACME capex", limit=20)
            assert len(private) == 1
            private_entry = (
                await runtime.memory.for_scope(private_scope.scope_id).get(GetMemoryEntryRequest(citation=private[0]))
            ).entry
            assert private_entry.sources[0].source_id == observations[1].name
            assert private[0] not in citations
            await memory.remember(
                RememberMemoryRequest(entries=(MemoryEntryInput(kind="note", text="ACME 2024 EBITDA capex summary"),))
            )
        async with open_builtin_runtime(config) as runtime:
            # A later head and process restart must not retarget earlier evidence.
            memory = runtime.memory.for_scope(scope.scope_id)
            for citation, entry in zip(citations, entries, strict=True):
                assert (await memory.get(GetMemoryEntryRequest(citation=citation))).entry == entry
            repeated = await search_pdf_pages(memory, "ACME 2024 EBITDA", limit=20)
            source_ids = [
                (await memory.get(GetMemoryEntryRequest(citation=citation))).entry.sources[0].source_id
                for citation in repeated
            ]
            assert len(source_ids) == len(set(source_ids))

    asyncio.run(scenario())
