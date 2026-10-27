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

"""Real Memory citation readback and independent public SciFact quality measurement."""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

import pytest

from powercontext.artifacts import ArtifactRef, MemoryCitation
from powercontext.builtin.artifacts.memory import MemoryCandidateRequest, MemoryEntryInput, MemoryHit
from powercontext.builtin.runtime import (
    BuiltinConfig,
    CaptureSource,
    ContextAssembly,
    ContextPack,
    GetMemoryEntryRequest,
    PrepareContextRequest,
    SearchMemoryRequest,
    open_builtin_runtime,
)
from powercontext.builtin.runtime.prepared_context import PreparedContextBuilder, PreparedMemoryCandidates
from powercontext.builtin.runtime.prepared_text import TRUST_POLICY
from powercontext.builtin.scope import ScopeDraft
from powercontext.builtin.sources import ContentSource

_FIXTURE = Path(__file__).parents[1] / "fixtures/evidence_selection/scifact-dev.json"
_SCOPE = "scope_" + "0" * 32
_REF = ArtifactRef(family="memory", artifact_id="memory", revision=1)


class _LiteralPipeline:
    async def extract(self, request: MemoryCandidateRequest, /) -> tuple[MemoryEntryInput, ...]:
        return tuple(
            MemoryEntryInput(kind="document", text=source.content, sources=(source,))
            for source in request.sources
            if isinstance(source, ContentSource)
        )


@pytest.mark.asyncio
async def test_native_prepare_and_incremental_delivery_preserve_exact_fragment_citations() -> None:
    text = "Administrative discussion. " * 100 + "The cobalt sensor detects mercury at 37 degrees."
    async with open_builtin_runtime(BuiltinConfig(), candidate_pipeline=_LiteralPipeline()) as runtime:
        assert runtime.scopes is not None
        scope = await runtime.scopes.create(
            ScopeDraft(title="Evidence", summary="Native evidence selection", idempotency_key="evidence")
        )
        memory = runtime.memory.for_scope(scope.scope_id)
        await runtime.sources.for_scope(scope.scope_id).capture(
            CaptureSource(source_id="document", content=text, metadata={})
        )
        while (await memory.flush()).processed:
            pass
        ordinary_request = PrepareContextRequest(query="cobalt sensor mercury", max_bytes=1200)
        context = runtime.context.for_scope(scope.scope_id)
        before = await context.prepare(ordinary_request)
        request = PrepareContextRequest(
            query="cobalt sensor mercury",
            max_bytes=1200,
            evidence_selection=True,
            include_code=True,
            assembly=ContextAssembly(sections=()),
        )
        selected = await context.prepare(request)
        assert selected.content is not None
        assert "mercury at 37 degrees" in selected.content
        assert selected.content_bytes == len(selected.content.encode()) <= request.max_bytes
        assert await context.prepare(ordinary_request) == before
        packet = await context.prepare_incremental(request)
        assert isinstance(packet, ContextPack)
        assert b"".join(ContextPack.from_bytes(packet.to_bytes()).items).decode() == selected.content
        hits = await memory.search(SearchMemoryRequest(query=request.query, mode="fts", limit=16))
        result = PreparedContextBuilder().build_scopes_result(
            request=request,
            current_scope_id=scope.scope_id,
            memory_candidates=(
                PreparedMemoryCandidates(scope_id=scope.scope_id, memory_ref=hits.memory_ref, hits=hits.hits),
            ),
        )
        assert result.context == selected
        for fragment in result.fragments:
            citation = MemoryCitation(
                memory_ref=fragment.address.memory.artifact,
                entry_id=fragment.address.entry_id,
                entry_version_id=fragment.address.entry_version_id,
            )
            record = await memory.get(GetMemoryEntryRequest(citation=citation))
            assert fragment.text == record.entry.text[fragment.start : fragment.end]


def _merged(intervals):
    result = []
    for start, end in sorted(intervals):
        if result and start <= result[-1][1]:
            result[-1] = (result[-1][0], max(end, result[-1][1]))
        else:
            result.append((start, end))
    return result


def _tokens(text: str) -> Counter[str]:
    return Counter(re.findall(r"[a-z]+|\d+", text.lower()))


def _oracle_gold_wire(case):
    items = []
    for index, document in enumerate(case["documents"]):
        for start, end in case["gold"].get(document["id"], ()):
            items.append({
                "citation": {
                    "scope_id": _SCOPE,
                    "memory_ref": _REF.model_dump(mode="json"),
                    "entry_id": "mem_ent_" + f"{index:032x}",
                    "entry_version_id": "mem_ver_" + f"{index:032x}",
                },
                "content": document["text"][start:end],
                "start": start,
                "end": end,
            })
    return (
        TRUST_POLICY
        + "\n\nBEGIN_POWERCONTEXT_PREPARED_CONTEXT_V1\n"
        + json.dumps({"trust": "untrusted_history", "items": items}, ensure_ascii=False, separators=(",", ":"))
        + "\nEND_POWERCONTEXT_PREPARED_CONTEXT_V1"
    ).encode()


def test_forty_eight_public_scifact_requests_meet_recall_and_coverage_thresholds() -> None:
    cases = json.loads(_FIXTURE.read_text(encoding="utf-8"))["cases"]
    assert len(cases) == 24
    recalls = []
    complete_requests = 0
    for case in cases:
        assert len(case["documents"]) == 3
        hits = tuple(
            MemoryHit(
                memory_ref=_REF,
                entry_id="mem_ent_" + f"{index:032x}",
                entry_version_id="mem_ver_" + f"{index:032x}",
                text=document["text"],
                score=1 / (index + 1),
                matched_by=("fts",),
            )
            for index, document in enumerate(case["documents"])
        )
        by_entry = {hit.entry_id: document for hit, document in zip(hits, case["documents"], strict=True)}
        for budget in (1400, 1800):
            assert len(_oracle_gold_wire(case)) <= budget
            result = PreparedContextBuilder().build_scopes_result(
                request=PrepareContextRequest(query=case["query"], max_bytes=budget, evidence_selection=True),
                current_scope_id=_SCOPE,
                memory_candidates=(PreparedMemoryCandidates(scope_id=_SCOPE, memory_ref=_REF, hits=hits),),
            )
            assert result.context.content
            assert result.context.content_bytes == len(result.context.content.encode()) <= budget
            assert result.origins == tuple(fragment.address for fragment in result.fragments)
            selected = {}
            for fragment in result.fragments:
                document = by_entry[fragment.address.entry_id]
                assert fragment.address.memory.scope_id == _SCOPE
                assert fragment.address.memory.artifact == _REF
                assert fragment.text == document["text"][fragment.start : fragment.end]
                selected.setdefault(document["id"], []).append((fragment.start, fragment.end))
            overlap = total = 0
            complete = True
            for doc_id, gold_intervals in case["gold"].items():
                document = next(document for document in case["documents"] if document["id"] == doc_id)
                intervals = _merged(selected.get(doc_id, []))
                evidence = _tokens(" ".join(document["text"][start:end] for start, end in _merged(gold_intervals)))
                delivered = _tokens(" ".join(document["text"][start:end] for start, end in intervals))
                total += sum(evidence.values())
                overlap += sum((delivered & evidence).values())
                complete &= all(
                    any(left <= start and right >= end for left, right in intervals) for start, end in gold_intervals
                )
            assert total > 0
            recalls.append(overlap / total)
            complete_requests += int(complete)
    report = {
        "requests": len(recalls),
        "mean_evidence_token_recall": sum(recalls) / len(recalls),
        "complete_requests": complete_requests,
        "complete_evidence_fraction": complete_requests / len(recalls),
    }
    print(json.dumps(report, sort_keys=True))
    assert report["requests"] == 48
    assert report["mean_evidence_token_recall"] >= 0.65
    assert report["complete_evidence_fraction"] >= 0.4
