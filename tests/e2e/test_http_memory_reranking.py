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

"""HTTP score selection through native Memory persistence and search."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx

from powercontext.builtin.artifacts.memory import HTTPMemoryReranker, MemoryEntryInput
from powercontext.builtin.persistence.sqlite import SQLiteConfig
from powercontext.builtin.runtime import BuiltinConfig, open_builtin_contexts


def test_http_reranking_preserves_exact_memory_entries_and_usage() -> None:
    seen: list[dict[str, Any]] = []
    scores = {1: 0.1, 2: 0.9, 3: 0.95, 4: 0.9, 5: -0.1}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(body)
        return httpx.Response(
            200,
            json={
                "scores": [
                    {"rank": item["rank"], "score": scores[item["rank"]]} for item in reversed(body["candidates"])
                ]
            },
        )

    async def scenario() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            reranker = HTTPMemoryReranker(
                "https://reranker.invalid/score", client=client, batch_size=2, policy_id="test-http-policy"
            )
            async with open_builtin_contexts(
                BuiltinConfig(database=SQLiteConfig()), memory_reranker=reranker
            ) as contexts:
                service = (await contexts.get("http-reranking")).artifacts.memory
                first = await service.remember(
                    memory=None,
                    entries=tuple(MemoryEntryInput(kind="fact", text=f"Project fact {number}.") for number in range(4)),
                    mode="append",
                )
                assert first is not None
                revised = await service.remember(
                    memory=first,
                    entries=(MemoryEntryInput(kind="fact", text="Project appended fact."),),
                    mode="append",
                )
                assert revised is not None
                before = await service.entries(revised)
                result = await service.search("project", memories=(revised,), limit=3, mode="fts")

                assert result.rerank is not None
                trace = result.rerank
                assert len(trace.candidate_hits) == 5
                assert trace.selected_ranks == (3, 2, 4)
                assert result.hits == tuple(trace.candidate_hits[rank - 1] for rank in (3, 2, 4))
                assert trace.policy_id == "test-http-policy"
                assert trace.usage.requests == len(seen) == 3
                assert sorted(item["rank"] for body in seen for item in body["candidates"]) == [1, 2, 3, 4, 5]
                for body in seen:
                    assert body["query"] == "project"
                    assert len(body["candidates"]) <= 2
                    for item in body["candidates"]:
                        assert item["text"] == trace.candidate_hits[item["rank"] - 1].text
                expanded = await service.expand(result.hits)
                for hit, entry in zip(result.hits, expanded, strict=True):
                    assert hit.memory_ref == revised.as_ref()
                    assert entry.entry_id == hit.entry_id
                    assert entry.entry_version_id == hit.entry_version_id
                    assert entry.text == hit.text
                    assert entry in before
                assert await service.entries(revised) == before
                assert await service.revision(first.as_ref()) == first
                assert await service.revision(revised.as_ref()) == revised

    asyncio.run(scenario())
