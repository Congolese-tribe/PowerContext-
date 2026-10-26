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

"""Observable batching, validation, ordering, and HTTP error behavior."""

from __future__ import annotations

import asyncio
import json
import math
from typing import Any

import httpx
import pytest

from powercontext.artifacts import ArtifactRef
from powercontext.builtin.artifacts.memory import HTTPMemoryReranker, MemoryHit


def _hit(rank: int) -> MemoryHit:
    return MemoryHit(
        memory_ref=ArtifactRef(family="memory", artifact_id=f"memory-{rank % 2}", revision=rank),
        entry_id=f"entry-{rank}",
        entry_version_id=f"version-{rank}",
        text=f"数据库连接超时 candidate {rank // 2}",
        score=1 / rank,
        matched_by=("fts",),
    )


@pytest.mark.parametrize("count,batch_size,limit", [(1, 32, 1), (4, 2, 2), (6, 2, 6), (65, 32, 10), (173, 7, 10)])
def test_global_selection_covers_all_candidates_and_counts_each_call(count: int, batch_size: int, limit: int) -> None:
    candidates = tuple(_hit(rank) for rank in range(1, count + 1))
    before = tuple(hit.model_dump() for hit in candidates)
    scores = {rank: (rank % 5) - 2 for rank in range(1, count + 1)}
    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert request.method == "POST"
        assert str(request.url) == "https://reranker.invalid/score"
        assert set(body) == {"query", "candidates"}
        assert body["query"] == "数据库连接超时怎么排查"
        assert 1 <= len(body["candidates"]) <= batch_size
        for item in body["candidates"]:
            assert set(item) == {"rank", "text"}
            assert type(item["rank"]) is int
            assert item["text"] == candidates[item["rank"] - 1].text
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
            reranker = HTTPMemoryReranker("https://reranker.invalid/score", client=client, batch_size=batch_size)
            assert reranker.policy_id == "http-cross-encoder-v1"
            for _ in range(2):
                seen.clear()
                decision = await reranker.rerank("数据库连接超时怎么排查", candidates, limit)
                assert decision.selected_ranks == tuple(sorted(scores, key=lambda rank: (-scores[rank], rank))[:limit])
                assert decision.usage.requests == len(seen) == math.ceil(count / batch_size)
                assert sorted(item["rank"] for body in seen for item in body["candidates"]) == list(range(1, count + 1))
                assert decision.discarded_rank_count == 0
                assert not decision.used_fallback
                assert not client.is_closed
        assert tuple(hit.model_dump() for hit in candidates) == before

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"scores": [], "extra": 1},
        {"scores": None},
        {"scores": {}},
        {"scores": []},
        {"scores": [None]},
        {"scores": [{}]},
        {"scores": [{"rank": 1}]},
        {"scores": [{"score": 0.5}]},
        {"scores": [{"rank": 1, "score": 0.5, "extra": 1}]},
        *({"scores": [{"rank": rank, "score": 0.5}]} for rank in (True, False, "1", 1.0, 0, -1, 2, None)),
        *(
            {"scores": [{"rank": 1, "score": score}]}
            for score in (True, False, "0.5", None, [], {}, float("nan"), float("inf"), -float("inf"))
        ),
        {"scores": [{"rank": 1, "score": 0.5}, {"rank": 1, "score": 0.4}]},
    ],
)
def test_rejects_invalid_response_schema(payload: object) -> None:
    async def scenario() -> None:
        transport = httpx.MockTransport(lambda _request: httpx.Response(200, content=json.dumps(payload).encode()))
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(ValueError):
                await HTTPMemoryReranker("https://reranker.invalid", client=client).rerank("query", (_hit(1),), 1)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "content", [b"", b"not json", b'{"scores":', b"\xff", b'{"scores":[{"rank":1,"score":1e999}]}']
)
def test_rejects_invalid_json_and_overflowing_scores(content: bytes) -> None:
    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, content=content))
        ) as client:
            with pytest.raises(ValueError):
                await HTTPMemoryReranker("https://reranker.invalid", client=client).rerank("query", (_hit(1),), 1)

    asyncio.run(scenario())


def test_rejects_a_rank_from_another_batch() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        body = json.loads(request.content)
        ranks = [item["rank"] for item in body["candidates"]]
        if len(seen) == 2:
            ranks[0] = 1
        return httpx.Response(200, json={"scores": [{"rank": rank, "score": 0.5} for rank in ranks]})

    async def scenario() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            reranker = HTTPMemoryReranker("https://reranker.invalid", client=client, batch_size=2)
            with pytest.raises(ValueError, match="each rank"):
                await reranker.rerank("query", tuple(_hit(rank) for rank in range(1, 4)), 1)

    asyncio.run(scenario())


@pytest.mark.parametrize("batch_size", [0, -1, True, 1.5, "2", None])
def test_rejects_invalid_batch_size(batch_size: int) -> None:
    async def scenario() -> None:
        async with httpx.AsyncClient() as client:
            with pytest.raises(ValueError):
                HTTPMemoryReranker("https://reranker.invalid", client=client, batch_size=batch_size)

    asyncio.run(scenario())


@pytest.mark.parametrize("count,limit", [(0, 1), (2, 0), (2, -1), (2, 3), (2, True), (2, 1.5), (2, "1"), (2, None)])
def test_rejects_invalid_candidate_bounds_before_requesting(count: int, limit: int) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid bounds must fail before making a request")

    async def scenario() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(ValueError):
                await HTTPMemoryReranker("https://reranker.invalid", client=client).rerank(
                    "query", tuple(_hit(rank) for rank in range(1, count + 1)), limit
                )

    asyncio.run(scenario())


@pytest.mark.parametrize("status", [199, 302, 400, 429, 500])
def test_preserves_http_status_errors_without_following_redirects(status: int) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status, headers={"location": "https://reranker.invalid/redirected"}, content=b"not json")

    async def scenario() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True) as client:
            with pytest.raises(httpx.HTTPStatusError) as error:
                await HTTPMemoryReranker("https://reranker.invalid", client=client).rerank("query", (_hit(1),), 1)
            assert error.value.response.status_code == status
            assert len(seen) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("error_type", [httpx.ConnectError, httpx.ReadTimeout])
def test_preserves_transport_exceptions(error_type: type[httpx.RequestError]) -> None:
    failure = error_type("offline")

    def handler(_request: httpx.Request) -> httpx.Response:
        raise failure

    async def scenario() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(error_type) as error:
                await HTTPMemoryReranker("https://reranker.invalid", client=client).rerank("query", (_hit(1),), 1)
            assert error.value is failure

    asyncio.run(scenario())
