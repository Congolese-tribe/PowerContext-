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

"""Batched HTTP scoring with stable selection from the complete Memory pool."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from powercontext.builtin.artifacts.memory.models import MemoryHit
from powercontext.builtin.artifacts.memory.reranking import MemoryRerankDecision
from powercontext.builtin.inference import InferenceUsage

if TYPE_CHECKING:
    import httpx


class _Score(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    rank: int
    score: float = Field(allow_inf_nan=False)


class _ScoreResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    scores: list[_Score]


class HTTPMemoryReranker:
    """Score each candidate once and return original ranks in global score order.

    The caller owns the supplied client and its lifetime. HTTP failures propagate
    as httpx exceptions; invalid responses fail with ValueError without fallback
    or retries. Scores only select hits and never replace stored text or identity.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        client: httpx.AsyncClient,
        batch_size: int = 32,
        policy_id: str = "http-cross-encoder-v1",
    ) -> None:
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")  # noqa: TRY003
        self._endpoint = endpoint
        self._client = client
        self._batch_size = batch_size
        self.policy_id = policy_id

    async def rerank(
        self,
        query: str,
        candidates: tuple[MemoryHit, ...],
        limit: int,
        /,
    ) -> MemoryRerankDecision:
        """Validate every batch before selecting the globally best ``limit`` hits."""

        if not candidates:
            raise ValueError("rerank requires at least one candidate")  # noqa: TRY003
        if type(limit) is not int or not 1 <= limit <= len(candidates):
            raise ValueError("rerank limit must be between one and the candidate count")  # noqa: TRY003

        scores: dict[int, float] = {}
        requests = 0
        for start in range(0, len(candidates), self._batch_size):
            batch = candidates[start : start + self._batch_size]
            response = await self._client.post(
                self._endpoint,
                json={
                    "query": query,
                    "candidates": [
                        {"rank": rank, "text": candidate.text} for rank, candidate in enumerate(batch, start=start + 1)
                    ],
                },
                follow_redirects=False,
            )
            requests += 1
            response.raise_for_status()
            result = _ScoreResponse.model_validate_json(response.content)
            batch_scores = {item.rank: item.score for item in result.scores}
            if len(batch_scores) != len(result.scores) or set(batch_scores) != set(
                range(start + 1, start + len(batch) + 1)
            ):
                raise ValueError("scores must cover each rank in the request batch exactly once")  # noqa: TRY003
            scores.update(batch_scores)

        ordered_ranks = sorted(scores, key=lambda rank: (-scores[rank], rank))
        return MemoryRerankDecision(
            selected_ranks=tuple(ordered_ranks[:limit]),
            usage=InferenceUsage(requests=requests),
        )


__all__ = ["HTTPMemoryReranker"]
