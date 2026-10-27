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

"""Event-loop-local embedding request sharing and bounded vector reuse."""

from __future__ import annotations

import asyncio
import math
from collections import OrderedDict
from dataclasses import dataclass

from powercontext.builtin.inference.models import EmbeddingResult, EmbeddingVector, InferenceUsage
from powercontext.builtin.inference.protocols import EmbeddingModel, QueryEmbeddingModel, embed_query


@dataclass(eq=False, slots=True)
class _BatchResult:
    # Identity belongs to the provider invocation, even if a provider reuses its result object.
    vectors: dict[str, EmbeddingVector]
    usage: InferenceUsage


class CoalescingEmbeddingModel:
    """Share queued/in-flight texts and cache successful vectors within one event loop.

    Each caller receives full provider usage for each distinct batch it awaited.
    Cache-only calls report zero requests. Cancelling a caller leaves shared work
    running, including when it has no remaining waiters. Query-aware providers
    have separate document/query keys under the same cache capacity; symmetric
    providers share both paths.
    """

    def __init__(
        self,
        model: EmbeddingModel,
        *,
        max_batch_size: int = 64,
        coalesce_ms: float = 5.0,
        cache_size: int = 1024,
    ) -> None:
        if not isinstance(max_batch_size, int) or max_batch_size < 1:
            message = "max_batch_size must be a positive integer"
            raise ValueError(message)
        if not math.isfinite(coalesce_ms) or coalesce_ms < 0:
            message = "coalesce_ms must be finite and non-negative"
            raise ValueError(message)
        if not isinstance(cache_size, int) or cache_size < 0:
            message = "cache_size must be a non-negative integer"
            raise ValueError(message)
        self.profile = model.profile
        self._model = model
        self._max_batch_size = max_batch_size
        self._coalesce_seconds = coalesce_ms / 1000
        self._cache_size = cache_size
        self._cache: OrderedDict[tuple[bool, str], EmbeddingVector] = OrderedDict()
        self._inflight: dict[tuple[bool, str], asyncio.Future[_BatchResult]] = {}
        self._pending: dict[bool, dict[str, asyncio.Future[_BatchResult]]] = {False: {}, True: {}}
        self._flush_scheduled = False
        self._tasks: set[asyncio.Task[None]] = set()

    async def embed(self, texts: tuple[str, ...], /) -> EmbeddingResult:
        """Embed documents, preserving the input order and duplicate positions."""

        return await self._embed(texts, query=False)

    async def embed_query(self, texts: tuple[str, ...], /) -> EmbeddingResult:
        """Preserve query-specific embeddings when the delegate supports them."""

        return await self._embed(texts, query=isinstance(self._model, QueryEmbeddingModel))

    async def _embed(self, texts: tuple[str, ...], *, query: bool) -> EmbeddingResult:
        loop = asyncio.get_running_loop()
        vectors: dict[str, EmbeddingVector] = {}
        waiting: dict[str, asyncio.Future[_BatchResult]] = {}
        # Registration has no suspension point, so other calls see every missing text.
        for text in texts:
            key = (query, text)
            if key in self._cache:
                vectors[text] = self._cache[key]
                self._cache.move_to_end(key)
            else:
                future = self._inflight.get(key)
                if future is None:
                    future = loop.create_future()
                    future.add_done_callback(_observe_exception)
                    self._inflight[key] = future
                    self._pending[query][text] = future
                waiting[text] = future

        if any(self._pending.values()) and not self._flush_scheduled:
            self._flush_scheduled = True
            loop.call_later(self._coalesce_seconds, self._flush)

        results = await asyncio.gather(*(asyncio.shield(future) for future in waiting.values()))
        for text, batch in zip(waiting, results, strict=True):
            vectors[text] = batch.vectors[text]
        usages = [batch.usage for batch in dict.fromkeys(results)]
        return EmbeddingResult(
            vectors=tuple(vectors[text] for text in texts),
            usage=InferenceUsage(
                requests=sum(usage.requests for usage in usages),
                input_tokens=_token_total([usage.input_tokens for usage in usages]),
                output_tokens=_token_total([usage.output_tokens for usage in usages]),
            ),
        )

    def _flush(self) -> None:
        self._flush_scheduled = False
        for query in (False, True):
            pending = tuple(self._pending[query].items())
            self._pending[query].clear()
            for start in range(0, len(pending), self._max_batch_size):
                batch = dict(pending[start : start + self._max_batch_size])
                task = asyncio.create_task(self._run_batch(batch, query=query))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

    async def _run_batch(self, pending: dict[str, asyncio.Future[_BatchResult]], *, query: bool) -> None:
        texts = tuple(pending)
        try:
            result = await embed_query(self._model, texts) if query else await self._model.embed(texts)
            if len(result.vectors) != len(texts):
                message = "embedding vector count does not match batch text count"
                raise ValueError(message)  # noqa: TRY301 - reject the whole batch through the same failure path
        except (Exception, asyncio.CancelledError) as error:
            for future in pending.values():
                future.set_exception(error)
        else:
            batch = _BatchResult(vectors=dict(zip(texts, result.vectors, strict=True)), usage=result.usage.model_copy())
            for text, future in pending.items():
                if self._cache_size:
                    key = (query, text)
                    self._cache[key] = batch.vectors[text]
                    self._cache.move_to_end(key)
                    if len(self._cache) > self._cache_size:
                        self._cache.popitem(last=False)
                future.set_result(batch)
        finally:
            for text in texts:
                del self._inflight[query, text]


def _observe_exception(future: asyncio.Future[_BatchResult]) -> None:
    # Work can fail after every caller has cancelled; still retrieve its exception.
    if not future.cancelled():
        future.exception()


def _token_total(values: list[int | None]) -> int | None:
    """Keep unknown provider totals unknown instead of reporting partial counts."""

    if not values or any(value is None for value in values):
        return None
    return sum(value for value in values if value is not None)


__all__ = ["CoalescingEmbeddingModel"]
