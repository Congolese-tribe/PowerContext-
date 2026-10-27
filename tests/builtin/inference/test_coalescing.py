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

"""Behavior of shared embedding work, cancellation, caching, and metering."""

from __future__ import annotations

import asyncio

import pytest

from powercontext.builtin.artifacts.memory import EmbeddingProfile
from powercontext.builtin.inference import CoalescingEmbeddingModel, EmbeddingResult, InferenceUsage, embed_query


def _vector(text: str) -> tuple[float, ...]:
    return (float(len(text)), float(sum(text.encode())))


class RecordingEmbedding:
    profile = EmbeddingProfile(profile_id="coalescing-test", model="offline", dimension=2)

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    async def embed(self, texts: tuple[str, ...], /) -> EmbeddingResult:
        self.calls.append(texts)
        return EmbeddingResult(
            vectors=tuple(_vector(text) for text in texts),
            usage=InferenceUsage(requests=3, input_tokens=len(texts) * 7, output_tokens=2),
        )


@pytest.mark.parametrize("coalesce_ms", [0, 5])
def test_overlapping_calls_restore_order_and_count_each_batch_once(coalesce_ms: float) -> None:
    async def scenario() -> None:
        base = RecordingEmbedding()
        model = CoalescingEmbeddingModel(base, max_batch_size=2, coalesce_ms=coalesce_ms, cache_size=4)
        assert model.profile is base.profile
        first, second = await asyncio.gather(model.embed(("a", "bb", "a", "ccc")), model.embed(("bb", "dddd", "bb")))
        assert first.vectors == tuple(_vector(text) for text in ("a", "bb", "a", "ccc"))
        assert second.vectors == tuple(_vector(text) for text in ("bb", "dddd", "bb"))
        assert base.calls == [("a", "bb"), ("ccc", "dddd")]
        assert first.usage == second.usage == InferenceUsage(requests=6, input_tokens=28, output_tokens=4)
        cached = await model.embed(("dddd", "a", "dddd"))
        assert cached.vectors == tuple(_vector(text) for text in ("dddd", "a", "dddd"))
        assert cached.usage.requests == 0
        assert len(base.calls) == 2

    asyncio.run(asyncio.wait_for(scenario(), 5))


def test_late_waiter_shares_running_work_with_cache_disabled_and_owner_cancelled() -> None:
    class Blocked(RecordingEmbedding):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def embed(self, texts: tuple[str, ...], /) -> EmbeddingResult:
            result = await super().embed(texts)
            self.started.set()
            await self.release.wait()
            return result

    async def scenario() -> None:
        base = Blocked()
        model = CoalescingEmbeddingModel(base, coalesce_ms=0, cache_size=0)
        owner = asyncio.create_task(model.embed(("a", "bb", "a")))
        await base.started.wait()
        late = asyncio.create_task(model.embed(("bb", "a", "bb")))
        await asyncio.sleep(0)
        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
        base.release.set()
        result = await late
        assert result.vectors == tuple(_vector(text) for text in ("bb", "a", "bb"))
        assert result.usage == InferenceUsage(requests=3, input_tokens=14, output_tokens=2)
        assert base.calls == [("a", "bb")]
        await model.embed(("a",))
        assert base.calls == [("a", "bb"), ("a",)]

    asyncio.run(asyncio.wait_for(scenario(), 5))


def test_cancelled_queued_caller_does_not_discard_shared_texts() -> None:
    async def scenario() -> None:
        base = RecordingEmbedding()
        model = CoalescingEmbeddingModel(base, coalesce_ms=20)
        owner = asyncio.create_task(model.embed(("queued",)))
        await asyncio.sleep(0)
        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
        result = await model.embed(("queued", "queued"))
        assert result.vectors == (_vector("queued"),) * 2
        assert base.calls == [("queued",)]

    asyncio.run(asyncio.wait_for(scenario(), 5))


def test_failure_reaches_dependent_callers_while_another_batch_finishes() -> None:
    class Flaky(RecordingEmbedding):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.fail = True

        async def embed(self, texts: tuple[str, ...], /) -> EmbeddingResult:
            result = await super().embed(texts)
            if texts == ("bad",) and self.fail:
                self.started.set()
                await self.release.wait()
                self.fail = False
                raise RuntimeError("temporary")
            return result

    async def scenario() -> None:
        base = Flaky()
        model = CoalescingEmbeddingModel(base, max_batch_size=1, coalesce_ms=0)
        mixed = asyncio.create_task(model.embed(("bad", "good")))
        await base.started.wait()
        late = asyncio.create_task(model.embed(("bad",)))
        good = await model.embed(("good",))
        assert good.vectors == (_vector("good"),)
        base.release.set()
        results = await asyncio.gather(mixed, late, return_exceptions=True)
        assert all(isinstance(result, RuntimeError) for result in results)
        assert (await model.embed(("bad", "good"))).vectors == (_vector("bad"), _vector("good"))
        assert base.calls == [("bad",), ("good",), ("bad",)]

    asyncio.run(asyncio.wait_for(scenario(), 5))


@pytest.mark.parametrize("returned_count", [0, 1, 3])
def test_invalid_vector_count_rejects_entire_batch_and_can_be_retried(returned_count: int) -> None:
    class InvalidOnce(RecordingEmbedding):
        async def embed(self, texts: tuple[str, ...], /) -> EmbeddingResult:
            result = await super().embed(texts)
            if len(self.calls) == 1:
                return EmbeddingResult(vectors=((1.0, 2.0),) * returned_count)
            return result

    async def scenario() -> None:
        base = InvalidOnce()
        model = CoalescingEmbeddingModel(base, coalesce_ms=0)
        with pytest.raises(ValueError):
            await model.embed(("a", "bb"))
        result = await model.embed(("a", "bb"))
        assert result.vectors == (_vector("a"), _vector("bb"))
        assert base.calls == [("a", "bb"), ("a", "bb")]

    asyncio.run(asyncio.wait_for(scenario(), 5))


def test_lru_refreshes_access_and_mixed_cache_hits_only_charge_new_batches() -> None:
    async def scenario() -> None:
        base = RecordingEmbedding()
        model = CoalescingEmbeddingModel(base, coalesce_ms=0, cache_size=2)
        assert (await model.embed(())).usage.requests == 0
        assert base.calls == []
        await model.embed(("a", "bb"))
        await model.embed(("a",))
        mixed = await model.embed(("ccc", "a", "ccc"))
        assert mixed.vectors == (_vector("ccc"), _vector("a"), _vector("ccc"))
        assert mixed.usage == InferenceUsage(requests=3, input_tokens=7, output_tokens=2)
        await model.embed(("a",))
        await model.embed(("bb",))
        assert base.calls == [("a", "bb"), ("ccc",), ("bb",)]

    asyncio.run(asyncio.wait_for(scenario(), 5))


@pytest.mark.parametrize("fail", [False, True])
def test_work_finishes_after_sole_waiter_cancels_and_failure_remains_retryable(fail: bool) -> None:
    class Abandoned(RecordingEmbedding):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.finished = asyncio.Event()

        async def embed(self, texts: tuple[str, ...], /) -> EmbeddingResult:
            result = await super().embed(texts)
            self.started.set()
            await self.release.wait()
            self.finished.set()
            if fail and len(self.calls) == 1:
                raise RuntimeError("abandoned")
            return result

    async def scenario() -> None:
        errors: list[dict[str, object]] = []
        asyncio.get_running_loop().set_exception_handler(lambda _loop, context: errors.append(context))
        base = Abandoned()
        model = CoalescingEmbeddingModel(base, coalesce_ms=0)
        owner = asyncio.create_task(model.embed(("alone",)))
        await base.started.wait()
        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
        base.release.set()
        await base.finished.wait()
        await asyncio.sleep(0)
        recovered = await model.embed(("alone",))
        assert recovered.vectors == (_vector("alone"),)
        assert len(base.calls) == (2 if fail else 1)
        assert recovered.usage.requests == (3 if fail else 0)
        await asyncio.sleep(0)
        assert errors == []

    asyncio.run(asyncio.wait_for(scenario(), 5))


def test_batch_identity_does_not_depend_on_provider_result_identity() -> None:
    class Reusing(RecordingEmbedding):
        result = EmbeddingResult(vectors=((1.0, 0.0),), usage=InferenceUsage(requests=2, input_tokens=3))

        async def embed(self, texts: tuple[str, ...], /) -> EmbeddingResult:
            self.calls.append(texts)
            return self.result

    async def scenario() -> None:
        result = await CoalescingEmbeddingModel(Reusing(), max_batch_size=1, coalesce_ms=0).embed(("a", "b", "a"))
        assert result.usage == InferenceUsage(requests=4, input_tokens=6)

    asyncio.run(asyncio.wait_for(scenario(), 5))


def test_unknown_token_totals_remain_unknown_across_batches() -> None:
    class PartiallyMetered(RecordingEmbedding):
        async def embed(self, texts: tuple[str, ...], /) -> EmbeddingResult:
            result = await super().embed(texts)
            if texts == ("unknown",):
                result.usage.input_tokens = None
            return result

    async def scenario() -> None:
        model = CoalescingEmbeddingModel(PartiallyMetered(), max_batch_size=1, coalesce_ms=0)
        result = await model.embed(("known", "unknown"))
        assert result.usage == InferenceUsage(requests=6, input_tokens=None, output_tokens=4)

    asyncio.run(asyncio.wait_for(scenario(), 5))


def test_query_aware_provider_has_distinct_document_and_query_vectors() -> None:
    class Asymmetric(RecordingEmbedding):
        def __init__(self) -> None:
            super().__init__()
            self.query_calls: list[tuple[str, ...]] = []

        async def embed_query(self, texts: tuple[str, ...], /) -> EmbeddingResult:
            self.query_calls.append(texts)
            return EmbeddingResult(vectors=tuple((9.0, 8.0) for _ in texts), usage=InferenceUsage(requests=1))

    async def scenario() -> None:
        base = Asymmetric()
        model = CoalescingEmbeddingModel(base, coalesce_ms=0, cache_size=2)
        document, query = await asyncio.gather(model.embed(("a",)), embed_query(model, ("a",)))
        assert document.vectors == (_vector("a"),)
        assert query.vectors == ((9.0, 8.0),)
        assert (await model.embed(("a",))).usage.requests == 0
        assert (await embed_query(model, ("a",))).usage.requests == 0
        await model.embed(("b",))
        await model.embed(("a",))
        assert base.calls == [("a",), ("b",), ("a",)]
        assert base.query_calls == [("a",)]

    asyncio.run(asyncio.wait_for(scenario(), 5))


def test_symmetric_provider_reuses_document_vectors_for_queries() -> None:
    async def scenario() -> None:
        base = RecordingEmbedding()
        model = CoalescingEmbeddingModel(base, coalesce_ms=0)
        await model.embed(("a",))
        query = await embed_query(model, ("a", "a"))
        assert query.vectors == (_vector("a"),) * 2
        assert query.usage.requests == 0
        assert base.calls == [("a",)]

    asyncio.run(asyncio.wait_for(scenario(), 5))


@pytest.mark.parametrize("value", [-1, float("inf"), float("nan")])
def test_invalid_coalescing_window_is_rejected(value: float) -> None:
    with pytest.raises(ValueError):
        CoalescingEmbeddingModel(RecordingEmbedding(), coalesce_ms=value)


def test_invalid_batch_and_cache_capacity_are_rejected() -> None:
    with pytest.raises(ValueError):
        CoalescingEmbeddingModel(RecordingEmbedding(), max_batch_size=0)
    with pytest.raises(ValueError):
        CoalescingEmbeddingModel(RecordingEmbedding(), cache_size=-1)
