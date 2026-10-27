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

"""Source lifecycle acceptance through persistent Runtime and native retrieval."""

from __future__ import annotations

import asyncio

import pytest
from pydantic import ValidationError

from powercontext.artifacts import ArtifactRef
from powercontext.builtin.artifacts.memory import EmbeddingProfile, MemoryEntryInput, MemoryService
from powercontext.builtin.inference import EmbeddingResult
from powercontext.builtin.persistence.sqlite import SQLiteConfig
from powercontext.builtin.records import ArtifactWrite
from powercontext.builtin.runtime import (
    BuiltinConfig,
    CaptureSource,
    GetMemoryEntryRequest,
    RememberMemoryRequest,
    SearchMemoryRequest,
    SearchTopicMemoryRequest,
    open_builtin_runtime,
)
from powercontext.builtin.runtime.source_lifecycle import SourceLifecycleError, SourceLifecycleEvent
from powercontext.builtin.scope import ScopeDraft
from powercontext.sources import SourceRef


def _event():
    return SourceLifecycleEvent(node_id="source", revision=3, sequence=1, available=False, reason="withdrawn")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("node_id", ""),
        ("node_id", " \t"),
        ("node_id", 1),
        ("node_id", b"source"),
        ("reason", ""),
        ("reason", "\n"),
        ("reason", None),
        ("reason", 1),
        ("revision", 0),
        ("revision", -1),
        ("revision", True),
        ("revision", 1.0),
        ("revision", "3"),
        ("sequence", 0),
        ("sequence", -1),
        ("sequence", False),
        ("sequence", 1.0),
        ("sequence", "1"),
        ("available", 0),
        ("available", 1),
        ("available", "false"),
        ("available", None),
        ("extra", True),
    ],
)
def test_event_rejects_invalid_fields(field, value):
    data = {"node_id": "source", "revision": 3, "sequence": 1, "available": False, "reason": "withdrawn"}
    data[field] = value
    with pytest.raises(ValidationError):
        SourceLifecycleEvent(**data)


async def _graph(runtime, key):
    scope = await runtime.scopes.create(ScopeDraft(title="Lifecycle", summary="Evidence", idempotency_key=key))
    service = runtime.lifecycle.for_scope(scope.scope_id)
    receipt = await runtime.sources.for_scope(scope.scope_id).capture(
        CaptureSource(source_id="evidence", content="captured evidence", metadata={})
    )
    await service.register_source("source", receipt.source_ref, revision=3)
    citations = []
    for name in ("alpha", "beta", "gamma"):
        result = await runtime.memory.for_scope(scope.scope_id).remember(
            RememberMemoryRequest(entries=(MemoryEntryInput(kind="fact", text=f"{name} evidence"),))
        )
        citations.append(result.entry.citation)
        await service.register_memory(name, result.entry.citation, parents=("source",), policy="all")
    artifact = await runtime.records.for_scope(scope.scope_id).create_artifact(
        "topic-memory", ArtifactWrite(content={"title": "lifecycle", "summary": "topic evidence", "detail": "detail"})
    )
    topic = ArtifactRef(family="topic-memory", artifact_id=artifact.artifact_id, revision=artifact.revision)
    await service.register_topic("topic", topic, parents=("alpha", "beta"), policy="all")
    return scope.scope_id, service, receipt.source_ref, citations, topic


def test_registration_identity_readability_and_resolution():
    async def run():
        async with open_builtin_runtime(BuiltinConfig(database=SQLiteConfig())) as runtime:
            scope, service, source, citations, topic = await _graph(runtime, "identities")
            original = await runtime.memory.for_scope(scope).list()
            await service.register_source("source", source, revision=3)
            await service.register_memory("alpha", citations[0], parents=("source",), policy="all")
            await service.register_topic("topic", topic, parents=("beta", "alpha"), policy="all")
            assert (await runtime.memory.for_scope(scope).list()).memory_ref == original.memory_ref
            for revision in (0, -1, True, 1.0, "3"):
                with pytest.raises(SourceLifecycleError):
                    await service.register_source("invalid", source, revision=revision)
            for node_id in ("", " \n", None, 1):
                with pytest.raises(SourceLifecycleError):
                    await service.register_source(node_id, source, revision=3)
            with pytest.raises(SourceLifecycleError):
                await service.register_source("source", source, revision=4)
            with pytest.raises(SourceLifecycleError):
                await service.register_source("alpha", source, revision=3)
            with pytest.raises(SourceLifecycleError):
                await service.register_source(
                    "missing", SourceRef(source_type="content", source_id="missing"), revision=3
                )
            invalid_citations = (
                citations[0].model_copy(update={"entry_version_id": "missing"}),
                citations[0].model_copy(
                    update={"memory_ref": citations[0].memory_ref.model_copy(update={"revision": 999})}
                ),
            )
            for citation in invalid_citations:
                with pytest.raises(SourceLifecycleError):
                    await service.register_memory("missing", citation, parents=("source",), policy="all")
            for ref in (topic.model_copy(update={"revision": 999}), topic.model_copy(update={"family": "memory"})):
                with pytest.raises(SourceLifecycleError):
                    await service.register_topic("missing", ref, parents=("source",), policy="all")
            for parents, policy in (
                ((), "any"),
                (("missing",), "all"),
                (("source", "source"), "any"),
                (("new",), "all"),
                (("source",), "invalid"),
            ):
                with pytest.raises(SourceLifecycleError):
                    await service.register_memory("new", citations[0], parents=parents, policy=policy)
            with pytest.raises(SourceLifecycleError):
                await service.register_memory("alpha", citations[1], parents=("source",), policy="all")
            with pytest.raises(SourceLifecycleError):
                await service.register_topic("topic", topic, parents=("source",), policy="any")
            for purpose in ("unknown", "", None):
                with pytest.raises(SourceLifecycleError):
                    await service.resolve("alpha", purpose=purpose)
            with pytest.raises(SourceLifecycleError):
                await service.resolve("missing", purpose="audit")
            await service.apply(_event())
            inactive = (await runtime.memory.for_scope(scope).list()).memory_ref
            await service.register_source("source", source, revision=3)
            await service.register_memory("alpha", citations[0], parents=("source",), policy="all")
            assert (await runtime.memory.for_scope(scope).list()).memory_ref == inactive
            for node in ("source", "alpha", "topic"):
                with pytest.raises(SourceLifecycleError):
                    await service.resolve(node, purpose="generation")
            assert await service.resolve("source", purpose="audit") == source
            assert await service.resolve("alpha", purpose="audit") == citations[0]
            assert await service.resolve("topic", purpose="audit") == topic
            for event in (
                SourceLifecycleEvent(node_id="alpha", revision=3, sequence=2, available=True, reason="invalid"),
                SourceLifecycleEvent(node_id="source", revision=3, sequence=1, available=False, reason="different"),
            ):
                with pytest.raises(SourceLifecycleError):
                    await service.apply(event)

    asyncio.run(run())


@pytest.mark.parametrize("after_commit", [False, True])
@pytest.mark.parametrize("available", [False, True])
def test_interrupted_native_writes_resume_after_restart_without_duplicate_revisions(
    tmp_path, monkeypatch, after_commit, available
):
    async def run():
        config = BuiltinConfig(database=SQLiteConfig(url=f"sqlite+aiosqlite:///{tmp_path / 'retry.db'}"))
        async with open_builtin_runtime(config) as runtime:
            scope, service, _, citations, topic = await _graph(runtime, "retry")
            if available:
                await service.apply(_event())
            operation_name = "reactivate" if available else "forget"
            operation = getattr(MemoryService, operation_name)
            attempts = 0

            async def interrupted(self, *args, **kwargs):
                nonlocal attempts
                attempts += 1
                if attempts == 2 and not after_commit:
                    raise OSError("simulated native state write interruption")  # noqa: TRY003
                result = await operation(self, *args, **kwargs)
                if attempts == 2:
                    raise OSError("simulated lost acknowledgement after commit")  # noqa: TRY003
                return result

            change = SourceLifecycleEvent(
                node_id="source", revision=3, sequence=2, available=available, reason="state change"
            )
            before = (await runtime.memory.for_scope(scope).list()).memory_ref
            with monkeypatch.context() as patch:
                patch.setattr(MemoryService, operation_name, interrupted)
                with pytest.raises(OSError):
                    await service.apply(change)
        async with open_builtin_runtime(config) as runtime:
            service = runtime.lifecycle.for_scope(scope)
            active = await service.apply(change)
            assert active == (["alpha", "beta", "gamma", "source", "topic"] if available else [])
            memory = runtime.memory.for_scope(scope)
            after = (await memory.list(include_inactive=True)).memory_ref
            assert before is not None and after is not None
            assert after.revision == before.revision + 3
            assert {entry.state for entry in (await memory.list(include_inactive=True)).entries} == {
                "active" if available else "inactive"
            }
            for citation in citations:
                assert (await memory.get(GetMemoryEntryRequest(citation=citation))).citation == citation
            assert await service.resolve("topic", purpose="audit") == topic
            for _ in range(2):
                assert await service.apply(change) == active
                assert (await memory.list()).memory_ref == after
            assert bool((await memory.search(SearchMemoryRequest(query="alpha", mode="fts"))).hits) == available
            assert (
                bool((await runtime.topic_memory.for_scope(scope).search(SearchTopicMemoryRequest(query="topic"))).hits)
                == available
            )

    asyncio.run(run())


def test_scope_isolation_and_topic_parent_support(tmp_path):
    async def run():
        config = BuiltinConfig(database=SQLiteConfig(url=f"sqlite+aiosqlite:///{tmp_path / 'scopes.db'}"))
        async with open_builtin_runtime(config) as runtime:
            left, service, _, citations, _ = await _graph(runtime, "left")
            right, other, _, _, _ = await _graph(runtime, "right")
            # A Topic can support another Memory; database ordering must not decide traversal.
            await service.register_memory("aardvark", citations[2], parents=("topic",), policy="any")
            with pytest.raises(SourceLifecycleError):
                await other.register_memory("foreign", citations[0], parents=("source",), policy="all")
            await service.apply(_event())
            assert len((await runtime.memory.for_scope(right).list()).entries) == 3
            assert (await runtime.memory.for_scope(left).list()).entries == ()
        async with open_builtin_runtime(config) as runtime:
            service = runtime.lifecycle.for_scope(left)
            with pytest.raises(SourceLifecycleError):
                await service.resolve("aardvark", purpose="generation")
            assert await runtime.lifecycle.for_scope(right).resolve("alpha", purpose="generation")
            restored = await service.apply(
                SourceLifecycleEvent(node_id="source", revision=3, sequence=2, available=True, reason="restored")
            )
            assert "aardvark" in restored
            assert len((await runtime.memory.for_scope(left).list()).entries) == 3
            assert await service.resolve("aardvark", purpose="generation") == citations[2]

    asyncio.run(run())


class _Embeddings:
    profile = EmbeddingProfile(profile_id="lifecycle", model="local-test", dimension=3)

    async def embed(self, texts, /):
        return EmbeddingResult(vectors=tuple((1.0, 0.0, 0.0) for _ in texts))


def test_topic_lifecycle_filters_every_native_search_channel(tmp_path):
    async def run():
        config = BuiltinConfig(database=SQLiteConfig(url=f"sqlite+aiosqlite:///{tmp_path / 'vectors.db'}"))
        async with open_builtin_runtime(config, embedding_model=_Embeddings()) as runtime:
            scope, lifecycle, _, _, reference = await _graph(runtime, "vectors")
            topics = runtime.topic_memory.for_scope(scope)
            # Exact Topic and detail terms exercise both FTS channels;
            # hybrid search also executes both native sqlite-vec channels.
            for available, sequence in ((False, 1), (True, 2)):
                await lifecycle.apply(
                    SourceLifecycleEvent(
                        node_id="source",
                        revision=3,
                        sequence=sequence,
                        available=available,
                        reason="channel eligibility",
                    )
                )
                for allow_embedding in (False, True):
                    for query in ("topic", "detail"):
                        page = await topics.search(
                            SearchTopicMemoryRequest(query=query), allow_embedding=allow_embedding
                        )
                        assert page.mode == ("hybrid" if allow_embedding else "fts")
                        assert bool(page.hits) == available
                        if available:
                            assert page.hits[0].artifact_ref == reference
                            if allow_embedding:
                                assert {"topic_vector", "detail_vector"} <= set(page.hits[0].matched_by)
                exact = await runtime.records.for_scope(scope).get_artifact_revision(
                    "topic-memory", reference.artifact_id, reference.revision
                )
                assert exact.content["detail"] == "detail"

    asyncio.run(run())


def test_failed_registration_can_be_retried_after_restart(tmp_path, monkeypatch):
    async def run():
        config = BuiltinConfig(database=SQLiteConfig(url=f"sqlite+aiosqlite:///{tmp_path / 'registration.db'}"))
        async with open_builtin_runtime(config) as runtime:
            scope, lifecycle, _, _, _ = await _graph(runtime, "registration")
            await lifecycle.apply(_event())
            result = await runtime.memory.for_scope(scope).remember(
                RememberMemoryRequest(entries=(MemoryEntryInput(kind="fact", text="late evidence"),))
            )
            assert result.entry is not None
            citation = result.entry.citation

            async def fail(*args, **kwargs):
                raise OSError("interrupted")

            with monkeypatch.context() as patch:
                patch.setattr(MemoryService, "forget", fail)
                with pytest.raises(OSError):
                    await lifecycle.register_memory("late", citation, parents=("source",), policy="all")
        async with open_builtin_runtime(config) as runtime:
            lifecycle = runtime.lifecycle.for_scope(scope)
            await lifecycle.register_memory("late", citation, parents=("source",), policy="all")
            memory = runtime.memory.for_scope(scope)
            assert (await memory.list()).entries == ()
            before = (await memory.list()).memory_ref
            await lifecycle.apply(_event())
            await lifecycle.apply(
                SourceLifecycleEvent(
                    node_id="source", revision=3, sequence=2, available=False, reason="still withdrawn"
                )
            )
            assert (await memory.list()).memory_ref == before
            assert await lifecycle.resolve("late", purpose="audit") == citation

    asyncio.run(run())
