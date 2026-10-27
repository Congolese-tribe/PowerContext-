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

"""Persist and propagate explicit source dependencies within one Runtime scope."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from powercontext.artifacts import ArtifactRef, MemoryCitation
from powercontext.builtin.persistence.errors import RepositoryNotFoundError
from powercontext.builtin.persistence.source_lifecycle import SourceLifecycleRepository
from powercontext.builtin.records import BaseAccessError
from powercontext.builtin.runtime.models import GetMemoryEntryRequest
from powercontext.builtin.source_lifecycle import LifecycleNode, SourceLifecycleError, SourceLifecycleEvent
from powercontext.builtin.sources import validate_scope_id
from powercontext.errors import PowerContextError
from powercontext.sources import SourceRef

if TYPE_CHECKING:
    from powercontext.builtin.runtime.application import BuiltinRuntime

__all__ = ["SourceLifecycleError", "SourceLifecycleEvent"]


class ScopedSourceLifecycle:
    """Single-writer dependency DAG with retryable native state projections."""

    def __init__(self, runtime: BuiltinRuntime, repository: SourceLifecycleRepository, scope_id: str) -> None:
        self._runtime = runtime
        self._database = repository.database
        self.scope_id = validate_scope_id(scope_id)
        self._repository = repository

    async def register_source(self, node_id: str, source_ref: SourceRef, *, revision: int) -> None:
        _require_node_id(node_id)
        if type(revision) is not int or revision <= 0:
            raise SourceLifecycleError("revision must be a positive integer")  # noqa: TRY003
        if not isinstance(source_ref, SourceRef):
            raise SourceLifecycleError("source_ref must be a SourceRef")  # noqa: TRY003
        await self._register(LifecycleNode(node_id=node_id, kind="source", source_ref=source_ref, revision=revision))

    async def register_memory(
        self, node_id: str, citation: MemoryCitation, *, parents: tuple[str, ...], policy: Literal["all", "any"]
    ) -> None:
        _require_node_id(node_id)
        _require_dependencies(node_id, parents, policy)
        if not isinstance(citation, MemoryCitation):
            raise SourceLifecycleError("citation must be a MemoryCitation")  # noqa: TRY003
        await self._register(
            LifecycleNode(
                node_id=node_id, kind="memory", citation=citation, parents=tuple(sorted(parents)), policy=policy
            )
        )

    async def register_topic(
        self, node_id: str, artifact_ref: ArtifactRef, *, parents: tuple[str, ...], policy: Literal["all", "any"]
    ) -> None:
        _require_node_id(node_id)
        _require_dependencies(node_id, parents, policy)
        if not isinstance(artifact_ref, ArtifactRef) or artifact_ref.family != "topic-memory":
            raise SourceLifecycleError("artifact_ref must reference Topic Memory")  # noqa: TRY003
        await self._register(
            LifecycleNode(
                node_id=node_id, kind="topic", topic_ref=artifact_ref, parents=tuple(sorted(parents)), policy=policy
            )
        )

    async def _register(self, node: LifecycleNode) -> None:
        async with self._runtime._scope_operation(self.scope_id), self._runtime._locked(self.scope_id):
            nodes = await self._load()
            previous = nodes.get(node.node_id)
            if previous is not None:
                if previous.model_copy(update={"available": True, "event": None}) != node:
                    raise SourceLifecycleError("node is already registered with a different binding")  # noqa: TRY003
            elif any(parent not in nodes for parent in node.parents):
                raise SourceLifecycleError("parents must already be registered")  # noqa: TRY003
            # Parent-first insertion and immutable bindings make cycles impossible.
            await self._require_readable(node)
            if previous is None:
                if node.parents:
                    node = node.model_copy(update={"available": _supported(node, nodes)})
                async with self._database.transaction() as connection:
                    await self._repository.save(connection, self.scope_id, node, create=True)
                nodes[node.node_id] = node
            await self._project_memories(nodes, reason="source lifecycle registration")

    async def _require_readable(self, node: LifecycleNode) -> None:
        try:
            if node.source_ref is not None:
                async with self._database.transaction() as connection:
                    await self._repository.sources.get(connection, self.scope_id, node.source_ref)
            elif node.citation is not None:
                await self._runtime.memory.for_scope(self.scope_id).get(GetMemoryEntryRequest(citation=node.citation))
            elif node.topic_ref is not None:
                ref = node.topic_ref
                await self._runtime.records.for_scope(self.scope_id).get_artifact_revision(
                    "topic-memory", ref.artifact_id, ref.revision
                )
        except (RepositoryNotFoundError, PowerContextError, BaseAccessError) as error:
            raise SourceLifecycleError("registered reference must exist and be readable in this scope") from error  # noqa: TRY003

    async def apply(self, event: SourceLifecycleEvent) -> list[str]:
        if not isinstance(event, SourceLifecycleEvent):
            raise SourceLifecycleError("event must be a SourceLifecycleEvent")  # noqa: TRY003
        async with self._runtime._scope_operation(self.scope_id), self._runtime._locked(self.scope_id):
            nodes = await self._load()
            source = nodes.get(event.node_id)
            if source is None or source.kind != "source":
                raise SourceLifecycleError("events require a registered Source")  # noqa: TRY003
            if source.revision != event.revision:
                raise SourceLifecycleError("event revision does not match the source lifecycle revision")  # noqa: TRY003
            if source.event is not None and (
                event.sequence < source.event.sequence
                or (event.sequence == source.event.sequence and event != source.event)
            ):
                raise SourceLifecycleError("event sequence is stale or conflicts with the accepted event")  # noqa: TRY003
            before = dict(nodes)
            nodes[source.node_id] = source.model_copy(update={"available": event.available, "event": event})
            _propagate(nodes)
            # Commit intent and every desired state together. A failed native write leaves
            # this exact event replayable, even after reopening the database.
            async with self._database.transaction() as connection:
                for node_id, node in nodes.items():
                    if node != before[node_id]:
                        await self._repository.save(connection, self.scope_id, node, create=False)
            await self._project_memories(nodes, reason=event.reason)
            return sorted(node_id for node_id, node in nodes.items() if node.available)

    async def resolve(
        self, node_id: str, *, purpose: Literal["generation", "audit"]
    ) -> SourceRef | MemoryCitation | ArtifactRef:
        _require_node_id(node_id)
        if purpose not in ("generation", "audit"):
            raise SourceLifecycleError("unknown lifecycle purpose")  # noqa: TRY003
        async with self._runtime._scope_operation(self.scope_id):
            node = (await self._load()).get(node_id)
            if node is None:
                raise SourceLifecycleError("unknown lifecycle node")  # noqa: TRY003
            if purpose == "generation" and not node.available:
                raise SourceLifecycleError("node has no available supporting evidence")  # noqa: TRY003
            return node.reference()

    async def _load(self) -> dict[str, LifecycleNode]:
        async with self._database.transaction() as connection:
            return await self._repository.load(connection, self.scope_id)

    async def _project_memories(self, nodes: dict[str, LifecycleNode], *, reason: str) -> None:
        memories = [node for node in nodes.values() if node.citation is not None]
        if not memories:
            return
        async with self._runtime._context(self.scope_id) as context:
            service = context.artifacts.memory
            current = await service.head(context.artifacts.memory_artifact_id)
            versions = {entry.entry_id: entry for entry in await service.entries(current)}
            for node in memories:
                citation = node.citation
                if citation is None:
                    continue
                entry = versions.get(citation.entry_id)
                if entry is None or entry.entry_version_id != citation.entry_version_id:
                    raise SourceLifecycleError("registered Memory body version has changed")  # noqa: TRY003
                state = next(item.state for item in current.content.manifest.entries if item.entry_id == entry.entry_id)
                if (state == "active") != node.available:
                    operation = service.reactivate if node.available else service.forget
                    current = await operation(current, entries=(entry,), reason=reason)


class SourceLifecycleApplication:
    """Select one persistent lifecycle graph in the composed deployment."""

    def __init__(self, runtime: BuiltinRuntime, repository: SourceLifecycleRepository | None) -> None:
        self._runtime = runtime
        self._repository = repository

    def for_scope(self, scope_id: str) -> ScopedSourceLifecycle:
        if self._repository is None:
            raise SourceLifecycleError("source lifecycle persistence is not configured")  # noqa: TRY003
        return ScopedSourceLifecycle(self._runtime, self._repository, scope_id)


def _require_node_id(node_id: str) -> None:
    if not isinstance(node_id, str) or not node_id.strip():
        raise SourceLifecycleError("node_id must contain non-whitespace text")  # noqa: TRY003


def _require_dependencies(node_id: str, parents: tuple[str, ...], policy: str) -> None:
    if policy not in ("all", "any"):
        raise SourceLifecycleError("policy must be all or any")  # noqa: TRY003
    if not isinstance(parents, (tuple, list)) or not parents:
        raise SourceLifecycleError("derived nodes require parents")  # noqa: TRY003
    for parent in parents:
        _require_node_id(parent)
    if len(set(parents)) != len(parents) or node_id in parents:
        raise SourceLifecycleError("parents must be distinct and cannot include the node itself")  # noqa: TRY003


def _supported(node: LifecycleNode, nodes: dict[str, LifecycleNode]) -> bool:
    states = (nodes[parent].available for parent in node.parents)
    return all(states) if node.policy == "all" else any(states)


def _propagate(nodes: dict[str, LifecycleNode]) -> None:
    # Scope graphs contain 16-20 nodes. Traverse only this scope in O(V+E),
    # without depending on database row order or scanning other scopes.
    visited: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visited:
            return
        node = nodes[node_id]
        for parent in node.parents:
            visit(parent)
        if node.parents:
            nodes[node_id] = node.model_copy(update={"available": _supported(node, nodes)})
        visited.add(node_id)

    for node_id in nodes:
        visit(node_id)
